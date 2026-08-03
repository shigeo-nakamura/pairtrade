use super::{
    ArcusSpotChainClient, ArcusSpotChainPreflightRequest, ArcusSpotExecutionAttempt,
    ArcusSpotExecutionIntent, ArcusSpotExecutionLedger, ArcusSpotExecutionLedgerStore,
    ArcusSpotExecutionPhase, ArcusSpotRotationPlan,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use dex_connector::{
    sign_arcus_spot_quote, ArcusSpotClient, ArcusSpotQuoteRoutePolicy,
    ArcusSpotSignableQuoteRequest, ArcusSpotSubmitError,
};
use ethers::{
    signers::Signer,
    types::{Address, H256, U256},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display, str::FromStr};

const ARCUS_VENUE: &str = "arcus";
const CANONICAL_PERMIT2: &str = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const HARD_MAX_DAILY_SWAPS: u32 = 10;
const HARD_MAX_SLIPPAGE_BPS: u32 = 100;
const HARD_MAX_PLAN_AGE_SECS: u64 = 60;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotLiveExecutorConfig {
    pub taker: String,
    pub permit2: String,
    pub slippage_bps: u32,
    pub minimum_gas_balance_wei: String,
    pub inventory_floor_raw: BTreeMap<String, String>,
    pub maximum_sell_amount_raw: BTreeMap<String, String>,
    pub max_swaps_per_utc_day: u32,
    pub max_plan_age_secs: u64,
}

impl ArcusSpotLiveExecutorConfig {
    pub fn validate(&self) -> Result<(Address, Address)> {
        let taker = parse_nonzero_address("taker", &self.taker)?;
        let permit2 = parse_nonzero_address("Permit2", &self.permit2)?;
        let canonical = Address::from_str(CANONICAL_PERMIT2).expect("valid Permit2 constant");
        if permit2 != canonical {
            bail!("Arcus live executor requires the canonical Permit2 address");
        }
        if self.slippage_bps > HARD_MAX_SLIPPAGE_BPS {
            bail!(
                "Arcus live slippage_bps {} exceeds hard cap {}",
                self.slippage_bps,
                HARD_MAX_SLIPPAGE_BPS
            );
        }
        if self.max_plan_age_secs == 0 || self.max_plan_age_secs > HARD_MAX_PLAN_AGE_SECS {
            bail!("Arcus max_plan_age_secs must be in 1..={HARD_MAX_PLAN_AGE_SECS}");
        }
        if self.max_swaps_per_utc_day == 0 || self.max_swaps_per_utc_day > HARD_MAX_DAILY_SWAPS {
            bail!("Arcus max_swaps_per_utc_day must be in 1..={HARD_MAX_DAILY_SWAPS}");
        }
        if parse_amount("minimum_gas_balance_wei", &self.minimum_gas_balance_wei)?.is_zero() {
            bail!("Arcus minimum gas balance must be positive");
        }
        validate_symbol_amount_map("inventory_floor_raw", &self.inventory_floor_raw, false)?;
        validate_symbol_amount_map(
            "maximum_sell_amount_raw",
            &self.maximum_sell_amount_raw,
            true,
        )?;
        Ok((taker, permit2))
    }
}

pub struct ArcusSpotLiveExecutor<S> {
    config: ArcusSpotLiveExecutorConfig,
    client: ArcusSpotClient,
    chain: ArcusSpotChainClient,
    signer: S,
    store: ArcusSpotExecutionLedgerStore,
    ledger: ArcusSpotExecutionLedger,
}

impl<S> ArcusSpotLiveExecutor<S>
where
    S: Signer + Sync,
    S::Error: Display,
{
    pub fn new(
        config: ArcusSpotLiveExecutorConfig,
        client: ArcusSpotClient,
        chain: ArcusSpotChainClient,
        signer: S,
        store: ArcusSpotExecutionLedgerStore,
    ) -> Result<Self> {
        let (taker, _) = config.validate()?;
        if client.config().chain_id != chain.chain_id()
            || signer.chain_id() != client.config().chain_id
        {
            bail!("Arcus client, chain RPC, and signer chain IDs must match");
        }
        if signer.address() != taker {
            bail!(
                "Arcus signer address {:#x} does not match configured taker {taker:#x}",
                signer.address()
            );
        }
        let ledger = store.load_or_create(Utc::now())?;
        Ok(Self {
            config,
            client,
            chain,
            signer,
            store,
            ledger,
        })
    }

    pub fn ledger(&self) -> &ArcusSpotExecutionLedger {
        &self.ledger
    }

    pub async fn execute_plan_once(
        &mut self,
        plan: &ArcusSpotRotationPlan,
    ) -> Result<ArcusSpotExecutionAttempt> {
        self.validate_plan(plan)?;
        let request = ArcusSpotSignableQuoteRequest::new(
            plan.sell_symbol.clone(),
            plan.buy_symbol.clone(),
            plan.sell_amount_raw.clone(),
            self.config.taker.clone(),
            self.config.slippage_bps,
            ArcusSpotQuoteRoutePolicy::DirectTokenOnly,
        );
        let observation = self
            .client
            .arcus_signable_quote_by_symbol(&request)
            .await
            .context("Arcus fresh signable quote failed")?;
        let mut matching_quotes = observation
            .response
            .payload
            .quotes
            .iter()
            .filter(|quote| quote.venue.eq_ignore_ascii_case(ARCUS_VENUE));
        let quote = matching_quotes
            .next()
            .context("fresh response omitted the direct Arcus venue quote")?;
        if matching_quotes.next().is_some() {
            bail!("fresh response contained duplicate Arcus venue quotes");
        }
        if quote.sell_amount != plan.sell_amount_raw {
            bail!("fresh Arcus quote changed the planned exact sell amount");
        }
        let minimum_buy = quote.minimum_received()?;
        let deadline = quote.expires_at()?;
        let sell_floor = symbol_amount(
            "inventory_floor_raw",
            &self.config.inventory_floor_raw,
            &plan.sell_symbol,
        )?;
        let buy_floor = symbol_amount(
            "inventory_floor_raw",
            &self.config.inventory_floor_raw,
            &plan.buy_symbol,
        )?;
        let preflight = self
            .chain
            .preflight(&ArcusSpotChainPreflightRequest {
                taker: self.config.taker.clone(),
                sell_token: observation.sell_token.address.clone(),
                buy_token: observation.buy_token.address.clone(),
                permit2: self.config.permit2.clone(),
                required_sell_amount_raw: plan.sell_amount_raw.clone(),
                sell_floor_raw: sell_floor.to_string(),
                buy_floor_raw: buy_floor.to_string(),
                minimum_gas_balance_wei: self.config.minimum_gas_balance_wei.clone(),
                permit_deadline: deadline,
            })
            .await
            .context("Arcus on-chain preflight failed")?;
        let submission = sign_arcus_spot_quote(
            &self.client,
            &observation,
            &self.signer,
            preflight.exact_value_permit.as_ref(),
        )
        .await
        .context("Arcus EIP-712 signing failed")?;
        let payload_hash = submission.payload_hash()?;
        let intent = ArcusSpotExecutionIntent {
            venue: ARCUS_VENUE.to_string(),
            sell_token: observation.sell_token.address,
            buy_token: observation.buy_token.address,
            sell_amount_raw: plan.sell_amount_raw.clone(),
            minimum_buy_amount_raw: minimum_buy.to_string(),
        };

        self.ledger
            .prepare(payload_hash, intent, preflight.balances, Utc::now())?;
        self.store
            .persist(&self.ledger)
            .context("failed to persist prepared Arcus execution")?;
        self.ledger.mark_dispatching(Utc::now())?;
        self.store
            .persist(&self.ledger)
            .context("failed to persist Arcus dispatch marker")?;

        match self.client.submit_signed_quote_once(&submission).await {
            Ok(observation) => {
                if let Err(error) = self
                    .ledger
                    .record_submit_status(&observation.payload, Utc::now())
                {
                    if self.active_phase() == Some(ArcusSpotExecutionPhase::Dispatching) {
                        self.ledger.record_submit_unknown(
                            format!("submit response validation failed: {error:#}"),
                            Utc::now(),
                        )?;
                    }
                    self.store.persist(&self.ledger)?;
                    return Err(error);
                }
                self.store.persist(&self.ledger)?;
            }
            Err(error) => {
                match &error {
                    ArcusSpotSubmitError::Preflight(_) | ArcusSpotSubmitError::Rejected { .. } => {
                        self.ledger
                            .record_submit_rejected(format!("{error}"), Utc::now())?;
                    }
                    ArcusSpotSubmitError::Unknown { .. } => {
                        self.ledger
                            .record_submit_unknown(format!("{error}"), Utc::now())?;
                    }
                }
                self.store.persist(&self.ledger)?;
                return Err(anyhow!(error));
            }
        }

        if self.active_phase() == Some(ArcusSpotExecutionPhase::Confirmed) {
            self.reconcile_confirmed().await?;
        }
        self.require_non_terminal_failure()?;
        self.active_attempt()
    }

    pub async fn resume_status_and_reconcile(&mut self) -> Result<ArcusSpotExecutionAttempt> {
        match self.active_phase() {
            Some(ArcusSpotExecutionPhase::Submitted) => {
                let active = self.active_attempt()?;
                let tx_hash = active
                    .tx_hash
                    .as_deref()
                    .context("submitted Arcus attempt omitted tx_hash")?;
                let tx_hash = H256::from_str(tx_hash).context("stored Arcus tx_hash is invalid")?;
                let status = self
                    .client
                    .swap_status(ARCUS_VENUE, tx_hash)
                    .await
                    .context("Arcus status poll failed")?;
                self.ledger
                    .record_polled_status(&status.payload, Utc::now())?;
                self.store.persist(&self.ledger)?;
            }
            Some(ArcusSpotExecutionPhase::Confirmed) => {}
            other => bail!("Arcus status resume is not allowed in phase {other:?}"),
        }
        if self.active_phase() == Some(ArcusSpotExecutionPhase::Confirmed) {
            self.reconcile_confirmed().await?;
        }
        self.require_non_terminal_failure()?;
        self.active_attempt()
    }

    pub fn archive_reconciled_after_runtime_commit(&mut self) -> Result<()> {
        self.ledger.archive_reconciled()?;
        self.store.persist(&self.ledger)
    }

    fn validate_plan(&self, plan: &ArcusSpotRotationPlan) -> Result<()> {
        if self.ledger.active.is_some() {
            bail!("Arcus execution ledger has an active attempt");
        }
        let plan_age = Utc::now().signed_duration_since(plan.quote_received_at);
        if plan_age.num_seconds() < 0
            || plan_age.num_seconds() > self.config.max_plan_age_secs as i64
        {
            bail!("Arcus strategy plan is stale or future-dated");
        }
        if !plan.venue.eq_ignore_ascii_case(ARCUS_VENUE) {
            bail!("initial live execution requires a direct Arcus strategy plan");
        }
        if plan.sell_symbol.eq_ignore_ascii_case(&plan.buy_symbol) {
            bail!("Arcus strategy plan symbols must be distinct");
        }
        let sell_amount = parse_amount("plan sell_amount_raw", &plan.sell_amount_raw)?;
        if sell_amount.is_zero() {
            bail!("Arcus strategy plan sell amount must be positive");
        }
        let maximum = symbol_amount(
            "maximum_sell_amount_raw",
            &self.config.maximum_sell_amount_raw,
            &plan.sell_symbol,
        )?;
        if sell_amount > maximum {
            bail!("Arcus strategy sell amount {sell_amount} exceeds configured maximum {maximum}");
        }
        symbol_amount(
            "inventory_floor_raw",
            &self.config.inventory_floor_raw,
            &plan.sell_symbol,
        )?;
        symbol_amount(
            "inventory_floor_raw",
            &self.config.inventory_floor_raw,
            &plan.buy_symbol,
        )?;
        let today = Utc::now().date_naive();
        let completed_today = self
            .ledger
            .history
            .iter()
            .filter(|attempt| attempt.prepared_at.date_naive() == today)
            .count();
        if completed_today >= self.config.max_swaps_per_utc_day as usize {
            bail!("Arcus UTC daily swap cap has been reached");
        }
        Ok(())
    }

    async fn reconcile_confirmed(&mut self) -> Result<()> {
        let active = self.active_attempt()?;
        let taker = parse_nonzero_address("taker", &self.config.taker)?;
        let sell_token = parse_nonzero_address("sell token", &active.intent.sell_token)?;
        let buy_token = parse_nonzero_address("buy token", &active.intent.buy_token)?;
        let post = self
            .chain
            .balances(taker, sell_token, buy_token)
            .await
            .context("Arcus post-submit balance read failed")?;
        let mutation = self.ledger.reconcile_balances(post, Utc::now());
        self.store.persist(&self.ledger)?;
        mutation
    }

    fn active_phase(&self) -> Option<ArcusSpotExecutionPhase> {
        self.ledger.active.as_ref().map(|attempt| attempt.phase)
    }

    fn active_attempt(&self) -> Result<ArcusSpotExecutionAttempt> {
        self.ledger
            .active
            .clone()
            .context("Arcus execution ledger has no active attempt")
    }

    fn require_non_terminal_failure(&self) -> Result<()> {
        match self.active_phase() {
            Some(ArcusSpotExecutionPhase::Unknown) => {
                bail!("Arcus execution is in sticky UNKNOWN state")
            }
            Some(ArcusSpotExecutionPhase::Failed) => {
                bail!("Arcus execution failed and requires operator review")
            }
            Some(ArcusSpotExecutionPhase::Rejected) => {
                bail!("Arcus execution was rejected and requires operator review")
            }
            _ => Ok(()),
        }
    }
}

fn validate_symbol_amount_map(
    label: &str,
    values: &BTreeMap<String, String>,
    require_positive: bool,
) -> Result<()> {
    if values.is_empty() {
        bail!("Arcus {label} must not be empty");
    }
    let mut normalized = BTreeMap::new();
    for (symbol, raw) in values {
        let key = symbol.trim().to_ascii_uppercase();
        if key.is_empty() || normalized.insert(key.clone(), ()).is_some() {
            bail!("Arcus {label} contains an empty or duplicate symbol {symbol:?}");
        }
        let amount = parse_amount(label, raw)?;
        if require_positive && amount.is_zero() {
            bail!("Arcus {label} value for {key} must be positive");
        }
    }
    Ok(())
}

fn symbol_amount(label: &str, values: &BTreeMap<String, String>, symbol: &str) -> Result<U256> {
    let raw = values
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol.trim()))
        .map(|(_, raw)| raw)
        .with_context(|| format!("Arcus {label} has no value for symbol {symbol}"))?;
    parse_amount(label, raw)
}

fn parse_nonzero_address(label: &str, raw: &str) -> Result<Address> {
    let address =
        Address::from_str(raw.trim()).with_context(|| format!("invalid Arcus {label} address"))?;
    if address == Address::zero() {
        bail!("Arcus {label} address must not be zero");
    }
    Ok(address)
}

fn parse_amount(label: &str, raw: &str) -> Result<U256> {
    U256::from_dec_str(raw.trim()).with_context(|| format!("invalid Arcus {label}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ArcusSpotLiveExecutorConfig {
        ArcusSpotLiveExecutorConfig {
            taker: "0x7600000000000000000000000000000000000001".to_string(),
            permit2: CANONICAL_PERMIT2.to_string(),
            slippage_bps: 50,
            minimum_gas_balance_wei: "1000000000000000".to_string(),
            inventory_floor_raw: BTreeMap::from([
                ("NVDA".to_string(), "100".to_string()),
                ("AMD".to_string(), "100".to_string()),
            ]),
            maximum_sell_amount_raw: BTreeMap::from([
                ("NVDA".to_string(), "1000".to_string()),
                ("AMD".to_string(), "1000".to_string()),
            ]),
            max_swaps_per_utc_day: 10,
            max_plan_age_secs: 30,
        }
    }

    #[test]
    fn validates_hard_live_caps() {
        config().validate().unwrap();
        let mut too_many = config();
        too_many.max_swaps_per_utc_day = HARD_MAX_DAILY_SWAPS + 1;
        assert!(too_many.validate().is_err());
        let mut too_much_slippage = config();
        too_much_slippage.slippage_bps = HARD_MAX_SLIPPAGE_BPS + 1;
        assert!(too_much_slippage.validate().is_err());
        let mut stale_plan_window = config();
        stale_plan_window.max_plan_age_secs = HARD_MAX_PLAN_AGE_SECS + 1;
        assert!(stale_plan_window.validate().is_err());
    }

    #[test]
    fn rejects_case_colliding_symbol_limits() {
        let mut value = config();
        value
            .inventory_floor_raw
            .insert("nvda".to_string(), "100".to_string());
        assert!(value.validate().is_err());
    }
}
