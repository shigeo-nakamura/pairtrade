use super::{
    ArcusSpotChainClient, ArcusSpotChainPreflightRequest, ArcusSpotDirection,
    ArcusSpotExecutionAttempt, ArcusSpotExecutionIntent, ArcusSpotExecutionLedger,
    ArcusSpotExecutionLedgerLock, ArcusSpotExecutionLedgerStore, ArcusSpotExecutionPhase,
    ArcusSpotRotationPlan,
};
use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use dex_connector::{
    sign_arcus_spot_quote, ArcusSpotClient, ArcusSpotPair, ArcusSpotQuoteRoutePolicy,
    ArcusSpotSignableQuoteRequest, ArcusSpotSubmitError,
};
use ethers::{
    signers::Signer,
    types::{Address, H256, U256},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Display, path::Path, str::FromStr};

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

#[derive(Debug, Clone, PartialEq)]
pub struct ArcusSpotReconciledRuntimeFill {
    pub actual_sell_quantity: Decimal,
    pub actual_buy_quantity: Decimal,
    pub reconciled_at: DateTime<Utc>,
    pub idempotency_key: String,
}

pub struct ArcusSpotLiveExecutor<S> {
    config: ArcusSpotLiveExecutorConfig,
    // Sourced solely from the runtime's own `pair` config field (see
    // `executor_from_config`), never a second, independently-editable YAML
    // entry: two copies of the same fact could otherwise silently drift.
    pair: ArcusSpotPair,
    client: ArcusSpotClient,
    chain: ArcusSpotChainClient,
    signer: S,
    store: ArcusSpotExecutionLedgerStore,
    ledger: ArcusSpotExecutionLedger,
    _ledger_lock: ArcusSpotExecutionLedgerLock,
}

impl<S> ArcusSpotLiveExecutor<S>
where
    S: Signer + Sync,
    S::Error: Display,
{
    pub fn new(
        config: ArcusSpotLiveExecutorConfig,
        pair: ArcusSpotPair,
        client: ArcusSpotClient,
        chain: ArcusSpotChainClient,
        signer: S,
        store: ArcusSpotExecutionLedgerStore,
        lock_namespace: &Path,
    ) -> Result<Self> {
        let (taker, _) = config.validate()?;
        if pair.sell_symbol.eq_ignore_ascii_case(&pair.buy_symbol) {
            bail!("Arcus runtime pair symbols must be distinct");
        }
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
        let ledger_lock = store.acquire_exclusive_lock(lock_namespace)?;
        let ledger = store.load_or_create(Utc::now())?;
        Ok(Self {
            config,
            pair,
            client,
            chain,
            signer,
            store,
            ledger,
            _ledger_lock: ledger_lock,
        })
    }

    pub fn ledger(&self) -> &ArcusSpotExecutionLedger {
        &self.ledger
    }

    pub async fn execute_plan_once(
        &mut self,
        plan: &ArcusSpotRotationPlan,
        plan_config_digest: &str,
    ) -> Result<ArcusSpotExecutionAttempt> {
        if plan_config_digest.trim().is_empty() {
            bail!("Arcus execute_plan_once requires a non-empty plan_config_digest");
        }
        // A one-swap approval is meant to authorize exactly one swap.
        // Checking only `active` (Codex P1 follow-up, pairtrade#181) lets
        // the identical approved digest be resubmitted after its first
        // attempt already reconciled and archived -- the ledger happily
        // assigns a new sequence/idempotency key, so this would dispatch
        // and sign a second real swap on the same one-time approval before
        // the runtime commit seam (which only guards inventory/regime
        // state, not the ledger) ever gets a chance to reject it. Treat any
        // digest already present in history as consumed.
        if self
            .ledger
            .history
            .iter()
            .any(|attempt| attempt.intent.plan_config_digest == plan_config_digest)
        {
            bail!("Arcus approval digest has already been used for a prior execution attempt");
        }
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
        // Nothing upstream cross-checks a plan's raw (on-chain, what
        // actually gets swapped) and decimal (what the runtime commits to
        // its checkpoint) amounts against each other. A plan whose
        // sell_amount_raw/sell_quantity or buy_amount_raw/buy_quantity are
        // inconsistent for the fresh token's own decimals would still swap
        // correctly on-chain (raw amounts drive the wire submission) while
        // finalize_reconciled_attempt records a decimal quantity that
        // doesn't describe what was actually swapped, diverging runtime
        // inventory from wallet balances even though every balance
        // reconciliation upstream reported exact (Codex P1 follow-up,
        // pairtrade#181).
        require_raw_matches_decimal_quantity(
            "sell",
            &plan.sell_amount_raw,
            plan.sell_quantity,
            observation.sell_token.decimals,
        )?;
        require_raw_matches_decimal_quantity(
            "buy",
            &plan.buy_amount_raw,
            plan.buy_quantity,
            observation.buy_token.decimals,
        )?;
        require_fresh_quote_token_addresses_match_plan(
            plan,
            &observation.sell_token.address,
            &observation.buy_token.address,
        )?;
        let minimum_buy = quote.minimum_received()?;
        require_fresh_quote_matches_approved_plan(plan, minimum_buy, self.config.slippage_bps)?;
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
            sell_symbol: plan.sell_symbol.clone(),
            buy_symbol: plan.buy_symbol.clone(),
            sell_token: observation.sell_token.address,
            buy_token: observation.buy_token.address,
            sell_amount_raw: plan.sell_amount_raw.clone(),
            minimum_buy_amount_raw: minimum_buy.to_string(),
            plan_config_digest: plan_config_digest.to_string(),
        };

        self.validate_plan_age(plan)
            .context("Arcus strategy plan expired before durable preparation")?;
        self.ledger.prepare(
            self.client.config().chain_id,
            self.config.taker.clone(),
            payload_hash,
            intent,
            preflight.balances,
            Utc::now(),
        )?;
        self.store
            .persist(&self.ledger)
            .context("failed to persist prepared Arcus execution")?;
        if let Err(error) = self.validate_plan_age(plan) {
            // Explicitly reject the just-persisted Prepared attempt rather
            // than leave it for the next invocation's restart recovery to
            // mislabel as OperatorHold (Codex P2 follow-up, pairtrade#181).
            self.ledger.cancel_prepared(
                format!("plan expired before dispatch: {error:#}"),
                Utc::now(),
            )?;
            self.store
                .persist(&self.ledger)
                .context("failed to persist Arcus prepared-attempt cancellation")?;
            return Err(error).context("Arcus strategy plan expired before dispatch");
        }
        self.ledger.mark_dispatching(Utc::now())?;
        self.store
            .persist(&self.ledger)
            .context("failed to persist Arcus dispatch marker")?;

        match self
            .client
            .submit_signed_quote_once(&submission, preflight.exact_value_permit.as_ref())
            .await
        {
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
                let mutation = self
                    .ledger
                    .record_polled_status(&status.payload, Utc::now());
                self.store.persist(&self.ledger)?;
                mutation?;
            }
            Some(ArcusSpotExecutionPhase::Confirmed) => {}
            // reconcile_confirmed can durably persist Reconciled and then
            // the process exits or errors before the caller's runtime
            // commit + ledger archive finish. execute refuses to start a
            // new plan while any attempt remains active, and this match
            // previously refused to even return the existing Reconciled
            // attempt, permanently blocking recovery -- even though
            // finalize_reconciled_attempt's commit path
            // (apply_confirmed_live_fill_once) is already idempotent by
            // execution key and safe to invoke again (Codex P1
            // follow-up, pairtrade#181).
            Some(ArcusSpotExecutionPhase::Reconciled) => {}
            other => bail!("Arcus status resume is not allowed in phase {other:?}"),
        }
        if self.active_phase() == Some(ArcusSpotExecutionPhase::Confirmed) {
            self.reconcile_confirmed().await?;
        }
        self.require_non_terminal_failure()?;
        self.active_attempt()
    }

    pub fn reconciled_runtime_fill(
        &self,
        plan: &ArcusSpotRotationPlan,
        plan_config_digest: &str,
    ) -> Result<ArcusSpotReconciledRuntimeFill> {
        let active = self.active_attempt()?;
        if active.phase != ArcusSpotExecutionPhase::Reconciled {
            bail!("Arcus runtime fill requires a reconciled execution attempt");
        }
        // Venue/symbols/sell_amount_raw alone don't prove `resume` (or a
        // fresh `execute` racing an existing attempt) was given the exact
        // plan that was actually prepared and dispatched: a different plan
        // can independently satisfy those same fields while differing in
        // sell_quantity/buy_quantity/direction/trigger, which
        // finalize_reconciled_attempt would then commit as if they
        // described the executed swap. The digest captures the full
        // approved plan (and config), so a mismatch here means this is
        // provably not the same plan (Codex P1 follow-up, pairtrade#181).
        if active.intent.plan_config_digest != plan_config_digest {
            bail!("Arcus reconciled attempt was prepared under a different approved plan/config");
        }
        if !active.intent.venue.eq_ignore_ascii_case(&plan.venue)
            || !active
                .intent
                .sell_symbol
                .eq_ignore_ascii_case(&plan.sell_symbol)
            || !active
                .intent
                .buy_symbol
                .eq_ignore_ascii_case(&plan.buy_symbol)
            || active.intent.sell_amount_raw != plan.sell_amount_raw
        {
            bail!("Arcus reconciled attempt does not match the approved runtime plan");
        }
        let post = active
            .post_balances
            .as_ref()
            .context("reconciled Arcus attempt omitted post balances")?;
        let pre_sell = parse_amount("pre sell balance", &active.pre_balances.sell_balance_raw)?;
        let pre_buy = parse_amount("pre buy balance", &active.pre_balances.buy_balance_raw)?;
        let post_sell = parse_amount("post sell balance", &post.sell_balance_raw)?;
        let post_buy = parse_amount("post buy balance", &post.buy_balance_raw)?;
        let sold_raw = pre_sell
            .checked_sub(post_sell)
            .context("reconciled Arcus sell balance increased")?;
        let bought_raw = post_buy
            .checked_sub(pre_buy)
            .context("reconciled Arcus buy balance decreased")?;
        if sold_raw != parse_amount("intent sell amount", &active.intent.sell_amount_raw)? {
            bail!("reconciled Arcus sell delta no longer matches the signed intent");
        }
        let planned_buy_raw = parse_amount("plan buy amount", &plan.buy_amount_raw)?;
        if planned_buy_raw.is_zero() || plan.buy_quantity <= Decimal::ZERO {
            bail!("approved Arcus plan has an invalid buy quantity");
        }
        let bought_decimal = u256_decimal("reconciled buy amount", bought_raw)?;
        let planned_buy_decimal = u256_decimal("planned buy amount", planned_buy_raw)?;
        let actual_buy_quantity = plan
            .buy_quantity
            .checked_mul(bought_decimal)
            .and_then(|value| value.checked_div(planned_buy_decimal))
            .context("reconciled Arcus buy quantity exceeds Decimal range")?;
        if plan.sell_quantity <= Decimal::ZERO || actual_buy_quantity <= Decimal::ZERO {
            bail!("reconciled Arcus runtime quantities must be positive");
        }
        // `updated_at` is bumped on every status transition, including a
        // `resume` that reconciles long after the swap actually confirmed
        // on-chain -- using it here would make the runtime's
        // `last_rotation_at` (and therefore `max_hold_secs`) start counting
        // from whenever resume happened to run instead of from the real
        // fill, letting a rotated position sit open arbitrarily longer than
        // configured. `dispatched_at` is set once, at submission, and never
        // overwritten afterward, so it is the right (conservative) fill
        // time to report (Codex P1 follow-up, pairtrade#181).
        let filled_at = active
            .dispatched_at
            .context("reconciled Arcus attempt is missing its dispatch time")?;
        Ok(ArcusSpotReconciledRuntimeFill {
            actual_sell_quantity: plan.sell_quantity,
            actual_buy_quantity,
            reconciled_at: filled_at,
            idempotency_key: active.idempotency_key,
        })
    }

    pub fn archive_reconciled_after_runtime_commit(&mut self) -> Result<()> {
        self.ledger.archive_reconciled()?;
        self.store.persist(&self.ledger)
    }

    fn validate_plan(&self, plan: &ArcusSpotRotationPlan) -> Result<()> {
        if self.ledger.active.is_some() {
            bail!("Arcus execution ledger has an active attempt");
        }
        self.validate_plan_age(plan)?;
        if !plan.venue.eq_ignore_ascii_case(ARCUS_VENUE) {
            bail!("initial live execution requires a direct Arcus strategy plan");
        }
        if plan.sell_symbol.eq_ignore_ascii_case(&plan.buy_symbol) {
            bail!("Arcus strategy plan symbols must be distinct");
        }
        require_plan_direction_matches_pair(plan, &self.pair)?;
        let sell_amount = parse_amount("plan sell_amount_raw", &plan.sell_amount_raw)?;
        if sell_amount.is_zero() {
            bail!("Arcus strategy plan sell amount must be positive");
        }
        let buy_amount = parse_amount("plan buy_amount_raw", &plan.buy_amount_raw)?;
        if buy_amount.is_zero() {
            bail!("Arcus strategy plan buy amount must be positive");
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
            .filter(|attempt| attempt.updated_at.date_naive() == today)
            .count();
        if completed_today >= self.config.max_swaps_per_utc_day as usize {
            bail!("Arcus UTC daily swap cap has been reached");
        }
        Ok(())
    }

    async fn reconcile_confirmed(&mut self) -> Result<()> {
        let active = self.active_attempt()?;
        let taker = parse_nonzero_address("taker", &self.config.taker)?;
        let original_taker = parse_nonzero_address("stored taker", &active.taker)?;
        if active.chain_id != self.chain.chain_id()
            || active.chain_id != self.client.config().chain_id
            || original_taker != taker
        {
            bail!("Arcus reconciliation config does not match the original chain and taker");
        }
        let sell_token = parse_nonzero_address("sell token", &active.intent.sell_token)?;
        let buy_token = parse_nonzero_address("buy token", &active.intent.buy_token)?;
        // Deliberately not the fallback-capable `balances()`: a secondary
        // provider that hasn't yet indexed this specific confirmed swap
        // would return the pre-swap balance as an apparently valid
        // snapshot, and reconcile_balances would misread that as a
        // genuine reconciliation failure and permanently mark the attempt
        // Unknown (Codex P1 follow-up, pairtrade#182).
        let post = self
            .chain
            .balances_requiring_primary_provider(taker, sell_token, buy_token)
            .await
            .context("Arcus post-submit balance read failed")?;
        let mutation = self.ledger.reconcile_balances(post, Utc::now());
        self.store.persist(&self.ledger)?;
        mutation
    }

    fn validate_plan_age(&self, plan: &ArcusSpotRotationPlan) -> Result<()> {
        let plan_age = Utc::now().signed_duration_since(plan.quote_received_at);
        if plan_age.num_seconds() < 0
            || plan_age.num_seconds() > self.config.max_plan_age_secs as i64
        {
            bail!("Arcus strategy plan is stale or future-dated");
        }
        Ok(())
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

fn require_fresh_quote_matches_approved_plan(
    plan: &ArcusSpotRotationPlan,
    fresh_minimum_buy: U256,
    slippage_bps: u32,
) -> Result<()> {
    let approved_buy = parse_amount("plan buy_amount_raw", &plan.buy_amount_raw)?;
    let retained_bps = 10_000_u32
        .checked_sub(slippage_bps)
        .context("Arcus slippage exceeds 10000 bps")?;
    let numerator = approved_buy
        .checked_mul(U256::from(retained_bps))
        .context("Arcus approved minimum buy calculation overflow")?;
    let approved_minimum = numerator
        .checked_add(U256::from(9_999_u32))
        .context("Arcus approved minimum buy rounding overflow")?
        / U256::from(10_000_u32);
    if fresh_minimum_buy < approved_minimum {
        bail!(
            "fresh Arcus minimum buy {fresh_minimum_buy} undercuts approved plan floor {approved_minimum}"
        );
    }
    Ok(())
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

fn u256_decimal(label: &str, value: U256) -> Result<Decimal> {
    Decimal::from_str(&value.to_string())
        .with_context(|| format!("Arcus {label} exceeds Decimal range"))
}

/// Require a plan's raw (on-chain base units) and decimal (human/runtime)
/// amounts to describe the same quantity under the token's real decimals,
/// rather than trusting them to already agree (Codex P1 follow-up,
/// pairtrade#181).
fn require_raw_matches_decimal_quantity(
    label: &str,
    raw: &str,
    quantity: Decimal,
    decimals: u32,
) -> Result<()> {
    let expected_raw = super::quantity_to_raw_amount(quantity, decimals)
        .map_err(|error| anyhow!("Arcus plan {label} amount: {error}"))?;
    if expected_raw != raw {
        bail!(
            "Arcus plan {label}_amount_raw {raw} does not match {label}_quantity {quantity} at {decimals} decimals (expected {expected_raw})"
        );
    }
    Ok(())
}

/// The approval digest covers the plan's symbols, not any token address --
/// a symbol registry can, legitimately or through compromise/
/// misconfiguration, resolve the same symbol to a different ERC-20
/// contract by execution time than it did when this plan was built and
/// approved. Without this check, preflight and signing would proceed
/// against whatever the *fresh* quote resolves to, with nothing to compare
/// it against, so the ledger and balance reconciliation could succeed
/// against a replacement contract while runtime inventory still accounts
/// for the originally intended pair (Codex P1 follow-up, pairtrade#181).
fn require_fresh_quote_token_addresses_match_plan(
    plan: &ArcusSpotRotationPlan,
    fresh_sell_token_address: &str,
    fresh_buy_token_address: &str,
) -> Result<()> {
    if !fresh_sell_token_address.eq_ignore_ascii_case(&plan.sell_token_address)
        || !fresh_buy_token_address.eq_ignore_ascii_case(&plan.buy_token_address)
    {
        bail!(
            "fresh Arcus quote resolved {}/{} to {}/{}, but the approved plan pinned {}/{}",
            plan.sell_symbol,
            plan.buy_symbol,
            fresh_sell_token_address,
            fresh_buy_token_address,
            plan.sell_token_address,
            plan.buy_token_address
        );
    }
    Ok(())
}

/// Distinct symbols alone don't prove a plan's direction actually matches
/// the runtime's configured pair: a plan claiming `TokenAToTokenB` while in
/// fact selling the configured token B would otherwise pass, and
/// `apply_confirmed_live_fill` later interprets that direction as A-to-B
/// regardless, mutating checkpoint inventory in the wrong orientation
/// relative to the real wallet delta (Codex P1 follow-up, pairtrade#181).
fn require_plan_direction_matches_pair(
    plan: &ArcusSpotRotationPlan,
    pair: &ArcusSpotPair,
) -> Result<()> {
    let (expected_sell, expected_buy) = match plan.direction {
        ArcusSpotDirection::TokenAToTokenB => (pair.sell_symbol.as_str(), pair.buy_symbol.as_str()),
        ArcusSpotDirection::TokenBToTokenA => (pair.buy_symbol.as_str(), pair.sell_symbol.as_str()),
    };
    if !plan.sell_symbol.eq_ignore_ascii_case(expected_sell)
        || !plan.buy_symbol.eq_ignore_ascii_case(expected_buy)
    {
        bail!(
            "Arcus strategy plan symbols {}/{} do not match the runtime pair {}/{} for direction {:?}",
            plan.sell_symbol,
            plan.buy_symbol,
            pair.sell_symbol,
            pair.buy_symbol,
            plan.direction
        );
    }
    Ok(())
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

    fn plan_with_buy_amount(buy_amount_raw: &str) -> ArcusSpotRotationPlan {
        ArcusSpotRotationPlan {
            direction: crate::arcus_spot::ArcusSpotDirection::TokenAToTokenB,
            trigger: crate::arcus_spot::ArcusSpotRotationTrigger::EntrySignal,
            sell_symbol: "NVDA".to_string(),
            buy_symbol: "AMD".to_string(),
            sell_token_address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            buy_token_address: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            sell_quantity: rust_decimal::Decimal::ONE,
            buy_quantity: rust_decimal::Decimal::ONE,
            sell_amount_raw: "1000".to_string(),
            buy_amount_raw: buy_amount_raw.to_string(),
            venue: ARCUS_VENUE.to_string(),
            quote_received_at: Utc::now(),
            optimistic_round_trip_loss_bps: rust_decimal::Decimal::ZERO,
            gas_buffer_bps: rust_decimal::Decimal::ZERO,
            settlement_buffer_bps: rust_decimal::Decimal::ZERO,
            all_in_round_trip_cost_bps: rust_decimal::Decimal::ZERO,
            predicted_inventory: crate::arcus_spot::ArcusSpotInventory {
                token_a: rust_decimal::Decimal::ONE,
                token_b: rust_decimal::Decimal::ONE,
            },
            predicted_inventory_imbalance_fraction: rust_decimal::Decimal::ZERO,
        }
    }

    #[test]
    fn fresh_quote_cannot_undercut_approved_plan_floor() {
        let plan = plan_with_buy_amount("1000");
        require_fresh_quote_matches_approved_plan(&plan, U256::from(995_u64), 50).unwrap();
        assert!(require_fresh_quote_matches_approved_plan(&plan, U256::from(994_u64), 50).is_err());
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

    fn pair() -> ArcusSpotPair {
        ArcusSpotPair {
            sell_symbol: "NVDA".to_string(),
            buy_symbol: "AMD".to_string(),
        }
    }

    #[test]
    fn plan_direction_must_match_the_runtime_pair() {
        let mut plan = plan_with_buy_amount("1000");
        plan.direction = crate::arcus_spot::ArcusSpotDirection::TokenAToTokenB;
        plan.sell_symbol = "NVDA".to_string();
        plan.buy_symbol = "AMD".to_string();
        require_plan_direction_matches_pair(&plan, &pair()).unwrap();

        plan.direction = crate::arcus_spot::ArcusSpotDirection::TokenBToTokenA;
        plan.sell_symbol = "AMD".to_string();
        plan.buy_symbol = "NVDA".to_string();
        require_plan_direction_matches_pair(&plan, &pair()).unwrap();
    }

    #[test]
    fn plan_claiming_a_to_b_while_actually_selling_token_b_is_rejected() {
        let mut plan = plan_with_buy_amount("1000");
        // Distinct symbols, but the wrong orientation for TokenAToTokenB
        // given the runtime pair sell_symbol=NVDA/buy_symbol=AMD.
        plan.direction = crate::arcus_spot::ArcusSpotDirection::TokenAToTokenB;
        plan.sell_symbol = "AMD".to_string();
        plan.buy_symbol = "NVDA".to_string();
        assert!(require_plan_direction_matches_pair(&plan, &pair()).is_err());
    }

    #[test]
    fn raw_matching_its_decimal_quantity_is_accepted() {
        require_raw_matches_decimal_quantity("sell", "1000000000000000000", Decimal::ONE, 18)
            .unwrap();
    }

    #[test]
    fn raw_inconsistent_with_its_decimal_quantity_is_rejected() {
        // 1000 raw units at 18 decimals is 0.000000000000001, not 1.
        assert!(
            require_raw_matches_decimal_quantity("sell", "1000", Decimal::ONE, 18).is_err()
        );
    }

    #[test]
    fn a_truncated_raw_amount_for_a_fractional_quantity_is_rejected() {
        // At decimals=0, quantity 1.9 has no exact raw representation; a raw
        // amount of "1" is the truncation Codex flagged, not a legitimate
        // match, and must be rejected rather than silently accepted.
        assert!(require_raw_matches_decimal_quantity(
            "sell",
            "1",
            Decimal::from_str("1.9").unwrap(),
            0
        )
        .is_err());
    }

    #[test]
    fn fresh_quote_token_addresses_matching_the_plan_are_accepted() {
        let plan = plan_with_buy_amount("1000");
        require_fresh_quote_token_addresses_match_plan(
            &plan,
            &plan.sell_token_address,
            &plan.buy_token_address,
        )
        .unwrap();
    }

    #[test]
    fn fresh_quote_token_address_drift_is_rejected() {
        let plan = plan_with_buy_amount("1000");
        // A registry that now resolves the approved sell symbol to a
        // different contract than the one pinned on the approved plan.
        assert!(require_fresh_quote_token_addresses_match_plan(
            &plan,
            "0x0000000000000000000000000000000000000099",
            &plan.buy_token_address,
        )
        .is_err());
    }
}
