use super::ArcusSpotBalanceSnapshot;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use dex_connector::ArcusSpotEip2612PermitContext;
use ethers::{
    abi::RawLog,
    contract::{abigen, EthLogDecode},
    providers::{Http, Middleware, Provider},
    types::{Address, TransactionReceipt, H256, U256},
};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc, time::Duration};

abigen!(
    ArcusSpotErc20,
    r#"[
        function balanceOf(address owner) external view returns (uint256)
        function allowance(address owner, address spender) external view returns (uint256)
        function nonces(address owner) external view returns (uint256)
        function name() external view returns (string)
        function version() external view returns (string)
    ]"#
);

abigen!(
    ArcusSpotSwapShell,
    r#"[
        event SwapExecuted(address indexed taker, address indexed tokenIn, address indexed tokenOut, uint256 minAmountOut, uint256 amountIn, uint256 quotedAmountIn, uint256 quotedAmountOut, uint256 amountOut, uint256 tokenInBenchmarkPrice, uint256 tokenOutBenchmarkPrice, address router, bytes32 routeTag, bool success, string reason)
    ]"#
);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotChainConfig {
    /// Tried in order on every call; a later entry is only used when every
    /// earlier one fails at the transport level (connection/timeout/RPC
    /// error), never as a substitute source of truth after a successful but
    /// unexpected response -- a chain-id mismatch or business-rule failure
    /// from the first reachable endpoint is reported as-is, not silently
    /// retried against a different endpoint that might paper over a real
    /// misconfiguration.
    pub rpc_urls: Vec<String>,
    pub chain_id: u64,
    pub request_interval_ms: u64,
}

impl ArcusSpotChainConfig {
    pub fn validate(&self) -> Result<()> {
        if self.chain_id == 0 {
            bail!("Arcus chain_id must be non-zero");
        }
        if self.request_interval_ms == 0 {
            bail!("Arcus request_interval_ms must be non-zero");
        }
        if self.rpc_urls.is_empty() {
            bail!("Arcus rpc_urls must not be empty");
        }
        let mut seen = std::collections::BTreeSet::new();
        for rpc_url in &self.rpc_urls {
            let url = url::Url::parse(rpc_url.trim()).context("invalid Arcus RPC URL")?;
            if !matches!(url.scheme(), "https" | "http") {
                bail!("Arcus RPC URL must use http or https");
            }
            if url.host_str().is_none() || url.username() != "" || url.password().is_some() {
                bail!("Arcus RPC URL must have a host and no inline credentials");
            }
            if !seen.insert(rpc_url.trim().to_string()) {
                // Redacted for the same reason the provider-failure log
                // line is: an RPC URL commonly carries an API token in its
                // path or query string, and this validation error can
                // surface wherever startup/config errors are displayed or
                // logged (Codex P2 follow-up, pairtrade#182, round 5).
                bail!(
                    "Arcus rpc_urls contains a duplicate entry: {}",
                    redact_rpc_url(rpc_url)
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotChainPreflightRequest {
    pub taker: String,
    pub sell_token: String,
    pub buy_token: String,
    pub permit2: String,
    pub required_sell_amount_raw: String,
    pub sell_floor_raw: String,
    pub buy_floor_raw: String,
    pub minimum_gas_balance_wei: String,
    pub permit_deadline: u64,
}

#[derive(Clone, Copy)]
struct ValidatedPreflightRequest {
    taker: Address,
    sell_token: Address,
    buy_token: Address,
    permit2: Address,
    required_sell: U256,
    sell_floor: U256,
    buy_floor: U256,
    minimum_gas: U256,
}

impl ArcusSpotChainPreflightRequest {
    fn validate(&self) -> Result<ValidatedPreflightRequest> {
        let taker = parse_nonzero_address("taker", &self.taker)?;
        let sell_token = parse_nonzero_address("sell_token", &self.sell_token)?;
        let buy_token = parse_nonzero_address("buy_token", &self.buy_token)?;
        let permit2 = parse_nonzero_address("permit2", &self.permit2)?;
        if sell_token == buy_token {
            bail!("Arcus preflight tokens must be distinct");
        }
        let required_sell =
            parse_amount("required_sell_amount_raw", &self.required_sell_amount_raw)?;
        let sell_floor = parse_amount("sell_floor_raw", &self.sell_floor_raw)?;
        let buy_floor = parse_amount("buy_floor_raw", &self.buy_floor_raw)?;
        let minimum_gas = parse_amount("minimum_gas_balance_wei", &self.minimum_gas_balance_wei)?;
        if required_sell.is_zero() {
            bail!("Arcus required sell amount must be positive");
        }
        if self.permit_deadline == 0 {
            bail!("Arcus permit_deadline must be non-zero");
        }
        Ok(ValidatedPreflightRequest {
            taker,
            sell_token,
            buy_token,
            permit2,
            required_sell,
            sell_floor,
            buy_floor,
            minimum_gas,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotChainPreflight {
    pub chain_id: u64,
    pub balances: ArcusSpotBalanceSnapshot,
    pub permit2_allowance_raw: String,
    pub exact_value_permit: Option<ArcusSpotEip2612PermitContext>,
}

/// Exact on-chain settlement facts that must be present in the canonical
/// SwapShell receipt before a hosted-router status can be reconciled into
/// the local execution ledger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotSettlementReceiptExpectation {
    pub venue: String,
    pub taker: String,
    pub sell_token: String,
    pub buy_token: String,
    pub sell_amount_raw: String,
    pub minimum_buy_amount_raw: String,
    pub swap_shell: String,
    pub venue_router: String,
}

#[derive(Clone, Copy)]
struct ValidatedSettlementReceiptExpectation {
    taker: Address,
    sell_token: Address,
    buy_token: Address,
    sell_amount: U256,
    minimum_buy_amount: U256,
    swap_shell: Address,
    venue_router: Address,
    route_tag: [u8; 32],
}

impl ArcusSpotSettlementReceiptExpectation {
    fn validate(&self) -> Result<ValidatedSettlementReceiptExpectation> {
        let venue = self.venue.trim().to_ascii_lowercase();
        let route_label = match venue.as_str() {
            "arcus" => b"ARCUS".as_slice(),
            "rialto" => b"RIALTO".as_slice(),
            other => bail!("unsupported Arcus Spot settlement venue {other}"),
        };
        let mut route_tag = [0_u8; 32];
        route_tag[..route_label.len()].copy_from_slice(route_label);

        let taker = parse_nonzero_address("settlement taker", &self.taker)?;
        let sell_token = parse_nonzero_address("settlement sell_token", &self.sell_token)?;
        let buy_token = parse_nonzero_address("settlement buy_token", &self.buy_token)?;
        let swap_shell = parse_nonzero_address("settlement swap_shell", &self.swap_shell)?;
        let venue_router = parse_nonzero_address("settlement venue_router", &self.venue_router)?;
        if sell_token == buy_token {
            bail!("Arcus Spot settlement tokens must be distinct");
        }
        let sell_amount = parse_amount("settlement sell_amount_raw", &self.sell_amount_raw)?;
        let minimum_buy_amount = parse_amount(
            "settlement minimum_buy_amount_raw",
            &self.minimum_buy_amount_raw,
        )?;
        if sell_amount.is_zero() || minimum_buy_amount.is_zero() {
            bail!("Arcus Spot settlement amounts must be positive");
        }

        Ok(ValidatedSettlementReceiptExpectation {
            taker,
            sell_token,
            buy_token,
            sell_amount,
            minimum_buy_amount,
            swap_shell,
            venue_router,
            route_tag,
        })
    }
}

#[derive(Clone)]
pub struct ArcusSpotChainClient {
    config: ArcusSpotChainConfig,
    providers: Vec<Arc<Provider<Http>>>,
}

/// Result of one complete, coherent preflight round-trip against a single
/// provider (balance/allowance reads plus, when needed, the EIP-2612
/// metadata fetch) -- one atomic attempt so a transport failure partway
/// through still retries the *whole* thing against the next provider
/// instead of leaving a half-finished result no caller asked for.
struct PreflightAttempt {
    sell_balance: U256,
    buy_balance: U256,
    gas_balance: U256,
    allowance: U256,
    exact_value_permit: Option<ArcusSpotEip2612PermitContext>,
}

struct RawBalanceReads {
    sell_balance: U256,
    buy_balance: U256,
    gas_balance: U256,
}

/// Distinguishes "this provider might just be unreachable right now, try
/// the next one" from "this provider answered, but with something a
/// different provider must never be allowed to silently paper over" --
/// namely a wrong chain id, which is a network-identity mismatch, not a
/// value that can legitimately differ by provider freshness the way a
/// balance or allowance can.
enum ProviderAttemptError {
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

impl From<anyhow::Error> for ProviderAttemptError {
    fn from(error: anyhow::Error) -> Self {
        Self::Transient(error)
    }
}

/// Result of one `ethers` contract call, split into the three shapes a
/// caller needs to react to differently: succeeded; failed at the
/// transport/RPC level (worth retrying against another provider); or
/// failed with the provider having genuinely answered -- a revert or a
/// decoding failure, meaning the contract itself doesn't behave as
/// expected, which no other provider on the same chain would answer any
/// differently.
enum ContractCallOutcome<T> {
    Ok(T),
    Transport(anyhow::Error),
    NonTransport(anyhow::Error),
}

fn classify_contract_call<T>(
    result: std::result::Result<T, ethers::contract::ContractError<Provider<Http>>>,
) -> ContractCallOutcome<T> {
    match result {
        Ok(value) => ContractCallOutcome::Ok(value),
        Err(ethers::contract::ContractError::MiddlewareError { e }) => {
            ContractCallOutcome::Transport(anyhow::Error::new(e))
        }
        Err(ethers::contract::ContractError::ProviderError { e }) => {
            ContractCallOutcome::Transport(anyhow::Error::new(e))
        }
        Err(other) => ContractCallOutcome::NonTransport(anyhow::anyhow!(other)),
    }
}

/// Extract the `Ok` value after the caller has already handled every
/// `NonTransport` (as Fatal) and `Transport` (as Transient) outcome in
/// the batch this belongs to -- by construction, only `Ok` can remain.
fn expect_ok<T>(outcome: ContractCallOutcome<T>) -> T {
    match outcome {
        ContractCallOutcome::Ok(value) => value,
        ContractCallOutcome::Transport(_) | ContractCallOutcome::NonTransport(_) => {
            unreachable!("expect_ok called after Fatal/Transient outcomes were already handled")
        }
    }
}

/// Strip everything but scheme/host/port before this URL can reach a log
/// line -- RPC providers commonly embed an API token in the path or query
/// string, and `ArcusSpotChainConfig::validate` only forbids inline
/// userinfo credentials, not that far more common pattern.
fn redact_rpc_url(rpc_url: &str) -> String {
    match url::Url::parse(rpc_url.trim()) {
        Ok(url) => match url.host_str() {
            Some(host) => match url.port() {
                Some(port) => format!("{}://{host}:{port}", url.scheme()),
                None => format!("{}://{host}", url.scheme()),
            },
            None => "<unparseable RPC URL>".to_string(),
        },
        Err(_) => "<unparseable RPC URL>".to_string(),
    }
}

fn validate_settlement_receipt(
    receipt: &TransactionReceipt,
    confirmed_tx_hash: H256,
    expected: ValidatedSettlementReceiptExpectation,
) -> Result<()> {
    if receipt.transaction_hash != confirmed_tx_hash {
        bail!("Arcus Spot receipt transaction hash does not match confirmed transaction");
    }
    if receipt.status.map(|status| status.as_u64()) != Some(1) {
        bail!("Arcus Spot settlement transaction did not succeed");
    }
    if receipt.to != Some(expected.swap_shell) {
        bail!("Arcus Spot settlement transaction did not target the canonical SwapShell");
    }

    let matching_events = receipt
        .logs
        .iter()
        .filter(|log| log.address == expected.swap_shell)
        .filter_map(|log| {
            SwapExecutedFilter::decode_log(&RawLog {
                topics: log.topics.clone(),
                data: log.data.to_vec(),
            })
            .ok()
        })
        .filter(|event| {
            event.taker == expected.taker
                && event.token_in == expected.sell_token
                && event.token_out == expected.buy_token
        })
        .collect::<Vec<_>>();
    if matching_events.len() != 1 {
        bail!(
            "Arcus Spot settlement receipt contains {} matching SwapExecuted events; expected exactly one",
            matching_events.len()
        );
    }
    let event = &matching_events[0];
    if event.router != expected.venue_router {
        bail!("Arcus Spot SwapExecuted router does not match the signed venue");
    }
    if event.route_tag != expected.route_tag {
        bail!("Arcus Spot SwapExecuted route tag does not match the signed venue");
    }
    if !event.success {
        bail!("Arcus Spot SwapExecuted reported an unsuccessful swap");
    }
    if event.amount_in != expected.sell_amount || event.quoted_amount_in != expected.sell_amount {
        bail!("Arcus Spot SwapExecuted input amount does not match the signed amount");
    }
    if event.min_amount_out != expected.minimum_buy_amount {
        bail!("Arcus Spot SwapExecuted minimum output does not match the signed minimum");
    }
    if event.quoted_amount_out < expected.minimum_buy_amount
        || event.amount_out < expected.minimum_buy_amount
    {
        bail!("Arcus Spot SwapExecuted output is below the signed minimum");
    }
    Ok(())
}

impl ArcusSpotChainClient {
    pub fn new(config: ArcusSpotChainConfig) -> Result<Self> {
        config.validate()?;
        let providers = config
            .rpc_urls
            .iter()
            .map(|rpc_url| {
                Provider::<Http>::try_from(rpc_url.trim())
                    .with_context(|| {
                        format!("could not construct Arcus RPC provider for {rpc_url}")
                    })
                    .map(|provider| {
                        Arc::new(
                            provider.interval(Duration::from_millis(config.request_interval_ms)),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { config, providers })
    }

    pub fn chain_id(&self) -> u64 {
        self.config.chain_id
    }

    /// Run `attempt` against each configured provider in order, returning
    /// the first success together with the provider that produced it (so a
    /// caller needing a follow-up read can stay on the same provider rather
    /// than risking an inconsistent mix). `ProviderAttemptError::Fatal`
    /// aborts immediately without trying later providers. Every attempt
    /// failing with only `Transient` errors returns the *last* one --
    /// earlier transport errors are still visible in logs via
    /// `log::warn!`, but the caller only needs one final cause.
    async fn try_providers<T, F, Fut>(&self, attempt: F) -> Result<(T, Arc<Provider<Http>>)>
    where
        F: Fn(Arc<Provider<Http>>) -> Fut,
        Fut: std::future::Future<Output = Result<T, ProviderAttemptError>>,
    {
        let mut last_error = None;
        for (index, provider) in self.providers.iter().enumerate() {
            match attempt(provider.clone()).await {
                Ok(value) => return Ok((value, provider.clone())),
                Err(ProviderAttemptError::Fatal(error)) => return Err(error),
                Err(ProviderAttemptError::Transient(error)) => {
                    // Only the top-level context message (e.g. "Arcus
                    // chainId read failed"), never the full `{:#}` source
                    // chain: the wrapped HTTP-client error for a
                    // connection/timeout failure commonly includes the
                    // request URL itself, which can carry an API token in
                    // its path or query -- the same leak `redact_rpc_url`
                    // exists to prevent, through a different value
                    // (Codex P1 follow-up, pairtrade#182).
                    log::warn!(
                        "[ARCUS_RPC] provider {index} ({}) failed: {error}",
                        redact_rpc_url(&self.config.rpc_urls[index])
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Arcus rpc_urls is empty")))
    }

    pub async fn preflight(
        &self,
        request: &ArcusSpotChainPreflightRequest,
    ) -> Result<ArcusSpotChainPreflight> {
        let request_values = request.validate()?;
        let permit_deadline = request.permit_deadline;
        let (attempt, _provider) = self
            .try_providers(|provider| async move {
                // Checked on its own, sequentially, before any of the
                // concurrent reads below: this is the one field where a
                // successful-but-wrong answer must never be masked by a
                // sibling call's transport error inside the same
                // tokio::join! (which would make try_providers treat a
                // definitively wrong-network provider as merely
                // "unreachable" and silently move on).
                let chain_id = provider
                    .get_chainid()
                    .await
                    .context("Arcus chainId read failed")?;
                if chain_id != U256::from(self.config.chain_id) {
                    return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                        "Arcus RPC chainId {chain_id} does not match configured {}",
                        self.config.chain_id
                    )));
                }

                let sell_contract = ArcusSpotErc20::new(request_values.sell_token, provider.clone());
                let buy_contract = ArcusSpotErc20::new(request_values.buy_token, provider.clone());
                let sell_balance_call = sell_contract.balance_of(request_values.taker);
                let buy_balance_call = buy_contract.balance_of(request_values.taker);
                let allowance_call =
                    sell_contract.allowance(request_values.taker, request_values.permit2);
                let (sell_balance_result, buy_balance_result, gas_balance_result, allowance_result) =
                    tokio::join!(
                        sell_balance_call.call(),
                        buy_balance_call.call(),
                        provider.get_balance(request_values.taker, None),
                        allowance_call.call(),
                    );
                let sell_balance_outcome = classify_contract_call(sell_balance_result);
                let buy_balance_outcome = classify_contract_call(buy_balance_result);
                let allowance_outcome = classify_contract_call(allowance_result);
                // get_balance is a direct provider call, not a contract
                // call, so it has no revert/decode concept -- any failure
                // there is transport-level by construction.
                let gas_balance_outcome = match gas_balance_result {
                    Ok(value) => ContractCallOutcome::Ok(value),
                    Err(error) => ContractCallOutcome::Transport(anyhow::Error::new(error)),
                };

                // Evaluate every outcome that succeeded immediately, as
                // Fatal, *before* looking at whether any sibling merely
                // transport-failed: a floor violation, an overbroad
                // allowance, or a non-transport contract-level failure
                // (a revert/decoding error on a reachable provider) is
                // real on-chain safety-relevant state this provider just
                // reported, not a sign it is unreachable. Discovering it
                // only after failing to unwrap every field together (as
                // the tuple-of-`?` version of this code did) would let a
                // sibling's ordinary transport error make the whole
                // attempt look Transient and mask it (Codex P1 follow-up,
                // pairtrade#182, round 5).
                if let ContractCallOutcome::NonTransport(error) = &sell_balance_outcome {
                    return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                        "Arcus sell balance read returned an unexpected response: {error}"
                    )));
                }
                if let ContractCallOutcome::Ok(value) = &sell_balance_outcome {
                    if let Err(error) = enforce_sell_floor(*value, &request_values) {
                        return Err(ProviderAttemptError::Fatal(error));
                    }
                }
                if let ContractCallOutcome::NonTransport(error) = &buy_balance_outcome {
                    return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                        "Arcus buy balance read returned an unexpected response: {error}"
                    )));
                }
                if let ContractCallOutcome::Ok(value) = &buy_balance_outcome {
                    if let Err(error) = enforce_buy_floor(*value, &request_values) {
                        return Err(ProviderAttemptError::Fatal(error));
                    }
                }
                if let ContractCallOutcome::Ok(value) = &gas_balance_outcome {
                    if let Err(error) = enforce_gas_floor(*value, &request_values) {
                        return Err(ProviderAttemptError::Fatal(error));
                    }
                }
                if let ContractCallOutcome::NonTransport(error) = &allowance_outcome {
                    return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                        "Arcus Permit2 allowance read returned an unexpected response: {error}"
                    )));
                }
                if let ContractCallOutcome::Ok(value) = &allowance_outcome {
                    if *value > request_values.required_sell {
                        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                            "Permit2 allowance {value} exceeds the exact required amount {}; refusing an overbroad approval",
                            request_values.required_sell
                        )));
                    }
                }

                // Nothing Fatal was found among whichever fields
                // succeeded; only now does a sibling's transport failure
                // matter, and only then is the whole attempt Transient.
                let mut transient_error: Option<anyhow::Error> = None;
                for (label, outcome) in [
                    ("sell balance", &sell_balance_outcome),
                    ("buy balance", &buy_balance_outcome),
                    ("gas balance", &gas_balance_outcome),
                    ("Permit2 allowance", &allowance_outcome),
                ] {
                    // Label only, never the underlying error's own Display:
                    // the wrapped HTTP-client error for a connection/
                    // timeout failure commonly embeds the request URL,
                    // which can carry an API token -- interpolating it
                    // into a *new* top-level message would leak it
                    // regardless of which format specifier the eventual
                    // log line uses, since it becomes part of what `{}`
                    // itself prints (Codex P1 follow-up, pairtrade#182,
                    // round 6).
                    if let ContractCallOutcome::Transport(_) = outcome {
                        transient_error.get_or_insert_with(|| {
                            anyhow::anyhow!("Arcus {label} read failed (transport)")
                        });
                    }
                }
                if let Some(error) = transient_error {
                    return Err(ProviderAttemptError::Transient(error));
                }

                let sell_balance = expect_ok(sell_balance_outcome);
                let buy_balance = expect_ok(buy_balance_outcome);
                let gas_balance = expect_ok(gas_balance_outcome);
                let allowance = expect_ok(allowance_outcome);

                let exact_value_permit = if allowance == request_values.required_sell {
                    None
                } else {
                    let token_name_call = sell_contract.name();
                    let nonce_call = sell_contract.nonces(request_values.taker);
                    // Same "collect every outcome, let any Fatal finding
                    // take precedence over a sibling's Transport error"
                    // shape as the balance/allowance batch above (Codex
                    // P1/P2 follow-up, pairtrade#182, rounds 4-5): a
                    // reachable provider's empty name, or its outright
                    // lack of support for one of these calls, must not be
                    // masked by the *other* call merely transport-failing.
                    let (name_result, nonce_result) =
                        tokio::join!(token_name_call.call(), nonce_call.call());
                    let name_outcome = classify_contract_call(name_result);
                    let nonce_outcome = classify_contract_call(nonce_result);

                    if let ContractCallOutcome::NonTransport(error) = &name_outcome {
                        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                            "sell token does not support the required EIP-2612 name(): {error}"
                        )));
                    }
                    if let ContractCallOutcome::Ok(name) = &name_outcome {
                        if name.trim().is_empty() {
                            return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                                "sell token returned an empty EIP-2612 name"
                            )));
                        }
                    }
                    if let ContractCallOutcome::NonTransport(error) = &nonce_outcome {
                        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                            "sell token does not support the required EIP-2612 nonces(): {error}"
                        )));
                    }

                    // Label only, same reasoning as the balance/allowance
                    // batch above -- these are Transport (HTTP-layer)
                    // failures, whose wrapped error can carry a leaked URL
                    // (Codex P1 follow-up, pairtrade#182, round 6).
                    let mut transient_error: Option<anyhow::Error> = None;
                    if let ContractCallOutcome::Transport(_) = &name_outcome {
                        transient_error
                            .get_or_insert_with(|| anyhow::anyhow!("Arcus EIP-2612 name() read failed (transport)"));
                    }
                    if let ContractCallOutcome::Transport(_) = &nonce_outcome {
                        transient_error.get_or_insert_with(|| {
                            anyhow::anyhow!("Arcus EIP-2612 nonces() read failed (transport)")
                        });
                    }
                    if let Some(error) = transient_error {
                        return Err(ProviderAttemptError::Transient(error));
                    }

                    let token_name = expect_ok(name_outcome);
                    let nonce = expect_ok(nonce_outcome);

                    // version() is different: only a confirmed
                    // non-transport response (the contract reverted, or
                    // its return data didn't decode -- i.e. it simply
                    // doesn't implement EIP-2612's optional version())
                    // defaults to the legacy "1", rather than being Fatal
                    // like name()/nonces() above, since a missing
                    // version() is a normal, spec-permitted token shape.
                    let token_version = match classify_contract_call(sell_contract.version().call().await) {
                        ContractCallOutcome::Ok(version) => version,
                        ContractCallOutcome::Transport(error) => {
                            return Err(ProviderAttemptError::Transient(
                                error.context("Arcus EIP-2612 version() read failed"),
                            ));
                        }
                        ContractCallOutcome::NonTransport(_) => "1".to_string(),
                    };
                    if token_version.trim().is_empty() {
                        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                            "sell token returned an empty EIP-2612 version"
                        )));
                    }

                    Some(ArcusSpotEip2612PermitContext {
                        token_name,
                        token_version,
                        nonce: nonce.to_string(),
                        deadline: permit_deadline,
                    })
                };

                Ok(PreflightAttempt {
                    sell_balance,
                    buy_balance,
                    gas_balance,
                    allowance,
                    exact_value_permit,
                })
            })
            .await?;
        let PreflightAttempt {
            sell_balance,
            buy_balance,
            gas_balance,
            allowance,
            exact_value_permit,
        } = attempt;

        Ok(ArcusSpotChainPreflight {
            chain_id: self.config.chain_id,
            balances: balance_snapshot(
                request_values.taker,
                request_values.sell_token,
                request_values.buy_token,
                sell_balance,
                buy_balance,
                gas_balance,
            ),
            permit2_allowance_raw: allowance.to_string(),
            exact_value_permit,
        })
    }

    pub async fn balances(
        &self,
        taker: Address,
        sell_token: Address,
        buy_token: Address,
    ) -> Result<ArcusSpotBalanceSnapshot> {
        if taker == Address::zero()
            || sell_token == Address::zero()
            || buy_token == Address::zero()
            || sell_token == buy_token
        {
            bail!("invalid Arcus balance request addresses");
        }
        let chain_id = self.config.chain_id;
        let (raw, _provider) = self
            .try_providers(|provider| {
                read_latest_balances_from_provider(provider, chain_id, taker, sell_token, buy_token)
            })
            .await?;
        let RawBalanceReads {
            sell_balance,
            buy_balance,
            gas_balance,
        } = raw;
        Ok(balance_snapshot(
            taker,
            sell_token,
            buy_token,
            sell_balance,
            buy_balance,
            gas_balance,
        ))
    }

    /// Like `balances`, but requires proof that whichever provider answers
    /// has actually observed `confirmed_tx_hash` before trusting any
    /// balance it reports.
    ///
    /// Earlier rounds of this method refused to fall back to a secondary
    /// provider at all, since a merely-reachable-but-lagging provider
    /// could otherwise return the pre-swap balance as an apparently valid
    /// snapshot and `reconcile_balances` would read that as a genuine
    /// reconciliation failure, permanently marking the attempt sticky
    /// `Unknown` (Codex P1 follow-up, pairtrade#182, rounds 4 and 6). Every
    /// attempt requires the confirmed transaction's own receipt to be
    /// present *and* this provider's own current block number to be at or
    /// beyond that receipt's block before its `latest` balances are
    /// trusted (rounds 7-8; re-derived under bot-strategy#880 after the
    /// EIP-1898-pinned-block design those rounds originally settled on
    /// turned out to depend on historical-state retention windows shorter
    /// than this bot's own reconciliation cadence in practice -- see
    /// `balances_requiring_receipt`'s inline comment for the full
    /// history). That proof is what makes a result trustworthy -- not
    /// which provider happened to answer. So falling back through every
    /// configured provider is safe, and restores availability a
    /// permanently-primary-only design would otherwise lose if provider 0
    /// specifically never catches up (Codex P1 follow-up, pairtrade#182,
    /// round 9): "hasn't indexed this tx yet" is `Transient` on any
    /// provider, and try_providers moves on.
    pub async fn balances_requiring_primary_provider(
        &self,
        taker: Address,
        sell_token: Address,
        buy_token: Address,
        confirmed_tx_hash: H256,
    ) -> Result<ArcusSpotBalanceSnapshot> {
        self.balances_requiring_receipt(taker, sell_token, buy_token, confirmed_tx_hash, None)
            .await
    }

    /// Reconciliation read that additionally requires one exact
    /// `SwapExecuted` event from the canonical Arcus SwapShell. This is the
    /// live execution path for both Arcus and Rialto hosted-router routes.
    pub async fn balances_requiring_settlement_receipt(
        &self,
        expectation: &ArcusSpotSettlementReceiptExpectation,
        confirmed_tx_hash: H256,
    ) -> Result<ArcusSpotBalanceSnapshot> {
        let expected = expectation.validate()?;
        self.balances_requiring_receipt(
            expected.taker,
            expected.sell_token,
            expected.buy_token,
            confirmed_tx_hash,
            Some(expected),
        )
        .await
    }

    async fn balances_requiring_receipt(
        &self,
        taker: Address,
        sell_token: Address,
        buy_token: Address,
        confirmed_tx_hash: H256,
        settlement: Option<ValidatedSettlementReceiptExpectation>,
    ) -> Result<ArcusSpotBalanceSnapshot> {
        if taker == Address::zero()
            || sell_token == Address::zero()
            || buy_token == Address::zero()
            || sell_token == buy_token
        {
            bail!("invalid Arcus balance request addresses");
        }
        let chain_id = self.config.chain_id;
        let (raw, _provider) = self
            .try_providers(|provider| async move {
                // Neither read depends on the other's *result* (only on
                // each having answered before the freshness check below),
                // so fetch them concurrently rather than paying two
                // sequential round trips against a provider this whole
                // change is trying to make reconciliation less latency-
                // sensitive to.
                let (receipt, current_block_number) = tokio::join!(
                    provider.get_transaction_receipt(confirmed_tx_hash),
                    provider.get_block_number(),
                );
                let receipt = receipt.context("Arcus confirmed-transaction receipt read failed")?;
                let Some(receipt) = receipt else {
                    return Err(ProviderAttemptError::Transient(anyhow::anyhow!(
                        "Arcus provider has not yet indexed confirmed tx {confirmed_tx_hash:#x}"
                    )));
                };
                if let Some(expected) = settlement {
                    validate_settlement_receipt(&receipt, confirmed_tx_hash, expected)
                        .map_err(ProviderAttemptError::Fatal)?;
                }
                // bot-strategy#880: this used to pin the balance reads to
                // the receipt's own block hash via EIP-1898
                // requireCanonical=true, so a stale/reorged backend would
                // reject the read instead of silently returning fork
                // state. In production, both configured RPC providers
                // turned out to retain that pinnable historical state for
                // only ~15-20 minutes -- far short of this bot's own
                // 15-minute live-tick cadence -- so the pinned read failed
                // on essentially every reconciliation attempt, requiring
                // repeated manual recovery.
                //
                // A single RPC URL commonly load-balances across a pool of
                // backend nodes, so the receipt lookup above and the
                // `latest` balance read below can still land on different
                // backends even when both requests go to "the same
                // provider" from this code's point of view. Proving the
                // receipt exists only shows *some* backend behind this URL
                // has caught up to the confirmed block -- it does not by
                // itself prove the backend that answers the balance read
                // has. Guard against that explicitly: require this
                // backend's own current block number to be at or beyond
                // the receipt's block number immediately before trusting
                // its `latest` balances. A backend that hasn't caught up
                // is Transient, so a later configured provider gets a
                // chance to serve consistent state instead.
                //
                // This intentionally still cannot prove the returned
                // balances reflect *only* this swap and nothing else that
                // touched the wallet in between -- unlike the retired
                // pinned-block read, which isolated exactly one block.
                // `reconciled_runtime_fill` (live_executor.rs) closes that
                // gap on the consumer side: it requires the computed sell
                // delta to equal the dispatched plan's own
                // `sell_amount_raw` exactly, and refuses (fail-closed, the
                // existing manual-recovery path takes over) rather than
                // commit a reconciliation that doesn't match.
                let receipt_block_number = receipt
                    .block_number
                    .context("Arcus confirmed-transaction receipt is missing its block number")?;
                // `.context(...)` rather than interpolating `{error}`
                // directly: `ProviderError::HTTPError` wraps a
                // `reqwest::Error` whose Display commonly includes the
                // failing request URL, and RPC URLs configured here can
                // carry an API key in the path/query (Codex P1 follow-up,
                // pairtrade#182, round 6 -- see `redact_rpc_url`). anyhow's
                // plain `{}`/`{error}` Display only renders the top-level
                // context, not the chained source, so this stays safe to
                // log/propagate unredacted the same way every other error
                // in this function already is.
                let current_block_number = current_block_number
                    .context("Arcus current block number read failed")
                    .map_err(ProviderAttemptError::Transient)?;
                if current_block_number < receipt_block_number {
                    return Err(ProviderAttemptError::Transient(anyhow::anyhow!(
                        "Arcus provider's latest block {current_block_number} has not caught up \
                         to confirmed tx's block {receipt_block_number}"
                    )));
                }
                read_latest_balances_from_provider(provider, chain_id, taker, sell_token, buy_token)
                    .await
            })
            .await?;
        Ok(balance_snapshot(
            taker,
            sell_token,
            buy_token,
            raw.sell_balance,
            raw.buy_balance,
            raw.gas_balance,
        ))
    }
}

/// Current-state (`latest`) reads, used by preflight/status and (as of
/// bot-strategy#880) also by reconciliation once a provider has proven
/// (via `balances_requiring_receipt`'s receipt + block-number checks) that
/// it is caught up to the confirmed transaction's block.
async fn read_latest_balances_from_provider(
    provider: Arc<Provider<Http>>,
    expected_chain_id: u64,
    taker: Address,
    sell_token: Address,
    buy_token: Address,
) -> Result<RawBalanceReads, ProviderAttemptError> {
    // Sequential and fatal-on-mismatch for the same reason as in
    // `preflight`: a wrong-but-successfully-fetched chain id must never be
    // masked by a sibling call's transport error inside the same
    // tokio::join!.
    let chain_id = provider
        .get_chainid()
        .await
        .context("Arcus chainId read failed")?;
    if chain_id != U256::from(expected_chain_id) {
        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
            "Arcus RPC chainId changed during balance reconciliation"
        )));
    }

    let sell_contract = ArcusSpotErc20::new(sell_token, provider.clone());
    let buy_contract = ArcusSpotErc20::new(buy_token, provider.clone());
    let sell_balance_call = sell_contract.balance_of(taker);
    let buy_balance_call = buy_contract.balance_of(taker);
    let (sell_balance_result, buy_balance_result, gas_balance_result) = tokio::join!(
        sell_balance_call.call(),
        buy_balance_call.call(),
        provider.get_balance(taker, None),
    );
    // Same "classify, let any Fatal finding outrank a sibling's Transport
    // error" shape as preflight's balance/allowance batch (Codex P2
    // follow-up, pairtrade#182, round 7): a reachable provider's revert/
    // decode failure on balanceOf is a real, permanent property of the
    // token contract, not evidence of unreachability, and must not be
    // masked by a sibling merely transport-failing.
    let sell_balance_outcome = classify_contract_call(sell_balance_result);
    let buy_balance_outcome = classify_contract_call(buy_balance_result);
    let gas_balance_outcome = match gas_balance_result {
        Ok(value) => ContractCallOutcome::Ok(value),
        Err(error) => ContractCallOutcome::Transport(anyhow::Error::new(error)),
    };

    if let ContractCallOutcome::NonTransport(error) = &sell_balance_outcome {
        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
            "Arcus sell balance read returned an unexpected response: {error}"
        )));
    }
    if let ContractCallOutcome::NonTransport(error) = &buy_balance_outcome {
        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
            "Arcus buy balance read returned an unexpected response: {error}"
        )));
    }

    // Label which field(s) transport-failed rather than collapsing to one
    // generic message: this is the only signal reconciliation gets once
    // every configured provider has failed the same way (try_providers
    // surfaces only the *last* attempt's error) and gas balance reads in
    // particular have no sell/buy-style Fatal path of their own to narrow
    // it down otherwise. Deliberately not interpolating the underlying
    // error's own Display here -- `ProviderError::HTTPError` can carry the
    // failing request URL, and a bare field label is enough to point an
    // operator at which read to investigate without risking a leak.
    let transient_fields: Vec<&str> = [
        ("sell", &sell_balance_outcome),
        ("buy", &buy_balance_outcome),
        ("gas", &gas_balance_outcome),
    ]
    .into_iter()
    .filter_map(|(label, outcome)| {
        matches!(outcome, ContractCallOutcome::Transport(_)).then_some(label)
    })
    .collect();
    if !transient_fields.is_empty() {
        return Err(ProviderAttemptError::Transient(anyhow::anyhow!(
            "Arcus balance read failed (transport): {}",
            transient_fields.join(", ")
        )));
    }

    Ok(RawBalanceReads {
        sell_balance: expect_ok(sell_balance_outcome),
        buy_balance: expect_ok(buy_balance_outcome),
        gas_balance: expect_ok(gas_balance_outcome),
    })
}

// Split into one function per field (rather than one combined check) so
// each can be evaluated the instant its own value is known, independent
// of whether a *sibling* read succeeded or transport-failed (Codex P1
// follow-up, pairtrade#182, round 5).
fn enforce_sell_floor(sell_balance: U256, request: &ValidatedPreflightRequest) -> Result<()> {
    let residual = sell_balance
        .checked_sub(request.required_sell)
        .context("sell balance is below the required amount")?;
    if residual < request.sell_floor {
        bail!(
            "post-swap sell balance {residual} would be below floor {}",
            request.sell_floor
        );
    }
    Ok(())
}

fn enforce_buy_floor(buy_balance: U256, request: &ValidatedPreflightRequest) -> Result<()> {
    if buy_balance < request.buy_floor {
        bail!(
            "current buy balance {buy_balance} is below floor {}",
            request.buy_floor
        );
    }
    Ok(())
}

fn enforce_gas_floor(gas_balance: U256, request: &ValidatedPreflightRequest) -> Result<()> {
    if gas_balance < request.minimum_gas {
        bail!(
            "gas balance {gas_balance} is below minimum {}",
            request.minimum_gas
        );
    }
    Ok(())
}

fn balance_snapshot(
    _taker: Address,
    sell_token: Address,
    buy_token: Address,
    sell_balance: U256,
    buy_balance: U256,
    gas_balance: U256,
) -> ArcusSpotBalanceSnapshot {
    ArcusSpotBalanceSnapshot {
        observed_at: Utc::now(),
        sell_token: format!("{sell_token:#x}"),
        buy_token: format!("{buy_token:#x}"),
        sell_balance_raw: sell_balance.to_string(),
        buy_balance_raw: buy_balance.to_string(),
        gas_balance_wei: gas_balance.to_string(),
    }
}

fn parse_nonzero_address(label: &str, raw: &str) -> Result<Address> {
    let address =
        Address::from_str(raw.trim()).with_context(|| format!("invalid Arcus {label}"))?;
    if address == Address::zero() {
        bail!("Arcus {label} must not be zero");
    }
    Ok(address)
}

fn parse_amount(label: &str, raw: &str) -> Result<U256> {
    U256::from_dec_str(raw.trim()).with_context(|| format!("invalid Arcus {label}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::{
        abi::{encode, Token},
        types::{Bytes, Log, U64},
        utils::keccak256,
    };
    use serde_json::{json, Value};
    use std::sync::Mutex;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    enum RpcReply {
        Result(Value),
        Error { code: i64, message: &'static str },
        Disconnect,
    }

    struct TestRpcServer {
        url: String,
        requests: Arc<Mutex<Vec<Value>>>,
        task: JoinHandle<()>,
    }

    impl Drop for TestRpcServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn spawn_rpc_server<F>(handler: F) -> TestRpcServer
    where
        F: Fn(&Value) -> RpcReply + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded_requests = requests.clone();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let handler = handler.clone();
                let recorded_requests = recorded_requests.clone();
                tokio::spawn(async move {
                    let request = read_json_rpc_request(&mut stream).await;
                    recorded_requests.lock().unwrap().push(request.clone());
                    let id = request.get("id").cloned().unwrap_or(Value::Null);
                    let response = match handler(&request) {
                        RpcReply::Result(result) => {
                            json!({"jsonrpc": "2.0", "id": id, "result": result})
                        }
                        RpcReply::Error { code, message } => json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {"code": code, "message": message}
                        }),
                        RpcReply::Disconnect => return,
                    };
                    let body = serde_json::to_vec(&response).unwrap();
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(headers.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                });
            }
        });
        TestRpcServer {
            url: format!("http://{address}"),
            requests,
            task,
        }
    }

    async fn read_json_rpc_request(stream: &mut tokio::net::TcpStream) -> Value {
        let mut bytes = Vec::new();
        let header_end = loop {
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            let mut buffer = [0_u8; 1024];
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0, "HTTP request ended before its headers");
            bytes.extend_from_slice(&buffer[..read]);
        };
        let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while bytes.len() < header_end + content_length {
            let mut buffer = [0_u8; 1024];
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0, "HTTP request ended before its JSON body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        serde_json::from_slice(&bytes[header_end..header_end + content_length]).unwrap()
    }

    fn rpc_config(urls: Vec<String>) -> ArcusSpotChainConfig {
        ArcusSpotChainConfig {
            rpc_urls: urls,
            chain_id: 4663,
            request_interval_ms: 1,
        }
    }

    fn test_addresses() -> (Address, Address, Address) {
        (
            Address::from_str("0x7600000000000000000000000000000000000001").unwrap(),
            Address::from_str("0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC").unwrap(),
            Address::from_str("0x86923f96303D656E4aa86D9d42D1e57ad2023fdC").unwrap(),
        )
    }

    fn receipt_json(block_hash: H256) -> Value {
        receipt_json_at(block_hash, TEST_RECEIPT_BLOCK_NUMBER)
    }

    fn receipt_json_at(block_hash: H256, block_number: u64) -> Value {
        let receipt = TransactionReceipt {
            block_hash: Some(block_hash),
            block_number: Some(U64::from(block_number)),
            ..Default::default()
        };
        serde_json::to_value(receipt).unwrap()
    }

    // Every mock server below answers `eth_blockNumber` with this value
    // unless a test overrides it, so the new freshness check in
    // `balances_requiring_receipt` (bot-strategy#880) sees this provider as
    // caught up to `TEST_RECEIPT_BLOCK_NUMBER` by default.
    const TEST_RECEIPT_BLOCK_NUMBER: u64 = 100;

    fn indexed_address(address: Address) -> H256 {
        let mut topic = [0_u8; 32];
        topic[12..].copy_from_slice(address.as_bytes());
        H256::from(topic)
    }

    fn settlement_expectation(
        venue: &str,
        router: Address,
    ) -> ArcusSpotSettlementReceiptExpectation {
        let (taker, sell_token, buy_token) = test_addresses();
        ArcusSpotSettlementReceiptExpectation {
            venue: venue.to_string(),
            taker: format!("{taker:#x}"),
            sell_token: format!("{sell_token:#x}"),
            buy_token: format!("{buy_token:#x}"),
            sell_amount_raw: "1000".to_string(),
            minimum_buy_amount_raw: "980".to_string(),
            swap_shell: "0x4262efBd176F02824af27010bEa218429c33c7E8".to_string(),
            venue_router: format!("{router:#x}"),
        }
    }

    fn settlement_receipt(
        route_tag: &str,
        router: Address,
        success: bool,
        amount_out: u64,
    ) -> TransactionReceipt {
        let (taker, sell_token, buy_token) = test_addresses();
        let swap_shell = Address::from_str("0x4262efBd176F02824af27010bEa218429c33c7E8").unwrap();
        let tx_hash = H256::from_low_u64_be(0x818);
        let mut tag = [0_u8; 32];
        tag[..route_tag.len()].copy_from_slice(route_tag.as_bytes());
        let event_signature = keccak256(
            "SwapExecuted(address,address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256,address,bytes32,bool,string)",
        );
        let data = encode(&[
            Token::Uint(U256::from(980)),
            Token::Uint(U256::from(1000)),
            Token::Uint(U256::from(1000)),
            Token::Uint(U256::from(990)),
            Token::Uint(U256::from(amount_out)),
            Token::Uint(U256::from(1)),
            Token::Uint(U256::from(1)),
            Token::Address(router),
            Token::FixedBytes(tag.to_vec()),
            Token::Bool(success),
            Token::String(String::new()),
        ]);
        TransactionReceipt {
            transaction_hash: tx_hash,
            to: Some(swap_shell),
            status: Some(U64::from(1)),
            logs: vec![Log {
                address: swap_shell,
                topics: vec![
                    H256::from(event_signature),
                    indexed_address(taker),
                    indexed_address(sell_token),
                    indexed_address(buy_token),
                ],
                data: Bytes::from(data),
                transaction_hash: Some(tx_hash),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn abi_u256(value: u64) -> Value {
        json!(format!("0x{value:064x}"))
    }

    fn successful_reconciliation_reply(
        request: &Value,
        block_hash: H256,
        sell_token: Address,
        buy_token: Address,
        sell_balance: u64,
        buy_balance: u64,
        gas_balance: u64,
    ) -> RpcReply {
        match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_blockNumber" => {
                RpcReply::Result(json!(format!("0x{TEST_RECEIPT_BLOCK_NUMBER:x}")))
            }
            "eth_call" => {
                let to = request["params"][0]["to"].as_str().unwrap();
                if to.eq_ignore_ascii_case(&format!("{sell_token:#x}")) {
                    RpcReply::Result(abi_u256(sell_balance))
                } else if to.eq_ignore_ascii_case(&format!("{buy_token:#x}")) {
                    RpcReply::Result(abi_u256(buy_balance))
                } else {
                    RpcReply::Error {
                        code: -32602,
                        message: "unexpected token",
                    }
                }
            }
            "eth_getBalance" => RpcReply::Result(json!(format!("0x{gas_balance:x}"))),
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        }
    }

    fn request_snapshot(server: &TestRpcServer) -> Vec<Value> {
        server.requests.lock().unwrap().clone()
    }

    fn request() -> ArcusSpotChainPreflightRequest {
        ArcusSpotChainPreflightRequest {
            taker: "0x7600000000000000000000000000000000000001".to_string(),
            sell_token: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            buy_token: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            permit2: "0x000000000022D473030F116dDEE9F6B43aC78BA3".to_string(),
            required_sell_amount_raw: "1000".to_string(),
            sell_floor_raw: "500".to_string(),
            buy_floor_raw: "500".to_string(),
            minimum_gas_balance_wei: "100".to_string(),
            permit_deadline: 2_000_000_000,
        }
    }

    #[test]
    fn validates_exact_preflight_request() {
        let values = request().validate().unwrap();
        assert_eq!(values.required_sell, U256::from(1000));
    }

    #[test]
    fn accepts_exact_arcus_and_rialto_swap_shell_events() {
        for (venue, route_tag, router) in [
            (
                "arcus",
                "ARCUS",
                Address::from_str("0x006102b16A04c20306A28b652745D3973D7D24fa").unwrap(),
            ),
            (
                "rialto",
                "RIALTO",
                Address::from_str("0xC94135b63772b91D79d0A2DaAb2a8801f32359bD").unwrap(),
            ),
        ] {
            let receipt = settlement_receipt(route_tag, router, true, 985);
            let expectation = settlement_expectation(venue, router).validate().unwrap();
            validate_settlement_receipt(&receipt, H256::from_low_u64_be(0x818), expectation)
                .unwrap();
        }
    }

    #[test]
    fn rejects_swap_shell_event_for_wrong_venue() {
        let rialto_router =
            Address::from_str("0xC94135b63772b91D79d0A2DaAb2a8801f32359bD").unwrap();
        let arcus_router = Address::from_str("0x006102b16A04c20306A28b652745D3973D7D24fa").unwrap();
        let receipt = settlement_receipt("RIALTO", rialto_router, true, 985);
        let expectation = settlement_expectation("arcus", arcus_router)
            .validate()
            .unwrap();
        let error =
            validate_settlement_receipt(&receipt, H256::from_low_u64_be(0x818), expectation)
                .unwrap_err();
        assert!(error.to_string().contains("router"));

        let wrong_tag = settlement_receipt("ARCUS", rialto_router, true, 985);
        let rialto_expectation = settlement_expectation("rialto", rialto_router)
            .validate()
            .unwrap();
        assert!(validate_settlement_receipt(
            &wrong_tag,
            H256::from_low_u64_be(0x818),
            rialto_expectation,
        )
        .unwrap_err()
        .to_string()
        .contains("route tag"));
    }

    #[test]
    fn rejects_failed_or_below_minimum_swap_shell_event() {
        let rialto_router =
            Address::from_str("0xC94135b63772b91D79d0A2DaAb2a8801f32359bD").unwrap();
        let expectation = settlement_expectation("rialto", rialto_router)
            .validate()
            .unwrap();

        let failed = settlement_receipt("RIALTO", rialto_router, false, 985);
        assert!(
            validate_settlement_receipt(&failed, H256::from_low_u64_be(0x818), expectation,)
                .unwrap_err()
                .to_string()
                .contains("unsuccessful")
        );

        let below_minimum = settlement_receipt("RIALTO", rialto_router, true, 979);
        assert!(validate_settlement_receipt(
            &below_minimum,
            H256::from_low_u64_be(0x818),
            expectation,
        )
        .unwrap_err()
        .to_string()
        .contains("below the signed minimum"));
    }

    #[test]
    fn rejects_floor_violation() {
        let values = request().validate().unwrap();
        assert!(enforce_sell_floor(U256::from(1499), &values).is_err());
        assert!(enforce_buy_floor(U256::from(500), &values).is_ok());
        assert!(enforce_gas_floor(U256::from(100), &values).is_ok());
    }

    #[test]
    fn granular_floor_checks_are_independent() {
        let values = request().validate().unwrap();
        assert!(enforce_buy_floor(U256::from(1), &values).is_err());
        assert!(enforce_gas_floor(U256::from(1), &values).is_err());
    }

    #[tokio::test]
    async fn reconciliation_reads_latest_balances_once_a_provider_has_caught_up_to_the_receipt() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x11);
        let block_hash = H256::from_low_u64_be(0x22);
        let server = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request, block_hash, sell_token, buy_token, 1_499, 500, 100,
            )
        })
        .await;
        let client = ArcusSpotChainClient::new(rpc_config(vec![server.url.clone()])).unwrap();

        let balances = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap();

        // Reconciliation returns exact on-chain balances; inventory floors
        // belong to preflight and are not silently reapplied or rounded here.
        assert_eq!(balances.sell_balance_raw, "1499");
        assert_eq!(balances.buy_balance_raw, "500");
        assert_eq!(balances.gas_balance_wei, "100");

        let requests = request_snapshot(&server);
        // bot-strategy#880: chainId, receipt, blockNumber (the new
        // freshness check), 2x eth_call, getBalance -- one more request
        // than the retired EIP-1898-pinned path, since there is no single
        // canonical-selector round trip covering all three balances.
        assert_eq!(requests.len(), 6);
        assert!(requests.iter().all(|request| matches!(
            request["method"].as_str().unwrap(),
            "eth_chainId"
                | "eth_getTransactionReceipt"
                | "eth_blockNumber"
                | "eth_call"
                | "eth_getBalance"
        )));
        assert!(!requests
            .iter()
            .any(|request| request["method"].as_str().unwrap().starts_with("eth_send")));

        let receipt_request = requests
            .iter()
            .find(|request| request["method"] == "eth_getTransactionReceipt")
            .unwrap();
        assert_eq!(receipt_request["params"], json!([format!("{tx_hash:#x}")]));

        // The retired design pinned every balance read to the receipt's
        // exact block hash via an EIP-1898 `{blockHash, requireCanonical}`
        // selector. bot-strategy#880 replaced that with plain `latest`
        // reads (guarded by the block-number freshness check proven
        // separately below) -- assert the *absence* of that selector
        // shape, not its presence.
        let eth_calls = requests
            .iter()
            .filter(|request| request["method"] == "eth_call")
            .collect::<Vec<_>>();
        assert_eq!(eth_calls.len(), 2);
        for request in &eth_calls {
            assert_ne!(
                request["params"][1],
                json!({"blockHash": format!("{block_hash:#x}"), "requireCanonical": true})
            );
        }
        let call_targets = requests
            .iter()
            .filter(|request| request["method"] == "eth_call")
            .map(|request| request["params"][0]["to"].as_str().unwrap().to_string())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            call_targets,
            std::collections::BTreeSet::from([
                format!("{sell_token:#x}"),
                format!("{buy_token:#x}")
            ])
        );
    }

    // bot-strategy#880: the core new safety property. A single RPC URL can
    // load-balance across backend nodes that haven't all caught up to the
    // same block -- proving the *receipt* exists (served by whichever
    // backend answered that specific request) does not by itself prove
    // the backend that will answer the *balance* reads has too. The first
    // provider here has the receipt but its own `eth_blockNumber` is still
    // behind the receipt's block, so it must be treated as not-yet-caught-up
    // (Transient) and skipped in favor of the second, fully-caught-up
    // provider -- never trusted for a `latest` balance read.
    #[tokio::test]
    async fn provider_lagging_behind_the_receipt_block_falls_back_to_the_next_provider() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x33);
        let block_hash = H256::from_low_u64_be(0x44);
        let receipt_block_number = 100;
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => {
                RpcReply::Result(receipt_json_at(block_hash, receipt_block_number))
            }
            "eth_blockNumber" => RpcReply::Result(json!("0x1")),
            _ => RpcReply::Error {
                code: -32601,
                message:
                    "unexpected method: balance reads must not be attempted on a lagging provider",
            },
        })
        .await;
        let second = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request, block_hash, sell_token, buy_token, 4_000, 2_985, 123,
            )
        })
        .await;
        let client =
            ArcusSpotChainClient::new(rpc_config(vec![first.url.clone(), second.url.clone()]))
                .unwrap();

        let balances = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap();
        assert_eq!(balances.sell_balance_raw, "4000");
        assert_eq!(balances.buy_balance_raw, "2985");
        // The lagging provider must never have been asked for a balance
        // (its handler would error on anything but chainId/receipt/blockNumber).
        assert_eq!(request_snapshot(&second).len(), 6);
    }

    #[tokio::test]
    async fn sell_balance_revert_is_fatal_without_fallback() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x45);
        let block_hash = H256::from_low_u64_be(0x46);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_blockNumber" => {
                RpcReply::Result(json!(format!("0x{TEST_RECEIPT_BLOCK_NUMBER:x}")))
            }
            "eth_call" if request["params"][0]["to"] == format!("{sell_token:#x}") => {
                RpcReply::Error {
                    code: 3,
                    message: "execution reverted: token paused",
                }
            }
            "eth_call" => RpcReply::Result(abi_u256(2_985)),
            "eth_getBalance" => RpcReply::Result(json!("0x64")),
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        })
        .await;
        let second = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request, block_hash, sell_token, buy_token, 4_000, 2_985, 100,
            )
        })
        .await;
        let client =
            ArcusSpotChainClient::new(rpc_config(vec![first.url.clone(), second.url.clone()]))
                .unwrap();

        let error = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("sell balance read returned an unexpected response"));
        assert!(request_snapshot(&second).is_empty());
    }

    #[tokio::test]
    async fn sell_balance_decode_error_is_fatal_without_fallback() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x47);
        let block_hash = H256::from_low_u64_be(0x48);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_blockNumber" => {
                RpcReply::Result(json!(format!("0x{TEST_RECEIPT_BLOCK_NUMBER:x}")))
            }
            "eth_call" if request["params"][0]["to"] == format!("{sell_token:#x}") => {
                // One byte instead of a 32-byte ABI word -- ethers' own
                // typed decode must reject this, not silently truncate it.
                RpcReply::Result(json!("0x01"))
            }
            "eth_call" => RpcReply::Result(abi_u256(2_985)),
            "eth_getBalance" => RpcReply::Result(json!("0x64")),
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        })
        .await;
        let second = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request, block_hash, sell_token, buy_token, 4_000, 2_985, 100,
            )
        })
        .await;
        let client =
            ArcusSpotChainClient::new(rpc_config(vec![first.url.clone(), second.url.clone()]))
                .unwrap();

        let error = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("sell balance read returned an unexpected response"));
        assert!(request_snapshot(&second).is_empty());
    }

    #[tokio::test]
    async fn transport_failure_falls_back_to_the_next_provider() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x49);
        let block_hash = H256::from_low_u64_be(0x4a);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_blockNumber" => {
                RpcReply::Result(json!(format!("0x{TEST_RECEIPT_BLOCK_NUMBER:x}")))
            }
            "eth_call" => RpcReply::Disconnect,
            "eth_getBalance" => RpcReply::Result(json!("0x64")),
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        })
        .await;
        let second = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request, block_hash, sell_token, buy_token, 4_000, 2_985, 100,
            )
        })
        .await;
        let client =
            ArcusSpotChainClient::new(rpc_config(vec![first.url.clone(), second.url.clone()]))
                .unwrap();

        let balances = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap();
        assert_eq!(balances.sell_balance_raw, "4000");
        assert_eq!(request_snapshot(&second).len(), 6);
    }

    #[tokio::test]
    async fn preflight_token_floor_violation_remains_fatal_without_fallback() {
        let (_taker, sell_token, buy_token) = test_addresses();
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getBalance" => RpcReply::Result(json!("0x64")),
            "eth_call" => {
                let to = request["params"][0]["to"].as_str().unwrap();
                let data = request["params"][0]["data"].as_str().unwrap();
                if data.starts_with("0xdd62ed3e") {
                    RpcReply::Result(abi_u256(1_000))
                } else if to.eq_ignore_ascii_case(&format!("{sell_token:#x}")) {
                    RpcReply::Result(abi_u256(1_499))
                } else if to.eq_ignore_ascii_case(&format!("{buy_token:#x}")) {
                    RpcReply::Result(abi_u256(500))
                } else {
                    RpcReply::Error {
                        code: -32602,
                        message: "unexpected token",
                    }
                }
            }
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        })
        .await;
        let second = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request,
                H256::from_low_u64_be(0x99),
                sell_token,
                buy_token,
                2_000,
                500,
                100,
            )
        })
        .await;
        let client =
            ArcusSpotChainClient::new(rpc_config(vec![first.url.clone(), second.url.clone()]))
                .unwrap();

        let error = client.preflight(&request()).await.unwrap_err();
        assert!(error.to_string().contains("below floor"));
        assert!(request_snapshot(&second).is_empty());
    }

    #[test]
    fn rejects_inline_rpc_credentials() {
        let config = ArcusSpotChainConfig {
            rpc_urls: vec!["https://user:secret@example.invalid".to_string()],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_empty_rpc_urls() {
        let config = ArcusSpotChainConfig {
            rpc_urls: vec![],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must not be empty"));
    }

    #[test]
    fn rejects_duplicate_rpc_urls() {
        let config = ArcusSpotChainConfig {
            rpc_urls: vec![
                "https://a.example.invalid".to_string(),
                "https://a.example.invalid".to_string(),
            ],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
    }

    #[test]
    fn accepts_multiple_distinct_rpc_urls() {
        let config = ArcusSpotChainConfig {
            rpc_urls: vec![
                "https://a.example.invalid".to_string(),
                "https://b.example.invalid".to_string(),
            ],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        config.validate().unwrap();
        let client = ArcusSpotChainClient::new(config).unwrap();
        assert_eq!(client.providers.len(), 2);
    }

    #[tokio::test]
    async fn try_providers_falls_back_after_an_earlier_provider_errors() {
        let config = ArcusSpotChainConfig {
            rpc_urls: vec![
                "https://a.example.invalid".to_string(),
                "https://b.example.invalid".to_string(),
            ],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        let client = ArcusSpotChainClient::new(config).unwrap();
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let result = client
            .try_providers(|_provider| {
                let attempts = attempts.clone();
                async move {
                    let seen = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if seen == 0 {
                        return Err(ProviderAttemptError::Transient(anyhow::anyhow!(
                            "simulated transport failure on the first provider"
                        )));
                    }
                    Ok(seen)
                }
            })
            .await
            .unwrap();
        assert_eq!(result.0, 1, "expected the second provider's result");
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn try_providers_returns_the_last_error_when_every_provider_fails() {
        let config = ArcusSpotChainConfig {
            rpc_urls: vec![
                "https://a.example.invalid".to_string(),
                "https://b.example.invalid".to_string(),
            ],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        let client = ArcusSpotChainClient::new(config).unwrap();
        let error = client
            .try_providers(|provider| async move {
                Err::<(), _>(ProviderAttemptError::Transient(anyhow::anyhow!(
                    "down: {provider:?}"
                )))
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("down"));
    }

    #[tokio::test]
    async fn try_providers_stops_immediately_on_a_fatal_error() {
        // A Fatal error (a definitively wrong chain id, not a transport
        // hiccup) must never be masked by falling back to another
        // provider -- a different, correctly-configured provider silently
        // "fixing" the response would hide that the first one is on the
        // wrong network entirely.
        let config = ArcusSpotChainConfig {
            rpc_urls: vec![
                "https://a.example.invalid".to_string(),
                "https://b.example.invalid".to_string(),
            ],
            chain_id: 4663,
            request_interval_ms: 100,
        };
        let client = ArcusSpotChainClient::new(config).unwrap();
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let error = client
            .try_providers(|_provider| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err::<(), _>(ProviderAttemptError::Fatal(anyhow::anyhow!(
                        "wrong chain id"
                    )))
                }
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("wrong chain id"));
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a fatal error must not trigger a fallback attempt on the next provider"
        );
    }

    #[test]
    fn redact_rpc_url_strips_path_and_query() {
        assert_eq!(
            redact_rpc_url("https://provider.example/v3/super-secret-api-key?id=1"),
            "https://provider.example"
        );
        assert_eq!(
            redact_rpc_url("https://provider.example:8545/v3/super-secret-api-key"),
            "https://provider.example:8545"
        );
        assert_eq!(redact_rpc_url("not a url"), "<unparseable RPC URL>");
    }
}
