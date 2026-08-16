use super::ArcusSpotBalanceSnapshot;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use dex_connector::ArcusSpotEip2612PermitContext;
use ethers::{
    contract::abigen,
    providers::{Http, Middleware, Provider, ProviderError, RpcError},
    types::{Address, Bytes, TransactionRequest, H256, U256},
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

/// EIP-1898 selector used only by post-confirmation reconciliation. ethers
/// 2.0.14's `BlockId::Hash` serializes `blockHash`, but cannot express the
/// `requireCanonical` field, so these reads deliberately use raw JSON-RPC.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CanonicalBlockSelector {
    block_hash: H256,
    require_canonical: bool,
}

impl CanonicalBlockSelector {
    fn new(block_hash: H256) -> Self {
        Self {
            block_hash,
            require_canonical: true,
        }
    }
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
    /// `Unknown` (Codex P1 follow-up, pairtrade#182, rounds 4 and 6). Now
    /// that every attempt requires the confirmed transaction's own
    /// receipt to be present, and pins every balance read to that
    /// receipt's exact block hash (rounds 7-8), that proof is what makes
    /// a result trustworthy -- not which provider happened to answer. So
    /// falling back through every configured provider is safe again, and
    /// restores availability a permanently-primary-only design would
    /// otherwise lose if provider 0 specifically never catches up (Codex
    /// P1 follow-up, pairtrade#182, round 9): "hasn't indexed this tx yet"
    /// is `Transient` on any provider, and try_providers moves on.
    pub async fn balances_requiring_primary_provider(
        &self,
        taker: Address,
        sell_token: Address,
        buy_token: Address,
        confirmed_tx_hash: H256,
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
                let receipt = provider
                    .get_transaction_receipt(confirmed_tx_hash)
                    .await
                    .context("Arcus confirmed-transaction receipt read failed")?;
                let Some(receipt) = receipt else {
                    return Err(ProviderAttemptError::Transient(anyhow::anyhow!(
                        "Arcus provider has not yet indexed confirmed tx {confirmed_tx_hash:#x}"
                    )));
                };
                // A single RPC URL commonly load-balances across a pool
                // of backend nodes: the receipt lookup above and the
                // balance reads below are separate requests that can land
                // on different backends. EIP-1898's requireCanonical=true
                // makes every backend reject a retained-but-orphaned block
                // hash instead of returning stale fork state. Such an RPC
                // error is Transient for this provider attempt, so a later
                // configured provider can still prove and serve the same
                // canonical block.
                let receipt_block_hash = receipt
                    .block_hash
                    .context("Arcus confirmed-transaction receipt is missing its block hash")?;
                read_canonical_balances_from_provider(
                    provider,
                    chain_id,
                    taker,
                    sell_token,
                    buy_token,
                    receipt_block_hash,
                )
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

/// Current-state reads used by preflight/status. Reconciliation has a
/// separate raw-RPC path below because it must express EIP-1898's
/// `requireCanonical`, which ethers 2.0.14's typed `BlockId` omits.
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

    let mut transient_error: Option<anyhow::Error> = None;
    for outcome in [
        &sell_balance_outcome,
        &buy_balance_outcome,
        &gas_balance_outcome,
    ] {
        if let ContractCallOutcome::Transport(_) = outcome {
            transient_error
                .get_or_insert_with(|| anyhow::anyhow!("Arcus balance read failed (transport)"));
        }
    }
    if let Some(error) = transient_error {
        return Err(ProviderAttemptError::Transient(error));
    }

    Ok(RawBalanceReads {
        sell_balance: expect_ok(sell_balance_outcome),
        buy_balance: expect_ok(buy_balance_outcome),
        gas_balance: expect_ok(gas_balance_outcome),
    })
}

/// Read the two ERC-20 balances and native gas balance at one exact,
/// canonical block. Only transport failures and JSON-RPC responses saying
/// that the requested block is unknown/non-canonical are retryable against
/// the next configured provider. Reverts and decode failures are Fatal,
/// matching the typed contract-call behavior used by current-state reads.
async fn read_canonical_balances_from_provider(
    provider: Arc<Provider<Http>>,
    expected_chain_id: u64,
    taker: Address,
    sell_token: Address,
    buy_token: Address,
    block_hash: H256,
) -> Result<RawBalanceReads, ProviderAttemptError> {
    let chain_id = provider
        .get_chainid()
        .await
        .context("Arcus chainId read failed")?;
    if chain_id != U256::from(expected_chain_id) {
        return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
            "Arcus RPC chainId changed during balance reconciliation"
        )));
    }

    let selector = CanonicalBlockSelector::new(block_hash);
    let sell_params = canonical_balance_of_params(sell_token, taker, selector);
    let buy_params = canonical_balance_of_params(buy_token, taker, selector);
    let gas_params = (taker, selector);
    let (sell_result, buy_result, gas_result) = tokio::join!(
        provider.request::<_, Bytes>("eth_call", sell_params),
        provider.request::<_, Bytes>("eth_call", buy_params),
        provider.request::<_, U256>("eth_getBalance", gas_params),
    );

    let sell_outcome = decode_canonical_erc20_balance("sell", sell_result);
    let buy_outcome = decode_canonical_erc20_balance("buy", buy_result);
    let gas_outcome = match gas_result {
        Ok(value) => ContractCallOutcome::Ok(value),
        Err(error) => classify_canonical_provider_error("gas", error),
    };

    for (label, outcome) in [("sell", &sell_outcome), ("buy", &buy_outcome)] {
        if let ContractCallOutcome::NonTransport(error) = outcome {
            return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                "Arcus canonical {label} balance read returned an unexpected response: {error}"
            )));
        }
    }

    if [&sell_outcome, &buy_outcome, &gas_outcome]
        .into_iter()
        .any(|outcome| matches!(outcome, ContractCallOutcome::Transport(_)))
    {
        return Err(ProviderAttemptError::Transient(anyhow::anyhow!(
            "Arcus canonical balance read failed (RPC)"
        )));
    }

    Ok(RawBalanceReads {
        sell_balance: expect_ok(sell_outcome),
        buy_balance: expect_ok(buy_outcome),
        gas_balance: expect_ok(gas_outcome),
    })
}

fn canonical_balance_of_params(
    token: Address,
    owner: Address,
    selector: CanonicalBlockSelector,
) -> (TransactionRequest, CanonicalBlockSelector) {
    // balanceOf(address) = 4-byte selector + one left-padded 32-byte word.
    let mut calldata = [0_u8; 36];
    calldata[..4].copy_from_slice(&[0x70, 0xa0, 0x82, 0x31]);
    calldata[16..].copy_from_slice(owner.as_bytes());
    (
        TransactionRequest::new()
            .to(token)
            .data(Bytes::from(calldata.to_vec())),
        selector,
    )
}

fn decode_canonical_erc20_balance(
    label: &str,
    result: std::result::Result<Bytes, ethers::providers::ProviderError>,
) -> ContractCallOutcome<U256> {
    match result {
        Ok(bytes) if bytes.len() == 32 => {
            ContractCallOutcome::Ok(U256::from_big_endian(bytes.as_ref()))
        }
        Ok(_) => ContractCallOutcome::NonTransport(anyhow::anyhow!(
            "Arcus canonical {label} balance response must be exactly one ABI uint256"
        )),
        Err(error) => classify_canonical_provider_error(label, error),
    }
}

/// Classify raw EIP-1898 request failures without carrying the provider
/// error's Display/source into logs or returned context: HTTP errors often
/// embed credential-bearing RPC URLs. A JSON-RPC error proves that the node
/// answered, so it is Fatal unless it specifically says that the requested
/// block is unknown/non-canonical. Serde/result decoding failures are also
/// Fatal because another provider must not hide an incompatible response.
fn classify_canonical_provider_error<T>(
    label: &str,
    error: ProviderError,
) -> ContractCallOutcome<T> {
    if let Some(response) = error.as_error_response() {
        if is_retryable_canonical_block_error(&response.message) {
            return ContractCallOutcome::Transport(anyhow::anyhow!(
                "Arcus canonical {label} balance block is unavailable"
            ));
        }
        return ContractCallOutcome::NonTransport(anyhow::anyhow!(
            "Arcus canonical {label} balance RPC returned a non-retryable response"
        ));
    }

    if matches!(error, ProviderError::HTTPError(_)) {
        return ContractCallOutcome::Transport(anyhow::anyhow!(
            "Arcus canonical {label} balance transport failed"
        ));
    }

    if error.as_serde_error().is_some() {
        return ContractCallOutcome::NonTransport(anyhow::anyhow!(
            "Arcus canonical {label} balance response could not be decoded"
        ));
    }

    ContractCallOutcome::NonTransport(anyhow::anyhow!(
        "Arcus canonical {label} balance provider returned an unexpected error"
    ))
}

fn is_retryable_canonical_block_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "not canonical",
        "non-canonical",
        "noncanonical",
        "unknown block",
        "block not found",
        "header not found",
        "cannot find block",
        "could not find block",
    ]
    .iter()
    .any(|needle| message.contains(needle))
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
    use ethers::types::TransactionReceipt;
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
        let receipt = TransactionReceipt {
            block_hash: Some(block_hash),
            ..Default::default()
        };
        serde_json::to_value(receipt).unwrap()
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
    async fn canonical_reconciliation_uses_exact_eip1898_read_only_params() {
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
        assert_eq!(requests.len(), 5);
        assert!(requests.iter().all(|request| matches!(
            request["method"].as_str().unwrap(),
            "eth_chainId" | "eth_getTransactionReceipt" | "eth_call" | "eth_getBalance"
        )));
        assert!(!requests
            .iter()
            .any(|request| request["method"].as_str().unwrap().starts_with("eth_send")));

        let receipt_request = requests
            .iter()
            .find(|request| request["method"] == "eth_getTransactionReceipt")
            .unwrap();
        assert_eq!(receipt_request["params"], json!([format!("{tx_hash:#x}")]));

        let canonical_selector = json!({
            "blockHash": format!("{block_hash:#x}"),
            "requireCanonical": true
        });
        let mut expected_calldata = [0_u8; 36];
        expected_calldata[..4].copy_from_slice(&[0x70, 0xa0, 0x82, 0x31]);
        expected_calldata[16..].copy_from_slice(taker.as_bytes());
        let expected_data = format!("0x{}", hex::encode(expected_calldata));
        let eth_calls = requests
            .iter()
            .filter(|request| request["method"] == "eth_call")
            .collect::<Vec<_>>();
        assert_eq!(eth_calls.len(), 2);
        for request in eth_calls {
            assert_eq!(request["params"][1], canonical_selector);
            assert_eq!(request["params"][0]["data"], expected_data);
            assert_eq!(request["params"][0].as_object().unwrap().len(), 2);
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
        let gas_request = requests
            .iter()
            .find(|request| request["method"] == "eth_getBalance")
            .unwrap();
        assert_eq!(
            gas_request["params"],
            json!([format!("{taker:#x}"), canonical_selector])
        );
    }

    #[tokio::test]
    async fn noncanonical_block_errors_fall_back_to_the_next_provider() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x33);
        let block_hash = H256::from_low_u64_be(0x44);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_call" | "eth_getBalance" => RpcReply::Error {
                code: -32000,
                message: "block is not canonical",
            },
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        })
        .await;
        let second = spawn_rpc_server(move |request| {
            successful_reconciliation_reply(
                request, block_hash, sell_token, buy_token, 4_000, 2_985, 123,
            )
        })
        .await;
        let client = ArcusSpotChainClient::new(rpc_config(vec![
            format!("{}/v3/super-secret-api-key", first.url),
            second.url.clone(),
        ]))
        .unwrap();

        let balances = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap();
        assert_eq!(balances.sell_balance_raw, "4000");
        assert_eq!(balances.buy_balance_raw, "2985");
        assert_eq!(request_snapshot(&first).len(), 5);
        assert_eq!(request_snapshot(&second).len(), 5);
    }

    #[test]
    fn canonical_block_error_taxonomy_is_narrow() {
        assert!(is_retryable_canonical_block_error("unknown block 0x1234"));
        assert!(is_retryable_canonical_block_error("header not found"));
        assert!(!is_retryable_canonical_block_error(
            "execution reverted: token paused"
        ));
        assert!(!is_retryable_canonical_block_error("invalid params"));
    }

    #[tokio::test]
    async fn canonical_contract_revert_is_fatal_without_fallback() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x45);
        let block_hash = H256::from_low_u64_be(0x46);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
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
        assert!(format!("{error:#}").contains("non-retryable response"));
        assert!(request_snapshot(&second).is_empty());
    }

    #[tokio::test]
    async fn canonical_rpc_result_decode_error_is_fatal_without_fallback() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x47);
        let block_hash = H256::from_low_u64_be(0x48);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_call" if request["params"][0]["to"] == format!("{sell_token:#x}") => {
                RpcReply::Result(json!({"unexpected": "object"}))
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
        assert!(format!("{error:#}").contains("could not be decoded"));
        assert!(request_snapshot(&second).is_empty());
    }

    #[tokio::test]
    async fn canonical_http_transport_failure_falls_back() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x49);
        let block_hash = H256::from_low_u64_be(0x4a);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
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
        assert_eq!(request_snapshot(&second).len(), 5);
    }

    #[tokio::test]
    async fn all_noncanonical_errors_are_retryable_and_redact_rpc_urls() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x55);
        let block_hash = H256::from_low_u64_be(0x66);
        let server = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_call" | "eth_getBalance" => RpcReply::Error {
                code: -32000,
                message: "orphaned block is not canonical",
            },
            _ => RpcReply::Error {
                code: -32601,
                message: "unexpected method",
            },
        })
        .await;
        let client = ArcusSpotChainClient::new(rpc_config(vec![format!(
            "{}/private/super-secret-token?key=also-secret",
            server.url
        )]))
        .unwrap();

        let error = client
            .balances_requiring_primary_provider(taker, sell_token, buy_token, tx_hash)
            .await
            .unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("canonical balance read failed (RPC)"));
        assert!(!rendered.contains("super-secret-token"));
        assert!(!rendered.contains("also-secret"));
    }

    #[tokio::test]
    async fn malformed_canonical_balance_is_fatal_without_fallback() {
        let (taker, sell_token, buy_token) = test_addresses();
        let tx_hash = H256::from_low_u64_be(0x77);
        let block_hash = H256::from_low_u64_be(0x88);
        let first = spawn_rpc_server(move |request| match request["method"].as_str().unwrap() {
            "eth_chainId" => RpcReply::Result(json!("0x1237")),
            "eth_getTransactionReceipt" => RpcReply::Result(receipt_json(block_hash)),
            "eth_call" if request["params"][0]["to"] == format!("{sell_token:#x}") => {
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
        assert!(format!("{error:#}").contains("exactly one ABI uint256"));
        assert!(request_snapshot(&second).is_empty());
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
