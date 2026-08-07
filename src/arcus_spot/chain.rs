use super::ArcusSpotBalanceSnapshot;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use dex_connector::ArcusSpotEip2612PermitContext;
use ethers::{
    contract::abigen,
    providers::{Http, Middleware, Provider},
    types::{Address, U256},
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
                bail!("Arcus rpc_urls contains a duplicate entry: {rpc_url}");
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
                    .with_context(|| format!("could not construct Arcus RPC provider for {rpc_url}"))
                    .map(|provider| {
                        Arc::new(provider.interval(Duration::from_millis(config.request_interval_ms)))
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
                    log::warn!(
                        "[ARCUS_RPC] provider {index} ({}) failed: {error:#}",
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
            .try_providers(|provider| {
                let sell_token = request_values.sell_token;
                let buy_token = request_values.buy_token;
                let taker = request_values.taker;
                let permit2 = request_values.permit2;
                let required_sell = request_values.required_sell;
                async move {
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

                    let coherent: Result<PreflightAttempt> = async {
                        let sell_contract = ArcusSpotErc20::new(sell_token, provider.clone());
                        let buy_contract = ArcusSpotErc20::new(buy_token, provider.clone());
                        let sell_balance_call = sell_contract.balance_of(taker);
                        let buy_balance_call = buy_contract.balance_of(taker);
                        let allowance_call = sell_contract.allowance(taker, permit2);
                        let (sell_balance, buy_balance, gas_balance, allowance) = tokio::join!(
                            sell_balance_call.call(),
                            buy_balance_call.call(),
                            provider.get_balance(taker, None),
                            allowance_call.call(),
                        );
                        let sell_balance = sell_balance.context("Arcus sell balance read failed")?;
                        let buy_balance = buy_balance.context("Arcus buy balance read failed")?;
                        let gas_balance = gas_balance.context("Arcus gas balance read failed")?;
                        let allowance = allowance.context("Arcus Permit2 allowance read failed")?;

                        // An overbroad allowance is rejected by the caller
                        // once this attempt returns, so there is no point
                        // spending a follow-up round-trip on metadata this
                        // preflight is about to fail anyway.
                        let exact_value_permit = if allowance >= required_sell {
                            None
                        } else {
                            let token_name_call = sell_contract.name();
                            let nonce_call = sell_contract.nonces(taker);
                            let (token_name, nonce) =
                                tokio::try_join!(token_name_call.call(), nonce_call.call())
                                    .context(
                                        "sell token does not expose the required EIP-2612 metadata",
                                    )?;
                            if token_name.trim().is_empty() {
                                bail!("sell token returned an empty EIP-2612 name");
                            }
                            let token_version = sell_contract
                                .version()
                                .call()
                                .await
                                .unwrap_or_else(|_| "1".to_string());
                            if token_version.trim().is_empty() {
                                bail!("sell token returned an empty EIP-2612 version");
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
                    }
                    .await;
                    coherent.map_err(ProviderAttemptError::Transient)
                }
            })
            .await?;
        let PreflightAttempt {
            sell_balance,
            buy_balance,
            gas_balance,
            allowance,
            exact_value_permit,
        } = attempt;
        enforce_balance_limits(sell_balance, buy_balance, gas_balance, &request_values)?;
        if allowance > request_values.required_sell {
            bail!(
                "Permit2 allowance {allowance} exceeds the exact required amount {}; refusing an overbroad approval",
                request_values.required_sell
            );
        }

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
        let (raw, _provider) = self
            .try_providers(|provider| async move {
                // Sequential and fatal-on-mismatch for the same reason as
                // in `preflight`: a wrong-but-successfully-fetched chain id
                // must never be masked by a sibling call's transport error
                // inside the same tokio::join!.
                let chain_id = provider
                    .get_chainid()
                    .await
                    .context("Arcus chainId read failed")?;
                if chain_id != U256::from(self.config.chain_id) {
                    return Err(ProviderAttemptError::Fatal(anyhow::anyhow!(
                        "Arcus RPC chainId changed during balance reconciliation"
                    )));
                }
                let coherent: Result<RawBalanceReads> = async {
                    let sell_contract = ArcusSpotErc20::new(sell_token, provider.clone());
                    let buy_contract = ArcusSpotErc20::new(buy_token, provider.clone());
                    let sell_balance_call = sell_contract.balance_of(taker);
                    let buy_balance_call = buy_contract.balance_of(taker);
                    let (sell_balance, buy_balance, gas_balance) = tokio::join!(
                        sell_balance_call.call(),
                        buy_balance_call.call(),
                        provider.get_balance(taker, None),
                    );
                    Ok(RawBalanceReads {
                        sell_balance: sell_balance.context("Arcus sell balance read failed")?,
                        buy_balance: buy_balance.context("Arcus buy balance read failed")?,
                        gas_balance: gas_balance.context("Arcus gas balance read failed")?,
                    })
                }
                .await;
                coherent.map_err(ProviderAttemptError::Transient)
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
}

fn enforce_balance_limits(
    sell_balance: U256,
    buy_balance: U256,
    gas_balance: U256,
    request: &ValidatedPreflightRequest,
) -> Result<()> {
    let residual = sell_balance
        .checked_sub(request.required_sell)
        .context("sell balance is below the required amount")?;
    if residual < request.sell_floor {
        bail!(
            "post-swap sell balance {residual} would be below floor {}",
            request.sell_floor
        );
    }
    if buy_balance < request.buy_floor {
        bail!(
            "current buy balance {buy_balance} is below floor {}",
            request.buy_floor
        );
    }
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
        assert!(enforce_balance_limits(
            U256::from(1499),
            U256::from(500),
            U256::from(100),
            &values,
        )
        .is_err());
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
