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
    pub rpc_url: String,
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
        let url = url::Url::parse(self.rpc_url.trim()).context("invalid Arcus RPC URL")?;
        if !matches!(url.scheme(), "https" | "http") {
            bail!("Arcus RPC URL must use http or https");
        }
        if url.host_str().is_none() || url.username() != "" || url.password().is_some() {
            bail!("Arcus RPC URL must have a host and no inline credentials");
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
    provider: Arc<Provider<Http>>,
}

impl ArcusSpotChainClient {
    pub fn new(config: ArcusSpotChainConfig) -> Result<Self> {
        config.validate()?;
        let provider = Provider::<Http>::try_from(config.rpc_url.trim())
            .context("could not construct Arcus RPC provider")?
            .interval(Duration::from_millis(config.request_interval_ms));
        Ok(Self {
            config,
            provider: Arc::new(provider),
        })
    }

    pub async fn preflight(
        &self,
        request: &ArcusSpotChainPreflightRequest,
    ) -> Result<ArcusSpotChainPreflight> {
        let request_values = request.validate()?;
        let sell_contract = ArcusSpotErc20::new(request_values.sell_token, self.provider.clone());
        let buy_contract = ArcusSpotErc20::new(request_values.buy_token, self.provider.clone());
        let sell_balance_call = sell_contract.balance_of(request_values.taker);
        let buy_balance_call = buy_contract.balance_of(request_values.taker);
        let allowance_call = sell_contract.allowance(request_values.taker, request_values.permit2);
        let (chain_id, sell_balance, buy_balance, gas_balance, allowance) = tokio::join!(
            self.provider.get_chainid(),
            sell_balance_call.call(),
            buy_balance_call.call(),
            self.provider.get_balance(request_values.taker, None),
            allowance_call.call(),
        );
        let chain_id = chain_id.context("Arcus chainId read failed")?;
        let sell_balance = sell_balance.context("Arcus sell balance read failed")?;
        let buy_balance = buy_balance.context("Arcus buy balance read failed")?;
        let gas_balance = gas_balance.context("Arcus gas balance read failed")?;
        let allowance = allowance.context("Arcus Permit2 allowance read failed")?;
        if chain_id != U256::from(self.config.chain_id) {
            bail!(
                "Arcus RPC chainId {chain_id} does not match configured {}",
                self.config.chain_id
            );
        }
        enforce_balance_limits(sell_balance, buy_balance, gas_balance, &request_values)?;
        if allowance > request_values.required_sell {
            bail!(
                "Permit2 allowance {allowance} exceeds the exact required amount {}; refusing an overbroad approval",
                request_values.required_sell
            );
        }

        let exact_value_permit = if allowance == request_values.required_sell {
            None
        } else {
            let token_name_call = sell_contract.name();
            let nonce_call = sell_contract.nonces(request_values.taker);
            let (token_name, nonce) = tokio::try_join!(token_name_call.call(), nonce_call.call(),)
                .context("sell token does not expose the required EIP-2612 metadata")?;
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
                deadline: request.permit_deadline,
            })
        };

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
        let sell_contract = ArcusSpotErc20::new(sell_token, self.provider.clone());
        let buy_contract = ArcusSpotErc20::new(buy_token, self.provider.clone());
        let sell_balance_call = sell_contract.balance_of(taker);
        let buy_balance_call = buy_contract.balance_of(taker);
        let (chain_id, sell_balance, buy_balance, gas_balance) = tokio::join!(
            self.provider.get_chainid(),
            sell_balance_call.call(),
            buy_balance_call.call(),
            self.provider.get_balance(taker, None),
        );
        let chain_id = chain_id.context("Arcus chainId read failed")?;
        let sell_balance = sell_balance.context("Arcus sell balance read failed")?;
        let buy_balance = buy_balance.context("Arcus buy balance read failed")?;
        let gas_balance = gas_balance.context("Arcus gas balance read failed")?;
        if chain_id != U256::from(self.config.chain_id) {
            bail!("Arcus RPC chainId changed during balance reconciliation");
        }
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
            rpc_url: "https://user:secret@example.invalid".to_string(),
            chain_id: 4663,
            request_interval_ms: 100,
        };
        assert!(config.validate().is_err());
    }
}
