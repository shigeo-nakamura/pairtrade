#[cfg(feature = "lighter-sdk")]
use debot_utils::decrypt_data_with_kms;
use rust_decimal::Error as DecimalParseError;
#[cfg(any(feature = "lighter-sdk", feature = "extended-sdk"))]
use std::env;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};

#[cfg(feature = "lighter-sdk")]
#[derive(Debug)]
pub struct LighterConfig {
    pub api_key: String,                        // X-API-KEY header for authentication
    pub private_key: String,                    // API private key for signing (40-byte)
    pub evm_wallet_private_key: Option<String>, // EVM wallet private key for API key registration
    pub api_key_index: u32,                     // API key index
    pub account_index: u64,                     // Account index
    pub wallet_address: Option<String>,         // Wallet L1 address for account discovery
    pub base_url: String,
    pub websocket_url: String,
}

#[cfg(feature = "extended-sdk")]
#[derive(Debug)]
pub struct ExtendedConfig {
    pub api_key: String,
    pub public_key: String,
    pub private_key: String,
    pub vault: u64,
    pub base_url: Option<String>,
    pub websocket_url: Option<String>,
}

#[derive(Debug)]
pub enum ConfigError {
    ParseIntError(ParseIntError),
    ParseFloatError(ParseFloatError),
    DecimalParseError(DecimalParseError),
    #[cfg(any(feature = "lighter-sdk", feature = "hyperliquid-sdk"))]
    OtherError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConfigError::ParseIntError(e) => write!(f, "Parse int error: {}", e),
            ConfigError::ParseFloatError(e) => write!(f, "Parse float error: {}", e),
            ConfigError::DecimalParseError(e) => write!(f, "Decimal parse error: {}", e),
            #[cfg(feature = "lighter-sdk")]
            ConfigError::OtherError(e) => write!(f, "Other error: {}", e),
        }
    }
}

impl From<ParseIntError> for ConfigError {
    fn from(err: ParseIntError) -> ConfigError {
        ConfigError::ParseIntError(err)
    }
}

impl From<ParseFloatError> for ConfigError {
    fn from(err: ParseFloatError) -> ConfigError {
        ConfigError::ParseFloatError(err)
    }
}

impl From<rust_decimal::Error> for ConfigError {
    fn from(err: rust_decimal::Error) -> ConfigError {
        ConfigError::DecimalParseError(err)
    }
}

/// Look up an env var for a Lighter credential, optionally falling back
/// from an instance-suffixed name to the unsuffixed name.
///
/// When `instance_id` is `Some("debot-pair-btceth-b")`, this checks
/// `<NAME>_DEBOT_PAIR_BTCETH_B` first, then `<NAME>`. The single-instance
/// path passes `None` and behaves exactly as today.
///
/// commit 3 of shigeo-nakamura/bot-strategy#25.
#[cfg(feature = "lighter-sdk")]
fn lighter_env(name: &str, instance_id: Option<&str>) -> Option<String> {
    if let Some(id) = instance_id {
        let suffix = id.to_uppercase().replace('-', "_");
        if let Ok(value) = env::var(format!("{name}_{suffix}")) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    env::var(name).ok().filter(|v| !v.is_empty())
}

#[cfg(feature = "lighter-sdk")]
pub async fn get_lighter_config_from_env(
    instance_id: Option<&str>,
) -> Result<LighterConfig, ConfigError> {
    // Check for plain (unencrypted) keys first
    let plain_private_api_key = lighter_env("LIGHTER_PLAIN_PRIVATE_API_KEY", instance_id);
    let plain_public_api_key = lighter_env("LIGHTER_PLAIN_PUBLIC_API_KEY", instance_id);
    let private_api_key = lighter_env("LIGHTER_PRIVATE_API_KEY", instance_id);
    let public_api_key = lighter_env("LIGHTER_PUBLIC_API_KEY", instance_id);
    let evm_wallet_private_key = lighter_env("LIGHTER_EVM_WALLET_PRIVATE_KEY", instance_id);

    let (api_key, private_key, evm_wallet_key) = if let (Some(plain_priv), Some(plain_pub)) =
        (plain_private_api_key, plain_public_api_key)
    {
        // Use plain keys, skip KMS decryption
        log::info!("Using plain text keys for testing");

        // Skip key validation for plain text keys - lighter-go doesn't provide key derivation function
        log::info!("Skipping key validation for plain text keys");

        // EVM wallet private key is always encrypted, even in plain text mode
        let evm_wallet_key = if let Some(evm_key) = evm_wallet_private_key {
            log::info!("Decrypting EVM wallet private key (always encrypted)");
            let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
                .expect("ENCRYPTED_DATA_KEY must be set")
                .replace(" ", ""); // Remove whitespace characters

            let evm_key_vec = decrypt_data_with_kms(&encrypted_data_key, evm_key, true)
                .await
                .map_err(|_| {
                    ConfigError::OtherError("decrypt evm_wallet_private_key".to_owned())
                })?;
            Some(String::from_utf8(evm_key_vec).unwrap())
        } else {
            log::info!("No EVM wallet private key provided");
            None
        };

        (plain_pub, plain_priv, evm_wallet_key)
    } else {
        // Use encrypted keys with KMS
        log::info!("Using KMS encrypted keys");

        let api_key = public_api_key.expect("LIGHTER_PUBLIC_API_KEY must be set");
        let private_key = private_api_key.expect("LIGHTER_PRIVATE_API_KEY must be set");

        let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
            .expect("ENCRYPTED_DATA_KEY must be set")
            .replace(" ", ""); // Remove whitespace characters

        let api_key_vec = decrypt_data_with_kms(&encrypted_data_key, api_key, true)
            .await
            .map_err(|_| ConfigError::OtherError("decrypt api_key".to_owned()))?;
        let api_key = String::from_utf8(api_key_vec).unwrap();

        let private_key_vec = decrypt_data_with_kms(&encrypted_data_key, private_key, true)
            .await
            .map_err(|_| ConfigError::OtherError("decrypt private_key".to_owned()))?;
        let private_key = String::from_utf8(private_key_vec).unwrap();

        // Decrypt EVM wallet private key if provided
        let evm_wallet_key = if let Some(evm_key) = evm_wallet_private_key {
            log::info!("Decrypting EVM wallet private key");
            let evm_key_vec = decrypt_data_with_kms(&encrypted_data_key, evm_key, true)
                .await
                .map_err(|_| {
                    ConfigError::OtherError("decrypt evm_wallet_private_key".to_owned())
                })?;
            Some(String::from_utf8(evm_key_vec).unwrap())
        } else {
            log::info!("No EVM wallet private key provided");
            None
        };

        (api_key, private_key, evm_wallet_key)
    };

    let base_url = env::var("REST_ENDPOINT")
        .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai/".to_string());

    let websocket_url = env::var("WEB_SOCKET_ENDPOINT")
        .unwrap_or_else(|_| "wss://mainnet.zklighter.elliot.ai/stream".to_string());

    // Read additional configuration (instance-suffixed values win over the
    // unsuffixed defaults so each strategy variant can point at its own
    // sub-account; see lighter_env() above).
    let api_key_index: u32 = lighter_env("LIGHTER_API_KEY_INDEX", instance_id)
        .unwrap_or_else(|| "0".to_string())
        .parse()
        .expect("LIGHTER_API_KEY_INDEX must be a valid u32");

    let account_index: u64 = lighter_env("LIGHTER_ACCOUNT_INDEX", instance_id)
        .unwrap_or_else(|| "0".to_string())
        .parse()
        .expect("LIGHTER_ACCOUNT_INDEX must be a valid u64");
    let wallet_address = lighter_env("LIGHTER_WALLET_ADDRESS", instance_id);

    Ok(LighterConfig {
        api_key,
        private_key,
        evm_wallet_private_key: evm_wallet_key,
        api_key_index,
        account_index,
        wallet_address,
        base_url,
        websocket_url,
    })
}

#[cfg(feature = "extended-sdk")]
pub async fn get_extended_config_from_env() -> Result<ExtendedConfig, ConfigError> {
    let api_key = env::var("EXTENDED_API_KEY").expect("EXTENDED_API_KEY must be set");
    let public_key = env::var("EXTENDED_PUBLIC_KEY").expect("EXTENDED_PUBLIC_KEY must be set");
    let private_key_encrypted =
        env::var("EXTENDED_PRIVATE_KEY").expect("EXTENDED_PRIVATE_KEY must be set");
    let vault: u64 = env::var("EXTENDED_VAULT")
        .expect("EXTENDED_VAULT must be set")
        .parse()
        .expect("EXTENDED_VAULT must be a valid u64");
    let base_url = env::var("REST_ENDPOINT").ok().filter(|v| !v.is_empty());
    let websocket_url = env::var("WEB_SOCKET_ENDPOINT")
        .ok()
        .filter(|v| !v.is_empty());

    let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
        .expect("ENCRYPTED_DATA_KEY must be set")
        .replace(" ", "");
    let private_key_vec =
        debot_utils::decrypt_data_with_kms(&encrypted_data_key, private_key_encrypted, true)
            .await
            .map_err(|e| {
                log::error!("Failed to decrypt EXTENDED_PRIVATE_KEY: {:?}", e);
                ConfigError::DecimalParseError(DecimalParseError::from("KMS decryption failed"))
            })?;
    let private_key = String::from_utf8(private_key_vec).unwrap();

    Ok(ExtendedConfig {
        api_key,
        public_key,
        private_key,
        vault,
        base_url,
        websocket_url,
    })
}

/// Instance-suffixed env lookup shared by the venue loaders: `NAME_<SUFFIX>`
/// (suffix = upper-cased instance id, '-' -> '_') wins over bare `NAME`.
#[cfg(feature = "hyperliquid-sdk")]
fn suffixed_env(name: &str, instance_id: Option<&str>) -> Option<String> {
    if let Some(id) = instance_id {
        let suffix = id.to_uppercase().replace('-', "_");
        if let Ok(value) = env::var(format!("{name}_{suffix}")) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    env::var(name).ok().filter(|v| !v.is_empty())
}

/// Hyperliquid *account* connector config (bot-strategy#894). Read-side only
/// when no signer key is provided (balances / fills for the configured
/// account); execution-capable when `HYPERLIQUID_SIGNER_PRIVATE_KEY` (KMS,
/// decrypted with `ENCRYPTED_DATA_KEY` like the Lighter keys) or
/// `HYPERLIQUID_PLAIN_SIGNER_PRIVATE_KEY` (testing only) is set.
///
/// Env (all accept the `_<INSTANCE>` suffix):
/// - `HYPERLIQUID_ACCOUNT_ADDRESS` (required) — the account that owns funds
///   (master or sub-account), NOT the API wallet address
/// - `HYPERLIQUID_SIGNER_PRIVATE_KEY` / `HYPERLIQUID_PLAIN_SIGNER_PRIVATE_KEY`
/// - `HYPERLIQUID_VAULT_ADDRESS` (optional)
/// - `HYPERLIQUID_IS_MAINNET` (default true)
/// - `HYPERLIQUID_NONCE_STATE_PATH` (required with a signer)
/// - `HYPERLIQUID_MAX_TAKER_NOTIONAL_USD` (required with a signer; IOC cap)
/// - `HYPERLIQUID_MAX_TAKER_SLIPPAGE_BPS` (default 50)
/// - `HYPERLIQUID_MAX_TAKER_BOOK_AGE_MS` (default 5000)
#[cfg(feature = "hyperliquid-sdk")]
pub async fn get_hyperliquid_account_config_from_env(
    instance_id: Option<&str>,
) -> Result<dex_connector::HyperliquidAccountConfig, ConfigError> {
    let account_address =
        suffixed_env("HYPERLIQUID_ACCOUNT_ADDRESS", instance_id).ok_or_else(|| {
            ConfigError::OtherError("HYPERLIQUID_ACCOUNT_ADDRESS must be set".to_owned())
        })?;
    let plain_key = suffixed_env("HYPERLIQUID_PLAIN_SIGNER_PRIVATE_KEY", instance_id);
    let encrypted_key = suffixed_env("HYPERLIQUID_SIGNER_PRIVATE_KEY", instance_id);
    let signer_private_key = match (plain_key, encrypted_key) {
        (Some(plain), _) => {
            log::warn!("[hyperliquid] using PLAIN signer key (testing only)");
            Some(plain)
        }
        (None, Some(encrypted)) => {
            let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
                .map_err(|_| {
                    ConfigError::OtherError(
                        "ENCRYPTED_DATA_KEY must be set to decrypt HYPERLIQUID_SIGNER_PRIVATE_KEY"
                            .to_owned(),
                    )
                })?
                .replace(' ', "");
            let bytes = decrypt_data_with_kms(&encrypted_data_key, encrypted, true)
                .await
                .map_err(|_| {
                    ConfigError::OtherError("decrypt HYPERLIQUID_SIGNER_PRIVATE_KEY".to_owned())
                })?;
            Some(String::from_utf8(bytes).map_err(|_| {
                ConfigError::OtherError("HYPERLIQUID_SIGNER_PRIVATE_KEY is not utf-8".to_owned())
            })?)
        }
        (None, None) => None,
    };
    let is_mainnet = suffixed_env("HYPERLIQUID_IS_MAINNET", instance_id)
        .map(|v| !matches!(v.to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let nonce_state_path =
        suffixed_env("HYPERLIQUID_NONCE_STATE_PATH", instance_id).map(std::path::PathBuf::from);
    let max_taker_notional = match suffixed_env("HYPERLIQUID_MAX_TAKER_NOTIONAL_USD", instance_id) {
        Some(v) => Some(v.parse::<rust_decimal::Decimal>()?),
        None => None,
    };
    let max_taker_slippage_bps =
        match suffixed_env("HYPERLIQUID_MAX_TAKER_SLIPPAGE_BPS", instance_id) {
            Some(v) => Some(v.parse::<u32>()?),
            None => Some(50),
        };
    let max_taker_book_age_ms = match suffixed_env("HYPERLIQUID_MAX_TAKER_BOOK_AGE_MS", instance_id)
    {
        Some(v) => v.parse::<u64>()?,
        None => 5_000,
    };
    if signer_private_key.is_some() {
        if nonce_state_path.is_none() {
            return Err(ConfigError::OtherError(
                "HYPERLIQUID_NONCE_STATE_PATH must be set when a signer key is configured"
                    .to_owned(),
            ));
        }
        if max_taker_notional.is_none() {
            return Err(ConfigError::OtherError(
                "HYPERLIQUID_MAX_TAKER_NOTIONAL_USD must be set when a signer key is configured"
                    .to_owned(),
            ));
        }
    }
    Ok(dex_connector::HyperliquidAccountConfig {
        account_address,
        signer_private_key,
        vault_address: suffixed_env("HYPERLIQUID_VAULT_ADDRESS", instance_id),
        is_mainnet,
        nonce_state_path,
        max_taker_notional,
        max_taker_slippage_bps,
        max_taker_book_age_ms,
    })
}
