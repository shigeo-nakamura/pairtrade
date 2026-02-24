#[cfg(feature = "lighter-sdk")]
use debot_utils::decrypt_data_with_kms;
use rust_decimal::Error as DecimalParseError;
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
    pub base_url: String,
    pub websocket_url: String,
}

#[derive(Debug)]
pub struct HyperliquidConfig {
    pub private_key: String,
    pub evm_wallet_address: String,
    pub vault_address: Option<String>,
}

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
pub enum RunMode {
    Dry,
    RealTrade,
    #[allow(dead_code)]
    RealTradeTest,
}

#[derive(Debug)]
pub enum ConfigError {
    ParseIntError(ParseIntError),
    ParseFloatError(ParseFloatError),
    DecimalParseError(DecimalParseError),
    #[cfg(feature = "lighter-sdk")]
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

#[cfg(feature = "lighter-sdk")]
pub async fn get_lighter_config_from_env() -> Result<LighterConfig, ConfigError> {
    // Check for plain (unencrypted) keys first
    let plain_private_api_key = env::var("LIGHTER_PLAIN_PRIVATE_API_KEY").ok();
    let plain_public_api_key = env::var("LIGHTER_PLAIN_PUBLIC_API_KEY").ok();
    let private_api_key = env::var("LIGHTER_PRIVATE_API_KEY").ok();
    let public_api_key = env::var("LIGHTER_PUBLIC_API_KEY").ok();
    let evm_wallet_private_key = env::var("LIGHTER_EVM_WALLET_PRIVATE_KEY").ok();

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

        let api_key = public_api_key
            .or_else(|| env::var("LIGHTER_PUBLIC_API_KEY").ok())
            .expect("LIGHTER_PUBLIC_API_KEY must be set");

        let private_key = private_api_key
            .or_else(|| env::var("LIGHTER_PRIVATE_API_KEY").ok())
            .expect("LIGHTER_PRIVATE_API_KEY must be set");

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

    // Read additional configuration
    let api_key_index: u32 = env::var("LIGHTER_API_KEY_INDEX")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("LIGHTER_API_KEY_INDEX must be a valid u32");

    let account_index: u64 = env::var("LIGHTER_ACCOUNT_INDEX")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("LIGHTER_ACCOUNT_INDEX must be a valid u64");

    Ok(LighterConfig {
        api_key,
        private_key,
        evm_wallet_private_key: evm_wallet_key,
        api_key_index,
        account_index,
        base_url,
        websocket_url,
    })
}

pub async fn get_hyperliquid_config_from_env(
    mode: RunMode,
) -> Result<HyperliquidConfig, ConfigError> {
    let private_key_encrypted = match mode {
        RunMode::Dry | RunMode::RealTradeTest => env::var("HYPERLIQUID_DRYRUN_PRIVATE_KEY")
            .expect("HYPERLIQUID_DRYRUN_PRIVATE_KEY must be set"),
        RunMode::RealTrade => {
            env::var("HYPERLIQUID_PRIVATE_KEY").expect("HYPERLIQUID_PRIVATE_KEY must be set")
        }
    };

    let evm_wallet_address = match mode {
        RunMode::Dry | RunMode::RealTradeTest => env::var("HYPERLIQUID_DRYRUN_EVM_WALLET_ADDRESS")
            .expect("HYPERLIQUID_DRYRUN_EVM_WALLET_ADDRESS must be set"),
        RunMode::RealTrade => env::var("HYPERLIQUID_EVM_WALLET_ADDRESS")
            .expect("HYPERLIQUID_EVM_WALLET_ADDRESS must be set"),
    };

    let vault_address = match mode {
        RunMode::Dry | RunMode::RealTradeTest => env::var("HYPERLIQUID_DRYRUN_VAULT_ADDRESS").ok(),
        RunMode::RealTrade => env::var("HYPERLIQUID_VAULT_ADDRESS").ok(),
    };

    // Decrypt private key using KMS
    let encrypted_data_key = env::var("ENCRYPTED_DATA_KEY")
        .expect("ENCRYPTED_DATA_KEY must be set")
        .replace(" ", ""); // Remove whitespace characters

    let private_key_vec =
        debot_utils::decrypt_data_with_kms(&encrypted_data_key, private_key_encrypted, true)
            .await
            .map_err(|e| {
                log::error!("Failed to decrypt private key: {:?}", e);
                ConfigError::DecimalParseError(DecimalParseError::from("KMS decryption failed"))
            })?;
    let private_key = String::from_utf8(private_key_vec).unwrap();

    Ok(HyperliquidConfig {
        private_key,
        evm_wallet_address,
        vault_address,
    })
}

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
