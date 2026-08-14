//! Pairtrade configuration: resolved shapes and the env/YAML →
//! resolved-config builder. The raw YAML deserialization schema types live
//! in the `schema` submodule (bot-strategy#502).

mod env_overrides;
mod env_util;
mod fingerprint;
mod from_env;
mod from_yaml;
mod params;
mod resolved;
mod risk;
mod schema;
mod strategy;
mod universe;
mod validate;

pub use fingerprint::EffectiveConfig;
pub use params::PairParams;
pub use resolved::{PairTradeConfig, WarmStartMode};
pub use risk::{DailyLossAction, RiskConfig};
pub use strategy::StrategyConfig;

pub use universe::PairSpec;

#[cfg(test)]
mod tests;
