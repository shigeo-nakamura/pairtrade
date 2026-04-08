//! YAML schema types for pairtrade configuration loading.
//!
//! These mirror the on-disk YAML layout one-to-one. The post-merge resolved
//! shapes (`PairTradeConfig`, `PairParams`) still live in the parent module
//! for now and will be migrated in a follow-up step.

use std::collections::HashMap;
use std::env;

use anyhow::{anyhow, Result};
use serde::Deserialize;

/// Resolved per-pair parameters (global defaults merged with any pair-specific overrides).
#[derive(Debug, Clone)]
pub struct PairParams {
    pub entry_z_base: f64,
    pub entry_z_min: f64,
    pub entry_z_max: f64,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub force_close_secs: u64,
    pub cooldown_secs: u64,
    pub max_loss_r_mult: f64,
    pub half_life_max_hours: f64,
    pub adf_p_threshold: f64,
    pub spread_velocity_max_sigma_per_min: f64,
    pub spread_trend_max_slope_sigma: f64,
    pub beta_divergence_max: f64,
    pub beta_min: f64,
    pub hedge_ratio_max_deviation: f64,
    pub lookback_hours_short: u64,
    pub lookback_hours_long: u64,
    pub entry_vol_lookback_hours: u64,
    pub warm_start_min_bars: usize,
    pub reeval_jump_z_mult: f64,
    pub vol_spike_mult: f64,
    pub circuit_breaker_tier1_losses: u32,
    pub circuit_breaker_tier1_cooldown_secs: u64,
    pub circuit_breaker_tier2_losses: u32,
    pub circuit_breaker_tier2_cooldown_secs: u64,
    pub entry_post_only_timeout_secs: u64,
    // Phase 2 filters (0.0 = disabled)
    pub entry_velocity_block_sigma_per_min: f64,
    pub funding_entry_z_scale: f64,
    pub beta_gap_entry_z_scale: f64,
}

#[derive(Debug, Clone)]
pub struct PairSpec {
    pub base: String,
    pub quote: String,
}

pub(super) fn env_has_universe_override() -> bool {
    env::var("UNIVERSE_PAIRS")
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || env::var("UNIVERSE_SYMBOLS")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
}

pub(super) fn parse_pairs_vec(pairs: &[String]) -> Result<Vec<PairSpec>> {
    let joined = pairs.join(",");
    parse_pairs_list(&joined)
}

pub(super) fn parse_symbols_vec(symbols: &[String]) -> Result<Vec<PairSpec>> {
    let syms: Vec<String> = symbols
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if syms.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    let mut pairs = Vec::new();
    for i in 0..syms.len() {
        for j in (i + 1)..syms.len() {
            let a = syms[i].clone();
            let b = syms[j].clone();
            let (base, quote) = if a < b { (a, b) } else { (b, a) };
            pairs.push(PairSpec { base, quote });
        }
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
    }
    Ok(pairs)
}

pub(super) fn resolve_universe_from_yaml(yaml: &PairTradeYaml) -> Result<Vec<PairSpec>> {
    if env_has_universe_override() {
        return parse_universe_pairs();
    }
    if let Some(pairs) = yaml.universe_pairs.clone() {
        let pairs = pairs.into_vec();
        if pairs.is_empty() {
            return Err(anyhow!("universe_pairs produced no valid pairs"));
        }
        return parse_pairs_vec(&pairs);
    }
    if let Some(symbols) = yaml.universe_symbols.clone() {
        let symbols = symbols.into_vec();
        if symbols.is_empty() {
            return Err(anyhow!("universe_symbols produced no valid pairs"));
        }
        return parse_symbols_vec(&symbols);
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

pub(super) fn parse_universe_pairs() -> Result<Vec<PairSpec>> {
    if let Ok(raw_pairs) = env::var("UNIVERSE_PAIRS") {
        if !raw_pairs.trim().is_empty() {
            return parse_pairs_list(&raw_pairs);
        }
    }
    if let Ok(symbols_raw) = env::var("UNIVERSE_SYMBOLS") {
        if !symbols_raw.trim().is_empty() {
            let syms: Vec<String> = symbols_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let mut pairs = Vec::new();
            if syms.len() == 1 {
                // Single-symbol mode: create a self-pair for data-dump collection
                pairs.push(PairSpec {
                    base: syms[0].clone(),
                    quote: syms[0].clone(),
                });
            } else {
                for i in 0..syms.len() {
                    for j in (i + 1)..syms.len() {
                        let a = syms[i].clone();
                        let b = syms[j].clone();
                        let (base, quote) = if a < b { (a, b) } else { (b, a) };
                        pairs.push(PairSpec { base, quote });
                    }
                }
            }
            if pairs.is_empty() {
                return Err(anyhow!("UNIVERSE_SYMBOLS produced no valid pairs"));
            }
            return Ok(pairs);
        }
    }
    let raw = "BTC/ETH,BTC/SOL,ETH/SOL".to_string();
    parse_pairs_list(&raw)
}

pub(super) fn parse_pairs_list(raw: &str) -> Result<Vec<PairSpec>> {
    let mut pairs = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut split = trimmed.split('/');
        let base = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        let quote = split
            .next()
            .ok_or_else(|| anyhow!("invalid pair: {}", trimmed))?;
        pairs.push(PairSpec {
            base: base.to_string(),
            quote: quote.to_string(),
        });
    }
    if pairs.is_empty() {
        return Err(anyhow!("UNIVERSE_PAIRS produced no valid pairs"));
    }
    Ok(pairs)
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub(super) enum StringOrVec {
    String(String),
    Vec(Vec<String>),
}

impl StringOrVec {
    pub(super) fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::String(value) => value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect(),
            StringOrVec::Vec(values) => values
                .into_iter()
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) struct PairTradeYaml {
    pub(super) dex_name: Option<String>,
    pub(super) rest_endpoint: Option<String>,
    pub(super) web_socket_endpoint: Option<String>,
    pub(super) dry_run: Option<bool>,
    pub(super) agent_name: Option<String>,
    pub(super) interval_secs: Option<u64>,
    pub(super) trading_period_secs: Option<u64>,
    pub(super) metrics_window_length: Option<usize>,
    pub(super) entry_z_score_base: Option<f64>,
    pub(super) entry_z_score_min: Option<f64>,
    pub(super) entry_z_score_max: Option<f64>,
    pub(super) exit_z_score: Option<f64>,
    pub(super) stop_loss_z_score: Option<f64>,
    pub(super) force_close_time_secs: Option<u64>,
    pub(super) cooldown_secs: Option<u64>,
    pub(super) net_funding_min_per_hour: Option<f64>,
    pub(super) spread_velocity_max_sigma_per_min: Option<f64>,
    pub(super) notional_per_leg_usd: Option<f64>,
    pub(super) risk_pct_per_trade: Option<f64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) equity_usd_fallback: Option<f64>,
    pub(super) universe_pairs: Option<StringOrVec>,
    pub(super) universe_symbols: Option<StringOrVec>,
    pub(super) pair_selection_lookback_hours_short: Option<u64>,
    pub(super) pair_selection_lookback_hours_long: Option<u64>,
    pub(super) half_life_max_hours: Option<f64>,
    pub(super) adf_p_threshold: Option<f64>,
    pub(super) entry_vol_lookback_hours: Option<u64>,
    pub(super) slippage_bps: Option<i32>,
    pub(super) fee_bps: Option<f64>,
    pub(super) max_leverage: Option<f64>,
    pub(super) reeval_jump_z_mult: Option<f64>,
    pub(super) vol_spike_mult: Option<f64>,
    pub(super) max_active_pairs: Option<usize>,
    pub(super) warm_start_mode: Option<String>,
    pub(super) warm_start_min_bars: Option<usize>,
    pub(super) order_timeout_secs: Option<u64>,
    pub(super) entry_partial_fill_max_retries: Option<u32>,
    pub(super) startup_force_close_attempts: Option<u32>,
    pub(super) startup_force_close_wait_secs: Option<u64>,
    pub(super) force_close_on_startup: Option<bool>,
    pub(super) enable_data_dump: Option<bool>,
    pub(super) data_dump_file: Option<String>,
    pub(super) observe_only: Option<bool>,
    pub(super) disable_history_persist: Option<bool>,
    pub(super) history_file: Option<String>,
    pub(super) backtest_mode: Option<bool>,
    pub(super) backtest_file: Option<String>,
    pub(super) spread_trend_max_slope_sigma: Option<f64>,
    pub(super) beta_divergence_max: Option<f64>,
    pub(super) beta_min: Option<f64>,
    pub(super) hedge_ratio_max_deviation: Option<f64>,
    pub(super) circuit_breaker_consecutive_losses: Option<u32>,
    pub(super) circuit_breaker_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier1_losses: Option<u32>,
    pub(super) circuit_breaker_tier1_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier2_losses: Option<u32>,
    pub(super) circuit_breaker_tier2_cooldown_secs: Option<u64>,
    pub(super) entry_post_only_timeout_secs: Option<u64>,
    // Phase 2 filters (default off: 0.0 disables)
    pub(super) entry_velocity_block_sigma_per_min: Option<f64>,
    pub(super) funding_entry_z_scale: Option<f64>,
    pub(super) beta_gap_entry_z_scale: Option<f64>,
    pub(super) pair_overrides: Option<HashMap<String, PairOverrideYaml>>,
    /// Graceful shutdown: max seconds to wait for natural exit on SIGTERM before
    /// force-closing both legs. 0 = immediate force close (legacy behavior).
    pub(super) shutdown_grace_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub(super) struct PairOverrideYaml {
    pub(super) entry_z_score_base: Option<f64>,
    pub(super) entry_z_score_min: Option<f64>,
    pub(super) entry_z_score_max: Option<f64>,
    pub(super) exit_z_score: Option<f64>,
    pub(super) stop_loss_z_score: Option<f64>,
    pub(super) force_close_time_secs: Option<u64>,
    pub(super) cooldown_secs: Option<u64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) half_life_max_hours: Option<f64>,
    pub(super) adf_p_threshold: Option<f64>,
    pub(super) spread_velocity_max_sigma_per_min: Option<f64>,
    pub(super) spread_trend_max_slope_sigma: Option<f64>,
    pub(super) beta_divergence_max: Option<f64>,
    pub(super) beta_min: Option<f64>,
    pub(super) hedge_ratio_max_deviation: Option<f64>,
    pub(super) pair_selection_lookback_hours_short: Option<u64>,
    pub(super) pair_selection_lookback_hours_long: Option<u64>,
    pub(super) entry_vol_lookback_hours: Option<u64>,
    pub(super) warm_start_min_bars: Option<usize>,
    pub(super) reeval_jump_z_mult: Option<f64>,
    pub(super) vol_spike_mult: Option<f64>,
    pub(super) circuit_breaker_tier1_losses: Option<u32>,
    pub(super) circuit_breaker_tier1_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier2_losses: Option<u32>,
    pub(super) circuit_breaker_tier2_cooldown_secs: Option<u64>,
    pub(super) entry_post_only_timeout_secs: Option<u64>,
}
