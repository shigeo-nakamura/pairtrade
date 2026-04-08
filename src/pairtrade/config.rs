//! YAML schema types for pairtrade configuration loading.
//!
//! These mirror the on-disk YAML layout one-to-one. The post-merge resolved
//! shapes (`PairTradeConfig`, `PairParams`) still live in the parent module
//! for now and will be migrated in a follow-up step.

use std::collections::HashMap;

use serde::Deserialize;

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
