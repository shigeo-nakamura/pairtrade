//! Raw YAML deserialization schema for pairtrade config (bot-strategy#502).
//!
//! Pure `serde` data types: the top-level `PairTradeYaml`, the `risk:`
//! block (`RiskYaml`), the per-strategy override block (`StrategyYaml`),
//! and the `StringOrVec` helper. Every field is `Option<T>` so the loader
//! in `super` can apply the YAML → env → `DEFAULT_*` fallback. The
//! resolved/public config shapes and the env/validation logic stay in
//! `config/mod.rs`.

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub(in crate::pairtrade) enum StringOrVec {
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

/// Raw YAML schema for the top-level pairtrade config.
///
/// Every field is `Option<T>` so the loader can apply a tiered fallback:
/// YAML value → `apply_env_overrides` env var → compile-time `DEFAULT_*`
/// constant. Operators set only the knobs they want to override in YAML
/// and leave the rest unset; missing fields are not an error.
///
/// Audit note (bot-strategy#398, 2026-05-14): obviously-dead fields from
/// past experiments (`notional_per_leg_usd`, `pair_overrides`, legacy
/// non-tiered circuit breaker) were pruned. Remaining `Option<T>` fields
/// are deliberately kept: each is either set in prod YAML today, or is
/// the surface for a feature-flagged code path (Kalman beta, regime
/// filter, `entry_z_short_multiplier`, `round_id`) where leaving the
/// YAML knob in place keeps the operator-facing schema and the engine
/// code path symmetrical.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::pairtrade) struct PairTradeYaml {
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
    pub(super) stop_loss_cooldown_secs: Option<u64>,
    pub(super) net_funding_min_per_hour: Option<f64>,
    pub(super) spread_velocity_max_sigma_per_min: Option<f64>,
    pub(super) risk_pct_per_trade: Option<f64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) equity_usd_reference: Option<f64>,
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
    pub(super) entry_partial_fill_giveup_retries: Option<u32>,
    pub(super) startup_force_close_attempts: Option<u32>,
    pub(super) startup_force_close_wait_secs: Option<u64>,
    pub(super) force_close_on_startup: Option<bool>,
    pub(super) enable_data_dump: Option<bool>,
    pub(super) data_dump_file: Option<String>,
    pub(super) observe_only: Option<bool>,
    pub(super) disable_history_persist: Option<bool>,
    pub(super) history_file: Option<String>,
    pub(super) history_archive_dir: Option<String>,
    pub(super) history_archive_retention_days: Option<u32>,
    pub(super) backtest_mode: Option<bool>,
    pub(super) backtest_file: Option<String>,
    pub(super) spread_trend_max_slope_sigma: Option<f64>,
    pub(super) beta_divergence_max: Option<f64>,
    pub(super) beta_min: Option<f64>,
    pub(super) hedge_ratio_max_deviation: Option<f64>,
    pub(super) circuit_breaker_tier1_losses: Option<u32>,
    pub(super) circuit_breaker_tier1_cooldown_secs: Option<u64>,
    pub(super) circuit_breaker_tier2_losses: Option<u32>,
    pub(super) circuit_breaker_tier2_cooldown_secs: Option<u64>,
    pub(super) entry_post_only_timeout_secs: Option<u64>,
    pub(super) exit_post_only_timeout_secs: Option<u64>,
    // Phase 2 filters (default off: 0.0 disables)
    pub(super) entry_velocity_block_sigma_per_min: Option<f64>,
    pub(super) funding_entry_z_scale: Option<f64>,
    pub(super) beta_gap_entry_z_scale: Option<f64>,
    pub(super) beta_gap_notional_scale: Option<f64>,
    pub(super) beta_gap_notional_floor: Option<f64>,
    // Signal-depth sizing (bot-strategy#515, default off: slope 0.0 disables)
    pub(super) depth_size_slope: Option<f64>,
    pub(super) depth_size_min: Option<f64>,
    pub(super) depth_size_max: Option<f64>,
    pub(super) rehedge_drift_threshold_pct: Option<f64>,
    pub(super) rehedge_cooldown_secs: Option<u64>,
    pub(super) rehedge_min_qty_notional_usd: Option<f64>,
    pub(super) rehedge_live_enabled: Option<bool>,
    pub(super) use_amend_on_partial_fill: Option<bool>,
    pub(super) rehedge_require_no_revert: Option<bool>,
    pub(super) rehedge_z_no_revert_factor: Option<f64>,
    pub(super) rehedge_velocity_projected_drift_min: Option<f64>,
    pub(super) beta_uncertainty_max: Option<f64>,
    pub(super) entry_z_short_multiplier: Option<f64>,
    pub(super) mtf_windows: Option<Vec<usize>>,
    pub(super) mtf_z_min: Option<f64>,
    pub(super) std_collapse_window_bars: Option<usize>,
    pub(super) std_collapse_min_ratio: Option<f64>,
    pub(super) std_collapse_hold_down_secs: Option<u64>,
    pub(super) std_collapse_observe_only: Option<bool>,
    /// bot-strategy#473: opt-in to frozen-β exit z. Default false.
    pub(super) use_frozen_beta_exit_z: Option<bool>,
    /// bot-strategy#494: opt-in to the persistent-regime entry gate. Default
    /// false (shadow-only). Top-level YAML override of `regime_block_entries`.
    pub(super) regime_block_entries: Option<bool>,
    /// Graceful shutdown: max seconds to wait for natural exit on SIGTERM before
    /// force-closing both legs. 0 = immediate force close (legacy behavior).
    pub(super) shutdown_grace_secs: Option<u64>,
    /// Optional list of strategy variants for the single-process A/B/C
    /// architecture (shigeo-nakamura/bot-strategy#25). When absent, the
    /// loader synthesizes a single strategy from the top-level scalars
    /// (legacy single-bot YAML format) so existing configs keep working.
    pub(super) strategies: Option<Vec<StrategyYaml>>,
    // Kalman filter beta estimation
    pub(super) use_kalman_beta: Option<bool>,
    pub(super) kalman_q: Option<f64>,
    pub(super) kalman_r: Option<f64>,
    pub(super) kalman_initial_p: Option<f64>,
    pub(super) kalman_min_updates: Option<u64>,
    // Regime filter
    pub(super) regime_vol_window: Option<usize>,
    pub(super) regime_vol_max: Option<f64>,
    pub(super) regime_trend_window: Option<usize>,
    pub(super) regime_trend_max: Option<f64>,
    pub(super) regime_reference_symbol: Option<String>,
    // Daily drawdown limit (bot-strategy#185 Phase 2)
    pub(super) risk: Option<RiskYaml>,
    /// Round identifier (e.g. `"round-4"`). When set, the engine compares
    /// this against the value persisted in `risk_state.json` at startup,
    /// and on transition (configured `Some(new)` != persisted `Some(old)`)
    /// resets round-bound per-instance fields (trade stats, equity samples,
    /// stop-loss cool-down anchors, session halt). Unset = legacy behavior
    /// (no auto-reset; operator runs `scripts/reset-round-state.sh` between
    /// rounds). bot-strategy#354.
    pub(super) round_id: Option<String>,
}

/// `risk:` YAML block for cross-session safety limits. Phase 2 covers
/// daily DD; Phase 3 (bot-strategy#185) adds session-level DD with
/// auto-flatten + manual ack and an absolute notional cap per hedge leg.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::pairtrade) struct RiskYaml {
    /// Threshold in basis points of `session_start_equity`, expressed in
    /// 1x-equivalent (market-move) units. The bot multiplies this by
    /// `max_leverage` at comparison time, so a single value covers any
    /// leverage and changing `max_leverage` does not require rewriting
    /// the bps. Typical 100–500 bps. 0 disables (default). A loss of
    /// `realized_pnl_today` ≥ effective threshold triggers
    /// `max_daily_loss_action`. See bot-strategy#185 leverage-
    /// neutralization amendment.
    pub(super) max_daily_loss_bps: Option<u32>,
    /// Action taken once the threshold trips. Phase 2 only implements
    /// `block` — new entries are refused, existing positions exit
    /// normally, auto-resume at the next UTC reset. `flatten` (force
    /// close all positions) lands in Phase 3.
    pub(super) max_daily_loss_action: Option<String>,
    /// Hour of day (UTC) at which `realized_pnl_today` resets to zero.
    /// 0 = UTC midnight (default), matching most prop-firm conventions.
    pub(super) daily_reset_utc_hour: Option<u32>,
    /// Phase 3-1: drawdown threshold in basis points of the rolling
    /// peak equity, expressed in 1x-equivalent (market-move) units.
    /// The bot multiplies this by `max_leverage` at comparison time so
    /// the halt fires at the same underlying market move regardless of
    /// leverage; equity DD scales ~linearly with leverage so the
    /// scaled threshold tracks observed dd_bps consistently. Typical
    /// 100–500 bps. 0 disables (default). On breach, the engine
    /// flattens the instance and stays halted until manually ack'd.
    /// See bot-strategy#185 leverage-neutralization amendment.
    pub(super) max_session_loss_bps: Option<u32>,
    /// Window for the rolling peak in seconds. Default 30 days.
    pub(super) session_dd_lookback_secs: Option<u64>,
    /// Sampling cadence for `equity_samples` in seconds. Default 1 h.
    /// Smaller values track the peak more tightly at the cost of more
    /// disk writes.
    pub(super) session_dd_sample_secs: Option<u64>,
    /// bot-strategy#575 ①: minimum unexplained equity jump (USD), observed
    /// while the instance is flat and settled, to classify a deposit /
    /// withdrawal as a capital event and rebaseline the rolling session-DD
    /// peak to the new equity (DD → 0). 0 disables. Default 5 USD.
    pub(super) session_dd_capital_event_min_usd: Option<f64>,
    /// bot-strategy#575 ①: how long (seconds) the instance must have been
    /// continuously flat before capital-event detection trusts the equity
    /// reading. Guards against reading a post-close collateral-settlement
    /// lag as a deposit. Default 60 s; a halted variant always satisfies it.
    pub(super) session_dd_capital_settle_secs: Option<u64>,
    /// Phase 3-4: hard cap on per-leg USD notional, expressed as a
    /// multiplier of `equity_reference_usd × max_leverage`. 0 disables
    /// (default). 1.0 means "exactly the intended leverage"; values in
    /// 1.0–1.2 give slippage / rounding headroom while still rejecting
    /// the bug-driven oversizing the cap is meant to defend against.
    /// The multiplicative form auto-scales across A/B/C with different
    /// `equity_reference_usd` and across hosts (Frankfurt vs Tokyo
    /// Lighter) with different equity or `max_leverage`, so a single
    /// YAML value covers the whole fleet without env split.
    pub(super) max_notional_headroom: Option<f64>,
}

/// Per-strategy override block in the multi-strategy YAML format
/// (shigeo-nakamura/bot-strategy#25). Every field is `Option<T>`: unset
/// fields inherit the corresponding top-level resolved value at instance
/// build time. Only the knobs that actually differ between A/B/C live
/// here — adding a new per-strategy override means (1) add the field as
/// `Option<T>` here, (2) carry it into `StrategyConfig`, (3) consume it
/// in `pairtrade::mod` when building each `StrategyInstance`.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::pairtrade) struct StrategyYaml {
    pub(super) id: Option<String>,
    pub(super) agent_name: Option<String>,
    pub(super) exit_z_score: Option<f64>,
    pub(super) stop_loss_z_score: Option<f64>,
    pub(super) max_loss_r_mult: Option<f64>,
    pub(super) equity_usd_reference: Option<f64>,
    // Per-strategy PairParams overrides (None = inherit from top-level)
    pub(super) force_close_time_secs: Option<u64>,
    pub(super) mtf_windows: Option<Vec<usize>>,
    pub(super) mtf_z_min: Option<f64>,
    pub(super) entry_z_score_base: Option<f64>,
    pub(super) entry_z_score_min: Option<f64>,
    pub(super) entry_z_score_max: Option<f64>,
    /// Per-strategy override of the global `beta_gap_entry_z_scale`.
    /// Round 5 (bot-strategy#461): variant C disables threshold-side
    /// scaling by setting this to 0.0; A/B inherit the global default.
    pub(super) beta_gap_entry_z_scale: Option<f64>,
    /// Per-strategy override of `beta_gap_notional_scale` (bot-strategy#461).
    pub(super) beta_gap_notional_scale: Option<f64>,
    /// Per-strategy override of `beta_gap_notional_floor` (bot-strategy#461).
    pub(super) beta_gap_notional_floor: Option<f64>,
    /// Per-strategy overrides for #515 signal-depth sizing — lets one
    /// challenger arm enable sizing while control variants inherit the
    /// disabled global default.
    pub(super) depth_size_slope: Option<f64>,
    pub(super) depth_size_min: Option<f64>,
    pub(super) depth_size_max: Option<f64>,
    /// Per-strategy overrides for #463 mid-hold re-hedge (Phase 1: detection only).
    pub(super) rehedge_drift_threshold_pct: Option<f64>,
    pub(super) rehedge_cooldown_secs: Option<u64>,
    pub(super) rehedge_min_qty_notional_usd: Option<f64>,
    pub(super) rehedge_live_enabled: Option<bool>,
    pub(super) use_amend_on_partial_fill: Option<bool>,
    pub(super) rehedge_require_no_revert: Option<bool>,
    pub(super) rehedge_z_no_revert_factor: Option<f64>,
    pub(super) rehedge_velocity_projected_drift_min: Option<f64>,
    pub(super) beta_uncertainty_max: Option<f64>,
    /// bot-strategy#500: per-variant override of `std_collapse_hold_down_secs`.
    /// Lets a single challenger test the defensive hold-down while controls
    /// inherit the global default.
    pub(super) std_collapse_hold_down_secs: Option<u64>,
    /// bot-strategy#473: per-variant override of `use_frozen_beta_exit_z`.
    /// Round 6 C opts in; A/B inherit the global default (false).
    pub(super) use_frozen_beta_exit_z: Option<bool>,
    /// bot-strategy#494 Phase 1: per-variant override of `regime_block_entries`.
    /// Lets a single challenger opt into the regime entry-gate while the
    /// control variants inherit the global default (false).
    pub(super) regime_block_entries: Option<bool>,
}
