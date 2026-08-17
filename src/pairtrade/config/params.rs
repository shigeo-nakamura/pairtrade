use std::env;

use super::super::defaults::*;
use super::env_util::{env_parse, env_parse_critical};
use super::schema::PairTradeYaml;

/// Resolved per-pair parameters (global defaults merged with any pair-specific overrides).
#[derive(Debug, Clone, Default)]
pub struct PairParams {
    pub entry_z_base: f64,
    pub entry_z_min: f64,
    pub entry_z_max: f64,
    pub exit_z: f64,
    pub stop_loss_z: f64,
    pub force_close_secs: u64,
    pub cooldown_secs: u64,
    /// Per-direction post-stop_loss_z cool-down (seconds). When > 0, blocks
    /// new entries in the same direction as the most recent stop_loss_z exit
    /// for this many seconds. Independent of the generic `cooldown_secs` and
    /// the global circuit breaker. 0 = disabled (legacy behavior).
    /// bot-strategy#316.
    pub stop_loss_cooldown_secs: u64,
    pub max_loss_r_mult: f64,
    pub half_life_max_hours: f64,
    pub adf_p_threshold: f64,
    pub spread_velocity_max_sigma_per_min: f64,
    pub spread_trend_max_slope_sigma: f64,
    pub beta_divergence_max: f64,
    pub beta_min: f64,
    /// Entry-side-only floor applied to `|beta|` when sizing leg B's
    /// notional (bot-strategy#798). 0.0 disables (legacy behavior: leg B
    /// notional = leg_A_notional * |beta|, unbounded below). Does not
    /// affect `beta_min` (entry eligibility) or z-score/spread math.
    pub sizing_beta_floor: f64,
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
    /// Per-leg fill timeout for exit post-only orders (seconds). 0 disables
    /// the monitor (legacy behavior: post-only legs rest until filled). On
    /// venues with `fee_bps == 0` this knob has no effect because exits do
    /// not enter the post-only path. See bot-strategy#306.
    pub exit_post_only_timeout_secs: u64,
    // Phase 2 filters (0.0 = disabled)
    pub entry_velocity_block_sigma_per_min: f64,
    pub funding_entry_z_scale: f64,
    pub beta_gap_entry_z_scale: f64,
    /// Linear notional shrink as `shared.beta_gap` grows: notional is
    /// multiplied by `clamp(1 - beta_gap_notional_scale * beta_gap,
    /// beta_gap_notional_floor, 1.0)`. Captures z-fire opportunities
    /// that the threshold-side `beta_gap_entry_z_scale` would discard
    /// entirely, at the cost of a smaller per-trade exposure when the
    /// hedge is uncertain. 0.0 = disabled. (bot-strategy#461)
    pub beta_gap_notional_scale: f64,
    /// Lower bound for `beta_gap_notional_scale`'s shrink. Default 0.5
    /// — never go below 50% of base size — so minimum-order / dust
    /// constraints stay satisfied. 0.0 still works as long as no min-
    /// notional issues; 1.0 disables (no shrink ever).
    pub beta_gap_notional_floor: f64,
    /// Signal-depth position sizing (bot-strategy#515): entry notional is
    /// multiplied by `clamp(depth_size_min + depth_size_slope *
    /// (|z_entry| - entry_z_base), depth_size_min, depth_size_max)`,
    /// concentrating capital on deeper (higher-|z|) entries without
    /// changing the entry threshold or trade count. The multiplier is
    /// resolved once at entry and frozen for the hold. 0.0 = disabled
    /// (multiplier 1.0).
    pub depth_size_slope: f64,
    /// Lower bound of the depth-sizing multiplier (default 0.5). Keep
    /// high enough that venue min-order / dust constraints stay
    /// satisfied at the smallest per-variant equity.
    pub depth_size_min: f64,
    /// Upper bound of the depth-sizing multiplier (default 1.5, hard
    /// cap 2.0). The absolute notional cap (`equity × max_leverage ×
    /// max_notional_headroom`) still applies after scaling, so leverage
    /// headroom is never exceeded.
    pub depth_size_max: f64,
    /// Re-hedge drift threshold (#463). Triggers a mid-hold re-hedge
    /// when `|β_now − β_entry| / β_entry ≥ rehedge_drift_threshold_pct`
    /// (a fraction, NOT a percentage — 0.15 = 15%). 0.0 = disabled.
    /// Phase 1 of #463 only logs / counts; Phase 2 places orders.
    pub rehedge_drift_threshold_pct: f64,
    /// Minimum seconds between re-hedges on the same position to avoid
    /// chasing β oscillation. Default 1800 (30 min). bot-strategy#463.
    pub rehedge_cooldown_secs: u64,
    /// Minimum *notional* (USD) of the would-be re-hedge order. Below
    /// this we skip — both because the fee bps cost dominates and to
    /// avoid sub-min-order rejections. Default $50. bot-strategy#463.
    pub rehedge_min_qty_notional_usd: f64,
    /// Live placement gate (bot-strategy#463 Phase 2). When `false`
    /// (default), detected re-hedges in live mode are logged + counted
    /// but no order is placed — Phase 1 behaviour. When `true`, a
    /// taker order is submitted to the connector. Dry-run and backtest
    /// modes ALWAYS simulate the re-hedge regardless of this flag, so
    /// BT verification works before live opt-in.
    pub rehedge_live_enabled: bool,
    /// bot-strategy#471: when `true`, the entry partial-fill reissue path
    /// amends the still-open order in place (atomic `modify_order`) instead
    /// of cancel + reissue, eliminating the race the bot-strategy#470 cap
    /// papers over. Opt-in per host; default `false` falls back to the
    /// legacy cancel+reissue path. Any `modify_order` error (incl. venues
    /// that don't support amend) also falls back, so the #470 cap stays the
    /// backstop.
    pub use_amend_on_partial_fill: bool,
    /// Additional gate (bot-strategy#465): only fire re-hedge when the
    /// spread is at least as far from the mean as it was at entry, i.e.
    /// `|z_now| >= |z_entry| * rehedge_z_no_revert_factor`. Default
    /// factor 1.0 disables the gate when `rehedge_require_no_revert =
    /// false`. Set `rehedge_require_no_revert = true` to enable. When
    /// active and the gate trips, the re-hedge is skipped and the
    /// position rides the natural mean reversion instead of locking in
    /// the drift-time β at a sub-optimal price.
    pub rehedge_require_no_revert: bool,
    pub rehedge_z_no_revert_factor: f64,
    /// β-velocity gate (bot-strategy#465 Option B). When > 0, the
    /// re-hedge fires only if the **projected** total drift over the
    /// remaining hold time exceeds this fraction. Computed as:
    ///
    ///   |β_velocity| × remaining_hold_secs / |β_entry|
    ///
    /// where `β_velocity = (β_now − β_prev) / (now_ts − prev_ts)`
    /// from the per-position `prev_beta_for_velocity` snapshot. A
    /// small instantaneous drift that has been stable for many ticks
    /// has tiny velocity → projects to small future drift → re-hedge
    /// skipped. A fast-developing drift even at the same |drift|
    /// magnitude projects to large future drift → re-hedge fires.
    /// 0.0 = disabled (legacy: only |drift| and NRV matter).
    pub rehedge_velocity_projected_drift_min: f64,
    /// Kalman β-uncertainty entry gate (bot-strategy#462 Phase 2). When
    /// > 0, blocks new entries while the Kalman posterior σ_β exceeds
    /// this threshold. Calibrated from the live distribution that
    /// Phase 1 (`pairtrade_beta_uncertainty` Prom gauge) has been
    /// collecting since 2026-05-20. 0.0 = disabled (Phase 1 behaviour
    /// preserved). Sensible starting value once calibrated:
    /// ~P85 of the observed distribution.
    pub beta_uncertainty_max: f64,
    /// Multiplicative scale applied to `entry_threshold` when the proposed
    /// direction is `ShortSpread`. 1.0 keeps the current direction-symmetric
    /// behavior; values > 1.0 require a deeper |z| for short entries (gates
    /// short-side sampling at a comparable tail percentile to the long side
    /// when the spread distribution is left-skewed). Use a very large value
    /// (e.g. 9999.0) to disable shorts entirely. <= 0.0 is treated as
    /// disabled / symmetric, matching the auto-derived `f64` default in
    /// non-production constructors. bot-strategy#358.
    pub entry_z_short_multiplier: f64,
    // Multi-timeframe z-score confluence (empty = disabled)
    pub mtf_windows: Vec<usize>,
    pub mtf_z_min: f64,
    // Std collapse guard (both 0 = disabled). See bot-strategy#62.
    pub std_collapse_window_bars: usize,
    pub std_collapse_min_ratio: f64,
    /// Entry hold-down after a recent std-collapse sample. 0 = disabled
    /// (legacy point-in-time guard only). bot-strategy#500.
    pub std_collapse_hold_down_secs: u64,
    pub std_collapse_observe_only: bool,
    /// Use frozen-β z for exit-side gates (`exit_z`, `stop_loss_z`,
    /// expected-value). When `true`, exit-side z is recomputed against
    /// `Position.entry_beta` and the current log prices instead of the
    /// rolling `shared.beta`, so a β drift during the hold does not
    /// produce a "z reverted but no actual mean-reversion" false
    /// signal. Default `false` preserves legacy behaviour. Entry /
    /// regime / dashboard checks keep using rolling-β z regardless.
    /// See bot-strategy#473.
    pub use_frozen_beta_exit_z: bool,
    /// Innovation-responsive persistent-regime gate (bot-strategy#494
    /// Phase 1). When `true`, blocks new entries while the
    /// `RegimeDetector` (CUSUM of normalised Δspread residuals) reports a
    /// persistent β/model shift. Default `false` keeps Phase 1 shadow
    /// behaviour: the `pairtrade_regime_*` gauges and `[REGIME]` logs are
    /// still emitted, but trading is unchanged. Flip on per host/variant
    /// (env `REGIME_BLOCK_ENTRIES` or top-level YAML) once the gauges have
    /// been calibrated and BT validates the entry-only gate.
    pub regime_block_entries: bool,
}

/// Resolve global per-pair defaults from environment variables, falling back
/// to compile-time `DEFAULT_*` constants for any missing entries.
pub(super) fn default_pair_params_from_env() -> PairParams {
    PairParams {
        entry_z_base: env_parse("ENTRY_Z_SCORE_BASE", DEFAULT_ENTRY_Z_BASE),
        entry_z_min: env_parse("ENTRY_Z_SCORE_MIN", DEFAULT_ENTRY_Z_MIN),
        entry_z_max: env_parse("ENTRY_Z_SCORE_MAX", DEFAULT_ENTRY_Z_MAX),
        exit_z: env_parse("EXIT_Z_SCORE", DEFAULT_EXIT_Z),
        stop_loss_z: env_parse("STOP_LOSS_Z_SCORE", DEFAULT_STOP_LOSS_Z),
        force_close_secs: env_parse("FORCE_CLOSE_TIME_SECS", DEFAULT_FORCE_CLOSE_SECS),
        cooldown_secs: env_parse("COOLDOWN_SECS", DEFAULT_COOLDOWN_SECS),
        stop_loss_cooldown_secs: env_parse(
            "STOP_LOSS_COOLDOWN_SECS",
            DEFAULT_STOP_LOSS_COOLDOWN_SECS,
        ),
        // bot-strategy#439: stop-loss multiplier — silent default revert
        // could place a trade with a much wider stop than intended.
        max_loss_r_mult: env_parse_critical("MAX_LOSS_R_MULT", DEFAULT_MAX_LOSS_R_MULT),
        half_life_max_hours: env_parse("HALF_LIFE_MAX_HOURS", DEFAULT_HALF_LIFE_MAX_HOURS),
        adf_p_threshold: env_parse("ADF_P_THRESHOLD", DEFAULT_ADF_P_THRESHOLD),
        spread_velocity_max_sigma_per_min: env_parse(
            "SPREAD_VELOCITY_MAX_SIGMA_PER_MIN",
            DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN,
        ),
        spread_trend_max_slope_sigma: env_parse(
            "SPREAD_TREND_MAX_SLOPE_SIGMA",
            DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA,
        ),
        beta_divergence_max: env_parse("BETA_DIVERGENCE_MAX", DEFAULT_BETA_DIVERGENCE_MAX),
        beta_min: env_parse("BETA_MIN", 0.0),
        sizing_beta_floor: env_parse("SIZING_BETA_FLOOR", 0.0),
        hedge_ratio_max_deviation: env_parse("HEDGE_RATIO_MAX_DEVIATION", 1.0),
        lookback_hours_short: env_parse(
            "PAIR_SELECTION_LOOKBACK_HOURS_SHORT",
            DEFAULT_LOOKBACK_HOURS_SHORT,
        ),
        lookback_hours_long: env_parse(
            "PAIR_SELECTION_LOOKBACK_HOURS_LONG",
            DEFAULT_LOOKBACK_HOURS_LONG,
        ),
        entry_vol_lookback_hours: env_parse(
            "ENTRY_VOL_LOOKBACK_HOURS",
            DEFAULT_ENTRY_VOL_LOOKBACK_HOURS,
        ),
        // Caller is responsible for filling warm_start_min_bars from
        // metrics_window when omitted.
        warm_start_min_bars: env_parse::<usize>("WARM_START_MIN_BARS", 0),
        reeval_jump_z_mult: env_parse("REEVAL_JUMP_Z_MULT", DEFAULT_REEVAL_JUMP_Z_MULT),
        vol_spike_mult: env_parse("VOL_SPIKE_MULT", DEFAULT_VOL_SPIKE_MULT),
        circuit_breaker_tier1_losses: env_parse(
            "CIRCUIT_BREAKER_TIER1_LOSSES",
            DEFAULT_CB_TIER1_LOSSES,
        ),
        circuit_breaker_tier1_cooldown_secs: env_parse(
            "CIRCUIT_BREAKER_TIER1_COOLDOWN_SECS",
            DEFAULT_CB_TIER1_COOLDOWN_SECS,
        ),
        circuit_breaker_tier2_losses: env_parse(
            "CIRCUIT_BREAKER_TIER2_LOSSES",
            DEFAULT_CB_TIER2_LOSSES,
        ),
        circuit_breaker_tier2_cooldown_secs: env_parse(
            "CIRCUIT_BREAKER_TIER2_COOLDOWN_SECS",
            DEFAULT_CB_TIER2_COOLDOWN_SECS,
        ),
        entry_post_only_timeout_secs: env_parse(
            "ENTRY_POST_ONLY_TIMEOUT_SECS",
            DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS,
        ),
        exit_post_only_timeout_secs: env_parse(
            "EXIT_POST_ONLY_TIMEOUT_SECS",
            DEFAULT_EXIT_POST_ONLY_TIMEOUT_SECS,
        ),
        entry_velocity_block_sigma_per_min: env_parse("ENTRY_VELOCITY_BLOCK_SIGMA_PER_MIN", 0.0),
        funding_entry_z_scale: env_parse("FUNDING_ENTRY_Z_SCALE", 0.0),
        beta_gap_entry_z_scale: env_parse("BETA_GAP_ENTRY_Z_SCALE", 0.0),
        beta_gap_notional_scale: env_parse("BETA_GAP_NOTIONAL_SCALE", 0.0),
        beta_gap_notional_floor: env_parse("BETA_GAP_NOTIONAL_FLOOR", 0.5),
        // bot-strategy#515 — default DISABLED (slope=0.0).
        depth_size_slope: env_parse("DEPTH_SIZE_SLOPE", 0.0),
        depth_size_min: env_parse("DEPTH_SIZE_MIN", 0.5),
        depth_size_max: env_parse("DEPTH_SIZE_MAX", 1.5),
        // bot-strategy#463 Phase 1 — default DISABLED (threshold=0.0).
        rehedge_drift_threshold_pct: env_parse("REHEDGE_DRIFT_THRESHOLD_PCT", 0.0),
        rehedge_cooldown_secs: env_parse("REHEDGE_COOLDOWN_SECS", 1800u64),
        rehedge_min_qty_notional_usd: env_parse("REHEDGE_MIN_QTY_NOTIONAL_USD", 50.0),
        // bot-strategy#463 Phase 2 — live order placement is OFF by
        // default. BT / dry_run always simulate; live opt-in requires
        // this to be flipped (env or per-strategy YAML).
        rehedge_live_enabled: env_parse("REHEDGE_LIVE_ENABLED", false),
        // bot-strategy#471 — amend-on-partial-fill is OFF by default; flip
        // per host (env or per-strategy YAML) after the Tokyo Extended
        // dry_run soak.
        use_amend_on_partial_fill: env_parse("USE_AMEND_ON_PARTIAL_FILL", false),
        rehedge_require_no_revert: env_parse("REHEDGE_REQUIRE_NO_REVERT", false),
        rehedge_z_no_revert_factor: env_parse("REHEDGE_Z_NO_REVERT_FACTOR", 1.0),
        rehedge_velocity_projected_drift_min: env_parse(
            "REHEDGE_VELOCITY_PROJECTED_DRIFT_MIN",
            0.0,
        ),
        beta_uncertainty_max: env_parse("BETA_UNCERTAINTY_MAX", 0.0),
        entry_z_short_multiplier: env_parse("ENTRY_Z_SHORT_MULTIPLIER", 1.0),
        mtf_windows: env::var("MTF_WINDOWS")
            .ok()
            .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
            .unwrap_or_default(),
        mtf_z_min: env_parse("MTF_Z_MIN", DEFAULT_MTF_Z_MIN),
        std_collapse_window_bars: env_parse(
            "STD_COLLAPSE_WINDOW_BARS",
            DEFAULT_STD_COLLAPSE_WINDOW_BARS,
        ),
        std_collapse_min_ratio: env_parse("STD_COLLAPSE_MIN_RATIO", DEFAULT_STD_COLLAPSE_MIN_RATIO),
        std_collapse_hold_down_secs: env_parse(
            "STD_COLLAPSE_HOLD_DOWN_SECS",
            DEFAULT_STD_COLLAPSE_HOLD_DOWN_SECS,
        ),
        std_collapse_observe_only: env::var("STD_COLLAPSE_OBSERVE_ONLY")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_STD_COLLAPSE_OBSERVE_ONLY),
        use_frozen_beta_exit_z: env::var("USE_FROZEN_BETA_EXIT_Z")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_USE_FROZEN_BETA_EXIT_Z),
        regime_block_entries: env::var("REGIME_BLOCK_ENTRIES")
            .ok()
            .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(DEFAULT_REGIME_BLOCK_ENTRIES),
    }
}

/// Resolve global per-pair defaults directly from a YAML document, falling
/// back to compile-time `DEFAULT_*` constants for any missing fields.
pub(super) fn default_pair_params_from_yaml(yaml: &PairTradeYaml) -> PairParams {
    PairParams {
        entry_z_base: yaml.entry_z_score_base.unwrap_or(DEFAULT_ENTRY_Z_BASE),
        entry_z_min: yaml.entry_z_score_min.unwrap_or(DEFAULT_ENTRY_Z_MIN),
        entry_z_max: yaml.entry_z_score_max.unwrap_or(DEFAULT_ENTRY_Z_MAX),
        exit_z: yaml.exit_z_score.unwrap_or(DEFAULT_EXIT_Z),
        stop_loss_z: yaml.stop_loss_z_score.unwrap_or(DEFAULT_STOP_LOSS_Z),
        force_close_secs: yaml
            .force_close_time_secs
            .unwrap_or(DEFAULT_FORCE_CLOSE_SECS),
        cooldown_secs: yaml.cooldown_secs.unwrap_or(DEFAULT_COOLDOWN_SECS),
        stop_loss_cooldown_secs: yaml
            .stop_loss_cooldown_secs
            .unwrap_or(DEFAULT_STOP_LOSS_COOLDOWN_SECS),
        max_loss_r_mult: yaml.max_loss_r_mult.unwrap_or(DEFAULT_MAX_LOSS_R_MULT),
        half_life_max_hours: yaml
            .half_life_max_hours
            .unwrap_or(DEFAULT_HALF_LIFE_MAX_HOURS),
        adf_p_threshold: yaml.adf_p_threshold.unwrap_or(DEFAULT_ADF_P_THRESHOLD),
        spread_velocity_max_sigma_per_min: yaml
            .spread_velocity_max_sigma_per_min
            .unwrap_or(DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN),
        spread_trend_max_slope_sigma: yaml
            .spread_trend_max_slope_sigma
            .unwrap_or(DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA),
        beta_divergence_max: yaml
            .beta_divergence_max
            .unwrap_or(DEFAULT_BETA_DIVERGENCE_MAX),
        beta_min: yaml.beta_min.unwrap_or(0.0),
        sizing_beta_floor: yaml.sizing_beta_floor.unwrap_or(0.0),
        hedge_ratio_max_deviation: yaml.hedge_ratio_max_deviation.unwrap_or(1.0),
        lookback_hours_short: yaml
            .pair_selection_lookback_hours_short
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_SHORT),
        lookback_hours_long: yaml
            .pair_selection_lookback_hours_long
            .unwrap_or(DEFAULT_LOOKBACK_HOURS_LONG),
        entry_vol_lookback_hours: yaml
            .entry_vol_lookback_hours
            .unwrap_or(DEFAULT_ENTRY_VOL_LOOKBACK_HOURS),
        // Caller is responsible for clamping warm_start_min_bars to
        // metrics_window when omitted (it has a cross-field default).
        warm_start_min_bars: yaml.warm_start_min_bars.unwrap_or(0),
        reeval_jump_z_mult: yaml
            .reeval_jump_z_mult
            .unwrap_or(DEFAULT_REEVAL_JUMP_Z_MULT),
        vol_spike_mult: yaml.vol_spike_mult.unwrap_or(DEFAULT_VOL_SPIKE_MULT),
        circuit_breaker_tier1_losses: yaml
            .circuit_breaker_tier1_losses
            .unwrap_or(DEFAULT_CB_TIER1_LOSSES),
        circuit_breaker_tier1_cooldown_secs: yaml
            .circuit_breaker_tier1_cooldown_secs
            .unwrap_or(DEFAULT_CB_TIER1_COOLDOWN_SECS),
        circuit_breaker_tier2_losses: yaml
            .circuit_breaker_tier2_losses
            .unwrap_or(DEFAULT_CB_TIER2_LOSSES),
        circuit_breaker_tier2_cooldown_secs: yaml
            .circuit_breaker_tier2_cooldown_secs
            .unwrap_or(DEFAULT_CB_TIER2_COOLDOWN_SECS),
        entry_post_only_timeout_secs: yaml
            .entry_post_only_timeout_secs
            .unwrap_or(DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS),
        exit_post_only_timeout_secs: yaml
            .exit_post_only_timeout_secs
            .unwrap_or(DEFAULT_EXIT_POST_ONLY_TIMEOUT_SECS),
        entry_velocity_block_sigma_per_min: yaml.entry_velocity_block_sigma_per_min.unwrap_or(0.0),
        funding_entry_z_scale: yaml.funding_entry_z_scale.unwrap_or(0.0),
        beta_gap_entry_z_scale: yaml.beta_gap_entry_z_scale.unwrap_or(0.0),
        beta_gap_notional_scale: yaml.beta_gap_notional_scale.unwrap_or(0.0),
        beta_gap_notional_floor: yaml.beta_gap_notional_floor.unwrap_or(0.5),
        // bot-strategy#515 defaults — disabled.
        depth_size_slope: yaml.depth_size_slope.unwrap_or(0.0),
        depth_size_min: yaml.depth_size_min.unwrap_or(0.5),
        depth_size_max: yaml.depth_size_max.unwrap_or(1.5),
        // bot-strategy#463 Phase 1 defaults — disabled.
        rehedge_drift_threshold_pct: yaml.rehedge_drift_threshold_pct.unwrap_or(0.0),
        rehedge_cooldown_secs: yaml.rehedge_cooldown_secs.unwrap_or(1800),
        rehedge_min_qty_notional_usd: yaml.rehedge_min_qty_notional_usd.unwrap_or(50.0),
        rehedge_live_enabled: yaml.rehedge_live_enabled.unwrap_or(false),
        use_amend_on_partial_fill: yaml.use_amend_on_partial_fill.unwrap_or(false),
        rehedge_require_no_revert: yaml.rehedge_require_no_revert.unwrap_or(false),
        rehedge_z_no_revert_factor: yaml.rehedge_z_no_revert_factor.unwrap_or(1.0),
        rehedge_velocity_projected_drift_min: yaml
            .rehedge_velocity_projected_drift_min
            .unwrap_or(0.0),
        beta_uncertainty_max: yaml.beta_uncertainty_max.unwrap_or(0.0),
        entry_z_short_multiplier: yaml.entry_z_short_multiplier.unwrap_or(1.0),
        mtf_windows: yaml.mtf_windows.clone().unwrap_or_default(),
        mtf_z_min: yaml.mtf_z_min.unwrap_or(DEFAULT_MTF_Z_MIN),
        std_collapse_window_bars: yaml
            .std_collapse_window_bars
            .unwrap_or(DEFAULT_STD_COLLAPSE_WINDOW_BARS),
        std_collapse_min_ratio: yaml
            .std_collapse_min_ratio
            .unwrap_or(DEFAULT_STD_COLLAPSE_MIN_RATIO),
        std_collapse_hold_down_secs: yaml
            .std_collapse_hold_down_secs
            .unwrap_or(DEFAULT_STD_COLLAPSE_HOLD_DOWN_SECS),
        std_collapse_observe_only: yaml
            .std_collapse_observe_only
            .unwrap_or(DEFAULT_STD_COLLAPSE_OBSERVE_ONLY),
        use_frozen_beta_exit_z: yaml
            .use_frozen_beta_exit_z
            .unwrap_or(DEFAULT_USE_FROZEN_BETA_EXIT_Z),
        regime_block_entries: yaml
            .regime_block_entries
            .unwrap_or(DEFAULT_REGIME_BLOCK_ENTRIES),
    }
}
