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
