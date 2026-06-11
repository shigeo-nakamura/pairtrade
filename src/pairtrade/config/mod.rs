//! Pairtrade configuration: resolved shapes and the env/YAML →
//! resolved-config builder. The raw YAML deserialization schema types live
//! in the `schema` submodule (bot-strategy#502).

mod env_overrides;
mod env_util;
mod from_env;
mod from_yaml;
mod risk;
mod schema;
mod strategy;
mod universe;
mod validate;

#[cfg(test)]
use risk::resolve_risk_config;
pub use risk::{DailyLossAction, RiskConfig};
use std::collections::HashMap;
use std::time::Duration;
pub use strategy::StrategyConfig;

use serde::Deserialize;

pub use universe::PairSpec;

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

#[derive(Debug, Clone)]
pub struct PairTradeConfig {
    pub dex_name: String,
    pub rest_endpoint: String,
    pub web_socket_endpoint: String,
    pub dry_run: bool,
    pub agent_name: Option<String>,
    pub interval_secs: u64,
    pub trading_period_secs: u64,
    pub metrics_window: usize,
    pub net_funding_min_per_hour: f64,
    pub risk_pct_per_trade: f64,
    pub equity_reference_usd: f64,
    pub universe: Vec<PairSpec>,
    pub slippage_bps: i32,
    pub fee_bps: f64,
    pub max_leverage: f64,
    pub max_active_pairs: usize,
    pub warm_start_mode: WarmStartMode,
    pub order_timeout_secs: u64,
    pub entry_partial_fill_max_retries: u32,
    /// Hard cap on partial-fill reissue retries before the bot gives up,
    /// flattens any filled legs and clears `pending_entry`. See
    /// `DEFAULT_ENTRY_PARTIAL_FILL_GIVEUP_RETRIES` and bot-strategy#480.
    pub entry_partial_fill_giveup_retries: u32,
    pub startup_force_close_attempts: u32,
    pub startup_force_close_wait_secs: u64,
    pub force_close_on_startup: bool,
    // For data dump feature
    pub enable_data_dump: bool,
    pub data_dump_file: Option<String>,
    // Safety guard to avoid real orders while observing market data
    pub observe_only: bool,
    pub disable_history_persist: bool,
    pub history_file: String,
    pub history_archive_dir: Option<String>,
    pub history_archive_retention_days: u32,
    // For backtest feature
    pub backtest_mode: bool,
    pub backtest_file: Option<String>,
    /// Path to a history snapshot file for BT warm-start. When set,
    /// the replay loads price history from this file before the first
    /// tick, giving the BT an identical starting state to a live bot.
    pub bt_warm_start_snapshot: Option<String>,
    /// Path to a file listing live eval firing timestamps (one UNIX
    /// second per line). In BT mode, when set, the pair re-evaluation
    /// gate is overridden to fire ONLY at these exact timestamps —
    /// replaying the exact wall-clock phase at which the live bot ran
    /// `evaluate_pair` so that `state.beta` (and therefore every
    /// subsequent spread = log_a − β·log_b written to
    /// `spread_history`) follows the live trajectory. Without this
    /// override, BT and live eval gates desync within a few hours due
    /// to 1s-level phase drift and the `last_eval_ts`-based interval
    /// gate, which compounds into a spread_history divergence large
    /// enough to suppress sub-minute std collapses in replay.
    /// See bot-strategy#27 comment 2026-04-16.
    pub bt_eval_timestamps: Option<std::collections::HashSet<i64>>,
    /// Path to a file listing UNIX seconds at which the live bot was
    /// restarted (from `systemd` / `journalctl -u ... | grep Started`).
    /// In BT mode, when `now_ts` equals one of these, the engine fires
    /// `warm_start_states_from_history` once — re-computing `state.beta`
    /// via a fresh OLS over the current 240-bar `history` and re-seeding
    /// `spread_history` with 240 single-beta spreads. That is exactly
    /// what the live bot does at every service restart, and the
    /// low-variance seeded spread_history is the mechanism behind the
    /// 2026-04-15 06:02 UTC "std collapse" incident (bot-strategy#62 is
    /// now known to be a restart artifact, not a market regime break).
    /// Firing is one-shot per timestamp: each matched ts is removed
    /// from the set after firing.
    pub bt_restart_timestamps: Option<std::collections::HashSet<i64>>,
    /// Simulated fill delay for BT exit orders (seconds). In live mode,
    /// exit orders take 1-5s to fill on the exchange; during that window
    /// the position is still held and the bot cannot enter a new trade.
    /// In dry_run BT mode exits are instant, which lets BT enter slightly
    /// earlier than live and cascades into entry-count mismatches.
    /// When > 0, the dry_run exit path defers position clearing by this
    /// many replay-seconds, keeping the position "held" during the delay.
    /// Env: BT_FILL_DELAY_SECS (default 0 = legacy instant-fill).
    pub bt_fill_delay_secs: i64,
    /// All per-pair tunables — z-score thresholds, hedge gates, lookback
    /// windows, circuit-breaker tiers, Phase 2 filters — live here. Engine
    /// reads them via `params_for(key)` so per-pair YAML overrides win.
    /// Currently always empty (no production YAML sets per-pair overrides);
    /// kept as the per-pair extension point so re-introducing pair-level
    /// tuning does not require re-wiring the engine.
    pub pair_params: HashMap<String, PairParams>,
    pub default_pair_params: PairParams,
    /// Graceful shutdown: max seconds to wait for natural pair exit on SIGTERM
    /// before force-closing both legs. 0 = immediate force close (legacy).
    pub shutdown_grace_secs: u64,
    /// Resolved strategy variants. Always non-empty: legacy single-bot YAML
    /// produces a single entry derived from top-level scalars; new
    /// multi-strategy YAML produces N entries (shigeo-nakamura/bot-strategy#25).
    pub strategies: Vec<StrategyConfig>,
    // Kalman filter beta estimation (log-only, disabled by default)
    pub use_kalman_beta: bool,
    pub kalman_q: f64,
    pub kalman_r: f64,
    pub kalman_initial_p: f64,
    pub kalman_min_updates: u64,
    // Regime filter (disabled by default: thresholds 0.0 → filter inactive)
    pub regime_vol_window: usize,
    pub regime_vol_max: f64,
    pub regime_trend_window: usize,
    pub regime_trend_max: f64,
    pub regime_reference_symbol: String,
    // Daily drawdown limit (bot-strategy#185 Phase 2)
    pub risk: RiskConfig,
    /// Round identifier from YAML. Drives the round-boundary auto-reset
    /// in `load_risk_state` (bot-strategy#354). None = legacy mode.
    pub round_id: Option<String>,
}

impl PairTradeConfig {
    pub fn params_for(&self, pair_key: &str) -> &PairParams {
        self.pair_params
            .get(pair_key)
            .unwrap_or(&self.default_pair_params)
    }

    pub(super) fn slippage_cost_bps(&self) -> f64 {
        self.slippage_bps.max(0) as f64
    }

    pub(super) fn circuit_breaker_cooldown_for(&self, losses: u32) -> Option<Duration> {
        let dpp = &self.default_pair_params;
        // Graduated tiers (check tier2 first as higher threshold)
        if dpp.circuit_breaker_tier2_losses > 0 && losses >= dpp.circuit_breaker_tier2_losses {
            return Some(Duration::from_secs(dpp.circuit_breaker_tier2_cooldown_secs));
        }
        if dpp.circuit_breaker_tier1_losses > 0 && losses >= dpp.circuit_breaker_tier1_losses {
            return Some(Duration::from_secs(dpp.circuit_breaker_tier1_cooldown_secs));
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmStartMode {
    Strict,
    Relaxed,
}

impl std::str::FromStr for WarmStartMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strict" => Ok(WarmStartMode::Strict),
            "relaxed" => Ok(WarmStartMode::Relaxed),
            other => Err(format!("expected 'strict' or 'relaxed', got {:?}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::schema::RiskYaml;
    use super::*;

    #[test]
    fn risk_config_defaults_when_block_absent() {
        let cfg = resolve_risk_config(None).unwrap();
        assert_eq!(cfg.max_daily_loss_bps, 0);
        assert_eq!(cfg.max_session_loss_bps, 0);
        assert_eq!(cfg.max_notional_headroom, 0.0);
        assert!(matches!(cfg.max_daily_loss_action, DailyLossAction::Block));
    }

    #[test]
    fn risk_config_resolves_phase3_fields() {
        let yaml = RiskYaml {
            max_session_loss_bps: Some(500),
            session_dd_lookback_secs: Some(1_209_600), // 14 d
            session_dd_sample_secs: Some(1_800),       // 30 m
            max_notional_headroom: Some(1.1),
            ..RiskYaml::default()
        };
        let cfg = resolve_risk_config(Some(&yaml)).unwrap();
        assert_eq!(cfg.max_session_loss_bps, 500);
        assert_eq!(cfg.session_dd_lookback_secs, 1_209_600);
        assert_eq!(cfg.session_dd_sample_secs, 1_800);
        assert!((cfg.max_notional_headroom - 1.1).abs() < 1e-9);
    }

    #[test]
    fn risk_config_rejects_negative_headroom() {
        let yaml = RiskYaml {
            max_notional_headroom: Some(-1.0),
            ..RiskYaml::default()
        };
        assert!(resolve_risk_config(Some(&yaml)).is_err());
    }

    #[test]
    fn risk_config_rejects_headroom_that_looks_like_dollars() {
        // Old schema took an absolute USD cap (e.g. 5000). Catch operators
        // copy-pasting the old value into the new field name.
        let yaml = RiskYaml {
            max_notional_headroom: Some(5_000.0),
            ..RiskYaml::default()
        };
        assert!(resolve_risk_config(Some(&yaml)).is_err());
    }

    #[test]
    fn risk_config_rejects_zero_sample_cadence() {
        let yaml = RiskYaml {
            session_dd_sample_secs: Some(0),
            ..RiskYaml::default()
        };
        assert!(resolve_risk_config(Some(&yaml)).is_err());
    }

    #[test]
    fn risk_config_rejects_lookback_smaller_than_sample() {
        let yaml = RiskYaml {
            session_dd_sample_secs: Some(3_600),
            session_dd_lookback_secs: Some(60), // would never include even one sample
            ..RiskYaml::default()
        };
        assert!(resolve_risk_config(Some(&yaml)).is_err());
    }

    #[test]
    fn risk_config_still_rejects_phase3_flatten_action() {
        // Sanity check: Phase 3 plumbing didn't accidentally enable
        // `max_daily_loss_action: flatten` (kept as Phase-3 follow-up
        // separate from session DD halt; daily DD remains block-only).
        let yaml = RiskYaml {
            max_daily_loss_action: Some("flatten".to_string()),
            ..RiskYaml::default()
        };
        assert!(resolve_risk_config(Some(&yaml)).is_err());
    }

    #[test]
    fn history_archive_env_overrides_yaml() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_history_archive_env.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
history_archive_dir: /yaml/archive
history_archive_retention_days: 12
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let prev_dir = std::env::var("HISTORY_ARCHIVE_DIR").ok();
        let prev_retention = std::env::var("HISTORY_ARCHIVE_RETENTION_DAYS").ok();
        std::env::set_var("HISTORY_ARCHIVE_DIR", "/env/archive");
        std::env::set_var("HISTORY_ARCHIVE_RETENTION_DAYS", "34");

        let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
        assert_eq!(
            cfg.history_archive_dir.as_deref(),
            Some("/env/archive"),
            "env archive dir overrides yaml"
        );
        assert_eq!(cfg.history_archive_retention_days, 34);

        match prev_dir {
            Some(v) => std::env::set_var("HISTORY_ARCHIVE_DIR", v),
            None => std::env::remove_var("HISTORY_ARCHIVE_DIR"),
        }
        match prev_retention {
            Some(v) => std::env::set_var("HISTORY_ARCHIVE_RETENTION_DAYS", v),
            None => std::env::remove_var("HISTORY_ARCHIVE_RETENTION_DAYS"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn per_strategy_equity_env_override() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_per_strategy_equity_env.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
equity_usd_reference: 1000
strategies:
  - id: a
    equity_usd_reference: 1000
  - id: b
    equity_usd_reference: 500
  - id: c
    equity_usd_reference: 500
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let prev_a = std::env::var("EQUITY_REFERENCE_USD_A").ok();
        let prev_b = std::env::var("EQUITY_REFERENCE_USD_B").ok();
        let prev_c = std::env::var("EQUITY_REFERENCE_USD_C").ok();

        std::env::set_var("EQUITY_REFERENCE_USD_A", "250");
        std::env::set_var("EQUITY_REFERENCE_USD_B", "250");
        std::env::remove_var("EQUITY_REFERENCE_USD_C");

        let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
        let by_id = |id: &str| {
            cfg.strategies
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("missing strategy {id}"))
                .equity_reference_usd
        };
        assert!((by_id("a") - 250.0).abs() < 1e-9, "A env override applied");
        assert!((by_id("b") - 250.0).abs() < 1e-9, "B env override applied");
        assert!(
            (by_id("c") - 500.0).abs() < 1e-9,
            "C unset env falls through to yaml per-strategy value"
        );

        // Restore so other tests in the same process see clean state.
        match prev_a {
            Some(v) => std::env::set_var("EQUITY_REFERENCE_USD_A", v),
            None => std::env::remove_var("EQUITY_REFERENCE_USD_A"),
        }
        match prev_b {
            Some(v) => std::env::set_var("EQUITY_REFERENCE_USD_B", v),
            None => std::env::remove_var("EQUITY_REFERENCE_USD_B"),
        }
        if let Some(v) = prev_c {
            std::env::set_var("EQUITY_REFERENCE_USD_C", v);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn per_strategy_entry_z_override_resolves() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_per_strategy_entry_z.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
entry_z_score_base: 1.5
entry_z_score_min: 1.0
entry_z_score_max: 2.0
strategies:
  - id: a
  - id: c
    entry_z_score_base: 2.5
    entry_z_score_min: 2.0
    entry_z_score_max: 3.0
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
        let by_id = |id: &str| {
            cfg.strategies
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("missing strategy {id}"))
                .clone()
        };
        let a = by_id("a");
        assert!(a.entry_z_base.is_none(), "A inherits top-level (None)");
        assert!(a.entry_z_min.is_none());
        assert!(a.entry_z_max.is_none());
        let c = by_id("c");
        assert_eq!(c.entry_z_base, Some(2.5), "C overrides entry_z_base");
        assert_eq!(c.entry_z_min, Some(2.0));
        assert_eq!(c.entry_z_max, Some(3.0));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn per_strategy_std_collapse_hold_down_override_resolves() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_per_strategy_std_hold_down.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
std_collapse_hold_down_secs: 0
strategies:
  - id: a
  - id: c
    std_collapse_hold_down_secs: 3600
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");
        assert_eq!(cfg.default_pair_params.std_collapse_hold_down_secs, 0);

        let by_id = |id: &str| {
            cfg.strategies
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("missing strategy {id}"))
                .clone()
        };

        assert_eq!(by_id("c").std_collapse_hold_down_secs, Some(3600));
        assert!(by_id("a").std_collapse_hold_down_secs.is_none());

        let global = cfg.default_pair_params.std_collapse_hold_down_secs;
        let resolved = |id: &str| by_id(id).std_collapse_hold_down_secs.unwrap_or(global);
        assert_eq!(resolved("a"), 0);
        assert_eq!(resolved("c"), 3600);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn per_strategy_regime_block_entries_override_resolves() {
        // bot-strategy#494 Phase 1: on the single-process A/B/C layout, a single
        // challenger must be able to opt into the regime entry-gate while the
        // control variants stay on the global default (false). This guards the
        // 4-site plumbing (StrategyYaml -> StrategyConfig -> mod.rs overlay)
        // against the silent-global-inherit trap (memory: strategy_yaml_silent_drop).
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_per_strategy_regime_block.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
strategies:
  - id: a
  - id: b
  - id: c
    regime_block_entries: true
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();

        let cfg = PairTradeConfig::from_yaml_path(&path).expect("yaml load");

        // Top-level default stays false (shadow-only) when no global override set.
        assert!(
            !cfg.default_pair_params.regime_block_entries,
            "global default must be false (shadow-only)"
        );

        let by_id = |id: &str| {
            cfg.strategies
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("missing strategy {id}"))
                .clone()
        };

        // Only the challenger carries the per-strategy override; controls inherit.
        assert_eq!(
            by_id("c").regime_block_entries,
            Some(true),
            "C opts in via per-strategy override"
        );
        assert!(
            by_id("a").regime_block_entries.is_none(),
            "A inherits the global default (None at the override layer)"
        );
        assert!(
            by_id("b").regime_block_entries.is_none(),
            "B inherits the global default (None at the override layer)"
        );

        // Reproduce the mod.rs overlay resolution to assert the final per-variant
        // boolean: C blocks while A/B remain false.
        let global = cfg.default_pair_params.regime_block_entries;
        let resolved = |id: &str| by_id(id).regime_block_entries.unwrap_or(global);
        assert!(resolved("c"), "C resolves to regime_block_entries = true");
        assert!(!resolved("a"), "A resolves to false (control)");
        assert!(!resolved("b"), "B resolves to false (control)");

        let _ = std::fs::remove_file(&path);
    }
}
