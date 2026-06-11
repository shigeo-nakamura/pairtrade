//! Pairtrade configuration: resolved shapes and the env/YAML →
//! resolved-config builder. The raw YAML deserialization schema types live
//! in the `schema` submodule (bot-strategy#502).

mod env_overrides;
mod env_util;
mod from_env;
mod from_yaml;
mod params;
mod risk;
mod schema;
mod strategy;
mod universe;
mod validate;

pub use params::PairParams;
#[cfg(test)]
use risk::resolve_risk_config;
pub use risk::{DailyLossAction, RiskConfig};
use std::collections::HashMap;
use std::time::Duration;
pub use strategy::StrategyConfig;

use serde::Deserialize;

pub use universe::PairSpec;

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
