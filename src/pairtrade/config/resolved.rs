use std::collections::HashMap;
use std::time::Duration;

use super::{PairParams, PairSpec, RiskConfig, StrategyConfig};

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
    /// Path for a per-tick regime-detector series dump (CSV). When set,
    /// every shared-tick regime update appends one row of
    /// `ts,key,innovation,beta,scale,norm,cusum,active`. Calibration aid
    /// for bot-strategy#534/#494: the 300s `[REGIME_SHADOW]` cadence is
    /// too coarse to rebuild alternative (e.g. dual-timescale) statistics
    /// offline, which needs the raw innovation at every tick.
    /// Env: BT_REGIME_SERIES_FILE.
    pub bt_regime_series_file: Option<String>,
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

    pub(in crate::pairtrade) fn slippage_cost_bps(&self) -> f64 {
        self.slippage_bps.max(0) as f64
    }

    pub(in crate::pairtrade) fn circuit_breaker_cooldown_for(
        &self,
        losses: u32,
    ) -> Option<Duration> {
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
