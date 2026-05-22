use anyhow::{Context, Result};
use dex_connector::{DexConnector, PositionSnapshot};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;

use crate::ports::replay_dex::ReplayConnector;

mod backtest;
mod bar;
mod config;
mod data_dump;
mod defaults;
mod engine;
mod entry;
mod exit;
mod funding_history;
mod history_io;
mod kalman;
mod market;
mod order_pricing;
mod pair_eval;
mod pnl_log;
pub(crate) mod prom;
mod regime;
mod rehedge;
mod risk_io;
mod s3_mirror;
mod sizing;
mod state;
mod stats;
mod status;
mod util;
use bar::BarBuilder;
pub use config::{PairTradeConfig, WarmStartMode};
use market::SymbolSnapshot;
use pnl_log::{PnlLogRecord, PnlLogger};
use stats::PriceSample;

/// Spawn the Prometheus exporter when `PROM_LISTEN` is set in the env.
/// Safe to call at most once at process boot from `main`. See
/// `pairtrade::prom`.
pub fn start_metrics_exporter() {
    prom::maybe_start_exporter();
}
use config::PairParams;
#[cfg(test)]
use config::PairSpec;
#[cfg(test)]
use defaults::*;
use engine::risk::risk_state_path_for;
use state::{PairSharedState, PairState, PositionDirection};
#[cfg(test)]
use state::{PendingLeg, PendingOrders};
use status::StatusReporter;
use util::{enforce_post_only_passive, round_price_by_tick, tail_std};

/// Max age of the per-instance equity cache before `refresh_equity_if_needed`
/// fetches a fresh value from the exchange. Now a low-frequency dashboard tick:
/// exit/loss-cut uses locally-computed PnL from WS prices, so `equity_cache`
/// only scales the slowly-drifting R-budget and feeds the status reporter.
/// Entry sizing fetches inline (see `fetch_equity_rest` call in the entry
/// branch of `step()`), which after dex-connector v4.2.83 is a WS-derived
/// cache hit in steady state. See bot-strategy#156, #239.
const EQUITY_REFRESH_CACHE_SECS: u64 = 300;

/// Sentinel file that, when present, blocks all new entries without
/// requiring `systemctl stop`. Existing positions still exit normally.
/// Manage via `ssh debot "sudo touch /opt/debot/KILL_SWITCH"` to engage
/// and `sudo rm /opt/debot/KILL_SWITCH` to release. Engages at the top
/// of every `step_shared` tick, so reaction latency matches
/// `interval_secs`. See bot-strategy#185 Phase 1-2.
const KILL_SWITCH_PATH: &str = "/opt/debot/KILL_SWITCH";

/// Manual-ack sentinel for clearing a session-DD halt (Phase 3-2). Drop
/// this file (any contents) on the host to lift the halt; the bot
/// consumes it at the top of `step_shared` so the file is removed even
/// if all instances were already clear. See bot-strategy#185 Phase 3-2.
const RISK_ACK_PATH: &str = "/opt/debot/RISK_ACK";

/// Apply the post-exit state transition: clear position, stamp
/// last_exit_{at,ts}, and — if the exit reason stashed by `exit_reason()`
/// is `stop_loss_z` — set `last_stop_loss_at` so the per-direction
/// post-stop cool-down (`stop_loss_cooldown_secs`) blocks an immediate
/// same-direction re-entry. The tag is dropped after consumption so a
/// later non-stop exit (force_close / exit_z) does not refresh it.
/// bot-strategy#316.
pub(in crate::pairtrade) fn apply_post_exit_state(
    state: &mut PairState,
    shared: Option<&PairSharedState>,
    direction: PositionDirection,
    now_ts: i64,
    variant: &str,
    pair: &str,
) {
    state.position = None;
    state.last_exit_at = Some(Instant::now());
    state.last_exit_ts = Some(now_ts);
    // Capture the z observed at exit before the post-take pending_exit_reason
    // is consumed, so the Grafana panel can correlate close reason with the
    // z that triggered it. bot-strategy#409.
    if let Some((z, _)) = shared.and_then(|s| s.z_score()) {
        prom::LAST_EXIT_Z.with_label_values(&[variant, pair]).set(z);
    }
    let reason = state.pending_exit_reason.take().unwrap_or("unknown");
    prom::CLOSE_REASON_TOTAL
        .with_label_values(&[variant, pair, reason])
        .inc();
    if reason == "stop_loss_z" {
        state.last_stop_loss_at = Some((direction, now_ts));
        log::info!(
            "[STOP_COOLDOWN] armed direction={:?} now_ts={}",
            direction,
            now_ts
        );
    }
}

struct StrategyInstance {
    #[allow(dead_code)]
    id: String,
    /// Per-strategy connector. For single-instance deployments this is the
    /// same `Arc` as `PairTradeEngine.connector`. For multi-strategy
    /// deployments each instance owns its own connector pointing at its
    /// sub-account credentials.
    #[allow(dead_code)]
    connector: Arc<dyn DexConnector + Send + Sync>,
    /// Per-instance live equity from the instance's connector.
    equity_cache: f64,
    last_equity_fetch: Option<Instant>,
    /// False until the first successful `fetch_equity_rest` writes a
    /// connector-sourced balance into `equity_cache`. Not persisted —
    /// always starts false on engine boot. Gates session-DD evaluation
    /// and equity sampling so a restart whose `equity_samples` deque
    /// already holds a real peak does not trip a phantom halt against
    /// the stale `equity_reference_usd` seed before the first WS
    /// account dump propagates. See bot-strategy#366.
    equity_initialized: bool,
    /// Per-strategy fixed equity reference from the YAML
    /// `equity_usd_reference`. Used as the base for risk thresholds
    /// (daily DD, exit risk_budget) AND position sizing so each
    /// variant operates against its own declared capital. Revised
    /// manually at the same monthly cadence as A/B/C parameter updates;
    /// `equity_cache` is kept separately for live monitoring only and
    /// is no longer mixed into the threshold/sizing math. See
    /// bot-strategy#222.
    equity_reference_usd: f64,
    states: HashMap<String, PairState>,
    pnl_logger: Option<PnlLogger>,
    status_reporter: Option<StatusReporter>,
    consecutive_losses: u32,
    circuit_breaker_until: Option<Instant>,
    /// Replay-aware companion to `circuit_breaker_until`. Compared against
    /// the per-step `now_ts` so backtest replays can honour the same
    /// cool-down logic as live.
    circuit_breaker_until_ts: Option<i64>,
    /// Daily-DD tracking (bot-strategy#185 Phase 2). Zero/None until the
    /// first `refresh_daily_session` reset populates them.
    session_start_equity: f64,
    session_start_ts: i64,
    realized_pnl_today: f64,
    /// Running sum of `funding_carry_usd` from cycles closed during the
    /// current UTC session. Updated at exit_fill / exit_dry_run when the
    /// cycle's funding carry was measured (`with_funding(...)` was called
    /// on the PnlLogRecord). Reset at the same session rollover as
    /// `realized_pnl_today`. Persisted via `InstanceRiskState` so it
    /// survives restarts within a single UTC day. Surfaced on
    /// status.json as `funding_carry_today` for dashboard attribution.
    /// bot-strategy#371.
    funding_carry_today: f64,
    /// True once `realized_pnl_today` has breached
    /// `max_daily_loss_bps`. Used for transition logging only
    /// (activate/clear); the live gate check is recomputed every tick
    /// from current state via `daily_loss_blocks`.
    daily_loss_halted: bool,
    /// Phase 3-1 rolling peak equity samples. Append-only at
    /// `risk.session_dd_sample_secs` cadence; entries older than
    /// `risk.session_dd_lookback_secs` are pruned in-place.
    equity_samples: Vec<risk_io::EquitySample>,
    /// Phase 3-1/3-2 sticky halt set on session-DD breach. Persists to
    /// `risk_state.json` so a crash inside the cool-off window does
    /// not silently re-arm the bot. Cleared only by writing
    /// `/opt/debot/RISK_ACK`.
    session_halted: bool,
    session_halt_reason: Option<String>,
    session_halt_ts: Option<i64>,
    total_trades: u64,
    total_wins: u64,
    total_pnl: f64,
    peak_pnl: f64,
    max_dd: f64,
    /// Per-instance pair parameter overrides. Built at `new_inner` time by
    /// overlaying the strategy's `exit_z` / `stop_loss_z` / `max_loss_r_mult`
    /// on top of the engine-wide defaults. Look up via
    /// `PairTradeEngine::pair_params_for(inst_idx, key)`.
    pair_params: HashMap<String, PairParams>,
    default_pair_params: PairParams,
}

pub struct PairTradeEngine {
    cfg: PairTradeConfig,
    connector: Arc<dyn DexConnector + Send + Sync>,
    instances: Vec<StrategyInstance>,
    history: HashMap<String, VecDeque<PriceSample>>,
    /// Per-pair quantities (β / spread / z / Kalman / eval result) shared
    /// across every `StrategyInstance` on the same pair. Computed exactly
    /// once per tick in `step_pair_shared`, so A/B/C variants observe
    /// byte-identical β / std / z. See bot-strategy#413.
    per_pair_state: HashMap<String, PairSharedState>,
    bar_builders: HashMap<String, BarBuilder>,
    last_metrics_log: Option<Instant>,
    last_ob_warn: HashMap<String, Instant>,
    last_ticker_warn: HashMap<String, Instant>,
    last_position_warn: HashMap<String, Instant>,
    min_order_warned: HashSet<String>,
    min_tick_warned: HashSet<String>,
    positions_ready: bool,
    open_positions: HashMap<String, PositionSnapshot>,
    history_path: PathBuf,
    /// Path for the risk-state persistence file (circuit breaker counters
    /// + cool-down deadline). Sibling of `history_path`. See bot-strategy#185.
    risk_state_path: PathBuf,
    /// Cached result of the most recent `KILL_SWITCH_PATH` existence check.
    /// Refreshed at the top of every `step_shared` tick. True blocks new
    /// entries across all instances.
    kill_switch_active: bool,
    data_dump_writer: Option<data_dump::RotatingDumpWriter>,
    replay_connector: Option<Arc<ReplayConnector>>,
    /// Rolling per-symbol funding-rate history observed from WS, used by
    /// `exit_fill` to compute `funding_carry_usd` on each cycle without an
    /// external REST fetch. bot-strategy#364.
    funding_history: funding_history::FundingHistory,
    /// Graceful shutdown flag. When true:
    ///   - new entries are blocked
    ///   - existing exit logic (exit_z / stop_loss_z / force_close_secs) runs normally
    ///   - live loop exits as soon as open_positions is empty, or after shutdown_grace_secs
    shutdown_pending: bool,
    /// Recent bar-emit timestamps per symbol, for the [BAR_RATE] canary
    /// (bot-strategy#341). Trimmed to the trailing 120 s; the canary warns
    /// if sustained < 0.8 emits/min, which would have caught the original
    /// Phase 2 β-freeze in <30 min instead of 78 h.
    bar_emit_log: HashMap<String, VecDeque<Instant>>,
    /// Last time a [BAR_RATE] WARN fired per symbol, used to rate-limit
    /// the warning to ~once per minute so a sustained low rate doesn't
    /// flood the journal.
    last_bar_rate_warn: HashMap<String, Instant>,
    /// Fingerprint of the most recently emitted `[WARM_START] snapshot
    /// loaded ...` INFO line. `load_history_from_disk` runs on every
    /// polling tick (engine/step.rs:511), so a naive INFO emit fires
    /// ~12×/min on the typical 5 s polling cadence. We dedup on this
    /// key so an operator rolling back a snapshot still sees the
    /// "loaded" line in journalctl (content changes → key differs →
    /// emit) while steady-state per-tick reloads stay quiet. WARN
    /// paths (stale-guard, parse-error, partial) are always emitted.
    last_warm_start_key: Option<String>,
}

impl PairTradeEngine {
    /// Create a new engine with a pre-loaded ReplayConnector (for batch mode).
    pub async fn new_with_replay(
        cfg: PairTradeConfig,
        replay: Arc<ReplayConnector>,
    ) -> Result<Self> {
        replay.reset();
        let primary: Arc<dyn DexConnector + Send + Sync> = replay.clone();
        let n = cfg.strategies.len().max(1);
        let instance_connectors = std::iter::repeat_n(primary.clone(), n).collect();
        Self::new_inner(cfg, primary, instance_connectors, Some(replay)).await
    }

    pub async fn new(cfg: PairTradeConfig) -> Result<Self> {
        let (connector, instance_connectors, replay_connector) =
            backtest::create_connector(&cfg).await?;
        Self::new_inner(cfg, connector, instance_connectors, replay_connector).await
    }

    async fn new_inner(
        cfg: PairTradeConfig,
        connector: Arc<dyn DexConnector + Send + Sync>,
        instance_connectors: Vec<Arc<dyn DexConnector + Send + Sync>>,
        replay_connector: Option<Arc<ReplayConnector>>,
    ) -> Result<Self> {
        let mut history = HashMap::new();
        let mut bar_builders = HashMap::new();
        let mut per_pair_state: HashMap<String, PairSharedState> = HashMap::new();
        for pair in &cfg.universe {
            history.insert(pair.base.clone(), VecDeque::new());
            history.insert(pair.quote.clone(), VecDeque::new());
            bar_builders.insert(pair.base.clone(), BarBuilder::new(cfg.trading_period_secs));
            bar_builders.insert(pair.quote.clone(), BarBuilder::new(cfg.trading_period_secs));
            let pair_key = format!("{}/{}", pair.base, pair.quote);
            let mut shared = PairSharedState::new(cfg.metrics_window);
            if cfg.use_kalman_beta {
                shared.kalman = Some(kalman::KalmanBeta::new(
                    1.0,
                    cfg.kalman_initial_p,
                    cfg.kalman_q,
                    cfg.kalman_r,
                ));
            }
            per_pair_state.insert(pair_key, shared);
        }

        let history_path = PathBuf::from(cfg.history_file.as_str());
        let risk_state_path = risk_state_path_for(&history_path);

        let min_order_warned = HashSet::new();
        let min_tick_warned = HashSet::new();
        let data_dump_writer = if cfg.enable_data_dump {
            let file_path = cfg.data_dump_file.as_ref().unwrap(); // is_none checked in from_env
            Some(data_dump::RotatingDumpWriter::new(file_path)?)
        } else {
            None
        };

        let backtest_mode = cfg.backtest_mode;
        let _ = backtest_mode;
        let multi_instance = cfg.strategies.len() > 1;

        // Build one StrategyInstance per entry in cfg.strategies. For legacy
        // single-strategy YAML this is exactly one instance whose parameters
        // match today's behavior (golden-test stable). For multi-strategy
        // YAML this produces N instances that share the engine's history /
        // bar_builders but each hold their own pair_params overlay,
        // connector, PnL log, and status reporter.
        let mut built_instances: Vec<StrategyInstance> = Vec::new();
        let strategies = cfg.strategies.clone();
        for (i, strategy) in strategies.iter().enumerate() {
            // Overlay per-strategy exit_z / stop_loss_z / max_loss_r_mult on
            // top of the engine's default_pair_params and per-pair overrides
            // so each variant evaluates z-exits at its own thresholds.
            let mut inst_default = cfg.default_pair_params.clone();
            inst_default.exit_z = strategy.exit_z;
            inst_default.stop_loss_z = strategy.stop_loss_z;
            inst_default.max_loss_r_mult = strategy.max_loss_r_mult;
            if let Some(fc) = strategy.force_close_time_secs {
                inst_default.force_close_secs = fc;
            }
            if let Some(ref w) = strategy.mtf_windows {
                inst_default.mtf_windows = w.clone();
            }
            if let Some(z) = strategy.mtf_z_min {
                inst_default.mtf_z_min = z;
            }
            if let Some(z) = strategy.entry_z_base {
                inst_default.entry_z_base = z;
            }
            if let Some(z) = strategy.entry_z_min {
                inst_default.entry_z_min = z;
            }
            if let Some(z) = strategy.entry_z_max {
                inst_default.entry_z_max = z;
            }
            if let Some(v) = strategy.beta_gap_entry_z_scale {
                inst_default.beta_gap_entry_z_scale = v;
            }
            if let Some(v) = strategy.beta_gap_notional_scale {
                inst_default.beta_gap_notional_scale = v;
            }
            if let Some(v) = strategy.beta_gap_notional_floor {
                inst_default.beta_gap_notional_floor = v;
            }
            if let Some(v) = strategy.rehedge_drift_threshold_pct {
                inst_default.rehedge_drift_threshold_pct = v;
            }
            if let Some(v) = strategy.rehedge_cooldown_secs {
                inst_default.rehedge_cooldown_secs = v;
            }
            if let Some(v) = strategy.rehedge_min_qty_notional_usd {
                inst_default.rehedge_min_qty_notional_usd = v;
            }
            if let Some(v) = strategy.rehedge_live_enabled {
                inst_default.rehedge_live_enabled = v;
            }
            if let Some(v) = strategy.rehedge_require_no_revert {
                inst_default.rehedge_require_no_revert = v;
            }
            if let Some(v) = strategy.rehedge_z_no_revert_factor {
                inst_default.rehedge_z_no_revert_factor = v;
            }
            if let Some(v) = strategy.rehedge_velocity_projected_drift_min {
                inst_default.rehedge_velocity_projected_drift_min = v;
            }
            if let Some(v) = strategy.beta_uncertainty_max {
                inst_default.beta_uncertainty_max = v;
            }

            let mut inst_pair_params: HashMap<String, PairParams> = HashMap::new();
            for (k, v) in cfg.pair_params.iter() {
                let mut pp = v.clone();
                pp.exit_z = strategy.exit_z;
                pp.stop_loss_z = strategy.stop_loss_z;
                pp.max_loss_r_mult = strategy.max_loss_r_mult;
                if let Some(fc) = strategy.force_close_time_secs {
                    pp.force_close_secs = fc;
                }
                if let Some(ref w) = strategy.mtf_windows {
                    pp.mtf_windows = w.clone();
                }
                if let Some(z) = strategy.mtf_z_min {
                    pp.mtf_z_min = z;
                }
                if let Some(z) = strategy.entry_z_base {
                    pp.entry_z_base = z;
                }
                if let Some(z) = strategy.entry_z_min {
                    pp.entry_z_min = z;
                }
                if let Some(z) = strategy.entry_z_max {
                    pp.entry_z_max = z;
                }
                if let Some(v) = strategy.beta_gap_entry_z_scale {
                    pp.beta_gap_entry_z_scale = v;
                }
                if let Some(v) = strategy.beta_gap_notional_scale {
                    pp.beta_gap_notional_scale = v;
                }
                if let Some(v) = strategy.beta_gap_notional_floor {
                    pp.beta_gap_notional_floor = v;
                }
                if let Some(v) = strategy.rehedge_drift_threshold_pct {
                    pp.rehedge_drift_threshold_pct = v;
                }
                if let Some(v) = strategy.rehedge_cooldown_secs {
                    pp.rehedge_cooldown_secs = v;
                }
                if let Some(v) = strategy.rehedge_min_qty_notional_usd {
                    pp.rehedge_min_qty_notional_usd = v;
                }
                if let Some(v) = strategy.rehedge_live_enabled {
                    pp.rehedge_live_enabled = v;
                }
                if let Some(v) = strategy.rehedge_require_no_revert {
                    pp.rehedge_require_no_revert = v;
                }
                if let Some(v) = strategy.rehedge_z_no_revert_factor {
                    pp.rehedge_z_no_revert_factor = v;
                }
                if let Some(v) = strategy.rehedge_velocity_projected_drift_min {
                    pp.rehedge_velocity_projected_drift_min = v;
                }
                if let Some(v) = strategy.beta_uncertainty_max {
                    pp.beta_uncertainty_max = v;
                }
                inst_pair_params.insert(k.clone(), pp);
            }

            let mut states = HashMap::new();
            for pair in &cfg.universe {
                let pair_key = format!("{}/{}", pair.base, pair.quote);
                let pp = inst_pair_params.get(&pair_key).unwrap_or(&inst_default);
                let ps = PairState::new(pp.entry_z_base);
                prom::init_close_reason_series(&strategy.id, &pair_key);
                prom::init_entry_reject_series(&strategy.id, &pair_key);
                states.insert(pair_key, ps);
            }

            let instance_connector = instance_connectors
                .get(i)
                .cloned()
                .unwrap_or_else(|| connector.clone());
            let pnl_logger = PnlLogger::from_env_for_instance(&cfg, &strategy.id, multi_instance);
            let status_reporter =
                StatusReporter::from_env_for_instance(&cfg, &strategy.id, multi_instance);

            // Stagger the per-instance equity-refresh cycle so N instances
            // don't all hit `/account` inside the same 5-min expiry boundary.
            // Each instance is phase-shifted by i * (CACHE_SECS / N) so over
            // a 5-min window the N calls are spread evenly (~100s apart for
            // N=3) instead of back-to-back. Avoids Lighter's short-window
            // 429 on the burst head (bot-strategy#142).
            let instance_count = strategies.len();
            let last_equity_fetch = if i == 0 || instance_count <= 1 {
                None
            } else {
                let offset_secs = (EQUITY_REFRESH_CACHE_SECS * i as u64) / instance_count as u64;
                let phase = EQUITY_REFRESH_CACHE_SECS.saturating_sub(offset_secs);
                Some(Instant::now() - Duration::from_secs(phase))
            };

            built_instances.push(StrategyInstance {
                id: strategy.id.clone(),
                connector: instance_connector,
                equity_cache: strategy.equity_reference_usd,
                last_equity_fetch,
                equity_initialized: false,
                equity_reference_usd: strategy.equity_reference_usd,
                states,
                pnl_logger,
                status_reporter,
                consecutive_losses: 0,
                circuit_breaker_until: None,
                circuit_breaker_until_ts: None,
                session_start_equity: 0.0,
                session_start_ts: 0,
                realized_pnl_today: 0.0,
                funding_carry_today: 0.0,
                daily_loss_halted: false,
                equity_samples: Vec::new(),
                session_halted: false,
                session_halt_reason: None,
                session_halt_ts: None,
                total_trades: 0,
                total_wins: 0,
                total_pnl: 0.0,
                peak_pnl: 0.0,
                max_dd: 0.0,
                pair_params: inst_pair_params,
                default_pair_params: inst_default,
            });
        }

        // Stamp Prometheus process-info gauges once per variant so the
        // `pairtrade_bot_version_info` series shows up on /metrics even
        // before the first tick. bot-strategy#409.
        let process_started = status::process_started_at();
        for inst in &built_instances {
            prom::record_process_info(inst.id.as_str(), process_started);
        }

        Ok(Self {
            cfg,
            connector,
            replay_connector,
            instances: built_instances,
            history,
            per_pair_state,
            bar_builders,
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned,
            min_tick_warned,
            positions_ready: backtest_mode,
            open_positions: HashMap::new(),
            history_path,
            risk_state_path,
            kill_switch_active: false,
            data_dump_writer,
            funding_history: funding_history::FundingHistory::new(),
            shutdown_pending: false,
            bar_emit_log: HashMap::new(),
            last_bar_rate_warn: HashMap::new(),
            last_warm_start_key: None,
        })
    }

    /// Whether every strategy instance is currently flat. For single-instance
    /// deployments this is exactly today's `self.open_positions.is_empty()`
    /// check (golden-test stable). For multi-instance deployments this also
    /// requires every per-pair `state.position` across every instance to be
    /// `None`, so SIGTERM waits for all A/B/C variants to flatten before
    /// exiting. commit 5 of shigeo-nakamura/bot-strategy#25.
    fn all_instances_flat(&self) -> bool {
        if self.instances.len() <= 1 {
            return self.open_positions.is_empty();
        }
        self.open_positions.is_empty()
            && self
                .instances
                .iter()
                .all(|inst| inst.states.values().all(|s| s.position.is_none()))
    }

    /// Total open-position count surfaced in shutdown log lines. Mirrors
    /// `all_instances_flat`'s split: single-instance returns today's count,
    /// multi-instance sums per-pair `state.position` presence across all
    /// instances so the log reflects everything graceful shutdown is
    /// waiting on.
    fn total_open_positions(&self) -> usize {
        if self.instances.len() <= 1 {
            return self.open_positions.len();
        }
        let from_states: usize = self
            .instances
            .iter()
            .map(|inst| {
                inst.states
                    .values()
                    .filter(|s| s.position.is_some())
                    .count()
            })
            .sum();
        from_states.max(self.open_positions.len())
    }

    /// Return the per-instance `PairParams` for a pair key, falling back to
    /// the instance's `default_pair_params` when the pair has no override.
    /// Use this inside any per-instance phase in place of
    /// `self.cfg.params_for(key)` so each variant sees its own
    /// `exit_z` / `stop_loss_z` / `max_loss_r_mult`.
    fn pair_params_for(&self, inst_idx: usize, key: &str) -> &PairParams {
        let inst = &self.instances[inst_idx];
        inst.pair_params
            .get(key)
            .unwrap_or(&inst.default_pair_params)
    }

    fn write_pnl_record(&mut self, inst_idx: usize, record: PnlLogRecord) {
        // Update trade stats
        self.instances[inst_idx].total_trades += 1;
        self.instances[inst_idx].total_pnl += record.pnl;
        if record.pnl > 0.0 {
            self.instances[inst_idx].total_wins += 1;
        }
        if self.instances[inst_idx].total_pnl > self.instances[inst_idx].peak_pnl {
            self.instances[inst_idx].peak_pnl = self.instances[inst_idx].total_pnl;
        }
        let dd = self.instances[inst_idx].peak_pnl - self.instances[inst_idx].total_pnl;
        if dd > self.instances[inst_idx].max_dd {
            self.instances[inst_idx].max_dd = dd;
        }

        // Update status reporter
        let inst = &mut self.instances[inst_idx];
        if let Some(reporter) = &mut inst.status_reporter {
            reporter.set_trade_stats_totals(
                inst.total_trades,
                inst.total_wins,
                inst.total_pnl,
                inst.max_dd,
            );
        }

        if let Some(logger) = &mut self.instances[inst_idx].pnl_logger {
            if let Err(err) = logger.log(record) {
                log::warn!("[PNL] failed to write pnl log: {:?}", err);
            }
        }
    }

    fn latest_log_price(&self, symbol: &str) -> Option<f64> {
        self.history
            .get(symbol)
            .and_then(|h| h.back())
            .map(|p| p.log_price)
    }

    fn clear_stale_pending(&mut self, inst_idx: usize, max_age: Duration, reason: &str) {
        let now_ts = self.current_now_ts();
        for (key, state) in self.instances[inst_idx].states.iter_mut() {
            let entry_age = state.pending_entry.as_ref().map(|p| p.placed_at.elapsed());
            let exit_age = state.pending_exit.as_ref().map(|p| p.placed_at.elapsed());
            let age = match (entry_age, exit_age) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            if let Some(age) = age {
                if age >= max_age {
                    log::warn!(
                        "[POSITION] {} pending cleared (reason={}, age={}s)",
                        key,
                        reason,
                        age.as_secs()
                    );
                    state.pending_entry = None;
                    state.pending_exit = None;
                    state.position = None;
                    state.position_guard = false;
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                }
            }
        }
    }

    fn compute_vol_median(&self) -> f64 {
        let tail_len = self.entry_vol_window();
        let mut vols: Vec<f64> = self
            .per_pair_state
            .values()
            .filter_map(|s| tail_std(&s.spread_history, tail_len))
            .collect();
        if vols.is_empty() {
            return 1.0;
        }
        vols.sort_by(|a, b| a.partial_cmp(b).unwrap());
        vols[vols.len() / 2].max(1e-9)
    }

    fn maybe_log_metrics(&mut self, inst_idx: usize) {
        // Refresh Prometheus gauges every tick. Cheap (a handful of
        // atomic stores per pair) and gives Grafana the same resolution as
        // the underlying state instead of the 300 s log cadence. See
        // bot-strategy#409.
        self.update_prom_metrics(inst_idx);

        const LOG_INTERVAL: u64 = 300;
        if self
            .last_metrics_log
            .map(|t| t.elapsed() < Duration::from_secs(LOG_INTERVAL))
            .unwrap_or(false)
        {
            return;
        }
        let mut lines = Vec::new();
        for k in self.instances[inst_idx].states.keys() {
            let Some(shared) = self.per_pair_state.get(k) else {
                continue;
            };
            let z = shared.z_score().map(|(z, _)| z).unwrap_or(0.0);
            lines.push(format!(
                "{} elig={} z={:.2} beta={:.2} hl={:.2}h p={:.3}",
                k, shared.eligible, z, shared.beta, shared.half_life_hours, shared.adf_p_value
            ));
        }
        lines.sort();
        if !lines.is_empty() {
            log::info!("[METRICS] {}", lines.join(" | "));
        }
        self.last_metrics_log = Some(Instant::now());
    }

    /// Push per-instance signal / position / risk state into the
    /// Prometheus registry. Bot-strategy#409 — meant for at-a-glance
    /// "how close are we to entry?" and "is anything blocking entry?"
    /// reading, not a byte-exact predicate.
    fn update_prom_metrics(&self, inst_idx: usize) {
        let inst = &self.instances[inst_idx];
        let instance = inst.id.as_str();
        let now_ts = chrono::Utc::now().timestamp();
        // --- per-pair gauges ---
        for (key, state) in &inst.states {
            let pp = inst
                .pair_params
                .get(key)
                .unwrap_or(&inst.default_pair_params);
            let shared = self.per_pair_state.get(key);
            let z = shared
                .and_then(|s| s.z_score().map(|(z, _)| z))
                .unwrap_or(0.0);
            let labels = [instance, key.as_str()];
            prom::Z.with_label_values(&labels).set(z);
            prom::BETA
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta).unwrap_or(1.0));
            prom::BETA_S
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta_short).unwrap_or(1.0));
            prom::BETA_L
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta_long).unwrap_or(1.0));
            prom::BETA_DIVERGENCE.with_label_values(&labels).set(
                shared
                    .map(|s| (s.beta_short - s.beta_long).abs())
                    .unwrap_or(0.0),
            );
            prom::BETA_GAP_RELATIVE
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta_gap).unwrap_or(0.0));
            prom::BETA_UNCERTAINTY
                .with_label_values(&labels)
                .set(shared.and_then(|s| s.kalman.as_ref().map(|k| k.posterior_std())).unwrap_or(0.0));
            prom::HALF_LIFE_HOURS
                .with_label_values(&labels)
                .set(shared.map(|s| s.half_life_hours).unwrap_or(0.0));
            prom::ADF_PVALUE
                .with_label_values(&labels)
                .set(shared.map(|s| s.adf_p_value).unwrap_or(1.0));
            prom::ELIGIBLE.with_label_values(&labels).set(
                if shared.map(|s| s.eligible).unwrap_or(false) {
                    1
                } else {
                    0
                },
            );
            let mut effective = state.z_entry;
            if pp.beta_gap_entry_z_scale > 0.0 {
                effective *=
                    1.0 + pp.beta_gap_entry_z_scale * shared.map(|s| s.beta_gap).unwrap_or(0.0);
            }
            prom::ENTRY_Z_THRESHOLD_EFFECTIVE
                .with_label_values(&labels)
                .set(effective);
            if let Some(pos) = state.position.as_ref() {
                prom::HAS_POSITION.with_label_values(&labels).set(1);
                prom::POSITION_AGE_SECONDS
                    .with_label_values(&labels)
                    .set((now_ts - pos.entered_ts).max(0) as f64);
                if let Some(ez) = pos.entry_z {
                    prom::LAST_ENTRY_Z.with_label_values(&labels).set(ez);
                }
            } else {
                prom::HAS_POSITION.with_label_values(&labels).set(0);
                prom::POSITION_AGE_SECONDS
                    .with_label_values(&labels)
                    .set(0.0);
            }
            let since_exit = match state.last_exit_ts {
                Some(ts) => (now_ts - ts).max(0) as f64,
                None => -1.0,
            };
            prom::TIME_SINCE_LAST_TRADE_SECONDS
                .with_label_values(&labels)
                .set(since_exit);
        }
        // --- per-instance scalars ---
        prom::KILL_SWITCH_ACTIVE
            .with_label_values(&[instance])
            .set(if self.kill_switch_active { 1 } else { 0 });
        prom::SESSION_DD_HALT_ACTIVE
            .with_label_values(&[instance])
            .set(if inst.session_halted { 1 } else { 0 });
        prom::DAILY_DD_HALT_ACTIVE
            .with_label_values(&[instance])
            .set(if inst.daily_loss_halted { 1 } else { 0 });
        let cb_active = match inst.circuit_breaker_until_ts {
            Some(until) => until > now_ts,
            None => false,
        };
        prom::CIRCUIT_BREAKER_ACTIVE
            .with_label_values(&[instance])
            .set(if cb_active { 1 } else { 0 });
        // Snapshot age — mtime of the on-disk history file. Bounded I/O
        // (single stat per tick per instance) is acceptable here; the
        // alternative is plumbing the writer's last-write timestamp
        // out through several layers.
        if let Ok(meta) = std::fs::metadata(&self.history_path) {
            if let Ok(modified) = meta.modified() {
                if let Ok(elapsed) = modified.elapsed() {
                    prom::SNAPSHOT_AGE_SECONDS
                        .with_label_values(&[instance])
                        .set(elapsed.as_secs_f64());
                }
            }
        }
    }

    fn state_score(&self, _inst_idx: usize, key: &str) -> f64 {
        self.per_pair_state
            .get(key)
            .map(|s| s.p_value_weighted_score)
            .unwrap_or(0.0)
    }

    fn should_log_ob_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ob_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn should_log_ticker_warn(&self, symbol: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_ticker_warn
            .get(symbol)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn should_log_position_warn(&self, key: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_position_warn
            .get(key)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn is_dust_position(
        &self,
        snapshot: &PositionSnapshot,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> bool {
        let Some(symbol_snapshot) = prices.get(&snapshot.symbol) else {
            return false;
        };
        let Some(min_order) = symbol_snapshot.min_order else {
            return false;
        };
        snapshot.size < min_order
    }

    fn entry_vol_window(&self) -> usize {
        ((self.cfg.default_pair_params.entry_vol_lookback_hours * 3600)
            / self.cfg.trading_period_secs)
            .max(1) as usize
    }

    /// Virtual clock used by all duration-based decisions. In live mode this
    /// is the wall-clock UTC second; in backtest mode it tracks the replay
    /// connector's logical timestamp so cooldown / force_close /
    /// circuit_breaker / re-eval intervals fire correctly under replay.
    fn current_now_ts(&self) -> i64 {
        if self.cfg.backtest_mode {
            self.replay_connector
                .as_ref()
                .and_then(|r| r.current_timestamp_secs())
                .unwrap_or_else(|| chrono::Utc::now().timestamp())
        } else {
            chrono::Utc::now().timestamp()
        }
    }

    fn max_history_len(&self) -> usize {
        let mut max_needed = 0usize;
        // Consider all per-pair params and the default
        let all_params =
            std::iter::once(&self.cfg.default_pair_params).chain(self.cfg.pair_params.values());
        for pp in all_params {
            let max_hrs = pp.lookback_hours_long.max(pp.lookback_hours_short);
            let needed = (max_hrs * 3600 / self.cfg.trading_period_secs) as usize;
            let vol_needed = ((pp.entry_vol_lookback_hours * 3600) / self.cfg.trading_period_secs)
                .max(1) as usize;
            max_needed = max_needed.max(needed).max(vol_needed);
        }
        max_needed.max(self.cfg.metrics_window)
    }

    fn post_only_supported(&self) -> bool {
        let dex = self.cfg.dex_name.to_ascii_lowercase();
        dex.contains("lighter") || dex.contains("extended")
    }

    fn should_post_only(&self) -> bool {
        self.cfg.fee_bps > 0.0 && self.post_only_supported()
    }

    fn order_reference_price_from_snapshot(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        let use_book = self.cfg.slippage_bps < 0 || self.should_post_only();
        if use_book {
            let side_price = match side {
                dex_connector::OrderSide::Long => snapshot.ask_price,
                dex_connector::OrderSide::Short => snapshot.bid_price,
            };
            if side_price.is_none() {
                log::debug!(
                    "[ORDER] {} missing top-of-book price; using ticker price",
                    symbol
                );
            }
            return side_price.unwrap_or(snapshot.price);
        }
        snapshot.price
    }

    fn order_reference_price(
        &self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        Some(self.order_reference_price_from_snapshot(symbol, side, snapshot))
    }

    fn limit_price_for(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        let snapshot = prices.get(symbol)?;
        let reference = self.order_reference_price_from_snapshot(symbol, side, snapshot);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot(symbol, adjusted, side, snapshot))
    }

    fn limit_price_for_snapshot(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Option<Decimal> {
        let reference = self.order_reference_price_from_snapshot(symbol, side, snapshot);
        let adjusted = self.apply_slippage(Some(reference), side)?;
        Some(self.quantize_order_price_with_snapshot(symbol, adjusted, side, snapshot))
    }

    async fn refreshed_limit_price(
        &mut self,
        symbol: &str,
        side: dex_connector::OrderSide,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Option<Decimal> {
        match self.refresh_symbol_snapshot(symbol).await {
            Ok(snapshot) => self.limit_price_for_snapshot(symbol, side, &snapshot),
            Err(err) => {
                log::debug!(
                    "[ORDER] Failed to refresh price snapshot for {}: {:?}",
                    symbol,
                    err
                );
                self.limit_price_for(symbol, side, prices)
            }
        }
    }

    async fn refresh_symbol_snapshot(&mut self, symbol: &str) -> Result<SymbolSnapshot> {
        let ticker = self
            .connector
            .get_ticker(symbol, None)
            .await
            .with_context(|| format!("ticker {}", symbol))?;
        let (bid_price, ask_price, bid_size, ask_size) =
            match self.connector.get_order_book(symbol, 1).await {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(err) => {
                    log::debug!(
                        "[ORDER] orderbook {} unavailable during retry: {:?}",
                        symbol,
                        err
                    );
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
        Ok(SymbolSnapshot {
            price: ticker.price,
            funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            min_order: ticker.min_order,
            min_tick: ticker.min_tick,
            size_decimals: ticker.size_decimals,
            exchange_ts: ticker.exchange_ts.map(|v| v as i64),
        })
    }

    fn order_spread_param(&self, limit: Option<Decimal>, allow_post_only: bool) -> Option<i64> {
        if allow_post_only && limit.is_some() && self.should_post_only() {
            Some(-2)
        } else {
            None
        }
    }

    fn apply_slippage(
        &self,
        price: Option<Decimal>,
        side: dex_connector::OrderSide,
    ) -> Option<Decimal> {
        order_pricing::apply_slippage(self.cfg.slippage_bps, price, side)
    }

    fn quantize_order_size(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size(symbol, size, prices)
    }

    fn quantize_order_size_exit(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size_exit(symbol, size, prices)
    }

    fn quantize_order_size_close(
        &self,
        symbol: &str,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Decimal {
        order_pricing::quantize_order_size_close(symbol, size, prices)
    }

    fn quantize_order_price_with_snapshot(
        &mut self,
        symbol: &str,
        price: Decimal,
        side: dex_connector::OrderSide,
        snapshot: &SymbolSnapshot,
    ) -> Decimal {
        let mut effective_tick_size = snapshot.min_tick;

        // Extended occasionally returns markets without `min_tick` populated
        // in the snapshot (dex-connector fills this from the markets cache,
        // which may lag a reconnect). Fall back to tick=1 so we don't spam
        // the "No min tick" warning every cycle.
        if effective_tick_size.is_none() && self.cfg.dex_name.contains("extended") {
            effective_tick_size = Some(Decimal::ONE);
        }

        let Some(tick_size) = effective_tick_size else {
            if !self.min_tick_warned.contains(symbol) {
                log::warn!(
                    "[ORDER] No min tick for {}; price rounding disabled",
                    symbol
                );

                self.min_tick_warned.insert(symbol.to_string());
            }

            return price;
        };

        if tick_size <= Decimal::ZERO {
            return price;
        }

        let rounded = round_price_by_tick(price, tick_size, side);

        // bot-strategy#216: tick rounding is a no-op when the touch price is
        // already a tick multiple (Extended BTC tick=1 with integer prices),
        // so post-only limits land at touch and get rejected/crossed.
        if self.should_post_only() {
            let touch = match side {
                dex_connector::OrderSide::Long => snapshot.ask_price,
                dex_connector::OrderSide::Short => snapshot.bid_price,
            };
            if let Some(touch) = touch {
                return enforce_post_only_passive(rounded, touch, tick_size, side);
            }
        }

        rounded
    }

    async fn fetch_latest_prices(&mut self) -> Result<HashMap<String, SymbolSnapshot>> {
        let symbols: Vec<String> = self
            .cfg
            .universe
            .iter()
            .flat_map(|p| [p.base.clone(), p.quote.clone()])
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let connector = self.connector.clone();
        let mut join_set = tokio::task::JoinSet::new();
        for sym in symbols.iter().cloned() {
            let conn = connector.clone();
            join_set.spawn(async move {
                let (ticker_res, ob_res) =
                    tokio::join!(conn.get_ticker(&sym, None), conn.get_order_book(&sym, 1),);
                (sym, ticker_res, ob_res)
            });
        }
        let mut results = Vec::new();
        while let Some(res) = join_set.join_next().await {
            results.push(res.expect("fetch task panicked"));
        }
        // Sort by symbol so any [TICKER] / [ORDERBOOK] warning emit order
        // is deterministic across runs — JoinSet completion order is
        // tokio-scheduler dependent and previously caused intermittent
        // golden_baseline drift.
        results.sort_by(|a, b| a.0.cmp(&b.0));

        let mut map = HashMap::new();
        for (symbol, ticker_res, ob_res) in results {
            let ticker = match ticker_res {
                Ok(ticker) => ticker,
                Err(e) => {
                    let msg = e.to_string();
                    if engine::error_class::is_ticker_auth_error(&msg) {
                        if self.should_log_ticker_warn(&symbol) {
                            log::warn!("ticker {} unavailable: {}", symbol, msg);
                            self.last_ticker_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("ticker {} unavailable: {}", symbol, msg);
                        }
                        continue;
                    }
                    if engine::error_class::is_ticker_rate_limited(&e, &msg) {
                        if self.should_log_ticker_warn(&symbol) {
                            log::warn!("ticker {} rate-limited (cooling down): {}", symbol, msg);
                            self.last_ticker_warn.insert(symbol.clone(), Instant::now());
                        } else {
                            log::debug!("ticker {} rate-limited (cooling down): {}", symbol, msg);
                        }
                        continue;
                    }
                    return Err(e).with_context(|| format!("ticker {}", symbol));
                }
            };
            let (top_bid_price, top_ask_price, top_bid_size, top_ask_size) = match ob_res {
                Ok(ob) => (
                    ob.bids.first().map(|l| l.price),
                    ob.asks.first().map(|l| l.price),
                    ob.bids.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                    ob.asks.first().map(|l| l.size).unwrap_or(Decimal::ZERO),
                ),
                Err(e) => {
                    let msg = format!("{:?}", e);
                    let is_stale = msg.contains("order book snapshot unavailable");
                    if is_stale {
                        log::debug!("orderbook {} unavailable: {}", symbol, msg);
                    } else if self.should_log_ob_warn(&symbol) {
                        log::warn!("orderbook {} unavailable: {}", symbol, msg);
                        self.last_ob_warn.insert(symbol.clone(), Instant::now());
                    } else {
                        log::debug!("orderbook {} unavailable: {}", symbol, msg);
                    }
                    (None, None, Decimal::ZERO, Decimal::ZERO)
                }
            };
            if ticker.min_order.is_none() && !self.min_order_warned.contains(&symbol) {
                let size_decimals_desc = ticker
                    .size_decimals
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_order (size_decimals={}); using fallback step",
                    symbol,
                    size_decimals_desc
                );
                self.min_order_warned.insert(symbol.clone());
            }
            if ticker.min_tick.is_none() && !self.min_tick_warned.contains(&symbol) {
                let min_tick_desc = ticker
                    .min_tick
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "none".into());
                log::warn!(
                    "[TICKER] {} missing min_tick (ticker reports {}); price will be rounded with fallback",
                    symbol,
                    min_tick_desc
                );
                self.min_tick_warned.insert(symbol.clone());
            }
            map.insert(
                symbol.clone(),
                SymbolSnapshot {
                    price: ticker.price,
                    funding_rate: ticker.funding_rate.unwrap_or(Decimal::ZERO),
                    bid_price: top_bid_price,
                    ask_price: top_ask_price,
                    bid_size: top_bid_size,
                    ask_size: top_ask_size,
                    min_order: ticker.min_order,
                    min_tick: ticker.min_tick,
                    size_decimals: ticker.size_decimals,
                    exchange_ts: ticker.exchange_ts.map(|v| v as i64),
                },
            );
            log::debug!(
                "[PRICE_SNAPSHOT] {} price={} bid={:?} ask={:?} bid_sz={} ask_sz={} min_order={:?} min_tick={:?}",
                symbol,
                ticker.price,
                top_bid_price,
                top_ask_price,
                top_bid_size,
                top_ask_size,
                ticker.min_order,
                ticker.min_tick
            );
        }
        Ok(map)
    }
}

#[cfg(test)]
impl PairTradeEngine {
    /// Construct a single-instance, no-state engine wired to `connector`.
    /// Universe = AAA/BBB, dry_run=true, default PairParams. Tests that
    /// need pair state seed `engine.instances[0].states` directly.
    /// `pub(in crate::pairtrade)` so cluster-module tests under
    /// `engine/<x>.rs` can drive engine state-mutation paths without
    /// duplicating the constructor. bot-strategy#396.
    pub(in crate::pairtrade) fn test_instance(
        connector: Arc<dyn DexConnector + Send + Sync>,
    ) -> Self {
        let cfg = PairTradeConfig {
            dex_name: "test".to_string(),
            rest_endpoint: "http://localhost".to_string(),
            web_socket_endpoint: "ws://localhost".to_string(),
            dry_run: true,
            agent_name: None,
            interval_secs: 1,
            trading_period_secs: 1,
            metrics_window: 1,
            net_funding_min_per_hour: 0.0,
            risk_pct_per_trade: 0.01,
            equity_reference_usd: DEFAULT_EQUITY_USD,
            universe: vec![PairSpec {
                base: "AAA".to_string(),
                quote: "BBB".to_string(),
            }],
            slippage_bps: 0,
            fee_bps: 0.0,
            max_leverage: 1.0,
            max_active_pairs: 1,
            warm_start_mode: WarmStartMode::Strict,
            order_timeout_secs: DEFAULT_ORDER_TIMEOUT_SECS,
            entry_partial_fill_max_retries: DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES,
            startup_force_close_attempts: DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS,
            startup_force_close_wait_secs: DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS,
            force_close_on_startup: false,
            enable_data_dump: false,
            data_dump_file: None,
            observe_only: false,
            disable_history_persist: true,
            history_file: "test-history.json".to_string(),
            history_archive_dir: None,
            history_archive_retention_days: 14,
            backtest_mode: false,
            backtest_file: None,
            bt_warm_start_snapshot: None,
            bt_eval_timestamps: None,
            bt_restart_timestamps: None,
            shutdown_grace_secs: 0,
            pair_params: HashMap::new(),
            default_pair_params: PairParams {
                entry_z_base: 2.0,
                entry_z_min: 1.8,
                entry_z_max: 2.3,
                exit_z: 0.5,
                stop_loss_z: 3.0,
                force_close_secs: 60,
                cooldown_secs: 1,
                max_loss_r_mult: 1.0,
                half_life_max_hours: 1.0,
                adf_p_threshold: 0.05,
                spread_velocity_max_sigma_per_min: 0.1,
                lookback_hours_short: 1,
                lookback_hours_long: 1,
                entry_vol_lookback_hours: 1,
                warm_start_min_bars: 1,
                hedge_ratio_max_deviation: 1.0,
                ..PairParams::default()
            },
            strategies: Vec::new(),
            use_kalman_beta: DEFAULT_USE_KALMAN_BETA,
            kalman_q: DEFAULT_KALMAN_Q,
            kalman_r: DEFAULT_KALMAN_R,
            kalman_initial_p: DEFAULT_KALMAN_INITIAL_P,
            kalman_min_updates: DEFAULT_KALMAN_MIN_UPDATES,
            regime_vol_window: DEFAULT_REGIME_VOL_WINDOW,
            regime_vol_max: DEFAULT_REGIME_VOL_MAX,
            regime_trend_window: DEFAULT_REGIME_TREND_WINDOW,
            regime_trend_max: DEFAULT_REGIME_TREND_MAX,
            regime_reference_symbol: DEFAULT_REGIME_REFERENCE_SYMBOL.to_string(),
            bt_fill_delay_secs: 0,
            risk: config::RiskConfig::default(),
            round_id: None,
        };

        let history_path = PathBuf::from(cfg.history_file.as_str());
        let risk_state_path = risk_state_path_for(&history_path);

        Self {
            cfg,
            connector: connector.clone(),
            per_pair_state: HashMap::new(),
            instances: vec![StrategyInstance {
                id: "default".to_string(),
                connector,
                equity_cache: DEFAULT_EQUITY_USD,
                last_equity_fetch: None,
                equity_initialized: false,
                equity_reference_usd: DEFAULT_EQUITY_USD,
                states: HashMap::new(),
                pnl_logger: None,
                status_reporter: None,
                consecutive_losses: 0,
                circuit_breaker_until: None,
                circuit_breaker_until_ts: None,
                session_start_equity: 0.0,
                session_start_ts: 0,
                realized_pnl_today: 0.0,
                funding_carry_today: 0.0,
                daily_loss_halted: false,
                equity_samples: Vec::new(),
                session_halted: false,
                session_halt_reason: None,
                session_halt_ts: None,
                total_trades: 0,
                total_wins: 0,
                total_pnl: 0.0,
                peak_pnl: 0.0,
                max_dd: 0.0,
                pair_params: HashMap::new(),
                default_pair_params: PairParams::default(),
            }],
            history: HashMap::new(),
            bar_builders: HashMap::new(),
            last_metrics_log: None,
            last_ob_warn: HashMap::new(),
            last_ticker_warn: HashMap::new(),
            last_position_warn: HashMap::new(),
            min_order_warned: HashSet::new(),
            min_tick_warned: HashSet::new(),
            positions_ready: false,
            open_positions: HashMap::new(),
            history_path,
            risk_state_path,
            kill_switch_active: false,
            data_dump_writer: None,
            replay_connector: None,
            funding_history: funding_history::FundingHistory::new(),
            shutdown_pending: false,
            bar_emit_log: HashMap::new(),
            last_bar_rate_warn: HashMap::new(),
            last_warm_start_key: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::util::{
        enforce_post_only_passive, quantize_size_by_step, quantize_size_by_step_ceiling,
    };
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn round_price_by_tick_rounds_long_down() {
        let price = dec("100.123");
        let step = dec("0.01");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
        assert_eq!(quantized, dec("100.12"));
    }

    #[test]
    fn round_price_by_tick_rounds_short_up() {
        let price = dec("100.123");
        let step = dec("0.01");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Short);
        assert_eq!(quantized, dec("100.13"));
    }

    #[test]
    fn round_price_by_tick_enforces_minimum_step() {
        let price = dec("0.0001");
        let step = dec("0.005");
        let quantized = round_price_by_tick(price, step, dex_connector::OrderSide::Long);
        assert_eq!(quantized, step);
    }

    // bot-strategy#216: post-only passive enforcement
    #[test]
    fn post_only_passive_long_extended_btc_at_touch() {
        // Extended BTC tick=1, ask=77641, ToNegativeInfinity rounding leaves
        // limit at touch (no-op). Must shift to ask - 1 tick.
        let rounded = dec("77641");
        let touch = dec("77641");
        let tick = dec("1");
        let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
        assert_eq!(limit, dec("77640"));
    }

    #[test]
    fn post_only_passive_short_extended_eth_at_touch() {
        // Extended ETH tick=0.1, bid=2315.5, ToPositiveInfinity rounding
        // leaves limit at touch. Must shift to bid + 1 tick.
        let rounded = dec("2315.5");
        let touch = dec("2315.5");
        let tick = dec("0.1");
        let limit =
            enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Short);
        assert_eq!(limit, dec("2315.6"));
    }

    #[test]
    fn post_only_passive_long_already_inside_no_op() {
        // Lighter passive-slippage path: rounded already below ask. Untouched.
        let rounded = dec("77640.0");
        let touch = dec("77640.5");
        let tick = dec("0.1");
        let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
        assert_eq!(limit, dec("77640.0"));
    }

    #[test]
    fn post_only_passive_short_already_inside_no_op() {
        let rounded = dec("2315.6");
        let touch = dec("2315.5");
        let tick = dec("0.1");
        let limit =
            enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Short);
        assert_eq!(limit, dec("2315.6"));
    }

    #[test]
    fn post_only_passive_long_above_touch_clamps() {
        // Defensive: if upstream produced a rounded price above ask (e.g.
        // aggressive slippage_bps>0 with should_post_only true), clamp it
        // back inside.
        let rounded = dec("77642");
        let touch = dec("77641");
        let tick = dec("1");
        let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
        assert_eq!(limit, dec("77640"));
    }

    #[test]
    fn post_only_passive_zero_tick_returns_input() {
        let rounded = dec("100");
        let touch = dec("100");
        let tick = dec("0");
        let limit = enforce_post_only_passive(rounded, touch, tick, dex_connector::OrderSide::Long);
        assert_eq!(limit, dec("100"));
    }

    // bot-strategy#258: Extended reduce-only error classification

    // bot-strategy#281: classify Lighter REST 429 / DexError::RateLimited so
    // the step skips quietly instead of erroring out per cycle.

    #[test]
    fn quantize_size_by_step_uses_size_decimals() {
        let size = dec("0.0023");
        let step = dec("0.001");
        let quantized = quantize_size_by_step(size, step, None);
        assert_eq!(quantized, dec("0.002"));
    }

    #[test]
    fn quantize_size_by_step_respects_min_order_floor() {
        let size = dec("0.0002");
        let step = dec("0.0001");
        let quantized = quantize_size_by_step(size, step, Some(dec("0.001")));
        assert_eq!(quantized, dec("0.001"));
    }

    #[test]
    fn quantize_size_by_step_ceiling_rounds_up() {
        let size = dec("0.0023");
        let step = dec("0.001");
        let quantized = quantize_size_by_step_ceiling(size, step, None);
        assert_eq!(quantized, dec("0.003"));
    }

    // bot-strategy#185 Phase 3-1: rolling-peak DD calculations.

    // bot-strategy#185 leverage-neutralization amendment:
    // `max_daily_loss_bps` and `max_session_loss_bps` are interpreted as
    // 1x-equivalent market-move bps and multiplied by `max_leverage` at
    // comparison time. Same YAML value should produce the same trip
    // behaviour at any leverage, so changing leverage doesn't silently
    // relax the gates.

    // bot-strategy#320: trade-stats fields round-trip through risk_state.json.

    // bot-strategy#320: an older snapshot without the trade-stats fields
    // must load cleanly with zeros, not panic on missing keys.

    // bot-strategy#354: round_id round-trips through persist/load.
    #[test]
    fn risk_state_round_id_round_trip() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");
        let instances: HashMap<String, risk_io::InstanceRiskState> = HashMap::new();

        risk_io::persist_risk_state(&path, Some("round-4"), &instances);
        let snapshot = risk_io::load_risk_state(&path);

        assert_eq!(snapshot.round_id.as_deref(), Some("round-4"));
        assert_eq!(snapshot.version, 2);
    }

    // bot-strategy#259: defensive cap on exit qty when exchange position
    // size momentarily over-reports vs the bot-recorded entry size after
    // partial-fill retry recovery on Tokyo Extended LongSpread.
    #[test]
    fn cap_exit_qty_caps_exchange_size_when_over_recorded() {
        let exch = Some(dec("0.092"));
        let recorded = Some(dec("0.046"));
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
        assert_eq!(
            q,
            dec("0.046"),
            "must cap to recorded entry size when exchange is 2x"
        );
    }

    #[test]
    fn cap_exit_qty_passes_exchange_within_5pct_tolerance() {
        // 0.046 * 1.04 = 0.04784 — under 5% threshold, exchange wins.
        let exch = Some(dec("0.04784"));
        let recorded = Some(dec("0.046"));
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
        assert_eq!(q, dec("0.04784"), "small drift (<5%) should pass through");
    }

    #[test]
    fn cap_exit_qty_caps_at_5pct_boundary() {
        // 0.046 * 1.06 = 0.04876 — over 5% threshold, cap.
        let exch = Some(dec("0.04876"));
        let recorded = Some(dec("0.046"));
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
        assert_eq!(q, dec("0.046"), "drift just over 5% must cap");
    }

    #[test]
    fn cap_exit_qty_falls_back_to_recorded_when_exchange_missing() {
        // Exchange snapshot absent (e.g. WS lag): use recorded entry size.
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", None, Some(dec("0.046")));
        assert_eq!(q, dec("0.046"));
    }

    #[test]
    fn cap_exit_qty_passes_exchange_when_no_recorded() {
        // Recovery on startup: recorded entry size unknown, trust exchange.
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", Some(dec("0.05")), None);
        assert_eq!(q, dec("0.05"));
    }

    #[test]
    fn cap_exit_qty_zero_when_both_missing() {
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", None, None);
        assert_eq!(q, Decimal::ZERO);
    }

    #[test]
    fn cap_exit_qty_passes_when_exchange_smaller_than_recorded() {
        // Partial close already happened on exchange — exit should use the
        // smaller exchange-reported residual, not the original recorded entry.
        let exch = Some(dec("0.020"));
        let recorded = Some(dec("0.046"));
        let q = PairTradeEngine::cap_exit_qty("BTC/ETH", "ETH", exch, recorded);
        assert_eq!(
            q,
            dec("0.020"),
            "exchange-side close already happened; trust the smaller residual"
        );
    }

    fn make_leg(symbol: &str, target: Decimal) -> PendingLeg {
        PendingLeg {
            symbol: symbol.to_string(),
            order_id: format!("oid-{}-{}", symbol, target),
            exchange_order_id: None,
            target,
            filled: target,
            side: dex_connector::OrderSide::Long,
            limit_price: None,
            reference_price: None,
        }
    }

    // Pairtrade entry-size under-record fix (companion to bot-strategy#259):
    // post-reissue pending.legs can hold two legs per symbol (kept + new);
    // assignment-only recording leaks only the last leg's target into
    // entry_size_a/b and breaks the cap_exit_qty invariant.
    #[test]
    fn sum_entry_sizes_simple_one_leg_per_symbol() {
        let legs = vec![
            make_leg("BTC", dec("0.0013")),
            make_leg("ETH", dec("0.046")),
        ];
        let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
        assert_eq!(a, Some(dec("0.0013")));
        assert_eq!(b, Some(dec("0.046")));
    }

    #[test]
    fn sum_entry_sizes_partial_fill_reissue_two_legs_same_symbol() {
        // Real shape after reissue_partial_legs: BTC partial-filled then
        // reissued, so pending.legs has the kept leg (target=filled) plus
        // the new leg (target=remaining quantized). ETH was full-filled in
        // one shot, so a single leg.
        let legs = vec![
            make_leg("BTC", dec("0.0008")), // kept leg, filled portion
            make_leg("BTC", dec("0.0005")), // reissued leg, remaining
            make_leg("ETH", dec("0.046")),
        ];
        let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
        assert_eq!(
            a,
            Some(dec("0.0013")),
            "BTC must sum kept (0.0008) + reissued (0.0005), not last-write-wins to 0.0005"
        );
        assert_eq!(b, Some(dec("0.046")));
    }

    #[test]
    fn sum_entry_sizes_both_symbols_reissued() {
        // Pathological case: both legs partial-filled and reissued.
        let legs = vec![
            make_leg("BTC", dec("0.0008")),
            make_leg("BTC", dec("0.0005")),
            make_leg("ETH", dec("0.030")),
            make_leg("ETH", dec("0.016")),
        ];
        let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
        assert_eq!(a, Some(dec("0.0013")));
        assert_eq!(b, Some(dec("0.046")));
    }

    #[test]
    fn sum_entry_sizes_returns_none_for_missing_symbol() {
        // Defensive: if a symbol has zero legs (shouldn't happen in
        // practice), preserve the previous Option::None semantics rather
        // than silently writing Decimal::ZERO into entry_size.
        let legs = vec![make_leg("BTC", dec("0.0013"))];
        let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
        assert_eq!(a, Some(dec("0.0013")));
        assert_eq!(b, None);
    }

    #[test]
    fn sum_entry_sizes_unknown_symbol_ignored() {
        // Legs for symbols outside the base/quote pair are not summed.
        let legs = vec![
            make_leg("BTC", dec("0.0013")),
            make_leg("SOL", dec("1.0")),
            make_leg("ETH", dec("0.046")),
        ];
        let (a, b) = PairTradeEngine::sum_entry_sizes_by_symbol(&legs, "BTC", "ETH");
        assert_eq!(a, Some(dec("0.0013")));
        assert_eq!(b, Some(dec("0.046")));
    }
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use async_trait::async_trait;
    use dex_connector::{
        BalanceResponse, CanceledOrdersResponse, CreateOrderResponse, DexConnector, DexError,
        FilledOrdersResponse, LastTradesResponse, OpenOrdersResponse, OrderBookSnapshot, OrderSide,
        PositionSnapshot, TickerResponse, TpSl, TriggerOrderStyle,
    };
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    /// Per-call record captured by the test-only `DummyConnector`:
    /// `(symbol, size, side, limit_price, post_only)`.
    type DummyCall = (String, Decimal, OrderSide, Option<Decimal>, bool);

    #[derive(Default)]
    struct DummyConnector {
        calls: Mutex<Vec<DummyCall>>,
        next_id: AtomicUsize,
        balance_calls: AtomicUsize,
        balance_equity: Mutex<Option<Decimal>>,
        /// bot-strategy#396: observe whether `force_close_on_startup` /
        /// `force_close_all_positions` reach the connector. Used to lock
        /// the dry_run short-circuit so a future refactor cannot silently
        /// turn it into a side-effecting path.
        positions_calls: AtomicUsize,
        close_all_calls: AtomicUsize,
        cancel_all_calls: AtomicUsize,
    }

    #[async_trait]
    impl DexConnector for DummyConnector {
        async fn start(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn restart(&self, _max_retries: i32) -> Result<(), DexError> {
            Ok(())
        }

        async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<(), DexError> {
            Ok(())
        }

        async fn get_ticker(
            &self,
            _symbol: &str,
            _test_price: Option<Decimal>,
        ) -> Result<TickerResponse, DexError> {
            Err(DexError::Permanent("not used".to_string()))
        }

        async fn get_filled_orders(&self, _symbol: &str) -> Result<FilledOrdersResponse, DexError> {
            Ok(FilledOrdersResponse::default())
        }

        async fn get_canceled_orders(
            &self,
            _symbol: &str,
        ) -> Result<CanceledOrdersResponse, DexError> {
            Ok(CanceledOrdersResponse::default())
        }

        async fn get_open_orders(&self, _symbol: &str) -> Result<OpenOrdersResponse, DexError> {
            Ok(OpenOrdersResponse::default())
        }

        async fn get_balance(&self, _symbol: Option<&str>) -> Result<BalanceResponse, DexError> {
            self.balance_calls.fetch_add(1, Ordering::SeqCst);
            let equity = self.balance_equity.lock().unwrap().unwrap_or_default();
            Ok(BalanceResponse {
                equity,
                balance: equity,
                position_entry_price: None,
                position_sign: None,
            })
        }

        async fn get_combined_balance(
            &self,
        ) -> Result<dex_connector::CombinedBalanceResponse, DexError> {
            Ok(dex_connector::CombinedBalanceResponse::default())
        }

        async fn get_positions(&self) -> Result<Vec<PositionSnapshot>, DexError> {
            self.positions_calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![])
        }

        async fn get_last_trades(&self, _symbol: &str) -> Result<LastTradesResponse, DexError> {
            Ok(LastTradesResponse::default())
        }

        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<OrderBookSnapshot, DexError> {
            Ok(OrderBookSnapshot::default())
        }

        async fn clear_filled_order(&self, _symbol: &str, _trade_id: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_all_filled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_canceled_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), DexError> {
            Ok(())
        }

        async fn clear_all_canceled_orders(&self) -> Result<(), DexError> {
            Ok(())
        }

        async fn create_order(
            &self,
            symbol: &str,
            size: Decimal,
            side: OrderSide,
            price: Option<Decimal>,
            _spread: Option<i64>,
            reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            let order_id = format!("test-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
            let ordered_price = price.unwrap_or(Decimal::ONE);
            self.calls
                .lock()
                .unwrap()
                .push((symbol.to_string(), size, side, price, reduce_only));
            Ok(CreateOrderResponse {
                order_id,
                exchange_order_id: None,
                ordered_price,
                ordered_size: size,
                client_order_id: None,
            })
        }

        async fn create_advanced_trigger_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _trigger_px: Decimal,
            _limit_px: Option<Decimal>,
            _order_style: TriggerOrderStyle,
            _slippage_bps: Option<u32>,
            _tpsl: TpSl,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<CreateOrderResponse, DexError> {
            Err(DexError::Permanent("not used".to_string()))
        }

        async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn cancel_all_orders(&self, _symbol: Option<String>) -> Result<(), DexError> {
            self.cancel_all_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn cancel_orders(
            &self,
            _symbol: Option<String>,
            _order_ids: Vec<String>,
        ) -> Result<(), DexError> {
            Ok(())
        }

        async fn close_all_positions(&self, _symbol: Option<String>) -> Result<(), DexError> {
            self.close_all_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn clear_last_trades(&self, _symbol: &str) -> Result<(), DexError> {
            Ok(())
        }

        async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
            false
        }

        async fn sign_evm_65b(&self, _message: &str) -> Result<String, DexError> {
            Ok("signed".to_string())
        }

        async fn sign_evm_65b_with_eip191(&self, _message: &str) -> Result<String, DexError> {
            Ok("signed".to_string())
        }
    }

    #[tokio::test]
    async fn reissue_partial_entry_leg_reorders_remaining() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        let pending = PendingOrders {
            legs: vec![PendingLeg {
                symbol: "AAA".to_string(),
                order_id: "leg1".to_string(),
                exchange_order_id: None,
                target: dec("0.05"),
                filled: Decimal::ZERO,
                side: OrderSide::Long,
                limit_price: None,
                reference_price: None,
            }],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            placed_ts_ms: 0,
            hedge_retry_count: 0,
            post_only_hybrid: false,
            exit_taker_takeover_at: None,
        };
        let mut price_map = HashMap::new();
        price_map.insert(
            "AAA".to_string(),
            SymbolSnapshot {
                price: dec("100.0"),
                funding_rate: Decimal::ZERO,
                bid_price: None,
                ask_price: None,
                bid_size: Decimal::ZERO,
                ask_size: Decimal::ZERO,
                min_order: Some(dec("0.001")),
                min_tick: Some(dec("0.001")),
                size_decimals: Some(3),
                exchange_ts: None,
            },
        );
        let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

        let result = engine
            .reissue_partial_legs(&pending, &filled_qtys, &price_map, false, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.legs.len(), 2);
        assert!(result
            .legs
            .iter()
            .any(|leg| leg.target == dec("0.02") && leg.filled == dec("0.02")));
        assert!(result
            .legs
            .iter()
            .any(|leg| leg.target == dec("0.03") && leg.filled == Decimal::ZERO));
        let calls = connector.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "AAA");
        assert_eq!(calls[0].3, Some(dec("100.0")));
        assert!(!calls[0].4);
    }

    #[tokio::test]
    async fn reissue_partial_entry_missing_price_keeps_pending() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        let pending = PendingOrders {
            legs: vec![PendingLeg {
                symbol: "AAA".to_string(),
                order_id: "leg1".to_string(),
                exchange_order_id: None,
                target: dec("0.05"),
                filled: Decimal::ZERO,
                side: OrderSide::Long,
                limit_price: None,
                reference_price: None,
            }],
            direction: PositionDirection::LongSpread,
            placed_at: Instant::now(),
            placed_ts_ms: 0,
            hedge_retry_count: 0,
            post_only_hybrid: false,
            exit_taker_takeover_at: None,
        };
        let filled_qtys = HashMap::from([(pending.legs[0].order_id.clone(), dec("0.02"))]);

        let result = engine
            .reissue_partial_legs(&pending, &filled_qtys, &HashMap::new(), false, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.legs.len(), 1);
        assert_eq!(result.legs[0].target, dec("0.05"));
        assert_eq!(result.legs[0].filled, dec("0.02"));
    }

    #[tokio::test]
    async fn refresh_equity_if_needed_skips_when_cache_is_fresh() {
        let connector = Arc::new(DummyConnector::default());
        *connector.balance_equity.lock().unwrap() = Some(dec("1234.56"));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        engine.instances[0].last_equity_fetch = Some(Instant::now());
        let initial_equity = engine.instances[0].equity_cache;

        engine.refresh_equity_if_needed(0).await.unwrap();

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 0);
        assert_eq!(engine.instances[0].equity_cache, initial_equity);
    }

    #[tokio::test]
    async fn refresh_equity_if_needed_fetches_when_cache_is_stale() {
        let connector = Arc::new(DummyConnector::default());
        *connector.balance_equity.lock().unwrap() = Some(dec("1234.56"));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        engine.instances[0].last_equity_fetch =
            Some(Instant::now() - Duration::from_secs(EQUITY_REFRESH_CACHE_SECS + 1));

        engine.refresh_equity_if_needed(0).await.unwrap();

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 1);
        assert!((engine.instances[0].equity_cache - 1234.56).abs() < 1e-6);
    }

    #[tokio::test]
    async fn fetch_equity_rest_bypasses_cache() {
        // Pre-entry path must hit REST regardless of cache age so the
        // about-to-be-placed order is sized against a current value.
        let connector = Arc::new(DummyConnector::default());
        *connector.balance_equity.lock().unwrap() = Some(dec("777.0"));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        engine.instances[0].last_equity_fetch = Some(Instant::now());

        engine.fetch_equity_rest(0).await;

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 1);
        assert!((engine.instances[0].equity_cache - 777.0).abs() < 1e-6);
    }

    // bot-strategy#366: reproduce the restart race that synthesised a 50%
    // DD on Frankfurt Round 4 Step 4 partial. Persisted `equity_samples`
    // hold yesterday's intraday peak (~$1003) but `equity_cache` is the
    // seed `equity_reference_usd` (~$500) until the first WS account
    // dump propagates. Pre-fix, `evaluate_session_dd` would trip
    // `session_halted=true` against the synthetic 5018 bps reading.
    // Post-fix, the `equity_initialized` gate suppresses the gate and
    // sampling until a connector-sourced balance lands.
    #[tokio::test]
    async fn session_dd_gated_until_equity_initialized() {
        use tempfile::TempDir;

        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        // Match the live config that produced the incident.
        engine.cfg.risk.max_session_loss_bps = 500;
        engine.cfg.max_leverage = 10.0;
        // Redirect risk-state persistence to a temp dir so the test does
        // not litter the working directory if a trip ever does fire.
        let dir = TempDir::new().unwrap();
        engine.risk_state_path = dir.path().join("risk_state.json");

        // Simulate restart state: persisted samples carry a $1003 peak,
        // `equity_cache` is still the $500 seed, and the connector has
        // not yet pushed a balance update (`equity_initialized=false`).
        // Sample ts is anchored close to "now" so the lookback prune
        // inside `update_equity_sample` does not discard it.
        let now_ts = engine.current_now_ts();
        {
            let inst = &mut engine.instances[0];
            inst.equity_samples = vec![risk_io::EquitySample {
                ts: now_ts - 60,
                equity: 1003.45,
            }];
            inst.equity_cache = 500.0;
            inst.equity_initialized = false;
            inst.session_halted = false;
        }

        // Phase 1: pre-WS-dump tick. Without the gate, peak=1003.45 vs
        // current=500 would compute dd_bps ≈ 5018 against an effective
        // threshold of 5000 bps and trip the halt. The gate must
        // suppress the evaluation entirely.
        let halted = engine.evaluate_session_dd(0).await;
        assert!(
            !halted,
            "session_dd must not trip while equity_initialized=false"
        );
        assert!(!engine.instances[0].session_halted);
        assert!(engine.instances[0].session_halt_reason.is_none());

        // Sampling must also be suppressed so the deque is not polluted
        // with the seed equity, which would distort post-init peaks.
        let pre_len = engine.instances[0].equity_samples.len();
        engine.update_equity_sample(0);
        assert_eq!(
            engine.instances[0].equity_samples.len(),
            pre_len,
            "update_equity_sample must not append while equity_initialized=false"
        );

        // The status snapshot path is gated identically so dashboards
        // do not render a phantom 50% DD card during the race window.
        assert!(engine.session_risk_snapshot(0).is_none());

        // Phase 2: WS dump lands. `equity_cache` is now the real wallet
        // balance and the gate releases.
        {
            let inst = &mut engine.instances[0];
            inst.equity_cache = 1000.84;
            inst.equity_initialized = true;
        }
        let halted = engine.evaluate_session_dd(0).await;
        assert!(
            !halted,
            "real equity ($1000.84 vs peak $1003.45) is well below threshold"
        );
        assert!(!engine.instances[0].session_halted);

        // The snapshot now exposes the real, non-phantom DD reading,
        // computed against the persisted peak. Check this before
        // `update_equity_sample` runs so the hourly-bucket replacement
        // in the sampler does not rewrite the persisted entry.
        let snapshot = engine
            .session_risk_snapshot(0)
            .expect("snapshot should exist post-init");
        assert!((snapshot.current_equity - 1000.84).abs() < 1e-6);
        assert!((snapshot.peak_equity - 1003.45).abs() < 1e-6);
        assert!(snapshot.dd_bps < 100.0);

        // Sampling now records the real value (possibly by replacing
        // the persisted entry if it falls in the same bucket).
        engine.update_equity_sample(0);
        assert!(
            engine.instances[0]
                .equity_samples
                .iter()
                .any(|s| (s.equity - 1000.84).abs() < 1e-6),
            "post-init sample with real equity must be persisted to the deque"
        );
    }

    // bot-strategy#382: dex-connector's WS-derived balance cache can return
    // Ok(equity=0) for the first few `get_balance` calls after restart,
    // before the first account dump lands. Pre-fix this propagated to
    // `reporter.update_equity(0)`, locking `equity_day_start = 0` for the
    // rest of the UTC day, and surfacing `pnl_today = +<full equity>` on
    // the dashboard once the real balance arrived. Observed live on Tokyo
    // Lighter B/C after the 2026-05-13 06:50 UTC restart: pnl_today=+$150
    // with no trades executed.
    //
    // The companion to bot-strategy#366 is to drop the 0-valued reading
    // entirely during the pre-init window; once the gate flips on the
    // first positive equity, subsequent 0 readings ARE accepted (a
    // genuinely rekt bot should be reflected on dashboards).
    #[tokio::test]
    async fn fetch_equity_rest_drops_zero_reading_before_init() {
        let connector = Arc::new(DummyConnector::default());
        // Phase 1: WS cache empty — connector returns equity=0.
        *connector.balance_equity.lock().unwrap() = Some(Decimal::from(0));
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        let seed_cache = engine.instances[0].equity_cache;
        assert!(
            !engine.instances[0].equity_initialized,
            "test_instance must start uninitialized"
        );

        engine.fetch_equity_rest(0).await;

        // The connector was hit (the function did not short-circuit), but
        // the 0 reading must not have armed the init flag, must not have
        // overwritten the seed cache, and must have left `last_equity_fetch`
        // populated so the refresh-cooldown timer still advances.
        assert_eq!(
            connector.balance_calls.load(Ordering::SeqCst),
            1,
            "fetch_equity_rest must hit the connector"
        );
        assert!(
            !engine.instances[0].equity_initialized,
            "0-valued reading must not arm equity_initialized"
        );
        assert!(
            (engine.instances[0].equity_cache - seed_cache).abs() < 1e-9,
            "0-valued reading must not overwrite equity_cache"
        );
        assert!(
            engine.instances[0].last_equity_fetch.is_some(),
            "last_equity_fetch must still be updated to advance the cooldown",
        );

        // Phase 2: WS dump lands — connector returns equity=150. The gate
        // releases and the normal init path runs.
        *connector.balance_equity.lock().unwrap() = Some(Decimal::from(150));
        engine.fetch_equity_rest(0).await;

        assert_eq!(
            connector.balance_calls.load(Ordering::SeqCst),
            2,
            "second call also hits the connector"
        );
        assert!(
            engine.instances[0].equity_initialized,
            "post-init: equity_initialized must arm on first positive equity"
        );
        assert!(
            (engine.instances[0].equity_cache - 150.0).abs() < 1e-9,
            "equity_cache must hold the real balance"
        );

        // Phase 3: post-init, a 0 reading IS accepted — a rekt bot's
        // dashboard must reflect the loss rather than silently pin to
        // the last positive value.
        *connector.balance_equity.lock().unwrap() = Some(Decimal::from(0));
        engine.fetch_equity_rest(0).await;

        assert_eq!(connector.balance_calls.load(Ordering::SeqCst), 3);
        assert!(
            (engine.instances[0].equity_cache - 0.0).abs() < 1e-9,
            "post-init: 0-valued reading IS accepted (bot may legitimately be at 0)"
        );
    }

    // bot-strategy#354: configured round_id != persisted round_id triggers a
    // reset of round-bound per-instance fields at engine startup, while
    // session-rolling fields (session_start_*, realized_pnl_today) survive.
    #[test]
    fn round_id_transition_zeros_round_bound_fields() {
        use tempfile::TempDir;

        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");
        engine.risk_state_path = path.clone();
        engine.cfg.round_id = Some("round-4".to_string());

        let mut instances = HashMap::new();
        instances.insert(
            "default".to_string(),
            risk_io::InstanceRiskState {
                consecutive_losses: 5,
                circuit_breaker_until_ts: Some(9_999_999_999),
                session_start_equity: 1234.5,
                session_start_ts: 4242,
                realized_pnl_today: -12.0,
                equity_samples: vec![risk_io::EquitySample {
                    ts: 10,
                    equity: 100.0,
                }],
                session_halted: true,
                session_halt_reason: Some("session_dd_500bps".to_string()),
                session_halt_ts: Some(8888),
                total_trades: 42,
                total_wins: 24,
                total_pnl: 99.9,
                peak_pnl: 150.0,
                max_dd: 30.0,
                ..Default::default()
            },
        );
        risk_io::persist_risk_state(&path, Some("round-3"), &instances);

        engine.load_risk_state();

        let inst = &engine.instances[0];
        // Round-bound fields zeroed.
        assert_eq!(inst.consecutive_losses, 0);
        assert!(inst.circuit_breaker_until_ts.is_none());
        assert!(inst.equity_samples.is_empty());
        assert!(!inst.session_halted);
        assert!(inst.session_halt_reason.is_none());
        assert!(inst.session_halt_ts.is_none());
        assert_eq!(inst.total_trades, 0);
        assert_eq!(inst.total_wins, 0);
        assert_eq!(inst.total_pnl, 0.0);
        assert_eq!(inst.peak_pnl, 0.0);
        assert_eq!(inst.max_dd, 0.0);
        // Session-rolling fields survive — UTC midnight rolls them, not the
        // round boundary.
        assert!((inst.session_start_equity - 1234.5).abs() < 1e-9);
        assert_eq!(inst.session_start_ts, 4242);
        assert!((inst.realized_pnl_today - (-12.0)).abs() < 1e-9);
    }

    // bot-strategy#354: configured round_id == persisted round_id preserves
    // all fields (the common in-round restart case).
    #[test]
    fn round_id_match_preserves_round_bound_fields() {
        use tempfile::TempDir;

        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");
        engine.risk_state_path = path.clone();
        engine.cfg.round_id = Some("round-4".to_string());

        let mut instances = HashMap::new();
        instances.insert(
            "default".to_string(),
            risk_io::InstanceRiskState {
                consecutive_losses: 3,
                total_trades: 7,
                total_wins: 4,
                total_pnl: 11.5,
                peak_pnl: 20.0,
                max_dd: 8.5,
                ..Default::default()
            },
        );
        risk_io::persist_risk_state(&path, Some("round-4"), &instances);

        engine.load_risk_state();

        let inst = &engine.instances[0];
        assert_eq!(inst.consecutive_losses, 3);
        assert_eq!(inst.total_trades, 7);
        assert_eq!(inst.total_wins, 4);
        assert!((inst.total_pnl - 11.5).abs() < 1e-9);
        assert!((inst.peak_pnl - 20.0).abs() < 1e-9);
        assert!((inst.max_dd - 8.5).abs() < 1e-9);
    }

    // bot-strategy#469: on restart, the status reporter's `trade_stats`
    // must be seeded from the persisted lifetime totals — not left at
    // its `Some(zeros)` init value until the first post-restart trade.
    // Frankfurt 2026-05-21 19:24 UTC restart surfaced this: A/B had 11
    // lifetime trades each on disk, but dashboard showed 0/0 for ~10h
    // until they next traded.
    #[test]
    fn status_reporter_seeded_from_persisted_totals_on_load() {
        use tempfile::TempDir;

        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");
        engine.risk_state_path = path.clone();
        engine.cfg.round_id = Some("round-5".to_string());

        // Attach an in-memory StatusReporter to the default instance
        // (test_instance constructs one with status_reporter = None).
        engine.instances[0].status_reporter = Some(status::StatusReporter::for_test(
            dir.path().join("status.json"),
        ));

        let mut instances = HashMap::new();
        instances.insert(
            "default".to_string(),
            risk_io::InstanceRiskState {
                total_trades: 11,
                total_wins: 6,
                total_pnl: -1.6885,
                peak_pnl: 0.0,
                max_dd: 10.3857,
                ..Default::default()
            },
        );
        risk_io::persist_risk_state(&path, Some("round-5"), &instances);

        // Reporter pre-load: still at the zero-init value from for_test.
        let pre = engine.instances[0]
            .status_reporter
            .as_ref()
            .and_then(|r| r.trade_stats_for_test())
            .cloned()
            .expect("trade_stats initialised by for_test");
        assert_eq!(pre.trades, 0);
        assert_eq!(pre.pnl, 0.0);

        engine.load_risk_state();

        // In-memory totals applied …
        let inst = &engine.instances[0];
        assert_eq!(inst.total_trades, 11);
        assert_eq!(inst.total_wins, 6);
        // … and immediately surfaced on the status reporter, no need to
        // wait for a post-restart trade to call write_pnl_record.
        let stats = inst
            .status_reporter
            .as_ref()
            .and_then(|r| r.trade_stats_for_test())
            .expect("trade_stats reseeded by load_risk_state");
        assert_eq!(stats.trades, 11);
        assert_eq!(stats.wins, 6);
        assert!((stats.win_rate - (6.0 / 11.0 * 100.0)).abs() < 1e-9);
        assert!((stats.max_dd - 10.3857).abs() < 1e-9);
        assert!((stats.pnl - (-1.6885)).abs() < 1e-9);
    }

    // bot-strategy#354: unset configured round_id (legacy mode) never resets.
    // Hosts without `round_id` in YAML (Tokyo Lighter / Extended /
    // xvenue-arb at v2 launch) must not lose state on restart.
    #[test]
    fn round_id_unset_skips_auto_reset() {
        use tempfile::TempDir;

        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");
        engine.risk_state_path = path.clone();
        engine.cfg.round_id = None;

        let mut instances = HashMap::new();
        instances.insert(
            "default".to_string(),
            risk_io::InstanceRiskState {
                total_trades: 9,
                ..Default::default()
            },
        );
        risk_io::persist_risk_state(&path, Some("round-3"), &instances);

        engine.load_risk_state();

        let inst = &engine.instances[0];
        assert_eq!(inst.total_trades, 9);
    }

    // ------------------------------------------------------------------
    // bot-strategy#396: state-mutation coverage for engine cluster paths
    // that previously had zero tests (reconcile / recovery / placement).
    // ------------------------------------------------------------------

    fn seed_state(engine: &mut PairTradeEngine, key: &str) {
        // Inserts an empty PairState for `key` on instance 0 + an empty
        // PairSharedState on the engine so the reconcile loop can find both.
        // Mirrors the production state-build path in `new_inner`.
        engine.instances[0]
            .states
            .insert(key.to_string(), state::PairState::new(2.0));
        engine
            .per_pair_state
            .entry(key.to_string())
            .or_insert_with(|| state::PairSharedState::new(8));
    }

    /// `reconcile_pending_orders` keys off the per-pair `states` map. A
    /// missing key is a configuration / build-order bug, not a transient
    /// runtime condition, and must surface loudly rather than silently
    /// skip.
    #[tokio::test]
    async fn reconcile_pending_orders_errors_when_state_missing() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        let price_map: HashMap<String, SymbolSnapshot> = HashMap::new();

        let result = engine
            .reconcile_pending_orders(0, "AAA/BBB", &price_map)
            .await;

        assert!(result.is_err(), "missing state key must surface as error");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("AAA/BBB"),
            "error message must name the missing pair key: {msg}"
        );
    }

    /// With state present but no pending orders, reconcile is a no-op
    /// (no connector calls, no state mutation). This is the steady-state
    /// path on every tick when the bot is flat — must stay free of
    /// side-effects, otherwise the per-tick connector RPC budget gets
    /// burned for nothing.
    #[tokio::test]
    async fn reconcile_pending_orders_noop_when_no_pendings() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector.clone());
        seed_state(&mut engine, "AAA/BBB");
        let price_map: HashMap<String, SymbolSnapshot> = HashMap::new();

        engine
            .reconcile_pending_orders(0, "AAA/BBB", &price_map)
            .await
            .expect("no pending = Ok");

        assert_eq!(
            connector.calls.lock().unwrap().len(),
            0,
            "no create_order calls"
        );
        assert_eq!(
            connector.cancel_all_calls.load(Ordering::SeqCst),
            0,
            "no cancel_all calls"
        );
        let state = engine.instances[0].states.get("AAA/BBB").unwrap();
        assert!(state.pending_entry.is_none());
        assert!(state.pending_exit.is_none());
        assert!(state.position.is_none());
    }

    /// `register_partial_leg_failure` is the bridge from the engine's
    /// place-leg error path back into per-pair pending state. An entry
    /// failure must land in `pending_entry` so the next reconcile tick
    /// can clean up the orphaned leg-A.
    #[test]
    fn register_partial_leg_failure_writes_pending_entry() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        seed_state(&mut engine, "AAA/BBB");
        let placed_legs = vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "leg-a".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Long,
            limit_price: None,
            reference_price: None,
        }];
        let partial_err: anyhow::Error = state::PartialOrderPlacementError::new(
            placed_legs.clone(),
            DexError::Transient("leg B failed".to_string()),
        )
        .into();

        engine.register_partial_leg_failure(
            0,
            "AAA/BBB",
            PositionDirection::LongSpread,
            &partial_err,
            false, // is_exit
        );

        let pending = engine.instances[0]
            .states
            .get("AAA/BBB")
            .unwrap()
            .pending_entry
            .as_ref()
            .expect("pending_entry must be populated");
        assert_eq!(pending.legs.len(), 1);
        assert_eq!(pending.legs[0].symbol, "AAA");
        assert_eq!(pending.legs[0].order_id, "leg-a");
        assert_eq!(pending.direction, PositionDirection::LongSpread);
    }

    /// Same surface, exit side: must land in `pending_exit` so the next
    /// tick re-attempts to close the orphan leg, not re-open a fresh
    /// entry.
    #[test]
    fn register_partial_leg_failure_writes_pending_exit() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        seed_state(&mut engine, "AAA/BBB");
        let placed_legs = vec![PendingLeg {
            symbol: "AAA".to_string(),
            order_id: "exit-leg-a".to_string(),
            exchange_order_id: None,
            target: dec("0.05"),
            filled: Decimal::ZERO,
            side: OrderSide::Short,
            limit_price: None,
            reference_price: None,
        }];
        let partial_err: anyhow::Error = state::PartialOrderPlacementError::new(
            placed_legs,
            DexError::Transient("leg B failed".to_string()),
        )
        .into();

        engine.register_partial_leg_failure(
            0,
            "AAA/BBB",
            PositionDirection::ShortSpread,
            &partial_err,
            true, // is_exit
        );

        let state_ref = engine.instances[0].states.get("AAA/BBB").unwrap();
        assert!(
            state_ref.pending_entry.is_none(),
            "exit must not touch pending_entry"
        );
        let pending = state_ref
            .pending_exit
            .as_ref()
            .expect("pending_exit must be populated");
        assert_eq!(pending.direction, PositionDirection::ShortSpread);
        assert_eq!(pending.legs[0].order_id, "exit-leg-a");
    }

    /// Errors that aren't `PartialOrderPlacementError` carry no leg list
    /// to recover (e.g. a pre-flight reference-price miss). The function
    /// silently no-ops — *not* writing a synthetic empty `PendingOrders`
    /// that the reconcile loop would then try to cancel.
    #[test]
    fn register_partial_leg_failure_ignores_non_partial_errors() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        seed_state(&mut engine, "AAA/BBB");
        let plain_err: anyhow::Error = anyhow::anyhow!("missing reference price");

        engine.register_partial_leg_failure(
            0,
            "AAA/BBB",
            PositionDirection::LongSpread,
            &plain_err,
            false,
        );

        let state_ref = engine.instances[0].states.get("AAA/BBB").unwrap();
        assert!(state_ref.pending_entry.is_none());
        assert!(state_ref.pending_exit.is_none());
    }

    /// Unknown state key: write must be silently skipped (the function
    /// uses `if let Some(state) = ... get_mut(key)`). Better to no-op
    /// than panic — a stale pair key in flight should not crash a live
    /// bot. Verifies no other instance's state is mutated.
    #[test]
    fn register_partial_leg_failure_silently_skips_unknown_pair() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector);
        seed_state(&mut engine, "AAA/BBB");
        let partial_err: anyhow::Error = state::PartialOrderPlacementError::new(
            vec![],
            DexError::Transient("placement failed".to_string()),
        )
        .into();

        engine.register_partial_leg_failure(
            0,
            "CCC/DDD",
            PositionDirection::LongSpread,
            &partial_err,
            false,
        );

        // Sibling key untouched.
        let other = engine.instances[0].states.get("AAA/BBB").unwrap();
        assert!(other.pending_entry.is_none());
        assert!(other.pending_exit.is_none());
    }

    /// `force_close_on_startup` is a no-op when `dry_run=true`
    /// (matches DRY_RUN windows used during live-readiness validation).
    /// The connector must not be touched — getting positions /
    /// canceling / closing during DRY_RUN burns rate limit and pollutes
    /// the live bot's order book if the same wallet is shared.
    #[tokio::test]
    async fn force_close_on_startup_dry_run_skips_connector_calls() {
        let connector = Arc::new(DummyConnector::default());
        let engine = PairTradeEngine::test_instance(connector.clone());
        // test_instance defaults dry_run=true.

        engine.force_close_on_startup().await.unwrap();

        assert_eq!(
            connector.positions_calls.load(Ordering::SeqCst),
            0,
            "dry_run must not query positions"
        );
        assert_eq!(
            connector.cancel_all_calls.load(Ordering::SeqCst),
            0,
            "dry_run must not issue cancel_all_orders"
        );
        assert_eq!(
            connector.close_all_calls.load(Ordering::SeqCst),
            0,
            "dry_run must not issue close_all_positions"
        );
    }

    /// `force_close_all_positions` (reconcile-loop emergency path) is
    /// also gated by dry_run / observe_only. The reconcile loop calls
    /// it after exit retries exhaust; on DRY_RUN we must not pretend
    /// to flatten on the exchange.
    #[tokio::test]
    async fn force_close_all_positions_dry_run_skips_connector_calls() {
        let connector = Arc::new(DummyConnector::default());
        let mut engine = PairTradeEngine::test_instance(connector.clone());

        engine.force_close_all_positions("AAA/BBB", "timeout").await;

        assert_eq!(
            connector.positions_calls.load(Ordering::SeqCst),
            0,
            "dry_run must not query positions"
        );
        assert_eq!(
            connector.close_all_calls.load(Ordering::SeqCst),
            0,
            "dry_run must not invoke close_all_positions"
        );
    }
}

#[cfg(test)]
mod shutdown_grace_tests {
    use super::*;

    fn config_path(name: &str) -> String {
        format!("{}/configs/pairtrade/{}", env!("CARGO_MANIFEST_DIR"), name)
    }

    #[test]
    fn default_when_yaml_omits_key() {
        // from_env() path with no env var set = default
        // Use a scoped env guard to avoid bleeding into other tests.
        let prev = std::env::var("SHUTDOWN_GRACE_SECS").ok();
        std::env::remove_var("SHUTDOWN_GRACE_SECS");
        // Also ensure required env vars have sensible fallbacks.
        std::env::set_var("DEX_NAME", "lighter");
        std::env::set_var("UNIVERSE_PAIRS", "BTC/ETH");
        let cfg = PairTradeConfig::from_env().expect("from_env failed");
        assert_eq!(cfg.shutdown_grace_secs, DEFAULT_SHUTDOWN_GRACE_SECS);
        assert_eq!(cfg.shutdown_grace_secs, 3660);
        if let Some(v) = prev {
            std::env::set_var("SHUTDOWN_GRACE_SECS", v);
        }
    }

    #[test]
    fn live_btceth_configs_pin_grace_above_force_close() {
        // The -b / -c YAMLs were folded into the single multi-strategy
        // debot-pair-btceth.yaml in commit 7 of #25; only the consolidated
        // file is checked here.
        //
        // Asserts the bot-strategy#50 invariant directly:
        //   shutdown_grace_secs >= max(force_close_time_secs across resolved
        //                              default + per-pair + per-strategy)
        //                          + 60s buffer
        // (Or shutdown_grace_secs == 0, the legacy immediate-close mode that
        // validate() also accepts.)
        //
        // The same check runs inside PairTradeConfig::validate() during
        // from_yaml_path, so a YAML drift will already block load. Asserting
        // here serves as documentation and a defense against accidental
        // validate() bypass. Pinning the literal expected value (e.g. 7260,
        // 10860) was the prior implementation but coupled the test to YAML
        // edits — every per-strategy fc bump (#278 Round 4 fc=10800 was the
        // first to hit this) needed a matching test edit. The invariant
        // form survives any YAML change that respects the rule.
        const BUFFER_SECS: u64 = 60;
        let configs = &["debot-pair-btceth.yaml"];
        for name in configs {
            let path = config_path(name);
            let cfg = PairTradeConfig::from_yaml_path(&path)
                .unwrap_or_else(|e| panic!("failed to load {path}: {e}"));
            if cfg.shutdown_grace_secs == 0 {
                continue;
            }
            let max_fc = std::iter::once(cfg.default_pair_params.force_close_secs)
                .chain(cfg.pair_params.values().map(|p| p.force_close_secs))
                .chain(
                    cfg.strategies
                        .iter()
                        .filter_map(|s| s.force_close_time_secs),
                )
                .max()
                .expect("at least default_pair_params.force_close_secs");
            let required = max_fc + BUFFER_SECS;
            assert!(
                cfg.shutdown_grace_secs >= required,
                "{name}: shutdown_grace_secs={} must be >= max(force_close_time_secs)={} + {}s buffer = {}",
                cfg.shutdown_grace_secs, max_fc, BUFFER_SECS, required
            );
        }
    }

    /// Regression guard for bot-strategy#50: if any strategy raises
    /// `force_close_time_secs` above `shutdown_grace_secs - 60s`, config load
    /// must fail rather than silently shipping a config that would
    /// prematurely force-close positions on SIGTERM.
    #[test]
    fn validate_rejects_strategy_force_close_exceeding_grace() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join("pairtrade_validate_regression.yaml");
        let yaml = r#"
dex_name: lighter
rest_endpoint: https://example
web_socket_endpoint: wss://example
dry_run: true
universe_pairs:
- BTC/ETH
force_close_time_secs: 3600
shutdown_grace_secs: 3660
strategies:
  - id: a
    force_close_time_secs: 7200
"#;
        std::fs::File::create(&path)
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();
        let err = PairTradeConfig::from_yaml_path(&path)
            .expect_err("validate() must reject grace=3660 when strategy A force_close=7200");
        let msg = format!("{err}");
        assert!(
            msg.contains("shutdown_grace_secs"),
            "error should mention shutdown_grace_secs, got: {msg}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
