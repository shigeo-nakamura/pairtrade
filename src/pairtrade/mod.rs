use anyhow::{Context, Result};
use dex_connector::{DexConnector, PositionSnapshot};
use rust_decimal::prelude::ToPrimitive;
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
mod sentinel;
mod sizing;
mod state;
mod stats;
mod status;
mod util;
// Re-exported at the pairtrade module root so existing `super::super::`
// references in engine submodules resolve unchanged (bot-strategy#502).
use bar::BarBuilder;
pub use config::{DailyLossAction, PairTradeConfig, WarmStartMode};
use market::SymbolSnapshot;
use pnl_log::{PnlLogRecord, PnlLogger};
pub(in crate::pairtrade) use sentinel::{risk_ack_path, KILL_SWITCH_PATH};
use stats::PriceSample;

/// Spawn the Prometheus exporter when `PROM_LISTEN` is set in the env.
/// Safe to call at most once at process boot from `main`. See
/// `pairtrade::prom`.
pub fn start_metrics_exporter() {
    prom::maybe_start_exporter();
}
use config::PairParams;
use engine::risk::risk_state_path_for;
use state::{PairSharedState, PairState, PositionDirection};
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
    state.recovery_recorded = false;
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
    /// not silently re-arm the bot. Cleared only by writing the
    /// manual-ack sentinel (default `/opt/debot/RISK_ACK`, overridable
    /// via the `RISK_ACK_PATH` env var per bot-strategy#488).
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
    /// One-shot reason marker set when the risk layer flattens this
    /// instance's positions outside the strategy exit path (e.g. the
    /// session-DD halt). Consumed by `sync_positions_from_exchange` when
    /// the exchange snapshot confirms the positions are gone, so the
    /// `recovery_no_pnl` context record carries the real trigger instead
    /// of the generic `exchange_snapshot_clear`. Not persisted.
    /// bot-strategy#514.
    external_flatten_reason: Option<String>,
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
            strategy.apply_pair_param_overrides(&mut inst_default);

            let mut inst_pair_params: HashMap<String, PairParams> = HashMap::new();
            for (k, v) in cfg.pair_params.iter() {
                let mut pp = v.clone();
                strategy.apply_pair_param_overrides(&mut pp);
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
                external_flatten_reason: None,
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

    fn write_pnl_context_record(&mut self, inst_idx: usize, record: PnlLogRecord) {
        if let Some(logger) = &mut self.instances[inst_idx].pnl_logger {
            if let Err(err) = logger.log(record) {
                log::warn!("[PNL] failed to write context pnl log: {:?}", err);
            }
        }
    }

    fn write_recovery_no_pnl_record(
        &mut self,
        inst_idx: usize,
        key: &str,
        fallback_direction: PositionDirection,
        recovery_reason: &str,
        now_ts: i64,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) {
        let Some((base, quote)) = key.split_once('/') else {
            return;
        };

        let inst = &self.instances[inst_idx];
        let state = inst.states.get(key);
        let pos = state.and_then(|s| s.position.as_ref());
        let direction = pos.map(|p| p.direction).unwrap_or(fallback_direction);
        let close_reason = state.and_then(|s| s.pending_exit_reason);
        let z_exit = self
            .per_pair_state
            .get(key)
            .and_then(|s| s.z_score().map(|(z, _)| z));
        let beta = pos
            .and_then(|p| p.entry_beta)
            .or_else(|| self.per_pair_state.get(key).map(|s| s.beta));
        let entry_a = pos.and_then(|p| p.entry_price_a.and_then(|v| v.to_f64()));
        let entry_b = pos.and_then(|p| p.entry_price_b.and_then(|v| v.to_f64()));
        let z_entry = pos.and_then(|p| p.entry_z);
        let hold_secs = pos.map(|p| now_ts.saturating_sub(p.entered_ts).max(0) as f64);
        let exit_a = price_map.get(base).and_then(|p| p.price.to_f64());
        let exit_b = price_map.get(quote).and_then(|p| p.price.to_f64());

        let record = PnlLogRecord::new(base, quote, direction, 0.0, now_ts, "recovery_no_pnl")
            .with_unavailable_pnl()
            .with_recovery_context(close_reason, recovery_reason)
            .with_trade_details(
                entry_a, entry_b, exit_a, exit_b, beta, z_entry, z_exit, hold_secs,
            );

        self.write_pnl_context_record(inst_idx, record);
    }

    fn latest_log_price(&self, symbol: &str) -> Option<f64> {
        self.history
            .get(symbol)
            .and_then(|h| h.back())
            .map(|p| p.log_price)
    }

    fn clear_stale_pending(&mut self, inst_idx: usize, max_age: Duration, reason: &str) {
        let now_ts = self.current_now_ts();
        let mut stale: Vec<String> = Vec::new();
        for (key, state) in self.instances[inst_idx].states.iter() {
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
                    stale.push(key.clone());
                }
            }
        }
        for key in stale {
            // bot-strategy#514: dropping a position here loses the close
            // context — record it first. If the exchange still holds the
            // legs, the next snapshot sync rebuilds the position, so this
            // record can occasionally describe a clear that was not a real
            // close; attribution treats recovery_no_pnl as context-only.
            let record_direction = self.instances[inst_idx].states.get(&key).and_then(|s| {
                s.position
                    .as_ref()
                    .filter(|_| !s.recovery_recorded)
                    .map(|p| p.direction)
            });
            if let Some(direction) = record_direction {
                let recovery_reason = format!("stale_pending_{}", reason);
                let no_prices: HashMap<String, SymbolSnapshot> = HashMap::new();
                self.write_recovery_no_pnl_record(
                    inst_idx,
                    &key,
                    direction,
                    &recovery_reason,
                    now_ts,
                    &no_prices,
                );
            }
            if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                state.pending_entry = None;
                state.pending_exit = None;
                state.position = None;
                state.recovery_recorded = false;
                state.position_guard = false;
                state.last_exit_at = Some(Instant::now());
                state.last_exit_ts = Some(now_ts);
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
#[path = "testing/pending_tests.rs"]
mod pending_tests;
#[cfg(test)]
#[path = "testing/shutdown_grace_tests.rs"]
mod shutdown_grace_tests;
#[cfg(test)]
mod testing;
#[cfg(test)]
#[path = "testing/tests.rs"]
mod tests;
