//! Orchestrator: `run` / `step` / `step_for_instance`.
//!
//! Final cluster of the god-module split (#291). All four fns stayed in
//! the same module until the rest of the engine had been carved out, so
//! that the orchestrator could keep cross-cluster `&mut self` calls with
//! no intermediate borrow gymnastics.
//!
//! Cluster layout:
//! - `run` — top-level lifecycle. Loads history / risk state / warm-start
//!   seeds, then drives the BT replay or the live wall-clock-aligned tick
//!   loop. Owns SIGTERM/SIGINT graceful shutdown.
//! - `step` — single tick: re-points `self.connector` at instances\[0\] for
//!   the shared phase, then per-instance.
//! - `shared_tick::step_shared` — once-per-tick shared work: kill-switch / risk-ack /
//!   daily-session refresh, fetch latest prices, run BT-restart simulation,
//!   feed the BarBuilder from polled snapshots, persist history.
//! - `step_for_instance` — per-instance signal generation: regime gate,
//!   reconcile pending, evaluate pair, build `PlannedAction` set, place
//!   exits then a single best entry, and emit the dashboard snapshot.
//!
//! Pure relocation; no semantic change.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use dex_connector::PriceUpdate;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::time::{sleep, Duration};

use super::super::apply_post_exit_state;
use super::super::exit::compute_pnl;
use super::super::funding_history;
use super::super::history_io;
use super::super::market::SymbolSnapshot;
use super::super::pnl_log::{PnlLogRecord, PnlTradeDetails};
use super::super::regime;
use super::super::state::{BtDeferredExit, PendingOrders, Position, PositionDirection};
use super::super::status::{ShutdownPosition, ShutdownStatus};
use super::super::PairTradeEngine;

use super::plan::{PlannedAction, TradeAction};

#[derive(Clone, Copy)]
pub(super) struct StepSetup {
    pub(super) maintenance_block_entries: bool,
    pub(super) vol_median: f64,
    pub(super) regime_ok: bool,
    pub(super) positions_clear: bool,
}

impl PairTradeEngine {
    /// Dump the `pairtrade_entry_reject_total` counter as a single
    /// `[ENTRY_REJECT_SUMMARY]` log block. Used at BT end-of-data so the
    /// breakdown is recoverable from the BT log without a /metrics
    /// scrape. Live runs read the counter via Prometheus instead.
    fn log_entry_reject_summary(&self) {
        use crate::pairtrade::prom::{ENTRY_REJECT_TOTAL, KNOWN_ENTRY_REJECT_REASONS};
        for inst in &self.instances {
            for pair in &self.cfg.universe {
                let pair_key = format!("{}/{}", pair.base, pair.quote);
                let mut parts: Vec<String> = Vec::new();
                let mut total: u64 = 0;
                for reason in KNOWN_ENTRY_REJECT_REASONS {
                    let v = ENTRY_REJECT_TOTAL
                        .with_label_values(&[inst.id.as_str(), pair_key.as_str(), reason])
                        .get();
                    if v > 0 {
                        parts.push(format!("{}={}", reason, v));
                        total += v;
                    }
                }
                if total > 0 {
                    log::info!(
                        "[ENTRY_REJECT_SUMMARY] variant={} pair={} total={} {}",
                        inst.id,
                        pair_key,
                        total,
                        parts.join(" ")
                    );
                }
            }
        }
    }

    /// Dump peak detector state at BT end-of-data. Transition-only logs are
    /// insufficient for threshold calibration when a known shift raises the
    /// CUSUM but does not cross the current `h_on`.
    fn log_regime_summary(&self) {
        for (pair, shared) in &self.per_pair_state {
            log::info!(
                "[REGIME_SUMMARY] pair={} peak_cusum={:.4} peak_event_ts={} final_cusum={:.4} active={}",
                pair,
                shared.regime.peak_cusum(),
                shared.regime.peak_cusum_ts().unwrap_or(0),
                shared.regime.cusum(),
                shared.regime.is_active(),
            );
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        log::info!("[CONFIG] DEX_NAME is: {}", self.cfg.dex_name);
        log::info!(
            "[CONFIG] FEE_BPS={} SLIPPAGE_BPS={} post_only_supported={} post_only_enabled={}",
            self.cfg.fee_bps,
            self.cfg.slippage_bps,
            self.post_only_supported(),
            self.should_post_only()
        );
        self.load_history_from_disk();
        self.load_risk_state();
        // BT warm-start: load a live history snapshot so the replay starts
        // with an identical spread_history / beta to the live bot, instead
        // of building from scratch over the first 4 hours of data.
        if self.cfg.backtest_mode {
            if let Some(ref path) = self.cfg.bt_warm_start_snapshot {
                let max_len = self.max_history_len();
                let mut loaded_spreads: HashMap<String, VecDeque<f64>> = HashMap::new();
                let mut loaded_betas: HashMap<String, f64> = HashMap::new();
                let mut loaded_kalman: HashMap<String, history_io::KalmanSnapshot> = HashMap::new();
                history_io::load_history_snapshot_for_bt(
                    &mut self.history,
                    &mut loaded_spreads,
                    &mut loaded_betas,
                    &mut loaded_kalman,
                    std::path::Path::new(path),
                    max_len,
                );
                // bot-strategy#413: shared per-pair store. Seed
                // spread_history / β / Kalman directly; pre-#413 we
                // fanned out across instances.
                let kalman_q = self.cfg.kalman_q;
                let kalman_r = self.cfg.kalman_r;
                for (pair_key, spreads) in &loaded_spreads {
                    if let Some(shared) = self.per_pair_state.get_mut(pair_key) {
                        shared.last_spread = spreads.back().copied();
                        shared.spread_history = spreads.clone();
                    }
                }
                for (pair_key, beta) in &loaded_betas {
                    if let Some(shared) = self.per_pair_state.get_mut(pair_key) {
                        shared.beta = *beta;
                    }
                }
                for (pair_key, kalman) in &loaded_kalman {
                    if let Some(shared) = self.per_pair_state.get_mut(pair_key) {
                        shared.kalman = Some(super::super::kalman::KalmanBeta::from_snapshot(
                            kalman.beta,
                            kalman.p,
                            kalman.updates,
                            kalman_q,
                            kalman_r,
                        ));
                    }
                }
            }
        }
        self.warm_start_states_from_history();

        if self.replay_connector.is_some() {
            // --- Backtest Mode ---
            log::info!("[BACKTEST] Running in backtest mode.");
            loop {
                if let Err(e) = self.step().await {
                    if self.regime_series_writer.is_some() {
                        return Err(e)
                            .context("backtest step failed while writing BT_REGIME_SERIES_FILE");
                    }
                    // Existing replay behavior: log step errors and keep scanning.
                    log::error!("[BACKTEST] Step failed: {:?}", e);
                }
                // Advance the replay connector to the next data point
                let has_more = {
                    let replay = self
                        .replay_connector
                        .as_ref()
                        .expect("replay connector should exist in backtest mode");
                    replay.tick()
                };
                if !has_more {
                    log::info!("[BACKTEST] End of data file reached. Backtest finished.");
                    self.log_entry_reject_summary();
                    self.log_regime_summary();
                    if let Some(writer) = self.regime_series_writer.as_mut() {
                        use std::io::Write;
                        writer.flush().context("flush BT_REGIME_SERIES_FILE")?;
                    }
                    break;
                }
            }
        } else {
            // --- Live Mode ---
            log::info!("[LIVE] Running in live mode.");
            // Allow the per-instance WS streams to warm up and populate their
            // position snapshots BEFORE force_close_on_startup probes them;
            // otherwise the first get_positions attempts fail with "positions
            // not ready from websocket" and the retry loop used to call
            // close_all_positions blindly (which in turn REST-hit /account and
            // occasionally 429'd during the multi-instance startup burst).
            // bot-strategy#143.
            sleep(Duration::from_secs(5)).await;
            if self.cfg.force_close_on_startup {
                self.force_close_on_startup().await?;
            }
            // Subscribe to the connector's real-time price-update broadcast
            // (bot-strategy#341 Phase 2 v2). On Lighter the connector emits
            // one PriceUpdate per orderbook change; the WS arm in the main
            // tokio::select drains them and feeds `update_close_only` so
            // bots subscribed to the same feed converge on the same close.
            // The polling arm in step_shared retains exclusive bucket-emit
            // authority. Connectors without a push channel (Extended /
            // Hyperliquid today) return Err here and ws_price_rx stays
            // None, leaving step_shared as the sole bar driver.
            let primary_connector = if !self.instances.is_empty() {
                self.instances[0].connector.clone()
            } else {
                self.connector.clone()
            };
            let mut ws_price_rx: Option<tokio::sync::broadcast::Receiver<PriceUpdate>> =
                match primary_connector.subscribe_price_updates() {
                    Ok(rx) => {
                        log::info!(
                            "[WS_BARS] subscribed to connector price-update broadcast; \
                         bar closes will be refined by WS ticks (polling still emits)"
                        );
                        Some(rx)
                    }
                    Err(e) => {
                        log::info!(
                            "[WS_BARS] connector does not publish price updates ({}); \
                         polling arm is the sole bar driver",
                            e
                        );
                        None
                    }
                };
            // Wall-clock aligned ticker: fires at floor(now/interval)*interval + interval boundaries
            // so every bot process observing the same stream ticks at identical wall-clock seconds.
            // This is required on top of the BarBuilder bucket alignment (pairtrade#4): without
            // aligning the tick phase itself, two bots would sample the last tick of a 60s bucket
            // at different wall-clock seconds and therefore see slightly different close prices,
            // which cascades into divergent beta/mean/std/z.
            let interval_secs = self.cfg.interval_secs.max(1);
            fn next_wall_clock_boundary(interval_secs: u64) -> tokio::time::Instant {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_unix_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let interval_ms = interval_secs.saturating_mul(1000);
                let next_boundary_ms = ((now_unix_ms / interval_ms) + 1) * interval_ms;
                let wait_ms = next_boundary_ms.saturating_sub(now_unix_ms);
                tokio::time::Instant::now() + Duration::from_millis(wait_ms)
            }
            let mut next_tick = next_wall_clock_boundary(interval_secs);
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("failed to register SIGTERM handler");
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("failed to register SIGINT handler");

            let grace = Duration::from_secs(self.cfg.shutdown_grace_secs);
            let mut shutdown_deadline: Option<Instant> = None;
            let mut force_shutdown = false;
            loop {
                // Graceful shutdown: exit as soon as positions are flat, or after grace expires.
                if self.shutdown_pending {
                    if self.all_instances_flat() {
                        log::info!("[PAIR] Shutdown: all positions flat, exiting");
                        break;
                    }
                    if let Some(dl) = shutdown_deadline {
                        if Instant::now() >= dl {
                            log::warn!(
                                "[PAIR] Shutdown grace ({}s) expired with {} open positions, force-closing",
                                self.cfg.shutdown_grace_secs,
                                self.total_open_positions()
                            );
                            force_shutdown = true;
                            break;
                        }
                    }
                }

                tokio::select! {
                    // WS price-update arm (bot-strategy#341 Phase 2 v2).
                    // Refines BarBuilder close fields via update_close_only;
                    // does NOT emit. Inline async block releases the borrow
                    // on ws_price_rx each iteration. When the connector
                    // doesn't publish price updates, the inner pending()
                    // never resolves and this arm is effectively disabled.
                    recv_result = async {
                        match ws_price_rx.as_mut() {
                            Some(rx) => Some(rx.recv().await),
                            None => {
                                std::future::pending::<()>().await;
                                None
                            }
                        }
                    } => {
                        match recv_result {
                            Some(Ok(update)) => {
                                self.ingest_price_update(update);
                            }
                            Some(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                                log::warn!(
                                    "[WS_BARS] dropped {} ticks (slow consumer); \
                                     bucket close may briefly fall back to a polled snapshot",
                                    n
                                );
                            }
                            Some(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                                log::error!(
                                    "[WS_BARS] connector broadcast channel closed; \
                                     polling arm continues to drive bars"
                                );
                                ws_price_rx = None;
                            }
                            None => {}
                        }
                    }
                    _ = tokio::time::sleep_until(next_tick) => {
                        next_tick = next_wall_clock_boundary(interval_secs);
                        // Monitor step() execution time. If it exceeds interval_secs,
                        // the next wall-clock boundary will be skipped, causing tick
                        // phase to drift across A/B/C bots and breaking bar alignment
                        // (pairtrade#4). WARN so we can spot it in production logs.
                        let step_start = Instant::now();
                        if let Err(e) = self.step().await {
                            self.log_inconsistent_state_debug(&e).await;
                            log::error!("pairtrade step failed: {:?}", e);
                        }
                        let step_elapsed = step_start.elapsed();
                        let interval = Duration::from_secs(interval_secs);
                        // Warn only on critical overrun (>=1.5x interval), where a
                        // wall-clock tick is genuinely skipped and A/B/C bars drift.
                        // Mild overruns (just past the boundary) are logged at info
                        // so they stay visible without inflating warn_count.
                        let critical = interval + interval / 2;
                        if step_elapsed >= critical {
                            log::warn!(
                                "[STEP_OVERRUN] step() took {:.2}s >= {:.2}s (1.5x interval_secs={}); \
                                 wall-clock tick skipped",
                                step_elapsed.as_secs_f64(),
                                critical.as_secs_f64(),
                                interval_secs
                            );
                        } else if step_elapsed >= interval {
                            log::info!(
                                "[STEP_OVERRUN] step() took {:.2}s >= interval_secs={} (mild)",
                                step_elapsed.as_secs_f64(),
                                interval_secs
                            );
                        }
                    }
                    _ = sigterm.recv() => {
                        if !self.shutdown_pending {
                            let flat = self.all_instances_flat();
                            // DRY_RUN holds only synthetic in-memory positions
                            // (#237 paper-fill); there is nothing on the
                            // exchange to protect, so the grace period adds
                            // restart latency for no benefit. Treat SIGTERM
                            // as immediate exit and skip the close_all_positions
                            // REST call (force_shutdown=false). See bot-strategy#239.
                            if flat || self.cfg.shutdown_grace_secs == 0 || self.cfg.dry_run {
                                log::info!(
                                    "[PAIR] SIGTERM received, shutting down (flat={}, grace={}s, dry_run={})",
                                    flat,
                                    self.cfg.shutdown_grace_secs,
                                    self.cfg.dry_run
                                );
                                force_shutdown = !flat && !self.cfg.dry_run;
                                break;
                            }
                            log::info!(
                                "[PAIR] SIGTERM received, entering graceful shutdown: \
                                 waiting for natural exit of {} open positions (grace={}s). \
                                 Send SIGTERM/SIGINT again to force.",
                                self.total_open_positions(),
                                self.cfg.shutdown_grace_secs
                            );
                            // Surface per-position force_close ETA so operators can
                            // see when each leg will be auto-flushed if it doesn't
                            // exit naturally. Iterates every instance so multi-strategy
                            // shutdown reports the union of A/B/C positions, not
                            // just the first instance. See pairtrade#6, extended in
                            // commit 5 of shigeo-nakamura/bot-strategy#25.
                            let now_ts = chrono::Utc::now().timestamp();
                            let grace_deadline_ts =
                                now_ts + self.cfg.shutdown_grace_secs as i64;
                            let per_instance_positions: Vec<Vec<ShutdownPosition>> = self
                                .instances
                                .iter()
                                .map(|inst| {
                                    let mut out = Vec::new();
                                    for (key, state) in &inst.states {
                                        if let Some(pos) = &state.position {
                                            let pp = inst
                                                .pair_params
                                                .get(key)
                                                .unwrap_or(&inst.default_pair_params);
                                            let elapsed =
                                                now_ts.saturating_sub(pos.entered_ts).max(0);
                                            let remaining = (pp.force_close_secs as i64)
                                                .saturating_sub(elapsed);
                                            let eta_ts =
                                                pos.entered_ts + pp.force_close_secs as i64;
                                            log::info!(
                                                "[PAIR] shutdown: [{}] {} held={}s \
                                                 force_close_secs={} force_close_in={}s",
                                                inst.id,
                                                key,
                                                elapsed,
                                                pp.force_close_secs,
                                                remaining.max(0),
                                            );
                                            out.push(ShutdownPosition {
                                                key: key.clone(),
                                                entered_ts: pos.entered_ts,
                                                force_close_eta_ts: eta_ts,
                                            });
                                        }
                                    }
                                    out
                                })
                                .collect();
                            for (inst, shutdown_positions) in
                                self.instances.iter_mut().zip(per_instance_positions.into_iter())
                            {
                                let earliest_eta = shutdown_positions
                                    .iter()
                                    .map(|p| p.force_close_eta_ts)
                                    .min();
                                if let Some(reporter) = &mut inst.status_reporter {
                                    reporter.set_shutdown_status(Some(ShutdownStatus {
                                        pending: true,
                                        grace_deadline_ts,
                                        force_close_eta_ts: earliest_eta,
                                        positions: shutdown_positions,
                                    }));
                                }
                            }
                            self.shutdown_pending = true;
                            shutdown_deadline = Some(Instant::now() + grace);
                        } else {
                            log::info!("[PAIR] Second SIGTERM received, force-closing immediately");
                            force_shutdown = true;
                            break;
                        }
                    }
                    _ = sigint.recv() => {
                        if self.shutdown_pending {
                            log::info!("[PAIR] SIGINT received during graceful shutdown, force-closing");
                            force_shutdown = true;
                            break;
                        } else {
                            log::info!("[PAIR] SIGINT received, shutting down...");
                            force_shutdown = !self.all_instances_flat();
                            break;
                        }
                    }
                }
            }

            if force_shutdown {
                log::info!("[PAIR] Force-closing all open positions on shutdown");
                if let Err(e) = self.connector.close_all_positions(None).await {
                    log::error!("[PAIR] close_all_positions on shutdown failed: {:?}", e);
                } else if !self.cfg.dry_run && !self.cfg.observe_only {
                    // bot-strategy#514: the shutdown bulk close realises DEX
                    // PnL without flowing through reconcile, and the process
                    // exits before the exchange-snapshot clear could record
                    // it. Only instances on the canonical connector are
                    // covered by this close_all_positions call, so only
                    // record those. On failure the position survives and the
                    // next boot's startup force close records it instead.
                    let now_ts = self.current_now_ts();
                    let to_record: Vec<(usize, String, PositionDirection)> = self
                        .instances
                        .iter()
                        .enumerate()
                        .filter(|(_, inst)| Arc::ptr_eq(&inst.connector, &self.connector))
                        .flat_map(|(idx, inst)| {
                            inst.states.iter().filter_map(move |(key, state)| {
                                state
                                    .position
                                    .as_ref()
                                    .filter(|_| !state.recovery_recorded)
                                    .map(|p| (idx, key.clone(), p.direction))
                            })
                        })
                        .collect();
                    let no_prices: HashMap<String, SymbolSnapshot> = HashMap::new();
                    for (idx, key, direction) in to_record {
                        self.write_recovery_no_pnl_record(
                            idx,
                            &key,
                            direction,
                            "shutdown_force_close",
                            now_ts,
                            &no_prices,
                        );
                        if let Some(state) = self.instances[idx].states.get_mut(&key) {
                            state.position = None;
                            state.recovery_recorded = false;
                        }
                    }
                }
            }
        }
        for inst in self.instances.iter_mut() {
            if let Some(reporter) = &mut inst.status_reporter {
                // Scope the snapshot read + any failure WARN to this
                // variant so the `error_summary` it embeds (and any
                // `[STATUS] failed` warn that would follow) attributes
                // correctly. See bot-strategy#367.
                let _attr = crate::error_counter::CurrentInstanceGuard::enter(&inst.id);
                if let Err(err) =
                    reporter.write_snapshot(&self.open_positions, self.positions_ready)
                {
                    log::warn!("[STATUS] failed to write status: {:?}", err);
                }
            }
        }
        Ok(())
    }

    pub async fn step(&mut self) -> Result<()> {
        // HistogramTimer observes on drop, including early cooldown returns
        // and error paths, so the count tracks every scheduled step attempt.
        let _step_timer = crate::pairtrade::prom::STEP_DURATION_SECONDS
            .with_label_values(&[])
            .start_timer();
        // One process, one shared WS subscription is the goal of #25. Until
        // the connector layer truly merges WS, instances[0]'s connector is
        // the canonical source for the shared price fetch. The per-instance
        // phase below will re-point self.connector at each instance's
        // connector for order placement / balance / sync calls.
        if !self.instances.is_empty() {
            self.connector = self.instances[0].connector.clone();
        }
        let Some((price_map, updated)) = self.step_shared().await? else {
            return Ok(());
        };
        for inst_idx in 0..self.instances.len() {
            self.connector = self.instances[inst_idx].connector.clone();
            // Per-instance attribution scope: every WARN/ERROR emitted by
            // session_dd / daily_dd / equity-fetch / position-sync code
            // paths lands in this variant's bucket rather than spilling
            // across A/B/C. See bot-strategy#367.
            let _attr =
                crate::error_counter::CurrentInstanceGuard::enter(&self.instances[inst_idx].id);
            self.step_for_instance(inst_idx, &price_map, &updated)
                .await?;
        }
        Ok(())
    }

    async fn step_for_instance(
        &mut self,
        inst_idx: usize,
        price_map: &HashMap<String, SymbolSnapshot>,
        updated: &HashSet<String>,
    ) -> Result<()> {
        let setup = self.step_setup(inst_idx, price_map).await?;
        let now_ts = self.current_now_ts();
        let planned = self
            .step_plan_pair_actions(inst_idx, price_map, updated, setup, now_ts)
            .await?;
        self.maybe_log_metrics(inst_idx);
        self.step_execute_exits(inst_idx, &planned, price_map, now_ts)
            .await?;
        self.step_execute_entry(inst_idx, &planned, price_map, now_ts)
            .await?;
        self.step_write_status_snapshot(inst_idx);
        Ok(())
    }

    async fn step_setup(
        &mut self,
        inst_idx: usize,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<StepSetup> {
        // Skip new entries if maintenance is upcoming within 1 hour
        let maintenance_status = self.connector.maintenance_status(1).await;
        let maintenance_block_entries = maintenance_status.is_some();
        if let Some(status) = maintenance_status.as_deref() {
            log::info!(
                "Maintenance/degraded exchange detected ({}); blocking new entries this cycle",
                status
            );
        }
        if let Some(reporter) = &mut self.instances[inst_idx].status_reporter {
            reporter.set_maintenance(maintenance_status.clone());
        }
        crate::pairtrade::prom::MAINTENANCE_ACTIVE
            .with_label_values(&[self.instances[inst_idx].id.as_str()])
            .set(if maintenance_block_entries { 1 } else { 0 });
        // Also stop inflating warn/error counters for the duration of the
        // detected maintenance window (bot-strategy#199). The WS reconnect
        // bursts / 503s / stale-price WARNs that follow are expected fallout
        // and — in addition to being filtered out workflow-side in
        // error-watch.yml — should not accumulate into error_summary.
        // Process-global flag: all A/B/C instances share the same Lighter
        // connector, so the last writer in this tick determines the state
        // and every instance observes the same maintenance verdict.
        crate::error_counter::set_counting_suppressed(maintenance_block_entries);

        self.refresh_equity_if_needed(inst_idx).await?;
        // Phase 3-1: sample current equity into the rolling-peak window
        // and check the session-DD threshold. On breach, this flattens
        // the instance's positions and sets `session_halted=true`; the
        // entry gate below picks up the halt.
        self.update_equity_sample(inst_idx);
        self.evaluate_session_dd(inst_idx).await;
        self.sync_positions_from_exchange(inst_idx, price_map)
            .await?;

        let vol_median = self.compute_vol_median();

        // Regime filter: compute once per step cycle (not per pair)
        let regime_state = if self.cfg.regime_vol_max > 0.0 || self.cfg.regime_trend_max > 0.0 {
            self.history
                .get(&self.cfg.regime_reference_symbol)
                .and_then(|h| {
                    regime::compute_regime(
                        h,
                        self.cfg.regime_vol_window,
                        self.cfg.regime_trend_window,
                    )
                })
        } else {
            None
        };
        let regime_ok = regime::regime_allows_entry(
            regime_state,
            self.cfg.regime_vol_max,
            self.cfg.regime_trend_max,
        );
        if let Some(rs) = regime_state {
            if !regime_ok {
                log::info!(
                    "[REGIME] entry blocked: vol={:.6} (max={:.6}) trend={:.4} (max={:.4}) ref={}",
                    rs.realized_vol,
                    self.cfg.regime_vol_max,
                    rs.trend_strength,
                    self.cfg.regime_trend_max,
                    self.cfg.regime_reference_symbol,
                );
            }
        }

        let positions_clear = self.open_positions.is_empty();
        let has_pending_orders = self.instances[inst_idx]
            .states
            .values()
            .any(|state| state.pending_entry.is_some() || state.pending_exit.is_some());
        if !positions_clear && !has_pending_orders && self.should_log_position_warn("entry_block") {
            log::info!(
                "[POSITION] open positions detected ({} symbols) with no pending orders; blocking new entries",
                self.open_positions.len()
            );
            self.last_position_warn
                .insert("entry_block".to_string(), Instant::now());
        }
        Ok(StepSetup {
            maintenance_block_entries,
            vol_median,
            regime_ok,
            positions_clear,
        })
    }

    async fn step_execute_exits(
        &mut self,
        inst_idx: usize,
        planned: &[PlannedAction],
        price_map: &HashMap<String, SymbolSnapshot>,
        now_ts: i64,
    ) -> Result<()> {
        // Process exits first
        for plan in planned.iter() {
            if let TradeAction::Close {
                direction,
                z,
                beta,
                force,
            } = plan.action
            {
                let qtys = self
                    .exit_sizes_for_pair(inst_idx, &plan.key, &plan.pair, beta, &plan.p1, &plan.p2)
                    .context("exit_sizes_for_pair")?;
                if qtys.0 <= Decimal::ZERO && qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} no open position sizes available; clearing state",
                        plan.key
                    );
                    // bot-strategy#514: record the close context before the
                    // position state is dropped — DEX-side accounting may
                    // still realise PnL for whatever was actually open.
                    let already_recorded = self.instances[inst_idx]
                        .states
                        .get(&plan.key)
                        .map(|s| s.position.is_none() || s.recovery_recorded)
                        .unwrap_or(true);
                    if !already_recorded {
                        self.write_recovery_no_pnl_record(
                            inst_idx,
                            &plan.key,
                            direction,
                            "no_exit_sizes",
                            now_ts,
                            price_map,
                        );
                    }
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = None;
                        state.recovery_recorded = false;
                        state.pending_exit = None;
                        state.position_guard = false;
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                    continue;
                }
                if qtys.0 <= Decimal::ZERO || qtys.1 <= Decimal::ZERO {
                    log::warn!(
                        "[EXIT] {} missing leg size (base={}, quote={}); closing available legs only",
                        plan.key,
                        qtys.0,
                        qtys.1
                    );
                }
                if self.cfg.dry_run {
                    let price_a = price_map
                        .get(&plan.pair.base)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let price_b = price_map
                        .get(&plan.pair.quote)
                        .map(|s| s.price)
                        .unwrap_or_default();
                    let pnl = self.instances[inst_idx]
                        .states
                        .get(&plan.key)
                        .and_then(|s| s.position.as_ref())
                        .and_then(|pos| compute_pnl(pos, price_a, price_b));
                    if let Some(pnl) = pnl {
                        if let Some(pnl_value) = pnl.to_f64() {
                            let pos_ref = self.instances[inst_idx]
                                .states
                                .get(&plan.key)
                                .and_then(|s| s.position.as_ref());
                            let hold_secs =
                                pos_ref.map(|p| now_ts.saturating_sub(p.entered_ts).max(0) as f64);
                            let entry_a = pos_ref
                                .and_then(|p| p.entry_price_a)
                                .and_then(|v| v.to_f64());
                            let entry_b = pos_ref
                                .and_then(|p| p.entry_price_b)
                                .and_then(|v| v.to_f64());
                            let (carry_usd, ticks_observed) = match pos_ref {
                                Some(p) => match (
                                    p.entry_size_a,
                                    p.entry_price_a,
                                    p.entry_size_b,
                                    p.entry_price_b,
                                ) {
                                    (Some(sa), Some(pa), Some(sb), Some(pb)) => {
                                        funding_history::compute_carry_usd(
                                            funding_history::FundingCarryInput {
                                                history: &self.funding_history,
                                                base_symbol: &plan.pair.base,
                                                quote_symbol: &plan.pair.quote,
                                                open_ts: p.entered_ts,
                                                close_ts: now_ts,
                                                direction,
                                                entry_size_a: sa,
                                                entry_price_a: pa,
                                                entry_size_b: sb,
                                                entry_price_b: pb,
                                            },
                                        )
                                    }
                                    _ => (0.0, 0),
                                },
                                None => (0.0, 0),
                            };
                            let mut record = PnlLogRecord::new(
                                &plan.pair.base,
                                &plan.pair.quote,
                                direction,
                                pnl_value,
                                now_ts,
                                "exit_dry_run",
                            )
                            .with_trade_details(PnlTradeDetails {
                                entry_a,
                                entry_b,
                                exit_a: price_a.to_f64(),
                                exit_b: price_b.to_f64(),
                                beta: Some(beta),
                                z_entry: Some(z),
                                z_exit: self
                                    .per_pair_state
                                    .get(&plan.key)
                                    .and_then(|s| s.last_spread.map(|_| z)),
                                hold_secs,
                            });
                            if ticks_observed > 0 {
                                record = record.with_funding(carry_usd, ticks_observed);
                            }
                            let close_reason = self.instances[inst_idx]
                                .states
                                .get(&plan.key)
                                .and_then(|s| s.pending_exit_reason)
                                .unwrap_or("unknown");
                            record = record.with_close_reason(close_reason);
                            self.write_pnl_record(inst_idx, record);
                            self.instances[inst_idx].realized_pnl_today += pnl_value;
                            self.instances[inst_idx].funding_carry_today += carry_usd;
                            // write_pnl_record always bumps total_trades / total_pnl
                            // (now persisted, bot-strategy#320), so the snapshot is
                            // dirty regardless of pnl sign.
                            let mut risk_state_dirty = true;
                            // Collect circuit-breaker history events here
                            // and emit them after the borrow on
                            // self.instances releases, since
                            // record_risk_event_for_instance needs &mut self.
                            let mut cb_event: Option<(&'static str, u32, Option<u64>)> = None;
                            if pnl_value < 0.0 {
                                self.instances[inst_idx].consecutive_losses += 1;
                                risk_state_dirty = true;
                                if let Some(cooldown) = self.cfg.circuit_breaker_cooldown_for(
                                    self.instances[inst_idx].consecutive_losses,
                                ) {
                                    self.instances[inst_idx].circuit_breaker_until =
                                        Some(Instant::now() + cooldown);
                                    self.instances[inst_idx].circuit_breaker_until_ts =
                                        Some(now_ts + cooldown.as_secs() as i64);
                                    log::warn!(
                                        "[CIRCUIT_BREAKER] activated after {} consecutive losses, cooldown {}s",
                                        self.instances[inst_idx].consecutive_losses, cooldown.as_secs()
                                    );
                                    cb_event = Some((
                                        "activated",
                                        self.instances[inst_idx].consecutive_losses,
                                        Some(cooldown.as_secs()),
                                    ));
                                }
                            } else if pnl_value > 0.0 {
                                if self.instances[inst_idx].consecutive_losses > 0 {
                                    log::info!("[CIRCUIT_BREAKER] reset after win (was {} consecutive losses)", self.instances[inst_idx].consecutive_losses);
                                    risk_state_dirty = true;
                                    let was_active =
                                        self.instances[inst_idx].circuit_breaker_until_ts.is_some();
                                    if was_active {
                                        cb_event = Some((
                                            "cleared",
                                            self.instances[inst_idx].consecutive_losses,
                                            None,
                                        ));
                                    }
                                }
                                self.instances[inst_idx].consecutive_losses = 0;
                                self.instances[inst_idx].circuit_breaker_until = None;
                                self.instances[inst_idx].circuit_breaker_until_ts = None;
                            }
                            if risk_state_dirty {
                                self.persist_risk_state();
                            }
                            if let Some((event_type, prior_losses, cooldown_secs)) = cb_event {
                                let detail = match cooldown_secs {
                                    Some(secs) => Some(serde_json::json!({
                                        "consecutive_losses": prior_losses,
                                        "cooldown_secs": secs,
                                        "pnl_value": pnl_value,
                                    })),
                                    None => Some(serde_json::json!({
                                        "prior_losses": prior_losses,
                                        "pnl_value": pnl_value,
                                        "trigger": "winning_trade",
                                    })),
                                };
                                self.record_risk_event_for_instance(
                                    inst_idx,
                                    "circuit_breaker",
                                    event_type,
                                    None,
                                    detail,
                                );
                            }
                        }
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} pnl={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            pnl,
                            now_ts
                        );
                    } else {
                        log::info!(
                            "[EXIT] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} force={} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            force,
                            now_ts
                        );
                    }
                    let inst_id = self.instances[inst_idx].id.clone();
                    let shared_for_exit = self.per_pair_state.get(&plan.key);
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        if self.cfg.backtest_mode && self.cfg.bt_fill_delay_secs > 0 {
                            // Defer position clearing to simulate exchange
                            // fill latency (bot-strategy#69). The deferred
                            // resolve site reads pending_exit_reason to
                            // tag last_stop_loss_at for #316.
                            state.bt_deferred_exit = Some(BtDeferredExit {
                                resolve_at_ts: now_ts + self.cfg.bt_fill_delay_secs,
                            });
                        } else {
                            apply_post_exit_state(
                                state,
                                shared_for_exit,
                                direction,
                                now_ts,
                                &inst_id,
                                plan.key.as_str(),
                            );
                        }
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[EXIT] observe-only mode; skipping close orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    let (legs, exit_taker_takeover_at) = match self
                        .close_pair_orders(&plan.pair, direction, qtys, price_map, force)
                        .await
                    {
                        Ok(out) => out,
                        Err(err) => {
                            self.register_partial_leg_failure(
                                inst_idx, &plan.key, direction, &err, true,
                            );
                            return Err(err);
                        }
                    };
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.pending_exit = Some(PendingOrders {
                            legs,
                            direction,
                            placed_at: Instant::now(),
                            placed_ts_ms: Utc::now().timestamp_millis(),
                            hedge_retry_count: 0,
                            post_only_hybrid: false,
                            exit_taker_takeover_at,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn step_execute_entry(
        &mut self,
        inst_idx: usize,
        planned: &[PlannedAction],
        price_map: &HashMap<String, SymbolSnapshot>,
        now_ts: i64,
    ) -> Result<()> {
        let mut active_symbols: HashSet<String> = self
            .cfg
            .universe
            .iter()
            .filter_map(|pair| {
                let key = format!("{}/{}", pair.base, pair.quote);
                let state = self.instances[inst_idx].states.get(&key)?;
                let is_active = state.position.is_some()
                    || state.pending_entry.is_some()
                    || state.pending_exit.is_some()
                    || state.bt_deferred_exit.is_some()
                    || state.position_guard;
                if is_active {
                    let mut symbols = HashSet::new();
                    symbols.insert(pair.base.clone());
                    symbols.insert(pair.quote.clone());
                    Some(symbols)
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        for symbol in self.open_positions.keys() {
            if self.history.contains_key(symbol) {
                active_symbols.insert(symbol.clone());
            }
        }

        // Among entry candidates, shortlist by model score then pick best by funding->score->liquidity->|z|
        let mut entry_candidates: Vec<&PlannedAction> = planned
            .iter()
            .filter(|p| matches!(p.action, TradeAction::Open { .. }))
            .filter(|p| {
                if active_symbols.is_empty() {
                    return true;
                }
                let overlaps =
                    active_symbols.contains(&p.pair.base) || active_symbols.contains(&p.pair.quote);
                if overlaps {
                    log::debug!(
                        "[OVERLAP] skipping {}/{} due to active symbol overlap",
                        p.pair.base,
                        p.pair.quote
                    );
                }
                !overlaps
            })
            .collect();
        entry_candidates.sort_by(|a, b| {
            self.state_score(inst_idx, &b.key)
                .partial_cmp(&self.state_score(inst_idx, &a.key))
                .unwrap_or(Ordering::Equal)
        });
        let shortlisted: Vec<&PlannedAction> = entry_candidates
            .into_iter()
            .take(self.cfg.max_active_pairs.max(1))
            .collect();
        let best_entry = shortlisted.into_iter().max_by(|a, b| {
            a.net_funding_per_hour
                .partial_cmp(&b.net_funding_per_hour)
                .unwrap_or(Ordering::Equal)
                .then_with(|| {
                    self.state_score(inst_idx, &a.key)
                        .partial_cmp(&self.state_score(inst_idx, &b.key))
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| {
                    a.liquidity_score
                        .partial_cmp(&b.liquidity_score)
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| a.abs_z.partial_cmp(&b.abs_z).unwrap_or(Ordering::Equal))
        });
        if let Some(plan) = best_entry {
            if let TradeAction::Open { direction, z, beta } = plan.action {
                // Force-fresh equity immediately before sizing: entries happen
                // rarely enough that this REST call is cheap, and it keeps
                // notional sized against the current balance rather than the
                // 30-min cache used for dashboard / R-budget. See
                // bot-strategy#156.
                self.fetch_equity_rest(inst_idx).await;
                // bot-strategy#461: shrink notional under beta-uncertainty.
                // Lookup pair-specific params (with default fallback) so the
                // YAML `pairs.<pair>.beta_gap_notional_scale` override works
                // identically to the other Phase 2 filters.
                let pair_key = format!("{}/{}", plan.pair.base, plan.pair.quote);
                let beta_gap = self
                    .per_pair_state
                    .get(&pair_key)
                    .map(|s| s.beta_gap)
                    .unwrap_or(0.0);
                let (
                    notional_scale_param,
                    notional_floor_param,
                    depth_slope,
                    depth_s_min,
                    depth_s_max,
                    entry_z_base,
                ) = {
                    let pp = self.pair_params_for(inst_idx, &pair_key);
                    (
                        pp.beta_gap_notional_scale,
                        pp.beta_gap_notional_floor,
                        pp.depth_size_slope,
                        pp.depth_size_min,
                        pp.depth_size_max,
                        pp.entry_z_base,
                    )
                };
                // bot-strategy#515: concentrate capital on deeper entries.
                // Resolved once here and frozen for the hold (the position
                // qtys carry it implicitly).
                let depth_mult = crate::pairtrade::sizing::depth_size_mult(
                    z.abs(),
                    entry_z_base,
                    depth_slope,
                    depth_s_min,
                    depth_s_max,
                );
                if depth_mult != 1.0 {
                    log::info!(
                        "[DEPTH_SIZE] pair={} z={:.2} entry_z_base={:.2} mult={:.3}",
                        pair_key,
                        z,
                        entry_z_base,
                        depth_mult
                    );
                }
                let notional_scale = crate::pairtrade::sizing::beta_gap_notional_scale(
                    beta_gap,
                    notional_scale_param,
                    notional_floor_param,
                ) * depth_mult;
                crate::pairtrade::prom::ENTRY_NOTIONAL_SCALE
                    .with_label_values(&[self.instances[inst_idx].id.as_str(), pair_key.as_str()])
                    .set(notional_scale);
                let qtys = self
                    .hedged_sizes(
                        inst_idx,
                        &plan.pair,
                        beta,
                        &plan.p1,
                        &plan.p2,
                        notional_scale,
                    )
                    .context("hedged_sizes")?;
                let price_a = price_map
                    .get(&plan.pair.base)
                    .map(|s| s.price)
                    .unwrap_or_default();
                let price_b = price_map
                    .get(&plan.pair.quote)
                    .map(|s| s.price)
                    .unwrap_or_default();
                if self.cfg.dry_run {
                    log::info!(
                            "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                            plan.pair.base,
                            plan.pair.quote,
                            direction,
                            qtys.0,
                            price_a,
                            qtys.1,
                            price_b,
                            z,
                            beta,
                            plan.net_funding_per_hour,
                            now_ts
                        );
                    let inst_id = self.instances[inst_idx].id.clone();
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = Some(Position {
                            direction,
                            entered_at: Instant::now(),
                            entered_ts: now_ts,
                            entry_price_a: Some(price_a),
                            entry_price_b: Some(price_b),
                            entry_size_a: Some(qtys.0),
                            entry_size_b: Some(qtys.1),
                            entry_z: Some(z),
                            // bot-strategy#463: snapshot the effective β
                            // used to size this hedge. Phase 1 only reads
                            // it for drift detection; Phase 2 will use it
                            // as the reference for re-balance qty.
                            entry_beta: Some(beta),
                            last_rehedge_ts: None,
                            rehedge_realized_pnl: None,
                            prev_beta_for_velocity: None,
                        });
                        state.recovery_recorded = false;
                        super::super::prom::LAST_ENTRY_Z
                            .with_label_values(&[&inst_id, plan.key.as_str()])
                            .set(z);
                    }
                } else if self.cfg.observe_only {
                    log::info!(
                        "[ENTRY] observe-only mode; skipping entry orders for {}/{}",
                        plan.pair.base,
                        plan.pair.quote
                    );
                } else {
                    log::info!(
                        "[ENTRY] pair={}/{} direction={:?} size_a={} price_a={} size_b={} price_b={} z={:.2} beta={:.2} carry={:.4} ts={}",
                        plan.pair.base,
                        plan.pair.quote,
                        direction,
                        qtys.0,
                        price_a,
                        qtys.1,
                        price_b,
                        z,
                        beta,
                        plan.net_funding_per_hour,
                        now_ts
                    );
                    let legs = match self
                        .place_pair_orders(inst_idx, &plan.pair, direction, qtys, price_map)
                        .await
                    {
                        Ok(legs) => legs,
                        Err(err) => {
                            self.register_partial_leg_failure(
                                inst_idx, &plan.key, direction, &err, false,
                            );
                            return Err(err);
                        }
                    };
                    // place_pair_orders returns Ok(Vec::new()) when the entry is
                    // gated (hedge-ratio deviation, zero qty). Treat that as
                    // "skip"; setting pending_entry with empty legs would let
                    // all_filled() be vacuously true on the next step and
                    // synthesize a phantom position. See bot-strategy#211.
                    if !legs.is_empty() {
                        let entry_pp = self.pair_params_for(inst_idx, &plan.key).clone();
                        let entry_pp = &entry_pp;
                        let hybrid =
                            entry_pp.entry_post_only_timeout_secs > 0 && self.post_only_supported();
                        if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                            state.pending_entry = Some(PendingOrders {
                                legs,
                                direction,
                                placed_at: Instant::now(),
                                placed_ts_ms: Utc::now().timestamp_millis(),
                                hedge_retry_count: 0,
                                post_only_hybrid: hybrid,
                                // Entry path — exit_taker_takeover_at only
                                // applies to exit pending orders (#408).
                                exit_taker_takeover_at: None,
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
