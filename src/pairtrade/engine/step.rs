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
//!   The gate setup lives in `gating`, the order placement/simulation in
//!   `execute` (bot-strategy#444).
//!
//! Pure relocation; no semantic change.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use dex_connector::PriceUpdate;
use tokio::time::{sleep, Duration};

use super::super::history_io;
use super::super::market::SymbolSnapshot;
use super::super::state::PositionDirection;
use super::super::status::{ShutdownPosition, ShutdownStatus};
use super::super::PairTradeEngine;

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
}
