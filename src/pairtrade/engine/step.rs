//! Orchestrator: `run` / `step` / `step_shared` / `step_for_instance`.
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
//! - `step_shared` — once-per-tick work: kill-switch / risk-ack /
//!   daily-session refresh, fetch latest prices, run BT-restart simulation,
//!   feed the BarBuilder from polled snapshots, persist history.
//! - `step_for_instance` — per-instance signal generation: regime gate,
//!   reconcile pending, evaluate pair, build `PlannedAction` set, place
//!   exits then a single best entry, and emit the dashboard snapshot.
//!
//! Pure relocation; no semantic change.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use dex_connector::PriceUpdate;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::time::{sleep, Duration};

use super::super::apply_post_exit_state;
use super::super::config::PairSpec;
use super::super::defaults::PAIR_SELECTION_INTERVAL_SECS;
use super::super::exit::compute_pnl;
use super::super::funding_history;
use super::super::history_io;
use super::super::market::{
    quote_sanity_check, tick_sanity_check, SymbolSnapshot, MAX_TICK_PRICE_ENVELOPE_BPS,
    MAX_TICK_SPREAD_BPS,
};
use super::super::pair_eval;
use super::super::pnl_log::PnlLogRecord;
use super::super::regime;
use super::super::state::{BtDeferredExit, PendingOrders, Position};
use super::super::stats::PriceSample;
use super::super::status::{ShutdownPosition, ShutdownStatus};
use super::super::util::tail_std;
use super::super::PairTradeEngine;

use super::plan::{PlannedAction, TradeAction};

#[derive(Serialize)]
struct DataDumpEntry<'a> {
    timestamp: i64,
    prices: &'a HashMap<String, SymbolSnapshot>,
}

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
                    // In backtest, we might want to stop on error. For now, just log it.
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

    /// Shared phase: run once per outer step. Fetches the canonical price
    /// tick, advances the ReplayConnector clock exactly once, updates the
    /// engine-wide history + bar builders, and returns the `(price_map,
    /// updated)` pair for the per-instance phase. Returns `Ok(None)` when a
    /// host-shared cooldown is active and every instance should skip.
    async fn step_shared(
        &mut self,
    ) -> Result<Option<(HashMap<String, SymbolSnapshot>, HashSet<String>)>> {
        // Lighter WAF cooldown is host-shared. Any REST call we make here
        // would be rejected anyway and would refresh the rolling window,
        // turning a 60s cooldown into a permanent block. Skip silently.
        // dex-connector logs once on engagement; the email goes out via
        // report_rate_limit. See bot-strategy#35.
        #[cfg(feature = "lighter-sdk")]
        if dex_connector::lighter_waf_cooldown::cooldown_remaining().is_some() {
            return Ok(None);
        }

        self.update_kill_switch_state();
        self.consume_risk_ack();
        self.refresh_daily_session();

        let price_map = self.fetch_latest_prices().await?;

        if let Some(writer) = &mut self.data_dump_writer {
            let dump_entry = DataDumpEntry {
                timestamp: Utc::now().timestamp_millis(),
                prices: &price_map,
            };
            if let Ok(json_string) = serde_json::to_string(&dump_entry) {
                if writer.write_line(&json_string).is_err() {
                    log::error!("[DataDump] Failed to write to dump file");
                }
            }
        }

        // Bar build + history update is engine-wide: all instances read
        // from the same `self.history`, so we must do it exactly once per
        // outer tick before any per-instance decision logic runs.
        let max_history_len = self.max_history_len();
        let now_ts = self.current_now_ts();
        self.load_history_from_disk();

        // BT restart simulation (bot-strategy#27 comment 2026-04-16): when
        // the replay crosses a timestamp listed in
        // `BT_RESTART_TIMESTAMPS_FILE`, re-run `warm_start_states_from_history`
        // to mirror what the live bot does at each systemd restart —
        // re-compute `state.beta` from OLS and re-seed `spread_history`
        // with 240 single-beta spreads. That seeded low-variance history
        // is the mechanism behind the 2026-04-15 06:02 UTC "std collapse"
        // (bot-strategy#62 — now known to be a restart artifact, not a
        // regime break). We fire on crossing, not exact match, because
        // the live dump has a gap (WS down) around the restart second, so
        // the exact `restart_ts` often has no replay record. Each matched
        // ts is removed from the set, so each restart fires at most once.
        let restart_passed = self
            .cfg
            .bt_restart_timestamps
            .as_mut()
            .map(|set| {
                let passed: Vec<i64> = set.iter().filter(|&&t| t <= now_ts).copied().collect();
                for t in &passed {
                    set.remove(t);
                }
                !passed.is_empty()
            })
            .unwrap_or(false);
        if restart_passed {
            log::warn!(
                "[BT_RESTART] simulating live service restart (now_ts={})",
                now_ts
            );
            self.warm_start_states_from_history();
        }
        let mut updated = HashSet::new();
        // Sort symbols before processing so [TICK_FILTER] / [BAR_FORCE_CLOSE]
        // log ordering and bar push ordering are deterministic across
        // builds — HashMap iteration order is intentionally randomized,
        // which previously caused intermittent golden_baseline mismatches.
        let mut sorted_symbols: Vec<&String> = price_map.keys().collect();
        sorted_symbols.sort();
        for symbol in sorted_symbols {
            let snapshot = price_map
                .get(symbol)
                .expect("just enumerated from price_map");
            // bot-strategy#346: drop corrupt orderbook frames before they
            // poison the bar builder / regression history. The data dump
            // above already recorded the raw frame for diagnostics.
            //
            // Logged at INFO (not WARN) because trips are designed-
            // informational — the filter is *protecting* against bad ticks,
            // not raising an actionable alarm. WARN-level was polluting the
            // dashboard's `warn_count_30m` (200-500/30min on Frankfurt due
            // to Lighter's hourly funding-cycle orderbook noise) and
            // burying genuinely-actionable WARNs. error-watch's TICK_FILTER
            // skip pattern (bot-strategy#356) becomes redundant after this
            // but is left in place as a safety net.
            if let Err(reason) =
                tick_sanity_check(snapshot, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS)
            {
                log::info!(
                    "[TICK_FILTER] rejected {} reason={} price={} bid={:?} ask={:?} bid_size={} ask_size={}",
                    symbol,
                    reason.as_str(),
                    snapshot.price,
                    snapshot.bid_price,
                    snapshot.ask_price,
                    snapshot.bid_size,
                    snapshot.ask_size,
                );
                continue;
            }
            // bot-strategy#364: record realized funding rate into the
            // rolling history so exit_fill can attribute per-cycle carry
            // without an external REST fetch. Lighter settles funding
            // hourly; `observe` dedupes unchanged rates so the buffer
            // averages 1 push per symbol per hour.
            self.funding_history
                .observe(symbol, now_ts, snapshot.funding_rate);
            if let Some(builder) = self.bar_builders.get_mut(symbol) {
                // `snapshot.exchange_ts` is ms post bot-strategy#274 / #276;
                // `now_ts` is wall-clock seconds, lift it to ms when the
                // connector did not surface an exchange timestamp.
                let tick_ts = snapshot
                    .exchange_ts
                    .unwrap_or_else(|| now_ts.saturating_mul(1000));
                let mut emits: Vec<(Decimal, i64)> = Vec::new();
                if let Some(close) = builder.push(tick_ts, snapshot.price) {
                    emits.push(close);
                }
                // Defensive backstop (bot-strategy#341): if the bucket has
                // been open longer than 1.5 × window without an emit (e.g.,
                // both WS and polling went quiet), force-close so the bar
                // stream doesn't stall. Live-only — backtest replays must
                // reproduce live-at-the-time behavior byte-exactly, and a
                // synthetic bar in BT would not match a pre-v2 production
                // run.
                if !self.cfg.backtest_mode {
                    let now_ms = now_ts.saturating_mul(1000);
                    if let Some(close) = builder.force_close_if_stale(now_ms) {
                        log::warn!(
                            "[BAR_FORCE_CLOSE] {} synthetic close at ts={} \
                             (no tick advanced bucket within 1.5 × window)",
                            symbol,
                            close.1
                        );
                        emits.push(close);
                    }
                }
                for (close_price, close_ts) in emits {
                    let entry = self.history.entry(symbol.clone()).or_default();
                    let log_price = close_price
                        .to_f64()
                        .ok_or_else(|| anyhow!("invalid price for {}", symbol))?
                        .ln();
                    if entry.back().map(|s| s.ts) != Some(close_ts) {
                        if entry.len() >= max_history_len {
                            entry.pop_front();
                        }
                        entry.push_back(PriceSample {
                            log_price,
                            ts: close_ts,
                        });
                    }
                    updated.insert(symbol.clone());
                    if !self.cfg.backtest_mode {
                        self.bar_emit_log
                            .entry(symbol.clone())
                            .or_default()
                            .push_back(Instant::now());
                    }
                }
            } else {
                log::debug!("no bar builder for {}", symbol);
            }
        }
        if !self.cfg.backtest_mode {
            self.check_bar_rate_canary();
        }

        // bot-strategy#413: run the spread / Kalman / eval pipeline once per
        // pair before the per-instance loop. All variants on a pair share
        // `self.per_pair_state[pair]`, so A/B/C observe byte-identical
        // β / std / z. The eval gate is OR'd across instances so eval
        // cadence matches the most-reactive variant's signal.
        let universe = self.cfg.universe.clone();
        let now_ts_shared = self.current_now_ts();
        for pair in &universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            if !(updated.contains(&pair.base) && updated.contains(&pair.quote)) {
                continue;
            }
            self.step_pair_shared(pair, &key, now_ts_shared);
        }

        self.persist_history_to_disk();

        Ok(Some((price_map, updated)))
    }

    /// Per-pair shared phase (bot-strategy#413). Runs the Kalman update,
    /// pushes the new spread, computes the OR'd re-eval gate across all
    /// `StrategyInstance`s on this pair, and commits the eval result into
    /// `self.per_pair_state[key]` so every variant reads the same
    /// β / spread_history / std / z. Emits the canonical [ZCHECK] +
    /// [KALMAN] diagnostic logs once per pair per tick (was 3× per tick
    /// pre-#413).
    fn step_pair_shared(&mut self, pair: &PairSpec, key: &str, now_ts: i64) {
        let Some(log_a) = self.latest_log_price(&pair.base) else {
            return;
        };
        let Some(log_b) = self.latest_log_price(&pair.quote) else {
            return;
        };
        let hist_a_prev = self
            .history
            .get(&pair.base)
            .and_then(|h| h.iter().rev().nth(1).map(|s| s.log_price));
        let hist_b_prev = self
            .history
            .get(&pair.quote)
            .and_then(|h| h.iter().rev().nth(1).map(|s| s.log_price));

        // Kalman update + spread push must run before we read z_snapshot
        // back out, because push_spread also recomputes
        // last_velocity_sigma_per_min and std_history.
        let metrics_window = self.cfg.metrics_window;
        {
            let Some(shared) = self.per_pair_state.get_mut(key) else {
                return;
            };
            // Per-bar log-return deltas, shared by the Kalman update and the
            // innovation-responsive regime detector. Needs a prior bar
            // (`last_spread`) and both legs' previous log prices.
            let deltas = if shared.last_spread.is_some() {
                match (hist_a_prev, hist_b_prev) {
                    (Some(a_prev), Some(b_prev)) => Some((log_b - b_prev, log_a - a_prev)),
                    _ => None,
                }
            } else {
                None
            };
            if let Some((dx, dy)) = deltas {
                if let Some(ref mut kf) = shared.kalman {
                    kf.update(dx, dy);
                }
                // bot-strategy#494 Phase 1 (shadow): feed the persistent-regime
                // detector the model's one-step innovation = Δspread under the
                // hedging β (`dy − β·dx`), independent of whether the Kalman
                // path is enabled. Capture the active duration before the
                // update so a `Cleared` transition can log how long the shift
                // lasted.
                let innovation = dy - shared.beta * dx;
                let beta = shared.beta;
                let active_secs_before = shared.regime.active_secs(now_ts);
                match shared.regime.update(innovation, now_ts) {
                    regime::RegimeTransition::Activated => log::warn!(
                        "[REGIME] {} persistent-shift ACTIVE event_ts={} cusum={:.2} scale={:.6} norm={:.2} beta={:.4}",
                        key,
                        now_ts,
                        shared.regime.cusum(),
                        shared.regime.residual_scale(),
                        shared.regime.last_normalized(),
                        beta,
                    ),
                    regime::RegimeTransition::Cleared => log::info!(
                        "[REGIME] {} persistent-shift CLEARED event_ts={} after {:.0}s cusum={:.2} scale={:.6}",
                        key,
                        now_ts,
                        active_secs_before,
                        shared.regime.cusum(),
                        shared.regime.residual_scale(),
                    ),
                    regime::RegimeTransition::None => {}
                }
            }
            let spread = log_a - shared.beta * log_b;
            shared.push_spread(spread, metrics_window, &self.cfg);
        }

        // Snapshot derived state post-push.
        let (z_snapshot, velocity, prev_eligible, last_eval_ts) = {
            let Some(shared) = self.per_pair_state.get(key) else {
                return;
            };
            (
                shared.z_score_details(),
                shared.last_velocity_sigma_per_min,
                shared.eligible,
                shared.last_evaluated_ts,
            )
        };
        let current_std = z_snapshot.map(|(_, std, _, _)| std).unwrap_or(0.0);
        let base_std = self
            .per_pair_state
            .get(key)
            .and_then(|s| tail_std(&s.spread_history, metrics_window));
        let z_abs = z_snapshot.map(|(z, _, _, _)| z.abs()).unwrap_or(0.0);

        // OR'd re-eval gate across every StrategyInstance on this pair.
        // Eval cadence is a pair-level concern post-#413; the variant
        // that would have triggered eval drives the cadence for all.
        let bt_eval_force = self
            .cfg
            .bt_eval_timestamps
            .as_ref()
            .map(|set| set.contains(&now_ts));
        let needs_eval_interval = last_eval_ts
            .map(|t| now_ts.saturating_sub(t) >= PAIR_SELECTION_INTERVAL_SECS as i64)
            .unwrap_or(true);
        let mut needs_eval_jump_any = false;
        let mut needs_eval_velocity_any = false;
        let mut vol_spike_any = false;
        let cfg_entry_z_base = self.cfg.default_pair_params.entry_z_base;
        for inst_idx in 0..self.instances.len() {
            let pp = self.pair_params_for(inst_idx, key);
            let z_entry = self.instances[inst_idx]
                .states
                .get(key)
                .map(|s| s.z_entry)
                .unwrap_or(cfg_entry_z_base);
            if z_abs >= z_entry * pp.reeval_jump_z_mult {
                needs_eval_jump_any = true;
            }
            if velocity.abs() >= pp.spread_velocity_max_sigma_per_min * pp.reeval_jump_z_mult {
                needs_eval_velocity_any = true;
            }
            if let Some(bs) = base_std {
                if bs > 1e-9 && current_std / bs >= pp.vol_spike_mult {
                    vol_spike_any = true;
                }
            }
        }
        let should_eval = match bt_eval_force {
            Some(force) => force,
            None => {
                needs_eval_interval
                    || needs_eval_jump_any
                    || needs_eval_velocity_any
                    || vol_spike_any
            }
        };

        let eval = if should_eval {
            let res = pair_eval::evaluate_pair(&self.cfg, &self.history, pair);
            if let Some(ref e) = res {
                log::info!(
                    "[EVAL] {} beta_s={:.3} beta_l={:.3} beta={:.3} hl={:.2}h p={:.3} eligible={} score={:.3}",
                    key,
                    e.beta_short,
                    e.beta_long,
                    e.beta_eff,
                    e.half_life_hours,
                    e.adf_p_value,
                    e.eligible,
                    e.score
                );
            } else {
                let (avail_a, avail_b) = (
                    self.history.get(&pair.base).map(|h| h.len()).unwrap_or(0),
                    self.history.get(&pair.quote).map(|h| h.len()).unwrap_or(0),
                );
                let pp = &self.cfg.default_pair_params;
                log::debug!(
                    "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                    key,
                    pair.base,
                    avail_a,
                    pp.lookback_hours_long.max(pp.lookback_hours_short) * 3600
                        / self.cfg.trading_period_secs,
                    (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                    self.cfg.warm_start_mode
                );
                log::debug!(
                    "[EVAL] {} insufficient history ({}:{}, need long/short (strict) {} / {}, mode={:?})",
                    key,
                    pair.quote,
                    avail_b,
                    pp.lookback_hours_long.max(pp.lookback_hours_short) * 3600
                        / self.cfg.trading_period_secs,
                    (pp.lookback_hours_short * 3600) / self.cfg.trading_period_secs,
                    self.cfg.warm_start_mode
                );
            }
            res
        } else {
            None
        };

        let use_kalman_beta = self.cfg.use_kalman_beta;
        let kalman_min_updates = self.cfg.kalman_min_updates;
        if let Some(eval) = eval {
            if let Some(shared) = self.per_pair_state.get_mut(key) {
                let kf_beta_warm = if use_kalman_beta {
                    shared
                        .kalman
                        .as_ref()
                        .filter(|kf| kf.is_warm(kalman_min_updates))
                        .map(|kf| kf.beta)
                } else {
                    None
                };
                let new_beta = kf_beta_warm.unwrap_or(eval.beta_eff);
                // bot-strategy#472 defense-in-depth — surface a single
                // collapsing-β tick as a WARN + Prom counter. Threshold
                // is "healthy interior" (> 0.5) to "near-floor" (≤ 0.15)
                // in one eval. Caught 5/22 06:30 in retrospect; with
                // this counter wired, a future event surfaces in the
                // dashboard error-watch (#168) without waiting for the
                // operator to notice a PnL anomaly.
                const BETA_COLLAPSE_PREV_FLOOR: f64 = 0.5;
                const BETA_COLLAPSE_NEW_CEILING: f64 = 0.15;
                if shared.beta > BETA_COLLAPSE_PREV_FLOOR && new_beta <= BETA_COLLAPSE_NEW_CEILING {
                    log::warn!(
                        "[BETA_COLLAPSE] {} beta {:.4} -> {:.4} \
                         (beta_short={:.4} beta_long={:.4}) — possible corrupt-bar event; \
                         see bot-strategy#472",
                        key,
                        shared.beta,
                        new_beta,
                        eval.beta_short,
                        eval.beta_long,
                    );
                    // β is per-pair (shared across A/B/C variants), so
                    // we use "*" for the variant label — matches the
                    // convention used by ENTRY_OVERSIZE_CAPPED_TOTAL
                    // for pair-level events.
                    crate::pairtrade::prom::BETA_COLLAPSE_EVENT_TOTAL
                        .with_label_values(&["*", key])
                        .inc();
                }
                shared.beta = new_beta;
                shared.beta_short = eval.beta_short;
                shared.beta_long = eval.beta_long;
                shared.half_life_hours = eval.half_life_hours;
                shared.adf_p_value = eval.adf_p_value;
                shared.eligible = eval.eligible;
                shared.p_value_weighted_score = eval.score;
                shared.beta_gap = eval.beta_gap;
                shared.last_evaluated = Some(Instant::now());
                shared.last_evaluated_ts = Some(now_ts);
                if prev_eligible != shared.eligible {
                    log::info!(
                        "[ELIGIBILITY] {} -> {} (p={:.3} hl={:.2}h beta_gap={:.3})",
                        key,
                        shared.eligible,
                        shared.adf_p_value,
                        shared.half_life_hours,
                        (shared.beta_short - shared.beta_long).abs()
                    );
                }
            }
        }

        // Canonical [ZCHECK] + [KALMAN] diagnostics (one emit per pair,
        // pre-#413 was once per StrategyInstance — 3× spam on A/B/C).
        let base_first_ts = self
            .history
            .get(&pair.base)
            .and_then(|h| h.front())
            .map(|s| s.ts);
        let quote_first_ts = self
            .history
            .get(&pair.quote)
            .and_then(|h| h.front())
            .map(|s| s.ts);
        let base_bar = self.history.get(&pair.base).and_then(|h| h.back()).cloned();
        let quote_bar = self
            .history
            .get(&pair.quote)
            .and_then(|h| h.back())
            .cloned();
        if let (Some(ba), Some(bq), Some(shared)) =
            (base_bar, quote_bar, self.per_pair_state.get(key))
        {
            if let Some((z, std, mean, latest)) = shared.z_score_details() {
                log::info!(
                    "[ZCHECK] {} bucket_ts={} bar_first_a={} bar_first_b={} bar_last_b={} \
                     close_a={:.6} close_b={:.6} \
                     beta_eff={:.4} beta_s={:.4} beta_l={:.4} mean={:.6} std={:.6} \
                     spread={:.6} z={:.4} hist={}",
                    key,
                    ba.ts,
                    base_first_ts.unwrap_or(0),
                    quote_first_ts.unwrap_or(0),
                    bq.ts,
                    ba.log_price,
                    bq.log_price,
                    shared.beta,
                    shared.beta_short,
                    shared.beta_long,
                    mean,
                    std,
                    latest,
                    z,
                    shared.spread_history.len(),
                );
            }
            if use_kalman_beta {
                if let Some(ref kf) = shared.kalman {
                    log::info!(
                        "[KALMAN] {} kalman_beta={:.4} ols_beta={:.4} diff={:.4} p={:.6} warm={}",
                        key,
                        kf.beta,
                        shared.beta,
                        kf.beta - shared.beta,
                        kf.p,
                        kf.is_warm(kalman_min_updates),
                    );
                }
            }
        }
    }

    /// Feed a single WebSocket price tick into the BarBuilder for `symbol`,
    /// refining the in-progress bucket close via `update_close_only`. The
    /// polling arm in `step_shared` retains exclusive bucket-emit authority
    /// — this fn never appends to `self.history` and never advances the
    /// builder past the current bucket.
    ///
    /// Phase 2 v2 (bot-strategy#341): keeping bucket-emit single-sourced
    /// avoids the original Phase 2 class of bugs where the WS arm could
    /// silently fail to emit across a bucket boundary while the polling
    /// arm was disabled, freezing β for hours.
    pub(in crate::pairtrade) fn ingest_price_update(&mut self, update: PriceUpdate) {
        // bot-strategy#472 — the WS arm previously passed `update.mid_price`
        // straight to `update_close_only` with zero sanity checking. The
        // polling arm above runs the same Lighter book through
        // `tick_sanity_check`, but the WS arm bypassed it. Result: a
        // corrupt orderbook frame (Frankfurt 2026-05-22 06:31 UTC, ETH
        // bid=$1770 ask=$3188, 5,700 bps spread) committed an outlier
        // bar close that dominated `var(ETH log-price)` in the 240-bar
        // OLS regression and pinned β to the floor clamp for ~1h47m
        // (issue #472 RCA). `PriceUpdate` doesn't carry order sizes, so
        // we use the price-only `quote_sanity_check` here — same
        // spread / envelope / crossed-book constants as the polling
        // path; the empty_size gates that only the polling path
        // surfaces aren't reachable from WS data.
        if let Err(reason) = quote_sanity_check(
            Some(update.best_bid),
            Some(update.best_ask),
            update.mid_price,
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        ) {
            log::info!(
                "[TICK_FILTER_WS] rejected {} reason={} mid={} bid={} ask={}",
                update.symbol,
                reason.as_str(),
                update.mid_price,
                update.best_bid,
                update.best_ask,
            );
            return;
        }
        let Some(builder) = self.bar_builders.get_mut(&update.symbol) else {
            log::debug!("[WS_BARS] no bar builder for {}", update.symbol);
            return;
        };
        builder.update_close_only(update.timestamp as i64, update.mid_price);
    }

    /// Sustained bar-emit-rate canary (bot-strategy#341). Walks
    /// `bar_emit_log`, drops entries older than the rolling window, and
    /// warns when emit-rate falls below the jitter floor. Rate-limited
    /// to one WARN per symbol per 60 s. Designed to surface the original
    /// Phase 2 β-freeze symptom (≤1 bar / 4 min for 78 h) within
    /// minutes, but tuned wider than 0.8× expected to ride out 60 s-bar
    /// cadence jitter (Tokyo Lighter Phase B canary on master 3e997d4,
    /// 2026-05-12 13:43–15:13 UTC, 2 spurious WARNs at n=1 over 90 s,
    /// no [BAR_FORCE_CLOSE]).
    pub(in crate::pairtrade) fn check_bar_rate_canary(&mut self) {
        let now = Instant::now();
        // 180 s window + 180 s minimum observation: with Lighter polling
        // jitter (3.5–6.5 s) the bucket-crossing tick does not always
        // arrive in the same 60 s slice, so a 120 s window observes n=1
        // at the jitter floor. 180 s ⇒ jitter floor is n=2 (≈ 0.67 /min).
        let window = Duration::from_secs(180);
        let warn_cooldown = Duration::from_secs(60);
        let min_observation = Duration::from_secs(180);

        // Threshold is 2/3 of expected: n=2 over 180 s ≈ 0.67 /min ⇒
        // healthy (jitter floor); n=1 over 180 s ≈ 0.33 /min ⇒ WARN
        // (real stall — would also trigger [BAR_FORCE_CLOSE] downstream).
        let period_secs = self.cfg.trading_period_secs.max(1);
        let expected_per_min = 60.0 / period_secs as f64;
        let threshold_per_min = (expected_per_min * 2.0 / 3.0).max(0.05);

        let symbols: Vec<String> = self.bar_emit_log.keys().cloned().collect();
        for symbol in symbols {
            let log = self.bar_emit_log.get_mut(&symbol).expect("just enumerated");
            while let Some(front) = log.front() {
                if now.duration_since(*front) > window {
                    log.pop_front();
                } else {
                    break;
                }
            }
            let count = log.len();
            let oldest = log.front().copied();
            let observed_for = oldest.map(|t| now.duration_since(t)).unwrap_or_default();
            if observed_for < min_observation {
                continue;
            }
            let rate_per_min = count as f64 / (observed_for.as_secs_f64() / 60.0).max(1e-9);
            if rate_per_min >= threshold_per_min {
                continue;
            }
            let last_warn = self.last_bar_rate_warn.get(&symbol).copied();
            if last_warn
                .map(|t| now.duration_since(t) < warn_cooldown)
                .unwrap_or(false)
            {
                continue;
            }
            log::warn!(
                "[BAR_RATE] {} rate={:.2}/min over {:.0}s (n={}, threshold={:.2}/min, \
                 expected={:.2}/min) — bar emission stalled, investigate WS/polling",
                symbol,
                rate_per_min,
                observed_for.as_secs_f64(),
                count,
                threshold_per_min,
                expected_per_min,
            );
            self.last_bar_rate_warn.insert(symbol, now);
        }
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
                    if let Some(state) = self.instances[inst_idx].states.get_mut(&plan.key) {
                        state.position = None;
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
                                            &self.funding_history,
                                            &plan.pair.base,
                                            &plan.pair.quote,
                                            p.entered_ts,
                                            now_ts,
                                            direction,
                                            sa,
                                            pa,
                                            sb,
                                            pb,
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
                            .with_trade_details(
                                entry_a,
                                entry_b,
                                price_a.to_f64(),
                                price_b.to_f64(),
                                Some(beta),
                                Some(z),
                                self.per_pair_state
                                    .get(&plan.key)
                                    .and_then(|s| s.last_spread.map(|_| z)),
                                hold_secs,
                            );
                            if ticks_observed > 0 {
                                record = record.with_funding(carry_usd, ticks_observed);
                            }
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
                let (notional_scale_param, notional_floor_param) = {
                    let pp = self.pair_params_for(inst_idx, &pair_key);
                    (pp.beta_gap_notional_scale, pp.beta_gap_notional_floor)
                };
                let notional_scale = crate::pairtrade::sizing::beta_gap_notional_scale(
                    beta_gap,
                    notional_scale_param,
                    notional_floor_param,
                );
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

    fn step_write_status_snapshot(&mut self, inst_idx: usize) {
        {
            let risk = self.daily_risk_snapshot(inst_idx);
            let session_risk = self.session_risk_snapshot(inst_idx);
            let circuit_breaker = self.circuit_breaker_snapshot(inst_idx);
            let kill_switch_active = self.kill_switch_active;
            let funding_today = self.instances[inst_idx].funding_carry_today;
            if let Some(reporter) = &mut self.instances[inst_idx].status_reporter {
                reporter.set_daily_risk(risk);
                reporter.set_session_risk(session_risk);
                reporter.set_circuit_breaker(circuit_breaker);
                reporter.set_kill_switch(kill_switch_active);
                reporter.set_funding_today(funding_today);
                if let Err(err) =
                    reporter.write_snapshot_if_due(&self.open_positions, self.positions_ready)
                {
                    log::warn!("[STATUS] failed to write status: {:?}", err);
                }
            }
        }
    }
}
