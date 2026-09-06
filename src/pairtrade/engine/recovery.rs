//! Startup / recovery flows for `PairTradeEngine`.
//!
//! Handles the bot's bookkeeping when the live exchange state and the
//! engine's in-memory state can disagree — at boot, after a force-close,
//! or when a single leg is left without its hedge:
//!
//! - `force_close_on_startup` — at boot, cancel orders and force-flatten
//!   any pre-existing position so the engine starts from a clean slate.
//! - `force_close_all_positions` — emergency close path called from the
//!   reconcile loop after exit retries are exhausted.
//! - `sync_positions_from_exchange` — reconcile each per-pair `state`
//!   against the connector's position snapshot once per tick.
//! - `try_close_unhedged_leg` — submit a market reduce-only order to
//!   neutralize a leg whose hedge counterpart has gone missing.
//! - `format_positions_summary` — pretty-print a position list for log
//!   / email output (cluster-internal helper).
//!
//! Pure relocation from the god-module split (#291); no semantic change.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use dex_connector::{DexError, FilledOrder, OrderSide, PositionSnapshot};
use rust_decimal::Decimal;
use tokio::time::sleep;

use super::super::engine;
use super::super::instance::ExternalFlattenFills;
use super::super::market::SymbolSnapshot;
use super::super::pnl_log;
use super::super::state::{Position, PositionDirection};
use super::super::PairTradeEngine;
use super::reconcile::ExitPnlInputs;
use crate::email_client::EmailClient;

/// How long the exchange-snapshot clear waits for an out-of-band flatten's
/// fill events before giving up on booking them (bot-strategy#932). Also
/// bounds `flatten_booking_in_progress`, so a flatten that never clears on
/// the venue cannot suppress normal exit planning for longer than this.
pub(in crate::pairtrade) const EXTERNAL_FLATTEN_FILL_GRACE: Duration = Duration::from_secs(30);

/// Outcome of `PairTradeEngine::try_book_external_flatten`. bot-strategy#932.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pairtrade) enum FlattenBooking {
    /// `exit_fill` record written and realized PnL recorded.
    Booked,
    /// Flatten fills not visible yet; keep the local position and retry.
    Deferred,
    /// Cannot attribute fills with full coverage; write `recovery_no_pnl`.
    Unavailable,
}

impl PairTradeEngine {
    fn configured_startup_symbols(&self) -> Vec<String> {
        let mut symbols = self
            .cfg
            .universe
            .iter()
            .flat_map(|pair| [pair.base.clone(), pair.quote.clone()])
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();
        symbols
    }

    /// Refuse to mutate an account that contains positions outside the
    /// configured pair universe. A Lighter account can be shared by another
    /// actor; startup cleanup must never interpret that actor's position as
    /// stale pairtrade exposure and MARKET-close it (bot-strategy#799).
    fn ensure_startup_positions_in_universe(&self, positions: &[PositionSnapshot]) -> Result<()> {
        let configured_symbols = self.configured_startup_symbols();
        let configured = configured_symbols
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let unexpected = positions
            .iter()
            .filter(|position| {
                position.sign != 0
                    && position.size > Decimal::ZERO
                    && !configured.contains(position.symbol.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        if unexpected.is_empty() {
            return Ok(());
        }

        let unexpected_summary = Self::format_positions_summary(&unexpected);
        let configured_summary = configured_symbols.join(",");
        log::error!(
            "[Startup] refusing cleanup: unexpected positions outside configured universe [{}]: {}",
            configured_summary,
            unexpected_summary
        );
        Err(anyhow!(
            "startup cleanup blocked by unexpected positions outside configured universe [{}]: {}",
            configured_summary,
            unexpected_summary
        ))
    }

    fn format_positions_summary(positions: &[PositionSnapshot]) -> String {
        let mut parts = Vec::with_capacity(positions.len());
        for position in positions {
            let side = match position.sign.cmp(&0) {
                Ordering::Greater => "LONG",
                Ordering::Less => "SHORT",
                Ordering::Equal => "FLAT",
            };
            let entry = position
                .entry_price
                .map(|price| price.to_string())
                .unwrap_or_else(|| "n/a".to_string());
            parts.push(format!(
                "{} {} size={} entry={}",
                position.symbol, side, position.size, entry
            ));
        }
        parts.join(", ")
    }

    /// Partition a position list into `(closable, dust)`.
    ///
    /// A position whose size is below the venue's per-symbol minimum order
    /// size can never be submitted to `close_all_positions` — the connector
    /// rejects sub-min sizes (`round_size_for_market` → `InvalidInput`), so
    /// the startup force-close would retry it `attempts` times and then
    /// escalate with a "still open" ERROR + email on every restart
    /// (bot-strategy#487: a 0.00001 BTC dust SHORT below Extended's 0.0001
    /// min). Such positions are treated as already flat: never closed, never
    /// escalated. Symbols whose min order size is unavailable (ticker fetch
    /// failed, or the venue advertises no minimum) are treated as closable so
    /// a genuine position is never silently skipped.
    async fn split_dust_positions(
        &self,
        positions: Vec<PositionSnapshot>,
    ) -> (Vec<PositionSnapshot>, Vec<PositionSnapshot>) {
        let mut closable = Vec::new();
        let mut dust = Vec::new();
        for position in positions {
            let is_dust = match self.connector.get_ticker(&position.symbol, None).await {
                Ok(ticker) => ticker
                    .min_order
                    .is_some_and(|min_order| position.size < min_order),
                Err(err) => {
                    log::warn!(
                        "[Startup] dust check: get_ticker {} failed, treating as closable: {:?}",
                        position.symbol,
                        err
                    );
                    false
                }
            };
            if is_dust {
                dust.push(position);
            } else {
                closable.push(position);
            }
        }
        (closable, dust)
    }

    /// Poll `get_positions()` until the WS account snapshot has arrived or
    /// `timeout` elapses. Lighter's connector returns
    /// `DexError::Transient("positions not ready from websocket")` until the
    /// `subscribed/account_all` frame populates the cache; cold-start
    /// typically takes 20-30 s on Frankfurt (market catalog load + WS
    /// handshake), so the legacy `attempts × wait_secs` retry budget on the
    /// caller side fires its 3 WARNs and an ERROR before WS is even
    /// connected (bot-strategy#405). Treating WS-not-ready as a wait state
    /// here keeps the WARN/ERROR for genuine failures only.
    ///
    /// Returns the first available position snapshot. Any non-readiness
    /// error and timeout fail startup before order cancellation or position
    /// closing, preserving the mutation-free preflight invariant from #799.
    async fn wait_for_startup_positions(&self, timeout: Duration) -> Result<Vec<PositionSnapshot>> {
        const POLL_INTERVAL: Duration = Duration::from_secs(2);
        const LOG_INTERVAL: Duration = Duration::from_secs(10);
        let start = Instant::now();
        let mut next_log = start + LOG_INTERVAL;
        loop {
            match self.connector.get_positions().await {
                Err(DexError::Transient(ref msg)) if msg.contains("not ready from websocket") => {
                    let now = Instant::now();
                    if now.duration_since(start) >= timeout {
                        return Err(anyhow!(
                            "startup positions snapshot not ready after {}s",
                            timeout.as_secs()
                        ));
                    }
                    if now >= next_log {
                        log::info!(
                            "[Startup] waiting for WS positions snapshot ({}s elapsed)",
                            now.duration_since(start).as_secs()
                        );
                        next_log = now + LOG_INTERVAL;
                    }
                    sleep(POLL_INTERVAL).await;
                }
                Ok(positions) => return Ok(positions),
                Err(err) => {
                    return Err(anyhow!("startup get_positions failed: {:?}", err));
                }
            }
        }
    }

    pub(in crate::pairtrade) async fn force_close_on_startup(&self) -> Result<()> {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::info!(
                "[Startup] DRY RUN/OBSERVE ONLY: Would cancel all orders and close all positions"
            );
            return Ok(());
        }
        let attempts = self.cfg.startup_force_close_attempts.max(1);
        let wait_secs = self.cfg.startup_force_close_wait_secs;
        log::info!(
            "[Startup] Force closing any existing orders/positions (attempts={}, wait_secs={})",
            attempts,
            wait_secs
        );
        let configured_symbols = self.configured_startup_symbols();
        if configured_symbols.is_empty() {
            return Err(anyhow!(
                "startup cleanup blocked: configured pair universe is empty"
            ));
        }

        // bot-strategy#799: the account may contain positions owned by a
        // different actor. Inspect before the first mutation, then cancel
        // only configured symbols so unrelated pending orders are untouched.
        let preflight_positions = self
            .wait_for_startup_positions(Duration::from_secs(60))
            .await?;
        self.ensure_startup_positions_in_universe(&preflight_positions)?;
        for symbol in &configured_symbols {
            if let Err(err) = self.connector.cancel_all_orders(Some(symbol.clone())).await {
                log::warn!("[Startup] cancel_all_orders({}) failed: {:?}", symbol, err);
            }
        }
        for attempt in 1..=attempts {
            let positions_result = self.connector.get_positions().await;
            match positions_result {
                Ok(positions) => {
                    // Re-check on every retry so an external position opened
                    // after preflight still aborts before any close request.
                    self.ensure_startup_positions_in_universe(&positions)?;
                    // bot-strategy#487: drop sub-min dust before deciding
                    // whether anything is left to close. Dust can never be
                    // submitted to close_all_positions, so counting it as
                    // "open" would spin the retry loop and escalate forever.
                    let (closable, dust) = self.split_dust_positions(positions).await;
                    if closable.is_empty() {
                        if dust.is_empty() {
                            if attempt == 1 {
                                log::info!("[Startup] No open positions detected");
                            } else {
                                log::info!("[Startup] All positions closed");
                            }
                        } else {
                            log::warn!(
                                "[Startup] only sub-min dust remains, treating as flat (bot-strategy#487): {}",
                                Self::format_positions_summary(&dust)
                            );
                        }
                        return Ok(());
                    }
                    log::info!(
                        "[Startup] close attempt {}/{}: {}",
                        attempt,
                        attempts,
                        Self::format_positions_summary(&closable)
                    );
                    if attempt == 1 {
                        // bot-strategy#269 Phase 3: record what is about to be
                        // force-closed so the kill event is visible beyond
                        // journalctl's 7-day retention. Only on the first
                        // attempt — subsequent retries see partial / shrinking
                        // residue of the same position set and would double-count.
                        if let Err(err) = pnl_log::log_startup_force_close(&self.cfg, &closable) {
                            log::warn!("[Startup] log_startup_force_close failed: {:?}", err);
                        }
                    }
                    // bot-strategy#487: close each closable leg by symbol
                    // rather than close_all_positions(None). The connector's
                    // close-all aborts on the first sub-min position
                    // (round_size_for_market → InvalidInput), which in a
                    // real+dust mix could strand a genuine position and leave
                    // startup running with live exposure. Per-symbol closes
                    // never pass dust to the connector, so the abort cannot
                    // block a real leg.
                    for position in &closable {
                        if let Err(err) = self
                            .connector
                            .close_all_positions(Some(position.symbol.clone()))
                            .await
                        {
                            log::error!(
                                "[Startup] close_all_positions({}) failed: {:?}",
                                position.symbol,
                                err
                            );
                        }
                    }
                }
                Err(err) => {
                    // Don't call close_all_positions when we can't confirm positions
                    // state from the WS cache — its internal /account REST call would
                    // burst the startup rate-limit window alongside the other
                    // instances' connects and 429, producing a spurious RateLimit
                    // email. Just wait for the WS to populate on the next attempt.
                    // See bot-strategy#143.
                    log::warn!(
                        "[Startup] get_positions failed on attempt {}/{}: {:?}",
                        attempt,
                        attempts,
                        err
                    );
                }
            }

            if attempt < attempts && wait_secs > 0 {
                sleep(Duration::from_secs(wait_secs)).await;
            }
        }

        if wait_secs > 0 {
            sleep(Duration::from_secs(wait_secs)).await;
        }
        match self.connector.get_positions().await {
            Ok(positions) => {
                self.ensure_startup_positions_in_universe(&positions)?;
                // bot-strategy#487: sub-min dust is not a force-close failure —
                // it can never be flattened, so do not ERROR/email on it.
                let (closable, dust) = self.split_dust_positions(positions).await;
                if closable.is_empty() {
                    if dust.is_empty() {
                        log::info!("[Startup] All positions closed");
                    } else {
                        log::warn!(
                            "[Startup] only sub-min dust remains after {} attempts, treating as flat (bot-strategy#487): {}",
                            attempts,
                            Self::format_positions_summary(&dust)
                        );
                    }
                    return Ok(());
                }
                let summary = Self::format_positions_summary(&closable);
                log::error!(
                    "[Startup] positions still open after {} attempts: {}",
                    attempts,
                    summary
                );
                let subject = match self.cfg.agent_name.as_deref() {
                    Some(name) => format!("[{}] Startup close failed", name),
                    None => format!(
                        "[Startup] Failed to close positions (dex={})",
                        self.cfg.dex_name
                    ),
                };
                let body = format!(
                    "Startup force close failed after {} attempts.\nOpen positions: {}",
                    attempts, summary
                );
                EmailClient::new().send(&subject, &body);
            }
            Err(err) => {
                log::error!(
                    "[Startup] get_positions failed after {} attempts: {:?}",
                    attempts,
                    err
                );
                return Err(anyhow!(
                    "startup cleanup could not verify final positions after {} attempts: {:?}",
                    attempts,
                    err
                ));
            }
        }
        Ok(())
    }

    /// Returns `true` when the close is confirmed (positions already flat
    /// on the exchange, or `close_all_positions` was submitted without
    /// error). `false` means nothing was flattened (mode skip or connector
    /// failure) — callers must not assume the exchange position is gone,
    /// and in particular must not suppress the later exchange-snapshot
    /// recovery record (bot-strategy#514).
    pub(in crate::pairtrade) async fn force_close_all_positions(
        &mut self,
        key: &str,
        reason: &str,
    ) -> bool {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[EXIT] {} force close skipped (mode) reason={}",
                key,
                reason
            );
            return false;
        }
        if let Some((base, quote)) = key.split_once('/') {
            if let Ok(positions) = self.connector.get_positions().await {
                let has_open = |sym: &str| {
                    positions
                        .iter()
                        .any(|p| p.symbol == sym && p.sign != 0 && p.size > Decimal::ZERO)
                };
                if !has_open(base) && !has_open(quote) {
                    self.open_positions.remove(base);
                    self.open_positions.remove(quote);
                    log::info!(
                        "[EXIT] {} close_all_positions skipped; positions already flat reason={}",
                        key,
                        reason
                    );
                    return true;
                }
            }
        }
        log::error!(
            "[EXIT] {} exceeded exit retries; invoking close_all_positions reason={}",
            key,
            reason
        );
        if let Err(err) = self.connector.close_all_positions(None).await {
            log::error!("[EXIT] close_all_positions failed: {:?}", err);
            return false;
        }
        true
    }

    pub(in crate::pairtrade) async fn sync_positions_from_exchange(
        &mut self,
        inst_idx: usize,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> Result<()> {
        if self.replay_connector.is_some() {
            return Ok(());
        }
        if self.cfg.dry_run {
            // DRY_RUN keeps a synthetic position locally (entry path
            // populates state.position without placing an order). The
            // exchange snapshot will never reflect those synthetic
            // positions, so reconciliation would clear them on the next
            // tick and re-trigger entry evaluation every cycle —
            // driving STEP_OVERRUN through repeated A/B/C balance fetches
            // (bot-strategy#218 Tokyo Lighter rollout). Skip entirely
            // and mark positions_ready so the entry gate is not blocked.
            self.positions_ready = true;
            return Ok(());
        }
        let now_ts = self.current_now_ts();
        let positions = match self.connector.get_positions().await {
            Ok(v) => v,
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("positions not ready from websocket") {
                    let stale_clear_secs = self.cfg.order_timeout_secs.max(1).saturating_mul(6);
                    self.clear_stale_pending(
                        inst_idx,
                        Duration::from_secs(stale_clear_secs),
                        "ws_not_ready",
                    );
                    // Startup transient: the Lighter WS hasn't pushed the
                    // initial position snapshot yet. Resolves within seconds
                    // of the first WS push. Log at INFO so it does not
                    // inflate error_summary and trigger the error-watch
                    // workflow (bot-strategy#49) on every restart. Other
                    // get_positions failures keep WARN below.
                    if self.should_log_position_warn(&self.cfg.dex_name) {
                        log::info!(
                            "[POSITION] waiting for initial WS positions on {}",
                            self.cfg.dex_name
                        );
                        self.last_position_warn
                            .insert(self.cfg.dex_name.clone(), Instant::now());
                    }
                    self.positions_ready = false;
                    return Ok(());
                }
                if self.should_log_position_warn(&self.cfg.dex_name) {
                    log::warn!(
                        "[POSITION] get_positions not available for {}: {:?}",
                        self.cfg.dex_name,
                        err
                    );
                    self.last_position_warn
                        .insert(self.cfg.dex_name.clone(), Instant::now());
                }
                return Ok(());
            }
        };
        self.positions_ready = true;

        let mut snapshots: HashMap<String, PositionSnapshot> = HashMap::new();
        for snapshot in positions {
            if snapshot.sign == 0 || snapshot.size <= Decimal::ZERO {
                continue;
            }
            if self.is_dust_position(&snapshot, prices) {
                continue;
            }
            snapshots.insert(snapshot.symbol.clone(), snapshot);
        }
        self.open_positions = snapshots.clone();

        let mut unhedged_attempted: HashSet<String> = HashSet::new();
        let mut unhedged_closures: Vec<(String, String, i32, Decimal)> = Vec::new();
        // bot-strategy#514: (key, reason kind, position_guard after clear)
        // for positions that vanished from the exchange snapshot. The clear
        // is deferred past the loop so the recovery_no_pnl context record is
        // written while the entry context (z/beta/hold) is still in state.
        let mut cleared_positions: Vec<(String, &'static str, bool)> = Vec::new();
        let flatten_armed = self.instances[inst_idx].external_flatten_reason.is_some();
        for pair in &self.cfg.universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let log_warn = self.should_log_position_warn(&key);

            let Some(state) = self.instances[inst_idx].states.get_mut(&key) else {
                continue;
            };

            let base = snapshots.get(&pair.base);
            let quote = snapshots.get(&pair.quote);

            if state.pending_entry.is_some() || state.pending_exit.is_some() {
                // Keep pending orders; reconciliation handles timeouts/hedging.
                // Exception (bot-strategy#932): an armed out-of-band flatten
                // that already emptied the venue supersedes whatever was
                // pending — retire it locally so the clear below can book
                // the flatten instead of waiting on orders that can no
                // longer fill against a position.
                let superseded =
                    flatten_armed && base.is_none() && quote.is_none() && state.position.is_some();
                if !superseded {
                    continue;
                }
                log::warn!(
                    "[FLATTEN_PNL] {} pending orders superseded by out-of-band flatten (venue flat)",
                    key
                );
                state.pending_entry = None;
                state.pending_exit = None;
            }

            match (base, quote) {
                (None, None) => {
                    if state.position.is_some() || state.position_guard {
                        log::info!("[POSITION] {} cleared by exchange snapshot", key);
                    }
                    if state.position.is_some() {
                        cleared_positions.push((key.clone(), "exchange_snapshot_clear", false));
                    } else {
                        state.position_guard = false;
                    }
                }
                (Some(b), Some(q)) => {
                    if b.sign * q.sign >= 0 {
                        if log_warn {
                            log::warn!(
                                "[POSITION] {} has mismatched legs (signs {} / {})",
                                key,
                                b.sign,
                                q.sign
                            );
                        }
                        if log_warn {
                            self.last_position_warn.insert(key.clone(), Instant::now());
                        }
                        if state.position.is_some() {
                            cleared_positions.push((key.clone(), "mismatched_legs", true));
                        } else {
                            state.position_guard = true;
                        }
                        continue;
                    }

                    let direction = if b.sign > 0 {
                        PositionDirection::LongSpread
                    } else {
                        PositionDirection::ShortSpread
                    };
                    let (entered_at, entered_ts) = state
                        .position
                        .as_ref()
                        .map(|p| (p.entered_at, p.entered_ts))
                        .unwrap_or((Instant::now(), now_ts));
                    let prev_entry_z = state.position.as_ref().and_then(|p| p.entry_z);
                    // Preserve existing β / re-hedge state if recovering
                    // an in-flight position; new positions have no β yet
                    // until #463 Phase 2 lands the exchange-side recovery.
                    let prev_entry_beta = state.position.as_ref().and_then(|p| p.entry_beta);
                    let prev_last_rehedge_ts =
                        state.position.as_ref().and_then(|p| p.last_rehedge_ts);
                    let prev_realized =
                        state.position.as_ref().and_then(|p| p.rehedge_realized_pnl);
                    let prev_velocity = state
                        .position
                        .as_ref()
                        .and_then(|p| p.prev_beta_for_velocity);
                    state.position = Some(Position {
                        direction,
                        entered_at,
                        entered_ts,
                        entry_price_a: b.entry_price,
                        entry_price_b: q.entry_price,
                        entry_size_a: Some(b.size),
                        entry_size_b: Some(q.size),
                        entry_z: prev_entry_z,
                        entry_beta: prev_entry_beta,
                        last_rehedge_ts: prev_last_rehedge_ts,
                        rehedge_realized_pnl: prev_realized,
                        prev_beta_for_velocity: prev_velocity,
                    });
                    state.position_guard = false;
                }
                _ => {
                    let active_for_warn = state.position.is_some()
                        || state.pending_entry.is_some()
                        || state.pending_exit.is_some();
                    if state.pending_entry.is_none() && state.pending_exit.is_none() {
                        if let Some((symbol, snapshot)) = base
                            .map(|b| (pair.base.clone(), b))
                            .or_else(|| quote.map(|q| (pair.quote.clone(), q)))
                        {
                            if unhedged_attempted.insert(symbol.clone()) {
                                unhedged_closures.push((
                                    key.clone(),
                                    symbol.clone(),
                                    snapshot.sign,
                                    snapshot.size,
                                ));
                            }
                        }
                    }
                    if log_warn && active_for_warn {
                        log::warn!(
                            "[POSITION] {} has unhedged leg (base={}, quote={})",
                            key,
                            base.is_some(),
                            quote.is_some()
                        );
                        self.last_position_warn.insert(key.clone(), Instant::now());
                        state.position_guard = true;
                    } else {
                        state.position_guard = false;
                    }
                    if !active_for_warn {
                        state.position = None;
                    }
                }
            }
        }

        // bot-strategy#514: positions that vanished from the exchange
        // snapshot without an in-flight strategy exit were closed
        // out-of-band (risk-layer flatten, manual close, liquidation, or
        // an earlier recovery close). Write a recovery_no_pnl context
        // record before clearing local state so attribution keeps the
        // z/beta/hold context. Skipped when the reconcile recovery path
        // already recorded this close (`recovery_recorded`).
        if !cleared_positions.is_empty() {
            let flatten_reason = self.instances[inst_idx].external_flatten_reason.clone();
            let mut flatten_deferred = false;
            for (key, kind, guard_after) in cleared_positions {
                let record_direction = match self.instances[inst_idx].states.get(&key) {
                    Some(state) => match state.position.as_ref() {
                        Some(position) if !state.recovery_recorded => Some(position.direction),
                        _ => None,
                    },
                    None => continue,
                };
                if let Some(direction) = record_direction {
                    // bot-strategy#932: an out-of-band risk flatten realises
                    // PnL on the venue. Book it from the flatten's own fills
                    // (isolated via the pre-flatten cache baseline) as a
                    // normal `exit_fill` so trade stats, realized_pnl_today
                    // and the circuit breaker see the loss. Only the
                    // pnl-less context record is written when the fills
                    // cannot be attributed with full value coverage.
                    if kind == "exchange_snapshot_clear" {
                        if let Some(reason) = flatten_reason.as_deref() {
                            match self
                                .try_book_external_flatten(inst_idx, &key, reason, now_ts)
                                .await
                            {
                                FlattenBooking::Booked => {
                                    // Same post-exit transition as a filled
                                    // strategy exit (cooldown stamps, defer
                                    // window release, close-reason counter)
                                    // so the pair does not re-enter without
                                    // the exit cooldown after an ack (Codex
                                    // review, pairtrade#282).
                                    let inst_id = self.instances[inst_idx].id.clone();
                                    let shared = self.per_pair_state.get(&key);
                                    if let Some(state) =
                                        self.instances[inst_idx].states.get_mut(&key)
                                    {
                                        state.pending_exit_reason = Some("risk_flatten");
                                        super::super::apply_post_exit_state(
                                            state, shared, direction, now_ts, &inst_id, &key,
                                        );
                                        state.position_guard = guard_after;
                                    }
                                    continue;
                                }
                                FlattenBooking::Deferred => {
                                    // Keep the local position one more tick
                                    // so the next snapshot sync retries once
                                    // the fill events have landed.
                                    flatten_deferred = true;
                                    continue;
                                }
                                FlattenBooking::Unavailable => {}
                            }
                        }
                    }
                    let reason = match kind {
                        "exchange_snapshot_clear" => flatten_reason.as_deref().unwrap_or(kind),
                        other => other,
                    };
                    self.write_recovery_no_pnl_record(
                        inst_idx, &key, direction, reason, now_ts, prices,
                    );
                }
                if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                    state.position = None;
                    state.position_guard = guard_after;
                    state.recovery_recorded = false;
                }
            }
            // One-shot marker: consumed once every position this flatten
            // covered has been booked or fallen back. A bulk close can
            // surface pair by pair across snapshots, so keep it armed while
            // any local position remains (Codex review, pairtrade#282). The
            // risk-ack path drops it regardless.
            let positions_remain = self.instances[inst_idx]
                .states
                .values()
                .any(|s| s.position.is_some() || s.pending_entry.is_some());
            if !flatten_deferred && !positions_remain {
                let inst = &mut self.instances[inst_idx];
                inst.external_flatten_reason = None;
                inst.external_flatten_fills = None;
            }
        }

        for (key, symbol, sign, size) in unhedged_closures {
            self.try_close_unhedged_leg(inst_idx, &key, &symbol, sign, size, prices)
                .await;
        }

        Ok(())
    }

    /// Identity of a cached fill for set-difference against the pre-flatten
    /// baseline. Lighter reports `trade_id` (unique per fill) but some
    /// connectors leave it empty/zero, so the key also folds in order id,
    /// size and value. bot-strategy#932.
    pub(in crate::pairtrade) fn fill_identity(order: &FilledOrder) -> String {
        format!(
            "{}|{}|{}|{}",
            order.order_id,
            order.trade_id,
            order.filled_size.map(|v| v.to_string()).unwrap_or_default(),
            order
                .filled_value
                .map(|v| v.to_string())
                .unwrap_or_default()
        )
    }

    /// Snapshot the connector's per-symbol fill cache for every symbol of a
    /// pair this instance currently holds. Call immediately *before*
    /// submitting an out-of-band flatten (`close_all_positions`). A symbol
    /// whose fetch fails is left out of the map, which makes
    /// `try_book_external_flatten` refuse to attribute fills for it rather
    /// than mistake stale cache entries for the flatten. bot-strategy#932.
    pub(in crate::pairtrade) async fn snapshot_fill_baseline(
        &self,
        inst_idx: usize,
    ) -> HashMap<String, HashSet<String>> {
        let mut symbols: Vec<String> = Vec::new();
        for pair in &self.cfg.universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            // Pairs with a retained pending entry are included too: if that
            // entry executed during its halt-time cancel, the flatten closes
            // it and its fills must be attributable.
            let held = self.instances[inst_idx]
                .states
                .get(&key)
                .is_some_and(|s| s.position.is_some() || s.pending_entry.is_some());
            if held {
                symbols.push(pair.base.clone());
                symbols.push(pair.quote.clone());
            }
        }
        symbols.sort();
        symbols.dedup();
        let mut baseline: HashMap<String, HashSet<String>> = HashMap::new();
        for symbol in symbols {
            match self.connector.get_filled_orders(&symbol).await {
                Ok(resp) => {
                    baseline.insert(
                        symbol,
                        resp.orders.iter().map(Self::fill_identity).collect(),
                    );
                }
                Err(err) => {
                    log::warn!(
                        "[FLATTEN_PNL] {} fill baseline fetch failed for {}: {:?}",
                        self.instances[inst_idx].id,
                        symbol,
                        err
                    );
                }
            }
        }
        baseline
    }

    /// Arm the one-shot out-of-band flatten marker together with its fill
    /// baseline. Called right after a successful flatten submission while
    /// a local position still exists for the snapshot clear to consume it
    /// on. bot-strategy#514 / #932.
    pub(in crate::pairtrade) fn arm_external_flatten(
        &mut self,
        inst_idx: usize,
        reason: String,
        baseline: HashMap<String, HashSet<String>>,
        attributable_order_ids: HashSet<String>,
        excluded_order_ids: HashSet<String>,
        now_ts: i64,
    ) {
        let inst = &mut self.instances[inst_idx];
        let positions_now: HashMap<String, Position> = inst
            .states
            .iter()
            .filter_map(|(key, state)| state.position.clone().map(|p| (key.clone(), p)))
            .collect();
        let mut excluded_now: HashSet<String> = inst
            .states
            .values()
            .filter_map(|state| state.pending_entry.as_ref())
            .flat_map(|pending| pending.legs.iter())
            .flat_map(|leg| {
                std::iter::once(leg.order_id.clone()).chain(leg.exchange_order_id.clone())
            })
            .collect();
        // Entry ids cancelled just before the flatten stay excluded even
        // when the cancel looked confirmed: a fill event can arrive later
        // than the open-orders removal (Codex review, pairtrade#282).
        excluded_now.extend(excluded_order_ids);
        let covered_now: HashSet<String> = inst
            .states
            .iter()
            .filter(|(_, state)| state.position.is_some() || state.pending_entry.is_some())
            .map(|(key, _)| key.clone())
            .collect();
        match inst.external_flatten_fills.as_mut() {
            // Re-arm (halted-exposure retry while the first flatten is still
            // being booked): merge, never overwrite — the original baseline
            // and full-size position snapshot must survive until the whole
            // close is booked, otherwise the first flatten's fills would be
            // excluded and PnL computed on the residual (Codex review,
            // pairtrade#282). Only pairs/symbols new to this retry are added.
            Some(existing) => {
                for (symbol, seen) in baseline {
                    existing.baseline.entry(symbol).or_insert(seen);
                }
                existing
                    .attributable_order_ids
                    .extend(attributable_order_ids);
                existing.excluded_order_ids.extend(excluded_now);
                existing.covered_pairs.extend(covered_now);
                for (key, pos) in positions_now {
                    existing.positions.entry(key).or_insert(pos);
                }
                existing.submitted_at = Instant::now();
                existing.submitted_ts = now_ts;
                if inst.external_flatten_reason.is_none() {
                    inst.external_flatten_reason = Some(reason);
                }
            }
            None => {
                inst.external_flatten_reason = Some(reason);
                inst.external_flatten_fills = Some(ExternalFlattenFills {
                    baseline,
                    attributable_order_ids,
                    excluded_order_ids: excluded_now,
                    covered_pairs: covered_now,
                    positions: positions_now,
                    submitted_at: Instant::now(),
                    submitted_ts: now_ts,
                });
            }
        }
    }

    /// Session-halt hygiene, run *before* the bulk flatten is submitted:
    /// cancel every pending **entry** order of this instance (with or
    /// without a local position — entry legs awaiting reconciliation are
    /// exactly the case). A halted instance never retries the flatten, so an
    /// entry leg that filled after it would recreate exposure nobody closes.
    /// The local `pending_entry` is dropped only once the venue confirms the
    /// tracked orders are gone; on a cancel error or an unconfirmed cancel it
    /// is kept, so the normal reconcile loop keeps managing (and, on fill,
    /// hedging/flattening) it. Cancels target the tracked order ids only.
    /// bot-strategy#932.
    pub(in crate::pairtrade) async fn cancel_pending_entries_for_halt(
        &mut self,
        inst_idx: usize,
    ) -> HashSet<String> {
        let mut processed_ids: HashSet<String> = HashSet::new();
        let universe = self.cfg.universe.clone();
        for pair in &universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let Some(pending) = self.instances[inst_idx]
                .states
                .get_mut(&key)
                .and_then(|state| state.pending_entry.take())
            else {
                continue;
            };
            let inst_id = self.instances[inst_idx].id.clone();
            let mut per_symbol: Vec<(String, Vec<String>)> = Vec::new();
            let mut fill_ids: Vec<(String, Vec<String>)> = Vec::new();
            for leg in &pending.legs {
                match per_symbol.iter_mut().find(|(sym, _)| *sym == leg.symbol) {
                    Some((_, ids)) => ids.push(leg.order_id.clone()),
                    None => per_symbol.push((leg.symbol.clone(), vec![leg.order_id.clone()])),
                }
                let mut ids = vec![leg.order_id.clone()];
                ids.extend(leg.exchange_order_id.clone());
                match fill_ids.iter_mut().find(|(sym, _)| *sym == leg.symbol) {
                    Some((_, v)) => v.extend(ids),
                    None => fill_ids.push((leg.symbol.clone(), ids)),
                }
            }
            for (_, ids) in &fill_ids {
                processed_ids.extend(ids.iter().cloned());
            }
            let mut cancel_failed = false;
            for (symbol, ids) in &per_symbol {
                if let Err(err) = self
                    .connector
                    .cancel_orders(Some(symbol.clone()), ids.clone())
                    .await
                {
                    log::warn!(
                        "[FLATTEN_PNL] {} {} cancel_orders({}, {:?}) before halt flatten failed: {:?}",
                        inst_id,
                        key,
                        symbol,
                        ids,
                        err
                    );
                    cancel_failed = true;
                }
            }
            // "Gone from open orders" does not distinguish cancelled from
            // executed: if any tracked leg shows a fill, keep the pending so
            // the (halt-aware) reconcile flattens what filled instead of the
            // fill vanishing unbooked (Codex review, pairtrade#282).
            let confirmed = !cancel_failed
                && self.tracked_orders_gone(&per_symbol).await
                && !self.tracked_orders_filled(&fill_ids).await;
            if confirmed {
                log::warn!(
                    "[FLATTEN_PNL] {} {} pending entry cancelled ahead of session halt (legs={})",
                    inst_id,
                    key,
                    pending.legs.len()
                );
            } else {
                log::warn!(
                    "[FLATTEN_PNL] {} {} pending entry kept: cancel {} — reconcile keeps managing it",
                    inst_id,
                    key,
                    if cancel_failed {
                        "failed"
                    } else {
                        "not confirmed by the venue"
                    }
                );
                if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                    state.pending_entry = Some(pending);
                }
            }
        }
        processed_ids
    }

    /// `true` when the connector's fill cache holds a fill for any of the
    /// given tracked order ids (client or exchange form). A fetch error
    /// counts as "filled" so the caller keeps the pending rather than
    /// dropping an order that may have executed. bot-strategy#932.
    pub(in crate::pairtrade) async fn tracked_orders_filled(
        &self,
        per_symbol: &[(String, Vec<String>)],
    ) -> bool {
        for (symbol, ids) in per_symbol {
            match self.connector.get_filled_orders(symbol).await {
                Ok(resp) => {
                    if resp
                        .orders
                        .iter()
                        .any(|f| !f.is_rejected && ids.contains(&f.order_id))
                    {
                        return true;
                    }
                }
                Err(_) => return true,
            }
        }
        false
    }

    /// Poll the venue (bounded, ~1.5 s worst case) until none of the given
    /// tracked orders remain open. `true` only on positive confirmation; an
    /// open-orders fetch error counts as "still open". Skipped (returns
    /// `true`) in backtest replay where cancels are synchronous.
    /// bot-strategy#932.
    pub(in crate::pairtrade) async fn tracked_orders_gone(
        &self,
        per_symbol: &[(String, Vec<String>)],
    ) -> bool {
        const CANCEL_ACK_ATTEMPTS: usize = 10;
        const CANCEL_ACK_DELAY_MS: u64 = 150;
        if self.cfg.backtest_mode {
            return true;
        }
        for attempt in 0..CANCEL_ACK_ATTEMPTS {
            if attempt > 0 {
                sleep(Duration::from_millis(CANCEL_ACK_DELAY_MS)).await;
            }
            let mut any_open = false;
            for (symbol, ids) in per_symbol {
                match self.connector.get_open_orders(symbol).await {
                    Ok(open) => {
                        if open.orders.iter().any(|o| ids.contains(&o.order_id)) {
                            any_open = true;
                        }
                    }
                    Err(_) => any_open = true,
                }
            }
            if !any_open {
                return true;
            }
        }
        false
    }

    /// Right after an out-of-band flatten was submitted successfully: cancel
    /// the still-open strategy **exit** legs of every pair this instance
    /// holds a position in (by their tracked order ids — never a symbol-wide
    /// sweep, which could catch the just-submitted flatten orders on an
    /// asynchronous venue) and drop the local `pending_exit` so the snapshot
    /// clear can consume the flatten. Returns the identifiers (client order
    /// id and exchange order id, whichever the connector reports fills under)
    /// of the retired exit legs — their fills close the same position and are
    /// attributable to the flatten even when they pre-date the fill baseline.
    /// Must not run before the flatten submission succeeded: on failure the
    /// pendings stay live and keep closing the exposure. bot-strategy#932.
    pub(in crate::pairtrade) async fn retire_pending_exits_for_flatten(
        &mut self,
        inst_idx: usize,
    ) -> HashSet<String> {
        let mut attributable: HashSet<String> = HashSet::new();
        let mut per_symbol: Vec<(String, Vec<String>)> = Vec::new();
        for pair in &self.cfg.universe {
            let key = format!("{}/{}", pair.base, pair.quote);
            let Some(state) = self.instances[inst_idx].states.get_mut(&key) else {
                continue;
            };
            if state.position.is_none() {
                continue;
            }
            let Some(pending) = state.pending_exit.take() else {
                continue;
            };
            for leg in &pending.legs {
                attributable.insert(leg.order_id.clone());
                if let Some(exchange_id) = &leg.exchange_order_id {
                    attributable.insert(exchange_id.clone());
                }
                match per_symbol.iter_mut().find(|(sym, _)| *sym == leg.symbol) {
                    Some((_, ids)) => ids.push(leg.order_id.clone()),
                    None => per_symbol.push((leg.symbol.clone(), vec![leg.order_id.clone()])),
                }
            }
            log::warn!(
                "[FLATTEN_PNL] {} {} pending exit retired after out-of-band flatten (legs={})",
                self.instances[inst_idx].id,
                key,
                pending.legs.len()
            );
        }
        for (symbol, ids) in per_symbol {
            if let Err(err) = self
                .connector
                .cancel_orders(Some(symbol.clone()), ids.clone())
                .await
            {
                log::warn!(
                    "[FLATTEN_PNL] {} cancel_orders({}, {:?}) after flatten failed: {:?}",
                    self.instances[inst_idx].id,
                    symbol,
                    ids,
                    err
                );
            }
        }
        attributable
    }

    /// `true` while an out-of-band flatten is armed for this instance, the
    /// pair still has a local position (venue confirmation / fill booking
    /// pending) and the flatten is younger than
    /// `EXTERNAL_FLATTEN_FILL_GRACE`. Exit / re-hedge planning must stand
    /// down in that window: the venue position is already gone, so a normal
    /// exit would only submit a spurious close and install a `pending_exit`
    /// that stops the fill attribution retries. The time bound keeps a
    /// flatten that never clears from suppressing exits for good.
    /// bot-strategy#932.
    pub(in crate::pairtrade) fn flatten_booking_in_progress(
        &self,
        inst_idx: usize,
        key: &str,
    ) -> bool {
        let inst = &self.instances[inst_idx];
        if inst.external_flatten_reason.is_none() {
            return false;
        }
        let Some(fills) = inst.external_flatten_fills.as_ref() else {
            return false;
        };
        if fills.submitted_at.elapsed() >= EXTERNAL_FLATTEN_FILL_GRACE {
            return false;
        }
        // Only a position the flatten actually covered is "being booked";
        // exposure that appeared afterwards on another pair must not hide
        // behind this marker (Codex review, pairtrade#282).
        fills.covered_pairs.contains(key)
            && inst.states.get(key).is_some_and(|s| s.position.is_some())
    }

    /// Try to book an out-of-band flatten (session-DD halt) as a real
    /// `exit_fill` from the venue fills that arrived after the flatten was
    /// submitted. Fill attribution rules (bot-strategy#932 / #750):
    /// - only fills absent from the pre-flatten baseline count;
    /// - a fill must be on the closing side of its leg (or side-less);
    /// - attributed quantity must cover the held size of both legs;
    /// - every attributed fill must report a value — no snapshot blending.
    ///
    /// While the fills have not landed yet, the clear is deferred for up to
    /// `EXTERNAL_FLATTEN_FILL_GRACE`; afterwards, or when attribution is
    /// impossible, the caller falls back to the pnl-less context record.
    pub(in crate::pairtrade) async fn try_book_external_flatten(
        &mut self,
        inst_idx: usize,
        key: &str,
        reason: &str,
        now_ts: i64,
    ) -> FlattenBooking {
        let inst_id = self.instances[inst_idx].id.clone();
        let Some(flatten) = self.instances[inst_idx].external_flatten_fills.clone() else {
            log::warn!(
                "[FLATTEN_PNL] {} {} flatten marker has no fill baseline; cannot attribute fills",
                inst_id,
                key
            );
            return FlattenBooking::Unavailable;
        };
        let within_grace = flatten.submitted_at.elapsed() < EXTERNAL_FLATTEN_FILL_GRACE;
        let pending_or_unavailable = |what: &str| {
            if within_grace {
                log::info!(
                    "[FLATTEN_PNL] {} {} waiting for flatten fills ({})",
                    inst_id,
                    key,
                    what
                );
                FlattenBooking::Deferred
            } else {
                log::warn!(
                    "[FLATTEN_PNL] {} {} flatten fills not attributable after {}s ({}); falling back to recovery_no_pnl",
                    inst_id,
                    key,
                    EXTERNAL_FLATTEN_FILL_GRACE.as_secs(),
                    what
                );
                FlattenBooking::Unavailable
            }
        };
        let Some((base, quote)) = key.split_once('/') else {
            return FlattenBooking::Unavailable;
        };
        // Price the flatten against the position as it stood when the
        // flatten was submitted, not the residual the per-tick snapshot
        // sync may have written since (Codex review, pairtrade#282).
        let Some(pos) = flatten.positions.get(key).cloned().or_else(|| {
            self.instances[inst_idx]
                .states
                .get(key)
                .and_then(|s| s.position.clone())
        }) else {
            return FlattenBooking::Unavailable;
        };
        let (Some(size_a), Some(size_b)) = (pos.entry_size_a, pos.entry_size_b) else {
            return FlattenBooking::Unavailable;
        };
        // Fills are attributed per symbol; two covered positions sharing a
        // leg symbol (BTC/ETH + BTC/SOL) would both absorb the same BTC
        // fills. Decline rather than corrupt both records (Codex review,
        // pairtrade#282).
        let shares_symbol = flatten
            .positions
            .keys()
            .any(|other| other != key && other.split('/').any(|sym| sym == base || sym == quote));
        if shares_symbol {
            log::warn!(
                "[FLATTEN_PNL] {} {} shares a leg symbol with another flattened pair; not attributing fills",
                inst_id,
                key
            );
            return FlattenBooking::Unavailable;
        }
        // Closing side per leg: LongSpread = long base / short quote.
        let (close_side_a, close_side_b) = match pos.direction {
            PositionDirection::LongSpread => (OrderSide::Short, OrderSide::Long),
            PositionDirection::ShortSpread => (OrderSide::Long, OrderSide::Short),
        };
        let mut vwaps: Vec<Decimal> = Vec::with_capacity(2);
        for (symbol, close_side, held) in
            [(base, close_side_a, size_a), (quote, close_side_b, size_b)]
        {
            let Some(seen) = flatten.baseline.get(symbol) else {
                return pending_or_unavailable(&format!("no baseline for {}", symbol));
            };
            let fills = match self.connector.get_filled_orders(symbol).await {
                Ok(resp) => resp.orders,
                Err(err) => {
                    return pending_or_unavailable(&format!(
                        "fill fetch failed for {}: {:?}",
                        symbol, err
                    ));
                }
            };
            let mut qty = Decimal::ZERO;
            let mut value = Decimal::ZERO;
            let mut value_missing = false;
            for fill in fills.iter().filter(|f| !f.is_rejected) {
                // Opening fills of an entry retained through the halt are
                // never part of the flatten, side-reported or not.
                if flatten.excluded_order_ids.contains(&fill.order_id) {
                    continue;
                }
                if seen.contains(&Self::fill_identity(fill))
                    && !flatten.attributable_order_ids.contains(&fill.order_id)
                {
                    continue;
                }
                if fill.filled_side.is_some_and(|side| side != close_side) {
                    continue;
                }
                let Some(sz) = fill.filled_size.filter(|v| *v > Decimal::ZERO) else {
                    continue;
                };
                qty += sz;
                match fill.filled_value {
                    Some(v) => value += v,
                    None => value_missing = true,
                }
            }
            // Allow venue size rounding, but never book a partially
            // covered flatten as if it were the whole position.
            let coverage_floor = held * Decimal::new(99, 2);
            if qty < coverage_floor {
                return pending_or_unavailable(&format!(
                    "{} attributed qty {} < held {}",
                    symbol, qty, held
                ));
            }
            if value_missing || value <= Decimal::ZERO {
                log::warn!(
                    "[FLATTEN_PNL] {} {} {} flatten fills lack value coverage; not booking",
                    inst_id,
                    key,
                    symbol
                );
                return FlattenBooking::Unavailable;
            }
            vwaps.push(value / qty);
        }
        let (exit_price_a, exit_price_b) = (vwaps[0], vwaps[1]);
        let z_exit = self
            .per_pair_state
            .get(key)
            .and_then(|s| s.z_score().map(|(z, _)| z));
        let beta_val = pos
            .entry_beta
            .or_else(|| self.per_pair_state.get(key).map(|s| s.beta));
        let Some((record, pnl_value, funding_value)) =
            Self::exit_pnl_record_from_prices(ExitPnlInputs {
                inst_id: &inst_id,
                key,
                pos: &pos,
                exit_price_a,
                exit_price_b,
                funding_history: &self.funding_history,
                z_exit,
                beta_val,
                now_ts,
                reason,
            })
        else {
            return FlattenBooking::Unavailable;
        };
        log::warn!(
            "[FLATTEN_PNL] {} {} booked out-of-band flatten as exit_fill reason={} exit_a={} exit_b={} pnl={:.4} funding={:.4} submitted_ts={}",
            inst_id,
            key,
            reason,
            exit_price_a,
            exit_price_b,
            pnl_value,
            funding_value,
            flatten.submitted_ts
        );
        self.write_pnl_record(inst_idx, record);
        self.record_exit_realized_pnl(inst_idx, now_ts, pnl_value, funding_value);
        FlattenBooking::Booked
    }

    async fn try_close_unhedged_leg(
        &mut self,
        inst_idx: usize,
        key: &str,
        symbol: &str,
        sign: i32,
        size: Decimal,
        prices: &HashMap<String, SymbolSnapshot>,
    ) {
        let now_ts = self.current_now_ts();
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[UNHEDGED] {} close skipped (mode) symbol={} size={}",
                key,
                symbol,
                size
            );
            return;
        }

        const UNHEDGED_CLOSE_COOLDOWN_SECS: u64 = 30;
        let last_exit = self.instances[inst_idx]
            .states
            .get(key)
            .and_then(|state| state.last_exit_at);
        if let Some(last_exit) = last_exit {
            if last_exit.elapsed() < Duration::from_secs(UNHEDGED_CLOSE_COOLDOWN_SECS) {
                return;
            }
        }

        let side = if sign >= 0 {
            dex_connector::OrderSide::Short
        } else {
            dex_connector::OrderSide::Long
        };
        let qty = self.quantize_order_size_close(symbol, size, prices);
        if qty <= Decimal::ZERO {
            log::warn!(
                "[UNHEDGED] {} close skipped (qty=0) symbol={} size={}",
                key,
                symbol,
                size
            );
            return;
        }

        log::warn!(
            "[UNHEDGED] {} closing lone leg symbol={} sign={} size={} qty={} side={:?}",
            key,
            symbol,
            sign,
            size,
            qty,
            side
        );

        let res = self
            .connector
            .create_order(symbol, qty, side, None, None, true, None)
            .await;

        match res {
            Ok(res) => {
                log::info!(
                    "[UNHEDGED] {} close submitted symbol={} order_id={}",
                    key,
                    symbol,
                    res.order_id
                );
                if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                    state.last_exit_at = Some(Instant::now());
                    state.last_exit_ts = Some(now_ts);
                }
            }
            Err(err) => {
                if engine::error_class::is_reduce_only_rejection(&err)
                    && self.confirm_reduce_only_position_missing(symbol).await
                {
                    log::info!(
                        "[UNHEDGED] {} close skipped; position already closed symbol={}",
                        key,
                        symbol
                    );
                    if let Some(state) = self.instances[inst_idx].states.get_mut(key) {
                        state.last_exit_at = Some(Instant::now());
                        state.last_exit_ts = Some(now_ts);
                    }
                } else {
                    log::error!(
                        "[UNHEDGED] {} close failed symbol={} err={:?}",
                        key,
                        symbol,
                        err
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Coverage for the static `format_positions_summary` log helper used
    //! by `force_close_on_startup`. The string this builds is the only
    //! surviving log evidence of a startup force-close (journalctl 7d
    //! retention; see feedback_no_cloudwatch and feedback_pairtrade_restart_force_closes),
    //! so its shape — direction tag + size + entry — is load-bearing for
    //! post-incident reconstruction. bot-strategy#396.
    use rust_decimal::Decimal;

    use dex_connector::PositionSnapshot;

    use super::PairTradeEngine;

    fn dec(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    fn pos(symbol: &str, size: &str, sign: i32, entry: Option<&str>) -> PositionSnapshot {
        PositionSnapshot {
            symbol: symbol.to_string(),
            size: dec(size),
            sign,
            entry_price: entry.map(dec),
        }
    }

    #[test]
    fn format_empty_positions_produces_empty_string() {
        let s = PairTradeEngine::format_positions_summary(&[]);
        assert_eq!(s, "");
    }

    #[test]
    fn format_long_position_renders_direction_and_entry() {
        let positions = vec![pos("BTC", "0.05", 1, Some("70000"))];
        let s = PairTradeEngine::format_positions_summary(&positions);
        assert_eq!(s, "BTC LONG size=0.05 entry=70000");
    }

    #[test]
    fn format_short_position_renders_direction_and_entry() {
        let positions = vec![pos("ETH", "1.2", -1, Some("3500"))];
        let s = PairTradeEngine::format_positions_summary(&positions);
        assert_eq!(s, "ETH SHORT size=1.2 entry=3500");
    }

    #[test]
    fn format_flat_position_renders_flat_tag() {
        // sign==0 is unusual but should fall under the FLAT branch
        // (defensive: a dust-skipped snapshot would be filtered before
        // reaching the log helper, but `force_close_on_startup` calls
        // this on the raw exchange list).
        let positions = vec![pos("BTC", "0.0", 0, None)];
        let s = PairTradeEngine::format_positions_summary(&positions);
        assert_eq!(s, "BTC FLAT size=0.0 entry=n/a");
    }

    #[test]
    fn format_missing_entry_price_renders_na() {
        let positions = vec![pos("BTC", "0.05", 1, None)];
        let s = PairTradeEngine::format_positions_summary(&positions);
        assert_eq!(s, "BTC LONG size=0.05 entry=n/a");
    }

    #[test]
    fn format_multiple_positions_joins_with_comma() {
        // Multi-pair format on a single line keeps the journalctl entry
        // greppable: one `[Startup]` line per attempt with all legs.
        let positions = vec![
            pos("BTC", "0.05", 1, Some("70000")),
            pos("ETH", "1.5", -1, Some("3500")),
        ];
        let s = PairTradeEngine::format_positions_summary(&positions);
        assert_eq!(
            s,
            "BTC LONG size=0.05 entry=70000, ETH SHORT size=1.5 entry=3500"
        );
    }
}
