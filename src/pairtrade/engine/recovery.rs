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

use anyhow::Result;
use dex_connector::{DexError, PositionSnapshot};
use rust_decimal::Decimal;
use tokio::time::sleep;

use super::super::engine;
use super::super::market::SymbolSnapshot;
use super::super::pnl_log;
use super::super::state::{Position, PositionDirection};
use super::super::PairTradeEngine;
use crate::email_client::EmailClient;

impl PairTradeEngine {
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
    /// Returns `Ok(())` when the connector returns positions (or any other
    /// non-WS-not-ready outcome — including a different `Err`, which the
    /// subsequent force-close loop will surface). Returns `Err` only on
    /// timeout.
    async fn wait_for_ws_ready(&self, timeout: Duration) -> Result<(), ()> {
        const POLL_INTERVAL: Duration = Duration::from_secs(2);
        const LOG_INTERVAL: Duration = Duration::from_secs(10);
        let start = Instant::now();
        let mut next_log = start + LOG_INTERVAL;
        loop {
            match self.connector.get_positions().await {
                Err(DexError::Transient(ref msg)) if msg.contains("not ready from websocket") => {
                    let now = Instant::now();
                    if now.duration_since(start) >= timeout {
                        return Err(());
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
                _ => return Ok(()),
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
        if let Err(err) = self.connector.cancel_all_orders(None).await {
            log::warn!("[Startup] cancel_all_orders failed: {:?}", err);
        }
        if self
            .wait_for_ws_ready(Duration::from_secs(60))
            .await
            .is_err()
        {
            log::warn!(
                "[Startup] WS positions snapshot not ready after 60s; proceeding to retry loop"
            );
        }
        for attempt in 1..=attempts {
            let positions_result = self.connector.get_positions().await;
            match positions_result {
                Ok(positions) if positions.is_empty() => {
                    if attempt == 1 {
                        log::info!("[Startup] No open positions detected");
                    } else {
                        log::info!("[Startup] All positions closed");
                    }
                    return Ok(());
                }
                Ok(positions) => {
                    log::info!(
                        "[Startup] close attempt {}/{}: {}",
                        attempt,
                        attempts,
                        Self::format_positions_summary(&positions)
                    );
                    if attempt == 1 {
                        // bot-strategy#269 Phase 3: record what is about to be
                        // force-closed so the kill event is visible beyond
                        // journalctl's 7-day retention. Only on the first
                        // attempt — subsequent retries see partial / shrinking
                        // residue of the same position set and would double-count.
                        if let Err(err) = pnl_log::log_startup_force_close(&self.cfg, &positions) {
                            log::warn!("[Startup] log_startup_force_close failed: {:?}", err);
                        }
                    }
                    if let Err(err) = self.connector.close_all_positions(None).await {
                        log::error!("[Startup] close_all_positions failed: {:?}", err);
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
            Ok(positions) if positions.is_empty() => {
                log::info!("[Startup] All positions closed");
            }
            Ok(positions) => {
                let summary = Self::format_positions_summary(&positions);
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
            }
        }
        Ok(())
    }

    pub(in crate::pairtrade) async fn force_close_all_positions(
        &mut self,
        key: &str,
        reason: &str,
    ) {
        if self.cfg.dry_run || self.cfg.observe_only {
            log::warn!(
                "[EXIT] {} force close skipped (mode) reason={}",
                key,
                reason
            );
            return;
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
                    return;
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
        }
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
                continue;
            }

            match (base, quote) {
                (None, None) => {
                    if state.position.is_some() || state.position_guard {
                        log::info!("[POSITION] {} cleared by exchange snapshot", key);
                    }
                    state.position = None;
                    state.position_guard = false;
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
                        state.position = None;
                        state.position_guard = true;
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
                    let prev_last_rehedge_ts = state.position.as_ref().and_then(|p| p.last_rehedge_ts);
                    let prev_realized = state.position.as_ref().and_then(|p| p.rehedge_realized_pnl);
                    let prev_velocity = state.position.as_ref().and_then(|p| p.prev_beta_for_velocity);
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

        for (key, symbol, sign, size) in unhedged_closures {
            self.try_close_unhedged_leg(inst_idx, &key, &symbol, sign, size, prices)
                .await;
        }

        Ok(())
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
