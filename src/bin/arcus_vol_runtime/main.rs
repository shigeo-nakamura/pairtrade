//! Arcus Perps maker-first volume runtime (bot-strategy#1093).
//!
//! Quotes post-only (ALO) at the touch on both sides of one market, skews
//! toward the reducing side, and flattens with a reduce-only IOC when
//! inventory reaches the cap or has been held too long. The goal is volume
//! for Arcus points at close to zero net cost, not alpha.
//!
//! Modes:
//! - DRY_RUN (default) = the G1 paper run: never sends an order. It reads the
//!   live book and the public `trades` tape and fills virtual quotes with the
//!   conservative queue model in `sim.rs`.
//! - Live needs `ARCUS_VOL_DRY_RUN=false`, `ARCUS_VOL_LIVE_CONFIRM=1093-G2`
//!   and a dedicated non-zero `ARCUS_ACCOUNT_INDEX` (plus the usual
//!   `ARCUS_ADDRESS` / `ARCUS_API_PRIVATE_KEY` + `ENCRYPTED_DATA_KEY`).
//!
//! Files in `ARCUS_VOL_STATE_DIR`: `state.json` (ledger, atomic), `status.json`
//! (every tick), `fills.jsonl` (fills + later markout rows), and the
//! sentinels `KILL_SWITCH` (cancel + flatten + halt while present) and `HALT`
//! (sticky cumulative stop; see `ledger.rs` for how to clear it).

mod config;
mod ledger;
mod logic;
mod sim;
mod tape;

use anyhow::{anyhow, bail, Context, Result};
use config::Config;
use debot::directional::{append_jsonl, load_json, persist_json};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{
    BatchModifyRequest, BatchOrderRequest, DexConnector, DexError, OrderBookLevel, OrderSide,
};
use ledger::{
    append_synced, book_fill, due_markouts, may_forget_fill, risk_check, Booking, FillIn, Halt,
    Ledger, PendingMarkout,
};
use logic::{
    book_stale, dms_armed, flatten_reason, flatten_steps, may_disarm_dms, plan_quotes,
    quote_action, shock, shutdown_steps, tick_plan, QSide, QuoteAction, QuoteParams, QuoteTarget,
    Resting, ShutdownStep, Step, TickInputs, TickPlan,
};
use rust_decimal::Decimal;
use serde_json::json;
use sim::VirtualQuote;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write as _;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const MARKOUT_HORIZONS: [u64; 3] = [5, 30, 60];

fn init_logger() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                record.level(),
                record.args()
            )
        })
        .init();
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn utc_day() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

struct BookView {
    bid: Decimal,
    ask: Decimal,
    bids: Vec<OrderBookLevel>,
    asks: Vec<OrderBookLevel>,
    ts_ms: Option<u64>,
}

impl BookView {
    fn mid(&self) -> Decimal {
        (self.bid + self.ask) / Decimal::TWO
    }
}

struct Runtime {
    cfg: Config,
    dex: DexConnectorBox,
    ledger: Ledger,
    state_path: PathBuf,
    status_path: PathBuf,
    fills_path: PathBuf,
    kill_path: PathBuf,
    halt_path: PathBuf,
    /// Live: our resting quotes as we believe they rest.
    resting: HashMap<QSide, Resting>,
    /// DRY_RUN: virtual quotes and their original size.
    virt: HashMap<QSide, (VirtualQuote, Decimal)>,
    book: Option<BookView>,
    mid_hist: VecDeque<(u64, Decimal)>,
    cooldown_until_ms: u64,
    backoff_until_ms: u64,
    need_reconcile: bool,
    startup_flatten: bool,
    flatten_inflight_until_ms: u64,
    pending_markouts: Vec<PendingMarkout>,
    quote_ids: HashSet<String>,
    ioc_ids: HashSet<String>,
    /// Newest public print (venue µs) applied to the paper sim.
    newest_print_ts_us: u64,
    position_mismatch_since_ms: Option<u64>,
    /// Last successful DMS arm/refresh, and whether the latest attempt failed.
    dms_last_ok_ms: Option<u64>,
    dms_last_failed: bool,
    last_reconcile_ms: u64,
    last_position_ms: u64,
    last_summary_ms: u64,
    last_book_warn_ms: u64,
    sim_seq: u64,
    halt: Option<Halt>,
}

impl Runtime {
    fn quote_params(&self) -> QuoteParams {
        QuoteParams {
            clip_usd: self.cfg.clip_usd,
            skew_usd: self.cfg.skew_usd,
            cap_usd: self.cfg.quote_cap_usd(),
            min_quote_usd: self.cfg.min_quote_usd,
            qty_decimals: self.cfg.qty_decimals,
        }
    }

    fn fee(&self, notional: Decimal, maker: bool) -> Decimal {
        let bps = if maker {
            self.cfg.maker_fee_bps
        } else {
            self.cfg.taker_fee_bps
        };
        notional * bps / Decimal::from(10_000)
    }

    /// Classify an order-path error: backs off on 429, schedules a
    /// reconcile on an ambiguous outcome, stays quiet on the expected
    /// post-only rejects.
    fn on_error(&mut self, ctx: &str, err: &DexError) {
        match err {
            DexError::RateLimited { until_unix } => {
                self.backoff_until_ms = (*until_unix).max(0) as u64 * 1_000;
                log::warn!("[ARCUS_VOL] {ctx}: rate limited until unix {until_unix}");
            }
            DexError::ReconciliationRequired { .. } => {
                self.need_reconcile = true;
                log::warn!("[ARCUS_VOL] {ctx}: {err} → reconcile before quoting again");
            }
            other => {
                let text = other.to_string();
                if text.contains("POST_ONLY") || text.to_ascii_lowercase().contains("would cross") {
                    log::debug!("[ARCUS_VOL] {ctx}: {text}");
                } else {
                    log::warn!("[ARCUS_VOL] {ctx}: {text}");
                }
            }
        }
    }

    /// Book one fill durably (fills.jsonl fsynced before the ledger moves,
    /// dedupe by trade id). The caller persists state.json afterwards.
    fn book(&mut self, fill: FillIn, now: u64) -> std::io::Result<Booking> {
        let path = self.fills_path.clone();
        let outcome = book_fill(&mut self.ledger, &fill, now, |row| {
            append_synced(&path, row)
        })?;
        if outcome != Booking::AlreadyBooked {
            log::info!(
                "[ARCUS_VOL] FILL {} {} {} @ {} fee {} inv {}",
                if fill.maker { "maker" } else { "taker" },
                if fill.buy { "buy" } else { "sell" },
                fill.qty,
                fill.px,
                fill.fee.round_dp(4),
                self.ledger.position.qty
            );
            self.pending_markouts.push(PendingMarkout {
                fill_id: fill.trade_id,
                ts_ms: now,
                px: fill.px,
                buy: fill.buy,
                horizons: MARKOUT_HORIZONS.to_vec(),
            });
        }
        Ok(outcome)
    }

    // ---------------------------------------------------------------- paper

    fn on_print(&mut self, p: tape::Print) {
        if !self.cfg.dry_run {
            return;
        }
        let now = now_ms();
        self.newest_print_ts_us = self.newest_print_ts_us.max(p.ts_us);
        for side in [QSide::Bid, QSide::Ask] {
            let Some((vq, _)) = self.virt.get_mut(&side) else {
                continue;
            };
            let filled = sim::apply_trade(vq, p.ts_us, p.px, p.qty, p.taker);
            if filled.is_zero() {
                continue;
            }
            let px = vq.px;
            let done = vq.remaining.is_zero();
            if done {
                self.virt.remove(&side);
            }
            self.sim_seq += 1;
            let fee = self.fee(filled * px, true);
            let fill = FillIn {
                trade_id: format!("sim-{}-{}", self.sim_seq, p.trade_id),
                buy: side == QSide::Bid,
                qty: filled,
                px,
                fee,
                maker: true,
                order_id: format!("sim-{}", side.as_str()),
            };
            // A print cannot be replayed, so a paper fill whose row fails to
            // write is lost (logged); live fills are retried instead.
            if let Err(e) = self.book(fill, now) {
                log::error!("[ARCUS_VOL] paper fill not recorded: {e}");
            }
        }
    }

    fn paper_quote(&mut self, side: QSide, action: QuoteAction) {
        let levels = match (&self.book, side) {
            (Some(b), QSide::Bid) => b.bids.clone(),
            (Some(b), QSide::Ask) => b.asks.clone(),
            (None, _) => Vec::new(),
        };
        match action {
            QuoteAction::Keep => {}
            QuoteAction::Replace(None) => {
                self.virt.remove(&side);
            }
            QuoteAction::Place(t) | QuoteAction::Modify(t) | QuoteAction::Replace(Some(t)) => {
                let placed = sim::placement_ts_us(
                    self.book.as_ref().and_then(|b| b.ts_ms),
                    self.newest_print_ts_us,
                );
                let vq = sim::join(side, t.px, t.qty, &levels, placed);
                self.virt.insert(side, (vq, t.qty));
            }
        }
    }

    fn paper_flatten(&mut self, now: u64) {
        // A paper flatten needs a touch to price at; without a book it waits
        // for the next tick (live flattens price off the connector instead).
        let Some(book) = &self.book else { return };
        let (bid, ask) = (book.bid, book.ask);
        for step in flatten_steps(
            self.ledger.position.qty,
            self.virt.contains_key(&QSide::Bid),
            self.virt.contains_key(&QSide::Ask),
        ) {
            match step {
                Step::Cancel(side) => {
                    self.virt.remove(&side);
                }
                Step::Ioc { side, qty } => {
                    let buy = side == OrderSide::Long;
                    let px = if buy { ask } else { bid };
                    let fee = self.fee(qty * px, false);
                    self.sim_seq += 1;
                    let fill = FillIn {
                        trade_id: format!("sim-{}-ioc", self.sim_seq),
                        buy,
                        qty,
                        px,
                        fee,
                        maker: false,
                        order_id: "sim-ioc".to_string(),
                    };
                    if let Err(e) = self.book(fill, now) {
                        log::error!("[ARCUS_VOL] paper flatten not recorded: {e}");
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------- live

    async fn live_fills(&mut self, now: u64) {
        let market = self.cfg.market.clone();
        let rows = match self.dex.get_filled_orders(&market).await {
            Ok(r) => r.orders,
            Err(e) => {
                self.on_error("get_filled_orders", &e);
                return;
            }
        };
        for f in rows {
            let bookable = match (f.filled_side, f.filled_size, f.filled_value) {
                (Some(side), Some(qty), Some(value)) if !f.is_rejected && !qty.is_zero() => {
                    Some((side, qty, value))
                }
                _ => None,
            };
            let Some((side, qty, value)) = bookable else {
                // Nothing to book: let the connector forget it.
                log::warn!(
                    "[ARCUS_VOL] fill {} not bookable (rejected/empty)",
                    f.trade_id
                );
                self.clear_fill(&market, &f.trade_id).await;
                continue;
            };
            let fee = f.filled_fee.unwrap_or(Decimal::ZERO);
            let maker = if self.ioc_ids.contains(&f.order_id) {
                false
            } else if self.quote_ids.contains(&f.order_id) {
                true
            } else {
                fee <= Decimal::ZERO
            };
            let fill = FillIn {
                trade_id: f.trade_id.clone(),
                buy: side == OrderSide::Long,
                qty,
                px: value / qty,
                fee,
                maker,
                order_id: f.order_id.clone(),
            };
            let booking = self.book(fill, now);
            match &booking {
                Ok(Booking::Booked(_)) => {
                    for r in self.resting.values_mut() {
                        if r.order_id == f.order_id {
                            r.filled += qty;
                        }
                    }
                    self.resting.retain(|_, r| r.filled < r.qty);
                }
                Ok(Booking::AlreadyBooked) => {}
                Err(e) => {
                    // Kept in the connector; retried next tick.
                    log::error!(
                        "[ARCUS_VOL] fill {} not written, will retry: {e}",
                        f.trade_id
                    );
                    continue;
                }
            }
            // The connector may forget the fill only once the booking is in
            // state.json (Codex P1, pairtrade#361).
            let persisted = match persist_json(&self.state_path, &self.ledger) {
                Ok(()) => true,
                Err(e) => {
                    log::error!(
                        "[ARCUS_VOL] state write failed after fill {}, will retry: {e:#}",
                        f.trade_id
                    );
                    false
                }
            };
            if may_forget_fill(&booking, persisted) {
                self.clear_fill(&market, &f.trade_id).await;
            }
        }
    }

    async fn clear_fill(&self, market: &str, trade_id: &str) {
        if let Err(e) = self.dex.clear_filled_order(market, trade_id).await {
            log::debug!("[ARCUS_VOL] clear_filled_order {trade_id}: {e}");
        }
    }

    async fn venue_qty(&mut self) -> Option<(Decimal, Option<Decimal>)> {
        match self.dex.get_positions().await {
            Ok(positions) => {
                let base = self
                    .cfg
                    .market
                    .trim_end_matches("-USD")
                    .to_ascii_uppercase();
                let hit = positions.into_iter().find(|p| {
                    p.symbol
                        .trim_end_matches("-USD")
                        .eq_ignore_ascii_case(&base)
                });
                Some(match hit {
                    Some(p) => (p.size.abs() * Decimal::from(p.sign.signum()), p.entry_price),
                    None => (Decimal::ZERO, None),
                })
            }
            Err(e) => {
                self.on_error("get_positions", &e);
                None
            }
        }
    }

    /// Adopt the venue position when it disagrees with the ledger for two
    /// polls in a row (a single disagreement is usually a fill in flight).
    async fn live_position_check(&mut self, now: u64, force: bool) {
        let Some((venue, entry)) = self.venue_qty().await else {
            return;
        };
        let ours = self.ledger.position.qty;
        if (venue - ours).abs() <= Decimal::new(1, 8) {
            self.position_mismatch_since_ms = None;
            return;
        }
        let persisted = self
            .position_mismatch_since_ms
            .is_some_and(|t| now.saturating_sub(t) >= self.cfg.position_poll_secs * 1_000);
        if force || persisted {
            log::warn!("[ARCUS_VOL] position: ledger {ours} ≠ venue {venue}; adopting venue");
            let p = &mut self.ledger.position;
            p.qty = venue;
            if venue.is_zero() {
                p.avg_px = Decimal::ZERO;
                p.opened_at_ms = None;
            } else {
                if let Some(e) = entry {
                    p.avg_px = e;
                }
                if p.opened_at_ms.is_none() {
                    p.opened_at_ms = Some(now);
                }
            }
            self.position_mismatch_since_ms = None;
        } else if self.position_mismatch_since_ms.is_none() {
            self.position_mismatch_since_ms = Some(now);
        }
    }

    async fn live_reconcile_orders(&mut self) {
        let market = self.cfg.market.clone();
        match self.dex.get_open_orders(&market).await {
            Ok(open) => {
                let ids: HashSet<String> = open.orders.iter().map(|o| o.order_id.clone()).collect();
                self.resting.retain(|_, r| ids.contains(&r.order_id));
                let ours: HashSet<String> =
                    self.resting.values().map(|r| r.order_id.clone()).collect();
                for stray in ids.difference(&ours) {
                    log::warn!("[ARCUS_VOL] cancelling stray open order {stray}");
                    if let Err(e) = self.dex.cancel_order(&market, stray).await {
                        self.on_error("cancel stray", &e);
                    }
                }
            }
            Err(e) => self.on_error("get_open_orders", &e),
        }
    }

    /// Hard reset after an ambiguous order outcome: cancel everything on the
    /// market, forget local quotes, re-read the position.
    async fn live_full_reconcile(&mut self, now: u64) {
        let market = self.cfg.market.clone();
        if let Err(e) = self.dex.cancel_all_orders(Some(market.clone())).await {
            self.on_error("reconcile cancel_all", &e);
            return;
        }
        self.resting.clear();
        self.live_position_check(now, true).await;
        self.need_reconcile = false;
        log::info!(
            "[ARCUS_VOL] reconciled: all quotes cancelled, inventory {}",
            self.ledger.position.qty
        );
    }

    async fn live_cancel(&mut self, side: QSide) -> bool {
        let Some(r) = self.resting.get(&side).cloned() else {
            return true;
        };
        match self
            .dex
            .cancel_order(&self.cfg.market.clone(), &r.order_id)
            .await
        {
            Ok(()) => {
                self.resting.remove(&side);
                true
            }
            Err(e) => {
                self.on_error(&format!("cancel {}", side.as_str()), &e);
                false
            }
        }
    }

    async fn live_flatten(&mut self, now: u64) {
        if now < self.flatten_inflight_until_ms {
            return;
        }
        let steps = flatten_steps(
            self.ledger.position.qty,
            self.resting.contains_key(&QSide::Bid),
            self.resting.contains_key(&QSide::Ask),
        );
        for step in steps {
            match step {
                Step::Cancel(side) => {
                    // Never IOC while our own quote may still rest on the
                    // side it would hit (no self-trade prevention on Arcus).
                    if !self.live_cancel(side).await {
                        return;
                    }
                }
                Step::Ioc { side, qty } => {
                    let market = self.cfg.market.clone();
                    match self
                        .dex
                        .create_order_taker_ioc(
                            &market,
                            qty,
                            side,
                            self.cfg.flatten_slippage_bps,
                            true,
                        )
                        .await
                    {
                        Ok(resp) => {
                            self.ioc_ids.insert(resp.order_id);
                        }
                        Err(e) => self.on_error("flatten IOC", &e),
                    }
                    // Let the fill arrive (and the position poll catch up)
                    // before a retry; reduce-only bounds any overlap anyway.
                    self.flatten_inflight_until_ms = now + 3_000;
                    self.last_position_ms = 0;
                }
            }
        }
    }

    async fn live_quotes(&mut self, plan: Vec<(QSide, QuoteAction)>) {
        let market = self.cfg.market.clone();
        let mut places: Vec<QuoteTarget> = Vec::new();
        let mut modifies: Vec<(QSide, QuoteTarget, String)> = Vec::new();
        for (side, action) in plan {
            match action {
                QuoteAction::Keep => {}
                QuoteAction::Place(t) => places.push(t),
                QuoteAction::Modify(t) => {
                    let id = self.resting[&side].order_id.clone();
                    modifies.push((side, t, id));
                }
                QuoteAction::Replace(target) => {
                    if self.live_cancel(side).await {
                        if let Some(t) = target {
                            places.push(t);
                        }
                    }
                }
            }
        }
        if !modifies.is_empty() {
            let reqs = modifies
                .iter()
                .map(|(side, t, id)| BatchModifyRequest {
                    symbol: market.clone(),
                    order_id: id.clone(),
                    side: side.order_side(),
                    target_total_size: t.qty,
                    price: t.px,
                    post_only: true,
                    reduce_only: false,
                })
                .collect();
            match self.dex.modify_orders_batch(reqs).await {
                Ok(rows) => {
                    for ((side, _, _), row) in modifies.into_iter().zip(rows) {
                        match row {
                            Ok(resp) => {
                                self.quote_ids.insert(resp.order_id.clone());
                                self.resting.insert(
                                    side,
                                    Resting {
                                        order_id: resp.order_id,
                                        px: resp.ordered_price,
                                        qty: resp.ordered_size,
                                        filled: Decimal::ZERO,
                                    },
                                );
                            }
                            Err(e) => self.on_error(&format!("modify {}", side.as_str()), &e),
                        }
                    }
                }
                Err(e) => self.on_error("modify batch", &e),
            }
        }
        if !places.is_empty() {
            let reqs = places
                .iter()
                .map(|t| BatchOrderRequest {
                    symbol: market.clone(),
                    side: t.side.order_side(),
                    size: t.qty,
                    price: t.px,
                    post_only: true,
                    reduce_only: false,
                })
                .collect();
            match self.dex.create_orders_batch(reqs).await {
                Ok(rows) => {
                    for (t, row) in places.into_iter().zip(rows) {
                        match row {
                            Ok(resp) => {
                                self.quote_ids.insert(resp.order_id.clone());
                                self.resting.insert(
                                    t.side,
                                    Resting {
                                        order_id: resp.order_id,
                                        px: resp.ordered_price,
                                        qty: resp.ordered_size,
                                        filled: Decimal::ZERO,
                                    },
                                );
                            }
                            Err(e) => self.on_error(&format!("place {}", t.side.as_str()), &e),
                        }
                    }
                }
                Err(e) => self.on_error("place batch", &e),
            }
        }
    }

    async fn pull_quotes(&mut self) {
        if self.cfg.dry_run {
            self.virt.clear();
            return;
        }
        for side in [QSide::Bid, QSide::Ask] {
            self.live_cancel(side).await;
        }
    }

    fn current_quote(&self, side: QSide) -> Option<Resting> {
        if self.cfg.dry_run {
            self.virt.get(&side).map(|(vq, orig)| Resting {
                order_id: "sim".to_string(),
                px: vq.px,
                qty: *orig,
                filled: *orig - vq.remaining,
            })
        } else {
            self.resting.get(&side).cloned()
        }
    }

    // ----------------------------------------------------------------- tick

    async fn read_book(&mut self, now: u64) {
        match self.dex.get_order_book(&self.cfg.market.clone(), 10).await {
            Ok(b) => {
                let (Some(bid), Some(ask)) = (b.bids.first(), b.asks.first()) else {
                    self.book = None;
                    return;
                };
                self.book = Some(BookView {
                    bid: bid.price,
                    ask: ask.price,
                    ts_ms: b.book_ts_ms,
                    bids: b.bids,
                    asks: b.asks,
                });
            }
            Err(e) => {
                self.book = None;
                if now.saturating_sub(self.last_book_warn_ms) > 30_000 {
                    log::warn!("[ARCUS_VOL] order book unavailable: {e}");
                    self.last_book_warn_ms = now;
                }
            }
        }
    }

    async fn tick(&mut self) {
        let now = now_ms();
        self.read_book(now).await;
        let mid = self.book.as_ref().map(BookView::mid);
        if let Some(m) = mid {
            self.mid_hist.push_back((now, m));
            let keep = self.cfg.shock_window_secs * 1_000 * 2;
            while self
                .mid_hist
                .front()
                .is_some_and(|(t, _)| now.saturating_sub(*t) > keep)
            {
                self.mid_hist.pop_front();
            }
        }
        let mark = mid.unwrap_or(self.ledger.position.avg_px);
        if self.ledger.rollover(&utc_day(), mark) {
            log::info!("[ARCUS_VOL] new UTC day {}", self.ledger.day);
        }

        if !self.cfg.dry_run {
            self.live_fills(now).await;
            // Refresh on schedule; after a failure, retry every tick (no new
            // quoting meanwhile, see `dms_armed` in the tick plan).
            let due = self.dms_last_failed
                || self
                    .dms_last_ok_ms
                    .is_none_or(|t| now.saturating_sub(t) >= self.cfg.dms_refresh_secs * 1_000);
            if due {
                match self.dex.schedule_cancel(Some(self.cfg.dms_secs)).await {
                    Ok(()) => {
                        self.dms_last_ok_ms = Some(now);
                        self.dms_last_failed = false;
                    }
                    Err(e) => {
                        self.dms_last_failed = true;
                        self.on_error("schedule_cancel (no new quotes until armed)", &e);
                    }
                }
            }
            if self.need_reconcile {
                self.live_full_reconcile(now).await;
                self.finish_tick(now, mid);
                return;
            }
            if now.saturating_sub(self.last_reconcile_ms) >= self.cfg.reconcile_secs * 1_000 {
                self.live_reconcile_orders().await;
                self.last_reconcile_ms = now;
            }
            if now.saturating_sub(self.last_position_ms) >= self.cfg.position_poll_secs * 1_000 {
                self.live_position_check(now, false).await;
                self.last_position_ms = now;
            }
        }
        if let Some(m) = mid {
            for (fill_id, h, bps) in due_markouts(&mut self.pending_markouts, now, m) {
                let row = json!({"kind": "markout", "ts_ms": now, "fill_id": fill_id,
                                 "horizon_s": h, "bps": bps.round_dp(3).to_string(),
                                 "mid": m.to_string()});
                if let Err(e) = append_jsonl(&self.fills_path, &row) {
                    log::warn!("[ARCUS_VOL] markout append failed: {e}");
                }
            }
        }

        // Stops.
        let kill = self.kill_path.exists();
        let halt_file = self.halt_path.exists();
        let risk = risk_check(
            &mut self.ledger,
            mark,
            kill,
            halt_file,
            self.cfg.daily_stop_usd,
            self.cfg.cum_stop_usd,
        );
        for e in &risk.events {
            log::warn!("[ARCUS_VOL] {e}");
        }
        if risk.write_halt_file {
            let body = self.ledger.sticky_reason.clone().unwrap_or_default();
            if let Err(e) = std::fs::write(&self.halt_path, format!("{body}\n")) {
                log::error!("[ARCUS_VOL] cannot write HALT: {e}");
            }
        }
        self.halt = risk.halt;

        // Startup / halt / kill / max-hold flatten need no book; only the
        // cap check uses the mid (Codex P1, pairtrade#361).
        let halt_label = self.halt.as_ref().map(Halt::label);
        let startup = self.startup_flatten && !self.ledger.position.qty.is_zero();
        if !startup {
            self.startup_flatten = false;
        }
        let flatten = flatten_reason(
            self.ledger.position.qty,
            mid,
            self.cfg.effective_cap_usd(),
            self.ledger.position.opened_at_ms,
            now,
            self.cfg.max_hold_secs,
            halt_label.as_deref(),
            startup,
        );
        let stale = book_stale(
            self.book.as_ref().and_then(|b| b.ts_ms),
            now,
            self.cfg.book_stale_secs,
        );
        if shock(
            &self.mid_hist,
            now,
            self.cfg.shock_window_secs * 1_000,
            self.cfg.shock_bps,
        ) {
            if now >= self.cooldown_until_ms {
                log::info!(
                    "[ARCUS_VOL] book shock: pulling quotes for {}s",
                    self.cfg.cooldown_secs
                );
            }
            self.cooldown_until_ms = now + self.cfg.cooldown_secs * 1_000;
        }
        // Safety actions are never gated by a 429 backoff; only new
        // placement / modification is (Codex P1, pairtrade#361).
        let plan = tick_plan(&TickInputs {
            has_book: mid.is_some(),
            halted: self.halt.is_some(),
            stale,
            shock_or_cooldown: now < self.cooldown_until_ms,
            backoff: now < self.backoff_until_ms,
            dms_armed: self.cfg.dry_run
                || dms_armed(
                    self.dms_last_ok_ms,
                    self.dms_last_failed,
                    now,
                    self.cfg.dms_secs,
                ),
            flatten,
        });
        match plan {
            TickPlan::Flatten(reason) => {
                log::info!(
                    "[ARCUS_VOL] flatten {:?}: inventory {}",
                    reason,
                    self.ledger.position.qty
                );
                if self.cfg.dry_run {
                    self.paper_flatten(now);
                } else {
                    self.live_flatten(now).await;
                }
                self.finish_tick(now, mid);
                return;
            }
            TickPlan::PullQuotes(_) => {
                self.pull_quotes().await;
                self.finish_tick(now, mid);
                return;
            }
            TickPlan::Wait => {
                self.finish_tick(now, mid);
                return;
            }
            TickPlan::Quote => {}
        }

        let (bid_px, ask_px) = {
            let b = self.book.as_ref().expect("mid implies book");
            (b.bid, b.ask)
        };
        let (bid, ask) = plan_quotes(
            bid_px,
            ask_px,
            self.ledger.position.qty,
            &self.quote_params(),
        );
        let plan: Vec<(QSide, QuoteAction)> = [(QSide::Bid, bid), (QSide::Ask, ask)]
            .into_iter()
            .map(|(side, target)| {
                let current = self.current_quote(side);
                (side, quote_action(current.as_ref(), target.as_ref()))
            })
            .collect();
        if self.cfg.dry_run {
            for (side, action) in plan {
                self.paper_quote(side, action);
            }
        } else {
            self.live_quotes(plan).await;
        }
        self.finish_tick(now, mid);
    }

    fn finish_tick(&mut self, now: u64, mid: Option<Decimal>) {
        if let Err(e) = persist_json(&self.state_path, &self.ledger) {
            log::error!("[ARCUS_VOL] state write failed: {e:#}");
        }
        let mark = mid.unwrap_or(self.ledger.position.avg_px);
        let quote = |side: QSide| {
            self.current_quote(side).map(|r| {
                json!({"px": r.px.to_string(), "qty": r.qty.to_string(),
                       "filled": r.filled.to_string(), "order_id": r.order_id})
            })
        };
        let l = &self.ledger;
        let status = json!({
            "bot": "arcus_vol_runtime",
            "ts_ms": now,
            "mode": self.cfg.mode(),
            "market": self.cfg.market,
            "book": self.book.as_ref().map(|b| json!({"bid": b.bid.to_string(), "ask": b.ask.to_string()})),
            "quotes": {"bid": quote(QSide::Bid), "ask": quote(QSide::Ask)},
            "inventory": {"qty": l.position.qty.to_string(),
                          "usd": (l.position.qty * mark).round_dp(2).to_string(),
                          "avg_px": l.position.avg_px.round_dp(4).to_string(),
                          "opened_at_ms": l.position.opened_at_ms},
            "pnl": {"daily_net": l.daily_net(mark).round_dp(4).to_string(),
                    "cum_net": l.cum_net(mark).round_dp(4).to_string(),
                    "cum_realized": l.cum_realized.round_dp(4).to_string(),
                    "cum_fees": l.cum_fees.round_dp(4).to_string()},
            "volume": {"day": l.day_volume.round_dp(2).to_string(),
                       "cum": l.cum_volume.round_dp(2).to_string(),
                       "cum_maker": l.cum_maker_volume.round_dp(2).to_string(),
                       "cum_taker": l.cum_taker_volume.round_dp(2).to_string(),
                       "fills": l.fills},
            "cost_per_1m_usd": l.cost_per_million(mark).map(|c| c.round_dp(2).to_string()),
            "halt": self.halt.as_ref().map(Halt::label),
            "cooldown": now < self.cooldown_until_ms,
            "backoff": now < self.backoff_until_ms,
            "effective_cap_usd": self.cfg.effective_cap_usd().to_string(),
        });
        if let Err(e) = debot::directional::atomic_write(&self.status_path, &status.to_string()) {
            log::warn!("[ARCUS_VOL] status write failed: {e}");
        }
        if now.saturating_sub(self.last_summary_ms) >= 60_000 {
            self.last_summary_ms = now;
            log::info!(
                "[ARCUS_VOL] {} inv {} day_net {} cum_net {} vol_day {} vol_cum {} cost/1M {} halt {:?}",
                self.cfg.mode(),
                l.position.qty,
                l.daily_net(mark).round_dp(2),
                l.cum_net(mark).round_dp(2),
                l.day_volume.round_dp(0),
                l.cum_volume.round_dp(0),
                l.cost_per_million(mark)
                    .map(|c| c.round_dp(2).to_string())
                    .unwrap_or_else(|| "-".to_string()),
                self.halt.as_ref().map(Halt::label),
            );
        }
    }

    async fn shutdown(&mut self) {
        let mut cancel_ok = false;
        let mut open_after: Option<usize> = None;
        for step in shutdown_steps(!self.cfg.dry_run) {
            match step {
                ShutdownStep::CancelAll => {
                    match self
                        .dex
                        .cancel_all_orders(Some(self.cfg.market.clone()))
                        .await
                    {
                        Ok(()) => {
                            cancel_ok = true;
                            self.resting.clear();
                        }
                        Err(e) => log::error!(
                            "[ARCUS_VOL] SHUTDOWN CANCEL_ALL FAILED: {e}; quotes may still rest, DMS stays armed"
                        ),
                    }
                }
                ShutdownStep::VerifyNoneResting => {
                    match self.dex.get_open_orders(&self.cfg.market.clone()).await {
                        Ok(open) => {
                            open_after = Some(open.orders.len());
                            if !open.orders.is_empty() {
                                log::error!(
                                    "[ARCUS_VOL] {} order(s) still resting after cancel_all: {:?}",
                                    open.orders.len(),
                                    open.orders.iter().map(|o| &o.order_id).collect::<Vec<_>>()
                                );
                            }
                        }
                        Err(e) => log::error!(
                            "[ARCUS_VOL] open-orders read-back failed: {e}; DMS stays armed"
                        ),
                    }
                }
                ShutdownStep::HarvestFills => {
                    // Fills that landed before the cancel acks; bounded so a
                    // dead venue cannot hold the shutdown hostage.
                    let harvest = async {
                        for _ in 0..3 {
                            self.live_fills(now_ms()).await;
                            tokio::time::sleep(Duration::from_millis(700)).await;
                        }
                    };
                    if tokio::time::timeout(Duration::from_secs(5), harvest)
                        .await
                        .is_err()
                    {
                        log::warn!("[ARCUS_VOL] shutdown fill harvest timed out");
                    }
                }
                ShutdownStep::Persist => {
                    self.virt.clear();
                    self.finish_tick(now_ms(), self.book.as_ref().map(BookView::mid));
                }
                ShutdownStep::DisarmDms => {
                    if !may_disarm_dms(cancel_ok, open_after) {
                        log::error!(
                            "[ARCUS_VOL] DMS LEFT ARMED (cancel_ok={cancel_ok}, open_after={open_after:?}); the venue cancels everything when it fires"
                        );
                    } else if let Err(e) = self.dex.schedule_cancel(None).await {
                        log::warn!("[ARCUS_VOL] shutdown DMS disarm failed: {e}");
                    }
                }
            }
        }
        log::info!(
            "[ARCUS_VOL] stopped; inventory left open: {} (not flattened on shutdown)",
            self.ledger.position.qty
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = Config::from_env()?;
    config::live_gate(cfg.dry_run, &cfg.live_confirm, cfg.account_index).map_err(|e| anyhow!(e))?;
    log::info!(
        "[ARCUS_VOL] start mode={} market={} clip=${} skew=${} hard_cap=${} margin=${}x{} → effective_cap=${} (quotes sized to ${}) max_hold={}s daily_stop=${} cum_stop=${} state={}",
        cfg.mode(),
        cfg.market,
        cfg.clip_usd,
        cfg.skew_usd,
        cfg.hard_cap_usd,
        cfg.margin_usd,
        cfg.leverage,
        cfg.effective_cap_usd(),
        cfg.quote_cap_usd(),
        cfg.max_hold_secs,
        cfg.daily_stop_usd,
        cfg.cum_stop_usd,
        cfg.state_dir.display()
    );
    std::fs::create_dir_all(&cfg.state_dir)
        .with_context(|| format!("create {}", cfg.state_dir.display()))?;
    let state_path = cfg.state_dir.join("state.json");
    let ledger = match load_json::<Ledger>(&state_path)? {
        Some(l) if l.mode != cfg.mode() => bail!(
            "{} was written in mode {} but this run is {}; move it aside first",
            state_path.display(),
            l.mode,
            cfg.mode()
        ),
        Some(l) => l,
        None => Ledger::new(cfg.mode(), &utc_day()),
    };
    let mut ledger = ledger;
    let fills_path = cfg.state_dir.join("fills.jsonl");
    // Close the crash window between the fsynced fills.jsonl row and the
    // state.json write (Codex P1, pairtrade#361): book any row state missed.
    if let Ok(text) = std::fs::read_to_string(&fills_path) {
        let rows: Vec<serde_json::Value> = text
            .lines()
            .filter_map(|line| match serde_json::from_str(line) {
                Ok(v) => Some(v),
                Err(_) => {
                    // A torn tail from a crash mid-write was never booked.
                    log::warn!("[ARCUS_VOL] skipping unparsable fills.jsonl line");
                    None
                }
            })
            .collect();
        let replayed = ledger::replay_fills(&mut ledger, &rows)
            .map_err(|e| anyhow!("fills.jsonl replay: {e}"))?;
        if replayed > 0 {
            persist_json(&state_path, &ledger)?;
            log::warn!(
                "[ARCUS_VOL] replayed {replayed} fill(s) from fills.jsonl missing in state.json"
            );
        }
    }

    let dex = DexConnectorBox::create(
        "arcus",
        cfg.dry_run,
        std::slice::from_ref(&cfg.market),
        None,
    )
    .await
    .context("create Arcus connector")?;
    dex.start().await.context("start Arcus connector")?;

    let mut rt = Runtime {
        dex,
        ledger,
        state_path,
        status_path: cfg.state_dir.join("status.json"),
        fills_path,
        kill_path: cfg.state_dir.join("KILL_SWITCH"),
        halt_path: cfg.state_dir.join("HALT"),
        resting: HashMap::new(),
        virt: HashMap::new(),
        book: None,
        mid_hist: VecDeque::new(),
        cooldown_until_ms: 0,
        backoff_until_ms: 0,
        need_reconcile: false,
        startup_flatten: false,
        flatten_inflight_until_ms: 0,
        pending_markouts: Vec::new(),
        quote_ids: HashSet::new(),
        ioc_ids: HashSet::new(),
        newest_print_ts_us: 0,
        position_mismatch_since_ms: None,
        dms_last_ok_ms: None,
        dms_last_failed: false,
        last_reconcile_ms: 0,
        last_position_ms: 0,
        last_summary_ms: 0,
        last_book_warn_ms: 0,
        sim_seq: 0,
        halt: None,
        cfg,
    };

    let (tx, mut rx) = mpsc::channel::<tape::Print>(4_096);
    if rt.cfg.dry_run {
        tokio::spawn(tape::run(rt.cfg.ws_url.clone(), rt.cfg.market.clone(), tx));
    } else {
        drop(tx);
        let market = rt.cfg.market.clone();
        rt.dex
            .set_leverage(&market, rt.cfg.leverage)
            .await
            .context("set_leverage")?;
        rt.dex
            .cancel_all_orders(Some(market))
            .await
            .context("startup cancel_all")?;
        rt.live_position_check(now_ms(), true).await;
        rt.startup_flatten = !rt.ledger.position.qty.is_zero();
        if rt.startup_flatten {
            log::warn!(
                "[ARCUS_VOL] startup inventory {} → flatten before quoting",
                rt.ledger.position.qty
            );
        }
    }

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("SIGTERM handler")?;
    // One persistent SIGINT stream: a `ctrl_c()` future re-created on every
    // loop turn misses a signal that lands while a tick is awaiting IO.
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("SIGINT handler")?;
    let mut interval = tokio::time::interval(Duration::from_millis(rt.cfg.tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => rt.tick().await,
            Some(p) = rx.recv() => rt.on_print(p),
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }
    rt.shutdown().await;
    Ok(())
}
