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
//! Presence quoting (`ARCUS_VOL_QUOTE_OFFSET_BPS` > 0, bot-strategy#1093
//! option B): quotes rest that many bp BEHIND the touch instead of at it and
//! are kept while their distance from the touch stays within
//! `ARCUS_VOL_REPEG_BAND_BPS` (> 0, < the offset) of the offset; outside the
//! band they are re-pegged by cancel + place. At an offset a quote fills
//! only when a sweep reaches it, so volume and adverse selection are near
//! zero; the mode exists to test whether resting quotes alone ("quoting
//! tight, deep, and consistently") earn Arcus market-making points. Details:
//! - Live sends the unrounded offset price; the connector rounds a resting
//!   price away from the touch to the venue tick and reports the price it
//!   used, which is what the band is judged on. The runtime needs no tick,
//!   so the band must be wider than one tick in bp of price: config
//!   requires a band of at least 1 bp, and on a coarse-tick market (tick
//!   ≥ 1 bp of price) the band must be set above the tick.
//! - Live, the peg reference excludes our own resting quote, so a quote
//!   that becomes the best price is not re-pegged off itself. With no
//!   reference on a side (only our quote is displayed there) nothing is
//!   placed and a resting offset quote stays where it is; one that must not
//!   quote, or whose size is off, is cancelled.
//! - The feed's book is at times crossed (a few ticks, or longer when one
//!   side of it freezes). A crossed book prices nothing: nothing is placed
//!   until it uncrosses, and resting offset quotes are not cancelled for
//!   it, except one whose size is off or that is no longer `offset − band`
//!   behind BOTH book tops (nothing says which side is the stale one).
//! - During a 429 backoff nothing is placed, but the plan still runs for
//!   its cancels, so a quote that must go is not left resting.
//! - After a sweep fill the inventory-reducing side quotes AT the touch,
//!   sized to the inventory, to exit as a maker; the growing side stays at
//!   the offset. Skew, cap, max-hold flatten, stops and the DMS are as at
//!   the touch.
//! - DRY_RUN reads a 100-level book and, best-effort, the venue tick, so a
//!   deep virtual quote is rounded like a live one and joins behind the
//!   size displayed at its price. Without a tick, or at a price with no
//!   displayed level, it joins with queue 0, so paper presence fills are
//!   approximate (optimistic on queue position, and they still need a print
//!   through the price).
//!
//! `status.json` shows `quote_offset_bps`, `repeg_band_bps`, `paper_tick`
//! (DRY_RUN only) and, per quote, `dist_bps` (from the peg reference the
//! band is judged on) and `dist_touch_bps` (from the raw touch). Offset 0
//! (default) is the at-touch behaviour, unchanged.
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

use anyhow::{anyhow, Context, Result};
use config::Config;
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{
    BatchModifyRequest, BatchOrderRequest, DexConnector, DexError, OrderBookLevel, OrderSide,
};
use ledger::{
    append_synced, due_markouts, may_forget_fill, risk_check, Booking, FillIn, Halt, Ledger,
    PendingMarkout,
};
use logic::{
    book_depth, dms_armed, dust_hold_clock, flatten_halt_label, flatten_reason, flatten_steps,
    fresh_mark, is_dust, market_position, may_disarm_dms, own_displayed, paper_tick_due,
    plan_inputs, plan_quotes, position_check, position_gate_after, quote_action, quote_dists,
    quote_pass, read_within, reconcile_cleared, send_gated, shock, shutdown_steps, spill_rows,
    startup_latch_after, tick_plan, touches, BatchSink, PlanState, PosCheck, PricePlan, QSide,
    QuoteAction, QuoteParams, QuoteTarget, Resting, ShutdownStep, SidePlan, Step, TickPlan,
};
use rust_decimal::Decimal;
use serde_json::json;
use sim::VirtualQuote;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write as _;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// A print stamped more than this ahead of our clock is clamped to now;
/// one older than `PRINT_MAX_AGE_MS` is not a live print and is skipped.
const PRINT_FUTURE_TOLERANCE_MS: u64 = 5_000;
const PRINT_MAX_AGE_MS: u64 = 300_000;
/// A connector fill record missing side/size/value waits this long, then
/// escalates to a sticky halt `fill_incomplete`.
const FILL_INCOMPLETE_WAIT_MS: u64 = 60_000;
/// A due markout horizon waits this long for a fresh mid, then is recorded
/// as `markout_missing`.
const MARKOUT_MAX_LATE_MS: u64 = 60_000;
/// Shutdown keeps polling incomplete fill records this long before
/// spilling the rest to pending_fills.jsonl (pre-G2, Codex P1 4146269818).
const SHUTDOWN_HARVEST_BUDGET: Duration = Duration::from_secs(30);
/// Minimum time the final shutdown spill read gets, even when the harvest
/// used up the budget.
const SPILL_READ_FLOOR: Duration = Duration::from_secs(5);

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

/// DRY_RUN presence quoting: how often the paper sim re-reads the venue tick
/// (tick tiers depend on price), how soon a failed read is retried, and how
/// long one read may take.
const TICK_REFRESH_MS: u64 = 60_000;
const TICK_RETRY_MS: u64 = 30_000;
const TICK_READ_TIMEOUT: Duration = Duration::from_secs(2);

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
    /// Durable, change-only state.json writes.
    state_writer: ledger::StateWriter,
    status_path: PathBuf,
    fills_path: PathBuf,
    /// Connector fill records still unbooked at the last shutdown (pre-G2,
    /// Codex P1 4146269818); resolved first at the next live start.
    pending_path: PathBuf,
    /// Trade ids still unresolved in pending_fills.jsonl (status.json).
    pending_unresolved: Vec<String>,
    /// Connector fill records the last harvest saw but couldn't book and
    /// clear: the fallback for a stalled shutdown spill read.
    unbooked_seen: Vec<dex_connector::FilledOrder>,
    last_pending_warn_ms: u64,
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
    /// The last periodic position check didn't clear (Pending / Unread):
    /// no new quotes, re-checked every tick (pre-G2, Codex P1 4146269811).
    position_pending: bool,
    startup_flatten: bool,
    /// Rate limit for the dust warning (one line per 10 min).
    dust_logged_until_ms: u64,
    /// The inventory while it was last dust (runtime only, never persisted):
    /// the max-hold clock restarts only once a fill changes it.
    dust_qty: Option<Decimal>,
    flatten_inflight_until_ms: u64,
    pending_markouts: Vec<PendingMarkout>,
    quote_ids: HashSet<String>,
    ioc_ids: HashSet<String>,
    /// Newest public print (venue µs) applied to the paper sim.
    newest_print_ts_us: u64,
    /// DRY_RUN: trades tape health (no virtual quotes until it is up).
    tape: tape::TapeHealth,
    /// Last mid from a live book (the UTC-day rollover mark).
    last_mark: Option<Decimal>,
    /// The last tick planned a flatten (paper prints are ignored meanwhile).
    flatten_pending: bool,
    /// A rollover whose state write failed: retried before any fill harvest.
    rollover_unpersisted: bool,
    /// The last `ensure_rolled` said no: nothing books, nothing new quotes.
    rollover_blocked: bool,
    /// The last tick plan, for status.json.
    last_plan: String,
    last_mark_warn_ms: u64,
    last_roll_warn_ms: u64,
    /// Connector fill records missing fields: first seen (ms).
    incomplete_since: HashMap<String, u64>,
    /// A failed append could not be rolled back: no appends, no new quoting
    /// until a restart repairs fills.jsonl.
    journal_unsafe: bool,
    /// Live fills with no reported fee: first seen (ms).
    fee_wait_since: HashMap<String, u64>,
    position_mismatch_since_ms: Option<u64>,
    /// Live: the last fill harvest succeeded and booked everything it got.
    fills_synced: bool,
    /// Last successful DMS arm/refresh, and whether the latest attempt failed.
    dms_last_ok_ms: Option<u64>,
    dms_last_failed: bool,
    last_reconcile_ms: u64,
    last_position_ms: u64,
    last_summary_ms: u64,
    last_book_warn_ms: u64,
    /// DRY_RUN presence quoting: the venue's price tick, read best-effort
    /// for the paper sim only (`refresh_paper_tick`). Live never needs one.
    paper_tick: Option<Decimal>,
    next_tick_read_ms: u64,
    last_peg_warn_ms: u64,
    sim_seq: u64,
    /// Process start (ms): the per-run part of simulated ids.
    run_id: u64,
    /// DRY_RUN: set when a paper fill could not be booked; the sim stops
    /// for the rest of this run rather than silently dropping executions.
    sim_halted: Option<String>,
    /// Live: the startup position read succeeded and is in the ledger.
    startup_reconciled: bool,
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
            presence: self.cfg.presence(),
        }
    }

    /// DRY_RUN presence quoting only: a best-effort read of the venue's
    /// price tick, so a virtual quote is rounded like a live one and lands
    /// on a real price level (`sim::paper_px`). It never gates quoting. It
    /// runs with the tick's other awaited reads, before the plan is decided
    /// on a fresh clock (so nothing is awaited between the decision and
    /// `paper_quote`), never during a 429 backoff, is bounded by
    /// `TICK_READ_TIMEOUT`, and a failed read goes through `on_error` (so a
    /// 429 backs off) and is retried later.
    async fn refresh_paper_tick(&mut self, now: u64) {
        if !paper_tick_due(
            self.cfg.dry_run,
            self.cfg.presence().on(),
            now,
            self.next_tick_read_ms,
            self.backoff_until_ms,
        ) {
            return;
        }
        self.next_tick_read_ms = now + TICK_RETRY_MS;
        let market = self.cfg.market.clone();
        let read = tokio::time::timeout(TICK_READ_TIMEOUT, self.dex.get_ticker(&market, None));
        match read.await {
            Ok(Ok(ticker)) => {
                if let Some(tick) = ticker.min_tick.filter(|t| *t > Decimal::ZERO) {
                    if self.paper_tick != Some(tick) {
                        log::info!("[ARCUS_VOL] paper tick for {market} = {tick}");
                    }
                    self.paper_tick = Some(tick);
                    self.next_tick_read_ms = now + TICK_REFRESH_MS;
                }
            }
            Ok(Err(e)) => self.on_error("paper tick", &e),
            Err(_) => log::warn!("[ARCUS_VOL] paper tick read timed out"),
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
        self.book_at(fill, now, now)
    }

    /// `book` with the fill's own time (`fill_ts`); `now` is only the
    /// receive time (Codex P2, pairtrade#361).
    fn book_at(&mut self, fill: FillIn, fill_ts: u64, now: u64) -> std::io::Result<Booking> {
        if self.journal_unsafe {
            return Err(std::io::Error::other(
                "journal unsafe: appends stopped until restart",
            ));
        }
        let path = self.fills_path.clone();
        let outcome = match ledger::book_fill_at(&mut self.ledger, &fill, fill_ts, now, |row| {
            append_synced(&path, row)
        }) {
            Ok(o) => o,
            Err(e) => {
                self.note_append_error(&e);
                return Err(e);
            }
        };
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
            self.pending_markouts
                .push(ledger::markouts_for(&fill, fill_ts));
        }
        Ok(outcome)
    }

    // ---------------------------------------------------------------- paper

    /// Tape health and prints (DRY_RUN). A disconnect pulls every virtual
    /// quote at once; the reconnect records the gap in fills.jsonl.
    fn on_tape(&mut self, event: tape::TapeEvent) {
        if !self.cfg.dry_run {
            return;
        }
        let now = now_ms();
        let action = self.tape.on_event(&event, now);
        self.apply_health(action);
        if let tape::TapeEvent::Print(p) = event {
            self.on_print(p);
        }
    }

    /// Act on a tape health change: pull the virtual quotes, or record the
    /// gap that just ended.
    fn apply_health(&mut self, action: tape::HealthAction) {
        match action {
            tape::HealthAction::None => {}
            tape::HealthAction::PullQuotes => self.virt.clear(),
            tape::HealthAction::GapEnded {
                start_ms,
                end_ms,
                reason,
            } => {
                let row = json!({"kind": "tape_gap", "seq": self.ledger.take_seq(),
                                 "mode": self.cfg.mode(),
                                 "market": self.cfg.market,
                                 "start_ms": start_ms, "end_ms": end_ms, "reason": reason,
                                 "secs": (end_ms.saturating_sub(start_ms)) as f64 / 1_000.0});
                log::warn!(
                    "[ARCUS_VOL] trades tape back after {}s gap ({reason})",
                    end_ms.saturating_sub(start_ms) / 1_000
                );
                if self.journal_unsafe {
                    log::error!("[ARCUS_VOL] tape_gap row not written: journal unsafe");
                } else if let Err(e) = append_synced(&self.fills_path, &row) {
                    self.note_append_error(&e);
                    log::error!("[ARCUS_VOL] tape_gap row not written: {e}");
                }
            }
        }
    }

    fn on_print(&mut self, p: tape::Print) {
        if !self.cfg.dry_run {
            return;
        }
        let now = now_ms();
        // A too-old print is a tape failure (pull, gap, not ready); the next
        // fresh print re-arms (Codex P1, pairtrade#361; see on_print_age).
        let time = sim::print_fill_time(p.ts_us, now, PRINT_FUTURE_TOLERANCE_MS, PRINT_MAX_AGE_MS);
        let fresh = !matches!(time, sim::PrintTime::TooOld { .. });
        let action = self.tape.on_print_age(fresh, now);
        self.apply_health(action);
        if let sim::PrintTime::TooOld { venue_ms } = time {
            log::warn!(
                "[ARCUS_VOL] print {} stamped {venue_ms} ms is too old vs {now}: tape not ready (stale_print), quotes pulled",
                p.trade_id
            );
            return;
        }
        if !sim::prints_apply(
            self.tape.ready,
            self.sim_halted.is_some(),
            self.halt.is_some(),
            self.flatten_pending,
        ) {
            return;
        }
        // Never book a paper fill onto a day whose rollover is not on disk:
        // the print is dropped (it cannot be replayed; conservative).
        if !self.ensure_rolled(now) {
            // No quote may rest while nothing can book (Codex P2, pairtrade#361).
            self.virt.clear();
            return;
        }
        // The fill's time is the print's venue time (Codex P2, pairtrade#361).
        let fill_ts =
            match sim::print_fill_time(p.ts_us, now, PRINT_FUTURE_TOLERANCE_MS, PRINT_MAX_AGE_MS) {
                sim::PrintTime::Venue(ms) => ms,
                sim::PrintTime::ClampedFuture { venue_ms, now_ms } => {
                    log::warn!(
                    "[ARCUS_VOL] print {} stamped {venue_ms} ms, ahead of us ({now_ms}); clamped",
                    p.trade_id
                );
                    now_ms
                }
                sim::PrintTime::TooOld { .. } => return,
            };
        self.newest_print_ts_us = self.newest_print_ts_us.max(p.ts_us);
        for side in [QSide::Bid, QSide::Ask] {
            let Some((vq, orig)) = self.virt.get(&side).cloned() else {
                continue;
            };
            // The quote only changes once its fill is durably booked.
            let result = sim::try_fill(&vq, p.ts_us, p.px, p.qty, p.taker, |filled| {
                self.sim_seq += 1;
                let fill = FillIn {
                    trade_id: sim::sim_id(self.run_id, self.sim_seq, &p.trade_id),
                    buy: side == QSide::Bid,
                    qty: filled,
                    px: vq.px,
                    fee: self.fee(filled * vq.px, true),
                    maker: true,
                    order_id: format!("sim-{}", side.as_str()),
                    fee_estimated: false,
                };
                self.book_at(fill, fill_ts, now).map(|_| ())
            });
            match result {
                Ok(None) => {}
                Ok(Some(next)) if next.remaining.is_zero() => {
                    self.virt.remove(&side);
                }
                Ok(Some(next)) => {
                    self.virt.insert(side, (next, orig));
                }
                Err(e) => {
                    self.halt_sim(format!("paper fill could not be recorded: {e}"));
                    return;
                }
            }
        }
    }

    fn note_append_error(&mut self, e: &std::io::Error) {
        if ledger::is_journal_unsafe(e) && !self.journal_unsafe {
            self.journal_unsafe = true;
            log::error!(
                "[ARCUS_VOL] JOURNAL UNSAFE: {e}; no appends and no new quoting until a restart repairs fills.jsonl"
            );
        }
    }

    /// Roll the UTC day if due and persist it before anything books; false
    /// = book nothing now (see `ledger::ensure_rolled`). Shared by the tick's
    /// fill harvest and the paper `on_print`.
    fn ensure_rolled(&mut self, now: u64) -> bool {
        let ok = self.ensure_rolled_inner(now);
        self.rollover_blocked = !ok;
        ok
    }

    fn ensure_rolled_inner(&mut self, now: u64) -> bool {
        let day_before = self.ledger.day.clone();
        let state_writer = &mut self.state_writer;
        let state_path = &self.state_path;
        let result = ledger::ensure_rolled(
            &mut self.ledger,
            &utc_day(),
            self.last_mark,
            &mut self.rollover_unpersisted,
            |l| state_writer.write(state_path, l).map(|_| ()),
        );
        if self.ledger.day != day_before {
            log::info!("[ARCUS_VOL] new UTC day {}", self.ledger.day);
        }
        match result {
            Ok(ok) => ok,
            Err(e) => {
                if now.saturating_sub(self.last_roll_warn_ms) >= 60_000 {
                    self.last_roll_warn_ms = now;
                    log::warn!("[ARCUS_VOL] {e}; no fill is booked until it is on disk");
                }
                false
            }
        }
    }

    /// Stop the paper sim for the rest of this run (sticky, loud).
    fn halt_sim(&mut self, reason: String) {
        log::error!("[ARCUS_VOL] PAPER SIM HALTED for this run: {reason}");
        self.virt.clear();
        self.sim_halted = Some(reason);
    }

    fn paper_quote(&mut self, side: QSide, action: QuoteAction) {
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
                let levels: &[OrderBookLevel] = match (&self.book, side) {
                    (Some(b), QSide::Bid) => &b.bids,
                    (Some(b), QSide::Ask) => &b.asks,
                    (None, _) => &[],
                };
                let px = sim::paper_px(side, t.px, self.paper_tick);
                let vq = sim::join(side, px, t.qty, levels, placed);
                self.virt.insert(side, (vq, t.qty));
            }
        }
    }

    fn paper_flatten(&mut self, now: u64) {
        if self.sim_halted.is_some() {
            return;
        }
        // Without a book the IOC waits for a price, but the quotes are pulled
        // now (`paper_flatten_steps`); live prices off the connector instead.
        // Only a fresh book prices the IOC (same predicate as the plan).
        let touch = sim::fresh_touch(
            self.book.as_ref().map(|b| (b.bid, b.ask, b.ts_ms)),
            now,
            self.cfg.book_stale_secs,
        );
        for step in sim::paper_flatten_steps(
            touch.is_some(),
            self.ledger.position.qty,
            self.virt.contains_key(&QSide::Bid),
            self.virt.contains_key(&QSide::Ask),
        ) {
            match step {
                Step::Cancel(side) => {
                    self.virt.remove(&side);
                }
                Step::Ioc { side, qty } => {
                    let Some((bid, ask)) = touch else { return };
                    // Same rollover guard as every other booking path
                    // (pre-G2, Codex P2 4146548930): the quotes are already
                    // pulled; the IOC waits until the rollover is on disk.
                    if !self.ensure_rolled(now) {
                        return;
                    }
                    let buy = side == OrderSide::Long;
                    let px = if buy { ask } else { bid };
                    let fee = self.fee(qty * px, false);
                    self.sim_seq += 1;
                    let fill = FillIn {
                        trade_id: sim::sim_id(self.run_id, self.sim_seq, "ioc"),
                        buy,
                        qty,
                        px,
                        fee,
                        maker: false,
                        order_id: "sim-ioc".to_string(),
                        fee_estimated: false,
                    };
                    if let Err(e) = self.book(fill, now) {
                        self.halt_sim(format!("paper flatten could not be recorded: {e}"));
                        return;
                    }
                }
            }
        }
    }

    // ----------------------------------------------------------------- live

    async fn live_fills(&mut self, now: u64) {
        self.harvest_fills(now, false).await;
    }

    /// Harvest and book fills. At shutdown (`shutting_down`) a fill without a
    /// fee is booked at once at the taker fee instead of waiting: the
    /// connector's fill cache dies with the process (Codex P1, pairtrade#361).
    async fn harvest_fills(&mut self, now: u64, shutting_down: bool) {
        // Every live booking path (tick, startup, shutdown) books only after
        // the UTC-day rollover is on disk (pre-G2, Codex 4146113066 /
        // 4146548930); unbooked records stay in the connector (and are
        // spilled at shutdown).
        if !self.ensure_rolled(now) {
            self.fills_synced = false;
            return;
        }
        let market = self.cfg.market.clone();
        let rows = match self.dex.get_filled_orders(&market).await {
            Ok(r) => r.orders,
            Err(e) => {
                self.fills_synced = false;
                self.on_error("get_filled_orders", &e);
                return;
            }
        };
        let snapshot = rows.clone();
        let mut cleared: HashSet<String> = HashSet::new();
        let mut all_booked = true;
        for f in rows {
            // Only a confirmed rejected / zero-size record is cleared; an
            // incomplete one waits, then halts (Codex P1, pairtrade#361).
            let first_seen = *self
                .incomplete_since
                .entry(f.trade_id.clone())
                .or_insert(now);
            let (side, qty, value) = match ledger::classify_fill_record(
                f.is_rejected,
                f.filled_side,
                f.filled_size,
                f.filled_value,
                first_seen,
                now,
                FILL_INCOMPLETE_WAIT_MS,
            ) {
                ledger::FillRecordAction::Book { side, qty, value } => (side, qty, value),
                ledger::FillRecordAction::Clear => {
                    log::warn!("[ARCUS_VOL] fill {} rejected/empty; cleared", f.trade_id);
                    self.incomplete_since.remove(&f.trade_id);
                    self.clear_fill(&market, &f.trade_id).await;
                    cleared.insert(f.trade_id.clone());
                    continue;
                }
                ledger::FillRecordAction::Pending => {
                    all_booked = false;
                    continue;
                }
                ledger::FillRecordAction::Halt => {
                    all_booked = false;
                    let reason = format!(
                        "fill_incomplete: trade {} still missing side/size/value after {}s",
                        f.trade_id,
                        FILL_INCOMPLETE_WAIT_MS / 1_000
                    );
                    if self.ledger.halt_sticky(reason.clone(), self.last_mark) {
                        log::error!("[ARCUS_VOL] STICKY HALT {reason}");
                    }
                    continue;
                }
            };
            self.incomplete_since.remove(&f.trade_id);
            // A missing fee is never booked as zero (Codex P1, pairtrade#361).
            let first_seen = *self.fee_wait_since.entry(f.trade_id.clone()).or_insert(now);
            let (fee, fee_estimated) = match ledger::fee_decision(
                f.filled_fee,
                first_seen,
                now,
                ledger::fee_wait_ms(shutting_down, self.cfg.fee_wait_secs * 1_000),
                value,
                self.cfg.taker_fee_bps,
            ) {
                ledger::FeeDecision::Wait => {
                    all_booked = false;
                    continue;
                }
                ledger::FeeDecision::Exact(fee) => (fee, false),
                ledger::FeeDecision::Estimated(fee) => {
                    log::error!(
                        "[ARCUS_VOL] fill {} still has no fee after {}s; booking the taker fee {fee} (fee_estimated)",
                        f.trade_id,
                        self.cfg.fee_wait_secs
                    );
                    (fee, true)
                }
            };
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
                fee_estimated,
            };
            let booking = self.book(fill, now);
            match &booking {
                Ok(Booking::Booked(_)) => {
                    self.fee_wait_since.remove(&f.trade_id);
                    for r in self.resting.values_mut() {
                        if r.order_id == f.order_id {
                            r.filled += qty;
                        }
                    }
                    self.resting.retain(|_, r| r.filled < r.qty);
                }
                Ok(Booking::AlreadyBooked) => {
                    self.fee_wait_since.remove(&f.trade_id);
                }
                Err(e) => {
                    // Kept in the connector; retried next tick.
                    log::error!(
                        "[ARCUS_VOL] fill {} not written, will retry: {e}",
                        f.trade_id
                    );
                    all_booked = false;
                    continue;
                }
            }
            // The connector may forget the fill only once the booking is in
            // state.json (Codex P1, pairtrade#361).
            let persisted = match self.state_writer.write(&self.state_path, &self.ledger) {
                Ok(_) => true,
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
                cleared.insert(f.trade_id.clone());
            } else {
                all_booked = false;
            }
        }
        self.fills_synced = all_booked;
        self.unbooked_seen = snapshot
            .into_iter()
            .filter(|r| !cleared.contains(&r.trade_id))
            .collect();
    }

    async fn clear_fill(&self, market: &str, trade_id: &str) {
        if let Err(e) = self.dex.clear_filled_order(market, trade_id).await {
            log::debug!("[ARCUS_VOL] clear_filled_order {trade_id}: {e}");
        }
    }

    async fn venue_qty(&mut self) -> Option<(Decimal, Option<Decimal>)> {
        match self.dex.get_positions().await {
            // Only this runtime's market (logic::market_position).
            Ok(positions) => Some(market_position(&positions, &self.cfg.market)),
            Err(e) => {
                self.on_error("get_positions", &e);
                None
            }
        }
    }

    /// Compare the ledger with the venue position; never adopts (see
    /// `logic::position_check`). A persistent mismatch sticky-halts.
    async fn check_position(&mut self, now: u64) -> PosCheck {
        let venue = self.venue_qty().await.map(|(q, _)| q);
        let (check, since) = position_check(
            self.ledger.position.qty,
            venue,
            self.fills_synced,
            self.position_mismatch_since_ms,
            now,
            self.cfg.position_poll_secs * 1_000,
        );
        self.position_mismatch_since_ms = since;
        if let (PosCheck::Mismatch, Some(v)) = (check, venue) {
            if self.ledger.halt_position_mismatch(v, self.last_mark) {
                log::error!(
                    "[ARCUS_VOL] POSITION MISMATCH: ledger {} vs venue {v}; sticky halt, quotes pulled, no auto-flatten",
                    self.ledger.position.qty
                );
            }
        }
        check
    }

    /// Resolve `pending_fills.jsonl`: book complete records (dedupe makes a
    /// repeat harmless), sticky-halt `fill_incomplete` on incomplete ones,
    /// then persist and set the file aside. False = retry next tick; no
    /// quoting meanwhile (startup isn't reconciled).
    fn resolve_pending_fills(&mut self, now: u64) -> bool {
        let pending = match ledger::read_pending(&self.pending_path) {
            Ok(p) => p,
            Err(e) => {
                let reason = format!("pending_fills unreadable: {e}");
                if self.ledger.halt_sticky(reason.clone(), self.last_mark) {
                    log::error!("[ARCUS_VOL] STICKY HALT {reason}");
                }
                return false;
            }
        };
        if pending.is_empty() {
            self.pending_unresolved.clear();
            return true;
        }
        if !self.ensure_rolled(now) {
            return false;
        }
        let (mut done, mut unresolved) = (Vec::new(), Vec::new());
        for p in &pending {
            match ledger::resolve_pending(p, self.cfg.taker_fee_bps) {
                ledger::PendingResolution::Book(fill) => {
                    // At its own time and day, never the restart time
                    // (pre-G2, Codex P1 4152482567).
                    if let Err(e) = self.book_spilled(fill, p.fill_time_ms(), now) {
                        log::error!(
                            "[ARCUS_VOL] pending fill {} not written, will retry: {e}",
                            p.trade_id
                        );
                        return false;
                    }
                    done.push(p.clone());
                }
                ledger::PendingResolution::Clear => done.push(p.clone()),
                ledger::PendingResolution::Halt(_) => unresolved.push(p.clone()),
            }
        }
        if let Err(e) = self.state_writer.write(&self.state_path, &self.ledger) {
            log::error!("[ARCUS_VOL] state write after pending fills failed, will retry: {e:#}");
            return false;
        }
        // Only the resolved subset leaves the pending file; every unresolved
        // record stays pending, durably (pre-G2, Codex P1 4152482577).
        let archive = self
            .pending_path
            .with_file_name("pending_fills.resolved.jsonl");
        if let Err(e) = ledger::settle_pending(&self.pending_path, &archive, &done, &unresolved) {
            log::error!("[ARCUS_VOL] could not settle pending_fills ({e}); will retry");
            return false;
        }
        if !done.is_empty() {
            log::warn!(
                "[ARCUS_VOL] resolved {} pending fill record(s) from the last shutdown (archived in {})",
                done.len(),
                archive.display()
            );
        }
        self.pending_unresolved = unresolved.iter().map(|p| p.trade_id.clone()).collect();
        if unresolved.is_empty() {
            return true;
        }
        // Every unresolved id is named, in the sticky halt (when not already
        // sticky), in the log and in status.json; quoting stays held (startup
        // not reconciled) until the file is resolved.
        let reason = format!(
            "fill_incomplete: {} record(s) still incomplete in pending_fills.jsonl: {}",
            unresolved.len(),
            self.pending_unresolved.join(",")
        );
        if self.ledger.halt_sticky(reason.clone(), self.last_mark) {
            log::error!("[ARCUS_VOL] STICKY HALT {reason}");
        } else if now.saturating_sub(self.last_pending_warn_ms) >= 60_000 {
            self.last_pending_warn_ms = now;
            log::error!("[ARCUS_VOL] {reason} (halt already set); resolve by hand");
        }
        false
    }

    /// Book a spilled fill at its own time (`fill_ts`), into today's counters
    /// only if it happened today, else as a cumulative-only late adjustment;
    /// horizons already too late are recorded missing (`restart`).
    fn book_spilled(&mut self, fill: FillIn, fill_ts: u64, now: u64) -> std::io::Result<Booking> {
        if self.journal_unsafe {
            return Err(std::io::Error::other(
                "journal unsafe: appends stopped until restart",
            ));
        }
        let scope = ledger::day_scope(&self.ledger.day, fill_ts);
        let path = self.fills_path.clone();
        let outcome =
            match ledger::book_fill_scoped(&mut self.ledger, &fill, fill_ts, now, scope, |row| {
                append_synced(&path, row)
            }) {
                Ok(o) => o,
                Err(e) => {
                    self.note_append_error(&e);
                    return Err(e);
                }
            };
        if outcome != Booking::AlreadyBooked {
            log::warn!(
                "[ARCUS_VOL] FILL (spilled, {scope:?}, ts {fill_ts}) {} {} {} @ {} fee {} inv {}",
                if fill.maker { "maker" } else { "taker" },
                if fill.buy { "buy" } else { "sell" },
                fill.qty,
                fill.px,
                fill.fee.round_dp(4),
                self.ledger.position.qty
            );
            let (owed, missing) = ledger::late_markouts(&fill, fill_ts, now, MARKOUT_MAX_LATE_MS);
            if let Some(m) = owed {
                self.pending_markouts.push(m);
            }
            for m in missing {
                if let ledger::Markout::Missing {
                    fill_id,
                    horizon_s,
                    reason,
                } = m
                {
                    let row = json!({"kind": "markout_missing", "seq": self.ledger.take_seq(),
                                     "ts_ms": now, "fill_id": fill_id,
                                     "market": self.cfg.market, "horizon_s": horizon_s,
                                     "reason": reason});
                    if let Err(e) = append_synced(&self.fills_path, &row) {
                        self.note_append_error(&e);
                        log::warn!("[ARCUS_VOL] markout_missing append failed: {e}");
                    }
                }
            }
        }
        Ok(outcome)
    }

    /// At shutdown, write every fill record the connector still holds (i.e.
    /// not yet booked and cleared) to `pending_fills.jsonl`, durably, so a
    /// restart can't lose it (pre-G2, Codex P1 4146269818).
    async fn spill_unbooked(&mut self, now: u64, limit: Duration) {
        let market = self.cfg.market.clone();
        // Bounded (pre-G2, Codex P2 4152541379): a stalled venue can't hang
        // the shutdown; then the harvest's unbooked records are spilled.
        let read = read_within(self.dex.get_filled_orders(&market), limit)
            .await
            .map(|r| r.map(|r| r.orders));
        match &read {
            Some(Ok(_)) => {}
            Some(Err(e)) => log::error!(
                "[ARCUS_VOL] SHUTDOWN: unbooked-fill read failed ({e}); spilling the {} record(s) the harvest saw",
                self.unbooked_seen.len()
            ),
            None => log::error!(
                "[ARCUS_VOL] SHUTDOWN: unbooked-fill read timed out after {limit:?}; spilling the {} record(s) the harvest saw",
                self.unbooked_seen.len()
            ),
        }
        let rows = spill_rows(read, std::mem::take(&mut self.unbooked_seen));
        if rows.is_empty() {
            return;
        }
        let pending: Vec<ledger::PendingFill> = rows
            .iter()
            .map(|f| {
                let hint = if self.ioc_ids.contains(&f.order_id) {
                    Some(false)
                } else if self.quote_ids.contains(&f.order_id) {
                    Some(true)
                } else {
                    None
                };
                ledger::PendingFill::from_record(f, hint, now)
            })
            .collect();
        match ledger::spill_pending(&self.pending_path, &pending) {
            Ok(()) => log::error!(
                "[ARCUS_VOL] SHUTDOWN: {} unbooked fill record(s) written to {}; resolved at the next start",
                pending.len(),
                self.pending_path.display()
            ),
            Err(e) => log::error!(
                "[ARCUS_VOL] SHUTDOWN: writing {} unbooked fill record(s) FAILED ({e}); reconcile by hand: {:?}",
                pending.len(),
                pending.iter().map(|p| &p.trade_id).collect::<Vec<_>>()
            ),
        }
    }

    /// Startup position read (live): until it succeeds no new quote goes
    /// out (Codex P1, pairtrade#361); a non-zero position is flattened first.
    async fn startup_reconcile(&mut self) {
        // Fills left unbooked by the last shutdown come first (pre-G2, Codex
        // P1 4146269818); they need the rollover on disk, like any booking.
        if !self.resolve_pending_fills(now_ms()) {
            return;
        }
        // Harvest first so the comparison sees every reported fill (the
        // harvest itself waits for a durable rollover: pre-G2, Codex
        // 4146113066).
        self.live_fills(now_ms()).await;
        match self.check_position(now_ms()).await {
            PosCheck::InSync => {}
            other => {
                log::warn!(
                    "[ARCUS_VOL] startup position check {other:?}; no quoting until in sync"
                );
                return;
            }
        }
        self.startup_reconciled = true;
        self.startup_flatten = !self.ledger.position.qty.is_zero();
        if self.startup_flatten {
            log::warn!(
                "[ARCUS_VOL] startup inventory {} → flatten before quoting",
                self.ledger.position.qty
            );
        } else {
            log::info!("[ARCUS_VOL] startup position reconciled: flat");
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
        // Cleared only when the position is in sync, or the mismatch has
        // escalated to the sticky halt (Codex P1, pairtrade#361); Pending
        // (a fill in flight) and Unread keep it and retry next tick.
        let check = self.check_position(now).await;
        if !reconcile_cleared(check) {
            log::warn!("[ARCUS_VOL] reconcile: position check {check:?}; still pending");
            return;
        }
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

    /// DMS armed right now (fresh clock), for the check just before a send.
    fn dms_armed_now(&self) -> bool {
        dms_armed(
            self.dms_last_ok_ms,
            self.dms_last_failed,
            now_ms(),
            self.cfg.dms_secs,
        )
    }

    async fn live_quotes(&mut self, plan: Vec<(QSide, QuoteAction)>) {
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
        // Cancels above always go; each new place/modify batch only while the
        // DMS is armed at its own send time (Codex P1, pairtrade#361).
        let mut batches = Vec::new();
        if !modifies.is_empty() {
            batches.push(QuoteBatch::Modify(modifies));
        }
        if !places.is_empty() {
            batches.push(QuoteBatch::Place(places));
        }
        send_gated(self, batches).await;
    }

    async fn send_modifies(&mut self, modifies: Vec<(QSide, QuoteTarget, String)>) {
        let market = self.cfg.market.clone();
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

    async fn send_places(&mut self, places: Vec<QuoteTarget>) {
        let market = self.cfg.market.clone();
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
        let depth = book_depth(self.cfg.dry_run, self.cfg.presence().on());
        match self
            .dex
            .get_order_book(&self.cfg.market.clone(), depth)
            .await
        {
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
        // Only a timestamp-fresh book may set the mark used for the rollover
        // and the stops (Codex P2, pairtrade#361); never the entry price.
        if let Some(m) = fresh_mark(
            mid,
            self.book.as_ref().and_then(|b| b.ts_ms),
            now,
            self.cfg.book_stale_secs,
        ) {
            self.last_mark = Some(m);
        }
        // A rollover is on disk before any new-day fill is booked (Codex P2,
        // pairtrade#361); `ensure_rolled` is the one guard for every path.
        let harvest_ok = self.ensure_rolled(now);

        if !self.cfg.dry_run {
            if harvest_ok {
                self.live_fills(now).await;
            } else {
                self.fills_synced = false;
            }
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
            if !self.startup_reconciled {
                self.startup_reconcile().await;
            }
            if self.need_reconcile {
                self.live_full_reconcile(now).await;
            }
            if now.saturating_sub(self.last_reconcile_ms) >= self.cfg.reconcile_secs * 1_000 {
                self.live_reconcile_orders().await;
                self.last_reconcile_ms = now;
            }
            if self.position_pending
                || now.saturating_sub(self.last_position_ms) >= self.cfg.position_poll_secs * 1_000
            {
                let check = self.check_position(now).await;
                self.position_pending = position_gate_after(check);
                if self.position_pending {
                    log::warn!("[ARCUS_VOL] position check {check:?}: no new quotes until in sync");
                }
                self.last_position_ms = now;
            }
        }
        self.refresh_paper_tick(now).await;
        // Every awaited call of the tick is done: decide on a fresh clock
        // (Codex P1, pairtrade#361).
        let now = now_ms();
        // Markouts only against a timestamp-fresh mid; late ones go missing
        // rather than price at a stale mid (Codex P2, pairtrade#361).
        let fresh_mid = fresh_mark(
            mid,
            self.book.as_ref().and_then(|b| b.ts_ms),
            now,
            self.cfg.book_stale_secs,
        );
        for m in due_markouts(
            &mut self.pending_markouts,
            now,
            fresh_mid,
            MARKOUT_MAX_LATE_MS,
        ) {
            let seq = self.ledger.take_seq();
            let row = match m {
                ledger::Markout::Priced {
                    fill_id,
                    horizon_s,
                    bps,
                } => json!({"kind": "markout", "seq": seq, "ts_ms": now, "fill_id": fill_id,
                            "market": self.cfg.market, "horizon_s": horizon_s,
                            "bps": bps.round_dp(3).to_string(),
                            "mid": fresh_mid.map(|m| m.to_string())}),
                ledger::Markout::Missing {
                    fill_id,
                    horizon_s,
                    reason,
                } => json!({"kind": "markout_missing", "seq": seq, "ts_ms": now,
                            "fill_id": fill_id, "market": self.cfg.market,
                            "horizon_s": horizon_s, "reason": reason}),
            };
            if self.journal_unsafe {
                continue;
            }
            // Rollback-safe like every journal append (Codex P1, pairtrade#361).
            if let Err(e) = append_synced(&self.fills_path, &row) {
                self.note_append_error(&e);
                log::warn!("[ARCUS_VOL] markout append failed: {e}");
            }
        }

        // Stops.
        let kill = self.kill_path.exists();
        let halt_file = self.halt_path.exists();
        let risk = risk_check(
            &mut self.ledger,
            self.last_mark,
            kill,
            halt_file,
            self.cfg.daily_stop_usd,
            self.cfg.cum_stop_usd,
        );
        for e in &risk.events {
            log::warn!("[ARCUS_VOL] {e}");
        }
        if risk.unrealized_deferred && now.saturating_sub(self.last_mark_warn_ms) >= 60_000 {
            self.last_mark_warn_ms = now;
            log::warn!(
                "[ARCUS_VOL] no fresh mark yet: stops use realized − fees only (unrealized deferred)"
            );
        }
        if risk.write_halt_file {
            let body = self.ledger.sticky_reason.clone().unwrap_or_default();
            if let Err(e) = std::fs::write(&self.halt_path, format!("{body}\n")) {
                log::error!("[ARCUS_VOL] cannot write HALT: {e}");
            }
        }
        self.halt = risk.halt.or_else(|| {
            self.sim_halted
                .as_ref()
                .map(|r| Halt::Sticky(format!("paper sim: {r}")))
        });

        // Startup / halt / kill / max-hold flatten need no book; only the
        // cap check uses the mid (Codex P1, pairtrade#361).
        let halt_label = self.halt.as_ref().map(Halt::label);
        let startup = self.startup_flatten && !self.ledger.position.qty.is_zero();
        if !startup {
            self.startup_flatten = false;
        }
        // Dust (below the venue minimum) is never flattened: every IOC for
        // it is rejected. Priced at the mid, else the entry.
        let mark = mid.or_else(|| {
            (self.ledger.position.avg_px > Decimal::ZERO).then_some(self.ledger.position.avg_px)
        });
        let dust = is_dust(
            self.ledger.position.qty,
            mark,
            self.cfg.min_order_qty,
            self.cfg.min_order_usd,
        );
        if dust && now >= self.dust_logged_until_ms {
            log::warn!(
                "[ARCUS_VOL] inventory {} is below the venue minimum (qty {} / ${}): dust, not flattened",
                self.ledger.position.qty,
                self.cfg.min_order_qty,
                self.cfg.min_order_usd
            );
            self.dust_logged_until_ms = now + 600_000;
        }
        let (dust_qty, clock) = dust_hold_clock(
            self.dust_qty,
            dust,
            self.ledger.position.qty,
            self.ledger.position.opened_at_ms,
            now,
        );
        self.dust_qty = dust_qty;
        if clock != self.ledger.position.opened_at_ms {
            self.ledger.position.opened_at_ms = clock;
        }
        // Startup dust can never be flattened: drop the latch so a later,
        // grown position is not flattened as Startup (Codex P2).
        let startup = startup_latch_after(startup, dust);
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
            flatten_halt_label(halt_label.as_deref()),
            startup,
            dust,
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
        let state = PlanState {
            dry_run: self.cfg.dry_run,
            has_book: mid.is_some(),
            halted: self.halt.is_some(),
            book_ts_ms: self.book.as_ref().and_then(|b| b.ts_ms),
            book_stale_secs: self.cfg.book_stale_secs,
            cooldown_until_ms: self.cooldown_until_ms,
            backoff_until_ms: self.backoff_until_ms,
            dms_last_ok_ms: self.dms_last_ok_ms,
            dms_last_failed: self.dms_last_failed,
            dms_secs: self.cfg.dms_secs,
            startup_reconciled: self.startup_reconciled,
            reconcile_pending: self.need_reconcile,
            position_pending: self.position_pending,
            fills_synced: self.fills_synced,
            tape_ready: self.tape.ready,
            journal_unsafe: self.journal_unsafe,
            rollover_pending: self.rollover_blocked,
            flatten,
        };
        let plan = tick_plan(&plan_inputs(&state, now));
        self.flatten_pending = matches!(plan, TickPlan::Flatten(_));
        self.last_plan = match &plan {
            TickPlan::Flatten(r) => format!("flatten:{r:?}"),
            TickPlan::PullQuotes(why) => format!("pull:{why}"),
            TickPlan::Wait => "wait".to_string(),
            TickPlan::Quote => "quote".to_string(),
        };
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
            TickPlan::Wait | TickPlan::Quote => {}
        }
        // `Quote` is the full pass; a presence-mode `Wait` (429 backoff)
        // still runs the plan, for its cancels only.
        let Some(pass) = quote_pass(&plan, self.cfg.presence().on()) else {
            self.finish_tick(now, mid);
            return;
        };

        let params = self.quote_params();
        let inv = self.ledger.position.qty;
        // Our own resting quote is excluded from its side's peg reference.
        let own = |side: QSide| own_displayed(self.cfg.dry_run, self.resting.get(&side));
        let book = self
            .book
            .as_ref()
            .and_then(|b| touches(&b.bids, &b.asks, own(QSide::Bid), own(QSide::Ask)));
        let Some(book) = book else {
            // Unreachable: quotes are only planned with a two-sided book.
            self.finish_tick(now, mid);
            return;
        };
        let (bid, ask) = plan_quotes(&book, inv, &params);
        let unpriced = |p: &SidePlan| p.qty.is_some() && !matches!(p.price, PricePlan::At { .. });
        if (unpriced(&bid) || unpriced(&ask)) && now.saturating_sub(self.last_peg_warn_ms) > 30_000
        {
            self.last_peg_warn_ms = now;
            log::warn!(
                "[ARCUS_VOL] no price for a side this tick (book {}/{}, peg {:?}/{:?}: crossed, or only our own quote is displayed): nothing new is placed; a resting quote stays unless a crossed book has come too close to it",
                book.bid,
                book.ask,
                book.peg_bid,
                book.peg_ask
            );
        }
        let plan: Vec<(QSide, QuoteAction)> = [bid, ask]
            .iter()
            .map(|p| {
                let action = quote_action(self.current_quote(p.side).as_ref(), p);
                (p.side, pass.apply(action))
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
        if let Err(e) = self.state_writer.write(&self.state_path, &self.ledger) {
            log::error!("[ARCUS_VOL] state write failed: {e:#}");
        }
        let mark = mid.unwrap_or(self.ledger.position.avg_px);
        // Presence-quoting monitor: each resting quote's distance from the
        // reference its band is judged on (`dist_bps`) and from the raw
        // touch (`dist_touch_bps`); null without a book / a reference.
        let own = |side: QSide| own_displayed(self.cfg.dry_run, self.resting.get(&side));
        let book = self
            .book
            .as_ref()
            .and_then(|b| touches(&b.bids, &b.asks, own(QSide::Bid), own(QSide::Ask)));
        let quote = |side: QSide| {
            self.current_quote(side).map(|r| {
                let dists = book.map(|t| quote_dists(side, r.px, &t));
                let show = |d: Decimal| d.round_dp(3).to_string();
                json!({"px": r.px.to_string(), "qty": r.qty.to_string(),
                       "filled": r.filled.to_string(), "order_id": r.order_id,
                       "dist_bps": dists.and_then(|(peg, _)| peg).map(show),
                       "dist_touch_bps": dists.map(|(_, raw)| show(raw))})
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
            "quote_offset_bps": self.cfg.quote_offset_bps.to_string(),
            "repeg_band_bps": self.cfg.repeg_band_bps.to_string(),
            "paper_tick": self.paper_tick.map(|t| t.to_string()),
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
            "tape": {"ready": self.tape.ready, "gap_since_ms": self.tape.gap_since_ms},
            "plan": self.last_plan,
            "pending_unresolved": self.pending_unresolved,
        });
        // status.json is informational (dashboards, humans): a plain atomic
        // replace without fsync is enough; state.json above is the durable one.
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
                    // At least 3 polls, then keep polling (bounded) while any
                    // record is still incomplete (pre-G2, Codex P1 4146269818);
                    // whatever is left is spilled to pending_fills.jsonl.
                    let harvest_started = std::time::Instant::now();
                    let harvest = async {
                        let mut polls = 0;
                        loop {
                            self.harvest_fills(now_ms(), true).await;
                            polls += 1;
                            if polls >= 3 && self.fills_synced {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(700)).await;
                        }
                    };
                    if tokio::time::timeout(SHUTDOWN_HARVEST_BUDGET, harvest)
                        .await
                        .is_err()
                    {
                        log::warn!("[ARCUS_VOL] shutdown fill harvest budget used up");
                    }
                    // The spill read gets what's left of the budget (at
                    // least a short floor), then persist / DMS run regardless.
                    let left = SHUTDOWN_HARVEST_BUDGET
                        .saturating_sub(harvest_started.elapsed())
                        .max(SPILL_READ_FLOOR);
                    self.spill_unbooked(now_ms(), left).await;
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

/// One batch call that creates or modifies resting exposure.
enum QuoteBatch {
    Modify(Vec<(QSide, QuoteTarget, String)>),
    Place(Vec<QuoteTarget>),
}

impl BatchSink<QuoteBatch> for Runtime {
    fn armed(&self) -> bool {
        self.dms_armed_now()
    }

    async fn send(&mut self, batch: QuoteBatch) {
        match batch {
            QuoteBatch::Modify(m) => self.send_modifies(m).await,
            QuoteBatch::Place(p) => self.send_places(p).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = Config::from_env()?;
    let allow_account0 = std::env::var("ARCUS_VOL_ALLOW_ACCOUNT0").unwrap_or_default();
    config::live_gate(
        cfg.dry_run,
        &cfg.live_confirm,
        cfg.account_index,
        &allow_account0,
    )
    .map_err(|e| anyhow!(e))?;
    if !cfg.dry_run && cfg.account_index == Some(0) {
        log::warn!(
            "[ARCUS_VOL] LIVE ON SUBACCOUNT 0 (shared with manual trading): stray {} orders get \
             cancelled, a {} position mismatch halts, and the dead man's switch cancels ALL \
             orders on the account (every market) if this process stops refreshing it",
            cfg.market,
            cfg.market
        );
    }
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
    if cfg.quote_offset_bps > Decimal::ZERO {
        log::info!(
            "[ARCUS_VOL] PRESENCE quoting: quotes rest {} bp behind the touch, kept while within ±{} bp of that; fills only on sweeps; the reducing side exits at the touch",
            cfg.quote_offset_bps,
            cfg.repeg_band_bps
        );
    } else {
        log::info!("[ARCUS_VOL] quoting at the touch (ARCUS_VOL_QUOTE_OFFSET_BPS=0)");
    }
    std::fs::create_dir_all(&cfg.state_dir)
        .with_context(|| format!("create {}", cfg.state_dir.display()))?;
    // Before state.json is read or the venue touched (Codex P1, pairtrade#361).
    let _state_lock = ledger::acquire_state_lock(&cfg.state_dir).map_err(|e| anyhow!(e))?;
    // Account-wide, independent of STATE_DIR (Codex P1, pairtrade#361).
    let _account_lock = match ledger::account_lock_path(
        &cfg.lock_dir,
        cfg.dry_run,
        cfg.arcus_address.as_deref(),
        cfg.account_index,
    )
    .map_err(|e| anyhow!(e))?
    {
        Some(path) => Some(ledger::acquire_lock_at(&path).map_err(|e| anyhow!(e))?),
        None => None,
    };
    let state_path = cfg.state_dir.join("state.json");
    let fills_path = cfg.state_dir.join("fills.jsonl");
    // Mode + market checked against state AND journal, torn tail repaired,
    // missed rows replayed, and a fresh state written durably, all before
    // any venue contact (Codex P1, pairtrade#361).
    let opened = ledger::open_state(&cfg.state_dir, cfg.mode(), &cfg.market, &utc_day())
        .map_err(|e| anyhow!(e))?;
    match &opened.repair {
        ledger::JournalRepair::Clean => {}
        ledger::JournalRepair::NewlineAdded => {
            log::warn!("[ARCUS_VOL] fills.jsonl: appended the missing final newline")
        }
        ledger::JournalRepair::Truncated {
            dropped,
            hex_preview,
        } => log::warn!(
            "[ARCUS_VOL] fills.jsonl: dropped a torn {dropped}-byte tail (hex {hex_preview}…)"
        ),
    }
    if opened.replayed > 0 {
        log::warn!(
            "[ARCUS_VOL] replayed {} fill(s) from fills.jsonl missing in state.json",
            opened.replayed
        );
    }
    let ledger = opened.ledger;
    let journal_rows = opened.rows;

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
        state_writer: ledger::StateWriter::default(),
        status_path: cfg.state_dir.join("status.json"),
        fills_path,
        pending_path: cfg.state_dir.join("pending_fills.jsonl"),
        pending_unresolved: Vec::new(),
        unbooked_seen: Vec::new(),
        last_pending_warn_ms: 0,
        kill_path: cfg.state_dir.join("KILL_SWITCH"),
        halt_path: cfg.state_dir.join("HALT"),
        resting: HashMap::new(),
        virt: HashMap::new(),
        book: None,
        mid_hist: VecDeque::new(),
        cooldown_until_ms: 0,
        backoff_until_ms: 0,
        need_reconcile: false,
        position_pending: false,
        startup_flatten: false,
        dust_logged_until_ms: 0,
        dust_qty: None,
        flatten_inflight_until_ms: 0,
        pending_markouts: Vec::new(),
        quote_ids: HashSet::new(),
        ioc_ids: HashSet::new(),
        newest_print_ts_us: 0,
        tape: tape::TapeHealth::default(),
        last_mark: None,
        flatten_pending: false,
        rollover_unpersisted: false,
        rollover_blocked: false,
        last_plan: String::new(),
        last_mark_warn_ms: 0,
        last_roll_warn_ms: 0,
        incomplete_since: HashMap::new(),
        journal_unsafe: false,
        fee_wait_since: HashMap::new(),
        position_mismatch_since_ms: None,
        fills_synced: false,
        dms_last_ok_ms: None,
        dms_last_failed: false,
        last_reconcile_ms: 0,
        last_position_ms: 0,
        last_summary_ms: 0,
        last_book_warn_ms: 0,
        paper_tick: None,
        next_tick_read_ms: 0,
        last_peg_warn_ms: 0,
        sim_seq: 0,
        run_id: now_ms(),
        sim_halted: None,
        startup_reconciled: false,
        halt: None,
        cfg,
    };

    // Markouts owed before a restart come back from the journal; those too
    // late to price are recorded as missing (Codex P2, pairtrade#361).
    let (restored, missing) =
        ledger::restore_markouts(&journal_rows, now_ms(), MARKOUT_MAX_LATE_MS)
            .map_err(|e| anyhow!("restore markouts: {e}"))?;
    if !restored.is_empty() || !missing.is_empty() {
        log::info!(
            "[ARCUS_VOL] markouts restored: {} fill(s) pending, {} horizon(s) missing (restart)",
            restored.len(),
            missing.len()
        );
    }
    rt.pending_markouts = restored;
    for m in missing {
        if let ledger::Markout::Missing {
            fill_id,
            horizon_s,
            reason,
        } = m
        {
            let row = json!({"kind": "markout_missing", "seq": rt.ledger.take_seq(),
                             "ts_ms": now_ms(), "fill_id": fill_id,
                             "market": rt.cfg.market, "horizon_s": horizon_s,
                             "reason": reason});
            append_synced(&rt.fills_path, &row).map_err(|e| {
                rt.note_append_error(&e);
                anyhow!("record restart markout_missing: {e}")
            })?;
        }
    }

    let (tx, mut rx) = mpsc::channel::<tape::TapeEvent>(4_096);
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
        rt.startup_reconcile().await;
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
            Some(ev) = rx.recv() => rt.on_tape(ev),
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }
    rt.shutdown().await;
    Ok(())
}
