//! Tick orchestration: reconcile → equity → risk → decision → flatten →
//! daily mark → status. Driven by the wall clock in `book_runtime` and by
//! a synthetic clock in [`super::replay`]. See `docs/book-runtime.md`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::{TimeZone, Utc};
use serde_json::json;

use super::config::BookConfig;
use super::executor::{Executor, FillReport, PreSendAbort};
use super::ledger::Ledger;
use super::rebalance::{self, IntentKind, LotMeta, OrderIntent, Plan, Side};
use super::risk::{utc_date, RiskEvent, RiskRails};
use super::schedule::{Decision, Scheduler};
use super::signal::{self, DecisionContext, SignalReject, ValidSignal};
use super::state::{BookState, DecisionOutcome, DecisionRecord};
use super::status::{
    self, BookBlock, DashboardStatus, DashboardTradeStats, StatusDoc, StatusWriter,
};

/// Where the runtime reads the producer's file from.
pub trait SignalSource: Send + Sync {
    /// Raw body for `decision_key` as of `now` (unix secs); `Ok(None)` when
    /// nothing is there yet -- or, for a source that can tell, when the
    /// file's own `generated_at` has not arrived as of `now`.
    fn read(&self, decision_key: &str, now: i64) -> std::io::Result<Option<String>>;
}

pub struct FileSignalSource {
    path: PathBuf,
}

impl FileSignalSource {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl SignalSource for FileSignalSource {
    fn read(&self, _decision_key: &str, _now: i64) -> std::io::Result<Option<String>> {
        // Live: a file that exists on disk has, by construction, already
        // arrived -- no `now`-gating needed.
        match std::fs::read_to_string(&self.path) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Per-decision signal directory (replay / tests): `<dir>/<key>.json`.
pub struct DirSignalSource {
    dir: PathBuf,
}

impl DirSignalSource {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }
}

impl SignalSource for DirSignalSource {
    fn read(&self, decision_key: &str, now: i64) -> std::io::Result<Option<String>> {
        let text = match std::fs::read_to_string(self.dir.join(format!("{decision_key}.json"))) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        // Unlike a live fetch, every fixture in this directory exists from
        // the start of the run, so its own `generated_at` -- not the
        // reader's clock -- is what says whether it has arrived yet. A
        // fixture that will not exist until later in the simulated
        // timeline must not be visible to an earlier tick just because the
        // file is already on disk; a parse failure here is left to
        // `signal::validate` to reject uniformly.
        // Rounded up, not floored (`signal::ceil_secs`): a fixture
        // generated at 23:59:59.250 must not be visible to the 23:59:59
        // final tick of the previous bar date and filled at that date's
        // close, since it could only ever have arrived on the next date.
        if let Ok(file) = serde_json::from_str::<super::signal::SignalFile>(&text) {
            if signal::ceil_secs(file.generated_at) > now {
                return Ok(None);
            }
        }
        Ok(Some(text))
    }
}

pub struct BookEngine {
    pub cfg: BookConfig,
    scheduler: Scheduler,
    exec: Arc<dyn Executor>,
    risk: RiskRails,
    ledger: Ledger,
    pnl: Ledger,
    status: StatusWriter,
    signals: Box<dyn SignalSource>,
    pub state: BookState,
    lots: HashMap<String, LotMeta>,
    last_signal_generated_at: Option<i64>,
    signal_status: String,
    last_status_write: i64,
    pub status_interval_secs: i64,
    /// Write the daily mark on the first tick of a new UTC date (live).
    /// Replay turns this off and calls [`BookEngine::daily_mark_now`] at
    /// the last tick of each bar date instead.
    pub mark_on_date_change: bool,
    last_flatten_attempt: i64,
    /// Whether the last venue reconcile succeeded (live) — trading is
    /// suppressed while it is false.
    positions_ready: bool,
    /// Live only: the last venue equity read succeeded. Opening intents are
    /// blocked while false (risk limits cannot be evaluated against a
    /// stale number); reductions and flattens still run.
    equity_ready: bool,
    config_fp: String,
}

/// Fold `from` (a later accrual attempt's per-symbol breakdown) onto
/// `into` (an earlier, unwritten one): `hours`/`est_usd` are summed so a
/// retried mark still reports the whole day, not just the sliver accrued
/// since the last failed attempt; every other field (`rate_hourly`,
/// `price_available`, `settled_pending_only`) takes the newer attempt's
/// value. A symbol absent from `into` is inserted as-is.
fn merge_funding_detail(
    into: &mut serde_json::Map<String, serde_json::Value>,
    from: serde_json::Map<String, serde_json::Value>,
) {
    for (sym, new) in from {
        let field = |v: &serde_json::Value, field: &str| {
            v.get(field).and_then(|v| v.as_f64()).unwrap_or(0.0)
        };
        let (hours, est_usd) = match into.get(&sym) {
            Some(prev) => (
                field(prev, "hours") + field(&new, "hours"),
                field(prev, "est_usd") + field(&new, "est_usd"),
            ),
            None => {
                into.insert(sym, new);
                continue;
            }
        };
        let mut merged = new;
        if let Some(obj) = merged.as_object_mut() {
            obj.insert("hours".into(), json!(hours));
            obj.insert("est_usd".into(), json!(est_usd));
        }
        into.insert(sym, merged);
    }
}

/// The budget key for a flip's reduce-only close: kept separate from the
/// plain symbol key (which tracks the paired opening) so a close that
/// keeps erroring or landing unfilled cannot resubmit forever just
/// because the opening it is blocking never spends its own budget.
fn flip_close_budget_key(symbol: &str) -> String {
    format!("{symbol}#close")
}

/// How one decision's execution ended.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecSummary {
    pub intents: usize,
    /// Intents actually handed to the executor (a retry that sent nothing
    /// because every opening was administratively blocked must not spend
    /// an execution attempt).
    pub sent: usize,
    /// Symbols (or, for a flip's reduce-only close, its own
    /// [`flip_close_budget_key`]) among those the venue actually saw. A
    /// flip's close spends its own separate budget rather than its paired
    /// opening's, so a close that keeps failing cannot resubmit
    /// unboundedly just because the opening it blocks never sends. A
    /// standalone reduction (no opening leg for the same symbol in this
    /// plan) still counts against the plain symbol key.
    pub sent_symbols: BTreeSet<String>,
    pub filled: usize,
    pub partial: usize,
    pub unfilled: usize,
    pub blocked: usize,
    pub errors: usize,
    pub residual: BTreeMap<String, f64>,
}

impl BookEngine {
    pub fn new(
        cfg: BookConfig,
        scheduler: Scheduler,
        exec: Arc<dyn Executor>,
        signals: Box<dyn SignalSource>,
        status: StatusWriter,
    ) -> Result<Self> {
        let state = BookState::load_or_new(&cfg.paths.state, &cfg.instance_id)
            .with_context(|| format!("load {}", cfg.paths.state.display()))?;
        let ledger = Ledger::new(cfg.paths.ledger.clone(), &cfg.instance_id);
        let pnl = Ledger::new(cfg.paths.pnl.clone(), &cfg.instance_id);
        let risk = RiskRails::new(cfg.risk.clone());
        let config_fp = cfg.fingerprint();
        // Reconstruct the process-local telemetry mirror from the
        // persisted decision record so a restart doesn't report
        // `signal_status: "none"` (and a bogus signal age) for up to a
        // full schedule period even though the book already holds an
        // accepted or partially applied signal.
        let signal_status = match state.last_decision.as_ref() {
            Some(r) => {
                let sha_prefix = r
                    .signal_sha256
                    .as_deref()
                    .map(|s| s[..12.min(s.len())].to_string());
                match r.outcome {
                    DecisionOutcome::Applied => {
                        format!("applied:{}", sha_prefix.as_deref().unwrap_or("-"))
                    }
                    DecisionOutcome::Partial => {
                        format!("partial:{}", sha_prefix.as_deref().unwrap_or("-"))
                    }
                    DecisionOutcome::Rejected => {
                        format!("rejected:{}", r.reject_reason.as_deref().unwrap_or("-"))
                    }
                    DecisionOutcome::Skipped => {
                        format!("skipped:{}", r.reject_reason.as_deref().unwrap_or("-"))
                    }
                    DecisionOutcome::Halted => "halted".to_string(),
                }
            }
            None => "none".to_string(),
        };
        // Independent of `last_decision`/`signal_status` above: the last
        // *accepted* signal's age must survive a later reject/skip
        // overwriting `last_decision`, so it is restored from its own
        // persisted field rather than from the current decision record.
        let last_signal_generated_at = state.last_accepted_signal_generated_at;
        Ok(Self {
            cfg,
            scheduler,
            exec,
            risk,
            ledger,
            pnl,
            status,
            signals,
            state,
            lots: HashMap::new(),
            last_signal_generated_at,
            signal_status,
            last_status_write: 0,
            status_interval_secs: 30,
            mark_on_date_change: true,
            last_flatten_attempt: 0,
            positions_ready: true,
            equity_ready: true,
            config_fp,
        })
    }

    pub fn state_path(&self) -> &Path {
        &self.cfg.paths.state
    }

    pub fn set_lot(&mut self, symbol: &str, lot: LotMeta) {
        self.lots.insert(symbol.to_string(), lot);
    }

    fn tracked_symbols(&self) -> Vec<String> {
        let mut v = self.cfg.universe.symbols.clone();
        for s in self.state.positions.keys() {
            if !v.contains(s) {
                v.push(s.clone());
            }
        }
        // A symbol can carry a `pending_funding_qty_hours` balance with no
        // open position and outside the configured universe -- adopted,
        // then fully closed while its funding rate was unavailable. Keep
        // fetching its price so the orphan-settlement pass in
        // `write_daily_mark` can eventually clear the carry once a rate
        // recovers, instead of skipping it forever at `prices.get(&sym)`.
        for s in self.state.pending_funding_qty_hours.keys() {
            if !v.contains(s) {
                v.push(s.clone());
            }
        }
        v
    }

    async fn lot_for(&mut self, symbol: &str) -> Option<LotMeta> {
        if let Some(l) = self.lots.get(symbol) {
            return Some(*l);
        }
        match self.exec.lot_meta(symbol).await {
            Ok(l) => {
                self.lots.insert(symbol.to_string(), l);
                Some(l)
            }
            Err(e) => {
                log::warn!("[LOT] {symbol}: {e}");
                None
            }
        }
    }

    async fn lots_for(&mut self, symbols: &[String]) -> HashMap<String, LotMeta> {
        let mut out = HashMap::new();
        for s in symbols {
            if let Some(l) = self.lot_for(s).await {
                out.insert(s.clone(), l);
            }
        }
        out
    }

    /// One engine step at `now` (unix seconds).
    pub async fn tick(&mut self, now: i64) -> Result<()> {
        let symbols = self.tracked_symbols();
        let mut prices = self.exec.prices(&symbols).await;

        if !self.exec.is_paper() {
            self.positions_ready = self.reconcile_with_venue(now, &mut prices).await;
        }

        // On the first tick of a new UTC date, accrue funding since the
        // prior mark *before* equity is computed and the rails evaluated,
        // and hold the mark row back until after any resulting halt or
        // flatten and this tick's decision (written at the bottom of this
        // method). Accruing afterwards, as the old `maybe_daily_mark` did,
        // let a midnight decision open the new target one tick before a
        // funding-triggered loss breach was noticed, and finalized that
        // date's sole mark before the halt/flatten it should reflect --
        // the same ordering replay's `daily_mark_now` already enforces.
        // A failed row write leaves `last_mark_date` unset so the next
        // tick retries; per-leg `funding_accrued_at` then only covers the
        // seconds since this accrual, never double-charging. That also
        // means each retry's own `funding_detail` only describes its own
        // sliver, not the whole day -- merged onto whatever a prior
        // failed attempt already accrued (`state.pending_mark_funding_detail`,
        // persisted below with the rest of `state` so a restart between a
        // failed append and the next retry does not lose it) so the
        // eventual successful row still reports the full breakdown.
        let today = utc_date(now);
        let pending_mark =
            if self.mark_on_date_change && self.state.last_mark_date.as_deref() != Some(&today) {
                let (accrual_date, funding_detail) = self.accrue_daily_funding(now, &prices).await;
                let mut merged = std::mem::take(&mut self.state.pending_mark_funding_detail);
                merge_funding_detail(&mut merged, funding_detail);
                // The date this cycle was *first* attempted for, not
                // whatever `accrue_daily_funding` just computed from `now`:
                // a retry that doesn't succeed until after a UTC rollover
                // must still write (and keep retrying) the date whose append
                // actually failed, not silently relabel it as `today`'s row
                // and permanently omit the stuck date. Funding accrual
                // itself is unaffected either way -- it is always just hours
                // elapsed since `funding_accrued_at`, independent of which
                // date label this row ends up under.
                let label = self.state.pending_mark_date.clone().unwrap_or(accrual_date);
                self.state.pending_mark_date = Some(label.clone());
                Some((label, merged))
            } else {
                None
            };

        let (equity, equity_ready) = self.compute_equity(&prices).await;
        self.equity_ready = equity_ready;

        // An ack re-anchors both loss windows at `equity`, so it is only
        // consumed while that number is a fresh venue value; a stale one
        // would hand the session an unintended cushion once reads recover.
        // The ack is only consumed once the halt's own flatten has
        // finished. Clearing it over an exposed book would stop the
        // flatten retry while the position is still open, and the halted
        // decision is not eligible for a residual retry either. A failed
        // live position read leaves `is_flat()` reading the last-known
        // (possibly stale) book, so it cannot stand in for flatness on
        // its own -- a fresh reconcile is required too.
        let halted_but_exposed =
            self.state.session.halted && (!self.positions_ready || !self.state.is_flat());
        if equity_ready && !halted_but_exposed {
            if let Some(ev) = self.risk.maybe_clear_halt(&mut self.state, now, equity) {
                log::warn!("[RISK] session halt cleared by RISK_ACK: {ev:?}");
                self.ledger
                    .write(now, "halt_cleared", None, json!({ "risk": ev }));
            }
        } else if self.state.session.halted && self.cfg.risk.risk_ack_path.exists() {
            let why = if halted_but_exposed {
                "the halted book is not confirmed flat yet"
            } else {
                "venue equity is unavailable"
            };
            log::warn!(
                "[RISK] RISK_ACK present but {why}; ack deferred (the file is left in place)"
            );
        }
        // Anchors, rollovers, and the loss limits are only ever evaluated
        // against a fresh venue equity (paper equity is always fresh): a
        // fallback value would anchor the session/day at a stale number.
        let events = if equity_ready {
            self.risk.evaluate(&mut self.state, now, equity)
        } else {
            Vec::new()
        };
        for ev in &events {
            match ev {
                RiskEvent::SessionHalt { .. } => {
                    log::error!("[RISK] SESSION HALT {ev:?}; flattening");
                    self.ledger
                        .write(now, "halt", None, json!({ "risk": ev, "equity": equity }));
                    if let Some(r) = self.state.last_decision.as_mut() {
                        r.outcome = DecisionOutcome::Halted;
                        r.at = now;
                    }
                    self.flatten_now(now, &prices, "session_halt").await;
                }
                RiskEvent::DailyHalt { .. } => {
                    log::warn!("[RISK] daily loss halt {ev:?}; opens blocked until next UTC day");
                    self.ledger
                        .write(now, "halt", None, json!({ "risk": ev, "equity": equity }));
                }
                RiskEvent::DailyRollover { .. } | RiskEvent::SessionHaltCleared { .. } => {
                    log::info!("[RISK] {ev:?}");
                }
            }
        }

        if self.state.session.halted && !self.state.is_flat() && self.positions_ready {
            if now - self.last_flatten_attempt >= 30 {
                self.flatten_now(now, &prices, "session_halt_retry").await;
            }
        } else if self.positions_ready {
            // An overdue fixed-window flatten always runs before a new
            // decision can touch the book.
            self.process_flatten(now, &prices).await;
            self.process_decision(now, &prices).await;
        }

        if let Some((date, funding_detail)) = pending_mark {
            // Error already logged inside; `last_mark_date` stays unset so
            // the next tick retries the row (see the accrual above). Keep
            // this attempt's merged detail for that retry to build on;
            // clear it once a row actually lands.
            match self
                .write_daily_mark_row(now, &prices, &date, funding_detail.clone())
                .await
            {
                Ok(()) => {
                    self.state.pending_mark_funding_detail.clear();
                    self.state.pending_mark_date = None;
                }
                Err(_) => self.state.pending_mark_funding_detail = funding_detail,
            }
        }

        let (equity, _) = self.compute_equity(&prices).await;
        self.write_status(now, &prices, equity);
        self.state
            .persist(&self.cfg.paths.state)
            .with_context(|| format!("persist {}", self.cfg.paths.state.display()))?;
        Ok(())
    }

    /// Live only: the venue position is the truth. Returns false when it
    /// could not be read (trading is suppressed for this tick).
    async fn reconcile_with_venue(&mut self, now: i64, prices: &mut HashMap<String, f64>) -> bool {
        let venue = match self.exec.positions().await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[RECONCILE] venue positions unreadable: {e}; no trading this tick");
                return false;
            }
        };
        let mut symbols: Vec<String> = venue.keys().cloned().collect();
        for s in self.state.positions.keys() {
            if !symbols.contains(s) {
                symbols.push(s.clone());
            }
        }
        // A venue-discovered leg outside the tracked set has no price in
        // `prices`; ask the executor (WS mid or ticker fallback) for it so
        // adoption is not deferred forever. The result is merged into the
        // caller's map, not a clone: the same tick's risk flatten and
        // decision must be able to price the leg they just adopted.
        let extra: Vec<String> = symbols
            .iter()
            .filter(|s| !prices.contains_key(*s))
            .cloned()
            .collect();
        if !extra.is_empty() {
            let fetched = self.exec.prices(&extra).await;
            prices.extend(fetched);
        }
        for sym in symbols {
            let venue_qty = venue.get(&sym).map(|p| p.qty).unwrap_or(0.0);
            let book_qty = self.state.positions.get(&sym).map(|p| p.qty).unwrap_or(0.0);
            let tol = self
                .lots
                .get(&sym)
                .map(|l| 10f64.powi(-(l.size_decimals as i32)) * 0.5)
                .unwrap_or(1e-9);
            let book_basis = self.state.positions.get(&sym).map(|p| p.avg_price);
            let book_basis_ok = book_basis.map_or(true, |a| a > 0.0);
            // The venue's own average entry is authoritative. It can differ
            // from the stored one at an unchanged quantity: a fill booked
            // at `mid_estimate` after a lost acknowledgement, or an
            // external close-and-reopen of the same net size between ticks.
            // Leaving it would corrupt unrealized and later realized PnL
            // for as long as the leg lives.
            let venue_basis_stale = match (
                venue
                    .get(&sym)
                    .and_then(|p| p.entry_price)
                    .filter(|e| *e > 0.0),
                book_basis.filter(|a| *a > 0.0),
            ) {
                (Some(v), Some(b)) => ((v - b) / b).abs() > 1e-6,
                _ => false,
            };
            if (venue_qty - book_qty).abs() <= tol && book_basis_ok && !venue_basis_stale {
                continue;
            }
            // The venue's own average entry, when it reports one. Only
            // this re-bases a leg the book already holds on the same side:
            // a mark is a fine price for *booking* a recovered reduction,
            // but writing it into `avg_price` would erase the remaining
            // leg's unrealized PnL.
            let venue_entry = venue
                .get(&sym)
                .and_then(|p| p.entry_price)
                .filter(|e| *e > 0.0);
            let entry = venue_entry
                .or_else(|| prices.get(&sym).copied().filter(|p| *p > 0.0))
                .or_else(|| {
                    self.state
                        .positions
                        .get(&sym)
                        .map(|p| p.avg_price)
                        .filter(|a| *a > 0.0)
                })
                .unwrap_or(0.0);
            // A leg the venue shows as smaller (or gone) closed without
            // this process booking it -- a fill that landed after the last
            // persist, or a crash between the send and `book_fill`. Book
            // the reduction at the current mark so realized PnL and the
            // trade counters are recovered instead of silently dropped;
            // the price is an estimate and both rows say so.
            let same_side = venue_qty != 0.0 && (venue_qty > 0.0) == (book_qty > 0.0);
            let closed_qty = if book_qty == 0.0 {
                0.0
            } else if venue_qty == 0.0 || !same_side {
                -book_qty
            } else if venue_qty.abs() < book_qty.abs() {
                venue_qty - book_qty
            } else {
                0.0
            };
            // Booking a recovered close needs a *current* mark. The stored
            // basis is not one: closing against it realizes exactly zero
            // and writes a `reconcile_mark` exit that buries whatever the
            // leg actually made, and the leg is then removed, so the real
            // number can never be recovered. Keep the leg and wait for a
            // price instead; trading stays suppressed meanwhile.
            let close_mark = prices
                .get(&sym)
                .copied()
                .filter(|p| p.is_finite() && *p > 0.0);
            if closed_qty != 0.0 && close_mark.is_none() {
                log::warn!(
                    "[ADOPT] {sym}: venue closed {closed_qty} of the book leg but no current mark is available; deferring the booking"
                );
                return false;
            }
            if venue_qty != 0.0 && entry <= 0.0 {
                // No basis yet (venue reports no entry price and no WS mid
                // has arrived): adopting now would book PnL from zero.
                // Wait for a price; trading stays suppressed meanwhile.
                log::warn!("[ADOPT] {sym}: venue qty {venue_qty} but no entry basis available yet; deferring");
                return false;
            }
            log::warn!(
                "[ADOPT] {sym}: venue qty {venue_qty} != book qty {book_qty}; adopting venue (entry={entry})"
            );
            self.ledger.write(
                now,
                "adopt",
                None,
                json!({
                    "symbol": sym,
                    "venue_qty": venue_qty,
                    "book_qty": book_qty,
                    "entry_price": entry,
                }),
            );
            // Settle the stored leg's funding up to now before its quantity
            // or basis changes, so the next accrual applies only to the
            // adopted leg from this instant.
            if self.state.positions.contains_key(&sym) {
                let rate = self.exec.funding_rate_hourly(&sym).await;
                let px = prices
                    .get(&sym)
                    .copied()
                    .or_else(|| self.state.positions.get(&sym).map(|p| p.avg_price))
                    .unwrap_or(0.0);
                self.accrue_funding(&sym, now, px, rate);
            }
            // Same signed quantity, different venue basis: either the leg
            // was closed and reopened outside this process, or an earlier
            // increase was booked at a mid-price estimate (a lost
            // acknowledgement, or an eventually-consistent fills endpoint)
            // that the venue's own average entry now supersedes. Position
            // data alone cannot tell the two apart -- inventing a close
            // price for the former would fabricate a trade, so leg
            // history (opened_at, realized_pnl) is left untouched here
            // and only avg_price is corrected below; the correction is
            // still recorded for reconciliation against the venue's own
            // trade history.
            if closed_qty == 0.0
                && venue_qty != 0.0
                && venue_basis_stale
                && (venue_qty - book_qty).abs() <= tol
            {
                let old_basis = book_basis.unwrap_or(0.0);
                log::warn!(
                    "[ADOPT] {sym}: same quantity {venue_qty} but the venue basis moved {old_basis} -> {entry}; could be an external close/reopen or a mid-estimate fill now corrected -- leg history preserved either way"
                );
                self.ledger.write(
                    now,
                    "basis_correction",
                    None,
                    json!({
                        "symbol": sym,
                        "qty": venue_qty,
                        "book_basis": old_basis,
                        "venue_basis": entry,
                        "realized_pnl_recoverable": false,
                    }),
                );
            }
            if closed_qty != 0.0 {
                // Guaranteed present: the deferral above returned without
                // touching the book when there was no mark.
                if let Some(mark) = close_mark {
                    let trades_closed_before = self.state.trades_closed;
                    let realized = self.state.apply_fill(&sym, closed_qty, mark, now);
                    let leg_closed = self.state.trades_closed != trades_closed_before;
                    log::warn!(
                        "[ADOPT] {sym}: booking an unrecorded close of {closed_qty} at the {mark} mark (realized ${realized:.4}, estimated price)"
                    );
                    let row = json!({
                        "symbol": sym,
                        "closed_qty": closed_qty,
                        "fill_price": mark,
                        "fill_price_source": "reconcile_mark",
                        "realized_usd": realized,
                        "recovered": true,
                        "paper": self.exec.is_paper(),
                    });
                    self.ledger.write(now, "recovered_close", None, row.clone());
                    // A recovered fill only fully closes the leg when
                    // `apply_fill` advances `trades_closed`; a recovered
                    // partial reduction (venue still holds a smaller
                    // same-side position) is a distinct event, matching the
                    // ordinary-fill path in `book_fill`.
                    let event = if leg_closed { "exit" } else { "partial_reduce" };
                    self.pnl.write(now, event, None, row);
                }
            }
            if venue_qty == 0.0 {
                self.state.positions.remove(&sym);
            } else {
                let p = self
                    .state
                    .positions
                    .entry(sym.clone())
                    .or_insert(super::state::Position {
                        qty: 0.0,
                        avg_price: entry,
                        opened_at: now,
                        funding_accrued_at: Some(now),
                        realized_pnl: 0.0,
                    });
                let kept_same_side = book_qty != 0.0 && (venue_qty > 0.0) == (book_qty > 0.0);
                p.qty = venue_qty;
                p.funding_accrued_at = Some(now);
                // Re-base on the venue's own average entry when it gives
                // one. Otherwise keep the stored basis for a leg that is
                // still on the same side (a recovered reduction already
                // realized its part against it); only a leg with no valid
                // basis, or one that flipped or appeared from nowhere,
                // falls back to the mark.
                let added = venue_qty.abs() - book_qty.abs();
                if let Some(v) = venue_entry {
                    p.avg_price = v;
                } else if kept_same_side && added > 0.0 && entry > 0.0 && p.avg_price > 0.0 {
                    // A recovered increase with no venue basis: fold the
                    // added quantity in at the mark, as `apply_fill` would
                    // have. Keeping the old basis unchanged would price
                    // the added units as if they were bought at the old
                    // level and overstate the unrealized PnL.
                    p.avg_price = (p.avg_price * book_qty.abs() + entry * added) / venue_qty.abs();
                } else if entry > 0.0 && (!kept_same_side || p.avg_price <= 0.0) {
                    p.avg_price = entry;
                }
            }
        }
        true
    }

    /// Current equity and whether it is a fresh venue value (paper equity
    /// is always fresh).
    async fn compute_equity(&self, prices: &HashMap<String, f64>) -> (f64, bool) {
        if self.exec.is_paper() {
            // `unrealized_usd` silently substitutes a held leg's entry
            // price when its mid is missing (expired WS feed, partial
            // outage), which would erase that leg's mark-to-market from
            // the rails while still reporting equity as fresh. A held
            // position without a fresh mark must instead take the same
            // not-fresh / opens-blocked path the live venue-unavailable
            // case does.
            let all_held_marked = self.state.positions.keys().all(|s| prices.contains_key(s));
            if all_held_marked {
                return (
                    self.cfg.risk.equity_reference_usd + self.state.cum_realized_usd
                        - self.state.cum_fees_usd
                        + self.state.cum_funding_est_usd
                        + self.state.unrealized_usd(prices),
                    true,
                );
            }
            log::warn!(
                "[EQUITY] paper: missing a fresh mark for a held leg; using last observation, opens blocked"
            );
            return (
                self.state
                    .last_equity
                    .map(|(_, e)| e)
                    .unwrap_or(self.cfg.risk.equity_reference_usd),
                false,
            );
        }
        match self.exec.equity().await {
            Ok(Some(e)) if e.is_finite() && e > 0.0 => (e, true),
            other => {
                log::warn!(
                    "[EQUITY] venue equity unavailable ({other:?}); using last observation, opens blocked"
                );
                (
                    self.state
                        .last_equity
                        .map(|(_, e)| e)
                        .unwrap_or(self.cfg.risk.equity_reference_usd),
                    false,
                )
            }
        }
    }

    fn opens_allowed(&self) -> bool {
        self.equity_ready && self.risk.opens_allowed(&self.state)
    }

    fn block_reason(&self) -> &'static str {
        if !self.equity_ready {
            "equity_unavailable"
        } else {
            self.risk.block_reason(&self.state).unwrap_or("blocked")
        }
    }

    /// The previous decision's flatten obligation, if it is overdue and
    /// not yet completed.
    fn overdue_flatten(&self, now: i64) -> Option<(String, i64)> {
        let r = self.state.last_decision.as_ref()?;
        match r.flatten_at {
            Some(f) if !r.flatten_done && now >= f => Some((r.key.clone(), f)),
            _ => None,
        }
    }

    async fn process_decision(&mut self, now: i64, prices: &HashMap<String, f64>) {
        let Some(d) = self.scheduler.current(now) else {
            self.signal_status = "no_decision_yet".to_string();
            return;
        };
        // A previous key's flatten obligation (overdue or, if schedules
        // somehow overlap despite the config checks, still in the future)
        // must be settled before its record can be replaced.
        if let Some(prev) = self.state.last_decision.clone() {
            if prev.key != d.key && prev.flatten_at.is_some() && !prev.flatten_done {
                if self.state.is_flat() {
                    if let Some(r) = self.state.last_decision.as_mut() {
                        r.flatten_done = true;
                    }
                } else {
                    log::warn!(
                        "[DECISION] key={} deferred: flatten of {} (due {:?}) still pending",
                        d.key,
                        prev.key,
                        prev.flatten_at
                    );
                    self.signal_status = format!("waiting_flatten:{}", prev.key);
                    return;
                }
            }
        }
        let rec = self.state.last_decision.clone().filter(|r| r.key == d.key);
        match rec {
            None => {
                if now > d.window_end {
                    self.finish_skipped(now, &d, "window_missed");
                } else {
                    self.try_apply(now, &d, prices, 0).await;
                }
            }
            Some(r) => match r.outcome {
                DecisionOutcome::Rejected if now <= d.window_end => {
                    self.try_apply(now, &d, prices, r.attempts).await;
                }
                DecisionOutcome::Rejected => {
                    self.finish_skipped(now, &d, "window_closed_after_reject");
                }
                DecisionOutcome::Partial
                    if now <= d.window_end
                        && r.any_symbol_under_budget(self.cfg.execution.max_attempts) =>
                {
                    self.retry_residual(now, &d, &r, prices).await;
                }
                _ => {}
            },
        }
    }

    /// Re-plan a partially applied decision from its persisted target
    /// quantities: one decision key stays tied to exactly one accepted
    /// vector, whatever the producer file says now.
    async fn retry_residual(
        &mut self,
        now: i64,
        d: &Decision,
        rec: &DecisionRecord,
        prices: &HashMap<String, f64>,
    ) {
        let mut symbols: Vec<String> = rec.target_qty.keys().cloned().collect();
        for s in self.state.positions.keys() {
            if !symbols.contains(s) {
                symbols.push(s.clone());
            }
        }
        let lots = self.lots_for(&symbols).await;
        let current = self.state.signed_qty();
        let plan = match rebalance::plan_targets(
            &rec.target_qty,
            &current,
            prices,
            &lots,
            &self.cfg.sizing,
        ) {
            Ok(p) => p,
            Err(e) => {
                // Nothing was sent, so this costs no attempt either (same
                // rule as a fully blocked tick): a transient missing price
                // or lot-metadata response must not exhaust the budget
                // while the residual is still inside its window.
                log::warn!(
                    "[REBALANCE] key={} cannot plan the residual: {e} (attempt budget untouched at {})",
                    d.key,
                    rec.attempts
                );
                if let Some(r) = self.state.last_decision.as_mut() {
                    r.at = now;
                    r.reject_reason = Some(e.to_string());
                }
                self.ledger.write(
                    now,
                    "decision",
                    Some(&d.key),
                    json!({ "outcome": "partial", "retry": rec.attempts, "reason": e.label(), "detail": e.to_string() }),
                );
                return;
            }
        };
        // A leg that has used its own budget is dropped from the retry;
        // the others still go out. Nothing left to send means nothing to
        // record either -- the record already says `partial`. A flip's
        // close spends a separate budget key from its paired opening (see
        // `flip_close_budget_key`): if the close alone has exhausted its
        // budget, both legs for that symbol are dropped together, since
        // sending the opening without a confirmed close would double the
        // venue exposure rather than complete the flip.
        let mut plan = plan;
        // Distinguish "nothing to do" from "everything dropped for budget"
        // before the filter below can turn either into the same empty
        // `plan.intents`.
        let planned_no_intents = plan.intents.is_empty();
        let max = self.cfg.execution.max_attempts;
        // A symbol only has a live flip in *this* plan when both its
        // reduce-only close and its opening leg are still present -- once
        // the close has already filled (or there is no old position left
        // to close), a fresh opening for the symbol is a plain open, and a
        // leftover "#close" budget entry from an earlier tick's flip must
        // not block it.
        let opening_symbols: std::collections::HashSet<&str> = plan
            .intents
            .iter()
            .filter(|i| !i.reduce_only)
            .map(|i| i.symbol.as_str())
            .collect();
        let true_flip_symbols: std::collections::HashSet<String> = plan
            .intents
            .iter()
            .filter(|i| i.reduce_only && opening_symbols.contains(i.symbol.as_str()))
            .map(|i| i.symbol.clone())
            .collect();
        let symbol_exhausted = |symbol: &str| -> bool {
            rec.attempts_for(symbol) >= max
                || (true_flip_symbols.contains(symbol)
                    && rec.attempts_for(&flip_close_budget_key(symbol)) >= max)
        };
        let dropped: Vec<String> = plan
            .intents
            .iter()
            .filter(|i| symbol_exhausted(&i.symbol))
            .map(|i| i.symbol.clone())
            .collect();
        if !dropped.is_empty() {
            plan.intents.retain(|i| !symbol_exhausted(&i.symbol));
            log::warn!(
                "[REBALANCE] key={} legs out of attempts, not retried: {dropped:?}",
                d.key
            );
        }
        if plan.intents.is_empty() {
            if !planned_no_intents {
                // Every remaining intent was dropped for exhausted budget,
                // not because the target is met: the residual is real but
                // unactionable this window.
                log::debug!("[REBALANCE] key={} nothing left to retry this tick", d.key);
                return;
            }
            // `plan_targets` itself found nothing to do: the persisted
            // target is already met (e.g. a restart's reconcile adopted a
            // fill the crashed process sent but never confirmed before
            // persisting). Leaving the record `Partial` here would keep
            // `pending_residual` true and repeat this no-op replan every
            // eligible tick for the rest of the window.
            log::info!(
                "[DECISION] key={} residual already satisfied on replan; marking applied",
                d.key
            );
            self.signal_status = format!(
                "applied:{}",
                rec.signal_sha256
                    .as_deref()
                    .map(|s| &s[..12.min(s.len())])
                    .unwrap_or("-")
            );
            if let Some(r) = self.state.last_decision.as_mut() {
                r.outcome = DecisionOutcome::Applied;
                r.at = now;
                r.reject_reason = None;
            }
            self.ledger.write(
                now,
                "decision",
                Some(&d.key),
                json!({
                    "outcome": "applied",
                    "retry": rec.attempts,
                    "signal_sha256": rec.signal_sha256,
                    "reason": "residual_already_satisfied",
                }),
            );
            return;
        }
        let attempts = rec.attempts + 1;
        log::info!(
            "[DECISION] key={} retry attempt={} intents={} residual_targets={}",
            d.key,
            attempts,
            plan.intents.len(),
            rec.target_qty.len()
        );
        let summary = self.execute_plan(now, &d.key, &plan, prices).await;
        let outcome = if summary.residual.is_empty() {
            DecisionOutcome::Applied
        } else {
            DecisionOutcome::Partial
        };
        // Per-leg again: a retry whose intents were all blocked by the
        // rails or a cap has not used the venue, and a leg that aborted
        // before its send keeps its budget for later in the window.
        let mut by_symbol = rec.attempts_by_symbol.clone();
        for sym in &summary.sent_symbols {
            *by_symbol.entry(sym.clone()).or_insert(0) += 1;
        }
        let attempts = by_symbol
            .values()
            .copied()
            .max()
            .unwrap_or(rec.attempts)
            .max(rec.attempts);
        if let Some(r) = self.state.last_decision.as_mut() {
            r.outcome = outcome;
            r.attempts = attempts;
            r.attempts_by_symbol = by_symbol;
            r.at = now;
            r.reject_reason = None;
        }
        let label = match outcome {
            DecisionOutcome::Applied => "applied",
            _ => "partial",
        };
        self.signal_status = format!(
            "{label}:{}",
            rec.signal_sha256
                .as_deref()
                .map(|s| &s[..12.min(s.len())])
                .unwrap_or("-")
        );
        self.ledger.write(
            now,
            "decision",
            Some(&d.key),
            json!({
                "outcome": label,
                "retry": attempts,
                "signal_sha256": rec.signal_sha256,
                "intents": summary.intents,
                "filled": summary.filled,
                "partial": summary.partial,
                "unfilled": summary.unfilled,
                "blocked": summary.blocked,
                "errors": summary.errors,
                "residual_qty": summary.residual,
            }),
        );
    }

    fn finish_skipped(&mut self, now: i64, d: &Decision, reason: &str) {
        log::warn!("[DECISION] key={} skipped: {reason}", d.key);
        let prev = self.state.last_decision.take().filter(|r| r.key == d.key);
        self.state.last_decision = Some(DecisionRecord {
            key: d.key.clone(),
            outcome: DecisionOutcome::Skipped,
            at: now,
            signal_sha256: None,
            signal_generated_at: None,
            reject_reason: Some(reason.to_string()),
            attempts: prev.as_ref().map(|r| r.attempts).unwrap_or(0),
            attempts_by_symbol: prev
                .as_ref()
                .map(|r| r.attempts_by_symbol.clone())
                .unwrap_or_default(),
            flatten_at: d.flatten_at,
            flatten_done: prev.map(|r| r.flatten_done).unwrap_or(false),
            target_qty: BTreeMap::new(),
        });
        self.signal_status = format!("skipped:{reason}");
        self.ledger.write(
            now,
            "decision",
            Some(&d.key),
            json!({ "outcome": "skipped", "reason": reason, "decision_at": d.decision_at }),
        );
    }

    fn record_reject(
        &mut self,
        now: i64,
        d: &Decision,
        label: &str,
        detail: String,
        attempts: u32,
    ) {
        let changed = self
            .state
            .last_decision
            .as_ref()
            .map(|r| r.key != d.key || r.reject_reason.as_deref() != Some(detail.as_str()))
            .unwrap_or(true);
        let prev = self.state.last_decision.take().filter(|r| r.key == d.key);
        self.state.last_decision = Some(DecisionRecord {
            key: d.key.clone(),
            outcome: DecisionOutcome::Rejected,
            at: now,
            signal_sha256: None,
            signal_generated_at: None,
            reject_reason: Some(detail.clone()),
            attempts,
            attempts_by_symbol: prev
                .as_ref()
                .map(|r| r.attempts_by_symbol.clone())
                .unwrap_or_default(),
            flatten_at: d.flatten_at,
            flatten_done: prev.map(|r| r.flatten_done).unwrap_or(false),
            target_qty: BTreeMap::new(),
        });
        self.signal_status = format!("rejected:{label}");
        if changed {
            log::warn!("[SIGNAL] rejected key={} reason={detail}", d.key);
            self.ledger.write(
                now,
                "decision",
                Some(&d.key),
                json!({ "outcome": "rejected", "reason": label, "detail": detail }),
            );
        }
    }

    async fn try_apply(
        &mut self,
        now: i64,
        d: &Decision,
        prices: &HashMap<String, f64>,
        attempts: u32,
    ) {
        let body = match self.signals.read(&d.key, now) {
            Ok(Some(b)) => b,
            Ok(None) => {
                self.signal_status = "waiting_for_file".to_string();
                if !matches!(
                    self.state.last_decision.as_ref(),
                    Some(r) if r.key == d.key
                ) {
                    log::info!(
                        "[SIGNAL] key={} waiting for {}",
                        d.key,
                        self.cfg.signal.path.display()
                    );
                }
                return;
            }
            Err(e) => {
                let r = SignalReject::Unreadable(e.to_string());
                self.record_reject(now, d, r.label(), r.to_string(), attempts);
                return;
            }
        };
        let ctx = DecisionContext {
            now: Utc.timestamp_opt(now, 0).single().unwrap_or_else(Utc::now),
            decision_key: &d.key,
            decision_at: Utc
                .timestamp_opt(d.decision_at, 0)
                .single()
                .unwrap_or_else(Utc::now),
        };
        let sig = match signal::validate(
            &body,
            &self.cfg.signal,
            &self.cfg.sizing,
            &self.cfg.universe,
            &ctx,
        ) {
            Ok(s) => s,
            Err(r) => {
                self.record_reject(now, d, r.label(), r.to_string(), attempts);
                return;
            }
        };
        self.apply_signal(now, d, &sig, prices, attempts).await;
    }

    async fn apply_signal(
        &mut self,
        now: i64,
        d: &Decision,
        sig: &ValidSignal,
        prices: &HashMap<String, f64>,
        attempts: u32,
    ) {
        let mut symbols: Vec<String> = sig.weights.keys().cloned().collect();
        for s in self.state.positions.keys() {
            if !symbols.contains(s) {
                symbols.push(s.clone());
            }
        }
        let lots = self.lots_for(&symbols).await;
        let current = self.state.signed_qty();
        let plan = match rebalance::plan(&sig.weights, &current, prices, &lots, &self.cfg.sizing) {
            Ok(p) => p,
            Err(r) => {
                self.record_reject(now, d, r.label(), r.to_string(), attempts);
                return;
            }
        };
        let prior_attempts = attempts;
        // Per-leg budgets carried over from an earlier tick on this key
        // (a rejected read, or a crash between the persist and the send).
        let prior_by_symbol = self
            .state
            .last_decision
            .as_ref()
            .filter(|r| r.key == d.key)
            .map(|r| r.attempts_by_symbol.clone())
            .unwrap_or_default();
        let attempts = attempts + 1;
        log::info!(
            "[DECISION] key={} sha={} attempt={} intents={} gross_target=${:.2} net_target=${:.2} skipped={}",
            d.key,
            &sig.payload_sha256[..12],
            attempts,
            plan.intents.len(),
            plan.gross_target_usd,
            plan.net_target_usd,
            plan.skipped.len()
        );
        // Durably record the accepted vector BEFORE the first order goes
        // out: a crash mid-execution then restarts with the hash and the
        // rounded targets on disk, so the residual is retried from them
        // (never from a rewritten or missing producer file). The attempt
        // count stays at its prior value here -- a crash before or during
        // the first submission must not consume an attempt, which with
        // max_attempts: 1 would otherwise strand the target forever.
        self.state.last_decision = Some(DecisionRecord {
            key: d.key.clone(),
            outcome: DecisionOutcome::Partial,
            at: now,
            signal_sha256: Some(sig.payload_sha256.clone()),
            signal_generated_at: Some(sig.generated_at.timestamp()),
            reject_reason: None,
            attempts: prior_attempts,
            attempts_by_symbol: prior_by_symbol.clone(),
            flatten_at: d.flatten_at,
            flatten_done: false,
            target_qty: plan.target_qty.clone(),
        });
        self.state.last_accepted_signal_generated_at = Some(sig.generated_at.timestamp());
        if let Err(e) = self.state.persist(&self.cfg.paths.state) {
            log::error!(
                "[DECISION] key={} cannot persist the accepted target ({e}); no orders sent this tick",
                d.key
            );
            self.state.last_decision = None;
            return;
        }
        let summary = self.execute_plan(now, &d.key, &plan, prices).await;
        let outcome = if summary.residual.is_empty() {
            DecisionOutcome::Applied
        } else {
            DecisionOutcome::Partial
        };
        // The budget is per leg: only the symbols the venue actually saw
        // spend an attempt, so a filled reduction cannot exhaust the
        // opening it was planned with. The scalar is the worst of them,
        // for the ledger and the logs.
        let mut by_symbol = prior_by_symbol;
        for sym in &summary.sent_symbols {
            *by_symbol.entry(sym.clone()).or_insert(0) += 1;
        }
        let attempts = by_symbol
            .values()
            .copied()
            .max()
            .unwrap_or(prior_attempts)
            .max(prior_attempts);
        self.last_signal_generated_at = Some(sig.generated_at.timestamp());
        self.state.last_accepted_signal_generated_at = self.last_signal_generated_at;
        self.signal_status = format!(
            "{}:{}",
            match outcome {
                DecisionOutcome::Applied => "applied",
                _ => "partial",
            },
            &sig.payload_sha256[..12]
        );
        self.state.last_decision = Some(DecisionRecord {
            key: d.key.clone(),
            outcome,
            at: now,
            signal_sha256: Some(sig.payload_sha256.clone()),
            signal_generated_at: Some(sig.generated_at.timestamp()),
            reject_reason: None,
            attempts,
            attempts_by_symbol: by_symbol,
            flatten_at: d.flatten_at,
            flatten_done: false,
            target_qty: plan.target_qty.clone(),
        });
        self.ledger.write(
            now,
            "decision",
            Some(&d.key),
            json!({
                "outcome": match outcome { DecisionOutcome::Applied => "applied", _ => "partial" },
                "signal_sha256": sig.payload_sha256,
                "signal_generated_at": sig.generated_at.to_rfc3339(),
                "signal_as_of": sig.as_of.to_rfc3339(),
                "attempt": attempts,
                "weights": sig.weights,
                "gross_target_usd": plan.gross_target_usd,
                "net_target_usd": plan.net_target_usd,
                "intents": summary.intents,
                "filled": summary.filled,
                "partial": summary.partial,
                "unfilled": summary.unfilled,
                "blocked": summary.blocked,
                "errors": summary.errors,
                "residual_qty": summary.residual,
                "skipped": plan.skipped,
            }),
        );
        if outcome == DecisionOutcome::Partial {
            log::warn!(
                "[REBALANCE] key={} residual after attempt {}: {:?}",
                d.key,
                attempts,
                summary.residual
            );
        }
    }

    /// Execute every intent of `plan` in order; opening intents are dropped
    /// while the risk rails block opens. Returns the residual against the
    /// plan's targets.
    async fn execute_plan(
        &mut self,
        now: i64,
        key: &str,
        plan: &Plan,
        prices: &HashMap<String, f64>,
    ) -> ExecSummary {
        let mut s = ExecSummary {
            intents: plan.intents.len(),
            sent: 0,
            sent_symbols: BTreeSet::new(),
            filled: 0,
            partial: 0,
            unfilled: 0,
            blocked: 0,
            errors: 0,
            residual: BTreeMap::new(),
        };
        // Set once a fill has moved the accounting, so the loss rails are
        // re-read before the next opening: a reduction that fills
        // adversely (or its fees) can cross a limit mid-plan, and the
        // tick's own evaluation already ran before this loop.
        let mut fills_since_check = false;
        // Once a rail is breached mid-plan it stays breached for every
        // remaining opening: the read-only check engages no halt, so
        // without latching only the first opening after the breach would
        // be stopped and the next would go out against the same rail.
        let mut rail_blocked: Option<String> = None;
        // Symbols with a paired opening leg in this plan: a sign flip is
        // split into a reduce-only close followed by an opening intent for
        // the same symbol (see `rebalance::plan_targets`). The close must
        // not spend that symbol's attempt budget itself, or a blocked or
        // aborted opening right after it would never get to retry.
        let flip_symbols: std::collections::HashSet<&str> = plan
            .intents
            .iter()
            .filter(|i| !i.reduce_only)
            .map(|i| i.symbol.as_str())
            .collect();
        // The subset of `flip_symbols` with an actual paired close in this
        // plan (a plain open or increase has no reduce-only sibling and
        // must not be gated on one). The opening leg of a real flip must
        // not reach the venue until its close has fully filled: a zero or
        // partial close leaves the old position live, so the paired
        // "open" would merely net against it on the venue rather than
        // establish the new side, stranding the target once both legs'
        // budgets are spent.
        let true_flip_symbols: std::collections::HashSet<&str> = plan
            .intents
            .iter()
            .filter(|i| i.reduce_only && flip_symbols.contains(i.symbol.as_str()))
            .map(|i| i.symbol.as_str())
            .collect();
        let mut flip_close_filled: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        // Funding rates for every intent's symbol, fetched concurrently up
        // front rather than one REST get_ticker (live) per intent inline
        // in the loop below: a session-halt or overdue flatten with
        // several legs must not be delayed by the sum of per-leg ticker
        // timeouts when usable WS prices are already available to close
        // with. A rate missed here (fetch failed, or the position closed
        // before this plan was built) still falls through to
        // accrue_funding's `None` path (pending_funding_qty_hours),
        // exactly like a live rate lookup failing ever did.
        let mut funding_fetches = tokio::task::JoinSet::new();
        for sym in plan
            .intents
            .iter()
            .map(|i| i.symbol.clone())
            .collect::<std::collections::HashSet<_>>()
        {
            if !self.state.positions.contains_key(&sym) {
                continue;
            }
            let exec = self.exec.clone();
            funding_fetches.spawn(async move {
                let rate = exec.funding_rate_hourly(&sym).await;
                (sym, rate)
            });
        }
        let mut funding_rates: HashMap<String, Option<f64>> = HashMap::new();
        while let Some(res) = funding_fetches.join_next().await {
            if let Ok((sym, rate)) = res {
                funding_rates.insert(sym, rate);
            }
        }
        for (idx, intent) in plan.intents.iter().enumerate() {
            // Re-check the caps against the book that actually exists (see
            // `opening_cap_breach`): reductions ahead of this intent may
            // have failed, in which case the plan's end state is no longer
            // the target the planner validated.
            let mut cap_breach = if intent.reduce_only {
                None
            } else {
                self.opening_cap_breach(&plan.intents[idx..], prices)
            };
            if !intent.reduce_only
                && cap_breach.is_none()
                && true_flip_symbols.contains(intent.symbol.as_str())
                && !flip_close_filled.contains(intent.symbol.as_str())
            {
                cap_breach = Some("flip_close_incomplete".to_string());
            }
            if !intent.reduce_only && cap_breach.is_none() {
                if let Some(rail) = &rail_blocked {
                    cap_breach = Some(rail.clone());
                } else if fills_since_check {
                    fills_since_check = false;
                    let (equity, ready) = self.compute_equity(prices).await;
                    if ready {
                        if let Some(rail) = self.risk.loss_rail_breached(&self.state, equity) {
                            rail_blocked = Some(rail.to_string());
                        }
                    } else {
                        // The rails cannot be checked against current venue
                        // equity, and a fill has already moved the book:
                        // block the rest of the openings rather than send
                        // them blind.
                        self.equity_ready = false;
                        rail_blocked = Some("equity_unavailable".to_string());
                    }
                    cap_breach = rail_blocked.clone();
                }
            }
            if !intent.reduce_only && (cap_breach.is_some() || !self.opens_allowed()) {
                let reason = cap_breach.unwrap_or_else(|| self.block_reason().to_string());
                log::warn!(
                    "[ORDER] blocked {} {} {} qty={} reason={reason}",
                    intent.kind_label(),
                    intent.side,
                    intent.symbol,
                    intent.qty
                );
                self.ledger.write(
                    now,
                    "order_blocked",
                    Some(key),
                    json!({ "intent": intent, "reason": reason }),
                );
                s.blocked += 1;
                continue;
            }

            if let Err(e) =
                self.ledger
                    .try_write(now, "order_intent", Some(key), json!({ "intent": intent }))
            {
                // The durable intent/fill record this row establishes is a
                // precondition for the send that follows, not just an
                // observation of it: without it, a fill would leave zero
                // audit trail for later fee/PnL reconciliation. Block the
                // send rather than risk that.
                log::error!(
                    "[ORDER] blocked {} {} {} qty={} reason=ledger_append_failed: {e}",
                    intent.kind_label(),
                    intent.side,
                    intent.symbol,
                    intent.qty
                );
                self.ledger.write(
                    now,
                    "order_blocked",
                    Some(key),
                    json!({ "intent": intent, "reason": "ledger_append_failed" }),
                );
                s.blocked += 1;
                continue;
            }
            let rate = funding_rates.get(&intent.symbol).copied().flatten();
            match self.exec.execute(intent).await {
                Ok(fill) => {
                    s.sent += 1;
                    // A flip's close spends its own budget key, not the
                    // paired opening's; a standalone reduction (no opening
                    // leg for this symbol in the plan) counts against the
                    // plain symbol key as before.
                    if intent.reduce_only && flip_symbols.contains(intent.symbol.as_str()) {
                        s.sent_symbols.insert(flip_close_budget_key(&intent.symbol));
                    } else {
                        s.sent_symbols.insert(intent.symbol.clone());
                    }
                    if fill.filled_qty > 0.0 {
                        self.accrue_funding(&intent.symbol, now, intent.reference_price, rate);
                        fills_since_check = true;
                    }
                    let result = self.book_fill(now, key, intent, &fill);
                    match result {
                        "filled" => s.filled += 1,
                        "partial" => s.partial += 1,
                        _ => s.unfilled += 1,
                    }
                    if intent.reduce_only
                        && result == "filled"
                        && true_flip_symbols.contains(intent.symbol.as_str())
                    {
                        flip_close_filled.insert(intent.symbol.as_str());
                    }
                }
                Err(e) => {
                    // A pre-send abort (no usable price, or the book moved
                    // past the slippage budget) never reached the venue, so
                    // it must not spend the decision's attempt budget: the
                    // condition is transient and the residual should still
                    // be retried inside the window.
                    let pre_send = e.downcast_ref::<PreSendAbort>().is_some();
                    if !pre_send {
                        s.sent += 1;
                        // Same flip-stage rule as the success path.
                        if intent.reduce_only && flip_symbols.contains(intent.symbol.as_str()) {
                            s.sent_symbols.insert(flip_close_budget_key(&intent.symbol));
                        } else {
                            s.sent_symbols.insert(intent.symbol.clone());
                        }
                        // The order was submitted and its outcome is
                        // unknown: it may have filled and moved both the
                        // book and the rails without us seeing it. Stop
                        // the remaining openings until the next tick has
                        // reconciled against the venue.
                        rail_blocked = Some("unconfirmed_fill".to_string());
                    }
                    log::error!(
                        "[ORDER] {} {} {} failed{}: {e}",
                        intent.side,
                        intent.symbol,
                        intent.qty,
                        if pre_send { " before sending" } else { "" }
                    );
                    self.ledger.write(
                        now,
                        "order_error",
                        Some(key),
                        json!({ "intent": intent, "error": e.to_string(), "pre_send": pre_send }),
                    );
                    s.errors += 1;
                }
            }
        }
        // Residual against the plan's own targets.
        let current = self.state.signed_qty();
        for (sym, target) in &plan.target_qty {
            let cur = current.get(sym).copied().unwrap_or(0.0);
            let diff = target - cur;
            let price = prices.get(sym).copied().unwrap_or(0.0);
            let diff_usd = diff.abs() * price;
            let closing = *target == 0.0 && cur != 0.0;
            // A residual only counts if the planner could actually send it
            // next time. Closes, flips and reductions always can (the
            // venue minimum does not apply to a reduce-only order), but an
            // opening or increase whose quantity is under the venue
            // minimum is skipped as `below_venue_min_qty` on every retry:
            // counting it would re-run the decision on each tick and leave
            // it Partial until its window closed, for a leg that can never
            // be reached.
            let flipping = cur != 0.0 && *target != 0.0 && (cur > 0.0) != (*target > 0.0);
            let reducing = cur != 0.0 && (cur > 0.0) == (*target > 0.0) && target.abs() < cur.abs();
            let reachable = closing
                || flipping
                || reducing
                || self
                    .lots
                    .get(sym)
                    .and_then(|l| l.min_order_qty)
                    .map_or(true, |m| diff.abs() >= m);
            let tradeable = closing
                || (reachable
                    && diff_usd >= self.cfg.sizing.rebalance_deadband_usd
                    && diff_usd >= self.cfg.sizing.min_order_usd);
            if diff != 0.0 && tradeable {
                s.residual.insert(sym.clone(), diff);
            }
        }
        self.ledger.write(
            now,
            "rebalance_summary",
            Some(key),
            json!({
                "intents": s.intents, "filled": s.filled, "partial": s.partial,
                "unfilled": s.unfilled, "blocked": s.blocked, "errors": s.errors,
                "residual_qty": s.residual,
                "book": self.state.signed_qty(),
                "gross_usd": self.state.gross_usd(prices),
                "net_usd": self.state.net_usd(prices),
            }),
        );
        s
    }

    /// Would executing `remaining` (this intent and every intent after it,
    /// assuming full fills) leave the *actual* book past a configured cap?
    ///
    /// The planner validated the caps for the final target, which assumes
    /// every reduction ahead of this point filled. When one did not, the
    /// end state of the plan is no longer that target — e.g. rotating A
    /// into B with A's close unfilled would hold both legs. Projecting the
    /// rest of the plan onto the live book catches exactly that, while a
    /// normal dollar-neutral rotation (whose end state is the validated
    /// target) still passes even though its intermediate states are
    /// one-sided.
    fn opening_cap_breach(
        &self,
        remaining: &[OrderIntent],
        prices: &HashMap<String, f64>,
    ) -> Option<String> {
        let sz = &self.cfg.sizing;
        let mut projected = self.state.signed_qty();
        for i in remaining {
            let signed = match i.side {
                Side::Buy => i.qty,
                Side::Sell => -i.qty,
            };
            *projected.entry(i.symbol.clone()).or_insert(0.0) += signed;
        }
        let px_of = |sym: &String| -> f64 {
            prices
                .get(sym)
                .copied()
                .filter(|p| p.is_finite() && *p > 0.0)
                .or_else(|| self.state.positions.get(sym).map(|p| p.avg_price))
                .or_else(|| {
                    remaining
                        .iter()
                        .find(|i| &i.symbol == sym)
                        .map(|i| i.reference_price)
                })
                .unwrap_or(0.0)
        };
        let mut gross = 0.0;
        let mut net = 0.0;
        for (sym, qty) in &projected {
            let px = px_of(sym);
            gross += qty.abs() * px;
            net += qty * px;
        }
        // Per-symbol cap only for the symbol being opened: another leg over
        // its own cap is not this intent's doing, and the gross/net checks
        // below already stop it from being compounded.
        let sym = &remaining[0].symbol;
        let px = px_of(sym);
        let one_lot_usd = self
            .lots
            .get(sym)
            .map(|l| 10f64.powi(-(l.size_decimals as i32)) * px)
            .unwrap_or(0.0);
        let sym_cap = sz.max_symbol_weight * sz.gross_notional_usd + one_lot_usd;
        let sym_notional = projected.get(sym).copied().unwrap_or(0.0).abs() * px;
        if sym_notional > sym_cap + 1e-9 {
            return Some(format!(
                "cap_symbol({sym} ${sym_notional:.2} > ${sym_cap:.2})"
            ));
        }
        if gross > sz.max_gross_usd + 1e-9 {
            return Some(format!("cap_gross(${gross:.2} > ${:.2})", sz.max_gross_usd));
        }
        if net.abs() > sz.max_net_usd + 1e-9 {
            return Some(format!("cap_net(${net:.2} > ${:.2})", sz.max_net_usd));
        }
        None
    }

    /// Book a fill into the state, write the ledger row, return the result
    /// label.
    fn book_fill(
        &mut self,
        now: i64,
        key: &str,
        intent: &OrderIntent,
        fill: &FillReport,
    ) -> &'static str {
        let signed = match intent.side {
            Side::Buy => fill.filled_qty,
            Side::Sell => -fill.filled_qty,
        };
        let trades_closed_before = self.state.trades_closed;
        let realized = self
            .state
            .apply_fill(&intent.symbol, signed, fill.fill_price, now);
        let leg_closed = self.state.trades_closed != trades_closed_before;
        // An unknown fee is left out of the total rather than counted as
        // zero; the ledger row carries `fee_known: false` so it can be
        // reconciled against the venue later.
        if let Some(fee) = fill.fee_usd {
            self.state.cum_fees_usd += fee;
        }
        let result = if fill.filled_qty <= 0.0 {
            "unfilled"
        } else if fill.filled_qty < intent.qty * (1.0 - 1e-6) {
            "partial"
        } else {
            "filled"
        };
        let slippage_bps = if fill.filled_qty > 0.0 && intent.reference_price > 0.0 {
            let signed_slip =
                (fill.fill_price - intent.reference_price) / intent.reference_price * 10_000.0;
            match intent.side {
                Side::Buy => signed_slip,
                Side::Sell => -signed_slip,
            }
        } else {
            0.0
        };
        log::info!(
            "[FILL] {} {} {} {} requested={} filled={} px={:.6} ({}) slip_bps={:.2} realized=${:.4} fee=${:.4} result={result}",
            intent.kind_label(),
            intent.side,
            intent.symbol,
            if intent.reduce_only { "reduce_only" } else { "open" },
            intent.qty,
            fill.filled_qty,
            fill.fill_price,
            fill.fill_price_source,
            slippage_bps,
            realized,
            fill.fee_usd.unwrap_or(f64::NAN)
        );
        self.ledger.write(
            now,
            "fill",
            Some(key),
            json!({
                "intent": intent,
                "fill": fill,
                "result": result,
                "slippage_bps_vs_reference": slippage_bps,
                "fee_known": fill.fee_usd.is_some(),
                "realized_usd": realized,
                "paper": self.exec.is_paper(),
            }),
        );
        // An unfilled reduce-only IOC is routine; writing an exit row for
        // it would put a phantom close in the PnL log while the leg is
        // still open. A Close/Reduce fill that only trims the leg (leaves
        // it open, possibly still realizing PnL) is a distinct event from
        // one that actually closes it: `trades_closed` (and any consumer
        // treating "exit" rows as completed legs) must only advance on the
        // latter.
        if fill.filled_qty > 0.0 && leg_closed {
            self.pnl.write(
                now,
                "exit",
                Some(key),
                json!({
                    "symbol": intent.symbol,
                    "closed_qty": fill.filled_qty,
                    "fill_price": fill.fill_price,
                    "realized_usd": realized,
                    "fee_usd": fill.fee_usd,
                    "fee_known": fill.fee_usd.is_some(),
                    "paper": self.exec.is_paper(),
                }),
            );
        } else if fill.filled_qty > 0.0
            && !leg_closed
            && (realized != 0.0 || matches!(intent.kind, IntentKind::Close | IntentKind::Reduce))
        {
            self.pnl.write(
                now,
                "partial_reduce",
                Some(key),
                json!({
                    "symbol": intent.symbol,
                    "reduced_qty": fill.filled_qty,
                    "fill_price": fill.fill_price,
                    "realized_usd": realized,
                    "fee_usd": fill.fee_usd,
                    "fee_known": fill.fee_usd.is_some(),
                    "paper": self.exec.is_paper(),
                }),
            );
        }
        result
    }

    async fn flatten_now(&mut self, now: i64, prices: &HashMap<String, f64>, reason: &str) {
        self.last_flatten_attempt = now;
        let current = self.state.signed_qty();
        if current.is_empty() {
            return;
        }
        // No lot fetch: `plan_flatten` targets every leg to zero, and
        // `plan`'s tq == 0.0 branch closes at the exact current venue
        // quantity without ever consulting `lots` (dust included,
        // deadband/minimum don't apply to a close) -- by design, so a
        // protective flatten can submit during a ticker-metadata outage.
        // Fetching lots here first would only add REST latency (blocking
        // the select! loop tick() runs inside) for metadata this plan
        // never reads.
        let plan =
            match rebalance::plan_flatten(&current, prices, &HashMap::new(), &self.cfg.sizing) {
                Ok(p) => p,
                Err(e) => {
                    log::error!("[FLATTEN] cannot plan ({reason}): {e}");
                    self.ledger.write(
                        now,
                        "flatten",
                        None,
                        json!({ "reason": reason, "error": e.to_string() }),
                    );
                    return;
                }
            };
        log::warn!("[FLATTEN] reason={reason} legs={}", plan.intents.len());
        let key = self
            .state
            .last_decision
            .as_ref()
            .map(|r| r.key.clone())
            .unwrap_or_default();
        let summary = self.execute_plan(now, &key, &plan, prices).await;
        self.ledger.write(
            now,
            "flatten",
            Some(&key),
            json!({
                "reason": reason,
                "intents": summary.intents,
                "filled": summary.filled,
                "residual_qty": summary.residual,
                "flat": self.state.is_flat(),
            }),
        );
    }

    async fn process_flatten(&mut self, now: i64, prices: &HashMap<String, f64>) {
        let Some((key, _)) = self.overdue_flatten(now) else {
            return;
        };
        if !self.state.is_flat() && now - self.last_flatten_attempt >= 30 {
            self.flatten_now(now, prices, "fixed_window").await;
        }
        if self.state.is_flat() {
            if let Some(r) = self.state.last_decision.as_mut() {
                if r.key == key {
                    r.flatten_done = true;
                }
            }
        }
    }

    /// Book the funding estimate accrued on `symbol` since its last accrual
    /// (used at daily marks and right before any fill touches the leg, so a
    /// leg closed between marks is not left unaccounted).
    fn accrue_funding(
        &mut self,
        symbol: &str,
        now: i64,
        price: f64,
        rate_hourly: Option<f64>,
    ) -> Option<f64> {
        let p = self.state.positions.get_mut(symbol)?;
        let since = p.funding_accrued_at.unwrap_or(p.opened_at);
        let hours = ((now - since).max(0)) as f64 / 3600.0;
        let qty_hours_now = p.qty * hours;
        // Always catch the leg up to `now`: the elapsed exposure is fully
        // captured below (in `cum_funding_est_usd` or, absent a rate, in
        // `pending_funding_qty_hours`), so leaving the timestamp behind
        // would only risk double-counting a segment the caller is often
        // about to mutate or close out from under us.
        p.funding_accrued_at = Some(now);
        let rate = match rate_hourly {
            Some(r) => r,
            None => {
                // Freeze this segment's notional (qty * hours, at the
                // quantity actually open during it) into a symbol-keyed
                // carry rather than the position itself, so an imminent
                // fill that resizes or fully closes -- and removes -- the
                // leg cannot erase or misattribute it. Folded back in and
                // settled the next time a rate is available for the
                // symbol, even across a close and later reopen.
                if qty_hours_now != 0.0 {
                    *self
                        .state
                        .pending_funding_qty_hours
                        .entry(symbol.to_string())
                        .or_insert(0.0) += qty_hours_now;
                }
                return None;
            }
        };
        let pending = self
            .state
            .pending_funding_qty_hours
            .remove(symbol)
            .unwrap_or(0.0);
        let est = -(pending + qty_hours_now) * price * rate;
        self.state.cum_funding_est_usd += est;
        Some(est)
    }

    /// Write the daily mark for `now` unconditionally (replay: at the last
    /// tick of a bar date, after every decision/flatten of that date).
    /// Propagates a pnl.jsonl append failure instead of only logging it:
    /// unlike the live loop, replay has no later tick on the same date to
    /// retry the mark, so a swallowed failure here would let `run` return
    /// success with that date's mark silently missing.
    pub async fn daily_mark_now(&mut self, now: i64) -> Result<()> {
        let symbols = self.tracked_symbols();
        let prices = self.exec.prices(&symbols).await;
        let (date, funding_detail) = self.accrue_daily_funding(now, &prices).await;
        // The funding accrual above can move equity down; re-check the
        // rails against it immediately, same date. Without this, the only
        // remaining evaluation is the next date's first tick, which rolls
        // the daily anchor to this already funding-reduced equity before
        // ever comparing it against today's start_equity -- a funding loss
        // above the daily threshold but below the session one would then
        // never be observed as a halt, unlike a live tick, where the mark
        // runs on the new date's first tick and the funding hit is instead
        // caught by evaluate() on that same date's *next* tick.
        let (equity, equity_ready) = self.compute_equity(&prices).await;
        if equity_ready {
            for ev in self.risk.evaluate(&mut self.state, now, equity) {
                match ev {
                    RiskEvent::DailyHalt { .. } => {
                        log::warn!(
                            "[RISK] daily loss halt {ev:?} (post-mark funding accrual); opens blocked until next UTC day"
                        );
                        self.ledger.write(
                            now,
                            "halt",
                            None,
                            json!({ "risk": ev, "equity": equity }),
                        );
                    }
                    RiskEvent::SessionHalt { .. } => {
                        log::error!(
                            "[RISK] SESSION HALT {ev:?} (post-mark funding accrual); flattening"
                        );
                        self.ledger.write(
                            now,
                            "halt",
                            None,
                            json!({ "risk": ev, "equity": equity }),
                        );
                        if let Some(r) = self.state.last_decision.as_mut() {
                            r.outcome = DecisionOutcome::Halted;
                            r.at = now;
                        }
                        self.flatten_now(now, &prices, "session_halt").await;
                    }
                    RiskEvent::DailyRollover { .. } | RiskEvent::SessionHaltCleared { .. } => {
                        log::info!("[RISK] {ev:?} (post-mark funding accrual)");
                    }
                }
            }
        }
        // Write the mark only now, after any halt/flatten above, so it
        // reflects the post-flatten book instead of the state from before
        // the day-end risk check: on the final replay day nothing later
        // corrects a mark taken before the flatten. Propagated (`?`):
        // replay must fail the whole run here rather than proceed to the
        // next bar date with this date's mark missing.
        self.write_daily_mark_row(now, &prices, &date, funding_detail)
            .await?;
        // The halt/flatten above can change positions and flags after the
        // last tick already wrote status.json for this date; re-write it so
        // a replay ending on this date doesn't leave status.json reporting
        // the pre-halt book.
        let (equity, _) = self.compute_equity(&prices).await;
        self.write_status(now, &prices, equity);
        // Propagated for the same reason as the mark append above: on the
        // final replay day nothing later persists, so a swallowed failure
        // here would let `run` report a summary computed from the
        // post-mark in-memory state while state.json still reflects the
        // pre-mark tick (no final funding accrual, no funding-triggered
        // halt/flatten).
        self.state
            .persist(&self.cfg.paths.state)
            .with_context(|| format!("[MARK] {date} persist failed"))?;
        Ok(())
    }

    /// Funding accrual per held/orphaned leg for `now`'s date, without
    /// writing the mark row yet: `daily_mark_now` runs its own risk
    /// evaluation (which the accrual above can move) and any resulting
    /// halt/flatten between this and `write_daily_mark_row`, so the row
    /// itself reflects the post-flatten book instead of the pre-halt one.
    async fn accrue_daily_funding(
        &mut self,
        now: i64,
        prices: &HashMap<String, f64>,
    ) -> (String, serde_json::Map<String, serde_json::Value>) {
        let date = utc_date(now);
        let mut funding_detail = serde_json::Map::new();
        let symbols: Vec<String> = self.state.positions.keys().cloned().collect();
        // Fetch every held leg's rate concurrently first (LiveExecutor's
        // funding_rate_hourly is a REST get_ticker per symbol), then apply
        // them below sequentially -- that part mutates `self.state` and
        // can't run concurrently. `tick` awaits the whole of this inside
        // book_runtime.rs's select!, so a sequential fetch per leg would
        // hold that arm for the sum of every symbol's request latency,
        // blocking WS handling, decisions, risk processing and SIGTERM on
        // the first tick of each UTC date, same as the paper-funding, lot
        // and price-fallback fetches already made concurrent.
        let mut rate_fetches = tokio::task::JoinSet::new();
        for sym in &symbols {
            // A held leg with no fresh mark must not consume its funding
            // interval at the entry-price fallback below: forcing `rate`
            // to `None` here routes it through the same
            // pending_funding_qty_hours carry a missing *rate* already
            // uses, instead of permanently misstating
            // cum_funding_est_usd from a price the venue never quoted.
            if !prices.contains_key(sym) {
                continue;
            }
            let exec = self.exec.clone();
            let sym = sym.clone();
            rate_fetches.spawn(async move {
                let rate = exec.funding_rate_hourly(&sym).await;
                (sym, rate)
            });
        }
        let mut rates: HashMap<String, Option<f64>> = HashMap::new();
        while let Some(res) = rate_fetches.join_next().await {
            if let Ok((sym, rate)) = res {
                rates.insert(sym, rate);
            }
        }
        for sym in symbols {
            let has_price = prices.contains_key(&sym);
            let rate = rates.get(&sym).copied().flatten();
            let Some(p) = self.state.positions.get(&sym) else {
                continue;
            };
            let since = p.funding_accrued_at.unwrap_or(p.opened_at);
            let hours = ((now - since).max(0)) as f64 / 3600.0;
            let price = prices.get(&sym).copied().unwrap_or(p.avg_price);
            let est = self.accrue_funding(&sym, now, price, rate).unwrap_or(0.0);
            funding_detail.insert(
                sym.clone(),
                json!({ "rate_hourly": rate, "hours": hours, "est_usd": est, "price_available": has_price }),
            );
        }
        // A leg that fully closed while its funding rate was unavailable
        // leaves its carry in `pending_funding_qty_hours` with no position
        // left for the loop above to revisit. Settle it here once a rate
        // is available again, instead of only on a later reopen of the
        // same symbol.
        let orphaned: Vec<String> = self
            .state
            .pending_funding_qty_hours
            .keys()
            .filter(|s| !self.state.positions.contains_key(*s))
            .cloned()
            .collect();
        for sym in orphaned {
            let Some(rate) = self.exec.funding_rate_hourly(&sym).await else {
                continue;
            };
            let Some(price) = prices.get(&sym).copied() else {
                continue;
            };
            let qty_hours = self
                .state
                .pending_funding_qty_hours
                .remove(&sym)
                .unwrap_or(0.0);
            let est = -qty_hours * price * rate;
            self.state.cum_funding_est_usd += est;
            funding_detail.insert(
                sym.clone(),
                json!({ "rate_hourly": rate, "hours": 0.0, "est_usd": est, "settled_pending_only": true }),
            );
        }
        (date, funding_detail)
    }

    /// Write the mark row for `date` and advance `last_mark_date`. Kept
    /// separate from `accrue_daily_funding` so both callers -- `tick` (live,
    /// on a date change) and `daily_mark_now` (replay) -- can run the risk
    /// evaluation and any halt/flatten between accrual and this call.
    /// Returns the append error (after logging it) rather than only
    /// logging: `tick` intentionally ignores it, since leaving
    /// `last_mark_date` unset already gets the live 5s loop to retry the
    /// mark next tick, but `daily_mark_now`'s replay caller has no such
    /// next tick for the same date -- it must fail the run instead of
    /// silently reporting success with that date's mark missing.
    async fn write_daily_mark_row(
        &mut self,
        now: i64,
        prices: &HashMap<String, f64>,
        date: &str,
        funding_detail: serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        let (equity, equity_ready) = self.compute_equity(prices).await;
        if !equity_ready {
            log::warn!(
                "[MARK] {date} deferred: a held leg has no fresh price yet, will retry next tick"
            );
            bail!("{date} mark deferred: a held leg has no fresh price yet");
        }
        // A held leg missing a fresh price (WS not connected yet on the
        // day's first tick, or mid-outage) makes `compute_equity` fall
        // back to the last observation and report not-ready. Writing the
        // mark anyway would durably record that stale fallback and
        // advance `last_mark_date`, so the date is considered complete
        // and this corrupted row is never retried once prices recover.
        // Treat it exactly like an append failure: bail before writing
        // anything, so the caller's existing retry path (unset
        // last_mark_date, retained funding_detail) picks it up next tick.

        let marks: BTreeMap<String, serde_json::Value> = self
            .state
            .positions
            .iter()
            .map(|(s, p)| {
                (
                    s.clone(),
                    json!({
                        "qty": p.qty,
                        "avg_price": p.avg_price,
                        "mark": prices.get(s),
                        "unrealized_usd": prices.get(s).map(|px| p.qty * (px - p.avg_price)),
                    }),
                )
            })
            .collect();
        // Advance `last_mark_date` only once the row is durably written: a
        // transient permissions or filesystem error must not permanently
        // remove this date's mark from the ledger, since every later tick
        // treats `last_mark_date` as proof the mark already happened and
        // will never retry it (see `maybe_daily_mark`).
        let wrote = self.pnl.try_write(
            now,
            "mark",
            self.state.last_decision.as_ref().map(|r| r.key.as_str()),
            json!({
                "date": date,
                "equity_usd": equity,
                "cum_realized_usd": self.state.cum_realized_usd,
                "cum_fees_usd": self.state.cum_fees_usd,
                "cum_funding_est_usd": self.state.cum_funding_est_usd,
                "unrealized_usd": self.state.unrealized_usd(prices),
                "gross_usd": self.state.gross_usd(prices),
                "net_usd": self.state.net_usd(prices),
                "n_positions": self.state.positions.len(),
                "positions": marks,
                "funding": funding_detail,
                "session_halted": self.state.session.halted,
                "paper": self.exec.is_paper(),
            }),
        );
        if let Err(e) = wrote {
            log::warn!("[MARK] {date} pnl append failed, will retry next tick: {e}");
            return Err(e);
        }
        log::info!(
            "[MARK] {date} equity=${equity:.2} realized=${:.2} unreal=${:.2} funding_est=${:.2} n_pos={}",
            self.state.cum_realized_usd,
            self.state.unrealized_usd(prices),
            self.state.cum_funding_est_usd,
            self.state.positions.len()
        );
        self.state.last_mark_date = Some(date.to_string());
        Ok(())
    }

    fn write_status(&mut self, now: i64, prices: &HashMap<String, f64>, equity: f64) {
        let next = self.scheduler.next_after(now);
        if now - self.last_status_write < self.status_interval_secs {
            return;
        }
        self.last_status_write = now;
        let today_start = self.state.daily.start_equity;
        let trades = self.state.trades_closed;
        let doc = StatusDoc {
            dashboard: DashboardStatus {
                ts: now,
                updated_at: status::rfc3339(now),
                id: self.cfg.instance_id.clone(),
                dex: self.cfg.venue.clone(),
                dry_run: self.exec.is_paper(),
                has_position: !self.state.is_flat(),
                position_count: self
                    .state
                    .positions
                    .values()
                    .filter(|p| p.qty != 0.0)
                    .count() as i32,
                positions_ready: self.positions_ready,
                positions: status::dashboard_positions(&self.state, prices),
                // `equity_reference_usd` is the *paper* base. Live equity
                // is the venue's, so subtracting it would publish the
                // whole account size as PnL before a single trade; anchor
                // to the session start the risk rails already use.
                pnl_total: equity
                    - if self.exec.is_paper() {
                        self.cfg.risk.equity_reference_usd
                    } else if self.state.session.start_equity > 0.0 {
                        self.state.session.start_equity
                    } else {
                        equity
                    },
                pnl_today: if today_start > 0.0 {
                    equity - today_start
                } else {
                    0.0
                },
                pnl_source: if self.exec.is_paper() {
                    "paper"
                } else {
                    "venue_equity"
                },
                kill_switch_active: self.risk.kill_switch_engaged(),
                trade_stats: DashboardTradeStats {
                    trades,
                    wins: self.state.trades_won,
                    win_rate: if trades > 0 {
                        self.state.trades_won as f64 / trades as f64
                    } else {
                        0.0
                    },
                    max_dd: self.state.max_drawdown_usd,
                    pnl: self.state.cum_realized_usd,
                },
            },
            book: BookBlock {
                instance_id: self.cfg.instance_id.clone(),
                config_fp: self.config_fp.clone(),
                venue: self.cfg.venue.clone(),
                equity_usd: equity,
                gross_usd: self.state.gross_usd(prices),
                net_usd: self.state.net_usd(prices),
                unrealized_usd: self.state.unrealized_usd(prices),
                cum_realized_usd: self.state.cum_realized_usd,
                cum_fees_usd: self.state.cum_fees_usd,
                cum_funding_est_usd: self.state.cum_funding_est_usd,
                session_halted: self.state.session.halted,
                session_halt_reason: self.state.session.halt_reason.clone(),
                daily_halted: self.state.daily.halted,
                next_decision_key: next.as_ref().map(|d| d.key.clone()),
                next_decision_at: next.as_ref().map(|d| status::rfc3339(d.decision_at)),
                last_decision: self.state.last_decision.clone(),
                signal_status: self.signal_status.clone(),
                pending_residual: matches!(
                    self.state.last_decision.as_ref().map(|r| r.outcome),
                    Some(DecisionOutcome::Partial)
                ),
                positions_source: if self.exec.is_paper() {
                    "paper"
                } else {
                    "venue"
                },
                equity_ready: self.equity_ready,
            },
        };
        self.status.write(&doc);
    }
}

impl OrderIntent {
    pub fn kind_label(&self) -> &'static str {
        match self.kind {
            IntentKind::Close => "close",
            IntentKind::Reduce => "reduce",
            IntentKind::Open => "open",
            IntentKind::Increase => "increase",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::config::{test_config_yaml, ScheduleKind};
    use crate::book::executor::{FillReport, PaperExecutor, VenuePosition};
    use crate::book::schedule::CalendarEntry;
    use crate::book::signal::testutil::signal_json;
    use crate::book::state::Position;
    use async_trait::async_trait;
    use chrono::{DateTime, Duration};
    use serde_json::Value;
    use std::sync::Mutex;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn merge_funding_detail_sums_hours_and_est_usd_across_retries() {
        let mut retained = serde_json::Map::new();
        // First (failed) attempt: 24h accrued for BTC, nothing yet for ETH.
        retained.insert(
            "BTC".to_string(),
            json!({ "rate_hourly": 0.00001, "hours": 24.0, "est_usd": -2.4, "price_available": true }),
        );
        // Second attempt, a few seconds later: its own sliver for BTC,
        // plus ETH's first-ever entry (opened between the two attempts).
        let mut second = serde_json::Map::new();
        second.insert(
            "BTC".to_string(),
            json!({ "rate_hourly": 0.00002, "hours": 0.0014, "est_usd": -0.00028, "price_available": true }),
        );
        second.insert(
            "ETH".to_string(),
            json!({ "rate_hourly": 0.00001, "hours": 0.0014, "est_usd": -0.000056, "price_available": true }),
        );
        merge_funding_detail(&mut retained, second);
        // BTC: the whole day's hours/est_usd, not just the retry's sliver;
        // rate_hourly (and every other field) takes the newer attempt's.
        assert!((retained["BTC"]["hours"].as_f64().unwrap() - 24.0014).abs() < 1e-9);
        assert!((retained["BTC"]["est_usd"].as_f64().unwrap() - (-2.40028)).abs() < 1e-9);
        assert_eq!(retained["BTC"]["rate_hourly"], json!(0.00002));
        // ETH: absent from the retained map, so inserted as-is.
        assert_eq!(retained["ETH"]["hours"], json!(0.0014));
    }

    fn secs(s: &str) -> i64 {
        ts(s).timestamp()
    }

    fn sandbox(cfg: &mut BookConfig, dir: &Path) {
        cfg.paths.state = dir.join("state.json");
        cfg.paths.ledger = dir.join("ledger.jsonl");
        cfg.paths.pnl = dir.join("pnl.jsonl");
        cfg.paths.status = dir.join("status.json");
        cfg.risk.kill_switch_path = dir.join("KILL_SWITCH");
        cfg.risk.risk_ack_path = dir.join("RISK_ACK");
        cfg.signal.path = dir.join("signals").join("unused.json");
        cfg.sizing.max_symbol_weight = 0.5;
    }

    fn write_signal(dir: &Path, key: &str, decision_at: DateTime<Utc>, weights: &[(&str, f64)]) {
        let body = signal_json(
            "test_producer",
            decision_at - Duration::minutes(10),
            decision_at - Duration::minutes(30),
            key,
            weights,
        );
        std::fs::create_dir_all(dir.join("signals")).unwrap();
        std::fs::write(dir.join("signals").join(format!("{key}.json")), body).unwrap();
    }

    fn rows(path: &Path) -> Vec<Value> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    async fn paper_engine(
        cfg: BookConfig,
        dir: &Path,
        calendar: Vec<CalendarEntry>,
    ) -> (BookEngine, Arc<PaperExecutor>) {
        let exec = Arc::new(PaperExecutor::new(0.0, 0.0));
        for (s, d) in [("BTC", 5u32), ("ETH", 4), ("SOL", 2), ("DOT", 1)] {
            exec.set_lot(
                s,
                LotMeta {
                    size_decimals: d,
                    min_order_qty: None,
                },
            )
            .await;
        }
        for (s, p) in [
            ("BTC", 100_000.0),
            ("ETH", 4_000.0),
            ("SOL", 200.0),
            ("DOT", 4.0),
        ] {
            exec.set_price(s, p).await;
            exec.set_funding_rate_hourly(s, 0.0001).await;
        }
        let scheduler = Scheduler::build(&cfg.schedule, calendar).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.join("signals")));
        let engine = BookEngine::new(cfg, scheduler, exec.clone(), signals, status).unwrap();
        (engine, exec)
    }

    #[tokio::test]
    async fn a_restart_restores_signal_telemetry_from_the_persisted_decision() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("DOT", -0.5)]);
        let (mut engine, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Applied);
        assert!(engine.signal_status.starts_with("applied:"));
        assert!(engine.last_signal_generated_at.is_some());
        drop(engine);

        // "Restart": a brand new engine over the same persisted state,
        // before any new decision has ticked. Its process-local telemetry
        // must reflect the already-accepted signal, not the zero-value
        // defaults a fresh process starts with.
        let (restarted, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        assert_eq!(restarted.state.last_decision, Some(rec.clone()));
        assert!(
            restarted.signal_status.starts_with("applied:"),
            "{}",
            restarted.signal_status
        );
        assert_eq!(restarted.last_signal_generated_at, rec.signal_generated_at);
        assert_eq!(
            restarted.last_signal_generated_at,
            Some(d.timestamp() - 600)
        );
    }

    #[tokio::test]
    async fn a_restart_after_a_rejected_or_skipped_decision_reports_no_signal_age() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        // No signal file on disk at all, and the tick lands after the
        // signal window has already closed (default grace 3600s): this
        // must land as Skipped ("window_missed"), never as an accepted
        // decision.
        let (mut engine, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.tick(d.timestamp() + 3601).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Skipped);
        assert_eq!(rec.signal_generated_at, None);
        drop(engine);

        // A restart much later must not fabricate a signal age from the
        // skip's own timestamp -- there was no accepted signal.
        let (restarted, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        assert_eq!(restarted.last_signal_generated_at, None);
    }

    #[tokio::test]
    async fn a_later_skip_does_not_erase_the_earlier_accepted_signals_age_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d1 = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d1, &[("BTC", 0.5), ("DOT", -0.5)]);
        let (mut engine, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.tick(d1.timestamp()).await.unwrap();
        let accepted_rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(accepted_rec.outcome, DecisionOutcome::Applied);
        let accepted_generated_at = engine.last_signal_generated_at;
        assert!(accepted_generated_at.is_some());

        // Next grid key (every_days: 5) has no signal file at all, and the
        // tick lands after its window closes: it lands as Skipped and
        // overwrites `last_decision`, but the book is still running on
        // the signal accepted for the prior key.
        let d2 = ts("2026-09-11T00:30:00Z");
        engine.tick(d2.timestamp() + 3601).await.unwrap();
        let skipped_rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(skipped_rec.outcome, DecisionOutcome::Skipped);
        assert_ne!(skipped_rec.key, accepted_rec.key);
        assert_eq!(
            engine.last_signal_generated_at, accepted_generated_at,
            "the in-process gauge must not reset on a later skip"
        );
        drop(engine);

        let (restarted, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        assert_eq!(
            restarted.last_signal_generated_at, accepted_generated_at,
            "a restart after the skip must still restore the earlier accepted signal's age"
        );
    }

    #[tokio::test]
    async fn a_missing_paper_mark_for_a_held_leg_is_not_fresh_equity() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("DOT", -0.5)]);
        let (mut engine, exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.tick(d.timestamp()).await.unwrap();
        assert_eq!(
            engine.state.last_decision.as_ref().unwrap().outcome,
            DecisionOutcome::Applied
        );
        assert!(engine.equity_ready);

        // BTC's mid goes missing (feed outage) while the leg is still
        // held. `unrealized_usd` would otherwise silently substitute the
        // entry price for it and this must not still read as fresh.
        exec.clear_observations().await;
        engine.tick(d.timestamp() + 5).await.unwrap();
        assert!(
            !engine.equity_ready,
            "a held leg with no fresh mark must not report equity as fresh"
        );
    }

    #[tokio::test]
    async fn a_daily_mark_is_deferred_not_corrupted_when_a_held_leg_has_no_fresh_price() {
        // Writing the mark despite equity_ready == false would durably
        // record stale fallback equity and advance last_mark_date, so
        // the date is considered complete and this corrupted row is
        // never retried once prices recover.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("DOT", -0.5)]);
        let (mut engine, exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        // Isolate daily_mark_now's own behavior: tick() would otherwise
        // also write today's mark while prices are still fresh.
        engine.mark_on_date_change = false;
        engine.tick(d.timestamp()).await.unwrap();
        assert_eq!(
            engine.state.last_decision.as_ref().unwrap().outcome,
            DecisionOutcome::Applied
        );
        assert!(engine.state.last_mark_date.is_none());

        // Feed outage at the moment the daily mark would run.
        exec.clear_observations().await;
        let mark_time = d.timestamp() + 3600;
        assert!(engine.daily_mark_now(mark_time).await.is_err());
        assert!(
            engine.state.last_mark_date.is_none(),
            "a mark based on stale fallback equity must not be considered done"
        );

        // Prices recover: the deferred mark now succeeds.
        exec.set_price("BTC", 100_000.0).await;
        exec.set_price("DOT", 4.0).await;
        engine.daily_mark_now(mark_time + 5).await.unwrap();
        assert!(engine.state.last_mark_date.is_some());
    }

    #[tokio::test]
    async fn a_failed_mark_retains_its_date_across_a_utc_rollover() {
        // If a stuck append doesn't succeed until after midnight, the
        // eventual row must still be labeled (and its funding attributed
        // to) the date whose append actually failed -- not silently
        // relabeled as the newer date, permanently omitting the stuck one.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("DOT", -0.5)]);
        // A directory at pnl's path makes every append fail.
        std::fs::create_dir_all(&cfg.paths.pnl).unwrap();
        let (mut engine, _exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.tick(d.timestamp()).await.unwrap();
        assert_eq!(
            engine.state.pending_mark_date.as_deref(),
            Some("2026-09-06")
        );
        assert!(engine.state.last_mark_date.is_none());

        // Still stuck, now on the next UTC day.
        let day2 = ts("2026-09-07T00:10:00Z").timestamp();
        engine.tick(day2).await.unwrap();
        assert_eq!(
            engine.state.pending_mark_date.as_deref(),
            Some("2026-09-06"),
            "must not relabel the stuck mark as the newer date"
        );
        assert!(engine.state.last_mark_date.is_none());

        // The outage clears.
        std::fs::remove_dir(&cfg.paths.pnl).unwrap();
        engine.tick(day2 + 5).await.unwrap();
        assert_eq!(
            engine.state.last_mark_date.as_deref(),
            Some("2026-09-06"),
            "the stuck date's own row must land, not one for today"
        );
        assert!(engine.state.pending_mark_date.is_none());
        assert!(engine.state.pending_mark_funding_detail.is_empty());
    }

    #[tokio::test]
    async fn overdue_flatten_runs_before_the_next_decision_after_a_restart_gap() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.schedule.kind = ScheduleKind::Calendar;
        cfg.schedule.calendar_path = Some(dir.path().join("cal.json"));
        cfg.signal.require_dollar_neutral = false;
        cfg.sizing.max_net_usd = 1_000.0;
        cfg.schedule.signal_grace_secs = 600;
        let d1 = ts("2026-09-08T06:30:00Z");
        let f1 = ts("2026-09-08T13:30:00Z");
        let d2 = ts("2026-09-09T06:30:00Z");
        let calendar = vec![
            CalendarEntry {
                decision_key: "2026-09-08".into(),
                decision_at: d1,
                flatten_at: Some(f1),
            },
            CalendarEntry {
                decision_key: "2026-09-09".into(),
                decision_at: d2,
                flatten_at: Some(ts("2026-09-09T13:30:00Z")),
            },
        ];
        write_signal(dir.path(), "2026-09-08", d1, &[("SOL", 0.5)]);
        write_signal(dir.path(), "2026-09-09", d2, &[("SOL", -0.5)]);
        let (mut engine, exec) = paper_engine(cfg.clone(), dir.path(), calendar.clone()).await;
        engine.tick(d1.timestamp()).await.unwrap();
        assert!(engine.state.positions["SOL"].qty > 0.0);
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.flatten_at, Some(f1.timestamp()));
        assert!(!rec.flatten_done);
        drop(engine);

        // "Restart" well past the flatten and after the next decision time:
        // the new paper book is seeded from the persisted state, as the
        // binary does at startup.
        let held = exec.positions().await.unwrap();
        let (mut engine, exec2) = paper_engine(cfg.clone(), dir.path(), calendar).await;
        exec2.seed_positions(held).await;
        let ledger_before = rows(&cfg.paths.ledger).len();
        engine.tick(d2.timestamp() + 60).await.unwrap();
        let ledger = rows(&cfg.paths.ledger);
        let new_rows: Vec<&Value> = ledger[ledger_before..].iter().collect();
        let flatten_idx = new_rows
            .iter()
            .position(|r| r["event"] == "flatten")
            .expect("flatten row");
        let decision_idx = new_rows
            .iter()
            .position(|r| r["event"] == "decision" && r["decision_key"] == "2026-09-09")
            .expect("09-09 decision");
        assert!(
            flatten_idx < decision_idx,
            "flatten must precede the next decision"
        );
        assert_eq!(new_rows[flatten_idx]["reason"], "fixed_window");
        assert_eq!(new_rows[flatten_idx]["decision_key"], "2026-09-08");
        // Then the 09-09 signal was applied onto a flat book: short SOL.
        assert_eq!(new_rows[decision_idx]["outcome"], "applied");
        assert!(engine.state.positions["SOL"].qty < 0.0);
        // Funding for the 09-08 leg was accrued at its close, not lost.
        assert!(engine.state.cum_funding_est_usd != 0.0);
    }

    #[tokio::test]
    async fn partial_decision_retries_from_the_persisted_target_without_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("DOT", -0.5)]);
        let (mut engine, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        // Kill switch on: both opens blocked → partial.
        std::fs::write(&cfg.risk.kill_switch_path, "").unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        // Nothing reached the venue, so no execution attempt was spent: a
        // kill switch held for a few ticks must not burn the retry budget.
        assert_eq!(rec.attempts, 0);
        assert_eq!(rec.target_qty.len(), 2);
        assert!(engine.state.is_flat());
        // Ticks while still blocked keep the budget at zero.
        engine.tick(d.timestamp() + 1).await.unwrap();
        engine.tick(d.timestamp() + 2).await.unwrap();
        assert_eq!(engine.state.last_decision.as_ref().unwrap().attempts, 0);
        // Kill switch lifted, producer file gone (and even a different vector
        // for the same key would be ignored): retry uses the persisted target.
        std::fs::remove_file(&cfg.risk.kill_switch_path).unwrap();
        write_signal(dir.path(), "2026-09-06", d, &[("ETH", 0.5), ("SOL", -0.5)]);
        engine.tick(d.timestamp() + 5).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Applied);
        assert_eq!(rec.attempts, 1);
        assert!(engine.state.positions["BTC"].qty > 0.0);
        assert!(engine.state.positions["DOT"].qty < 0.0);
        assert!(!engine.state.positions.contains_key("ETH"));
        let ledger = rows(&cfg.paths.ledger);
        let retry = ledger
            .iter()
            .find(|r| r["event"] == "decision" && r["outcome"] == "applied")
            .expect("applied retry row");
        assert_eq!(retry["retry"], 1);
        assert_eq!(retry["signal_sha256"], rec.signal_sha256.clone().unwrap());
    }

    #[tokio::test]
    async fn a_retry_whose_target_is_already_met_is_marked_applied_not_left_partial_forever() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("DOT", -0.5)]);
        let (mut engine, _) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        std::fs::write(&cfg.risk.kill_switch_path, "").unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        let (btc_target, dot_target) = (rec.target_qty["BTC"], rec.target_qty["DOT"]);

        // Simulate a restart whose reconcile already found the venue at
        // the persisted target (a fill the crashed process sent but never
        // confirmed before its last state persist) -- nothing left for
        // `plan_targets` to do on the next eligible tick.
        engine.state.positions.insert(
            "BTC".to_string(),
            Position {
                qty: btc_target,
                avg_price: 100_000.0,
                opened_at: d.timestamp(),
                funding_accrued_at: None,
                realized_pnl: 0.0,
            },
        );
        engine.state.positions.insert(
            "DOT".to_string(),
            Position {
                qty: dot_target,
                avg_price: 4.0,
                opened_at: d.timestamp(),
                funding_accrued_at: None,
                realized_pnl: 0.0,
            },
        );
        std::fs::remove_file(&cfg.risk.kill_switch_path).unwrap();
        engine.tick(d.timestamp() + 5).await.unwrap();

        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(
            rec.outcome,
            DecisionOutcome::Applied,
            "an already-satisfied residual must not stay Partial forever"
        );
        assert!(engine.signal_status.starts_with("applied:"));
        let applied = rows(&cfg.paths.ledger)
            .into_iter()
            .find(|r| r["event"] == "decision" && r["outcome"] == "applied")
            .expect("applied row");
        assert_eq!(applied["reason"], "residual_already_satisfied");
    }

    #[tokio::test]
    async fn an_opening_is_blocked_when_the_projected_book_would_breach_a_cap() {
        let d = ts("2026-09-06T00:30:00Z");

        // Baseline: a flat book takes the $1000 target without complaint.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.sizing.max_gross_usd = 1_100.0;
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("ETH", -0.5)]);
        let (mut ok_engine, _ok_exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        ok_engine.tick(d.timestamp()).await.unwrap();
        assert!(ok_engine.state.positions["BTC"].qty > 0.0);
        assert!(ok_engine.state.positions["ETH"].qty < 0.0);
        assert!(rows(&cfg.paths.ledger)
            .iter()
            .all(|r| r["event"] != "order_blocked"));

        // Now a $900 SOL leg that no intent closes (a reduction that failed
        // on an earlier tick, or a leg adopted from the venue): the plan's
        // projected end state is $1900 gross, past the $1100 cap, so both
        // openings are refused instead of compounding the breach. Each
        // opening is judged against the intents still to be sent, so one
        // that would leave the book inside every cap is still allowed --
        // the caps are the contract, not the plan's shape.
        let dir2 = tempfile::tempdir().unwrap();
        let mut cfg2 = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg2, dir2.path());
        cfg2.sizing.max_gross_usd = 1_100.0;
        write_signal(dir2.path(), "2026-09-06", d, &[("BTC", 0.5), ("ETH", -0.5)]);
        let (mut engine2, _exec2) = paper_engine(cfg2.clone(), dir2.path(), vec![]).await;
        engine2
            .state
            .apply_fill("SOL", 4.5, 200.0, d.timestamp() - 60);
        engine2.tick(d.timestamp()).await.unwrap();
        let blocked: Vec<Value> = rows(&cfg2.paths.ledger)
            .into_iter()
            .filter(|r| r["event"] == "order_blocked")
            .collect();
        assert_eq!(blocked.len(), 2, "{blocked:?}");
        assert!(
            blocked[0]["reason"]
                .as_str()
                .unwrap()
                .starts_with("cap_gross"),
            "{blocked:?}"
        );
        assert!(engine2.state.positions.get("BTC").is_none());
        assert!(engine2.state.positions.get("ETH").is_none());
    }

    #[tokio::test]
    async fn a_failed_ledger_append_blocks_the_send_instead_of_only_logging_it() {
        let d = ts("2026-09-06T00:30:00Z");
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        // The ledger's parent is actually a plain file, so `append_jsonl`'s
        // `create_dir_all` -- and therefore every `Ledger::write`/
        // `try_write` -- fails on every call, the shape of a full disk or
        // an unwritable ledger path in production.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        cfg.paths.ledger = blocker.join("ledger.jsonl");
        write_signal(dir.path(), "2026-09-06", d, &[("BTC", 0.5), ("ETH", -0.5)]);
        let (mut engine, _exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.tick(d.timestamp()).await.unwrap();

        // The `order_intent` audit row is a precondition for the send, not
        // just an observation of it: with it failing, neither opening must
        // reach the venue, whatever the ledger itself can still report.
        assert!(
            !engine.state.positions.contains_key("BTC"),
            "{:?}",
            engine.state.positions.get("BTC")
        );
        assert!(
            !engine.state.positions.contains_key("ETH"),
            "{:?}",
            engine.state.positions.get("ETH")
        );
    }

    #[tokio::test]
    async fn a_close_that_happened_off_book_is_recovered_into_the_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Persisted book: long 2 SOL @ 100. The venue is flat -- the close
        // filled but the process died before booking it.
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 2.0, 100.0, secs("2026-09-05T23:00:00Z"));
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 130.0)].into(),
            positions: Mutex::new(BTreeMap::new()),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        assert!(engine.state.is_flat());
        // (130 - 100) * 2 booked, and the trade counted as a win.
        assert!((engine.state.cum_realized_usd - 60.0).abs() < 1e-9);
        assert_eq!(engine.state.trades_closed, 1);
        assert_eq!(engine.state.trades_won, 1);
        let recovered = rows(&cfg.paths.ledger)
            .into_iter()
            .find(|r| r["event"] == "recovered_close")
            .expect("recovered_close row");
        assert_eq!(recovered["symbol"], "SOL");
        assert_eq!(recovered["closed_qty"], -2.0);
        assert_eq!(recovered["fill_price_source"], "reconcile_mark");
        assert!(rows(&cfg.paths.pnl)
            .iter()
            .any(|r| r["event"] == "exit" && r["recovered"] == true));
    }

    #[tokio::test]
    async fn a_recovered_partial_reduction_is_not_labeled_an_exit() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Persisted book: long 3 SOL @ 100. The venue shows only 1 SOL left
        // on the same side -- a reduce-only fill landed without this
        // process booking it, but the leg is still open.
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 3.0, 100.0, secs("2026-09-05T23:00:00Z"));
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 130.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 1.0,
                        entry_price: Some(100.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        // The leg is still open, so this must not count as a completed
        // trade even though the recovered reduction realized PnL.
        assert_eq!(engine.state.positions["SOL"].qty, 1.0);
        assert_eq!(engine.state.trades_closed, 0);
        let recovered = rows(&cfg.paths.ledger)
            .into_iter()
            .find(|r| r["event"] == "recovered_close")
            .expect("recovered_close row");
        assert_eq!(recovered["closed_qty"], -2.0);
        let pnl_rows = rows(&cfg.paths.pnl);
        assert!(
            !pnl_rows.iter().any(|r| r["event"] == "exit"),
            "a recovered partial reduction must not be logged as a completed exit: {pnl_rows:?}"
        );
        assert!(pnl_rows
            .iter()
            .any(|r| r["event"] == "partial_reduce" && r["recovered"] == true));
    }

    #[tokio::test]
    async fn funding_pending_across_a_full_close_is_settled_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let (mut engine, _exec) = paper_engine(cfg, dir.path(), vec![]).await;

        let opened_at = secs("2026-09-06T00:00:00Z");
        engine.state.apply_fill("BTC", 2.0, 100_000.0, opened_at);

        // An hour passes with the funding rate unavailable: the 2 BTC * 1h
        // exposure must be frozen rather than silently discarded once the
        // leg closes right after this failed lookup.
        let after_one_hour = opened_at + 3600;
        assert!(engine
            .accrue_funding("BTC", after_one_hour, 100_000.0, None)
            .is_none());
        assert_eq!(
            engine.state.pending_funding_qty_hours.get("BTC").copied(),
            Some(2.0)
        );
        assert_eq!(engine.state.cum_funding_est_usd, 0.0);

        // The leg fully closes while the rate is still unknown -- removing
        // it must not erase the carried notional.
        engine.state.positions.remove("BTC");
        assert_eq!(
            engine.state.pending_funding_qty_hours.get("BTC").copied(),
            Some(2.0)
        );

        // A new leg reopens on the same symbol; once a rate is available,
        // the carried notional is folded into the settlement instead of
        // being lost or restarted from the reopen.
        engine
            .state
            .apply_fill("BTC", 1.0, 100_000.0, after_one_hour);
        let est = engine
            .accrue_funding("BTC", after_one_hour, 100_000.0, Some(0.0001))
            .unwrap();
        assert!((est - (-2.0 * 100_000.0 * 0.0001)).abs() < 1e-9);
        assert!(engine.state.pending_funding_qty_hours.get("BTC").is_none());
        assert!((engine.state.cum_funding_est_usd - est).abs() < 1e-9);
    }

    #[tokio::test]
    async fn a_missing_price_defers_funding_like_a_missing_rate_instead_of_using_entry_price() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let (mut engine, exec) = paper_engine(cfg, dir.path(), vec![]).await;
        let opened_at = secs("2026-09-06T00:00:00Z");
        engine.state.apply_fill("BTC", 2.0, 100_000.0, opened_at);

        // BTC has a funding rate but no fresh price (feed outage): the 2
        // BTC * 1h exposure must be frozen the same way a missing *rate*
        // already is, not consumed at the stale entry price.
        exec.clear_observations().await;
        exec.set_funding_rate_hourly("BTC", 0.0001).await;
        let after_one_hour = opened_at + 3600;
        let prices = exec.prices(&["BTC".to_string()]).await;
        assert!(prices.get("BTC").is_none());
        let (_, funding_detail) = engine.accrue_daily_funding(after_one_hour, &prices).await;

        assert_eq!(
            engine.state.pending_funding_qty_hours.get("BTC").copied(),
            Some(2.0)
        );
        assert_eq!(engine.state.cum_funding_est_usd, 0.0);
        assert_eq!(
            funding_detail["BTC"]["rate_hourly"],
            serde_json::Value::Null
        );
        assert_eq!(funding_detail["BTC"]["price_available"], false);
    }

    #[tokio::test]
    async fn funding_pending_after_a_full_close_settles_without_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let (mut engine, _exec) = paper_engine(cfg, dir.path(), vec![]).await;

        let opened_at = secs("2026-09-06T00:00:00Z");
        engine.state.apply_fill("BTC", 2.0, 100_000.0, opened_at);
        let after_one_hour = opened_at + 3600;
        assert!(engine
            .accrue_funding("BTC", after_one_hour, 100_000.0, None)
            .is_none());
        assert_eq!(
            engine.state.pending_funding_qty_hours.get("BTC").copied(),
            Some(2.0)
        );

        // The leg closes for good this time -- there is no reopen for
        // `write_daily_mark`'s position-keyed loop to revisit.
        engine.state.positions.remove("BTC");

        // The next daily mark runs with the rate feed recovered
        // (`paper_engine`'s default 0.0001/hr): the orphaned carry must be
        // settled here, not left stranded until a reopen that never comes.
        engine.daily_mark_now(after_one_hour + 3600).await.unwrap();
        assert!(
            !engine.state.pending_funding_qty_hours.contains_key("BTC"),
            "orphaned pending funding must be settled once the rate recovers"
        );
        let expected = -2.0 * 100_000.0 * 0.0001;
        assert!(
            (engine.state.cum_funding_est_usd - expected).abs() < 1e-9,
            "cum_funding_est_usd={}, expected={expected}",
            engine.state.cum_funding_est_usd
        );
    }

    #[tokio::test]
    async fn a_pending_funding_carry_outside_the_universe_is_still_priced_and_settled() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let (mut engine, exec) = paper_engine(cfg, dir.path(), vec![]).await;

        // XRP is outside the configured universe (BTC/ETH/SOL/DOT) and has
        // no open position: an adopted leg that accrued an unrateable
        // funding carry and then fully closed. Neither of `tracked_symbols`'
        // other two sources -- the universe list or `state.positions` --
        // would include it, so its price must come from the carry itself.
        exec.set_price("XRP", 2.0).await;
        exec.set_funding_rate_hourly("XRP", 0.0001).await;
        engine
            .state
            .pending_funding_qty_hours
            .insert("XRP".to_string(), 100.0);
        assert!(!engine.state.positions.contains_key("XRP"));

        engine
            .daily_mark_now(secs("2026-09-06T00:00:00Z"))
            .await
            .unwrap();
        assert!(
            engine.state.pending_funding_qty_hours.get("XRP").is_none(),
            "an out-of-universe orphaned carry must still be priced and settled"
        );
        let expected = -100.0 * 2.0 * 0.0001;
        assert!(
            (engine.state.cum_funding_est_usd - expected).abs() < 1e-9,
            "cum_funding_est_usd={}, expected={expected}",
            engine.state.cum_funding_est_usd
        );
    }

    #[tokio::test]
    async fn a_halt_is_not_cleared_by_ack_without_a_confirmed_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let (mut engine, _exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;

        engine.state.session.halted = true;
        engine.state.session.halt_reason = Some("test".to_string());
        engine.state.session.halted_at = Some(secs("2026-09-05T00:00:00Z"));
        std::fs::write(&cfg.risk.risk_ack_path, "").unwrap();

        // A failed live position read: the persisted book happens to be
        // flat (nothing open), but that is not the same as a confirmed
        // reconcile -- residual or externally opened venue exposure could
        // still be sitting there unseen.
        engine.positions_ready = false;

        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();

        assert!(
            engine.state.session.halted,
            "a halt must not clear on an ack until positions are confirmed reconciled"
        );
        assert!(
            cfg.risk.risk_ack_path.exists(),
            "the ack must be left in place, not consumed, while unconfirmed"
        );
    }

    #[tokio::test]
    async fn a_pre_send_abort_costs_no_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("SOL", 0.5), ("DOT", -0.5)]);
        // DOT's order never reaches the venue (no send-time price, or the
        // drift guard); SOL fills.
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0), ("DOT".to_string(), 4.0)].into(),
            positions: Mutex::new(BTreeMap::new()),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: Some("DOT".to_string()),
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        // Nothing reached the venue, so no attempt was spent: DOT aborted
        // before sending, and opening SOL alone would have left a $500
        // one-sided book, past the $150 net cap -- so the cap re-check
        // blocked it rather than compounding the half-applied plan.
        assert_eq!(rec.attempts, 0);
        assert!(engine.state.is_flat());
        let ledger = rows(&cfg.paths.ledger);
        let err = ledger
            .iter()
            .find(|r| r["event"] == "order_error")
            .expect("order_error row");
        assert_eq!(err["intent"]["symbol"], "DOT");
        assert_eq!(err["pre_send"], true);
        let blocked = ledger
            .iter()
            .find(|r| r["event"] == "order_blocked")
            .expect("order_blocked row");
        assert_eq!(blocked["intent"]["symbol"], "SOL");
        assert!(blocked["reason"].as_str().unwrap().starts_with("cap_net"));
    }

    #[tokio::test]
    async fn a_decision_whose_every_intent_aborts_pre_send_spends_no_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        cfg.signal.require_dollar_neutral = false;
        cfg.sizing.max_net_usd = 1_000.0;
        let d = ts("2026-09-06T00:30:00Z");
        write_signal(dir.path(), "2026-09-06", d, &[("SOL", 0.5)]);
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0)].into(),
            positions: Mutex::new(BTreeMap::new()),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: Some("SOL".to_string()),
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        assert_eq!(rec.attempts, 0, "nothing reached the venue");
        assert!(engine.state.is_flat());
    }

    #[tokio::test]
    async fn a_venue_basis_correction_is_adopted_even_at_an_unchanged_quantity() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // The book booked its fill at an estimated mid of 100; the venue's
        // own average entry is 112 for the same quantity.
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 2.0, 100.0, secs("2026-09-05T23:00:00Z"));
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 130.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.0,
                        entry_price: Some(112.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        let p = &engine.state.positions["SOL"];
        assert_eq!(p.qty, 2.0);
        assert_eq!(p.avg_price, 112.0, "the venue basis is authoritative");
        // Nothing closed, so nothing was realized by the correction.
        assert_eq!(engine.state.cum_realized_usd, 0.0);
        assert!(rows(&cfg.paths.ledger)
            .iter()
            .any(|r| r["event"] == "adopt" && r["symbol"] == "SOL"));
    }

    #[tokio::test]
    async fn a_venue_basis_correction_preserves_the_legs_realized_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Book: opened long 4 @ 100, then reduced by 2 @ 120 -- a real
        // partial reduction that already realized $40 onto this leg's
        // history, leaving qty 2 with that $40 still carried on it. Same
        // quantity + a different venue basis is ambiguous by itself (an
        // external close/reopen, OR an earlier increase that was booked
        // at a mid-price estimate and is only now being corrected to the
        // venue's authoritative entry -- the latter is the SAME leg, not
        // a new one), so this correction must not reset opened_at or
        // realized_pnl: doing so would erase real history whenever it is
        // actually the latter case.
        let t0 = secs("2026-09-05T20:00:00Z");
        let t1 = secs("2026-09-05T23:00:00Z");
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 4.0, 100.0, t0);
        state.apply_fill("SOL", -2.0, 120.0, t1);
        assert_eq!(state.positions["SOL"].realized_pnl, 40.0);
        assert_eq!(state.positions["SOL"].opened_at, t0);
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 130.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.0,
                        entry_price: Some(112.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        let now = secs("2026-09-06T00:00:00Z");
        engine.tick(now).await.unwrap();
        let p = &engine.state.positions["SOL"];
        assert_eq!(p.qty, 2.0);
        assert_eq!(p.avg_price, 112.0, "the venue basis is authoritative");
        assert_eq!(
            p.realized_pnl, 40.0,
            "a genuine earlier realization must survive a basis correction"
        );
        assert_eq!(
            p.opened_at, t0,
            "leg history is preserved, not reset, on this ambiguous signal"
        );
        assert!(rows(&cfg.paths.ledger)
            .iter()
            .any(|r| r["event"] == "basis_correction" && r["symbol"] == "SOL"));
    }

    #[tokio::test]
    async fn a_recovered_increase_folds_the_added_quantity_in_at_the_mark() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Book: long 1 SOL @ 100. Venue: long 2, no entry price, mark 130.
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 1.0, 100.0, secs("2026-09-05T23:00:00Z"));
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 130.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.0,
                        entry_price: None,
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        let p = &engine.state.positions["SOL"];
        assert_eq!(p.qty, 2.0);
        // (100 * 1 + 130 * 1) / 2 -- not the old 100, which would claim a
        // $60 unrealized gain instead of the real $30.
        assert!(
            (p.avg_price - 115.0).abs() < 1e-9,
            "avg_price = {}",
            p.avg_price
        );
        let px: HashMap<String, f64> = [("SOL".to_string(), 130.0)].into();
        assert!((engine.state.unrealized_usd(&px) - 30.0).abs() < 1e-9);
        assert_eq!(engine.state.cum_realized_usd, 0.0);
        // An increase is not an external close-and-reopen.
        assert!(!rows(&cfg.paths.ledger)
            .iter()
            .any(|r| r["event"] == "basis_correction"));
    }

    #[tokio::test]
    async fn a_recovered_reduction_keeps_the_remaining_legs_basis() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Book: long 2 SOL @ 100. Venue: long 1, no entry price reported.
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 2.0, 100.0, secs("2026-09-05T23:00:00Z"));
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 130.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 1.0,
                        entry_price: None,
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        let p = &engine.state.positions["SOL"];
        assert_eq!(p.qty, 1.0);
        // The closed half realized (130-100)*1; the remaining half keeps
        // its $100 basis, so its $30 unrealized gain survives.
        assert!((engine.state.cum_realized_usd - 30.0).abs() < 1e-9);
        assert_eq!(p.avg_price, 100.0);
        let px: HashMap<String, f64> = [("SOL".to_string(), 130.0)].into();
        assert!((engine.state.unrealized_usd(&px) - 30.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn an_adopted_out_of_universe_leg_is_priced_for_the_same_tick_flatten() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // XRP is not in the config universe, so the tick's price request
        // (built from universe + persisted book) does not include it; only
        // the reconcile-time lookup can price it.
        let venue = Arc::new(MockVenue {
            prices: [("XRP".to_string(), 2.0)].into(),
            positions: Mutex::new(
                [(
                    "XRP".to_string(),
                    VenuePosition {
                        qty: 100.0,
                        entry_price: Some(2.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine =
            BookEngine::new(cfg.clone(), scheduler, venue.clone(), signals, status).unwrap();
        // Already halted: the tick must adopt XRP *and* flatten it now.
        engine.state.session.halted = true;
        engine.state.session.start_equity = 1000.0;
        engine.state.session.start_at = 1;
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        let ledger = rows(&cfg.paths.ledger);
        assert!(ledger
            .iter()
            .any(|r| r["event"] == "adopt" && r["symbol"] == "XRP"));
        let flatten = ledger
            .iter()
            .find(|r| r["event"] == "flatten")
            .expect("flatten row");
        assert_eq!(flatten["flat"], true, "{flatten:?}");
        assert!(engine.state.is_flat());
        assert!(venue.positions.lock().unwrap().is_empty());
    }

    /// Minimal non-paper venue: canned positions and prices, full fills.
    struct MockVenue {
        prices: HashMap<String, f64>,
        positions: Mutex<BTreeMap<String, VenuePosition>>,
        equity_ok: std::sync::atomic::AtomicBool,
        /// When set, `execute` asserts that `state.json` at this path already
        /// carries the decision record (hash + targets) for the order.
        state_path: Option<std::path::PathBuf>,
        /// Symbol whose *opening* orders abort before reaching the venue
        /// (the shape of a missing send-time price or a drift-guard
        /// rejection). Reduce-only orders for the same symbol still go
        /// through, so a flip's close can be made to succeed while its
        /// paired opening aborts.
        abort_symbol: Option<String>,
        /// Symbol whose orders fill only half, so the leg stays residual
        /// after the venue has seen it.
        half_fill_symbol: Option<String>,
    }

    #[async_trait]
    impl Executor for MockVenue {
        fn is_paper(&self) -> bool {
            false
        }
        async fn prices(&self, symbols: &[String]) -> HashMap<String, f64> {
            symbols
                .iter()
                .filter_map(|s| self.prices.get(s).map(|p| (s.clone(), *p)))
                .collect()
        }
        async fn lot_meta(&self, _symbol: &str) -> Result<LotMeta> {
            Ok(LotMeta {
                size_decimals: 2,
                min_order_qty: None,
            })
        }
        async fn positions(&self) -> Result<BTreeMap<String, VenuePosition>> {
            Ok(self.positions.lock().unwrap().clone())
        }
        async fn equity(&self) -> Result<Option<f64>> {
            if self.equity_ok.load(std::sync::atomic::Ordering::Relaxed) {
                Ok(Some(1000.0))
            } else {
                Err(anyhow::anyhow!("get_balance: timeout"))
            }
        }
        async fn funding_rate_hourly(&self, _symbol: &str) -> Option<f64> {
            Some(0.001)
        }
        async fn execute(&self, intent: &OrderIntent) -> Result<FillReport> {
            if !intent.reduce_only && self.abort_symbol.as_deref() == Some(intent.symbol.as_str()) {
                return Err(anyhow::anyhow!(PreSendAbort(format!(
                    "live: no price for {}",
                    intent.symbol
                ))));
            }
            if let Some(p) = &self.state_path {
                let persisted = BookState::load_or_new(p, "test-book").unwrap();
                let rec = persisted
                    .last_decision
                    .expect("decision record persisted before the first order");
                assert!(rec.signal_sha256.is_some());
                assert!(rec.target_qty.contains_key(&intent.symbol));
            }
            let qty = if self.half_fill_symbol.as_deref() == Some(intent.symbol.as_str()) {
                intent.qty / 2.0
            } else {
                intent.qty
            };
            let signed = match intent.side {
                Side::Buy => qty,
                Side::Sell => -qty,
            };
            let mut p = self.positions.lock().unwrap();
            let e = p.entry(intent.symbol.clone()).or_default();
            e.qty += signed;
            if e.entry_price.is_none() {
                e.entry_price = Some(intent.reference_price);
            }
            let after = e.qty;
            if after == 0.0 {
                p.remove(&intent.symbol);
            }
            Ok(FillReport {
                requested_qty: intent.qty,
                filled_qty: qty,
                fill_price: intent.reference_price,
                fill_price_source: "mid_estimate",
                fee_usd: Some(0.0),
                order_id: Some("o".into()),
                venue_error: None,
                latency_ms: 0,
                position_after: Some(after),
            })
        }
    }

    #[tokio::test]
    async fn live_reconcile_adopts_venue_quantity_and_entry_price() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Book remembers 1 SOL @ 100; the venue says 3 SOL @ 150 (external add).
        let mut state = BookState::new(&cfg.instance_id);
        // Opened one hour before the reconcile tick, funding never accrued.
        state.apply_fill("SOL", 1.0, 100.0, secs("2026-09-05T23:00:00Z"));
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 160.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 3.0,
                        entry_price: Some(150.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine =
            BookEngine::new(cfg.clone(), scheduler, venue.clone(), signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        let p = &engine.state.positions["SOL"];
        assert_eq!(p.qty, 3.0);
        assert_eq!(p.avg_price, 150.0);
        // The old 1-lot leg was settled for its hour at the mid before the
        // adoption, and the adopted leg accrues from the reconcile instant.
        assert_eq!(p.funding_accrued_at, Some(secs("2026-09-06T00:00:00Z")));
        assert!((engine.state.cum_funding_est_usd - (-1.0 * 160.0 * 0.001 * 1.0)).abs() < 1e-9);
        let adopt = rows(&cfg.paths.ledger)
            .into_iter()
            .find(|r| r["event"] == "adopt")
            .unwrap();
        assert_eq!(adopt["venue_qty"], 3.0);
        assert_eq!(adopt["book_qty"], 1.0);
        assert_eq!(adopt["entry_price"], 150.0);
        // Venue flat → book leg removed.
        venue.positions.lock().unwrap().clear();
        engine.tick(secs("2026-09-06T00:00:05Z")).await.unwrap();
        assert!(engine.state.is_flat());
    }

    #[tokio::test]
    async fn a_recovered_close_waits_for_a_real_mark_instead_of_booking_zero() {
        // Book holds 1 SOL @ 100, the venue is flat: the leg closed without
        // this process booking it. Closing it against the stored basis
        // would realize exactly zero and destroy the evidence, so with no
        // current price the booking has to wait.
        let mk = |px: Option<f64>| {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
            sandbox(&mut cfg, dir.path());
            cfg.dry_run = false;
            let mut state = BookState::new(&cfg.instance_id);
            state.apply_fill("SOL", 1.0, 100.0, secs("2026-09-05T23:00:00Z"));
            state.persist(&cfg.paths.state).unwrap();
            let venue = Arc::new(MockVenue {
                prices: px
                    .map(|p| [("SOL".to_string(), p)].into())
                    .unwrap_or_default(),
                positions: Mutex::new(BTreeMap::new()),
                equity_ok: std::sync::atomic::AtomicBool::new(true),
                state_path: None,
                abort_symbol: None,
                half_fill_symbol: None,
            });
            let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
            let status = StatusWriter::new(cfg.paths.status.clone(), None);
            let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
            let engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
            (dir, cfg, engine)
        };

        let (_dir, cfg, mut engine) = mk(None);
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        assert_eq!(
            engine.state.positions["SOL"].qty, 1.0,
            "the leg stays on the book until it can be booked at a real mark"
        );
        assert_eq!(engine.state.trades_closed, 0);
        assert_eq!(engine.state.cum_realized_usd, 0.0);
        assert!(!rows(&cfg.paths.ledger)
            .iter()
            .any(|r| r["event"] == "recovered_close"));

        // The same tick with a mark books the close for what it really was.
        let (_dir2, cfg2, mut engine2) = mk(Some(90.0));
        engine2.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        assert!(engine2.state.is_flat());
        let rec = rows(&cfg2.paths.ledger)
            .into_iter()
            .find(|r| r["event"] == "recovered_close")
            .expect("recovered_close row");
        assert_eq!(rec["fill_price"], 90.0);
        assert!((rec["realized_usd"].as_f64().unwrap() - -10.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn a_target_below_the_venue_minimum_is_not_a_pending_residual() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        let d = ts("2026-09-06T00:30:00Z");
        // SOL @ 200: 0.1 * $1000 = 0.5 SOL, under the venue's 2.0 minimum.
        // The planner skips it on every retry, so counting it as residual
        // would re-run the decision each tick and leave it Partial. The
        // legs are small enough that the remaining DOT side still clears
        // the net cap once SOL is dropped.
        write_signal(dir.path(), "2026-09-06", d, &[("SOL", 0.1), ("DOT", -0.1)]);
        let (mut engine, exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        exec.set_lot(
            "SOL",
            LotMeta {
                size_decimals: 2,
                min_order_qty: Some(2.0),
            },
        )
        .await;
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Applied);
        assert!(engine.state.positions["DOT"].qty < 0.0);
        assert!(!engine.state.positions.contains_key("SOL"));
        let summary = rows(&cfg.paths.ledger)
            .into_iter()
            .find(|r| r["event"] == "rebalance_summary")
            .unwrap();
        assert_eq!(summary["residual_qty"], json!({}));
        // And the next tick does not re-run the decision.
        let before = rows(&cfg.paths.ledger).len();
        engine.tick(d.timestamp() + 5).await.unwrap();
        assert!(!rows(&cfg.paths.ledger)[before..]
            .iter()
            .any(|r| r["event"] == "rebalance_summary"));
    }

    #[tokio::test]
    async fn a_filled_reduction_does_not_spend_the_attempt_of_an_unsent_opening() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        cfg.execution.max_attempts = 1;
        let d = ts("2026-09-06T00:30:00Z");
        // Book is long $500 of SOL; the target halves it and opens a DOT
        // short. The reduction reaches the venue and only half fills, so it
        // stays residual too; the opening aborts before it is sent, so the
        // venue never saw that leg at all. The shared counter would call
        // the whole decision one attempt and, at max_attempts: 1, never
        // retry DOT.
        write_signal(
            dir.path(),
            "2026-09-06",
            d,
            &[("SOL", 0.25), ("DOT", -0.25)],
        );
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 2.5, 200.0, d.timestamp() - 3600);
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0), ("DOT".to_string(), 4.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.5,
                        entry_price: Some(200.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: Some("DOT".to_string()),
            half_fill_symbol: Some("SOL".to_string()),
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        assert!(rec.target_qty.contains_key("DOT"));
        // Half of the 1.25 reduction filled.
        assert_eq!(engine.state.positions["SOL"].qty, 1.875);
        // The leg is still open (1.875 > 0): the partial fill must not be
        // logged as a completed exit, only as a partial reduction.
        assert!(
            !rows(&cfg.paths.pnl)
                .iter()
                .any(|r| r["event"] == "exit" && r["symbol"] == "SOL"),
            "a partial reduce that leaves the leg open is not an exit"
        );
        assert!(
            rows(&cfg.paths.pnl)
                .iter()
                .any(|r| r["event"] == "partial_reduce" && r["symbol"] == "SOL"),
            "the partial reduction is still logged, under its own label"
        );
        assert_eq!(rec.attempts_for("SOL"), 1, "the venue saw the reduction");
        assert_eq!(
            rec.attempts_for("DOT"),
            0,
            "the opening never reached the venue, so it spent no attempt"
        );
        // With max_attempts: 1 the DOT budget has to be intact for the
        // residual to be retried at all, while SOL is done.
        let before = rows(&cfg.paths.ledger).len();
        engine.tick(d.timestamp() + 5).await.unwrap();
        let new_rows = &rows(&cfg.paths.ledger)[before..];
        assert!(
            new_rows
                .iter()
                .any(|r| r["event"] == "order_intent" && r["intent"]["symbol"] == "DOT"),
            "the unsent opening is retried"
        );
        assert!(
            !new_rows
                .iter()
                .any(|r| r["event"] == "order_intent" && r["intent"]["symbol"] == "SOL"),
            "the leg that used its attempt is not retried"
        );
    }

    #[tokio::test]
    async fn a_flip_close_does_not_spend_the_attempt_of_its_own_aborted_opening() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        cfg.execution.max_attempts = 1;
        cfg.signal.require_dollar_neutral = false;
        let d = ts("2026-09-06T00:30:00Z");
        // Book is long $500 of SOL; the signal flips it to a $100 short.
        // The close (reduce-only, full $500) reaches the venue and fills,
        // but the opening leg of the same flip aborts before it is sent (no
        // send-time price). A per-symbol counter that does not distinguish
        // the flip's two stages would treat the close's send as spending
        // SOL's sole attempt and never retry the still-unsent short.
        write_signal(dir.path(), "2026-09-06", d, &[("SOL", -0.1)]);
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 2.5, 200.0, d.timestamp() - 3600);
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.5,
                        entry_price: Some(200.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: Some("SOL".to_string()),
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        assert!(engine.state.is_flat(), "the close fully filled");
        assert_eq!(
            rec.attempts_for("SOL"),
            0,
            "the close's send must not spend the still-unsent opening's attempt"
        );
        // With max_attempts: 1 the opening still has to have budget left to
        // retry at all.
        let before = rows(&cfg.paths.ledger).len();
        engine.tick(d.timestamp() + 5).await.unwrap();
        let new_rows = &rows(&cfg.paths.ledger)[before..];
        assert!(
            new_rows.iter().any(|r| r["event"] == "order_intent"
                && r["intent"]["symbol"] == "SOL"
                && r["intent"]["reduce_only"] == false),
            "the flip's opening is retried"
        );
    }

    #[tokio::test]
    async fn a_flip_open_is_blocked_until_its_close_confirms_full() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        cfg.execution.max_attempts = 1;
        cfg.signal.require_dollar_neutral = false;
        let d = ts("2026-09-06T00:30:00Z");
        // Book is long 2.5 SOL; the signal flips it to a 0.1 short. The
        // venue only ever half-fills SOL orders, so the reduce-only close
        // leaves 1.25 SOL still long instead of reaching flat. Without
        // gating the paired opening on the close's own confirmed-full
        // result, the opening would still be sent and merely net against
        // the still-long position on the venue instead of establishing
        // the short target.
        write_signal(dir.path(), "2026-09-06", d, &[("SOL", -0.1)]);
        let mut state = BookState::new(&cfg.instance_id);
        state.apply_fill("SOL", 2.5, 200.0, d.timestamp() - 3600);
        state.persist(&cfg.paths.state).unwrap();
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0)].into(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.5,
                        entry_price: Some(200.0),
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: Some("SOL".to_string()),
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        let rec = engine.state.last_decision.clone().unwrap();
        assert_eq!(rec.outcome, DecisionOutcome::Partial);
        assert!(
            engine.state.signed_qty().get("SOL").copied().unwrap_or(0.0) > 0.0,
            "the half-filled close leaves the book still long, never netted short by an unconfirmed open"
        );
        let sent_open = rows(&cfg.paths.ledger).into_iter().any(|r| {
            r["event"] == "order_intent"
                && r["intent"]["symbol"] == "SOL"
                && r["intent"]["reduce_only"] == false
        });
        assert!(
            !sent_open,
            "the opening leg must not be sent while its close is still incomplete"
        );
        let blocked = rows(&cfg.paths.ledger).into_iter().any(|r| {
            r["event"] == "order_blocked"
                && r["intent"]["symbol"] == "SOL"
                && r["reason"] == "flip_close_incomplete"
        });
        assert!(
            blocked,
            "the opening leg is explicitly blocked, not silently dropped"
        );
    }

    #[tokio::test]
    async fn a_post_mark_session_halt_refreshes_status_not_just_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        // Isolate the session rail: the daily one must not fire first and
        // muddy which halt this test is about.
        cfg.risk.max_daily_loss_bps = 1_000_000.0;
        let (mut engine, exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.status_interval_secs = 0;
        let t0 = secs("2026-09-06T00:00:00Z");
        // A long SOL position, anchored as the whole session/day: $1000 of
        // notional against the $1000 paper base. The paper venue keeps its
        // own mirrored book, so the reduce-only flatten fills against that.
        engine.state.apply_fill("SOL", 5.0, 200.0, t0);
        exec.seed_positions(
            [(
                "SOL".to_string(),
                VenuePosition {
                    qty: 5.0,
                    entry_price: Some(200.0),
                },
            )]
            .into(),
        )
        .await;
        engine.state.session.start_equity = 1000.0;
        engine.state.session.start_at = t0;
        engine.state.daily.date = crate::book::risk::utc_date(t0);
        engine.state.daily.start_equity = 1000.0;
        engine.state.peak_equity = 1000.0;
        // A live tick "yesterday" writes status.json while the book is
        // still open and unhalted -- the baseline this bug leaves stale.
        engine.write_status(t0, &[("SOL".to_string(), 200.0)].into(), 1000.0);
        let before: Value =
            serde_json::from_str(&std::fs::read_to_string(&cfg.paths.status).unwrap()).unwrap();
        assert_eq!(before["has_position"], true);
        assert_eq!(before["position_count"], 1);
        // A punishing funding rate over 100h drives session equity down
        // more than the 5% session limit ($50 of $1000) -- enough to
        // session-halt and flatten, entirely inside `daily_mark_now`'s own
        // post-mark risk re-check (finding: this path only persisted
        // state.json, leaving status.json reporting the pre-halt book).
        exec.set_funding_rate_hourly("SOL", 0.001).await;
        let t1 = t0 + 100 * 3600;
        engine.daily_mark_now(t1).await.unwrap();
        assert!(engine.state.session.halted, "session should be halted");
        assert!(engine.state.is_flat(), "session halt flattens the book");
        let after: Value =
            serde_json::from_str(&std::fs::read_to_string(&cfg.paths.status).unwrap()).unwrap();
        assert_eq!(
            after["book"]["session_halted"], true,
            "status.json must reflect the halt, not the pre-halt tick: {after}"
        );
        assert_eq!(
            after["has_position"], false,
            "status.json must reflect the flatten, not the pre-halt position: {after}"
        );
        assert_eq!(after["position_count"], 0);
    }

    #[tokio::test]
    async fn a_post_mark_session_halt_mark_row_reflects_the_flattened_book() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.risk.max_daily_loss_bps = 1_000_000.0;
        let (mut engine, exec) = paper_engine(cfg.clone(), dir.path(), vec![]).await;
        engine.status_interval_secs = 0;
        let t0 = secs("2026-09-06T00:00:00Z");
        engine.state.apply_fill("SOL", 5.0, 200.0, t0);
        exec.seed_positions(
            [(
                "SOL".to_string(),
                VenuePosition {
                    qty: 5.0,
                    entry_price: Some(200.0),
                },
            )]
            .into(),
        )
        .await;
        engine.state.session.start_equity = 1000.0;
        engine.state.session.start_at = t0;
        engine.state.daily.date = crate::book::risk::utc_date(t0);
        engine.state.daily.start_equity = 1000.0;
        engine.state.peak_equity = 1000.0;

        // Same punishing-funding session-halt setup as the status.json
        // test above, but this checks the *mark row itself*
        // (pnl.jsonl), which `daily_mark_now` used to write BEFORE its
        // own post-mark risk check could flatten the book: on the final
        // day of a replay nothing later corrects that mark.
        exec.set_funding_rate_hourly("SOL", 0.001).await;
        let t1 = t0 + 100 * 3600;
        engine.daily_mark_now(t1).await.unwrap();
        assert!(engine.state.session.halted);
        assert!(engine.state.is_flat());

        let mark = rows(&cfg.paths.pnl)
            .into_iter()
            .find(|r| r["event"] == "mark")
            .expect("mark row");
        assert_eq!(
            mark["session_halted"], true,
            "the mark must reflect the halt, not the pre-halt tick: {mark}"
        );
        assert_eq!(
            mark["n_positions"], 0,
            "the mark must reflect the flatten, not the pre-halt position: {mark}"
        );
    }

    #[tokio::test]
    async fn live_pnl_total_is_measured_against_the_session_not_the_paper_base() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // The paper base is $100 while the live account holds $1000: the
        // difference is the account, not profit.
        cfg.risk.equity_reference_usd = 100.0;
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0)].into(),
            positions: Mutex::new(BTreeMap::new()),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue, signals, status).unwrap();
        engine.status_interval_secs = 0;
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        let doc: Value =
            serde_json::from_str(&std::fs::read_to_string(&cfg.paths.status).unwrap()).unwrap();
        assert_eq!(doc["pnl_source"], "venue_equity");
        assert!(
            doc["pnl_total"].as_f64().unwrap().abs() < 1e-9,
            "nothing has been traded: {}",
            doc["pnl_total"]
        );
    }

    #[tokio::test]
    async fn live_opens_are_blocked_while_venue_equity_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        let d = ts("2026-09-06T00:30:00Z");
        // SOL/DOT: MockVenue reports 2 size decimals for every symbol, which
        // would round a $500 BTC leg to zero.
        write_signal(dir.path(), "2026-09-06", d, &[("SOL", 0.5), ("DOT", -0.5)]);
        let venue = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 200.0), ("DOT".to_string(), 4.0)].into(),
            positions: Mutex::new(BTreeMap::new()),
            equity_ok: std::sync::atomic::AtomicBool::new(false),
            state_path: Some(cfg.paths.state.clone()),
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine =
            BookEngine::new(cfg.clone(), scheduler, venue.clone(), signals, status).unwrap();
        engine.tick(d.timestamp()).await.unwrap();
        assert!(engine.state.is_flat());
        // No anchors while equity is stale.
        assert_eq!(engine.state.session.start_at, 0);
        assert_eq!(engine.state.session.start_equity, 0.0);
        assert!(engine.state.daily.date.is_empty());
        let blocked: Vec<Value> = rows(&cfg.paths.ledger)
            .into_iter()
            .filter(|r| r["event"] == "order_blocked")
            .collect();
        assert_eq!(blocked.len(), 2);
        assert_eq!(blocked[0]["reason"], "equity_unavailable");
        assert_eq!(
            engine.state.last_decision.as_ref().unwrap().outcome,
            DecisionOutcome::Partial
        );
        // Equity back → the retry from the persisted target fills.
        venue
            .equity_ok
            .store(true, std::sync::atomic::Ordering::Relaxed);
        engine.tick(d.timestamp() + 5).await.unwrap();
        assert_eq!(
            engine.state.last_decision.as_ref().unwrap().outcome,
            DecisionOutcome::Applied
        );
        assert!(!engine.state.is_flat());
        // Anchored on the first fresh reading.
        assert_eq!(engine.state.session.start_equity, 1000.0);
        assert_eq!(engine.state.daily.date, "2026-09-06");
    }

    #[tokio::test]
    async fn adoption_waits_for_a_price_basis_and_never_books_from_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        sandbox(&mut cfg, dir.path());
        cfg.dry_run = false;
        // Venue holds 2 SOL, reports no entry price, and no mid has arrived.
        let venue = Arc::new(MockVenue {
            prices: HashMap::new(),
            positions: Mutex::new(
                [(
                    "SOL".to_string(),
                    VenuePosition {
                        qty: 2.0,
                        entry_price: None,
                    },
                )]
                .into(),
            ),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine =
            BookEngine::new(cfg.clone(), scheduler, venue.clone(), signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:00Z")).await.unwrap();
        assert!(
            engine.state.positions.is_empty(),
            "no adoption without a basis"
        );
        assert!(!engine.positions_ready);
        // A mid arrives → adopted at the mid.
        let venue2 = Arc::new(MockVenue {
            prices: [("SOL".to_string(), 210.0)].into(),
            positions: Mutex::new(venue.positions.lock().unwrap().clone()),
            equity_ok: std::sync::atomic::AtomicBool::new(true),
            state_path: None,
            abort_symbol: None,
            half_fill_symbol: None,
        });
        let scheduler = Scheduler::build(&cfg.schedule, vec![]).unwrap();
        let status = StatusWriter::new(cfg.paths.status.clone(), None);
        let signals = Box::new(DirSignalSource::new(dir.path().join("signals")));
        let mut engine = BookEngine::new(cfg.clone(), scheduler, venue2, signals, status).unwrap();
        engine.tick(secs("2026-09-06T00:00:05Z")).await.unwrap();
        assert!(engine.positions_ready);
        assert_eq!(engine.state.positions["SOL"].qty, 2.0);
        assert_eq!(engine.state.positions["SOL"].avg_price, 210.0);
    }
}
