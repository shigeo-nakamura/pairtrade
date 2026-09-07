//! Tick orchestration: reconcile → equity → risk → decision → flatten →
//! daily mark → status. Driven by the wall clock in `book_runtime` and by
//! a synthetic clock in [`super::replay`]. See `docs/book-runtime.md`.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
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
    /// Raw body for `decision_key`; `Ok(None)` when nothing is there yet.
    fn read(&self, decision_key: &str) -> std::io::Result<Option<String>>;
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
    fn read(&self, _decision_key: &str) -> std::io::Result<Option<String>> {
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
    fn read(&self, decision_key: &str) -> std::io::Result<Option<String>> {
        match std::fs::read_to_string(self.dir.join(format!("{decision_key}.json"))) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
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

/// How one decision's execution ended.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecSummary {
    pub intents: usize,
    /// Intents actually handed to the executor (a retry that sent nothing
    /// because every opening was administratively blocked must not spend
    /// an execution attempt).
    pub sent: usize,
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
        status::CONFIG_INFO
            .with_label_values(&[&cfg.instance_id, &config_fp])
            .set(1);
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
            last_signal_generated_at: None,
            signal_status: "none".to_string(),
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

        let (equity, equity_ready) = self.compute_equity(&prices).await;
        self.equity_ready = equity_ready;

        // An ack re-anchors both loss windows at `equity`, so it is only
        // consumed while that number is a fresh venue value; a stale one
        // would hand the session an unintended cushion once reads recover.
        if equity_ready {
            if let Some(ev) = self.risk.maybe_clear_halt(&mut self.state, now, equity) {
                log::warn!("[RISK] session halt cleared by RISK_ACK: {ev:?}");
                self.ledger
                    .write(now, "halt_cleared", None, json!({ "risk": ev }));
            }
        } else if self.state.session.halted && self.cfg.risk.risk_ack_path.exists() {
            log::warn!("[RISK] RISK_ACK present but venue equity is unavailable; ack deferred");
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
                    status::DECISION_TOTAL
                        .with_label_values(&[&self.cfg.instance_id, "halted"])
                        .inc();
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

        if self.mark_on_date_change {
            self.maybe_daily_mark(now, &prices).await;
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
            // Same signed quantity, different venue basis: the leg was
            // closed and reopened outside this process. Position data
            // alone cannot say at what price it closed -- inventing one
            // would fabricate a trade, and staying silent would lose it --
            // so the correction is recorded explicitly for reconciliation
            // against the venue's own trade history.
            if closed_qty == 0.0 && venue_qty != 0.0 && venue_basis_stale {
                let old_basis = book_basis.unwrap_or(0.0);
                log::warn!(
                    "[ADOPT] {sym}: same quantity {venue_qty} but the venue basis moved {old_basis} -> {entry};                      any realized PnL from that external close/reopen is NOT in this book's accounting"
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
                let mark = prices
                    .get(&sym)
                    .copied()
                    .filter(|p| p.is_finite() && *p > 0.0)
                    .or(if entry > 0.0 { Some(entry) } else { None })
                    .or_else(|| self.state.positions.get(&sym).map(|p| p.avg_price))
                    .unwrap_or(0.0);
                if mark > 0.0 {
                    let realized = self.state.apply_fill(&sym, closed_qty, mark, now);
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
                    self.pnl.write(now, "exit", None, row);
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
                if let Some(v) = venue_entry {
                    p.avg_price = v;
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
            return (
                self.cfg.risk.equity_reference_usd + self.state.cum_realized_usd
                    - self.state.cum_fees_usd
                    + self.state.cum_funding_est_usd
                    + self.state.unrealized_usd(prices),
                true,
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
                    if now <= d.window_end && r.attempts < self.cfg.execution.max_attempts =>
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
        let attempts = rec.attempts + 1;
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
        // A retry that sent nothing (every remaining intent was blocked by
        // the risk rails or a cap) has not used the venue at all: keep the
        // attempt budget for when the block clears inside the window.
        let attempts = if summary.sent == 0 {
            rec.attempts
        } else {
            attempts
        };
        if let Some(r) = self.state.last_decision.as_mut() {
            r.outcome = outcome;
            r.attempts = attempts;
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
        status::DECISION_TOTAL
            .with_label_values(&[&self.cfg.instance_id, label])
            .inc();
    }

    fn finish_skipped(&mut self, now: i64, d: &Decision, reason: &str) {
        log::warn!("[DECISION] key={} skipped: {reason}", d.key);
        let prev = self.state.last_decision.take().filter(|r| r.key == d.key);
        self.state.last_decision = Some(DecisionRecord {
            key: d.key.clone(),
            outcome: DecisionOutcome::Skipped,
            at: now,
            signal_sha256: None,
            reject_reason: Some(reason.to_string()),
            attempts: prev.as_ref().map(|r| r.attempts).unwrap_or(0),
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
        status::DECISION_TOTAL
            .with_label_values(&[&self.cfg.instance_id, "skipped"])
            .inc();
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
            reject_reason: Some(detail.clone()),
            attempts,
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
            status::DECISION_TOTAL
                .with_label_values(&[&self.cfg.instance_id, "rejected"])
                .inc();
        }
    }

    async fn try_apply(
        &mut self,
        now: i64,
        d: &Decision,
        prices: &HashMap<String, f64>,
        attempts: u32,
    ) {
        let body = match self.signals.read(&d.key) {
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
            reject_reason: None,
            attempts: prior_attempts,
            flatten_at: d.flatten_at,
            flatten_done: false,
            target_qty: plan.target_qty.clone(),
        });
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
        // Same rule as the retry path: a first application whose intents
        // were all blocked has not spent an execution attempt.
        let attempts = if summary.sent == 0 {
            prior_attempts
        } else {
            attempts
        };
        self.last_signal_generated_at = Some(sig.generated_at.timestamp());
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
            reject_reason: None,
            attempts,
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
        status::DECISION_TOTAL
            .with_label_values(&[
                &self.cfg.instance_id,
                match outcome {
                    DecisionOutcome::Applied => "applied",
                    _ => "partial",
                },
            ])
            .inc();
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
                status::ORDER_TOTAL
                    .with_label_values(&[&self.cfg.instance_id, "blocked"])
                    .inc();
                s.blocked += 1;
                continue;
            }

            self.ledger
                .write(now, "order_intent", Some(key), json!({ "intent": intent }));
            let rate = if self.state.positions.contains_key(&intent.symbol) {
                self.exec.funding_rate_hourly(&intent.symbol).await
            } else {
                None
            };
            match self.exec.execute(intent).await {
                Ok(fill) => {
                    s.sent += 1;
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
                    status::ORDER_TOTAL
                        .with_label_values(&[&self.cfg.instance_id, "error"])
                        .inc();
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
            let tradeable = closing
                || (diff_usd >= self.cfg.sizing.rebalance_deadband_usd
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
        let realized = self
            .state
            .apply_fill(&intent.symbol, signed, fill.fill_price, now);
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
        // still open.
        if fill.filled_qty > 0.0
            && (realized != 0.0 || matches!(intent.kind, IntentKind::Close | IntentKind::Reduce))
        {
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
        }
        status::ORDER_TOTAL
            .with_label_values(&[&self.cfg.instance_id, result])
            .inc();
        result
    }

    async fn flatten_now(&mut self, now: i64, prices: &HashMap<String, f64>, reason: &str) {
        self.last_flatten_attempt = now;
        let current = self.state.signed_qty();
        if current.is_empty() {
            return;
        }
        let symbols: Vec<String> = current.keys().cloned().collect();
        let lots = self.lots_for(&symbols).await;
        let plan = match rebalance::plan_flatten(&current, prices, &lots, &self.cfg.sizing) {
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
        status::DECISION_TOTAL
            .with_label_values(&[&self.cfg.instance_id, "flatten"])
            .inc();
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
        let est = rate_hourly
            .map(|r| -p.qty * price * r * hours)
            .unwrap_or(0.0);
        p.funding_accrued_at = Some(now);
        self.state.cum_funding_est_usd += est;
        Some(est)
    }

    async fn maybe_daily_mark(&mut self, now: i64, prices: &HashMap<String, f64>) {
        let date = utc_date(now);
        if self.state.last_mark_date.as_deref() == Some(date.as_str()) {
            return;
        }
        self.write_daily_mark(now, prices).await;
    }

    /// Write the daily mark for `now` unconditionally (replay: at the last
    /// tick of a bar date, after every decision/flatten of that date).
    pub async fn daily_mark_now(&mut self, now: i64) {
        let symbols = self.tracked_symbols();
        let prices = self.exec.prices(&symbols).await;
        self.write_daily_mark(now, &prices).await;
        if let Err(e) = self.state.persist(&self.cfg.paths.state) {
            log::error!("[MARK] persist failed: {e}");
        }
    }

    async fn write_daily_mark(&mut self, now: i64, prices: &HashMap<String, f64>) {
        let date = utc_date(now);
        // Funding accrual per leg (estimate from the current rate).
        let mut funding_detail = serde_json::Map::new();
        let symbols: Vec<String> = self.state.positions.keys().cloned().collect();
        for sym in symbols {
            let rate = self.exec.funding_rate_hourly(&sym).await;
            let Some(p) = self.state.positions.get(&sym) else {
                continue;
            };
            let since = p.funding_accrued_at.unwrap_or(p.opened_at);
            let hours = ((now - since).max(0)) as f64 / 3600.0;
            let price = prices.get(&sym).copied().unwrap_or(p.avg_price);
            let est = self.accrue_funding(&sym, now, price, rate).unwrap_or(0.0);
            funding_detail.insert(
                sym.clone(),
                json!({ "rate_hourly": rate, "hours": hours, "est_usd": est }),
            );
        }
        let (equity, _) = self.compute_equity(prices).await;
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
        self.pnl.write(
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
        log::info!(
            "[MARK] {date} equity=${equity:.2} realized=${:.2} unreal=${:.2} funding_est=${:.2} n_pos={}",
            self.state.cum_realized_usd,
            self.state.unrealized_usd(prices),
            self.state.cum_funding_est_usd,
            self.state.positions.len()
        );
        self.state.last_mark_date = Some(date);
    }

    fn write_status(&mut self, now: i64, prices: &HashMap<String, f64>, equity: f64) {
        let next = self.scheduler.next_after(now);
        let signal_age = self.last_signal_generated_at.map(|g| now - g);
        status::record_gauges(
            &self.cfg.instance_id,
            &self.state,
            prices,
            equity,
            self.risk.kill_switch_engaged(),
            signal_age,
            next.as_ref().map(|d| d.decision_at),
        );
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
                pnl_total: equity - self.cfg.risk.equity_reference_usd,
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
    use async_trait::async_trait;
    use chrono::{DateTime, Duration};
    use serde_json::Value;
    use std::sync::Mutex;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
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
        /// Symbol whose orders abort before reaching the venue (the shape
        /// of a missing send-time price or a drift-guard rejection).
        abort_symbol: Option<String>,
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
            if self.abort_symbol.as_deref() == Some(intent.symbol.as_str()) {
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
            let signed = match intent.side {
                Side::Buy => intent.qty,
                Side::Sell => -intent.qty,
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
                filled_qty: intent.qty,
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
