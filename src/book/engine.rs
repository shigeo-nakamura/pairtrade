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
use super::executor::{Executor, FillReport};
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
    last_flatten_attempt: i64,
    /// Whether the last venue reconcile succeeded (live) — trading is
    /// suppressed while it is false.
    positions_ready: bool,
    config_fp: String,
}

/// How one decision's execution ended.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecSummary {
    pub intents: usize,
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
            last_flatten_attempt: 0,
            positions_ready: true,
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
        let prices = self.exec.prices(&symbols).await;

        if !self.exec.is_paper() {
            self.positions_ready = self.reconcile_with_venue(now, &prices).await;
        }

        let equity = self.compute_equity(&prices).await;

        if let Some(ev) = self.risk.maybe_clear_halt(&mut self.state, now, equity) {
            log::warn!("[RISK] session halt cleared by RISK_ACK: {ev:?}");
            self.ledger
                .write(now, "halt_cleared", None, json!({ "risk": ev }));
        }
        let events = self.risk.evaluate(&mut self.state, now, equity);
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
            self.process_decision(now, &prices).await;
            self.process_flatten(now, &prices).await;
        }

        self.maybe_daily_mark(now, &prices).await;

        let equity = self.compute_equity(&prices).await;
        self.write_status(now, &prices, equity);
        self.state
            .persist(&self.cfg.paths.state)
            .with_context(|| format!("persist {}", self.cfg.paths.state.display()))?;
        Ok(())
    }

    /// Live only: the venue position is the truth. Returns false when it
    /// could not be read (trading is suppressed for this tick).
    async fn reconcile_with_venue(&mut self, now: i64, prices: &HashMap<String, f64>) -> bool {
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
        for sym in symbols {
            let venue_qty = venue.get(&sym).map(|p| p.qty).unwrap_or(0.0);
            let book_qty = self.state.positions.get(&sym).map(|p| p.qty).unwrap_or(0.0);
            let tol = self
                .lots
                .get(&sym)
                .map(|l| 10f64.powi(-(l.size_decimals as i32)) * 0.5)
                .unwrap_or(1e-9);
            if (venue_qty - book_qty).abs() <= tol {
                continue;
            }
            let entry = venue
                .get(&sym)
                .and_then(|p| p.entry_price)
                .or_else(|| prices.get(&sym).copied())
                .or_else(|| self.state.positions.get(&sym).map(|p| p.avg_price))
                .unwrap_or(0.0);
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
                    });
                p.qty = venue_qty;
                if p.avg_price <= 0.0 {
                    p.avg_price = entry;
                }
            }
        }
        true
    }

    async fn compute_equity(&self, prices: &HashMap<String, f64>) -> f64 {
        if self.exec.is_paper() {
            return self.cfg.risk.equity_reference_usd + self.state.cum_realized_usd
                - self.state.cum_fees_usd
                + self.state.cum_funding_est_usd
                + self.state.unrealized_usd(prices);
        }
        match self.exec.equity().await {
            Ok(Some(e)) if e.is_finite() && e > 0.0 => e,
            other => {
                log::warn!("[EQUITY] venue equity unavailable ({other:?}); using last observation");
                self.state
                    .last_equity
                    .map(|(_, e)| e)
                    .unwrap_or(self.cfg.risk.equity_reference_usd)
            }
        }
    }

    async fn process_decision(&mut self, now: i64, prices: &HashMap<String, f64>) {
        let Some(d) = self.scheduler.current(now) else {
            self.signal_status = "no_decision_yet".to_string();
            return;
        };
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
                    self.try_apply(now, &d, prices, r.attempts).await;
                }
                _ => {}
            },
        }
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
        let summary = self.execute_plan(now, &d.key, &plan, prices).await;
        let outcome = if summary.residual.is_empty() {
            DecisionOutcome::Applied
        } else {
            DecisionOutcome::Partial
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
            filled: 0,
            partial: 0,
            unfilled: 0,
            blocked: 0,
            errors: 0,
            residual: BTreeMap::new(),
        };
        for intent in &plan.intents {
            if !intent.reduce_only && !self.risk.opens_allowed(&self.state) {
                let reason = self.risk.block_reason(&self.state).unwrap_or("blocked");
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
            match self.exec.execute(intent).await {
                Ok(fill) => {
                    let result = self.book_fill(now, key, intent, &fill);
                    match result {
                        "filled" => s.filled += 1,
                        "partial" => s.partial += 1,
                        _ => s.unfilled += 1,
                    }
                }
                Err(e) => {
                    log::error!(
                        "[ORDER] {} {} {} failed: {e}",
                        intent.side,
                        intent.symbol,
                        intent.qty
                    );
                    self.ledger.write(
                        now,
                        "order_error",
                        Some(key),
                        json!({ "intent": intent, "error": e.to_string() }),
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
        self.state.cum_fees_usd += fill.fee_usd;
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
            fill.fee_usd
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
                "realized_usd": realized,
                "paper": self.exec.is_paper(),
            }),
        );
        if realized != 0.0 || matches!(intent.kind, IntentKind::Close | IntentKind::Reduce) {
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
        let Some(d) = self.scheduler.current(now) else {
            return;
        };
        let Some(flatten_at) = d.flatten_at else {
            return;
        };
        if now < flatten_at {
            return;
        }
        let done = self
            .state
            .last_decision
            .as_ref()
            .map(|r| r.key == d.key && r.flatten_done)
            .unwrap_or(false);
        if done {
            return;
        }
        if !self.state.is_flat() && now - self.last_flatten_attempt >= 30 {
            self.flatten_now(now, prices, "fixed_window").await;
        }
        if self.state.is_flat() {
            match self.state.last_decision.as_mut() {
                Some(r) if r.key == d.key => r.flatten_done = true,
                _ => {
                    self.state.last_decision = Some(DecisionRecord {
                        key: d.key.clone(),
                        outcome: DecisionOutcome::Skipped,
                        at: now,
                        signal_sha256: None,
                        reject_reason: Some("flatten_only".to_string()),
                        attempts: 0,
                        flatten_done: true,
                        target_qty: BTreeMap::new(),
                    })
                }
            }
        }
    }

    async fn maybe_daily_mark(&mut self, now: i64, prices: &HashMap<String, f64>) {
        let date = utc_date(now);
        if self.state.last_mark_date.as_deref() == Some(date.as_str()) {
            return;
        }
        // Funding accrual per leg (estimate from the current rate).
        let mut funding_detail = serde_json::Map::new();
        let symbols: Vec<String> = self.state.positions.keys().cloned().collect();
        for sym in symbols {
            let rate = self.exec.funding_rate_hourly(&sym).await;
            let Some(p) = self.state.positions.get_mut(&sym) else {
                continue;
            };
            let since = p.funding_accrued_at.unwrap_or(p.opened_at);
            let hours = ((now - since).max(0)) as f64 / 3600.0;
            let price = prices.get(&sym).copied().unwrap_or(p.avg_price);
            let est = match rate {
                Some(r) => -p.qty * price * r * hours,
                None => 0.0,
            };
            p.funding_accrued_at = Some(now);
            self.state.cum_funding_est_usd += est;
            funding_detail.insert(
                sym.clone(),
                json!({ "rate_hourly": rate, "hours": hours, "est_usd": est }),
            );
        }
        let equity = self.compute_equity(prices).await;
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
