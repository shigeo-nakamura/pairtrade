//! Pluggable quote gate (bot-strategy#1120, design
//! `~/bot/logs/studies/2026-10-04-quote-gate-1120/DESIGN.md`).
//!
//! Sits between `plan_quotes` and `quote_action` in the tick. Per side it
//! decides `Quote` or `Pull` from a model file evaluated on features computed
//! strictly before the decision time τ (`features.rs`), or falls back with a
//! named reason when it cannot (no model, warmup, stale / recovering
//! reference feed, a feature it cannot form). The gate can only REMOVE a
//! quote the baseline planned (`GateOutput::apply`): a side the baseline
//! suppressed, or that has no price, is returned untouched, so the gate's
//! output is always a subset of the baseline and there is no path to "quote
//! more" (design D1).
//!
//! Modes (`ARCUS_VOL_GATE_MODE`):
//! - `off` (default): nothing is computed or logged, the tick is as before;
//! - `shadow`: everything is computed and logged (`gate_YYYYMMDD.jsonl` in
//!   the state dir, a `gate` stamp on each fill row, a `gate` block in
//!   status.json) and NOTHING is applied: `apply` is the identity;
//! - `enforce`: pulls are applied. Needs `ARCUS_VOL_GATE_ENFORCE_CONFIRM`
//!   (config.rs). A `GATE_OFF` sentinel in the state dir demotes a running
//!   enforce to shadow from the next tick (no restart).
//!
//! Widen is not generated in this version (design D2: only Quote/Pull were
//! validated).

pub mod features;
pub mod feed;
pub mod model;
/// Study tape reader: golden / no-lookahead tests (and a future replay bin).
#[cfg(test)]
pub mod xtape;

use crate::logic::{PricePlan, QSide, SidePlan};
use features::{FeatureEngine, FeatureVector, LastRx};
use model::GateModel;
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Order / cancel latency the labels were built with (10-01 calibration,
/// CUTOFF §2): a fill at `t` was last avoidable by the gate cycle at
/// `τ <= t - this`.
pub const CANCEL_LATENCY_MS: u64 = 300;
/// Gate cycles kept for fill stamping (`stamp_for_fill`) and the 5-minute
/// pull rate: 2 Hz × 5 min, with slack.
const RING_CAP: usize = 1_200;
const PULL_RATE_WINDOW_MS: u64 = 300_000;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateMode {
    Off,
    Shadow,
    Enforce,
}

impl GateMode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(GateMode::Off),
            "shadow" => Ok(GateMode::Shadow),
            "enforce" => Ok(GateMode::Enforce),
            other => Err(format!("gate mode `{other}` is not off|shadow|enforce")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            GateMode::Off => "off",
            GateMode::Shadow => "shadow",
            GateMode::Enforce => "enforce",
        }
    }

    pub fn on(self) -> bool {
        self != GateMode::Off
    }
}

/// What a Fallback does (design §6.2). Shadow never acts on it either way;
/// the chosen action is still recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPolicy {
    /// The current behaviour: quote as the baseline planned.
    Baseline,
    Pull,
    /// Baseline while the run of consecutive fallbacks is shorter than
    /// `secs`, then pull (a short WS blip is baseline, a long outage pulls).
    BaselineThenPull {
        secs: u64,
    },
}

impl FallbackPolicy {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        match s.to_ascii_lowercase().as_str() {
            "baseline" => return Ok(FallbackPolicy::Baseline),
            "pull" => return Ok(FallbackPolicy::Pull),
            _ => {}
        }
        if let Some(rest) = s.strip_prefix("baseline_then_pull:") {
            let secs: u64 = rest
                .trim()
                .parse()
                .map_err(|_| format!("gate fallback `{s}`: seconds must be an integer"))?;
            if secs == 0 {
                return Err("gate fallback baseline_then_pull needs > 0 seconds".into());
            }
            return Ok(FallbackPolicy::BaselineThenPull { secs });
        }
        Err(format!(
            "gate fallback `{s}` is not baseline|pull|baseline_then_pull:<secs>"
        ))
    }

    pub fn label(self) -> String {
        match self {
            FallbackPolicy::Baseline => "baseline".into(),
            FallbackPolicy::Pull => "pull".into(),
            FallbackPolicy::BaselineThenPull { secs } => format!("baseline_then_pull:{secs}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateAction {
    Quote,
    Pull,
}

impl GateAction {
    pub fn as_str(self) -> &'static str {
        match self {
            GateAction::Quote => "quote",
            GateAction::Pull => "pull",
        }
    }
}

/// Why the model was not evaluated this cycle (design D6: never zero-fill).
#[derive(Debug, Clone, PartialEq)]
pub enum FallbackReason {
    ModelMissing,
    Warmup {
        secs_left: u64,
    },
    /// `age_ms` None = no record at all yet.
    RefStale {
        feed: &'static str,
        age_ms: Option<u64>,
    },
    RefGapCooldown {
        feed: &'static str,
        secs_left: u64,
    },
    /// A feature could not be formed (no history, NaN).
    FeatureGap {
        name: &'static str,
    },
    /// A feed the model needs is not up: it never connected, or it reported
    /// Down and has not come back. Its stream is silently stale (own trades
    /// can legitimately be quiet, so age alone cannot tell), so no scoring
    /// until its `Up` (Codex rounds 1-2 on pairtrade#390).
    FeedDown {
        feed: &'static str,
    },
}

impl FallbackReason {
    /// Stable key for counters.
    pub fn key(&self) -> &'static str {
        match self {
            FallbackReason::ModelMissing => "model_missing",
            FallbackReason::Warmup { .. } => "warmup",
            FallbackReason::RefStale { .. } => "ref_stale",
            FallbackReason::RefGapCooldown { .. } => "ref_gap_cooldown",
            FallbackReason::FeatureGap { .. } => "feature_gap",
            FallbackReason::FeedDown { .. } => "feed_down",
        }
    }

    /// Row label, e.g. `ref_stale:binance:12040`.
    pub fn label(&self) -> String {
        match self {
            FallbackReason::ModelMissing => "model_missing".into(),
            FallbackReason::Warmup { secs_left } => format!("warmup:{secs_left}"),
            FallbackReason::RefStale { feed, age_ms } => match age_ms {
                Some(a) => format!("ref_stale:{feed}:{a}"),
                None => format!("ref_stale:{feed}:none"),
            },
            FallbackReason::RefGapCooldown { feed, secs_left } => {
                format!("ref_gap_cooldown:{feed}:{secs_left}")
            }
            FallbackReason::FeatureGap { name } => format!("feature_gap:{name}"),
            FallbackReason::FeedDown { feed } => format!("feed_down:{feed}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GateDecision {
    /// The model ran; `pred_bps` is the side-signed predicted 30 s markout.
    Scored { action: GateAction, pred_bps: f32 },
    /// The model did not run; `action` is what the fallback policy chose.
    Fallback {
        reason: FallbackReason,
        action: GateAction,
    },
}

impl GateDecision {
    pub fn action(&self) -> GateAction {
        match self {
            GateDecision::Scored { action, .. } | GateDecision::Fallback { action, .. } => *action,
        }
    }

    fn json(&self) -> serde_json::Value {
        match self {
            GateDecision::Scored { action, pred_bps } => json!({
                "decision": "scored", "action": action.as_str(),
                "pred_bps": round4(f64::from(*pred_bps)),
            }),
            GateDecision::Fallback { reason, action } => json!({
                "decision": "fallback", "reason": reason.label(), "action": action.as_str(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SideOutcome {
    pub decision: GateDecision,
    /// The baseline's planned size (None = the baseline itself suppressed
    /// the side), for the readout's "what would have been pulled".
    pub baseline_qty: Option<Decimal>,
    /// This side reduces the current inventory (design Q7 variant).
    pub reducing_side: bool,
}

/// Receive-time ages at τ, for the row and status.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Ages {
    pub own_book_ms: Option<u64>,
    pub own_tape_ms: Option<u64>,
    pub binance_ms: Option<u64>,
    pub hyperliquid_ms: Option<u64>,
}

impl Ages {
    fn json(&self) -> serde_json::Value {
        json!({"own_book": self.own_book_ms, "own_tape": self.own_tape_ms,
               "binance": self.binance_ms, "hyperliquid": self.hyperliquid_ms})
    }
}

/// One gate cycle.
#[derive(Debug, Clone, PartialEq)]
pub struct GateOutput {
    /// τ: the tick's decision clock.
    pub ts_ms: u64,
    /// The EFFECTIVE mode this cycle (enforce demoted to shadow by the
    /// sentinel shows as `Shadow` with `forced_shadow`).
    pub mode: GateMode,
    pub forced_shadow: bool,
    pub model_sha: Option<String>,
    pub features: Option<FeatureVector>,
    pub ages: Ages,
    pub bid: SideOutcome,
    pub ask: SideOutcome,
}

impl GateOutput {
    pub fn applied(&self) -> bool {
        self.mode == GateMode::Enforce
    }

    pub fn side(&self, side: QSide) -> &SideOutcome {
        match side {
            QSide::Bid => &self.bid,
            QSide::Ask => &self.ask,
        }
    }

    /// The gate ⊆ baseline guarantee (design D1 / §3 contract 2), by
    /// construction: anything but Enforce is the identity; a side the
    /// baseline suppressed (`qty` None) or left unpriced is returned as it
    /// is (the gate never adds a quote or a price); Enforce + Pull clears
    /// the size, which `quote_action` turns into the existing cancel path;
    /// Enforce + Quote is the identity.
    pub fn apply(&self, plan: SidePlan) -> SidePlan {
        if self.mode != GateMode::Enforce {
            return plan;
        }
        if plan.qty.is_none() || !matches!(plan.price, PricePlan::At { .. }) {
            return plan;
        }
        match self.side(plan.side).decision.action() {
            GateAction::Quote => plan,
            GateAction::Pull => SidePlan { qty: None, ..plan },
        }
    }

    /// The summary embedded under `gate` on a fill row (design §7.2).
    pub fn stamp(&self, side: QSide) -> serde_json::Value {
        let mut v = json!({
            "ts_ms": self.ts_ms, "mode": self.mode.as_str(),
            "model_sha": self.model_sha.as_deref().map(|s| &s[..12.min(s.len())]),
            "applied": self.applied(),
        });
        if let Some(o) = v.as_object_mut() {
            if let Some(d) = self.side(side).decision.json().as_object() {
                for (k, x) in d {
                    o.insert(k.clone(), x.clone());
                }
            }
        }
        v
    }

    /// The `gate.jsonl` row (design §7.1).
    pub fn row(&self, market: &str, with_features: bool) -> serde_json::Value {
        let side = |o: &SideOutcome| {
            let mut v = o.decision.json();
            if let Some(m) = v.as_object_mut() {
                m.insert(
                    "baseline_qty".into(),
                    json!(o.baseline_qty.map(|q| q.to_string())),
                );
                m.insert("reducing_side".into(), json!(o.reducing_side));
            }
            v
        };
        let features = match (&self.features, with_features) {
            (Some(f), true) => {
                let names = features::DIR_NAMES.iter().chain(features::ND_NAMES.iter());
                json!(names
                    .zip(f.values.iter())
                    .map(|(n, v)| (n.to_string(), json!(round4(*v))))
                    .collect::<serde_json::Map<String, serde_json::Value>>())
            }
            _ => serde_json::Value::Null,
        };
        json!({
            "kind": "gate", "ts_ms": self.ts_ms, "market": market,
            "mode": self.mode.as_str(), "forced_shadow": self.forced_shadow,
            "model_sha": self.model_sha.as_deref().map(|s| &s[..12.min(s.len())]),
            "ref_age_ms": self.ages.json(),
            "features": features,
            "bid": side(&self.bid), "ask": side(&self.ask),
            "applied": self.applied(),
        })
    }
}

fn round4(x: f64) -> f64 {
    (x * 1e4).round() / 1e4
}

/// Reference-feed health, written by the feed tasks.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FeedState {
    pub up: bool,
    /// Time of the last connected / gap event (cooldown input).
    pub last_event_ms: Option<u64>,
}

impl FeedState {
    fn on(&mut self, now_ms: u64, up: bool) {
        self.up = up;
        self.last_event_ms = Some(now_ms);
    }
}

/// The engine plus feed health, shared with the feed tasks under one lock.
pub struct RefState {
    pub engine: FeatureEngine,
    pub binance: FeedState,
    pub hyperliquid: FeedState,
    pub own_tape: FeedState,
    /// Warmup clock: process start, restarted at every reference feed
    /// connect (first or re-): the grid state and history start over then.
    pub warmup_since_ms: u64,
}

impl RefState {
    pub fn new(origin_ms: u64) -> Self {
        RefState {
            engine: FeatureEngine::new(origin_ms),
            binance: FeedState::default(),
            hyperliquid: FeedState::default(),
            own_tape: FeedState::default(),
            warmup_since_ms: origin_ms,
        }
    }

    /// A reference feed event, stamped `rx_ms` by the feed task.
    pub fn on_ref(&mut self, feed: feed::RefFeed, rx_ms: u64, ev: feed::RefEvent) {
        use feed::{RefEvent, RefFeed};
        let state = match feed {
            RefFeed::Binance => &mut self.binance,
            RefFeed::Hyperliquid => &mut self.hyperliquid,
        };
        match ev {
            RefEvent::Up => {
                state.on(rx_ms, true);
                // Every connect — the first one (possibly long after start,
                // Codex round 3 on pairtrade#390) or a recovery (the gap
                // forward-filled stale values into the grid state, round 2)
                // — restarts the warmup and the grid, and drops the history
                // gathered before it. At startup the feeds connect within
                // seconds, so this costs nothing there.
                self.warmup_since_ms = rx_ms;
                self.engine.reset_grid(rx_ms);
            }
            RefEvent::Down(_) => state.on(rx_ms, false),
            RefEvent::BnL1 { bid, ask, bsz, asz } => {
                self.engine.on_bn_l1(rx_ms, bid, ask, bsz, asz)
            }
            RefEvent::BnTrade {
                maker_is_buyer,
                px,
                qty,
            } => self.engine.on_bn_trade(rx_ms, maker_is_buyer, px, qty),
            RefEvent::HlL1 { bid, ask } => self.engine.on_hl_l1(rx_ms, bid, ask),
            RefEvent::HlTrade { buy, px, qty } => self.engine.on_hl_trade(rx_ms, buy, px, qty),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GateConfig {
    pub mode: GateMode,
    pub fallback: FallbackPolicy,
    pub ref_stale_ms: u64,
    pub event_cooldown_ms: u64,
    pub warmup_ms: u64,
    pub log_features: bool,
}

/// Appends gate rows to `gate_YYYYMMDD.jsonl` (UTC day of the row), no
/// fsync (design D7). A failed write is warned once a minute, never fatal.
pub struct GateLog {
    dir: PathBuf,
    open: Option<(String, std::fs::File)>,
    last_warn_ms: u64,
}

impl GateLog {
    pub fn new(dir: &Path) -> Self {
        GateLog {
            dir: dir.to_path_buf(),
            open: None,
            last_warn_ms: 0,
        }
    }

    pub fn path_for(dir: &Path, day: &str) -> PathBuf {
        dir.join(format!("gate_{day}.jsonl"))
    }

    fn day_of(ts_ms: u64) -> String {
        chrono::DateTime::from_timestamp_millis(ts_ms as i64)
            .map(|d| d.format("%Y%m%d").to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub fn write(&mut self, ts_ms: u64, row: &serde_json::Value) {
        use std::io::Write as _;
        let day = Self::day_of(ts_ms);
        if self.open.as_ref().is_none_or(|(d, _)| *d != day) {
            let path = Self::path_for(&self.dir, &day);
            match std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
            {
                Ok(f) => self.open = Some((day, f)),
                Err(e) => {
                    self.warn(ts_ms, &format!("open {}: {e}", path.display()));
                    return;
                }
            }
        }
        let Some((_, f)) = self.open.as_mut() else {
            return;
        };
        let mut line = row.to_string();
        line.push('\n');
        if let Err(e) = f.write_all(line.as_bytes()) {
            self.warn(ts_ms, &format!("write: {e}"));
            self.open = None;
        }
    }

    fn warn(&mut self, now_ms: u64, what: &str) {
        if now_ms.saturating_sub(self.last_warn_ms) >= 60_000 {
            self.last_warn_ms = now_ms;
            log::warn!("[GATE] gate.jsonl {what}");
        }
    }
}

/// Fill-stamp ring entry: one cycle's per-side decisions.
#[derive(Debug, Clone)]
struct Cycle {
    ts_ms: u64,
    bid: serde_json::Value,
    ask: serde_json::Value,
    bid_pull: bool,
    ask_pull: bool,
}

pub struct QuoteGate {
    cfg: GateConfig,
    model: Option<GateModel>,
    shared: Arc<Mutex<RefState>>,
    log: Option<GateLog>,
    ring: VecDeque<Cycle>,
    counts: BTreeMap<&'static str, u64>,
    cycles: u64,
    fallback_since_ms: Option<u64>,
    /// Current fallback label (for the start / end WARN edges).
    fallback_active: Option<String>,
    sentinel_logged: bool,
    last_status: serde_json::Value,
}

impl QuoteGate {
    /// `log_dir` None = no gate.jsonl (tests). `origin_ms` anchors the
    /// feature grid and the warmup clock.
    pub fn new(
        cfg: GateConfig,
        model: Option<GateModel>,
        origin_ms: u64,
        log_dir: Option<&Path>,
    ) -> Self {
        QuoteGate {
            cfg,
            model,
            shared: Arc::new(Mutex::new(RefState::new(origin_ms))),
            log: log_dir.map(GateLog::new),
            ring: VecDeque::with_capacity(RING_CAP),
            counts: BTreeMap::new(),
            cycles: 0,
            fallback_since_ms: None,
            fallback_active: None,
            sentinel_logged: false,
            last_status: serde_json::Value::Null,
        }
    }

    pub fn on(&self) -> bool {
        self.cfg.mode.on()
    }

    pub fn model(&self) -> Option<&GateModel> {
        self.model.as_ref()
    }

    /// Handle for the feed tasks.
    pub fn shared(&self) -> Arc<Mutex<RefState>> {
        Arc::clone(&self.shared)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RefState> {
        self.shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Own book as read this tick (`rx_ms` = when the runtime got it):
    /// `(price, size)` per side, best first. L1 is the first level of each;
    /// a one-sided book is not recorded.
    pub fn on_own_book(&self, rx_ms: u64, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        let (Some(&(bid, bid_sz)), Some(&(ask, ask_sz))) = (bids.first(), asks.first()) else {
            return;
        };
        let mut s = self.state();
        s.engine.on_own_l1(rx_ms, bid, ask, bid_sz, ask_sz);
        s.engine.on_own_l2(rx_ms, bids, asks);
    }

    pub fn on_own_trade(&self, rx_ms: u64, taker_buy: bool, px: f64, qty: f64) {
        self.state().engine.on_own_trade(rx_ms, taker_buy, px, qty);
    }

    /// Own trades tape health (its Up / Down are cooldown events).
    pub fn on_own_tape(&self, now_ms: u64, up: bool) {
        self.state().own_tape.on(now_ms, up);
    }

    /// Decide both sides at τ. `inv_sign` is the sign of the inventory
    /// (−1 / 0 / +1) for `reducing_side`; `sentinel` is whether `GATE_OFF`
    /// exists (demotes enforce to shadow). No IO, no await, never fails.
    pub fn evaluate(
        &mut self,
        tau_ms: u64,
        bid: &SidePlan,
        ask: &SidePlan,
        inv_sign: i8,
        sentinel: bool,
    ) -> GateOutput {
        let forced_shadow = self.cfg.mode == GateMode::Enforce && sentinel;
        if forced_shadow && !self.sentinel_logged {
            self.sentinel_logged = true;
            log::warn!(
                "[GATE] GATE_OFF sentinel present: enforce demoted to shadow until it is removed"
            );
        } else if !sentinel && self.sentinel_logged {
            self.sentinel_logged = false;
            log::warn!("[GATE] GATE_OFF sentinel removed: enforce resumes");
        }
        let mode = if forced_shadow {
            GateMode::Shadow
        } else {
            self.cfg.mode
        };
        let (ages, verdict) = {
            let mut s = self.state();
            let rx: LastRx = s.engine.last_rx();
            let age = |t: Option<u64>| t.map(|t| tau_ms.saturating_sub(t));
            let ages = Ages {
                own_book_ms: age(rx.own_l1),
                own_tape_ms: age(rx.own_trade),
                binance_ms: age(rx.bn_l1),
                hyperliquid_ms: age(rx.hl_l1),
            };
            // The grid (EMA / vol state) advances and retention is enforced
            // on every cycle, not only when the features are computed (Codex
            // rounds 1-2 on pairtrade#390): a gate that never scores would
            // otherwise grow its rings without bound, and a gate in warmup
            // would reach its first score with an EMA initialised from the
            // trimmed tail instead of the whole warmup.
            s.engine.advance_grid(tau_ms);
            s.engine.trim(tau_ms);
            let verdict = self.verdict(&mut s, tau_ms, &ages);
            (ages, verdict)
        };
        let (features, bid_d, ask_d) = match verdict {
            Ok(f) => {
                let model = self.model.as_ref().expect("scored only with a model");
                let theta = model.thresholds.quote_bps;
                let score = |s: f64| {
                    let pred = model.predict_bps(&f.side_input(s));
                    let action = if f64::from(pred) >= theta {
                        GateAction::Quote
                    } else {
                        GateAction::Pull
                    };
                    GateDecision::Scored {
                        action,
                        pred_bps: pred,
                    }
                };
                self.fallback_since_ms = None;
                if let Some(label) = self.fallback_active.take() {
                    log::warn!("[GATE] fallback ended ({label}); model scoring again");
                }
                let (bid_d, ask_d) = (score(1.0), score(-1.0));
                (Some(f), bid_d, ask_d)
            }
            Err(reason) => {
                let action = self.fallback_action(tau_ms);
                let label = reason.label();
                *self.counts.entry(reason.key()).or_insert(0) += 1;
                let key = reason.key().to_string();
                if self
                    .fallback_active
                    .as_deref()
                    .map(|l| l.split(':').next().unwrap_or(""))
                    != Some(key.as_str())
                {
                    log::warn!(
                        "[GATE] fallback {label}: {} (policy {})",
                        action.as_str(),
                        self.cfg.fallback.label()
                    );
                }
                self.fallback_active = Some(label);
                let d = GateDecision::Fallback { reason, action };
                (None, d.clone(), d)
            }
        };
        let outcome = |plan: &SidePlan, d: GateDecision| SideOutcome {
            reducing_side: match plan.side {
                QSide::Bid => inv_sign < 0,
                QSide::Ask => inv_sign > 0,
            },
            baseline_qty: plan.qty,
            decision: d,
        };
        let out = GateOutput {
            ts_ms: tau_ms,
            mode,
            forced_shadow,
            model_sha: self.model.as_ref().map(|m| m.sha256_hex.clone()),
            features,
            ages,
            bid: outcome(bid, bid_d),
            ask: outcome(ask, ask_d),
        };
        self.cycles += 1;
        self.ring.push_back(Cycle {
            ts_ms: tau_ms,
            bid: out.stamp(QSide::Bid),
            ask: out.stamp(QSide::Ask),
            bid_pull: out.bid.decision.action() == GateAction::Pull,
            ask_pull: out.ask.decision.action() == GateAction::Pull,
        });
        while self.ring.len() > RING_CAP {
            self.ring.pop_front();
        }
        out
    }

    /// The fail-safe chain (design §2.2 / §6.2), then the features.
    fn verdict(
        &self,
        s: &mut RefState,
        tau_ms: u64,
        ages: &Ages,
    ) -> Result<FeatureVector, FallbackReason> {
        if self.model.is_none() {
            return Err(FallbackReason::ModelMissing);
        }
        let warm = tau_ms.saturating_sub(s.warmup_since_ms);
        if warm < self.cfg.warmup_ms {
            return Err(FallbackReason::Warmup {
                secs_left: (self.cfg.warmup_ms - warm).div_ceil(1_000),
            });
        }
        for (feed, age) in [
            ("own_book", ages.own_book_ms),
            ("binance", ages.binance_ms),
            ("hyperliquid", ages.hyperliquid_ms),
        ] {
            if age.is_none_or(|a| a > self.cfg.ref_stale_ms) {
                return Err(FallbackReason::RefStale { feed, age_ms: age });
            }
        }
        for (feed, st) in [
            ("binance", &s.binance),
            ("hyperliquid", &s.hyperliquid),
            ("own_tape", &s.own_tape),
        ] {
            // Not up — never connected, or Down and not back: fallback until
            // its Up, however long ago (Codex rounds 1-2 on pairtrade#390:
            // the own trades tape can be quiet while connected, so the age
            // check above cannot cover it, and the cooldown below expires).
            if !st.up {
                return Err(FallbackReason::FeedDown { feed });
            }
            if let Some(t) = st.last_event_ms {
                let since = tau_ms.saturating_sub(t);
                if since < self.cfg.event_cooldown_ms {
                    return Err(FallbackReason::RefGapCooldown {
                        feed,
                        secs_left: (self.cfg.event_cooldown_ms - since).div_ceil(1_000),
                    });
                }
            }
        }
        s.engine
            .features_at(tau_ms)
            .map_err(|g| FallbackReason::FeatureGap { name: g.feature() })
    }

    fn fallback_action(&mut self, tau_ms: u64) -> GateAction {
        match self.cfg.fallback {
            FallbackPolicy::Baseline => GateAction::Quote,
            FallbackPolicy::Pull => GateAction::Pull,
            FallbackPolicy::BaselineThenPull { secs } => {
                let since = *self.fallback_since_ms.get_or_insert(tau_ms);
                if tau_ms.saturating_sub(since) < secs * 1_000 {
                    GateAction::Quote
                } else {
                    GateAction::Pull
                }
            }
        }
    }

    /// Append the cycle to gate.jsonl.
    pub fn log(&mut self, out: &GateOutput, market: &str) {
        let with_features = self.cfg.log_features;
        if let Some(l) = self.log.as_mut() {
            l.write(out.ts_ms, &out.row(market, with_features));
        }
    }

    /// The gate decision in force for a fill at `fill_ts_ms` on `side`: the
    /// last cycle at or before `fill_ts − CANCEL_LATENCY_MS` (design §7.2,
    /// the labels' τ). Null when no such cycle is known (restart, or the
    /// gate was off).
    pub fn stamp_for_fill(&self, side: QSide, fill_ts_ms: u64) -> serde_json::Value {
        let cutoff = fill_ts_ms.saturating_sub(CANCEL_LATENCY_MS);
        self.ring
            .iter()
            .rev()
            .find(|c| c.ts_ms <= cutoff)
            .map(|c| match side {
                QSide::Bid => c.bid.clone(),
                QSide::Ask => c.ask.clone(),
            })
            .unwrap_or(serde_json::Value::Null)
    }

    /// Pull rate over the last 5 minutes, per side (None = no cycle).
    fn pull_rate(&self, now_ms: u64) -> (Option<f64>, Option<f64>) {
        let lo = now_ms.saturating_sub(PULL_RATE_WINDOW_MS);
        let recent: Vec<&Cycle> = self.ring.iter().filter(|c| c.ts_ms >= lo).collect();
        if recent.is_empty() {
            return (None, None);
        }
        let n = recent.len() as f64;
        let b = recent.iter().filter(|c| c.bid_pull).count() as f64 / n;
        let a = recent.iter().filter(|c| c.ask_pull).count() as f64 / n;
        (Some(b), Some(a))
    }

    /// The `gate` block of status.json (design §7.4), refreshed from the
    /// last output; `last` is the last cycle's output if the tick produced
    /// one (None keeps the previous decision fields).
    pub fn status(
        &mut self,
        now_ms: u64,
        last: Option<&GateOutput>,
        sentinel: bool,
    ) -> serde_json::Value {
        let (pb, pa) = self.pull_rate(now_ms);
        let warmup_secs_left = {
            let s = self.state();
            let warm = now_ms.saturating_sub(s.warmup_since_ms);
            self.cfg.warmup_ms.saturating_sub(warm).div_ceil(1_000)
        };
        let mut v = json!({
            "mode": self.cfg.mode.as_str(),
            "effective_mode": if self.cfg.mode == GateMode::Enforce && sentinel { "shadow" } else { self.cfg.mode.as_str() },
            "forced_shadow": self.cfg.mode == GateMode::Enforce && sentinel,
            "fallback_policy": self.cfg.fallback.label(),
            "model": self.model.as_ref().map(|m| json!({
                "kind": m.kind, "sha": m.sha_short(), "cell": m.cell,
                "theta_bps": m.thresholds.quote_bps, "frozen_at": m.thresholds.frozen_at})),
            "cycles": self.cycles,
            "fallback_counts": self.counts,
            "fallback_active": self.fallback_active,
            "pull_rate_5m": {"bid": pb.map(round4), "ask": pa.map(round4)},
            "warmup_secs_left": warmup_secs_left,
        });
        if let Some(o) = last {
            let pred = |d: &GateDecision| match d {
                GateDecision::Scored { pred_bps, .. } => Some(round4(f64::from(*pred_bps))),
                GateDecision::Fallback { .. } => None,
            };
            if let Some(m) = v.as_object_mut() {
                m.insert("last_ts_ms".into(), json!(o.ts_ms));
                m.insert("last_action".into(), json!({"bid": o.bid.decision.action().as_str(), "ask": o.ask.decision.action().as_str()}));
                m.insert(
                    "pred_bps_last".into(),
                    json!({"bid": pred(&o.bid.decision), "ask": pred(&o.ask.decision)}),
                );
                m.insert("ref_age_ms".into(), o.ages.json());
            }
            self.last_status = v.clone();
        } else if let Some(prev) = self.last_status.as_object() {
            if let Some(m) = v.as_object_mut() {
                for k in ["last_ts_ms", "last_action", "pred_bps_last", "ref_age_ms"] {
                    if let Some(x) = prev.get(k) {
                        m.insert(k.into(), x.clone());
                    }
                }
            }
        }
        v
    }

    /// The gate part of the 60 s summary line.
    pub fn summary(&self, now_ms: u64) -> String {
        let (pb, pa) = self.pull_rate(now_ms);
        let pct = |x: Option<f64>| x.map_or("-".to_string(), |x| format!("{:.0}%", x * 100.0));
        format!(
            "gate={} pull5m b={} a={} fb={}",
            self.cfg.mode.as_str(),
            pct(pb),
            pct(pa),
            self.fallback_active.as_deref().unwrap_or("none")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::features::N_FEATURES;
    use super::*;
    use crate::logic::PriceTol;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn plan(side: QSide, qty: Option<&str>, price: PricePlan) -> SidePlan {
        SidePlan {
            side,
            qty: qty.map(d),
            price,
        }
    }

    fn at(px: &str) -> PricePlan {
        PricePlan::At {
            px: d(px),
            tol: PriceTol::Exact,
        }
    }

    fn output(mode: GateMode, bid: GateDecision, ask: GateDecision) -> GateOutput {
        let o = |decision| SideOutcome {
            decision,
            baseline_qty: Some(d("0.005")),
            reducing_side: false,
        };
        GateOutput {
            ts_ms: 1_000,
            mode,
            forced_shadow: false,
            model_sha: Some("ab".repeat(32)),
            features: None,
            ages: Ages::default(),
            bid: o(bid),
            ask: o(ask),
        }
    }

    const PULL: GateDecision = GateDecision::Scored {
        action: GateAction::Pull,
        pred_bps: -1.0,
    };
    const QUOTE: GateDecision = GateDecision::Scored {
        action: GateAction::Quote,
        pred_bps: 1.0,
    };

    fn cfg(mode: GateMode) -> GateConfig {
        GateConfig {
            mode,
            fallback: FallbackPolicy::Baseline,
            ref_stale_ms: 10_000,
            event_cooldown_ms: 60_000,
            warmup_ms: 900_000,
            log_features: true,
        }
    }

    fn shipped() -> GateModel {
        GateModel::parse(model::tests::SHIPPED, "arcus", "BTC-USD").unwrap()
    }

    /// Shadow invariant (design D8): whatever the gate decided, in shadow
    /// (and off) the plan that reaches `quote_action` is byte-for-byte the
    /// baseline's.
    #[test]
    fn shadow_and_off_never_change_the_plan() {
        let plans = [
            plan(QSide::Bid, Some("0.005"), at("84000")),
            plan(QSide::Ask, Some("0.005"), at("84000.1")),
            plan(QSide::Bid, None, at("84000")),
            plan(QSide::Ask, Some("0.005"), PricePlan::Unpriced),
            plan(
                QSide::Bid,
                Some("0.005"),
                PricePlan::Crossed {
                    near: d("1"),
                    min_bps: d("1"),
                },
            ),
        ];
        let decisions = [
            PULL,
            QUOTE,
            GateDecision::Fallback {
                reason: FallbackReason::ModelMissing,
                action: GateAction::Pull,
            },
        ];
        for mode in [GateMode::Shadow, GateMode::Off] {
            for bd in &decisions {
                for ad in &decisions {
                    let out = output(mode, bd.clone(), ad.clone());
                    assert!(!out.applied());
                    for p in &plans {
                        assert_eq!(out.apply(p.clone()), *p, "{mode:?} {bd:?}/{ad:?} {p:?}");
                    }
                }
            }
        }
    }

    /// Enforce: a Pull clears the size of a priced side the baseline wanted
    /// (the cancel path of `quote_action`); nothing else changes, and a side
    /// the baseline suppressed or left unpriced is never touched (the gate
    /// is a subset of the baseline, design D1).
    #[test]
    fn enforce_pull_is_a_subset_of_the_baseline() {
        let out = output(GateMode::Enforce, PULL, QUOTE);
        assert!(out.applied());
        let bid = plan(QSide::Bid, Some("0.005"), at("84000"));
        let pulled = out.apply(bid.clone());
        assert_eq!(pulled.qty, None);
        assert_eq!(pulled.price, bid.price, "the price is kept for the log");
        assert_eq!(pulled.side, QSide::Bid);
        // Quote side: identity.
        let ask = plan(QSide::Ask, Some("0.005"), at("84000.1"));
        assert_eq!(out.apply(ask.clone()), ask);
        // Suppressed / unpriced sides: identity even under Pull (no price is
        // ever invented, no quote re-added).
        let none = plan(QSide::Bid, None, at("84000"));
        assert_eq!(out.apply(none.clone()), none);
        let unpriced = plan(QSide::Bid, Some("0.005"), PricePlan::Unpriced);
        assert_eq!(out.apply(unpriced.clone()), unpriced);
        // ... and under a Quote decision too: a Quote never ADDS size to a
        // side the baseline suppressed, nor a price to an unpriced one.
        let quote_both = output(GateMode::Enforce, QUOTE, QUOTE);
        for p in [
            none.clone(),
            plan(QSide::Ask, None, at("84000.1")),
            plan(QSide::Ask, None, PricePlan::Unpriced),
            plan(
                QSide::Bid,
                Some("0.005"),
                PricePlan::Crossed {
                    near: d("1"),
                    min_bps: d("1"),
                },
            ),
        ] {
            assert_eq!(quote_both.apply(p.clone()), p);
            assert_eq!(out.apply(p.clone()), p);
        }
        // A fallback Pull applies the same way under enforce (policy=pull).
        let fb = output(
            GateMode::Enforce,
            GateDecision::Fallback {
                reason: FallbackReason::Warmup { secs_left: 1 },
                action: GateAction::Pull,
            },
            QUOTE,
        );
        assert_eq!(fb.apply(bid.clone()).qty, None);
        // The sentinel-demoted output is shadow: identity.
        let demoted = GateOutput {
            mode: GateMode::Shadow,
            forced_shadow: true,
            ..output(GateMode::Enforce, PULL, PULL)
        };
        assert_eq!(demoted.apply(bid.clone()), bid);
    }

    /// End to end on the real tape slice with the shipped model: the
    /// decision is the 10-01 rule on `pdev_bn_60s`, and the fail-safe chain
    /// yields the named fallbacks instead of a score.
    #[test]
    fn evaluate_scores_the_10_01_rule_and_falls_back_by_reason() {
        let g: serde_json::Value =
            serde_json::from_str(include_str!("testdata/golden_features.json")).unwrap();
        let origin = g["grid_origin_ms"].as_u64().unwrap();
        let records = xtape::parse_jsonl(include_str!("testdata/xtape_slice.jsonl")).unwrap();
        let bid = plan(QSide::Bid, Some("0.005"), at("84000"));
        let ask = plan(QSide::Ask, Some("0.005"), at("84000.1"));
        let mut gate = QuoteGate::new(cfg(GateMode::Shadow), Some(shipped()), origin, None);
        let tau = g["rows"][0]["tau_ms"].as_u64().unwrap();
        {
            let mut s = gate.state();
            // Records received before τ (live pattern; retention is relative
            // to the newest record).
            let mut idx = 0;
            features::tests::feed_until(&mut s.engine, &records, &mut idx, tau);
            // Feeds came up before the origin; no gap since.
            s.binance.on(origin - 900, true);
            s.hyperliquid.on(origin - 900, true);
            s.own_tape.on(origin - 900, true);
        }
        // 1) warmup (900 s from the origin; τ is ~125 s in).
        let out = gate.evaluate(tau, &bid, &ask, 0, false);
        assert!(
            matches!(&out.bid.decision, GateDecision::Fallback { reason: FallbackReason::Warmup { secs_left }, action: GateAction::Quote } if (770..=776).contains(secs_left)),
            "{:?}",
            out.bid.decision
        );
        assert_eq!(out.features, None);
        assert_eq!(out.mode, GateMode::Shadow);
        assert_eq!(out.apply(bid.clone()), bid);
        // 2) warm: scored. pred is the side-oriented pdev_bn_60s (golden
        //    value at this τ): bid quotes iff pdev >= -0.25, ask iff
        //    -pdev >= -0.25. The fixture's τ has |pdev| > 0.25, so the two
        //    sides decide differently.
        gate.cfg.warmup_ms = 0;
        let out = gate.evaluate(tau, &bid, &ask, -1, false);
        let pdev = g["rows"][0]["features"][15].as_f64().unwrap();
        assert!(pdev.abs() > 0.25, "weak fixture: {pdev}");
        let want = |x: f64| {
            if x >= -0.25 {
                GateAction::Quote
            } else {
                GateAction::Pull
            }
        };
        match (&out.bid.decision, &out.ask.decision) {
            (
                GateDecision::Scored {
                    action: ba,
                    pred_bps: bp,
                },
                GateDecision::Scored {
                    action: aa,
                    pred_bps: ap,
                },
            ) => {
                assert_eq!(*ba, want(pdev));
                assert_eq!(*aa, want(-pdev));
                assert_ne!(ba, aa);
                assert!((f64::from(*bp) - pdev).abs() < 1e-6, "{bp} vs {pdev}");
                assert!((f64::from(*ap) + pdev).abs() < 1e-6, "{ap} vs {pdev}");
            }
            other => panic!("{other:?}"),
        }
        let (pulled, kept) = if want(pdev) == GateAction::Pull {
            (&bid, &ask)
        } else {
            (&ask, &bid)
        };
        assert!(out.bid.reducing_side, "short inventory: the bid reduces it");
        assert!(!out.ask.reducing_side);
        assert_eq!(out.ages.binance_ms.map(|a| a < 10_000), Some(true));
        assert!(out.features.is_some());
        // Shadow: the plan is untouched even for the pulled side.
        assert_eq!(out.apply(pulled.clone()), *pulled);
        assert_eq!(out.apply(kept.clone()), *kept);
        // The row and the fill stamp carry the decision.
        let row = out.row("BTC-USD", true);
        assert_eq!(row["kind"], "gate");
        assert_eq!(row[pulled.side.as_str()]["action"], "pull");
        assert_eq!(row[kept.side.as_str()]["action"], "quote");
        assert_eq!(row["bid"]["reducing_side"], true);
        assert_eq!(row["applied"], false);
        assert_eq!(row["features"].as_object().unwrap().len(), N_FEATURES);
        assert!(row["features"]["pdev_bn_60s"].as_f64().is_some());
        assert_eq!(
            out.row("BTC-USD", false)["features"],
            serde_json::Value::Null
        );
        let stamp = out.stamp(pulled.side);
        assert_eq!(stamp["action"], "pull");
        assert_eq!(stamp["decision"], "scored");
        assert_eq!(stamp["mode"], "shadow");
        assert_eq!(stamp["applied"], false);
        assert_eq!(stamp["model_sha"].as_str().unwrap().len(), 12);
        // Fill stamping: a fill 0.3 s or more after τ gets this cycle; an
        // earlier fill gets the cycle before it (none here → null).
        assert_eq!(
            gate.stamp_for_fill(pulled.side, tau + 300)["action"],
            "pull"
        );
        assert_eq!(gate.stamp_for_fill(kept.side, tau + 300)["action"], "quote");
        assert_eq!(
            gate.stamp_for_fill(pulled.side, tau + 299),
            serde_json::Value::Null
        );
        let later = gate.evaluate(tau + 500, &bid, &ask, 0, false);
        assert!(matches!(later.bid.decision, GateDecision::Scored { .. }));
        assert_eq!(gate.stamp_for_fill(pulled.side, tau + 700)["ts_ms"], tau);
        assert_eq!(
            gate.stamp_for_fill(pulled.side, tau + 800)["ts_ms"],
            tau + 500
        );
        // 3) enforce applies the pull; the sentinel demotes it to shadow.
        gate.cfg.mode = GateMode::Enforce;
        let out = gate.evaluate(tau, &bid, &ask, 0, false);
        assert!(out.applied());
        assert_eq!(out.apply(pulled.clone()).qty, None);
        assert_eq!(out.apply(kept.clone()), *kept);
        let out = gate.evaluate(tau, &bid, &ask, 0, true);
        assert_eq!(out.mode, GateMode::Shadow);
        assert!(out.forced_shadow);
        assert_eq!(out.apply(pulled.clone()), *pulled);
        assert_eq!(out.row("BTC-USD", false)["forced_shadow"], true);
        gate.cfg.mode = GateMode::Shadow;
        // 4) gap cooldown: a reconnect event 10 s ago.
        gate.state().binance.on(tau - 10_000, true);
        let out = gate.evaluate(tau, &bid, &ask, 0, false);
        assert!(
            matches!(
                &out.bid.decision,
                GateDecision::Fallback {
                    reason: FallbackReason::RefGapCooldown {
                        feed: "binance",
                        secs_left: 50
                    },
                    ..
                }
            ),
            "{:?}",
            out.bid.decision
        );
        gate.state().binance.last_event_ms = Some(origin - 900);
        // 5) a NaN in a reference record: feature gap, not a zero (at τ, before
        //    the clock moves on: retention is enforced on every cycle).
        {
            let mut s = gate.state();
            s.engine.on_bn_l1(tau - 10, f64::NAN, f64::NAN, 1.0, 1.0);
        }
        let out = gate.evaluate(tau, &bid, &ask, 0, false);
        assert!(
            matches!(&out.bid.decision, GateDecision::Fallback { reason: FallbackReason::FeatureGap { name }, .. } if name.starts_with("bn_ret")),
            "{:?}",
            out.bid.decision
        );
        assert_eq!(
            out.row("BTC-USD", true)["bid"]["reason"],
            "feature_gap:bn_ret_0.5s"
        );
        // 6) stale feed: τ far past the last Binance record.
        let out = gate.evaluate(tau + 60_000, &bid, &ask, 0, false);
        assert!(
            matches!(&out.bid.decision, GateDecision::Fallback { reason: FallbackReason::RefStale { feed: "own_book", age_ms: Some(a) }, .. } if *a > 10_000),
            "{:?}",
            out.bid.decision
        );
        // 7) no model: model_missing.
        let mut no_model = QuoteGate::new(cfg(GateMode::Shadow), None, origin, None);
        let out = no_model.evaluate(tau, &bid, &ask, 0, false);
        assert!(matches!(
            &out.bid.decision,
            GateDecision::Fallback {
                reason: FallbackReason::ModelMissing,
                action: GateAction::Quote
            }
        ));
        assert_eq!(out.model_sha, None);
        // 8) the own trades tape goes Down and stays down past the cooldown:
        //    fallback until its Up (then the cooldown), never a score on
        //    silently stale own-flow features. Same for a reference feed.
        {
            let mut s = gate.state();
            s.engine
                .on_bn_l1(tau + 70_000 - 5, 84000.0, 84000.1, 1.0, 1.0);
            s.engine.on_hl_l1(tau + 70_000 - 5, 84000.0, 84000.1);
            s.engine
                .on_own_l1(tau + 70_000 - 5, 84000.0, 84000.1, 1.0, 1.0);
            s.own_tape.on(origin - 900, true);
            s.own_tape.on(tau, false);
        }
        let t2 = tau + 70_000;
        let out = gate.evaluate(t2, &bid, &ask, 0, false);
        assert!(
            matches!(
                &out.bid.decision,
                GateDecision::Fallback {
                    reason: FallbackReason::FeedDown { feed: "own_tape" },
                    ..
                }
            ),
            "{:?}",
            out.bid.decision
        );
        assert_eq!(
            out.row("BTC-USD", false)["bid"]["reason"],
            "feed_down:own_tape"
        );
        gate.state().own_tape.on(t2, true);
        let out = gate.evaluate(t2 + 1, &bid, &ask, 0, false);
        assert!(
            matches!(
                &out.bid.decision,
                GateDecision::Fallback {
                    reason: FallbackReason::RefGapCooldown {
                        feed: "own_tape",
                        ..
                    },
                    ..
                }
            ),
            "{:?}",
            out.bid.decision
        );
        gate.state().hyperliquid.on(tau, false);
        let out = gate.evaluate(t2, &bid, &ask, 0, false);
        assert!(
            matches!(
                &out.bid.decision,
                GateDecision::Fallback {
                    reason: FallbackReason::FeedDown {
                        feed: "hyperliquid"
                    },
                    ..
                }
            ),
            "{:?}",
            out.bid.decision
        );
        // Counters and status reflect the fallbacks.
        let st = gate.status(tau, Some(&out), false);
        assert!(st["fallback_counts"]["warmup"].as_u64().unwrap() >= 1);
        assert!(st["fallback_counts"]["feature_gap"].as_u64().unwrap() >= 1);
        assert_eq!(st["mode"], "shadow");
        assert_eq!(st["model"]["theta_bps"], -0.25);
        assert!(st["pull_rate_5m"][pulled.side.as_str()].as_f64().unwrap() > 0.0);
        assert!(gate.summary(tau).starts_with("gate=shadow pull5m"));
    }

    /// Codex round 2 on pairtrade#390: a feed that never connected (no Up,
    /// no Down) is not a usable feed — never score on empty own flow.
    #[test]
    fn a_feed_that_never_came_up_keeps_the_gate_in_fallback() {
        let g: serde_json::Value =
            serde_json::from_str(include_str!("testdata/golden_features.json")).unwrap();
        let origin = g["grid_origin_ms"].as_u64().unwrap();
        let records = xtape::parse_jsonl(include_str!("testdata/xtape_slice.jsonl")).unwrap();
        let bid = plan(QSide::Bid, Some("0.005"), at("84000"));
        let ask = plan(QSide::Ask, Some("0.005"), at("84000.1"));
        let mut c = cfg(GateMode::Shadow);
        c.warmup_ms = 0;
        let mut gate = QuoteGate::new(c, Some(shipped()), origin, None);
        let tau = g["rows"][0]["tau_ms"].as_u64().unwrap();
        {
            let mut s = gate.state();
            let mut idx = 0;
            features::tests::feed_until(&mut s.engine, &records, &mut idx, tau);
            s.binance.on(origin - 900, true);
            s.hyperliquid.on(origin - 900, true);
            // own tape: never an Up, never a Down.
        }
        let out = gate.evaluate(tau, &bid, &ask, 0, false);
        assert!(
            matches!(
                &out.bid.decision,
                GateDecision::Fallback {
                    reason: FallbackReason::FeedDown { feed: "own_tape" },
                    ..
                }
            ),
            "{:?}",
            out.bid.decision
        );
        gate.state().own_tape.on(origin - 900, true);
        let out = gate.evaluate(tau, &bid, &ask, 0, false);
        assert!(
            matches!(out.bid.decision, GateDecision::Scored { .. }),
            "{:?}",
            out.bid.decision
        );
    }

    /// Codex round 2 on pairtrade#390: the grid (EMA / vol state) must keep
    /// advancing through warmup cycles while the rings are trimmed behind
    /// it, so the first score after warmup equals the offline value computed
    /// on the whole history (the golden row), not one initialised from the
    /// last 150 s.
    #[test]
    fn warmup_cycles_advance_the_grid_so_the_first_score_matches_offline() {
        let g: serde_json::Value =
            serde_json::from_str(include_str!("testdata/golden_features.json")).unwrap();
        let origin = g["grid_origin_ms"].as_u64().unwrap();
        let records = xtape::parse_jsonl(include_str!("testdata/xtape_slice.jsonl")).unwrap();
        let rows = g["rows"].as_array().unwrap();
        let last = rows.last().unwrap();
        let tau_end = last["tau_ms"].as_u64().unwrap();
        let bid = plan(QSide::Bid, Some("0.005"), at("84000"));
        let ask = plan(QSide::Ask, Some("0.005"), at("84000.1"));
        let mut c = cfg(GateMode::Shadow);
        c.warmup_ms = 10_000_000; // never warm during the feed
        let mut gate = QuoteGate::new(c, Some(shipped()), origin, None);
        {
            // One lock: three `state()` guards at once would deadlock.
            let mut st = gate.state();
            st.binance.on(origin - 900, true);
            st.hyperliquid.on(origin - 900, true);
            st.own_tape.on(origin - 900, true);
        }
        // Feed records in rx order with a gate cycle every 500 ms (warmup
        // fallback each time), as live.
        let mut next_cycle = origin + 50;
        let mut cycles = 0;
        for r in &records {
            while r.rx_ms >= next_cycle && next_cycle <= tau_end {
                let out = gate.evaluate(next_cycle, &bid, &ask, 0, false);
                assert!(matches!(
                    out.bid.decision,
                    GateDecision::Fallback {
                        reason: FallbackReason::Warmup { .. },
                        ..
                    }
                ));
                next_cycle += 500;
                cycles += 1;
            }
            xtape::apply(&mut gate.state().engine, r);
        }
        assert!(cycles > 250, "{cycles} warmup cycles");
        // History older than 150 s before τ_end is gone...
        assert!(gate.state().engine.record_count() < 4_000);
        // ... yet the first score equals the offline golden features.
        gate.cfg.warmup_ms = 0;
        let out = gate.evaluate(tau_end, &bid, &ask, 0, false);
        let f = out.features.expect("scored");
        let want: Vec<f64> = last["features"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        for (k, (a, b)) in f.values.iter().zip(want.iter()).enumerate() {
            assert!(
                (a - b).abs() <= 1e-6 * b.abs().max(1.0),
                "feature {k}: {a} vs {b}"
            );
        }
    }

    /// Codex round 2 on pairtrade#390: a reference feed's reconnect restarts
    /// the grid state together with the warmup, never continuing from the
    /// values the gap forward-filled.
    #[test]
    fn a_reference_feed_recovery_resets_the_grid_state() {
        let mut s = RefState::new(500);
        s.on_ref(feed::RefFeed::Binance, 1_000, feed::RefEvent::Up);
        for t in 0..50u64 {
            let rx = 2_000 + t * 100;
            s.engine.on_own_l1(rx, 100.0, 100.1, 1.0, 1.0);
            s.engine.on_bn_l1(rx, 100.0, 100.1, 1.0, 1.0);
            s.engine.on_hl_l1(rx, 100.0, 100.1);
        }
        s.engine.advance_grid(7_000);
        let (o, next, init) = s.engine.grid_state();
        assert_eq!((o, init), (1_000, true));
        assert!(next > 50);
        s.on_ref(
            feed::RefFeed::Binance,
            8_000,
            feed::RefEvent::Down("x".into()),
        );
        s.on_ref(feed::RefFeed::Binance, 9_000, feed::RefEvent::Up);
        assert_eq!(s.warmup_since_ms, 9_000);
        assert_eq!(s.engine.grid_state(), (9_000, 0, false));
        // The pre-gap history is gone too (Codex round 3): nothing before
        // the new origin can seed the EMA.
        assert_eq!(s.engine.record_count(), 0);
        s.engine.advance_grid(9_300);
        assert_eq!(
            s.engine.grid_state(),
            (9_000, 4, false),
            "no complete point without new records"
        );
        // New records after the recovery initialise the EMA from themselves.
        s.engine.on_own_l1(9_350, 100.0, 100.1, 1.0, 1.0);
        s.engine.on_bn_l1(9_350, 100.0, 100.1, 1.0, 1.0);
        s.engine.on_hl_l1(9_350, 100.0, 100.1);
        s.engine.advance_grid(9_600);
        let (o2, _, init2) = s.engine.grid_state();
        assert_eq!((o2, init2), (9_000, true));
    }

    /// Codex P1 on pairtrade#390: a gate that never scores (no model) must
    /// still bound its feature history.
    #[test]
    fn history_is_trimmed_even_when_scoring_is_bypassed() {
        let bid = plan(QSide::Bid, Some("0.005"), at("84000"));
        let ask = plan(QSide::Ask, Some("0.005"), at("84000.1"));
        let mut g = QuoteGate::new(cfg(GateMode::Shadow), None, 0, None);
        let l2: Vec<(f64, f64)> = (0..20).map(|i| (84000.0 - i as f64 * 0.1, 0.5)).collect();
        for k in 0..20_000u64 {
            let t = 1_000 + k * 500;
            g.on_own_book(t, &l2, &l2);
            g.on_own_trade(t, true, 84000.0, 0.01);
            let out = g.evaluate(t + 1, &bid, &ask, 0, false);
            assert!(matches!(
                out.bid.decision,
                GateDecision::Fallback {
                    reason: FallbackReason::ModelMissing,
                    ..
                }
            ));
        }
        // 150 s of retention at 2 Hz = ~300 records per ring, not 20,000.
        let n = g.state().engine.record_count();
        assert!(n < 1_000, "{n} records retained");
    }

    #[test]
    fn fallback_policies_choose_the_recorded_action() {
        let bid = plan(QSide::Bid, Some("0.005"), at("84000"));
        let ask = plan(QSide::Ask, Some("0.005"), at("84000.1"));
        let mut c = cfg(GateMode::Shadow);
        c.fallback = FallbackPolicy::Pull;
        let mut g = QuoteGate::new(c.clone(), None, 0, None);
        assert_eq!(
            g.evaluate(1_000, &bid, &ask, 0, false)
                .bid
                .decision
                .action(),
            GateAction::Pull
        );
        c.fallback = FallbackPolicy::BaselineThenPull { secs: 60 };
        let mut g = QuoteGate::new(c, None, 0, None);
        assert_eq!(
            g.evaluate(1_000, &bid, &ask, 0, false)
                .bid
                .decision
                .action(),
            GateAction::Quote
        );
        assert_eq!(
            g.evaluate(60_999, &bid, &ask, 0, false)
                .bid
                .decision
                .action(),
            GateAction::Quote
        );
        assert_eq!(
            g.evaluate(61_000, &bid, &ask, 0, false)
                .ask
                .decision
                .action(),
            GateAction::Pull
        );
        // Mode / policy parsing.
        assert_eq!(GateMode::parse(" Shadow ").unwrap(), GateMode::Shadow);
        assert!(GateMode::parse("on").is_err());
        assert_eq!(
            FallbackPolicy::parse("baseline_then_pull:60").unwrap(),
            FallbackPolicy::BaselineThenPull { secs: 60 }
        );
        assert!(FallbackPolicy::parse("baseline_then_pull:0").is_err());
        assert!(FallbackPolicy::parse("baseline_then_pull:x").is_err());
        assert!(FallbackPolicy::parse("quote").is_err());
    }

    #[test]
    fn gap_recovery_restarts_warmup() {
        let mut s = RefState::new(1_000);
        s.on_ref(feed::RefFeed::Binance, 2_000, feed::RefEvent::Up);
        assert_eq!(
            s.warmup_since_ms, 2_000,
            "the first Up restarts the warmup too"
        );
        // A first connect long after start (Codex round 3): same.
        s.on_ref(feed::RefFeed::Hyperliquid, 950_000, feed::RefEvent::Up);
        assert_eq!(s.warmup_since_ms, 950_000);
        assert_eq!(s.engine.grid_state().0, 950_000);
        s.on_ref(
            feed::RefFeed::Binance,
            5_000,
            feed::RefEvent::Down("x".into()),
        );
        assert!(!s.binance.up);
        s.on_ref(feed::RefFeed::Binance, 9_000, feed::RefEvent::Up);
        assert_eq!(s.warmup_since_ms, 9_000);
        assert_eq!(s.binance.last_event_ms, Some(9_000));
        s.on_ref(
            feed::RefFeed::Hyperliquid,
            9_500,
            feed::RefEvent::HlL1 { bid: 1.0, ask: 2.0 },
        );
        assert_eq!(s.engine.last_rx().hl_l1, Some(9_500));
    }

    #[test]
    fn gate_log_rotates_by_utc_day() {
        let dir = tempfile::tempdir().unwrap();
        let mut l = GateLog::new(dir.path());
        let day1 = 1_791_010_400_000; // 2026-10-03
        l.write(day1, &json!({"a": 1}));
        l.write(day1 + 1, &json!({"a": 2}));
        l.write(day1 + 86_400_000, &json!({"a": 3}));
        let f1 = std::fs::read_to_string(GateLog::path_for(dir.path(), "20261003")).unwrap();
        assert_eq!(f1.lines().count(), 2);
        assert_eq!(f1.lines().next().unwrap(), r#"{"a":1}"#);
        let f2 = std::fs::read_to_string(GateLog::path_for(dir.path(), "20261004")).unwrap();
        assert_eq!(f2, "{\"a\":3}\n");
    }
}
