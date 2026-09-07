//! Persistent runtime state (`state.json`), see `docs/book-runtime.md` §8.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::directional::{load_json, persist_json};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    /// Signed base quantity (long positive).
    pub qty: f64,
    /// Volume-weighted average entry price of the current leg.
    pub avg_price: f64,
    /// Unix seconds the leg was opened (reset when it flips or is closed).
    pub opened_at: i64,
    /// Last funding accrual timestamp (unix seconds).
    #[serde(default)]
    pub funding_accrued_at: Option<i64>,
    /// Realized PnL booked against *this* leg so far. A leg reduced over
    /// several fills is one trade, so the win/loss classification at its
    /// final close must use the total, not the last fill: +$50 then -$10
    /// is a $40 winner, and the reverse order is a loser either way.
    /// Reset when a new leg opens (including a flip).
    #[serde(default)]
    pub realized_pnl: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionOutcome {
    /// Every intent filled (or nothing to do).
    Applied,
    /// Plan executed but a residual is still pending.
    Partial,
    /// Signal or plan rejected; book untouched.
    Rejected,
    /// Window closed without a valid signal; book untouched.
    Skipped,
    /// Halted by risk; book flattened.
    Halted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub key: String,
    pub outcome: DecisionOutcome,
    /// Unix seconds of the last update to this record.
    pub at: i64,
    #[serde(default)]
    pub signal_sha256: Option<String>,
    /// Unix seconds of the signal file's own `generated_at`, set only when
    /// this decision actually accepted a signal (Applied/Partial). `at`
    /// above is when this record was last written -- the apply time, a
    /// retry, or a halt -- and can be much later than the signal's true
    /// generation time, so `book_signal_age_seconds` must restore from
    /// this field rather than from `at`.
    #[serde(default)]
    pub signal_generated_at: Option<i64>,
    #[serde(default)]
    pub reject_reason: Option<String>,
    /// Highest per-symbol attempt count on this key; reported in the
    /// ledger and the logs. The budget itself is per symbol, below.
    #[serde(default)]
    pub attempts: u32,
    /// Attempts spent per symbol: an order for that symbol reached the
    /// venue that many times. A leg that never got sent -- blocked by a
    /// rail, or aborted before the send -- keeps its budget even when
    /// another leg of the same plan was submitted, so with
    /// `max_attempts: 1` one filled reduction cannot strand the opening
    /// it was paired with.
    #[serde(default)]
    pub attempts_by_symbol: BTreeMap<String, u32>,
    /// Unix seconds at which this decision's book must be flat again
    /// (fixed-window strategies); persisted so an overdue flatten survives
    /// a restart that lands after the next decision time.
    #[serde(default)]
    pub flatten_at: Option<i64>,
    /// Fixed-window flatten completed for this key.
    #[serde(default)]
    pub flatten_done: bool,
    /// Signed target quantity per symbol the last plan aimed at; used to
    /// re-plan a residual after a partial fill or a restart.
    #[serde(default)]
    pub target_qty: BTreeMap<String, f64>,
}

impl DecisionRecord {
    pub fn attempts_for(&self, symbol: &str) -> u32 {
        self.attempts_by_symbol.get(symbol).copied().unwrap_or(0)
    }

    /// Is any leg of this decision still allowed to reach the venue?
    pub fn any_symbol_under_budget(&self, max_attempts: u32) -> bool {
        self.target_qty
            .keys()
            .any(|s| self.attempts_for(s) < max_attempts)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SessionRisk {
    pub start_equity: f64,
    pub start_at: i64,
    pub halted: bool,
    #[serde(default)]
    pub halt_reason: Option<String>,
    #[serde(default)]
    pub halted_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DailyRisk {
    /// `YYYY-MM-DD` UTC.
    pub date: String,
    pub start_equity: f64,
    pub halted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookState {
    pub schema_version: u32,
    pub instance_id: String,
    #[serde(default)]
    pub positions: BTreeMap<String, Position>,
    #[serde(default)]
    pub last_decision: Option<DecisionRecord>,
    /// Unix seconds of the most recently *accepted* signal's own
    /// `generated_at`, independent of `last_decision`: a later decision
    /// key that is Rejected or Skipped overwrites `last_decision` entirely
    /// (with no accepted signal of its own), but the book is still running
    /// on the last one it did accept, so `book_signal_age_seconds` must
    /// keep tracking that signal's age -- not reset to unknown -- across
    /// both a live reject/skip and a restart afterward.
    #[serde(default)]
    pub last_accepted_signal_generated_at: Option<i64>,
    #[serde(default)]
    pub session: SessionRisk,
    #[serde(default)]
    pub daily: DailyRisk,
    #[serde(default)]
    pub peak_equity: f64,
    #[serde(default)]
    pub cum_realized_usd: f64,
    #[serde(default)]
    pub cum_fees_usd: f64,
    #[serde(default)]
    pub cum_funding_est_usd: f64,
    /// Funding notional (signed qty * elapsed hours, no rate/price applied
    /// yet) frozen off a leg by a fill that changed its quantity while the
    /// funding rate was unavailable. Settled -- at whatever rate is next
    /// available for the symbol -- the next time `accrue_funding` succeeds,
    /// even across a full close and later reopen. Keyed by symbol.
    #[serde(default)]
    pub pending_funding_qty_hours: BTreeMap<String, f64>,
    #[serde(default)]
    pub last_mark_date: Option<String>,
    /// Per-symbol funding breakdown accrued but not yet durably written to
    /// pnl.jsonl for the live daily mark still pending (`last_mark_date`
    /// unset for today): merged onto the next retry's own accrual so the
    /// eventual successful row still reports the whole day's funding, not
    /// just the sliver accrued since the last attempt. Persisted (not
    /// engine-local) so a restart between a failed append and the next
    /// retry does not lose it -- `cum_funding_est_usd` above already
    /// includes it regardless; this is only the per-symbol row detail.
    #[serde(default)]
    pub pending_mark_funding_detail: serde_json::Map<String, serde_json::Value>,
    /// The date `pending_mark_funding_detail` belongs to. Without this, a
    /// retry that doesn't succeed until after a UTC rollover would
    /// recompute the label from the new `now` and merge both days'
    /// accrual into one row for the newer date, permanently omitting the
    /// date whose append actually failed. Set on the first accrual
    /// attempt for a date, cleared only once that date's row is
    /// successfully written -- a later tick's own accrual is held off
    /// until this drains, so at most one date is ever pending.
    #[serde(default)]
    pub pending_mark_date: Option<String>,
    /// Last equity observation (unix secs, usd).
    #[serde(default)]
    pub last_equity: Option<(i64, f64)>,
    #[serde(default)]
    pub trades_closed: u64,
    #[serde(default)]
    pub trades_won: u64,
    #[serde(default)]
    pub max_drawdown_usd: f64,
}

impl BookState {
    pub fn new(instance_id: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            instance_id: instance_id.to_string(),
            positions: BTreeMap::new(),
            last_decision: None,
            last_accepted_signal_generated_at: None,
            session: SessionRisk::default(),
            daily: DailyRisk::default(),
            peak_equity: 0.0,
            cum_realized_usd: 0.0,
            cum_fees_usd: 0.0,
            cum_funding_est_usd: 0.0,
            pending_funding_qty_hours: BTreeMap::new(),
            last_mark_date: None,
            pending_mark_funding_detail: serde_json::Map::new(),
            pending_mark_date: None,
            last_equity: None,
            trades_closed: 0,
            trades_won: 0,
            max_drawdown_usd: 0.0,
        }
    }

    /// Load from disk. Missing file → fresh state; corrupt file → error (a
    /// silently reset book would double-open positions).
    pub fn load_or_new(path: &Path, instance_id: &str) -> Result<Self> {
        match load_json::<BookState>(path)? {
            Some(s) => {
                if s.schema_version != SCHEMA_VERSION {
                    anyhow::bail!(
                        "state {} has schema_version {} (expected {})",
                        path.display(),
                        s.schema_version,
                        SCHEMA_VERSION
                    );
                }
                if s.instance_id != instance_id {
                    anyhow::bail!(
                        "state {} belongs to instance {:?}, config says {:?}",
                        path.display(),
                        s.instance_id,
                        instance_id
                    );
                }
                Ok(s)
            }
            None => Ok(Self::new(instance_id)),
        }
    }

    pub fn persist(&self, path: &Path) -> Result<()> {
        persist_json(path, self)
    }

    /// Signed quantity per held symbol (zero legs omitted).
    pub fn signed_qty(&self) -> BTreeMap<String, f64> {
        self.positions
            .iter()
            .filter(|(_, p)| p.qty != 0.0)
            .map(|(s, p)| (s.clone(), p.qty))
            .collect()
    }

    pub fn is_flat(&self) -> bool {
        self.positions.values().all(|p| p.qty == 0.0)
    }

    pub fn gross_usd(&self, prices: &std::collections::HashMap<String, f64>) -> f64 {
        self.positions
            .iter()
            .map(|(s, p)| p.qty.abs() * prices.get(s).copied().unwrap_or(p.avg_price))
            .sum()
    }

    pub fn net_usd(&self, prices: &std::collections::HashMap<String, f64>) -> f64 {
        self.positions
            .iter()
            .map(|(s, p)| p.qty * prices.get(s).copied().unwrap_or(p.avg_price))
            .sum()
    }

    pub fn unrealized_usd(&self, prices: &std::collections::HashMap<String, f64>) -> f64 {
        self.positions
            .iter()
            .map(|(s, p)| {
                let px = prices.get(s).copied().unwrap_or(p.avg_price);
                p.qty * (px - p.avg_price)
            })
            .sum()
    }

    /// Apply a fill to the book. `signed_qty` is +buy / -sell in base units.
    /// Returns the realized PnL this fill produced (0 for opens/increases).
    pub fn apply_fill(&mut self, symbol: &str, signed_qty: f64, price: f64, now: i64) -> f64 {
        if signed_qty == 0.0 {
            return 0.0;
        }
        let pos = self
            .positions
            .entry(symbol.to_string())
            .or_insert(Position {
                qty: 0.0,
                avg_price: price,
                opened_at: now,
                funding_accrued_at: Some(now),
                realized_pnl: 0.0,
            });
        let mut realized = 0.0;
        let same_side = pos.qty == 0.0 || (pos.qty > 0.0) == (signed_qty > 0.0);
        if same_side {
            let new_qty = pos.qty + signed_qty;
            if pos.qty == 0.0 {
                pos.avg_price = price;
                pos.opened_at = now;
                pos.funding_accrued_at = Some(now);
                pos.realized_pnl = 0.0;
            } else {
                pos.avg_price =
                    (pos.avg_price * pos.qty.abs() + price * signed_qty.abs()) / new_qty.abs();
            }
            pos.qty = new_qty;
        } else {
            // Reducing (possibly through zero).
            let closing = signed_qty.abs().min(pos.qty.abs());
            let dir = if pos.qty > 0.0 { 1.0 } else { -1.0 };
            realized = (price - pos.avg_price) * closing * dir;
            pos.realized_pnl += realized;
            // Snapped to zero here, before classifying: a venue-reported
            // fill that exactly closes a position accumulated through
            // several fills (e.g. 0.1 + 0.2, closed by 0.3) leaves a
            // same-sign residual of a few ULPs, not exactly 0.0. Left
            // unsnapped, that residual takes the *reducing* branch below
            // instead of *closed*, so trades_closed/trades_won never
            // increment and book_fill's `leg_closed` (which compares
            // trades_closed before/after) reports this as a partial
            // reduction even though the tail cleanup a few lines down
            // removes the leg from `positions` moments later.
            let remaining = pos.qty + signed_qty;
            let remaining = if remaining.abs() < 1e-12 { 0.0 } else { remaining };
            if remaining == 0.0 || (remaining > 0.0) != (pos.qty > 0.0) {
                // Closed, maybe flipped. One trade = one leg's lifetime,
                // so classify on everything it realized, not this fill.
                self.trades_closed += 1;
                if pos.realized_pnl > 0.0 {
                    self.trades_won += 1;
                }
                if remaining != 0.0 {
                    pos.avg_price = price;
                    pos.opened_at = now;
                    pos.funding_accrued_at = Some(now);
                }
                pos.realized_pnl = 0.0;
                pos.qty = remaining;
            } else {
                pos.qty = remaining;
            }
            self.cum_realized_usd += realized;
        }
        // Drop empty legs so `positions` is always the live book.
        if pos.qty == 0.0 {
            self.positions.remove(symbol);
        }
        // Normalise float noise on tiny residuals.
        if let Some(p) = self.positions.get_mut(symbol) {
            if p.qty.abs() < 1e-12 {
                self.positions.remove(symbol);
            }
        }
        realized
    }

    /// Record an equity observation and update peak / drawdown.
    pub fn observe_equity(&mut self, now: i64, equity: f64) {
        self.last_equity = Some((now, equity));
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
        let dd = self.peak_equity - equity;
        if dd > self.max_drawdown_usd {
            self.max_drawdown_usd = dd;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_track_average_price_and_realize_on_reduce_and_flip() {
        let mut s = BookState::new("t");
        assert_eq!(s.apply_fill("BTC", 1.0, 100.0, 1), 0.0);
        assert_eq!(s.apply_fill("BTC", 1.0, 120.0, 2), 0.0);
        assert_eq!(s.positions["BTC"].avg_price, 110.0);
        // reduce half at 130 → realized (130-110)*1 = 20
        assert_eq!(s.apply_fill("BTC", -1.0, 130.0, 3), 20.0);
        assert_eq!(s.positions["BTC"].qty, 1.0);
        assert_eq!(s.trades_closed, 0);
        // flip through zero: close 1 @ 90 (realized -20), open short 1 @ 90
        let r = s.apply_fill("BTC", -2.0, 90.0, 4);
        assert_eq!(r, -20.0);
        assert_eq!(s.positions["BTC"].qty, -1.0);
        assert_eq!(s.positions["BTC"].avg_price, 90.0);
        assert_eq!(s.trades_closed, 1);
        assert_eq!(s.trades_won, 0);
        assert_eq!(s.cum_realized_usd, 0.0);
        // close the short at 80 → +10, leg removed
        assert_eq!(s.apply_fill("BTC", 1.0, 80.0, 5), 10.0);
        assert!(!s.positions.contains_key("BTC"));
        assert_eq!(s.trades_won, 1);
        assert!(s.is_flat());
    }

    #[test]
    fn a_leg_reduced_over_several_fills_is_one_trade_classified_on_its_total() {
        let mut s = BookState::new("t");
        s.apply_fill("SOL", 2.0, 100.0, 1);
        // Half out at +$50, remainder out at -$10: one $40 winner.
        assert_eq!(s.apply_fill("SOL", -1.0, 150.0, 2), 50.0);
        assert_eq!(s.trades_closed, 0);
        assert_eq!(s.apply_fill("SOL", -1.0, 90.0, 3), -10.0);
        assert_eq!(s.trades_closed, 1);
        assert_eq!(
            s.trades_won, 1,
            "the last fill lost, but the trade made $40"
        );
        assert!((s.cum_realized_usd - 40.0).abs() < 1e-9);

        // Reverse order: -$10 then +$50 is the same $40 winner.
        let mut s = BookState::new("t");
        s.apply_fill("SOL", 2.0, 100.0, 1);
        s.apply_fill("SOL", -1.0, 90.0, 2);
        s.apply_fill("SOL", -1.0, 150.0, 3);
        assert_eq!((s.trades_closed, s.trades_won), (1, 1));

        // A genuinely losing leg still counts as a loss.
        let mut s = BookState::new("t");
        s.apply_fill("SOL", 2.0, 100.0, 1);
        s.apply_fill("SOL", -1.0, 95.0, 2);
        s.apply_fill("SOL", -1.0, 90.0, 3);
        assert_eq!((s.trades_closed, s.trades_won), (1, 0));

        // A flip starts a fresh leg: the next close is judged on its own.
        let mut s = BookState::new("t");
        s.apply_fill("SOL", 1.0, 100.0, 1);
        s.apply_fill("SOL", -2.0, 80.0, 2); // close -$20, open short 1 @ 80
        assert_eq!((s.trades_closed, s.trades_won), (1, 0));
        assert_eq!(s.positions["SOL"].realized_pnl, 0.0);
        s.apply_fill("SOL", 1.0, 70.0, 3); // short closed +$10
        assert_eq!((s.trades_closed, s.trades_won), (2, 1));
    }

    #[test]
    fn a_venue_fill_that_closes_a_float_noisy_accumulation_still_counts_as_closed() {
        // 0.1 + 0.2 != 0.3 exactly in f64; a venue-reported fill of the
        // "same" quantity leaves a same-sign residual of a few ULPs, which
        // must not take the reducing (not closed) branch just because it
        // isn't exactly 0.0.
        let mut s = BookState::new("t");
        s.apply_fill("SOL", 0.1, 100.0, 1);
        s.apply_fill("SOL", 0.2, 100.0, 2);
        let qty_before = s.positions["SOL"].qty;
        assert_ne!(qty_before, 0.3, "fixture assumption: float noise present");
        s.apply_fill("SOL", -0.3, 110.0, 3);
        assert_eq!((s.trades_closed, s.trades_won), (1, 1));
        assert!(!s.positions.contains_key("SOL"));
    }

    #[test]
    fn persist_roundtrip_and_instance_guard() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("state.json");
        let mut s = BookState::new("a");
        s.apply_fill("ETH", 2.0, 10.0, 1);
        s.persist(&p).unwrap();
        let back = BookState::load_or_new(&p, "a").unwrap();
        assert_eq!(back, s);
        assert!(BookState::load_or_new(&p, "b").is_err());
        std::fs::write(&p, "{").unwrap();
        assert!(BookState::load_or_new(&p, "a").is_err());
        let fresh = BookState::load_or_new(&dir.path().join("none.json"), "a").unwrap();
        assert!(fresh.positions.is_empty());
    }

    #[test]
    fn marks_and_drawdown() {
        let mut s = BookState::new("t");
        s.apply_fill("SOL", 10.0, 100.0, 1);
        let px: std::collections::HashMap<String, f64> = [("SOL".to_string(), 90.0)].into();
        assert_eq!(s.unrealized_usd(&px), -100.0);
        assert_eq!(s.gross_usd(&px), 900.0);
        assert_eq!(s.net_usd(&px), 900.0);
        s.observe_equity(1, 1000.0);
        s.observe_equity(2, 950.0);
        s.observe_equity(3, 980.0);
        assert_eq!(s.peak_equity, 1000.0);
        assert_eq!(s.max_drawdown_usd, 50.0);
    }
}
