//! Position / PnL book-keeping, the UTC-day rollover and the stops.
//!
//! Net PnL counts fees as a cost: `realized − fees + unrealized`. The daily
//! figure measures unrealized PnL from the value it had at the day's start,
//! so an open position carried over midnight is not charged twice.
//!
//! Sticky halt (cumulative stop): the bot writes `HALT` in the state dir and
//! stays halted while that file exists (and across restarts). To resume,
//! stop the bot's losses review, then delete `HALT` by hand: the bot notices,
//! clears the halt and re-bases the cumulative stop at the current net
//! (`cum_baseline`), so the next halt needs a further `CUM_STOP_USD` loss.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Position {
    /// Signed base quantity (+long).
    pub qty: Decimal,
    /// Average entry of the open quantity (0 when flat).
    pub avg_px: Decimal,
    /// When the position last left flat (ms); drives MAX_HOLD.
    pub opened_at_ms: Option<u64>,
}

impl Position {
    /// Apply a fill; returns the realized PnL (before fees).
    pub fn apply(&mut self, buy: bool, qty: Decimal, px: Decimal, now_ms: u64) -> Decimal {
        if qty <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let signed = if buy { qty } else { -qty };
        let old = self.qty;
        if old.is_zero() || (old > Decimal::ZERO) == buy {
            let total = old.abs() + qty;
            self.avg_px = (old.abs() * self.avg_px + qty * px) / total;
            self.qty = old + signed;
            if old.is_zero() {
                self.opened_at_ms = Some(now_ms);
            }
            return Decimal::ZERO;
        }
        let closed = qty.min(old.abs());
        let direction = if old > Decimal::ZERO {
            Decimal::ONE
        } else {
            Decimal::NEGATIVE_ONE
        };
        let realized = closed * (px - self.avg_px) * direction;
        self.qty = old + signed;
        if self.qty.is_zero() {
            self.avg_px = Decimal::ZERO;
            self.opened_at_ms = None;
        } else if (self.qty > Decimal::ZERO) != (old > Decimal::ZERO) {
            // Flipped through flat: the remainder opens fresh at this price.
            self.avg_px = px;
            self.opened_at_ms = Some(now_ms);
        }
        realized
    }

    pub fn unrealized(&self, mark: Decimal) -> Decimal {
        if self.qty.is_zero() {
            Decimal::ZERO
        } else {
            self.qty * (mark - self.avg_px)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Ledger {
    /// "dry_run" / "live": a state file is never reused across modes.
    pub mode: String,
    /// UTC day (YYYY-MM-DD) the day_* fields belong to.
    pub day: String,
    pub position: Position,
    pub day_realized: Decimal,
    pub day_fees: Decimal,
    pub day_start_unrealized: Decimal,
    pub day_volume: Decimal,
    pub day_maker_volume: Decimal,
    pub day_taker_volume: Decimal,
    pub cum_realized: Decimal,
    pub cum_fees: Decimal,
    pub cum_volume: Decimal,
    pub cum_maker_volume: Decimal,
    pub cum_taker_volume: Decimal,
    pub fills: u64,
    /// Cumulative net at the last manual HALT clear (see module docs).
    pub cum_baseline: Decimal,
    pub day_halt: bool,
    pub sticky_halt: bool,
    pub sticky_reason: Option<String>,
}

impl Ledger {
    pub fn new(mode: &str, day: &str) -> Self {
        Ledger {
            mode: mode.to_string(),
            day: day.to_string(),
            ..Ledger::default()
        }
    }

    pub fn record_fill(
        &mut self,
        buy: bool,
        qty: Decimal,
        px: Decimal,
        fee: Decimal,
        maker: bool,
        now_ms: u64,
    ) -> Decimal {
        let realized = self.position.apply(buy, qty, px, now_ms);
        let notional = qty * px;
        self.day_realized += realized;
        self.cum_realized += realized;
        self.day_fees += fee;
        self.cum_fees += fee;
        self.day_volume += notional;
        self.cum_volume += notional;
        if maker {
            self.day_maker_volume += notional;
            self.cum_maker_volume += notional;
        } else {
            self.day_taker_volume += notional;
            self.cum_taker_volume += notional;
        }
        self.fills += 1;
        realized
    }

    /// Start a new UTC day. Returns true when it rolled.
    pub fn rollover(&mut self, today: &str, mark: Decimal) -> bool {
        if self.day == today {
            return false;
        }
        self.day = today.to_string();
        self.day_realized = Decimal::ZERO;
        self.day_fees = Decimal::ZERO;
        self.day_volume = Decimal::ZERO;
        self.day_maker_volume = Decimal::ZERO;
        self.day_taker_volume = Decimal::ZERO;
        self.day_start_unrealized = self.position.unrealized(mark);
        self.day_halt = false;
        true
    }

    pub fn daily_net(&self, mark: Decimal) -> Decimal {
        self.day_realized - self.day_fees + self.position.unrealized(mark)
            - self.day_start_unrealized
    }

    pub fn cum_net(&self, mark: Decimal) -> Decimal {
        self.cum_realized - self.cum_fees + self.position.unrealized(mark)
    }

    /// Net cost per $1M of volume (positive = we paid).
    pub fn cost_per_million(&self, mark: Decimal) -> Option<Decimal> {
        (self.cum_volume > Decimal::ZERO)
            .then(|| -self.cum_net(mark) / self.cum_volume * Decimal::from(1_000_000))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Halt {
    Sticky(String),
    Kill,
    Day,
}

impl Halt {
    pub fn label(&self) -> String {
        match self {
            Halt::Sticky(r) => format!("sticky: {r}"),
            Halt::Kill => "kill_switch".to_string(),
            Halt::Day => "daily_stop".to_string(),
        }
    }
}

/// Update the stop flags from the current marks and return the halt in
/// force, if any (sticky > kill > day). `halt_file` is whether `HALT`
/// exists; a sticky halt whose file was deleted by hand is cleared and the
/// cumulative stop re-based (see module docs). Returns (halt, events).
pub fn risk_check(
    l: &mut Ledger,
    mark: Decimal,
    kill: bool,
    halt_file: bool,
    daily_stop: Decimal,
    cum_stop: Decimal,
) -> (Option<Halt>, Vec<String>) {
    let mut events = Vec::new();
    if l.sticky_halt && !halt_file {
        l.sticky_halt = false;
        l.sticky_reason = None;
        l.cum_baseline = l.cum_net(mark);
        events.push(format!(
            "HALT cleared by hand; cumulative stop re-based at net {}",
            l.cum_baseline.round_dp(2)
        ));
    }
    let cum_loss = l.cum_baseline - l.cum_net(mark);
    if !l.sticky_halt && cum_loss > cum_stop {
        l.sticky_halt = true;
        let reason = format!(
            "cumulative net loss {} > {} (since baseline)",
            cum_loss.round_dp(2),
            cum_stop
        );
        events.push(format!("STICKY HALT: {reason}"));
        l.sticky_reason = Some(reason);
    }
    let day_loss = -l.daily_net(mark);
    if !l.day_halt && day_loss > daily_stop {
        l.day_halt = true;
        events.push(format!(
            "DAILY STOP: net loss {} > {} until the next UTC day",
            day_loss.round_dp(2),
            daily_stop
        ));
    }
    let halt = if l.sticky_halt {
        Some(Halt::Sticky(l.sticky_reason.clone().unwrap_or_default()))
    } else if kill {
        Some(Halt::Kill)
    } else if l.day_halt {
        Some(Halt::Day)
    } else {
        None
    };
    (halt, events)
}

/// A fill waiting for its +5/+30/+60 s markouts.
#[derive(Debug, Clone)]
pub struct PendingMarkout {
    pub fill_id: String,
    pub ts_ms: u64,
    pub px: Decimal,
    pub buy: bool,
    pub horizons: Vec<u64>,
}

/// Markout in bp from the fill's side: positive = the mid moved our way.
pub fn markout_bps(buy: bool, px: Decimal, mid: Decimal) -> Decimal {
    if px.is_zero() {
        return Decimal::ZERO;
    }
    let sign = if buy {
        Decimal::ONE
    } else {
        Decimal::NEGATIVE_ONE
    };
    sign * (mid - px) / px * Decimal::from(10_000)
}

/// Pop every horizon that is due; returns (fill_id, horizon_secs, bps).
pub fn due_markouts(
    pending: &mut Vec<PendingMarkout>,
    now_ms: u64,
    mid: Decimal,
) -> Vec<(String, u64, Decimal)> {
    let mut out = Vec::new();
    for p in pending.iter_mut() {
        p.horizons.retain(|h| {
            if now_ms >= p.ts_ms + h * 1_000 {
                out.push((p.fill_id.clone(), *h, markout_bps(p.buy, p.px, mid)));
                false
            } else {
                true
            }
        });
    }
    pending.retain(|p| !p.horizons.is_empty());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn position_averages_realizes_and_flips_with_non_binary_prices() {
        let mut p = Position::default();
        assert_eq!(p.apply(true, d("0.1"), d("83642.9"), 1), Decimal::ZERO);
        assert_eq!(p.apply(true, d("0.2"), d("83643.1"), 2), Decimal::ZERO);
        // avg = (0.1*83642.9 + 0.2*83643.1)/0.3 = 83643.0333…
        assert_eq!(p.avg_px.round_dp(4), d("83643.0333"));
        assert_eq!(p.opened_at_ms, Some(1));
        // sell 0.1 at 83643.3: realized 0.1 * (83643.3 − 83643.0333…) = 0.02666…
        let r = p.apply(false, d("0.1"), d("83643.3"), 3);
        assert_eq!(r.round_dp(5), d("0.02667"));
        assert_eq!(p.qty, d("0.2"));
        assert_eq!(p.opened_at_ms, Some(1));
        // sell 0.3: closes 0.2, flips 0.1 short at 83642.7
        let r = p.apply(false, d("0.3"), d("83642.7"), 4);
        assert_eq!(r.round_dp(5), d("-0.06667"));
        assert_eq!(p.qty, d("-0.1"));
        assert_eq!(p.avg_px, d("83642.7"));
        assert_eq!(p.opened_at_ms, Some(4));
        // buy back 0.1 at 83642.6: short gains 0.01
        assert_eq!(p.apply(true, d("0.1"), d("83642.6"), 5), d("0.01"));
        assert_eq!(p, Position::default());
    }

    #[test]
    fn ledger_nets_fees_and_splits_maker_taker_volume() {
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("0.1"), d("83642.9"), Decimal::ZERO, true, 1);
        l.record_fill(false, d("0.1"), d("83642.8"), d("1.882"), false, 2);
        assert_eq!(l.cum_maker_volume, d("8364.29"));
        assert_eq!(l.cum_taker_volume, d("8364.28"));
        // realized −0.01, fees 1.882
        assert_eq!(l.cum_net(d("83642.8")), d("-1.892"));
        assert_eq!(l.daily_net(d("83642.8")), d("-1.892"));
        let cost = l.cost_per_million(d("83642.8")).unwrap();
        assert_eq!(cost.round_dp(2), d("113.10"));
    }

    #[test]
    fn rollover_resets_the_day_and_rebases_carried_unrealized() {
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("0.1"), d("100000"), d("2"), false, 1);
        l.day_halt = true;
        // mark 99990: unrealized −1
        assert!(!l.rollover("2026-09-30", d("99990")));
        assert!(l.rollover("2026-10-01", d("99990")));
        assert!(!l.day_halt);
        assert_eq!(l.day_fees, Decimal::ZERO);
        assert_eq!(l.day_start_unrealized, d("-1"));
        assert_eq!(l.daily_net(d("99990")), Decimal::ZERO);
        assert_eq!(l.daily_net(d("99980")), d("-1"));
        // cumulative keeps everything: −2 fee −2 unrealized
        assert_eq!(l.cum_net(d("99980")), d("-4"));
    }

    #[test]
    fn daily_stop_halts_until_rollover_and_cum_stop_is_sticky() {
        let (daily, cum) = (d("50"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("1"), d("100000"), Decimal::ZERO, true, 1);
        // −49.9: nothing
        assert_eq!(
            risk_check(&mut l, d("99950.1"), false, false, daily, cum).0,
            None
        );
        // −60: day halt
        let (h, ev) = risk_check(&mut l, d("99940"), false, false, daily, cum);
        assert_eq!(h, Some(Halt::Day));
        assert_eq!(ev.len(), 1);
        // recovering intraday does not lift it
        assert_eq!(
            risk_check(&mut l, d("100000"), false, false, daily, cum).0,
            Some(Halt::Day)
        );
        l.rollover("2026-10-01", d("100000"));
        assert_eq!(
            risk_check(&mut l, d("100000"), false, false, daily, cum).0,
            None
        );
        // kill switch outranks a day halt
        assert_eq!(
            risk_check(&mut l, d("100000"), true, false, daily, cum).0,
            Some(Halt::Kill)
        );
        // −300 cumulative: sticky, survives rollover and a recovery
        let (h, _) = risk_check(&mut l, d("99700"), false, true, daily, cum);
        assert!(matches!(h, Some(Halt::Sticky(_))));
        l.rollover("2026-10-02", d("100000"));
        assert!(matches!(
            risk_check(&mut l, d("100000"), false, true, daily, cum).0,
            Some(Halt::Sticky(_))
        ));
    }

    #[test]
    fn deleting_halt_by_hand_clears_and_rebases_the_cumulative_stop() {
        let (daily, cum) = (d("1000"), d("250"));
        let mut l = Ledger::new("dry_run", "2026-09-30");
        l.record_fill(true, d("1"), d("100000"), Decimal::ZERO, true, 1);
        let (h, _) = risk_check(&mut l, d("99700"), false, true, daily, cum);
        assert!(matches!(h, Some(Halt::Sticky(_))));
        // HALT file removed → cleared, baseline = −300
        let (h, ev) = risk_check(&mut l, d("99700"), false, false, daily, cum);
        assert_eq!(h, None);
        assert_eq!(ev.len(), 1);
        assert_eq!(l.cum_baseline, d("-300"));
        // another −200 is inside the re-based stop, −260 is not
        assert_eq!(
            risk_check(&mut l, d("99500"), false, false, daily, cum).0,
            None
        );
        assert!(matches!(
            risk_check(&mut l, d("99440"), false, false, daily, cum).0,
            Some(Halt::Sticky(_))
        ));
    }

    #[test]
    fn markouts_are_signed_from_the_fill_side_and_pop_when_due() {
        assert_eq!(markout_bps(true, d("100000"), d("100010")), d("1"));
        assert_eq!(markout_bps(false, d("100000"), d("100010")), d("-1"));
        let mut pending = vec![PendingMarkout {
            fill_id: "f".into(),
            ts_ms: 1_000,
            px: d("100000"),
            buy: true,
            horizons: vec![5, 30, 60],
        }];
        assert!(due_markouts(&mut pending, 5_999, d("1")).is_empty());
        let out = due_markouts(&mut pending, 6_000, d("99990"));
        assert_eq!(out, vec![("f".to_string(), 5, d("-1"))]);
        due_markouts(&mut pending, 31_000, d("1"));
        assert_eq!(pending.len(), 1);
        due_markouts(&mut pending, 61_000, d("1"));
        assert!(pending.is_empty());
    }
}
