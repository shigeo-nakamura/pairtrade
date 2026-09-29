//! DRY_RUN (G1 paper) fill simulation against the public trade tape.
//!
//! Conservative queue model: a virtual quote at price P joins the BACK of the
//! displayed queue at P (queue_ahead = displayed size at P when placed; 0 if
//! P is not a displayed level). A trade at exactly P against our side eats
//! queue_ahead first and only the remainder fills us (partial fills allowed).
//! A trade THROUGH P (a sell below our bid, a buy above our ask) fills us
//! completely. Any requote (new price or size) is a new virtual order at the
//! back of the queue: Arcus modify may be cancel-replace, so priority is not
//! assumed to survive it.
//!
//! Time: a virtual quote only sees prints stamped at or after its placement
//! (Codex P1, pairtrade#361): a print that happened before we "placed" could
//! neither queue ahead of us nor trade through us. Placement time is in venue
//! time (µs), taken by the caller as the later of the book frame's exchange
//! timestamp and the newest print already applied (+1 µs), so local clock
//! skew never enters. A print still in flight when we placed and stamped
//! before both is ignored too; one stamped between them can still count,
//! which is the residual optimism of this model.

use crate::logic::QSide;
use dex_connector::{OrderBookLevel, OrderSide};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct VirtualQuote {
    pub side: QSide,
    pub px: Decimal,
    pub remaining: Decimal,
    pub queue_ahead: Decimal,
    /// Venue time (µs) the quote was placed; earlier prints never touch it.
    pub placed_ts_us: u64,
}

/// Venue-time placement stamp for a new virtual quote: the later of the
/// book frame's exchange time and the newest print already applied (+1 µs).
/// No local clock involved (see module docs).
pub fn placement_ts_us(book_ts_ms: Option<u64>, newest_print_ts_us: u64) -> u64 {
    let book_us = book_ts_ms.map_or(0, |ms| ms.saturating_mul(1_000));
    book_us.max(newest_print_ts_us.saturating_add(1))
}

/// Join the back of the queue at `px` on `side` of the book.
pub fn join(
    side: QSide,
    px: Decimal,
    qty: Decimal,
    levels: &[OrderBookLevel],
    placed_ts_us: u64,
) -> VirtualQuote {
    let queue_ahead = levels
        .iter()
        .find(|l| l.price == px)
        .map(|l| l.size)
        .unwrap_or(Decimal::ZERO);
    VirtualQuote {
        side,
        px,
        remaining: qty,
        queue_ahead,
        placed_ts_us,
    }
}

/// Apply one public trade (`taker` = aggressor side). Returns the quantity
/// that filled our virtual quote.
pub fn apply_trade(
    q: &mut VirtualQuote,
    ts_us: u64,
    px: Decimal,
    qty: Decimal,
    taker: OrderSide,
) -> Decimal {
    if q.remaining <= Decimal::ZERO || qty <= Decimal::ZERO || ts_us < q.placed_ts_us {
        return Decimal::ZERO;
    }
    // Only an aggressor on the other side can hit us.
    let against_us = matches!(
        (q.side, taker),
        (QSide::Bid, OrderSide::Short) | (QSide::Ask, OrderSide::Long)
    );
    if !against_us {
        return Decimal::ZERO;
    }
    let through = match q.side {
        QSide::Bid => px < q.px,
        QSide::Ask => px > q.px,
    };
    let fill = if through {
        q.queue_ahead = Decimal::ZERO;
        q.remaining
    } else if px == q.px {
        let eaten = qty.min(q.queue_ahead);
        q.queue_ahead -= eaten;
        (qty - eaten).min(q.remaining)
    } else {
        Decimal::ZERO
    };
    q.remaining -= fill;
    fill
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn levels() -> Vec<OrderBookLevel> {
        vec![
            OrderBookLevel {
                price: d("83642.9"),
                size: d("0.3"),
            },
            OrderBookLevel {
                price: d("83642.8"),
                size: d("1.2"),
            },
        ]
    }

    #[test]
    fn joins_behind_the_displayed_size_at_its_price() {
        let q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 0);
        assert_eq!(q.queue_ahead, d("0.3"));
        let q = join(QSide::Bid, d("83643.0"), d("0.12"), &levels(), 0);
        assert_eq!(q.queue_ahead, Decimal::ZERO);
    }

    #[test]
    fn trades_at_our_price_eat_the_queue_first_then_fill_partially() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 0);
        assert_eq!(
            apply_trade(&mut q, 10, d("83642.9"), d("0.25"), OrderSide::Short),
            Decimal::ZERO
        );
        assert_eq!(q.queue_ahead, d("0.05"));
        // 0.05 ahead, then 0.03 to us
        assert_eq!(
            apply_trade(&mut q, 10, d("83642.9"), d("0.08"), OrderSide::Short),
            d("0.03")
        );
        assert_eq!(q.remaining, d("0.09"));
        // capped at what is left
        assert_eq!(
            apply_trade(&mut q, 10, d("83642.9"), d("5"), OrderSide::Short),
            d("0.09")
        );
        assert_eq!(q.remaining, Decimal::ZERO);
        assert_eq!(
            apply_trade(&mut q, 10, d("83642.9"), d("5"), OrderSide::Short),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_trade_through_our_price_fills_everything_left() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 0);
        assert_eq!(
            apply_trade(&mut q, 10, d("83642.8"), d("0.001"), OrderSide::Short),
            d("0.12")
        );
        let mut a = join(QSide::Ask, d("83643.0"), d("0.12"), &[], 0);
        assert_eq!(
            apply_trade(&mut a, 10, d("83643.1"), d("0.001"), OrderSide::Long),
            d("0.12")
        );
    }

    #[test]
    fn same_side_aggressors_and_trades_away_from_us_do_not_fill() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &[], 0);
        assert_eq!(
            apply_trade(&mut q, 10, d("83642.9"), d("1"), OrderSide::Long),
            Decimal::ZERO
        );
        assert_eq!(
            apply_trade(&mut q, 10, d("83643.0"), d("1"), OrderSide::Short),
            Decimal::ZERO
        );
        let mut a = join(QSide::Ask, d("83643.0"), d("0.12"), &[], 0);
        assert_eq!(
            apply_trade(&mut a, 10, d("83642.9"), d("1"), OrderSide::Long),
            Decimal::ZERO
        );
    }

    #[test]
    fn prints_stamped_before_placement_neither_queue_nor_fill() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 1_000);
        // Stale trade-through and stale at-price prints: ignored.
        assert_eq!(
            apply_trade(&mut q, 999, d("83642.8"), d("1"), OrderSide::Short),
            Decimal::ZERO
        );
        assert_eq!(
            apply_trade(&mut q, 999, d("83642.9"), d("1"), OrderSide::Short),
            Decimal::ZERO
        );
        assert_eq!(q.queue_ahead, d("0.3"));
        assert_eq!(q.remaining, d("0.12"));
        // At/after placement they count.
        assert_eq!(
            apply_trade(&mut q, 1_000, d("83642.9"), d("0.4"), OrderSide::Short),
            d("0.1")
        );
        // A requote resets the clock: a print before the new placement is ignored.
        let mut r = join(QSide::Ask, d("83643.0"), d("0.12"), &[], 5_000);
        assert_eq!(
            apply_trade(&mut r, 4_999, d("83643.1"), d("1"), OrderSide::Long),
            Decimal::ZERO
        );
        assert_eq!(
            apply_trade(&mut r, 5_001, d("83643.1"), d("1"), OrderSide::Long),
            d("0.12")
        );
    }

    #[test]
    fn placement_uses_venue_time_never_before_an_applied_print() {
        assert_eq!(placement_ts_us(Some(2_000), 1_500_000), 2_000_000);
        assert_eq!(placement_ts_us(Some(1_000), 1_500_000), 1_500_001);
        assert_eq!(placement_ts_us(None, 7), 8);
    }

    #[test]
    fn a_requote_rejoins_at_the_back() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 0);
        apply_trade(&mut q, 10, d("83642.9"), d("0.3"), OrderSide::Short);
        assert_eq!(q.queue_ahead, Decimal::ZERO);
        // Requote (even to the same price) = new virtual order behind the book.
        let q2 = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 0);
        assert_eq!(q2.queue_ahead, d("0.3"));
    }
}
