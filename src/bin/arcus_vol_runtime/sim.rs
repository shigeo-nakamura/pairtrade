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

use crate::logic::QSide;
use dex_connector::{OrderBookLevel, OrderSide};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub struct VirtualQuote {
    pub side: QSide,
    pub px: Decimal,
    pub remaining: Decimal,
    pub queue_ahead: Decimal,
}

/// Join the back of the queue at `px` on `side` of the book.
pub fn join(side: QSide, px: Decimal, qty: Decimal, levels: &[OrderBookLevel]) -> VirtualQuote {
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
    }
}

/// Apply one public trade (`taker` = aggressor side). Returns the quantity
/// that filled our virtual quote.
pub fn apply_trade(q: &mut VirtualQuote, px: Decimal, qty: Decimal, taker: OrderSide) -> Decimal {
    if q.remaining <= Decimal::ZERO || qty <= Decimal::ZERO {
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
        let q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels());
        assert_eq!(q.queue_ahead, d("0.3"));
        let q = join(QSide::Bid, d("83643.0"), d("0.12"), &levels());
        assert_eq!(q.queue_ahead, Decimal::ZERO);
    }

    #[test]
    fn trades_at_our_price_eat_the_queue_first_then_fill_partially() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels());
        assert_eq!(
            apply_trade(&mut q, d("83642.9"), d("0.25"), OrderSide::Short),
            Decimal::ZERO
        );
        assert_eq!(q.queue_ahead, d("0.05"));
        // 0.05 ahead, then 0.03 to us
        assert_eq!(
            apply_trade(&mut q, d("83642.9"), d("0.08"), OrderSide::Short),
            d("0.03")
        );
        assert_eq!(q.remaining, d("0.09"));
        // capped at what is left
        assert_eq!(
            apply_trade(&mut q, d("83642.9"), d("5"), OrderSide::Short),
            d("0.09")
        );
        assert_eq!(q.remaining, Decimal::ZERO);
        assert_eq!(
            apply_trade(&mut q, d("83642.9"), d("5"), OrderSide::Short),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_trade_through_our_price_fills_everything_left() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels());
        assert_eq!(
            apply_trade(&mut q, d("83642.8"), d("0.001"), OrderSide::Short),
            d("0.12")
        );
        let mut a = join(QSide::Ask, d("83643.0"), d("0.12"), &[]);
        assert_eq!(
            apply_trade(&mut a, d("83643.1"), d("0.001"), OrderSide::Long),
            d("0.12")
        );
    }

    #[test]
    fn same_side_aggressors_and_trades_away_from_us_do_not_fill() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &[]);
        assert_eq!(
            apply_trade(&mut q, d("83642.9"), d("1"), OrderSide::Long),
            Decimal::ZERO
        );
        assert_eq!(
            apply_trade(&mut q, d("83643.0"), d("1"), OrderSide::Short),
            Decimal::ZERO
        );
        let mut a = join(QSide::Ask, d("83643.0"), d("0.12"), &[]);
        assert_eq!(
            apply_trade(&mut a, d("83642.9"), d("1"), OrderSide::Long),
            Decimal::ZERO
        );
    }

    #[test]
    fn a_requote_rejoins_at_the_back() {
        let mut q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels());
        apply_trade(&mut q, d("83642.9"), d("0.3"), OrderSide::Short);
        assert_eq!(q.queue_ahead, Decimal::ZERO);
        // Requote (even to the same price) = new virtual order behind the book.
        let q2 = join(QSide::Bid, d("83642.9"), d("0.12"), &levels());
        assert_eq!(q2.queue_ahead, d("0.3"));
    }
}
