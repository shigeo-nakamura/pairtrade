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

/// Whether a public print may touch the virtual quotes at all (Codex P1,
/// pairtrade#361): not while the sim is halted, the tape is down, the
/// runtime is halted, or a flatten is pending — those states must not add
/// fills the flatten then has to chase.
pub fn prints_apply(
    tape_ready: bool,
    sim_halted: bool,
    halted: bool,
    flatten_pending: bool,
) -> bool {
    tape_ready && !sim_halted && !halted && !flatten_pending
}

/// The touch a paper flatten may price at: only a FRESH book, judged by the
/// same predicate the tick plan uses (Codex P1, pairtrade#361). A stale book
/// is treated exactly like no book.
pub fn fresh_touch(
    book: Option<(Decimal, Decimal, Option<u64>)>,
    now_ms: u64,
    stale_secs: u64,
) -> Option<(Decimal, Decimal)> {
    let (bid, ask, ts) = book?;
    (!crate::logic::book_stale(ts, now_ms, stale_secs)).then_some((bid, ask))
}

/// The paper flatten sequence. With a book it is `flatten_steps` (cancel
/// quotes, then the IOC at the touch). Without one the IOC cannot be
/// priced, but every virtual quote is still pulled at once so nothing can
/// fill while the flatten waits for a book (Codex P1, pairtrade#361).
pub fn paper_flatten_steps(
    has_book: bool,
    inv_qty: Decimal,
    resting_bid: bool,
    resting_ask: bool,
) -> Vec<crate::logic::Step> {
    use crate::logic::{flatten_steps, Step};
    if has_book {
        return flatten_steps(inv_qty, resting_bid, resting_ask);
    }
    let mut steps = Vec::new();
    if resting_bid {
        steps.push(Step::Cancel(QSide::Bid));
    }
    if resting_ask {
        steps.push(Step::Cancel(QSide::Ask));
    }
    steps
}

/// Simulated order/fill id: `run_id` (the process start, ms) keeps ids
/// unique across restarts even though `seq` starts again at 1, so a paper
/// fill after a restart never collides with a booked id (Codex P2,
/// pairtrade#361).
pub fn sim_id(run_id: u64, seq: u64, tag: &str) -> String {
    format!("sim-{run_id}-{seq}-{tag}")
}

/// Apply a print to a COPY of `q` and hand the fill to `book`; the updated
/// quote is returned only when booking succeeded, so a fill that could not
/// be recorded is never consumed (Codex P2, pairtrade#361). `Ok(None)` =
/// the print did not touch the quote.
pub fn try_fill<E>(
    q: &VirtualQuote,
    ts_us: u64,
    px: Decimal,
    qty: Decimal,
    taker: OrderSide,
    book: impl FnOnce(Decimal) -> Result<(), E>,
) -> Result<Option<VirtualQuote>, E> {
    let mut next = q.clone();
    let filled = apply_trade(&mut next, ts_us, px, qty, taker);
    if filled.is_zero() {
        return Ok(None);
    }
    book(filled)?;
    Ok(Some(next))
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
    fn sim_ids_never_collide_across_restarts() {
        let mut ledger = crate::ledger::Ledger::new("dry_run", "2026-09-30");
        let first_run = sim_id(1_790_700_000_000, 1, "ioc");
        let fill = crate::ledger::FillIn {
            trade_id: first_run.clone(),
            buy: true,
            qty: d("0.1"),
            px: d("83642.9"),
            fee: Decimal::ZERO,
            maker: false,
            order_id: "sim-ioc".to_string(),
            fee_estimated: false,
        };
        crate::ledger::book_fill(&mut ledger, &fill, 1, |_| Ok(())).unwrap();
        // Restart: seq starts at 1 again, the run id differs.
        let second_run = sim_id(1_790_700_060_000, 1, "ioc");
        assert_ne!(first_run, second_run);
        assert!(!ledger.has_booked(&second_run));
    }

    #[test]
    fn a_fill_that_fails_to_book_does_not_consume_the_quote() {
        let q = join(QSide::Bid, d("83642.9"), d("0.12"), &levels(), 0);
        let err: Result<Option<VirtualQuote>, &str> =
            try_fill(&q, 10, d("83642.9"), d("0.35"), OrderSide::Short, |_| {
                Err("disk")
            });
        assert!(err.is_err());
        // Original untouched: same queue and size.
        assert_eq!(q.queue_ahead, d("0.3"));
        assert_eq!(q.remaining, d("0.12"));
        let mut booked = Decimal::ZERO;
        let ok = try_fill(&q, 10, d("83642.9"), d("0.35"), OrderSide::Short, |f| {
            booked = f;
            Ok::<(), &str>(())
        })
        .unwrap()
        .unwrap();
        assert_eq!(booked, d("0.05"));
        assert_eq!(ok.remaining, d("0.07"));
        assert_eq!(ok.queue_ahead, Decimal::ZERO);
        // A print that does not touch us books nothing.
        assert_eq!(
            try_fill(&q, 10, d("83643.0"), d("1"), OrderSide::Short, |_| Err(
                "unused"
            )),
            Ok(None)
        );
    }

    #[test]
    fn a_halt_without_a_book_pulls_quotes_and_later_prints_cannot_fill() {
        use crate::logic::Step;
        use std::collections::HashMap;
        let mut virt: HashMap<QSide, VirtualQuote> = HashMap::new();
        virt.insert(
            QSide::Bid,
            join(QSide::Bid, d("83642.9"), d("0.12"), &[], 0),
        );
        virt.insert(
            QSide::Ask,
            join(QSide::Ask, d("83643.0"), d("0.12"), &[], 0),
        );
        // Halt with inventory and no book: no IOC, but both quotes pulled.
        let steps = paper_flatten_steps(false, d("0.1"), true, true);
        assert_eq!(
            steps,
            vec![Step::Cancel(QSide::Bid), Step::Cancel(QSide::Ask)]
        );
        for s in steps {
            if let Step::Cancel(side) = s {
                virt.remove(&side);
            }
        }
        assert!(virt.is_empty());
        // A print arriving now is not applied (halted / flatten pending).
        assert!(!prints_apply(true, false, true, false));
        assert!(!prints_apply(true, false, false, true));
        assert!(!prints_apply(false, false, false, false));
        assert!(!prints_apply(true, true, false, false));
        assert!(prints_apply(true, false, false, false));
        // With a book the flatten includes its IOC.
        assert!(matches!(
            paper_flatten_steps(true, d("0.1"), false, false).as_slice(),
            [Step::Ioc { .. }]
        ));
    }

    #[test]
    fn a_paper_flatten_prices_only_at_a_fresh_touch() {
        use crate::logic::Step;
        let book = Some((d("83642.9"), d("83643.0"), Some(10_000)));
        // Stale (older than 5 s at plan time): like no book, quotes pulled,
        // no IOC, so no fill is booked.
        let touch = fresh_touch(book, 15_001, 5);
        assert_eq!(touch, None);
        assert_eq!(
            paper_flatten_steps(touch.is_some(), d("0.1"), true, true),
            vec![Step::Cancel(QSide::Bid), Step::Cancel(QSide::Ask)]
        );
        // REST-served book without a feed time: also not fresh.
        assert_eq!(fresh_touch(Some((d("1"), d("2"), None)), 15_000, 5), None);
        // Fresh: the IOC sells at that bid.
        let touch = fresh_touch(book, 15_000, 5);
        assert_eq!(touch, Some((d("83642.9"), d("83643.0"))));
        assert!(matches!(
            paper_flatten_steps(touch.is_some(), d("0.1"), false, false).as_slice(),
            [Step::Ioc {
                side: OrderSide::Short,
                ..
            }]
        ));
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
