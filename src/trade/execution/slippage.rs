//! Price-bound semantics shared by every taker-IOC sender (bot-strategy#971,
//! #978).
//!
//! A configured `slippage_bps` is a bound **against the mid** everywhere an
//! operator or a paper fill reads it. The sender therefore computes the
//! absolute limit price itself -- `mid * (1 ± slippage_bps)` off the same
//! snapshot the drift guard judged -- and hands it to
//! `create_order_taker_ioc_at`, which never re-anchors it. What the
//! percentage path did instead was hand the venue a bps figure that it
//! multiplied onto *its own* touch at submit time, so a spread that widened
//! in between moved the realised cap out with it; #971 (book runtime) and
//! #918 (engine_b_live) each converted mid-relative to touch-relative to
//! narrow that window and recorded the remainder as a residual. This module
//! is what closes it.
//!
//! [`send_limit_price`] decides the one thing the venue cannot: whether a
//! price inside the bound is marketable at all, and what that means for an
//! entry (refuse, nothing is lost) versus an exit (cross at the touch, a
//! stranded position is worse). Paper fill, pre-send drift guard and live
//! send now all read `slippage_bps` as the same number against the same
//! mid.
//!
//! The two exit branches deliberately keep the *percentage* send, because
//! their contract is "this gets flat" rather than "this respects the
//! bound": only the connector, pricing off the book at submit time and
//! adding its own tick, can guarantee an IOC crosses. An absolute price
//! computed here is a snapshot, and a snapshot can be behind the book.

use dex_connector::OrderSide;

/// Slack for the "does the bound reach the touch" comparison only.
/// `mid * (1 + b)` is not exact in `f64` -- `100.0 * 1.005` is
/// `100.49999999999999` -- so a bound that mathematically lands *on* the
/// touch can read as an ULP short of it and refuse an entry that is
/// exactly at its budget. One part in 1e9 is ~1e-5 bps: far below any
/// venue tick, and at a tie the touch itself is what gets sent (see
/// [`send_limit_price`]), so nothing sub-tick reaches the book.
const TOUCH_TIE_TOLERANCE: f64 = 1e-9;

/// The bound one send carries, decided from the mid-relative
/// `slippage_bps` and the book the sender observed itself.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SendLimit {
    /// A marketable price exists at or inside the bound: send this
    /// absolute limit price. The venue may only round it *inward*.
    Bounded(f64),
    /// Reduce-only on a book whose touch sits outside the bound: cross at
    /// the touch, and let the **connector** price that from its own live
    /// read (`create_order_taker_ioc` at its 1 bp minimum) rather than
    /// from the observation here. This branch has already given up the
    /// mid bound -- its one job is that the position gets flat -- so it
    /// wants the guarantee the percentage path carries and the absolute
    /// one cannot: the venue prices off the book at submit time, and adds
    /// its own tick, so the order always crosses. Still a bound (the best
    /// offer, not the venue's ±20 % protection price), and a position
    /// left inside a tear is the worse outcome.
    AtTouch,
    /// Reduce-only with no usable book of our own: there is no observation
    /// to price against, so fall back to the touch-relative
    /// `create_order_taker_ioc` with the configured bound against the
    /// connector's own touch. A missing observation must not strand a
    /// position.
    Unchecked,
}

/// Decide the price bound for one order. `mid` and `touch` must come from
/// the **same** observation -- the snapshot the caller's drift guard
/// judged -- and from the caller's own feed rather than the connector's
/// cache: the caller is the side that knows how fresh its quote is.
/// `touch` is `(best_bid, best_ask)`, `None` when nothing fresh was seen.
///
/// Entries are asymmetric with exits: an entry whose bound cannot be
/// checked against a book, or whose bounded price would not cross, is
/// refused (`Err`, with the reason), because not opening costs nothing.
/// An exit is always sendable, see [`SendLimit`].
///
/// A book that is crossed, one-sided or non-finite counts as no book at
/// all rather than as a tradeable one: the bound would still hold against
/// it, but the mid that produced the bound is exactly what such a book
/// calls into question.
pub fn send_limit_price(
    bound_bps: u32,
    mid: f64,
    touch: Option<(f64, f64)>,
    side: OrderSide,
    reduce_only: bool,
) -> Result<SendLimit, String> {
    let usable = touch.filter(|(bid, ask)| {
        bid.is_finite()
            && ask.is_finite()
            && *bid > 0.0
            && *ask >= *bid
            && mid.is_finite()
            && mid > 0.0
    });
    let Some((best_bid, best_ask)) = usable else {
        if reduce_only {
            return Ok(SendLimit::Unchecked);
        }
        return Err(format!(
            "no usable book to bound an entry against (mid={mid} touch={touch:?}); refusing to \
             send it"
        ));
    };
    let bound = f64::from(bound_bps) / 10_000.0;
    // `cross` is the touch the order has to reach to trade at all.
    let (limit, cross, marketable) = match side {
        OrderSide::Long => {
            let limit = mid * (1.0 + bound);
            let cross = best_ask;
            (limit, cross, limit >= cross * (1.0 - TOUCH_TIE_TOLERANCE))
        }
        OrderSide::Short => {
            let limit = mid * (1.0 - bound);
            let cross = best_bid;
            (limit, cross, limit <= cross * (1.0 + TOUCH_TIE_TOLERANCE))
        }
    };
    if marketable {
        // Never send a price that would not cross: where the bound only
        // reaches the touch within the tie tolerance, the touch is the
        // price. Everywhere else this is the bound itself.
        let limit = match side {
            OrderSide::Long => limit.max(cross),
            OrderSide::Short => limit.min(cross),
        };
        return Ok(SendLimit::Bounded(limit));
    }
    if reduce_only {
        return Ok(SendLimit::AtTouch);
    }
    Err(format!(
        "the {bound_bps}bps bound from mid={mid} stops at {limit} and does not reach the touch \
         (bid={best_bid} ask={best_ask}): no entry price within the bound"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use OrderSide::{Long, Short};

    fn bounded(r: Result<SendLimit, String>) -> f64 {
        match r {
            Ok(SendLimit::Bounded(p)) => p,
            other => panic!("expected a bounded price, got {other:?}"),
        }
    }

    /// The whole point of the absolute path: what comes back is the
    /// mid-relative bound itself, on any book shape.
    #[test]
    fn a_bounded_price_is_exactly_the_mid_relative_bound() {
        // Tight book (1 bp half-spread): the full budget survives, unlike
        // the touch-relative conversion this replaces, which had to spend
        // the half-spread and then floor to a whole bp (50 bps became 48).
        let long = bounded(send_limit_price(
            50,
            100.0,
            Some((99.99, 100.01)),
            Long,
            false,
        ));
        assert!((long - 100.5).abs() < 1e-9, "{long}");
        let short = bounded(send_limit_price(
            50,
            100.0,
            Some((99.99, 100.01)),
            Short,
            false,
        ));
        assert!((short - 99.5).abs() < 1e-9, "{short}");
        // The book that motivated #971: a 25 bps half-spread around a 1700
        // mid. The conversion sent 24 bps off the 1704.25 ask; the bound is
        // 1708.50 and that is now what goes out.
        let torn = bounded(send_limit_price(
            50,
            1700.0,
            Some((1695.75, 1704.25)),
            Long,
            false,
        ));
        assert!((torn - 1708.5).abs() < 1e-9, "{torn}");
        // A sub-basis-point-per-tick budget is expressible now; the bps
        // path had a 1 bp floor because the connector took a `u32`.
        let fine = bounded(send_limit_price(
            1,
            1700.0,
            Some((1699.9, 1700.1)),
            Long,
            false,
        ));
        assert!((fine - 1700.17).abs() < 1e-9, "{fine}");
    }

    #[test]
    fn a_bound_short_of_the_touch_refuses_an_entry_and_crosses_an_exit() {
        // 90/110 book, 50 bps budget: a buy may pay 100.5, the offer is
        // 110. Nothing inside the bound trades.
        let e = send_limit_price(50, 100.0, Some((90.0, 110.0)), Long, false).unwrap_err();
        assert!(e.contains("bid=90") && e.contains("ask=110"), "{e}");
        assert_eq!(
            send_limit_price(50, 100.0, Some((90.0, 110.0)), Long, true),
            Ok(SendLimit::AtTouch)
        );
        assert_eq!(
            send_limit_price(50, 100.0, Some((90.0, 110.0)), Short, true),
            Ok(SendLimit::AtTouch)
        );
        // Exactly at the touch is marketable: a limit resting on the
        // opposing touch still crosses, so it is sent rather than
        // refused -- and it is sent as the touch, not as the f64 product
        // `100.0 * 1.005 = 100.49999999999999`, which would have rested
        // an ULP inside the ask and never traded.
        let at = bounded(send_limit_price(
            50,
            100.0,
            Some((99.5, 100.5)),
            Long,
            false,
        ));
        assert_eq!(at, 100.5, "the tie must send the touch itself");
        let at = bounded(send_limit_price(
            50,
            100.0,
            Some((99.5, 100.5)),
            Short,
            true,
        ));
        assert_eq!(at, 99.5);
    }

    #[test]
    fn no_usable_book_refuses_an_entry_and_leaves_an_exit_to_the_connector() {
        for touch in [
            None,
            Some((101.0, 100.0)),        // crossed
            Some((0.0, 100.0)),          // no bid
            Some((f64::NAN, 100.0)),     // not a number
            Some((99.0, f64::INFINITY)), // not a number
        ] {
            assert!(
                send_limit_price(50, 100.0, touch, Long, false).is_err(),
                "{touch:?} must refuse an entry"
            );
            assert_eq!(
                send_limit_price(50, 100.0, touch, Long, true),
                Ok(SendLimit::Unchecked),
                "{touch:?} must still let an exit out"
            );
        }
        // A book whose mid is not a price is the same case: the bound is
        // computed from the mid, so an unusable mid is an unusable bound.
        for mid in [0.0, -1.0, f64::NAN] {
            assert!(send_limit_price(50, mid, Some((99.99, 100.01)), Long, false).is_err());
            assert_eq!(
                send_limit_price(50, mid, Some((99.99, 100.01)), Short, true),
                Ok(SendLimit::Unchecked)
            );
        }
    }

    /// Property: over a grid of books and budgets, a `Bounded` price is
    /// never outside the advertised bound and always crosses, and an
    /// `AtTouch` price is the opposing touch itself.
    #[test]
    fn the_sent_price_never_exceeds_the_bound_and_always_crosses() {
        for &bound in &[1u32, 5, 10, 30, 50, 100, 300, 1000] {
            for &half_bps in &[0.0, 0.1, 0.5, 1.0, 4.9, 5.0, 9.0, 25.0, 49.0, 120.0, 999.0] {
                let mid = 100.0;
                let h = half_bps / 10_000.0;
                let (bid, ask) = (mid * (1.0 - h), mid * (1.0 + h));
                let b = f64::from(bound) / 10_000.0;
                for (side, cross) in [(Long, ask), (Short, bid)] {
                    match send_limit_price(bound, mid, Some((bid, ask)), side, true).unwrap() {
                        SendLimit::Bounded(p) => {
                            let inside = match side {
                                Long => {
                                    p <= mid * (1.0 + b) * (1.0 + TOUCH_TIE_TOLERANCE) && p >= cross
                                }
                                Short => {
                                    p >= mid * (1.0 - b) * (1.0 - TOUCH_TIE_TOLERANCE) && p <= cross
                                }
                            };
                            assert!(inside, "{side:?} bound={bound} half={half_bps} p={p}");
                        }
                        SendLimit::AtTouch => {
                            // AtTouch only where the bound genuinely does
                            // not reach; otherwise it would be a silent
                            // widening of every send.
                            assert!(
                                half_bps > f64::from(bound),
                                "bound={bound} half={half_bps} crossed at the touch unnecessarily"
                            );
                        }
                        SendLimit::Unchecked => panic!("book was usable"),
                    }
                }
            }
        }
    }
}
