//! Price-bound semantics shared by every taker-IOC sender (bot-strategy#971).
//!
//! A configured `slippage_bps` is a bound **against the mid** everywhere an
//! operator or a paper fill reads it. The venue's `create_order_taker_ioc`
//! crosses the **touch** by the bps it is handed, so the same number means
//! `mid + half-spread + bps` there: on a tight book the two agree, on a
//! torn book the touch-relative send has no bound at all. Callers convert
//! with [`mid_relative_slippage_bps`] and decide what a refusal means with
//! [`send_bound_bps`], so paper, pre-send guard and live send all speak
//! mid-relative. Introduced for `engine_b_live` under bot-strategy#918
//! (pairtrade#304) and shared with the book runtime under bot-strategy#971.

use dex_connector::OrderSide;

/// Convert a *mid-relative* price bound into the touch-relative
/// `slippage_bps` that `create_order_taker_ioc` takes, so the resulting
/// limit price is at most `bound_bps` from the mid (bot-strategy#918
/// Codex review).
///
/// The connector crosses the **touch** by `slippage_bps`, while the
/// requirements doc (§6.3) states the bound against the *mid*. Those
/// agree only on a tight book: on a 90/110 book, handing the connector
/// the full 50 bps caps a buy near 110.55 -- 10.5% above the 100 mid,
/// i.e. no bound at all in exactly the torn-book tail the bound exists
/// for.
///
/// The conversion is **multiplicative and side-aware**, not a
/// subtraction. The connector applies its collar to the touch, so for a
/// buy the adverse move against the mid is `(1+h)(1+s) - 1 = h + s +
/// h*s`, not `h + s`; subtracting the half-spread in bps leaves the
/// cross term. With `h` the half-spread and `b` the budget, both as
/// fractions:
///
/// - buy:  `s = (1 + b) / (1 + h) - 1`
/// - sell: `s = 1 - (1 - b) / (1 - h)`
///
/// `None` means the half-spread alone already exceeds the budget, so no
/// marketable price inside the bound exists. That is a refusal for an
/// entry; the exit path treats it separately, because a position that
/// cannot be closed is worse than one closed at the touch.
///
/// Rounded **down** to a whole bp (the connector takes `u32`), so the
/// conversion can only tighten, never widen -- the same direction as the
/// connector's own inward tick rounding.
///
/// Not modelled: the connector crosses by one tick *before* applying the
/// collar, so the realised cap is up to one tick wider than `bound_bps`
/// from the mid. On SNDK near 1700 with 2 price decimals that is
/// 0.01/1700 ≈ 0.06 bps. Modelling it would need the market's
/// `price_decimals`, which lives in the connector, not here.
pub fn mid_relative_slippage_bps(
    bound_bps: u32,
    best_bid: f64,
    best_ask: f64,
    side: OrderSide,
) -> Option<u32> {
    if !best_bid.is_finite() || !best_ask.is_finite() || best_bid <= 0.0 || best_ask < best_bid {
        return None;
    }
    let mid = (best_bid + best_ask) / 2.0;
    if mid <= 0.0 {
        return None;
    }
    let budget = f64::from(bound_bps) / 10_000.0;
    let half_spread = (best_ask - best_bid) / 2.0 / mid;
    // `best_bid > 0` and `best_ask >= best_bid` bound `half_spread` to
    // `[0, 1)`, so the sell denominator cannot be zero or negative.
    let allowance = match side {
        OrderSide::Long => (1.0 + budget) / (1.0 + half_spread) - 1.0,
        OrderSide::Short => 1.0 - (1.0 - budget) / (1.0 - half_spread),
    };
    // The nano-bp tolerance is for f64 representation error, not slack in
    // the rule: `100.01 - 99.99` is 0.020000000000010232, which would
    // otherwise cost a whole basis point to the floor below.
    let allowance_bps = allowance * 10_000.0 + 1e-9;
    if allowance_bps < 1.0 {
        return None;
    }
    // Both callers validate the configured bound into the connector's
    // 1..=1000 (`EngineBLiveConfig::validate`, `BookConfig::validate`),
    // and the conversion only ever shrinks one; the clamp is here so a
    // future caller cannot smuggle a wider one through.
    Some((allowance_bps.floor() as u32).min(1000))
}

/// What to hand the connector for one send, given the mid-relative bound
/// and the book observed by the sender itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendBound {
    /// A marketable price exists inside the bound: send this
    /// touch-relative allowance.
    Inside(u32),
    /// Reduce-only on a book whose half-spread alone exceeds the bound:
    /// cross at the touch (the connector's 1 bp minimum). Still a bound --
    /// the best offer, not the venue's +/-20 % -- and a position left
    /// inside a tear is the worse outcome.
    AtTouch,
    /// Reduce-only with no observed book: send the configured bound
    /// against the connector's own touch, without a half-spread
    /// allowance. A missing observation must not strand a position.
    Unchecked,
}

impl SendBound {
    /// The `slippage_bps` to pass to `create_order_taker_ioc`.
    pub fn bps(self, configured_bps: u32) -> u32 {
        match self {
            SendBound::Inside(b) => b,
            SendBound::AtTouch => 1,
            SendBound::Unchecked => configured_bps,
        }
    }
}

/// Decide the send bound for one order. `touch` is `(best_bid, best_ask)`
/// as observed by the caller's own feed (never the connector's cache: a
/// small disagreement can only make the sent bound tighter than the
/// mid-relative budget, never wider), `None` when nothing fresh was seen.
///
/// Entries are asymmetric with exits: an entry whose bound cannot be
/// checked against the mid, or has no marketable price inside it, is
/// refused (`Err`, the reason), because not opening costs nothing. An
/// exit is always sendable, see [`SendBound`].
pub fn send_bound_bps(
    bound_bps: u32,
    touch: Option<(f64, f64)>,
    side: OrderSide,
    reduce_only: bool,
) -> Result<SendBound, String> {
    let Some((best_bid, best_ask)) = touch else {
        if reduce_only {
            return Ok(SendBound::Unchecked);
        }
        return Err(
            "no observed book; refusing to send an entry whose price bound cannot be checked \
             against the mid"
                .to_string(),
        );
    };
    match mid_relative_slippage_bps(bound_bps, best_bid, best_ask, side) {
        Some(bps) => Ok(SendBound::Inside(bps)),
        None if reduce_only => Ok(SendBound::AtTouch),
        None => Err(format!(
            "half-spread exceeds the {bound_bps}bps bound (bid={best_bid} ask={best_ask}): no entry \
             price within the bound"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use OrderSide::{Long, Short};

    #[test]
    fn the_touch_relative_bound_is_the_mid_relative_one_net_of_the_half_spread() {
        // Tight book (1 bp half-spread): nearly the whole budget
        // survives. Long lands at 48.9951 bps and short at 49.0049 --
        // the sell side gains from the same multiplication the buy side
        // loses to, which is why the conversion is side-aware.
        assert_eq!(mid_relative_slippage_bps(50, 99.99, 100.01, Long), Some(48));
        assert_eq!(
            mid_relative_slippage_bps(50, 99.99, 100.01, Short),
            Some(49)
        );
        // A 90/110 book: handing the connector 50 bps would cap a buy near
        // 110.55 -- 10.5% above the 100 mid.
        assert_eq!(mid_relative_slippage_bps(50, 90.0, 110.0, Long), None);
        assert_eq!(mid_relative_slippage_bps(50, 90.0, 110.0, Short), None);
        // Near the boundary: 48 bps of half-spread leaves ~2, and a
        // half-spread that eats the whole budget leaves nothing.
        assert_eq!(mid_relative_slippage_bps(50, 99.52, 100.48, Long), Some(1));
        assert_eq!(mid_relative_slippage_bps(50, 99.52, 100.48, Short), Some(2));
        assert_eq!(mid_relative_slippage_bps(50, 99.5, 100.5, Long), None);
        assert_eq!(mid_relative_slippage_bps(50, 99.5, 100.5, Short), None);
        // Nonsense books are refusals, not silent full-budget sends.
        assert_eq!(
            mid_relative_slippage_bps(50, 101.0, 100.0, Long),
            None,
            "crossed"
        );
        assert_eq!(
            mid_relative_slippage_bps(50, 0.0, 100.0, Long),
            None,
            "no bid"
        );
        assert_eq!(mid_relative_slippage_bps(50, f64::NAN, 100.0, Long), None);
    }

    #[test]
    fn the_conversion_is_multiplicative_not_a_subtraction() {
        // On a 1695.75/1704.25 book with a 50 bps budget the half-spread
        // is 25 bps, so a subtraction says 25. But the connector
        // multiplies its collar onto the ask, and 1704.25 * 1.0025 =
        // 1708.510625 is above the 1708.50 that 50 bps from the 1700 mid
        // allows -- the `h * s` cross term. The exact ratio gives 24.9377
        // bps, which floors to 24.
        assert_eq!(
            mid_relative_slippage_bps(50, 1695.75, 1704.25, Long),
            Some(24)
        );
        let mid = 1700.0_f64;
        let sent = f64::from(mid_relative_slippage_bps(50, 1695.75, 1704.25, Long).unwrap());
        let realised_cap = 1704.25 * (1.0 + sent / 10_000.0);
        assert!(
            realised_cap <= mid * (1.0 + 50.0 / 10_000.0),
            "realised cap {realised_cap} must stay inside the advertised bound"
        );
    }

    #[test]
    fn the_realised_cap_never_exceeds_the_mid_relative_bound() {
        // Property over a grid of books and budgets: whatever the helper
        // hands the connector, touch * (1 +/- sent) stays inside
        // mid * (1 +/- bound).
        for &bound in &[1u32, 5, 10, 30, 50, 100, 300, 1000] {
            for &half_bps in &[0.0, 0.1, 0.5, 1.0, 4.9, 5.0, 9.0, 25.0, 49.0, 120.0, 999.0] {
                let mid = 100.0;
                let h = half_bps / 10_000.0;
                let (bid, ask) = (mid * (1.0 - h), mid * (1.0 + h));
                let b = f64::from(bound) / 10_000.0;
                if let Some(s) = mid_relative_slippage_bps(bound, bid, ask, Long) {
                    let cap = ask * (1.0 + f64::from(s) / 10_000.0);
                    assert!(
                        cap <= mid * (1.0 + b) + 1e-9,
                        "long bound={bound} half={half_bps} sent={s} cap={cap}"
                    );
                    assert!(s >= 1);
                }
                if let Some(s) = mid_relative_slippage_bps(bound, bid, ask, Short) {
                    let cap = bid * (1.0 - f64::from(s) / 10_000.0);
                    assert!(
                        cap >= mid * (1.0 - b) - 1e-9,
                        "short bound={bound} half={half_bps} sent={s} cap={cap}"
                    );
                    assert!(s >= 1);
                }
            }
        }
    }

    #[test]
    fn entries_are_refused_where_exits_still_go_out() {
        // Tight book: both sides send the converted allowance.
        assert_eq!(
            send_bound_bps(50, Some((99.99, 100.01)), Long, false),
            Ok(SendBound::Inside(48))
        );
        assert_eq!(
            send_bound_bps(50, Some((99.99, 100.01)), Short, true),
            Ok(SendBound::Inside(49))
        );
        // Torn book: the entry is refused with the book in the reason, the
        // exit crosses at the touch.
        let torn = send_bound_bps(50, Some((90.0, 110.0)), Long, false).unwrap_err();
        assert!(
            torn.contains("bid=90") && torn.contains("ask=110"),
            "{torn}"
        );
        assert_eq!(
            send_bound_bps(50, Some((90.0, 110.0)), Long, true),
            Ok(SendBound::AtTouch)
        );
        // No book observed: entry refused, exit goes out unchecked.
        assert!(send_bound_bps(50, None, Short, false)
            .unwrap_err()
            .contains("no observed book"));
        assert_eq!(
            send_bound_bps(50, None, Short, true),
            Ok(SendBound::Unchecked)
        );
        // A crossed or empty book is "no marketable price", not "no book".
        assert_eq!(
            send_bound_bps(50, Some((101.0, 100.0)), Long, true),
            Ok(SendBound::AtTouch)
        );
        assert!(send_bound_bps(50, Some((0.0, 100.0)), Long, false).is_err());
    }

    #[test]
    fn the_bps_handed_to_the_connector_follow_the_decision() {
        assert_eq!(SendBound::Inside(24).bps(50), 24);
        assert_eq!(SendBound::AtTouch.bps(50), 1);
        assert_eq!(SendBound::Unchecked.bps(50), 50);
    }
}
