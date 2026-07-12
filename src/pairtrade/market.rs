//! Market data snapshot type and small per-snapshot helpers.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Maximum bid/ask spread (bps of mid) we accept on an inbound tick before
/// treating it as a corrupt orderbook snapshot. Normal Lighter BTC/ETH
/// spreads sit at 1-15 bps; the bot-strategy#346 incident produced a
/// 6,680 bps spread (ask=$159,598 vs bid=$79,650 with ask_size=0). 200 bps
/// is >10x normal noise and >30x below the observed bad tick.
pub(super) const MAX_TICK_SPREAD_BPS: f64 = 200.0;

/// Maximum distance from the bid/ask touch (bps) we tolerate for the
/// `price` (mid) field. Lighter occasionally prints last-trade slightly
/// outside touch; 50 bps is well above that without admitting a price
/// half-way between a real bid and a phantom ask.
pub(super) const MAX_TICK_PRICE_ENVELOPE_BPS: f64 = 50.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SymbolSnapshot {
    pub(super) price: Decimal,
    pub(super) funding_rate: Decimal,
    pub(super) bid_price: Option<Decimal>,
    pub(super) ask_price: Option<Decimal>,
    pub(super) bid_size: Decimal,
    pub(super) ask_size: Decimal,
    pub(super) min_order: Option<Decimal>,
    pub(super) min_tick: Option<Decimal>,
    pub(super) size_decimals: Option<u32>,
    /// Exchange-side timestamp (Unix milliseconds) for the most recent price
    /// update from the connector. When `Some`, all bots observing the same feed
    /// see identical values for the same update — used to align bar buckets
    /// across processes (pairtrade#4). Bumped from seconds to ms in
    /// bot-strategy#274 / #276 so within-second tick orderings survive into the
    /// bar bucketing layer. Pre-bump dumps and snapshots that store seconds
    /// values are auto-detected and migrated at load time.
    #[serde(default)]
    pub(super) exchange_ts: Option<i64>,
}

/// Per-hour funding signal used by the entry filter (`entry.rs`). Input
/// is fraction-per-hour (bot-strategy#414 normalized both venues). Both
/// exchanges settle hourly (verified bot-strategy#352 / #363); after
/// #414, magnitude matches `/api/v1/funding-rates` and the CSV-settled
/// `Rate` column.
///
/// **Sign convention (bot-strategy#517).** Returns strategy-side
/// *received* carry per hour: positive ⇒ the planned position collects
/// funding, negative ⇒ it pays. With perp-funding sign (`+rate ⇒ longs
/// pay`), a short leg receives its symbol's rate and a long leg pays
/// its symbol's rate. This matches the `PnlLogRecord::funding_carry_usd`
/// convention (#414) and what the entry-filter consumers
/// (`FUNDING_CARRY_ENTRY_DISCOUNT`, `funding_entry_z_scale`,
/// `net_funding_min_per_hour`) assume: all treat positive as
/// carry-favorable. Before #517 this helper returned the inverse
/// (documented latent since #414). The flip IS an entry-behavior
/// change: `FUNDING_CARRY_ENTRY_DISCOUNT` in `entry.rs` is always-on
/// (not config-gated) and previously eased the entry threshold for
/// carry-*adverse* positions — the opposite of its stated intent. It
/// now eases carry-favorable entries as intended. Multi-pair candidate
/// ordering in `engine/step.rs` also sorts on this value (non-binding
/// on live single-pair BTC/ETH configs). The configurable gates remain
/// disabled / non-binding live (`funding_entry_z_scale: 0.0`,
/// `net_funding_min_per_hour: -0.01`).
pub(super) fn net_funding_for_direction(z: f64, p1: &SymbolSnapshot, p2: &SymbolSnapshot) -> f64 {
    if z > 0.0 {
        // plan to short base (p1) and long quote (p2): receive p1's
        // rate on the short leg, pay p2's rate on the long leg
        (p1.funding_rate - p2.funding_rate).to_f64().unwrap_or(0.0)
    } else {
        // plan to long base (p1) and short quote (p2): pay p1's rate
        // on the long leg, receive p2's rate on the short leg
        (p2.funding_rate - p1.funding_rate).to_f64().unwrap_or(0.0)
    }
}

pub(super) fn liquidity_score(p1: &SymbolSnapshot, p2: &SymbolSnapshot) -> f64 {
    let s1 = p1.bid_size.min(p1.ask_size).to_f64().unwrap_or(0.0);
    let s2 = p2.bid_size.min(p2.ask_size).to_f64().unwrap_or(0.0);
    (s1 + s2).max(0.0)
}

/// Reason a snapshot was rejected by `tick_sanity_check`. Used by the
/// caller to emit a diagnostic-rich WARN without forcing the helper to
/// format strings on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TickRejectReason {
    EmptyBidSize,
    EmptyAskSize,
    MissingBid,
    MissingAsk,
    NonPositiveQuote,
    CrossedBook,
    SpreadTooWide,
    PriceOutsideEnvelope,
}

impl TickRejectReason {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::EmptyBidSize => "empty_bid_size",
            Self::EmptyAskSize => "empty_ask_size",
            Self::MissingBid => "missing_bid",
            Self::MissingAsk => "missing_ask",
            Self::NonPositiveQuote => "nonpositive_quote",
            Self::CrossedBook => "crossed_book",
            Self::SpreadTooWide => "spread_too_wide",
            Self::PriceOutsideEnvelope => "price_outside_envelope",
        }
    }
}

/// Validate that a snapshot's quoted bid/ask/price are mutually consistent
/// before we admit them into the rolling regression. Returns `Ok(())` if
/// the tick is sane, `Err(reason)` otherwise.
///
/// Catches the bot-strategy#346 failure mode: Lighter occasionally emits
/// orderbook frames with `ask_size=0` and a phantom ask at ~2x mid, which
/// the old code passed straight to `bar_builder.push(snapshot.price)`,
/// committing a corrupt close to the price history and blowing up the
/// rolling-OLS beta on the next regression update.
pub(super) fn tick_sanity_check(
    snap: &SymbolSnapshot,
    max_spread_bps: f64,
    max_envelope_bps: f64,
) -> Result<(), TickRejectReason> {
    if snap.bid_size <= Decimal::ZERO {
        return Err(TickRejectReason::EmptyBidSize);
    }
    if snap.ask_size <= Decimal::ZERO {
        return Err(TickRejectReason::EmptyAskSize);
    }
    let bid = snap.bid_price.ok_or(TickRejectReason::MissingBid)?;
    let ask = snap.ask_price.ok_or(TickRejectReason::MissingAsk)?;
    quote_sanity_check(
        Some(bid),
        Some(ask),
        snap.price,
        max_spread_bps,
        max_envelope_bps,
    )
}

/// Price-side gates of [`tick_sanity_check`] without the `bid_size /
/// ask_size` checks. Used by the WebSocket arm (`ingest_price_update`)
/// where the connector's `PriceUpdate` only carries prices — no sizes,
/// no funding. Same `TickRejectReason` enum / same constants as the
/// polling path so the diagnostic surface stays uniform.
///
/// Catches the bot-strategy#472 failure mode: Lighter WS occasionally
/// emits an orderbook frame with `bid ≈ 1770, ask ≈ 3188` (5,700 bps
/// spread) which the unfiltered WS path (`step.rs::ingest_price_update`)
/// committed straight to the bar close, collapsing β to floor for the
/// next ~1h47m until the bad bar rolled out of the long lookback.
pub(super) fn quote_sanity_check(
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    price: Decimal,
    max_spread_bps: f64,
    max_envelope_bps: f64,
) -> Result<(), TickRejectReason> {
    let bid = bid.ok_or(TickRejectReason::MissingBid)?;
    let ask = ask.ok_or(TickRejectReason::MissingAsk)?;
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(TickRejectReason::NonPositiveQuote);
    }
    if ask < bid {
        return Err(TickRejectReason::CrossedBook);
    }
    let bid_f = bid.to_f64().unwrap_or(0.0);
    let ask_f = ask.to_f64().unwrap_or(0.0);
    let mid = (bid_f + ask_f) * 0.5;
    if mid <= 0.0 {
        return Err(TickRejectReason::NonPositiveQuote);
    }
    let spread_bps = (ask_f - bid_f) / mid * 10_000.0;
    if spread_bps > max_spread_bps {
        return Err(TickRejectReason::SpreadTooWide);
    }
    let price_f = price.to_f64().unwrap_or(0.0);
    let envelope = max_envelope_bps / 10_000.0;
    let lower = bid_f * (1.0 - envelope);
    let upper = ask_f * (1.0 + envelope);
    if price_f < lower || price_f > upper {
        return Err(TickRejectReason::PriceOutsideEnvelope);
    }
    Ok(())
}

/// Per-symbol accepted-tick freshness, maintained by the shared tick
/// (`engine/shared_tick.rs`) for the #531 guard's `stale` signal. The raw
/// snapshot's `exchange_ts` is refreshed by *every* connector frame —
/// including corrupt ones the tick filter rejects — so during a rejection
/// storm it stays fresh while the engine's accepted view is frozen. This
/// tracks the clock the guard actually needs: ticks that passed
/// `tick_sanity_check`.
#[derive(Debug, Clone, Copy)]
pub(super) struct FeedHealth {
    /// Wall/replay-clock seconds of the last snapshot that passed
    /// `tick_sanity_check`.
    pub(super) last_accepted_ts: i64,
    /// Wall/replay-clock seconds of the accepted tick that ended a gap
    /// longer than the guard's stale threshold. The guard keeps deferring
    /// through a holddown after this, so the first close decision after a
    /// rejection storm is not fired into the just-recovered book.
    pub(super) gap_recovered_ts: Option<i64>,
}

/// Book-quality check for the ineligible-close deferral guard
/// (bot-strategy#531). Returns `Some(reason)` when either leg's snapshot
/// looks too degraded to price a forced exit against:
///
/// - `spread`: bid/ask spread above `max_spread_bps` (default 20 bps — the
///   BT degraded-book artifact threshold, an order of magnitude below the
///   200 bps tick-filter reject so it catches "wide but not corrupt" books).
/// - `stale`: the engine's view of the leg is not trustworthy-fresh. Three
///   sub-signals, any of which counts (all against `max_stale_secs`):
///   the raw `exchange_ts` is old (quiet venue serving cached frames); the
///   last tick that passed `tick_sanity_check` is old (rejection storm in
///   progress — the raw `exchange_ts` stays fresh in that shape because
///   corrupt frames keep arriving, see `FeedHealth`); or an accepted tick
///   ended a longer-than-threshold gap less than `max_stale_secs` ago
///   (post-storm recovery holddown — the 06-10 incident closed on the
///   first bar after ~5 min of `[TICK_FILTER]` rejections, exactly when
///   both other signals look fresh again).
///
/// Missing bid/ask, a missing `exchange_ts`, or missing feed-health info
/// contribute nothing (accepted snapshots always carry bid/ask; a venue
/// without exchange timestamps just forgoes that sub-signal). `now_ts` is
/// Unix seconds; `exchange_ts` is milliseconds, with legacy seconds-scale
/// values auto-detected the same way the snapshot loader migrates them
/// (bot-strategy#274/#276).
pub(super) fn ineligible_close_book_degraded(
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
    feed: [Option<&FeedHealth>; 2],
    now_ts: i64,
    max_spread_bps: f64,
    max_stale_secs: i64,
) -> Option<&'static str> {
    for (snap, health) in [(p1, feed[0]), (p2, feed[1])] {
        if let (Some(bid), Some(ask)) = (snap.bid_price, snap.ask_price) {
            let bid_f = bid.to_f64().unwrap_or(0.0);
            let ask_f = ask.to_f64().unwrap_or(0.0);
            let mid = (bid_f + ask_f) * 0.5;
            if mid > 0.0 && (ask_f - bid_f) / mid * 10_000.0 > max_spread_bps {
                return Some("spread");
            }
        }
        if let Some(ts) = snap.exchange_ts {
            let ts_ms = if ts < 1_000_000_000_000 {
                ts * 1000
            } else {
                ts
            };
            if now_ts * 1000 - ts_ms > max_stale_secs * 1000 {
                return Some("stale");
            }
        }
        if let Some(h) = health {
            if now_ts.saturating_sub(h.last_accepted_ts) > max_stale_secs {
                return Some("stale");
            }
            if let Some(recovered) = h.gap_recovered_ts {
                if now_ts.saturating_sub(recovered) < max_stale_secs {
                    return Some("stale");
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn snap(
        price: Decimal,
        bid: Option<Decimal>,
        ask: Option<Decimal>,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> SymbolSnapshot {
        SymbolSnapshot {
            price,
            funding_rate: dec!(0),
            bid_price: bid,
            ask_price: ask,
            bid_size,
            ask_size,
            min_order: None,
            min_tick: None,
            size_decimals: None,
            exchange_ts: None,
        }
    }

    #[test]
    fn accepts_normal_btc_quote() {
        // From real Lighter dump: ts=1778198400005
        let s = snap(
            dec!(79963.1),
            Some(dec!(79884.4)),
            Some(dec!(79974.3)),
            dec!(0.00700),
            dec!(0.00375),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Ok(())
        );
    }

    #[test]
    fn rejects_bot_strategy_346_corrupt_tick() {
        // The exact failure: Lighter dump 2026-05-08T09:21:00Z BTC frame.
        // ask_size=0 with a phantom ask 2x normal -> mid prints at $119,775.
        let s = snap(
            dec!(119775.9),
            Some(dec!(79650.4)),
            Some(dec!(159598.8)),
            dec!(0.82506),
            dec!(0),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Err(TickRejectReason::EmptyAskSize)
        );
    }

    #[test]
    fn rejects_wide_spread_even_with_sizes() {
        // Construct a tick with both sides present but spread far too wide
        // to be a real BTC quote (would distort OLS even without ask_size=0).
        let s = snap(
            dec!(85000),
            Some(dec!(80000)),
            Some(dec!(90000)),
            dec!(1),
            dec!(1),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Err(TickRejectReason::SpreadTooWide)
        );
    }

    #[test]
    fn rejects_crossed_book() {
        let s = snap(
            dec!(80000),
            Some(dec!(80100)),
            Some(dec!(79900)),
            dec!(1),
            dec!(1),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Err(TickRejectReason::CrossedBook)
        );
    }

    #[test]
    fn rejects_empty_bid_size() {
        let s = snap(
            dec!(80000),
            Some(dec!(79990)),
            Some(dec!(80010)),
            dec!(0),
            dec!(1),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Err(TickRejectReason::EmptyBidSize)
        );
    }

    #[test]
    fn rejects_price_far_outside_touch() {
        // bid/ask sane but reported mid is 5% above ask -> reject (defensive).
        let s = snap(
            dec!(84000),
            Some(dec!(79990)),
            Some(dec!(80010)),
            dec!(1),
            dec!(1),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Err(TickRejectReason::PriceOutsideEnvelope)
        );
    }

    #[test]
    fn accepts_price_just_outside_touch_within_envelope() {
        // last-trade prints at bid - ~25bps. Should still pass with the
        // 50 bps envelope.
        let s = snap(
            dec!(79790),
            Some(dec!(79800)),
            Some(dec!(79820)),
            dec!(1),
            dec!(1),
        );
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Ok(())
        );
    }

    #[test]
    fn rejects_missing_quote() {
        let s = snap(dec!(80000), None, Some(dec!(80010)), dec!(1), dec!(1));
        assert_eq!(
            tick_sanity_check(&s, MAX_TICK_SPREAD_BPS, MAX_TICK_PRICE_ENVELOPE_BPS),
            Err(TickRejectReason::MissingBid)
        );
    }

    fn snap_with_funding(funding_rate: Decimal) -> SymbolSnapshot {
        SymbolSnapshot {
            price: dec!(0),
            funding_rate,
            bid_price: None,
            ask_price: None,
            bid_size: dec!(0),
            ask_size: dec!(0),
            min_order: None,
            min_tick: None,
            size_decimals: None,
            exchange_ts: None,
        }
    }

    // bot-strategy#472 regression — the WS arm of `ingest_price_update`
    // previously bypassed `tick_sanity_check` and committed the
    // corrupt frame straight to the bar close, collapsing β. The new
    // `quote_sanity_check` gate uses the same constants as the
    // polling path but is callable from the WS arm where order sizes
    // are not available.

    #[test]
    fn quote_sanity_rejects_472_corrupt_eth_ws_frame() {
        // Frankfurt 2026-05-22 06:31:00 UTC. Lighter book frame:
        // bid=1770.84 ask=3188.72 mid=1948.32 — spread ≈ 71%.
        // The polling path rejected this with reason=spread_too_wide
        // (TICK_FILTER); the WS path silently accepted it pre-fix.
        let r = quote_sanity_check(
            Some(dec!(1770.84)),
            Some(dec!(3188.72)),
            dec!(1948.32),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Err(TickRejectReason::SpreadTooWide));
    }

    #[test]
    fn quote_sanity_accepts_normal_btc_ws_frame() {
        // From a normal Lighter WS update — bid/ask spread ≈ 13 bps.
        let r = quote_sanity_check(
            Some(dec!(77451.2)),
            Some(dec!(77461.4)),
            dec!(77456.3),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Ok(()));
    }

    #[test]
    fn quote_sanity_rejects_crossed_ws_book() {
        // Defensive: pre-fix the WS arm had no crossed-book check
        // either. A genuine cross is rare but observable on Lighter
        // during fast moves.
        let r = quote_sanity_check(
            Some(dec!(80100)),
            Some(dec!(79900)),
            dec!(80000),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Err(TickRejectReason::CrossedBook));
    }

    #[test]
    fn quote_sanity_rejects_nonpositive_mid() {
        let r = quote_sanity_check(
            Some(dec!(80000)),
            Some(dec!(80010)),
            dec!(0),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Err(TickRejectReason::NonPositiveQuote));
    }

    #[test]
    fn quote_sanity_rejects_price_outside_envelope() {
        let r = quote_sanity_check(
            Some(dec!(79990)),
            Some(dec!(80010)),
            dec!(84000),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Err(TickRejectReason::PriceOutsideEnvelope));
    }

    #[test]
    fn quote_sanity_rejects_missing_bid() {
        let r = quote_sanity_check(
            None,
            Some(dec!(80010)),
            dec!(80000),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Err(TickRejectReason::MissingBid));
    }

    #[test]
    fn quote_sanity_and_tick_sanity_share_gates_on_corrupt_frame() {
        // Reusing the bot-strategy#346 BTC corrupt frame from
        // `rejects_bot_strategy_346_corrupt_tick`. The size-side
        // EmptyAskSize gate is unreachable from `quote_sanity_check`
        // (WS frames don't carry sizes), but the SpreadTooWide gate
        // should still catch this frame on the price side.
        let r = quote_sanity_check(
            Some(dec!(79650.4)),
            Some(dec!(159598.8)),
            dec!(119775.9),
            MAX_TICK_SPREAD_BPS,
            MAX_TICK_PRICE_ENVELOPE_BPS,
        );
        assert_eq!(r, Err(TickRejectReason::SpreadTooWide));
    }

    #[test]
    fn net_funding_passes_per_hour_through_unchanged() {
        // Inputs are fraction-per-hour after the bot-strategy#414
        // normalization in dex-connector; this verifies both direction
        // branches return the raw delta with no further scaling, in the
        // strategy-side received-carry convention (bot-strategy#517):
        // z > 0 plans short-p1/long-p2, which receives p1's rate and
        // pays p2's, so positive p1 + negative p2 is carry-favorable.
        // Realistic Lighter-scale inputs: 0.0000125 fraction/h is
        // 0.125 bps/h (about 1 bp over an 8h funding interval).
        let p1 = snap_with_funding(dec!(0.0000125));
        let p2 = snap_with_funding(dec!(-0.0000035));
        let z_pos = net_funding_for_direction(1.0, &p1, &p2);
        let z_neg = net_funding_for_direction(-1.0, &p1, &p2);
        assert!((z_pos - 0.000016).abs() < 1e-15, "z_pos was {}", z_pos);
        assert!((z_neg - (-0.000016)).abs() < 1e-15, "z_neg was {}", z_neg);
    }

    /// bot-strategy#531 book-quality guard for the ineligible flatten.
    /// `now_ts` in seconds, `exchange_ts` in ms (10 bps spread reference
    /// book: bid 63800.0 / ask 63863.9 on a ~63832 mid).
    fn guard_snap(bid: Decimal, ask: Decimal, exchange_ts: Option<i64>) -> SymbolSnapshot {
        let mut s = snap(
            (bid + ask) / dec!(2),
            Some(bid),
            Some(ask),
            dec!(1),
            dec!(1),
        );
        s.exchange_ts = exchange_ts;
        s
    }

    /// Feed health for a symbol whose accepted feed is healthy: last
    /// accepted tick `age_secs` ago, no recent gap recovery.
    fn feed(
        last_accepted_age_secs: i64,
        gap_recovered_age_secs: Option<i64>,
        now: i64,
    ) -> FeedHealth {
        FeedHealth {
            last_accepted_ts: now - last_accepted_age_secs,
            gap_recovered_ts: gap_recovered_age_secs.map(|a| now - a),
        }
    }

    #[test]
    fn ineligible_guard_passes_fresh_tight_book() {
        let now = 1_783_651_400_i64;
        let p1 = guard_snap(dec!(63837.9), dec!(63838.0), Some(now * 1000 - 5_000));
        let p2 = guard_snap(dec!(1768.11), dec!(1768.14), Some(now * 1000 - 5_000));
        let h = feed(5, None, now);
        assert_eq!(
            ineligible_close_book_degraded(&p1, &p2, [Some(&h), Some(&h)], now, 20.0, 30),
            None
        );
    }

    #[test]
    fn ineligible_guard_flags_wide_spread_on_either_leg() {
        let now = 1_783_651_400_i64;
        let tight = guard_snap(dec!(63837.9), dec!(63838.0), Some(now * 1000));
        // ~25 bps spread on the ETH leg (1768.0 vs 1772.4).
        let wide = guard_snap(dec!(1768.0), dec!(1772.4), Some(now * 1000));
        assert_eq!(
            ineligible_close_book_degraded(&tight, &wide, [None, None], now, 20.0, 30),
            Some("spread")
        );
        assert_eq!(
            ineligible_close_book_degraded(&wide, &tight, [None, None], now, 20.0, 30),
            Some("spread")
        );
        // The same book passes with the guard threshold above the spread.
        assert_eq!(
            ineligible_close_book_degraded(&tight, &wide, [None, None], now, 30.0, 30),
            None
        );
    }

    #[test]
    fn ineligible_guard_flags_stale_tick() {
        let now = 1_783_651_400_i64;
        let fresh = guard_snap(dec!(63837.9), dec!(63838.0), Some(now * 1000 - 5_000));
        // Raw snapshot 5 minutes old — quiet venue serving cached frames.
        let stale = guard_snap(dec!(1768.11), dec!(1768.14), Some(now * 1000 - 300_000));
        assert_eq!(
            ineligible_close_book_degraded(&fresh, &stale, [None, None], now, 20.0, 30),
            Some("stale")
        );
    }

    #[test]
    fn ineligible_guard_flags_frozen_accepted_feed_despite_fresh_exchange_ts() {
        let now = 1_783_651_400_i64;
        // The 06-10 rejection-storm shape (Codex PR #166 review): corrupt
        // frames keep arriving so the raw exchange_ts stays fresh, but no
        // tick has passed the sanity filter for 5 minutes.
        let p1 = guard_snap(dec!(63837.9), dec!(63838.0), Some(now * 1000 - 2_000));
        let p2 = guard_snap(dec!(1768.11), dec!(1768.14), Some(now * 1000 - 2_000));
        let healthy = feed(5, None, now);
        let frozen = feed(300, None, now);
        assert_eq!(
            ineligible_close_book_degraded(
                &p1,
                &p2,
                [Some(&healthy), Some(&frozen)],
                now,
                20.0,
                30
            ),
            Some("stale")
        );
    }

    #[test]
    fn ineligible_guard_holds_through_gap_recovery_then_clears() {
        let now = 1_783_651_400_i64;
        // First valid bar after a rejection storm: the snapshot is fresh
        // and sane, the accepted feed just resumed — without the holddown
        // the close would fire into the just-recovered book (the exact
        // 06-10 / Codex scenario). Inside the holddown: still stale.
        let p1 = guard_snap(dec!(63837.9), dec!(63838.0), Some(now * 1000 - 2_000));
        let p2 = guard_snap(dec!(1768.11), dec!(1768.14), Some(now * 1000 - 2_000));
        let healthy = feed(5, None, now);
        let just_recovered = feed(2, Some(10), now);
        assert_eq!(
            ineligible_close_book_degraded(
                &p1,
                &p2,
                [Some(&healthy), Some(&just_recovered)],
                now,
                20.0,
                30
            ),
            Some("stale")
        );
        // Holddown elapsed (recovery 30s ago == threshold): trustworthy again.
        let recovered = feed(2, Some(30), now);
        assert_eq!(
            ineligible_close_book_degraded(
                &p1,
                &p2,
                [Some(&healthy), Some(&recovered)],
                now,
                20.0,
                30
            ),
            None
        );
    }

    #[test]
    fn ineligible_guard_migrates_legacy_seconds_exchange_ts() {
        let now = 1_783_651_400_i64;
        // Legacy pre-#276 seconds-scale exchange_ts, 5s old: sane.
        let fresh = guard_snap(dec!(63837.9), dec!(63838.0), Some(now - 5));
        // Same scale, 5 minutes old: stale.
        let stale = guard_snap(dec!(1768.11), dec!(1768.14), Some(now - 300));
        assert_eq!(
            ineligible_close_book_degraded(&fresh, &fresh, [None, None], now, 20.0, 30),
            None
        );
        assert_eq!(
            ineligible_close_book_degraded(&fresh, &stale, [None, None], now, 20.0, 30),
            Some("stale")
        );
    }

    #[test]
    fn ineligible_guard_ignores_missing_signals() {
        let now = 1_783_651_400_i64;
        // No exchange_ts, no bid/ask, no feed health: nothing to assess,
        // not degraded.
        let bare = snap(dec!(63838.0), None, None, dec!(1), dec!(1));
        assert_eq!(
            ineligible_close_book_degraded(&bare, &bare, [None, None], now, 20.0, 30),
            None
        );
    }
}
