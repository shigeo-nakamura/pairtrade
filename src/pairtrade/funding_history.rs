//! Per-symbol rolling history of realized funding rates from the WS feed.
//!
//! Lighter settles funding hourly (verified bot-strategy#352 / #363:
//! `market_stats.funding_rate` updates every 1h on `:00:05` boundaries). We
//! observe the WS rate on every step and append a tick whenever the rate
//! changes. At exit time, the cycle's `[open_ts, close_ts)` window is walked
//! over the buffer and per-leg payments are summed to produce
//! `funding_carry_usd` for `PnlLogRecord` (bot-strategy#364).
//!
//! Sign convention mirrors `market::net_funding_for_direction`:
//! `funding_carry_usd > 0` means the strategy "received net carry" in the
//! sense that entry.rs treats as positive carry (i.e., long-leg rate −
//! short-leg rate weighted by per-leg notional is positive). If a later
//! investigation shows Lighter's WS rate uses the opposite sign convention
//! to the bot's mental model, both this field and the existing
//! `net_funding_per_hour` log line move together — they stay consistent.

use std::collections::{HashMap, VecDeque};

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::state::PositionDirection;

/// One observed funding rate, time-stamped at the moment the WS pushed the
/// new value. `ts_secs` is the engine's `now_ts` (seconds, Unix UTC).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct FundingTick {
    pub(super) ts_secs: i64,
    pub(super) rate: Decimal,
}

/// Per-symbol cap on retained ticks. 1h funding cadence × 720 = 30 days,
/// well beyond any plausible hold time (max force_close is 3h on variant B
/// per Round 4) but small enough that ~3 symbols × 720 × 24 bytes ≈ 50KB
/// is trivial.
const MAX_TICKS_PER_SYMBOL: usize = 720;

#[derive(Debug, Default)]
pub(super) struct FundingHistory {
    by_symbol: HashMap<String, VecDeque<FundingTick>>,
}

impl FundingHistory {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Record the latest WS funding rate for `symbol`. The tick is appended
    /// only when (a) the buffer is empty, (b) the rate changed since the
    /// last entry, or (c) the timestamp advanced by at least one hour
    /// (3600 s) — the third condition makes the buffer resilient to
    /// WS reconnects that re-push an unchanged rate while still recording
    /// the new observation time so the per-tick aggregation window stays
    /// dense enough across a long hold.
    pub(super) fn observe(&mut self, symbol: &str, ts_secs: i64, rate: Decimal) {
        let entry = self.by_symbol.entry(symbol.to_string()).or_default();
        let should_push = match entry.back() {
            None => true,
            Some(last) => last.rate != rate || ts_secs.saturating_sub(last.ts_secs) >= 3600,
        };
        if should_push {
            entry.push_back(FundingTick { ts_secs, rate });
            while entry.len() > MAX_TICKS_PER_SYMBOL {
                entry.pop_front();
            }
        }
    }

    /// Return the realized funding rate snapshots whose `ts_secs` falls in
    /// `[open_ts, close_ts)` for `symbol`. The caller treats each tick as
    /// the payment for one funding interval (1h on Lighter); partial
    /// intervals at the edges are not interpolated — accuracy is ≤1 funding
    /// tick on each end.
    pub(super) fn ticks_in_range(
        &self,
        symbol: &str,
        open_ts: i64,
        close_ts: i64,
    ) -> Vec<FundingTick> {
        let Some(buf) = self.by_symbol.get(symbol) else {
            return Vec::new();
        };
        buf.iter()
            .filter(|t| t.ts_secs >= open_ts && t.ts_secs < close_ts)
            .copied()
            .collect()
    }
}

/// Aggregate funding carry over the cycle window. Returns
/// `(carry_usd, ticks_observed)` where:
///
/// - `carry_usd` follows `net_funding_for_direction` sign convention:
///   `+ long_leg_rate × notional_long − short_leg_rate × notional_short`
///   summed across funding ticks during `[open_ts, close_ts)`. Per-leg
///   notional captured at entry (size × price) — small drift from
///   intra-hold price changes is on the order of the per-tick fee already
///   being approximated to integer hours, well within reporting tolerance.
/// - `ticks_observed` is the count of ticks summed over both legs
///   (`ticks_in_range(base) + ticks_in_range(quote)`). Caller can divide
///   by 2 to get hours-equivalent.
///
/// Returns `(0.0, 0)` if either leg's notional is zero (defensive — never
/// happens in production but keeps the helper total).
pub(super) fn compute_carry_usd(
    history: &FundingHistory,
    base_symbol: &str,
    quote_symbol: &str,
    open_ts: i64,
    close_ts: i64,
    direction: PositionDirection,
    entry_size_a: Decimal,
    entry_price_a: Decimal,
    entry_size_b: Decimal,
    entry_price_b: Decimal,
) -> (f64, u32) {
    let notional_a = (entry_size_a * entry_price_a).to_f64().unwrap_or(0.0).abs();
    let notional_b = (entry_size_b * entry_price_b).to_f64().unwrap_or(0.0).abs();
    if notional_a <= 0.0 || notional_b <= 0.0 || close_ts <= open_ts {
        return (0.0, 0);
    }
    let base_ticks = history.ticks_in_range(base_symbol, open_ts, close_ts);
    let quote_ticks = history.ticks_in_range(quote_symbol, open_ts, close_ts);
    let (long_leg_notional, long_leg_ticks, short_leg_notional, short_leg_ticks) = match direction {
        PositionDirection::LongSpread => (notional_a, &base_ticks, notional_b, &quote_ticks),
        PositionDirection::ShortSpread => (notional_b, &quote_ticks, notional_a, &base_ticks),
    };
    let long_sum: f64 = long_leg_ticks
        .iter()
        .map(|t| t.rate.to_f64().unwrap_or(0.0))
        .sum();
    let short_sum: f64 = short_leg_ticks
        .iter()
        .map(|t| t.rate.to_f64().unwrap_or(0.0))
        .sum();
    let carry = long_sum * long_leg_notional - short_sum * short_leg_notional;
    let ticks = (long_leg_ticks.len() + short_leg_ticks.len()) as u32;
    (carry, ticks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn observe_dedupes_unchanged_rate_then_pushes_on_change() {
        let mut h = FundingHistory::new();
        h.observe("BTC", 1000, dec!(0.0001));
        h.observe("BTC", 1100, dec!(0.0001)); // same rate, recent — skipped
        h.observe("BTC", 2000, dec!(0.0003)); // changed → push
        let ticks = h.ticks_in_range("BTC", 0, 10_000);
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].rate, dec!(0.0001));
        assert_eq!(ticks[1].rate, dec!(0.0003));
    }

    #[test]
    fn observe_pushes_after_hour_gap_even_when_rate_unchanged() {
        // WS reconnect case: same rate re-broadcast after >1h with no
        // intermediate update. We want the tick recorded so a long hold
        // doesn't undercount.
        let mut h = FundingHistory::new();
        h.observe("BTC", 1000, dec!(0.0001));
        h.observe("BTC", 1000 + 3600, dec!(0.0001));
        let ticks = h.ticks_in_range("BTC", 0, 10_000);
        assert_eq!(ticks.len(), 2);
    }

    #[test]
    fn ticks_in_range_excludes_boundary_close() {
        let mut h = FundingHistory::new();
        h.observe("BTC", 1000, dec!(0.0001));
        h.observe("BTC", 2000, dec!(0.0002));
        h.observe("BTC", 3000, dec!(0.0003));
        let ticks = h.ticks_in_range("BTC", 1000, 3000);
        assert_eq!(ticks.len(), 2); // 1000 included, 3000 excluded
    }

    #[test]
    fn buffer_caps_at_max_ticks() {
        let mut h = FundingHistory::new();
        for i in 0..(MAX_TICKS_PER_SYMBOL + 50) {
            h.observe("BTC", i as i64 * 3600, Decimal::new(i as i64, 6));
        }
        let buf = h.by_symbol.get("BTC").unwrap();
        assert_eq!(buf.len(), MAX_TICKS_PER_SYMBOL);
    }

    #[test]
    fn carry_short_spread_matches_per_leg_formula() {
        // ShortSpread = short BTC, long ETH. Bot convention:
        //   carry = +rate_quote * N_eth - rate_base * N_btc
        let mut h = FundingHistory::new();
        // Two BTC ticks (short leg) at +0.001 and +0.0005
        h.observe("BTC", 1000, dec!(0.001));
        h.observe("BTC", 4600, dec!(0.0005));
        // Two ETH ticks (long leg) at +0.0002 and -0.0001
        h.observe("ETH", 1000, dec!(0.0002));
        h.observe("ETH", 4600, dec!(-0.0001));

        let (carry, n) = compute_carry_usd(
            &h,
            "BTC",
            "ETH",
            500,
            5000,
            PositionDirection::ShortSpread,
            dec!(0.01),    // BTC size
            dec!(80000),   // BTC price → N_btc = $800
            dec!(0.5),     // ETH size
            dec!(2400),    // ETH price → N_eth = $1200
            // For ShortSpread: long_leg=ETH (N=1200), short_leg=BTC (N=800)
            // carry = (0.0002 + (-0.0001)) * 1200 - (0.001 + 0.0005) * 800
            //       = 0.0001 * 1200 - 0.0015 * 800
            //       = 0.12 - 1.2 = -1.08
        );
        assert_eq!(n, 4);
        assert!((carry - (-1.08)).abs() < 1e-9, "carry was {}", carry);
    }

    #[test]
    fn carry_long_spread_flips_leg_assignment() {
        // LongSpread = long BTC, short ETH.
        //   carry = +rate_base * N_btc - rate_quote * N_eth
        let mut h = FundingHistory::new();
        h.observe("BTC", 1000, dec!(0.001));
        h.observe("ETH", 1000, dec!(0.0002));
        let (carry, n) = compute_carry_usd(
            &h,
            "BTC",
            "ETH",
            500,
            2000,
            PositionDirection::LongSpread,
            dec!(0.01),
            dec!(80000),  // N_btc = 800
            dec!(0.5),
            dec!(2400),   // N_eth = 1200
            // carry = 0.001 * 800 - 0.0002 * 1200 = 0.8 - 0.24 = 0.56
        );
        assert_eq!(n, 2);
        assert!((carry - 0.56).abs() < 1e-9, "carry was {}", carry);
    }

    #[test]
    fn carry_zero_when_no_ticks_in_range() {
        let mut h = FundingHistory::new();
        h.observe("BTC", 100, dec!(0.001));
        h.observe("ETH", 100, dec!(0.0002));
        let (carry, n) = compute_carry_usd(
            &h,
            "BTC",
            "ETH",
            500,
            1000, // window starts after the observed tick
            PositionDirection::LongSpread,
            dec!(0.01),
            dec!(80000),
            dec!(0.5),
            dec!(2400),
        );
        assert_eq!(n, 0);
        assert_eq!(carry, 0.0);
    }
}
