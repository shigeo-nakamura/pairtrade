//! OHLC bar aggregation extracted from the monolithic pairtrade module.
//!
//! `BarBuilder` accumulates ticks into wall-clock-aligned buckets and emits
//! a deterministic close price per bucket so that multiple bots observing the
//! same WS feed converge on identical bars. See pairtrade#4.
//!
//! Tick timestamps and the emitted bar-close timestamp are Unix milliseconds.
//! The constructor still takes `window_secs` for caller ergonomics (config is
//! authored in seconds) and converts to ms internally. Bumped from seconds in
//! bot-strategy#274 / #276 so two ticks landing in the same wall-clock second
//! no longer have their ordering flattened by the close-selection rule.

use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub(super) struct BarBuilder {
    window_ms: i64,
    start_ts: Option<i64>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    /// Exchange timestamp (ms) of the tick currently used as `close`. Used to
    /// keep the bar close monotonic with respect to the exchange clock so that
    /// two bots observing the same WS feed converge on the same close price
    /// for the same bucket. Updates from older ts are ignored even if they
    /// arrive later in wall-clock time, and the open price is locked to the
    /// earliest tick of the bucket. See pairtrade#4.
    close_ts: Option<i64>,
    open_ts: Option<i64>,
}

impl BarBuilder {
    pub(super) fn new(window_secs: u64) -> Self {
        Self {
            window_ms: (window_secs as i64).saturating_mul(1000),
            start_ts: None,
            open: Decimal::ZERO,
            high: Decimal::ZERO,
            low: Decimal::ZERO,
            close: Decimal::ZERO,
            close_ts: None,
            open_ts: None,
        }
    }

    /// Align a timestamp (ms) down to the wall-clock bucket boundary.
    ///
    /// Buckets are anchored to the Unix epoch (`floor(ts / window) * window`),
    /// so all bots observing the same stream produce identical bucket IDs
    /// regardless of their own startup phase. This is required for multi-bot
    /// A/B fairness: without this, each process anchors its first bar to its
    /// own first tick, causing beta/mean/std/z to diverge across bots even
    /// though they share the same price feed. See pairtrade#4.
    fn bucket_start(&self, ts: i64) -> i64 {
        if self.window_ms <= 0 {
            return ts;
        }
        ts - ts.rem_euclid(self.window_ms)
    }

    pub(super) fn push(&mut self, ts: i64, price: Decimal) -> Option<(Decimal, i64)> {
        let current_bucket = self.bucket_start(ts);
        match self.start_ts {
            None => {
                self.start_ts = Some(current_bucket);
                self.open = price;
                self.high = price;
                self.low = price;
                self.close = price;
                self.close_ts = Some(ts);
                self.open_ts = Some(ts);
                None
            }
            Some(start) => {
                if current_bucket > start {
                    let prev_close = self.close;
                    let bar_close_ts = start.saturating_add(self.window_ms);
                    self.start_ts = Some(current_bucket);
                    self.open = price;
                    self.high = price;
                    self.low = price;
                    self.close = price;
                    self.close_ts = Some(ts);
                    self.open_ts = Some(ts);
                    Some((prev_close, bar_close_ts))
                } else {
                    // Within the same bucket: pick the tick with the largest
                    // exchange ts as the canonical close (deterministic across
                    // processes); fall back to last-write-wins if ts info is
                    // missing. The open price is locked to the earliest ts.
                    if price > self.high {
                        self.high = price;
                    }
                    if price < self.low || self.low.is_zero() {
                        self.low = price;
                    }
                    match self.close_ts {
                        Some(prev_close_ts) if ts < prev_close_ts => {
                            // older tick — leave close unchanged
                        }
                        _ => {
                            self.close = price;
                            self.close_ts = Some(ts);
                        }
                    }
                    match self.open_ts {
                        Some(prev_open_ts) if ts >= prev_open_ts => {
                            // newer tick — open already locked to earlier ts
                        }
                        _ => {
                            self.open = price;
                            self.open_ts = Some(ts);
                        }
                    }
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromPrimitive;

    fn d(v: f64) -> Decimal {
        Decimal::from_f64(v).unwrap()
    }

    #[test]
    fn bucket_start_aligns_on_window_in_ms() {
        let b = BarBuilder::new(60);
        // 60s window = 60_000ms. Bucket for ts=1_777_680_125_400ms is the
        // 1_777_680_120_000ms boundary.
        assert_eq!(b.bucket_start(1_777_680_125_400), 1_777_680_120_000);
        assert_eq!(b.bucket_start(1_777_680_180_000), 1_777_680_180_000);
    }

    #[test]
    fn within_bucket_largest_ts_wins_at_ms_resolution() {
        // Two ticks in the same wall-clock second but different ms — the
        // later one wins. Pre-#276 (seconds resolution) both ticks looked
        // identical and last-write-wins produced non-determinism across
        // hosts.
        let mut b = BarBuilder::new(60);
        assert!(b.push(1_777_680_125_100, d(78_100.0)).is_none());
        // Older ms inside the same second — must NOT overwrite close.
        assert!(b.push(1_777_680_125_050, d(99_999.0)).is_none());
        // Newer ms inside the same second — must overwrite close.
        assert!(b.push(1_777_680_125_900, d(78_500.0)).is_none());
        // Crossing into next bucket emits the prior close = 78_500.
        let emitted = b.push(1_777_680_180_000, d(78_600.0));
        assert!(emitted.is_some());
        let (close, close_ts) = emitted.unwrap();
        assert_eq!(close, d(78_500.0));
        // Bar close ts is the bucket end, not the last-tick ts.
        assert_eq!(close_ts, 1_777_680_180_000);
    }

    #[test]
    fn cross_bucket_emits_at_window_boundary_ms() {
        let mut b = BarBuilder::new(60);
        b.push(1_777_680_120_000, d(100.0));
        let out = b.push(1_777_680_240_000, d(200.0)).unwrap();
        // Two buckets apart: emitted bar close = bucket_start of first
        // bucket + window = 1_777_680_120_000 + 60_000.
        assert_eq!(out.1, 1_777_680_180_000);
    }
}
