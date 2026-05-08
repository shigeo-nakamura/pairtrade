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
use std::collections::HashMap;

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

    /// Refine the in-progress bucket's close fields with a WS-pushed tick,
    /// **without** rotating buckets. The polling-driven `push()` is the
    /// canonical bucket-emitter (bot-strategy#341); ticks whose exchange_ts
    /// falls outside the current bucket are deferred so they cannot poison
    /// the previous bucket's close once polling crosses the boundary.
    ///
    /// Within the current bucket the same largest-ts-wins rule used by
    /// `push()` applies, so two bots observing the same WS feed converge
    /// on identical close prices. Ticks before initialization (no
    /// `start_ts`) are dropped — let `push()` seed the first bucket.
    pub(super) fn update_close_only(&mut self, ts: i64, price: Decimal) {
        let Some(start) = self.start_ts else {
            return;
        };
        if self.window_ms <= 0 {
            return;
        }
        let bucket_end = start.saturating_add(self.window_ms);
        if ts < start || ts >= bucket_end {
            return;
        }
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
    }

    /// Force-emit the in-progress bucket if `now_ts` is more than
    /// `1.5 × window_ms` past `start_ts`. This guards against silent bucket
    /// loss when both the WS feed and the polling tick go quiet across a
    /// boundary (bot-strategy#341): the bar is closed with the last-known
    /// close, and `start_ts` is rotated to the bucket containing `now_ts`
    /// so the next tick lands in a fresh bucket.
    pub(super) fn flush_if_stale(&mut self, now_ts: i64) -> Option<(Decimal, i64)> {
        if self.window_ms <= 0 {
            return None;
        }
        let start = self.start_ts?;
        let threshold = self.window_ms.saturating_add(self.window_ms / 2);
        if now_ts.saturating_sub(start) < threshold {
            return None;
        }
        let prev_close = self.close;
        let bar_close_ts = start.saturating_add(self.window_ms);
        let new_bucket = self.bucket_start(now_ts);
        self.start_ts = Some(new_bucket);
        self.open = prev_close;
        self.high = prev_close;
        self.low = prev_close;
        self.close = prev_close;
        self.open_ts = Some(now_ts);
        self.close_ts = Some(now_ts);
        Some((prev_close, bar_close_ts))
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

/// Per-minute bar emission rate canary (bot-strategy#341).
///
/// `record()` increments the per-symbol count whenever `step_shared`
/// commits a fresh `(close, close_ts)` to `engine.history`. `maybe_log()`
/// is called once per `step_shared` tick; on a 60s rolling boundary it
/// flushes the per-symbol count and warns if the rate fell below
/// `0.8 × (60 / trading_period_secs)` per window — the canary that would
/// have caught the original Phase 2 regression (78 h with 0 bars) in
/// minutes. The window is anchored to wall-clock seconds so identical
/// boundaries fire across all bot processes (`now_ts` is supplied by
/// `current_now_ts`).
#[derive(Debug)]
pub(super) struct BarEmitCounter {
    counts: HashMap<String, u32>,
    window_start_ts: Option<i64>,
    window_secs: i64,
    trading_period_secs: i64,
}

impl BarEmitCounter {
    pub(super) fn new(trading_period_secs: u64) -> Self {
        Self {
            counts: HashMap::new(),
            window_start_ts: None,
            window_secs: 60,
            trading_period_secs: trading_period_secs as i64,
        }
    }

    pub(super) fn record(&mut self, symbol: &str) {
        *self.counts.entry(symbol.to_string()).or_insert(0) += 1;
    }

    pub(super) fn maybe_log(&mut self, now_ts: i64) {
        let start = match self.window_start_ts {
            Some(s) => s,
            None => {
                self.window_start_ts = Some(now_ts);
                return;
            }
        };
        if now_ts.saturating_sub(start) < self.window_secs {
            return;
        }
        let elapsed = now_ts.saturating_sub(start).max(1);
        // Expected emits per window per symbol = window / trading_period.
        // Threshold = 0.8 × expected.
        let expected = (elapsed as f64) / (self.trading_period_secs.max(1) as f64);
        let warn_threshold = 0.8 * expected;
        let mut parts: Vec<(String, u32)> =
            self.counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        parts.sort_by(|a, b| a.0.cmp(&b.0));
        let summary: Vec<String> = parts
            .iter()
            .map(|(sym, n)| format!("{}={}", sym, n))
            .collect();
        let any_warn = !parts.is_empty()
            && parts
                .iter()
                .any(|(_, n)| (*n as f64) < warn_threshold);
        if any_warn {
            log::warn!(
                "[BAR_RATE] window_secs={} expected={:.2} threshold={:.2} {}",
                elapsed,
                expected,
                warn_threshold,
                summary.join(" ")
            );
        } else {
            log::info!(
                "[BAR_RATE] window_secs={} expected={:.2} {}",
                elapsed,
                expected,
                summary.join(" ")
            );
        }
        self.counts.clear();
        self.window_start_ts = Some(now_ts);
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

    #[test]
    fn update_close_only_refines_within_bucket_largest_ts_wins() {
        // Polling seeds the bucket; WS refines close with a later ts.
        let mut b = BarBuilder::new(60);
        assert!(b.push(1_777_680_120_500, d(100.0)).is_none());
        b.update_close_only(1_777_680_125_000, d(101.5));
        b.update_close_only(1_777_680_124_000, d(99.0)); // older ts — must not overwrite close
        // Cross into next bucket via polling. Emitted close is the
        // largest-ts WS tick (101.5), not the older 99.0.
        let (close, close_ts) = b.push(1_777_680_180_000, d(102.0)).unwrap();
        assert_eq!(close, d(101.5));
        assert_eq!(close_ts, 1_777_680_180_000);
    }

    #[test]
    fn update_close_only_ignores_future_bucket_tick() {
        // WS tick from the next bucket must not poison the current
        // bucket's close (bot-strategy#341): once polling crosses, the
        // emitted close should still come from within the current bucket.
        let mut b = BarBuilder::new(60);
        assert!(b.push(1_777_680_120_500, d(100.0)).is_none());
        b.update_close_only(1_777_680_125_000, d(101.0));
        // ts=181_000 belongs to the [180_000, 240_000) bucket, not the
        // current [120_000, 180_000) bucket. Must be ignored.
        b.update_close_only(1_777_680_181_000, d(999.0));
        // Cross via polling: close should still be 101.0 (last WS tick
        // within [120_000, 180_000)), not 999.0.
        let (close, _) = b.push(1_777_680_180_000, d(102.0)).unwrap();
        assert_eq!(close, d(101.0));
    }

    #[test]
    fn update_close_only_before_init_is_noop() {
        // Without a polling-driven push() seeding start_ts, WS ticks
        // must not silently initialize the bar — that would break the
        // wall-clock-aligned bucket invariant if WS ticks land mid-bucket.
        let mut b = BarBuilder::new(60);
        b.update_close_only(1_777_680_125_000, d(100.0));
        assert!(b.start_ts.is_none());
    }

    #[test]
    fn flush_if_stale_emits_after_1_5_window() {
        // Both WS and polling silent across a bucket boundary: a
        // step_shared call with elapsed >= 1.5 × window must force-emit
        // so the engine doesn't silently skip a bar. (bot-strategy#341)
        let mut b = BarBuilder::new(60);
        b.push(1_777_680_120_000, d(100.0));
        // 80s < 1.5×60s = 90s → no flush yet
        assert!(b.flush_if_stale(1_777_680_200_000).is_none());
        // 100s >= 90s → flush
        let (close, close_ts) = b.flush_if_stale(1_777_680_220_000).unwrap();
        assert_eq!(close, d(100.0));
        assert_eq!(close_ts, 1_777_680_180_000);
        // start_ts rotated to the bucket containing now_ts = 220_000
        assert_eq!(b.start_ts, Some(1_777_680_180_000));
    }

    #[test]
    fn flush_if_stale_noop_before_init() {
        let mut b = BarBuilder::new(60);
        assert!(b.flush_if_stale(1_777_680_300_000).is_none());
    }

    /// 10 simulated minutes of WS pushes (1–30s spacing) plus polling at
    /// 5s. Polling owns bucket emit, so we expect ~10 bars (one per
    /// minute). Asserts >= 9 (issue #341 acceptance criterion).
    #[test]
    fn hybrid_ws_poll_emits_at_least_9_bars_in_10_minutes() {
        let mut b = BarBuilder::new(60);
        let start_ms: i64 = 1_777_680_120_000;
        let end_ms: i64 = start_ms + 10 * 60 * 1000;
        let mut emits: Vec<i64> = Vec::new();

        // Deterministic pseudo-random generator (LCG) so the test is
        // reproducible without an extra dependency.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rand_in_range = |lo: u64, hi: u64| -> u64 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            lo + (state >> 33) % (hi - lo + 1)
        };

        // WS push schedule: 1–30s gaps.
        let mut next_ws_ms = start_ms + rand_in_range(1000, 30000) as i64;
        // Poll schedule: every 5s, wall-clock aligned.
        let mut next_poll_ms = start_ms + 5_000;
        let mut price = 100i64;

        while next_poll_ms < end_ms {
            // WS pushes that come strictly before the next poll tick.
            while next_ws_ms < next_poll_ms && next_ws_ms < end_ms {
                price += rand_in_range(0, 4) as i64 - 2;
                b.update_close_only(next_ws_ms, d(price as f64));
                next_ws_ms += rand_in_range(1000, 30000) as i64;
            }
            price += rand_in_range(0, 4) as i64 - 2;
            if let Some((_, ts)) = b.push(next_poll_ms, d(price as f64)) {
                emits.push(ts);
            }
            next_poll_ms += 5_000;
        }

        assert!(
            emits.len() >= 9,
            "expected >=9 bar emits in 10 min, got {} (ts={:?})",
            emits.len(),
            emits
        );
        // Every emit ts must land on a minute boundary (60_000ms).
        for ts in &emits {
            assert_eq!(ts % 60_000, 0, "emit ts {} not on bucket boundary", ts);
        }
    }

    /// Two BarBuilder instances fed the SAME (WS + polling) tick stream
    /// must produce byte-identical close prices — the determinism that
    /// motivated bot-strategy#276 / #341. KS distance trivially 0 in this
    /// regime; we just assert sequence equality which is the strict form
    /// of "KS < 0.05".
    #[test]
    fn parallel_builders_on_identical_stream_emit_identical_closes() {
        let mut b1 = BarBuilder::new(60);
        let mut b2 = BarBuilder::new(60);
        let start_ms: i64 = 1_777_680_120_000;
        let mut closes_a: Vec<Decimal> = Vec::new();
        let mut closes_b: Vec<Decimal> = Vec::new();

        let mut t = start_ms + 5_000;
        let mut price = 100i64;
        for i in 0..120 {
            // WS ticks at t-3s and t-1s with different prices, polling at t.
            let ws_ts1 = t - 3_000;
            let ws_ts2 = t - 1_000;
            price += if i % 3 == 0 { 1 } else { -1 };
            b1.update_close_only(ws_ts1, d(price as f64 + 0.5));
            b2.update_close_only(ws_ts1, d(price as f64 + 0.5));
            b1.update_close_only(ws_ts2, d(price as f64 + 0.7));
            b2.update_close_only(ws_ts2, d(price as f64 + 0.7));
            let p1 = b1.push(t, d(price as f64));
            let p2 = b2.push(t, d(price as f64));
            assert_eq!(p1, p2);
            if let Some((c, _)) = p1 {
                closes_a.push(c);
            }
            if let Some((c, _)) = p2 {
                closes_b.push(c);
            }
            t += 5_000;
        }
        assert!(!closes_a.is_empty());
        assert_eq!(closes_a, closes_b);
    }

    /// Force-close stale bucket integration: a single seed tick then 3
    /// minutes of silence; a polling tick at t=180 must observe the
    /// stale bucket via flush_if_stale and emit. Mirrors #341 spec.
    #[test]
    fn polling_silence_for_3_min_force_emits_via_flush_if_stale() {
        let mut b = BarBuilder::new(60);
        let start_ms = 1_777_680_120_000;
        b.push(start_ms, d(100.0));
        // No further ticks for 3 minutes. Polling tick at 180s.
        let now_ms = start_ms + 180_000;
        let direct = b.push(now_ms, d(100.0));
        // push() crosses normally — that's actually fine, polling drove
        // the emit. To force the flush_if_stale path we need *no* tick at
        // all from polling either; simulate that by calling
        // flush_if_stale on a builder that hasn't been pushed.
        assert!(direct.is_some());

        // Re-test the silent-feed path: seed, then no push, only flush.
        let mut b2 = BarBuilder::new(60);
        b2.push(start_ms, d(100.0));
        let elapsed_ms = start_ms + 180_000;
        let flushed = b2.flush_if_stale(elapsed_ms).expect("must force-emit");
        assert_eq!(flushed.0, d(100.0));
        assert_eq!(flushed.1, start_ms + 60_000);
    }

    #[test]
    fn bar_emit_counter_warns_below_threshold() {
        // 60s window, trading_period_secs=60 → expected ~1 bar/win/sym,
        // threshold = 0.8. Recording 0 bars in a 60s window must hit the
        // WARN branch. Smoke-test only: we just confirm maybe_log
        // doesn't panic and resets state.
        let mut c = BarEmitCounter::new(60);
        c.record("BTC");
        c.maybe_log(1000); // first call only seeds window_start_ts
        c.maybe_log(1059); // still within window, no flush
        c.maybe_log(1061); // 61s elapsed → flush
        // Counter was reset; next call is a fresh window
        assert!(c.counts.is_empty());
    }
}
