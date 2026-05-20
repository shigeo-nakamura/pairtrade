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
                    self.refine_in_bucket(ts, price);
                    None
                }
            }
        }
    }

    /// Refine the in-progress bucket's OHLC from a tick that does NOT own
    /// emit authority. Used by the WS arm in Phase 2 v2 (bot-strategy#341):
    /// WS pushes refine the close (largest-ts-wins) so that bots subscribed
    /// to the same Lighter feed converge on the same close, but only the
    /// polling arm's `push` advances the bucket boundary. Ticks past the
    /// current bucket are dropped here — the polling arm will pick them up.
    pub(super) fn update_close_only(&mut self, ts: i64, price: Decimal) {
        let current_bucket = self.bucket_start(ts);
        match self.start_ts {
            None => {
                // Bootstrap from a WS tick if polling has not yet seeded the
                // builder. The polling arm will advance the bucket on the
                // next boundary crossing as usual.
                self.start_ts = Some(current_bucket);
                self.open = price;
                self.high = price;
                self.low = price;
                self.close = price;
                self.close_ts = Some(ts);
                self.open_ts = Some(ts);
            }
            Some(start) => {
                if current_bucket != start {
                    // Tick is for a different (past or future) bucket. The
                    // polling arm owns bucket transitions; ignore here so
                    // the two paths never race on `start_ts`.
                    return;
                }
                self.refine_in_bucket(ts, price);
            }
        }
    }

    /// Force-emit the in-progress bucket if it has been open longer than
    /// `1.5 * window_ms` relative to `now_ms`, advancing `start_ts` to
    /// `bucket_start(now_ms)`. Returns `Some((close, close_ts))` on emit.
    /// This is a defensive backstop for the case where both WS and polling
    /// stop delivering ticks — without it, BarBuilder would wait
    /// indefinitely for the next crossing tick. bot-strategy#341.
    pub(super) fn force_close_if_stale(&mut self, now_ms: i64) -> Option<(Decimal, i64)> {
        let start = self.start_ts?;
        if self.window_ms <= 0 {
            return None;
        }
        let elapsed = now_ms.saturating_sub(start);
        let threshold = self.window_ms.saturating_mul(3) / 2; // 1.5 × window
        if elapsed < threshold {
            return None;
        }
        let prev_close = self.close;
        let bar_close_ts = start.saturating_add(self.window_ms);
        let new_bucket = self.bucket_start(now_ms);
        self.start_ts = Some(new_bucket);
        // The new bucket has no observed tick yet; carry the previous close
        // as a placeholder so the next real tick can refine. close_ts is
        // reset to None so any incoming tick wins via largest-ts-wins.
        self.open = prev_close;
        self.high = prev_close;
        self.low = prev_close;
        self.close = prev_close;
        self.close_ts = None;
        self.open_ts = None;
        Some((prev_close, bar_close_ts))
    }

    fn refine_in_bucket(&mut self, ts: i64, price: Decimal) {
        // Within the same bucket: pick the tick with the largest exchange
        // ts as the canonical close (deterministic across processes); fall
        // back to last-write-wins if ts info is missing. The open price
        // is locked to the earliest ts.
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
    fn update_close_only_refines_close_without_emitting() {
        let mut b = BarBuilder::new(60);
        // Polling seeds the bucket
        assert!(b.push(1_777_680_125_000, d(100.0)).is_none());
        // WS pushes a later tick within the same bucket — refines close
        b.update_close_only(1_777_680_140_000, d(105.0));
        // Polling at boundary emits with the WS-refined close
        let out = b.push(1_777_680_180_000, d(110.0));
        assert!(out.is_some());
        let (close, close_ts) = out.unwrap();
        assert_eq!(close, d(105.0));
        assert_eq!(close_ts, 1_777_680_180_000);
    }

    #[test]
    fn update_close_only_largest_ts_wins() {
        let mut b = BarBuilder::new(60);
        b.push(1_777_680_125_500, d(100.0));
        // Older WS tick must NOT overwrite close
        b.update_close_only(1_777_680_125_100, d(999.0));
        // Newer WS tick wins
        b.update_close_only(1_777_680_125_900, d(105.0));
        let out = b.push(1_777_680_180_000, d(110.0)).unwrap();
        assert_eq!(out.0, d(105.0));
    }

    #[test]
    fn update_close_only_drops_ticks_outside_current_bucket() {
        let mut b = BarBuilder::new(60);
        b.push(1_777_680_125_000, d(100.0));
        // Tick from a future bucket — must not advance start_ts (polling
        // owns transitions). Close should be unchanged when next polling
        // tick crosses the boundary.
        b.update_close_only(1_777_680_240_000, d(999.0));
        let out = b.push(1_777_680_180_000, d(110.0)).unwrap();
        assert_eq!(out.0, d(100.0));
    }

    #[test]
    fn update_close_only_bootstraps_when_no_polling_seed() {
        let mut b = BarBuilder::new(60);
        // WS arrives first, before any polling tick
        b.update_close_only(1_777_680_125_000, d(100.0));
        // Polling at boundary emits the WS-seeded close
        let out = b.push(1_777_680_180_000, d(200.0)).unwrap();
        assert_eq!(out.0, d(100.0));
        assert_eq!(out.1, 1_777_680_180_000);
    }

    #[test]
    fn force_close_if_stale_emits_after_threshold() {
        let mut b = BarBuilder::new(60);
        // Seed bucket at 12:00:00.000
        b.push(1_777_680_120_000, d(100.0));
        // 1.0 × window: not yet stale
        assert!(b.force_close_if_stale(1_777_680_180_000).is_none());
        // 1.5 × window: emit
        let out = b.force_close_if_stale(1_777_680_210_000).unwrap();
        assert_eq!(out.0, d(100.0));
        assert_eq!(out.1, 1_777_680_180_000);
    }

    #[test]
    fn force_close_advances_to_current_bucket() {
        let mut b = BarBuilder::new(60);
        b.push(1_777_680_120_000, d(100.0));
        // Force-close after 3 buckets of silence
        b.force_close_if_stale(1_777_680_300_000).unwrap();
        // Next push at 1_777_680_360_000 should emit the carried close
        // (bucket starting at 1_777_680_300_000 + window = 1_777_680_360_000)
        let out = b.push(1_777_680_360_000, d(200.0)).unwrap();
        assert_eq!(out.0, d(100.0));
        assert_eq!(out.1, 1_777_680_360_000);
    }

    #[test]
    fn force_close_returns_none_when_uninitialized() {
        let mut b = BarBuilder::new(60);
        assert!(b.force_close_if_stale(1_777_680_300_000).is_none());
    }

    /// bot-strategy#341 Phase 2 v2 regression test.
    ///
    /// Original Phase 2 (#276) had a layer-1 bug: WS-driven `push()` only
    /// emitted on bucket-boundary crossings, so any WS gap across a minute
    /// boundary produced 0 bars for that minute (78 h of post-restart
    /// β-freeze on Tokyo Lighter). v2 hands emit authority back to the
    /// polling arm; even with WS pushing irregular ticks (1-30 s gaps,
    /// plus a 5 min silence), polling at the configured interval
    /// guarantees ~10 bars over 10 simulated minutes.
    #[test]
    fn polling_emits_bars_under_irregular_ws_cadence() {
        let window_secs: u64 = 60;
        let interval_secs: u64 = 5;
        let total_secs: u64 = 10 * 60;

        let mut b = BarBuilder::new(window_secs);
        let mut emits: Vec<i64> = Vec::new();
        let mut price = 100.0;

        // Construct a deterministic-but-uneven WS schedule: gaps drawn
        // from {1, 5, 12, 30}s plus a single 5 min silence at t≈4 min.
        let ws_schedule: Vec<u64> = {
            let mut v: Vec<u64> = Vec::new();
            let mut t: u64 = 1;
            let gaps = [1u64, 5, 12, 30];
            let mut gi = 0;
            while t < 4 * 60 {
                v.push(t);
                t += gaps[gi % gaps.len()];
                gi += 1;
            }
            // 5 min silence: jump straight from ~t=240 to t=540
            t = 540;
            while t < total_secs {
                v.push(t);
                t += gaps[gi % gaps.len()];
                gi += 1;
            }
            v
        };
        let mut ws_idx = 0;

        let base_ts: i64 = 1_777_680_120_000; // bucket-aligned
        for sec in 1..=total_secs {
            // Drain any WS ticks scheduled before this poll
            while ws_idx < ws_schedule.len() && ws_schedule[ws_idx] <= sec {
                let ts = base_ts + (ws_schedule[ws_idx] as i64) * 1000;
                price += 0.1;
                b.update_close_only(ts, d(price));
                ws_idx += 1;
            }
            if sec % interval_secs == 0 {
                let ts = base_ts + (sec as i64) * 1000;
                if let Some((_close, close_ts)) = b.push(ts, d(price)) {
                    emits.push(close_ts);
                }
            }
        }
        // 10 minutes / 1 bar per minute = 10 expected emits. Allow 9 to
        // tolerate the trailing minute being still in-progress when the
        // loop ends.
        assert!(
            emits.len() >= 9,
            "expected ≥9 bars over 10 min, got {} (emits={:?})",
            emits.len(),
            emits
        );
    }

    /// bot-strategy#341 Phase 2 v2 cross-host determinism.
    ///
    /// Two BarBuilders observe the same WS feed but have different polling
    /// phases (one offset 2 s from the other). They must produce identical
    /// bar closes — that's the original Phase 2 motivation. Largest-ts-wins
    /// in update_close_only ensures the WS-driven close is the same across
    /// hosts even though the polling cadences differ.
    #[test]
    fn two_builders_converge_on_same_close_under_phase_offset() {
        let window_secs: u64 = 60;
        let total_secs: u64 = 5 * 60;
        let base_ts: i64 = 1_777_680_120_000;

        let mut a = BarBuilder::new(window_secs);
        let mut b = BarBuilder::new(window_secs);
        let mut closes_a: Vec<(Decimal, i64)> = Vec::new();
        let mut closes_b: Vec<(Decimal, i64)> = Vec::new();

        // Generate WS ticks at 1.3 s nominal cadence with deterministic
        // jitter so within-bucket close is non-trivial.
        let mut ws: Vec<(u64, f64)> = Vec::new();
        let mut t_ms: u64 = 100;
        let mut p = 100.0_f64;
        let mut step_ms = 1300u64;
        while t_ms / 1000 < total_secs {
            ws.push((t_ms, p));
            p += 0.05;
            t_ms = t_ms.saturating_add(step_ms);
            step_ms = if step_ms == 1300 { 1700 } else { 1300 };
        }

        // Bot A polls at sec % 5 == 0
        // Bot B polls at sec % 5 == 2 (phase-shifted)
        // Loop a bit past total_secs so both polling phases get a chance
        // to cross every bucket boundary that occurred during the WS feed.
        let mut wsi = 0;
        for sec in 1..=(total_secs + 5) {
            // Drain WS up to this second for both bots
            while wsi < ws.len() && ws[wsi].0 / 1000 <= sec - 1 {
                let (ts_ms_inner, price) = ws[wsi];
                let ts = base_ts + ts_ms_inner as i64;
                let dp = d(price);
                a.update_close_only(ts, dp);
                b.update_close_only(ts, dp);
                wsi += 1;
            }
            if sec % 5 == 0 {
                let ts = base_ts + (sec as i64) * 1000;
                if let Some(out) = a.push(ts, d(p)) {
                    closes_a.push(out);
                }
            }
            if sec % 5 == 2 {
                let ts = base_ts + (sec as i64) * 1000;
                if let Some(out) = b.push(ts, d(p)) {
                    closes_b.push(out);
                }
            }
        }

        // Both bots must have emitted bars at the same wall-clock
        // boundaries with identical closes. A's emits land at
        // bucket_end (every 60 s). B's emits also land at bucket_end
        // because bucket_end = bucket_start + window, independent of
        // polling phase.
        assert_eq!(
            closes_a.len(),
            closes_b.len(),
            "emit counts diverge: A={} B={}",
            closes_a.len(),
            closes_b.len()
        );
        for (i, (xa, xb)) in closes_a.iter().zip(closes_b.iter()).enumerate() {
            assert_eq!(xa.0, xb.0, "close diverges at bucket #{}", i);
            assert_eq!(xa.1, xb.1, "close_ts diverges at bucket #{}", i);
        }
    }
}
