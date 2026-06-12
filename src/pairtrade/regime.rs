//! Regime filter: block entries during high-volatility or strong-trend
//! periods of a reference asset (typically BTC).
//!
//! This module also hosts the innovation-responsive persistent-regime
//! detector (`RegimeDetector`, bot-strategy#494) — a separate signal that
//! watches the hedge-ratio model's one-step residuals to distinguish a
//! *persistent* β/relationship shift from a single corrupted bar or normal
//! estimator noise. See the `RegimeDetector` docs below.

use std::collections::VecDeque;

use super::stats::PriceSample;

#[derive(Debug, Clone, Copy)]
pub(super) struct RegimeState {
    pub(super) realized_vol: f64,
    pub(super) trend_strength: f64,
}

/// Compute regime indicators from a symbol's price history.
///
/// `realized_vol` – standard deviation of per-bar log returns over the
/// last `vol_window` bars.
///
/// `trend_strength` – |slope / std| of log prices over the last
/// `trend_window` bars (same normalisation as `spread_slope_sigma`).
///
/// Returns `None` when there is not enough data.
pub(super) fn compute_regime(
    history: &VecDeque<PriceSample>,
    vol_window: usize,
    trend_window: usize,
) -> Option<RegimeState> {
    let need = vol_window.max(trend_window) + 1;
    if history.len() < need {
        return None;
    }

    // --- realized vol (std of log returns) ---
    let vol = {
        let start = history.len() - vol_window - 1;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        for i in 1..=vol_window {
            let r = history[start + i].log_price - history[start + i - 1].log_price;
            sum += r;
            sum_sq += r * r;
        }
        let n = vol_window as f64;
        let mean = sum / n;
        let var = (sum_sq / n) - mean * mean;
        var.max(0.0).sqrt()
    };

    // --- trend strength (|slope / std| of log prices) ---
    let trend = {
        let start = history.len() - trend_window;
        let n = trend_window as f64;
        let mean_i = (n - 1.0) / 2.0;
        let mut mean_p = 0.0;
        for j in 0..trend_window {
            mean_p += history[start + j].log_price;
        }
        mean_p /= n;
        let mut cov = 0.0;
        let mut var_i = 0.0;
        let mut var_p = 0.0;
        for j in 0..trend_window {
            let di = j as f64 - mean_i;
            let dp = history[start + j].log_price - mean_p;
            cov += di * dp;
            var_i += di * di;
            var_p += dp * dp;
        }
        let std_p = (var_p / n).max(0.0).sqrt();
        let slope = if var_i.abs() < 1e-15 {
            0.0
        } else {
            cov / var_i
        };
        if std_p < 1e-9 {
            0.0
        } else {
            (slope / std_p).abs()
        }
    };

    Some(RegimeState {
        realized_vol: vol,
        trend_strength: trend,
    })
}

/// Returns `true` when the current regime allows entry.
/// A threshold of 0.0 disables that dimension.
pub(super) fn regime_allows_entry(
    regime: Option<RegimeState>,
    vol_max: f64,
    trend_max: f64,
) -> bool {
    if vol_max <= 0.0 && trend_max <= 0.0 {
        return true; // filter disabled
    }
    let Some(r) = regime else {
        return true; // no data → allow
    };
    if vol_max > 0.0 && r.realized_vol > vol_max {
        return false;
    }
    if trend_max > 0.0 && r.trend_strength > trend_max {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Innovation-responsive persistent-regime detector (bot-strategy#494)
// ---------------------------------------------------------------------------

/// EWMA smoothing for the robust residual scale. ~`1/alpha` ≈ 50-sample
/// memory — long enough to be stable, short enough to track a genuine
/// volatility regime change in the residuals.
const SCALE_ALPHA: f64 = 0.02;

/// Winsorise the per-tick input to the scale EWMA at this multiple of the
/// current scale. Without it a single corrupted bar would inflate the scale
/// estimate (and then suppress detection of the real shift that follows).
/// MAD-like robustness for the denominator.
const SCALE_SPIKE_CAP: f64 = 8.0;

/// Expected magnitude of the normalised innovation under the null. The robust
/// scale is an EWMA of `|innovation|`, so `E[|innovation| / scale]` is ~1. The
/// CUSUM accumulates the *excess* of `|normalised innovation|` over this
/// baseline: a beta/relationship shift makes the residual `~ (beta_true-beta)
/// * dx`, whose magnitude rises persistently while its sign tracks the
/// zero-mean, sign-alternating `dx`. Tracking magnitude — not signed value —
/// is what lets the detector see such a shift at all.
const MAG_REF: f64 = 1.0;

/// `|normalised innovation|` at or above this is treated as an outlier:
/// winsorised out of the scale EWMA and contributing only the slack decay
/// (never growth) to the CUSUM. A genuine persistent shift shows up as *many*
/// moderately-elevated bars (~2-4 sigma) once the scale absorbs it, not as the
/// much-larger-than-4-sigma spike(s) of a corrupted observation.
const INNOVATION_CLIP: f64 = 4.0;

/// CUSUM slack / reference value `k` (Page's test). Once the per-tick excess
/// magnitude beats `k` the statistic accumulates; under the null it decays
/// toward zero, so the detector responds to a *persistent* magnitude shift
/// rather than a single jump.
const CUSUM_K: f64 = 0.5;

/// Activation threshold `h_on`. The statistic must exceed this for the
/// persistent-shift state to turn on.
const CUSUM_H_ON: f64 = 6.0;

/// Deactivation threshold `h_off` (< `h_on`). Hysteresis: once active, the
/// state stays on until the statistic decays below this, so it does not
/// flap on a statistic hovering at `h_on`.
const CUSUM_H_OFF: f64 = 3.0;

/// Warm-up: ticks required before the detector emits any signal, so an
/// unstable early scale estimate cannot create false activations.
const MIN_UPDATES: u64 = 60;

// The detectable band must be non-empty: only bars with `|z| < INNOVATION_CLIP`
// accumulate, and a moderately-elevated such bar has to produce a *positive*
// CUSUM increment (`mag - MAG_REF - CUSUM_K > 0` for some `mag < INNOVATION_CLIP`),
// otherwise nothing could ever accumulate. Equivalent to the cutoff sitting
// above the slack+baseline.
const _: () = assert!(
    MAG_REF + CUSUM_K < INNOVATION_CLIP,
    "sub-outlier elevated bars must be able to accumulate",
);
const _: () = assert!(
    CUSUM_H_OFF < CUSUM_H_ON,
    "hysteresis requires the clear threshold below the activation threshold",
);
// NOTE on corrupted-observation immunity: it is enforced in `update`'s control
// flow, not by a numeric inequality. *Every* outlier (`|z| ≥ INNOVATION_CLIP`)
// only ever *decays* the CUSUM. A single corrupted price bar is a difference
// input, so it spans a *pair* of consecutive outlier innovations (spike +
// reversal); because both — and any-length outlier burst — are decay-only, no
// corrupted observation can push the detector toward activation from any
// pre-existing CUSUM level. A genuine persistent shift is still caught once the
// winsorised scale EWMA absorbs the new level (a few bars) and the residual
// drops into the accumulating `|z| < INNOVATION_CLIP` band. See
// `single_corrupted_price_bar_roundtrip_does_not_activate`.

/// Outcome of a single `RegimeDetector::update`, used by the caller to log
/// state transitions exactly once (not every tick).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RegimeTransition {
    /// No state change this tick.
    None,
    /// The persistent-shift state turned on this tick.
    Activated,
    /// The persistent-shift state turned off this tick.
    Cleared,
}

/// Innovation-responsive persistent-regime detector (bot-strategy#494).
///
/// Consumes the hedge-ratio model's one-step innovation each bar:
///
/// ```text
///   innovation_t = Δspread_t = dy_t − β_t · dx_t
/// ```
///
/// where `dx`/`dy` are the consecutive log-return differences of the
/// quote / base assets and `β_t` is the hedging β actually in use (so the
/// signal is valid whether or not the Kalman path is enabled). The
/// innovation is normalised by a robust, winsorised EWMA scale; the
/// *magnitude* of that normalised residual feeds a one-sided CUSUM (Page's
/// test) with hysteresis.
///
/// Magnitude — not signed value — is the right statistic here. A β shift
/// makes the residual `≈ (β_true − β)·dx`; because `dx` is ~zero-mean and
/// flips sign tick-to-tick, the *signed* innovation alternates and would
/// cancel in a signed CUSUM, but its magnitude stays persistently elevated.
/// Accumulating the excess of `|normalised innovation|` over its null mean
/// therefore catches both a one-sided mispricing drift *and* an
/// alternating-sign variance/relationship break. A *persistent* rise past
/// `h_on` is the signature of a broken/shifted relationship; normal noise and
/// single corrupted bars do not reach it.
///
/// Phase 1 is shadow-only: the engine emits `pairtrade_regime_*` gauges and
/// logs transitions, and `is_active()` only gates entries when a variant
/// opts in via `regime_block_entries` (default off).
#[derive(Debug, Clone, Default)]
pub(super) struct RegimeDetector {
    /// Robust residual scale (winsorised EWMA of `|innovation|`).
    scale: f64,
    /// One-sided CUSUM on the excess of `|normalised innovation|` over
    /// `MAG_REF`. Detects a persistent rise in residual *magnitude* (a
    /// β/relationship shift), independent of the innovation's sign.
    cusum: f64,
    updates: u64,
    active: bool,
    /// Replay timestamp at which the current active state began; `None`
    /// while inactive. Drives `active_secs`.
    active_since_ts: Option<i64>,
    /// Last normalised innovation, surfaced as a shadow gauge.
    last_normalized: f64,
    /// Highest CUSUM observed since process start and its replay/market
    /// timestamp. Used by byte-exact calibration even when `h_on` is never
    /// crossed and no transition log is emitted.
    peak_cusum: f64,
    peak_cusum_ts: Option<i64>,
    /// Highest CUSUM observed since the last `take_interval_peak` call.
    /// Drives the periodic `[REGIME_SHADOW]` series log so threshold
    /// calibration sees the per-interval envelope, not just the sampled
    /// point value (the CUSUM can spike and decay between samples).
    interval_peak: f64,
}

impl RegimeDetector {
    /// Feed one bar's innovation. Returns whether the active state changed
    /// this tick so the caller can log the transition once.
    pub(super) fn update(&mut self, innovation: f64, now_ts: i64) -> RegimeTransition {
        self.updates += 1;

        // Robust residual scale: winsorised EWMA of |innovation|. Seeding on
        // the first sample avoids a long ramp from zero.
        let abs = innovation.abs();
        if self.scale <= 0.0 {
            self.scale = abs;
        } else {
            let capped = abs.min(self.scale * SCALE_SPIKE_CAP);
            self.scale = (1.0 - SCALE_ALPHA) * self.scale + SCALE_ALPHA * capped;
        }

        // Suppress any signal until warmed up and the scale is usable.
        if self.updates < MIN_UPDATES || self.scale < 1e-12 {
            self.last_normalized = 0.0;
            return RegimeTransition::None;
        }

        let normalized = innovation / self.scale;
        self.last_normalized = normalized;
        let mag = normalized.abs();

        // Magnitude CUSUM with slack `k`. The increment is the excess of the
        // normalised residual magnitude over its null mean (`MAG_REF`), so the
        // statistic decays under normal noise and only climbs on a *persistent*
        // magnitude shift — the innovation's sign is irrelevant, so an
        // alternating-sign β break accumulates just like a one-sided drift.
        if mag >= INNOVATION_CLIP {
            // Outlier. A single corrupted *price* observation is a difference
            // input, so it shows up as a *pair* of consecutive outlier
            // innovations — the spike on entry and its opposite-sign reversal
            // when the price returns — and an extreme genuine shift looks like
            // outliers too until the scale adapts. None of these carry a
            // reliable persistence signal, so every outlier contributes ONLY
            // the slack decay, never growth. No corrupted observation, however
            // many outlier innovations it spans, can move the CUSUM toward
            // activation from any pre-existing level (structural immunity).
            //
            // A genuine persistent shift is still caught: the winsorised scale
            // EWMA absorbs the new level within a few bars, after which the
            // residual sits in the moderately-elevated (`|z| < INNOVATION_CLIP`)
            // band and accumulates normally.
            self.cusum = (self.cusum - CUSUM_K).max(0.0);
        } else {
            self.cusum = (self.cusum + (mag - MAG_REF) - CUSUM_K).max(0.0);
        }
        let stat = self.cusum;
        if stat > self.peak_cusum {
            self.peak_cusum = stat;
            self.peak_cusum_ts = Some(now_ts);
        }
        if stat > self.interval_peak {
            self.interval_peak = stat;
        }

        if !self.active {
            if stat >= CUSUM_H_ON {
                self.active = true;
                self.active_since_ts = Some(now_ts);
                return RegimeTransition::Activated;
            }
        } else if stat <= CUSUM_H_OFF {
            self.active = false;
            self.active_since_ts = None;
            return RegimeTransition::Cleared;
        }
        RegimeTransition::None
    }

    pub(super) fn is_active(&self) -> bool {
        self.active
    }

    /// The magnitude-CUSUM statistic compared against the on/off thresholds.
    /// Surfaced as a shadow gauge.
    pub(super) fn cusum(&self) -> f64 {
        self.cusum
    }

    pub(super) fn residual_scale(&self) -> f64 {
        self.scale
    }

    pub(super) fn last_normalized(&self) -> f64 {
        self.last_normalized
    }

    pub(super) fn peak_cusum(&self) -> f64 {
        self.peak_cusum
    }

    pub(super) fn peak_cusum_ts(&self) -> Option<i64> {
        self.peak_cusum_ts
    }

    /// Highest CUSUM since the previous call, for the periodic
    /// `[REGIME_SHADOW]` series log. Resets the tracker to the *current*
    /// statistic so the next interval's peak starts from the present level
    /// rather than zero.
    pub(super) fn take_interval_peak(&mut self) -> f64 {
        let peak = self.interval_peak;
        self.interval_peak = self.cusum;
        peak
    }

    /// Seconds the detector has been continuously active, or 0 when
    /// inactive. Captured before `update` by the caller so a `Cleared`
    /// transition can log the duration that just ended.
    pub(super) fn active_secs(&self, now_ts: i64) -> f64 {
        self.active_since_ts
            .map(|since| (now_ts - since).max(0) as f64)
            .unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_history(log_prices: &[f64]) -> VecDeque<PriceSample> {
        log_prices
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64 * 60,
            })
            .collect()
    }

    #[test]
    fn flat_market_low_vol_and_trend() {
        // 100 bars of constant price
        let h = make_history(&vec![10.0; 100]);
        let r = compute_regime(&h, 60, 60).unwrap();
        assert!(r.realized_vol < 1e-9);
        assert!(r.trend_strength < 1e-9);
        assert!(regime_allows_entry(Some(r), 0.001, 0.5));
    }

    #[test]
    fn trending_market_detected() {
        // Steady uptrend: 0.001 per bar over 100 bars
        let prices: Vec<f64> = (0..100).map(|i| 10.0 + 0.001 * i as f64).collect();
        let h = make_history(&prices);
        let r = compute_regime(&h, 60, 60).unwrap();
        // slope/std for a linear ramp ≈ 0.058 (sqrt(3)/n normalisation)
        assert!(
            r.trend_strength > 0.01,
            "trend_strength {} should be > 0.01 for a steady trend",
            r.trend_strength
        );
    }

    #[test]
    fn volatile_market_detected() {
        // Alternating up/down: high vol, zero trend
        let prices: Vec<f64> = (0..100)
            .map(|i| 10.0 + if i % 2 == 0 { 0.01 } else { -0.01 })
            .collect();
        let h = make_history(&prices);
        let r = compute_regime(&h, 60, 60).unwrap();
        assert!(
            r.realized_vol > 0.005,
            "realized_vol {} should be > 0.005",
            r.realized_vol
        );
    }

    #[test]
    fn disabled_when_thresholds_zero() {
        assert!(regime_allows_entry(None, 0.0, 0.0));
        let r = RegimeState {
            realized_vol: 999.0,
            trend_strength: 999.0,
        };
        assert!(regime_allows_entry(Some(r), 0.0, 0.0));
    }

    #[test]
    fn blocks_when_vol_exceeds_threshold() {
        let r = RegimeState {
            realized_vol: 0.005,
            trend_strength: 0.01,
        };
        assert!(!regime_allows_entry(Some(r), 0.003, 0.0));
        assert!(regime_allows_entry(Some(r), 0.01, 0.0));
    }

    #[test]
    fn blocks_when_trend_exceeds_threshold() {
        let r = RegimeState {
            realized_vol: 0.001,
            trend_strength: 0.8,
        };
        assert!(!regime_allows_entry(Some(r), 0.0, 0.5));
        assert!(regime_allows_entry(Some(r), 0.0, 1.0));
    }

    // --- innovation-responsive persistent-regime detector (#494) ---

    /// Drive the detector through `MIN_UPDATES` ticks of zero-mean noise so
    /// the robust scale settles around `scale` and the warm-up gate opens.
    /// Innovations alternate ±`scale` so the scale EWMA converges to it.
    fn warmup(det: &mut RegimeDetector, scale: f64, mut ts: i64) -> i64 {
        for i in 0..(MIN_UPDATES as i64 + 5) {
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            det.update(sign * scale, ts);
            ts += 60;
        }
        ts
    }

    #[test]
    fn warmup_suppresses_signal() {
        let mut det = RegimeDetector::default();
        // A huge innovation before warm-up completes must not activate.
        for i in 0..(MIN_UPDATES as i64 - 1) {
            assert_eq!(det.update(50.0, i * 60), RegimeTransition::None);
            assert!(!det.is_active());
        }
    }

    #[test]
    fn single_outlier_does_not_activate() {
        let mut det = RegimeDetector::default();
        let scale = 1e-3;
        let mut ts = warmup(&mut det, scale, 0);

        // One extreme corrupted bar.
        assert_eq!(
            det.update(1000.0 * scale, ts),
            RegimeTransition::None,
            "a single outlier must not flip the regime state"
        );
        assert!(!det.is_active());
        let after_spike = det.cusum();
        // An isolated outlier only decays the CUSUM; from the post-warm-up zero
        // it cannot rise at all, and in general it never grows.
        assert!(
            after_spike < CUSUM_H_OFF,
            "cusum {} should stay well below h_off {} after one isolated outlier",
            after_spike,
            CUSUM_H_OFF,
        );
        ts += 60;

        // Back to quiet noise: the accumulator stays down, never activating.
        for i in 0..20 {
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            assert_eq!(det.update(sign * scale, ts), RegimeTransition::None);
            assert!(!det.is_active());
            ts += 60;
        }
        assert!(
            det.cusum() <= after_spike,
            "cusum must not have grown across the outlier + quiet window",
        );
    }

    #[test]
    fn sustained_shift_activates_then_recovers() {
        let mut det = RegimeDetector::default();
        let scale = 1e-3;
        let mut ts = warmup(&mut det, scale, 0);

        // Sustained one-sided drift at ~3σ of the residual scale.
        let mut activated_at = None;
        for step in 0..10 {
            let t = det.update(3.0 * scale, ts);
            ts += 60;
            if t == RegimeTransition::Activated {
                activated_at = Some(step);
                break;
            }
        }
        assert!(
            activated_at.is_some(),
            "a sustained 3-sigma shift must activate the detector",
        );
        assert!(det.is_active());
        assert!(det.active_secs(ts) > 0.0);
        assert!(det.peak_cusum() >= CUSUM_H_ON);
        assert!(det.peak_cusum_ts().is_some());

        // Relationship repairs: innovations fall back inside the band and the
        // state clears (hysteresis lets it ride out a couple of ticks first).
        let mut cleared = false;
        for _ in 0..50 {
            let t = det.update(0.0, ts);
            ts += 60;
            if t == RegimeTransition::Cleared {
                cleared = true;
                break;
            }
        }
        assert!(cleared, "the detector must clear once the drift subsides");
        assert!(!det.is_active());
        assert_eq!(det.active_secs(ts), 0.0);
    }

    #[test]
    fn beta_shift_alternating_sign_activates() {
        // Regression for the signed-CUSUM blind spot: a β/relationship shift
        // makes the residual ≈ (β_true − β)·dx, and since dx alternates sign
        // tick-to-tick the *signed* innovation alternates ±3σ even though its
        // magnitude is persistently elevated. The old two-sided signed CUSUM
        // decayed on every sign flip and never activated; the magnitude CUSUM
        // must fire.
        let mut det = RegimeDetector::default();
        let scale = 1e-3;
        let mut ts = warmup(&mut det, scale, 0);

        let mut activated = false;
        for step in 0..40 {
            let sign = if step % 2 == 0 { 1.0 } else { -1.0 };
            if det.update(sign * 3.0 * scale, ts) == RegimeTransition::Activated {
                activated = true;
                break;
            }
            ts += 60;
        }
        assert!(
            activated,
            "an alternating-sign 3-sigma magnitude shift must activate the detector \
             (cusum={})",
            det.cusum(),
        );
        assert!(det.is_active());
    }

    #[test]
    fn single_corrupted_price_bar_roundtrip_does_not_activate() {
        // Regression for the difference-input corruption shape: the detector
        // sees Δspread, so ONE corrupted price observation produces a *pair* of
        // consecutive outlier innovations — a large spike on entry and a
        // large opposite-sign reversal when the price returns. Build the CUSUM
        // to a sub-threshold "danger zone" (high enough that a naive design
        // crediting the second outlier as persistence would tip it over h_on),
        // then fire the +1000σ / −1000σ round-trip. Neither bar may activate.
        let mut det = RegimeDetector::default();
        let scale = 1e-3;
        let mut ts = warmup(&mut det, scale, 0);

        // A naive design that accumulated the (clamped) second consecutive
        // outlier would add `INNOVATION_CLIP − MAG_REF − CUSUM_K`; any pre-load
        // above that margin below h_on is in the danger zone.
        let danger = CUSUM_H_ON - (INNOVATION_CLIP - MAG_REF - CUSUM_K);
        // Sustained ~2.5σ (non-outlier) pre-loads the CUSUM into that zone
        // without activating; stop as soon as we are safely inside it.
        let mut pre = 0.0;
        for _ in 0..40 {
            let t = det.update(2.5 * scale, ts);
            ts += 60;
            assert_ne!(
                t,
                RegimeTransition::Activated,
                "the 2.5-sigma pre-load must stay sub-threshold"
            );
            assert!(!det.is_active());
            pre = det.cusum();
            if pre > danger {
                break;
            }
        }
        assert!(
            pre > danger,
            "pre-load {pre} should reach the danger zone (> {danger}) a naive design could tip over h_on",
        );
        assert!(
            pre < CUSUM_H_ON,
            "pre-load {pre} must still be sub-threshold"
        );

        // The corrupted bar's spike, then its reversal on the next bar. A
        // single corrupted observation must not create a persistent regime
        // state: neither outlier activates, and the statistic only decays.
        for innov in [1000.0 * scale, -1000.0 * scale] {
            assert_eq!(
                det.update(innov, ts),
                RegimeTransition::None,
                "a corrupted-bar outlier ({innov}) must not activate from a high pre-load",
            );
            assert!(!det.is_active());
            ts += 60;
        }
        assert!(
            det.cusum() < pre,
            "the spike+reversal pair must decay (not grow) the CUSUM: {} !< {}",
            det.cusum(),
            pre,
        );
    }

    #[test]
    fn interval_peak_tracks_max_and_resets_to_current() {
        let mut det = RegimeDetector::default();
        let scale = 1e-3;
        let mut ts = warmup(&mut det, scale, 0);

        // Build the CUSUM up with sustained elevated bars, then let it decay
        // with quiet bars. The interval peak must remember the high-water
        // mark even after the decay.
        for _ in 0..6 {
            det.update(3.0 * scale, ts);
            ts += 60;
        }
        let high = det.cusum();
        assert!(high > 0.0, "elevated bars must accumulate (cusum={high})");
        for i in 0..4 {
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            det.update(sign * scale, ts);
            ts += 60;
        }
        let decayed = det.cusum();
        assert!(decayed < high, "quiet bars must decay the cusum");

        let peak = det.take_interval_peak();
        assert!(
            (peak - high).abs() < 1e-12,
            "interval peak {peak} must equal the high-water mark {high}",
        );
        // After the take, the tracker restarts from the *current* statistic,
        // not zero: a second take with no further updates returns the
        // decayed level.
        let peak2 = det.take_interval_peak();
        assert!(
            (peak2 - decayed).abs() < 1e-12,
            "post-reset peak {peak2} must equal the current statistic {decayed}",
        );
    }

    #[test]
    fn extreme_persistent_shift_still_activates_after_scale_adapts() {
        // The flip side of decay-only outliers: a genuine *persistent* large
        // shift must still be detected. Its first few bars read as outliers
        // (decay-only) while the winsorised scale catches up, after which the
        // residual drops into the accumulating band and the CUSUM climbs.
        let mut det = RegimeDetector::default();
        let scale = 1e-3;
        let mut ts = warmup(&mut det, scale, 0);

        let mut activated = false;
        for _ in 0..120 {
            if det.update(6.0 * scale, ts) == RegimeTransition::Activated {
                activated = true;
                break;
            }
            ts += 60;
        }
        assert!(
            activated,
            "a sustained 6-sigma shift must still activate once the scale adapts (cusum={})",
            det.cusum(),
        );
        assert!(det.is_active());
    }
}
