//! Entry-decision helpers extracted from the monolithic pairtrade module.
//! Pure functions over config, params, and per-pair state.

use std::collections::VecDeque;

use super::config::{PairParams, PairTradeConfig};
use super::state::{PairSharedState, PairState, PositionDirection};
use super::stats::spread_slope_sigma;
use super::util::tail_std;

fn median_of(values: &VecDeque<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut buf: Vec<f64> = values.iter().copied().collect();
    buf.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = buf.len() / 2;
    if buf.len() % 2 == 0 {
        Some((buf[mid - 1] + buf[mid]) / 2.0)
    } else {
        Some(buf[mid])
    }
}

/// Returns `true` when the std-collapse guard should block a new entry.
/// The z-score denominator has fallen far below its own recent median, so
/// the current |z| is no longer a meaningful mean-reversion signal
/// (bot-strategy#62).
pub(super) fn std_collapsed(
    std: f64,
    std_history: &VecDeque<f64>,
    window_bars: usize,
    min_ratio: f64,
) -> bool {
    if window_bars == 0 || min_ratio <= 0.0 {
        return false;
    }
    let min_samples = (window_bars / 2).max(2);
    if std_history.len() < min_samples {
        return false;
    }
    let Some(median) = median_of(std_history) else {
        return false;
    };
    if median <= 1e-9 {
        return false;
    }
    std / median < min_ratio
}

/// Lower bound for any dynamic entry-z scaling factor (vol or funding).
/// Prevents the threshold from collapsing on noisy single-bar inputs.
const ENTRY_Z_SCALE_MIN: f64 = 0.5;
/// Upper bound for any dynamic entry-z scaling factor.
const ENTRY_Z_SCALE_MAX: f64 = 2.0;
/// Discount applied to the entry-z threshold when net funding is positive,
/// nudging the strategy to take small carry-favorable trades it would
/// otherwise skip. The continuous `funding_entry_z_scale` filter (PairParams)
/// is layered on top.
const FUNDING_CARRY_ENTRY_DISCOUNT: f64 = 0.9;

pub(super) fn entry_z_for_pair(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    shared: &PairSharedState,
    vol_median: f64,
) -> f64 {
    let entry_vol_len =
        ((pp.entry_vol_lookback_hours * 3600) / cfg.trading_period_secs).max(1) as usize;
    let vol_pair = tail_std(&shared.spread_history, entry_vol_len).unwrap_or(1.0);
    let alpha = (vol_pair / vol_median).clamp(ENTRY_Z_SCALE_MIN, ENTRY_Z_SCALE_MAX);
    let z = pp.entry_z_base * alpha;
    z.clamp(pp.entry_z_min, pp.entry_z_max)
}

/// Asymmetric entry-threshold scaling for the short side (bot-strategy#358).
///
/// The BTC/ETH log-spread is left-skewed, so a direction-symmetric |z| gate
/// samples short entries from the noisier middle of the distribution while
/// long entries come from the deeper tail. Multiplying the short-side
/// threshold pushes both sides to a comparable tail percentile.
///
/// Semantics:
/// - `entry_z_short_multiplier <= 0.0` → disabled (no scaling). Matches the
///   auto-derived `f64` default of 0.0 used in test constructors with
///   `..PairParams::default()` — without this gate, those constructors would
///   silently zero out the short-side threshold and let any |z| through.
/// - `entry_z_short_multiplier == 1.0` → no-op (production default).
/// - `entry_z_short_multiplier > 1.0` → harder threshold for short entries.
/// - Very large values (e.g. 9999.0) effectively disable shorts entirely.
pub(super) fn apply_short_multiplier(
    threshold: f64,
    pp: &PairParams,
    direction: PositionDirection,
) -> f64 {
    if direction == PositionDirection::ShortSpread && pp.entry_z_short_multiplier > 0.0 {
        threshold * pp.entry_z_short_multiplier
    } else {
        threshold
    }
}

/// Per-direction post-stop_loss_z cool-down (bot-strategy#316). Returns
/// `false` (blocks entry) when `stop_loss_cooldown_secs` is set, the most
/// recent exit was a `stop_loss_z`, the proposed direction matches that
/// stop, and the elapsed time is still inside the window. Reverse-direction
/// entries are NOT blocked — they're reversal trades on a different signal.
///
/// 2026-05-04 Frankfurt DD: 100% of post-stop same-direction re-entries
/// within 30 min were losers (n=2/2 live, plus a long-gap n=1 that was
/// also a loser). A follow-up entry into a still-widening spread piles
/// loss on loss because beta is actively breaking down.
pub(super) fn post_stop_cooldown_allows(
    pp: &PairParams,
    state: &PairState,
    now_ts: i64,
    proposed_direction: PositionDirection,
) -> bool {
    if pp.stop_loss_cooldown_secs == 0 {
        return true;
    }
    let Some((stop_dir, stop_ts)) = state.last_stop_loss_at else {
        return true;
    };
    if stop_dir != proposed_direction {
        return true;
    }
    let elapsed = now_ts.saturating_sub(stop_ts);
    if elapsed >= pp.stop_loss_cooldown_secs as i64 {
        return true;
    }
    log::info!(
        "[STOP_COOLDOWN] {:?} blocked, elapsed={}s of {}s",
        proposed_direction,
        elapsed,
        pp.stop_loss_cooldown_secs,
    );
    false
}

/// Result of `should_enter`. `Ok(())` means take the entry. `Err(reason)` is
/// the static identifier of the gate that fired — kept in sync with
/// `prom::KNOWN_ENTRY_REJECT_REASONS`.
pub(super) fn should_enter(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    shared: &PairSharedState,
    z: f64,
    std: f64,
    net_funding: f64,
    now_ts: i64,
    proposed_direction: PositionDirection,
) -> Result<(), &'static str> {
    if let Some(last_exit_ts) = state.last_exit_ts {
        if now_ts.saturating_sub(last_exit_ts) < pp.cooldown_secs as i64 {
            return Err("cooldown");
        }
    }

    if !post_stop_cooldown_allows(pp, state, now_ts, proposed_direction) {
        return Err("post_stop_cooldown");
    }

    // --- Phase 2 filter: spread momentum block ---
    // Block entry when spread is moving fast (likely trending, not mean-reverting).
    // Disabled when entry_velocity_block_sigma_per_min == 0.0.
    if pp.entry_velocity_block_sigma_per_min > 0.0
        && shared.last_velocity_sigma_per_min.abs() >= pp.entry_velocity_block_sigma_per_min
    {
        return Err("velocity");
    }

    // --- Std collapse guard (bot-strategy#62) ---
    // z = (latest - mean) / std; when std collapses relative to its own recent
    // history the z-score stops being a meaningful mean-reversion signal.
    // In observe_only mode the guard logs but lets the entry through — lets
    // operators measure trigger frequency on live data without disturbing
    // the #41 A/B/C test window.
    if std_collapsed(
        std,
        &shared.std_history,
        pp.std_collapse_window_bars,
        pp.std_collapse_min_ratio,
    ) {
        let median = median_of(&shared.std_history).unwrap_or(0.0);
        let ratio = if median > 1e-9 { std / median } else { 0.0 };
        if pp.std_collapse_observe_only {
            log::warn!(
                "[STD_COLLAPSE_OBSERVE] z={:.2} std={:.6} median={:.6} ratio={:.4} threshold={:.4} (observe-only, entry allowed)",
                z,
                std,
                median,
                ratio,
                pp.std_collapse_min_ratio,
            );
        } else {
            log::warn!(
                "[STD_COLLAPSE_BLOCK] z={:.2} std={:.6} median={:.6} ratio={:.4} threshold={:.4}",
                z,
                std,
                median,
                ratio,
                pp.std_collapse_min_ratio,
            );
            return Err("std_collapse");
        }
    }

    let mut entry_threshold = if net_funding > 0.0 {
        // prefer positive carry by easing the required entry slightly
        state.z_entry * FUNDING_CARRY_ENTRY_DISCOUNT
    } else {
        state.z_entry
    };

    // --- Phase 2 filter: funding rate continuous scaling ---
    if pp.funding_entry_z_scale > 0.0 {
        let adjustment = 1.0 - pp.funding_entry_z_scale * net_funding;
        entry_threshold *= adjustment.clamp(ENTRY_Z_SCALE_MIN, ENTRY_Z_SCALE_MAX);
    }

    // --- Phase 2 filter: beta gap dynamic adjustment ---
    if pp.beta_gap_entry_z_scale > 0.0 {
        entry_threshold *= 1.0 + pp.beta_gap_entry_z_scale * shared.beta_gap;
    }

    // --- Asymmetric entry threshold (bot-strategy#358) ---
    entry_threshold = apply_short_multiplier(entry_threshold, pp, proposed_direction);

    // Avoid entering when the current z already triggers stop-loss exit.
    if z.abs() >= pp.stop_loss_z {
        return Err("stop_loss_z");
    }
    // Spread trend filter: block entry if spread is trending
    if let Some(slope_sigma) = spread_slope_sigma(&shared.spread_history, cfg.metrics_window) {
        if slope_sigma > pp.spread_trend_max_slope_sigma {
            return Err("spread_trend");
        }
    }
    // Beta stability filter: block entry if beta_s and beta_l diverge
    if shared.beta_gap > pp.beta_divergence_max {
        return Err("beta_divergence");
    }
    // Beta minimum filter: block entry if beta is too low (hedge leg too small)
    if pp.beta_min > 0.0 && shared.beta < pp.beta_min {
        return Err("beta_min");
    }
    // Account for estimated cost (fees + slippage) in sigma units
    let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0; // two legs
    let cost_ratio = total_cost_bps / 10_000.0;
    let cost_in_sigma = if std <= 1e-9 { 0.0 } else { cost_ratio / std };
    if z.abs() < entry_threshold {
        return Err("z_below_threshold");
    }

    // Multi-timeframe z-score confluence filter.
    // All configured windows must show z in the same direction and above mtf_z_min.
    // Disabled when mtf_windows is empty or mtf_z_min == 0.0.
    if !pp.mtf_windows.is_empty() && pp.mtf_z_min > 0.0 {
        let primary_sign = z.signum();
        for &w in &pp.mtf_windows {
            if let Some(z_w) = shared.z_score_for_window(w) {
                if z_w.signum() != primary_sign || z_w.abs() < pp.mtf_z_min {
                    return Err("mtf");
                }
            }
            // Insufficient data for this window → skip (permissive)
        }
    }

    if z.abs() < entry_threshold + cost_in_sigma {
        return Err("z_below_threshold");
    }
    if net_funding < cfg.net_funding_min_per_hour {
        return Err("net_funding_min");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_history(values: &[f64]) -> VecDeque<f64> {
        values.iter().copied().collect()
    }

    #[test]
    fn std_collapsed_disabled_when_window_zero() {
        let h = make_history(&[1.0, 1.0, 1.0, 1.0]);
        assert!(!std_collapsed(0.001, &h, 0, 0.2));
    }

    #[test]
    fn std_collapsed_disabled_when_ratio_zero() {
        let h = make_history(&[1.0, 1.0, 1.0, 1.0]);
        assert!(!std_collapsed(0.001, &h, 30, 0.0));
    }

    #[test]
    fn std_collapsed_permissive_before_warmup() {
        // window=30 → min_samples=15; three samples is well under that
        let h = make_history(&[1.0, 1.0, 1.0]);
        assert!(!std_collapsed(0.001, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_blocks_when_current_far_below_median() {
        // Replicates bot-strategy#62: median ≈ 1.0, current = 0.0016 → ratio 0.0016
        let samples: Vec<f64> = vec![1.0; 30];
        let h = make_history(&samples);
        assert!(std_collapsed(0.0016, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_allows_when_current_near_median() {
        let samples: Vec<f64> = vec![1.0; 30];
        let h = make_history(&samples);
        assert!(!std_collapsed(0.9, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_boundary_inclusive_allows_equal_ratio() {
        // std / median == min_ratio → not blocked (strict less-than)
        let samples: Vec<f64> = vec![1.0; 30];
        let h = make_history(&samples);
        assert!(!std_collapsed(0.2, &h, 30, 0.2));
    }

    #[test]
    fn std_collapsed_handles_zero_median() {
        let samples: Vec<f64> = vec![0.0; 30];
        let h = make_history(&samples);
        assert!(!std_collapsed(0.001, &h, 30, 0.2));
    }

    // ---- bot-strategy#316: post-stop_loss_z cool-down ----

    fn cooldown_state(stop: Option<(PositionDirection, i64)>) -> PairState {
        let mut s = PairState::new(2.0);
        s.last_stop_loss_at = stop;
        s
    }

    fn cooldown_params(stop_loss_cooldown_secs: u64) -> PairParams {
        let mut p = PairParams::default();
        p.stop_loss_cooldown_secs = stop_loss_cooldown_secs;
        p
    }

    #[test]
    fn stop_cooldown_blocks_same_direction_within_window() {
        let pp = cooldown_params(1800);
        let s = cooldown_state(Some((PositionDirection::LongSpread, 1000)));
        // Re-attempt at t=1300 (5 min after the stop, well inside 1800s).
        assert!(!post_stop_cooldown_allows(
            &pp,
            &s,
            1300,
            PositionDirection::LongSpread
        ));
    }

    #[test]
    fn stop_cooldown_allows_opposite_direction() {
        let pp = cooldown_params(1800);
        let s = cooldown_state(Some((PositionDirection::LongSpread, 1000)));
        // ShortSpread reversal is on a different signal — must not be blocked.
        assert!(post_stop_cooldown_allows(
            &pp,
            &s,
            1300,
            PositionDirection::ShortSpread
        ));
    }

    #[test]
    fn stop_cooldown_allows_after_window_elapses() {
        let pp = cooldown_params(1800);
        let s = cooldown_state(Some((PositionDirection::LongSpread, 1000)));
        // 1s past the 1800s window.
        assert!(post_stop_cooldown_allows(
            &pp,
            &s,
            1000 + 1801,
            PositionDirection::LongSpread
        ));
    }

    #[test]
    fn stop_cooldown_boundary_inclusive_at_window_end() {
        // elapsed == cooldown → allowed (>= comparison in the helper).
        let pp = cooldown_params(1800);
        let s = cooldown_state(Some((PositionDirection::LongSpread, 1000)));
        assert!(post_stop_cooldown_allows(
            &pp,
            &s,
            1000 + 1800,
            PositionDirection::LongSpread
        ));
    }

    #[test]
    fn stop_cooldown_disabled_when_zero_secs() {
        let pp = cooldown_params(0); // legacy / disabled
        let s = cooldown_state(Some((PositionDirection::LongSpread, 1000)));
        assert!(post_stop_cooldown_allows(
            &pp,
            &s,
            1300,
            PositionDirection::LongSpread
        ));
    }

    #[test]
    fn stop_cooldown_no_prior_stop_allows() {
        let pp = cooldown_params(1800);
        let s = cooldown_state(None);
        assert!(post_stop_cooldown_allows(
            &pp,
            &s,
            1_000_000,
            PositionDirection::LongSpread
        ));
    }

    // ---- bot-strategy#358: asymmetric entry_z (short multiplier) ----

    fn short_mult_params(entry_z_short_multiplier: f64) -> PairParams {
        let mut p = PairParams::default();
        p.entry_z_short_multiplier = entry_z_short_multiplier;
        p
    }

    #[test]
    fn short_multiplier_no_op_at_one() {
        // 1.0 = production default = symmetric.
        let pp = short_mult_params(1.0);
        assert_eq!(
            apply_short_multiplier(2.0, &pp, PositionDirection::ShortSpread),
            2.0,
        );
        assert_eq!(
            apply_short_multiplier(2.0, &pp, PositionDirection::LongSpread),
            2.0,
        );
    }

    #[test]
    fn short_multiplier_scales_short_only() {
        // 2.0 → short threshold doubles, long threshold untouched.
        let pp = short_mult_params(2.0);
        assert_eq!(
            apply_short_multiplier(1.5, &pp, PositionDirection::ShortSpread),
            3.0,
        );
        assert_eq!(
            apply_short_multiplier(1.5, &pp, PositionDirection::LongSpread),
            1.5,
        );
    }

    #[test]
    fn short_multiplier_disabled_when_nonpositive() {
        // 0.0 (auto-derived `f64` default) and any negative value are treated as
        // disabled rather than zeroing the threshold — guards `..PairParams::default()`
        // test constructors from accidentally passing every short |z|.
        for sentinel in [0.0_f64, -1.0_f64] {
            let pp = short_mult_params(sentinel);
            assert_eq!(
                apply_short_multiplier(1.5, &pp, PositionDirection::ShortSpread),
                1.5,
                "multiplier {} should be treated as disabled",
                sentinel
            );
        }
    }

    #[test]
    fn short_multiplier_disables_shorts_at_huge_value() {
        // Variant C in the issue's grid: entry_z_short_multiplier=9999.0
        // produces a threshold no realistic |z| can clear → shorts disabled.
        let pp = short_mult_params(9999.0);
        let threshold = apply_short_multiplier(1.5, &pp, PositionDirection::ShortSpread);
        assert!(
            threshold > 1000.0,
            "expected huge threshold, got {}",
            threshold
        );
    }

    #[test]
    fn short_multiplier_emulates_should_enter_decision() {
        // Issue acceptance: with entry_z_short_multiplier=2.0 and base
        // threshold 1.5, |z|=2.0 enters long but NOT short; |z|=3.5 enters
        // both. We assert the threshold-vs-|z| decision the same way
        // should_enter does (line: `z.abs() < entry_threshold` early
        // return).
        let pp = short_mult_params(2.0);
        let base = 1.5;

        let long_thr = apply_short_multiplier(base, &pp, PositionDirection::LongSpread);
        let short_thr = apply_short_multiplier(base, &pp, PositionDirection::ShortSpread);

        // |z| = 2.0
        assert!(2.0_f64 >= long_thr, "long: should enter at |z|=2.0");
        assert!(2.0_f64 < short_thr, "short: should NOT enter at |z|=2.0");
        // |z| = 3.5
        assert!(3.5_f64 >= long_thr, "long: should enter at |z|=3.5");
        assert!(3.5_f64 >= short_thr, "short: should enter at |z|=3.5");
    }

    #[test]
    fn median_of_odd_and_even() {
        let odd = make_history(&[3.0, 1.0, 2.0]);
        assert_eq!(median_of(&odd), Some(2.0));
        let even = make_history(&[1.0, 2.0, 3.0, 4.0]);
        assert_eq!(median_of(&even), Some(2.5));
        assert_eq!(median_of(&VecDeque::<f64>::new()), None);
    }
}
