//! Mid-hold re-hedge decision logic (bot-strategy#463).
//!
//! Phase 1 (this module): pure decision function `should_rehedge` —
//! returns `Some(RehedgeDecision)` when the per-pair β has drifted
//! beyond the configured threshold relative to the position's entry β,
//! the cool-down has elapsed, and the notional swing is large enough
//! to be worth a fee.
//!
//! Phase 1 is observability-only: callers log `[REHEDGE_NEEDED]` and
//! increment the prom counter, but do not place orders. Phase 2 will
//! consume `RehedgeDecision` to issue a single-side rebalance and set
//! `Position::last_rehedge_ts` on fill.
//!
//! All three thresholds are per-strategy overridable (see
//! `PairParams::rehedge_*` / `StrategyYaml::rehedge_*`), so an
//! operator can tune them without a recompile.

use dex_connector::OrderSide;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use super::config::PairParams;
use super::state::{Position, PositionDirection};

/// Outcome of `should_rehedge`. `notional_swing_usd` is what the
/// re-hedge leg would trade — useful for the log line + future
/// Phase 2 size sanity-checks.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RehedgeDecision {
    pub entry_beta: f64,
    pub current_beta: f64,
    pub drift_pct: f64,
    pub notional_swing_usd: f64,
}

/// Decide whether a re-hedge should fire RIGHT NOW for the given
/// position. Returns `None` when:
/// - The feature is disabled (`rehedge_drift_threshold_pct <= 0`).
/// - The position has no recorded `entry_beta` (legacy / pre-#463 fill).
/// - `entry_beta` is too small for a stable percentage calculation.
/// - Drift is below the threshold.
/// - We are inside the cool-down window from a prior re-hedge.
/// - The required leg-B notional swing is below the min-qty floor.
pub(super) fn should_rehedge(
    pp: &PairParams,
    position: &Position,
    current_beta: f64,
    current_z: Option<f64>,
    force_close_secs: u64,
    now_ts: i64,
) -> Option<RehedgeDecision> {
    let threshold = pp.rehedge_drift_threshold_pct;
    if threshold <= 0.0 {
        return None;
    }
    let entry_beta = position.entry_beta?;
    if entry_beta.abs() < 1e-6 {
        // Avoid division blow-up. Position with near-zero β is itself
        // pathological and Phase 2 should reject the re-hedge anyway.
        return None;
    }
    let drift = (current_beta - entry_beta).abs() / entry_beta.abs();
    if drift < threshold {
        return None;
    }
    // bot-strategy#465: optional gate — only re-hedge when the spread
    // has NOT reverted toward the mean since entry. If `|z_now|` has
    // already dropped below `|z_entry| * rehedge_z_no_revert_factor`,
    // the position is mid-reversion and the β-drift will likely correct
    // itself; locking in a rebalance now caps the win. Skip the
    // re-hedge.
    if pp.rehedge_require_no_revert {
        if let (Some(z_now), Some(z_entry)) = (current_z, position.entry_z) {
            let z_entry_abs = z_entry.abs();
            let z_now_abs = z_now.abs();
            if z_now_abs < z_entry_abs * pp.rehedge_z_no_revert_factor {
                return None;
            }
        }
    }
    // bot-strategy#465 Option B — β-velocity gate. Compute the
    // instantaneous drift rate from `prev_beta_for_velocity` and
    // project to remaining hold time. Skip when projected total
    // drift is below the configured minimum (slow drifts tend to
    // self-correct before they hurt).
    if pp.rehedge_velocity_projected_drift_min > 0.0 {
        if let Some((prev_beta, prev_ts)) = position.prev_beta_for_velocity {
            let dt = (now_ts - prev_ts) as f64;
            if dt > 0.0 {
                let beta_velocity = (current_beta - prev_beta) / dt;
                let elapsed = (now_ts - position.entered_ts).max(0) as i64;
                let remaining =
                    (force_close_secs as i64).saturating_sub(elapsed).max(0) as f64;
                let projected_total_drift =
                    drift + (beta_velocity.abs() * remaining) / entry_beta.abs();
                if projected_total_drift < pp.rehedge_velocity_projected_drift_min {
                    return None;
                }
            }
        }
    }
    // Cool-down: only consult `last_rehedge_ts`; if `None` we have
    // never re-hedged this position, so the cool-down is trivially OK.
    if let Some(prev_ts) = position.last_rehedge_ts {
        let elapsed = now_ts.saturating_sub(prev_ts);
        if elapsed < pp.rehedge_cooldown_secs as i64 {
            return None;
        }
    }
    // Notional swing on leg B: the qty delta is roughly
    // `|β_new − β_old| * leg_a_qty * price_b`. Use entry_size_a /
    // entry_price_b as a robust proxy — Position records both, and
    // they reflect the actually-filled hedge.
    let notional_swing_usd = match (
        position.entry_size_a,
        position.entry_price_b,
    ) {
        (Some(sa), Some(pb)) => {
            let sa_f = sa.to_f64().unwrap_or(0.0);
            let pb_f = pb.to_f64().unwrap_or(0.0);
            (current_beta - entry_beta).abs() * sa_f * pb_f
        }
        _ => 0.0,
    };
    if notional_swing_usd < pp.rehedge_min_qty_notional_usd {
        return None;
    }
    Some(RehedgeDecision {
        entry_beta,
        current_beta,
        drift_pct: drift,
        notional_swing_usd,
    })
}

/// Planned re-hedge order: which leg (always quote / B for now), which
/// side, and which absolute qty in leg-B units. `expected_new_entry_size_b`
/// is what `Position::entry_size_b` will become after the fill (cumulative,
/// not the delta) — reconcile uses this directly to avoid recomputing
/// the sign convention.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RehedgeOrderPlan {
    pub side: OrderSide,
    pub qty: Decimal,
    pub expected_new_entry_size_b: Decimal,
}

/// Build the one-sided order plan for a triggered re-hedge.
///
/// The base (leg A) size is held fixed; only the quote (leg B) hedge is
/// resized to match the new β. Sign convention:
///
///   target_size_b = |β_now| * entry_size_a * price_a / price_b
///   delta_b       = target_size_b - current_entry_size_b
///
/// For a LongSpread position (long base + short quote), `delta_b > 0`
/// means we need to grow the short → trade more quote on the **Short**
/// side. For ShortSpread (short base + long quote), `delta_b > 0` means
/// grow the long → **Long** side. The signs flip for `delta_b < 0`.
///
/// Returns `None` when the position is missing the entry sizes / prices
/// needed for the calculation (legacy positions or partial state).
pub(super) fn plan_rehedge_order(
    position: &Position,
    current_beta: f64,
) -> Option<RehedgeOrderPlan> {
    let size_a = position.entry_size_a?;
    let size_b_current = position.entry_size_b?;
    let price_a = position.entry_price_a?;
    let price_b = position.entry_price_b?;
    if price_b.is_zero() {
        return None;
    }

    // target_size_b = |β_now| * size_a * (price_a / price_b)
    let beta_dec = Decimal::from_f64(current_beta.abs())?;
    let target_size_b = (size_a * beta_dec * price_a) / price_b;
    let delta = target_size_b - size_b_current;
    if delta.is_zero() {
        return None;
    }
    let qty = delta.abs();
    let grow = delta > Decimal::ZERO;
    let side = match (position.direction, grow) {
        (PositionDirection::LongSpread, true) => OrderSide::Short, // grow short
        (PositionDirection::LongSpread, false) => OrderSide::Long, // shrink short → buy
        (PositionDirection::ShortSpread, true) => OrderSide::Long, // grow long
        (PositionDirection::ShortSpread, false) => OrderSide::Short, // shrink long → sell
    };
    Some(RehedgeOrderPlan {
        side,
        qty,
        expected_new_entry_size_b: target_size_b,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pairtrade::state::PositionDirection;
    use std::str::FromStr;
    use std::time::Instant;

    fn pp(threshold: f64, cooldown: u64, min_usd: f64) -> PairParams {
        PairParams {
            rehedge_drift_threshold_pct: threshold,
            rehedge_cooldown_secs: cooldown,
            rehedge_min_qty_notional_usd: min_usd,
            ..PairParams::default()
        }
    }

    fn pos(entry_beta: Option<f64>, last_rehedge_ts: Option<i64>) -> Position {
        Position {
            direction: PositionDirection::LongSpread,
            entered_at: Instant::now(),
            entered_ts: 1_000_000,
            entry_price_a: Some(Decimal::from_str("80000.0").unwrap()),
            entry_price_b: Some(Decimal::from_str("2000.0").unwrap()),
            entry_size_a: Some(Decimal::from_str("0.025").unwrap()),
            entry_size_b: Some(Decimal::from_str("1.0").unwrap()),
            entry_z: Some(-2.0),
            entry_beta,
            last_rehedge_ts,
            rehedge_realized_pnl: None,
            prev_beta_for_velocity: None,
        }
    }

    #[test]
    fn disabled_when_threshold_is_zero() {
        let p = pp(0.0, 1800, 50.0);
        let r = should_rehedge(&p, &pos(Some(1.0), None), 1.5, None, 7200u64, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn skipped_when_entry_beta_unknown() {
        let p = pp(0.15, 1800, 50.0);
        let r = should_rehedge(&p, &pos(None, None), 1.5, None, 7200u64, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn skipped_when_drift_under_threshold() {
        let p = pp(0.20, 1800, 50.0);
        // drift = |1.10 - 1.00| / 1.00 = 0.10 < 0.20
        let r = should_rehedge(&p, &pos(Some(1.0), None), 1.10, None, 7200u64, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn fires_when_drift_meets_threshold_and_swing_above_floor() {
        let p = pp(0.15, 1800, 5.0);
        // drift = |0.70 - 1.00| / 1.00 = 0.30 >= 0.15
        // notional swing = 0.30 * 0.025 * 2000 = $15.00 >= $5
        let r = should_rehedge(&p, &pos(Some(1.0), None), 0.70, None, 7200u64, 2_000_000);
        let d = r.expect("should fire");
        assert!((d.drift_pct - 0.30).abs() < 1e-9);
        assert!((d.notional_swing_usd - 15.0).abs() < 1e-9);
    }

    #[test]
    fn skipped_when_inside_cooldown() {
        let p = pp(0.15, 1800, 5.0);
        // last rehedge 900 s ago < 1800 s cool-down
        let r = should_rehedge(&p, &pos(Some(1.0), Some(2_000_000 - 900)), 0.70, None, 7200u64, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn fires_after_cooldown_elapses() {
        let p = pp(0.15, 1800, 5.0);
        // 1801 s ago > 1800 s
        let r = should_rehedge(&p, &pos(Some(1.0), Some(2_000_000 - 1801)), 0.70, None, 7200u64, 2_000_000);
        assert!(r.is_some());
    }

    #[test]
    fn skipped_when_swing_below_min_notional() {
        let p = pp(0.15, 1800, 100.0);
        // notional swing = 0.30 * 0.025 * 2000 = $15.00 < $100 floor
        let r = should_rehedge(&p, &pos(Some(1.0), None), 0.70, None, 7200u64, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn handles_negative_drift_direction() {
        let p = pp(0.20, 1800, 5.0);
        // β grew 60% (drift = +0.60), still triggers
        let r = should_rehedge(&p, &pos(Some(1.0), None), 1.60, None, 7200u64, 2_000_000);
        assert!(r.is_some());
        // β shrunk 60% (drift = +0.60 still — absolute value),
        let r = should_rehedge(&p, &pos(Some(1.0), None), 0.40, None, 7200u64, 2_000_000);
        assert!(r.is_some());
    }

    fn pos_full() -> Position {
        // entry_beta=1.0, size_a=0.025, size_b=1.0, price_a=80000, price_b=2000
        // target_b at β=1.0 is 0.025 * 1.0 * 80000 / 2000 = 1.0 ✓
        pos(Some(1.0), None)
    }

    #[test]
    fn plan_long_spread_grow_short() {
        // β grew from 1.0 to 1.2; LongSpread holds short quote, so we
        // need to grow the short → Short side.
        // target_b = 0.025 * 1.2 * 80000 / 2000 = 1.2; delta = +0.2
        let plan = plan_rehedge_order(&pos_full(), 1.2).expect("should plan");
        assert_eq!(plan.side, OrderSide::Short);
        assert_eq!(plan.qty, Decimal::from_str("0.2").unwrap());
        assert_eq!(plan.expected_new_entry_size_b, Decimal::from_str("1.2").unwrap());
    }

    #[test]
    fn plan_long_spread_shrink_short() {
        // β shrank to 0.8; LongSpread, shrink the short → Long (buy back).
        // target_b = 0.025 * 0.8 * 80000 / 2000 = 0.8; delta = -0.2 → qty 0.2
        let plan = plan_rehedge_order(&pos_full(), 0.8).expect("should plan");
        assert_eq!(plan.side, OrderSide::Long);
        assert_eq!(plan.qty, Decimal::from_str("0.2").unwrap());
        assert_eq!(plan.expected_new_entry_size_b, Decimal::from_str("0.8").unwrap());
    }

    fn pos_short_spread() -> Position {
        let mut p = pos_full();
        p.direction = PositionDirection::ShortSpread;
        p
    }

    #[test]
    fn plan_short_spread_grow_long() {
        // β grew to 1.2; ShortSpread holds long quote, grow the long → Long.
        let plan = plan_rehedge_order(&pos_short_spread(), 1.2).expect("should plan");
        assert_eq!(plan.side, OrderSide::Long);
        assert_eq!(plan.qty, Decimal::from_str("0.2").unwrap());
    }

    #[test]
    fn plan_short_spread_shrink_long() {
        // β shrank to 0.8; shrink the long → Short.
        let plan = plan_rehedge_order(&pos_short_spread(), 0.8).expect("should plan");
        assert_eq!(plan.side, OrderSide::Short);
        assert_eq!(plan.qty, Decimal::from_str("0.2").unwrap());
    }

    #[test]
    fn plan_skipped_when_delta_zero() {
        // current_beta exactly matches the entry hedge → no change needed.
        let plan = plan_rehedge_order(&pos_full(), 1.0);
        assert_eq!(plan, None);
    }
}
