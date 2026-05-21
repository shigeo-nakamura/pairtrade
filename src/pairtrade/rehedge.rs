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

use rust_decimal::prelude::ToPrimitive;

use super::config::PairParams;
use super::state::Position;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pairtrade::state::PositionDirection;
    use rust_decimal::Decimal;
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
        }
    }

    #[test]
    fn disabled_when_threshold_is_zero() {
        let p = pp(0.0, 1800, 50.0);
        let r = should_rehedge(&p, &pos(Some(1.0), None), 1.5, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn skipped_when_entry_beta_unknown() {
        let p = pp(0.15, 1800, 50.0);
        let r = should_rehedge(&p, &pos(None, None), 1.5, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn skipped_when_drift_under_threshold() {
        let p = pp(0.20, 1800, 50.0);
        // drift = |1.10 - 1.00| / 1.00 = 0.10 < 0.20
        let r = should_rehedge(&p, &pos(Some(1.0), None), 1.10, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn fires_when_drift_meets_threshold_and_swing_above_floor() {
        let p = pp(0.15, 1800, 5.0);
        // drift = |0.70 - 1.00| / 1.00 = 0.30 >= 0.15
        // notional swing = 0.30 * 0.025 * 2000 = $15.00 >= $5
        let r = should_rehedge(&p, &pos(Some(1.0), None), 0.70, 2_000_000);
        let d = r.expect("should fire");
        assert!((d.drift_pct - 0.30).abs() < 1e-9);
        assert!((d.notional_swing_usd - 15.0).abs() < 1e-9);
    }

    #[test]
    fn skipped_when_inside_cooldown() {
        let p = pp(0.15, 1800, 5.0);
        // last rehedge 900 s ago < 1800 s cool-down
        let r = should_rehedge(&p, &pos(Some(1.0), Some(2_000_000 - 900)), 0.70, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn fires_after_cooldown_elapses() {
        let p = pp(0.15, 1800, 5.0);
        // 1801 s ago > 1800 s
        let r = should_rehedge(&p, &pos(Some(1.0), Some(2_000_000 - 1801)), 0.70, 2_000_000);
        assert!(r.is_some());
    }

    #[test]
    fn skipped_when_swing_below_min_notional() {
        let p = pp(0.15, 1800, 100.0);
        // notional swing = 0.30 * 0.025 * 2000 = $15.00 < $100 floor
        let r = should_rehedge(&p, &pos(Some(1.0), None), 0.70, 2_000_000);
        assert_eq!(r, None);
    }

    #[test]
    fn handles_negative_drift_direction() {
        let p = pp(0.20, 1800, 5.0);
        // β grew 60% (drift = +0.60), still triggers
        let r = should_rehedge(&p, &pos(Some(1.0), None), 1.60, 2_000_000);
        assert!(r.is_some());
        // β shrunk 60% (drift = +0.60 still — absolute value),
        let r = should_rehedge(&p, &pos(Some(1.0), None), 0.40, 2_000_000);
        assert!(r.is_some());
    }
}
