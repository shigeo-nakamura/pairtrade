//! Position-sizing helpers extracted from the monolithic pairtrade module.

use anyhow::{anyhow, Result};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use super::config::PairTradeConfig;
use super::market::SymbolSnapshot;

/// Apply the resolved per-leg notional cap to the leg-A target notional.
/// The cap itself is computed by the caller as
/// `equity_reference_usd × max_leverage × risk.max_notional_headroom`
/// so the dollar threshold tracks per-instance equity and per-host leverage
/// automatically (bot-strategy#185 Phase 3-4 amendment).
///
/// The intended hedge is `(leg_notional, leg_notional · |beta|)`, so when
/// `|beta| > 1` the leg-B notional is the binding constraint — dividing by
/// `max(1, |beta|)` keeps the hedge ratio intact while ensuring NEITHER leg
/// exceeds the cap. Returns `None` when the cap is disabled (≤ 0) or the
/// leg is already within budget.
pub(super) fn cap_leg_notional(leg_notional: f64, beta: f64, cap: f64) -> Option<f64> {
    if cap <= 0.0 {
        return None;
    }
    let beta_abs = beta.abs().max(1.0);
    let allowed = cap / beta_abs;
    if leg_notional > allowed {
        Some(allowed)
    } else {
        None
    }
}

/// Resolve the multiplicative size factor for the beta-gap notional shrink
/// (bot-strategy#461). Returns 1.0 (no shrink) when the feature is disabled
/// via `scale_param == 0.0`. Otherwise computes
/// `clamp(1 - scale_param * beta_gap, floor, 1.0)`.
pub(super) fn beta_gap_notional_scale(beta_gap: f64, scale_param: f64, floor_param: f64) -> f64 {
    if scale_param <= 0.0 {
        return 1.0;
    }
    let raw = 1.0 - scale_param * beta_gap.max(0.0);
    let floor = floor_param.clamp(0.0, 1.0);
    raw.clamp(floor, 1.0)
}

/// Resolve the beta used for leg-B sizing only (bot-strategy#798). Leg B's
/// notional is `leg_A_notional * sizing_beta`, so a low-but-stable `|beta|`
/// (passes `beta_min` and `beta_divergence_max` cleanly — this is not about
/// estimate instability) still produces a dollar-notional-asymmetric pair:
/// the oversized, less-hedged leg then carries directional exposure to any
/// move common to both legs, regardless of whether the spread itself
/// converges. `floor <= 0.0` disables (returns `|beta|` unchanged, the
/// legacy behavior). This floor is sizing-only — it must not be used for
/// `beta_min` (entry eligibility) or z-score/spread math.
pub(super) fn resolve_sizing_beta(beta: f64, floor: f64) -> f64 {
    beta.abs().max(floor.max(0.0))
}

/// Defensive ceiling on the combined notional scale passed into
/// `hedged_sizes`. Depth sizing (bot-strategy#515) legitimately pushes the
/// scale above 1.0, so the old `clamp(0.0, 1.0)` no longer applies; this
/// cap only guards against a mis-configured `depth_size_max`. The absolute
/// dollar cap (`equity × max_leverage × max_notional_headroom`) is still
/// enforced downstream by `cap_leg_notional`.
const MAX_NOTIONAL_SCALE: f64 = 2.0;

/// Resolve the signal-depth size multiplier (bot-strategy#515). Returns
/// 1.0 when disabled (`slope <= 0.0`). Otherwise computes
/// `clamp(s_min + slope * (|z_entry| - entry_z_base), s_min, s_max)` —
/// capital concentrates on deeper entries while the entry threshold (and
/// therefore trade count) stays unchanged. The multiplier is resolved
/// once at entry and frozen for the lifetime of the position, same
/// principle as `entry_beta` (#463).
pub(super) fn depth_size_mult(
    abs_z: f64,
    entry_z_base: f64,
    slope: f64,
    s_min: f64,
    s_max: f64,
) -> f64 {
    if slope <= 0.0 {
        return 1.0;
    }
    let s_min = s_min.clamp(0.0, MAX_NOTIONAL_SCALE);
    let s_max = s_max.clamp(s_min, MAX_NOTIONAL_SCALE);
    let depth = (abs_z - entry_z_base).max(0.0);
    (s_min + slope * depth).clamp(s_min, s_max)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn hedged_sizes(
    cfg: &PairTradeConfig,
    equity: f64,
    max_leverage: f64,
    beta: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
    notional_scale: f64,
    sizing_beta_floor: f64,
) -> Result<(Decimal, Decimal)> {
    // `equity` is the per-instance fixed `equity_reference_usd` and
    // `max_leverage` the per-instance resolved leverage (bot-strategy#810;
    // both default to a legacy single-instance value equal to the shared
    // top-level scalars), so each variant sizes against its own declared
    // capital and leverage. Live equity is no longer mixed in here — see
    // StrategyInstance.equity_reference_usd and bot-strategy#222.
    let total_risk = equity * cfg.risk_pct_per_trade * max_leverage;
    let base_leg = (total_risk / 2.0).max(10.0);
    // Combined per-entry scale: beta-gap shrink (#461, ≤ 1.0) × depth
    // sizing (#515, may exceed 1.0). Applied before the notional cap
    // (caller supplies the resolved product, default 1.0).
    let scale = notional_scale.clamp(0.0, MAX_NOTIONAL_SCALE);
    let mut leg_notional = (base_leg * scale).max(10.0);
    let sizing_beta = resolve_sizing_beta(beta, sizing_beta_floor);
    let notional_cap = equity * max_leverage * cfg.risk.max_notional_headroom;
    if let Some(capped) = cap_leg_notional(leg_notional, sizing_beta, notional_cap) {
        log::warn!(
            "[RISK_NOTIONAL_CAP] leg_notional {:.2} → {:.2} (cap={:.2}, equity={:.2}, max_leverage={:.2}, headroom={:.3}, |beta|={:.4})",
            leg_notional,
            capped,
            notional_cap,
            equity,
            max_leverage,
            cfg.risk.max_notional_headroom,
            sizing_beta
        );
        leg_notional = capped;
    }
    let notional = Decimal::from_f64(leg_notional).ok_or_else(|| anyhow!("invalid notional"))?;

    let qty_a = if p1.price == Decimal::ZERO {
        Decimal::ZERO
    } else {
        let mut qty = notional / p1.price;
        if let Some(decimals) = p1.size_decimals {
            qty = qty.round_dp(decimals);
        }
        if let Some(min_ord) = p1.min_order {
            if qty > Decimal::ZERO && qty < min_ord {
                qty = min_ord;
            }
        }
        qty
    };
    // Compute qty_b from the actual notional of leg A (after min_order adjustment)
    // so that the hedge ratio matches beta: notional_b = notional_a * sizing_beta
    let actual_notional_a = qty_a * p1.price;
    let qty_b = if p2.price == Decimal::ZERO {
        Decimal::ZERO
    } else {
        let beta_dec = Decimal::from_f64(sizing_beta).unwrap_or(Decimal::ONE);
        let mut qty = (actual_notional_a * beta_dec) / p2.price;
        if let Some(decimals) = p2.size_decimals {
            qty = qty.round_dp(decimals);
        }
        if let Some(min_ord) = p2.min_order {
            if qty > Decimal::ZERO && qty < min_ord {
                qty = min_ord;
            }
        }
        qty
    };
    Ok((qty_a, qty_b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_disabled_returns_none() {
        assert_eq!(cap_leg_notional(100_000.0, 1.0, 0.0), None);
        assert_eq!(cap_leg_notional(100_000.0, 1.0, -1.0), None);
    }

    #[test]
    fn cap_no_op_when_under_budget() {
        // 5_000 leg, cap 50_000 → no clamp.
        assert_eq!(cap_leg_notional(5_000.0, 1.0, 50_000.0), None);
    }

    #[test]
    fn cap_clamps_when_beta_le_one() {
        // beta=0.8, leg=100_000, cap=50_000 → max(1, 0.8)=1.0 → allowed = 50_000.
        // leg_b would have been 80_000, but after clamp leg_a=50_000 → leg_b=40_000.
        assert_eq!(cap_leg_notional(100_000.0, 0.8, 50_000.0), Some(50_000.0));
    }

    #[test]
    fn cap_clamps_when_beta_gt_one() {
        // beta=2.0, leg=100_000, cap=50_000 → max(1, 2)=2 → allowed = 25_000.
        // After clamp: leg_a=25_000, leg_b=25_000*2=50_000 (== cap).
        assert_eq!(cap_leg_notional(100_000.0, 2.0, 50_000.0), Some(25_000.0));
    }

    #[test]
    fn cap_handles_negative_beta() {
        // OLS β can come out negative for cointegrated pairs; |beta| is what
        // determines leg-B notional. -2.0 must clamp identically to +2.0.
        assert_eq!(cap_leg_notional(100_000.0, -2.0, 50_000.0), Some(25_000.0));
    }

    #[test]
    fn cap_at_exact_threshold_no_clamp() {
        // leg_notional == allowed → no clamp (use > comparison, not ≥).
        assert_eq!(cap_leg_notional(50_000.0, 1.0, 50_000.0), None);
    }

    #[test]
    fn beta_gap_notional_scale_disabled_returns_one() {
        // scale_param == 0 → feature disabled, regardless of beta_gap.
        assert_eq!(beta_gap_notional_scale(0.0, 0.0, 0.5), 1.0);
        assert_eq!(beta_gap_notional_scale(0.3, 0.0, 0.5), 1.0);
        assert_eq!(beta_gap_notional_scale(0.3, -0.5, 0.5), 1.0);
    }

    #[test]
    fn beta_gap_notional_scale_linear_in_gap() {
        // scale=1.0, gap=0.20 → 1 - 0.20 = 0.80
        assert!((beta_gap_notional_scale(0.20, 1.0, 0.0) - 0.80).abs() < 1e-9);
        // scale=2.0, gap=0.20 → 1 - 0.40 = 0.60
        assert!((beta_gap_notional_scale(0.20, 2.0, 0.0) - 0.60).abs() < 1e-9);
    }

    #[test]
    fn beta_gap_notional_scale_clamps_at_floor() {
        // scale=10, gap=0.30 → would give 1 - 3.0 = -2.0; clamp to floor=0.5.
        assert_eq!(beta_gap_notional_scale(0.30, 10.0, 0.5), 0.5);
        // scale=1, gap=0.20, floor=0.9 → would give 0.80, clamp up to 0.9.
        assert_eq!(beta_gap_notional_scale(0.20, 1.0, 0.9), 0.9);
    }

    #[test]
    fn beta_gap_notional_scale_negative_gap_is_zero() {
        // Defensive: beta_gap should never be negative, but clamp to 0.
        assert_eq!(beta_gap_notional_scale(-0.5, 1.0, 0.5), 1.0);
    }

    #[test]
    fn resolve_sizing_beta_disabled_returns_raw_beta() {
        // floor <= 0.0 → legacy behavior, |beta| unchanged.
        assert_eq!(resolve_sizing_beta(0.48, 0.0), 0.48);
        assert_eq!(resolve_sizing_beta(-0.48, 0.0), 0.48);
    }

    #[test]
    fn resolve_sizing_beta_floors_low_beta() {
        // bot-strategy#798 trade #8: beta=0.48, floor=0.6 → 0.6 used for sizing.
        assert_eq!(resolve_sizing_beta(0.48, 0.6), 0.6);
        assert_eq!(resolve_sizing_beta(-0.48, 0.6), 0.6);
    }

    #[test]
    fn resolve_sizing_beta_no_op_above_floor() {
        // beta already above the floor → floor is a no-op.
        assert_eq!(resolve_sizing_beta(1.05, 0.6), 1.05);
    }

    #[test]
    fn resolve_sizing_beta_negative_floor_disabled() {
        // Defensive: a misconfigured negative floor must not raise beta above
        // its own absolute value.
        assert_eq!(resolve_sizing_beta(0.48, -1.0), 0.48);
    }

    #[test]
    fn depth_size_disabled_returns_one() {
        // slope == 0 → feature disabled regardless of z depth.
        assert_eq!(depth_size_mult(5.0, 2.0, 0.0, 0.5, 1.5), 1.0);
        assert_eq!(depth_size_mult(2.0, 2.0, -1.0, 0.5, 1.5), 1.0);
    }

    #[test]
    fn depth_size_at_threshold_is_s_min() {
        // |z| == entry_z_base → depth 0 → multiplier bottoms out at s_min.
        assert_eq!(depth_size_mult(2.0, 2.0, 0.5, 0.5, 1.5), 0.5);
        // Below threshold (shouldn't happen at entry, but defensive):
        // negative depth clamps to 0 → still s_min.
        assert_eq!(depth_size_mult(1.5, 2.0, 0.5, 0.5, 1.5), 0.5);
    }

    #[test]
    fn depth_size_linear_in_depth() {
        // slope=0.5: |z|=3.0 vs base 2.0 → 0.5 + 0.5*1.0 = 1.0.
        assert!((depth_size_mult(3.0, 2.0, 0.5, 0.5, 1.5) - 1.0).abs() < 1e-9);
        // |z|=3.5 → 0.5 + 0.75 = 1.25.
        assert!((depth_size_mult(3.5, 2.0, 0.5, 0.5, 1.5) - 1.25).abs() < 1e-9);
    }

    #[test]
    fn depth_size_clamps_at_s_max() {
        // Deep |z|=6.0 → raw 0.5 + 2.0 = 2.5, clamp to s_max=1.5.
        assert_eq!(depth_size_mult(6.0, 2.0, 0.5, 0.5, 1.5), 1.5);
    }

    #[test]
    fn depth_size_guards_misconfigured_bounds() {
        // s_max above the hard cap clamps to MAX_NOTIONAL_SCALE.
        assert_eq!(depth_size_mult(20.0, 2.0, 1.0, 0.5, 10.0), 2.0);
        // s_max below s_min collapses to s_min (degenerate but safe).
        assert_eq!(depth_size_mult(3.0, 2.0, 0.5, 1.0, 0.4), 1.0);
    }
}
