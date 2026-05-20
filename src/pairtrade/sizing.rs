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

pub(super) fn hedged_sizes(
    cfg: &PairTradeConfig,
    equity: f64,
    beta: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
    notional_scale: f64,
) -> Result<(Decimal, Decimal)> {
    // `equity` is the per-instance fixed `equity_reference_usd` so each
    // variant sizes against its own declared capital. Live equity is no
    // longer mixed in here — see StrategyInstance.equity_reference_usd
    // and bot-strategy#222.
    let total_risk = equity * cfg.risk_pct_per_trade * cfg.max_leverage;
    let base_leg = (total_risk / 2.0).max(10.0);
    // bot-strategy#461: shrink notional under beta-uncertainty before the
    // notional cap (caller supplies the resolved scale, default 1.0).
    let scale = notional_scale.clamp(0.0, 1.0);
    let mut leg_notional = (base_leg * scale).max(10.0);
    let notional_cap = equity * cfg.max_leverage * cfg.risk.max_notional_headroom;
    if let Some(capped) = cap_leg_notional(leg_notional, beta, notional_cap) {
        log::warn!(
            "[RISK_NOTIONAL_CAP] leg_notional {:.2} → {:.2} (cap={:.2}, equity={:.2}, max_leverage={:.2}, headroom={:.3}, |beta|={:.4})",
            leg_notional,
            capped,
            notional_cap,
            equity,
            cfg.max_leverage,
            cfg.risk.max_notional_headroom,
            beta.abs()
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
    // so that the hedge ratio matches beta: notional_b = notional_a * beta
    let actual_notional_a = qty_a * p1.price;
    let qty_b = if p2.price == Decimal::ZERO {
        Decimal::ZERO
    } else {
        let beta_dec = Decimal::from_f64(beta.abs()).unwrap_or(Decimal::ONE);
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
}
