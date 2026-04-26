//! Position-sizing helpers extracted from the monolithic pairtrade module.

use anyhow::{anyhow, Result};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;

use super::config::PairTradeConfig;
use super::market::SymbolSnapshot;

/// Apply the `risk.max_notional_usd_per_leg` hard cap to the leg-A target
/// notional. The intended hedge is `(leg_notional, leg_notional · |beta|)`,
/// so when `|beta| > 1` the leg-B notional is the binding constraint —
/// dividing by `max(1, |beta|)` keeps the hedge ratio intact while ensuring
/// NEITHER leg exceeds the cap. Returns `None` when the cap is disabled
/// (≤ 0) or the leg is already within budget. bot-strategy#185 Phase 3-4.
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

pub(super) fn hedged_sizes(
    cfg: &PairTradeConfig,
    equity: f64,
    beta: f64,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
) -> Result<(Decimal, Decimal)> {
    // `equity` is expected to be pre-floored by the caller with the
    // per-instance equity_usd_fallback so each variant sizes against
    // its own sub-account capacity. See StrategyInstance.equity_usd_fallback.
    let total_risk = equity * cfg.risk_pct_per_trade * cfg.max_leverage;
    let mut leg_notional = (total_risk / 2.0).max(10.0);
    if let Some(capped) = cap_leg_notional(leg_notional, beta, cfg.risk.max_notional_usd_per_leg) {
        log::warn!(
            "[RISK_NOTIONAL_CAP] leg_notional {:.2} → {:.2} (cap={:.2}, |beta|={:.4})",
            leg_notional,
            capped,
            cfg.risk.max_notional_usd_per_leg,
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
}
