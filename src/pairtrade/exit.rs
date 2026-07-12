//! Exit-decision helpers extracted from the monolithic pairtrade module.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

use super::config::{PairParams, PairTradeConfig};
use super::market::SymbolSnapshot;
use super::state::{PairSharedState, PairState, Position, PositionDirection};

pub(super) struct ExitCheck<'a> {
    pub(super) cfg: &'a PairTradeConfig,
    pub(super) pp: &'a PairParams,
    pub(super) state: &'a PairState,
    pub(super) shared: &'a PairSharedState,
    pub(super) z: f64,
    pub(super) std: f64,
    pub(super) p1: &'a SymbolSnapshot,
    pub(super) p2: &'a SymbolSnapshot,
    pub(super) equity_base: f64,
    pub(super) now_ts: i64,
}

pub(super) fn exit_reason(check: ExitCheck<'_>) -> Option<&'static str> {
    let ExitCheck {
        cfg,
        pp,
        state,
        shared,
        z,
        std,
        p1,
        p2,
        equity_base,
        now_ts,
    } = check;

    let pos = state.position.as_ref()?;

    let (z_for_exit, std_for_exit) = exit_z_view(pp, shared, pos, p1, p2, z, std);

    if z_for_exit.abs() >= pp.stop_loss_z {
        return Some("stop_loss_z");
    }
    if now_ts.saturating_sub(pos.entered_ts) >= pp.force_close_secs as i64 {
        return Some("force_close");
    }
    if pp.exit_z > 0.0 && z_for_exit.abs() <= pp.exit_z {
        return Some("exit_z");
    }
    let pnl = compute_pnl(pos, p1.price, p2.price);
    if let Some(reason) = pnl_risk_exit(cfg, pp, pnl, equity_base) {
        return Some(reason);
    }
    if std_for_exit > 1e-9 {
        if let Some(pnl) = pnl {
            if pnl > Decimal::ZERO {
                let half_life_hours = shared.half_life_hours;
                if half_life_hours.is_finite() && half_life_hours > 0.0 {
                    let elapsed_secs = now_ts.saturating_sub(pos.entered_ts).max(0) as f64;
                    let remaining_secs = (pp.force_close_secs as f64) - elapsed_secs;
                    if remaining_secs > 0.0 {
                        let half_life_secs = half_life_hours * 3600.0;
                        let k = (2.0_f64).ln() / half_life_secs;
                        let decay = (-k * remaining_secs).exp();
                        let expected_improvement = z_for_exit.abs() * (1.0 - decay);
                        let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0;
                        let cost_ratio = total_cost_bps / 10_000.0;
                        let cost_in_sigma = cost_ratio / std_for_exit;
                        if expected_improvement <= cost_in_sigma {
                            return Some("expected_value");
                        }
                    }
                }
            }
        }
    }
    None
}

/// The risk-triggered subset of `exit_reason`: `stop_loss_z`, `max_loss_r`
/// and `risk_budget`, under the same frozen-β view and precedence those gates
/// have in `exit_reason`. Used by the ineligible-close book-quality guard
/// (bot-strategy#531, PR #166 review): a position that is already stopped
/// out, past its loss budget or at its risk-budget target must close
/// immediately even into a degraded book — only the timing-only exits may be
/// deferred.
pub(super) fn risk_exit_reason(check: ExitCheck<'_>) -> Option<&'static str> {
    let ExitCheck {
        cfg,
        pp,
        state,
        shared,
        z,
        std,
        p1,
        p2,
        equity_base,
        now_ts: _,
    } = check;

    let pos = state.position.as_ref()?;
    let (z_for_exit, _) = exit_z_view(pp, shared, pos, p1, p2, z, std);
    if z_for_exit.abs() >= pp.stop_loss_z {
        return Some("stop_loss_z");
    }
    pnl_risk_exit(cfg, pp, compute_pnl(pos, p1.price, p2.price), equity_base)
}

/// bot-strategy#473: when the YAML opts into frozen-β exit and the position
/// has an entry_beta on file, recompute z under entry_beta and use it for the
/// exit-side gates (`stop_loss_z`, `exit_z`, `expected_value`). Entry /
/// regime / dashboards keep using the rolling-β z that came in via the `z`
/// arg, so the rest of the bot is unaffected.
fn exit_z_view(
    pp: &PairParams,
    shared: &PairSharedState,
    pos: &Position,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
    z: f64,
    std: f64,
) -> (f64, f64) {
    if pp.use_frozen_beta_exit_z {
        position_z_for_exit(shared, pos, p1, p2).unwrap_or((z, std))
    } else {
        (z, std)
    }
}

/// The PnL-based risk gates shared by `exit_reason` and `risk_exit_reason`:
/// `max_loss_r` (loss past the R-multiple of the per-trade risk budget) and
/// `risk_budget` (profit target reached).
fn pnl_risk_exit(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    pnl: Option<Decimal>,
    equity_base: f64,
) -> Option<&'static str> {
    let pnl = pnl?;
    let risk_budget = equity_base * cfg.risk_pct_per_trade;
    let target = Decimal::from_f64(risk_budget)?;
    if target > Decimal::ZERO {
        if pp.max_loss_r_mult > 0.0 {
            let loss_mult = Decimal::from_f64(pp.max_loss_r_mult).unwrap_or(Decimal::ONE);
            let max_loss = -target * loss_mult;
            if pnl <= max_loss {
                return Some("max_loss_r");
            }
        }
        if pnl >= target {
            return Some("risk_budget");
        }
    }
    None
}

/// Compute the position-frozen z (and std) for exit-side gates.
/// Returns `None` when entry_beta is missing or current prices can't be
/// converted to f64 — callers should fall back to the rolling-β z.
fn position_z_for_exit(
    shared: &PairSharedState,
    pos: &Position,
    p1: &SymbolSnapshot,
    p2: &SymbolSnapshot,
) -> Option<(f64, f64)> {
    let entry_beta = pos.entry_beta?;
    let log_a = p1.price.to_f64()?.ln();
    let log_b = p2.price.to_f64()?.ln();
    if !log_a.is_finite() || !log_b.is_finite() {
        return None;
    }
    shared.position_z(entry_beta, log_a, log_b)
}

pub(super) fn compute_pnl(
    pos: &Position,
    exit_price_a: Decimal,
    exit_price_b: Decimal,
) -> Option<Decimal> {
    let entry_price_a = pos.entry_price_a?;
    let entry_price_b = pos.entry_price_b?;
    let entry_size_a = pos.entry_size_a?;
    let entry_size_b = pos.entry_size_b?;
    let (pnl_a, pnl_b) = match pos.direction {
        PositionDirection::LongSpread => (
            (exit_price_a - entry_price_a) * entry_size_a,
            (entry_price_b - exit_price_b) * entry_size_b,
        ),
        PositionDirection::ShortSpread => (
            (entry_price_a - exit_price_a) * entry_size_a,
            (exit_price_b - entry_price_b) * entry_size_b,
        ),
    };
    // bot-strategy#463 Phase 2: include realized PnL from any
    // mid-hold re-hedges that shrunk the position. Grows are absorbed
    // into the volume-weighted `entry_price_b` and do not appear here.
    let realized = pos.rehedge_realized_pnl.unwrap_or(Decimal::ZERO);
    Some(pnl_a + pnl_b + realized)
}
