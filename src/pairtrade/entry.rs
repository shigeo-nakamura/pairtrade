//! Entry-decision helpers extracted from the monolithic pairtrade module.
//! Pure functions over config, params, and per-pair state.

use super::config::{PairParams, PairTradeConfig};
use super::state::PairState;
use super::stats::spread_slope_sigma;
use super::util::tail_std;

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
    state: &PairState,
    vol_median: f64,
) -> f64 {
    let entry_vol_len =
        ((pp.entry_vol_lookback_hours * 3600) / cfg.trading_period_secs).max(1) as usize;
    let vol_pair = tail_std(&state.spread_history, entry_vol_len).unwrap_or(1.0);
    let alpha = (vol_pair / vol_median).clamp(ENTRY_Z_SCALE_MIN, ENTRY_Z_SCALE_MAX);
    let z = pp.entry_z_base * alpha;
    z.clamp(pp.entry_z_min, pp.entry_z_max)
}

pub(super) fn should_enter(
    cfg: &PairTradeConfig,
    pp: &PairParams,
    state: &PairState,
    z: f64,
    std: f64,
    net_funding: f64,
    now_ts: i64,
) -> bool {
    if let Some(last_exit_ts) = state.last_exit_ts {
        if now_ts.saturating_sub(last_exit_ts) < pp.cooldown_secs as i64 {
            return false;
        }
    }

    // --- Phase 2 filter: spread momentum block ---
    // Block entry when spread is moving fast (likely trending, not mean-reverting).
    // Disabled when entry_velocity_block_sigma_per_min == 0.0.
    if pp.entry_velocity_block_sigma_per_min > 0.0
        && state.last_velocity_sigma_per_min.abs() >= pp.entry_velocity_block_sigma_per_min
    {
        return false;
    }

    let mut entry_threshold = if net_funding > 0.0 {
        // prefer positive carry by easing the required entry slightly
        state.z_entry * FUNDING_CARRY_ENTRY_DISCOUNT
    } else {
        state.z_entry
    };

    // --- Phase 2 filter: funding rate continuous scaling ---
    // Scale entry_z based on funding magnitude (beyond the simple discount
    // above). funding_entry_z_scale > 0: entry_z *= 1.0 - scale * net_funding
    //   positive funding → lower threshold (easier entry)
    //   negative funding → higher threshold (harder entry)
    // Disabled when funding_entry_z_scale == 0.0.
    if pp.funding_entry_z_scale > 0.0 {
        let adjustment = 1.0 - pp.funding_entry_z_scale * net_funding;
        entry_threshold *= adjustment.clamp(ENTRY_Z_SCALE_MIN, ENTRY_Z_SCALE_MAX);
    }

    // --- Phase 2 filter: beta gap dynamic adjustment ---
    // Raise entry threshold when beta_s and beta_l diverge (hedge unreliable).
    // entry_z *= 1.0 + scale * beta_gap
    // Disabled when beta_gap_entry_z_scale == 0.0.
    if pp.beta_gap_entry_z_scale > 0.0 {
        entry_threshold *= 1.0 + pp.beta_gap_entry_z_scale * state.beta_gap;
    }

    // Avoid entering when the current z already triggers stop-loss exit.
    if z.abs() >= pp.stop_loss_z {
        return false;
    }
    // Spread trend filter: block entry if spread is trending
    if let Some(slope_sigma) = spread_slope_sigma(&state.spread_history, cfg.metrics_window) {
        if slope_sigma > pp.spread_trend_max_slope_sigma {
            return false;
        }
    }
    // Beta stability filter: block entry if beta_s and beta_l diverge
    if state.beta_gap > pp.beta_divergence_max {
        return false;
    }
    // Beta minimum filter: block entry if beta is too low (hedge leg too small)
    if pp.beta_min > 0.0 && state.beta < pp.beta_min {
        return false;
    }
    // Account for estimated cost (fees + slippage) in sigma units
    let total_cost_bps = cfg.fee_bps * 2.0 + cfg.slippage_cost_bps() * 2.0; // two legs
    let cost_ratio = total_cost_bps / 10_000.0;
    let cost_in_sigma = if std <= 1e-9 { 0.0 } else { cost_ratio / std };
    if z.abs() < entry_threshold {
        return false;
    }

    z.abs() >= entry_threshold + cost_in_sigma && net_funding >= cfg.net_funding_min_per_hour
}
