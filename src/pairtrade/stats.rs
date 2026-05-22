//! Pure statistical helpers extracted from the monolithic pairtrade module.
//! No dependencies on engine state.

use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub(super) struct PriceSample {
    pub(super) log_price: f64,
    pub(super) ts: i64,
}

pub(super) fn tail_samples(history: &VecDeque<PriceSample>, len: usize) -> Vec<PriceSample> {
    let take = len.min(history.len());
    let mut v: Vec<PriceSample> = history.iter().rev().take(take).cloned().collect();
    v.reverse();
    v
}

/// Lower bound for OLS beta on BTC/ETH (and other major-major pairs).
/// Observed live entry betas have ranged 0.45-1.03 over months of running.
pub(super) const BETA_CLAMP_MIN: f64 = 0.1;
/// Upper bound for OLS beta. Tightened from 10.0 (bot-strategy#346): a
/// single corrupt tick had pushed beta to 6.15, still inside the old clamp.
/// 5.0 is ~5x the historical entry max while keeping a wide safety margin
/// for legitimate regime shifts; the tick-sanity filter is the primary
/// defense and this clamp is a backstop for anything that slips through.
pub(super) const BETA_CLAMP_MAX: f64 = 5.0;

/// Sample rejection threshold (in std-of-x units) used by
/// [`regression_beta`] to discard outlier bars whose `(log_x - mean_x)`
/// deviation is more than `K * std_x` from the window mean.
///
/// Defense-in-depth #2 from bot-strategy#472 RCA. Even with the WS-arm
/// `tick_sanity_check` (the primary fix in pairtrade#68), a future
/// corrupt frame slipping past the gate (new failure mode, missing
/// constant, etc.) could still dominate `var(x)` in the 240-bar OLS
/// window. Rejecting samples beyond `K * σ_x` makes the regression
/// resilient to up to ~10% outliers; beyond that the data has shifted
/// regime and a different mitigation is appropriate.
///
/// K = 3.0 rejects ~0.27% of normal-distributed data, an acceptable
/// information loss in the benign case while collapsing the influence
/// of a 200σ outlier (the 5/22 06:31 ETH bar) to zero.
pub(super) const REGRESSION_OUTLIER_K_SIGMA: f64 = 3.0;

pub(super) fn regression_beta(x: &[PriceSample], y: &[PriceSample]) -> f64 {
    let n = x.len().min(y.len());
    if n < 2 {
        return 1.0;
    }
    // Pass 1 — preliminary mean of x using ALL samples + raw σ_x.
    // Outliers bias both, but only enough to widen the σ envelope —
    // not enough to hide a 200σ spike, which is the regime we care
    // about (bot-strategy#472). y is mean-centered later from kept
    // samples only, so no `sum_y_all` is needed here.
    let mut sum_x_all = 0.0;
    for sample in &x[..n] {
        sum_x_all += sample.log_price;
    }
    let mean_x_initial = sum_x_all / n as f64;
    let mut var_x_raw = 0.0;
    for i in 0..n {
        let dx = x[i].log_price - mean_x_initial;
        var_x_raw += dx * dx;
    }
    let std_x_initial = (var_x_raw / n as f64).sqrt();
    let outlier_limit = REGRESSION_OUTLIER_K_SIGMA * std_x_initial;
    // Pass 2 — recompute mean over the surviving samples ONLY. This
    // step is what makes the regression resilient to one bad bar:
    // the outlier's contribution to the slope-estimate mean is zero
    // by construction, not just clipped.
    let (mut sum_x_kept, mut sum_y_kept) = (0.0, 0.0);
    let mut n_kept = 0usize;
    for i in 0..n {
        let dx_initial = x[i].log_price - mean_x_initial;
        if outlier_limit > 0.0 && dx_initial.abs() > outlier_limit {
            continue;
        }
        sum_x_kept += x[i].log_price;
        sum_y_kept += y[i].log_price;
        n_kept += 1;
    }
    if n_kept < 2 {
        return 1.0;
    }
    let mean_x = sum_x_kept / n_kept as f64;
    let mean_y = sum_y_kept / n_kept as f64;
    // Pass 3 — OLS cov / var around the refined mean, again over the
    // surviving samples only.
    let mut cov = 0.0;
    let mut var_x = 0.0;
    for i in 0..n {
        let dx_initial = x[i].log_price - mean_x_initial;
        if outlier_limit > 0.0 && dx_initial.abs() > outlier_limit {
            continue;
        }
        let dx = x[i].log_price - mean_x;
        let dy = y[i].log_price - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
    }
    if var_x.abs() < 1e-9 {
        1.0
    } else {
        (cov / var_x).clamp(BETA_CLAMP_MIN, BETA_CLAMP_MAX)
    }
}

pub(super) fn spread_slope_sigma(history: &VecDeque<f64>, window: usize) -> Option<f64> {
    let len = history.len().min(window);
    if len < 3 {
        return None;
    }
    let start = history.len() - len;
    let n = len as f64;
    let mean_i = (n - 1.0) / 2.0;
    let (mut mean_x, mut cov, mut var_i) = (0.0, 0.0, 0.0);
    for j in 0..len {
        mean_x += history[start + j];
    }
    mean_x /= n;
    for j in 0..len {
        let di = j as f64 - mean_i;
        let dx = history[start + j] - mean_x;
        cov += di * dx;
        var_i += di * di;
    }
    if var_i.abs() < 1e-15 {
        return None;
    }
    let slope = cov / var_i;
    let mut sum_sq = 0.0;
    for j in 0..len {
        let dx = history[start + j] - mean_x;
        sum_sq += dx * dx;
    }
    let std = (sum_sq / n).max(0.0).sqrt();
    if std < 1e-9 {
        return None;
    }
    Some((slope / std).abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples(prices: &[f64]) -> Vec<PriceSample> {
        prices
            .iter()
            .enumerate()
            .map(|(i, &p)| PriceSample {
                log_price: p.ln(),
                ts: i as i64,
            })
            .collect()
    }

    // ---- regression_beta — clean cases (existing behaviour preserved) ----

    #[test]
    fn regression_beta_recovers_unit_slope_on_clean_data() {
        // y = x in log-space → beta = 1.0.
        let x: Vec<f64> = (1..=240).map(|i| 100.0 + 0.01 * i as f64).collect();
        let y: Vec<f64> = (1..=240).map(|i| 100.0 + 0.01 * i as f64).collect();
        let xs = samples(&x);
        let ys = samples(&y);
        let beta = regression_beta(&xs, &ys);
        assert!(
            (beta - 1.0).abs() < 0.01,
            "beta {} should be ~1.0 on identical series",
            beta
        );
    }

    #[test]
    fn regression_beta_recovers_known_slope_on_clean_data() {
        // log(y) = 0.6 * log(x) + const + small noise — expect β ≈ 0.6.
        let x_logs: Vec<f64> = (0..240).map(|i| (i as f64) * 0.001).collect();
        let y_logs: Vec<f64> = x_logs.iter().map(|&lx| 0.6 * lx + 5.0).collect();
        let xs: Vec<PriceSample> = x_logs
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64,
            })
            .collect();
        let ys: Vec<PriceSample> = y_logs
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64,
            })
            .collect();
        let beta = regression_beta(&xs, &ys);
        assert!(
            (beta - 0.6).abs() < 0.01,
            "beta {} should be ~0.6 on clean β=0.6 data",
            beta
        );
    }

    // ---- regression_beta — bot-strategy#472 outlier rejection ----

    #[test]
    fn regression_beta_survives_single_outlier_on_x() {
        // bot-strategy#472 reproduction: one bad ETH bar in a 240-bar
        // window pre-fix collapsed β to the floor clamp. Post-fix, the
        // K*σ rejection drops the outlier and recovers the true slope.
        let mut x: Vec<f64> = (1..=240).map(|i| 100.0 + 0.01 * i as f64).collect();
        let y: Vec<f64> = (1..=240).map(|i| 100.0 + 0.01 * i as f64).collect();
        // Replace one x-sample with a 10× outlier; y at the same index
        // stays normal (the corrupt ETH frame did not move BTC).
        x[100] = 1500.0;
        let xs = samples(&x);
        let ys = samples(&y);
        let beta = regression_beta(&xs, &ys);
        assert!(
            beta > 0.5,
            "beta {} should NOT collapse to floor — outlier was rejected",
            beta
        );
        assert!(
            (beta - 1.0).abs() < 0.05,
            "beta {} should approximate the slope of the remaining 239 clean samples",
            beta
        );
    }

    #[test]
    fn regression_beta_clamps_to_floor_without_outlier_rejection_failsafe() {
        // Verify that the bare clamp still fires for the degenerate
        // case where rejection cannot recover (e.g. var_x explodes
        // genuinely, like a strong regime shift dominating the
        // window). The clamp constants are the last line of defense.
        let x: Vec<f64> = vec![100.0; 240]; // constant — var_x = 0
        let y: Vec<f64> = (0..240).map(|i| 100.0 + 0.01 * i as f64).collect();
        let xs = samples(&x);
        let ys = samples(&y);
        let beta = regression_beta(&xs, &ys);
        // var_x ≈ 0 → returns 1.0 per the n_kept/var_x guard.
        assert!((beta - 1.0).abs() < 1e-9, "got {}", beta);
    }

    #[test]
    fn regression_beta_returns_one_on_short_input() {
        let xs = samples(&[100.0]);
        let ys = samples(&[100.0]);
        assert_eq!(regression_beta(&xs, &ys), 1.0);
    }

    #[test]
    fn regression_beta_respects_clamp_min() {
        // True slope way below the floor — clamp must catch it.
        let x_logs: Vec<f64> = (0..240).map(|i| (i as f64) * 0.001).collect();
        let y_logs: Vec<f64> = x_logs.iter().map(|&lx| 0.01 * lx + 5.0).collect();
        let xs: Vec<PriceSample> = x_logs
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64,
            })
            .collect();
        let ys: Vec<PriceSample> = y_logs
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64,
            })
            .collect();
        let beta = regression_beta(&xs, &ys);
        assert!(
            (beta - BETA_CLAMP_MIN).abs() < 1e-9,
            "expected floor {}, got {}",
            BETA_CLAMP_MIN,
            beta
        );
    }

    #[test]
    fn regression_beta_respects_clamp_max() {
        // True slope above the ceiling — clamp must catch it.
        let x_logs: Vec<f64> = (0..240).map(|i| (i as f64) * 0.001).collect();
        let y_logs: Vec<f64> = x_logs.iter().map(|&lx| 8.0 * lx + 5.0).collect();
        let xs: Vec<PriceSample> = x_logs
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64,
            })
            .collect();
        let ys: Vec<PriceSample> = y_logs
            .iter()
            .enumerate()
            .map(|(i, &lp)| PriceSample {
                log_price: lp,
                ts: i as i64,
            })
            .collect();
        let beta = regression_beta(&xs, &ys);
        assert!(
            (beta - BETA_CLAMP_MAX).abs() < 1e-9,
            "expected ceiling {}, got {}",
            BETA_CLAMP_MAX,
            beta
        );
    }
}
