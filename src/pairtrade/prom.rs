//! In-process Prometheus exporter (bot-strategy#314 / #409).
//!
//! The exporter is always defined — callers update gauges and counters
//! unconditionally on the hot path. The HTTP `/metrics` server is bound
//! only when `PROM_LISTEN` is present in the environment (e.g.
//! `PROM_LISTEN=127.0.0.1:9464`), otherwise the metrics are recorded but
//! never scraped. This keeps the production rollout opt-in per host.

use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{
    Encoder, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use std::env;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const ENV_LISTEN: &str = "PROM_LISTEN";

/// Process-wide registry. All metrics are registered here at first
/// access.
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

fn register_gauge(name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(Opts::new(name, help), labels)
        .expect("prometheus GaugeVec construction never fails for static names");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("prometheus registry rejected duplicate metric");
    g
}

fn register_int_gauge(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels)
        .expect("prometheus IntGaugeVec construction never fails for static names");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("prometheus registry rejected duplicate metric");
    g
}

fn register_int_counter(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels)
        .expect("prometheus IntCounterVec construction never fails for static names");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("prometheus registry rejected duplicate metric");
    c
}

fn register_histogram(name: &str, help: &str, labels: &[&str], buckets: Vec<f64>) -> HistogramVec {
    let h = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
        .expect("prometheus HistogramVec construction never fails for static names");
    REGISTRY
        .register(Box::new(h.clone()))
        .expect("prometheus registry rejected duplicate metric");
    h
}

// === Signal / cointegration ===

pub static Z: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_z",
        "Latest z-score per pair.",
        &["variant", "pair"],
    )
});

pub static BETA: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_beta",
        "Combined beta (Kalman) used for spread construction.",
        &["variant", "pair"],
    )
});

pub static BETA_S: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_beta_s",
        "Short-window beta input to the combined beta.",
        &["variant", "pair"],
    )
});

pub static BETA_L: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_beta_l",
        "Long-window beta input to the combined beta.",
        &["variant", "pair"],
    )
});

pub static BETA_DIVERGENCE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_beta_divergence",
        "Absolute gap |beta_s - beta_l|. Reference only — the actual entry gate uses pairtrade_beta_gap_relative.",
        &["variant", "pair"],
    )
});

pub static BETA_GAP_RELATIVE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_beta_gap_relative",
        "Relative beta divergence |beta_s - beta_l| / beta_eff, the value gated by beta_divergence_max and used by beta_gap_entry_z_scale.",
        &["variant", "pair"],
    )
});

pub static BETA_UNCERTAINTY: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_beta_uncertainty",
        "Kalman posterior 1-sigma uncertainty of the current beta estimate \
         (sqrt of the filter's `p`). Rigorous alternative to the \
         beta_s/beta_l divergence proxy. Phase 1: gauge only, not yet \
         used as an entry gate — pending calibration period. \
         (bot-strategy#462)",
        &["variant", "pair"],
    )
});

pub static HALF_LIFE_HOURS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_half_life_hours",
        "Estimated mean-reversion half-life in hours.",
        &["variant", "pair"],
    )
});

// --- Innovation-responsive persistent-regime detector (bot-strategy#494) ---
// Phase 1 shadow gauges. The detector state is pair-level (shared across
// A/B/C), so all variant series for a pair carry identical values.

pub static REGIME_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_regime_active",
        "1 while the innovation-responsive detector flags a persistent \
         β/model shift (CUSUM of normalised Δspread residuals past h_on, \
         with hysteresis). Phase 1: shadow only — gates entries solely for \
         variants that opt in via `regime_block_entries`. (bot-strategy#494)",
        &["variant", "pair"],
    )
});

pub static REGIME_CUSUM: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_regime_cusum",
        "magnitude-CUSUM statistic of the regime detector (excess of \
         |normalised innovation| over its null mean) — compared against the \
         activation (h_on) / deactivation (h_off) thresholds. (bot-strategy#494)",
        &["variant", "pair"],
    )
});

pub static REGIME_RESIDUAL_SCALE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_regime_residual_scale",
        "Robust residual scale (winsorised EWMA of |innovation|) used to \
         normalise the model's one-step Δspread residual. (bot-strategy#494)",
        &["variant", "pair"],
    )
});

pub static REGIME_INNOVATION_NORMALIZED: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_regime_innovation_normalized",
        "Latest normalised innovation (Δspread / residual_scale) fed to the \
         regime CUSUM. (bot-strategy#494)",
        &["variant", "pair"],
    )
});

pub static ADF_PVALUE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_adf_pvalue",
        "ADF cointegration test p-value (lower is more cointegrated).",
        &["variant", "pair"],
    )
});

pub static ELIGIBLE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_eligible",
        "1 when the pair passes the primary cointegration eligibility filter.",
        &["variant", "pair"],
    )
});

pub static EXIT_ELIGIBLE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_exit_eligible",
        "1 when a held position may remain open after raw eligibility plus the bounded #742 grace.",
        &["variant", "pair"],
    )
});

pub static ELIGIBILITY_MARGIN_GRACE_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_eligibility_margin_grace_active",
        "1 while the pair-level held-position eligibility margin grace is active (bot-strategy#742).",
        &["variant", "pair"],
    )
});

pub static ENTRY_Z_THRESHOLD_EFFECTIVE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_entry_z_threshold_effective",
        "Per-variant entry-z threshold after beta_gap_entry_z_scale adjustment.",
        &["variant", "pair"],
    )
});

pub static REHEDGE_EXECUTED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_rehedge_executed_total",
        "Cumulative count of re-hedges that actually fired (#463 Phase 2). \
         `mode` is `dry_run` (BT or live DRY_RUN — simulated fill) or \
         `live` (taker order placed on the venue). Always ≤ \
         `pairtrade_rehedge_needed_total` because the dispatch can still \
         skip on pending-order-in-flight / missing-plan / live-disabled.",
        &["variant", "pair", "mode"],
    )
});

pub static REHEDGE_NEEDED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_rehedge_needed_total",
        "Cumulative count of ticks where #463's `should_rehedge` returned \
         a decision — i.e. the open position's β drifted beyond \
         `rehedge_drift_threshold_pct` and the cool-down + min-notional \
         floors allowed firing. Phase 1: no order is actually placed — \
         this counter is the observability hook for tuning. Phase 2 will \
         add a sibling `_executed_total` counter at fill time.",
        &["variant", "pair"],
    )
});

pub static ENTRY_NOTIONAL_SCALE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_entry_notional_scale",
        "Multiplicative scale applied to per-leg notional on the most \
         recent entry: beta-gap shrink (`beta_gap_notional_scale × \
         beta_gap`, clamped to `beta_gap_notional_floor`, bot-strategy#461) \
         × signal-depth multiplier (`depth_size_slope`, bot-strategy#515, \
         may exceed 1.0). 1.0 = flat sizing.",
        &["variant", "pair"],
    )
});

pub static MAINTENANCE_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_maintenance_active",
        "1 when the per-tick gate sees maintenance_status() != None on the \
         connector (RSS-announced or observed-degraded). Shared across A/B/C \
         since the Lighter connector is process-global; the per-variant label \
         records which variant last wrote the value this tick. (#427)",
        &["variant"],
    )
});

// === Position / activity ===

pub static HAS_POSITION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_has_position",
        "1 when the variant currently holds a pair position.",
        &["variant", "pair"],
    )
});

pub static POSITION_AGE_SECONDS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_position_age_seconds",
        "Seconds since the current position was opened. 0 when flat.",
        &["variant", "pair"],
    )
});

pub static TIME_SINCE_LAST_TRADE_SECONDS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_time_since_last_trade_seconds",
        "Seconds since the most recent exit. NaN-coerced to -1 before first exit.",
        &["variant", "pair"],
    )
});

pub static LAST_ENTRY_Z: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_last_entry_z",
        "z-score at the most recent entry.",
        &["variant", "pair"],
    )
});

pub static LAST_EXIT_Z: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_last_exit_z",
        "z-score observed at the most recent exit.",
        &["variant", "pair"],
    )
});

/// Single-shot warning counter for β estimator collapses — every
/// transition from a healthy interior β (> 0.5) to near-floor
/// (≤ 0.15) within a single eval tick. Defense-in-depth #1 from the
/// bot-strategy#472 RCA. The primary fix is the WS-arm
/// `tick_sanity_check`; this counter surfaces any future event that
/// slips past that gate so we hear about it immediately instead of
/// via downstream symptom (PnL anomaly, dashboard sleuthing).
pub static BETA_COLLAPSE_EVENT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_beta_collapse_event_total",
        "Single-tick β transitions from > 0.5 to ≤ 0.15. Should normally \
         stay at 0; every increment indicates a corrupt-bar event that \
         the WS-arm tick_sanity_check did not catch (bot-strategy#472).",
        &["variant", "pair"],
    )
});

/// Cumulative count of entry-side partial-fill reissues where the
/// exchange-reported position size already met (or exceeded) `leg.target`
/// at the moment the bot was about to send a fresh order — i.e. the
/// race that bot-strategy#470 patched. Should normally stay at 0;
/// every increment is a near-miss / actual over-fill prevented.
pub static ENTRY_OVERSIZE_CAPPED_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_entry_oversize_capped_total",
        "Entry reissue attempts where exchange-reported position equalled \
         or exceeded leg target, prompting the cap in reissue_partial_legs \
         (bot-strategy#470). Normally 0; any increment indicates the \
         cancel-then-reissue race fired.",
        &["variant", "pair", "symbol"],
    )
});

/// bot-strategy#480 hard-cap counter. Cumulative count of partial-fill
/// reissue loops that crossed `entry_partial_fill_giveup_retries`
/// without `all_filled` clearing. Each increment corresponds to a
/// `[ORDER][GIVEUP]` ERROR log line + `force_close_all_positions` on
/// the pair. Should normally stay at 0 in steady state; any increment
/// surfaces a stuck-loop episode where reissues never converged
/// (e.g. the Tokyo Extended 54 k-retry incident on 2026-06-03..06-06).
pub static ENTRY_REISSUE_GIVEUP_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_entry_reissue_giveup_total",
        "Partial-fill reissue loops that hit the hard cap \
         (entry_partial_fill_giveup_retries) and flattened any filled \
         leg via force_close_all_positions. Normally 0; any increment \
         indicates a stuck reissue loop was broken out of.",
        &["variant", "pair"],
    )
});

/// bot-strategy#721: post-entry venue-position reconciliation found the
/// actual per-leg exposure outside the size-tick tolerance of the intended
/// target. `kind` = overfill | underfill | sign_flip | fetch_failed.
/// Normally 0; every overfill increment is a live late-fill TOCTOU event
/// of the 2026-07-08 09:42 UTC shape.
pub static ENTRY_RECONCILE_MISMATCH_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_entry_reconcile_mismatch_total",
        "Post-entry venue-position reconciliations (bot-strategy#721) that \
         found actual exposure outside tolerance of the intended target, \
         by kind (overfill/underfill/sign_flip/fetch_failed).",
        &["variant", "pair", "symbol", "kind"],
    )
});

/// bot-strategy#721: reduce-only trims of confirmed entry overfill.
/// `outcome` = attempted | succeeded | failed.
pub static ENTRY_RECONCILE_TRIM_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_entry_reconcile_trim_total",
        "Reduce-only excess trims issued by the post-entry reconciliation \
         (bot-strategy#721), by outcome (attempted/succeeded/failed).",
        &["variant", "pair", "symbol", "outcome"],
    )
});

/// bot-strategy#721: residual excess quantity (base units) still on the
/// venue after the post-entry reconciliation finished. 0 when the entry
/// reconciled clean or the trim restored the intended exposure.
pub static ENTRY_RECONCILE_RESIDUAL_EXCESS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_entry_reconcile_residual_excess",
        "Residual excess quantity (base units) left after the post-entry \
         reconciliation (bot-strategy#721). Non-zero means an unresolved \
         exposure mismatch.",
        &["variant", "pair", "symbol"],
    )
});

/// bot-strategy#721: 1 while new entries for the pair are fail-closed
/// behind an unresolved entry-exposure mismatch (trim failed / fetch
/// failed / sign flip). Cleared by the RISK_ACK sentinel.
pub static ENTRY_EXPOSURE_BLOCKED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_entry_exposure_blocked",
        "1 while new entries are fail-closed for the pair after an \
         unrepaired entry-exposure mismatch (bot-strategy#721); cleared \
         only by RISK_ACK.",
        &["variant", "pair"],
    )
});

pub static CLOSE_REASON_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_close_reason_total",
        "Cumulative count of position closes broken down by exit reason.",
        &["variant", "pair", "reason"],
    )
});

/// Every close-reason string that `apply_post_exit_state` may receive.
/// Kept in sync with the literals in `engine/exit.rs`, `engine/step.rs`
/// and the `"unknown"` fallback in `mod.rs`. The unit test below asserts
/// each new variant lands here so `increase(pairtrade_close_reason_total)`
/// stays correct on low-volume reasons.
pub const KNOWN_CLOSE_REASONS: &[&str] = &[
    "stop_loss_z",
    "beta_floor",
    "force_close",
    "exit_z",
    "max_loss_r",
    "risk_budget",
    "expected_value",
    "maintenance_preempt",
    "ineligible",
    "unknown",
];

/// Materialize every `(variant, pair, reason)` series for
/// `pairtrade_close_reason_total` at value 0. Without this, `IntCounterVec`
/// only emits a series from the moment `.inc()` is first called, so the
/// first scrape after the first close already sees value=1 with no prior
/// baseline — and `increase([range])` returns 0 for any reason that only
/// fires once in the window. bot-strategy#416.
pub fn init_close_reason_series(variant: &str, pair: &str) {
    for reason in KNOWN_CLOSE_REASONS {
        CLOSE_REASON_TOTAL
            .with_label_values(&[variant, pair, reason])
            .inc_by(0);
    }
}

pub static ENTRY_REJECT_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_entry_reject_total",
        "Cumulative count of entry attempts blocked, broken down by the \
         gate that fired. Counts pre-`should_enter` gates (KILL_SWITCH, \
         session_halted, daily_loss, circuit_breaker, waiting_first_eval, \
         regime) and the in-`should_enter` filters \
         (cooldown, post_stop_cooldown, velocity, std_collapse, std_collapse_hold_down, \
         stop_loss_z, spread_trend, beta_divergence, beta_min, beta_floor, \
         z_below_threshold, mtf, net_funding_min). Round-4 follow-up \
         (bot-strategy#355).",
        &["variant", "pair", "reason"],
    )
});

/// Pair-level outcomes of the held-position-only eligibility margin grace
/// (bot-strategy#742). It is intentionally not variant-labelled because one
/// shared evaluation and deadline governs every A/B/C variant on the pair.
pub static ELIGIBILITY_MARGIN_GRACE_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_eligibility_margin_grace_total",
        "Cumulative held-position eligibility grace transitions by outcome.",
        &["pair", "outcome"],
    )
});

pub const KNOWN_ELIGIBILITY_MARGIN_GRACE_OUTCOMES: &[&str] =
    &["started", "recovered", "expired", "severe_bypass"];

pub fn init_eligibility_margin_grace_series(pair: &str) {
    for outcome in KNOWN_ELIGIBILITY_MARGIN_GRACE_OUTCOMES {
        ELIGIBILITY_MARGIN_GRACE_TOTAL
            .with_label_values(&[pair, outcome])
            .inc_by(0);
    }
}

/// Ineligible-close deferral guard (bot-strategy#531): counts ticks on
/// which an ineligible flatten was deferred because the book looked
/// degraded (`reason` = `spread` | `stale`), plus `cap_exceeded` when the
/// deferral window ran out and the close fired into the still-degraded
/// book. Zero under normal operation — any movement here is a venue-data
/// incident worth a look.
pub static INELIGIBLE_CLOSE_DEFER_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_ineligible_close_defer_total",
        "Cumulative count of ineligible-close deferral guard events \
         (bot-strategy#531), broken down by reason (spread, stale, \
         cap_exceeded).",
        &["variant", "pair", "reason"],
    )
});

/// Every reason string `pairtrade_ineligible_close_defer_total` may receive.
/// Kept in sync with the literals `ineligible_close_book_degraded`
/// (market.rs) returns plus the `"cap_exceeded"` literal in
/// `step_plan_pair_actions` (engine/plan.rs).
pub const KNOWN_INELIGIBLE_CLOSE_DEFER_REASONS: &[&str] = &["spread", "stale", "cap_exceeded"];

/// Materialize every `(variant, pair, reason)` series for
/// `pairtrade_ineligible_close_defer_total` at value 0, for the same reason
/// as `init_close_reason_series` (bot-strategy#416): the counter is only
/// touched during a venue-data incident, so without a zero baseline the
/// first scrape after a one-tick deferral already sees value=1 and
/// `increase([range])` misses exactly the first/only incident.
pub fn init_ineligible_close_defer_series(variant: &str, pair: &str) {
    for reason in KNOWN_INELIGIBLE_CLOSE_DEFER_REASONS {
        INELIGIBLE_CLOSE_DEFER_TOTAL
            .with_label_values(&[variant, pair, reason])
            .inc_by(0);
    }
}

/// Every entry-reject reason string `pairtrade_entry_reject_total` may receive.
/// Kept in sync with the literals in `engine/step.rs` (pre-`should_enter`) and
/// `entry.rs::should_enter` (in-filter). The unit test below asserts that.
pub const KNOWN_ENTRY_REJECT_REASONS: &[&str] = &[
    // pre-should_enter (step.rs)
    "kill_switch",
    "session_halted",
    "daily_loss",
    "circuit_breaker",
    "entry_exposure_mismatch",
    "waiting_first_eval",
    "regime",
    // in-should_enter (entry.rs)
    "cooldown",
    "post_stop_cooldown",
    "velocity",
    "std_collapse",
    "std_collapse_hold_down",
    "stop_loss_z",
    "spread_trend",
    "beta_divergence",
    "beta_min",
    "beta_floor",
    "beta_uncertainty",
    "beta_clamp",
    "regime_innovation",
    "z_below_threshold",
    "mtf",
    "net_funding_min",
];

pub fn init_entry_reject_series(variant: &str, pair: &str) {
    for reason in KNOWN_ENTRY_REJECT_REASONS {
        ENTRY_REJECT_TOTAL
            .with_label_values(&[variant, pair, reason])
            .inc_by(0);
    }
}

// === Execution quality (#314 Group 4) ===
//
// Observed at the exit_fill site (engine/reconcile.rs). `gross_pnl_bps` is
// the price-move PnL `compute_pnl()` returns, normalized by the sum of
// per-leg notionals at entry. `funding_carry_bps` is the WS-derived funding
// over the hold from the same site, in the same denominator. Fees and
// slippage are not yet observable in-process (would need a dex-connector
// trait extension); BT/live gap analysis (#306) should compare BT gross to
// live gross until that lands.

pub static CLOSE_GROSS_PNL_BPS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram(
        "pairtrade_close_gross_pnl_bps",
        "Gross PnL per close, in bps of (|size_a*price_a| + |size_b*price_b|) at entry. \
         `reason` mirrors `pairtrade_close_reason_total` and lets the scatter panel \
         color points by exit cause (bot-strategy#421).",
        &["variant", "pair", "reason"],
        vec![
            -200.0, -100.0, -50.0, -25.0, -15.0, -10.0, -5.0, -2.0, 0.0, 2.0, 5.0, 10.0, 15.0,
            25.0, 50.0, 100.0, 200.0,
        ],
    )
});

pub static CLOSE_FUNDING_BPS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram(
        "pairtrade_close_funding_bps",
        "Net funding paid (-) or received (+) over the hold, in bps of entry notional.",
        &["variant", "pair"],
        vec![
            -50.0, -20.0, -10.0, -5.0, -2.0, -1.0, 0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0,
        ],
    )
});

// === Per-leg execution quality (#314 Group 4-B / partial 4-C) ===
//
// Observed per leg at fill-resolve time. Volume-weighted average fill
// price comes from `FilledOrder.filled_value / filled_size` aggregated
// across partial fills. `leg` is "entry" or "exit".
//
// LEG_SLIPPAGE_BPS only fires when `PendingLeg.limit_price = Some` —
// i.e. post-only / limit fills. Taker fallback legs (`use_market`) have
// `limit_price = None`; capturing their slippage needs a decision-time
// reference price in PendingLeg, deferred to a Group 4-B follow-up.
//
// Sign convention: positive = bot paid more / received less than the
// posted limit (cost). For post-only fills this is typically 0 or
// negative (price improvement).

pub static LEG_SLIPPAGE_BPS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram(
        "pairtrade_leg_slippage_bps",
        "Per-leg slippage vs decision-time reference price, signed by side \
         (positive = cost). `order_type` is \"post_only\" for limit/post-only \
         fills and \"taker\" for market/taker fills; the taker bucket is the \
         adverse-slippage tail the BT/live gap analysis (#306) cares about.",
        &["variant", "pair", "leg", "order_type"],
        vec![
            -50.0, -20.0, -10.0, -5.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0,
        ],
    )
});

pub static LEG_FEE_BPS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram(
        "pairtrade_leg_fee_bps",
        "Per-leg fee paid as bps of filled notional. Venues that bill no fee \
         (Lighter) typically leave this unset and emit nothing.",
        &["variant", "pair", "leg"],
        vec![0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.5, 10.0, 20.0],
    )
});

pub static LEG_FILL_LATENCY_MS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram(
        "pairtrade_leg_fill_latency_ms",
        "Wall-clock latency from PendingOrders placement to leg fill completion, in ms. \
         Requires venue-reported FilledOrder.filled_ts_ms; venues that omit it \
         (Lighter) emit nothing. (#314 Group 4-C)",
        &["variant", "pair", "leg"],
        vec![
            10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
            60_000.0, 120_000.0, 300_000.0,
        ],
    )
});

pub static EXIT_POST_ONLY_TAKEOVER_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "pairtrade_exit_post_only_takeover_total",
        "Post-only exit deadlines that fired and reissued remaining legs as taker.",
        &["variant", "pair"],
    )
});

pub static STEP_DURATION_SECONDS: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram(
        "pairtrade_step_duration_seconds",
        "Wall-clock duration of one engine-wide Strategy::step() call.",
        &[],
        vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ],
    )
});

// === Risk / kill state ===

pub static KILL_SWITCH_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_kill_switch_active",
        "1 when the process-wide KILL_SWITCH file is present.",
        &["variant"],
    )
});

pub static SESSION_DD_HALT_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_session_dd_halt_active",
        "1 when the variant is in a sticky session-DD halt.",
        &["variant"],
    )
});

pub static DAILY_DD_HALT_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_daily_dd_halt_active",
        "1 when the variant has tripped today's daily-DD threshold.",
        &["variant"],
    )
});

pub static CIRCUIT_BREAKER_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_circuit_breaker_active",
        "1 while the consecutive-loss circuit breaker cooldown is in effect.",
        &["variant"],
    )
});

// === System health ===

pub static SNAPSHOT_AGE_SECONDS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_snapshot_age_seconds",
        "Age of pairtrade_history_*.json on disk (file mtime delta).",
        &["variant"],
    )
});

pub static PROCESS_START_TIMESTAMP_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_process_start_timestamp_seconds",
        "Unix timestamp of process boot.",
        &["variant"],
    )
});

pub static BOT_VERSION_INFO: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_bot_version_info",
        "Always 1; carries version and git_sha labels.",
        &["variant", "version", "git_sha", "dex_connector_sha"],
    )
});

// === Capital / config drift ===

pub static EQUITY_REFERENCE_USD: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_equity_reference_usd",
        "Configured per-variant equity reference used for sizing.",
        &["variant"],
    )
});

pub static MAX_LEVERAGE_CONFIG: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_max_leverage_config",
        "Configured process-wide maximum leverage, repeated per variant for drift comparison.",
        &["variant"],
    )
});

// === Effective-config fingerprint (bot-strategy#580) ===
//
// These make the *running* effective config observable so a drift monitor can
// catch "deployed but not loaded" (deploy ≠ restart) within minutes instead of
// at readout. `record_config_info` stamps them once at engine construction.

pub static CONFIG_FINGERPRINT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_config_fingerprint",
        "Always 1; the `fp` label is a sha256-12 over the effective per-variant trading config.",
        &["variant", "fp"],
    )
});

pub static CONFIG_FILE_INFO: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_config_file_info",
        "Always 1; carries the source YAML `path` and its content `sha` (sha256-12) as read at \
         boot. Compare against `sha256sum` of the on-disk file to detect a config rewritten \
         after the process started (deploy-not-loaded). Repeated per variant.",
        &["variant", "path", "sha"],
    )
});

pub static CONFIG_FILE_MTIME_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_config_file_mtime_seconds",
        "Unix mtime of the source config file as read at boot. mtime > process_start_timestamp \
         on the live file means a config was deployed but not yet loaded. Repeated per variant.",
        &["variant"],
    )
});

pub static EFFECTIVE_FORCE_CLOSE_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_force_close_secs",
        "Effective per-variant force_close hold ceiling (seconds) the running process is using.",
        &["variant"],
    )
});

pub static EFFECTIVE_EXIT_Z: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_exit_z",
        "Effective per-variant exit z-score threshold the running process is using.",
        &["variant"],
    )
});

pub static EFFECTIVE_STOP_LOSS_Z: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_stop_loss_z",
        "Effective per-variant stop_loss z-score threshold the running process is using.",
        &["variant"],
    )
});

pub static EFFECTIVE_SIZING_BETA_FLOOR: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_sizing_beta_floor",
        "Effective entry sizing beta floor for this variant; 0 disables.",
        &["variant"],
    )
});

pub static EFFECTIVE_EXIT_ON_SIZING_BETA_FLOOR: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_effective_exit_on_sizing_beta_floor",
        "1 when held positions close after rolling beta falls below the sizing floor.",
        &["variant"],
    )
});

pub static EFFECTIVE_FROZEN_BETA_EXIT_Z: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_effective_frozen_beta_exit_z",
        "1 if the running process evaluates the exit z against a frozen entry-time beta \
         (use_frozen_beta_exit_z) for this variant, else 0.",
        &["variant"],
    )
});

pub static EFFECTIVE_EXIT_POST_ONLY_ENABLED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "pairtrade_effective_exit_post_only_enabled",
        "1 when the running process allows maker-first exits on zero-fee venues.",
        &["variant"],
    )
});

pub static EFFECTIVE_EXIT_POST_ONLY_TIMEOUT_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_exit_post_only_timeout_secs",
        "Effective maker-first exit deadline before taker takeover, in seconds.",
        &["variant"],
    )
});

pub static EFFECTIVE_INELIGIBLE_CLOSE_DEFER_CAP_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_ineligible_close_defer_cap_secs",
        "Effective ineligible-close book-quality deferral cap (seconds) the running process \
         is using; 0 means the guard is disabled (bot-strategy#531). Process-wide, repeated \
         per variant so the drift preflight can assert it alongside the other effective params.",
        &["variant"],
    )
});

pub static EFFECTIVE_INELIGIBLE_CLOSE_DEFER_SPREAD_BPS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_ineligible_close_defer_spread_bps",
        "Effective per-leg spread threshold (bps) above which the ineligible-close guard \
         treats the book as degraded (bot-strategy#531). Process-wide, repeated per variant.",
        &["variant"],
    )
});

pub static EFFECTIVE_INELIGIBLE_CLOSE_DEFER_STALE_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_ineligible_close_defer_stale_secs",
        "Effective feed-staleness threshold (seconds) above which the ineligible-close guard \
         treats a leg's feed as degraded (bot-strategy#531). Process-wide, repeated per variant.",
        &["variant"],
    )
});

pub static EFFECTIVE_ELIGIBILITY_MARGIN_GRACE_SECS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_eligibility_margin_grace_secs",
        "Effective held-position eligibility margin grace in seconds; 0 disables (bot-strategy#742).",
        &["variant"],
    )
});

pub static EFFECTIVE_ELIGIBILITY_BETA_GAP_EXIT: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_effective_eligibility_beta_gap_exit",
        "Effective upper relative beta-gap bound for the held-position grace (bot-strategy#742).",
        &["variant"],
    )
});

/// Spawn the metrics HTTP server if `PROM_LISTEN` is set in the
/// environment. The address must parse as `host:port`. Failures during
/// bind are logged at WARN and do not abort the bot — the gauges keep
/// updating in-process and a later /metrics scrape can be re-enabled by
/// restart with a valid address.
pub fn maybe_start_exporter() {
    let addr_str = match env::var(ENV_LISTEN) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            log::info!(
                "[PROM] {} not set; metrics recorded but /metrics endpoint disabled",
                ENV_LISTEN
            );
            return;
        }
    };
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(e) => {
            log::warn!(
                "[PROM] failed to parse {}={}: {}; exporter disabled",
                ENV_LISTEN,
                addr_str,
                e
            );
            return;
        }
    };
    tokio::spawn(async move {
        if let Err(e) = serve(addr).await {
            log::warn!("[PROM] exporter exited: {:?}", e);
        }
    });
}

async fn serve(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("[PROM] exporter listening on http://{}/metrics", addr);
    loop {
        let (mut sock, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                log::warn!("[PROM] accept error: {}", e);
                continue;
            }
        };
        tokio::spawn(async move {
            // Drain the request line + headers (we ignore them; localhost
            // scraping doesn't need routing precision). Use a small read
            // budget so a malicious peer can't keep the task alive.
            let mut buf = [0u8; 1024];
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(2), sock.read(&mut buf)).await;
            let body = match encode_metrics() {
                Ok(b) => b,
                Err(e) => {
                    log::warn!("[PROM] encode error for {}: {}", peer, e);
                    return;
                }
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                TextEncoder::new().format_type(),
                body.len()
            );
            if let Err(e) = sock.write_all(resp.as_bytes()).await {
                log::debug!("[PROM] write header to {} failed: {}", peer, e);
                return;
            }
            let _ = sock.write_all(&body).await;
        });
    }
}

fn encode_metrics() -> Result<Vec<u8>> {
    let encoder = TextEncoder::new();
    let mf = REGISTRY.gather();
    let mut buf = Vec::with_capacity(8 * 1024);
    encoder.encode(&mf, &mut buf)?;
    Ok(buf)
}

/// Stamp version / process-start gauges. Idempotent; safe to call from
/// engine boot. `variant` is the bot-internal A/B/C identifier
/// (`StrategyInstance.id`); the Prometheus `instance` label is owned by
/// the scrape config (Alloy sets it to the host id, e.g. `debot-tokyo`).
pub fn record_process_info(variant: &str, process_started_at: i64) {
    PROCESS_START_TIMESTAMP_SECONDS
        .with_label_values(&[variant])
        .set(process_started_at);
    BOT_VERSION_INFO
        .with_label_values(&[
            variant,
            env!("CARGO_PKG_VERSION"),
            option_env!("PAIRTRADE_GIT_SHA").unwrap_or("unknown"),
            option_env!("DEX_CONNECTOR_GIT_HASH").unwrap_or("unknown"),
        ])
        .set(1);
}

/// Stamp the effective-config / fingerprint gauges for one variant
/// (bot-strategy#580). Called once per variant at engine construction so the
/// running config is observable on `/metrics` before the first tick. `fp` is
/// the sha256-12 of the effective trading params; `file_path` / `file_sha` /
/// `file_mtime` describe the source YAML as read at boot (empty / 0 for
/// env-only builds).
#[allow(clippy::too_many_arguments)]
pub fn record_config_info(
    variant: &str,
    fp: &str,
    force_close_secs: u64,
    exit_z: f64,
    stop_loss_z: f64,
    sizing_beta_floor: f64,
    exit_on_sizing_beta_floor: bool,
    frozen_beta_exit_z: bool,
    exit_post_only_enabled: bool,
    exit_post_only_timeout_secs: u64,
    equity_reference_usd: f64,
    max_leverage: f64,
    ineligible_close_defer_cap_secs: i64,
    ineligible_close_defer_spread_bps: f64,
    ineligible_close_defer_stale_secs: i64,
    eligibility_margin_grace_secs: i64,
    eligibility_beta_gap_exit: f64,
    file_path: &str,
    file_sha: &str,
    file_mtime: i64,
) {
    CONFIG_FINGERPRINT.with_label_values(&[variant, fp]).set(1);
    CONFIG_FILE_INFO
        .with_label_values(&[variant, file_path, file_sha])
        .set(1);
    CONFIG_FILE_MTIME_SECONDS
        .with_label_values(&[variant])
        .set(file_mtime);
    EFFECTIVE_FORCE_CLOSE_SECS
        .with_label_values(&[variant])
        .set(force_close_secs as f64);
    EFFECTIVE_EXIT_Z.with_label_values(&[variant]).set(exit_z);
    EFFECTIVE_STOP_LOSS_Z
        .with_label_values(&[variant])
        .set(stop_loss_z);
    EFFECTIVE_SIZING_BETA_FLOOR
        .with_label_values(&[variant])
        .set(sizing_beta_floor);
    EFFECTIVE_EXIT_ON_SIZING_BETA_FLOOR
        .with_label_values(&[variant])
        .set(if exit_on_sizing_beta_floor { 1 } else { 0 });
    EFFECTIVE_FROZEN_BETA_EXIT_Z
        .with_label_values(&[variant])
        .set(if frozen_beta_exit_z { 1 } else { 0 });
    EFFECTIVE_EXIT_POST_ONLY_ENABLED
        .with_label_values(&[variant])
        .set(if exit_post_only_enabled { 1 } else { 0 });
    EFFECTIVE_EXIT_POST_ONLY_TIMEOUT_SECS
        .with_label_values(&[variant])
        .set(exit_post_only_timeout_secs as f64);
    // Every field committed in round.json must be assertable from /metrics at
    // boot (bot-strategy#580 review). equity_reference_usd / max_leverage are
    // also refreshed per-tick in prom_metrics.rs; stamping them here makes the
    // full config set observable before the first tick so the preflight never
    // sees a partial series.
    EQUITY_REFERENCE_USD
        .with_label_values(&[variant])
        .set(equity_reference_usd);
    MAX_LEVERAGE_CONFIG
        .with_label_values(&[variant])
        .set(max_leverage);
    EFFECTIVE_INELIGIBLE_CLOSE_DEFER_CAP_SECS
        .with_label_values(&[variant])
        .set(ineligible_close_defer_cap_secs as f64);
    EFFECTIVE_INELIGIBLE_CLOSE_DEFER_SPREAD_BPS
        .with_label_values(&[variant])
        .set(ineligible_close_defer_spread_bps);
    EFFECTIVE_INELIGIBLE_CLOSE_DEFER_STALE_SECS
        .with_label_values(&[variant])
        .set(ineligible_close_defer_stale_secs as f64);
    EFFECTIVE_ELIGIBILITY_MARGIN_GRACE_SECS
        .with_label_values(&[variant])
        .set(eligibility_margin_grace_secs as f64);
    EFFECTIVE_ELIGIBILITY_BETA_GAP_EXIT
        .with_label_values(&[variant])
        .set(eligibility_beta_gap_exit);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_close_reason_series_registers_every_known_reason_at_zero() {
        let variant = "test-init-zero";
        let pair = "BTC/ETH";
        init_close_reason_series(variant, pair);
        for reason in KNOWN_CLOSE_REASONS {
            let value = CLOSE_REASON_TOTAL
                .with_label_values(&[variant, pair, reason])
                .get();
            assert_eq!(value, 0, "reason {} should be pre-registered at 0", reason);
        }
    }

    #[test]
    fn init_ineligible_close_defer_series_registers_every_known_reason_at_zero() {
        let variant = "test-init-defer-zero";
        let pair = "BTC/ETH";
        init_ineligible_close_defer_series(variant, pair);
        for reason in KNOWN_INELIGIBLE_CLOSE_DEFER_REASONS {
            let value = INELIGIBLE_CLOSE_DEFER_TOTAL
                .with_label_values(&[variant, pair, reason])
                .get();
            assert_eq!(value, 0, "reason {} should be pre-registered at 0", reason);
        }
    }

    #[test]
    fn init_close_reason_series_is_idempotent_and_does_not_clobber_increments() {
        let variant = "test-init-idempotent";
        let pair = "BTC/ETH";
        init_close_reason_series(variant, pair);
        CLOSE_REASON_TOTAL
            .with_label_values(&[variant, pair, "exit_z"])
            .inc();
        // Second call simulates a hypothetical re-init path; must not zero
        // out the live counter.
        init_close_reason_series(variant, pair);
        assert_eq!(
            CLOSE_REASON_TOTAL
                .with_label_values(&[variant, pair, "exit_z"])
                .get(),
            1
        );
    }

    #[test]
    fn step_duration_histogram_has_expected_buckets() {
        // The histogram is process-global and the halt-gate integration
        // tests (bot-strategy#537) drive real `step()` ticks that observe
        // into it concurrently, so assert on the delta produced by our own
        // observation rather than an absolute sample count.
        fn sample_count() -> u64 {
            let metric = REGISTRY
                .gather()
                .into_iter()
                .find(|family| family.get_name() == "pairtrade_step_duration_seconds")
                .expect("step duration metric should be registered");
            metric.get_metric()[0].get_histogram().get_sample_count()
        }
        let before = sample_count();
        STEP_DURATION_SECONDS.with_label_values(&[]).observe(0.5);
        assert!(
            sample_count() >= before + 1,
            "our observation must land in the registered histogram"
        );
        let metric = REGISTRY
            .gather()
            .into_iter()
            .find(|family| family.get_name() == "pairtrade_step_duration_seconds")
            .expect("step duration metric should be registered");
        let histogram = metric.get_metric()[0].get_histogram();
        assert!(histogram
            .get_bucket()
            .iter()
            .any(|bucket| bucket.get_upper_bound() == 10.0));
    }
}
