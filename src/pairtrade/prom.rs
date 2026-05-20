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

fn register_histogram(
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> HistogramVec {
    let h = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
        .expect("prometheus HistogramVec construction never fails for static names");
    REGISTRY
        .register(Box::new(h.clone()))
        .expect("prometheus registry rejected duplicate metric");
    h
}

// === Signal / cointegration ===

pub static Z: Lazy<GaugeVec> =
    Lazy::new(|| register_gauge("pairtrade_z", "Latest z-score per pair.", &["variant", "pair"]));

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

pub static HALF_LIFE_HOURS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_half_life_hours",
        "Estimated mean-reversion half-life in hours.",
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

pub static ENTRY_Z_THRESHOLD_EFFECTIVE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "pairtrade_entry_z_threshold_effective",
        "Per-variant entry-z threshold after beta_gap_entry_z_scale adjustment.",
        &["variant", "pair"],
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
        vec![
            0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 7.5, 10.0, 20.0,
        ],
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
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                sock.read(&mut buf),
            )
            .await;
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
}
