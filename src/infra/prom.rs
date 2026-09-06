//! In-process Prometheus exporter shared by every runtime (bot-strategy#314 /
//! #409, generalised for bot-strategy#937).
//!
//! Metrics are always defined and updated on the hot path; the HTTP
//! `/metrics` server is bound only when `PROM_LISTEN` is present in the
//! environment (e.g. `PROM_LISTEN=127.0.0.1:9464`). This keeps the rollout
//! opt-in per host: Alloy scrapes the port when it is configured, otherwise
//! the gauges are recorded but never read.
//!
//! Strategy modules register their own metric families against
//! [`REGISTRY`] through the `register_*` helpers; only the process-level
//! gauges every binary shares live here.

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

/// Process-wide registry. All metrics are registered here at first access.
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub fn register_gauge(name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(Opts::new(name, help), labels)
        .expect("prometheus GaugeVec construction never fails for static names");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("prometheus registry rejected duplicate metric");
    g
}

pub fn register_int_gauge(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(Opts::new(name, help), labels)
        .expect("prometheus IntGaugeVec construction never fails for static names");
    REGISTRY
        .register(Box::new(g.clone()))
        .expect("prometheus registry rejected duplicate metric");
    g
}

pub fn register_int_counter(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels)
        .expect("prometheus IntCounterVec construction never fails for static names");
    REGISTRY
        .register(Box::new(c.clone()))
        .expect("prometheus registry rejected duplicate metric");
    c
}

pub fn register_histogram(
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

/// Unix seconds at which this process booted, per instance label.
pub static PROCESS_START_TIMESTAMP_SECONDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "debot_process_start_timestamp_seconds",
        "Unix time at which the bot process started.",
        &["instance"],
    )
});

/// Constant-1 info gauge carrying the crate version and the git shas of this
/// repo and dex-connector, so a deployed-but-not-restarted binary is visible
/// on `/metrics`.
pub static BOT_VERSION_INFO: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "debot_version_info",
        "Build identity of the running bot (value is always 1).",
        &["instance", "version", "git_sha", "dex_connector_sha"],
    )
});

/// Spawn the metrics HTTP server if `PROM_LISTEN` is set in the environment.
/// The address must parse as `host:port`. Failures during bind are logged at
/// WARN and do not abort the bot: the gauges keep updating in-process and a
/// later scrape can be re-enabled by restarting with a valid address. Must be
/// called from within a tokio runtime.
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
            // Drain the request line + headers (ignored; localhost scraping
            // does not need routing precision). Small read budget so a
            // misbehaving peer cannot keep the task alive.
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

/// Render the whole registry in the Prometheus text exposition format.
pub fn encode_metrics() -> Result<Vec<u8>> {
    let encoder = TextEncoder::new();
    let mf = REGISTRY.gather();
    let mut buf = Vec::with_capacity(8 * 1024);
    encoder.encode(&mf, &mut buf)?;
    Ok(buf)
}

/// Stamp the version / process-start gauges. Idempotent; safe to call from
/// engine boot. `instance` is the bot-internal identifier; the Prometheus
/// `instance` label of the scrape target itself is owned by the scrape
/// config (Alloy sets it to the host id).
pub fn record_process_info(instance: &str, process_started_at: i64) {
    PROCESS_START_TIMESTAMP_SECONDS
        .with_label_values(&[instance])
        .set(process_started_at);
    BOT_VERSION_INFO
        .with_label_values(&[
            instance,
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
    fn process_info_is_exported_in_text_format() {
        record_process_info("test-instance", 1_700_000_000);
        let text = String::from_utf8(encode_metrics().unwrap()).unwrap();
        assert!(text.contains(
            "debot_process_start_timestamp_seconds{instance=\"test-instance\"} 1700000000"
        ));
        assert!(text.contains("debot_version_info{"));
    }
}
