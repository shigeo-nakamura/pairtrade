//! `status.json` (debot-dashboard schema + `book` block), S3 mirror, and
//! Prometheus gauges. See `docs/book-runtime.md` §8.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use once_cell::sync::Lazy;
use prometheus::{GaugeVec, IntCounterVec, IntGaugeVec};
use serde::Serialize;

use crate::directional::atomic_write;
use crate::infra::prom::{register_gauge, register_int_counter, register_int_gauge};
use crate::infra::s3_mirror::S3Mirror;

use super::state::BookState;

pub static GROSS_USD: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "book_gross_usd",
        "Sum of |leg notional| at the last mark.",
        &["instance"],
    )
});
pub static NET_USD: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "book_net_usd",
        "Signed sum of leg notional at the last mark.",
        &["instance"],
    )
});
pub static POSITION_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "book_position_count",
        "Number of non-zero legs.",
        &["instance"],
    )
});
pub static EQUITY_USD: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "book_equity_usd",
        "Equity used by the risk rails (venue or paper).",
        &["instance"],
    )
});
pub static SESSION_HALTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "book_session_halted",
        "1 while the sticky session halt is engaged.",
        &["instance"],
    )
});
pub static DAILY_HALTED: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "book_daily_halted",
        "1 while the daily loss halt blocks opens.",
        &["instance"],
    )
});
pub static KILL_SWITCH: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "book_kill_switch_active",
        "1 while the KILL_SWITCH file exists.",
        &["instance"],
    )
});
pub static SIGNAL_AGE_SECONDS: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge(
        "book_signal_age_seconds",
        "now - generated_at of the last accepted signal (-1 = none yet).",
        &["instance"],
    )
});
pub static NEXT_DECISION_TS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "book_next_decision_timestamp_seconds",
        "Unix time of the next decision.",
        &["instance"],
    )
});
pub static DECISION_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "book_decision_total",
        "Decisions by outcome (applied|partial|rejected|skipped|halted|flatten).",
        &["instance", "outcome"],
    )
});
pub static ORDER_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter(
        "book_order_total",
        "Order attempts by result (filled|partial|unfilled|error|blocked).",
        &["instance", "result"],
    )
});
pub static CONFIG_INFO: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge(
        "book_config_info",
        "Effective config fingerprint of the running process (value 1).",
        &["instance", "fp"],
    )
});

#[derive(Debug, Clone, Serialize)]
pub struct DashboardPosition {
    pub symbol: String,
    pub side: &'static str,
    pub size: String,
    pub entry_price: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardTradeStats {
    pub trades: u64,
    pub wins: u64,
    pub win_rate: f64,
    pub max_dd: f64,
    pub pnl: f64,
}

/// debot-dashboard's `StatusData` fields (same by-hand sync as
/// `engine_b_live`'s `DashboardStatus`).
#[derive(Debug, Clone, Serialize)]
pub struct DashboardStatus {
    pub ts: i64,
    pub updated_at: String,
    pub id: String,
    pub dex: String,
    pub dry_run: bool,
    pub has_position: bool,
    pub position_count: i32,
    pub positions_ready: bool,
    pub positions: Vec<DashboardPosition>,
    pub pnl_total: f64,
    pub pnl_today: f64,
    pub pnl_source: &'static str,
    pub kill_switch_active: bool,
    pub trade_stats: DashboardTradeStats,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct BookBlock {
    pub instance_id: String,
    pub config_fp: String,
    pub venue: String,
    pub equity_usd: f64,
    pub gross_usd: f64,
    pub net_usd: f64,
    pub unrealized_usd: f64,
    pub cum_realized_usd: f64,
    pub cum_fees_usd: f64,
    pub cum_funding_est_usd: f64,
    pub session_halted: bool,
    pub session_halt_reason: Option<String>,
    pub daily_halted: bool,
    pub next_decision_key: Option<String>,
    pub next_decision_at: Option<String>,
    pub last_decision: Option<super::state::DecisionRecord>,
    pub signal_status: String,
    pub pending_residual: bool,
    pub positions_source: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusDoc {
    #[serde(flatten)]
    pub dashboard: DashboardStatus,
    pub book: BookBlock,
}

pub struct StatusWriter {
    path: PathBuf,
    mirror: Option<Arc<S3Mirror>>,
}

impl StatusWriter {
    pub fn new(path: PathBuf, mirror: Option<Arc<S3Mirror>>) -> Self {
        Self { path, mirror }
    }

    pub fn write(&self, doc: &StatusDoc) {
        let json = match serde_json::to_string_pretty(doc) {
            Ok(j) => j,
            Err(e) => {
                log::warn!("[STATUS] serialize failed: {e}");
                return;
            }
        };
        if let Err(e) = atomic_write(&self.path, &json) {
            log::warn!("[STATUS] write {} failed: {e}", self.path.display());
        }
        if let Some(m) = &self.mirror {
            m.put_async("status.json", json.into_bytes());
        }
    }
}

pub fn rfc3339(ts: i64) -> String {
    Utc.timestamp_opt(ts, 0)
        .single()
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

pub fn dashboard_positions(
    state: &BookState,
    prices: &HashMap<String, f64>,
) -> Vec<DashboardPosition> {
    state
        .positions
        .iter()
        .filter(|(_, p)| p.qty != 0.0)
        .map(|(s, p)| DashboardPosition {
            symbol: s.clone(),
            side: if p.qty > 0.0 { "long" } else { "short" },
            size: format!("{}", p.qty.abs()),
            entry_price: format!(
                "{:.6}",
                prices
                    .get(s)
                    .copied()
                    .map(|_| p.avg_price)
                    .unwrap_or(p.avg_price)
            ),
        })
        .collect()
}

pub fn record_gauges(
    instance: &str,
    state: &BookState,
    prices: &HashMap<String, f64>,
    equity: f64,
    kill_switch: bool,
    signal_age_secs: Option<i64>,
    next_decision_ts: Option<i64>,
) {
    GROSS_USD
        .with_label_values(&[instance])
        .set(state.gross_usd(prices));
    NET_USD
        .with_label_values(&[instance])
        .set(state.net_usd(prices));
    POSITION_COUNT
        .with_label_values(&[instance])
        .set(state.positions.values().filter(|p| p.qty != 0.0).count() as i64);
    EQUITY_USD.with_label_values(&[instance]).set(equity);
    SESSION_HALTED
        .with_label_values(&[instance])
        .set(state.session.halted as i64);
    DAILY_HALTED
        .with_label_values(&[instance])
        .set(state.daily.halted as i64);
    KILL_SWITCH
        .with_label_values(&[instance])
        .set(kill_switch as i64);
    SIGNAL_AGE_SECONDS
        .with_label_values(&[instance])
        .set(signal_age_secs.map(|s| s as f64).unwrap_or(-1.0));
    NEXT_DECISION_TS
        .with_label_values(&[instance])
        .set(next_decision_ts.unwrap_or(0));
}
