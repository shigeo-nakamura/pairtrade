//! `status.json` (debot-dashboard schema + `book` block) and its S3 mirror.
//! See `docs/book-runtime.md` §8.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde::Serialize;

use crate::directional::atomic_write;
use crate::infra::s3_mirror::S3Mirror;

use super::state::BookState;

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
    /// Seconds since the last *accepted* signal was generated (restored from
    /// `state.json` across restarts, untouched by later rejects/skips).
    /// `None` until a signal has been accepted.
    pub signal_age_secs: Option<i64>,
    pub pending_residual: bool,
    pub positions_source: &'static str,
    pub equity_ready: bool,
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
