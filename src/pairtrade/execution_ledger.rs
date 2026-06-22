use std::env;
use std::path::PathBuf;

use chrono::Utc;
use rust_decimal::Decimal;
use serde::Serialize;

use super::config::PairTradeConfig;
use super::data_dump::RotatingDumpWriter;
use super::pnl_log::sanitize_pnl_tag;

#[derive(Debug, Serialize)]
pub(in crate::pairtrade) struct ExecutionLegFillRecord {
    pub event: &'static str,
    pub ts_ms: i64,
    pub variant: String,
    pub pair: String,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    pub leg_symbol: String,
    pub side: String,
    pub target_qty: Decimal,
    pub filled_qty: Decimal,
    pub remaining_qty: Decimal,
    pub order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_order_id: Option<String>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub order_type: String,
    pub attempt: u32,
    pub placed_ts_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_ts_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_submit_fill_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_bid: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_ask: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_value: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filled_fee: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_bps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_bps_vs_decision: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_usd_vs_decision: Option<f64>,
    pub overfill_detected: bool,
    pub underfill_detected: bool,
}

#[derive(Debug, Serialize)]
pub(in crate::pairtrade) struct ExecutionPairSummaryRecord {
    pub event: &'static str,
    pub ts_ms: i64,
    pub trade_id: String,
    pub variant: String,
    pub pair: String,
    pub phase: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    pub leg_count: usize,
    pub filled_leg_count: usize,
    pub notional_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gross_execution_slippage_bps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gross_execution_slippage_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leg_sync_gap_ms: Option<i64>,
    pub overfill_detected: bool,
    pub underfill_detected: bool,
}

pub(in crate::pairtrade) struct ExecutionLedger {
    writer: RotatingDumpWriter,
}

impl ExecutionLedger {
    pub(in crate::pairtrade) fn from_env(cfg: &PairTradeConfig) -> Option<Self> {
        let enabled = env::var("DEBOT_EXECUTION_LEDGER")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                !(v == "0" || v == "false" || v == "no")
            })
            .unwrap_or(!cfg.backtest_mode);
        if !enabled {
            return None;
        }
        let dir = env::var("DEBOT_EXECUTION_LEDGER_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                env::var("DEBOT_PNL_DIR")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .map(PathBuf::from)
            })
            .or_else(|| {
                env::var("HOME")
                    .ok()
                    .map(|home| PathBuf::from(home).join("debot_pnl"))
            })
            .unwrap_or_else(|| PathBuf::from("debot_pnl"));
        let tag = env::var("DEBOT_EXECUTION_LEDGER_TAG")
            .ok()
            .or_else(|| env::var("DEBOT_PNL_TAG").ok())
            .or_else(|| env::var("AGENT_NAME").ok())
            .or_else(|| cfg.agent_name.clone())
            .or_else(|| env::var("DEX_NAME").ok())
            .or_else(|| Some(cfg.dex_name.clone()))
            .map(|v| sanitize_pnl_tag(&v))
            .filter(|v| !v.is_empty());
        let mut name = String::from("execution");
        if let Some(tag) = tag {
            name.push('-');
            name.push_str(&tag);
        }
        name.push_str(".jsonl");
        let path = dir.join(name);
        match RotatingDumpWriter::new(path.to_string_lossy().as_ref()) {
            Ok(writer) => Some(Self { writer }),
            Err(err) => {
                log::warn!(
                    "[EXEC_LEDGER] disabled: failed to open {}: {}",
                    path.display(),
                    err
                );
                None
            }
        }
    }

    pub(in crate::pairtrade) fn write_leg_fill(&mut self, record: &ExecutionLegFillRecord) {
        self.write_record(record);
    }

    pub(in crate::pairtrade) fn write_pair_summary(&mut self, record: &ExecutionPairSummaryRecord) {
        self.write_record(record);
    }

    fn write_record<T: Serialize>(&mut self, record: &T) {
        match serde_json::to_string(record) {
            Ok(line) => {
                if let Err(err) = self.writer.write_line(&line) {
                    log::warn!("[EXEC_LEDGER] write failed: {}", err);
                }
            }
            Err(err) => log::warn!("[EXEC_LEDGER] serialize failed: {}", err),
        }
    }
}

pub(in crate::pairtrade) fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}
