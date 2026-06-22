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
                } else if let Err(err) = self.writer.flush() {
                    log::warn!("[EXEC_LEDGER] flush failed: {}", err);
                }
            }
            Err(err) => log::warn!("[EXEC_LEDGER] serialize failed: {}", err),
        }
    }
}

pub(in crate::pairtrade) fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_leg_fill_record_core_fields() {
        let record = ExecutionLegFillRecord {
            event: "leg_fill",
            ts_ms: 1_789_000_000_000,
            variant: "A".to_string(),
            pair: "BTC/ETH".to_string(),
            phase: "exit".to_string(),
            close_reason: Some("force_close".to_string()),
            leg_symbol: "BTC".to_string(),
            side: "Long".to_string(),
            target_qty: Decimal::new(100, 3),
            filled_qty: Decimal::new(100, 3),
            remaining_qty: Decimal::ZERO,
            order_id: "local-1".to_string(),
            exchange_order_id: Some("venue-1".to_string()),
            post_only: false,
            reduce_only: true,
            order_type: "taker".to_string(),
            attempt: 2,
            placed_ts_ms: 1_789_000_000_100,
            fill_ts_ms: Some(1_789_000_000_250),
            latency_submit_fill_ms: Some(150),
            reference_price: Some(Decimal::new(100_000, 2)),
            limit_price: None,
            best_bid: Some(Decimal::new(99_990, 2)),
            best_ask: Some(Decimal::new(100_010, 2)),
            fill_value: Some(Decimal::new(10_000, 2)),
            fill_price: Some(100.0),
            filled_fee: Some(Decimal::new(5, 2)),
            fee_bps: Some(5.0),
            slippage_bps_vs_decision: Some(1.2),
            slippage_usd_vs_decision: Some(0.12),
            overfill_detected: false,
            underfill_detected: false,
        };

        let value = serde_json::to_value(&record).unwrap();

        assert_eq!(value["event"], "leg_fill");
        assert_eq!(value["variant"], "A");
        assert_eq!(value["pair"], "BTC/ETH");
        assert_eq!(value["phase"], "exit");
        assert_eq!(value["reduce_only"], true);
        assert_eq!(value["order_type"], "taker");
        assert_eq!(value["latency_submit_fill_ms"], 150);
        assert_eq!(value["overfill_detected"], false);
        assert_eq!(value["underfill_detected"], false);
    }
}
