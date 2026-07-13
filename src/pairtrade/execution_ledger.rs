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
    pub ts_decision_ms: i64,
    pub ts_submit_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_ack_ms: Option<i64>,
    pub leg_symbol: String,
    pub side: String,
    pub target_qty: Decimal,
    pub submitted_qty: Decimal,
    pub filled_qty: Decimal,
    pub remaining_qty: Decimal,
    pub order_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
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
    pub submit_reference_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submit_mid: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submit_bid: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submit_ask: Option<Decimal>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_bps_vs_submit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_usd_vs_submit: Option<f64>,
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

/// bot-strategy#721: one row per (entry, symbol) written by the post-entry
/// venue-position reconciliation. Quantity fields are authoritative:
/// `intended_qty` / `actual_qty` are signed (Long positive, Short negative),
/// `excess_qty` is `|actual| - |intended|` (positive = overfill).
#[derive(Debug, Serialize)]
pub(in crate::pairtrade) struct ExecutionEntryReconcileRecord {
    pub event: &'static str,
    pub ts_ms: i64,
    pub variant: String,
    pub pair: String,
    pub symbol: String,
    pub intended_qty: Decimal,
    pub actual_qty: Decimal,
    pub excess_qty: Decimal,
    pub tolerance: Decimal,
    /// ok | trimmed | trim_failed | excess_below_min_lot | underfill |
    /// sign_flip | fetch_failed
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trim_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trim_qty: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub residual_excess: Option<Decimal>,
    pub entries_blocked: bool,
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

    pub(in crate::pairtrade) fn write_entry_reconcile(
        &mut self,
        record: &ExecutionEntryReconcileRecord,
    ) {
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
    fn serializes_entry_reconcile_record_core_fields() {
        // bot-strategy#721: the reconciliation audit row must carry the
        // signed intended/actual quantities and the action verbatim so the
        // ledger join can separate execution artifacts from strategy PnL.
        let record = ExecutionEntryReconcileRecord {
            event: "entry_reconcile",
            ts_ms: 1_789_000_000_000,
            variant: "A".to_string(),
            pair: "BTC/ETH".to_string(),
            symbol: "ETH".to_string(),
            intended_qty: Decimal::new(-24291, 4),
            actual_qty: Decimal::new(-26016, 4),
            excess_qty: Decimal::new(1725, 4),
            tolerance: Decimal::new(1, 4),
            action: "trimmed".to_string(),
            trim_order_id: Some("trim-1".to_string()),
            trim_qty: Some(Decimal::new(1725, 4)),
            residual_excess: Some(Decimal::ZERO),
            entries_blocked: false,
        };

        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["event"], "entry_reconcile");
        assert_eq!(value["symbol"], "ETH");
        assert_eq!(value["intended_qty"], "-2.4291");
        assert_eq!(value["actual_qty"], "-2.6016");
        assert_eq!(value["excess_qty"], "0.1725");
        assert_eq!(value["action"], "trimmed");
        assert_eq!(value["entries_blocked"], false);
    }

    #[test]
    fn serializes_leg_fill_record_core_fields() {
        let record = ExecutionLegFillRecord {
            event: "leg_fill",
            ts_ms: 1_789_000_000_000,
            variant: "A".to_string(),
            pair: "BTC/ETH".to_string(),
            phase: "exit".to_string(),
            close_reason: Some("force_close".to_string()),
            ts_decision_ms: 1_789_000_000_050,
            ts_submit_ms: 1_789_000_000_100,
            ts_ack_ms: Some(1_789_000_000_130),
            leg_symbol: "BTC".to_string(),
            side: "Long".to_string(),
            target_qty: Decimal::new(100, 3),
            submitted_qty: Decimal::new(100, 3),
            filled_qty: Decimal::new(100, 3),
            remaining_qty: Decimal::ZERO,
            order_id: "local-1".to_string(),
            exchange_order_id: Some("venue-1".to_string()),
            client_order_id: Some("client-1".to_string()),
            post_only: false,
            reduce_only: true,
            order_type: "taker".to_string(),
            attempt: 2,
            placed_ts_ms: 1_789_000_000_100,
            fill_ts_ms: Some(1_789_000_000_250),
            latency_submit_fill_ms: Some(150),
            reference_price: Some(Decimal::new(100_000, 2)),
            submit_reference_price: Some(Decimal::new(100_010, 2)),
            submit_mid: Some(Decimal::new(100_000, 2)),
            limit_price: None,
            submit_bid: Some(Decimal::new(99_995, 2)),
            submit_ask: Some(Decimal::new(100_025, 2)),
            best_bid: Some(Decimal::new(99_990, 2)),
            best_ask: Some(Decimal::new(100_010, 2)),
            fill_value: Some(Decimal::new(10_000, 2)),
            fill_price: Some(100.0),
            filled_fee: Some(Decimal::new(5, 2)),
            fee_bps: Some(5.0),
            slippage_bps_vs_decision: Some(1.2),
            slippage_usd_vs_decision: Some(0.12),
            slippage_bps_vs_submit: Some(1.1),
            slippage_usd_vs_submit: Some(0.11),
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
