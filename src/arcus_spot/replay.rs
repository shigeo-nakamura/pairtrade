use super::{ArcusSpotRuntime, ArcusSpotRuntimeState};
use anyhow::{Context, Result};
use dex_connector::ArcusSpotRecorderSnapshot;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotReplaySummary {
    pub input_records: u64,
    pub emitted_events: u64,
    pub final_state: ArcusSpotRuntimeState,
}

/// Replay recorder JSONL without consulting wall-clock time.
///
/// Each non-empty input line produces exactly one compact event line. Parse
/// errors stop at the first bad record so archives cannot silently diverge.
pub fn replay_jsonl<R: BufRead, W: Write>(
    runtime: &mut ArcusSpotRuntime,
    reader: R,
    mut writer: W,
) -> Result<ArcusSpotReplaySummary> {
    let mut input_records = 0_u64;
    let mut emitted_events = 0_u64;
    let mut last_timestamp = None;
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.with_context(|| format!("failed to read input line {line_number}"))?;
        if line.trim().is_empty() {
            continue;
        }
        input_records = input_records.saturating_add(1);
        let snapshot: ArcusSpotRecorderSnapshot = serde_json::from_str(&line)
            .with_context(|| format!("invalid recorder snapshot at line {line_number}"))?;
        // `step` derives its evaluation clock from `collection_finished_at`
        // alone (see `step`/`step_at`), so an archive with records out of
        // chronological order -- e.g. files concatenated in the wrong order
        // -- would feed an older price into the signal window, let max-hold
        // elapsed time move backward, and make `update_risk_baselines`
        // treat each backward/forward date change as a new day, silently
        // corrupting signals and loss-halt results. Fail loudly instead of
        // replaying a non-monotonic archive.
        if let Some(previous) = last_timestamp {
            if snapshot.collection_finished_at < previous {
                anyhow::bail!(
                    "line {line_number} has collection_finished_at {} before the preceding \
                     record's {previous}; replay archives must be chronologically ordered",
                    snapshot.collection_finished_at
                );
            }
        }
        last_timestamp = Some(snapshot.collection_finished_at);
        let event = runtime.step(&snapshot);
        serde_json::to_writer(&mut writer, &event)
            .with_context(|| format!("failed to serialize event for line {line_number}"))?;
        writer
            .write_all(b"\n")
            .with_context(|| format!("failed to write event for line {line_number}"))?;
        emitted_events = emitted_events.saturating_add(1);
    }
    writer.flush().context("failed to flush replay output")?;
    Ok(ArcusSpotReplaySummary {
        input_records,
        emitted_events,
        final_state: runtime.state().clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arcus_spot::{ArcusSpotInventory, ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode};
    use dex_connector::ArcusSpotPair;
    use rust_decimal::Decimal;
    use std::io::Cursor;

    fn config() -> ArcusSpotRuntimeConfig {
        ArcusSpotRuntimeConfig {
            mode: ArcusSpotRuntimeMode::ReadOnly,
            chain_id: 4663,
            pair: ArcusSpotPair {
                sell_symbol: "NVDA".to_string(),
                buy_symbol: "AMD".to_string(),
            },
            notional_usd: Decimal::from(5),
            initial_inventory: ArcusSpotInventory {
                token_a: Decimal::ONE,
                token_b: Decimal::ONE,
            },
            inventory_floors: ArcusSpotInventory {
                token_a: Decimal::ZERO,
                token_b: Decimal::ZERO,
            },
            max_rotation_fraction: Decimal::ONE,
            signal_window_samples: 3,
            min_signal_samples: 2,
            entry_z_score: 2.0,
            exit_z_score: 0.25,
            max_quote_age_secs: 30,
            max_hold_secs: 3600,
            max_all_in_round_trip_cost_bps: Decimal::from(100),
            gas_buffer_bps: Decimal::ZERO,
            settlement_buffer_bps: Decimal::ZERO,
            max_inventory_imbalance_fraction: Decimal::ONE,
            daily_loss_limit_usd: Decimal::from(100),
            cumulative_loss_limit_usd: Decimal::from(100),
        }
    }

    #[test]
    fn identical_invalid_snapshots_replay_byte_identically() {
        let line = r#"{"schema_version":2,"mode":"public_indicative_read_only","chain_id":4663,"collection_started_at":"2026-07-27T00:00:00Z","collection_finished_at":"2026-07-27T00:00:01Z","indexer_stats":{"status":"error","error":{"stage":"indexer_stats","classification":"http","retryable":false,"message":"x"}},"token_metadata":{"status":"error","error":{"stage":"token_metadata","classification":"http","retryable":false,"message":"x"}},"reference_overview":{"status":"error","error":{"stage":"reference_overview","classification":"http","retryable":false,"message":"x"}},"round_trips":[]}"#;
        let input = format!("{line}\n{line}\n");
        let mut first_runtime = ArcusSpotRuntime::new(config()).unwrap();
        let mut first_output = Vec::new();
        let first = replay_jsonl(
            &mut first_runtime,
            Cursor::new(input.as_bytes()),
            &mut first_output,
        )
        .unwrap();

        let mut second_runtime = ArcusSpotRuntime::new(config()).unwrap();
        let mut second_output = Vec::new();
        let second = replay_jsonl(
            &mut second_runtime,
            Cursor::new(input.as_bytes()),
            &mut second_output,
        )
        .unwrap();

        assert_eq!(first_output, second_output);
        assert_eq!(first, second);
        assert_eq!(first.input_records, 2);
        assert_eq!(first.emitted_events, 2);
    }

    #[test]
    fn non_monotonic_timestamps_are_rejected() {
        let earlier = r#"{"schema_version":2,"mode":"public_indicative_read_only","chain_id":4663,"collection_started_at":"2026-07-27T00:00:00Z","collection_finished_at":"2026-07-27T00:00:01Z","indexer_stats":{"status":"error","error":{"stage":"indexer_stats","classification":"http","retryable":false,"message":"x"}},"token_metadata":{"status":"error","error":{"stage":"token_metadata","classification":"http","retryable":false,"message":"x"}},"reference_overview":{"status":"error","error":{"stage":"reference_overview","classification":"http","retryable":false,"message":"x"}},"round_trips":[]}"#;
        let later = r#"{"schema_version":2,"mode":"public_indicative_read_only","chain_id":4663,"collection_started_at":"2026-07-27T00:01:00Z","collection_finished_at":"2026-07-27T00:01:01Z","indexer_stats":{"status":"error","error":{"stage":"indexer_stats","classification":"http","retryable":false,"message":"x"}},"token_metadata":{"status":"error","error":{"stage":"token_metadata","classification":"http","retryable":false,"message":"x"}},"reference_overview":{"status":"error","error":{"stage":"reference_overview","classification":"http","retryable":false,"message":"x"}},"round_trips":[]}"#;
        let input = format!("{later}\n{earlier}\n");
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let mut output = Vec::new();
        let error =
            replay_jsonl(&mut runtime, Cursor::new(input.as_bytes()), &mut output).unwrap_err();
        assert!(
            error.to_string().contains("chronologically ordered"),
            "unexpected error: {error}"
        );
    }
}
