//! Per-tick entry-gate setup for the pairtrade engine.
//!
//! Extracted from `engine/step.rs` (bot-strategy#444): this module owns
//! `StepSetup` and `step_setup`, which compute the per-instance entry
//! gates (maintenance window, session-DD refresh, regime filter, open
//! position / pending order overlap) consumed by the planning pass.
//! Behaviour is unchanged from the prior in-`step.rs` implementation.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;

use super::super::market::SymbolSnapshot;
use super::super::regime;
use super::super::PairTradeEngine;

#[derive(Clone, Copy)]
pub(super) struct StepSetup {
    pub(super) maintenance_block_entries: bool,
    pub(super) vol_median: f64,
    pub(super) regime_ok: bool,
    pub(super) positions_clear: bool,
}

impl PairTradeEngine {
    pub(super) async fn step_setup(
        &mut self,
        inst_idx: usize,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) -> Result<StepSetup> {
        // Skip new entries if maintenance is upcoming within 1 hour
        let maintenance_status = self.connector.maintenance_status(1).await;
        let maintenance_block_entries = maintenance_status.is_some();
        if let Some(status) = maintenance_status.as_deref() {
            log::info!(
                "Maintenance/degraded exchange detected ({}); blocking new entries this cycle",
                status
            );
        }
        if let Some(reporter) = &mut self.instances[inst_idx].status_reporter {
            reporter.set_maintenance(maintenance_status.clone());
        }
        crate::pairtrade::prom::MAINTENANCE_ACTIVE
            .with_label_values(&[self.instances[inst_idx].id.as_str()])
            .set(if maintenance_block_entries { 1 } else { 0 });
        // Also stop inflating warn/error counters for the duration of the
        // detected maintenance window (bot-strategy#199). The WS reconnect
        // bursts / 503s / stale-price WARNs that follow are expected fallout
        // and — in addition to being filtered out workflow-side in
        // error-watch.yml — should not accumulate into error_summary.
        // Process-global flag: all A/B/C instances share the same Lighter
        // connector, so the last writer in this tick determines the state
        // and every instance observes the same maintenance verdict.
        crate::error_counter::set_counting_suppressed(maintenance_block_entries);

        self.refresh_equity_if_needed(inst_idx).await?;
        // bot-strategy#575 ①: detect a deposit / withdrawal (flat + settled
        // equity jump) and rebaseline the rolling peak to current equity
        // before sampling, so a top-up into a halted variant restores its DD
        // headroom instead of leaving it pinned under a sticky 30-day peak.
        self.detect_capital_event_and_rebaseline(inst_idx);
        // Phase 3-1: sample current equity into the rolling-peak window
        // and check the session-DD threshold. On breach, this flattens
        // the instance's positions and sets `session_halted=true`; the
        // entry gate below picks up the halt.
        self.update_equity_sample(inst_idx);
        self.evaluate_session_dd(inst_idx).await;
        self.sync_positions_from_exchange(inst_idx, price_map)
            .await?;

        let vol_median = self.compute_vol_median();

        // Regime filter: compute once per step cycle (not per pair)
        let regime_state = if self.cfg.regime_vol_max > 0.0 || self.cfg.regime_trend_max > 0.0 {
            self.history
                .get(&self.cfg.regime_reference_symbol)
                .and_then(|h| {
                    regime::compute_regime(
                        h,
                        self.cfg.regime_vol_window,
                        self.cfg.regime_trend_window,
                    )
                })
        } else {
            None
        };
        let regime_ok = regime::regime_allows_entry(
            regime_state,
            self.cfg.regime_vol_max,
            self.cfg.regime_trend_max,
        );
        if let Some(rs) = regime_state {
            if !regime_ok {
                log::info!(
                    "[REGIME] entry blocked: vol={:.6} (max={:.6}) trend={:.4} (max={:.4}) ref={}",
                    rs.realized_vol,
                    self.cfg.regime_vol_max,
                    rs.trend_strength,
                    self.cfg.regime_trend_max,
                    self.cfg.regime_reference_symbol,
                );
            }
        }

        let positions_clear = self.open_positions.is_empty();
        let has_pending_orders = self.instances[inst_idx]
            .states
            .values()
            .any(|state| state.pending_entry.is_some() || state.pending_exit.is_some());
        if !positions_clear && !has_pending_orders && self.should_log_position_warn("entry_block") {
            log::info!(
                "[POSITION] open positions detected ({} symbols) with no pending orders; blocking new entries",
                self.open_positions.len()
            );
            self.last_position_warn
                .insert("entry_block".to_string(), Instant::now());
        }
        Ok(StepSetup {
            maintenance_block_entries,
            vol_median,
            regime_ok,
            positions_clear,
        })
    }
}
