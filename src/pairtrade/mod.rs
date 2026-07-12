use dex_connector::PositionSnapshot;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use std::time::Instant;
use tokio::time::Duration;

mod backtest;
mod bar;
mod config;
mod data_dump;
mod defaults;
mod engine;
mod entry;
mod execution_ledger;
mod exit;
mod funding_history;
mod history_io;
mod instance;
mod kalman;
mod market;
mod order_pricing;
mod pair_eval;
mod pnl_log;
pub(crate) mod prom;
mod regime;
mod rehedge;
mod risk_io;
mod s3_mirror;
mod sentinel;
mod sizing;
mod state;
mod stats;
mod status;
mod util;
// Re-exported at the pairtrade module root so existing `super::super::`
// references in engine submodules resolve unchanged (bot-strategy#502).
pub use config::{DailyLossAction, PairTradeConfig, WarmStartMode};
pub use engine::PairTradeEngine;
pub(in crate::pairtrade) use instance::{StrategyInstance, EQUITY_REFRESH_CACHE_SECS};
use market::SymbolSnapshot;
use pnl_log::{PnlLogRecord, PnlTradeDetails};
pub(in crate::pairtrade) use sentinel::{kill_switch_path, risk_ack_path};

/// Spawn the Prometheus exporter when `PROM_LISTEN` is set in the env.
/// Safe to call at most once at process boot from `main`. See
/// `pairtrade::prom`.
pub fn start_metrics_exporter() {
    prom::maybe_start_exporter();
}
use config::PairParams;

use state::{PairSharedState, PairState, PositionDirection};

use util::tail_std;

/// Apply the post-exit state transition: clear position, stamp
/// last_exit_{at,ts}, and — if the exit reason stashed by `exit_reason()`
/// is `stop_loss_z` — set `last_stop_loss_at` so the per-direction
/// post-stop cool-down (`stop_loss_cooldown_secs`) blocks an immediate
/// same-direction re-entry. The tag is dropped after consumption so a
/// later non-stop exit (force_close / exit_z) does not refresh it.
/// bot-strategy#316.
pub(in crate::pairtrade) fn apply_post_exit_state(
    state: &mut PairState,
    shared: Option<&PairSharedState>,
    direction: PositionDirection,
    now_ts: i64,
    variant: &str,
    pair: &str,
) {
    state.position = None;
    state.recovery_recorded = false;
    // The ineligible-close deferral window (bot-strategy#531) is anchored to
    // the position it is obligated to flatten: it must survive planning-time
    // fire attempts (a failed placement would otherwise restart the cap from
    // scratch, PR #166 Codex review) and is released only here, when the
    // position is actually gone.
    state.ineligible_defer_since_ts = None;
    state.last_exit_at = Some(Instant::now());
    state.last_exit_ts = Some(now_ts);
    // Capture the z observed at exit before the post-take pending_exit_reason
    // is consumed, so the Grafana panel can correlate close reason with the
    // z that triggered it. bot-strategy#409.
    if let Some((z, _)) = shared.and_then(|s| s.z_score()) {
        prom::LAST_EXIT_Z.with_label_values(&[variant, pair]).set(z);
    }
    let reason = state.pending_exit_reason.take().unwrap_or("unknown");
    prom::CLOSE_REASON_TOTAL
        .with_label_values(&[variant, pair, reason])
        .inc();
    if reason == "stop_loss_z" {
        state.last_stop_loss_at = Some((direction, now_ts));
        log::info!(
            "[STOP_COOLDOWN] armed direction={:?} now_ts={}",
            direction,
            now_ts
        );
    }
}

impl PairTradeEngine {
    /// Whether every strategy instance is currently flat. For single-instance
    /// deployments this is exactly today's `self.open_positions.is_empty()`
    /// check (golden-test stable). For multi-instance deployments this also
    /// requires every per-pair `state.position` across every instance to be
    /// `None`, so SIGTERM waits for all A/B/C variants to flatten before
    /// exiting. commit 5 of shigeo-nakamura/bot-strategy#25.
    fn all_instances_flat(&self) -> bool {
        if self.instances.len() <= 1 {
            return self.open_positions.is_empty();
        }
        self.open_positions.is_empty()
            && self
                .instances
                .iter()
                .all(|inst| inst.states.values().all(|s| s.position.is_none()))
    }

    /// Total open-position count surfaced in shutdown log lines. Mirrors
    /// `all_instances_flat`'s split: single-instance returns today's count,
    /// multi-instance sums per-pair `state.position` presence across all
    /// instances so the log reflects everything graceful shutdown is
    /// waiting on.
    fn total_open_positions(&self) -> usize {
        if self.instances.len() <= 1 {
            return self.open_positions.len();
        }
        let from_states: usize = self
            .instances
            .iter()
            .map(|inst| {
                inst.states
                    .values()
                    .filter(|s| s.position.is_some())
                    .count()
            })
            .sum();
        from_states.max(self.open_positions.len())
    }

    /// Return the per-instance `PairParams` for a pair key, falling back to
    /// the instance's `default_pair_params` when the pair has no override.
    /// Use this inside any per-instance phase in place of
    /// `self.cfg.params_for(key)` so each variant sees its own
    /// `exit_z` / `stop_loss_z` / `max_loss_r_mult`.
    fn pair_params_for(&self, inst_idx: usize, key: &str) -> &PairParams {
        let inst = &self.instances[inst_idx];
        inst.pair_params
            .get(key)
            .unwrap_or(&inst.default_pair_params)
    }

    fn write_pnl_record(&mut self, inst_idx: usize, record: PnlLogRecord) {
        // Update trade stats
        self.instances[inst_idx].total_trades += 1;
        self.instances[inst_idx].total_pnl += record.pnl;
        if record.pnl > 0.0 {
            self.instances[inst_idx].total_wins += 1;
        }
        if self.instances[inst_idx].total_pnl > self.instances[inst_idx].peak_pnl {
            self.instances[inst_idx].peak_pnl = self.instances[inst_idx].total_pnl;
        }
        let dd = self.instances[inst_idx].peak_pnl - self.instances[inst_idx].total_pnl;
        if dd > self.instances[inst_idx].max_dd {
            self.instances[inst_idx].max_dd = dd;
        }

        // Update status reporter
        let inst = &mut self.instances[inst_idx];
        if let Some(reporter) = &mut inst.status_reporter {
            reporter.set_trade_stats_totals(
                inst.total_trades,
                inst.total_wins,
                inst.total_pnl,
                inst.max_dd,
            );
        }

        if let Some(logger) = &mut self.instances[inst_idx].pnl_logger {
            if let Err(err) = logger.log(record) {
                log::warn!("[PNL] failed to write pnl log: {:?}", err);
            }
        }
    }

    fn write_pnl_context_record(&mut self, inst_idx: usize, record: PnlLogRecord) {
        if let Some(logger) = &mut self.instances[inst_idx].pnl_logger {
            if let Err(err) = logger.log(record) {
                log::warn!("[PNL] failed to write context pnl log: {:?}", err);
            }
        }
    }

    fn write_recovery_no_pnl_record(
        &mut self,
        inst_idx: usize,
        key: &str,
        fallback_direction: PositionDirection,
        recovery_reason: &str,
        now_ts: i64,
        price_map: &HashMap<String, SymbolSnapshot>,
    ) {
        let Some((base, quote)) = key.split_once('/') else {
            return;
        };

        let inst = &self.instances[inst_idx];
        let state = inst.states.get(key);
        let pos = state.and_then(|s| s.position.as_ref());
        let direction = pos.map(|p| p.direction).unwrap_or(fallback_direction);
        let close_reason = state.and_then(|s| s.pending_exit_reason);
        let z_exit = self
            .per_pair_state
            .get(key)
            .and_then(|s| s.z_score().map(|(z, _)| z));
        let beta = pos
            .and_then(|p| p.entry_beta)
            .or_else(|| self.per_pair_state.get(key).map(|s| s.beta));
        let entry_a = pos.and_then(|p| p.entry_price_a.and_then(|v| v.to_f64()));
        let entry_b = pos.and_then(|p| p.entry_price_b.and_then(|v| v.to_f64()));
        let z_entry = pos.and_then(|p| p.entry_z);
        let hold_secs = pos.map(|p| now_ts.saturating_sub(p.entered_ts).max(0) as f64);
        let exit_a = price_map.get(base).and_then(|p| p.price.to_f64());
        let exit_b = price_map.get(quote).and_then(|p| p.price.to_f64());

        let record = PnlLogRecord::new(base, quote, direction, 0.0, now_ts, "recovery_no_pnl")
            .with_unavailable_pnl()
            .with_recovery_context(close_reason, recovery_reason)
            .with_trade_details(PnlTradeDetails {
                entry_a,
                entry_b,
                exit_a,
                exit_b,
                beta,
                z_entry,
                z_exit,
                hold_secs,
            });

        self.write_pnl_context_record(inst_idx, record);
    }

    fn latest_log_price(&self, symbol: &str) -> Option<f64> {
        self.history
            .get(symbol)
            .and_then(|h| h.back())
            .map(|p| p.log_price)
    }

    fn clear_stale_pending(&mut self, inst_idx: usize, max_age: Duration, reason: &str) {
        let now_ts = self.current_now_ts();
        let mut stale: Vec<String> = Vec::new();
        for (key, state) in self.instances[inst_idx].states.iter() {
            let entry_age = state.pending_entry.as_ref().map(|p| p.placed_at.elapsed());
            let exit_age = state.pending_exit.as_ref().map(|p| p.placed_at.elapsed());
            let age = match (entry_age, exit_age) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            if let Some(age) = age {
                if age >= max_age {
                    log::warn!(
                        "[POSITION] {} pending cleared (reason={}, age={}s)",
                        key,
                        reason,
                        age.as_secs()
                    );
                    stale.push(key.clone());
                }
            }
        }
        for key in stale {
            // bot-strategy#514: dropping a position here loses the close
            // context — record it first. If the exchange still holds the
            // legs, the next snapshot sync rebuilds the position, so this
            // record can occasionally describe a clear that was not a real
            // close; attribution treats recovery_no_pnl as context-only.
            let record_direction = self.instances[inst_idx].states.get(&key).and_then(|s| {
                s.position
                    .as_ref()
                    .filter(|_| !s.recovery_recorded)
                    .map(|p| p.direction)
            });
            if let Some(direction) = record_direction {
                let recovery_reason = format!("stale_pending_{}", reason);
                let no_prices: HashMap<String, SymbolSnapshot> = HashMap::new();
                self.write_recovery_no_pnl_record(
                    inst_idx,
                    &key,
                    direction,
                    &recovery_reason,
                    now_ts,
                    &no_prices,
                );
            }
            if let Some(state) = self.instances[inst_idx].states.get_mut(&key) {
                state.pending_entry = None;
                state.pending_exit = None;
                state.position = None;
                state.recovery_recorded = false;
                state.position_guard = false;
                state.last_exit_at = Some(Instant::now());
                state.last_exit_ts = Some(now_ts);
            }
        }
    }

    fn compute_vol_median(&self) -> f64 {
        let tail_len = self.entry_vol_window();
        let mut vols: Vec<f64> = self
            .per_pair_state
            .values()
            .filter_map(|s| tail_std(&s.spread_history, tail_len))
            .collect();
        if vols.is_empty() {
            return 1.0;
        }
        vols.sort_by(|a, b| a.partial_cmp(b).unwrap());
        vols[vols.len() / 2].max(1e-9)
    }

    fn maybe_log_metrics(&mut self, inst_idx: usize) {
        // Refresh Prometheus gauges every tick. Cheap (a handful of
        // atomic stores per pair) and gives Grafana the same resolution as
        // the underlying state instead of the 300 s log cadence. See
        // bot-strategy#409.
        self.update_prom_metrics(inst_idx);

        const LOG_INTERVAL: u64 = 300;
        if self
            .last_metrics_log
            .map(|t| t.elapsed() < Duration::from_secs(LOG_INTERVAL))
            .unwrap_or(false)
        {
            return;
        }
        let mut lines = Vec::new();
        for k in self.instances[inst_idx].states.keys() {
            let Some(shared) = self.per_pair_state.get(k) else {
                continue;
            };
            let z = shared.z_score().map(|(z, _)| z).unwrap_or(0.0);
            lines.push(format!(
                "{} elig={} z={:.2} beta={:.2} hl={:.2}h p={:.3}",
                k, shared.eligible, z, shared.beta, shared.half_life_hours, shared.adf_p_value
            ));
        }
        lines.sort();
        if !lines.is_empty() {
            log::info!("[METRICS] {}", lines.join(" | "));
        }
        self.last_metrics_log = Some(Instant::now());
    }

    fn state_score(&self, _inst_idx: usize, key: &str) -> f64 {
        self.per_pair_state
            .get(key)
            .map(|s| s.p_value_weighted_score)
            .unwrap_or(0.0)
    }

    fn should_log_position_warn(&self, key: &str) -> bool {
        const WARN_INTERVAL: u64 = 300;
        self.last_position_warn
            .get(key)
            .map(|t| t.elapsed() >= Duration::from_secs(WARN_INTERVAL))
            .unwrap_or(true)
    }

    fn is_dust_position(
        &self,
        snapshot: &PositionSnapshot,
        prices: &HashMap<String, SymbolSnapshot>,
    ) -> bool {
        let Some(symbol_snapshot) = prices.get(&snapshot.symbol) else {
            return false;
        };
        let Some(min_order) = symbol_snapshot.min_order else {
            return false;
        };
        snapshot.size < min_order
    }

    fn entry_vol_window(&self) -> usize {
        ((self.cfg.default_pair_params.entry_vol_lookback_hours * 3600)
            / self.cfg.trading_period_secs)
            .max(1) as usize
    }

    /// Virtual clock used by all duration-based decisions. In live mode this
    /// is the wall-clock UTC second; in backtest mode it tracks the replay
    /// connector's logical timestamp so cooldown / force_close /
    /// circuit_breaker / re-eval intervals fire correctly under replay.
    fn current_now_ts(&self) -> i64 {
        if self.cfg.backtest_mode {
            self.replay_connector
                .as_ref()
                .and_then(|r| r.current_timestamp_secs())
                .unwrap_or_else(|| chrono::Utc::now().timestamp())
        } else {
            chrono::Utc::now().timestamp()
        }
    }

    fn max_history_len(&self) -> usize {
        let mut max_needed = 0usize;
        // Consider all per-pair params and the default
        let all_params =
            std::iter::once(&self.cfg.default_pair_params).chain(self.cfg.pair_params.values());
        for pp in all_params {
            let max_hrs = pp.lookback_hours_long.max(pp.lookback_hours_short);
            let needed = (max_hrs * 3600 / self.cfg.trading_period_secs) as usize;
            let vol_needed = ((pp.entry_vol_lookback_hours * 3600) / self.cfg.trading_period_secs)
                .max(1) as usize;
            max_needed = max_needed.max(needed).max(vol_needed);
        }
        max_needed.max(self.cfg.metrics_window)
    }
}

#[cfg(test)]
#[path = "testing/halt_gate_tests.rs"]
mod halt_gate_tests;
#[cfg(test)]
#[path = "testing/hyperliquid_maker_tests.rs"]
mod hyperliquid_maker_tests;
#[cfg(test)]
#[path = "testing/pending_tests.rs"]
mod pending_tests;
#[cfg(test)]
#[path = "testing/shutdown_grace_tests.rs"]
mod shutdown_grace_tests;
#[cfg(test)]
mod testing;
#[cfg(test)]
#[path = "testing/tests.rs"]
mod tests;
