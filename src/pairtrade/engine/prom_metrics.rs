//! Prometheus gauge assembly for `PairTradeEngine`.
//!
//! Pure relocation from `pairtrade::mod` for bot-strategy#502. The engine
//! still records the same metrics on the same tick cadence; this module only
//! keeps the per-pair/per-instance metric plumbing out of the core engine type.

use super::super::prom;
use super::super::PairTradeEngine;

impl PairTradeEngine {
    /// Push per-instance signal / position / risk state into the
    /// Prometheus registry. Bot-strategy#409 — meant for at-a-glance
    /// "how close are we to entry?" and "is anything blocking entry?"
    /// reading, not a byte-exact predicate.
    pub(in crate::pairtrade) fn update_prom_metrics(&self, inst_idx: usize) {
        let inst = &self.instances[inst_idx];
        let instance = inst.id.as_str();
        let now_ts = chrono::Utc::now().timestamp();
        // --- per-pair gauges ---
        for (key, state) in &inst.states {
            let pp = inst
                .pair_params
                .get(key)
                .unwrap_or(&inst.default_pair_params);
            let shared = self.per_pair_state.get(key);
            let z = shared
                .and_then(|s| s.z_score().map(|(z, _)| z))
                .unwrap_or(0.0);
            let labels = [instance, key.as_str()];
            prom::Z.with_label_values(&labels).set(z);
            prom::BETA
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta).unwrap_or(1.0));
            prom::BETA_S
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta_short).unwrap_or(1.0));
            prom::BETA_L
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta_long).unwrap_or(1.0));
            prom::BETA_DIVERGENCE.with_label_values(&labels).set(
                shared
                    .map(|s| (s.beta_short - s.beta_long).abs())
                    .unwrap_or(0.0),
            );
            prom::BETA_GAP_RELATIVE
                .with_label_values(&labels)
                .set(shared.map(|s| s.beta_gap).unwrap_or(0.0));
            prom::BETA_UNCERTAINTY.with_label_values(&labels).set(
                shared
                    .and_then(|s| s.kalman.as_ref().map(|k| k.posterior_std()))
                    .unwrap_or(0.0),
            );
            // bot-strategy#494 Phase 1 — persistent-regime detector shadow
            // gauges (pair-level state; identical across variant series).
            prom::REGIME_ACTIVE.with_label_values(&labels).set(
                if shared.map(|s| s.regime.is_active()).unwrap_or(false) {
                    1
                } else {
                    0
                },
            );
            prom::REGIME_CUSUM
                .with_label_values(&labels)
                .set(shared.map(|s| s.regime.cusum()).unwrap_or(0.0));
            prom::REGIME_RESIDUAL_SCALE
                .with_label_values(&labels)
                .set(shared.map(|s| s.regime.residual_scale()).unwrap_or(0.0));
            prom::REGIME_INNOVATION_NORMALIZED
                .with_label_values(&labels)
                .set(shared.map(|s| s.regime.last_normalized()).unwrap_or(0.0));
            prom::HALF_LIFE_HOURS
                .with_label_values(&labels)
                .set(shared.map(|s| s.half_life_hours).unwrap_or(0.0));
            prom::ADF_PVALUE
                .with_label_values(&labels)
                .set(shared.map(|s| s.adf_p_value).unwrap_or(1.0));
            prom::ELIGIBLE.with_label_values(&labels).set(
                if shared.map(|s| s.eligible).unwrap_or(false) {
                    1
                } else {
                    0
                },
            );
            prom::EXIT_ELIGIBLE.with_label_values(&labels).set(
                if shared.map(|s| s.exit_eligible()).unwrap_or(false) {
                    1
                } else {
                    0
                },
            );
            prom::ELIGIBILITY_MARGIN_GRACE_ACTIVE
                .with_label_values(&labels)
                .set(
                    if shared
                        .and_then(|s| s.eligibility_margin_grace_until_ts)
                        .is_some()
                    {
                        1
                    } else {
                        0
                    },
                );
            let mut effective = state.z_entry;
            if pp.beta_gap_entry_z_scale > 0.0 {
                effective *=
                    1.0 + pp.beta_gap_entry_z_scale * shared.map(|s| s.beta_gap).unwrap_or(0.0);
            }
            prom::ENTRY_Z_THRESHOLD_EFFECTIVE
                .with_label_values(&labels)
                .set(effective);
            if let Some(pos) = state.position.as_ref() {
                prom::HAS_POSITION.with_label_values(&labels).set(1);
                prom::POSITION_AGE_SECONDS
                    .with_label_values(&labels)
                    .set((now_ts - pos.entered_ts).max(0) as f64);
                if let Some(ez) = pos.entry_z {
                    prom::LAST_ENTRY_Z.with_label_values(&labels).set(ez);
                }
            } else {
                prom::HAS_POSITION.with_label_values(&labels).set(0);
                prom::POSITION_AGE_SECONDS
                    .with_label_values(&labels)
                    .set(0.0);
            }
            let since_exit = match state.last_exit_ts {
                Some(ts) => (now_ts - ts).max(0) as f64,
                None => -1.0,
            };
            prom::TIME_SINCE_LAST_TRADE_SECONDS
                .with_label_values(&labels)
                .set(since_exit);
        }
        // --- per-instance scalars ---
        prom::KILL_SWITCH_ACTIVE
            .with_label_values(&[instance])
            .set(if self.kill_switch_active { 1 } else { 0 });
        prom::SESSION_DD_HALT_ACTIVE
            .with_label_values(&[instance])
            .set(if inst.session_halted { 1 } else { 0 });
        prom::DAILY_DD_HALT_ACTIVE
            .with_label_values(&[instance])
            .set(if inst.daily_loss_halted { 1 } else { 0 });
        let cb_active = match inst.circuit_breaker_until_ts {
            Some(until) => until > now_ts,
            None => false,
        };
        prom::CIRCUIT_BREAKER_ACTIVE
            .with_label_values(&[instance])
            .set(if cb_active { 1 } else { 0 });
        prom::EQUITY_REFERENCE_USD
            .with_label_values(&[instance])
            .set(inst.equity_reference_usd);
        prom::MAX_LEVERAGE_CONFIG
            .with_label_values(&[instance])
            .set(inst.max_leverage);
        // Snapshot age — mtime of the on-disk history file. Bounded I/O
        // (single stat per tick per instance) is acceptable here; the
        // alternative is plumbing the writer's last-write timestamp
        // out through several layers.
        if let Ok(meta) = std::fs::metadata(&self.history_path) {
            if let Ok(modified) = meta.modified() {
                if let Ok(elapsed) = modified.elapsed() {
                    prom::SNAPSHOT_AGE_SECONDS
                        .with_label_values(&[instance])
                        .set(elapsed.as_secs_f64());
                }
            }
        }
    }
}
