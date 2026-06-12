//! Per-instance status snapshot emission.

use super::super::PairTradeEngine;

impl PairTradeEngine {
    pub(super) fn step_write_status_snapshot(&mut self, inst_idx: usize) {
        {
            let risk = self.daily_risk_snapshot(inst_idx);
            let session_risk = self.session_risk_snapshot(inst_idx);
            let circuit_breaker = self.circuit_breaker_snapshot(inst_idx);
            let kill_switch_active = self.kill_switch_active;
            let funding_today = self.instances[inst_idx].funding_carry_today;
            if let Some(reporter) = &mut self.instances[inst_idx].status_reporter {
                reporter.set_daily_risk(risk);
                reporter.set_session_risk(session_risk);
                reporter.set_circuit_breaker(circuit_breaker);
                reporter.set_kill_switch(kill_switch_active);
                reporter.set_funding_today(funding_today);
                if let Err(err) =
                    reporter.write_snapshot_if_due(&self.open_positions, self.positions_ready)
                {
                    log::warn!("[STATUS] failed to write status: {:?}", err);
                }
            }
        }
    }
}
