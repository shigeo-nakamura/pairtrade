//! Portfolio risk rails, see `docs/book-runtime.md` §7.
//!
//! - kill switch file: no opening intents (reductions and flattens run)
//! - session drawdown halt: sticky, flatten + block opens, cleared only by a
//!   consumed RISK_ACK file, which re-anchors the session equity
//! - daily loss halt: block opens until the next UTC day, no flatten

use chrono::{TimeZone, Utc};
use serde::Serialize;

use super::config::RiskConfig;
use super::state::BookState;
use crate::directional::Sentinels;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RiskEvent {
    SessionHalt { loss_usd: f64, limit_usd: f64 },
    SessionHaltCleared { new_start_equity: f64 },
    DailyHalt { loss_usd: f64, limit_usd: f64 },
    DailyRollover { date: String, start_equity: f64 },
}

#[derive(Debug, Clone)]
pub struct RiskRails {
    cfg: RiskConfig,
    sentinels: Sentinels,
}

pub fn utc_date(now: i64) -> String {
    Utc.timestamp_opt(now, 0)
        .single()
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

impl RiskRails {
    pub fn new(cfg: RiskConfig) -> Self {
        let sentinels = Sentinels::new(cfg.kill_switch_path.clone(), cfg.risk_ack_path.clone());
        Self { cfg, sentinels }
    }

    pub fn kill_switch_engaged(&self) -> bool {
        self.sentinels.kill_switch_engaged()
    }

    /// Initialise session/daily anchors on a fresh state.
    pub fn ensure_anchors(&self, state: &mut BookState, now: i64, equity: f64) {
        if state.session.start_at == 0 || state.session.start_equity <= 0.0 {
            state.session.start_equity = equity;
            state.session.start_at = now;
        }
        if state.daily.date.is_empty() {
            state.daily.date = utc_date(now);
            state.daily.start_equity = equity;
        }
        if state.peak_equity <= 0.0 {
            state.peak_equity = equity;
        }
    }

    /// Consume a RISK_ACK if present while halted. Returns the clear event.
    pub fn maybe_clear_halt(
        &self,
        state: &mut BookState,
        now: i64,
        equity: f64,
    ) -> Option<RiskEvent> {
        if !state.session.halted {
            // A stray ack must not survive to clear a *future* halt.
            let _ = self.sentinels.take_risk_ack();
            return None;
        }
        if !self.sentinels.take_risk_ack() {
            return None;
        }
        state.session.halted = false;
        state.session.halt_reason = None;
        state.session.halted_at = None;
        state.session.start_equity = equity;
        state.session.start_at = now;
        // The operator has reconciled the book: the daily window is
        // re-anchored too, otherwise the day's loss (which by construction
        // includes the session loss) would keep opens blocked.
        state.daily.date = utc_date(now);
        state.daily.start_equity = equity;
        state.daily.halted = false;
        Some(RiskEvent::SessionHaltCleared {
            new_start_equity: equity,
        })
    }

    /// Roll the daily window and evaluate both loss limits against the
    /// current equity. Returns the events that fired this call.
    pub fn evaluate(&self, state: &mut BookState, now: i64, equity: f64) -> Vec<RiskEvent> {
        let mut events = Vec::new();
        self.ensure_anchors(state, now, equity);
        let today = utc_date(now);
        if state.daily.date != today {
            // Isolate the overnight move -- against equity as of the last
            // observation (yesterday's closing mark), not the far-off
            // morning anchor -- before resetting today's anchor to
            // `equity`. `tick` accrues overnight funding before calling
            // this, so `equity` already reflects it on this first tick of
            // the new date; resetting the anchor to it first would make
            // `daily_loss` compute to zero against itself, silently
            // absorbing the move and never comparing it to the daily
            // limit at all -- not even one tick later, unlike every other
            // loss this rail is meant to catch. Comparing against the
            // *last observed* equity rather than the morning anchor keeps
            // a loss already handled by yesterday's own halt (equity never
            // recovered, nothing moved overnight) from re-triggering every
            // rollover: the daily halt is documented to clear each UTC day
            // regardless of recovery, and only a genuinely new move in the
            // crossover window should carry forward.
            let old_daily_limit = self.cfg.max_daily_loss_bps / 10_000.0 * state.daily.start_equity;
            let overnight_loss = state.last_equity.map_or(0.0, |(_, e)| e) - equity;
            let carried_halt = state.daily.start_equity > 0.0 && overnight_loss > old_daily_limit;
            if carried_halt {
                events.push(RiskEvent::DailyHalt {
                    loss_usd: overnight_loss,
                    limit_usd: old_daily_limit,
                });
            }
            state.daily = super::state::DailyRisk {
                date: today.clone(),
                start_equity: equity,
                halted: carried_halt,
            };
            events.push(RiskEvent::DailyRollover {
                date: today,
                start_equity: equity,
            });
        }
        state.observe_equity(now, equity);

        let session_limit = self.cfg.max_session_loss_bps / 10_000.0 * state.session.start_equity;
        let session_loss = state.session.start_equity - equity;
        if !state.session.halted && session_loss > session_limit {
            state.session.halted = true;
            state.session.halted_at = Some(now);
            state.session.halt_reason = Some(format!(
                "session loss ${session_loss:.2} > limit ${session_limit:.2} ({} bps of ${:.2})",
                self.cfg.max_session_loss_bps, state.session.start_equity
            ));
            events.push(RiskEvent::SessionHalt {
                loss_usd: session_loss,
                limit_usd: session_limit,
            });
        }

        let daily_limit = self.cfg.max_daily_loss_bps / 10_000.0 * state.daily.start_equity;
        let daily_loss = state.daily.start_equity - equity;
        if !state.daily.halted && daily_loss > daily_limit {
            state.daily.halted = true;
            events.push(RiskEvent::DailyHalt {
                loss_usd: daily_loss,
                limit_usd: daily_limit,
            });
        }
        events
    }

    /// Read-only check of the two loss rails against `equity`, without
    /// engaging a halt or touching state. Used mid-plan, after a fill has
    /// moved the accounting, to stop the openings that follow: the tick's
    /// own `evaluate` still owns the actual halt and its flatten.
    pub fn loss_rail_breached(&self, state: &BookState, equity: f64) -> Option<&'static str> {
        if state.session.start_equity > 0.0 {
            let limit = self.cfg.max_session_loss_bps / 10_000.0 * state.session.start_equity;
            if state.session.start_equity - equity > limit {
                return Some("session_loss_limit");
            }
        }
        if state.daily.start_equity > 0.0 {
            let limit = self.cfg.max_daily_loss_bps / 10_000.0 * state.daily.start_equity;
            if state.daily.start_equity - equity > limit {
                return Some("daily_loss_limit");
            }
        }
        None
    }

    /// Whether opening / increasing intents may be sent right now.
    pub fn opens_allowed(&self, state: &BookState) -> bool {
        !state.session.halted && !state.daily.halted && !self.kill_switch_engaged()
    }

    pub fn block_reason(&self, state: &BookState) -> Option<&'static str> {
        if state.session.halted {
            Some("session_halted")
        } else if state.daily.halted {
            Some("daily_halted")
        } else if self.kill_switch_engaged() {
            Some("kill_switch")
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::config::{test_config_yaml, BookConfig};

    fn rails(dir: &std::path::Path) -> RiskRails {
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap().risk;
        cfg.kill_switch_path = dir.join("KILL_SWITCH");
        cfg.risk_ack_path = dir.join("RISK_ACK");
        RiskRails::new(cfg)
    }

    const T0: i64 = 1_788_652_800; // 2026-09-05T00:00:00Z

    #[test]
    fn session_halt_is_sticky_until_ack_and_ack_reanchors() {
        let dir = tempfile::tempdir().unwrap();
        let r = rails(dir.path());
        let mut s = BookState::new("t");
        assert!(r.evaluate(&mut s, T0, 1000.0).is_empty());
        // 5% limit = $50; lose $60
        let ev = r.evaluate(&mut s, T0 + 60, 940.0);
        assert!(matches!(ev[0], RiskEvent::SessionHalt { .. }));
        assert!(s.session.halted);
        assert!(!r.opens_allowed(&s));
        // recovery of equity does not clear it
        r.evaluate(&mut s, T0 + 120, 1000.0);
        assert!(s.session.halted);
        assert!(r.maybe_clear_halt(&mut s, T0 + 130, 1000.0).is_none());
        std::fs::write(dir.path().join("RISK_ACK"), "").unwrap();
        let ev = r.maybe_clear_halt(&mut s, T0 + 140, 990.0).unwrap();
        assert!(matches!(ev, RiskEvent::SessionHaltCleared { .. }));
        assert!(!s.session.halted);
        assert_eq!(s.session.start_equity, 990.0);
        assert!(!dir.path().join("RISK_ACK").exists());
        // The $60 loss also tripped the 3% daily halt; the ack re-anchors
        // the daily window as well, so opens are allowed again.
        assert!(!s.daily.halted);
        assert_eq!(s.daily.start_equity, 990.0);
        assert!(r.opens_allowed(&s));
    }

    #[test]
    fn stray_ack_is_consumed_without_effect() {
        let dir = tempfile::tempdir().unwrap();
        let r = rails(dir.path());
        let mut s = BookState::new("t");
        r.evaluate(&mut s, T0, 1000.0);
        std::fs::write(dir.path().join("RISK_ACK"), "").unwrap();
        assert!(r.maybe_clear_halt(&mut s, T0 + 1, 1000.0).is_none());
        assert!(!dir.path().join("RISK_ACK").exists());
    }

    #[test]
    fn loss_rail_breached_reads_the_limits_without_engaging_a_halt() {
        let dir = tempfile::tempdir().unwrap();
        let r = rails(dir.path());
        let mut s = BookState::new("t");
        r.evaluate(&mut s, T0, 1000.0);
        assert_eq!(r.loss_rail_breached(&s, 1000.0), None);
        // 3% daily on $1000 = $30.
        assert_eq!(r.loss_rail_breached(&s, 965.0), Some("daily_loss_limit"));
        // 5% session = $50; the session rail is reported first.
        assert_eq!(r.loss_rail_breached(&s, 940.0), Some("session_loss_limit"));
        // Nothing was mutated: no halt engaged, no equity observed.
        assert!(!s.session.halted);
        assert!(!s.daily.halted);
        assert_eq!(s.last_equity, Some((T0, 1000.0)));
    }

    #[test]
    fn daily_halt_resets_on_utc_rollover_and_kill_switch_blocks_opens() {
        let dir = tempfile::tempdir().unwrap();
        let r = rails(dir.path());
        let mut s = BookState::new("t");
        r.evaluate(&mut s, T0, 1000.0);
        // 3% daily = $30; lose $35 (but below the 5% session limit)
        let ev = r.evaluate(&mut s, T0 + 3600, 965.0);
        assert!(matches!(ev[0], RiskEvent::DailyHalt { .. }));
        assert!(s.daily.halted);
        assert!(!s.session.halted);
        assert_eq!(r.block_reason(&s), Some("daily_halted"));
        // next UTC day → rollover clears the daily halt and re-anchors
        let ev = r.evaluate(&mut s, T0 + 86_400 + 1, 965.0);
        assert!(matches!(ev[0], RiskEvent::DailyRollover { .. }));
        assert!(!s.daily.halted);
        assert_eq!(s.daily.start_equity, 965.0);
        assert!(r.opens_allowed(&s));
        std::fs::write(dir.path().join("KILL_SWITCH"), "").unwrap();
        assert!(!r.opens_allowed(&s));
        assert_eq!(r.block_reason(&s), Some("kill_switch"));
    }

    #[test]
    fn an_overnight_move_at_rollover_carries_the_daily_halt_into_the_new_day() {
        // `tick` accrues overnight funding before calling `evaluate`, so on
        // the first tick of a new date `equity` already reflects it. That
        // move must be caught against the *old* day's anchor before the
        // rollover below replaces it with `equity` itself (which would
        // make the same-day loss compute to zero and open the new day's
        // decision as if nothing happened).
        let dir = tempfile::tempdir().unwrap();
        let r = rails(dir.path());
        let mut s = BookState::new("t");
        r.evaluate(&mut s, T0, 1000.0);
        // Last tick before midnight: no loss yet.
        let ev = r.evaluate(&mut s, T0 + 86_000, 1000.0);
        assert!(ev.is_empty());
        // First tick of the next day: a $50 overnight funding accrual (3%
        // daily limit on $1000 = $30) has already dropped equity by the
        // time this call happens.
        let ev = r.evaluate(&mut s, T0 + 86_400 + 1, 950.0);
        assert!(matches!(ev[0], RiskEvent::DailyHalt { .. }), "{ev:?}");
        assert!(matches!(ev[1], RiskEvent::DailyRollover { .. }), "{ev:?}");
        assert!(s.daily.halted);
        assert_eq!(s.daily.start_equity, 950.0);
        assert_eq!(r.block_reason(&s), Some("daily_halted"));
    }

    #[test]
    fn a_stale_unrecovered_loss_does_not_re_halt_every_rollover() {
        // The daily halt is documented to clear each UTC day regardless of
        // recovery (see the test above this one): a loss already handled
        // by yesterday's own halt, with equity simply never recovering
        // overnight, must not look like a fresh overnight move and
        // re-trigger the halt on every subsequent rollover.
        let dir = tempfile::tempdir().unwrap();
        let r = rails(dir.path());
        let mut s = BookState::new("t");
        r.evaluate(&mut s, T0, 1000.0);
        let ev = r.evaluate(&mut s, T0 + 3600, 965.0);
        assert!(matches!(ev[0], RiskEvent::DailyHalt { .. }));
        // Two more UTC days pass with no further movement at all.
        for days in 1..=2 {
            let ev = r.evaluate(&mut s, T0 + days * 86_400 + 1, 965.0);
            assert_eq!(ev.len(), 1, "{ev:?}");
            assert!(matches!(ev[0], RiskEvent::DailyRollover { .. }), "{ev:?}");
            assert!(!s.daily.halted);
        }
    }
}
