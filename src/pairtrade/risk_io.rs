//! On-disk persistence for runtime risk state (circuit breaker counters
//! and cool-down deadlines). Without this, a restart/crash during an
//! active cool-down silently clears the counter and lets the bot re-enter
//! immediately after N consecutive losses. See bot-strategy#185 Phase 1-3.
//!
//! File lives next to `history_file` (typically `/opt/debot/`) and is
//! written atomically via tmpfile + rename, same pattern as `history_io`.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::state::PositionDirection;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(super) struct InstanceRiskState {
    #[serde(default)]
    pub consecutive_losses: u32,
    #[serde(default)]
    pub circuit_breaker_until_ts: Option<i64>,
    /// Per-pair last stop_loss_z exit (direction + timestamp) so the
    /// post-stop cool-down (`stop_loss_cooldown_secs`, bot-strategy#316)
    /// survives restart. Key = pair key (e.g. "BTC/ETH"). Stale entries
    /// (older than the cool-down window) are harmless — `should_enter`
    /// computes elapsed at check time.
    #[serde(default)]
    pub last_stop_loss_per_pair: HashMap<String, StopLossMark>,
    /// Instance equity at the start of the current UTC session, captured
    /// on rollover. Denominator for `max_daily_loss_bps`. Zero until the
    /// first session reset runs (which the engine always performs on
    /// load when the persisted `session_start_ts` is in a prior day).
    /// bot-strategy#185 Phase 2.
    #[serde(default)]
    pub session_start_equity: f64,
    /// UNIX-seconds timestamp of the current session's rollover. Used to
    /// detect day-boundary crossings; compared against `now_ts` via
    /// `session_day()` which buckets by `daily_reset_utc_hour`.
    #[serde(default)]
    pub session_start_ts: i64,
    /// Running sum of realized PnL for the current session (closed trades
    /// only; unrealized mark-to-market is intentionally excluded so the
    /// threshold is deterministic and independent of /account cadence).
    #[serde(default)]
    pub realized_pnl_today: f64,
    /// Running sum of `funding_carry_usd` from cycles closed during the
    /// current UTC session (bot-strategy#371). Mirrors `realized_pnl_today`
    /// in cadence: incremented at exit_fill / exit_dry_run, zeroed on the
    /// session rollover that also resets `realized_pnl_today`. Surfaced
    /// in status.json so the dashboard can show today's funding next to
    /// `pnl_today` for at-a-glance attribution.
    #[serde(default)]
    pub funding_carry_today: f64,
    /// Periodic equity samples used to compute the rolling peak for
    /// `max_session_loss_bps` (Phase 3-1). One sample per
    /// `session_dd_sample_secs`; entries older than
    /// `session_dd_lookback_secs` are pruned on every update so the
    /// vec stays bounded (≤ lookback / sample ≈ 720 entries at the
    /// default 30 d / 1 h cadence).
    #[serde(default)]
    pub equity_samples: Vec<EquitySample>,
    /// Last equity reading captured while the instance was continuously
    /// flat and settled, used as the reference for deposit / withdrawal
    /// (capital-event) detection. 0.0 = unset (re-seeded on the next
    /// settled flat tick). Persisted so a deposit made while the bot was
    /// stopped is caught on the next boot instead of being silently folded
    /// into the rolling peak. bot-strategy#575 ①.
    #[serde(default)]
    pub capital_baseline_equity: f64,
    /// Sticky halt flag set when the rolling-peak DD threshold trips.
    /// Persists across restarts so a crash inside the cooling-off
    /// window does not silently re-arm the bot. Cleared only by the
    /// manual-ack sentinel (default `/opt/debot/RISK_ACK`, overridable
    /// via `RISK_ACK_PATH`) — there is no auto-resume.
    #[serde(default)]
    pub session_halted: bool,
    /// Free-form tag identifying which guard tripped the halt
    /// (e.g. `"session_dd_500bps"`). Surfaced in logs and status.json
    /// so an operator can tell at a glance why the bot stopped.
    #[serde(default)]
    pub session_halt_reason: Option<String>,
    /// UNIX-seconds timestamp at which the halt engaged. Used purely
    /// for human inspection; the gate itself only consults
    /// `session_halted`.
    #[serde(default)]
    pub session_halt_ts: Option<i64>,
    /// Lifetime trade-stats counters surfaced on the dashboard. Persisted
    /// here so `total_trades` / `total_wins` survive restart and stay in
    /// scope with `consecutive_losses` (otherwise the dashboard can show
    /// `consecutive_losses > total_trades` after a restart). bot-strategy#320.
    #[serde(default)]
    pub total_trades: u64,
    #[serde(default)]
    pub total_wins: u64,
    #[serde(default)]
    pub total_pnl: f64,
    #[serde(default)]
    pub peak_pnl: f64,
    #[serde(default)]
    pub max_dd: f64,
}

impl InstanceRiskState {
    /// Clear fields that should not survive a Round N → N+1 transition.
    /// Session-rolling fields (`session_start_*`, `realized_pnl_today`,
    /// `funding_carry_today`) are deliberately not reset — those have
    /// their own daily-rolling lifecycle. bot-strategy#354.
    pub(super) fn reset_round_bound(&mut self) {
        self.consecutive_losses = 0;
        self.circuit_breaker_until_ts = None;
        self.last_stop_loss_per_pair.clear();
        self.equity_samples.clear();
        self.capital_baseline_equity = 0.0;
        self.session_halted = false;
        self.session_halt_reason = None;
        self.session_halt_ts = None;
        self.total_trades = 0;
        self.total_wins = 0;
        self.total_pnl = 0.0;
        self.peak_pnl = 0.0;
        self.max_dd = 0.0;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub(super) struct EquitySample {
    pub ts: i64,
    pub equity: f64,
}

/// Persisted record of the most recent stop_loss_z exit for a single pair.
/// Drives the post-stop cool-down restore on engine startup. bot-strategy#316.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub(super) struct StopLossMark {
    pub direction: PositionDirection,
    pub ts: i64,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub(super) struct RiskStateSnapshot {
    #[serde(rename = "_v")]
    pub version: u32,
    /// Round identifier from the YAML config at the time this snapshot was
    /// written. On startup the engine compares this against the configured
    /// `round_id` and, on transition, resets round-bound per-instance fields
    /// (trade stats, equity samples, stop-loss cool-down anchors, session
    /// halt). bot-strategy#354.
    #[serde(default)]
    pub round_id: Option<String>,
    #[serde(default)]
    pub instances: HashMap<String, InstanceRiskState>,
}

impl RiskStateSnapshot {
    /// If a Round N → N+1 transition is implied by the configured vs
    /// persisted `round_id` (both `Some` and different), clear round-bound
    /// fields on every persisted instance and return `true`. Returns
    /// `false` (no-op) in every other case — including initial opt-in
    /// (`persisted = None`, `configured = Some`) and operator backing out
    /// the field (`configured = None`). For the initial opt-in path the
    /// operator must run `scripts/reset-round-state.sh` once. bot-strategy#354.
    pub(super) fn apply_round_transition(&mut self, configured: Option<&str>) -> bool {
        let persisted = self.round_id.as_deref();
        let transition = matches!(
            (configured, persisted),
            (Some(new), Some(old)) if new != old
        );
        if transition {
            for state in self.instances.values_mut() {
                state.reset_round_bound();
            }
        }
        transition
    }
}

pub(super) fn persist_risk_state(
    path: &Path,
    round_id: Option<&str>,
    instances: &HashMap<String, InstanceRiskState>,
) {
    let snapshot = RiskStateSnapshot {
        version: 2,
        round_id: round_id.map(|s| s.to_string()),
        instances: instances.clone(),
    };
    let Ok(json) = serde_json::to_string(&snapshot) else {
        log::warn!("[RISK_STATE] serialize failed");
        return;
    };
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "risk_state.json".to_string());
    let tmp = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));
    if let Err(e) = fs::write(&tmp, json) {
        log::warn!("[RISK_STATE] tmp write failed: {:?}", e);
        return;
    }
    if let Err(e) = fs::rename(&tmp, path) {
        log::warn!("[RISK_STATE] rename failed: {:?}", e);
        let _ = fs::remove_file(&tmp);
    }
}

pub(super) fn load_risk_state(path: &Path) -> RiskStateSnapshot {
    let content = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return RiskStateSnapshot::default(),
        Err(e) => {
            log::warn!("[RISK_STATE] read failed ({}): {:?}", path.display(), e);
            return RiskStateSnapshot::default();
        }
    };
    match serde_json::from_str::<RiskStateSnapshot>(&content) {
        Ok(snap) => snap,
        Err(e) => {
            log::warn!("[RISK_STATE] parse failed ({}): {:?}", path.display(), e);
            RiskStateSnapshot::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_preserves_funding_carry_today() {
        // bot-strategy#371: funding_carry_today must persist across restart
        // alongside realized_pnl_today so the dashboard's per-day funding
        // attribution survives a bot bounce mid-session.
        let mut instances = HashMap::new();
        instances.insert(
            "a".to_string(),
            InstanceRiskState {
                realized_pnl_today: -4.20,
                funding_carry_today: -0.85,
                ..InstanceRiskState::default()
            },
        );
        let tmpdir = std::env::temp_dir().join(format!(
            "risk_io_test_{}_{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = fs::create_dir_all(&tmpdir);
        let path = tmpdir.join("risk_state.json");
        persist_risk_state(&path, Some("test-round"), &instances);

        let loaded = load_risk_state(&path);
        let restored = loaded.instances.get("a").expect("instance present");
        assert!(
            (restored.funding_carry_today - (-0.85)).abs() < 1e-9,
            "funding_carry_today round-trip mismatch: {}",
            restored.funding_carry_today
        );
        assert!(
            (restored.realized_pnl_today - (-4.20)).abs() < 1e-9,
            "realized_pnl_today round-trip mismatch: {}",
            restored.realized_pnl_today
        );

        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir(&tmpdir);
    }

    fn populated_instance() -> InstanceRiskState {
        let mut last_stop_loss_per_pair = std::collections::HashMap::new();
        last_stop_loss_per_pair.insert(
            "BTC/ETH".to_string(),
            StopLossMark {
                direction: PositionDirection::LongSpread,
                ts: 1_234_567_000,
            },
        );
        InstanceRiskState {
            consecutive_losses: 3,
            circuit_breaker_until_ts: Some(1_234_567_890),
            last_stop_loss_per_pair,
            session_start_equity: 500.0,
            session_start_ts: 1_700_000_000,
            realized_pnl_today: -4.20,
            funding_carry_today: -0.85,
            equity_samples: vec![EquitySample {
                ts: 1_700_000_000,
                equity: 500.0,
            }],
            capital_baseline_equity: 500.0,
            session_halted: true,
            session_halt_reason: Some("session_dd_500bps".to_string()),
            session_halt_ts: Some(1_700_000_500),
            total_trades: 42,
            total_wins: 30,
            total_pnl: 12.5,
            peak_pnl: 18.0,
            max_dd: -5.5,
        }
    }

    #[test]
    fn reset_round_bound_clears_round_fields_preserves_session_fields() {
        // bot-strategy#354: the round-transition reset must zero
        // round-lifetime state (trade stats, halt flags, cool-down anchors,
        // equity samples) but leave session-rolling fields alone so a
        // mid-session round flip doesn't lose today's PnL accounting.
        let mut s = populated_instance();
        s.reset_round_bound();

        // round-bound fields zeroed
        assert_eq!(s.consecutive_losses, 0);
        assert_eq!(s.circuit_breaker_until_ts, None);
        assert!(s.last_stop_loss_per_pair.is_empty());
        assert!(s.equity_samples.is_empty());
        assert_eq!(s.capital_baseline_equity, 0.0);
        assert!(!s.session_halted);
        assert_eq!(s.session_halt_reason, None);
        assert_eq!(s.session_halt_ts, None);
        assert_eq!(s.total_trades, 0);
        assert_eq!(s.total_wins, 0);
        assert_eq!(s.total_pnl, 0.0);
        assert_eq!(s.peak_pnl, 0.0);
        assert_eq!(s.max_dd, 0.0);

        // session-rolling fields preserved (have their own daily lifecycle)
        assert_eq!(s.session_start_equity, 500.0);
        assert_eq!(s.session_start_ts, 1_700_000_000);
        assert!((s.realized_pnl_today - (-4.20)).abs() < 1e-9);
        assert!((s.funding_carry_today - (-0.85)).abs() < 1e-9);
    }

    fn make_snapshot(round_id: Option<&str>) -> RiskStateSnapshot {
        let mut instances = std::collections::HashMap::new();
        instances.insert("a".to_string(), populated_instance());
        instances.insert("b".to_string(), populated_instance());
        RiskStateSnapshot {
            version: 2,
            round_id: round_id.map(|s| s.to_string()),
            instances,
        }
    }

    #[test]
    fn apply_round_transition_matrix() {
        // bot-strategy#354 conservative transition policy: reset fires ONLY
        // when configured and persisted are both Some and differ. Initial
        // opt-in (persisted=None) and operator backing out (configured=None)
        // are no-ops; in those cases the operator runs reset-round-state.sh
        // explicitly.
        for (configured, persisted, expected_fire, label) in [
            (None, None, false, "(None, None)"),
            (Some("a"), None, false, "(Some(a), None) — initial opt-in"),
            (
                None,
                Some("a"),
                false,
                "(None, Some(a)) — operator removed round_id",
            ),
            (
                Some("a"),
                Some("a"),
                false,
                "(Some(a), Some(a)) — same round",
            ),
            (
                Some("a"),
                Some("b"),
                true,
                "(Some(a), Some(b)) — transition",
            ),
        ] {
            let mut snap = make_snapshot(persisted);
            let fired = snap.apply_round_transition(configured);
            assert_eq!(fired, expected_fire, "case {label}: fired mismatch");

            let inst = snap.instances.get("a").expect("instance present");
            if expected_fire {
                assert_eq!(inst.total_trades, 0, "case {label}: round fields not reset");
                assert_eq!(
                    inst.consecutive_losses, 0,
                    "case {label}: counter not reset"
                );
            } else {
                assert_eq!(
                    inst.total_trades, 42,
                    "case {label}: round fields wrongly reset"
                );
                assert_eq!(
                    inst.consecutive_losses, 3,
                    "case {label}: counter wrongly reset"
                );
            }
            // round_id itself is not touched by the reset — only instance
            // fields are. Caller (engine/persistence.rs) rewrites round_id
            // on the next persist_risk_state.
            assert_eq!(
                snap.round_id.as_deref(),
                persisted,
                "case {label}: round_id should not be mutated by transition",
            );
        }
    }

    #[test]
    fn apply_round_transition_resets_every_instance() {
        // Multi-instance snapshot (A/B/C-style live deployment): the reset
        // must hit every instance, not just one. Otherwise inheritance
        // pollution survives on the un-reset instance.
        let mut snap = make_snapshot(Some("round-3"));
        let fired = snap.apply_round_transition(Some("round-4"));
        assert!(fired);
        for (id, inst) in &snap.instances {
            assert_eq!(inst.total_trades, 0, "instance {id}: not reset");
            assert_eq!(inst.total_wins, 0, "instance {id}: not reset");
            assert_eq!(inst.consecutive_losses, 0, "instance {id}: not reset");
            assert!(
                inst.last_stop_loss_per_pair.is_empty(),
                "instance {id}: stop-loss anchors not cleared"
            );
            assert!(
                inst.equity_samples.is_empty(),
                "instance {id}: equity samples not cleared"
            );
        }
    }

    #[test]
    fn missing_funding_carry_field_defaults_to_zero() {
        // Older risk_state.json written by binaries before #371 lands.
        // The new field must read as 0.0 via #[serde(default)] so the
        // first post-upgrade restart isn't a parse failure.
        let json = r#"{
            "_v": 2,
            "round_id": "test",
            "instances": {
                "a": {
                    "consecutive_losses": 0,
                    "realized_pnl_today": -1.5
                }
            }
        }"#;
        let snap: RiskStateSnapshot =
            serde_json::from_str(json).expect("legacy snapshot parses with new field defaulted");
        let inst = snap.instances.get("a").expect("instance present");
        assert_eq!(inst.funding_carry_today, 0.0);
        assert!((inst.realized_pnl_today - (-1.5)).abs() < 1e-9);
    }
}
