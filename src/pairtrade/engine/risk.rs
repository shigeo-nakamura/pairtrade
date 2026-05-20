//! Risk-management helpers for `PairTradeEngine`.
//!
//! Daily / session DD evaluation, kill-switch + ack handling, equity
//! sampling, and the dashboard-facing risk snapshots. Pure relocation
//! from the god-module split (#291); no semantic change.
//!
//! Logical layers:
//! - **Snapshots** (`daily_risk_snapshot`, `circuit_breaker_snapshot`,
//!   `session_risk_snapshot`) — read-only views consumed by the status
//!   reporter / dashboard.
//! - **Equity refresh** (`refresh_equity_if_needed`, `fetch_equity_rest`,
//!   `update_equity_sample`) — pull margin balance from the connector and
//!   feed the rolling-peak window.
//! - **Daily DD** (`refresh_daily_session`, `daily_loss_blocks`) —
//!   rollover bookkeeping plus the new-entry gate.
//! - **Kill switch / ack** (`update_kill_switch_state`, `consume_risk_ack`,
//!   `record_risk_event_*`) — sentinel-file flags and risk-history events.
//! - **Session DD** (`evaluate_session_dd`, `rolling_peak`) — rolling
//!   window check + auto-flatten on breach.
//! - **Pure helpers** (`session_day`, `daily_loss_breaches_threshold`,
//!   `session_dd_breaches_threshold`, `risk_state_path_for`) — leverage-
//!   invariant threshold maths exposed for unit tests.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;

use super::super::risk_io;
use super::super::status;
use super::super::PairTradeEngine;
use super::super::StrategyInstance;
use super::super::EQUITY_REFRESH_CACHE_SECS;
use super::super::KILL_SWITCH_PATH;
use super::super::RISK_ACK_PATH;

impl PairTradeEngine {
    pub(in crate::pairtrade) fn daily_risk_snapshot(
        &self,
        inst_idx: usize,
    ) -> Option<status::DailyRiskSnapshot> {
        let threshold_bps = self.cfg.risk.max_daily_loss_bps;
        if threshold_bps == 0 {
            return None;
        }
        let inst = &self.instances[inst_idx];
        if inst.session_start_ts == 0 {
            return None;
        }
        let daily_pnl_bps = if inst.session_start_equity > 0.0 {
            (inst.realized_pnl_today / inst.session_start_equity) * 10_000.0
        } else {
            0.0
        };
        Some(status::DailyRiskSnapshot {
            daily_pnl: inst.realized_pnl_today,
            daily_pnl_bps,
            session_start_equity: inst.session_start_equity,
            session_start_ts: inst.session_start_ts,
            max_daily_loss_bps: threshold_bps,
            effective_max_daily_loss_bps: threshold_bps as f64 * self.cfg.max_leverage,
            risk_halted: inst.daily_loss_halted,
        })
    }

    /// Build a `CircuitBreakerSnapshot` for the dashboard. Always returns
    /// a value (even when no losses have occurred yet) so the dashboard
    /// can display "0 / N losses, no cooldown" as a steady-state — the
    /// alternative would be an Option that flips to Some only at the
    /// first loss, making the field appear-on-fire instead of always-on.
    pub(in crate::pairtrade) fn circuit_breaker_snapshot(
        &self,
        inst_idx: usize,
    ) -> Option<status::CircuitBreakerSnapshot> {
        let inst = &self.instances[inst_idx];
        let now_ts = self.current_now_ts();
        let cooldown_remaining_secs = inst.circuit_breaker_until_ts.and_then(|until| {
            let remaining = until - now_ts;
            if remaining > 0 {
                Some(remaining)
            } else {
                None
            }
        });
        let active = cooldown_remaining_secs.is_some();
        Some(status::CircuitBreakerSnapshot {
            consecutive_losses: inst.consecutive_losses,
            active,
            until_ts: inst.circuit_breaker_until_ts,
            cooldown_remaining_secs,
            tier1_threshold: self.cfg.default_pair_params.circuit_breaker_tier1_losses,
            tier2_threshold: self.cfg.default_pair_params.circuit_breaker_tier2_losses,
        })
    }

    /// Build a `SessionRiskSnapshot` from the rolling-peak window. Returns
    /// `None` when the threshold is disabled (no point surfacing data
    /// nobody acts on) or no equity samples have been taken yet. See
    /// bot-strategy#185 Phase 3-1.
    pub(in crate::pairtrade) fn session_risk_snapshot(
        &self,
        inst_idx: usize,
    ) -> Option<status::SessionRiskSnapshot> {
        let threshold_bps = self.cfg.risk.max_session_loss_bps;
        if threshold_bps == 0 {
            return None;
        }
        let inst = &self.instances[inst_idx];
        // bot-strategy#366: suppress the snapshot until the connector
        // balance has landed, matching the gate inside
        // `evaluate_session_dd`. Dashboards/error-watch would otherwise
        // render a phantom 50% DD against the seeded `equity_cache`.
        if !inst.equity_initialized {
            return None;
        }
        let current = inst.equity_cache;
        if current <= 0.0 || inst.equity_samples.is_empty() {
            return None;
        }
        let (peak, dd_bps) = Self::rolling_peak(&inst.equity_samples, current)?;
        Some(status::SessionRiskSnapshot {
            current_equity: current,
            peak_equity: peak,
            dd_bps,
            max_session_loss_bps: threshold_bps,
            effective_max_session_loss_bps: threshold_bps as f64 * self.cfg.max_leverage,
            lookback_secs: self.cfg.risk.session_dd_lookback_secs,
            sample_count: inst.equity_samples.len(),
            session_halted: inst.session_halted,
            halt_reason: inst.session_halt_reason.clone(),
            halt_ts: inst.session_halt_ts,
        })
    }

    pub(in crate::pairtrade) async fn refresh_equity_if_needed(
        &mut self,
        inst_idx: usize,
    ) -> Result<()> {
        const CACHE_SECS: u64 = EQUITY_REFRESH_CACHE_SECS;
        if self.instances[inst_idx]
            .last_equity_fetch
            .map(|t| t.elapsed() < Duration::from_secs(CACHE_SECS))
            .unwrap_or(false)
        {
            return Ok(());
        }
        self.fetch_equity_rest(inst_idx).await;
        Ok(())
    }

    pub(in crate::pairtrade) async fn fetch_equity_rest(&mut self, inst_idx: usize) {
        // No pairtrade-side throttle: dex-connector v4.2.83 (#239) populates
        // `balance_cache` from WS-derived equity (assets[USDC].margin_balance
        // + sum(positions.unrealized_pnl)), so `get_balance(None)` is a cache
        // hit in steady state — sub-millisecond, no REST call. The previous
        // `MIN_ACCOUNT_SPACING=5.5s` sleep across A/B/C variants was added
        // (#122) to dodge Lighter's per-wallet short-window throttle on
        // /account; with WS-derived equity that REST path is rarely
        // exercised and the connector's existing 429 retry handles the
        // transient case. Removing the sleep eliminates the 11s entry burst
        // that drove STEP_OVERRUN warnings on every trade (#235, #236, #238).
        match self.connector.get_balance(None).await {
            Ok(resp) => {
                if let Some(eq) = resp.equity.to_f64() {
                    let inst = &mut self.instances[inst_idx];
                    // bot-strategy#382: a 0-valued reading during the pre-init
                    // warm-up window is the dex-connector's WS-derived balance
                    // cache being empty (no account dump yet), not a real
                    // wallet balance. Pre-fix this raced `update_equity(0)`
                    // into the status reporter, which locked
                    // `equity_day_start = 0` for the rest of the UTC day and
                    // surfaced `pnl_today = +<full equity>` on the dashboard
                    // (observed on Tokyo Lighter B/C after the 2026-05-13
                    // 06:50 UTC restart: pnl_today = +$150 with no trades).
                    // Skip writes until the first positive equity lands.
                    // Post-init zero readings ARE accepted — a genuinely
                    // rekt bot should still surface on dashboards.
                    if eq <= 0.0 && !inst.equity_initialized {
                        inst.last_equity_fetch = Some(Instant::now());
                        return;
                    }
                    inst.equity_cache = eq.max(0.0);
                    inst.last_equity_fetch = Some(Instant::now());
                    // bot-strategy#366: arm the session-DD gate only after a
                    // connector-sourced balance has actually landed. Before
                    // this point `equity_cache` is still the YAML
                    // `equity_reference_usd` seed, which can race the
                    // first WS account dump against the persisted
                    // `equity_samples` peak and synthesise a 50% DD.
                    if !inst.equity_initialized {
                        log::info!(
                            "[SESSION_DD] {} equity initialized: cache={:.2}",
                            inst.id,
                            inst.equity_cache
                        );
                        inst.equity_initialized = true;
                    }
                    if let Some(reporter) = &mut inst.status_reporter {
                        reporter.update_equity(inst.equity_cache);
                    }
                }
            }
            Err(err) => {
                log::warn!(
                    "equity refresh failed for {}: {:?}",
                    self.instances[inst_idx].id,
                    err
                );
                self.instances[inst_idx].last_equity_fetch = Some(Instant::now());
            }
        }
    }

    /// Cross the UTC session boundary when `now_ts` lands in a different
    /// day bucket from the persisted `session_start_ts`. Called once per
    /// `step_shared` tick. On rollover, realized_pnl_today is zeroed and
    /// `session_start_equity` is reset to the fixed `equity_reference_usd`
    /// so the daily-DD denominator stays constant within (and across)
    /// sessions, regardless of intra-session live equity drift. Also
    /// initialises state on the very first tick (when
    /// `session_start_ts == 0`). See bot-strategy#185 Phase 2 and
    /// bot-strategy#222 (semantic switch from live floor to fixed
    /// reference).
    pub(in crate::pairtrade) fn refresh_daily_session(&mut self) {
        if self.cfg.backtest_mode {
            return;
        }
        let reset_hour = self.cfg.risk.daily_reset_utc_hour;
        let threshold_bps = self.cfg.risk.max_daily_loss_bps;
        let leverage = self.cfg.max_leverage;
        // Threshold is configured in 1x-equivalent (market-move) units and
        // scaled by max_leverage at comparison time so a `max_leverage`
        // change doesn't silently relax the gate. See
        // `daily_loss_blocks` for the full rationale.
        let effective_threshold_bps = threshold_bps as f64 * leverage;
        let now_ts = self.current_now_ts();
        let current_day = session_day(now_ts, reset_hour);
        let mut dirty = false;
        // Collect transitions during the &mut iter so we can call
        // record_risk_event_for_instance after the loop ends (the
        // recorder needs &mut self, conflicting with the iter borrow).
        let mut transitions: Vec<(
            usize,
            &'static str,
            Option<String>,
            Option<serde_json::Value>,
        )> = Vec::new();
        for (inst_idx, inst) in self.instances.iter_mut().enumerate() {
            let prior_day = if inst.session_start_ts > 0 {
                Some(session_day(inst.session_start_ts, reset_hour))
            } else {
                None
            };
            let needs_rollover = match prior_day {
                None => true,
                Some(prev) => prev != current_day,
            };
            if needs_rollover {
                let equity_base = inst.equity_reference_usd;
                let prev_pnl = inst.realized_pnl_today;
                inst.session_start_ts = now_ts;
                inst.session_start_equity = equity_base;
                inst.realized_pnl_today = 0.0;
                inst.funding_carry_today = 0.0;
                if inst.daily_loss_halted {
                    log::warn!("[DAILY_DD] {} halt cleared by session rollover", inst.id);
                    transitions.push((
                        inst_idx,
                        "cleared",
                        Some("session_rollover".to_string()),
                        None,
                    ));
                }
                inst.daily_loss_halted = false;
                dirty = true;
                if prior_day.is_some() {
                    log::info!(
                        "[DAILY_DD] {} session rolled: prev_pnl={:.4} -> reset; new session_start_equity={:.2}",
                        inst.id, prev_pnl, equity_base
                    );
                } else {
                    log::info!(
                        "[DAILY_DD] {} session initialised: session_start_equity={:.2}",
                        inst.id,
                        equity_base
                    );
                }
            }

            // Transition logging for the halt gate. Recomputed each tick so
            // we catch both the activation (when realized_pnl_today crosses
            // the threshold after a losing close) and any clear (should be
            // rare: Phase 2 doesn't expose a manual recovery path, so this
            // branch fires only after a rollover reset above or a manual
            // `risk_state.json` edit).
            let currently_blocks = threshold_bps > 0
                && inst.session_start_equity > 0.0
                && inst.realized_pnl_today < 0.0
                && {
                    let loss_bps =
                        (-inst.realized_pnl_today / inst.session_start_equity) * 10_000.0;
                    loss_bps >= effective_threshold_bps
                };
            if currently_blocks && !inst.daily_loss_halted {
                let loss_bps = (-inst.realized_pnl_today / inst.session_start_equity) * 10_000.0;
                log::warn!(
                    "[DAILY_DD] {} halted: realized_pnl_today={:.4} loss_bps={:.1} threshold={}bps × leverage={:.1} = effective={:.1}bps (new entries blocked until UTC {:02}:00)",
                    inst.id, inst.realized_pnl_today, loss_bps, threshold_bps, leverage, effective_threshold_bps, reset_hour
                );
                inst.daily_loss_halted = true;
                dirty = true;
                transitions.push((
                    inst_idx,
                    "activated",
                    Some(format!("{:.0}_bps_loss", loss_bps)),
                    Some(serde_json::json!({
                        "loss_bps": loss_bps,
                        "threshold_bps": threshold_bps,
                        "leverage": leverage,
                        "effective_threshold_bps": effective_threshold_bps,
                        "realized_pnl_today": inst.realized_pnl_today,
                    })),
                ));
            } else if !currently_blocks && inst.daily_loss_halted {
                // Usually unreachable in Phase 2; kept so manual edits
                // to risk_state.json don't leave a stale halt flag.
                log::warn!("[DAILY_DD] {} halt cleared", inst.id);
                inst.daily_loss_halted = false;
                dirty = true;
                transitions.push((inst_idx, "cleared", None, None));
            }
        }
        for (inst_idx, event_type, reason, detail) in transitions {
            self.record_risk_event_for_instance(inst_idx, "daily_dd", event_type, reason, detail);
        }
        if dirty {
            self.persist_risk_state();
        }
    }

    /// Whether `realized_pnl_today` has breached `max_daily_loss_bps`
    /// against `session_start_equity`. Returns false when the threshold
    /// is disabled (0 bps), the equity baseline is still zero (pre-first
    /// rollover), or the running PnL is non-negative.
    ///
    /// The configured `max_daily_loss_bps` is interpreted as a 1x-equivalent
    /// market-move threshold and multiplied by `max_leverage` internally,
    /// so `realized_pnl_today` (which scales linearly with leverage) is
    /// compared against an equivalently-scaled threshold. Net effect: the
    /// halt fires at the same underlying market move regardless of leverage,
    /// so changing `max_leverage` does not require rewriting the bps value.
    pub(in crate::pairtrade) fn daily_loss_blocks(&self, inst: &StrategyInstance) -> bool {
        daily_loss_breaches_threshold(
            inst.realized_pnl_today,
            inst.session_start_equity,
            self.cfg.risk.max_daily_loss_bps,
            self.cfg.max_leverage,
        )
    }

    /// Refresh `kill_switch_active` from the sentinel file. Called at the
    /// top of every `step_shared` tick. Logs on state transitions so the
    /// journal shows exactly when entries were blocked / resumed without
    /// spamming the log on every check. See bot-strategy#185 Phase 1-2.
    pub(in crate::pairtrade) fn update_kill_switch_state(&mut self) {
        if self.cfg.backtest_mode {
            return;
        }
        let present = std::path::Path::new(KILL_SWITCH_PATH).exists();
        if present && !self.kill_switch_active {
            log::warn!(
                "[KILL_SWITCH] activated: {} detected; new entries blocked, existing positions will exit normally",
                KILL_SWITCH_PATH
            );
            self.kill_switch_active = true;
            self.record_risk_event_all_instances(
                "kill_switch",
                "activated",
                Some(KILL_SWITCH_PATH.to_string()),
                None,
            );
        } else if !present && self.kill_switch_active {
            log::warn!(
                "[KILL_SWITCH] cleared: {} removed; new entries resumed",
                KILL_SWITCH_PATH
            );
            self.kill_switch_active = false;
            self.record_risk_event_all_instances(
                "kill_switch",
                "cleared",
                Some(KILL_SWITCH_PATH.to_string()),
                None,
            );
        }
    }

    /// Record a risk-history event on every instance's status_reporter.
    /// Used for fleet-level transitions (KILL_SWITCH file flag) where
    /// the gate affects all instances at once. See bot-strategy#231
    /// Phase B.
    pub(in crate::pairtrade) fn record_risk_event_all_instances(
        &mut self,
        kind: &str,
        event_type: &str,
        reason: Option<String>,
        detail: Option<serde_json::Value>,
    ) {
        if self.cfg.backtest_mode {
            return;
        }
        let now_ts = self.current_now_ts();
        for inst in &mut self.instances {
            let id = inst.id.clone();
            if let Some(reporter) = &mut inst.status_reporter {
                reporter.record_risk_event(status::RiskHistoryEvent {
                    ts: now_ts,
                    instance_id: id,
                    kind: kind.to_string(),
                    event_type: event_type.to_string(),
                    reason: reason.clone(),
                    detail: detail.clone(),
                });
            }
        }
    }

    /// Record a risk-history event for a single instance. Used for
    /// per-instance transitions (daily/session DD, circuit breaker)
    /// where the gate fires on one variant without affecting peers.
    pub(in crate::pairtrade) fn record_risk_event_for_instance(
        &mut self,
        inst_idx: usize,
        kind: &str,
        event_type: &str,
        reason: Option<String>,
        detail: Option<serde_json::Value>,
    ) {
        if self.cfg.backtest_mode {
            return;
        }
        let now_ts = self.current_now_ts();
        let inst = &mut self.instances[inst_idx];
        let id = inst.id.clone();
        if let Some(reporter) = &mut inst.status_reporter {
            reporter.record_risk_event(status::RiskHistoryEvent {
                ts: now_ts,
                instance_id: id,
                kind: kind.to_string(),
                event_type: event_type.to_string(),
                reason,
                detail,
            });
        }
    }

    /// Consume `/opt/debot/RISK_ACK` if present and clear `session_halted`
    /// across all instances. The file is unconditionally removed so a
    /// stale ack from a prior incident never silently re-arms. See
    /// bot-strategy#185 Phase 3-2.
    pub(in crate::pairtrade) fn consume_risk_ack(&mut self) {
        if self.cfg.backtest_mode {
            return;
        }
        let path = std::path::Path::new(RISK_ACK_PATH);
        if !path.exists() {
            return;
        }
        // Read the file (best-effort) so the ack reason makes it into the
        // journal — useful when chasing why a halt cleared days later.
        let payload = std::fs::read_to_string(path).unwrap_or_default();
        let trimmed = payload.trim();
        let mut cleared_any = false;
        let mut cleared_indices: Vec<(usize, String)> = Vec::new();
        for (inst_idx, inst) in self.instances.iter_mut().enumerate() {
            if inst.session_halted {
                let prior_reason = inst
                    .session_halt_reason
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                log::warn!(
                    "[SESSION_DD] {} halt cleared by ack at {} (reason was: {}, ack payload: {:?})",
                    inst.id,
                    RISK_ACK_PATH,
                    prior_reason,
                    trimmed
                );
                inst.session_halted = false;
                inst.session_halt_reason = None;
                inst.session_halt_ts = None;
                cleared_any = true;
                cleared_indices.push((inst_idx, prior_reason));
            }
        }
        let ack_payload = if trimmed.is_empty() {
            None
        } else {
            Some(serde_json::json!({ "ack_payload": trimmed }))
        };
        for (inst_idx, prior_reason) in cleared_indices {
            self.record_risk_event_for_instance(
                inst_idx,
                "session_dd",
                "ack",
                Some(prior_reason),
                ack_payload.clone(),
            );
        }
        if let Err(e) = std::fs::remove_file(path) {
            log::warn!(
                "[SESSION_DD] failed to remove {} after ack: {:?}",
                RISK_ACK_PATH,
                e
            );
        } else {
            log::info!("[SESSION_DD] {} consumed", RISK_ACK_PATH);
        }
        if cleared_any {
            self.persist_risk_state();
        }
    }

    /// Append the current equity to `equity_samples`, prune entries
    /// outside the rolling window, and update the cached peak. Called
    /// after a successful equity refresh. No-op when the threshold is
    /// disabled (0 bps) so disabled instances don't grow disk state.
    pub(in crate::pairtrade) fn update_equity_sample(&mut self, inst_idx: usize) {
        if self.cfg.backtest_mode {
            return;
        }
        let threshold_bps = self.cfg.risk.max_session_loss_bps;
        if threshold_bps == 0 {
            return;
        }
        let lookback = self.cfg.risk.session_dd_lookback_secs as i64;
        let sample_secs = self.cfg.risk.session_dd_sample_secs as i64;
        let now_ts = self.current_now_ts();
        let cutoff = now_ts.saturating_sub(lookback);
        let inst = &mut self.instances[inst_idx];
        // bot-strategy#366: do not pollute the rolling-peak deque with the
        // `equity_reference_usd` seed before the first connector-sourced
        // balance lands. Sampling a synthetic equity would skew later
        // peak computations and defeat the gate added in
        // `evaluate_session_dd`.
        if !inst.equity_initialized {
            return;
        }
        let equity = inst.equity_cache;
        if equity <= 0.0 {
            return;
        }
        // Keep at most one sample per `sample_secs` window so 30 d at 1 h
        // cadence ≤ 720 entries (≈ 17 KB JSON). Newer sample replaces an
        // entry within the same bucket so peak stays current without
        // unbounded growth.
        let bucket_start = (now_ts / sample_secs) * sample_secs;
        let mut replaced = false;
        if let Some(last) = inst.equity_samples.last_mut() {
            if last.ts >= bucket_start {
                last.equity = equity;
                last.ts = now_ts;
                replaced = true;
            }
        }
        if !replaced {
            inst.equity_samples
                .push(risk_io::EquitySample { ts: now_ts, equity });
        }
        let pre_len = inst.equity_samples.len();
        inst.equity_samples.retain(|s| s.ts >= cutoff);
        let pruned = pre_len.saturating_sub(inst.equity_samples.len());
        if pruned > 0 {
            log::debug!(
                "[SESSION_DD] {} pruned {} expired equity samples (window {}s)",
                inst.id,
                pruned,
                lookback
            );
        }
        // Persistence is cheap (atomic rename of a small JSON file) but
        // we don't write on every tick — only when something changed
        // materially. Sampling refreshes cadence; `step_shared` calls
        // `evaluate_session_dd` immediately after, which triggers
        // persistence on halt transitions.
    }

    /// Compute the rolling peak from `equity_samples` and return the
    /// peak / current ratio. Returns `None` when there are no samples
    /// or the peak is non-positive (cannot define a percentage DD).
    pub(in crate::pairtrade) fn rolling_peak(
        samples: &[risk_io::EquitySample],
        current: f64,
    ) -> Option<(f64, f64)> {
        let mut peak = current;
        for s in samples {
            if s.equity > peak {
                peak = s.equity;
            }
        }
        if peak <= 0.0 {
            return None;
        }
        let dd_bps = if current >= peak {
            0.0
        } else {
            ((peak - current) / peak) * 10_000.0
        };
        Some((peak, dd_bps))
    }

    /// Check whether the rolling peak DD has breached the threshold and
    /// engage the session halt + auto-flatten if so. Idempotent: a
    /// repeated breach while already halted re-runs `close_all_positions`
    /// at most once (the second call succeeds quickly if the account is
    /// already flat). Returns true if the halt is currently active for
    /// the instance after the check.
    pub(in crate::pairtrade) async fn evaluate_session_dd(&mut self, inst_idx: usize) -> bool {
        if self.cfg.backtest_mode {
            return false;
        }
        let threshold_bps = self.cfg.risk.max_session_loss_bps;
        if threshold_bps == 0 {
            return self.instances[inst_idx].session_halted;
        }
        let inst = &self.instances[inst_idx];
        if inst.session_halted {
            // Halt is sticky — once tripped, stay halted until ack'd.
            // No need to re-flatten on every tick; one shot is enough,
            // and `close_all_positions` was already invoked on trip.
            return true;
        }
        // bot-strategy#366: refuse to trip until the connector has fed at
        // least one real balance into `equity_cache`. Otherwise the seed
        // `equity_reference_usd` races the persisted-peak `equity_samples`
        // for the first 5–60s after restart and synthesises a 50% DD.
        if !inst.equity_initialized {
            return false;
        }
        let current = inst.equity_cache;
        if current <= 0.0 {
            return false;
        }
        let Some((peak, dd_bps)) = Self::rolling_peak(&inst.equity_samples, current) else {
            return false;
        };
        // Threshold is interpreted in 1x-equivalent (market-move) bps and
        // multiplied by max_leverage so the halt fires at the same
        // underlying market move regardless of leverage. Equity DD scales
        // ~linearly with leverage, so the multiplied threshold tracks
        // observed dd_bps consistently. See bot-strategy#185 leverage-
        // neutralization amendment.
        let leverage = self.cfg.max_leverage;
        let effective_threshold_bps = threshold_bps as f64 * leverage;
        if !session_dd_breaches_threshold(dd_bps, threshold_bps, leverage) {
            return false;
        }
        let now_ts = self.current_now_ts();
        let reason = format!("session_dd_{}bps_lev{:.1}", threshold_bps, leverage);
        log::error!(
            "[SESSION_DD] {} breach: equity={:.2} peak={:.2} dd_bps={:.1} threshold={}bps × leverage={:.1} = effective={:.1}bps; flattening positions and halting (ack via {})",
            inst.id, current, peak, dd_bps, threshold_bps, leverage, effective_threshold_bps, RISK_ACK_PATH
        );
        {
            let inst_mut = &mut self.instances[inst_idx];
            inst_mut.session_halted = true;
            inst_mut.session_halt_reason = Some(reason.clone());
            inst_mut.session_halt_ts = Some(now_ts);
        }
        self.record_risk_event_for_instance(
            inst_idx,
            "session_dd",
            "activated",
            Some(reason),
            Some(serde_json::json!({
                "current_equity": current,
                "peak_equity": peak,
                "dd_bps": dd_bps,
                "threshold_bps": threshold_bps,
                "leverage": leverage,
                "effective_threshold_bps": effective_threshold_bps,
            })),
        );
        self.persist_risk_state();
        // Flatten this instance's positions. `self.connector` was already
        // pointed at `instances[inst_idx].connector` by the caller in
        // `step()`, so close_all_positions hits the right sub-account.
        if !self.cfg.dry_run && !self.cfg.observe_only {
            if let Err(err) = self.connector.close_all_positions(None).await {
                log::error!(
                    "[SESSION_DD] {} close_all_positions failed: {:?}",
                    self.instances[inst_idx].id,
                    err
                );
            } else {
                log::warn!(
                    "[SESSION_DD] {} close_all_positions invoked",
                    self.instances[inst_idx].id
                );
            }
        }
        true
    }
}

/// Compute `risk_state.json` path as a sibling of the history file. When
/// `history_path` is a bare filename (the production default), this falls
/// back to a relative path resolved against CWD — which is `/opt/debot/`
/// under the systemd unit.
pub(in crate::pairtrade) fn risk_state_path_for(history_path: &std::path::Path) -> PathBuf {
    match history_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(dir) => dir.join("risk_state.json"),
        None => PathBuf::from("risk_state.json"),
    }
}

/// Bucket a UNIX-seconds timestamp into a "session day" where day
/// boundaries fall at `reset_hour:00` UTC instead of midnight. Two
/// timestamps share a session day iff the function returns the same
/// value for both. `div_euclid` keeps the math correct for non-zero
/// reset hours near pre-epoch timestamps (impossible in practice but
/// cheap to preserve). See bot-strategy#185 Phase 2.
pub(in crate::pairtrade) fn session_day(ts_secs: i64, reset_hour: u32) -> i64 {
    let shift = (reset_hour as i64) * 3600;
    (ts_secs - shift).div_euclid(86400)
}

/// Pure-fn split of the daily DD threshold check (`daily_loss_blocks`),
/// exposed so unit tests can verify the leverage-invariance property
/// without wiring up a full engine. `threshold_bps` is the 1x-equivalent
/// market-move bps from YAML; the comparison multiplies it by
/// `max_leverage` so the same value covers any leverage. See
/// bot-strategy#185 leverage-neutralization amendment.
pub(in crate::pairtrade) fn daily_loss_breaches_threshold(
    realized_pnl_today: f64,
    session_start_equity: f64,
    threshold_bps: u32,
    max_leverage: f64,
) -> bool {
    if threshold_bps == 0 || session_start_equity <= 0.0 {
        return false;
    }
    if realized_pnl_today >= 0.0 {
        return false;
    }
    let loss_bps = (-realized_pnl_today / session_start_equity) * 10_000.0;
    let effective_threshold = threshold_bps as f64 * max_leverage;
    loss_bps >= effective_threshold
}

/// Pure-fn split of the session-DD threshold check (`evaluate_session_dd`),
/// exposed so unit tests can verify the leverage-invariance property
/// without wiring up a full engine. Same scaling rule as
/// `daily_loss_breaches_threshold`: configured `threshold_bps` is
/// 1x-equivalent and is multiplied by `max_leverage` at comparison time.
pub(in crate::pairtrade) fn session_dd_breaches_threshold(
    dd_bps: f64,
    threshold_bps: u32,
    max_leverage: f64,
) -> bool {
    if threshold_bps == 0 {
        return false;
    }
    let effective_threshold = threshold_bps as f64 * max_leverage;
    dd_bps >= effective_threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-04-23 18:29:50 UTC — 1745432990 — Thu of day 20203 (UNIX/86400)
    const TS_2026_04_23_18_29: i64 = 1_745_432_990;
    // 2026-04-24 00:00:05 UTC — 1745452805 — Fri of day 20204
    const TS_2026_04_24_00_00: i64 = 1_745_452_805;
    // 2026-04-23 23:59:55 UTC — 1745452795 — still Thu of day 20203
    const TS_2026_04_23_23_59: i64 = 1_745_452_795;

    #[test]
    fn session_day_midnight_reset_same_bucket_before_boundary() {
        let a = session_day(TS_2026_04_23_18_29, 0);
        let b = session_day(TS_2026_04_23_23_59, 0);
        assert_eq!(
            a, b,
            "timestamps within the same UTC day should share bucket"
        );
    }

    #[test]
    fn session_day_midnight_reset_boundary_crosses() {
        let before = session_day(TS_2026_04_23_23_59, 0);
        let after = session_day(TS_2026_04_24_00_00, 0);
        assert_eq!(
            after - before,
            1,
            "UTC midnight should advance bucket by exactly 1"
        );
    }

    #[test]
    fn session_day_custom_reset_hour_shifts_boundary() {
        // reset_hour=6 ⇒ session boundary at UTC 06:00. A timestamp just
        // before 06:00 and one just after should fall in adjacent buckets.
        let ts_0400_utc = 1_745_467_200; // 2026-04-24 04:00 UTC
        let ts_0700_utc = 1_745_478_000; // 2026-04-24 07:00 UTC
        let bucket_0400 = session_day(ts_0400_utc, 6);
        let bucket_0700 = session_day(ts_0700_utc, 6);
        assert_eq!(bucket_0700 - bucket_0400, 1);
    }

    // bot-strategy#185 Phase 3-1: rolling-peak DD calculations.
    fn sample(ts: i64, equity: f64) -> risk_io::EquitySample {
        risk_io::EquitySample { ts, equity }
    }

    #[test]
    fn rolling_peak_no_samples_uses_current_as_peak() {
        // Bot just started: no samples yet, so peak = current and DD = 0.
        let (peak, dd) = PairTradeEngine::rolling_peak(&[], 1_000.0).unwrap();
        assert_eq!(peak, 1_000.0);
        assert_eq!(dd, 0.0);
    }

    #[test]
    fn rolling_peak_picks_max_across_samples_and_current() {
        let s = vec![
            sample(100, 1_000.0),
            sample(200, 1_500.0),
            sample(300, 1_200.0),
        ];
        let (peak, dd) = PairTradeEngine::rolling_peak(&s, 900.0).unwrap();
        assert_eq!(peak, 1_500.0);
        // (1500 - 900)/1500 * 10000 = 4000 bps
        assert!((dd - 4_000.0).abs() < 1e-6);
    }

    #[test]
    fn rolling_peak_zero_dd_when_current_is_new_peak() {
        let s = vec![sample(100, 1_000.0)];
        let (peak, dd) = PairTradeEngine::rolling_peak(&s, 1_500.0).unwrap();
        assert_eq!(peak, 1_500.0);
        assert_eq!(dd, 0.0);
    }

    #[test]
    fn rolling_peak_returns_none_for_non_positive_equity() {
        // Pre-funded account / connector hiccup → equity reads 0; can't
        // define a percent DD, so caller treats it as "no signal".
        assert!(PairTradeEngine::rolling_peak(&[], 0.0).is_none());
        assert!(PairTradeEngine::rolling_peak(&[], -10.0).is_none());
    }

    // bot-strategy#185 leverage-neutralization amendment:
    // `max_daily_loss_bps` and `max_session_loss_bps` are interpreted as
    // 1x-equivalent market-move bps and multiplied by `max_leverage` at
    // comparison time. Same YAML value should produce the same trip
    // behaviour at any leverage, so changing leverage doesn't silently
    // relax the gates.

    #[test]
    fn daily_loss_disabled_when_threshold_zero() {
        // 0 bps means "disabled" regardless of how big the loss is.
        assert!(!daily_loss_breaches_threshold(-1_000.0, 1_000.0, 0, 5.0));
    }

    #[test]
    fn daily_loss_no_trip_when_pnl_non_negative() {
        // A profitable or flat day never trips the halt.
        assert!(!daily_loss_breaches_threshold(0.0, 1_000.0, 300, 5.0));
        assert!(!daily_loss_breaches_threshold(50.0, 1_000.0, 300, 5.0));
    }

    #[test]
    fn daily_loss_no_trip_when_equity_baseline_zero() {
        // Pre-first-rollover state: session_start_equity=0, no comparison
        // is meaningful.
        assert!(!daily_loss_breaches_threshold(-100.0, 0.0, 300, 5.0));
    }

    #[test]
    fn daily_loss_leverage_invariance_3pct_market_move() {
        // 3% adverse market move on a fully-leveraged position. Same
        // YAML threshold (300 bps = 3% market-move equivalent) should
        // trip identically at 1x and 5x leverage.
        //
        // At 1x: 3% market move → -3% equity → realized_pnl = -30 against
        // session_start_equity = 1000. loss_bps = 300, threshold × 1 = 300,
        // breach = true (boundary).
        let trips_1x = daily_loss_breaches_threshold(-30.0, 1_000.0, 300, 1.0);
        // At 5x: same 3% market move → -15% equity → realized_pnl = -150.
        // loss_bps = 1500, threshold × 5 = 1500, breach = true (boundary).
        let trips_5x = daily_loss_breaches_threshold(-150.0, 1_000.0, 300, 5.0);
        assert!(trips_1x);
        assert!(trips_5x);
    }

    #[test]
    fn daily_loss_leverage_invariance_2pct_market_move_under_threshold() {
        // 2% adverse market move with a 300 bps (3% equivalent) threshold
        // should NOT trip at any leverage — the gate is leverage-invariant
        // in market-move units.
        let trips_1x = daily_loss_breaches_threshold(-20.0, 1_000.0, 300, 1.0);
        let trips_5x = daily_loss_breaches_threshold(-100.0, 1_000.0, 300, 5.0);
        assert!(!trips_1x);
        assert!(!trips_5x);
    }

    #[test]
    fn daily_loss_pre_amendment_value_loosens_the_gate_5x() {
        // Migration trap documented: if an operator copy-pastes the
        // pre-amendment 1500 bps into the new leverage-invariant schema
        // at max_leverage=5, the effective threshold becomes 1500 × 5 =
        // 7500 bps, so the gate now requires a 15% equivalent market
        // move (vs the pre-amendment 3%) before halting. This is a 5×
        // loosening, not a silent disable, but still bad — the parser
        // warning at config-resolution time exists to catch this.
        //
        // Pre-amendment intent (= 3% market move at 5x = 15% equity loss):
        // observed loss_bps = 1500 against equity = 1000.
        let pre_amendment_trip_zone = daily_loss_breaches_threshold(-150.0, 1_000.0, 1_500, 5.0);
        // 15% market move at 5x = 75% equity loss = observed 7500 bps =
        // boundary of the new (looser) effective threshold.
        let new_effective_trip_zone = daily_loss_breaches_threshold(-750.0, 1_000.0, 1_500, 5.0);
        assert!(
            !pre_amendment_trip_zone,
            "pre-amendment 1500 bps no longer trips at the original 3% market move; \
             the gate has loosened by 5x — operator must rewrite to 300 bps"
        );
        assert!(
            new_effective_trip_zone,
            "with stale 1500 bps the gate still fires at the new 15% market-move boundary"
        );
    }

    #[test]
    fn session_dd_disabled_when_threshold_zero() {
        assert!(!session_dd_breaches_threshold(5_000.0, 0, 5.0));
    }

    #[test]
    fn session_dd_leverage_invariance() {
        // A 500 bps (5% market-move equivalent) threshold trips at the
        // same underlying market move at any leverage. Equity DD scales
        // ~linearly with leverage, so observed dd_bps at 5x is roughly
        // 5x the dd_bps at 1x for the same trajectory.
        //
        // 1x trajectory: peak 1000, current 950 → dd_bps = 500.
        let trips_1x = session_dd_breaches_threshold(500.0, 500, 1.0);
        // 5x trajectory of same 5% market move: peak 1500, current 750
        // (compounded leveraged moves). dd_bps = 5000, threshold × 5 = 2500.
        let trips_5x = session_dd_breaches_threshold(2_500.0, 500, 5.0);
        assert!(trips_1x);
        assert!(trips_5x);
    }

    #[test]
    fn session_dd_no_trip_below_threshold() {
        // 1% market move with 500 bps (5%) threshold should never trip.
        let trips_1x = session_dd_breaches_threshold(100.0, 500, 1.0);
        let trips_5x = session_dd_breaches_threshold(500.0, 500, 5.0);
        assert!(!trips_1x);
        assert!(!trips_5x);
    }

    #[test]
    fn risk_state_path_falls_back_to_cwd_for_bare_filename() {
        let p = risk_state_path_for(std::path::Path::new("pairtrade_history_BTC_ETH.json"));
        assert_eq!(p, std::path::PathBuf::from("risk_state.json"));
    }

    #[test]
    fn risk_state_path_joined_to_parent_dir_when_absolute() {
        let p = risk_state_path_for(std::path::Path::new(
            "/opt/debot/pairtrade_history_BTC_ETH.json",
        ));
        assert_eq!(p, std::path::PathBuf::from("/opt/debot/risk_state.json"));
    }

    // bot-strategy#320: trade-stats fields round-trip through risk_state.json.
    #[test]
    fn risk_state_persists_trade_stats_round_trip() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");

        let mut instances = HashMap::new();
        instances.insert(
            "inst-a".to_string(),
            risk_io::InstanceRiskState {
                consecutive_losses: 3,
                total_trades: 42,
                total_wins: 25,
                total_pnl: 12.34,
                peak_pnl: 15.0,
                max_dd: 2.66,
                ..Default::default()
            },
        );

        risk_io::persist_risk_state(&path, None, &instances);
        let loaded = risk_io::load_risk_state(&path);

        let restored = loaded.instances.get("inst-a").expect("instance restored");
        assert_eq!(restored.total_trades, 42);
        assert_eq!(restored.total_wins, 25);
        assert!((restored.total_pnl - 12.34).abs() < 1e-9);
        assert!((restored.peak_pnl - 15.0).abs() < 1e-9);
        assert!((restored.max_dd - 2.66).abs() < 1e-9);
        assert_eq!(restored.consecutive_losses, 3);
    }

    // bot-strategy#320: an older snapshot without the trade-stats fields
    // must load cleanly with zeros, not panic on missing keys.
    #[test]
    fn risk_state_loads_pre_320_snapshot_with_default_trade_stats() {
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("risk_state.json");
        // v1 snapshot shape from before bot-strategy#320.
        let legacy = r#"{"_v":1,"instances":{"inst-a":{"consecutive_losses":7}}}"#;
        std::fs::write(&path, legacy).unwrap();

        let loaded = risk_io::load_risk_state(&path);
        let restored = loaded.instances.get("inst-a").expect("instance restored");
        assert_eq!(restored.consecutive_losses, 7);
        assert_eq!(restored.total_trades, 0);
        assert_eq!(restored.total_wins, 0);
        assert_eq!(restored.total_pnl, 0.0);
        assert_eq!(restored.peak_pnl, 0.0);
        assert_eq!(restored.max_dd, 0.0);
    }
}
