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

use super::super::kill_switch_path;
use super::super::risk_ack_path;
use super::super::risk_io;
use super::super::status;
use super::super::PairTradeEngine;
use super::super::StrategyInstance;
use super::super::EQUITY_REFRESH_CACHE_SECS;

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
        if self.cfg.observe_only {
            self.instances[inst_idx].last_equity_fetch = Some(Instant::now());
            return;
        }
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
    /// `session_start_equity` is preserved across subsequent rollovers: it
    /// already represents the fixed reference plus any flat-and-settled
    /// capital events observed since startup. Resetting it to the configured
    /// reference would resurrect withdrawn capital, then count a later
    /// redeposit again (bot-strategy#752). The configured reference is used
    /// only to initialise brand-new state; a reference change on restart is
    /// reconciled by `detect_capital_event_and_rebaseline` once the instance
    /// is flat and settled. Realized PnL still resets every session. See
    /// bot-strategy#185 Phase 2 and bot-strategy#222.
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
                let first_session = prior_day.is_none();
                if first_session {
                    inst.session_start_equity = inst.equity_reference_usd;
                    inst.session_equity_reference_usd = inst.equity_reference_usd;
                }
                let equity_base = inst.session_start_equity;
                let prev_pnl = inst.realized_pnl_today;
                inst.session_start_ts = now_ts;
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
                        "[DAILY_DD] {} session rolled: prev_pnl={:.4} -> reset; preserved session_start_equity={:.2}",
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
        let sentinel = kill_switch_path();
        let present = std::path::Path::new(sentinel).exists();
        if present && !self.kill_switch_active {
            log::warn!(
                "[KILL_SWITCH] activated: {} detected; new entries blocked, existing positions will exit normally",
                sentinel
            );
            self.kill_switch_active = true;
            self.record_risk_event_all_instances(
                "kill_switch",
                "activated",
                Some(sentinel.to_string()),
                None,
            );
        } else if !present && self.kill_switch_active {
            log::warn!(
                "[KILL_SWITCH] cleared: {} removed; new entries resumed",
                sentinel
            );
            self.kill_switch_active = false;
            self.record_risk_event_all_instances(
                "kill_switch",
                "cleared",
                Some(sentinel.to_string()),
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

    /// Consume the manual-ack sentinel (default `/opt/debot/RISK_ACK`,
    /// overridable via the `RISK_ACK_PATH` env var) if present and clear
    /// `session_halted` across all instances. The file is unconditionally
    /// removed so a stale ack from a prior incident never silently re-arms.
    /// See bot-strategy#185 Phase 3-2 / bot-strategy#488.
    pub(in crate::pairtrade) fn consume_risk_ack(&mut self) {
        if self.cfg.backtest_mode {
            return;
        }
        let ack_path = risk_ack_path();
        let path = std::path::Path::new(ack_path);
        if !path.exists() {
            return;
        }
        // Read the file (best-effort) so the ack reason makes it into the
        // journal — useful when chasing why a halt cleared days later.
        let payload = std::fs::read_to_string(path).unwrap_or_default();
        let trimmed = payload.trim();
        // bot-strategy#575 ②: an ack payload carrying `reanchor=true` (or
        // JSON `{"reanchor": true}`) additionally rebaselines the rolling
        // peak to the instance's current equity so DD → 0 on clear. Without
        // it, the ack re-breaches in the same tick whenever equity is still
        // below `peak × (1 − eff_threshold/10000)` — the exact failure the
        // 2026-06-15 #471 recovery hit (two acks both re-halted at the
        // boundary). Operator-gated and audited via the logged payload.
        let reanchor = ack_requests_reanchor(trimmed);
        let now_ts = self.current_now_ts();
        let mut cleared_any = false;
        let mut cleared_indices: Vec<(usize, String)> = Vec::new();
        for (inst_idx, inst) in self.instances.iter_mut().enumerate() {
            if inst.session_halted {
                let prior_reason = inst
                    .session_halt_reason
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                let reanchor_note = if reanchor {
                    let equity = inst.equity_cache;
                    reanchor_peak_samples(&mut inst.equity_samples, equity, now_ts);
                    // Keep the capital-event baseline consistent with the new
                    // peak so the next detection compares against current
                    // equity rather than the pre-ack value.
                    if equity > 0.0 {
                        inst.capital_baseline_equity = equity;
                        inst.capital_baseline_accounted_pnl =
                            Some(inst.total_pnl + inst.total_funding_carry);
                        inst.capital_position_seen_since_baseline = false;
                        inst.capital_rebaseline_deferred = false;
                        inst.capital_rebaseline_deferred_since = None;
                    }
                    format!(", peak re-anchored to equity={:.2} (DD→0)", equity)
                } else {
                    String::new()
                };
                log::warn!(
                    "[SESSION_DD] {} halt cleared by ack at {} (reason was: {}, ack payload: {:?}{})",
                    inst.id,
                    ack_path,
                    prior_reason,
                    trimmed,
                    reanchor_note
                );
                inst.session_halted = false;
                inst.session_halt_reason = None;
                inst.session_halt_ts = None;
                // bot-strategy#514: drop any unconsumed flatten marker with
                // the halt — after an ack the next snapshot clear belongs to
                // a new incident and must not inherit this halt's reason.
                inst.external_flatten_reason = None;
                cleared_any = true;
                cleared_indices.push((inst_idx, prior_reason));
            }
            // bot-strategy#721: the same ack sentinel clears the fail-closed
            // entry block left by an unrepaired entry-exposure mismatch.
            // The operator is expected to have verified/flattened the venue
            // position before acking (see runbook_risk_ack).
            if !inst.entry_blocked_pairs.is_empty() {
                for (pair_key, reason) in inst.entry_blocked_pairs.drain() {
                    log::warn!(
                        "[ENTRY_RECONCILE] {} {} entry block cleared by ack at {} (reason was: {})",
                        inst.id,
                        pair_key,
                        ack_path,
                        reason
                    );
                    crate::pairtrade::prom::ENTRY_EXPOSURE_BLOCKED
                        .with_label_values(&[inst.id.as_str(), pair_key.as_str()])
                        .set(0);
                }
                cleared_any = true;
            }
        }
        let ack_payload = if trimmed.is_empty() && !reanchor {
            None
        } else {
            Some(serde_json::json!({ "ack_payload": trimmed, "reanchor": reanchor }))
        };
        for (inst_idx, prior_reason) in cleared_indices {
            self.record_risk_event_for_instance(
                inst_idx,
                "session_dd",
                if reanchor { "ack_reanchor" } else { "ack" },
                Some(prior_reason),
                ack_payload.clone(),
            );
        }
        if let Err(e) = std::fs::remove_file(path) {
            log::warn!(
                "[SESSION_DD] failed to remove {} after ack: {:?}",
                ack_path,
                e
            );
        } else {
            log::info!("[SESSION_DD] {} consumed", ack_path);
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
        if self.cfg.backtest_mode || self.cfg.observe_only {
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
        //
        // bot-strategy#575 ③: this runs every tick regardless of
        // `session_halted`, so a halted (flat) instance keeps its latest
        // `equity_samples` entry synced to live collateral — the deposit
        // made while halted is visible to `detect_capital_event_and_rebaseline`
        // without a restart, instead of freezing at the persisted sample.
    }

    /// Detect a verified capital event (deposit / withdrawal / sub-account
    /// transfer) and rebaseline the rolling session-DD peak. bot-strategy#575
    /// ① / bot-strategy#783.
    ///
    /// A flat account can still move after a close: pairtrade records the
    /// realized fill PnL/funding before Lighter's cached account equity shows
    /// the corresponding settlement. Therefore raw `equity - baseline` is
    /// not capital evidence. We reconcile it against the round-scoped trade
    /// PnL and funding accumulated since the baseline:
    ///
    /// `inferred capital = raw equity delta - accounted PnL delta`.
    ///
    /// A delta is allowed to erase the DD peak only when it is still material
    /// and no material accounted-PnL movement makes the classification
    /// ambiguous. Ambiguous observations are audited and fail safe: the peak
    /// and daily denominator remain untouched until exchange equity catches
    /// up. A genuine transfer made after the prior PnL has reconciled retains
    /// the automatic #575 behavior.
    pub(in crate::pairtrade) fn detect_capital_event_and_rebaseline(&mut self, inst_idx: usize) {
        if self.cfg.backtest_mode {
            return;
        }
        // In live dry_run mode, exits are simulated: execute.rs's
        // exit_dry_run path still bumps total_pnl/total_funding_carry (the
        // two inputs to `accounted_pnl` below) via write_pnl_record, but
        // those simulated results never touch the connector-sourced
        // equity_cache real equity is read from, and never will -- a
        // dry-run trade has no matching real settlement to ever catch up
        // to. Marking capital_position_seen_since_baseline defers detection
        // (as a still-open real position does) rather than corrupting it,
        // but the gap it defers on only ever grows over a long dry-run
        // session, so the deferral never clears and capital-event detection
        // is permanently disabled for the rest of the session either way.
        // Skip it outright instead, so at least it is visibly off rather
        // than silently stuck (Codex P2 follow-up, bot-strategy#783).
        if self.cfg.dry_run {
            return;
        }
        let threshold_bps = self.cfg.risk.max_session_loss_bps;
        let daily_threshold_bps = self.cfg.risk.max_daily_loss_bps;
        let min_usd = self.cfg.risk.session_dd_capital_event_min_usd;
        let (legacy_reference, reference_reconciliation_pending) = {
            let inst = &self.instances[inst_idx];
            let legacy = inst.session_equity_reference_usd <= 0.0;
            (
                legacy,
                legacy
                    || (inst.session_equity_reference_usd - inst.equity_reference_usd).abs() > 1e-9,
            )
        };
        if (threshold_bps == 0 && daily_threshold_bps == 0 || min_usd <= 0.0)
            && !reference_reconciliation_pending
        {
            return;
        }
        let settle_secs = self.cfg.risk.session_dd_capital_settle_secs;
        let now_ts = self.current_now_ts();

        enum Rebaseline {
            Capital {
                delta: f64,
                equity: f64,
                prev_start_equity: f64,
                reference_change: Option<(f64, f64)>,
                reconciliation: CapitalReconciliation,
            },
            Reference {
                equity: f64,
                prev_start_equity: f64,
                previous_reference: f64,
                new_reference: f64,
                observed_delta: Option<f64>,
            },
            Deferred {
                equity: f64,
                baseline_equity: f64,
                reconciliation: CapitalReconciliation,
                baseline_advanced: bool,
            },
            DeferredCleared {
                equity: f64,
                reconciliation: CapitalReconciliation,
            },
            /// A deferred ambiguity that could never satisfy
            /// `baseline_advanced` (the accounted delta itself is material,
            /// e.g. a real transfer landed alongside a material close PnL)
            /// stayed unresolved for `CAPITAL_REBASELINE_GIVEUP_SECS`. The
            /// anchor is force-advanced without crediting either
            /// accounting or a transfer, so future capital events remain
            /// detectable instead of being blocked forever.
            DeferredGivenUp {
                equity: f64,
                reconciliation: CapitalReconciliation,
                stuck_secs: u64,
            },
            BaselineMigrated {
                previous_equity: f64,
                equity: f64,
                accounted_pnl: f64,
                raw_equity_delta: f64,
                reference_change: Option<(f64, f64)>,
                prev_start_equity: f64,
            },
            /// A settled trade/funding move reconciled cleanly with no
            /// reference change and no prior deferral -- nothing to log or
            /// report, but the paired baseline was advanced in memory and
            /// must still be persisted (see the comment at its call site).
            ReconciledQuietly,
            /// `capital_position_seen_since_baseline` just latched false-to-
            /// true (a position opened after the last settled baseline) --
            /// nothing to log, but this transition must survive a restart
            /// (see the comment at its call site).
            PositionActivityLatched,
        }

        let detected: Option<Rebaseline> = 'reconcile: {
            let inst = &mut self.instances[inst_idx];
            if !inst.equity_initialized {
                return;
            }
            let equity = inst.equity_cache;
            if equity <= 0.0 {
                return;
            }
            let flat = inst.states.values().all(|s| {
                s.position.is_none() && s.pending_entry.is_none() && s.pending_exit.is_none()
            });
            if !flat {
                // Preserve the last settled baseline while the position is
                // open. Unrealized PnL makes the current reading unusable,
                // but the old baseline is exactly what lets the next flat
                // observation reconcile the newly realized PnL. Only the
                // false-to-true transition needs persisting (and only that
                // transition -- not every tick a position stays open): if
                // the service stops while this flag is true only in memory,
                // restart restores the last-persisted (false) value, and a
                // startup force-close that flattens without recording
                // realized PnL then gets misclassified as a verified
                // deposit/withdrawal instead of correctly deferring (Codex
                // P1 follow-up, bot-strategy#783).
                let was_seen = std::mem::replace(&mut inst.capital_position_seen_since_baseline, true);
                inst.flat_since = None;
                break 'reconcile if was_seen {
                    None
                } else {
                    Some(Rebaseline::PositionActivityLatched)
                };
            }
            let settled = match inst.flat_since {
                Some(since) => since.elapsed().as_secs() >= settle_secs,
                None => {
                    inst.flat_since = Some(Instant::now());
                    settle_secs == 0
                }
            };
            if !settled {
                return;
            }

            let accounted_pnl = inst.total_pnl + inst.total_funding_carry;
            let baseline = inst.capital_baseline_equity;
            if baseline <= 0.0 {
                inst.capital_baseline_equity = equity;
                inst.capital_baseline_accounted_pnl = Some(accounted_pnl);
                inst.capital_position_seen_since_baseline = false;
                inst.capital_rebaseline_deferred = false;
                inst.capital_rebaseline_deferred_since = None;
                if reference_reconciliation_pending {
                    let prev_start_equity = inst.session_start_equity;
                    let previous_reference = inst.session_equity_reference_usd;
                    if !legacy_reference {
                        inst.session_start_equity = inst.equity_reference_usd;
                    }
                    inst.session_equity_reference_usd = inst.equity_reference_usd;
                    Some(Rebaseline::Reference {
                        equity,
                        prev_start_equity,
                        previous_reference,
                        new_reference: inst.equity_reference_usd,
                        observed_delta: None,
                    })
                } else {
                    None
                }
            } else {
                let raw_equity_delta = equity - baseline;
                let baseline_accounted = inst.capital_baseline_accounted_pnl;

                match baseline_accounted {
                    None => {
                        // A pre-#783 snapshot does not say how much accounted PnL
                        // was already reflected in its raw equity baseline. Seed a
                        // guarded paired baseline without erasing DD. One further
                        // stable settle window clears the guard; any intervening
                        // balance movement advances the candidate, never the peak.
                        let prev_start_equity = inst.session_start_equity;
                        let previous_reference = inst.session_equity_reference_usd;
                        let reference_change = reference_reconciliation_pending
                            .then_some((previous_reference, inst.equity_reference_usd));
                        if reference_reconciliation_pending {
                            let current_capital_basis = equity - accounted_pnl;
                            let legacy_denominator_trustworthy = legacy_reference
                                && (prev_start_equity - current_capital_basis).abs() < min_usd;
                            if !legacy_denominator_trustworthy {
                                inst.session_start_equity = inst.equity_reference_usd;
                            }
                            inst.session_equity_reference_usd = inst.equity_reference_usd;
                        }
                        inst.capital_baseline_equity = equity;
                        inst.capital_baseline_accounted_pnl = Some(accounted_pnl);
                        inst.capital_position_seen_since_baseline = true;
                        inst.capital_rebaseline_deferred = false;
                        inst.capital_rebaseline_deferred_since = None;
                        inst.flat_since = Some(Instant::now());
                        Some(Rebaseline::BaselineMigrated {
                            previous_equity: baseline,
                            equity,
                            accounted_pnl,
                            raw_equity_delta,
                            reference_change,
                            prev_start_equity,
                        })
                    }
                    Some(baseline_accounted_pnl) => {
                        let reconciliation = reconcile_capital_delta(
                            baseline,
                            equity,
                            baseline_accounted_pnl,
                            accounted_pnl,
                            inst.capital_position_seen_since_baseline,
                            min_usd,
                        );

                        match reconciliation.disposition {
                            CapitalDisposition::Reconciled => {
                                // Trade/funding settlement (or sub-threshold
                                // noise) is now reflected by exchange equity.
                                // Advance both halves of the paired baseline.
                                // Captured before mutation so the ordinary
                                // (reference-unchanged, not-previously-
                                // deferred) branch below can tell whether
                                // this tick actually moved anything -- on a
                                // truly idle account (equity_cache untouched
                                // since the last tick, which is common: it's
                                // only refreshed on its own cache cadence,
                                // not every gating cycle) nothing here
                                // changes at all, and persisting anyway would
                                // rewrite the whole risk-state file every
                                // single tick for every instance (Codex P2
                                // follow-up, bot-strategy#783).
                                let baseline_or_latch_changed = inst.capital_baseline_equity
                                    != equity
                                    || inst.capital_baseline_accounted_pnl != Some(accounted_pnl)
                                    || inst.capital_position_seen_since_baseline;
                                inst.capital_baseline_equity = equity;
                                inst.capital_baseline_accounted_pnl = Some(accounted_pnl);
                                let was_deferred =
                                    std::mem::replace(&mut inst.capital_rebaseline_deferred, false);
                                inst.capital_rebaseline_deferred_since = None;
                                // A position closing does not guarantee its
                                // effect on equity has landed yet:
                                // equity_cache only refreshes every
                                // EQUITY_REFRESH_CACHE_SECS, so the very
                                // first quiet observation right after a
                                // close (e.g. a startup force-close, or the
                                // guard a pre-#783 migration seeds
                                // unconditionally) reflects that the cache
                                // simply has not been checked against fresh
                                // equity yet -- not that accounting has
                                // genuinely reconciled with it. Clearing the
                                // guard here regardless would let a *later*,
                                // unrelated capital event slip through
                                // misclassified once the stale cache finally
                                // refreshes. Only clear it once either a
                                // real movement was actually observed and
                                // resolved via the deferred path above, or
                                // enough time has passed since becoming flat
                                // that the cache has had a full chance to
                                // catch up (Codex P1 follow-up,
                                // bot-strategy#783).
                                if was_deferred
                                    || !reconciliation.position_seen_since_baseline
                                    || inst.flat_since.is_some_and(|since| {
                                        since.elapsed().as_secs() >= EQUITY_REFRESH_CACHE_SECS
                                    })
                                {
                                    inst.capital_position_seen_since_baseline = false;
                                }
                                if reference_reconciliation_pending {
                                    let prev_start_equity = inst.session_start_equity;
                                    let previous_reference = inst.session_equity_reference_usd;
                                    let current_capital_basis = equity - accounted_pnl;
                                    let legacy_denominator_trustworthy = legacy_reference
                                        && (prev_start_equity - current_capital_basis).abs()
                                            < min_usd;
                                    if !legacy_denominator_trustworthy {
                                        inst.session_start_equity = inst.equity_reference_usd;
                                    }
                                    inst.session_equity_reference_usd = inst.equity_reference_usd;
                                    Some(Rebaseline::Reference {
                                        equity,
                                        prev_start_equity,
                                        previous_reference,
                                        new_reference: inst.equity_reference_usd,
                                        observed_delta: Some(reconciliation.inferred_capital_delta),
                                    })
                                } else if was_deferred {
                                    Some(Rebaseline::DeferredCleared {
                                        equity,
                                        reconciliation,
                                    })
                                } else if baseline_or_latch_changed {
                                    // The ordinary case: a settled trade/funding
                                    // move reconciled cleanly with no reference
                                    // change and no prior deferral, nothing worth
                                    // logging. But the paired baseline above was
                                    // still advanced in memory -- without
                                    // persisting it here, a restart between this
                                    // tick and the next *interesting* rebaseline
                                    // would reload the previous, now-stale
                                    // baseline, double-count everything settled
                                    // since, and (per the Ambiguous branch's own
                                    // guard) can leave a later real deposit stuck
                                    // Ambiguous forever (Codex P1 follow-up,
                                    // bot-strategy#783).
                                    Some(Rebaseline::ReconciledQuietly)
                                } else {
                                    // Nothing moved at all: same equity, same
                                    // accounted PnL, no latch to clear. Skip
                                    // the write entirely.
                                    None
                                }
                            }
                            CapitalDisposition::Ambiguous => {
                                // When the only ambiguity is prior position
                                // activity, move the guarded candidate to the
                                // latest flat reading and require another settle
                                // window. This eventually clears missing-accounting
                                // recovery paths without ever reanchoring the peak.
                                //
                                // Gating purely on "this tick's accounted
                                // delta is individually sub-threshold" used
                                // to let repeated small closes each slip
                                // under min_usd and independently advance
                                // the anchor, the same accumulation bug the
                                // no-position Reconciled path had (fixed
                                // above) -- e.g. two $4 wins before the
                                // 300-second equity-cache refresh, each
                                // advancing the accounted baseline while
                                // equity stays put, so the eventual delayed
                                // $8 refresh reconciles against an anchor
                                // that already silently absorbed both closes
                                // instead of being compared as a whole.
                                // Mirrors the no-position Reconciled path's
                                // own guard exactly: a quick advance is only
                                // trusted when nothing new is unaccounted
                                // for THIS tick (accounted_pnl_delta is
                                // genuinely ~0 -- equity is simply catching
                                // up to what the accounting already fully
                                // explained, e.g. a delayed-settlement
                                // migration candidate). Any other
                                // sub-threshold-but-nonzero accounted move
                                // must instead wait for flat_since to show a
                                // full EQUITY_REFRESH_CACHE_SECS chance for
                                // equity to catch up with *everything*
                                // realized since the position closed, not
                                // just this tick's own delta (Codex P1
                                // follow-up, bot-strategy#783).
                                let accounted_delta_settled =
                                    reconciliation.accounted_pnl_delta.abs() <= CAPITAL_DELTA_EPSILON;
                                let position_guard_can_clear = accounted_delta_settled
                                    || inst.flat_since.is_some_and(|since| {
                                        since.elapsed().as_secs() >= EQUITY_REFRESH_CACHE_SECS
                                    });
                                let baseline_advanced = reconciliation.position_seen_since_baseline
                                    && reconciliation.accounted_pnl_delta.abs() < min_usd
                                    && position_guard_can_clear;
                                if baseline_advanced {
                                    // Advance the guarded *candidate* only --
                                    // capital_position_seen_since_baseline
                                    // stays latched. Clearing it is left to
                                    // the Reconciled branch's own
                                    // was_deferred path on a *subsequent*
                                    // tick, once this now-advanced candidate
                                    // itself holds steady through another
                                    // observation, rather than trusting a
                                    // single tick's advance as sufficient.
                                    inst.capital_baseline_equity = equity;
                                    inst.capital_baseline_accounted_pnl = Some(accounted_pnl);
                                    inst.flat_since = Some(Instant::now());
                                }
                                let was_deferred = inst.capital_rebaseline_deferred;
                                if !was_deferred || baseline_advanced {
                                    // A fresh deferral, or the candidate just
                                    // advanced to a new point: (re)start the
                                    // give-up clock below so a run of
                                    // harmless small-PnL advances never
                                    // counts toward giving up on a
                                    // different, still-unresolved ambiguity.
                                    inst.capital_rebaseline_deferred_since = Some(Instant::now());
                                }
                                inst.capital_rebaseline_deferred = true;

                                // Some ambiguous observations can never
                                // satisfy baseline_advanced: it requires the
                                // accounted delta itself to be sub-threshold,
                                // which a genuine material close PnL landing
                                // alongside a real transfer never is. Left
                                // unresolved, that leaves the account
                                // deferred forever with no path to detect
                                // any later, unrelated capital event either
                                // (Codex P2 follow-up, bot-strategy#783).
                                // After a long, continuous deferred streak,
                                // give up: advance the anchor to the current
                                // reading so future events remain
                                // detectable. session_start_equity and the
                                // DD peak are deliberately left untouched --
                                // this specific event cannot be disentangled
                                // between accounting and a transfer, so
                                // neither is credited.
                                let stuck_secs = inst
                                    .capital_rebaseline_deferred_since
                                    .map(|since| since.elapsed().as_secs())
                                    .unwrap_or(0);
                                let gave_up =
                                    !baseline_advanced && stuck_secs >= CAPITAL_REBASELINE_GIVEUP_SECS;

                                if gave_up {
                                    inst.capital_baseline_equity = equity;
                                    inst.capital_baseline_accounted_pnl = Some(accounted_pnl);
                                    inst.capital_position_seen_since_baseline = false;
                                    inst.capital_rebaseline_deferred = false;
                                    inst.capital_rebaseline_deferred_since = None;
                                    Some(Rebaseline::DeferredGivenUp {
                                        equity,
                                        reconciliation,
                                        stuck_secs,
                                    })
                                } else if was_deferred && !baseline_advanced {
                                    None
                                } else {
                                    Some(Rebaseline::Deferred {
                                        equity,
                                        baseline_equity: baseline,
                                        reconciliation,
                                        baseline_advanced,
                                    })
                                }
                            }
                            CapitalDisposition::Verified(delta) => {
                                if threshold_bps > 0 {
                                    reanchor_peak_samples(&mut inst.equity_samples, equity, now_ts);
                                }
                                let prev_start_equity = inst.session_start_equity;
                                if legacy_reference {
                                    let baseline_capital_basis = baseline - baseline_accounted_pnl;
                                    let legacy_denominator_trustworthy =
                                        (prev_start_equity - baseline_capital_basis).abs()
                                            < min_usd;
                                    inst.session_start_equity = if legacy_denominator_trustworthy {
                                        (prev_start_equity + delta).max(0.0)
                                    } else {
                                        inst.equity_reference_usd
                                    };
                                } else {
                                    inst.session_start_equity =
                                        (inst.session_start_equity + delta).max(0.0);
                                }
                                inst.capital_baseline_equity = equity;
                                inst.capital_baseline_accounted_pnl = Some(accounted_pnl);
                                inst.capital_position_seen_since_baseline = false;
                                inst.capital_rebaseline_deferred = false;
                                inst.capital_rebaseline_deferred_since = None;
                                let reference_change =
                                    reference_reconciliation_pending.then_some({
                                        (
                                            inst.session_equity_reference_usd,
                                            inst.equity_reference_usd,
                                        )
                                    });
                                if reference_reconciliation_pending {
                                    inst.session_equity_reference_usd = inst.equity_reference_usd;
                                }
                                Some(Rebaseline::Capital {
                                    delta,
                                    equity,
                                    prev_start_equity,
                                    reference_change,
                                    reconciliation,
                                })
                            }
                        }
                    }
                }
            }
        };

        match detected {
            Some(Rebaseline::Capital {
                delta,
                equity,
                prev_start_equity,
                reference_change,
                reconciliation,
            }) => {
                let kind = if delta > 0.0 { "deposit" } else { "withdrawal" };
                let new_start_equity = self.instances[inst_idx].session_start_equity;
                log::warn!(
                    "[SESSION_DD] {} verified capital {}: inferred={:.2} raw_equity_delta={:.2} accounted_pnl_delta={:.2} equity={:.2}; rolling peak rebaselined to current (DD→0), session_start_equity {:.2} -> {:.2}",
                    self.instances[inst_idx].id,
                    kind,
                    delta,
                    reconciliation.raw_equity_delta,
                    reconciliation.accounted_pnl_delta,
                    equity,
                    prev_start_equity,
                    new_start_equity,
                );
                self.record_risk_event_for_instance(
                    inst_idx,
                    "session_dd",
                    "capital_rebaseline",
                    Some(kind.to_string()),
                    Some(serde_json::json!({
                        "evidence": "equity_minus_realized_pnl_and_funding",
                        "delta_usd": delta,
                        "raw_equity_delta_usd": reconciliation.raw_equity_delta,
                        "accounted_pnl_delta_usd": reconciliation.accounted_pnl_delta,
                        "inferred_capital_delta_usd": reconciliation.inferred_capital_delta,
                        "position_seen_since_baseline": reconciliation.position_seen_since_baseline,
                        "equity": equity,
                        "prev_session_start_equity": prev_start_equity,
                        "new_session_start_equity": new_start_equity,
                        "reference_change": reference_change.map(|(previous, new)| serde_json::json!({
                            "previous": previous,
                            "new": new,
                        })),
                    })),
                );
                self.persist_risk_state();
            }
            Some(Rebaseline::Reference {
                equity,
                prev_start_equity,
                previous_reference,
                new_reference,
                observed_delta,
            }) => {
                let source = if previous_reference <= 0.0 {
                    "legacy_snapshot"
                } else {
                    "config_reference_change"
                };
                log::warn!(
                    "[DAILY_DD] {} equity reference reconciled source={} {:.2} -> {:.2} while flat/settled at equity={:.2} observed_delta={:?}; session_start_equity {:.2} -> {:.2}",
                    self.instances[inst_idx].id,
                    source,
                    previous_reference,
                    new_reference,
                    equity,
                    observed_delta,
                    prev_start_equity,
                    self.instances[inst_idx].session_start_equity,
                );
                self.record_risk_event_for_instance(
                    inst_idx,
                    "daily_dd",
                    "reference_rebaseline",
                    Some(source.to_string()),
                    Some(serde_json::json!({
                        "equity": equity,
                        "previous_reference": previous_reference,
                        "new_reference": new_reference,
                        "observed_delta_usd": observed_delta,
                        "prev_session_start_equity": prev_start_equity,
                        "new_session_start_equity": self.instances[inst_idx].session_start_equity,
                    })),
                );
                self.persist_risk_state();
            }
            Some(Rebaseline::Deferred {
                equity,
                baseline_equity,
                reconciliation,
                baseline_advanced,
            }) => {
                let action = if baseline_advanced {
                    "advance_guarded_baseline_retain_peak_and_denominator"
                } else {
                    "retain_peak_and_denominator"
                };
                log::warn!(
                    "[SESSION_DD] {} capital rebaseline deferred: raw_equity_delta={:.2} accounted_pnl_delta={:.2} inferred_capital_delta={:.2} baseline={:.2} equity={:.2} action={}",
                    self.instances[inst_idx].id,
                    reconciliation.raw_equity_delta,
                    reconciliation.accounted_pnl_delta,
                    reconciliation.inferred_capital_delta,
                    baseline_equity,
                    equity,
                    action,
                );
                self.record_risk_event_for_instance(
                    inst_idx,
                    "session_dd",
                    "capital_rebaseline_deferred",
                    Some("ambiguous_with_accounting_or_position_activity".to_string()),
                    Some(serde_json::json!({
                        "evidence": "equity_minus_realized_pnl_and_funding",
                        "raw_equity_delta_usd": reconciliation.raw_equity_delta,
                        "accounted_pnl_delta_usd": reconciliation.accounted_pnl_delta,
                        "inferred_capital_delta_usd": reconciliation.inferred_capital_delta,
                        "position_seen_since_baseline": reconciliation.position_seen_since_baseline,
                        "baseline_equity": baseline_equity,
                        "equity": equity,
                        "action": action,
                    })),
                );
                self.persist_risk_state();
            }
            Some(Rebaseline::DeferredGivenUp {
                equity,
                reconciliation,
                stuck_secs,
            }) => {
                log::warn!(
                    "[SESSION_DD] {} capital rebaseline deferral gave up after {}s stuck (limit {}s): raw_equity_delta={:.2} accounted_pnl_delta={:.2} inferred_capital_delta={:.2} equity={:.2}; anchor force-advanced, neither accounting nor a transfer credited for this event",
                    self.instances[inst_idx].id,
                    stuck_secs,
                    CAPITAL_REBASELINE_GIVEUP_SECS,
                    reconciliation.raw_equity_delta,
                    reconciliation.accounted_pnl_delta,
                    reconciliation.inferred_capital_delta,
                    equity,
                );
                self.record_risk_event_for_instance(
                    inst_idx,
                    "session_dd",
                    "capital_rebaseline_deferred_gave_up",
                    Some("unresolvable_ambiguity_timeout".to_string()),
                    Some(serde_json::json!({
                        "evidence": "equity_minus_realized_pnl_and_funding",
                        "raw_equity_delta_usd": reconciliation.raw_equity_delta,
                        "accounted_pnl_delta_usd": reconciliation.accounted_pnl_delta,
                        "inferred_capital_delta_usd": reconciliation.inferred_capital_delta,
                        "position_seen_since_baseline": reconciliation.position_seen_since_baseline,
                        "equity": equity,
                        "stuck_secs": stuck_secs,
                        "giveup_limit_secs": CAPITAL_REBASELINE_GIVEUP_SECS,
                        "action": "force_advance_anchor_no_credit",
                    })),
                );
                self.persist_risk_state();
            }
            Some(Rebaseline::DeferredCleared {
                equity,
                reconciliation,
            }) => {
                log::info!(
                    "[SESSION_DD] {} deferred capital classification cleared after equity/PnL reconciliation: raw_equity_delta={:.2} accounted_pnl_delta={:.2} inferred_capital_delta={:.2} equity={:.2}",
                    self.instances[inst_idx].id,
                    reconciliation.raw_equity_delta,
                    reconciliation.accounted_pnl_delta,
                    reconciliation.inferred_capital_delta,
                    equity,
                );
                self.record_risk_event_for_instance(
                    inst_idx,
                    "session_dd",
                    "capital_rebaseline_deferred_cleared",
                    Some("equity_pnl_reconciled".to_string()),
                    Some(serde_json::json!({
                        "raw_equity_delta_usd": reconciliation.raw_equity_delta,
                        "accounted_pnl_delta_usd": reconciliation.accounted_pnl_delta,
                        "inferred_capital_delta_usd": reconciliation.inferred_capital_delta,
                        "position_seen_since_baseline": reconciliation.position_seen_since_baseline,
                        "equity": equity,
                        "action": "baseline_advanced_without_reanchor",
                    })),
                );
                self.persist_risk_state();
            }
            Some(Rebaseline::BaselineMigrated {
                previous_equity,
                equity,
                accounted_pnl,
                raw_equity_delta,
                reference_change,
                prev_start_equity,
            }) => {
                log::warn!(
                    "[SESSION_DD] {} pre-#783 capital baseline migrated without reanchor: baseline={:.2} equity={:.2} raw_delta={:.2} accounted_pnl={:.2}; guarded until a stable settle window, retaining DD peak",
                    self.instances[inst_idx].id,
                    previous_equity,
                    equity,
                    raw_equity_delta,
                    accounted_pnl,
                );
                self.record_risk_event_for_instance(
                    inst_idx,
                    "session_dd",
                    "capital_baseline_migrated",
                    Some("pre_783_snapshot_unpaired".to_string()),
                    Some(serde_json::json!({
                        "previous_baseline_equity": previous_equity,
                        "equity": equity,
                        "raw_equity_delta_usd": raw_equity_delta,
                        "accounted_pnl": accounted_pnl,
                        "reference_change": reference_change.map(|(previous, new)| serde_json::json!({
                            "previous": previous,
                            "new": new,
                        })),
                        "prev_session_start_equity": prev_start_equity,
                        "new_session_start_equity": self.instances[inst_idx].session_start_equity,
                        "action": "seed_guarded_paired_baseline_without_reanchor",
                    })),
                );
                self.persist_risk_state();
            }
            Some(Rebaseline::ReconciledQuietly) => {
                self.persist_risk_state();
            }
            Some(Rebaseline::PositionActivityLatched) => {
                self.persist_risk_state();
            }
            None => {}
        }
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
        if self.cfg.backtest_mode || self.cfg.observe_only {
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
            inst.id, current, peak, dd_bps, threshold_bps, leverage, effective_threshold_bps, risk_ack_path()
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
            Some(reason.clone()),
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
                // bot-strategy#514: tag the out-of-band flatten so the
                // exchange-snapshot clear writes a recovery_no_pnl record
                // with the real trigger instead of `exchange_snapshot_clear`.
                // Armed only after a successful close submission AND only
                // when a local position exists for the snapshot clear to
                // consume it on — otherwise the one-shot marker would leak
                // onto a future unrelated clear and mislabel it.
                let inst_mut = &mut self.instances[inst_idx];
                if inst_mut.states.values().any(|s| s.position.is_some()) {
                    inst_mut.external_flatten_reason = Some(reason.clone());
                }
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

/// Collapse the rolling-peak window to a single sample at `equity` so the
/// next `rolling_peak` computes peak == current (DD → 0). Used by both the
/// deposit-aware rebaseline (bot-strategy#575 ①) and the RISK_ACK re-anchor
/// path (②). A non-positive `equity` just clears the window — peak becomes
/// whatever the next real sample is.
pub(in crate::pairtrade) fn reanchor_peak_samples(
    samples: &mut Vec<risk_io::EquitySample>,
    equity: f64,
    ts: i64,
) {
    samples.clear();
    if equity > 0.0 {
        samples.push(risk_io::EquitySample { ts, equity });
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct CapitalReconciliation {
    raw_equity_delta: f64,
    accounted_pnl_delta: f64,
    inferred_capital_delta: f64,
    position_seen_since_baseline: bool,
    disposition: CapitalDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CapitalDisposition {
    /// Equity and the bot's realized trade/funding accounting agree within
    /// the configured materiality threshold. Advance the paired baseline;
    /// do not erase the rolling DD peak.
    Reconciled,
    /// A material inferred capital delta with no material accounting move in
    /// flight. This is strong enough for the existing automatic rebaseline.
    Verified(f64),
    /// Both accounted PnL/funding and inferred capital moved materially.
    /// Equity-only data cannot distinguish a transfer from delayed or
    /// imperfect settlement, so rebaseline must fail safe.
    Ambiguous,
}

/// Floating-point tolerance for "is this value exactly zero" checks in
/// `reconcile_capital_delta`, distinct from the operator-configured
/// `min_usd` materiality threshold: `min_usd` decides whether a residual is
/// large enough to matter; this decides whether it is genuinely zero (safe
/// to forget) versus merely small (must be retained -- see
/// `inferred_capital_delta`'s use in the disposition match).
const CAPITAL_DELTA_EPSILON: f64 = 1e-9;

/// How long a deferred ambiguity may stay unresolved before
/// `detect_capital_event_and_rebaseline` gives up on disentangling it from
/// accounting and force-advances the anchor anyway (see
/// `capital_rebaseline_deferred_since`'s doc comment on `StrategyInstance`).
/// Deliberately independent of the operator-configured
/// `session_dd_capital_settle_secs`, which can legitimately be 0 for fast
/// detection -- multiplying a 0 settle window would give up immediately,
/// defeating the fail-safe deferral this is meant to eventually escape
/// rather than replace.
pub(in crate::pairtrade) const CAPITAL_REBASELINE_GIVEUP_SECS: u64 = 900;

/// Reconcile a flat account-equity movement against trade/funding accounting.
/// Raw equity changes alone are not capital evidence because connector balance
/// caching can publish the close PnL several minutes after pairtrade records
/// the fill. bot-strategy#783.
fn reconcile_capital_delta(
    baseline_equity: f64,
    current_equity: f64,
    baseline_accounted_pnl: f64,
    current_accounted_pnl: f64,
    position_seen_since_baseline: bool,
    min_usd: f64,
) -> CapitalReconciliation {
    let raw_equity_delta = current_equity - baseline_equity;
    let accounted_pnl_delta = current_accounted_pnl - baseline_accounted_pnl;
    let inferred_capital_delta = raw_equity_delta - accounted_pnl_delta;
    let baseline_capital_basis = baseline_equity - baseline_accounted_pnl;
    let current_capital_basis = current_equity - current_accounted_pnl;
    let disposition =
        match classify_capital_basis_delta(baseline_capital_basis, current_capital_basis, min_usd)
        {
            // The basis is within min_usd, but that alone does not mean
            // there is zero outstanding debt -- only that whatever debt
            // exists is currently small enough to hide under the
            // materiality threshold. Resetting the anchor to the current
            // (equity, accounted_pnl) point here discards that residual
            // entirely; a run of several such resets, each individually
            // sub-threshold but all draining equity's catch-up in the same
            // direction (accounted moves ahead, equity trails behind by a
            // few dollars every tick), can accumulate a materially larger
            // gap that never gets compared as a whole, since each tick's
            // reset erases the previous one's contribution before the next
            // is even measured. Both conditions matter: accounted_pnl_delta
            // alone catches "did accounting move at all," but a pure
            // equity-only drift (accounted_pnl_delta == 0, e.g. ordinary
            // mark noise) must still reconcile normally, or a still-open-
            // position latch that only ever clears via Reconciled/Verified
            // would never clear. inferred_capital_delta alone (the earlier
            // version of this guard) instead falsely flagged that same
            // pure equity drift, since it is nonzero whenever raw equity
            // and accounted PnL merely *differ* rather than specifically
            // when accounting moved. Requiring both: accounting genuinely
            // moved this tick AND equity has not (yet) moved enough to
            // fully explain it relative to the retained anchor -- is what
            // distinguishes "still accumulating debt" (defer) from "this
            // tick's equity move fully explains the retained debt" (safe
            // to reconcile, however that debt built up across however many
            // prior deferred ticks) (Codex P1 follow-up, bot-strategy#783).
            None if accounted_pnl_delta.abs() > CAPITAL_DELTA_EPSILON
                && inferred_capital_delta.abs() > CAPITAL_DELTA_EPSILON =>
            {
                CapitalDisposition::Ambiguous
            }
            None => CapitalDisposition::Reconciled,
            Some(_) if accounted_pnl_delta.abs() >= min_usd || position_seen_since_baseline => {
                CapitalDisposition::Ambiguous
            }
            Some(delta) => CapitalDisposition::Verified(delta),
        };
    CapitalReconciliation {
        raw_equity_delta,
        accounted_pnl_delta,
        inferred_capital_delta,
        position_seen_since_baseline,
        disposition,
    }
}

/// Whether an unexplained delta in a derived capital-*basis* value
/// (`equity - accounted PnL`) observed while flat-and-settled is a capital
/// event (deposit / withdrawal). Returns the signed delta when
/// `|current − baseline| ≥ min_usd`, else `None`. Pure split of the
/// gating maths in `reconcile_capital_delta` for unit testing.
///
/// Deliberately has no non-positivity guard on `baseline`/`current`: unlike
/// raw equity, a capital basis can legitimately be zero or negative -- e.g.
/// a baseline of $1,053.27 with $53.27 already-accounted realized PnL
/// (basis $1,000), followed by a withdrawal down to $0.01 equity with that
/// same accounted PnL still outstanding (basis -$53.26) -- without that
/// meaning "reference not yet established". An earlier version of this
/// reconciliation reused a raw-equity-oriented `baseline <= 0.0 || current
/// <= 0.0` guard here, which silently reclassified exactly that withdrawal
/// as `Reconciled` instead of `Verified`, and then -- because the paired
/// baseline advances to the same negative basis on every following
/// observation -- permanently blocked capital-event detection for the rest
/// of the round, since every subsequent baseline stayed <= 0 too (a later
/// redeposit would have been silently ignored the same way). The "not yet
/// established" case for this call site is already handled upstream, by
/// `capital_baseline_equity <= 0.0` and `capital_baseline_accounted_pnl ==
/// None`, so no non-positivity guard belongs here at all (Codex P1
/// follow-up, bot-strategy#783).
fn classify_capital_basis_delta(baseline: f64, current: f64, min_usd: f64) -> Option<f64> {
    if min_usd <= 0.0 {
        return None;
    }
    let delta = current - baseline;
    if delta.abs() >= min_usd {
        Some(delta)
    } else {
        None
    }
}

/// Parse a RISK_ACK payload for the optional re-anchor request
/// (bot-strategy#575 ②). Accepts JSON `{"reanchor": true}` or a plain-text
/// `reanchor=true` / `reanchor: true` token (case-insensitive), so an
/// operator can drop either form. Any other payload (including an empty
/// file) leaves today's clear-flag-only behaviour intact.
pub(in crate::pairtrade) fn ack_requests_reanchor(payload: &str) -> bool {
    let trimmed = payload.trim();
    if trimmed.is_empty() {
        return false;
    }
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if value.get("reanchor").and_then(|v| v.as_bool()) == Some(true) {
            return true;
        }
    }
    let lower = trimmed.to_ascii_lowercase();
    lower.contains("reanchor=true")
        || lower.contains("reanchor: true")
        || lower.contains("reanchor:true")
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

    // bot-strategy#783: delayed close settlement is not a capital event.
    #[test]
    fn capital_reconciliation_matches_realized_pnl_and_funding() {
        let reconciled = reconcile_capital_delta(1_000.0, 1_025.0, 0.0, 25.0, false, 5.0);
        assert_eq!(reconciled.disposition, CapitalDisposition::Reconciled);
        assert!((reconciled.inferred_capital_delta).abs() < 1e-9);
    }

    #[test]
    fn capital_reconciliation_defers_stale_equity_after_close() {
        let deferred = reconcile_capital_delta(1_000.0, 1_000.0, 0.0, 25.0, false, 5.0);
        assert_eq!(deferred.disposition, CapitalDisposition::Ambiguous);
        assert!((deferred.inferred_capital_delta - (-25.0)).abs() < 1e-9);
    }

    #[test]
    fn capital_reconciliation_verifies_transfer_without_accounting_move() {
        let deposit = reconcile_capital_delta(1_000.0, 1_500.0, 25.0, 25.0, false, 5.0);
        assert_eq!(deposit.disposition, CapitalDisposition::Verified(500.0));
    }

    #[test]
    fn capital_reconciliation_defers_unaccounted_move_after_position() {
        let deferred = reconcile_capital_delta(1_000.0, 950.0, 0.0, 0.0, true, 5.0);
        assert_eq!(deferred.disposition, CapitalDisposition::Ambiguous);
        assert!(deferred.position_seen_since_baseline);
    }

    // bot-strategy#783 (Codex P1 follow-up): equity is bit-for-bit unchanged
    // (not just "small" -- exactly the same cached value, i.e. it has not
    // been refreshed yet) while a $4 accounted move keeps the capital basis
    // itself under the $5 threshold. Reconciling this silently would let a
    // second such move accumulate past min_usd completely unnoticed, since
    // each individual sub-threshold move would keep resetting the anchor it
    // is measured against.
    #[test]
    fn capital_reconciliation_defers_a_sub_threshold_accounted_move_with_frozen_equity() {
        let deferred = reconcile_capital_delta(1_000.0, 1_000.0, 0.0, 4.0, false, 5.0);
        assert_eq!(deferred.disposition, CapitalDisposition::Ambiguous);
    }

    // The accumulation scenario end to end: two individually sub-threshold
    // wins ($4 each against a $5 threshold) while equity_cache stays frozen
    // must not silently reconcile away the growing gap, and once equity
    // finally catches up to the full $8, that must resolve as delayed
    // settlement -- not a $8 verified deposit that resets the DD peak and
    // inflates the daily denominator.
    #[test]
    fn capital_reconciliation_accumulates_sub_threshold_accounted_moves_until_equity_catches_up() {
        let min_usd = 5.0;
        // The anchor stays pinned at (1_000.0, 0.0) throughout: Ambiguous
        // does not advance it while position_seen_since_baseline is false,
        // mirroring detect_capital_event_and_rebaseline's baseline_advanced
        // gate.
        let baseline_equity = 1_000.0;
        let baseline_accounted_pnl = 0.0;

        let tick1 =
            reconcile_capital_delta(baseline_equity, 1_000.0, baseline_accounted_pnl, 4.0, false, min_usd);
        assert_eq!(
            tick1.disposition,
            CapitalDisposition::Ambiguous,
            "the first sub-threshold accounted move defers instead of silently resetting the anchor"
        );

        let tick2 =
            reconcile_capital_delta(baseline_equity, 1_000.0, baseline_accounted_pnl, 8.0, false, min_usd);
        assert_eq!(
            tick2.disposition,
            CapitalDisposition::Ambiguous,
            "the accumulated $8 gap against the un-advanced anchor now exceeds min_usd on its own"
        );

        let tick3 = reconcile_capital_delta(
            baseline_equity,
            1_008.0,
            baseline_accounted_pnl,
            8.0,
            false,
            min_usd,
        );
        assert_eq!(
            tick3.disposition,
            CapitalDisposition::Reconciled,
            "equity catching up to the full accounted total resolves as delayed settlement, not an $8 verified deposit"
        );
        assert!(tick3.inferred_capital_delta.abs() < 1e-9);
    }

    // bot-strategy#783 (Codex P1 follow-up, second round): equity does not
    // have to be bit-exact frozen for the same accumulation to happen --
    // partial catch-up each tick (raw equity moves, just not by as much as
    // accounted_pnl) leaves the same kind of residual behind, and each
    // tick's residual is individually below min_usd.
    #[test]
    fn capital_reconciliation_accumulates_partial_catch_up_residuals_until_equity_fully_catches_up()
    {
        let min_usd = 5.0;
        let baseline_equity = 1_000.0;
        let baseline_accounted_pnl = 0.0;

        let tick1 = reconcile_capital_delta(
            baseline_equity,
            1_001.0,
            baseline_accounted_pnl,
            4.0,
            false,
            min_usd,
        );
        assert_eq!(
            tick1.disposition,
            CapitalDisposition::Ambiguous,
            "a $3 residual (equity +1 vs accounted +4) defers instead of resetting the anchor"
        );

        let tick2 = reconcile_capital_delta(
            baseline_equity,
            1_002.0,
            baseline_accounted_pnl,
            8.0,
            false,
            min_usd,
        );
        assert_eq!(
            tick2.disposition,
            CapitalDisposition::Ambiguous,
            "the accumulated $6 residual against the un-advanced anchor now exceeds min_usd on its own"
        );

        let tick3 = reconcile_capital_delta(
            baseline_equity,
            1_008.0,
            baseline_accounted_pnl,
            8.0,
            false,
            min_usd,
        );
        assert_eq!(
            tick3.disposition,
            CapitalDisposition::Reconciled,
            "equity fully catching up to the accounted total resolves as delayed settlement, not a $6 verified deposit"
        );
        assert!(tick3.inferred_capital_delta.abs() < 1e-9);
    }

    // A pure equity-only drift (nothing traded, accounted_pnl unchanged)
    // must still reconcile normally even while position_seen_since_baseline
    // is stuck true from an earlier migration -- the P1 accumulation guard
    // must not block the ordinary Reconciled path this depends on to ever
    // clear the latch (regression for a fix that briefly broke
    // deposit_while_flat_rebaselines_peak_without_clearing_halt).
    #[test]
    fn capital_reconciliation_reconciles_pure_equity_drift_with_no_accounted_move() {
        let drift = reconcile_capital_delta(950.0, 953.0, 0.0, 0.0, true, 5.0);
        assert_eq!(drift.disposition, CapitalDisposition::Reconciled);
    }

    // bot-strategy#783 (Codex P1 follow-up): a near-full withdrawal after a
    // positive round makes the capital *basis* (equity - accounted PnL)
    // negative even though live equity ($0.01) is completely valid. This
    // must still verify as a real capital event, not be silently swallowed
    // as "no signal" the way reusing classify_capital_event's raw-equity
    // ≤0 guard did.
    #[test]
    fn capital_reconciliation_verifies_a_withdrawal_that_makes_the_basis_negative() {
        // baseline: $1,053.27 equity, $53.27 already-accounted PnL -> basis $1,000.
        // current: $0.01 equity, same $53.27 accounted PnL -> basis -$53.26.
        let withdrawal = reconcile_capital_delta(1_053.27, 0.01, 53.27, 53.27, false, 5.0);
        assert_eq!(
            withdrawal.disposition,
            CapitalDisposition::Verified(-1_053.26)
        );
    }

    #[test]
    fn capital_reconciliation_verifies_a_redeposit_after_a_negative_basis_baseline() {
        // Continues the withdrawal above: the paired baseline has advanced
        // to the negative basis (-$53.26 = $0.01 equity - $53.27 accounted
        // PnL). A subsequent redeposit to $500.01 must still be detected --
        // the old code permanently stopped detecting anything once the
        // baseline itself went <= 0.
        let redeposit = reconcile_capital_delta(0.01, 500.01, 53.27, 53.27, false, 5.0);
        assert_eq!(redeposit.disposition, CapitalDisposition::Verified(500.0));
    }

    // bot-strategy#575 ① / #783: capital-basis-delta classification. A
    // deposit/withdrawal of at least `min_usd` while flat is an event; a
    // smaller drift is not. Unlike the raw-equity classifier this replaced,
    // a non-positive baseline/current basis is not special-cased -- see
    // classify_capital_basis_delta's own doc comment.
    #[test]
    fn classify_capital_basis_delta_detects_deposit_and_withdrawal() {
        assert_eq!(classify_capital_basis_delta(950.0, 960.0, 5.0), Some(10.0));
        assert_eq!(classify_capital_basis_delta(950.0, 940.0, 5.0), Some(-10.0));
    }

    #[test]
    fn classify_capital_basis_delta_ignores_sub_threshold_drift() {
        assert_eq!(classify_capital_basis_delta(950.0, 953.0, 5.0), None);
        assert_eq!(classify_capital_basis_delta(950.0, 950.0, 5.0), None);
    }

    #[test]
    fn classify_capital_basis_delta_boundary_is_inclusive() {
        // Exactly min_usd counts as an event (≥, not >).
        assert_eq!(classify_capital_basis_delta(950.0, 955.0, 5.0), Some(5.0));
    }

    #[test]
    fn classify_capital_basis_delta_disabled_when_min_usd_zero() {
        assert_eq!(classify_capital_basis_delta(950.0, 9_000.0, 0.0), None);
    }

    #[test]
    fn classify_capital_basis_delta_handles_a_non_positive_basis() {
        // A negative basis (equity below already-accounted PnL) is a real,
        // ordinary value here, not a sentinel for "unset" -- see
        // reconcile_capital_delta's own tests for the full withdrawal/
        // redeposit scenario this exists to support.
        assert_eq!(
            classify_capital_basis_delta(-53.26, 446.74, 5.0),
            Some(500.0)
        );
    }

    // bot-strategy#575 ②: RISK_ACK re-anchor token parsing.
    #[test]
    fn ack_requests_reanchor_json_and_plaintext() {
        assert!(ack_requests_reanchor(r#"{"reanchor": true}"#));
        assert!(ack_requests_reanchor(r#"{"reanchor":true,"by":"op"}"#));
        assert!(ack_requests_reanchor("ack by op: reanchor=true"));
        assert!(ack_requests_reanchor("reanchor: true"));
        assert!(ack_requests_reanchor("REANCHOR=TRUE"));
    }

    #[test]
    fn ack_requests_reanchor_absent_or_false() {
        assert!(!ack_requests_reanchor(""));
        assert!(!ack_requests_reanchor("ack: manual review complete"));
        assert!(!ack_requests_reanchor(r#"{"reanchor": false}"#));
        assert!(!ack_requests_reanchor("reanchor=false"));
    }

    // bot-strategy#575 ①/②: re-anchoring collapses the rolling window to a
    // single current-equity sample so the next rolling_peak gives DD = 0.
    #[test]
    fn reanchor_peak_samples_collapses_to_current() {
        let mut samples = vec![
            risk_io::EquitySample {
                ts: 100,
                equity: 1_003.0,
            },
            risk_io::EquitySample {
                ts: 200,
                equity: 980.0,
            },
        ];
        reanchor_peak_samples(&mut samples, 960.0, 300);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].ts, 300);
        assert!((samples[0].equity - 960.0).abs() < 1e-9);
        // Peak now equals current → DD 0.
        let (peak, dd) = PairTradeEngine::rolling_peak(&samples, 960.0).unwrap();
        assert!((peak - 960.0).abs() < 1e-9);
        assert_eq!(dd, 0.0);
    }

    #[test]
    fn reanchor_peak_samples_non_positive_equity_just_clears() {
        let mut samples = vec![risk_io::EquitySample {
            ts: 100,
            equity: 1_003.0,
        }];
        reanchor_peak_samples(&mut samples, 0.0, 300);
        assert!(samples.is_empty());
    }
}
