//! Per-strategy instance state (bot-strategy#444).
//!
//! `StrategyInstance` is the unit of A/B/C variant isolation: each instance
//! owns its own connector, equity cache, per-pair states, PnL log, status
//! reporter, risk counters, and pair-parameter overlay. The engine holds a
//! `Vec<StrategyInstance>` and addresses them by index.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use dex_connector::DexConnector;

use super::config::PairParams;
use super::pnl_log::PnlLogger;
use super::risk_io;
use super::state::PairState;
use super::status::StatusReporter;

/// Max age of the per-instance equity cache before `refresh_equity_if_needed`
/// fetches a fresh value from the exchange. Now a low-frequency dashboard tick:
/// exit/loss-cut uses locally-computed PnL from WS prices, so `equity_cache`
/// only scales the slowly-drifting R-budget and feeds the status reporter.
/// Entry sizing fetches inline (see `fetch_equity_rest` call in the entry
/// branch of `step()`), which after dex-connector v4.2.83 is a WS-derived
/// cache hit in steady state. See bot-strategy#156, #239.
pub(in crate::pairtrade) const EQUITY_REFRESH_CACHE_SECS: u64 = 300;

pub(in crate::pairtrade) struct StrategyInstance {
    #[allow(dead_code)]
    pub(in crate::pairtrade) id: String,
    /// Per-strategy connector. For single-instance deployments this is the
    /// same `Arc` as `PairTradeEngine.connector`. For multi-strategy
    /// deployments each instance owns its own connector pointing at its
    /// sub-account credentials.
    #[allow(dead_code)]
    pub(in crate::pairtrade) connector: Arc<dyn DexConnector + Send + Sync>,
    /// Per-instance live equity from the instance's connector.
    pub(in crate::pairtrade) equity_cache: f64,
    pub(in crate::pairtrade) last_equity_fetch: Option<Instant>,
    /// `equity_cache`'s value captured at the moment
    /// `detect_capital_event_and_rebaseline` most recently (re)started
    /// requiring proof that a fresh observation has landed (whenever
    /// `flat_since` or `capital_rebaseline_deferred_since` resets). A
    /// timestamp of "when did a fetch last succeed" is not a reliable
    /// freshness signal on its own: dex-connector's WS-derived
    /// `balance_cache` means a "successful" fetch can return the
    /// identical still-stale value if the underlying push that would
    /// update it was never received (e.g. a missed fill event after a
    /// startup force-close). Comparing the current reading against this
    /// snapshot instead requires the actual *value* to have moved,
    /// proving a genuine new observation landed (Codex P1 follow-up,
    /// bot-strategy#783). Runtime only, not persisted.
    pub(in crate::pairtrade) capital_guard_equity_snapshot: Option<f64>,
    /// `equity_cache` as of the most recent tick that examined it, updated
    /// unconditionally every `detect_capital_event_and_rebaseline` call
    /// regardless of disposition. Paired with `capital_guard_stable_since`
    /// to detect not just "has equity moved past the untrusted snapshot"
    /// but "has it *stopped* moving": an unaccounted settlement that lands
    /// in several separate updates (e.g. a two-leg close reported as -$2
    /// then later -$8 more) must not be trusted the moment the first
    /// partial update is observed, or the remaining, still-pending portion
    /// gets compared against a baseline that already absorbed the first
    /// partial move and gets misclassified as a fresh capital event once
    /// it lands (Codex P1 follow-up, bot-strategy#783). Runtime only, not
    /// persisted.
    pub(in crate::pairtrade) capital_guard_last_observed_equity: Option<f64>,
    /// When `capital_guard_last_observed_equity` most recently changed
    /// value. A reading is only trusted as a *complete* settlement once it
    /// has held steady for `CAPITAL_GUARD_STABILITY_SECS` -- not merely
    /// once, on the very first tick that differs from the pre-close
    /// snapshot. Runtime only, not persisted.
    pub(in crate::pairtrade) capital_guard_stable_since: Option<Instant>,
    /// Incremented every time `fetch_equity_rest` writes a genuinely
    /// connector-sourced value into `equity_cache` (not on a fetch failure,
    /// and not on the pre-init zero-reading skip). `CAPITAL_GUARD_STABILITY_SECS`
    /// alone measures wall-clock time since `capital_guard_stable_since`
    /// armed, but `equity_cache` is only actually refreshed on its own
    /// `EQUITY_REFRESH_CACHE_SECS` (300s) cadence -- longer than the 60s
    /// stability window. A value that lands, then sits frozen in the cache
    /// for the next 60s with zero refresh attempts, would otherwise satisfy
    /// the elapsed-time check without ever having been re-observed from the
    /// exchange even once, so a still-in-flight multi-leg settlement's
    /// remaining portion lands against a guard that already cleared (Codex
    /// P1 follow-up, bot-strategy#783). Runtime only, not persisted.
    pub(in crate::pairtrade) equity_fetch_generation: u64,
    /// `equity_fetch_generation`'s value captured at the moment
    /// `capital_guard_stable_since` most recently (re)armed. Stability is
    /// only trusted once `equity_fetch_generation` has advanced past this,
    /// proving at least one genuine connector observation landed during the
    /// window (Codex P1 follow-up, bot-strategy#783). Runtime only, not
    /// persisted.
    pub(in crate::pairtrade) capital_guard_stable_since_generation: u64,
    /// False until the first successful `fetch_equity_rest` writes a
    /// connector-sourced balance into `equity_cache`. Not persisted —
    /// always starts false on engine boot. Gates session-DD evaluation
    /// and equity sampling so a restart whose `equity_samples` deque
    /// already holds a real peak does not trip a phantom halt against
    /// the stale `equity_reference_usd` seed before the first WS
    /// account dump propagates. See bot-strategy#366.
    pub(in crate::pairtrade) equity_initialized: bool,
    /// Per-strategy fixed equity reference from the YAML
    /// `equity_usd_reference`. Used as the base for risk thresholds
    /// (daily DD, exit risk_budget) AND position sizing so each
    /// variant operates against its own declared capital. Revised
    /// manually at the same monthly cadence as A/B/C parameter updates;
    /// `equity_cache` is kept separately for live monitoring only and
    /// is no longer mixed into the threshold/sizing math. See
    /// bot-strategy#222.
    pub(in crate::pairtrade) equity_reference_usd: f64,
    /// Per-strategy leverage override (bot-strategy#810). Resolved from the
    /// YAML `strategies[].max_leverage` (or `MAX_LEVERAGE_<ID>` env var);
    /// `None` in either inherits the top-level `PairTradeConfig::max_leverage`.
    /// Drives sizing (`hedged_sizes`) and the leverage-neutralized risk
    /// gates (daily/session DD, market-move thresholds) so A/B/C variants
    /// can run at different leverage against a shared process.
    pub(in crate::pairtrade) max_leverage: f64,
    pub(in crate::pairtrade) states: HashMap<String, PairState>,
    pub(in crate::pairtrade) pnl_logger: Option<PnlLogger>,
    pub(in crate::pairtrade) status_reporter: Option<StatusReporter>,
    pub(in crate::pairtrade) consecutive_losses: u32,
    pub(in crate::pairtrade) circuit_breaker_until: Option<Instant>,
    /// Replay-aware companion to `circuit_breaker_until`. Compared against
    /// the per-step `now_ts` so backtest replays can honour the same
    /// cool-down logic as live.
    pub(in crate::pairtrade) circuit_breaker_until_ts: Option<i64>,
    /// Daily-DD tracking (bot-strategy#185 Phase 2). Zero/None until the
    /// first `refresh_daily_session` reset populates them.
    pub(in crate::pairtrade) session_start_equity: f64,
    /// Configured equity reference associated with the current daily-DD
    /// denominator. A changed reference is reconciled only after the
    /// instance is flat and settled, alongside capital-event detection, so
    /// a matching deposit is not counted twice. Persisted via
    /// `InstanceRiskState`. bot-strategy#752.
    pub(in crate::pairtrade) session_equity_reference_usd: f64,
    pub(in crate::pairtrade) session_start_ts: i64,
    pub(in crate::pairtrade) realized_pnl_today: f64,
    /// Running sum of `funding_carry_usd` from cycles closed during the
    /// current UTC session. Updated at exit_fill / exit_dry_run when the
    /// cycle's funding carry was measured (`with_funding(...)` was called
    /// on the PnlLogRecord). Reset at the same session rollover as
    /// `realized_pnl_today`. Persisted via `InstanceRiskState` so it
    /// survives restarts within a single UTC day. Surfaced on
    /// status.json as `funding_carry_today` for dashboard attribution.
    /// bot-strategy#371.
    pub(in crate::pairtrade) funding_carry_today: f64,
    /// Round-scoped cumulative funding carry. Unlike `funding_carry_today`,
    /// this does not reset at UTC rollover: capital-event reconciliation
    /// needs the full funding movement since its last settled baseline to
    /// distinguish exchange settlement from a transfer. bot-strategy#783.
    pub(in crate::pairtrade) total_funding_carry: f64,
    /// True once `realized_pnl_today` has breached
    /// `max_daily_loss_bps`. Used for transition logging only
    /// (activate/clear); the live gate check is recomputed every tick
    /// from current state via `daily_loss_blocks`.
    pub(in crate::pairtrade) daily_loss_halted: bool,
    /// Phase 3-1 rolling peak equity samples. Append-only at
    /// `risk.session_dd_sample_secs` cadence; entries older than
    /// `risk.session_dd_lookback_secs` are pruned in-place.
    pub(in crate::pairtrade) equity_samples: Vec<risk_io::EquitySample>,
    /// Last settled account equity used by capital reconciliation. It is
    /// paired with `capital_baseline_accounted_pnl` so realized trade PnL and
    /// funding that settle after a close are backed out before a transfer is
    /// inferred. 0.0 = unset. Persisted via `InstanceRiskState`.
    pub(in crate::pairtrade) capital_baseline_equity: f64,
    /// `total_pnl + total_funding_carry` captured with
    /// `capital_baseline_equity`. `None` identifies a pre-#783 snapshot and
    /// is migrated on the next flat/settled observation.
    pub(in crate::pairtrade) capital_baseline_accounted_pnl: Option<f64>,
    /// True after any position was observed since the paired baseline. Persisted
    /// so an unaccounted recovery close cannot be mistaken for a transfer.
    pub(in crate::pairtrade) capital_position_seen_since_baseline: bool,
    /// Runtime-only latch that prevents an ambiguous post-close settlement
    /// from emitting the same risk-history event on every tick. It never
    /// authorizes a rebaseline; it clears only after accounting and equity
    /// reconcile or a verified capital event lands.
    pub(in crate::pairtrade) capital_rebaseline_deferred: bool,
    /// Runtime-only: when the *current* deferred streak began. Some
    /// ambiguous observations (e.g. a real transfer landing exactly when a
    /// position closes with material PnL) can never satisfy the ordinary
    /// `baseline_advanced` exit -- that requires the accounted delta itself
    /// to be sub-threshold, which a genuine material close PnL never is --
    /// leaving the account stuck deferred forever with no way to detect any
    /// later capital event either. After `CAPITAL_REBASELINE_GIVEUP_SECS`
    /// of continuous deferral, `detect_capital_event_and_rebaseline` gives
    /// up: the anchor advances to the current reading so future events
    /// remain detectable, without crediting this specific one to either
    /// accounting or a transfer since it cannot disentangle them. Reset to
    /// `None` whenever the deferred streak ends or restarts (mirrors
    /// `flat_since`; not persisted).
    pub(in crate::pairtrade) capital_rebaseline_deferred_since: Option<Instant>,
    /// bot-strategy#575 ①: when this instance most recently became flat
    /// (no open or pending positions). `detect_capital_event_and_rebaseline`
    /// requires `session_dd_capital_settle_secs` of continuous flatness
    /// before trusting an equity reading as a capital event. Runtime only —
    /// reset to None whenever the instance is not flat, so a brief flatten
    /// between trades does not arm detection. Not persisted.
    pub(in crate::pairtrade) flat_since: Option<Instant>,
    /// Phase 3-1/3-2 sticky halt set on session-DD breach. Persists to
    /// `risk_state.json` so a crash inside the cool-off window does
    /// not silently re-arm the bot. Cleared only by writing the
    /// manual-ack sentinel (default `/opt/debot/RISK_ACK`, overridable
    /// via the `RISK_ACK_PATH` env var per bot-strategy#488).
    pub(in crate::pairtrade) session_halted: bool,
    pub(in crate::pairtrade) session_halt_reason: Option<String>,
    pub(in crate::pairtrade) session_halt_ts: Option<i64>,
    pub(in crate::pairtrade) total_trades: u64,
    pub(in crate::pairtrade) total_wins: u64,
    pub(in crate::pairtrade) total_pnl: f64,
    pub(in crate::pairtrade) peak_pnl: f64,
    pub(in crate::pairtrade) max_dd: f64,
    /// Per-instance pair parameter overrides. Built at `new_inner` time by
    /// overlaying the strategy's `exit_z` / `stop_loss_z` / `max_loss_r_mult`
    /// on top of the engine-wide defaults. Look up via
    /// `PairTradeEngine::pair_params_for(inst_idx, key)`.
    pub(in crate::pairtrade) pair_params: HashMap<String, PairParams>,
    pub(in crate::pairtrade) default_pair_params: PairParams,
    /// One-shot reason marker set when the risk layer flattens this
    /// instance's positions outside the strategy exit path (e.g. the
    /// session-DD halt). Consumed by `sync_positions_from_exchange` when
    /// the exchange snapshot confirms the positions are gone, so the
    /// `recovery_no_pnl` context record carries the real trigger instead
    /// of the generic `exchange_snapshot_clear`. Not persisted.
    /// bot-strategy#514.
    pub(in crate::pairtrade) external_flatten_reason: Option<String>,
    /// Pairs whose NEW entries are fail-closed because the post-entry
    /// venue-position reconciliation (bot-strategy#721) found an exposure
    /// mismatch it could not repair (trim failed, position fetch failed,
    /// or the venue position sign contradicts the intended entry). Key =
    /// pair key (e.g. "BTC/ETH"), value = reason tag. Exit management is
    /// unaffected. Persisted via `InstanceRiskState`; cleared only by the
    /// RISK_ACK sentinel — there is no auto-resume.
    pub(in crate::pairtrade) entry_blocked_pairs: HashMap<String, String>,
}
