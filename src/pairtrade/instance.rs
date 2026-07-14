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
    /// True once `realized_pnl_today` has breached
    /// `max_daily_loss_bps`. Used for transition logging only
    /// (activate/clear); the live gate check is recomputed every tick
    /// from current state via `daily_loss_blocks`.
    pub(in crate::pairtrade) daily_loss_halted: bool,
    /// Phase 3-1 rolling peak equity samples. Append-only at
    /// `risk.session_dd_sample_secs` cadence; entries older than
    /// `risk.session_dd_lookback_secs` are pruned in-place.
    pub(in crate::pairtrade) equity_samples: Vec<risk_io::EquitySample>,
    /// bot-strategy#575 ①: last equity captured while continuously flat and
    /// settled, the reference for deposit / withdrawal detection. 0.0 =
    /// unset. Persisted via `InstanceRiskState`.
    pub(in crate::pairtrade) capital_baseline_equity: f64,
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
