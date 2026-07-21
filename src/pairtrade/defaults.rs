//! Default values and magic constants for the pairtrade engine. Extracted
//! from the monolithic pairtrade module as part of bot-strategy#26.

pub(super) const DEFAULT_INTERVAL_SECS: u64 = 20;
pub(super) const DEFAULT_TRADING_PERIOD_SECS: u64 = 60;
pub(super) const DEFAULT_METRICS_WINDOW: usize = 240;
pub(super) const DEFAULT_ENTRY_Z_BASE: f64 = 2.0;
pub(super) const DEFAULT_ENTRY_Z_MIN: f64 = 1.8;
pub(super) const DEFAULT_ENTRY_Z_MAX: f64 = 2.3;
pub(super) const DEFAULT_EXIT_Z: f64 = 0.5;
pub(super) const DEFAULT_STOP_LOSS_Z: f64 = 3.3;
pub(super) const DEFAULT_FORCE_CLOSE_SECS: u64 = 3600;
pub(super) const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 3660; // DEFAULT_FORCE_CLOSE_SECS + 60s buffer
/// Held-position-only eligibility grace (bot-strategy#742). Zero preserves
/// legacy immediate ineligible closes; the replay-backed candidate is 60s.
pub(super) const DEFAULT_ELIGIBILITY_MARGIN_GRACE_SECS: i64 = 0;
/// Upper relative beta-gap bound for the narrow #742 grace. The raw entry
/// eligibility threshold remains 0.20; only (0.20, exit] may be held.
pub(super) const DEFAULT_ELIGIBILITY_BETA_GAP_EXIT: f64 = 0.25;
pub(super) const DEFAULT_COOLDOWN_SECS: u64 = 30;
/// Per-direction post-stop_loss_z cool-down (seconds). 0 = disabled (legacy
/// behavior). Independent of `DEFAULT_COOLDOWN_SECS` and the global circuit
/// breaker. See bot-strategy#316.
pub(super) const DEFAULT_STOP_LOSS_COOLDOWN_SECS: u64 = 0;
pub(super) const MAX_EXIT_RETRIES: u32 = 3;
pub(super) const DEFAULT_NET_FUNDING_MIN_PER_HOUR: f64 = -0.005;
pub(super) const DEFAULT_SPREAD_VELOCITY_MAX_SIGMA_PER_MIN: f64 = 0.1;
pub(super) const DEFAULT_RISK_PCT_PER_TRADE: f64 = 0.01;
pub(super) const DEFAULT_MAX_LOSS_R_MULT: f64 = 1.0;
pub(super) const DEFAULT_EQUITY_USD: f64 = 10_000.0;
pub(super) const DEFAULT_LOOKBACK_HOURS_SHORT: u64 = 4;
pub(super) const DEFAULT_LOOKBACK_HOURS_LONG: u64 = 24;
pub(super) const DEFAULT_HALF_LIFE_MAX_HOURS: f64 = 1.5;
pub(super) const DEFAULT_ADF_P_THRESHOLD: f64 = 0.05;
/// Fixed raw-entry eligibility threshold on relative beta divergence.
pub(super) const ELIGIBILITY_BETA_GAP_MAX: f64 = 0.20;
pub(super) const PAIR_SELECTION_INTERVAL_SECS: u64 = 3600;
pub(super) const DEFAULT_ENTRY_VOL_LOOKBACK_HOURS: u64 = 24;
pub(super) const DEFAULT_SLIPPAGE_BPS: i32 = 0;
pub(super) const DEFAULT_FEE_BPS: f64 = 0.0;
pub(super) const DEFAULT_MAX_LEVERAGE: f64 = 5.0;
pub(super) const DEFAULT_REEVAL_JUMP_Z_MULT: f64 = 1.5;
pub(super) const DEFAULT_VOL_SPIKE_MULT: f64 = 2.5;
pub(super) const DEFAULT_MAX_ACTIVE_PAIRS: usize = 3;
pub(super) const DEFAULT_WARM_START_MODE: &str = "strict";
pub(super) const DEFAULT_ORDER_TIMEOUT_SECS: u64 = 120;
pub(super) const DEFAULT_ENTRY_PARTIAL_FILL_MAX_RETRIES: u32 = 3;
/// Hard cap on `hedge_retry_count` for the partial-fill reissue loop
/// (bot-strategy#480). Once the per-tick reconcile has rotated through
/// this many reissues without `all_filled` clearing, the bot gives up:
/// pending orders are cancelled, any filled legs are flattened via
/// `force_close_all_positions`, and `pending_entry` is cleared so the
/// next ENTRY signal can fire from a clean slate. 0 disables the cap
/// (legacy unbounded behaviour). Default 30 = 10× the post-only retry
/// budget, ~2.5 min at the 5 s tick cadence — long enough to absorb
/// normal partial-fill recovery, short enough to surface a stuck loop
/// before it spans hours / days.
pub(super) const DEFAULT_ENTRY_PARTIAL_FILL_GIVEUP_RETRIES: u32 = 30;
pub(super) const DEFAULT_FORCE_CLOSE_ON_STARTUP: bool = true;
pub(super) const DEFAULT_STARTUP_FORCE_CLOSE_ATTEMPTS: u32 = 3;
pub(super) const DEFAULT_STARTUP_FORCE_CLOSE_WAIT_SECS: u64 = 3;
pub(super) const POST_ONLY_ENTRY_ATTEMPTS: usize = 3;
pub(super) const POST_ONLY_EXIT_ATTEMPTS: usize = 3;
pub(super) const POST_ONLY_RETRY_DELAY_MS: u64 = 200;
pub(super) const POST_ONLY_RETRY_MAX_ELAPSED_MS: u64 = 1500;
pub(super) const DEFAULT_SPREAD_TREND_MAX_SLOPE_SIGMA: f64 = 0.5;
pub(super) const DEFAULT_BETA_DIVERGENCE_MAX: f64 = 0.15;
pub(super) const DEFAULT_CB_TIER1_LOSSES: u32 = 0;
pub(super) const DEFAULT_CB_TIER1_COOLDOWN_SECS: u64 = 0;
pub(super) const DEFAULT_CB_TIER2_LOSSES: u32 = 0;
pub(super) const DEFAULT_CB_TIER2_COOLDOWN_SECS: u64 = 0;
pub(super) const DEFAULT_ENTRY_POST_ONLY_TIMEOUT_SECS: u64 = 0;
/// Per-leg fill timeout for exit post-only orders (seconds). After both exit
/// legs are placed as post-only limits, the bot polls fill state and, once
/// this many seconds have elapsed, cancels any leg still resting unfilled and
/// re-places it as a taker (market) order. 0 disables the monitor entirely
/// (legacy behavior: post-only legs rest until they fill or the next pair
/// cycle replaces them). The monitor is also gated on
/// `should_post_only() == true` (i.e. `fee_bps > 0`), so on Frankfurt
/// (fee_bps=0) this knob has no effect regardless of the configured value.
/// See bot-strategy#306.
pub(super) const DEFAULT_EXIT_POST_ONLY_TIMEOUT_SECS: u64 = 0;
// EXIT_FILL_POLL_MS / EXIT_CANCEL_SETTLE_MS were the in-step
// `monitor_exit_legs_with_timeout` polling constants and were removed when
// the post-only-exit taker takeover moved to the reconcile loop
// (bot-strategy#408). The reconcile loop polls at step cadence
// (`interval_secs`, typically 5s) and reuses `cancel_pending_orders` for
// cancellation, so these knobs no longer have a use site.

/// Use frozen-β z for exit-side gates (`exit_z`, `stop_loss_z`, and the
/// expected-value gate). When `true`, exit-side z is recomputed against
/// `Position.entry_beta` and the position's current log prices instead
/// of `shared.beta`, so a β drift during the hold does not produce a
/// "z reverted but no actual mean-reversion" false signal. Default
/// `false` preserves legacy behaviour. Entry / regime / dashboards keep
/// using the rolling-β z regardless. See bot-strategy#473.
pub(super) const DEFAULT_USE_FROZEN_BETA_EXIT_Z: bool = false;

// Multi-timeframe z-score confluence (disabled by default)
pub(super) const DEFAULT_MTF_Z_MIN: f64 = 0.0;

// Kalman filter beta estimation (disabled by default)
pub(super) const DEFAULT_USE_KALMAN_BETA: bool = false;
pub(super) const DEFAULT_KALMAN_Q: f64 = 1e-5;
pub(super) const DEFAULT_KALMAN_R: f64 = 1e-3;
pub(super) const DEFAULT_KALMAN_INITIAL_P: f64 = 1.0;
pub(super) const DEFAULT_KALMAN_MIN_UPDATES: u64 = 60;

// Innovation-responsive persistent-regime gate (bot-strategy#494). Phase 1
// is shadow-only: the detector and its gauges always run, but blocking
// entries is opt-in and OFF by default.
pub(super) const DEFAULT_REGIME_BLOCK_ENTRIES: bool = false;

// Std collapse guard (disabled by default: window=0 or ratio=0.0 → filter inactive).
// See bot-strategy#62: on 2026-04-15 the BTC/ETH spread std collapsed from
// 1.018 → 0.0016 within minutes, producing meaningless z-scores that all three
// bots interpreted as deep mean-reversion signals and lost on. Guard blocks
// entry when the current full-window std is a small fraction of the rolling
// median of recent stds, i.e. the z denominator is no longer trustworthy.
pub(super) const DEFAULT_STD_COLLAPSE_WINDOW_BARS: usize = 0;
pub(super) const DEFAULT_STD_COLLAPSE_MIN_RATIO: f64 = 0.0;
/// Optional hold-down after a recent std-collapse sample. 0 = disabled and
/// preserves the legacy point-in-time guard. See bot-strategy#500.
pub(super) const DEFAULT_STD_COLLAPSE_HOLD_DOWN_SECS: u64 = 0;
/// Observe-only mode: when true, the guard only logs that it *would* block
/// the entry, but lets the trade through. Lets operators measure trigger
/// frequency against live data before enabling the block. See bot-strategy#62.
pub(super) const DEFAULT_STD_COLLAPSE_OBSERVE_ONLY: bool = false;

// Regime filter (disabled by default: thresholds 0.0 → filter inactive)
pub(super) const DEFAULT_REGIME_VOL_WINDOW: usize = 60;
pub(super) const DEFAULT_REGIME_VOL_MAX: f64 = 0.0;
pub(super) const DEFAULT_REGIME_TREND_WINDOW: usize = 60;
pub(super) const DEFAULT_REGIME_TREND_MAX: f64 = 0.0;
pub(super) const DEFAULT_REGIME_REFERENCE_SYMBOL: &str = "BTC";

// Daily DD limit — bot-strategy#185 Phase 2. Disabled by default (0 bps →
// no block). Reset hour is UTC; 0 = UTC midnight rollover.
pub(super) const DEFAULT_MAX_DAILY_LOSS_BPS: u32 = 0;
pub(super) const DEFAULT_DAILY_RESET_UTC_HOUR: u32 = 0;

// Session DD + max-notional cap — bot-strategy#185 Phase 3. All disabled by
// default. `max_session_loss_bps` is the threshold against the rolling peak
// equity over `session_dd_lookback_secs`; on breach the bot flattens the
// instance's positions and halts entries until the manual-ack sentinel
// (default `/opt/debot/RISK_ACK`, overridable via the `RISK_ACK_PATH` env)
// is dropped — no auto-resume. `max_notional_headroom` caps each
// hedge leg's USD notional at `equity_reference_usd × max_leverage × headroom`
// so the same value works across hosts with different equity / leverage
// (Frankfurt $1k×5x, Tokyo Lighter $150×5x, etc.). Both legs of a trade
// respect the cap.
pub(super) const DEFAULT_MAX_SESSION_LOSS_BPS: u32 = 0;
pub(super) const DEFAULT_SESSION_DD_LOOKBACK_SECS: u64 = 30 * 24 * 60 * 60;
pub(super) const DEFAULT_SESSION_DD_SAMPLE_SECS: u64 = 3600;
pub(super) const DEFAULT_MAX_NOTIONAL_HEADROOM: f64 = 0.0;

// Deposit-aware DD rebaseline — bot-strategy#575 ①. While an instance is
// flat (so live equity == pure collateral, no unrealized mark noise) and has
// been flat for at least `session_dd_capital_settle_secs`, an unexplained
// equity jump of at least `session_dd_capital_event_min_usd` is treated as a
// capital event (deposit / withdrawal / sub-account transfer) and the rolling
// session-DD peak is rebaselined to the new equity (DD → 0). This stops a
// sticky 30-day peak from pinning a halted variant at the boundary after a
// top-up. 0 USD disables the detection. The settle window guards against
// reading a post-close collateral-settlement lag as a deposit; a halted
// (long-flat) variant always clears it, which is the primary recovery case.
pub(super) const DEFAULT_SESSION_DD_CAPITAL_EVENT_MIN_USD: f64 = 5.0;
pub(super) const DEFAULT_SESSION_DD_CAPITAL_SETTLE_SECS: u64 = 60;
