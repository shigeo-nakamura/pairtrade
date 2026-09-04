//! Bull-mode holder (bot-strategy#893 / #894) — PROTOTYPE, DRY_RUN only.
//!
//! Operator-declared regime, β with discipline. The bot never decides whether
//! a bull market is on: the operator ARMs it. While armed it holds, per symbol,
//! two legs and does nothing else:
//!
//! - Hyperliquid **spot** 1x  (UBTC/USDC, UETH/USDC): the base, no funding
//! - Lighter **perp** 0.5x   (BTC, ETH long): the lift; funding only on this leg
//!
//! Exactly one exit rule, evaluated once per UTC day on the completed daily
//! close: `close < peak_close × (1 − exit_dd)` → close BOTH legs and go to
//! `Exited`, where it stays until the operator ARMs again. No re-entry logic,
//! no MA/trailing tweaks (bot-strategy#885/#891 showed every such rule costs
//! more upside than it saves). A Lighter exchange-side stop-loss (reduce_only,
//! market on trigger) rests at a *wider* level (`stop_dd`) as insurance for the
//! perp leg while the process is down; it is re-placed whenever the peak moves.
//!
//! Evidence / gates: bot-strategy#893 (P1 lift only with the spot base, P2
//! insurance, P3 no whipsaw). Phase 0 script:
//! bot-strategy `scripts/strategy_probes/bull_holder_893/`.
//!
//! Operator surface (files under `BULL_HOLDER_BASE_DIR`, default
//! /opt/debot-bull-holder — isolated from the pairtrade A/B/C tree):
//! - `bull_holder/ARM`        touch → arm (consumed). The book is built in
//!   `BULL_HOLDER_ENTRY_TRANCHES` equal daily tranches (default 1 = all at
//!   once): the first fills immediately, the rest one per UTC day at the
//!   daily-eval slot. Pinpointing the entry day is not possible, so a short
//!   ladder (recommended 5, see bot-strategy#893 addendum: P1 still passes
//!   up to N=7) trades a few points of upside for insurance against ARMing
//!   into a local spike. The exit rule is live from the first tranche; an
//!   exit (rule or DISARM) cancels the remaining tranches — a 30% drop mid-
//!   ladder means the regime call was wrong, it is not a dip to buy. Days
//!   the bot is down push the ladder out; tranches never clump.
//! - `bull_holder/ADD`        touch (optionally containing an integer K) →
//!   schedule K more tranches (default 1) of the size fixed at ARM, while
//!   On. Adds run one per UTC day like the ladder. Ignored (consumed) when
//!   nothing is held or KILL_SWITCH is engaged; deferred while halted.
//! - `bull_holder/DISARM`     touch → close BOTH legs of every symbol now and
//!   go to `Exited` (consumed). The operator's manual exit; the only other
//!   exit is the automatic 30% daily-close rule. While halted the file is
//!   left in place and acted on once RISK_ACK clears the halt (state may be
//!   inconsistent during a halt, so no order is sent from it blindly). If
//!   ARM and DISARM are both present, DISARM wins and the ARM is discarded.
//! - `bull_holder/KILL_SWITCH` exists → no arming / no further tranches or
//!   ADDs / no stop re-placement (protective exits, including DISARM, still run)
//! - `bull_holder/RISK_ACK`   touch → clear a reconcile/data halt (consumed)
//!
//! KNOWN GAPS before any live use (bot-strategy#895 rollout gates):
//! - `BULL_HOLDER_DRY_RUN=false` is refused at startup (code change to lift).
//! - Lighter trigger orders have not been exercised live by pairtrade.
//! - Prometheus export is not wired (status JSON only); follow-up.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{FixedOffset, TimeZone, Timelike, Utc};
use debot::directional::{
    append_jsonl, config_fingerprint, load_json, persist_json, refuse_live, Sentinels,
};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, OrderSide, TpSl, TriggerOrderStyle};
use env_logger::Builder;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BOT: &str = "bull_holder";

fn init_logger() {
    let offset_seconds = std::env::var("TIMEZONE_OFFSET")
        .unwrap_or_else(|_| "3600".to_string())
        .parse::<i32>()
        .unwrap_or(3600);
    let offset = FixedOffset::east_opt(offset_seconds).unwrap_or(FixedOffset::east_opt(0).unwrap());
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    Builder::from_env(env)
        .format(move |buf, record| {
            let local_now = Utc::now().with_timezone(&offset);
            writeln!(
                buf,
                "{} [{}] - {}",
                local_now.format("%Y-%m-%dT%H:%M:%S%z"),
                record.level(),
                record.args()
            )
        })
        .init();
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// ---------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}
fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
struct Config {
    instance_id: String,
    dry_run: bool,
    /// Perp symbols on Lighter, e.g. ["BTC", "ETH"].
    symbols: Vec<String>,
    /// Lighter perp symbol → Hyperliquid spot market (e.g. BTC → UBTC/USDC).
    hl_spot_market: BTreeMap<String, String>,
    /// Total capital the holder manages across both venues (USD).
    equity_usd: f64,
    /// Fraction of `equity_usd` deployed as spot (all symbols together).
    spot_fraction: f64,
    /// Fraction of `equity_usd` deployed as perp notional (all symbols together).
    perp_fraction: f64,
    /// Number of equal daily tranches the book is built in after ARM (1 = all
    /// at once). Per-symbol tranche notionals are fixed at ARM time.
    entry_tranches: u32,
    /// Daily-close exit: close < peak × (1 − exit_dd_pct/100).
    exit_dd_pct: f64,
    /// Lighter exchange stop (insurance) level: peak × (1 − stop_dd_pct/100).
    stop_dd_pct: f64,
    /// Main loop cadence.
    tick_secs: u64,
    /// Daily evaluation runs once the UTC day is at least this old (seconds
    /// after 00:00 UTC), so the venue's daily candle is closed and published.
    daily_eval_after_utc_secs: u32,
    /// Consecutive daily-close fetch failures that trigger a halt.
    max_close_fetch_failures: u32,
    /// IOC slippage tolerance for the Hyperliquid spot legs (bps).
    hl_taker_slippage_bps: u32,
    /// Reconcile: |expected − actual| / expected above this (%) halts.
    reconcile_tolerance_pct: f64,
    reconcile_every_secs: u64,
    hl_info_url: String,
    arm_path: PathBuf,
    add_path: PathBuf,
    disarm_path: PathBuf,
    kill_switch_path: PathBuf,
    risk_ack_path: PathBuf,
    state_path: PathBuf,
    status_path: PathBuf,
    pnl_log_path: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self> {
        let instance_id = env_string("BULL_HOLDER_INSTANCE_ID", "bull-holder");
        let base_dir = PathBuf::from(env_string("BULL_HOLDER_BASE_DIR", "/opt/debot-bull-holder"));
        let dir = base_dir.join("bull_holder");
        let symbols: Vec<String> = env_string("BULL_HOLDER_SYMBOLS", "BTC,ETH")
            .split(',')
            .map(|s| s.trim().to_ascii_uppercase())
            .filter(|s| !s.is_empty())
            .collect();
        if symbols.is_empty() {
            bail!("BULL_HOLDER_SYMBOLS is empty");
        }
        let mut hl_spot_market = BTreeMap::new();
        for s in &symbols {
            let default = match s.as_str() {
                "BTC" => "UBTC/USDC",
                "ETH" => "UETH/USDC",
                "SOL" => "USOL/USDC",
                "HYPE" => "HYPE/USDC",
                _ => "",
            };
            let m = env_string(&format!("BULL_HOLDER_HL_SPOT_{s}"), default);
            if m.is_empty() {
                bail!("no Hyperliquid spot market for {s}: set BULL_HOLDER_HL_SPOT_{s}");
            }
            hl_spot_market.insert(s.clone(), m);
        }
        let cfg = Self {
            instance_id,
            dry_run: env_bool("BULL_HOLDER_DRY_RUN", true),
            symbols,
            hl_spot_market,
            equity_usd: env_f64("BULL_HOLDER_EQUITY_USD", 1_000.0),
            spot_fraction: env_f64("BULL_HOLDER_SPOT_FRACTION", 0.90),
            perp_fraction: env_f64("BULL_HOLDER_PERP_FRACTION", 0.45),
            entry_tranches: env_u32("BULL_HOLDER_ENTRY_TRANCHES", 1),
            exit_dd_pct: env_f64("BULL_HOLDER_EXIT_DD_PCT", 30.0),
            stop_dd_pct: env_f64("BULL_HOLDER_STOP_DD_PCT", 35.0),
            tick_secs: env_u64("BULL_HOLDER_TICK_SECS", 60),
            daily_eval_after_utc_secs: env_u32("BULL_HOLDER_DAILY_EVAL_AFTER_UTC_SECS", 300),
            max_close_fetch_failures: env_u32("BULL_HOLDER_MAX_CLOSE_FETCH_FAILURES", 3),
            hl_taker_slippage_bps: env_u32("BULL_HOLDER_HL_TAKER_SLIPPAGE_BPS", 30),
            reconcile_tolerance_pct: env_f64("BULL_HOLDER_RECONCILE_TOLERANCE_PCT", 2.0),
            reconcile_every_secs: env_u64("BULL_HOLDER_RECONCILE_EVERY_SECS", 600),
            hl_info_url: env_string(
                "BULL_HOLDER_HL_INFO_URL",
                "https://api.hyperliquid.xyz/info",
            ),
            arm_path: PathBuf::from(env_string(
                "BULL_HOLDER_ARM_PATH",
                &dir.join("ARM").to_string_lossy(),
            )),
            add_path: PathBuf::from(env_string(
                "BULL_HOLDER_ADD_PATH",
                &dir.join("ADD").to_string_lossy(),
            )),
            disarm_path: PathBuf::from(env_string(
                "BULL_HOLDER_DISARM_PATH",
                &dir.join("DISARM").to_string_lossy(),
            )),
            kill_switch_path: PathBuf::from(env_string(
                "BULL_HOLDER_KILL_SWITCH_PATH",
                &dir.join("KILL_SWITCH").to_string_lossy(),
            )),
            risk_ack_path: PathBuf::from(env_string(
                "BULL_HOLDER_RISK_ACK_PATH",
                &dir.join("RISK_ACK").to_string_lossy(),
            )),
            state_path: PathBuf::from(env_string(
                "BULL_HOLDER_STATE_PATH",
                &dir.join("state.json").to_string_lossy(),
            )),
            status_path: PathBuf::from(env_string(
                "BULL_HOLDER_STATUS_PATH",
                &dir.join("status.json").to_string_lossy(),
            )),
            pnl_log_path: PathBuf::from(env_string(
                "BULL_HOLDER_PNL_LOG_PATH",
                &dir.join("pnl_log.jsonl").to_string_lossy(),
            )),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if !(0.0 < self.exit_dd_pct && self.exit_dd_pct < 100.0) {
            bail!("BULL_HOLDER_EXIT_DD_PCT must be in (0,100)");
        }
        if !(self.stop_dd_pct > self.exit_dd_pct && self.stop_dd_pct < 100.0) {
            bail!("BULL_HOLDER_STOP_DD_PCT must be > EXIT_DD_PCT and < 100 (the exchange stop is insurance outside the daily rule)");
        }
        if self.equity_usd <= 0.0 {
            bail!("BULL_HOLDER_EQUITY_USD must be > 0");
        }
        if !(0.0 < self.spot_fraction && self.spot_fraction <= 1.0) {
            bail!("BULL_HOLDER_SPOT_FRACTION must be in (0,1]");
        }
        if !(0.0 <= self.perp_fraction && self.perp_fraction <= 1.0) {
            bail!("BULL_HOLDER_PERP_FRACTION must be in [0,1]");
        }
        if self.hl_taker_slippage_bps == 0 || self.hl_taker_slippage_bps > 1_000 {
            bail!("BULL_HOLDER_HL_TAKER_SLIPPAGE_BPS must be in 1..=1000");
        }
        // `daily_eval_due` compares against `num_seconds_from_midnight()`,
        // which only ever returns 0..86_400; a value at or above that would
        // make the daily-close exit rule silently never fire again.
        if self.entry_tranches == 0 || self.entry_tranches > 30 {
            bail!("BULL_HOLDER_ENTRY_TRANCHES must be in 1..=30 (a ladder longer than a month is DCA, not an entry)");
        }
        if self.daily_eval_after_utc_secs >= 86_400 {
            bail!("BULL_HOLDER_DAILY_EVAL_AFTER_UTC_SECS must be < 86400 (seconds after UTC midnight)");
        }
        Ok(())
    }

    fn fingerprint(&self) -> String {
        config_fingerprint(&[
            ("symbols", self.symbols.join(",")),
            ("equity_usd", format!("{:.2}", self.equity_usd)),
            ("spot_fraction", format!("{:.4}", self.spot_fraction)),
            ("perp_fraction", format!("{:.4}", self.perp_fraction)),
            ("entry_tranches", self.entry_tranches.to_string()),
            ("exit_dd_pct", format!("{:.2}", self.exit_dd_pct)),
            ("stop_dd_pct", format!("{:.2}", self.stop_dd_pct)),
            (
                "hl_taker_slippage_bps",
                self.hl_taker_slippage_bps.to_string(),
            ),
        ])
    }
}

// ---------------------------------------------------------------------
// Pure decision logic (unit-tested)
// ---------------------------------------------------------------------

/// Exit level for a given peak close and drawdown percentage.
fn level_below_peak(peak: f64, dd_pct: f64) -> f64 {
    peak * (1.0 - dd_pct / 100.0)
}

/// The single exit rule: completed daily close below the exit level.
fn should_exit(close: f64, peak: f64, exit_dd_pct: f64) -> bool {
    close < level_below_peak(peak, exit_dd_pct)
}

/// Per-symbol target notionals (USD) for the two legs.
fn leg_notionals(
    equity_usd: f64,
    spot_fraction: f64,
    perp_fraction: f64,
    n_symbols: usize,
) -> (f64, f64) {
    let n = n_symbols.max(1) as f64;
    (
        equity_usd * spot_fraction / n,
        equity_usd * perp_fraction / n,
    )
}

/// Round a notional/price size DOWN to the venue's size decimals.
fn size_from_notional(notional_usd: f64, price: f64, size_decimals: u32) -> Decimal {
    if price <= 0.0 || notional_usd <= 0.0 {
        return Decimal::ZERO;
    }
    let raw = Decimal::from_f64(notional_usd / price).unwrap_or(Decimal::ZERO);
    raw.round_dp_with_strategy(size_decimals, RoundingStrategy::ToZero)
}

/// Relative mismatch check used by reconcile.
fn within_tolerance(expected: f64, actual: f64, tol_pct: f64) -> bool {
    if expected.abs() < 1e-12 {
        return actual.abs() < 1e-12;
    }
    ((actual - expected) / expected).abs() * 100.0 <= tol_pct
}

/// UTC calendar date string for a unix timestamp.
fn utc_date(ts: u64) -> String {
    Utc.timestamp_opt(ts as i64, 0)
        .single()
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// Whether the daily evaluation is due: we have not yet evaluated yesterday's
/// completed close and the UTC day is old enough for the candle to be closed.
fn daily_eval_due(now: u64, last_evaluated_close_date: Option<&str>, after_utc_secs: u32) -> bool {
    if !past_daily_slot(now, after_utc_secs) {
        return false;
    }
    let yesterday = utc_date(now.saturating_sub(86_400));
    last_evaluated_close_date != Some(yesterday.as_str())
}

/// What the operator's sentinel files ask the bot to do on this tick. Pure so
/// the precedence rules are unit-testable without connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperatorIntent {
    /// Nothing requested.
    Idle,
    /// DISARM present while a book is held: close every leg now.
    DisarmNow,
    /// DISARM present but the bot is halted: leave the file, act after RISK_ACK.
    DisarmDeferredByHalt,
    /// DISARM present with nothing held (Off/Exited): consume it (and any ARM
    /// alongside it — DISARM always wins over ARM).
    DisarmNothingToDo { cancels_arm: bool },
    /// ARM present with nothing held and no kill switch: open the book.
    ArmNow,
    /// ARM present but KILL_SWITCH engaged: leave the file, do not arm.
    ArmBlockedByKill,
    /// ARM present while already On: consume it, nothing else (never double).
    ArmIgnoredAlreadyOn,
}

fn resolve_operator_intent(
    mode: Mode,
    halted: bool,
    kill: bool,
    arm_present: bool,
    disarm_present: bool,
) -> OperatorIntent {
    if disarm_present {
        if halted {
            return OperatorIntent::DisarmDeferredByHalt;
        }
        return match mode {
            Mode::On => OperatorIntent::DisarmNow,
            Mode::Off | Mode::Exited => OperatorIntent::DisarmNothingToDo {
                cancels_arm: arm_present,
            },
        };
    }
    if halted || !arm_present {
        return OperatorIntent::Idle;
    }
    match mode {
        Mode::On => OperatorIntent::ArmIgnoredAlreadyOn,
        Mode::Off | Mode::Exited => {
            if kill {
                OperatorIntent::ArmBlockedByKill
            } else {
                OperatorIntent::ArmNow
            }
        }
    }
}

/// Whether the next entry tranche is due: something remains, the UTC day is
/// old enough for the daily slot, and no tranche has run yet today. Keyed on
/// today's date (unlike `daily_eval_due`, which is keyed on yesterday's
/// completed close), so days the bot is down simply push the ladder out.
fn tranche_due(
    now: u64,
    remaining: u32,
    last_tranche_date: Option<&str>,
    after_utc_secs: u32,
) -> bool {
    if remaining == 0 {
        return false;
    }
    if !past_daily_slot(now, after_utc_secs) {
        return false;
    }
    last_tranche_date != Some(utc_date(now).as_str())
}

/// The one time gate both daily schedules share: is the UTC day at least
/// `after_utc_secs` old (so a completed daily candle is published)?
fn past_daily_slot(now: u64, after_utc_secs: u32) -> bool {
    match Utc.timestamp_opt(now as i64, 0).single() {
        Some(dt) => dt.num_seconds_from_midnight() >= after_utc_secs,
        None => false,
    }
}

/// Which legs of a symbol still need an order in the current tranche attempt.
/// `(need_spot, need_perp)`; a zero perp allocation never needs a perp order.
fn legs_pending(progress: &LegProgress, perp_allocated: bool) -> (bool, bool) {
    (!progress.spot_done, perp_allocated && !progress.perp_done)
}

/// Whether the resting Lighter stop already covers the current book: same
/// trigger level AND same size. A tranche that grows `perp_size` on a day the
/// Lighter peak did not move must still re-place the stop, or the new
/// exposure is unprotected (pairtrade#268 review).
fn stop_is_current(
    prev_level: Option<f64>,
    prev_size: Option<f64>,
    has_order: bool,
    level: f64,
    size: f64,
) -> bool {
    has_order
        && prev_level.is_some_and(|l| (l - level).abs() < 1e-9)
        && prev_size.is_some_and(|z| (z - size).abs() < 1e-12)
}

/// Parse the optional tranche count in an ADD file: empty/whitespace → 1,
/// a positive integer → that, anything else → None (caller warns and
/// ignores the request rather than guessing).
fn parse_add_count(contents: &str) -> Option<u32> {
    let t = contents.trim();
    if t.is_empty() {
        return Some(1);
    }
    t.parse::<u32>().ok().filter(|k| *k >= 1 && *k <= 30)
}

#[derive(Debug, Clone, PartialEq)]
struct DailyCandle {
    /// Candle open time (ms).
    t_ms: u64,
    /// Candle close time (ms, inclusive end).
    close_time_ms: u64,
    close: f64,
}

/// Pick the most recent candle that is fully closed at `now_ms`.
fn last_completed_candle(candles: &[DailyCandle], now_ms: u64) -> Option<&DailyCandle> {
    candles
        .iter()
        .filter(|c| c.close_time_ms < now_ms)
        .max_by_key(|c| c.t_ms)
}

fn parse_hl_candles(v: &serde_json::Value) -> Result<Vec<DailyCandle>> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("candleSnapshot: not an array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for c in arr {
        let t_ms = c["t"].as_u64().ok_or_else(|| anyhow!("candle missing t"))?;
        let close_time_ms = c["T"].as_u64().ok_or_else(|| anyhow!("candle missing T"))?;
        let close: f64 = c["c"]
            .as_str()
            .ok_or_else(|| anyhow!("candle missing c"))?
            .parse()
            .context("candle close parse")?;
        if close <= 0.0 {
            bail!("candle close <= 0");
        }
        out.push(DailyCandle {
            t_ms,
            close_time_ms,
            close,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------
// State
// ---------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Mode {
    #[default]
    Off,
    On,
    Exited,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct LegState {
    spot_size: f64,
    spot_cost_usd: f64,
    perp_size: f64,
    perp_cost_usd: f64,
    /// Peak Hyperliquid daily close since ARM — drives the daily exit rule
    /// (`exit_level`) per the module doc.
    peak_close: f64,
    exit_level: f64,
    /// Peak Lighter mark/index price observed at stop-placement time. The
    /// Lighter exchange-side stop's trigger price is derived from THIS, not
    /// from `peak_close`, because the stop fires against Lighter's own
    /// price: pricing it off the Hyperliquid close would drift with any
    /// cross-venue basis (liquidity crunch, funding-driven divergence) and
    /// fire at an unintended level relative to the safety margin it is
    /// meant to provide (see pairtrade#258 review).
    #[serde(default)]
    lighter_peak: f64,
    stop_level: Option<f64>,
    stop_order_id: Option<String>,
    /// perp_size the resting stop was placed for; re-place when it changes.
    #[serde(default)]
    stop_size: Option<f64>,
    /// Date (UTC) of the last completed daily close that was evaluated.
    last_close_date: Option<String>,
    last_close: Option<f64>,
    close_fetch_failures: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct State {
    #[serde(default)]
    mode: Mode,
    #[serde(default)]
    armed_at: Option<u64>,
    #[serde(default)]
    exited_at: Option<u64>,
    #[serde(default)]
    exit_reason: Option<String>,
    #[serde(default)]
    legs: BTreeMap<String, LegState>,
    #[serde(default)]
    halted: bool,
    #[serde(default)]
    halt_reason: Option<String>,
    #[serde(default)]
    realized_pnl_total_usd: f64,
    #[serde(default)]
    cycles: u64,
    /// Per-symbol notionals of ONE tranche (USD), fixed at ARM.
    #[serde(default)]
    tranche_spot_usd: f64,
    #[serde(default)]
    tranche_perp_usd: f64,
    #[serde(default)]
    tranches_remaining: u32,
    #[serde(default)]
    tranches_done: u32,
    /// UTC date of the last tranche that ran (one per day).
    #[serde(default)]
    last_tranche_date: Option<String>,
    /// Per-symbol progress of the tranche attempt currently in flight. A
    /// failed attempt (halt) resumes from here after RISK_ACK instead of
    /// re-sending orders for legs that already filled (pairtrade#268 review).
    /// Empty when no attempt is in flight.
    #[serde(default)]
    tranche_progress: BTreeMap<String, LegProgress>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LegProgress {
    spot_done: bool,
    perp_done: bool,
}

// ---------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------

struct Engine {
    cfg: Config,
    hl: Arc<dyn DexConnector + Send + Sync>,
    lt: Arc<dyn DexConnector + Send + Sync>,
    http: reqwest::Client,
    sentinels: Sentinels,
    state: State,
    last_status_write: u64,
    last_reconcile: u64,
}

struct Quote {
    price: f64,
    size_decimals: u32,
    min_order: f64,
}

impl Engine {
    fn persist(&self) {
        if let Err(e) = persist_json(&self.cfg.state_path, &self.state) {
            log::error!("[STATE] persist failed: {e:?}");
        }
    }

    fn halt(&mut self, reason: String) {
        log::error!(
            "[HALT] {reason} — automatic actions blocked until RISK_ACK at {}",
            self.cfg.risk_ack_path.display()
        );
        self.state.halted = true;
        self.state.halt_reason = Some(reason);
        self.persist();
    }

    async fn quote(
        &self,
        venue: &Arc<dyn DexConnector + Send + Sync>,
        symbol: &str,
    ) -> Result<Quote> {
        let t = venue
            .get_ticker(symbol, None)
            .await
            .with_context(|| format!("get_ticker {symbol}"))?;
        let price = t.price.to_f64().unwrap_or(0.0);
        if price <= 0.0 {
            bail!("ticker {symbol}: non-positive price");
        }
        Ok(Quote {
            price,
            size_decimals: t.size_decimals.unwrap_or(4),
            min_order: t.min_order.and_then(|d| d.to_f64()).unwrap_or(0.0),
        })
    }

    // --------------------------------------------------------------- orders

    /// Hyperliquid spot IOC (buy or sell) — dry-run returns the requested size.
    async fn hl_spot_ioc(&self, market: &str, size: Decimal, side: OrderSide) -> Result<Decimal> {
        if self.cfg.dry_run {
            log::info!("[DRY_RUN] HL spot IOC {side} {market} size={size}");
            return Ok(size);
        }
        let resp = self
            .hl
            .create_order_taker_ioc(market, size, side, self.cfg.hl_taker_slippage_bps, false)
            .await
            .with_context(|| format!("HL spot IOC {side} {market}"))?;
        Ok(resp.ordered_size)
    }

    /// Lighter perp taker (price=None → IOC with protection price; the same
    /// path every pairtrade taker caller uses).
    async fn lt_perp_taker(
        &self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        reduce_only: bool,
    ) -> Result<Decimal> {
        if self.cfg.dry_run {
            log::info!("[DRY_RUN] Lighter perp taker {side} {symbol} size={size} reduce_only={reduce_only}");
            return Ok(size);
        }
        let resp = self
            .lt
            .create_order(symbol, size, side, None, None, reduce_only, None)
            .await
            .with_context(|| format!("Lighter perp {side} {symbol}"))?;
        Ok(resp.ordered_size)
    }

    /// Rest (or move) the Lighter exchange-side stop for the perp leg.
    async fn place_stop(&mut self, symbol: &str) -> Result<()> {
        let Some(mut leg) = self.state.legs.get(symbol).cloned() else {
            return Ok(());
        };
        if leg.perp_size <= 0.0 {
            return Ok(());
        }
        // The stop must fire against Lighter's own price, not Hyperliquid's
        // daily close — refresh the Lighter-native peak here.
        let lighter_price = self.quote(&self.lt, symbol).await?.price;
        leg.lighter_peak = leg.lighter_peak.max(lighter_price);
        let level = level_below_peak(leg.lighter_peak, self.cfg.stop_dd_pct);
        if stop_is_current(
            leg.stop_level,
            leg.stop_size,
            leg.stop_order_id.is_some(),
            level,
            leg.perp_size,
        ) {
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.lighter_peak = leg.lighter_peak;
            }
            self.persist();
            return Ok(());
        }
        // Cancel the previous stop first so we never rest two. Only clear the
        // old id/level once the cancel actually succeeds (or there was
        // nothing to cancel) — on a failed cancel the old order may still be
        // resting, and forgetting its id here would make it permanently
        // untracked (this guard could no longer detect and retry it).
        let mut prior_cancel_failed = false;
        if let Some(old) = &leg.stop_order_id {
            if !self.cfg.dry_run {
                if let Err(e) = self.lt.cancel_order(symbol, old).await {
                    log::warn!(
                        "[STOP] cancel previous stop {old} failed, keeping it tracked for retry: {e:?}"
                    );
                    prior_cancel_failed = true;
                }
            }
        }
        if prior_cancel_failed {
            // Persist the refreshed peak but leave the old stop_order_id/
            // stop_level in place so the next call retries the cancel before
            // resting a new one.
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.lighter_peak = leg.lighter_peak;
            }
            self.persist();
            bail!("Lighter stop {symbol}: could not cancel the previous stop, deferring re-place");
        }
        let size = Decimal::from_f64(leg.perp_size).unwrap_or(Decimal::ZERO);
        let trigger = Decimal::from_f64(level).unwrap_or(Decimal::ZERO);
        let order_id = if self.cfg.dry_run {
            format!("dry-run-stop-{}", now_secs())
        } else {
            // `side` is the POSITION side for Lighter's TP/SL helper (long
            // position → sell stop).
            let resp = self
                .lt
                .create_advanced_trigger_order(
                    symbol,
                    size,
                    OrderSide::Long,
                    trigger,
                    None,
                    TriggerOrderStyle::Market,
                    None,
                    TpSl::Sl,
                    true,
                    None,
                )
                .await
                .with_context(|| format!("Lighter stop {symbol} @ {level:.2}"))?;
            resp.order_id
        };
        log::info!(
            "[STOP] {symbol} stop-loss resting at {level:.2} for size {} (lighter_peak {:.2}, {}%) id={order_id}",
            leg.perp_size,
            leg.lighter_peak,
            self.cfg.stop_dd_pct
        );
        if let Some(l) = self.state.legs.get_mut(symbol) {
            l.lighter_peak = leg.lighter_peak;
            l.stop_level = Some(level);
            l.stop_size = Some(leg.perp_size);
            l.stop_order_id = Some(order_id);
        }
        self.persist();
        Ok(())
    }

    async fn cancel_stop(&mut self, symbol: &str) {
        let Some(leg) = self.state.legs.get(symbol).cloned() else {
            return;
        };
        let Some(id) = leg.stop_order_id else {
            return;
        };
        if !self.cfg.dry_run {
            if let Err(e) = self.lt.cancel_order(symbol, &id).await {
                log::warn!(
                    "[STOP] cancel {id} failed, leaving it tracked in state (order may still be resting): {e:?}"
                );
                return;
            }
        }
        if let Some(l) = self.state.legs.get_mut(symbol) {
            l.stop_order_id = None;
            l.stop_level = None;
            l.stop_size = None;
        }
    }

    // ------------------------------------------------------------ lifecycle

    async fn arm(&mut self) -> Result<()> {
        let n = self.cfg.symbols.len();
        let (spot_notional, perp_notional) = leg_notionals(
            self.cfg.equity_usd,
            self.cfg.spot_fraction,
            self.cfg.perp_fraction,
            n,
        );
        let k = self.cfg.entry_tranches.max(1);
        let (spot_tr, perp_tr) = (spot_notional / k as f64, perp_notional / k as f64);
        log::info!(
            "[ARM] arming {} symbols: spot ${spot_notional:.0} + perp ${perp_notional:.0} each, in {k} tranche(s) of spot ${spot_tr:.0} + perp ${perp_tr:.0}",
            n
        );
        // Fresh arm: any legs from a previous, unrelated cycle must already be
        // gone (mode is only Off/Exited here). Clear defensively so a retry
        // after a failed arm never mixes stale sizes into the new attempt.
        // `mode` stays Off until the first order actually fills below —
        // flipping it to On here (before any exposure exists) would strand
        // the bot in On with zero legs and no way to re-ARM if a read-only
        // quote fails before any order is sent.
        self.state.legs.clear();
        self.state.tranche_spot_usd = spot_tr;
        self.state.tranche_perp_usd = perp_tr;
        self.state.tranches_remaining = k;
        self.state.tranches_done = 0;
        self.state.last_tranche_date = None;
        self.state.tranche_progress.clear();
        self.persist();
        self.buy_tranche("ARM").await
    }

    /// Buy one tranche (`state.tranche_*_usd` per symbol) of both legs for
    /// every symbol. The first call of a cycle creates the legs and flips the
    /// book to On; later calls add to them. Each fill is persisted as it
    /// happens so a failure partway never leaves exposure unrecorded.
    async fn buy_tranche(&mut self, why: &str) -> Result<()> {
        let spot_notional = self.state.tranche_spot_usd;
        let perp_notional = self.state.tranche_perp_usd;
        let n_th = self.state.tranches_done + 1;
        if !self.state.tranche_progress.is_empty() {
            log::warn!(
                "[ENTRY] resuming tranche {n_th} after a partial failure; already filled: {:?}",
                self.state.tranche_progress
            );
        }
        for sym in self.cfg.symbols.clone() {
            let progress = self
                .state
                .tranche_progress
                .get(&sym)
                .copied()
                .unwrap_or_default();
            let (need_spot, need_perp) = legs_pending(&progress, perp_notional > 0.0);
            if !need_spot && !need_perp {
                log::info!("[ENTRY] {sym}: tranche {n_th} already filled, skipping");
                continue;
            }
            let market = self.cfg.hl_spot_market[&sym].clone();
            // Read-only: safe to bail before any order for this symbol is sent.
            let hq = self.quote(&self.hl, &market).await?;
            let lq = self.quote(&self.lt, &sym).await?;
            let spot_size = size_from_notional(spot_notional, hq.price, hq.size_decimals);
            let perp_size = size_from_notional(perp_notional, lq.price, lq.size_decimals);
            if need_spot && spot_size.to_f64().unwrap_or(0.0) < hq.min_order {
                bail!(
                    "{market}: tranche spot size {spot_size} below min_order {} (raise EQUITY_USD or lower ENTRY_TRANCHES)",
                    hq.min_order
                );
            }
            if need_perp && perp_size.to_f64().unwrap_or(0.0) < lq.min_order {
                bail!(
                    "{sym}: tranche perp size {perp_size} below min_order {} (raise EQUITY_USD or lower ENTRY_TRANCHES)",
                    lq.min_order
                );
            }
            // Record the leg BEFORE/AS EACH order fills so a failure partway
            // through this symbol (or the next one) never leaves a filled
            // position invisible to state, the daily exit rule, or the stop.
            // A stray re-ARM after such a failure must never double the
            // position it already holds.
            let mut leg = match self.state.legs.get(&sym) {
                Some(l) => l.clone(),
                None => {
                    let peak = hq.price.max(lq.price);
                    LegState {
                        spot_size: 0.0,
                        spot_cost_usd: 0.0,
                        perp_size: 0.0,
                        perp_cost_usd: 0.0,
                        peak_close: peak,
                        exit_level: level_below_peak(peak, self.cfg.exit_dd_pct),
                        lighter_peak: lq.price,
                        stop_level: None,
                        stop_order_id: None,
                        stop_size: None,
                        last_close_date: None,
                        last_close: None,
                        close_fetch_failures: 0,
                    }
                }
            };
            let mut fs = 0.0;
            if need_spot {
                let filled_spot = self
                    .hl_spot_ioc(&market, spot_size, OrderSide::Long)
                    .await
                    .with_context(|| {
                        format!(
                            "{sym}: spot {why} tranche {n_th} failed (this leg took no exposure; legs already filled in this attempt are recorded and will be skipped on retry)"
                        )
                    })?;
                fs = filled_spot.to_f64().unwrap_or(0.0);
                leg.spot_size += fs;
                leg.spot_cost_usd += fs * hq.price;
            }
            self.state.legs.insert(sym.clone(), leg.clone());
            self.state
                .tranche_progress
                .entry(sym.clone())
                .or_default()
                .spot_done = true;
            // Real exposure now exists: flip to On (only once) so a crash or
            // a later error in this loop never leaves a filled position
            // recorded under Off/Exited, where a stray re-ARM would double it.
            if self.state.mode != Mode::On {
                self.state.mode = Mode::On;
                self.state.armed_at = Some(now_secs());
                self.state.exited_at = None;
                self.state.exit_reason = None;
                self.state.cycles += 1;
            }
            self.persist();
            let mut fp = 0.0;
            if need_perp {
                let filled_perp = self
                    .lt_perp_taker(&sym, perp_size, OrderSide::Long, false)
                    .await
                    .with_context(|| {
                        format!(
                            "{sym}: perp {why} tranche {n_th} failed AFTER spot filled ({fs} @ {:.2}) — the spot part is recorded and will be skipped on retry; only this perp leg is re-sent",
                            hq.price
                        )
                    })?;
                fp = filled_perp.to_f64().unwrap_or(0.0);
                leg.perp_size += fp;
                leg.perp_cost_usd += fp * lq.price;
                self.state.legs.insert(sym.clone(), leg.clone());
            }
            self.state
                .tranche_progress
                .entry(sym.clone())
                .or_default()
                .perp_done = true;
            self.persist();
            log::info!(
                "[ENTRY] {sym} {why} tranche {n_th}/{}: +spot {} @ {:.2} +perp {} @ {:.2}; book spot {} (${:.0}) perp {} (${:.0}); peak={:.2} exit_level={:.2}",
                self.state.tranches_done + self.state.tranches_remaining,
                fs, hq.price, fp, lq.price,
                leg.spot_size, leg.spot_cost_usd, leg.perp_size, leg.perp_cost_usd, leg.peak_close, leg.exit_level
            );
            if let Err(e) = self.place_stop(&sym).await {
                log::error!("[STOP] stop (re)placement for {sym} failed: {e:?}");
            }
        }
        self.state.tranche_progress.clear();
        self.state.tranches_done += 1;
        self.state.tranches_remaining = self.state.tranches_remaining.saturating_sub(1);
        self.state.last_tranche_date = Some(utc_date(now_secs()));
        self.persist();
        if self.state.tranches_remaining > 0 {
            log::info!(
                "[ENTRY] {} tranche(s) remaining, next on the following UTC day",
                self.state.tranches_remaining
            );
        }
        Ok(())
    }

    /// Best-effort reference price for PnL accounting: a live quote, falling
    /// back to the last known daily close from state. Returns `None` (never
    /// `0.0`) when nothing trustworthy is available, so a leg that closed
    /// without a known price is recorded as pnl_known=false rather than a
    /// fabricated large loss. The exit order itself never depends on this —
    /// it is placed regardless of whether a price could be found.
    async fn exit_reference_price(
        &self,
        venue: &Arc<dyn DexConnector + Send + Sync>,
        symbol: &str,
        fallback: Option<f64>,
    ) -> (Option<f64>, &'static str) {
        match self.quote(venue, symbol).await {
            Ok(q) if q.price > 0.0 => (Some(q.price), "quote"),
            Ok(_) => (fallback.filter(|f| *f > 0.0), "fallback_last_close"),
            Err(e) => {
                log::warn!(
                    "[EXIT] {symbol} pre-exit quote failed, PnL will use a fallback price if any: {e:?}"
                );
                (fallback.filter(|f| *f > 0.0), "fallback_last_close")
            }
        }
    }

    async fn exit_all(&mut self, reason: &str) {
        log::warn!("[EXIT] closing all legs: {reason}");
        let mut total = 0.0;
        let mut any_leg_still_open = false;
        for sym in self.cfg.symbols.clone() {
            let Some(mut leg) = self.state.legs.get(&sym).cloned() else {
                continue;
            };
            self.cancel_stop(&sym).await;
            let market = self.cfg.hl_spot_market[&sym].clone();
            let orig_spot_size = leg.spot_size;
            let orig_perp_size = leg.perp_size;
            let mut spot_pnl: Option<f64> = None;
            let mut spot_px_source = "n/a";
            let mut perp_pnl: Option<f64> = None;
            let mut perp_px_source = "n/a";

            if orig_spot_size > 0.0 {
                let (px, src) = self
                    .exit_reference_price(&self.hl, &market, leg.last_close)
                    .await;
                let size = Decimal::from_f64(orig_spot_size).unwrap_or(Decimal::ZERO);
                match self.hl_spot_ioc(&market, size, OrderSide::Short).await {
                    Ok(f) => {
                        log::info!("[EXIT] {market} spot sold {f}");
                        leg.spot_size = 0.0; // closed regardless of whether we could price it
                        spot_px_source = src;
                        spot_pnl = px.map(|p| p * orig_spot_size - leg.spot_cost_usd);
                        if px.is_none() {
                            log::warn!(
                                "[EXIT] {market} closed but no trustworthy price available — PnL for this leg is unknown, not zero"
                            );
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "[EXIT] {market} spot sell FAILED, leg remains OPEN in state: {e:?}"
                        );
                        self.halt(format!("spot exit failed for {market}: {e}"));
                        any_leg_still_open = true;
                    }
                }
            }
            if orig_perp_size > 0.0 {
                let (px, src) = self
                    .exit_reference_price(&self.lt, &sym, leg.last_close)
                    .await;
                let size = Decimal::from_f64(orig_perp_size).unwrap_or(Decimal::ZERO);
                match self.lt_perp_taker(&sym, size, OrderSide::Short, true).await {
                    Ok(f) => {
                        log::info!("[EXIT] {sym} perp closed {f}");
                        leg.perp_size = 0.0;
                        perp_px_source = src;
                        perp_pnl = px.map(|p| p * orig_perp_size - leg.perp_cost_usd);
                        if px.is_none() {
                            log::warn!(
                                "[EXIT] {sym} perp closed but no trustworthy price available — PnL for this leg is unknown, not zero"
                            );
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "[EXIT] {sym} perp close FAILED, leg remains OPEN in state: {e:?}"
                        );
                        self.halt(format!("perp exit failed for {sym}: {e}"));
                        any_leg_still_open = true;
                    }
                }
            }

            let pnl_known = orig_spot_size <= 0.0 || spot_pnl.is_some();
            let pnl_known = pnl_known && (orig_perp_size <= 0.0 || perp_pnl.is_some());
            let leg_total = spot_pnl.unwrap_or(0.0) + perp_pnl.unwrap_or(0.0);
            if pnl_known {
                total += leg_total;
            }
            let rec = serde_json::json!({
                "ts": now_secs(), "symbol": sym, "reason": reason,
                "spot_size_closed": orig_spot_size, "spot_cost_usd": leg.spot_cost_usd,
                "spot_pnl_usd": spot_pnl, "spot_px_source": spot_px_source,
                "perp_size_closed": orig_perp_size, "perp_cost_usd": leg.perp_cost_usd,
                "perp_pnl_usd": perp_pnl, "perp_px_source": perp_px_source,
                "peak_close": leg.peak_close, "exit_level": leg.exit_level,
                "pnl_usd_ex_funding": if pnl_known { Some(leg_total) } else { None },
                "pnl_known": pnl_known,
                "dry_run": self.cfg.dry_run,
            });
            if let Err(e) = append_jsonl(&self.cfg.pnl_log_path, &rec) {
                log::warn!("[PNL_LOG] append failed: {e:?}");
            }
            // Reset the cost basis only for what actually closed so a
            // remaining open leg's cost_usd (used by the next exit attempt)
            // still reflects its real, un-exited exposure.
            if leg.spot_size <= 0.0 {
                leg.spot_cost_usd = 0.0;
            }
            if leg.perp_size <= 0.0 {
                leg.perp_cost_usd = 0.0;
            }
            self.state.legs.insert(sym.clone(), leg.clone());
            self.persist();
            log::info!(
                "[EXIT] {sym} pnl(ex-funding)={} peak={:.2} exit_level={:.2}",
                if pnl_known {
                    format!("${leg_total:+.2}")
                } else {
                    "unknown".to_string()
                },
                leg.peak_close,
                leg.exit_level
            );
        }
        self.state.realized_pnl_total_usd += total;
        if self.state.tranches_remaining > 0 {
            log::warn!(
                "[EXIT] cancelling {} remaining entry tranche(s)",
                self.state.tranches_remaining
            );
            self.state.tranches_remaining = 0;
        }
        self.state.tranche_progress.clear();
        if any_leg_still_open {
            log::error!(
                "[EXIT] one or more legs failed to close — staying in mode=On (NOT marking Exited) until the operator resolves it via RISK_ACK"
            );
        } else {
            self.state.mode = Mode::Exited;
            self.state.exited_at = Some(now_secs());
            self.state.exit_reason = Some(reason.to_string());
        }
        self.persist();
    }

    // ---------------------------------------------------------- daily eval

    async fn fetch_daily_candles(&self, coin: &str) -> Result<Vec<DailyCandle>> {
        let now_ms = now_secs() * 1000;
        let body = serde_json::json!({
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": "1d", "startTime": now_ms.saturating_sub(6 * 86_400_000), "endTime": now_ms}
        });
        let v: serde_json::Value = self
            .http
            .post(&self.cfg.hl_info_url)
            .json(&body)
            .send()
            .await
            .context("candleSnapshot request")?
            .error_for_status()
            .context("candleSnapshot status")?
            .json()
            .await
            .context("candleSnapshot json")?;
        parse_hl_candles(&v)
    }

    async fn daily_eval(&mut self) {
        let now = now_secs();
        let mut exit_syms = Vec::new();
        for sym in self.cfg.symbols.clone() {
            let Some(leg) = self.state.legs.get(&sym).cloned() else {
                continue;
            };
            if !daily_eval_due(
                now,
                leg.last_close_date.as_deref(),
                self.cfg.daily_eval_after_utc_secs,
            ) {
                continue;
            }
            let candles = match self.fetch_daily_candles(&sym).await {
                Ok(c) => c,
                Err(e) => {
                    let n = leg.close_fetch_failures + 1;
                    log::warn!(
                        "[DAILY] {sym} close fetch failed ({n}/{}): {e:?}",
                        self.cfg.max_close_fetch_failures
                    );
                    if let Some(l) = self.state.legs.get_mut(&sym) {
                        l.close_fetch_failures = n;
                    }
                    if n >= self.cfg.max_close_fetch_failures {
                        self.halt(format!("{sym}: {n} consecutive daily-close fetch failures"));
                    }
                    self.persist();
                    continue;
                }
            };
            let Some(c) = last_completed_candle(&candles, now * 1000).cloned() else {
                log::warn!("[DAILY] {sym} no completed candle in snapshot");
                continue;
            };
            let close_date = utc_date(c.t_ms / 1000);
            if leg.last_close_date.as_deref() == Some(close_date.as_str()) {
                continue; // venue has not published a newer candle yet
            }
            let new_peak = leg.peak_close.max(c.close);
            let exit = should_exit(c.close, new_peak, self.cfg.exit_dd_pct);
            let exit_level = level_below_peak(new_peak, self.cfg.exit_dd_pct);
            log::info!(
                "[DAILY] {sym} close[{close_date}]={:.2} peak={:.2} exit_level={:.2} dd={:.1}% -> {}",
                c.close, new_peak, exit_level, (1.0 - c.close / new_peak) * 100.0,
                if exit { "EXIT" } else { "hold" }
            );
            if let Some(l) = self.state.legs.get_mut(&sym) {
                l.peak_close = new_peak;
                l.exit_level = exit_level;
                l.last_close_date = Some(close_date);
                l.last_close = Some(c.close);
                l.close_fetch_failures = 0;
            }
            self.persist();
            if exit {
                exit_syms.push(sym.clone());
            } else if new_peak > leg.peak_close && !self.sentinels.kill_switch_engaged() {
                if let Err(e) = self.place_stop(&sym).await {
                    log::error!("[STOP] re-place for {sym} failed: {e:?}");
                }
            }
        }
        if !exit_syms.is_empty() {
            // One symbol breaching its exit closes the whole book: the regime
            // call was wrong, not one asset.
            let reason = format!("daily close below exit level: {}", exit_syms.join(","));
            self.exit_all(&reason).await;
        }
    }

    // ----------------------------------------------------------- reconcile

    async fn reconcile(&mut self) {
        if self.cfg.dry_run {
            return; // nothing real to compare against
        }
        // Both reads are account-wide (all symbols in one call); fetch each
        // once instead of once per symbol.
        let positions = match self.lt.get_positions().await {
            Ok(p) => Some(p),
            Err(e) => {
                log::warn!("[RECONCILE] Lighter get_positions failed: {e:?}");
                None
            }
        };
        let balance = match self.hl.get_combined_balance().await {
            Ok(b) => Some(b),
            Err(e) => {
                log::warn!("[RECONCILE] HL get_combined_balance failed: {e:?}");
                None
            }
        };
        for sym in self.cfg.symbols.clone() {
            let Some(leg) = self.state.legs.get(&sym).cloned() else {
                continue;
            };
            // Perp leg: Lighter positions.
            if let Some(pos) = &positions {
                let actual = pos
                    .iter()
                    .filter(|p| p.symbol.eq_ignore_ascii_case(&sym))
                    .map(|p| p.size.to_f64().unwrap_or(0.0) * if p.sign < 0 { -1.0 } else { 1.0 })
                    .sum::<f64>();
                if !within_tolerance(leg.perp_size, actual, self.cfg.reconcile_tolerance_pct) {
                    self.halt(format!(
                        "{sym} perp mismatch: expected {:.6} actual {actual:.6}",
                        leg.perp_size
                    ));
                    return;
                }
            }
            // Spot leg: Hyperliquid spot balances (base token of the market).
            let market = self.cfg.hl_spot_market[&sym].clone();
            let base = market.split('/').next().unwrap_or("").to_ascii_uppercase();
            if let Some(b) = &balance {
                let actual = b
                    .spot_assets
                    .iter()
                    .filter(|a| a.symbol.eq_ignore_ascii_case(&base))
                    .map(|a| a.balance.to_f64().unwrap_or(0.0))
                    .sum::<f64>();
                if !within_tolerance(leg.spot_size, actual, self.cfg.reconcile_tolerance_pct) {
                    self.halt(format!(
                        "{market} spot mismatch: expected {:.6} actual {actual:.6}",
                        leg.spot_size
                    ));
                    return;
                }
            }
        }
        log::debug!("[RECONCILE] ok");
    }

    // ---------------------------------------------------------------- tick

    async fn tick(&mut self) {
        let now = now_secs();
        if self.state.halted && self.sentinels.take_risk_ack() {
            log::warn!(
                "[RISK_ACK] halt cleared (was: {:?})",
                self.state.halt_reason
            );
            self.state.halted = false;
            self.state.halt_reason = None;
            self.persist();
        }
        let kill = self.sentinels.kill_switch_engaged();
        let intent = resolve_operator_intent(
            self.state.mode,
            self.state.halted,
            kill,
            self.cfg.arm_path.exists(),
            self.cfg.disarm_path.exists(),
        );

        match intent {
            OperatorIntent::DisarmNow => {
                let _ = std::fs::remove_file(&self.cfg.disarm_path);
                if self.cfg.arm_path.exists() {
                    let _ = std::fs::remove_file(&self.cfg.arm_path);
                    log::warn!("[DISARM] a pending ARM was also present; discarded");
                }
                log::warn!("[DISARM] operator requested exit — closing all legs");
                self.exit_all("operator DISARM").await;
            }
            OperatorIntent::DisarmDeferredByHalt => {
                log::error!(
                    "[DISARM] requested but the bot is HALTED ({:?}) — state may be inconsistent, \
                     so no exit order is sent from here. Clear the halt with RISK_ACK (or close the \
                     positions manually); the DISARM file is left in place and will run right after.",
                    self.state.halt_reason
                );
            }
            OperatorIntent::DisarmNothingToDo { cancels_arm } => {
                let _ = std::fs::remove_file(&self.cfg.disarm_path);
                if cancels_arm {
                    let _ = std::fs::remove_file(&self.cfg.arm_path);
                    log::warn!(
                        "[DISARM] nothing held (mode={:?}); pending ARM cancelled",
                        self.state.mode
                    );
                } else {
                    log::info!(
                        "[DISARM] nothing held (mode={:?}); ignored",
                        self.state.mode
                    );
                }
            }
            OperatorIntent::ArmNow => {
                let _ = std::fs::remove_file(&self.cfg.arm_path);
                if let Err(e) = self.arm().await {
                    log::error!("[ARM] failed: {e:?}");
                    self.halt(format!("arm failed: {e}"));
                }
            }
            OperatorIntent::ArmBlockedByKill => {
                log::warn!("[ARM] ignored: KILL_SWITCH engaged (file left in place)");
            }
            OperatorIntent::ArmIgnoredAlreadyOn => {
                let _ = std::fs::remove_file(&self.cfg.arm_path);
                log::warn!("[ARM] ignored: already On with a live book (never double-arm)");
            }
            OperatorIntent::Idle => {}
        }

        // ADD: schedule more tranches of the size fixed at ARM.
        if self.cfg.add_path.exists() {
            if self.state.halted {
                log::error!(
                    "[ADD] requested but the bot is HALTED; file left in place until RISK_ACK"
                );
            } else if self.state.mode != Mode::On {
                let _ = std::fs::remove_file(&self.cfg.add_path);
                log::warn!(
                    "[ADD] ignored: nothing held (mode={:?}); use ARM to open a book",
                    self.state.mode
                );
            } else if kill {
                let _ = std::fs::remove_file(&self.cfg.add_path);
                log::warn!("[ADD] ignored: KILL_SWITCH engaged");
            } else {
                let contents = std::fs::read_to_string(&self.cfg.add_path).unwrap_or_default();
                let _ = std::fs::remove_file(&self.cfg.add_path);
                match parse_add_count(&contents) {
                    Some(k) => {
                        self.state.tranches_remaining += k;
                        self.persist();
                        log::warn!(
                            "[ADD] +{k} tranche(s) scheduled (spot ${:.0} + perp ${:.0} per symbol each); {} remaining",
                            self.state.tranche_spot_usd, self.state.tranche_perp_usd, self.state.tranches_remaining
                        );
                    }
                    None => log::error!(
                        "[ADD] ignored: file content {contents:?} is not a count in 1..=30"
                    ),
                }
            }
        }

        if self.state.halted {
            log::error!("[HALT] active: {:?}", self.state.halt_reason);
        } else if self.state.mode == Mode::On && intent != OperatorIntent::DisarmNow {
            self.daily_eval().await;
            // Next entry tranche, only if still On after the daily exit check
            // and the operator has not blocked entries.
            if self.state.mode == Mode::On
                && !kill
                && tranche_due(
                    now,
                    self.state.tranches_remaining,
                    self.state.last_tranche_date.as_deref(),
                    self.cfg.daily_eval_after_utc_secs,
                )
            {
                if let Err(e) = self.buy_tranche("scheduled").await {
                    log::error!("[ENTRY] tranche failed: {e:?}");
                    self.halt(format!("tranche failed: {e}"));
                }
            }
            if self.state.mode == Mode::On
                && now.saturating_sub(self.last_reconcile) >= self.cfg.reconcile_every_secs
            {
                self.last_reconcile = now;
                self.reconcile().await;
            }
        }
        self.write_status_if_due(now, kill);
    }

    fn write_status_if_due(&mut self, now: u64, kill: bool) {
        if now.saturating_sub(self.last_status_write) < 30 {
            return;
        }
        self.last_status_write = now;
        let legs: serde_json::Map<String, serde_json::Value> = self
            .state
            .legs
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::to_value(v).unwrap_or_default()))
            .collect();
        let status = serde_json::json!({
            "ts": now,
            "bot": BOT,
            "instance_id": self.cfg.instance_id,
            "dry_run": self.cfg.dry_run,
            "mode": self.state.mode,
            "armed_at": self.state.armed_at,
            "exited_at": self.state.exited_at,
            "exit_reason": self.state.exit_reason,
            "halted": self.state.halted,
            "halt_reason": self.state.halt_reason,
            "kill_switch": kill,
            "realized_pnl_total_usd": self.state.realized_pnl_total_usd,
            "cycles": self.state.cycles,
            "tranches_done": self.state.tranches_done,
            "tranches_remaining": self.state.tranches_remaining,
            "tranche_spot_usd": self.state.tranche_spot_usd,
            "tranche_perp_usd": self.state.tranche_perp_usd,
            "last_tranche_date": self.state.last_tranche_date,
            "config_fp": self.cfg.fingerprint(),
            "legs": legs,
        });
        if let Err(e) = persist_json(&self.cfg.status_path, &status) {
            log::warn!("[STATUS] write failed: {e:?}");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = Config::from_env()?;
    log::info!(
        "[CONFIG] bot={BOT} instance={} dry_run={} symbols={} equity=${:.0} spot_frac={} perp_frac={} tranches={} exit_dd={}% stop_dd={}% hl_slip={}bps fp={}",
        cfg.instance_id, cfg.dry_run, cfg.symbols.join(","), cfg.equity_usd, cfg.spot_fraction, cfg.perp_fraction,
        cfg.entry_tranches, cfg.exit_dd_pct, cfg.stop_dd_pct, cfg.hl_taker_slippage_bps, cfg.fingerprint()
    );
    refuse_live(
        cfg.dry_run,
        BOT,
        "prototype not yet cleared by the bot-strategy#895 rollout gates (see KNOWN GAPS in this file's module doc)",
    )?;

    let hl_markets: Vec<String> = cfg.hl_spot_market.values().cloned().collect();
    let hl = DexConnectorBox::create(
        "hyperliquid-account",
        cfg.dry_run,
        &hl_markets,
        Some(cfg.instance_id.as_str()),
    )
    .await
    .context("init Hyperliquid account connector")?;
    let lt = DexConnectorBox::create(
        "lighter",
        cfg.dry_run,
        &cfg.symbols,
        Some(cfg.instance_id.as_str()),
    )
    .await
    .context("init Lighter connector")?;
    hl.start().await.context("start Hyperliquid connector")?;
    lt.start().await.context("start Lighter connector")?;

    let state: State = load_json(&cfg.state_path)?.unwrap_or_default();
    log::info!(
        "[STARTUP] mode={:?} legs={} halted={} realized_total=${:.2}",
        state.mode,
        state.legs.len(),
        state.halted,
        state.realized_pnl_total_usd
    );

    let mut engine = Engine {
        sentinels: Sentinels::new(cfg.kill_switch_path.clone(), cfg.risk_ack_path.clone()),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("http client")?,
        hl: Arc::new(hl),
        lt: Arc::new(lt),
        state,
        last_status_write: 0,
        last_reconcile: 0,
        cfg,
    };
    // A restart while On must re-verify the book before doing anything else.
    if engine.state.mode == Mode::On {
        engine.reconcile().await;
    }
    let tick = Duration::from_secs(engine.cfg.tick_secs.max(5));
    loop {
        engine.tick().await;
        tokio::time::sleep(tick).await;
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_rule_is_strictly_below_level() {
        assert!(!should_exit(70.0, 100.0, 30.0));
        assert!(should_exit(69.99, 100.0, 30.0));
        assert!(!should_exit(95.0, 100.0, 30.0));
        assert!((level_below_peak(82_210.07, 30.0) - 57_547.049).abs() < 1e-6);
    }

    #[test]
    fn notionals_split_equally_across_symbols() {
        let (s, p) = leg_notionals(10_000.0, 0.9, 0.45, 2);
        assert!((s - 4_500.0).abs() < 1e-9);
        assert!((p - 2_250.0).abs() < 1e-9);
        let (s1, _) = leg_notionals(10_000.0, 0.9, 0.45, 0);
        assert!((s1 - 9_000.0).abs() < 1e-9);
    }

    #[test]
    fn size_rounds_down_to_venue_decimals() {
        assert_eq!(
            size_from_notional(4_500.0, 77_000.0, 5).to_string(),
            "0.05844"
        );
        assert_eq!(
            size_from_notional(2_250.0, 2_400.0, 4).to_string(),
            "0.9375"
        );
        assert_eq!(size_from_notional(100.0, 0.0, 4), Decimal::ZERO);
    }

    #[test]
    fn tolerance_check() {
        assert!(within_tolerance(1.0, 1.01, 2.0));
        assert!(!within_tolerance(1.0, 1.03, 2.0));
        assert!(within_tolerance(0.0, 0.0, 2.0));
        assert!(!within_tolerance(0.0, 0.5, 2.0));
    }

    #[test]
    fn daily_eval_due_respects_publication_delay_and_dedup() {
        // 2026-09-03 00:02:00 UTC — too early
        let t = Utc
            .with_ymd_and_hms(2026, 9, 3, 0, 2, 0)
            .unwrap()
            .timestamp() as u64;
        assert!(!daily_eval_due(t, None, 300));
        // 00:06 — due, yesterday not yet evaluated
        let t = Utc
            .with_ymd_and_hms(2026, 9, 3, 0, 6, 0)
            .unwrap()
            .timestamp() as u64;
        assert!(daily_eval_due(t, None, 300));
        assert!(daily_eval_due(t, Some("2026-09-01"), 300));
        assert!(!daily_eval_due(t, Some("2026-09-02"), 300));
    }

    #[test]
    fn candle_parsing_and_completed_selection() {
        let v: serde_json::Value = serde_json::from_str(
            r#"[{"t":1756771200000,"T":1756857599999,"c":"77340.01","o":"1","h":"1","l":"1"},
                {"t":1756857600000,"T":1756943999999,"c":"77877.99","o":"1","h":"1","l":"1"}]"#,
        )
        .unwrap();
        let candles = parse_hl_candles(&v).unwrap();
        assert_eq!(candles.len(), 2);
        // now inside the second candle → only the first is completed
        let now_ms = 1756900000000;
        let c = last_completed_candle(&candles, now_ms).unwrap();
        assert_eq!(c.t_ms, 1756771200000);
        assert!((c.close - 77340.01).abs() < 1e-9);
        // after the second closes → the second
        let c = last_completed_candle(&candles, 1756944000000).unwrap();
        assert_eq!(c.t_ms, 1756857600000);
        assert_eq!(utc_date(c.t_ms / 1000), "2025-09-03");
    }

    #[test]
    fn candle_parsing_rejects_bad_rows() {
        let v: serde_json::Value = serde_json::from_str(r#"[{"t":1,"T":2,"c":"0"}]"#).unwrap();
        assert!(parse_hl_candles(&v).is_err());
        let v: serde_json::Value = serde_json::from_str(r#"{"x":1}"#).unwrap();
        assert!(parse_hl_candles(&v).is_err());
    }

    #[test]
    fn state_roundtrip_and_defaults() {
        let mut s = State::default();
        assert_eq!(s.mode, Mode::Off);
        s.mode = Mode::On;
        s.legs.insert(
            "BTC".into(),
            LegState {
                peak_close: 80_000.0,
                exit_level: 56_000.0,
                ..Default::default()
            },
        );
        let json = serde_json::to_string(&s).unwrap();
        let back: State = serde_json::from_str(&json).unwrap();
        assert_eq!(back.mode, Mode::On);
        assert!((back.legs["BTC"].peak_close - 80_000.0).abs() < 1e-9);
        // Unknown/older files: missing fields default
        let back: State = serde_json::from_str(r#"{"mode":"Exited"}"#).unwrap();
        assert_eq!(back.mode, Mode::Exited);
        assert!(back.legs.is_empty());
    }

    #[test]
    fn tranche_due_one_per_utc_day_after_slot() {
        let t = Utc
            .with_ymd_and_hms(2026, 9, 5, 0, 2, 0)
            .unwrap()
            .timestamp() as u64;
        assert!(!tranche_due(t, 3, None, 300)); // too early in the day
        let t = Utc
            .with_ymd_and_hms(2026, 9, 5, 0, 6, 0)
            .unwrap()
            .timestamp() as u64;
        assert!(tranche_due(t, 3, None, 300));
        assert!(tranche_due(t, 3, Some("2026-09-04"), 300)); // yesterday's ran
        assert!(!tranche_due(t, 3, Some("2026-09-05"), 300)); // already ran today
        assert!(!tranche_due(t, 0, Some("2026-09-04"), 300)); // nothing left
                                                              // A gap of several days just runs the next one (no clumping is by
                                                              // construction: one call per day).
        assert!(tranche_due(t, 2, Some("2026-08-30"), 300));
    }

    #[test]
    fn stop_replaced_when_size_grows_even_if_level_unchanged() {
        // Same level, same size, order resting -> current.
        assert!(stop_is_current(Some(70.0), Some(1.0), true, 70.0, 1.0));
        // Same level but perp_size grew by a tranche -> must re-place.
        assert!(!stop_is_current(Some(70.0), Some(1.0), true, 70.0, 1.5));
        // Level moved -> re-place.
        assert!(!stop_is_current(Some(70.0), Some(1.0), true, 75.0, 1.0));
        // No resting order -> place.
        assert!(!stop_is_current(Some(70.0), Some(1.0), false, 70.0, 1.0));
        // Legacy state without stop_size -> re-place once.
        assert!(!stop_is_current(Some(70.0), None, true, 70.0, 1.0));
    }

    #[test]
    fn tranche_retry_skips_filled_legs() {
        let fresh = LegProgress::default();
        assert_eq!(legs_pending(&fresh, true), (true, true));
        assert_eq!(legs_pending(&fresh, false), (true, false));
        let spot_only = LegProgress {
            spot_done: true,
            perp_done: false,
        };
        assert_eq!(legs_pending(&spot_only, true), (false, true));
        let both = LegProgress {
            spot_done: true,
            perp_done: true,
        };
        assert_eq!(legs_pending(&both, true), (false, false));
        // A resumed tranche keeps the counters untouched until it completes:
        // tranche_due must still fire the same day for the retry.
        let t = Utc
            .with_ymd_and_hms(2026, 9, 5, 0, 6, 0)
            .unwrap()
            .timestamp() as u64;
        assert!(tranche_due(t, 3, Some("2026-09-04"), 300));
    }

    #[test]
    fn add_count_parsing() {
        assert_eq!(parse_add_count(""), Some(1));
        assert_eq!(parse_add_count("  \n"), Some(1));
        assert_eq!(parse_add_count("3"), Some(3));
        assert_eq!(parse_add_count(" 7 \n"), Some(7));
        assert_eq!(parse_add_count("0"), None);
        assert_eq!(parse_add_count("31"), None);
        assert_eq!(parse_add_count("lots"), None);
        assert_eq!(parse_add_count("-2"), None);
    }

    #[test]
    fn config_validation_bounds_entry_tranches() {
        let mut cfg = test_config();
        cfg.entry_tranches = 0;
        assert!(cfg.validate().is_err());
        cfg.entry_tranches = 31;
        assert!(cfg.validate().is_err());
        cfg.entry_tranches = 1;
        assert!(cfg.validate().is_ok());
        cfg.entry_tranches = 30;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn operator_intent_precedence() {
        use OperatorIntent::*;
        // DISARM while holding: exit, regardless of kill switch.
        assert_eq!(
            resolve_operator_intent(Mode::On, false, false, false, true),
            DisarmNow
        );
        assert_eq!(
            resolve_operator_intent(Mode::On, false, true, false, true),
            DisarmNow
        );
        // DISARM beats a simultaneous ARM.
        assert_eq!(
            resolve_operator_intent(Mode::On, false, false, true, true),
            DisarmNow
        );
        assert_eq!(
            resolve_operator_intent(Mode::Off, false, false, true, true),
            DisarmNothingToDo { cancels_arm: true }
        );
        assert_eq!(
            resolve_operator_intent(Mode::Exited, false, false, false, true),
            DisarmNothingToDo { cancels_arm: false }
        );
        // Halt defers DISARM (file kept) and suppresses ARM entirely.
        assert_eq!(
            resolve_operator_intent(Mode::On, true, false, false, true),
            DisarmDeferredByHalt
        );
        assert_eq!(
            resolve_operator_intent(Mode::Off, true, false, true, false),
            Idle
        );
        // ARM paths.
        assert_eq!(
            resolve_operator_intent(Mode::Off, false, false, true, false),
            ArmNow
        );
        assert_eq!(
            resolve_operator_intent(Mode::Exited, false, false, true, false),
            ArmNow
        );
        assert_eq!(
            resolve_operator_intent(Mode::Off, false, true, true, false),
            ArmBlockedByKill
        );
        assert_eq!(
            resolve_operator_intent(Mode::On, false, false, true, false),
            ArmIgnoredAlreadyOn
        );
        // Nothing requested.
        assert_eq!(
            resolve_operator_intent(Mode::On, false, false, false, false),
            Idle
        );
        assert_eq!(
            resolve_operator_intent(Mode::Off, false, true, false, false),
            Idle
        );
    }

    #[test]
    fn config_validation_rejects_stop_inside_exit() {
        let mut cfg = test_config();
        cfg.stop_dd_pct = 25.0;
        assert!(cfg.validate().is_err());
        cfg.stop_dd_pct = 35.0;
        assert!(cfg.validate().is_ok());
        cfg.exit_dd_pct = 0.0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_validation_rejects_daily_eval_delay_past_midnight() {
        // Regression: num_seconds_from_midnight() only ever returns
        // 0..86_400, so >= 86_400 here would make daily_eval_due() always
        // false and silently disable the only exit rule forever.
        let mut cfg = test_config();
        cfg.daily_eval_after_utc_secs = 86_400;
        assert!(cfg.validate().is_err());
        cfg.daily_eval_after_utc_secs = 86_399;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn env_bool_trims_whitespace() {
        std::env::set_var("BULL_HOLDER_TEST_ENV_BOOL", " true \n");
        assert!(env_bool("BULL_HOLDER_TEST_ENV_BOOL", false));
        std::env::remove_var("BULL_HOLDER_TEST_ENV_BOOL");
    }

    #[test]
    fn fingerprint_changes_with_exit_level() {
        let a = test_config();
        let mut b = test_config();
        b.exit_dd_pct = 25.0;
        assert_ne!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.fingerprint(), test_config().fingerprint());
    }

    fn test_config() -> Config {
        let dir = std::env::temp_dir();
        Config {
            instance_id: "t".into(),
            dry_run: true,
            symbols: vec!["BTC".into(), "ETH".into()],
            hl_spot_market: [
                ("BTC".to_string(), "UBTC/USDC".to_string()),
                ("ETH".to_string(), "UETH/USDC".to_string()),
            ]
            .into_iter()
            .collect(),
            equity_usd: 10_000.0,
            spot_fraction: 0.9,
            perp_fraction: 0.45,
            entry_tranches: 5,
            exit_dd_pct: 30.0,
            stop_dd_pct: 35.0,
            tick_secs: 60,
            daily_eval_after_utc_secs: 300,
            max_close_fetch_failures: 3,
            hl_taker_slippage_bps: 30,
            reconcile_tolerance_pct: 2.0,
            reconcile_every_secs: 600,
            hl_info_url: "http://127.0.0.1:1/info".into(),
            arm_path: dir.join("ARM"),
            add_path: dir.join("ADD"),
            disarm_path: dir.join("DISARM"),
            kill_switch_path: dir.join("KILL"),
            risk_ack_path: dir.join("ACK"),
            state_path: dir.join("state.json"),
            status_path: dir.join("status.json"),
            pnl_log_path: dir.join("pnl.jsonl"),
        }
    }
}
