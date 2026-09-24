//! Bull-mode holder (bot-strategy#893 / #894 / #895).
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
//! Lighter collateral guard (bot-strategy#909): the HL spot base never backs
//! the Lighter perp margin, so an under-funded Lighter sub-account is
//! liquidated by a drawdown smaller than the exit/stop levels (the #893
//! example — $1,000 behind $4,500 of perp — went at ~21%). Two checks, both
//! against Lighter's own account equity (collateral + unrealised PnL):
//! - **Pre-order**: ARM and every scheduled tranche require equity ≥
//!   `BULL_HOLDER_PERP_MARGIN_MIN_PCT` of the total perp notional AFTER the
//!   order (default 47.55% = ride to the 35% stop + worst 30-day funding +
//!   execution buffer + liquidation fee; see `perp_margin_min_pct`). A short
//!   ARM fails (halt, deposit, RISK_ACK, re-ARM);
//!   a short scheduled tranche is deferred to the next UTC day.
//! - **Runtime**: every reconcile the symmetric-drawdown liquidation point is
//!   compared with the resting stops; if funding erosion or a drawdown puts
//!   liquidation inside the stop (+1% clearance) the bot logs `[MARGIN]
//!   BREACH` with the top-up amount and reports it in status.json. It does
//!   NOT halt (the daily exit rule and the exchange stop keep running) and
//!   never de-risks on its own — the remedy is an operator deposit.
//!
//! DRY_RUN evaluates both checks but never blocks on them (the DRY_RUN
//! account holds no real collateral).
//!
//! Live flip (bot-strategy#895, 2026-09-21): `BULL_HOLDER_DRY_RUN=false` is
//! accepted after the DRY_RUN observation (14 d, daily evals matched the
//! replay) and the operator drills (KILL_SWITCH, DISARM, injected HALT →
//! RISK_ACK → ARM) passed with the owner's explicit small-live approval.
//! Still unverified until the first live ARM (bot-strategy#950): whether
//! Lighter reports resting trigger orders on the open-orders channel, so
//! `stop_covers` trusts the tracked `stop_order_id` instead of cross-checking.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{FixedOffset, TimeZone, Timelike, Utc};
use debot::directional::{append_jsonl, config_fingerprint, load_json, persist_json, Sentinels};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, FundingPayment, OrderSide, TpSl, TriggerOrderStyle};
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
/// Runtime collateral guard: the liquidation point must sit at least this
/// many percentage points of drawdown beyond the furthest resting stop
/// (stop-fill slippage; bot-strategy#909 execution buffer).
const STOP_LIQ_CLEARANCE_PCT: f64 = 1.0;

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
/// `BULL_HOLDER_DRY_RUN`, parsed strictly: only an explicit `false`/`0`/`no`
/// goes live. `env_bool` reads every unrecognised value as `false`, which
/// here would mean a typo (`ture`, an empty export) silently builds
/// execution-capable connectors — the one switch that must never fail open.
fn parse_dry_run(raw: Option<&str>) -> Result<bool> {
    let Some(raw) = raw else {
        return Ok(true);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => bail!(
            "BULL_HOLDER_DRY_RUN={other:?} is not a recognised value (true/false only); refusing to guess"
        ),
    }
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
    /// Lighter maintenance-margin fraction (%) of the perp markets, used to
    /// locate the cross-margin liquidation point. 1.20% for BTC/ETH per
    /// `orderBookDetails` on 2026-09-05 (bot-strategy#909); re-check if
    /// Lighter changes its risk parameters.
    lighter_mmr_pct: f64,
    /// Minimum Lighter account equity as a percentage of the TOTAL perp
    /// notional (all symbols, after the order being considered). Default
    /// 47.55% is the sum of four terms (matches
    /// `bot-strategy scripts/strategy_probes/bull_holder_909_910/collateral_model.py`):
    /// 35.78 to ride the 35% stop out (35 loss + 1.2 × 0.65 maintenance on
    /// what is left) + 9.77 worst observed 30-day funding (ETH, Binance
    /// proxy) + 1.00 execution buffer (stop-fill slippage) + 1.00 Lighter
    /// `liquidation_fee` — bot-strategy#909 "30-day buffer" allocation.
    /// Recomputing it after a funding or MMR change must keep all four.
    /// Validated to exceed the bare liquidation floor for `stop_dd_pct`.
    perp_margin_min_pct: f64,
    /// Main loop cadence.
    tick_secs: u64,
    /// Daily evaluation runs once the UTC day is at least this old (seconds
    /// after 00:00 UTC), so the venue's daily candle is closed and published.
    daily_eval_after_utc_secs: u32,
    /// Consecutive daily-close fetch failures that trigger a halt.
    max_close_fetch_failures: u32,
    /// IOC slippage tolerance for the Hyperliquid spot legs (bps).
    hl_taker_slippage_bps: u32,
    /// Protective limit of the Lighter exchange stop, below its trigger
    /// (bps). Wide on purpose: the stop is the 35 % insurance under the 30 %
    /// daily rule, execution matters more than price.
    stop_slippage_bps: u32,
    /// Reconcile: |expected − actual| / expected above this (%) halts.
    reconcile_tolerance_pct: f64,
    reconcile_every_secs: u64,
    hl_info_url: String,
    /// Lighter's public account endpoint and the account index behind it.
    /// Read-only REST: the authority on whether an order rests, used where
    /// the connector's WebSocket-fed views cannot prove absence.
    lighter_account_url: String,
    /// Empty when the deployment leaves it to the connector's
    /// auto-discovery (index unset or `0`); resolved from the wallet
    /// address on first use.
    lighter_account_index: String,
    lighter_wallet_address: String,
    /// Hyperliquid account the spot leg trades. The API wallet that signs
    /// for it is approved on that account's MASTER and expires; the bot
    /// cannot renew it (re-approval needs the master's own signature), so
    /// it publishes the date and lets the operator act.
    hl_account_address: String,
    /// Address of the API wallet whose key this bot signs with. The only
    /// identity that proves the watched approval belongs to the signer:
    /// a rotated key leaves the old named agent approved, and watching
    /// its expiry would report authorisation the bot does not have.
    hl_agent_address: String,
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
            instance_id: instance_id.clone(),
            dry_run: parse_dry_run(std::env::var("BULL_HOLDER_DRY_RUN").ok().as_deref())?,
            symbols,
            hl_spot_market,
            equity_usd: env_f64("BULL_HOLDER_EQUITY_USD", 1_000.0),
            spot_fraction: env_f64("BULL_HOLDER_SPOT_FRACTION", 0.90),
            perp_fraction: env_f64("BULL_HOLDER_PERP_FRACTION", 0.45),
            entry_tranches: env_u32("BULL_HOLDER_ENTRY_TRANCHES", 1),
            exit_dd_pct: env_f64("BULL_HOLDER_EXIT_DD_PCT", 30.0),
            stop_dd_pct: env_f64("BULL_HOLDER_STOP_DD_PCT", 35.0),
            lighter_mmr_pct: env_f64("BULL_HOLDER_LIGHTER_MMR_PCT", 1.2),
            perp_margin_min_pct: env_f64("BULL_HOLDER_PERP_MARGIN_MIN_PCT", 47.55),
            tick_secs: env_u64("BULL_HOLDER_TICK_SECS", 60),
            daily_eval_after_utc_secs: env_u32("BULL_HOLDER_DAILY_EVAL_AFTER_UTC_SECS", 300),
            max_close_fetch_failures: env_u32("BULL_HOLDER_MAX_CLOSE_FETCH_FAILURES", 3),
            hl_taker_slippage_bps: env_u32("BULL_HOLDER_HL_TAKER_SLIPPAGE_BPS", 30),
            stop_slippage_bps: env_u32("BULL_HOLDER_STOP_SLIPPAGE_BPS", 500),
            reconcile_tolerance_pct: env_f64("BULL_HOLDER_RECONCILE_TOLERANCE_PCT", 2.0),
            reconcile_every_secs: env_u64("BULL_HOLDER_RECONCILE_EVERY_SECS", 600),
            // Same resolution the connector uses (`lighter_env` in
            // config.rs): the instance-suffixed value wins, so this check
            // can never read a different account or network than the one
            // `DexConnectorBox` trades on.
            lighter_account_url: lighter_env("REST_ENDPOINT", &instance_id)
                .unwrap_or_else(|| "https://mainnet.zklighter.elliot.ai".to_string()),
            lighter_account_index: lighter_env("LIGHTER_ACCOUNT_INDEX", &instance_id)
                .filter(|v| v.trim() != "0")
                .unwrap_or_default(),
            lighter_wallet_address: lighter_env("LIGHTER_WALLET_ADDRESS", &instance_id)
                .unwrap_or_default(),
            hl_account_address: lighter_env("HYPERLIQUID_ACCOUNT_ADDRESS", &instance_id)
                .unwrap_or_default(),
            // Instance-suffixed like every other venue setting: two
            // bull-holders sharing an environment sign with different
            // wallets, and an unsuffixed value would have both watch one
            // of them (Codex review).
            hl_agent_address: lighter_env("BULL_HOLDER_HL_AGENT_ADDRESS", &instance_id)
                .unwrap_or_default(),
            // Follows the connector's own network selector, so a testnet
            // deployment does not silently read mainnet candles and
            // mainnet agent approvals (`HYPERLIQUID_IS_MAINNET=false` is
            // supported by `get_hyperliquid_account_config_from_env`).
            hl_info_url: env_string(
                "BULL_HOLDER_HL_INFO_URL",
                if lighter_env("HYPERLIQUID_IS_MAINNET", &instance_id).is_some_and(|v| {
                    matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no")
                }) {
                    "https://api.hyperliquid-testnet.xyz/info"
                } else {
                    "https://api.hyperliquid.xyz/info"
                },
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
        // Collateral settings only bind a book that actually opens perp legs.
        // A spot-only book (`PERP_FRACTION=0`) never places a Lighter order
        // and never runs the collateral check, so coupling these to
        // `stop_dd_pct` there would let an irrelevant stop value refuse
        // startup (e.g. STOP_DD_PCT=50 makes the floor 50.6 > the 47.55
        // default).
        if self.perp_fraction > 0.0 {
            if !(0.0 < self.lighter_mmr_pct && self.lighter_mmr_pct < 10.0) {
                bail!("BULL_HOLDER_LIGHTER_MMR_PCT must be in (0,10) (Lighter BTC/ETH maintenance margin is 1.2%)");
            }
            let floor = liquidation_floor_pct(self.stop_dd_pct, self.lighter_mmr_pct);
            if !(self.perp_margin_min_pct > floor && self.perp_margin_min_pct <= 100.0) {
                bail!(
                    "BULL_HOLDER_PERP_MARGIN_MIN_PCT={} must be > {floor:.2} and <= 100: below {floor:.2}% collateral a {}% drawdown liquidates the Lighter account before the exchange stop can fire (bot-strategy#909)",
                    self.perp_margin_min_pct,
                    self.stop_dd_pct
                );
            }
        }
        if !(0.0 < self.spot_fraction && self.spot_fraction <= 1.0) {
            bail!("BULL_HOLDER_SPOT_FRACTION must be in (0,1]");
        }
        if !(0.0 <= self.perp_fraction && self.perp_fraction <= 1.0) {
            bail!("BULL_HOLDER_PERP_FRACTION must be in [0,1]");
        }
        if self.stop_slippage_bps == 0 || self.stop_slippage_bps > 2_000 {
            bail!(
                "BULL_HOLDER_STOP_SLIPPAGE_BPS={} out of range (1..=2000)",
                self.stop_slippage_bps
            );
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
            ("lighter_mmr_pct", format!("{:.2}", self.lighter_mmr_pct)),
            (
                "perp_margin_min_pct",
                format!("{:.2}", self.perp_margin_min_pct),
            ),
            (
                "hl_taker_slippage_bps",
                self.hl_taker_slippage_bps.to_string(),
            ),
            ("stop_slippage_bps", self.stop_slippage_bps.to_string()),
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

/// Base token of a Hyperliquid spot market string (`UBTC/USDC` → `UBTC`).
fn spot_base(market: &str) -> String {
    market.split('/').next().unwrap_or("").to_ascii_uppercase()
}

/// How long a live fill is given to show up in the venue's holding before
/// the observed (possibly partial) change is taken as final.
const FILL_CONFIRM_ATTEMPTS: u32 = 8;
const FILL_CONFIRM_STEP_MS: u64 = 1_500;

/// The observed change covers the request (f64 round-trips and a
/// base-denominated fee are absorbed by the tolerance).
fn fill_complete(observed: f64, requested: f64) -> bool {
    requested <= 0.0 || observed >= requested * (1.0 - FILL_COMPLETE_TOL)
}
const FILL_COMPLETE_TOL: f64 = 0.005;

/// A partially filled leg whose remainder is below the venue minimum is
/// marked done (it would otherwise block the ladder forever). Returns true
/// when it did so.
fn remainder_is_dust(
    filled_usd: f64,
    remainder: Decimal,
    min_order: f64,
    progress: &mut BTreeMap<String, LegProgress>,
    sym: &str,
    spot: bool,
) -> bool {
    if filled_usd <= 0.0 || remainder.to_f64().unwrap_or(0.0) >= min_order {
        return false;
    }
    let pr = progress.entry(sym.to_string()).or_default();
    if spot {
        pr.spot_done = true;
    } else {
        pr.perp_done = true;
    }
    true
}

/// Resting orders for `symbol` in a Lighter `/api/v1/account` response:
/// the market's own resting / position-tied / pending orders plus the
/// account-wide pending count (the account is dedicated to this bot).
/// `None` when the response does not carry the market — never 0, which
/// would read as "nothing rests".
/// Instance-suffixed env lookup, mirroring `config::lighter_env` so the
/// venue reads here resolve to the same account as the connector's.
fn lighter_env(name: &str, instance_id: &str) -> Option<String> {
    let suffix = instance_id.to_uppercase().replace('-', "_");
    std::env::var(format!("{name}_{suffix}"))
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| std::env::var(name).ok().filter(|v| !v.is_empty()))
}

/// `initial_margin_fraction` (percent) for `symbol` in a Lighter
/// `/api/v1/account` response — the margin behind a position, which bounds
/// how far a stop may sit from the mark.
fn margin_fraction_for(v: &serde_json::Value, symbol: &str) -> Option<f64> {
    let account = v.get("accounts")?.as_array()?.first()?;
    account
        .get("positions")?
        .as_array()?
        .iter()
        .find(|p| p.get("symbol").and_then(|x| x.as_str()) == Some(symbol))?
        .get("initial_margin_fraction")?
        .as_str()?
        .parse()
        .ok()
}

fn resting_orders_for(v: &serde_json::Value, symbol: &str) -> Option<u64> {
    let account = v.get("accounts")?.as_array()?.first()?;
    // Strict: a count that is absent or not a number makes the response
    // unreadable, never zero. Reading a malformed reply as "no orders"
    // would clear a stop that is still live.
    let n = |o: &serde_json::Value, k: &str| -> Option<u64> { o.get(k)?.as_u64() };
    for p in account.get("positions")?.as_array()? {
        if p.get("symbol").and_then(|x| x.as_str()) != Some(symbol) {
            continue;
        }
        // ONLY this market's own counts. Folding in the account-wide
        // pending count made one symbol's resting stop read as "an order
        // rests for the other symbol too", so with BTC covered the ETH
        // leg could never settle its ghost and never got a stop (live,
        // 2026-09-23).
        return Some(
            n(p, "open_order_count")?
                + n(p, "position_tied_order_count")?
                + n(p, "pending_order_count")?,
        );
    }
    // The market row can be omitted when it carries neither a position nor
    // an order. Only then do the account-wide counts answer the question:
    // zero orders anywhere means zero for this market. They must be
    // present too.
    let account_total = n(account, "total_order_count")?
        + n(account, "total_isolated_order_count")?
        + n(account, "pending_order_count")?;
    Some(account_total)
}

/// How long the connector's order list may keep omitting the tracked
/// stop, while the venue reports a resting order for the market, before
/// it is treated as gone. The list is WebSocket-fed and briefly empty
/// after a reconnect; measuring elapsed time rather than attempts keeps
/// the answer independent of how often anything retries.
const STOP_UNLISTED_GRACE_SECS: u64 = 30 * 60;

/// An API-wallet approval closer than this is reported as an error: the
/// operator needs lead time to approve a new one on the master wallet.
const AGENT_EXPIRY_WARN_DAYS: f64 = 30.0;
const AGENT_POLL_EVERY_SECS: u64 = 86_400;

/// This bot's approved agent, as (name, valid_until_secs). Matched by
/// name when one is configured; otherwise only an unambiguous single
/// agent counts — guessing among several could watch the wrong wallet's
/// expiry and report safety that is not there. Hyperliquid reports
/// `validUntil` in milliseconds.
#[derive(Debug, PartialEq)]
enum AgentLookup {
    /// The wallet, with its approval's expiry in seconds.
    Found(String, i64),
    /// Read successfully, and this wallet holds no approval — revoked,
    /// renamed, or never there. Conclusive.
    Absent,
    /// The record exists but could not be read (no numeric `validUntil`),
    /// or several agents and no name to pick one. Not evidence of
    /// anything: the last known expiry stands.
    Unreadable,
}

/// Whose approved-agent list covers `account`: its master when the venue
/// names one, the account itself when the venue calls it a master.
/// `None` for anything else — an unrecognised role must not be read as
/// "this account is its own master".
fn agent_owner(role: &serde_json::Value, account: &str) -> Option<String> {
    if let Some(master) = role.pointer("/data/master").and_then(|x| x.as_str()) {
        return Some(master.to_string());
    }
    match role.get("role").and_then(|x| x.as_str()) {
        Some("master") | Some("user") => Some(account.to_string()),
        _ => None,
    }
}

fn agent_expiry(agents: &serde_json::Value, configured_address: &str) -> AgentLookup {
    let read = |a: &serde_json::Value| -> Option<(String, i64)> {
        let until = a.get("validUntil")?.as_i64()? / 1_000;
        let name = a
            .get("name")
            .and_then(|x| x.as_str())
            .unwrap_or("unnamed")
            .to_string();
        Some((name, until))
    };
    let Some(agents) = agents.as_array() else {
        return AgentLookup::Unreadable;
    };
    // Address only. A name is a label the operator chose, not the signing
    // identity: rotating the key leaves the old wallet approved under the
    // same name, and publishing its expiry would report authorisation
    // this process does not have. Without the address there is nothing to
    // attribute an approval to, so there is no fallback that would.
    if configured_address.is_empty() {
        return AgentLookup::Unreadable;
    }
    let mut found = None;
    let mut unidentifiable = false;
    for a in agents {
        match a.get("address").and_then(|x| x.as_str()) {
            Some(addr) if addr.eq_ignore_ascii_case(configured_address) => {
                found = Some(a);
                break;
            }
            Some(_) => {}
            // A record whose address cannot be read might be the wallet
            // being looked for, so the rest not matching is not proof of
            // absence — and absence is what clears a live expiry.
            None => unidentifiable = true,
        }
    }
    if found.is_none() && unidentifiable {
        return AgentLookup::Unreadable;
    }
    match found {
        None => AgentLookup::Absent,
        Some(a) => match read(a) {
            Some((name, until)) => AgentLookup::Found(name, until),
            None => AgentLookup::Unreadable,
        },
    }
}

/// A holding the book does not carry counts as flat below this notional.
const RECONCILE_DUST_USD: f64 = 5.0;

/// How long a freshly placed stop is given to appear in the venue's own
/// order list before it is treated as never placed.
const STOP_CONFIRM_ATTEMPTS: u32 = 6;
const STOP_CONFIRM_STEP_MS: u64 = 2_000;

/// Startup verification of the book against the venues: retries for
/// transient read failures before halting.
const STARTUP_RECONCILE_ATTEMPTS: u32 = 6;
const STARTUP_RECONCILE_STEP_SECS: u64 = 10;

/// An unsettled order's holding must sit exactly on its recorded baseline
/// (f64 round-trips through state.json aside) to be declared "no fill".
const PENDING_RESOLVE_EPS: f64 = 1e-9;

/// Combine the order acknowledgement with the confirmed holding change into
/// the fill to record. The venue's holding is authoritative:
/// - ack Ok, change observed → that change (partial fills are recorded as
///   what they are; the caller logs the shortfall);
/// - ack Err, change observed → the order DID (at least partly) fill before
///   the error: record it, never retry the full size;
/// - ack Ok, no change → the venue never showed the fill: unknown, so an
///   error (the caller halts; a RISK_ACK retry re-reads the holding first);
/// - ack Err, no change → the error, no exposure taken;
/// - holding unreadable → error regardless of the ack.
/// Did the venue definitively NOT accept the order? Only an application-
/// level rejection or a failure before submission counts. A transport
/// timeout, a parse failure on the response, or a "reconcile required"
/// error may all follow a venue-side acceptance, and a fill can then show
/// up after the confirmation window — such an order stays pending.
fn order_rejected_definitively(e: &dex_connector::DexError) -> bool {
    use dex_connector::DexError::*;
    matches!(
        e,
        ServerResponse(_)
            | Permanent(_)
            | InvalidInput { .. }
            | UpcomingMaintenance
            | ApiKeyRegistrationRequired
            | RateLimited { .. }
            | NoConnection
    )
}

/// The verdict on a tracked stop the venue's order list does not name,
/// while some order still rests for the market. `Some((rests,
/// absence_confirmed))`, or `None` to defer.
///
/// `elapsed` is how long the id has been unlisted — or `None` when the
/// absence is ALREADY established, which must not be re-timed. The same
/// answer repeating is not a fresh doubt: restarting the window would
/// leave an externally cancelled stop uncovered for another whole grace
/// after every failed cancel retry, instead of retrying on the next
/// reconcile. Only the id turning up listed undoes it.
///
/// `cancelled` — the venue naming the id among its cancelled orders — is
/// the venue's own word, so it alone confirms the absence. Running out
/// the grace is an inference about a stale WebSocket cache and does not.
fn unlisted_verdict(cancelled: bool, elapsed: Option<u64>) -> Option<(bool, bool)> {
    match elapsed {
        // Established: act now.
        None => Some((false, cancelled)),
        Some(e) if cancelled || e >= STOP_UNLISTED_GRACE_SECS => Some((false, cancelled)),
        Some(_) => None,
    }
}

/// May a tracked stop id leave state? The ONE rule for every site that
/// drops one, because the cost of being wrong is the same everywhere: an
/// untracked reduce-only trigger that a later ARM inherits against a new
/// position. Only the venue's own word counts — the cancel just
/// succeeded, or the venue already said the order is not there
/// (`stop_absence_confirmed`: nothing rests for this market, or it named
/// the id among its cancelled orders).
///
/// A cancel that merely errored proves nothing: a transport blip looks
/// exactly like an absent order. Neither does the unlisted-for-the-whole-
/// grace inference, which is about a stale WebSocket cache — that is
/// enough to ATTEMPT a replacement (the attempt cancels first, in the
/// right order) but not to forget the id.
fn may_forget_stop(cancel_ok: bool, absence_confirmed: bool) -> bool {
    cancel_ok || absence_confirmed
}

/// Is the outcome of a live order settled? Only when the holding could be
/// read AND (it changed, or the order was definitively rejected and the
/// holding did not change).
fn order_outcome_known(acked: bool, rejected_definitively: bool, observed: Option<f64>) -> bool {
    match observed {
        None => false,
        Some(o) if o > 0.0 => true,
        Some(_) => !acked && rejected_definitively,
    }
}

fn settle_fill(
    what: &str,
    requested: Decimal,
    sent: Result<dex_connector::CreateOrderResponse>,
    observed: Result<f64>,
) -> Result<Decimal> {
    let observed = observed.with_context(|| {
        format!("{what}: exposure after the order is UNKNOWN (reconcile before any retry)")
    })?;
    let filled = Decimal::from_f64(observed).unwrap_or(Decimal::ZERO);
    match sent {
        Ok(_) if observed > 0.0 => {
            if !fill_complete(observed, requested.to_f64().unwrap_or(0.0)) {
                log::warn!("[FILL] {what}: PARTIAL — requested {requested}, confirmed {filled}");
            }
            Ok(filled)
        }
        Ok(_) => bail!(
            "{what}: order acknowledged but the venue holding did not change within the confirmation window; treating the fill as UNKNOWN (reconcile before any retry)"
        ),
        Err(e) if observed > 0.0 => {
            log::error!(
                "[FILL] {what}: order call failed but the venue shows {filled} filled — recording it, NOT retrying: {e:?}"
            );
            Ok(filled)
        }
        Err(e) => Err(e),
    }
}

/// What is left of a leg after a close that the venue confirmed as `closed`
/// of `orig`. Only residue worth less than `RECONCILE_DUST_USD` at `price`
/// is dust (fees, size rounding) and reads as 0; without a price nothing
/// is dust. A percentage would let a 98 % fill of a large leg "close" it
/// with a stop-less remainder left at the venue.
fn remaining_after_close(orig: f64, closed: f64, price: Option<f64>) -> f64 {
    let remaining = (orig - closed).max(0.0);
    if orig <= 0.0 || remaining <= 0.0 {
        return 0.0;
    }
    match price {
        Some(p) if p > 0.0 && remaining * p < RECONCILE_DUST_USD => 0.0,
        _ => remaining,
    }
}

/// Round a notional/price size DOWN to the venue's size decimals.
fn size_from_notional(notional_usd: f64, price: f64, size_decimals: u32) -> Decimal {
    if price <= 0.0 || notional_usd <= 0.0 {
        return Decimal::ZERO;
    }
    let raw = Decimal::from_f64(notional_usd / price).unwrap_or(Decimal::ZERO);
    raw.round_dp_with_strategy(size_decimals, RoundingStrategy::ToZero)
}

// ---- Lighter collateral guard (bot-strategy#909) --------------------------

/// Collateral (% of perp notional) at which a symmetric drawdown of
/// `stop_dd_pct` liquidates a Lighter cross-margin account: the loss on the
/// notional plus the maintenance requirement on what is left. Any minimum
/// below this lets liquidation reach the account before the exchange stop.
fn liquidation_floor_pct(stop_dd_pct: f64, mmr_pct: f64) -> f64 {
    stop_dd_pct + mmr_pct * (1.0 - stop_dd_pct / 100.0)
}

/// Symmetric drawdown (%) from the current marks at which `equity_usd` of
/// cross collateral stops covering `perp_notional_usd`: solves
/// `E − N·d = N·(1 − d)·m` for `d`. `None` with no perp exposure. Negative
/// means the account is already below maintenance.
fn liquidation_distance_pct(equity_usd: f64, perp_notional_usd: f64, mmr_pct: f64) -> Option<f64> {
    if perp_notional_usd <= 0.0 {
        return None;
    }
    let m = mmr_pct / 100.0;
    Some((equity_usd / perp_notional_usd - m) / (1.0 - m) * 100.0)
}

/// Inverse of `liquidation_distance_pct`: equity that puts the liquidation
/// point `target_distance_pct` below the marks.
fn equity_for_distance_usd(perp_notional_usd: f64, target_distance_pct: f64, mmr_pct: f64) -> f64 {
    let m = mmr_pct / 100.0;
    perp_notional_usd * (target_distance_pct / 100.0 * (1.0 - m) + m)
}

/// Lighter equity required to hold `perp_notional_usd` at `min_pct`.
fn required_collateral_usd(perp_notional_usd: f64, min_pct: f64) -> f64 {
    perp_notional_usd * min_pct / 100.0
}

/// Distance (%) from `mark` down to a resting stop trigger, never negative.
/// A mark already below a still-resting stop (trigger canceled, rejected,
/// delayed or unfilled) means the stop offers no protection on any further
/// decline: at best it fires here and now, so clamp to 0 rather than let a
/// negative distance make `margin_breached` accept a liquidation point that
/// no stop can pre-empt.
fn stop_distance_pct(mark: f64, stop_level: f64) -> f64 {
    if mark <= 0.0 {
        return 0.0;
    }
    ((mark - stop_level) / mark * 100.0).max(0.0)
}

/// Per-symbol perp size backing the collateral calculation: the LARGER of the
/// recorded book and the venue's actual position. A venue position bigger than
/// the book is exactly the case `reconcile` halts on, and valuing collateral
/// off the smaller recorded size there would understate the notional and hide
/// a real shortfall.
///
/// The second return lists exposure this guard cannot model and must fail
/// closed on: venue symbols that are not configured (no price here, yet they
/// consume the same cross collateral) and SHORT venue positions (this bot is
/// long-only; a short loses on the way up, where the long's sell stop offers
/// no protection at all). A short still contributes its magnitude to the
/// margin requirement, so it is counted in `sizes` as well as flagged.
fn merge_perp_sizes(
    configured: &[String],
    recorded: &BTreeMap<String, f64>,
    venue: &[(String, f64, i32)],
) -> (BTreeMap<String, f64>, Vec<String>) {
    let mut sizes: BTreeMap<String, f64> = configured
        .iter()
        .map(|s| (s.clone(), recorded.get(s).copied().unwrap_or(0.0).max(0.0)))
        .collect();
    let mut unsupported = Vec::new();
    for (sym, size, sign) in venue {
        let size = size.abs();
        if size <= 0.0 {
            continue;
        }
        let key = sym.to_ascii_uppercase();
        match sizes.get_mut(&key) {
            Some(v) => {
                *v = v.max(size);
                if *sign < 0 {
                    unsupported.push(format!("{key} (short)"));
                }
            }
            None => unsupported.push(format!("{key} (not configured)")),
        }
    }
    sizes.retain(|_, v| *v > 0.0);
    (sizes, unsupported)
}

/// Perp notional (USD) for the given sizes at the given marks.
fn perp_notional_usd(sizes: &BTreeMap<String, f64>, marks: &BTreeMap<String, f64>) -> f64 {
    sizes
        .iter()
        .map(|(sym, size)| size * marks.get(sym).copied().unwrap_or(0.0))
        .sum()
}

/// Does a leg's resting stop actually cover `size`? A tracked order is not
/// enough: a stop placed for less than the position now open (a failed
/// re-placement after a tranche, or a venue position larger than the book)
/// leaves the remainder with no exchange-side protection.
///
/// KNOWN LIMIT (bot-strategy#950): this trusts `stop_order_id` to still be
/// resting on Lighter. A stop canceled at the venue, or a cancel whose
/// response was lost, keeps the id in state and reads here as covered.
/// Cross-checking `get_open_orders` was deliberately NOT added yet: that
/// connector path returns WS-tracked orders only (no REST fallback), and
/// whether Lighter publishes resting TRIGGER orders on that channel is
/// unverified — pairtrade has never exercised Lighter trigger orders live
/// (see this file's KNOWN GAPS and bot-strategy#895). Wiring the check
/// against a channel that omits them would report every stop as missing,
/// which is worse than the gap it closes. Settle it with the #895 live
/// stop-order check, then add the cross-check.
fn stop_covers(has_order: bool, stop_size: Option<f64>, size: f64) -> bool {
    // 1e-9 absorbs f64 round-trips through state.json.
    has_order && stop_size.map(|ss| ss + 1e-9 >= size).unwrap_or(false)
}

/// Runtime guard: liquidation must stay at least `clearance_pct` of drawdown
/// beyond the furthest resting stop, or a drawdown liquidates the account
/// before that stop can fire.
fn margin_breached(
    liq_distance_pct: f64,
    worst_stop_distance_pct: f64,
    clearance_pct: f64,
) -> bool {
    liq_distance_pct < worst_stop_distance_pct + clearance_pct
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
    /// The leg's cost basis no longer describes what is held: an exit found
    /// the venue holding something other than the book and closed only part
    /// of it. PnL for this leg is unknown from here until it is flat.
    #[serde(default)]
    cost_basis_unknown: bool,
    /// Same, for the perp leg (the two holdings are independent).
    #[serde(default)]
    perp_cost_basis_unknown: bool,
    /// A stop the venue acknowledged but never confirmed as resting. It is
    /// NOT cover (it may not exist), and it must NOT be forgotten either
    /// (it may): `get_open_orders` is served from the Lighter WebSocket
    /// cache, so an empty read during a reconnect is not proof of absence.
    /// Cancelled by id — which does not depend on that cache — before the
    /// next stop is placed, and cleared only when that cancel succeeds.
    #[serde(default)]
    stop_unconfirmed_id: Option<String>,
    /// When the connector's order list first stopped naming the tracked
    /// stop while the venue still reported a resting order for the
    /// market. A cache catching up answers that way for moments; an order
    /// that is really gone answers that way forever, so the elapsed time
    /// tells them apart. Cleared whenever the order is seen, the list
    /// cannot be read (a reconnect), a new stop is placed, or the process
    /// restarts — all of which end the continuous observation this
    /// measures. Time, not a count: retry frequency then has no say in
    /// whether a live stop gets cancelled.
    #[serde(default)]
    stop_unlisted_since: Option<u64>,
    /// The tracked stop is believed gone on evidence (see `rests` in
    /// `ensure_stops`), but the cancel that proves it has not been sent
    /// yet. It stops counting as cover immediately; the id is kept so the
    /// replacement path cancels it in the right order — after its own
    /// quote and leverage checks — and a cancel that then fails does not
    /// block the replacement, because the order is not expected to be
    /// there in the first place.
    #[serde(default)]
    stop_presumed_gone: bool,
    /// ...and that absence came from the venue naming it, not from an
    /// inference: no order at all rests for this market, or the venue
    /// listed this id among its cancelled orders. The unlisted-for-the-
    /// whole-grace fallback does NOT set this — it is strong enough to
    /// attempt a replacement (which cancels first, in the right order)
    /// but not to throw the id away on a cancel that merely errored.
    #[serde(default)]
    stop_absence_confirmed: bool,
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
    /// Settled Lighter funding on the configured perp legs since ARM, USD,
    /// signed as the venue netted it (negative = paid). `None` until the
    /// account history has been read at least once after ARM: a carry cost
    /// that is unknown must not read as zero (bot-strategy#963).
    #[serde(default)]
    cum_funding_usdc: Option<f64>,
    /// Unix seconds of the last successful funding-history read.
    #[serde(default)]
    cum_funding_as_of: Option<u64>,
    /// Venue payment ids already counted, with their settlement time, so a
    /// re-read of an overlapping window never double-counts. Pruned to the
    /// re-read window (`FUNDING_REREAD_SECS`).
    #[serde(default)]
    funding_seen: BTreeMap<i64, i64>,
    /// Execution mode that created the current book (`Some(true)` = DRY_RUN
    /// simulated fills, `Some(false)` = live venue fills). Set when the book
    /// opens, checked at startup: a book must only ever be operated in the
    /// mode that created it — a live book loaded under DRY_RUN would be
    /// "closed" by simulated sells while the venue positions and the stop
    /// stay open; a simulated book loaded live would send real sells for
    /// holdings that do not exist. `None` = flat, or a pre-#895 file.
    #[serde(default)]
    book_dry_run: Option<bool>,
    /// See [`PendingOrder`].
    #[serde(default)]
    pending_order: Option<PendingOrder>,
    /// The Hyperliquid API wallet this bot signs with, and when its
    /// approval expires (unix seconds). Read from the venue, refreshed
    /// once a day; `None` until a read succeeds. The bot cannot renew it —
    /// re-approval is a master-wallet signature — so the value exists to
    /// be watched (bot-strategy#1054).
    #[serde(default)]
    hl_agent_name: Option<String>,
    #[serde(default)]
    hl_agent_valid_until: Option<i64>,
    #[serde(default)]
    hl_agent_as_of: Option<u64>,
    /// Which account+wallet the recorded expiry belongs to. A change here
    /// (the operator repointed the bot) makes the stored value describe
    /// something else, so the daily timer must not suppress the refresh.
    #[serde(default)]
    hl_agent_key: Option<String>,
}

/// Does the persisted book carry exposure that the process must not touch
/// in the wrong execution mode?
fn book_is_open(state: &State) -> bool {
    state.mode == Mode::On
        || state.pending_order.is_some()
        || state
            .legs
            .values()
            .any(|l| l.spot_size > 0.0 || l.perp_size > 0.0)
}

/// Startup gate for the persisted state vs the configured execution mode.
/// `Ok(())` = safe to operate; `Err` = refuse to start (never "fix" it here:
/// the operator must DISARM under the mode that created the book, or clear
/// the state file by hand after confirming the venues are flat).
fn check_book_mode(state: &State, dry_run: bool) -> Result<()> {
    if !book_is_open(state) {
        return Ok(());
    }
    match state.book_dry_run {
        Some(created_dry_run) if created_dry_run == dry_run => Ok(()),
        Some(created_dry_run) => bail!(
            "state.json holds an open book created with dry_run={created_dry_run} but this process runs dry_run={dry_run}; \
             DISARM it under the original mode (or clear the state file after confirming the venues are flat) before switching"
        ),
        None => bail!(
            "state.json holds an open book with no recorded execution mode (pre-#895 file); \
             DISARM it under the mode that created it before running this build"
        ),
    }
}

/// How far behind the last successful read the next funding-history read
/// starts. Lighter settles hourly; six hours absorbs a late-appearing
/// settlement without re-reading the whole holding period every time.
const FUNDING_REREAD_SECS: u64 = 6 * 3600;

/// How long after EXIT the funding history keeps being read. The perp leg
/// is closed at EXIT, so only settlements already accrued can still land;
/// a day covers the venue's hourly cycle with margin, after which polling
/// a flat book is just load on the account endpoint.
const FUNDING_POLL_AFTER_EXIT_SECS: u64 = 24 * 3600;

/// Whether the funding history is due for a read: only once ARMed (the
/// total is per holding period), on the reconcile cadence, and not
/// indefinitely after EXIT (`armed_at` is never cleared).
fn funding_poll_due(
    armed_at: Option<u64>,
    exited_at: Option<u64>,
    now: u64,
    last_poll: u64,
    every_secs: u64,
) -> bool {
    if armed_at.is_none() {
        return false;
    }
    if let Some(exited) = exited_at {
        if now.saturating_sub(exited) > FUNDING_POLL_AFTER_EXIT_SECS {
            return false;
        }
    }
    now.saturating_sub(last_poll) >= every_secs
}

/// Fold newly settled payments into the running total. Pure so the dedupe
/// and the symbol filter can be asserted without a venue: counts a payment
/// once (by venue id), only on configured symbols, only at or after `since`.
/// Returns the amount added.
fn apply_funding_payments(
    state: &mut State,
    payments: &[FundingPayment],
    symbols: &[String],
    since: u64,
) -> f64 {
    let mut added = 0.0;
    for p in payments {
        if p.timestamp_secs < since as i64 || !symbols.iter().any(|s| s == &p.symbol) {
            continue;
        }
        if state.funding_seen.contains_key(&p.payment_id) {
            continue;
        }
        let amount = p.amount_usdc.to_string().parse::<f64>().unwrap_or(0.0);
        state.funding_seen.insert(p.payment_id, p.timestamp_secs);
        added += amount;
    }
    let total = state.cum_funding_usdc.unwrap_or(0.0) + added;
    state.cum_funding_usdc = Some(total);
    state.funding_seen.retain(|_, ts| *ts >= since as i64);
    added
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq)]
struct LegProgress {
    spot_done: bool,
    perp_done: bool,
    /// Notional (USD at the fill quote) already filled for this tranche's
    /// legs. A partial fill halts the ladder with these recorded; the
    /// RISK_ACK retry orders only the remainder, never the full size again.
    #[serde(default)]
    spot_usd: f64,
    #[serde(default)]
    perp_usd: f64,
}

/// A live order whose outcome has not been settled against the venue yet.
/// Written (and persisted) BEFORE the order is sent, cleared once the fill
/// was confirmed or a no-fill was confirmed. Anything else — a crash, an
/// unreadable holding, an acknowledged order the venue never showed — leaves
/// it in place, and no further order is sent for the book until an
/// account-wide reconcile has compared every configured symbol with the
/// venues (`reconcile_before_orders`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct PendingOrder {
    venue: String,
    symbol: String,
    side: String,
    requested: f64,
    /// Venue holding read just before the order (the baseline the
    /// confirmation compares against).
    holding_before: f64,
    ts: u64,
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
    /// When `margin_monitor` last ran (it also runs while halted).
    last_margin_check: u64,
    /// Last runtime collateral-guard evaluation (status.json `margin`).
    last_margin: Option<MarginSnapshot>,
    /// When the Lighter funding history was last polled (bot-strategy#963).
    last_funding_poll: u64,
    /// Account index the venue reads use, resolved once from the wallet
    /// address when the config leaves it to auto-discovery.
    resolved_account_index: tokio::sync::Mutex<Option<String>>,
}

/// Result of one runtime collateral-guard evaluation (bot-strategy#909).
#[derive(Serialize, Debug, Clone)]
struct MarginSnapshot {
    ts: u64,
    /// Lighter account equity (collateral + unrealised PnL), USD.
    equity_usd: Option<f64>,
    /// Perp notional held, at Lighter marks, USD.
    perp_notional_usd: f64,
    /// equity / notional, %.
    margin_pct: Option<f64>,
    /// Symmetric drawdown that liquidates the account, %.
    liq_distance_pct: Option<f64>,
    /// Drawdown to the furthest resting stop, %.
    worst_stop_distance_pct: Option<f64>,
    ok: bool,
    detail: String,
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
    //
    // Live fills are CONFIRMED from the venue's own holding, never inferred
    // from the order acknowledgement: `CreateOrderResponse.ordered_size` is
    // what was requested, an IOC can fill partially, and a timeout can land
    // after the venue accepted the order. Each live order is bracketed by a
    // read of the holding it changes (HL spot base balance / Lighter perp
    // position) and the recorded fill is the observed change. An order
    // whose effect cannot be read is reported as an error so the caller
    // halts — it must never be retried on the assumption that nothing filled.

    /// Hyperliquid spot holding of the market's base token.
    async fn hl_spot_holding(&self, base: &str) -> Result<f64> {
        let b = self
            .hl
            .get_combined_balance()
            .await
            .map_err(|e| anyhow!("HL get_combined_balance: {e:?}"))?;
        Ok(b.spot_assets
            .iter()
            .filter(|a| a.symbol.eq_ignore_ascii_case(base))
            .map(|a| a.balance.to_f64().unwrap_or(0.0))
            .sum())
    }

    /// Lighter signed perp position for the symbol (long > 0).
    async fn lt_perp_holding(&self, symbol: &str) -> Result<f64> {
        let pos = self
            .lt
            .get_positions()
            .await
            .map_err(|e| anyhow!("Lighter get_positions: {e:?}"))?;
        Ok(pos
            .iter()
            .filter(|p| p.symbol.eq_ignore_ascii_case(symbol))
            .map(|p| p.size.to_f64().unwrap_or(0.0) * if p.sign < 0 { -1.0 } else { 1.0 })
            .sum())
    }

    /// Hyperliquid spot IOC (buy or sell). Returns the CONFIRMED fill (change
    /// in the base holding, net of any base-denominated fee); dry-run returns
    /// the requested size.
    async fn hl_spot_ioc(
        &mut self,
        market: &str,
        size: Decimal,
        side: OrderSide,
    ) -> Result<Decimal> {
        if self.cfg.dry_run {
            log::info!("[DRY_RUN] HL spot IOC {side} {market} size={size}");
            return Ok(size);
        }
        let base = spot_base(market);
        let before = self.hl_spot_holding(&base).await.with_context(|| {
            format!("{market}: pre-order holding unreadable, NOT ordering (fill could not be confirmed)")
        })?;
        self.mark_pending("hyperliquid", market, side, size, before)?;
        let sent = self
            .hl
            .create_order_taker_ioc(market, size, side, self.cfg.hl_taker_slippage_bps, false)
            .await;
        let rejected = sent
            .as_ref()
            .err()
            .map_or(false, order_rejected_definitively);
        let sent = sent.map_err(|e| anyhow!("HL spot IOC {side} {market}: {e:?}"));
        let observed = self
            .confirm_fill(
                || self.hl_spot_holding(&base),
                before,
                side,
                size.to_f64().unwrap_or(0.0),
            )
            .await;
        self.settle_pending(&sent, rejected, &observed);
        settle_fill(&format!("HL spot {side} {market}"), size, sent, observed)
    }

    /// Lighter perp taker (price=None → IOC with protection price; the same
    /// path every pairtrade taker caller uses). Returns the CONFIRMED fill
    /// (change in the venue position); dry-run returns the requested size.
    async fn lt_perp_taker(
        &mut self,
        symbol: &str,
        size: Decimal,
        side: OrderSide,
        reduce_only: bool,
    ) -> Result<Decimal> {
        if self.cfg.dry_run {
            log::info!(
                "[DRY_RUN] Lighter perp taker {side} {symbol} size={size} reduce_only={reduce_only}"
            );
            return Ok(size);
        }
        let before = self.lt_perp_holding(symbol).await.with_context(|| {
            format!("{symbol}: pre-order position unreadable, NOT ordering (fill could not be confirmed)")
        })?;
        self.mark_pending("lighter", symbol, side, size, before)?;
        let sent = self
            .lt
            .create_order(symbol, size, side, None, None, reduce_only, None)
            .await;
        let rejected = sent
            .as_ref()
            .err()
            .map_or(false, order_rejected_definitively);
        let sent = sent.map_err(|e| anyhow!("Lighter perp {side} {symbol}: {e:?}"));
        let observed = self
            .confirm_fill(
                || self.lt_perp_holding(symbol),
                before,
                side,
                size.to_f64().unwrap_or(0.0),
            )
            .await;
        self.settle_pending(&sent, rejected, &observed);
        settle_fill(
            &format!("Lighter perp {side} {symbol}"),
            size,
            sent,
            observed,
        )
    }

    /// Persist the order about to be sent, so a crash between send and
    /// confirmation leaves a trace that blocks further orders until the
    /// book has been reconciled against the venues.
    fn mark_pending(
        &mut self,
        venue: &str,
        symbol: &str,
        side: OrderSide,
        size: Decimal,
        before: f64,
    ) -> Result<()> {
        self.state.pending_order = Some(PendingOrder {
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            side: side.to_string(),
            requested: size.to_f64().unwrap_or(0.0),
            holding_before: before,
            ts: now_secs(),
        });
        // The book's provenance starts with its first order, filled or not
        // — stamped on every new cycle, so a previous cycle's mode never
        // survives into this one (the startup gate would otherwise refuse
        // a live restart after a crash between this order and open_book).
        if self.state.mode != Mode::On {
            self.state.book_dry_run = Some(self.cfg.dry_run);
        }
        // Fail closed: the marker is the only thing that makes a crash
        // between here and the confirmation recoverable. Unwritten → no
        // order.
        persist_json(&self.cfg.state_path, &self.state).with_context(|| {
            format!("{symbol}: could not persist the pending-order marker, NOT ordering")
        })?;
        Ok(())
    }

    /// Clear the pending marker only when the outcome is KNOWN: the holding
    /// was readable and either changed, or the venue definitively rejected
    /// the order and nothing changed. An acknowledged order the venue never
    /// showed, a timed-out submission, or an unreadable holding keeps it.
    fn settle_pending(
        &mut self,
        sent: &Result<dex_connector::CreateOrderResponse>,
        rejected_definitively: bool,
        observed: &Result<f64>,
    ) {
        let observed_fill = observed.as_ref().ok().copied();
        if order_outcome_known(sent.is_ok(), rejected_definitively, observed_fill) {
            // A confirmed NO-fill has nothing to record: clear now. A
            // confirmed fill stays marked until the caller has persisted
            // the leg (`clear_pending_after_record`): a crash in between
            // would otherwise leave real exposure with no leg and no
            // marker, i.e. a "flat" book.
            if observed_fill.unwrap_or(0.0) <= 0.0 {
                self.state.pending_order = None;
            }
        } else {
            log::error!(
                "[FILL] outcome of {:?} is UNKNOWN — it stays recorded as pending; no further order until the book is reconciled",
                self.state.pending_order
            );
        }
        self.persist();
    }

    /// The fill an order produced is now in state: the marker can go. Called
    /// right after the leg is persisted, so the two writes are adjacent and
    /// a crash between them leaves the (safe) marker, not a flat book.
    fn clear_pending_after_record(&mut self) {
        if self.state.pending_order.take().is_some() {
            self.persist();
        }
    }

    /// Re-read the holding until it reflects the whole requested change or
    /// the wait budget runs out; returns the change observed in the order's
    /// direction (never negative). `Err` only if the holding could not be
    /// read at all — the exposure is then unknown, not zero.
    async fn confirm_fill<F, Fut>(
        &self,
        read: F,
        before: f64,
        side: OrderSide,
        requested: f64,
    ) -> Result<f64>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<f64>>,
    {
        let dir = if side == OrderSide::Short { -1.0 } else { 1.0 };
        let mut last_err = None;
        // Best successful observation so far. A read failure AFTER a
        // successful read must not discard what was already seen: the
        // exposure is known to be at least that, and returning an error
        // instead would route the caller to the unknown-fill path.
        let mut observed: Option<f64> = None;
        for attempt in 0..FILL_CONFIRM_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(FILL_CONFIRM_STEP_MS)).await;
            match read().await {
                Ok(after) => {
                    let seen = ((after - before) * dir).max(0.0);
                    observed = Some(observed.map_or(seen, |o: f64| o.max(seen)));
                    if fill_complete(seen, requested) {
                        return Ok(seen);
                    }
                    log::debug!(
                        "[FILL] attempt {}: observed {seen} of {requested}, waiting",
                        attempt + 1
                    );
                }
                Err(e) => {
                    log::warn!(
                        "[FILL] holding re-read failed (attempt {}): {e:?}",
                        attempt + 1
                    );
                    last_err = Some(e);
                }
            }
        }
        match (observed, last_err) {
            (Some(o), _) => Ok(o),
            (None, Some(e)) => Err(e).context("holding unreadable after the order was sent"),
            (None, None) => Ok(0.0),
        }
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
        if leg.stop_unconfirmed_id.is_none()
            // A stop the venue is believed to have dropped is never
            // "already current", however well its recorded level and size
            // match. The metadata is kept precisely so it can be trusted
            // again if the id turns up listed (`stop_seen_listed`).
            && !leg.stop_presumed_gone
            && stop_is_current(
                leg.stop_level,
                leg.stop_size,
                leg.stop_order_id.is_some(),
                level,
                leg.perp_size,
            )
        {
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.lighter_peak = leg.lighter_peak;
            }
            self.persist();
            return Ok(());
        }
        // Before touching the stop that is already there: a stop the
        // market's leverage cannot carry would be accepted over REST and
        // dropped, and cancelling first would turn a protected leg into a
        // permanently uncovered one (Codex review).
        let carried = self
            .stop_distance_supported(symbol, lighter_price, level)
            .await;
        // Unknown counts as "do not touch a stop that exists": cancelling
        // it and then having Lighter drop the replacement would leave the
        // leg uncovered on the strength of a failed read. With no stop to
        // lose, an attempt is the better bet.
        // A presumed-gone id is not protection to preserve: the venue
        // reported no order, named it cancelled, or stopped listing it for
        // the whole grace. Counting it here would turn a failed margin
        // read into an indefinitely uncovered leg that never even tried.
        let has_stop = (leg.stop_order_id.is_some() && !leg.stop_presumed_gone)
            || leg.stop_unconfirmed_id.is_some();
        if carried == Some(false) || (carried.is_none() && has_stop) {
            // Keep the refreshed peak even though the move is refused: the
            // peak is the exit rule's own record, and dropping a new high
            // here would place the stop off a stale one once the leverage
            // is corrected.
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.lighter_peak = leg.lighter_peak;
            }
            self.persist();
            bail!(
                "Lighter stop {symbol}: {} for a stop at {level:.2}; the existing stop is left alone",
                if carried.is_none() {
                    "the market's leverage could not be read"
                } else {
                    "this market's leverage cannot carry one (see [STOP] above)"
                }
            );
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
                    if may_forget_stop(false, leg.stop_absence_confirmed) {
                        // Cancelling an order the VENUE has already
                        // reported gone fails for the obvious reason.
                        // Deferring on that would block the replacement
                        // this verdict exists to trigger.
                        log::warn!(
                            "[STOP] cancel of {old} failed and the venue already reported it gone; replacing it: {e:?}"
                        );
                    } else {
                        log::warn!(
                            "[STOP] cancel previous stop {old} failed, keeping it tracked for retry: {e:?}"
                        );
                        prior_cancel_failed = true;
                    }
                }
            }
            if !prior_cancel_failed {
                // The cancel is confirmed: drop the old stop from state NOW,
                // before the replacement is attempted. If creating it fails
                // below, state must show the leg uncovered so `ensure_stops`
                // retries — keeping the cancelled id would read as covered
                // (same level, same size) and nothing would ever retry.
                leg.stop_order_id = None;
                leg.stop_level = None;
                leg.stop_size = None;
                if let Some(l) = self.state.legs.get_mut(symbol) {
                    l.stop_order_id = None;
                    l.stop_level = None;
                    l.stop_size = None;
                    l.lighter_peak = leg.lighter_peak;
                }
                self.persist();
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
            // An earlier stop whose existence is unknown must be cancelled
            // by id first — the sweep below cannot see what the WS cache
            // does not carry, so placing a replacement while it is still
            // tracked risks TWO live stops on the position.
            if !self.drop_unconfirmed_stop(symbol).await {
                bail!(
                    "Lighter stop {symbol}: an earlier stop with unknown state is still tracked and could not be cancelled; not placing another one"
                );
            }
            // Sweep whatever else rests on this symbol before resting the
            // new stop: a trigger submission whose response was lost (timed
            // out after acceptance) never got an id recorded, and this is
            // the only way it is ever removed. The account is dedicated and
            // the bot rests nothing but this stop, so a sweep costs nothing.
            // Whether Lighter's cancel-all covers trigger orders is part of
            // the #950 live check; a failed sweep defers the placement.
            self.lt
                .cancel_all_orders(Some(symbol.to_string()))
                .await
                .map_err(|e| {
                    anyhow!("Lighter cancel-all on {symbol} before resting a stop: {e:?}")
                })?;
            // `side` is the POSITION side for Lighter's TP/SL helper (long
            // position → sell stop).
            //
            // Style: stop-LIMIT with a wide protective limit (trigger minus
            // `stop_slippage_bps`), Lighter type 3. The connector's `Market`
            // style (type 2) sends execution price 0, which the Lighter
            // signer rejects ("OrderPrice should not be less than 1") — found
            // on the first live ARM (bot-strategy#895 / #950). Once
            // triggered, the limit sits far below the trigger, so it fills
            // as a taker unless the market gapped through it, in which case
            // it rests there instead of being lost.
            let resp = self
                .lt
                .create_advanced_trigger_order(
                    symbol,
                    size,
                    OrderSide::Long,
                    trigger,
                    None,
                    TriggerOrderStyle::MarketWithSlippageControl,
                    Some(self.cfg.stop_slippage_bps),
                    TpSl::Sl,
                    true,
                    None,
                )
                .await
                .with_context(|| format!("Lighter stop {symbol} @ {level:.2}"))?;
            // `sendTx` returning 200 means the transaction was ACCEPTED for
            // processing, not that the order rests: Lighter validates it
            // asynchronously and can drop it silently (first live stops,
            // 2026-09-22 — 2 of 3 acknowledged stops never appeared at the
            // venue while the bot recorded their ids and stopped retrying).
            // The order only exists once the venue reports it.
            // Persist the id as unconfirmed BEFORE the confirmation poll: a
            // crash inside that window would otherwise leave an accepted
            // order with no record anywhere. It is promoted to real cover
            // below, only if the venue shows it.
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.stop_unconfirmed_id = Some(resp.order_id.clone());
            }
            self.persist();
            self.confirm_stop_rests(symbol, &resp.order_id).await?;
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.stop_unconfirmed_id = None;
            }
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
            // The run, and the doubt, belonged to the order this one
            // replaces.
            l.stop_unlisted_since = None;
            l.stop_presumed_gone = false;
            l.stop_absence_confirmed = false;
        }
        self.persist();
        Ok(())
    }

    /// Every leg's observation ends — used where stop checking itself
    /// stops, so an interval nobody was watching cannot be counted as
    /// time the stop went unlisted.
    fn clear_all_unlisted_since(&mut self) {
        let symbols: Vec<String> = self.state.legs.keys().cloned().collect();
        let mut changed = false;
        for sym in symbols {
            changed |= self.end_unlisted_observation(&sym);
        }
        if changed {
            self.persist();
        }
    }

    /// The observation ends without a verdict: the connection, the read
    /// or the check itself went away, so the elapsed time stops counting
    /// towards "gone" and the window restarts from the next miss.
    ///
    /// An absence ALREADY established stays established. An answer that
    /// could not be read is not evidence the order came back, and
    /// forgetting that would let `place_stop` count the absent id as
    /// cover it must preserve — leaving the leg uncovered for as long as
    /// the reads stay inconclusive, without ever attempting a
    /// replacement. Only seeing the id listed, or placing a new stop,
    /// clears that (`stop_seen_listed` / `place_stop`).
    ///
    /// Returns whether anything changed, so the caller only writes state
    /// when it did.
    fn end_unlisted_observation(&mut self, symbol: &str) -> bool {
        match self.state.legs.get_mut(symbol) {
            Some(l) if l.stop_unlisted_since.is_some() => {
                l.stop_unlisted_since = None;
                true
            }
            _ => false,
        }
    }

    /// Record that the tracked stop is believed not to be resting.
    /// `absence_confirmed` says whether the VENUE gave that answer (see
    /// `may_forget_stop`) or it was inferred from the grace.
    ///
    /// `stop_level` and `stop_size` are deliberately KEPT. They used to
    /// be cleared so the leg could not read as covered, but
    /// `stop_presumed_gone` says that now — and throwing the metadata
    /// away is unrecoverable: if the absence was only inferred and the
    /// order turns out to still rest, the leg has no level or size to be
    /// judged current against, so the next pass cancels and re-places a
    /// perfectly good stop. That churn is exactly the live failure of
    /// 2026-09-23, and each round of it can leave the leg uncovered.
    fn mark_stop_presumed_gone(&mut self, symbol: &str, absence_confirmed: bool) {
        if let Some(l) = self.state.legs.get_mut(symbol) {
            l.stop_unlisted_since = None;
            l.stop_presumed_gone = true;
            // Never downgrade. Once the venue itself has said the order is
            // not there, a later pass that only re-observes the absence
            // (another order rests, this id still unlisted) says nothing
            // new — and demoting to "inferred" would make a failed cancel
            // of the already-absent id defer forever, for as long as that
            // unrelated order rests. Only a new stop, or the id turning up
            // listed, clears it.
            l.stop_absence_confirmed |= absence_confirmed;
        }
        self.persist();
    }

    /// The venue's order list names the tracked stop: it rests. Both the
    /// run and the doubt it raised are over.
    fn stop_seen_listed(&mut self, symbol: &str) -> bool {
        match self.state.legs.get_mut(symbol) {
            Some(l) if l.stop_unlisted_since.is_some() || l.stop_presumed_gone => {
                l.stop_unlisted_since = None;
                l.stop_presumed_gone = false;
                l.stop_absence_confirmed = false;
                true
            }
            _ => false,
        }
    }

    /// Cancel a previously unconfirmed stop by id (no WS cache involved)
    /// and forget it once the venue confirms the cancel. A failed cancel
    /// keeps it recorded for the next attempt — the id is the only handle
    /// on an order that may or may not exist.
    /// `true` = nothing uncertain is left for this symbol (there was none,
    /// or the cancel was confirmed). `false` = an order that may be resting
    /// is still tracked, and no new stop may be placed on top of it.
    async fn drop_unconfirmed_stop(&mut self, symbol: &str) -> bool {
        let Some(id) = self
            .state
            .legs
            .get(symbol)
            .and_then(|l| l.stop_unconfirmed_id.clone())
        else {
            return true;
        };
        if self.cfg.dry_run {
            if let Some(l) = self.state.legs.get_mut(symbol) {
                l.stop_unconfirmed_id = None;
            }
            self.persist();
            return true;
        }
        // HTTP 200 on the cancel is the same weak signal as on the
        // placement: accepted for processing, not "the order is gone". And
        // the motivating failure — a stop dropped during Lighter's
        // asynchronous validation — never existed, so it can never appear
        // in the cancelled feed either. Two independent settlements:
        // the venue reports it cancelled, or a READY account snapshot
        // shows it is not resting.
        if let Err(e) = self.lt.cancel_order(symbol, &id).await {
            // A timed-out cancel may still have been processed, and a
            // repeat cancel of an already-cancelled id returns not-found —
            // so the request's outcome never gates the evidence below.
            log::warn!("[STOP] {symbol}: cancel of unconfirmed stop {id} failed: {e:?}");
        }
        let settled = self.cancel_confirmed(symbol, &id).await
            || self.stop_absent_confirmed(symbol, &id).await;
        if !settled {
            log::warn!(
                "[STOP] {symbol}: {id} is still unsettled (neither reported cancelled nor shown absent by a ready snapshot); keeping it tracked"
            );
            return false;
        }
        log::warn!("[STOP] {symbol}: unconfirmed stop {id} settled — it is not resting");
        if let Some(l) = self.state.legs.get_mut(symbol) {
            l.stop_unconfirmed_id = None;
        }
        self.persist();
        true
    }

    /// Is `order_id` provably NOT resting? The connector's order list is
    /// served from the Lighter WebSocket cache, which can be empty or
    /// stale (a reconnect, or a stalled connection whose first snapshot
    /// still satisfies the positions-ready flag), so its silence is never
    /// proof. Lighter's account endpoint is REST and fetched per call:
    /// zero resting orders for the market settles it. This is the only
    /// path that can clear a stop Lighter dropped during validation, which
    /// never existed and so can never appear in the cancelled feed.
    /// The account these venue reads target. Configured index when the
    /// deployment pins one; otherwise discovered from the wallet address
    /// (the connector's own auto-discovery case) and cached. A wallet with
    /// several accounts cannot be disambiguated here without the API-key
    /// probe the connector does, so it resolves to `None` — which makes
    /// every absence check inconclusive rather than wrong.
    async fn lighter_account_index(&self) -> Option<String> {
        if !self.cfg.lighter_account_index.is_empty() {
            return Some(self.cfg.lighter_account_index.clone());
        }
        let mut cached = self.resolved_account_index.lock().await;
        if let Some(idx) = cached.as_ref() {
            return Some(idx.clone());
        }
        if self.cfg.lighter_wallet_address.is_empty() {
            log::warn!(
                "[STOP] no Lighter account index and no wallet address to discover one; venue order checks are inconclusive"
            );
            return None;
        }
        let url = format!(
            "{}/api/v1/account?by=l1_address&value={}",
            self.cfg.lighter_account_url.trim_end_matches('/'),
            self.cfg.lighter_wallet_address
        );
        let v: serde_json::Value = match self.http.get(&url).send().await {
            Ok(r) => r.json().await.ok()?,
            Err(e) => {
                log::warn!("[STOP] account discovery read failed: {e:?}");
                return None;
            }
        };
        let accounts = v.get("accounts")?.as_array()?;
        let indices: Vec<u64> = accounts
            .iter()
            .filter_map(|a| a.get("account_index").and_then(|x| x.as_u64()))
            .collect();
        let idx = match indices.as_slice() {
            [] => {
                log::warn!(
                    "[STOP] wallet {} has no accounts",
                    self.cfg.lighter_wallet_address
                );
                return None;
            }
            [only] => only.to_string(),
            // Several accounts behind one wallet: the API key picks the
            // one the connector trades on, the same probe it does.
            many => self.account_for_api_key(many).await?.to_string(),
        };
        log::info!("[STOP] resolved Lighter account index {idx} from the wallet address");
        *cached = Some(idx.clone());
        Some(idx)
    }

    /// Can a stop sit `stop_dd_pct` below the peak on this market? Lighter
    /// validates a trigger order against the market's leverage: a move that
    /// would cost more than the margin behind the position is refused, and
    /// the refusal is silent (the transaction is accepted over REST and
    /// dropped during execution — bot-strategy#950). `initial_margin_fraction`
    /// is that margin as a percentage, so the stop distance must fit inside
    /// it. `None` = the account could not be read.
    async fn stop_distance_supported(&self, symbol: &str, mark: f64, trigger: f64) -> Option<bool> {
        if self.cfg.dry_run {
            // The simulated stop is never submitted, so the venue never
            // judges it. Refusing here would leave a DRY_RUN book recorded
            // as uncovered on an account whose leverage is irrelevant to it.
            return Some(true);
        }
        // What Lighter validates is how far the TRIGGER sits from the
        // current mark, not the trailing percentage: after a drawdown the
        // peak-based level can be much closer to the mark than
        // `stop_dd_pct` suggests, and rejecting that would leave the leg
        // uncovered for no reason (Codex review).
        let distance_pct = if mark > 0.0 {
            100.0 * (1.0 - trigger / mark)
        } else {
            return None;
        };
        if distance_pct <= 0.0 {
            return Some(true); // at or above the mark: not this guard's case
        }
        let index = self.lighter_account_index().await?;
        let url = format!(
            "{}/api/v1/account?by=index&value={index}",
            self.cfg.lighter_account_url.trim_end_matches('/'),
        );
        let v: serde_json::Value = self.http.get(&url).send().await.ok()?.json().await.ok()?;
        let imf = margin_fraction_for(&v, symbol)?;
        // The margin has to outlast the drawdown AND the maintenance
        // requirement that bites on the way there — the same floor the
        // collateral guard uses. An initial margin a hair above the
        // trigger distance is liquidated before the stop can fire, and
        // Lighter refuses such an order silently.
        let floor = liquidation_floor_pct(distance_pct, self.cfg.lighter_mmr_pct);
        if imf + 1e-9 < floor {
            log::error!(
                "[STOP] {symbol}: the market's initial margin is {imf:.2}% but a stop at {trigger:.2}, {distance_pct:.2}% below the mark {mark:.2}, needs {floor:.2}% to survive liquidation at {:.2}% maintenance — Lighter will refuse it. Lower this market's leverage to {:.1}x or less (currently {:.1}x).",
                self.cfg.lighter_mmr_pct,
                100.0 / floor,
                100.0 / imf.max(f64::EPSILON)
            );
            return Some(false);
        }
        Some(true)
    }

    /// Which of `candidates` carries this bot's API key — the account the
    /// connector resolved. Mirrors its discovery probe (an `apikeys` read
    /// per candidate). `None` when no key index is configured or none
    /// matches, which leaves the venue checks inconclusive.
    async fn account_for_api_key(&self, candidates: &[u64]) -> Option<u64> {
        // Same default as the connector's config loader: slot 0.
        let key_index = lighter_env("LIGHTER_API_KEY_INDEX", &self.cfg.instance_id)
            .unwrap_or_else(|| "0".to_string());
        let base = self.cfg.lighter_account_url.trim_end_matches('/');
        let mut matched: Vec<u64> = Vec::new();
        // A probe that could not be read leaves the set incomplete: the
        // account it would have matched may be the real one, so "only one
        // match" among the rest is not a conclusion.
        let mut incomplete = false;
        for &idx in candidates {
            let url =
                format!("{base}/api/v1/apikeys?account_index={idx}&api_key_index={key_index}");
            let v: serde_json::Value = match self.http.get(&url).send().await {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!("[STOP] api-key probe for account {idx} unreadable: {e:?}");
                        incomplete = true;
                        continue;
                    }
                },
                Err(e) => {
                    log::warn!("[STOP] api-key probe for account {idx} failed: {e:?}");
                    incomplete = true;
                    continue;
                }
            };
            let has_key = v
                .get("api_keys")
                .and_then(|k| k.as_array())
                .is_some_and(|k| !k.is_empty());
            if v.get("code").and_then(|c| c.as_u64()) == Some(200) && has_key {
                matched.push(idx);
            }
        }
        match matched.as_slice() {
            [only] if !incomplete => Some(*only),
            // Zero matches, or several accounts with a key in the same
            // slot: this probe cannot tell them apart (the bot's own
            // public key is KMS ciphertext here, not comparable material),
            // and guessing could point the checks at another account.
            _ => {
                log::warn!(
                    "[STOP] {} of the wallet's {} accounts carry API key index {key_index}{}; set LIGHTER_ACCOUNT_INDEX to name the one this bot trades — venue order checks are inconclusive until then",
                    matched.len(),
                    candidates.len(),
                    if incomplete { " (and some could not be read)" } else { "" }
                );
                None
            }
        }
    }

    /// Orders resting for `symbol` according to Lighter's own account
    /// endpoint (REST, not the WebSocket cache): `None` when the read or
    /// the parse failed. Counts the market's resting and position-tied
    /// orders, plus the account-wide pending count — the account is
    /// dedicated to this bot, which rests nothing but its stops.
    async fn lighter_resting_orders(&self, symbol: &str) -> Option<u64> {
        let index = self.lighter_account_index().await?;
        let url = format!(
            "{}/api/v1/account?by=index&value={index}",
            self.cfg.lighter_account_url.trim_end_matches('/'),
        );
        let v: serde_json::Value = match self.http.get(&url).send().await {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("[STOP] {symbol}: account endpoint parse failed: {e:?}");
                    return None;
                }
            },
            Err(e) => {
                log::warn!("[STOP] {symbol}: account endpoint read failed: {e:?}");
                return None;
            }
        };
        match resting_orders_for(&v, symbol) {
            Some(n) => Some(n),
            None => {
                log::warn!("[STOP] {symbol}: account endpoint response not understood");
                None
            }
        }
    }

    async fn stop_absent_confirmed(&self, symbol: &str, order_id: &str) -> bool {
        for attempt in 1..=STOP_CONFIRM_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(STOP_CONFIRM_STEP_MS)).await;
            // The REST account endpoint is fetched fresh on every call, so
            // it cannot be a stale cache from before this placement or
            // cancel — which is what the WebSocket-fed views cannot rule
            // out. Zero orders for the market is proof nothing rests.
            match self.lighter_resting_orders(symbol).await {
                Some(0) => return true,
                Some(n) => log::warn!(
                    "[STOP] {symbol}: the venue reports {n} resting order(s) for this market, {order_id} may be one of them (attempt {attempt}/{STOP_CONFIRM_ATTEMPTS})"
                ),
                None => log::warn!(
                    "[STOP] {symbol}: venue account read unavailable (attempt {attempt}/{STOP_CONFIRM_ATTEMPTS}); absence proves nothing"
                ),
            }
        }
        false
    }

    /// Did the venue itself report `order_id` as cancelled? Positive
    /// evidence only: the cancelled-order feed naming it. An order list
    /// that merely no longer shows it is not proof (the same WS cache can
    /// be empty during a reconnect), so "not seen" keeps it tracked.
    async fn cancel_confirmed(&self, symbol: &str, order_id: &str) -> bool {
        for attempt in 1..=STOP_CONFIRM_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(STOP_CONFIRM_STEP_MS)).await;
            match self.lt.get_canceled_orders(symbol).await {
                Ok(resp) if resp.orders.iter().any(|o| o.order_id == order_id) => return true,
                Ok(_) => log::debug!(
                    "[STOP] {symbol}: {order_id} not in the cancelled feed yet (attempt {attempt}/{STOP_CONFIRM_ATTEMPTS})"
                ),
                Err(e) => log::warn!(
                    "[STOP] {symbol}: cancelled-order read failed (attempt {attempt}/{STOP_CONFIRM_ATTEMPTS}): {e:?}"
                ),
            }
        }
        false
    }

    /// Poll the venue's own order list until it reports `order_id`. The
    /// Lighter connector serves this from its WebSocket order tracking,
    /// which was confirmed live to carry trigger orders (a stop placed at
    /// 16:48 UTC on 2026-09-22 was found and cancelled through it).
    /// `Err` = the venue never showed it; the caller must not record it.
    async fn confirm_stop_rests(&self, symbol: &str, order_id: &str) -> Result<()> {
        let mut last: Option<String> = None;
        for attempt in 1..=STOP_CONFIRM_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(STOP_CONFIRM_STEP_MS)).await;
            match self.lt.get_open_orders(symbol).await {
                Ok(resp) => {
                    if resp.orders.iter().any(|o| o.order_id == order_id) {
                        return Ok(());
                    }
                    let ids: Vec<&str> = resp.orders.iter().map(|o| o.order_id.as_str()).collect();
                    log::warn!(
                        "[STOP] {symbol}: stop {order_id} not visible yet (attempt {attempt}/{STOP_CONFIRM_ATTEMPTS}); venue shows {ids:?}"
                    );
                    last = None;
                }
                Err(e) => {
                    log::warn!(
                        "[STOP] {symbol}: open-order read failed (attempt {attempt}/{STOP_CONFIRM_ATTEMPTS}): {e:?}"
                    );
                    last = Some(format!("{e:?}"));
                }
            }
        }
        match last {
            Some(e) => bail!(
                "stop {order_id} for {symbol} could not be confirmed: the venue's order list was unreadable ({e})"
            ),
            None => bail!(
                "stop {order_id} for {symbol} was acknowledged but never appeared at the venue (sendTx 200 is not an order); it is NOT recorded, so the next reconcile re-places it"
            ),
        }
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
                // Keeping it tracked is right while the order MIGHT be
                // resting — but not when the VENUE already said it is
                // not: no order rests for this market, or it named this
                // id cancelled. Cancelling something absent is exactly
                // what fails here, so holding the id would keep the book
                // On/halted forever and no retried DISARM could ever
                // clear it.
                //
                // Deliberately not `stop_presumed_gone`: that is also set
                // by the unlisted-for-the-whole-grace fallback, which is
                // an inference about a stale WebSocket cache. A transient
                // transport error on top of it would drop an id whose
                // reduce-only trigger may still rest — and a later ARM
                // would inherit it against the new position.
                if !may_forget_stop(false, leg.stop_absence_confirmed) {
                    log::warn!(
                        "[STOP] cancel {id} failed, leaving it tracked in state (order may still be resting): {e:?}"
                    );
                    return;
                }
                log::warn!(
                    "[STOP] cancel {id} failed, but the venue already reported it gone — dropping it: {e:?}"
                );
            }
        }
        if let Some(l) = self.state.legs.get_mut(symbol) {
            l.stop_order_id = None;
            l.stop_level = None;
            l.stop_size = None;
            // The id is gone from state, so the doubt about it has
            // nothing left to attach to.
            l.stop_unlisted_since = None;
            l.stop_presumed_gone = false;
            l.stop_absence_confirmed = false;
        }
        // Persist now, before the (slow) exit orders that follow: a crash
        // in between would otherwise restart with the cancelled id still
        // trusted as cover.
        self.persist();
    }

    // ------------------------------------------------- collateral guard

    /// Lighter account equity (collateral + unrealised PnL, USD): the cross-
    /// margin figure the venue liquidates against. Fails closed.
    async fn lighter_equity_usd(&self) -> Result<f64> {
        let b = self
            .lt
            .get_balance(None)
            .await
            .map_err(|e| anyhow!("Lighter get_balance: {e:?}"))?;
        let e = b
            .equity
            .to_f64()
            .ok_or_else(|| anyhow!("Lighter equity {} not representable", b.equity))?;
        if !e.is_finite() {
            bail!("Lighter equity {e} is not finite");
        }
        Ok(e)
    }

    /// Current Lighter price for exactly the given symbols (read-only). Only
    /// symbols that actually carry perp size are priced: a quote failure on an
    /// unrelated configured symbol must not abort the collateral evaluation
    /// for the legs that are open.
    async fn lighter_marks_for(
        &self,
        symbols: &BTreeMap<String, f64>,
    ) -> Result<BTreeMap<String, f64>> {
        let mut marks = BTreeMap::new();
        for sym in symbols.keys() {
            marks.insert(sym.clone(), self.quote(&self.lt, sym).await?.price);
        }
        Ok(marks)
    }

    /// Perp sizes the collateral guard must value: recorded book merged with
    /// the venue's actual open positions (see `merge_perp_sizes`).
    async fn perp_sizes(&self) -> Result<(BTreeMap<String, f64>, Vec<String>)> {
        let positions = self
            .lt
            .get_positions()
            .await
            .map_err(|e| anyhow!("Lighter get_positions: {e:?}"))?;
        let venue: Vec<(String, f64, i32)> = positions
            .iter()
            .map(|p| (p.symbol.clone(), p.size.to_f64().unwrap_or(0.0), p.sign))
            .collect();
        let recorded: BTreeMap<String, f64> = self
            .state
            .legs
            .iter()
            .map(|(sym, leg)| (sym.clone(), leg.perp_size))
            .collect();
        Ok(merge_perp_sizes(&self.cfg.symbols, &recorded, &venue))
    }

    /// Pre-order collateral check (bot-strategy#909): the perp book may only
    /// grow while Lighter equity covers the notional AFTER the order at
    /// `perp_margin_min_pct`. `Ok(None)` = covered; `Ok(Some(msg))` = short
    /// (msg names the deposit needed); `Err` = could not verify (callers
    /// fail closed). DRY_RUN evaluates and logs but never blocks — the
    /// DRY_RUN account holds no real collateral, so it would always be short.
    async fn margin_precheck(&self, adding_perp_usd: f64) -> Result<Option<String>> {
        // `PERP_FRACTION=0` is a valid spot-only book. Decide that from state
        // alone, BEFORE any venue read, so a Lighter outage cannot halt an
        // ARM (or postpone a tranche) that needs no Lighter collateral.
        let holds_perp = self.state.legs.values().any(|l| l.perp_size > 0.0);
        if adding_perp_usd <= 0.0 && !holds_perp {
            return Ok(None);
        }
        let (sizes, unpriced) = match self.perp_sizes().await {
            Ok(v) => v,
            Err(e) if self.cfg.dry_run => {
                log::warn!(
                    "[MARGIN] DRY_RUN: Lighter positions unavailable, precheck skipped: {e:?}"
                );
                return Ok(None);
            }
            Err(e) => return Err(e.context("collateral precheck: cannot verify Lighter positions")),
        };
        if !unpriced.is_empty() && !self.cfg.dry_run {
            return Ok(Some(format!(
                "Lighter holds exposure the collateral guard cannot model ({}); it consumes the same cross collateral and the long-only stop cannot cover a short, so the shortfall cannot be computed — close it (or add the symbol to BULL_HOLDER_SYMBOLS) before arming",
                unpriced.join(", ")
            )));
        }
        let marks = self.lighter_marks_for(&sizes).await?;
        let after = perp_notional_usd(&sizes, &marks) + adding_perp_usd.max(0.0);
        if after <= 0.0 {
            return Ok(None);
        }
        let equity = match self.lighter_equity_usd().await {
            Ok(e) => e,
            Err(e) if self.cfg.dry_run => {
                log::warn!("[MARGIN] DRY_RUN: Lighter equity unavailable, precheck skipped: {e:?}");
                return Ok(None);
            }
            Err(e) => return Err(e.context("collateral precheck: cannot verify Lighter equity")),
        };
        let required = required_collateral_usd(after, self.cfg.perp_margin_min_pct);
        if equity >= required {
            log::info!(
                "[MARGIN] precheck ok: Lighter equity ${equity:.2} >= ${required:.2} for perp ${after:.0} after this order ({}%)",
                self.cfg.perp_margin_min_pct
            );
            return Ok(None);
        }
        let msg = format!(
            "Lighter equity ${equity:.2} < ${required:.2} required to hold perp ${after:.0} at {}% (BULL_HOLDER_PERP_MARGIN_MIN_PCT): deposit at least ${:.2} more USDC to the Lighter sub-account first",
            self.cfg.perp_margin_min_pct,
            required - equity
        );
        if self.cfg.dry_run {
            log::warn!("[MARGIN] DRY_RUN (not enforced): {msg}");
            return Ok(None);
        }
        Ok(Some(msg))
    }

    /// Runtime collateral guard, run with every reconcile while On: compares
    /// the symmetric-drawdown liquidation point with the resting stops.
    /// Alert + status only — never halts (the exit rule and the exchange stop
    /// must keep running) and never de-risks; the remedy is a deposit.
    async fn margin_monitor(&mut self) {
        let now = now_secs();
        self.last_margin_check = now;
        // What actually backs the collateral: the venue's own positions
        // merged with the recorded book (never the smaller of the two).
        let (sizes, unpriced) = match self.perp_sizes().await {
            Ok(v) => v,
            Err(e) => {
                log::warn!(
                    "[MARGIN] Lighter positions unavailable, guard skipped this cycle: {e:?}"
                );
                self.last_margin = Some(MarginSnapshot {
                    ts: now,
                    equity_usd: None,
                    perp_notional_usd: 0.0,
                    margin_pct: None,
                    liq_distance_pct: None,
                    worst_stop_distance_pct: None,
                    ok: false,
                    detail: format!("Lighter positions unavailable, guard not evaluated: {e}"),
                });
                return;
            }
        };
        if !unpriced.is_empty() {
            log::error!(
                "[MARGIN] Lighter holds exposure the guard cannot model ({}) on the same cross collateral; the guard cannot be trusted until it is closed or configured",
                unpriced.join(", ")
            );
        }
        // Spot-only book (`PERP_FRACTION=0`) or nothing open: no Lighter
        // collateral risk, and no reason to read marks at all.
        if sizes.is_empty() && unpriced.is_empty() {
            self.last_margin = Some(MarginSnapshot {
                ts: now,
                equity_usd: None,
                perp_notional_usd: 0.0,
                margin_pct: None,
                liq_distance_pct: None,
                worst_stop_distance_pct: None,
                ok: true,
                detail: "no perp exposure".into(),
            });
            return;
        }
        let marks = match self.lighter_marks_for(&sizes).await {
            Ok(m) => m,
            Err(e) => {
                log::warn!("[MARGIN] Lighter marks unavailable, guard skipped this cycle: {e:?}");
                // Never leave a stale `ok: true` in status.json when the
                // guard could not run.
                self.last_margin = Some(MarginSnapshot {
                    ts: now,
                    equity_usd: None,
                    perp_notional_usd: 0.0,
                    margin_pct: None,
                    liq_distance_pct: None,
                    worst_stop_distance_pct: None,
                    ok: false,
                    detail: format!("Lighter marks unavailable, guard not evaluated: {e}"),
                });
                return;
            }
        };
        let notional = perp_notional_usd(&sizes, &marks);
        if notional <= 0.0 {
            // Zero priced notional is only good news when there is nothing
            // the guard failed to model: an unconfigured or short venue
            // position prices to nothing here yet still consumes collateral.
            let ok = unpriced.is_empty();
            self.last_margin = Some(MarginSnapshot {
                ts: now,
                equity_usd: None,
                perp_notional_usd: 0.0,
                margin_pct: None,
                liq_distance_pct: None,
                worst_stop_distance_pct: None,
                ok,
                detail: if ok {
                    "no perp exposure".into()
                } else {
                    format!(
                        "Lighter exposure the guard cannot model: {}",
                        unpriced.join(", ")
                    )
                },
            });
            return;
        }
        let equity = match self.lighter_equity_usd().await {
            Ok(e) => e,
            Err(e) => {
                log::warn!("[MARGIN] Lighter equity unavailable, guard skipped this cycle: {e:?}");
                self.last_margin = Some(MarginSnapshot {
                    ts: now,
                    equity_usd: None,
                    perp_notional_usd: notional,
                    margin_pct: None,
                    liq_distance_pct: None,
                    worst_stop_distance_pct: None,
                    ok: false,
                    detail: format!("Lighter equity unavailable: {e}"),
                });
                return;
            }
        };
        // Furthest protection level, plus the legs that have none. A stop
        // only counts when it is tracked AND was placed for at least the size
        // now open: a failed (re)placement, or a venue position larger than
        // the stop we rest, leaves exposure the exchange cannot close. Such a
        // leg keeps the configured distance for the numeric bar (the
        // conservative choice — a larger distance demands more collateral)
        // but is reported separately, because "collateral is sufficient" and
        // "the position is protected" are different claims.
        let mut worst_stop = f64::NEG_INFINITY;
        let mut unprotected: Vec<String> = Vec::new();
        for (sym, size) in &sizes {
            let leg = self.state.legs.get(sym);
            let covered = leg
                .map(|l| stop_covers(l.stop_order_id.is_some(), l.stop_size, *size))
                .unwrap_or(false);
            match (leg.and_then(|l| l.stop_level), marks.get(sym), covered) {
                (Some(level), Some(&mark), true) => {
                    if mark < level {
                        // Separate from collateral: the stop should have
                        // fired. Surface it — the guard treats it as zero
                        // protection via the clamp in stop_distance_pct.
                        log::error!(
                            "[STOP] {sym}: mark {mark:.2} is below the resting stop {level:.2} but the perp leg is still open — verify the trigger order on Lighter"
                        );
                    }
                    worst_stop = worst_stop.max(stop_distance_pct(mark, level));
                }
                _ => {
                    unprotected.push(sym.clone());
                    worst_stop = worst_stop.max(self.cfg.stop_dd_pct);
                }
            }
        }
        let liq = liquidation_distance_pct(equity, notional, self.cfg.lighter_mmr_pct)
            .unwrap_or(f64::INFINITY);
        let margin_pct = equity / notional * 100.0;
        let short_of_collateral = margin_breached(liq, worst_stop, STOP_LIQ_CLEARANCE_PCT);
        let breached = short_of_collateral || !unpriced.is_empty() || !unprotected.is_empty();
        // A top-up figure is only meaningful when the shortfall is purely a
        // collateral one. With exposure the guard cannot model, or with legs
        // the exchange cannot close, the notional and the stop used for that
        // arithmetic are both wrong — say what has to be fixed instead of
        // quoting a number that could be short or even negative.
        let detail = if !unpriced.is_empty() {
            format!(
                "Lighter exposure the guard cannot model ({}); no top-up can be computed while it is open — close it or add the symbol to BULL_HOLDER_SYMBOLS. Modelled legs alone: liquidation at {liq:.1}% drawdown",
                unpriced.join(", ")
            )
        } else if !unprotected.is_empty() {
            format!(
                "perp exposure with no covering exchange stop ({}) — re-place the stop; liquidation at {liq:.1}% drawdown{}",
                unprotected.join(", "),
                if short_of_collateral {
                    format!(
                        ", and collateral is short by ${:.2}",
                        equity_for_distance_usd(
                            notional,
                            worst_stop + STOP_LIQ_CLEARANCE_PCT,
                            self.cfg.lighter_mmr_pct,
                        ) - equity
                    )
                } else {
                    String::new()
                }
            )
        } else if short_of_collateral {
            let need = equity_for_distance_usd(
                notional,
                worst_stop + STOP_LIQ_CLEARANCE_PCT,
                self.cfg.lighter_mmr_pct,
            ) - equity;
            format!(
                "liquidation at {liq:.1}% drawdown is inside the exchange stop at {worst_stop:.1}% (+{STOP_LIQ_CLEARANCE_PCT}% clearance): deposit at least ${need:.2} more USDC to Lighter"
            )
        } else {
            format!("liquidation at {liq:.1}% drawdown, furthest stop at {worst_stop:.1}%")
        };
        if breached && !self.cfg.dry_run {
            log::error!(
                "[MARGIN] BREACH: Lighter equity ${equity:.2} vs perp ${notional:.0} ({margin_pct:.1}%): {detail} — new tranches/ADDs are refused by the precheck; the daily exit rule and the exchange stop keep running"
            );
        } else if breached {
            log::info!(
                "[MARGIN] DRY_RUN (no real collateral, not enforced): equity ${equity:.2} vs perp ${notional:.0}: {detail}"
            );
        } else {
            log::info!(
                "[MARGIN] ok: Lighter equity ${equity:.2} vs perp ${notional:.0} ({margin_pct:.1}%): {detail}"
            );
        }
        self.last_margin = Some(MarginSnapshot {
            ts: now,
            equity_usd: Some(equity),
            perp_notional_usd: notional,
            margin_pct: Some(margin_pct),
            liq_distance_pct: Some(liq),
            worst_stop_distance_pct: Some(worst_stop),
            ok: !breached,
            detail,
        });
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
        // Collateral precheck against the WHOLE planned book (all tranches,
        // all symbols), read-only and before any order: the deposit contract
        // is for EQUITY_USD × PERP_FRACTION, not for one tranche — a ladder
        // must not be allowed to start on a fifth of the collateral. Short →
        // the ARM fails and the caller halts: deposit, RISK_ACK, ARM again.
        // Scheduled tranches re-check incrementally as the book grows.
        if let Some(short) = self.margin_precheck(perp_notional * n as f64).await? {
            bail!("{short}");
        }
        self.buy_tranche("ARM").await
    }

    /// Buy one tranche (`state.tranche_*_usd` per symbol) of both legs for
    /// every symbol. The first call of a cycle creates the legs and flips the
    /// book to On; later calls add to them. Each fill is persisted as it
    /// happens so a failure partway never leaves exposure unrecorded, and
    /// `tranche_progress` carries the notional filled per leg so a retry
    /// after a partial fill orders only the remainder.
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
        self.reconcile_before_orders(&format!("{why} tranche {n_th}"))
            .await?;
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
            // Remainders: what this tranche still owes each leg.
            let spot_size = size_from_notional(
                spot_notional - progress.spot_usd,
                hq.price,
                hq.size_decimals,
            );
            let perp_size = size_from_notional(
                perp_notional - progress.perp_usd,
                lq.price,
                lq.size_decimals,
            );
            // A remainder too small to order (a partial fill that stopped
            // just short) is settled as done rather than blocking forever.
            let need_spot = need_spot
                && !remainder_is_dust(
                    progress.spot_usd,
                    spot_size,
                    hq.min_order,
                    &mut self.state.tranche_progress,
                    &sym,
                    true,
                );
            let need_perp = need_perp
                && !remainder_is_dust(
                    progress.perp_usd,
                    perp_size,
                    lq.min_order,
                    &mut self.state.tranche_progress,
                    &sym,
                    false,
                );
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
            if !need_spot && !need_perp {
                self.persist();
                log::info!(
                    "[ENTRY] {sym}: tranche {n_th} remainder below min_order, settled as filled"
                );
                continue;
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
                        cost_basis_unknown: false,
                        perp_cost_basis_unknown: false,
                        stop_unconfirmed_id: None,
                        stop_unlisted_since: None,
                        stop_presumed_gone: false,
                        stop_absence_confirmed: false,
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
                            "{sym}: spot {why} tranche {n_th} failed (legs already filled in this attempt are recorded and will be skipped on retry)"
                        )
                    })?;
                fs = filled_spot.to_f64().unwrap_or(0.0);
                leg.spot_size += fs;
                leg.spot_cost_usd += fs * hq.price;
            }
            self.state.legs.insert(sym.clone(), leg.clone());
            // Completion is measured against what was actually ORDERED, not
            // the tranche's target: `size_from_notional` rounds down to the
            // venue's size decimals, so a $99 target can only ever be a
            // $98.19 order on a 5-decimal market. Comparing the fill with
            // the target read that rounding as a short fill and halted the
            // ladder (live, 2026-09-23). The lost fraction is a fraction of
            // one size tick and is settled as dust by `remainder_is_dust`.
            let spot_ordered = spot_size.to_f64().unwrap_or(0.0) * hq.price;
            let spot_done = {
                let pr = self.state.tranche_progress.entry(sym.clone()).or_default();
                pr.spot_usd += fs * hq.price;
                pr.spot_done = !need_spot || fill_complete(fs * hq.price, spot_ordered);
                pr.spot_done
            };
            // Real exposure now exists: flip to On (only once) so a crash or
            // a later error in this loop never leaves a filled position
            // recorded under Off/Exited, where a stray re-ARM would double it.
            if self.state.mode != Mode::On && (leg.spot_size > 0.0 || leg.perp_size > 0.0) {
                self.open_book();
            }
            self.persist();
            self.clear_pending_after_record();
            if !spot_done {
                // Recorded, protected by the stop below on the next pass,
                // but NOT completed: a thin-book partial must not pass as a
                // full tranche (the perp leg would size against it and the
                // book would end up leveraged asymmetrically). The retry
                // after RISK_ACK orders the remainder only.
                if let Err(e) = self.place_stop(&sym).await {
                    log::error!("[STOP] stop for the partial {market} entry failed, reconcile will retry: {e:?}");
                }
                bail!(
                    "{market}: spot {why} tranche {n_th} filled only {fs} (${:.2} of the ${spot_ordered:.2} ordered, target ${spot_notional:.0}); recorded, remainder retried on RISK_ACK",
                    fs * hq.price
                );
            }
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
            let perp_ordered = perp_size.to_f64().unwrap_or(0.0) * lq.price;
            let perp_done = {
                let pr = self.state.tranche_progress.entry(sym.clone()).or_default();
                pr.perp_usd += fp * lq.price;
                pr.perp_done = !need_perp || fill_complete(fp * lq.price, perp_ordered);
                pr.perp_done
            };
            self.persist();
            self.clear_pending_after_record();
            log::info!(
                "[ENTRY] {sym} {why} tranche {n_th}/{}: +spot {} @ {:.2} +perp {} @ {:.2}; book spot {} (${:.0}) perp {} (${:.0}); peak={:.2} exit_level={:.2}",
                self.state.tranches_done + self.state.tranches_remaining,
                fs, hq.price, fp, lq.price,
                leg.spot_size, leg.spot_cost_usd, leg.perp_size, leg.perp_cost_usd, leg.peak_close, leg.exit_level
            );
            if let Err(e) = self.place_stop(&sym).await {
                // Exposure is recorded, but the documented exchange-side
                // protection is missing: never complete the tranche
                // silently. `reconcile` re-places uncovered stops on its
                // cadence; the halt makes the gap visible meanwhile.
                self.halt(format!("{sym}: stop placement failed after entry: {e}"));
                bail!("{sym}: entry filled but the stop could not be placed: {e:?}");
            }
            if !perp_done {
                bail!(
                    "{sym}: perp {why} tranche {n_th} filled only {fp} (${:.2} of the ${perp_ordered:.2} ordered, target ${perp_notional:.0}); recorded and stop-covered, remainder retried on RISK_ACK",
                    fp * lq.price
                );
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

    /// First exposure of a cycle: the book is On from here.
    fn open_book(&mut self) {
        self.state.mode = Mode::On;
        self.state.book_dry_run = Some(self.cfg.dry_run);
        self.state.armed_at = Some(now_secs());
        self.state.exited_at = None;
        self.state.exit_reason = None;
        self.state.cycles += 1;
        // Funding is reported per holding period: a fresh ARM starts a
        // fresh total, unknown until the first read.
        self.state.cum_funding_usdc = None;
        self.state.cum_funding_as_of = None;
        self.state.funding_seen.clear();
    }

    /// Size to close on exit: the book's size, or the venue's holding when
    /// that is larger beyond the reconcile tolerance (never smaller: a
    /// venue read that is short is treated as unreadable for this purpose
    /// and the book's size is sold). DRY_RUN: the book's size.
    async fn exit_target(&self, book: f64, spot_market: Option<&str>, symbol: &str) -> Result<f64> {
        if self.cfg.dry_run {
            return Ok(book);
        }
        let holding = match spot_market {
            Some(m) => self.hl_spot_holding(&spot_base(m)).await,
            None => self.lt_perp_holding(symbol).await,
        };
        let what = spot_market.unwrap_or(symbol);
        match holding {
            Ok(h) if h < 0.0 => bail!(
                "{what}: venue holds a SHORT ({h}) where the book has a long of {book} — not flat, not ours to sell; close it by hand"
            ),
            Ok(h) if within_tolerance(book, h, self.cfg.reconcile_tolerance_pct) => Ok(book),
            Ok(h) => {
                // Larger: an order the state never recorded — sell it all.
                // Smaller: the exchange stop (or a manual close) already
                // took part of it — selling the stale book size would be
                // rejected/partial and halt forever instead of converging.
                log::error!(
                    "[EXIT] {what}: venue holds {h} but the book says {book} — closing the venue amount"
                );
                Ok(h)
            }
            Err(e) => {
                log::warn!("[EXIT] {what}: holding unreadable, selling the book's size: {e:?}");
                Ok(book)
            }
        }
    }

    /// Refresh the API wallet's expiry from Hyperliquid: the configured
    /// account's master, then that master's approved agents, matched on
    /// the signer address the connector reports. Read-only and best
    /// effort — a failure leaves the last known value in place.
    /// Identifies the approval being watched: a change means the stored
    /// expiry is about a different wallet.
    fn agent_key(&self) -> String {
        // The endpoint is part of the identity: the same account and
        // wallet name on testnet is a different approval, and switching
        // networks must not leave the daily timer serving the other one's
        // date.
        format!(
            "{}|{}|{}",
            self.cfg.hl_account_address.to_ascii_lowercase(),
            self.cfg.hl_agent_address.to_ascii_lowercase(),
            self.cfg.hl_info_url.to_ascii_lowercase()
        )
    }

    async fn poll_hl_agent(&mut self) {
        if self.cfg.dry_run || self.cfg.hl_account_address.is_empty() {
            return;
        }
        if self.cfg.hl_agent_address.is_empty() {
            // The watch is off: without the signer's address an approval
            // cannot be attributed to this process, so there is nothing
            // the two requests could establish. Record the observation so
            // this says so once a day rather than on every tick.
            log::error!(
                "[AGENT] BULL_HOLDER_HL_AGENT_ADDRESS is not set — no expiry is published. Set it to the API wallet whose key HYPERLIQUID_SIGNER_PRIVATE_KEY holds."
            );
            self.state.hl_agent_as_of = Some(now_secs());
            self.state.hl_agent_key = Some(self.agent_key());
            self.state.hl_agent_name = None;
            self.state.hl_agent_valid_until = None;
            self.persist();
            return;
        }
        // A changed identity makes the stored values describe a different
        // wallet. Drop them before the read rather than after it succeeds:
        // a refresh that fails would otherwise keep publishing the old
        // wallet's expiry as this one's (Codex review).
        if self.state.hl_agent_key.as_deref() != Some(self.agent_key().as_str())
            && self.state.hl_agent_key.is_some()
        {
            log::warn!("[AGENT] the watched API wallet changed; dropping the previous expiry");
            self.state.hl_agent_name = None;
            self.state.hl_agent_valid_until = None;
            self.state.hl_agent_key = Some(self.agent_key());
            self.state.hl_agent_as_of = None;
            self.persist();
        }
        let post = |body: serde_json::Value| {
            let http = self.http.clone();
            let url = self.cfg.hl_info_url.clone();
            async move {
                // A 429 or a 5xx still carries a JSON body — an error
                // object, not an agent list. Letting it through would read
                // as "no such approval" and clear a live expiry on a
                // transient failure (Codex review).
                http.post(&url)
                    .json(&body)
                    .send()
                    .await
                    .ok()?
                    .error_for_status()
                    .ok()?
                    .json::<serde_json::Value>()
                    .await
                    .ok()
            }
        };
        let role = match post(serde_json::json!({
            "type": "userRole", "user": self.cfg.hl_account_address
        }))
        .await
        {
            Some(v) => v,
            None => {
                log::warn!("[AGENT] userRole read failed; expiry not refreshed");
                return;
            }
        };
        // A sub-account's agents are approved on its master; a master
        // answers for itself. Anything else is an unrecognised response,
        // not permission to treat this account as its own master: that
        // would query the wrong owner, find no agents, and erase a valid
        // approval as "conclusively absent" (Codex review).
        let Some(owner) = agent_owner(&role, &self.cfg.hl_account_address) else {
            log::warn!(
                "[AGENT] userRole did not identify {} as a master or name one; expiry not refreshed",
                self.cfg.hl_account_address
            );
            return;
        };
        let Some(agents) = post(serde_json::json!({"type": "extraAgents", "user": owner})).await
        else {
            log::warn!("[AGENT] extraAgents read failed; expiry not refreshed");
            return;
        };
        let (name, valid_until) = match agent_expiry(&agents, &self.cfg.hl_agent_address) {
            AgentLookup::Found(name, until) => (name, until),
            AgentLookup::Unreadable => {
                log::warn!(
                    "[AGENT] {owner}'s approved agents could not be read — the last known expiry stands"
                );
                return;
            }
            AgentLookup::Absent => {
                log::error!(
                    "[AGENT] {owner} holds no approval for this bot's API wallet — the spot leg cannot sign"
                );
                // Conclusive: revoked, renamed, or never there. Keeping the
                // last known expiry would advertise an authorisation that
                // no longer exists. The observation time is recorded, so
                // the card reads "checked, unknown" rather than a stale
                // future date.
                self.state.hl_agent_name = None;
                self.state.hl_agent_valid_until = None;
                self.state.hl_agent_as_of = Some(now_secs());
                self.state.hl_agent_key = Some(self.agent_key());
                self.persist();
                return;
            }
        };
        let days = (valid_until - now_secs() as i64) as f64 / 86_400.0;
        if days < AGENT_EXPIRY_WARN_DAYS {
            log::error!("[AGENT] API wallet {name} expires in {days:.1} d — approve a new one on the master wallet");
        } else {
            log::info!("[AGENT] API wallet {name} expires in {days:.0} d");
        }
        self.state.hl_agent_name = Some(name);
        self.state.hl_agent_valid_until = Some(valid_until);
        self.state.hl_agent_as_of = Some(now_secs());
        self.state.hl_agent_key = Some(self.agent_key());
        self.persist();
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
            if !self.state.legs.contains_key(&sym) {
                continue;
            }
            // Cancel first, then snapshot: `cancel_stop` clears the stop
            // fields in state, and the clone below is written back at the
            // end of this iteration — cloning before the cancel would
            // resurrect the cancelled stop's id/level/size in state.
            self.cancel_stop(&sym).await;
            let Some(mut leg) = self.state.legs.get(&sym).cloned() else {
                continue;
            };
            if leg.stop_order_id.is_some() {
                // The cancel did not go through (see `cancel_stop`): the
                // trigger may still rest at the venue. Close the legs
                // anyway (that is the exit's job), but the book must not
                // reach Exited with it: a later ARM clears the legs and the
                // orphaned reduce-only trigger would act on the new
                // position. It stays tracked and halted until a retried
                // DISARM (after RISK_ACK) confirms the cancel.
                log::error!(
                    "[EXIT] {sym}: stop {:?} could not be cancelled — legs are closed below but the book stays On/halted until the cancel is confirmed",
                    leg.stop_order_id
                );
                any_leg_still_open = true;
                self.halt(format!(
                    "{sym}: stop cancel unconfirmed on exit; retry DISARM after RISK_ACK"
                ));
            }
            let market = self.cfg.hl_spot_market[&sym].clone();
            let orig_spot_size = leg.spot_size;
            let orig_perp_size = leg.perp_size;
            let mut spot_pnl: Option<f64> = None;
            let mut spot_px_source = "n/a";
            let mut perp_pnl: Option<f64> = None;
            let mut perp_px_source = "n/a";

            let mut spot_closed = 0.0;
            let mut perp_closed = 0.0;
            if orig_spot_size > 0.0 {
                let (px, src) = self
                    .exit_reference_price(&self.hl, &market, leg.last_close)
                    .await;
                // Sell what the venue actually holds when that is more than
                // the book (an order the state never recorded): an exit
                // must not leave exposure behind because the ledger is short.
                let (target, sell) =
                    match self.exit_target(orig_spot_size, Some(&market), &sym).await {
                        Err(e) => (orig_spot_size, Err(e)),
                        Ok(t) if t <= 0.0 => {
                            log::warn!("[EXIT] {market}: nothing left at the venue, no sell sent");
                            (t, Ok(Decimal::ZERO))
                        }
                        Ok(t) => {
                            let size = Decimal::from_f64(t).unwrap_or(Decimal::ZERO);
                            (t, self.hl_spot_ioc(&market, size, OrderSide::Short).await)
                        }
                    };
                match sell {
                    Ok(f) => {
                        let sold = f.to_f64().unwrap_or(0.0);
                        spot_closed = sold;
                        let remaining = remaining_after_close(target, sold, px);
                        log::info!("[EXIT] {market} spot sold {f} (remaining {remaining})");
                        leg.spot_size = remaining;
                        spot_px_source = src;
                        // The venue held something other than the book (a
                        // stop or manual close took part of it, or an
                        // unrecorded fill added to it): that part's price /
                        // cost is unknown here, so the leg's PnL is unknown
                        // rather than a number that ignores it.
                        let venue_differs = (target - orig_spot_size).abs() > 1e-12;
                        if venue_differs && remaining > 0.0 {
                            leg.cost_basis_unknown = true;
                        }
                        spot_pnl = if venue_differs || leg.cost_basis_unknown {
                            None
                        } else {
                            px.map(|p| {
                                p * sold - leg.spot_cost_usd * (sold / orig_spot_size).min(1.0)
                            })
                        };
                        if px.is_none() {
                            log::warn!(
                                "[EXIT] {market} closed but no trustworthy price available — PnL for this leg is unknown, not zero"
                            );
                        }
                        if remaining > 0.0 {
                            log::error!(
                                "[EXIT] {market} spot only PARTIALLY closed ({sold} of {target}), leg remains OPEN in state"
                            );
                            self.halt(format!(
                                "spot exit partial for {market}: {remaining} still held"
                            ));
                            any_leg_still_open = true;
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
                let (target, close) = match self.exit_target(orig_perp_size, None, &sym).await {
                    Err(e) => (orig_perp_size, Err(e)),
                    Ok(t) if t <= 0.0 => {
                        log::warn!("[EXIT] {sym}: no perp position left at the venue (stop already closed it?), no order sent");
                        (t, Ok(Decimal::ZERO))
                    }
                    Ok(t) => {
                        let size = Decimal::from_f64(t).unwrap_or(Decimal::ZERO);
                        (
                            t,
                            self.lt_perp_taker(&sym, size, OrderSide::Short, true).await,
                        )
                    }
                };
                match close {
                    Ok(f) => {
                        let closed = f.to_f64().unwrap_or(0.0);
                        perp_closed = closed;
                        let remaining = remaining_after_close(target, closed, px);
                        log::info!("[EXIT] {sym} perp closed {f} (remaining {remaining})");
                        leg.perp_size = remaining;
                        perp_px_source = src;
                        let venue_differs = (target - orig_perp_size).abs() > 1e-12;
                        if venue_differs && remaining > 0.0 {
                            leg.perp_cost_basis_unknown = true;
                        }
                        perp_pnl = if venue_differs || leg.perp_cost_basis_unknown {
                            None
                        } else {
                            px.map(|p| {
                                p * closed - leg.perp_cost_usd * (closed / orig_perp_size).min(1.0)
                            })
                        };
                        if px.is_none() {
                            log::warn!(
                                "[EXIT] {sym} perp closed but no trustworthy price available — PnL for this leg is unknown, not zero"
                            );
                        }
                        if remaining > 0.0 {
                            log::error!(
                                "[EXIT] {sym} perp only PARTIALLY closed ({closed} of {target}), leg remains OPEN in state"
                            );
                            self.halt(format!(
                                "perp exit partial for {sym}: {remaining} still held"
                            ));
                            any_leg_still_open = true;
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
            // The record carries what was CONFIRMED closed and the cost
            // basis of that part; a partial close is then visible as such.
            let spot_cost_closed = if orig_spot_size > 0.0 {
                leg.spot_cost_usd * (spot_closed / orig_spot_size).min(1.0)
            } else {
                0.0
            };
            let perp_cost_closed = if orig_perp_size > 0.0 {
                leg.perp_cost_usd * (perp_closed / orig_perp_size).min(1.0)
            } else {
                0.0
            };
            let rec = serde_json::json!({
                "ts": now_secs(), "symbol": sym, "reason": reason,
                "spot_size_closed": spot_closed, "spot_size_before": orig_spot_size,
                "spot_cost_usd": spot_cost_closed,
                "spot_pnl_usd": spot_pnl, "spot_px_source": spot_px_source,
                "perp_size_closed": perp_closed, "perp_size_before": orig_perp_size,
                "perp_cost_usd": perp_cost_closed,
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
            } else if orig_spot_size > 0.0 {
                leg.spot_cost_usd *= leg.spot_size / orig_spot_size;
            }
            if leg.perp_size <= 0.0 {
                leg.perp_cost_usd = 0.0;
            } else if orig_perp_size > 0.0 {
                leg.perp_cost_usd *= leg.perp_size / orig_perp_size;
            }
            self.state.legs.insert(sym.clone(), leg.clone());
            self.persist();
            self.clear_pending_after_record();
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
        // Every leg is closed before any of this: settling an
        // acknowledged-but-unconfirmed stop polls the venue for up to ~2
        // minutes per symbol, and nothing may hold a drawdown exit open
        // for that long — least of all the symbols not yet reached.
        // Unsettled ones still keep the book On/halted, which is what
        // stops a later ARM inheriting an orphaned reduce-only trigger.
        for sym in self.cfg.symbols.clone() {
            if self
                .state
                .legs
                .get(&sym)
                .and_then(|l| l.stop_unconfirmed_id.as_ref())
                .is_none()
            {
                continue;
            }
            if !self.drop_unconfirmed_stop(&sym).await {
                log::error!(
                    "[EXIT] {sym}: a stop with unknown state ({:?}) could not be settled — the legs are closed, but the book stays On/halted until it is",
                    self.state
                        .legs
                        .get(&sym)
                        .and_then(|l| l.stop_unconfirmed_id.clone())
                );
                any_leg_still_open = true;
                self.halt(format!(
                    "{sym}: unconfirmed stop still tracked after the exit; retry DISARM after RISK_ACK"
                ));
            }
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

    /// Compare the book with the venues for EVERY configured symbol — a
    /// symbol without a leg must be flat at both venues (dust below
    /// `RECONCILE_DUST_USD` aside), or an order the state never recorded is
    /// sitting there. Halts on the first divergence. Returns whether the
    /// comparison could be made and passed: `false` on a halt AND on a read
    /// failure, so callers that gate an order on it never proceed on "could
    /// not check".
    async fn reconcile(&mut self) -> bool {
        self.margin_monitor().await;
        if self.cfg.dry_run {
            return true; // nothing real to compare against
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
        let mut complete = positions.is_some() && balance.is_some();
        for sym in self.cfg.symbols.clone() {
            let leg = self.state.legs.get(&sym).cloned().unwrap_or_default();
            // Perp leg: Lighter positions.
            if let Some(pos) = &positions {
                let actual = pos
                    .iter()
                    .filter(|p| p.symbol.eq_ignore_ascii_case(&sym))
                    .map(|p| p.size.to_f64().unwrap_or(0.0) * if p.sign < 0 { -1.0 } else { 1.0 })
                    .sum::<f64>();
                match self
                    .leg_matches(&self.lt.clone(), &sym, leg.perp_size, actual)
                    .await
                {
                    Some(true) => {}
                    Some(false) => {
                        self.halt(format!(
                            "{sym} perp mismatch: expected {:.6} actual {actual:.6}",
                            leg.perp_size
                        ));
                        return false;
                    }
                    None => complete = false,
                }
            }
            // Spot leg: Hyperliquid spot balances (base token of the market).
            let market = self.cfg.hl_spot_market[&sym].clone();
            let base = spot_base(&market);
            if let Some(b) = &balance {
                let actual = b
                    .spot_assets
                    .iter()
                    .filter(|a| a.symbol.eq_ignore_ascii_case(&base))
                    .map(|a| a.balance.to_f64().unwrap_or(0.0))
                    .sum::<f64>();
                match self
                    .leg_matches(&self.hl.clone(), &market, leg.spot_size, actual)
                    .await
                {
                    Some(true) => {}
                    Some(false) => {
                        self.halt(format!(
                            "{market} spot mismatch: expected {:.6} actual {actual:.6}",
                            leg.spot_size
                        ));
                        return false;
                    }
                    None => complete = false,
                }
            }
        }
        if complete {
            log::debug!("[RECONCILE] ok");
        } else {
            log::warn!("[RECONCILE] incomplete: a venue read failed, the book is unverified");
        }
        self.ensure_stops().await;
        complete
    }

    /// Every perp leg with size must have a resting stop covering it; a
    /// placement that failed at entry (or a size the stop no longer covers)
    /// is retried here on the reconcile cadence, in every mode but under
    /// KILL_SWITCH (which blocks stop re-placement by design).
    async fn ensure_stops(&mut self) {
        if self.sentinels.kill_switch_engaged() {
            self.clear_all_unlisted_since();
            return;
        }
        for (sym, leg) in self.state.legs.clone() {
            if leg.perp_size <= 0.0 {
                continue;
            }
            // Only against a position the venue confirms right now: state
            // may be stale after a restart, and a stop sized from it would
            // be oversized or orphaned. A read failure or a divergence
            // skips (reconcile halts on the divergence).
            let venue_size = if self.cfg.dry_run {
                Ok(leg.perp_size) // no venue book to compare against
            } else {
                self.lt_perp_holding(&sym).await
            };
            match venue_size {
                Ok(actual)
                    if within_tolerance(
                        leg.perp_size,
                        actual,
                        self.cfg.reconcile_tolerance_pct,
                    ) => {}
                Ok(actual) => {
                    log::error!(
                        "[STOP] {sym}: venue perp {actual} differs from the book {} — not placing a stop from stale state",
                        leg.perp_size
                    );
                    continue;
                }
                Err(e) => {
                    log::warn!("[STOP] {sym}: position unreadable, stop check deferred: {e:?}");
                    continue;
                }
            }
            // The recorded stop must still be RESTING at the venue — a
            // stop can be dropped after its `sendTx` was acknowledged, or
            // cancelled outside the bot; in both cases state's id is a
            // ghost and nothing else would ever re-place it.
            // The venue's own order count for this market, not the
            // connector's WebSocket cache: that cache is empty for a moment
            // after every restart, which read a live stop as missing and
            // had the bot cancel and re-place a perfectly good one on each
            // start (live, 2026-09-23). The count cannot say WHICH order
            // rests, but this account is dedicated and the bot rests
            // nothing but this stop.
            // `Some((rests, confirmed))`. `confirmed` is true only when
            // the VENUE named the absence — nothing rests for this market,
            // or it listed this id as cancelled. The unlisted-for-the-
            // whole-grace fallback is an inference and leaves it false:
            // enough to re-place (that path cancels first), not enough to
            // throw the id away on a cancel that merely errored.
            let rests = match (&leg.stop_order_id, self.cfg.dry_run) {
                (None, _) => Some((false, false)),
                (Some(_), true) => Some((true, false)), // no venue book in DRY_RUN
                (Some(id), false) => {
                    match self.lighter_resting_orders(&sym).await {
                        // Nothing rests for this market: conclusive, whatever
                        // the connector's cache thinks.
                        Some(0) => {
                            if self.end_unlisted_observation(&sym) {
                                self.persist();
                            }
                            Some((false, true))
                        }
                        // Something rests, but the count cannot say what. The
                        // connector's order view names ids, so require it to
                        // carry this one before calling the leg covered —
                        // otherwise an order placed outside the bot would
                        // vouch for a stop that is no longer there. Right
                        // after a restart that view is briefly empty, so
                        // "unknown" defers instead of cancelling a live stop.
                        Some(_) => match self.lt.get_open_orders(&sym).await {
                            Ok(resp) if resp.orders.iter().any(|o| &o.order_id == id) => {
                                // Persist the reset: a run of misses that
                                // only cleared in memory would be reloaded
                                // after a restart, and the empty cache
                                // that restart produces could then supply
                                // the final miss and cancel a live stop.
                                if self.stop_seen_listed(&sym) {
                                    self.persist();
                                }
                                Some((true, false))
                            }
                            // The id is not listed. That is absence only
                            // if the list is from the CURRENT connection:
                            // the connector clears its positions-ready
                            // flag on every reconnect and fails that read
                            // until the account snapshot arrives
                            // (bot-strategy#911), so a successful
                            // `get_positions` is the readiness proof.
                            // Treating the two alike would defer forever
                            // once an operator cancelled the stop while
                            // another order rested on the market.
                            Ok(_) => {
                                // The id is not listed. Positions being
                                // readable proves nothing here — this
                                // iteration already read them — and the
                                // connector exposes no readiness flag for
                                // the ORDER snapshot. Two signals settle
                                // it instead: the venue naming the id as
                                // cancelled, or the same answer repeating.
                                // A cache catching up after a restart
                                // agrees within a check or two; an order
                                // that is really gone never comes back.
                                let cancelled = matches!(
                                    self.lt.get_canceled_orders(&sym).await,
                                    Ok(c) if c.orders.iter().any(|o| &o.order_id == id)
                                );
                                // An absence already established is not
                                // re-timed (see `unlisted_verdict`), so
                                // the window is only started, and only
                                // persisted, while there is still a
                                // question to answer.
                                let elapsed = if leg.stop_presumed_gone {
                                    None
                                } else {
                                    let now = now_secs();
                                    let since = self
                                        .state
                                        .legs
                                        .get_mut(&sym)
                                        .map(|l| *l.stop_unlisted_since.get_or_insert(now))
                                        .unwrap_or(now);
                                    self.persist();
                                    Some(now.saturating_sub(since))
                                };
                                let verdict = unlisted_verdict(cancelled, elapsed);
                                match (verdict, elapsed) {
                                    (Some(_), None) => log::error!(
                                        "[STOP] {sym}: {id} is still not listed and already presumed gone — retrying the replacement"
                                    ),
                                    (Some(_), Some(e)) => log::error!(
                                        "[STOP] {sym}: {id} is gone from the venue's order list ({}) while another order rests — re-placing",
                                        if cancelled {
                                            "reported cancelled".to_string()
                                        } else {
                                            format!("unlisted for {e}s")
                                        }
                                    ),
                                    (None, Some(e)) => log::warn!(
                                        "[STOP] {sym}: {id} not listed yet ({e}s of {STOP_UNLISTED_GRACE_SECS}s) — deferring, the cache may still be catching up"
                                    ),
                                    (None, None) => unreachable!(
                                        "an established absence is never deferred"
                                    ),
                                }
                                verdict
                            }
                            Err(e) => {
                                // The list could not be read at all: the
                                // connection is not the one the run was
                                // observed on, so the run ends here.
                                if self.end_unlisted_observation(&sym) {
                                    self.persist();
                                }
                                log::warn!("[STOP] {sym}: order list unreadable, stop check deferred: {e:?}");
                                None
                            }
                        },
                        // The venue's own count could not be read, so
                        // neither answer was reached: the observation is
                        // broken, like any other interruption.
                        None => {
                            if self.end_unlisted_observation(&sym) {
                                self.persist();
                            }
                            None
                        }
                    }
                }
            };
            let Some((rests, absence_confirmed)) = rests else {
                continue;
            };
            if !rests && leg.stop_order_id.is_some() {
                log::error!(
                    "[STOP] {sym}: recorded stop {:?} is NOT resting — it stops counting as cover and a replacement follows",
                    leg.stop_order_id
                );
                // No cancel from here. `place_stop` checks the quote and
                // the market's leverage BEFORE it touches an existing
                // stop, precisely so a replacement that cannot be placed
                // never costs the protection already there — and this
                // verdict, however well evidenced, is still an inference.
                // The id stays for that path to cancel in the right
                // order; `stop_presumed_gone` tells it a failing cancel is
                // expected rather than a reason to defer.
                self.mark_stop_presumed_gone(&sym, absence_confirmed);
            }
            // Covered in size AND resting at the level the refreshed peak
            // calls for: a move whose cancel failed leaves the old, lower
            // trigger tracked, and only a retry here (place_stop cancels
            // it again first) brings it up.
            let want = level_below_peak(leg.lighter_peak, self.cfg.stop_dd_pct);
            if rests
                && stop_covers(leg.stop_order_id.is_some(), leg.stop_size, leg.perp_size)
                && stop_is_current(
                    leg.stop_level,
                    leg.stop_size,
                    leg.stop_order_id.is_some(),
                    want,
                    leg.perp_size,
                )
            {
                continue;
            }
            log::warn!(
                "[STOP] {sym}: perp {} is not stop-covered at the current level (order {:?} size {:?} level {:?}, want {want:.2}) — re-placing",
                leg.perp_size,
                leg.stop_order_id,
                leg.stop_size,
                leg.stop_level
            );
            if let Err(e) = self.place_stop(&sym).await {
                log::error!("[STOP] {sym}: re-placement failed, will retry next reconcile: {e:?}");
            }
        }
    }

    /// Does the venue holding match the book for one leg? A leg the book
    /// does not hold must be flat at the venue, where "flat" tolerates dust
    /// worth less than `RECONCILE_DUST_USD` (size rounding, base fees) —
    /// which needs a price; `None` if that price could not be read.
    async fn leg_matches(
        &self,
        venue: &Arc<dyn DexConnector + Send + Sync>,
        symbol: &str,
        expected: f64,
        actual: f64,
    ) -> Option<bool> {
        if expected > 0.0 {
            return Some(within_tolerance(
                expected,
                actual,
                self.cfg.reconcile_tolerance_pct,
            ));
        }
        if actual.abs() < 1e-12 {
            return Some(true);
        }
        match self.quote(venue, symbol).await {
            Ok(q) => Some(actual.abs() * q.price < RECONCILE_DUST_USD),
            Err(e) => {
                log::warn!("[RECONCILE] {symbol}: unexpected holding {actual} and no price to size it: {e:?}");
                None
            }
        }
    }

    /// Gate before any order that changes the book: an unsettled order in
    /// state means exposure may exist that the book does not show, so the
    /// venues are compared first and the marker is cleared only if they
    /// agree with the book. `Err` = do not order.
    async fn reconcile_before_orders(&mut self, what: &str) -> Result<()> {
        let Some(pending) = self.state.pending_order.clone() else {
            return Ok(());
        };
        log::warn!(
            "[FILL] {what}: an earlier order is unsettled ({pending:?}); checking its own baseline and the whole book before any new order"
        );
        // The order's OWN baseline first: the general reconcile tolerates
        // 2 % / dust, which is exactly the size of a late partial fill —
        // that would clear the marker and let the remainder be re-sent.
        let now_holding = match pending.venue.as_str() {
            "hyperliquid" => self.hl_spot_holding(&spot_base(&pending.symbol)).await,
            _ => self.lt_perp_holding(&pending.symbol).await,
        }
        .with_context(|| format!("{what} refused: the unsettled order's holding is unreadable"))?;
        let dir = if pending.side == OrderSide::Short.to_string() {
            -1.0
        } else {
            1.0
        };
        let moved = (now_holding - pending.holding_before) * dir;
        if moved.abs() > PENDING_RESOLVE_EPS {
            self.halt(format!(
                "unsettled {} {} {} on {} DID move the holding by {moved:+} since it was sent (baseline {}, now {now_holding}); \
                 record it in state.json by hand, then RISK_ACK",
                pending.side, pending.requested, pending.symbol, pending.venue, pending.holding_before
            ));
            bail!("{what} refused: the unsettled order filled (see [HALT])");
        }
        if !self.reconcile().await {
            bail!(
                "{what} refused: an earlier order is unsettled ({pending:?}) and the book could not be verified against the venues; \
                 fix state.json by hand if it diverges, then RISK_ACK"
            );
        }
        log::warn!(
            "[FILL] unsettled order resolved: its holding is unchanged since it was sent and the venues agree with the book"
        );
        self.state.pending_order = None;
        self.persist();
        Ok(())
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
        if kill {
            // Stop checking is off while KILL is engaged, so no elapsed
            // time accrues against a stop. Doing this on the tick rather
            // than inside `ensure_stops` covers a KILL that comes and goes
            // between two reconciles, which that function never sees.
            self.clear_all_unlisted_since();
        }
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
                    if let Some(p) = &self.state.pending_order {
                        log::error!(
                            "[DISARM] the book records nothing held, but an order is unsettled ({p:?}); \
                             the venues are compared on the reconcile cadence — check them by hand before assuming flat"
                        );
                    }
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
            // A halt blocks orders, not observation: perp collateral keeps
            // eroding while the operator investigates, so keep the read-only
            // collateral guard running on the reconcile cadence. Without this
            // the last [MARGIN] snapshot would freeze until RISK_ACK.
            if self.state.mode == Mode::On
                && now.saturating_sub(self.last_margin_check) >= self.cfg.reconcile_every_secs
            {
                self.margin_monitor().await;
                // Protection is not an "action" a halt should block: a
                // partial exit (stop already cancelled) or a failed stop
                // placement halts with live perp size uncovered — re-cover
                // it while the operator investigates.
                self.ensure_stops().await;
            }
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
                // Precheck against the whole tranche even when resuming a
                // partial one (slightly over-requires; never under).
                let adding = self.state.tranche_perp_usd * self.cfg.symbols.len() as f64;
                match self.margin_precheck(adding).await {
                    Ok(None) => {
                        if let Err(e) = self.buy_tranche("scheduled").await {
                            log::error!("[ENTRY] tranche failed: {e:?}");
                            self.halt(format!("tranche failed: {e}"));
                        }
                    }
                    Ok(Some(short)) => {
                        // Not a halt: the exit rule must keep running. The
                        // ladder is pushed out a day, like a day the bot is
                        // down; it retries at the next daily slot.
                        log::error!(
                            "[MARGIN] scheduled tranche deferred to the next UTC day: {short}"
                        );
                        self.state.last_tranche_date = Some(utc_date(now));
                        self.persist();
                    }
                    Err(e) => {
                        // Transient read failure: retry next tick, do not
                        // burn the day.
                        log::warn!(
                            "[MARGIN] tranche postponed, cannot verify Lighter collateral: {e:?}"
                        );
                    }
                }
            }
            if now.saturating_sub(self.last_reconcile) >= self.cfg.reconcile_every_secs {
                self.last_reconcile = now;
                let _ = self.reconcile().await;
            }
        }
        // An unsettled order while Off/Exited (the first entry of a cycle
        // may have filled without a leg recorded): only a venue comparison
        // can surface that, so it runs outside the On-gated lifecycle above,
        // halted or not.
        if self.state.mode != Mode::On
            && self.state.pending_order.is_some()
            && now.saturating_sub(self.last_reconcile) >= self.cfg.reconcile_every_secs
        {
            self.last_reconcile = now;
            let _ = self.reconcile().await;
        }
        // Once a day: the API wallet's approval is a slow-moving fact the
        // operator has to act on, not a tick-rate signal. Runs while
        // halted too — an expiry does not wait for a RISK_ACK.
        if now.saturating_sub(self.state.hl_agent_as_of.unwrap_or(0)) >= AGENT_POLL_EVERY_SECS
            || self.state.hl_agent_key.as_deref() != Some(self.agent_key().as_str())
        {
            self.poll_hl_agent().await;
        }
        // Read-only, so it runs while halted and for a day after EXIT (the
        // last settlements land after the perp leg is closed); gated on ARM
        // because the total is per holding period.
        if funding_poll_due(
            self.state.armed_at,
            self.state.exited_at,
            now,
            self.last_funding_poll,
            self.cfg.reconcile_every_secs,
        ) {
            self.last_funding_poll = now;
            self.poll_funding(now).await;
        }
        self.write_status_if_due(now, kill);
    }

    /// Fold the venue's settled funding since ARM into `cum_funding_usdc`
    /// (bot-strategy#963). A failed read keeps the last known total and its
    /// `as_of`, so staleness is visible rather than papered over with a zero.
    async fn poll_funding(&mut self, now: u64) {
        let Some(armed_at) = self.state.armed_at else {
            return;
        };
        let since = match self.state.cum_funding_as_of {
            Some(as_of) => as_of.saturating_sub(FUNDING_REREAD_SECS).max(armed_at),
            None => armed_at,
        };
        match self.lt.get_funding_payments(since as i64).await {
            Ok(payments) => {
                let added =
                    apply_funding_payments(&mut self.state, &payments, &self.cfg.symbols, since);
                self.state.cum_funding_as_of = Some(now);
                if added != 0.0 {
                    log::info!(
                        "[FUNDING] +{added:.4} USD settled since {since} -> cum {:.4}",
                        self.state.cum_funding_usdc.unwrap_or(0.0)
                    );
                }
                self.persist();
            }
            Err(e) => {
                log::warn!(
                    "[FUNDING] history read failed (cum stays {:?}, as_of {:?}): {e:?}",
                    self.state.cum_funding_usdc,
                    self.state.cum_funding_as_of
                );
            }
        }
    }

    fn write_status_if_due(&mut self, now: u64, kill: bool) {
        if now.saturating_sub(self.last_status_write) < 30 {
            return;
        }
        self.last_status_write = now;
        let status = status_value(&self.cfg, &self.state, &self.last_margin, now, kill);
        if let Err(e) = persist_json(&self.cfg.status_path, &status) {
            log::warn!("[STATUS] write failed: {e:?}");
        }
    }
}

/// The monitoring projection, built apart from the write so it can be
/// asserted without an Engine and its two venue connectors.
fn status_value(
    cfg: &Config,
    state: &State,
    margin: &Option<MarginSnapshot>,
    now: u64,
    kill: bool,
) -> serde_json::Value {
    let legs: serde_json::Map<String, serde_json::Value> = state
        .legs
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::to_value(v).unwrap_or_default()))
        .collect();
    serde_json::json!({
        "ts": now,
        "bot": BOT,
        "instance_id": cfg.instance_id,
        "dry_run": cfg.dry_run,
        "mode": state.mode,
        "armed_at": state.armed_at,
        "exited_at": state.exited_at,
        "exit_reason": state.exit_reason,
        "halted": state.halted,
        "halt_reason": state.halt_reason,
        "kill_switch": kill,
        "realized_pnl_total_usd": state.realized_pnl_total_usd,
        "cycles": state.cycles,
        "tranches_done": state.tranches_done,
        "tranches_remaining": state.tranches_remaining,
        "tranche_spot_usd": state.tranche_spot_usd,
        "tranche_perp_usd": state.tranche_perp_usd,
        "last_tranche_date": state.last_tranche_date,
        "config_fp": cfg.fingerprint(),
        "margin": margin,
        // Settled funding on the perp legs since ARM (null = not read yet),
        // and its read time. Fees stay unreported: Lighter is 0-fee on this
        // account and the HL spot taker fee is a one-off the connector does
        // not surface per fill (bot-strategy#963).
        "cum_funding_usdc": state.cum_funding_usdc,
        "cum_funding_as_of": state.cum_funding_as_of,
        "cum_fees_usdc": serde_json::Value::Null,
        // The Hyperliquid API wallet's approval. The bot cannot renew it,
        // so the date is published to be watched (bot-strategy#1054).
        "hl_agent_name": state.hl_agent_name,
        "hl_agent_valid_until": state.hl_agent_valid_until,
        "hl_agent_as_of": state.hl_agent_as_of,
        // The configured book, which `legs` only describes once a tranche
        // has filled. A monitor checking that its buy & hold anchor covers
        // the whole book has nothing to check against before ARM
        // otherwise, and an anchor missing a leg is not a partial
        // benchmark but a different portfolio: the whole spot allocation
        // goes into the legs it does list (bot-strategy#963).
        "configured_symbols": cfg.symbols,
        "legs": legs,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = Config::from_env()?;
    log::info!(
        "[CONFIG] bot={BOT} instance={} dry_run={} symbols={} equity=${:.0} spot_frac={} perp_frac={} tranches={} exit_dd={}% stop_dd={}% stop_slip={}bps mmr={}% margin_min={}% hl_slip={}bps fp={}",
        cfg.instance_id, cfg.dry_run, cfg.symbols.join(","), cfg.equity_usd, cfg.spot_fraction, cfg.perp_fraction,
        cfg.entry_tranches, cfg.exit_dd_pct, cfg.stop_dd_pct, cfg.stop_slippage_bps, cfg.lighter_mmr_pct, cfg.perp_margin_min_pct,
        cfg.hl_taker_slippage_bps, cfg.fingerprint()
    );
    if !cfg.dry_run {
        log::warn!(
            "[CONFIG] LIVE: orders will be signed and sent (bot-strategy#895 small-live approval)"
        );
    }

    // Before any connector (and any signer) is built: an open book is only
    // operated in the mode that created it.
    let mut state: State = load_json(&cfg.state_path)?.unwrap_or_default();
    // The unlisted-stop tolerance counts misses within ONE connection.
    // Every start brings a fresh connector whose order cache is empty
    // until the venue's first snapshot, so the miss it produces is
    // expected — carrying counts across restarts would let three quick
    // restarts (or a crash loop) add up to "the stop is gone" and cancel
    // a live one, while carrying them in the other direction would let a
    // stale count plus this start's own miss do it immediately.
    for leg in state.legs.values_mut() {
        leg.stop_unlisted_since = None;
    }
    log::info!(
        "[STARTUP] mode={:?} legs={} halted={} book_dry_run={:?} realized_total=${:.2}",
        state.mode,
        state.legs.len(),
        state.halted,
        state.book_dry_run,
        state.realized_pnl_total_usd
    );
    check_book_mode(&state, cfg.dry_run)?;

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
        last_margin_check: 0,
        last_margin: None,
        last_funding_poll: 0,
        resolved_account_index: tokio::sync::Mutex::new(None),
        cfg,
    };
    // A live restart re-verifies the book against the venues before doing
    // anything else — in every mode: a flat book must be flat at the venues
    // too, or an order the state never recorded is sitting there.
    if !engine.cfg.dry_run {
        let mut verified = false;
        for attempt in 1..=STARTUP_RECONCILE_ATTEMPTS {
            if engine.reconcile().await {
                verified = true;
                break;
            }
            if engine.state.halted {
                break; // a divergence, not a read failure: the halt says it
            }
            log::warn!("[RECONCILE] startup check incomplete (attempt {attempt}), retrying");
            tokio::time::sleep(Duration::from_secs(STARTUP_RECONCILE_STEP_SECS)).await;
        }
        if !verified && !engine.state.halted {
            // Never let an ARM file or a due tranche run on an unverified
            // book: the operator clears this once the venues are readable.
            engine.halt("startup reconcile could not verify the book against the venues (venue reads failed); RISK_ACK once they are reachable".into());
        }
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
    use rust_decimal::Decimal;

    fn payment(symbol: &str, id: i64, ts: i64, usd: &str) -> FundingPayment {
        FundingPayment {
            symbol: symbol.into(),
            timestamp_secs: ts,
            amount_usdc: usd.parse::<Decimal>().unwrap(),
            payment_id: id,
        }
    }

    #[test]
    fn funding_total_counts_each_settlement_once_on_the_book_only() {
        let symbols = vec!["BTC".to_string(), "ETH".to_string()];
        let mut state = State::default();
        let since = 1_000_000u64;

        // Unknown until the first read; then the settled amounts, signed as
        // the venue netted them (bot-strategy#963).
        assert_eq!(state.cum_funding_usdc, None);
        let first = [
            payment("BTC", 11, 1_003_600, "-0.25"),
            payment("ETH", 12, 1_003_600, "-0.10"),
            payment("SOL", 13, 1_003_600, "-9.00"), // not a configured leg
            payment("BTC", 10, 999_999, "-5.00"),   // before ARM
        ];
        let added = apply_funding_payments(&mut state, &first, &symbols, since);
        assert!((added - -0.35).abs() < 1e-9, "added {added}");
        assert!((state.cum_funding_usdc.unwrap() - -0.35).abs() < 1e-9);

        // An overlapping re-read (newest first, ids already seen) adds only
        // the genuinely new settlement.
        let second = [
            payment("BTC", 21, 1_007_200, "0.05"),
            payment("BTC", 11, 1_003_600, "-0.25"),
            payment("ETH", 12, 1_003_600, "-0.10"),
        ];
        let added = apply_funding_payments(&mut state, &second, &symbols, since);
        assert!((added - 0.05).abs() < 1e-9, "added {added}");
        assert!((state.cum_funding_usdc.unwrap() - -0.30).abs() < 1e-9);
        assert_eq!(state.funding_seen.len(), 3);

        // Pruning follows the re-read window: ids that can no longer be
        // re-read are dropped, the total keeps them.
        let added = apply_funding_payments(&mut state, &[], &symbols, 1_005_000);
        assert_eq!(added, 0.0);
        assert_eq!(
            state.funding_seen.keys().copied().collect::<Vec<_>>(),
            vec![21]
        );
        assert!((state.cum_funding_usdc.unwrap() - -0.30).abs() < 1e-9);

        // An empty read after ARM is a known zero, not an unknown.
        let mut fresh = State::default();
        apply_funding_payments(&mut fresh, &[], &symbols, since);
        assert_eq!(fresh.cum_funding_usdc, Some(0.0));
    }

    #[test]
    fn funding_poll_runs_only_while_armed_and_for_a_day_after_exit() {
        let every = 600;
        // Never ARMed: nothing to attribute the total to.
        assert!(!funding_poll_due(None, None, 10_000, 0, every));
        // ARMed: on the cadence.
        assert!(funding_poll_due(Some(1_000), None, 10_000, 0, every));
        assert!(!funding_poll_due(Some(1_000), None, 10_000, 9_500, every));
        assert!(funding_poll_due(Some(1_000), None, 10_000, 9_400, every));
        // After EXIT the settlements already accrued still land, so keep
        // reading for a day; `armed_at` is never cleared, so without the
        // exit cutoff a flat book would be polled forever.
        let exited = 50_000;
        assert!(funding_poll_due(
            Some(1_000),
            Some(exited),
            exited + 3_600,
            0,
            every
        ));
        assert!(funding_poll_due(
            Some(1_000),
            Some(exited),
            exited + FUNDING_POLL_AFTER_EXIT_SECS,
            0,
            every
        ));
        assert!(!funding_poll_due(
            Some(1_000),
            Some(exited),
            exited + FUNDING_POLL_AFTER_EXIT_SECS + 1,
            0,
            every
        ));
    }

    #[test]
    fn status_reports_funding_as_null_until_read_then_as_the_total() {
        let cfg = test_config();
        let mut state = State::default();
        let before = status_value(&cfg, &state, &None, 1_788_810_248, false);
        assert!(before["cum_funding_usdc"].is_null());
        assert!(before["cum_funding_as_of"].is_null());
        assert!(before["cum_fees_usdc"].is_null());

        state.cum_funding_usdc = Some(-1.25);
        state.cum_funding_as_of = Some(1_788_810_000);
        let after = status_value(&cfg, &state, &None, 1_788_810_248, false);
        assert_eq!(after["cum_funding_usdc"], serde_json::json!(-1.25));
        assert_eq!(
            after["cum_funding_as_of"],
            serde_json::json!(1_788_810_000u64)
        );
        assert!(after["cum_fees_usdc"].is_null());
    }

    #[test]
    fn status_reports_the_configured_book_before_any_leg_exists() {
        let cfg = test_config();
        let mut state = State::default();

        // Pre-ARM: `legs` is empty, so a monitor validating that its buy &
        // hold anchor covers the whole book has nothing to check against
        // unless the configured universe is reported (bot-strategy#963).
        let before = status_value(&cfg, &state, &None, 1_788_810_248, false);
        assert_eq!(before["legs"].as_object().unwrap().len(), 0);
        assert_eq!(
            before["configured_symbols"],
            serde_json::json!(["BTC", "ETH"])
        );

        // It stays the configured universe once legs open, rather than
        // tracking whichever legs happen to be filled.
        state.legs.insert("BTC".into(), LegState::default());
        let after = status_value(&cfg, &state, &None, 1_788_810_248, false);
        assert_eq!(after["legs"].as_object().unwrap().len(), 1);
        assert_eq!(
            after["configured_symbols"],
            serde_json::json!(["BTC", "ETH"])
        );

        // The rest of the projection is unchanged by the extraction.
        assert_eq!(after["bot"], BOT);
        assert_eq!(after["ts"], 1_788_810_248u64);
        assert_eq!(after["dry_run"], true);
        assert_eq!(after["config_fp"], cfg.fingerprint());
    }

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
            ..Default::default()
        };
        assert_eq!(legs_pending(&spot_only, true), (false, true));
        let both = LegProgress {
            spot_done: true,
            perp_done: true,
            ..Default::default()
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
    fn dry_run_switch_only_goes_live_on_an_explicit_false() {
        assert_eq!(parse_dry_run(None).unwrap(), true);
        assert_eq!(parse_dry_run(Some(" true \n")).unwrap(), true);
        assert_eq!(parse_dry_run(Some("FALSE")).unwrap(), false);
        assert_eq!(parse_dry_run(Some("0")).unwrap(), false);
        // A typo or an empty export must not fail open into live.
        for bad in ["ture", "", "  ", "off", "flase", "2"] {
            assert!(
                parse_dry_run(Some(bad)).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn liquidation_point_matches_909_worked_example() {
        // #893's approved example: $1,000 behind $4,500 of perp at 1.2% MMR
        // liquidates at ~21.28% — before the 30% exit and the 35% stop.
        let d = liquidation_distance_pct(1_000.0, 4_500.0, 1.2).unwrap();
        assert!((d - 21.28).abs() < 0.02, "{d}");
        // Inverse round-trips.
        let e = equity_for_distance_usd(4_500.0, d, 1.2);
        assert!((e - 1_000.0).abs() < 1e-6, "{e}");
        // No exposure → no liquidation point; already-underwater → negative.
        assert!(liquidation_distance_pct(1_000.0, 0.0, 1.2).is_none());
        assert!(liquidation_distance_pct(10.0, 4_500.0, 1.2).unwrap() < 0.0);
        // Floor for the 35% stop, and the #909 30-day-buffer deposit.
        assert!((liquidation_floor_pct(35.0, 1.2) - 35.78).abs() < 1e-9);
        assert!((required_collateral_usd(4_500.0, 47.55) - 2_139.75).abs() < 1e-9);
    }

    #[test]
    fn runtime_guard_breach_and_clearance() {
        // Freshly armed at the #909 allocation: liquidation 46.9% out, stop
        // 35% out → fine.
        let liq = liquidation_distance_pct(2_139.75, 4_500.0, 1.2).unwrap();
        assert!((liq - 46.9).abs() < 0.1, "{liq}");
        assert!(!margin_breached(liq, 35.0, STOP_LIQ_CLEARANCE_PCT));
        // Funding erodes equity to the floor: liquidation lands inside the
        // stop (+ clearance) → breach.
        let eroded = equity_for_distance_usd(4_500.0, 35.5, 1.2);
        let liq2 = liquidation_distance_pct(eroded, 4_500.0, 1.2).unwrap();
        assert!(margin_breached(liq2, 35.0, STOP_LIQ_CLEARANCE_PCT));
        assert!(!margin_breached(liq2, 35.0, 0.0));
        // A drawdown to the 30% exit day at the #909 allocation still keeps
        // liquidation beyond the (now much closer) stop.
        let equity = 2_139.75 - 0.30 * 4_500.0;
        let notional = 0.70 * 4_500.0;
        let liq3 = liquidation_distance_pct(equity, notional, 1.2).unwrap();
        let stop = stop_distance_pct(70.0, 65.0);
        assert!(liq3 > stop + STOP_LIQ_CLEARANCE_PCT, "{liq3} vs {stop}");
        assert_eq!(stop_distance_pct(0.0, 65.0), 0.0);
    }

    #[test]
    fn perp_sizes_take_the_larger_of_book_and_venue() {
        let configured = vec!["BTC".to_string(), "ETH".to_string()];
        let recorded: BTreeMap<String, f64> = [("BTC".to_string(), 0.01), ("ETH".to_string(), 0.0)]
            .into_iter()
            .collect();
        // The venue holds twice the recorded BTC (the case reconcile halts
        // on) and a short ETH leg; both must be valued, ETH by magnitude.
        let venue = vec![
            ("BTC".to_string(), 0.02, 1),
            ("ETH".to_string(), 0.5, -1),
            ("SOL".to_string(), 3.0, 1),
        ];
        let (sizes, unsupported) = merge_perp_sizes(&configured, &recorded, &venue);
        assert_eq!(sizes.get("BTC"), Some(&0.02));
        // A short still consumes margin: counted, and flagged as unmodellable
        // because the long-only sell stop cannot cover an upward move.
        assert_eq!(sizes.get("ETH"), Some(&0.5));
        assert_eq!(
            unsupported,
            vec![
                "ETH (short)".to_string(),
                "SOL (not configured)".to_string()
            ]
        );
        // Recorded larger than the venue reports (a stale or partial venue
        // read): keep the recorded size, never undercount the notional.
        let (stale, none) = merge_perp_sizes(
            &configured,
            &[("BTC".to_string(), 0.03)].into_iter().collect(),
            &[("BTC".to_string(), 0.02, 1)],
        );
        assert_eq!(stale.get("BTC"), Some(&0.03));
        assert!(none.is_empty());
        // Nothing anywhere → nothing to price (no venue read needed).
        let (empty, clean) = merge_perp_sizes(&configured, &BTreeMap::new(), &[]);
        assert!(empty.is_empty() && clean.is_empty());
        // Only an unconfigured position open: nothing priceable, but the
        // caller must NOT read that as "no exposure".
        let (no_sizes, flagged) = merge_perp_sizes(
            &configured,
            &BTreeMap::new(),
            &[("SOL".to_string(), 3.0, 1)],
        );
        assert!(no_sizes.is_empty());
        assert_eq!(flagged, vec!["SOL (not configured)".to_string()]);
        // Notional uses only the symbols that carry size.
        let marks: BTreeMap<String, f64> =
            [("BTC".to_string(), 80_000.0), ("ETH".to_string(), 2_500.0)]
                .into_iter()
                .collect();
        assert!(
            (perp_notional_usd(&sizes, &marks) - (0.02 * 80_000.0 + 0.5 * 2_500.0)).abs() < 1e-9
        );
    }

    #[test]
    fn stop_only_counts_when_it_covers_the_open_size() {
        // Exact and over-sized stops cover the position.
        assert!(stop_covers(true, Some(0.02), 0.02));
        assert!(stop_covers(true, Some(0.03), 0.02));
        // A stop resting for LESS than what is open leaves the remainder
        // unprotected — the venue-position-larger-than-book case.
        assert!(!stop_covers(true, Some(0.01), 0.02));
        // No resting order, or none recorded: no protection either way.
        assert!(!stop_covers(false, Some(0.02), 0.02));
        assert!(!stop_covers(true, None, 0.02));
        // A state.json round-trip must not read as under-covered.
        let round_tripped: f64 =
            serde_json::from_str(&serde_json::to_string(&0.02).unwrap()).unwrap();
        assert!(stop_covers(true, Some(round_tripped), 0.02));
    }

    #[test]
    fn crossed_stop_offers_no_protection() {
        // Mark already below a still-resting stop: distance clamps to 0, so
        // the guard demands the liquidation point stay clear of the mark
        // itself instead of accepting it as "beyond" a negative distance.
        assert_eq!(stop_distance_pct(60.0, 65.0), 0.0);
        assert!(margin_breached(0.5, 0.0, STOP_LIQ_CLEARANCE_PCT));
        assert!(!margin_breached(2.0, 0.0, STOP_LIQ_CLEARANCE_PCT));
        // Without the clamp a −8.3% distance would have accepted this.
        assert!(margin_breached(
            0.5,
            stop_distance_pct(60.0, 65.0),
            STOP_LIQ_CLEARANCE_PCT
        ));
    }

    #[test]
    fn config_validation_rejects_margin_min_below_liquidation_floor() {
        let mut cfg = test_config();
        cfg.perp_margin_min_pct = 35.0; // below the 35.78% floor for a 35% stop
        assert!(cfg.validate().is_err());
        cfg.perp_margin_min_pct = 35.79;
        assert!(cfg.validate().is_ok());
        cfg.perp_margin_min_pct = 100.01;
        assert!(cfg.validate().is_err());
        cfg.perp_margin_min_pct = 47.55;
        cfg.lighter_mmr_pct = 0.0;
        assert!(cfg.validate().is_err());
        // Spot-only: no Lighter order and no collateral check ever runs, so
        // a stop value that would otherwise raise the floor above the
        // default minimum must not refuse startup.
        let mut spot_only = test_config();
        spot_only.perp_fraction = 0.0;
        spot_only.stop_dd_pct = 50.0;
        assert!(spot_only.validate().is_ok());
        // The same values with perp exposure are still rejected.
        let mut with_perp = spot_only.clone();
        with_perp.perp_fraction = 0.45;
        assert!(with_perp.validate().is_err());
    }

    #[test]
    fn fingerprint_changes_with_margin_min() {
        let a = test_config();
        let mut b = test_config();
        b.perp_margin_min_pct = 60.0;
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_exit_level() {
        let a = test_config();
        let mut b = test_config();
        b.exit_dd_pct = 25.0;
        assert_ne!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.fingerprint(), test_config().fingerprint());
    }

    /// Connector stub for the DRY_RUN exit path: only `get_ticker` answers
    /// (the pre-exit reference price); every order/cancel call is skipped
    /// by `dry_run` before reaching the venue, so anything else is a bug.
    struct QuoteOnly {
        price: Decimal,
        /// Live-path knob: `cancel_order` fails (the venue kept the stop).
        cancel_fails: bool,
        /// Live-path knob: signed perp position `get_positions` reports for
        /// every symbol (`None` = the call is unexpected).
        perp_position: Option<f64>,
        /// Live-path knob: record `create_advanced_trigger_order` arguments
        /// (style, slippage_bps, tpsl, reduce_only) instead of panicking.
        trigger_calls: Option<std::sync::Mutex<Vec<(String, Option<u32>, String, bool)>>>,
        /// Live-path knob: the order id `get_open_orders` reports as
        /// resting. `None` = the venue shows nothing (the 2026-09-22
        /// failure: `sendTx` 200 but no order).
        stop_rests: Option<String>,
        /// Live-path knob: ids the venue reports as cancelled. A cancel is
        /// only settled when its id shows up here.
        canceled: Vec<String>,
    }

    #[async_trait::async_trait]
    impl DexConnector for QuoteOnly {
        async fn start(&self) -> Result<(), dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: start must not be called on the DRY_RUN exit path")
        }
        async fn get_funding_payments(
            &self,
            _since_secs: i64,
        ) -> Result<Vec<dex_connector::FundingPayment>, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: get_funding_payments must not be called on the DRY_RUN exit path"
            )
        }
        async fn stop(&self) -> Result<(), dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: stop must not be called on the DRY_RUN exit path")
        }
        async fn restart(&self, _max_retries: i32) -> Result<(), dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: restart must not be called on the DRY_RUN exit path")
        }
        async fn set_leverage(
            &self,
            _symbol: &str,
            _leverage: u32,
        ) -> Result<(), dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: set_leverage must not be called on the DRY_RUN exit path"
            )
        }
        async fn get_ticker(
            &self,
            _symbol: &str,
            _test_price: Option<Decimal>,
        ) -> Result<dex_connector::TickerResponse, dex_connector::DexError> {
            Ok(dex_connector::TickerResponse {
                symbol: _symbol.to_string(),
                price: self.price,
                size_decimals: Some(4),
                ..Default::default()
            })
        }
        async fn get_filled_orders(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::FilledOrdersResponse, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: get_filled_orders must not be called on the DRY_RUN exit path"
            )
        }
        async fn get_canceled_orders(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::CanceledOrdersResponse, dex_connector::DexError> {
            if self.trigger_calls.is_none() {
                unimplemented!(
                    "QuoteOnly stub: get_canceled_orders must not be called on the DRY_RUN exit path"
                )
            }
            Ok(dex_connector::CanceledOrdersResponse {
                orders: self
                    .canceled
                    .iter()
                    .map(|id| dex_connector::CanceledOrder {
                        order_id: id.clone(),
                        canceled_timestamp: 1,
                    })
                    .collect(),
            })
        }
        async fn get_open_orders(
            &self,
            symbol: &str,
        ) -> Result<dex_connector::OpenOrdersResponse, dex_connector::DexError> {
            let Some(calls) = &self.trigger_calls else {
                unimplemented!(
                    "QuoteOnly stub: get_open_orders must not be called on the DRY_RUN exit path"
                )
            };
            let _ = calls;
            let orders = if let Some(id) = &self.stop_rests {
                vec![dex_connector::OpenOrder {
                    order_id: id.clone(),
                    symbol: symbol.to_string(),
                    side: OrderSide::Short,
                    size: Decimal::ONE,
                    price: Decimal::ONE,
                    status: "open".into(),
                }]
            } else {
                Vec::new()
            };
            Ok(dex_connector::OpenOrdersResponse { orders })
        }
        async fn get_balance(
            &self,
            _symbol: Option<&str>,
        ) -> Result<dex_connector::BalanceResponse, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: get_balance must not be called on the DRY_RUN exit path"
            )
        }
        async fn get_combined_balance(
            &self,
        ) -> Result<dex_connector::CombinedBalanceResponse, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: get_combined_balance must not be called on the DRY_RUN exit path"
            )
        }
        async fn get_positions(
            &self,
        ) -> Result<Vec<dex_connector::PositionSnapshot>, dex_connector::DexError> {
            let Some(p) = self.perp_position else {
                if self.trigger_calls.is_some() {
                    // Live-path stub without a position knob: the account
                    // snapshot is NOT ready (what dex-connector returns
                    // until the first `account_all` of a connection).
                    return Err(dex_connector::DexError::Transient(
                        "positions not ready from websocket".into(),
                    ));
                }
                unimplemented!(
                    "QuoteOnly stub: get_positions must not be called on the DRY_RUN exit path"
                )
            };
            Ok(["BTC", "ETH"]
                .iter()
                .map(|sym| dex_connector::PositionSnapshot {
                    symbol: sym.to_string(),
                    size: Decimal::from_f64(p.abs()).unwrap(),
                    sign: if p < 0.0 { -1 } else { 1 },
                    entry_price: None,
                })
                .collect())
        }
        async fn get_last_trades(
            &self,
            _symbol: &str,
        ) -> Result<dex_connector::LastTradesResponse, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: get_last_trades must not be called on the DRY_RUN exit path"
            )
        }
        async fn get_order_book(
            &self,
            _symbol: &str,
            _depth: usize,
        ) -> Result<dex_connector::OrderBookSnapshot, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: get_order_book must not be called on the DRY_RUN exit path"
            )
        }
        async fn clear_filled_order(
            &self,
            _symbol: &str,
            _trade_id: &str,
        ) -> Result<(), dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: clear_filled_order must not be called on the DRY_RUN exit path"
            )
        }
        async fn clear_all_filled_orders(&self) -> Result<(), dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: clear_all_filled_orders must not be called on the DRY_RUN exit path")
        }
        async fn clear_canceled_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: clear_canceled_order must not be called on the DRY_RUN exit path"
            )
        }
        async fn clear_all_canceled_orders(&self) -> Result<(), dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: clear_all_canceled_orders must not be called on the DRY_RUN exit path")
        }
        async fn create_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _price: Option<Decimal>,
            _spread: Option<i64>,
            _reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: create_order must not be called on the DRY_RUN exit path"
            )
        }
        async fn create_advanced_trigger_order(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _trigger_px: Decimal,
            _limit_px: Option<Decimal>,
            order_style: dex_connector::TriggerOrderStyle,
            slippage_bps: Option<u32>,
            tpsl: dex_connector::TpSl,
            reduce_only: bool,
            _expiry_secs: Option<u64>,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            let Some(calls) = &self.trigger_calls else {
                unimplemented!("QuoteOnly stub: create_advanced_trigger_order must not be called on the DRY_RUN exit path")
            };
            calls.lock().unwrap().push((
                format!("{order_style:?}"),
                slippage_bps,
                format!("{tpsl:?}"),
                reduce_only,
            ));
            Ok(dex_connector::CreateOrderResponse {
                order_id: "stop-1".into(),
                exchange_order_id: None,
                ordered_price: Decimal::ONE,
                ordered_size: _size,
                client_order_id: None,
            })
        }
        async fn create_order_taker_ioc(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _slippage_bps: u32,
            _reduce_only: bool,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: create_order_taker_ioc must not be called on the DRY_RUN exit path")
        }
        async fn create_order_taker_ioc_at(
            &self,
            _symbol: &str,
            _size: Decimal,
            _side: OrderSide,
            _limit_price: Decimal,
            _reduce_only: bool,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: create_order_taker_ioc_at must not be called on the DRY_RUN exit path")
        }
        async fn modify_order(
            &self,
            _symbol: &str,
            _order_id: &str,
            _side: OrderSide,
            _target_total_size: Decimal,
            _open_remaining_size: Decimal,
            _price: Option<Decimal>,
            _spread: Option<i64>,
            _reduce_only: bool,
        ) -> Result<dex_connector::CreateOrderResponse, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: modify_order must not be called on the DRY_RUN exit path"
            )
        }
        async fn cancel_order(
            &self,
            _symbol: &str,
            _order_id: &str,
        ) -> Result<(), dex_connector::DexError> {
            if self.cancel_fails {
                Err(dex_connector::DexError::Transient(
                    "cancel timed out".into(),
                ))
            } else {
                Ok(())
            }
        }
        async fn cancel_all_orders(
            &self,
            _symbol: Option<String>,
        ) -> Result<(), dex_connector::DexError> {
            if self.trigger_calls.is_some() {
                return Ok(());
            }
            unimplemented!(
                "QuoteOnly stub: cancel_all_orders must not be called on the DRY_RUN exit path"
            )
        }
        async fn cancel_orders(
            &self,
            _symbol: Option<String>,
            _order_ids: Vec<String>,
        ) -> Result<(), dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: cancel_orders must not be called on the DRY_RUN exit path"
            )
        }
        async fn close_all_positions(
            &self,
            _symbol: Option<String>,
        ) -> Result<(), dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: close_all_positions must not be called on the DRY_RUN exit path"
            )
        }
        async fn clear_last_trades(&self, _symbol: &str) -> Result<(), dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: clear_last_trades must not be called on the DRY_RUN exit path"
            )
        }
        async fn is_upcoming_maintenance(&self, _hours_ahead: i64) -> bool {
            unimplemented!("QuoteOnly stub: is_upcoming_maintenance must not be called on the DRY_RUN exit path")
        }
        async fn sign_evm_65b(&self, _message: &str) -> Result<String, dex_connector::DexError> {
            unimplemented!(
                "QuoteOnly stub: sign_evm_65b must not be called on the DRY_RUN exit path"
            )
        }
        async fn sign_evm_65b_with_eip191(
            &self,
            _message: &str,
        ) -> Result<String, dex_connector::DexError> {
            unimplemented!("QuoteOnly stub: sign_evm_65b_with_eip191 must not be called on the DRY_RUN exit path")
        }
        fn subscribe_price_updates(
            &self,
        ) -> Result<
            tokio::sync::broadcast::Receiver<dex_connector::PriceUpdate>,
            dex_connector::DexError,
        > {
            unimplemented!("QuoteOnly stub: subscribe_price_updates must not be called on the DRY_RUN exit path")
        }
    }

    fn engine_with_stops(dir: &std::path::Path) -> Engine {
        let mut cfg = test_config();
        cfg.state_path = dir.join("state.json");
        cfg.status_path = dir.join("status.json");
        cfg.pnl_log_path = dir.join("pnl.jsonl");
        let mut state = State::default();
        state.mode = Mode::On;
        for (sym, px) in [("BTC", 80_000.0), ("ETH", 2_600.0)] {
            state.legs.insert(
                sym.to_string(),
                LegState {
                    spot_size: 0.01,
                    spot_cost_usd: px * 0.01,
                    perp_size: 0.005,
                    perp_cost_usd: px * 0.005,
                    peak_close: px,
                    exit_level: level_below_peak(px, 30.0),
                    lighter_peak: px,
                    stop_level: Some(level_below_peak(px, 35.0)),
                    stop_order_id: Some(format!("stop-{sym}")),
                    stop_size: Some(0.005),
                    last_close_date: None,
                    last_close: Some(px),
                    close_fetch_failures: 0,
                    cost_basis_unknown: false,
                    perp_cost_basis_unknown: false,
                    stop_unconfirmed_id: None,
                    stop_unlisted_since: None,
                    stop_presumed_gone: false,
                    stop_absence_confirmed: false,
                },
            );
        }
        let quote: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(1),
            cancel_fails: false,
            perp_position: None,
            trigger_calls: None,
            stop_rests: None,
            canceled: Vec::new(),
        });
        Engine {
            cfg: cfg.clone(),
            hl: quote.clone(),
            lt: quote,
            http: reqwest::Client::new(),
            sentinels: Sentinels::new(cfg.kill_switch_path.clone(), cfg.risk_ack_path.clone()),
            state,
            last_status_write: 0,
            last_reconcile: 0,
            last_margin_check: 0,
            last_margin: None,
            last_funding_poll: 0,
            resolved_account_index: tokio::sync::Mutex::new(None),
        }
    }

    /// Regression (found in the 2026-09-21 DISARM drill): `exit_all` used
    /// to clone the leg BEFORE `cancel_stop` cleared its stop fields, then
    /// wrote the stale clone back — state kept reporting a stop that had
    /// been cancelled at the venue. Harmless once Exited, but on the
    /// partial-exit failure path (mode stays On) `stop_covers` would have
    /// read the open leg as protected by an order that no longer rests.
    #[tokio::test]
    async fn exit_clears_the_cancelled_stop_from_state() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_exit_stop_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.exit_all("test").await;
        assert_eq!(e.state.mode, Mode::Exited);
        for sym in ["BTC", "ETH"] {
            let leg = &e.state.legs[sym];
            assert_eq!(leg.spot_size, 0.0, "{sym} spot closed");
            assert_eq!(leg.perp_size, 0.0, "{sym} perp closed");
            assert_eq!(
                leg.stop_order_id, None,
                "{sym} stop id must not survive the exit"
            );
            assert_eq!(
                leg.stop_level, None,
                "{sym} stop level must not survive the exit"
            );
            assert_eq!(
                leg.stop_size, None,
                "{sym} stop size must not survive the exit"
            );
            assert!(
                !stop_covers(leg.stop_order_id.is_some(), leg.stop_size, 0.005),
                "{sym}: a cancelled stop must not read as covering anything"
            );
        }
        // The persisted copy is what a restart reads back.
        let back: State = load_json(&e.cfg.state_path).unwrap().unwrap();
        assert!(back.legs.values().all(|l| l.stop_order_id.is_none()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn ack() -> Result<dex_connector::CreateOrderResponse> {
        Ok(dex_connector::CreateOrderResponse {
            order_id: "1".into(),
            exchange_order_id: None,
            ordered_price: Decimal::ONE,
            ordered_size: Decimal::ONE,
            client_order_id: None,
        })
    }

    #[test]
    fn live_fill_is_the_venue_holding_change_not_the_ack() {
        let req = Decimal::from_str("0.01").unwrap();
        // Ack + full change → the change.
        assert_eq!(
            settle_fill("t", req, ack(), Ok(0.01)).unwrap(),
            Decimal::from_str("0.01").unwrap()
        );
        // Ack + partial change → the partial, never the requested size.
        assert_eq!(
            settle_fill("t", req, ack(), Ok(0.004)).unwrap(),
            Decimal::from_str("0.004").unwrap()
        );
        // Ack but nothing observed → unknown, an error (caller halts).
        assert!(settle_fill("t", req, ack(), Ok(0.0)).is_err());
        // Error after the venue filled → record the fill, do not surface the
        // error as "no exposure" (a retry would double the position).
        assert_eq!(
            settle_fill("t", req, Err(anyhow!("timeout")), Ok(0.01)).unwrap(),
            Decimal::from_str("0.01").unwrap()
        );
        // Error and nothing filled → the error.
        assert!(settle_fill("t", req, Err(anyhow!("rejected")), Ok(0.0)).is_err());
        // Holding unreadable → error even with a clean ack.
        assert!(settle_fill("t", req, ack(), Err(anyhow!("503"))).is_err());
    }

    #[test]
    fn size_rounding_is_not_a_short_fill() {
        // The live case (2026-09-23): a $99 perp tranche at ~$86,127 on a
        // 5-decimal market. The largest orderable size is 0.00114 =
        // $98.18, so measuring the fill against the $99 TARGET reads a
        // full fill as 0.8% short — past the tolerance — and halts the
        // ladder. Against what was ordered it is complete.
        let price = 86_127.0;
        let size = size_from_notional(99.0, price, 5);
        let ordered = size.to_f64().unwrap() * price;
        assert!(ordered < 99.0 && ordered > 98.0, "ordered {ordered}");
        assert!(!fill_complete(ordered, 99.0), "the old comparison halts");
        assert!(fill_complete(ordered, ordered), "the new one completes");
        // A genuinely short fill is still short.
        assert!(!fill_complete(ordered * 0.9, ordered));
    }

    #[test]
    fn fill_completion_tolerates_a_base_fee_but_not_a_short_fill() {
        assert!(fill_complete(0.01, 0.01));
        assert!(fill_complete(0.00996, 0.01)); // 0.04% base fee
        assert!(!fill_complete(0.009, 0.01));
        assert!(fill_complete(0.0, 0.0));
    }

    #[test]
    fn partial_close_leaves_the_remainder_open_and_dust_closed() {
        let px = Some(80_000.0);
        assert_eq!(remaining_after_close(0.01, 0.01, px), 0.0);
        // $4 of residue is dust; $15 (1.5 % of a $1,000 leg) is NOT.
        assert_eq!(remaining_after_close(0.01, 0.00995, px), 0.0);
        let r = remaining_after_close(0.0125, 0.0123125, px);
        assert!((r - 0.0001875).abs() < 1e-12);
        let r = remaining_after_close(0.01, 0.006, px);
        assert!((r - 0.004).abs() < 1e-12);
        // No price → nothing is dust.
        assert!(remaining_after_close(0.01, 0.00999, None) > 0.0);
        assert_eq!(remaining_after_close(0.0, 0.0, px), 0.0);
    }

    #[test]
    fn open_book_is_only_operated_in_the_mode_that_created_it() {
        let mut st = State::default();
        // Flat: any mode is fine, including a pre-#895 file.
        assert!(check_book_mode(&st, true).is_ok());
        assert!(check_book_mode(&st, false).is_ok());
        st.mode = Mode::On;
        st.book_dry_run = Some(true);
        assert!(check_book_mode(&st, true).is_ok());
        assert!(
            check_book_mode(&st, false).is_err(),
            "simulated book must not go live"
        );
        st.book_dry_run = Some(false);
        assert!(check_book_mode(&st, false).is_ok());
        assert!(
            check_book_mode(&st, true).is_err(),
            "live book must not be simulated away"
        );
        st.book_dry_run = None;
        assert!(
            check_book_mode(&st, true).is_err(),
            "unknown provenance with exposure is refused"
        );
        // Exited but a leg still open (partial exit) counts as open.
        st.mode = Mode::Exited;
        st.book_dry_run = Some(false);
        st.legs.insert(
            "BTC".into(),
            LegState {
                spot_size: 0.001,
                ..Default::default()
            },
        );
        assert!(check_book_mode(&st, true).is_err());
        assert!(check_book_mode(&st, false).is_ok());
    }

    #[test]
    fn opening_the_book_records_the_execution_mode() {
        // Round-trip: the field must survive persist/load (it is what the
        // startup gate reads).
        let mut st = State::default();
        st.mode = Mode::On;
        st.book_dry_run = Some(false);
        let js = serde_json::to_string(&st).unwrap();
        let back: State = serde_json::from_str(&js).unwrap();
        assert_eq!(back.book_dry_run, Some(false));
        // A pre-#895 file (no field) loads as None.
        let old: State = serde_json::from_str(r#"{"mode":"On"}"#).unwrap();
        assert_eq!(old.book_dry_run, None);
    }

    #[test]
    fn order_outcome_is_known_only_with_a_readable_holding() {
        // Readable + changed → known (ack or not).
        assert!(order_outcome_known(true, false, Some(0.01)));
        assert!(order_outcome_known(false, false, Some(0.01)));
        // Readable + unchanged: known only on a DEFINITIVE rejection. A
        // timed-out submission may have been accepted and fill late.
        assert!(order_outcome_known(false, true, Some(0.0)));
        assert!(!order_outcome_known(false, false, Some(0.0)));
        assert!(!order_outcome_known(true, false, Some(0.0)));
        // Unreadable → never known.
        assert!(!order_outcome_known(true, false, None));
        assert!(!order_outcome_known(false, true, None));
    }

    #[test]
    fn only_venue_rejections_and_pre_submission_failures_are_definitive() {
        use dex_connector::DexError;
        assert!(order_rejected_definitively(&DexError::ServerResponse(
            "reduce-only".into()
        )));
        assert!(order_rejected_definitively(&DexError::Permanent(
            "no market".into()
        )));
        assert!(order_rejected_definitively(&DexError::NoConnection));
        assert!(!order_rejected_definitively(&DexError::Transient(
            "timeout".into()
        )));
        assert!(!order_rejected_definitively(
            &DexError::ReconciliationRequired {
                action: "order".into(),
                nonce: 1,
                detail: "ambiguous".into(),
            }
        ));
        assert!(!order_rejected_definitively(&DexError::Serde(
            serde_json::from_str::<serde_json::Value>("{").unwrap_err()
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_keeps_a_partial_observation_across_a_later_read_failure() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_confirm_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let e = engine_with_stops(&dir);
        // First read sees 40% of the request, every later read fails.
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let c = calls.clone();
        let read = move || {
            let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if n == 0 {
                    Ok(1.004)
                } else {
                    Err(anyhow!("503"))
                }
            }
        };
        let got = e
            .confirm_fill(read, 1.0, OrderSide::Long, 0.01)
            .await
            .unwrap();
        assert!(
            (got - 0.004).abs() < 1e-12,
            "the 0.004 seen must survive, got {got}"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            FILL_CONFIRM_ATTEMPTS
        );
        // All reads failing is the only unreadable outcome.
        let all_fail = || async { Err::<f64, _>(anyhow!("503")) };
        assert!(e
            .confirm_fill(all_fail, 1.0, OrderSide::Long, 0.01)
            .await
            .is_err());
        // A sell is measured as a decrease.
        let sold = || async { Ok(0.99) };
        let got = e
            .confirm_fill(sold, 1.0, OrderSide::Short, 0.01)
            .await
            .unwrap();
        assert!((got - 0.01).abs() < 1e-12);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sub_minimum_remainder_settles_the_leg_as_done() {
        let mut progress = BTreeMap::new();
        // Nothing filled yet: a small computed size is a real problem, not dust.
        assert!(!remainder_is_dust(
            0.0,
            Decimal::from_str("0.00001").unwrap(),
            0.0001,
            &mut progress,
            "BTC",
            true
        ));
        assert!(progress.is_empty());
        // Partially filled and the remainder is below min_order: done.
        assert!(remainder_is_dust(
            390.0,
            Decimal::from_str("0.00001").unwrap(),
            0.0001,
            &mut progress,
            "BTC",
            true
        ));
        assert!(progress["BTC"].spot_done);
        assert!(!progress["BTC"].perp_done);
        // Remainder still orderable: not dust.
        assert!(!remainder_is_dust(
            200.0,
            Decimal::from_str("0.002").unwrap(),
            0.0001,
            &mut progress,
            "BTC",
            false
        ));
        assert!(!progress["BTC"].perp_done);
    }

    #[test]
    fn unsettled_order_counts_as_an_open_book_and_survives_persist() {
        let mut st = State::default();
        assert!(!book_is_open(&st));
        st.pending_order = Some(PendingOrder {
            venue: "hyperliquid".into(),
            symbol: "UBTC/USDC".into(),
            side: "long".into(),
            requested: 0.005,
            holding_before: 0.0,
            ts: 1,
        });
        st.book_dry_run = Some(false);
        assert!(
            book_is_open(&st),
            "an unsettled order is exposure of unknown size"
        );
        assert!(
            check_book_mode(&st, true).is_err(),
            "must not be simulated away under DRY_RUN"
        );
        assert!(check_book_mode(&st, false).is_ok());
        let js = serde_json::to_string(&st).unwrap();
        let back: State = serde_json::from_str(&js).unwrap();
        assert_eq!(back.pending_order, st.pending_order);
        // Progress notional round-trips too (a partial fill's remainder
        // depends on it after a restart).
        let mut st2 = State::default();
        st2.tranche_progress.insert(
            "ETH".into(),
            LegProgress {
                spot_done: false,
                perp_done: false,
                spot_usd: 123.4,
                perp_usd: 0.0,
            },
        );
        let back: State = serde_json::from_str(&serde_json::to_string(&st2).unwrap()).unwrap();
        assert_eq!(back.tranche_progress["ETH"].spot_usd, 123.4);
        let old: State = serde_json::from_str(
            r#"{"tranche_progress":{"ETH":{"spot_done":true,"perp_done":false}}}"#,
        )
        .unwrap();
        assert_eq!(old.tranche_progress["ETH"].spot_usd, 0.0);
    }

    /// Live exit where the venue already flattened the legs (the exchange
    /// stop fired) but the stop cancel fails: the book must NOT reach
    /// Exited — a later ARM would clear the leg and forget a trigger that
    /// may still rest at the venue.
    #[tokio::test(start_paused = true)]
    async fn exit_with_an_unconfirmed_stop_cancel_stays_on_and_halts() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_exit_cancel_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(1),
            cancel_fails: true,
            perp_position: None,
            trigger_calls: None,
            stop_rests: None,
            canceled: Vec::new(),
        });
        e.hl = venue.clone();
        e.lt = venue;
        for leg in e.state.legs.values_mut() {
            leg.spot_size = 0.0;
            leg.perp_size = 0.0;
        }
        e.exit_all("test").await;
        assert_eq!(
            e.state.mode,
            Mode::On,
            "must not be Exited with a stop unconfirmed"
        );
        assert!(e.state.halted);
        assert!(
            e.state.legs.values().all(|l| l.stop_order_id.is_some()),
            "stop stays tracked"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same exit, but the venue itself already said the stop is gone
    /// (nothing rests for the market / the id is listed as cancelled).
    /// Cancelling an absent order fails by definition, so holding the id
    /// for "a retried DISARM" would halt the book on an order that does
    /// not exist and no retry could ever clear.
    #[tokio::test(start_paused = true)]
    async fn exit_drops_a_stop_the_venue_already_reported_gone() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_exit_gone_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(1),
            cancel_fails: true,
            perp_position: None,
            trigger_calls: None,
            stop_rests: None,
            canceled: Vec::new(),
        });
        e.hl = venue.clone();
        e.lt = venue;
        for leg in e.state.legs.values_mut() {
            leg.spot_size = 0.0;
            leg.perp_size = 0.0;
            leg.stop_presumed_gone = true;
            leg.stop_absence_confirmed = true;
        }
        e.exit_all("test").await;
        assert_eq!(
            e.state.mode,
            Mode::Exited,
            "an absence the venue established must not block the exit"
        );
        assert!(!e.state.halted);
        assert!(
            e.state
                .legs
                .values()
                .all(|l| l.stop_order_id.is_none() && !l.stop_presumed_gone),
            "the id and the doubt about it are both dropped"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Knowledge is not undone by a weaker later read — here at the one
    /// site that records an absence. Re-observing the same absence says
    /// nothing new, so it must not demote the venue's own answer to an
    /// inference.
    #[tokio::test(start_paused = true)]
    async fn re_marking_an_absence_never_downgrades_a_confirmed_one() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_no_downgrade_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        // The venue's own answer: nothing rests / named cancelled.
        e.mark_stop_presumed_gone("BTC", true);
        assert!(e.state.legs["BTC"].stop_absence_confirmed);
        // A later pass only re-observes it (another order rests, this id
        // still unlisted) — `unlisted_verdict(false, None)`.
        e.mark_stop_presumed_gone("BTC", false);
        assert!(
            e.state.legs["BTC"].stop_absence_confirmed,
            "a re-observation must not demote the venue's own answer"
        );
        // Only the id turning up listed clears it.
        assert!(e.stop_seen_listed("BTC"));
        assert!(!e.state.legs["BTC"].stop_absence_confirmed);
        // ...and an inference alone never sets it.
        e.mark_stop_presumed_gone("BTC", false);
        assert!(e.state.legs["BTC"].stop_presumed_gone);
        assert!(!e.state.legs["BTC"].stop_absence_confirmed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A stop believed gone is never "already current", so the recorded
    /// level and size can be KEPT — and if the absence was only inferred
    /// and the order turns out to still rest, the leg is judged against
    /// them again instead of being cancelled and re-placed for nothing.
    #[tokio::test(start_paused = true)]
    async fn a_stop_believed_gone_is_replaced_though_its_metadata_still_matches() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_gone_meta_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue = Arc::new(QuoteOnly {
            price: Decimal::from(80_000),
            cancel_fails: false,
            perp_position: Some(0.005),
            trigger_calls: Some(std::sync::Mutex::new(Vec::new())),
            stop_rests: None,
            canceled: Vec::new(),
        });
        let dyn_venue: Arc<dyn DexConnector + Send + Sync> = venue.clone();
        e.lt = dyn_venue.clone();
        e.hl = dyn_venue;
        // Exactly what `engine_with_stops` recorded: at this price the
        // stop IS current, so only the flag can make a replacement due.
        let before = e.state.legs["BTC"].clone();
        assert!(stop_is_current(
            before.stop_level,
            before.stop_size,
            true,
            level_below_peak(80_000.0, e.cfg.stop_dd_pct),
            before.perp_size
        ));
        e.state.legs.get_mut("BTC").unwrap().stop_presumed_gone = true;
        let _ = e.place_stop("BTC").await;
        assert!(
            !venue
                .trigger_calls
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .is_empty(),
            "a stop believed gone must be replaced, not read as current"
        );
        // And the metadata was never thrown away, so a leg whose stop
        // turns out to still rest can be judged current again.
        e.state.legs.insert("BTC".into(), before.clone());
        e.mark_stop_presumed_gone("BTC", false);
        assert!(e.stop_seen_listed("BTC"));
        let after = &e.state.legs["BTC"];
        assert_eq!(after.stop_level, before.stop_level);
        assert_eq!(after.stop_size, before.stop_size);
        assert!(stop_is_current(
            after.stop_level,
            after.stop_size,
            true,
            level_below_peak(80_000.0, e.cfg.stop_dd_pct),
            after.perp_size
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An absence the bot already established is acted on NOW, not
    /// re-timed. Restarting the window on every repeat of the same
    /// answer would leave an externally cancelled stop uncovered for
    /// another whole grace after each failed cancel retry.
    #[test]
    fn an_established_absence_is_not_re_timed() {
        // Still unlisted, already established: act, and the grace has no
        // say in it.
        assert_eq!(unlisted_verdict(false, None), Some((false, false)));
        // The venue naming it cancelled is its own word, so the absence
        // is confirmed — that is what lets the id be forgotten later.
        assert_eq!(unlisted_verdict(true, None), Some((false, true)));
        assert_eq!(unlisted_verdict(true, Some(0)), Some((false, true)));
        // Not yet established: moments decide nothing, however many
        // reads land in them.
        for e in [0, 1, 60, STOP_UNLISTED_GRACE_SECS - 1] {
            assert_eq!(unlisted_verdict(false, Some(e)), None, "must defer at {e}s");
        }
        // Running out the grace is an inference about a stale cache: it
        // is enough to re-place, never enough to confirm.
        assert_eq!(
            unlisted_verdict(false, Some(STOP_UNLISTED_GRACE_SECS)),
            Some((false, false))
        );
    }

    /// The one rule, and the two sites that drop a stop id, agree: only
    /// the venue's own word lets the bot stop tracking one.
    #[test]
    fn a_stop_id_is_forgotten_only_on_the_venues_own_word() {
        // The cancel went through: the order is gone, whatever was
        // inferred about it beforehand.
        assert!(may_forget_stop(true, false));
        assert!(may_forget_stop(true, true));
        // The venue already named the absence: cancelling something that
        // is not there fails by definition, so that failure may not hold
        // the id hostage.
        assert!(may_forget_stop(false, true));
        // A cancel that merely errored, with nothing but an inference
        // behind it — a transport blip looks exactly like an absent
        // order, and the trigger may still rest.
        assert!(!may_forget_stop(false, false));
    }

    /// `place_stop` cancels the order it replaces. When absence is only
    /// inferred and that cancel errors, it must defer rather than forget
    /// the id — the same rule `cancel_stop` follows, at the other site.
    #[tokio::test(start_paused = true)]
    async fn a_replacement_defers_when_the_previous_stop_is_only_inferred_gone() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_replace_inferred_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue = Arc::new(QuoteOnly {
            price: Decimal::from(80_000),
            cancel_fails: true,
            perp_position: Some(0.005),
            trigger_calls: Some(std::sync::Mutex::new(Vec::new())),
            stop_rests: None,
            canceled: Vec::new(),
        });
        let dyn_venue: Arc<dyn DexConnector + Send + Sync> = venue.clone();
        e.lt = dyn_venue.clone();
        e.hl = dyn_venue;
        for leg in e.state.legs.values_mut() {
            leg.stop_level = Some(1.0); // stale, so a replacement is due
            leg.stop_presumed_gone = true; // inferred from the grace only
            leg.stop_absence_confirmed = false;
        }
        let err = format!("{:#}", e.place_stop("BTC").await.unwrap_err());
        assert!(
            err.contains("could not cancel the previous stop"),
            "unexpected error: {err}"
        );
        assert_eq!(
            e.state.legs["BTC"].stop_order_id.as_deref(),
            Some("stop-BTC"),
            "an id that may still be resting stays tracked for the retry"
        );
        assert!(
            venue
                .trigger_calls
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .is_empty(),
            "and no second trigger is rested on top of it"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The unlisted-for-the-whole-grace fallback is an inference about a
    /// stale WebSocket cache, not the venue naming an absence. A cancel
    /// that merely errors — a transport blip is enough — must not be
    /// allowed to drop that id: the reduce-only trigger may still rest,
    /// and a later ARM would inherit it against the new position.
    #[tokio::test(start_paused = true)]
    async fn exit_keeps_a_stop_only_inferred_to_be_gone() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_exit_inferred_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(1),
            cancel_fails: true,
            perp_position: None,
            trigger_calls: None,
            stop_rests: None,
            canceled: Vec::new(),
        });
        e.hl = venue.clone();
        e.lt = venue;
        for leg in e.state.legs.values_mut() {
            leg.spot_size = 0.0;
            leg.perp_size = 0.0;
            leg.stop_presumed_gone = true; // inferred from the grace only
            leg.stop_absence_confirmed = false;
        }
        e.exit_all("test").await;
        assert_eq!(
            e.state.mode,
            Mode::On,
            "an inference must not be enough to reach Exited"
        );
        assert!(e.state.halted);
        assert!(
            e.state.legs.values().all(|l| l.stop_order_id.is_some()),
            "the id stays tracked for a retried DISARM"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An absence the venue established is knowledge; a read that failed
    /// is not. Only the id turning up listed (or a new stop replacing it)
    /// may undo it — otherwise an inconclusive check would restore the
    /// absent id as cover `place_stop` must preserve, and the leg would
    /// sit uncovered without ever attempting a replacement.
    #[tokio::test(start_paused = true)]
    async fn an_established_absence_survives_an_inconclusive_check() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_absence_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        let leg = e.state.legs.get_mut("BTC").unwrap();
        leg.stop_presumed_gone = true;
        leg.stop_unlisted_since = Some(now_secs());

        // An interruption ends the observation and nothing else.
        assert!(e.end_unlisted_observation("BTC"));
        let leg = &e.state.legs["BTC"];
        assert_eq!(leg.stop_unlisted_since, None, "the window restarts");
        assert!(
            leg.stop_presumed_gone,
            "an unreadable answer is not the order coming back"
        );
        // It is the absence, so the replacement guard does not count the
        // recorded id as protection to preserve.
        assert!(
            !(leg.stop_order_id.is_some() && !leg.stop_presumed_gone)
                && leg.stop_unconfirmed_id.is_none(),
            "a presumed-gone id must not read as cover"
        );
        // Idempotent: with nothing left to end, nothing changed.
        assert!(!e.end_unlisted_observation("BTC"));

        // Seeing it listed is the one read that settles it the other way.
        assert!(e.stop_seen_listed("BTC"));
        assert!(!e.state.legs["BTC"].stop_presumed_gone);
        assert!(!e.stop_seen_listed("BTC"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Live exit where the venue shows a SHORT where the book has a long
    /// (manual action / unrecorded order): never "closed", no order sent
    /// for it, the book halts with the leg as recorded.
    #[tokio::test(start_paused = true)]
    async fn exit_refuses_a_reversed_venue_position() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_exit_rev_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(1),
            cancel_fails: false,
            perp_position: Some(-0.005),
            trigger_calls: None,
            stop_rests: None,
            canceled: Vec::new(),
        });
        e.hl = venue.clone();
        e.lt = venue;
        for leg in e.state.legs.values_mut() {
            leg.spot_size = 0.0; // perp-only book for this test
        }
        e.exit_all("test").await;
        assert_eq!(e.state.mode, Mode::On);
        assert!(
            e.state.halted,
            "a reversed position must halt, not read as flat"
        );
        assert!(
            e.state.legs.values().all(|l| l.perp_size == 0.005),
            "the recorded leg is untouched"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unsettled order is resolved against ITS baseline, not the 2 %
    /// reconcile tolerance: a late fill smaller than the tolerance must
    /// still halt (the remainder would otherwise be re-sent on top of it).
    #[tokio::test]
    async fn unsettled_order_is_resolved_against_its_own_baseline() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_pending_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        // Venue shows 0.00505 long on both symbols; the book holds 0.005
        // (within the 2 % tolerance), but the pending order's baseline
        // was 0.005 → it moved by 0.00005 → a late fill.
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(1),
            cancel_fails: false,
            perp_position: Some(0.00505),
            trigger_calls: None,
            stop_rests: None,
            canceled: Vec::new(),
        });
        e.hl = venue.clone();
        e.lt = venue;
        e.state.pending_order = Some(PendingOrder {
            venue: "lighter".into(),
            symbol: "BTC".into(),
            side: OrderSide::Long.to_string(),
            requested: 0.0005,
            holding_before: 0.005,
            ts: 1,
        });
        assert!(e.reconcile_before_orders("test").await.is_err());
        assert!(e.state.halted, "a late fill inside the tolerance must halt");
        assert!(
            e.state.pending_order.is_some(),
            "marker kept for the operator"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The live Lighter stop must be the slippage-controlled stop-LIMIT
    /// (type 3) with the configured protective slippage, reduce-only, SL:
    /// the connector's `Market` style sends execution price 0 and the
    /// Lighter signer rejects it (first live ARM, bot-strategy#895/#950).
    #[tokio::test(start_paused = true)]
    async fn live_stop_is_a_slippage_controlled_stop_limit() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_stop_style_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        e.cfg.stop_slippage_bps = 700;
        let venue = Arc::new(QuoteOnly {
            price: Decimal::from(80_000),
            cancel_fails: false,
            perp_position: None,
            trigger_calls: Some(std::sync::Mutex::new(Vec::new())),
            stop_rests: Some("stop-1".into()),
            canceled: Vec::new(),
        });
        let dyn_venue: Arc<dyn DexConnector + Send + Sync> = venue.clone();
        e.lt = dyn_venue;
        // No stop tracked yet → place_stop must create one.
        if let Some(l) = e.state.legs.get_mut("BTC") {
            l.stop_order_id = None;
            l.stop_level = None;
            l.stop_size = None;
        }
        e.place_stop("BTC").await.unwrap();
        let calls = venue
            .trigger_calls
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .clone();
        assert_eq!(calls.len(), 1);
        let (style, slip, tpsl, reduce_only) = &calls[0];
        assert_eq!(style, "MarketWithSlippageControl");
        assert_eq!(*slip, Some(700));
        assert_eq!(tpsl, "Sl");
        assert!(*reduce_only);
        assert_eq!(e.state.legs["BTC"].stop_order_id.as_deref(), Some("stop-1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `sendTx` returning 200 is not an order: when the venue never shows
    /// the stop, `place_stop` must fail and record NOTHING, so the next
    /// reconcile re-places it. (2026-09-22: two acknowledged stops never
    /// rested while the bot kept their ids and stopped retrying.)
    #[tokio::test(start_paused = true)]
    async fn an_acknowledged_stop_the_venue_never_shows_is_not_recorded() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_stop_ghost_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(80_000),
            cancel_fails: false,
            perp_position: Some(0.005), // a ready account snapshot
            trigger_calls: Some(std::sync::Mutex::new(Vec::new())),
            stop_rests: None, // acknowledged, never rests
            canceled: Vec::new(),
        });
        e.lt = venue;
        if let Some(l) = e.state.legs.get_mut("BTC") {
            l.stop_order_id = None;
            l.stop_level = None;
            l.stop_size = None;
        }
        let err = e.place_stop("BTC").await.unwrap_err();
        assert!(
            format!("{err:#}").contains("never appeared at the venue"),
            "unexpected error: {err:#}"
        );
        let leg = &e.state.legs["BTC"];
        assert_eq!(
            leg.stop_order_id, None,
            "a ghost stop must not count as cover"
        );
        assert_eq!(leg.stop_level, None);
        assert_eq!(leg.stop_size, None);
        assert!(
            !stop_covers(leg.stop_order_id.is_some(), leg.stop_size, leg.perp_size),
            "the leg must read as uncovered so ensure_stops retries"
        );
        // ...but the id is kept: an empty WS-cache read is not proof the
        // order is absent, so it must be cancellable by id later.
        assert_eq!(
            leg.stop_unconfirmed_id.as_deref(),
            Some("stop-1"),
            "an unconfirmed stop id must never be forgotten"
        );
        // It survives a restart, and the next placement cancels it first.
        let back: State = load_json(&e.cfg.state_path).unwrap().unwrap();
        assert_eq!(
            back.legs["BTC"].stop_unconfirmed_id.as_deref(),
            Some("stop-1")
        );
        // It is not settled while the venue cannot be read (this test's
        // account endpoint is unreachable): absence is never assumed.
        assert!(!e.drop_unconfirmed_stop("BTC").await);
        assert_eq!(
            e.state.legs["BTC"].stop_unconfirmed_id.as_deref(),
            Some("stop-1"),
            "an unreadable venue keeps the id"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A cancel that fails leaves the unconfirmed stop tracked: forgetting
    /// an order that may be resting is the failure mode this guards.
    #[tokio::test(start_paused = true)]
    async fn a_failed_cancel_keeps_the_unconfirmed_stop_tracked() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_stop_keep_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue: Arc<dyn DexConnector + Send + Sync> = Arc::new(QuoteOnly {
            price: Decimal::from(80_000),
            cancel_fails: true,
            perp_position: None,
            trigger_calls: Some(std::sync::Mutex::new(Vec::new())),
            stop_rests: None,
            canceled: Vec::new(),
        });
        e.lt = venue;
        if let Some(l) = e.state.legs.get_mut("BTC") {
            l.stop_unconfirmed_id = Some("ghost-7".into());
        }
        e.drop_unconfirmed_stop("BTC").await;
        assert_eq!(
            e.state.legs["BTC"].stop_unconfirmed_id.as_deref(),
            Some("ghost-7")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// While a stop with unknown state is still tracked, no replacement may
    /// be sent (two live stops on one position) and an exit may not reach
    /// Exited (a later ARM would inherit an orphaned reduce-only trigger).
    #[tokio::test(start_paused = true)]
    async fn an_unsettled_stop_blocks_replacement_and_exit() {
        let dir = std::env::temp_dir().join(format!(
            "bull_holder_stop_block_{}_{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = engine_with_stops(&dir);
        e.cfg.dry_run = false;
        let venue = Arc::new(QuoteOnly {
            price: Decimal::from(80_000),
            cancel_fails: true,         // the uncertain stop cannot be settled
            perp_position: Some(0.005), // ready snapshot...
            trigger_calls: Some(std::sync::Mutex::new(Vec::new())),
            stop_rests: Some("ghost-9".into()), // ...and it shows the stop IS resting
            canceled: Vec::new(),
        });
        let dyn_venue: Arc<dyn DexConnector + Send + Sync> = venue.clone();
        e.lt = dyn_venue.clone();
        e.hl = dyn_venue;
        for leg in e.state.legs.values_mut() {
            leg.stop_order_id = None;
            leg.stop_level = None;
            leg.stop_size = None;
            leg.stop_unconfirmed_id = Some("ghost-9".into());
        }
        // No replacement while it is unsettled. (The leverage read also
        // fails against this stub, which blocks for the same reason: a
        // leg that already carries a stop is never left without one on
        // the strength of a read that did not answer.)
        let err = e.place_stop("BTC").await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("could not be cancelled") || msg.contains("could not be read"),
            "unexpected error: {msg}"
        );
        assert!(
            venue
                .trigger_calls
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .is_empty(),
            "no trigger order may be submitted while a stop is unsettled"
        );
        assert_eq!(
            e.state.legs["BTC"].stop_unconfirmed_id.as_deref(),
            Some("ghost-9")
        );
        // And an exit must not reach Exited.
        for leg in e.state.legs.values_mut() {
            leg.spot_size = 0.0;
            leg.perp_size = 0.0;
        }
        e.exit_all("test").await;
        assert_eq!(
            e.state.mode,
            Mode::On,
            "must not be Exited with a live-maybe stop"
        );
        assert!(e.state.halted);
        // The exposure is closed first: settling the stop polls the venue
        // for up to ~2 min per symbol and must never delay the exit.
        for (sym, leg) in &e.state.legs {
            assert_eq!(leg.spot_size, 0.0, "{sym} spot must be closed");
            assert_eq!(leg.perp_size, 0.0, "{sym} perp must be closed");
            assert!(
                leg.stop_unconfirmed_id.is_some(),
                "{sym} keeps the unsettled stop id"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn venue_reads_resolve_the_instance_suffixed_account() {
        // Same rule as config::lighter_env — the suffixed value wins, so
        // this check can never read a different account than the one the
        // connector trades on.
        std::env::set_var("BULL_HOLDER_TEST_IDX", "shared");
        std::env::set_var("BULL_HOLDER_TEST_IDX_BULL_HOLDER", "dedicated");
        assert_eq!(
            lighter_env("BULL_HOLDER_TEST_IDX", "bull-holder").as_deref(),
            Some("dedicated")
        );
        assert_eq!(
            lighter_env("BULL_HOLDER_TEST_IDX", "other").as_deref(),
            Some("shared")
        );
        std::env::set_var("BULL_HOLDER_TEST_IDX_BULL_HOLDER", "");
        assert_eq!(
            lighter_env("BULL_HOLDER_TEST_IDX", "bull-holder").as_deref(),
            Some("shared"),
            "an empty suffixed value falls back, it is not a value"
        );
        std::env::remove_var("BULL_HOLDER_TEST_IDX");
        std::env::remove_var("BULL_HOLDER_TEST_IDX_BULL_HOLDER");
        assert_eq!(lighter_env("BULL_HOLDER_TEST_IDX", "bull-holder"), None);
    }

    #[test]
    fn the_guard_measures_the_trigger_against_the_mark_not_the_peak() {
        // What Lighter validates is the distance from the CURRENT mark to
        // the trigger. After a drawdown a 35%-below-peak stop can sit much
        // closer than 35% below the mark, and a margin that carries it
        // must not be rejected (Codex review).
        let distance = |mark: f64, trigger: f64| 100.0 * (1.0 - trigger / mark);
        // Peak 100, mark 80, 35% trailing → trigger 65 = 18.75% below mark.
        let trigger = level_below_peak(100.0, 35.0);
        assert!((trigger - 65.0).abs() < 1e-9);
        let d = distance(80.0, trigger);
        assert!((d - 18.75).abs() < 1e-9, "distance {d}");
        // 3x (33.33% margin) carries that; measuring against the peak's
        // 35% would have refused it forever.
        assert!(33.33 >= d);
        assert!(33.33 < 35.0);
        // At the peak the two agree.
        assert!((distance(100.0, trigger) - 35.0).abs() < 1e-9);
    }

    #[test]
    fn the_margin_must_outlast_the_drawdown_and_maintenance() {
        // A stop 35% below the mark is not carried by 35.5% of initial
        // margin: maintenance bites on the way down, and the real floor
        // is 35 + 1.2 × (1 − 0.35) = 35.78% — the same number the
        // collateral guard uses.
        let floor = liquidation_floor_pct(35.0, 1.2);
        assert!((floor - 35.78).abs() < 1e-9, "floor {floor}");
        assert!(35.5 < floor, "35.5% initial margin must NOT pass");
        assert!(50.0 >= floor, "2x (50%) carries it");
        // Closer stops need less, and the floor still exceeds the bare
        // distance at every level.
        for d in [5.0, 18.75, 35.0] {
            let f = liquidation_floor_pct(d, 1.2);
            assert!(f > d, "floor {f} must exceed the distance {d}");
        }
    }

    #[test]
    fn the_margin_fraction_bounds_how_far_a_stop_may_sit() {
        // Lighter reports the margin behind a position as a percentage:
        // "50.00" is 2x, "2.00" is 50x. A 35% stop needs at least 35%
        // margin behind it, or the venue accepts the transaction and
        // drops it (bot-strategy#950).
        let at = |imf: &str| {
            serde_json::json!({"accounts":[{"positions":[
                {"symbol":"BTC","initial_margin_fraction":imf}]}]})
        };
        assert_eq!(margin_fraction_for(&at("50.00"), "BTC"), Some(50.0)); // 2x
        assert_eq!(margin_fraction_for(&at("2.00"), "BTC"), Some(2.0)); // 50x
        assert_eq!(margin_fraction_for(&at("33.33"), "BTC"), Some(33.33)); // 3x
                                                                           // 2x carries a 35% stop; 3x and 50x do not.
        assert!(50.0 >= 35.0);
        assert!(33.33 < 35.0);
        // Anything unreadable is unknown, never a number.
        assert_eq!(margin_fraction_for(&at("50.00"), "ETH"), None);
        assert_eq!(
            margin_fraction_for(&serde_json::json!({"accounts":[{"positions":[]}]}), "BTC"),
            None
        );
        let numeric = serde_json::json!({"accounts":[{"positions":[
            {"symbol":"BTC","initial_margin_fraction":50.0}]}]});
        assert_eq!(
            margin_fraction_for(&numeric, "BTC"),
            None,
            "the venue sends it as a string"
        );
    }

    #[test]
    fn resting_orders_are_read_from_the_account_endpoint() {
        let flat = serde_json::json!({"accounts":[{"pending_order_count":0,"positions":[
            {"symbol":"BTC","open_order_count":0,"position_tied_order_count":0,"pending_order_count":0},
            {"symbol":"ETH","open_order_count":0,"position_tied_order_count":0,"pending_order_count":0}]}]});
        assert_eq!(resting_orders_for(&flat, "BTC"), Some(0));
        let resting = serde_json::json!({"accounts":[{"pending_order_count":0,"positions":[
            {"symbol":"BTC","open_order_count":1,"position_tied_order_count":0,"pending_order_count":0}]}]});
        assert_eq!(resting_orders_for(&resting, "BTC"), Some(1));
        let tied = serde_json::json!({"accounts":[{"pending_order_count":0,"positions":[
            {"symbol":"BTC","open_order_count":0,"position_tied_order_count":1,"pending_order_count":0}]}]});
        assert_eq!(resting_orders_for(&tied, "BTC"), Some(1));
        // The market's OWN pending count is this market's.
        let pending = serde_json::json!({"accounts":[{"pending_order_count":1,"positions":[
            {"symbol":"BTC","open_order_count":0,"position_tied_order_count":0,"pending_order_count":1}]}]});
        assert_eq!(resting_orders_for(&pending, "BTC"), Some(1));
        // Another market's order must NOT count as this one's: with BTC
        // covered, the account-wide pending is 1 while ETH is flat, and
        // folding it in left ETH's ghost stop unsettleable forever
        // (live, 2026-09-23).
        let other_market_covered = serde_json::json!({"accounts":[{"pending_order_count":1,"positions":[
            {"symbol":"BTC","open_order_count":1,"position_tied_order_count":1,"pending_order_count":1},
            {"symbol":"ETH","open_order_count":0,"position_tied_order_count":0,"pending_order_count":0}]}]});
        assert_eq!(resting_orders_for(&other_market_covered, "ETH"), Some(0));
        assert_eq!(resting_orders_for(&other_market_covered, "BTC"), Some(3));
        // A count that is missing, or not a number, makes the response
        // unreadable — it must never settle as "zero orders".
        let missing = serde_json::json!({"accounts":[{"pending_order_count":0,"positions":[
            {"symbol":"BTC","open_order_count":0,"position_tied_order_count":0}]}]});
        assert_eq!(resting_orders_for(&missing, "BTC"), None);
        let wrong_type = serde_json::json!({"accounts":[{"pending_order_count":0,"positions":[
            {"symbol":"BTC","open_order_count":"0","position_tied_order_count":0,"pending_order_count":0}]}]});
        assert_eq!(resting_orders_for(&wrong_type, "BTC"), None);
        // A complete market row answers on its own — the account-level
        // counters are about the whole account, not this market.
        let no_account_count = serde_json::json!({"accounts":[{"positions":[
            {"symbol":"BTC","open_order_count":0,"position_tied_order_count":0,"pending_order_count":0}]}]});
        assert_eq!(resting_orders_for(&no_account_count, "BTC"), Some(0));
        // An omitted market row settles only against account-wide counts:
        // all zero means nothing rests anywhere, so nothing rests here.
        let omitted_flat = serde_json::json!({"accounts":[{"total_order_count":0,
            "total_isolated_order_count":0,"pending_order_count":0,
            "positions":[{"symbol":"ETH","open_order_count":0}]}]});
        assert_eq!(resting_orders_for(&omitted_flat, "BTC"), Some(0));
        // ...but not while the account carries orders somewhere.
        let omitted_busy = serde_json::json!({"accounts":[{"total_order_count":1,
            "total_isolated_order_count":0,"pending_order_count":0,
            "positions":[{"symbol":"ETH","open_order_count":1}]}]});
        assert_eq!(resting_orders_for(&omitted_busy, "BTC"), Some(1));
        // Neither the market row nor a count: unknown, NOT zero.
        let other =
            serde_json::json!({"accounts":[{"positions":[{"symbol":"ETH","open_order_count":0}]}]});
        assert_eq!(resting_orders_for(&other, "BTC"), None);
        assert_eq!(
            resting_orders_for(&serde_json::json!({"code":500}), "BTC"),
            None
        );
    }

    /// The connector's order view is WebSocket-fed with no readiness
    /// flag, so "not listed" is ambiguous.
    /// A cache catching up answers "not listed" for moments; an order
    /// that is really gone answers that way forever. Measuring elapsed
    /// time rather than attempts keeps retry frequency — startup retries,
    /// a crash loop, a busy tick — out of the decision entirely.
    #[test]
    fn an_unlisted_stop_is_judged_by_elapsed_time_not_attempts() {
        let gone = |since: u64, now: u64| now.saturating_sub(since) >= STOP_UNLISTED_GRACE_SECS;
        let t0 = 1_000_000u64;
        // Moments, however many reads land in them, decide nothing.
        for t in [t0, t0 + 1, t0 + 60, t0 + STOP_UNLISTED_GRACE_SECS - 1] {
            assert!(!gone(t0, t), "must still defer at {}s", t - t0);
        }
        assert!(gone(t0, t0 + STOP_UNLISTED_GRACE_SECS));
        // Any interruption ends the observation, so the window restarts
        // from the next miss rather than resuming.
        let mut leg = LegState {
            stop_unlisted_since: Some(t0),
            ..Default::default()
        };
        leg.stop_unlisted_since = None; // seen again / unreadable / restart
        let restarted = *leg
            .stop_unlisted_since
            .get_or_insert(t0 + STOP_UNLISTED_GRACE_SECS);
        assert!(!gone(restarted, t0 + STOP_UNLISTED_GRACE_SECS));
        // It survives within a connection, so a slow cadence cannot
        // stretch the grace indefinitely either.
        let js = serde_json::to_string(&LegState {
            stop_unlisted_since: Some(t0),
            ..Default::default()
        })
        .unwrap();
        let back: LegState = serde_json::from_str(&js).unwrap();
        assert_eq!(back.stop_unlisted_since, Some(t0));
    }

    #[test]
    fn the_agent_owner_is_only_resolved_from_a_recognised_role() {
        let acct = "0x7a4c";
        // A sub-account names its master.
        let sub = serde_json::json!({"role":"subAccount","data":{"master":"0xa2c7"}});
        assert_eq!(agent_owner(&sub, acct).as_deref(), Some("0xa2c7"));
        // A master (or a plain user) answers for itself.
        for role in ["master", "user"] {
            let v = serde_json::json!({"role": role});
            assert_eq!(agent_owner(&v, acct).as_deref(), Some(acct));
        }
        // Anything unrecognised is NOT permission to query this account
        // as its own master: that finds no agents and would erase a valid
        // approval as conclusively absent.
        assert_eq!(agent_owner(&serde_json::json!({}), acct), None);
        assert_eq!(
            agent_owner(&serde_json::json!({"role":"vault"}), acct),
            None
        );
        assert_eq!(
            agent_owner(&serde_json::json!({"role":"subAccount"}), acct),
            None,
            "a sub-account with no master named is unreadable, not self-owned"
        );
    }

    #[test]
    fn the_expiry_is_attributed_to_the_signer_address_and_nothing_else() {
        use AgentLookup::*;
        let agents = serde_json::json!([
            {"name":"hype-accumulator","address":"0x07","validUntil":1804084485748i64},
            {"name":"bull-holder","address":"0x80","validUntil":1805606835174i64},
        ]);
        // The address picks the wallet, and the name in the result is
        // whatever the venue calls it.
        assert_eq!(
            agent_expiry(&agents, "0x80"),
            Found("bull-holder".into(), 1805606835)
        );
        assert_eq!(
            agent_expiry(&agents, "0X80"),
            Found("bull-holder".into(), 1805606835)
        );
        // A signer with no approval is conclusively absent, even while a
        // same-named wallet is still approved — that is the rotated-key
        // case this watch exists to catch.
        assert_eq!(agent_expiry(&agents, "0xdead"), Absent);
        assert_eq!(agent_expiry(&serde_json::json!([]), "0x80"), Absent);
        // Without an address there is nothing to attribute an approval
        // to. Never a name, never "the only one listed": both would
        // publish an expiry the signer may not hold.
        assert_eq!(agent_expiry(&agents, ""), Unreadable);
        let one =
            serde_json::json!([{"name":"solo","address":"0x11","validUntil":1805606835174i64}]);
        assert_eq!(agent_expiry(&one, ""), Unreadable);
        // A matched record without a usable expiry is not evidence of
        // absence — clearing a live date on a malformed field would be
        // the opposite of what this watch is for.
        assert_eq!(
            agent_expiry(&serde_json::json!([{"address":"0x80"}]), "0x80"),
            Unreadable
        );
        assert_eq!(
            agent_expiry(&serde_json::json!({"code":429}), "0x80"),
            Unreadable
        );
        // A record whose address cannot be read might be the one being
        // looked for, so the rest not matching is not proof of absence.
        let opaque = serde_json::json!([
            {"name":"other","address":"0x07","validUntil":1804084485748i64},
            {"name":"???","validUntil":1805606835174i64},
        ]);
        assert_eq!(agent_expiry(&opaque, "0x80"), Unreadable);
        // ...but a real match still wins over an unreadable neighbour.
        let mixed = serde_json::json!([
            {"name":"???","validUntil":1805606835174i64},
            {"name":"bull-holder","address":"0x80","validUntil":1805606835174i64},
        ]);
        assert_eq!(
            agent_expiry(&mixed, "0x80"),
            Found("bull-holder".into(), 1805606835)
        );
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
            lighter_mmr_pct: 1.2,
            perp_margin_min_pct: 47.55,
            tick_secs: 60,
            daily_eval_after_utc_secs: 300,
            max_close_fetch_failures: 3,
            hl_taker_slippage_bps: 30,
            stop_slippage_bps: 500,
            reconcile_tolerance_pct: 2.0,
            reconcile_every_secs: 600,
            lighter_account_url: "http://127.0.0.1:1".into(),
            lighter_account_index: "1".into(),
            lighter_wallet_address: String::new(),
            hl_account_address: String::new(),
            hl_agent_address: String::new(),
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
