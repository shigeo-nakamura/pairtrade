//! Cross-venue hedge holder (bot-strategy#1046) — Lighter on Robinhood Chain
//! long / Lighter Core short, held for the weekly points drop.
//!
//! Per symbol, two legs of equal size on two Lighter deployments:
//!
//! - **long leg** (default instance `rh`): perp long on `api.rh.lighter.xyz`,
//!   the leg the points program rewards ("hedge your Robinhood Chain longs
//!   on Lighter Core" — the venue's own weekly announcement).
//! - **short leg** (default instance `core`): perp short of the same size on
//!   `mainnet.zklighter.elliot.ai`, so the book carries no BTC delta.
//!
//! Funding on the two deployments has been identical to the hour (bot-
//! strategy#1046 Phase 0: 473/838 h equal, |diff| p95 0.07 bps/h), so the
//! long pays what the short receives and holding costs ~nothing; the only
//! costs are the two taker round trips (~1 bps of one leg) and the venue
//! basis (mean −1.3 bps, sd 1 bps). The bot therefore does as little as
//! possible: build both legs, keep them equal, and get out cleanly.
//!
//! **Venues (bot-strategy#1080).** Each leg's exchange is configurable:
//! `HEDGE_LONG_VENUE` / `HEDGE_SHORT_VENUE` ∈ {`lighter` (default), `arcus`}
//! (Arcus Perps). The instance (`HEDGE_*_INSTANCE`) selects that venue's
//! credentials and endpoints (`LIGHTER_*_<INSTANCE>` / `ARCUS_*_<INSTANCE>`,
//! see `config::get_arcus_config_from_env`). The intended use is RH-Lighter
//! long / Arcus short, so the short leg can earn Arcus points while the long
//! leg keeps earning RH points. Arcus reports markets as `BTC-USD`; books are
//! keyed by the bare symbol. An env without the venue variables behaves, and
//! fingerprints, exactly as before. A live leg on Arcus needs a second token,
//! `HEDGE_ARCUS_LIVE_CONFIRM=1080`, on top of `HEDGE_LIVE_CONFIRM`. Arcus
//! acknowledges orders asynchronously (HTTP 202), so an Arcus deployment
//! should lengthen the post-IOC position re-reads (`HEDGE_FILL_WAIT_SECS`,
//! default `2,4`, e.g. `3,6,10`). Arcus and Lighter funding are NOT
//! identical, unlike RH vs Core: the carry on the pair is a real (measured)
//! cost or income, not assumed zero.
//!
//! Several symbols (`HEDGE_SYMBOLS=BTC:1.2,META:3,...`, each with the
//! venue's maintenance-margin % for it, or `SYM:LONG_MMR/SHORT_MMR` when the
//! two legs' venues charge different MMRs, e.g. `BTC:1.2/1.667` for
//! RH-Lighter long / Arcus short) are held as independent books on
//! the same two accounts. Lighter margins cross-collateral, so the
//! liquidation and leverage guards are per ACCOUNT over every hedged book
//! (notional weighted by each symbol's MMR for that leg), never per symbol; a breach
//! closes every armed book. The legacy one-symbol env (`HEDGE_SYMBOL` +
//! `HEDGE_MMR_PCT`) and its state.json still load unchanged.
//!
//! Operator surface (files under `HEDGE_BASE_DIR/hedge`, default
//! /opt/debot-hedge-holder/hedge):
//! - `ARM`         one symbol: touch (optionally containing a per-leg
//!   notional in USD, else `HEDGE_TARGET_NOTIONAL_USD`); several symbols:
//!   one `SYMBOL USD` line per book to (re)arm → build (consumed). Any bad
//!   line rejects the whole file.
//! - `DISARM`      touch → close every book; `SYMBOL` lines → close those
//!   (consumed). DISARM beats ARM when both are present. A halted bot still
//!   honours DISARM: closing is the protective direction.
//! - `KILL_SWITCH` exists → no growth on either leg. Reductions still run.
//! - `RISK_ACK`    touch → clear a halt (consumed).
//!
//! Invariants the tick loop enforces, in this order:
//! 1. **Net exposure**: |long − short| × mark must stay under
//!    `HEDGE_NET_TOLERANCE_USD`. While it is over, exactly one order runs
//!    per tick and it levels the book: while building, the smaller leg is
//!    grown up to the larger one; otherwise (or while halted / KILL_SWITCH)
//!    the larger leg is reduced down to the smaller one. One IOC clip is
//!    the most the book can be lopsided by design: the legs are built and
//!    unwound in alternating clips of at most `HEDGE_CLIP_USD`, and the
//!    second leg of a tick is not sent when the first did not fill. Over
//!    tolerance for `HEDGE_NET_BREACH_TICKS` consecutive ticks → halt.
//! 2. **Liquidation guard**: each venue's equity is compared with its leg's
//!    notional. Below `HEDGE_LIQ_GUARD_PCT` of headroom on either side both
//!    legs are closed and the bot halts (`liq_guard`). Cross-venue margin is
//!    not shared: a long-side gain cannot rescue a short-side liquidation,
//!    so a lopsided liquidation is the one way this book can lose real
//!    money, and the guard fires well before the venue would.
//! 3. **Leverage guard**: growth is refused (halt `leverage`) when a leg's
//!    notional after the order would exceed `HEDGE_MAX_LEVERAGE` × equity,
//!    or — only while the Arcus-leg roll is enabled — (halt `growth
//!    headroom`) when it would leave that venue's liquidation headroom
//!    under `HEDGE_LIQ_GUARD_PCT`.
//!    Checked for the whole tick's orders before the first is sent.
//!
//! `Off` never trades: whatever the venues hold in `Off` (an operator's
//! own position, a live book whose `state.json` was lost) is left alone
//! until an ARM adopts it. A `state.json` written in the other mode
//! (DRY_RUN ↔ live) is dropped to `Off` at startup. While the two venues'
//! marks disagree by more than 2 % nothing is sent and status says why.
//!
//! DRY_RUN (default) never sends an order: fills are assumed at the mark and
//! the book is kept in `state.json`; venue equity is `HEDGE_DRY_RUN_EQUITY_USD`.
//! Live needs `HEDGE_DRY_RUN=false` **and** `HEDGE_LIVE_CONFIRM=1046-G0-PASSED`
//! (the pre-registered G0 readout, bot-strategy#1046), so a stray env flip
//! cannot go live on its own.
//!
//! Status: `status.json` next to the sentinels every tick, mirrored to
//! `HEDGE_STATUS_S3_URI` (if set) every `HEDGE_STATUS_S3_EVERY_SECS`. Fills,
//! arms, halts and exits are appended to `events.jsonl`.

use anyhow::{anyhow, bail, Context, Result};
use debot::directional::{append_jsonl, config_fingerprint, load_json, persist_json, Sentinels};
use debot::trade::execution::dex_connector_box::DexConnectorBox;
use dex_connector::{DexConnector, DexError, OrderSide};
use env_logger::Builder;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const BOT: &str = "xvenue_hedge_holder";
const LIVE_CONFIRM_TOKEN: &str = "1046-G0-PASSED";
/// Second live gate for any leg on Arcus Perps (bot-strategy#1080): the
/// venue's execution path is new, so a stray `HEDGE_*_VENUE=arcus` flip on a
/// live deployment must not trade without an explicit, separate token.
const ARCUS_LIVE_CONFIRM_TOKEN: &str = "1080";
/// Default position re-reads after an IOC (`HEDGE_FILL_WAIT_SECS`).
const DEFAULT_FILL_WAIT_SECS: &[u64] = &[2, 4];

fn init_logger() {
    Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                record.level(),
                record.args()
            )
        })
        .init();
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `true` only for a finite value strictly above zero (NaN is not).
fn positive(v: f64) -> bool {
    v.is_finite() && v > 0.0
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(default)
}

// ------------------------------------------------------------------ config

/// One hedged market and the maintenance-margin fraction the venue charges
/// on it (%, `maintenance_margin_fraction` / 100 in Lighter's
/// orderBookDetails: BTC/ETH 1.2, most single stocks 3 or 6). The guards
/// weight each symbol's notional by it, so it is required per symbol —
/// a stock booked at the BTC rate would overstate the headroom 2.5–5x.
///
/// The two legs may sit on venues with different MMRs (Arcus BTC 1.667 %
/// vs Lighter 1.2 %, Arcus stocks 6.667 %): `SYM:LONG/SHORT` gives each leg
/// its own, and the liquidation / headroom guard of a leg's account uses
/// that leg's figure. `SYM:MMR` applies one MMR to both legs (use the max of
/// the two venues when they differ).
#[derive(Debug, Clone, PartialEq)]
struct SymbolCfg {
    symbol: String,
    /// Long-leg MMR (%). Also the single-MMR value of the legacy forms.
    mmr_pct: f64,
    /// Short-leg MMR (%); equals `mmr_pct` unless `SYM:LONG/SHORT` is used.
    short_mmr_pct: f64,
}

impl SymbolCfg {
    fn mmr(&self, leg: Leg) -> f64 {
        match leg {
            Leg::Long => self.mmr_pct,
            Leg::Short => self.short_mmr_pct,
        }
    }

    /// `SYM:MMR`, or `SYM:LONG/SHORT` when the legs differ — the form the
    /// fingerprint and logs use, identical to the legacy one when they don't.
    fn spec(&self, decimals: usize) -> String {
        if self.short_mmr_pct == self.mmr_pct {
            format!("{}:{:.*}", self.symbol, decimals, self.mmr_pct)
        } else {
            format!(
                "{}:{:.*}/{:.*}",
                self.symbol, decimals, self.mmr_pct, decimals, self.short_mmr_pct
            )
        }
    }
}

/// `HEDGE_SYMBOLS` = `SYM:MMR[,SYM:MMR...]`, e.g. `BTC:1.2,META:3,AMZN:6`;
/// a per-leg pair is `SYM:LONG_MMR/SHORT_MMR`, e.g. `BTC:1.2/1.667`.
/// Symbols are upper-cased; the first one is the primary (the one the
/// single-symbol status fields describe).
fn parse_symbols(spec: &str) -> Result<Vec<SymbolCfg>> {
    let mut out: Vec<SymbolCfg> = Vec::new();
    for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (sym, mmr) = item
            .split_once(':')
            .ok_or_else(|| anyhow!("HEDGE_SYMBOLS entry '{item}' needs SYMBOL:MMR_PCT"))?;
        let symbol = sym.trim().to_ascii_uppercase();
        let num = |raw: &str| -> Result<f64> {
            raw.trim()
                .parse()
                .map_err(|_| anyhow!("HEDGE_SYMBOLS entry '{item}': MMR_PCT is not a number"))
        };
        let (mmr_pct, short_mmr_pct) = match mmr.split_once('/') {
            Some((l, s)) => (num(l)?, num(s)?),
            None => {
                let v = num(mmr)?;
                (v, v)
            }
        };
        if symbol.is_empty() || !positive(mmr_pct) || !positive(short_mmr_pct) {
            bail!("HEDGE_SYMBOLS entry '{item}': empty symbol or MMR_PCT <= 0");
        }
        if out.iter().any(|c| c.symbol == symbol) {
            bail!("HEDGE_SYMBOLS lists {symbol} twice");
        }
        out.push(SymbolCfg {
            symbol,
            mmr_pct,
            short_mmr_pct,
        });
    }
    if out.is_empty() {
        bail!("HEDGE_SYMBOLS is empty");
    }
    Ok(out)
}

/// Which exchange a leg trades on (`HEDGE_LONG_VENUE` / `HEDGE_SHORT_VENUE`).
/// The instance id (`HEDGE_*_INSTANCE`) picks that venue's credentials and
/// endpoints (`LIGHTER_*_<INSTANCE>` / `ARCUS_*_<INSTANCE>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum VenueKind {
    #[default]
    Lighter,
    Arcus,
}

impl VenueKind {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "lighter" => Ok(Self::Lighter),
            "arcus" => Ok(Self::Arcus),
            other => bail!("unknown venue '{other}' (expected lighter or arcus)"),
        }
    }

    /// The `DexConnectorBox::create` name.
    fn dex_name(self) -> &'static str {
        match self {
            Self::Lighter => "lighter",
            Self::Arcus => "arcus",
        }
    }
}

/// Symbol as a venue reports it → the holder's book key: upper-case, with
/// Arcus' `-USD` market suffix removed (`BTC-USD` → `BTC`). Lighter already
/// reports bare symbols.
fn book_symbol(venue_symbol: &str) -> String {
    let up = venue_symbol.trim().to_ascii_uppercase();
    match up.strip_suffix("-USD") {
        Some(base) if !base.is_empty() => base.to_string(),
        _ => up,
    }
}

/// `HEDGE_FILL_WAIT_SECS` = comma list of the waits (s) before each position
/// re-read after an IOC, e.g. `2,4` (default) or `3,6,10` for a venue whose
/// order ack is asynchronous (Arcus answers 202 before the engine fills).
fn parse_fill_waits(spec: &str) -> Result<Vec<u64>> {
    let waits = spec
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| anyhow!("HEDGE_FILL_WAIT_SECS entry '{s}' is not a whole number"))
        })
        .collect::<Result<Vec<_>>>()?;
    if waits.is_empty() || waits.contains(&0) {
        bail!("HEDGE_FILL_WAIT_SECS needs one or more waits > 0");
    }
    if waits.iter().sum::<u64>() > 60 {
        bail!("HEDGE_FILL_WAIT_SECS totals more than 60 s");
    }
    Ok(waits)
}

#[derive(Debug, Clone)]
struct Config {
    dry_run: bool,
    live_confirm: String,
    /// Hedged markets, primary first (`HEDGE_SYMBOLS`, or the legacy
    /// `HEDGE_SYMBOL` + `HEDGE_MMR_PCT` pair as a one-symbol list).
    symbols: Vec<SymbolCfg>,
    /// Exchange of the long leg (`HEDGE_LONG_VENUE`, default lighter).
    long_venue: VenueKind,
    /// Exchange of the short leg (`HEDGE_SHORT_VENUE`, default lighter).
    short_venue: VenueKind,
    /// Env suffix of the long leg (credentials + endpoints).
    long_instance: String,
    /// Env suffix of the short leg.
    short_instance: String,
    /// `HEDGE_ARCUS_LIVE_CONFIRM`: must equal `ARCUS_LIVE_CONFIRM_TOKEN`
    /// when a leg is on Arcus and the bot is live.
    arcus_live_confirm: String,
    /// Waits (s) before each position re-read after an IOC.
    fill_wait_secs: Vec<u64>,
    /// Seconds an uncertain order may stay unsettled before the bot halts
    /// for the operator (`HEDGE_UNCERTAIN_GRACE_SECS`, default 60). It is
    /// never released on time alone — only on terminal evidence or RISK_ACK.
    uncertain_grace_secs: u64,
    /// Arcus-leg roll (bot-strategy#1080), default off. See `RollCfg`.
    roll: RollCfg,
    /// Per-leg notional built on a bare ARM (USD); single-symbol only.
    target_notional_usd: f64,
    /// Hard cap on the per-leg notional any ARM may request, per symbol (USD).
    max_notional_usd: f64,
    /// Largest single IOC (USD); the legs grow/shrink in alternating clips.
    clip_usd: f64,
    /// Taker IOC bound, bps past the touch (dex-connector rounds inward).
    taker_slippage_bps: u32,
    /// |long − short| × mark tolerated per symbol before growth stops (USD).
    net_tolerance_usd: f64,
    /// Consecutive ticks over tolerance that halt the bot.
    net_breach_ticks: u32,
    /// Account headroom (%) below which every armed book is closed.
    liq_guard_pct: f64,
    /// Refuse growth beyond this gross notional / equity per venue.
    max_leverage: f64,
    dry_run_equity_usd: f64,
    tick_secs: u64,
    status_s3_uri: String,
    status_s3_every_secs: u64,
    /// The points collector's history file (bot-strategy#938); the row
    /// series for the long leg's account feeds the `subsidy` block.
    points_history_path: PathBuf,
    /// Account index of the long leg (`LIGHTER_ACCOUNT_INDEX_<LONG>`),
    /// used only to pick that account's rows out of the points history.
    long_account_index: Option<u64>,
    arm_path: PathBuf,
    disarm_path: PathBuf,
    kill_switch_path: PathBuf,
    risk_ack_path: PathBuf,
    state_path: PathBuf,
    status_path: PathBuf,
    events_path: PathBuf,
}

impl Config {
    fn from_env() -> Result<Self> {
        let base_dir = PathBuf::from(env_string("HEDGE_BASE_DIR", "/opt/debot-hedge-holder"));
        let dir = base_dir.join("hedge");
        let symbols = match std::env::var("HEDGE_SYMBOLS")
            .ok()
            .filter(|v| !v.trim().is_empty())
        {
            Some(spec) => parse_symbols(&spec)?,
            None => {
                let mmr = env_f64("HEDGE_MMR_PCT", 1.2);
                vec![SymbolCfg {
                    symbol: env_string("HEDGE_SYMBOL", "BTC").to_ascii_uppercase(),
                    mmr_pct: mmr,
                    short_mmr_pct: mmr,
                }]
            }
        };
        check_roll_mode(
            &env_string("HEDGE_ROLL_MODE", ""),
            env_bool("HEDGE_ROLL_ENABLED", false),
        )?;
        let cfg = Self {
            dry_run: env_bool("HEDGE_DRY_RUN", true),
            live_confirm: env_string("HEDGE_LIVE_CONFIRM", ""),
            symbols,
            long_venue: VenueKind::parse(&env_string("HEDGE_LONG_VENUE", "lighter"))
                .context("HEDGE_LONG_VENUE")?,
            short_venue: VenueKind::parse(&env_string("HEDGE_SHORT_VENUE", "lighter"))
                .context("HEDGE_SHORT_VENUE")?,
            long_instance: env_string("HEDGE_LONG_INSTANCE", "rh"),
            short_instance: env_string("HEDGE_SHORT_INSTANCE", "core"),
            arcus_live_confirm: env_string("HEDGE_ARCUS_LIVE_CONFIRM", ""),
            fill_wait_secs: match std::env::var("HEDGE_FILL_WAIT_SECS")
                .ok()
                .filter(|v| !v.trim().is_empty())
            {
                Some(spec) => parse_fill_waits(&spec)?,
                None => DEFAULT_FILL_WAIT_SECS.to_vec(),
            },
            uncertain_grace_secs: env_u64("HEDGE_UNCERTAIN_GRACE_SECS", 60),
            roll: RollCfg {
                enabled: env_bool("HEDGE_ROLL_ENABLED", false),
                interval_secs: env_u64("HEDGE_ROLL_INTERVAL_SECS", 3_600),
                clip_usd: env_f64("HEDGE_ROLL_CLIP_USD", 0.0),
                weekly_volume_usd: env_f64("HEDGE_ROLL_WEEKLY_VOLUME_USD", 0.0),
                weekly_cost_usd: env_f64("HEDGE_ROLL_WEEKLY_COST_USD", 0.0),
            },
            target_notional_usd: env_f64("HEDGE_TARGET_NOTIONAL_USD", 20_000.0),
            max_notional_usd: env_f64("HEDGE_MAX_NOTIONAL_USD", 30_000.0),
            clip_usd: env_f64("HEDGE_CLIP_USD", 10_000.0),
            taker_slippage_bps: env_u32("HEDGE_TAKER_SLIPPAGE_BPS", 3),
            net_tolerance_usd: env_f64("HEDGE_NET_TOLERANCE_USD", 500.0),
            net_breach_ticks: env_u32("HEDGE_NET_BREACH_TICKS", 3),
            liq_guard_pct: env_f64("HEDGE_LIQ_GUARD_PCT", 8.0),
            max_leverage: env_f64("HEDGE_MAX_LEVERAGE", 6.0),
            dry_run_equity_usd: env_f64("HEDGE_DRY_RUN_EQUITY_USD", 4_000.0),
            tick_secs: env_u64("HEDGE_TICK_SECS", 30),
            status_s3_uri: env_string("HEDGE_STATUS_S3_URI", ""),
            status_s3_every_secs: env_u64("HEDGE_STATUS_S3_EVERY_SECS", 60),
            points_history_path: PathBuf::from(env_string(
                "HEDGE_POINTS_HISTORY_PATH",
                "/home/ec2-user/debot_status/robinhood-points/points_history.jsonl",
            )),
            long_account_index: {
                let suffix = env_string("HEDGE_LONG_INSTANCE", "rh")
                    .to_uppercase()
                    .replace('-', "_");
                points_account_index(
                    VenueKind::parse(&env_string("HEDGE_LONG_VENUE", "lighter")).ok(),
                    std::env::var(format!("LIGHTER_ACCOUNT_INDEX_{suffix}")).ok(),
                )
            },
            arm_path: dir.join("ARM"),
            disarm_path: dir.join("DISARM"),
            kill_switch_path: dir.join("KILL_SWITCH"),
            risk_ack_path: dir.join("RISK_ACK"),
            state_path: dir.join("state.json"),
            status_path: dir.join("status.json"),
            events_path: dir.join("events.jsonl"),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.long_venue == self.short_venue
            && self
                .long_instance
                .eq_ignore_ascii_case(&self.short_instance)
        {
            bail!("the two legs name the same venue and instance (HEDGE_*_VENUE / HEDGE_*_INSTANCE): two accounts, two venues");
        }
        if self.fill_wait_secs.is_empty() || self.fill_wait_secs.contains(&0) {
            bail!("HEDGE_FILL_WAIT_SECS needs one or more waits > 0");
        }
        if self.symbols.is_empty() {
            bail!("no hedged symbol configured");
        }
        // `parse_symbols` checks its own entries; the legacy HEDGE_SYMBOL +
        // HEDGE_MMR_PCT pair is built directly and must pass the same bar
        // (a NaN MMR makes every headroom comparison false while holding).
        for c in &self.symbols {
            if c.symbol.is_empty() || !positive(c.mmr_pct) || !positive(c.short_mmr_pct) {
                bail!(
                    "symbol '{}': empty name or MMR_PCT <= 0 / non-finite (HEDGE_SYMBOLS or HEDGE_MMR_PCT)",
                    c.symbol
                );
            }
        }
        if !positive(self.target_notional_usd) || !positive(self.max_notional_usd) {
            bail!("HEDGE_TARGET_NOTIONAL_USD and HEDGE_MAX_NOTIONAL_USD must be > 0");
        }
        if self.target_notional_usd > self.max_notional_usd {
            bail!("HEDGE_TARGET_NOTIONAL_USD exceeds HEDGE_MAX_NOTIONAL_USD");
        }
        if !positive(self.clip_usd) {
            bail!("HEDGE_CLIP_USD must be > 0");
        }
        if self.taker_slippage_bps == 0 || self.taker_slippage_bps > 50 {
            bail!("HEDGE_TAKER_SLIPPAGE_BPS must be 1..=50");
        }
        // Headroom is measured ABOVE the maintenance requirement (it already
        // subtracts every book's MMR), so the guard only has to be positive:
        // a 5 % guard with a 6 % MMR stock is a valid setting.
        if !positive(self.liq_guard_pct) {
            bail!("HEDGE_LIQ_GUARD_PCT must be > 0");
        }
        if !self.max_leverage.is_finite() || self.max_leverage < 1.0 {
            bail!("HEDGE_MAX_LEVERAGE must be >= 1");
        }
        if self.tick_secs == 0 || self.net_breach_ticks == 0 {
            bail!("HEDGE_TICK_SECS and HEDGE_NET_BREACH_TICKS must be > 0");
        }
        if self.uncertain_grace_secs == 0 {
            bail!("HEDGE_UNCERTAIN_GRACE_SECS must be > 0");
        }
        if self.roll.enabled {
            if self.roll_leg().is_none() {
                bail!("HEDGE_ROLL_ENABLED needs an Arcus leg (the Lighter leg is never rolled)");
            }
            if !positive(self.roll.clip_usd) {
                bail!("HEDGE_ROLL_CLIP_USD must be > 0 when the roll is enabled");
            }
            if self.roll.clip_usd > self.clip_usd {
                bail!("HEDGE_ROLL_CLIP_USD must be <= HEDGE_CLIP_USD");
            }
            // Between the close and the re-open the book is one roll clip
            // lopsided; above the net tolerance that would count as a
            // breach (and a slow re-open could halt the bot).
            if self.roll.clip_usd > self.net_tolerance_usd {
                bail!("HEDGE_ROLL_CLIP_USD must be <= HEDGE_NET_TOLERANCE_USD");
            }
            if !positive(self.roll.weekly_volume_usd) || !positive(self.roll.weekly_cost_usd) {
                bail!("HEDGE_ROLL_WEEKLY_VOLUME_USD and HEDGE_ROLL_WEEKLY_COST_USD must be > 0 when the roll is enabled");
            }
            if self.roll.interval_secs < self.tick_secs {
                bail!("HEDGE_ROLL_INTERVAL_SECS must be >= HEDGE_TICK_SECS");
            }
        }
        if !self.dry_run && self.live_confirm != LIVE_CONFIRM_TOKEN {
            bail!(
                "HEDGE_DRY_RUN=false needs HEDGE_LIVE_CONFIRM={LIVE_CONFIRM_TOKEN} \
                 (the bot-strategy#1046 G0 readout is the gate)"
            );
        }
        if !self.dry_run && self.uses_arcus() && self.arcus_live_confirm != ARCUS_LIVE_CONFIRM_TOKEN
        {
            bail!(
                "a live leg on Arcus needs HEDGE_ARCUS_LIVE_CONFIRM={ARCUS_LIVE_CONFIRM_TOKEN} \
                 (bot-strategy#1080: the Arcus execution path is gated separately)"
            );
        }
        Ok(())
    }

    fn uses_arcus(&self) -> bool {
        self.long_venue == VenueKind::Arcus || self.short_venue == VenueKind::Arcus
    }

    /// The leg whose executions book into the roll week: the roll leg, but
    /// only while the roll is enabled (disabled, the roll counters stay
    /// untouched — nothing books, nothing grows).
    fn roll_accounting_leg(&self) -> Option<Leg> {
        if self.roll.enabled {
            self.roll_leg()
        } else {
            None
        }
    }

    /// The leg the roll trades: the Arcus leg (the short when both are
    /// Arcus). `None` when no leg is on Arcus — the Lighter leg is never
    /// rolled.
    fn roll_leg(&self) -> Option<Leg> {
        if self.short_venue == VenueKind::Arcus {
            Some(Leg::Short)
        } else if self.long_venue == VenueKind::Arcus {
            Some(Leg::Long)
        } else {
            None
        }
    }

    fn venue(&self, leg: Leg) -> VenueKind {
        match leg {
            Leg::Long => self.long_venue,
            Leg::Short => self.short_venue,
        }
    }

    fn primary(&self) -> &str {
        &self.symbols[0].symbol
    }

    fn symbol_names(&self) -> Vec<String> {
        self.symbols.iter().map(|c| c.symbol.clone()).collect()
    }

    fn fingerprint(&self) -> String {
        // A one-symbol config hashes exactly the fields it did before the
        // multi-symbol change, so an unchanged BTC deployment keeps its fp.
        let mut fields = vec![
            ("symbol", self.primary().to_string()),
            ("long", self.long_instance.clone()),
            ("short", self.short_instance.clone()),
            ("target", format!("{:.2}", self.target_notional_usd)),
            ("max_notional", format!("{:.2}", self.max_notional_usd)),
            ("clip", format!("{:.2}", self.clip_usd)),
            ("slip_bps", self.taker_slippage_bps.to_string()),
            ("net_tol", format!("{:.2}", self.net_tolerance_usd)),
            ("mmr", format!("{:.3}", self.symbols[0].mmr_pct)),
            ("liq_guard", format!("{:.3}", self.liq_guard_pct)),
            ("max_lev", format!("{:.2}", self.max_leverage)),
        ];
        // Fields added after 7b1849e are hashed only when they differ from
        // the default, so the running RH-long / Core-short Lighter deployment
        // keeps its fingerprint (bot-strategy#1080).
        if self.long_venue != VenueKind::Lighter {
            fields.push(("long_venue", self.long_venue.dex_name().to_string()));
        }
        if self.short_venue != VenueKind::Lighter {
            fields.push(("short_venue", self.short_venue.dex_name().to_string()));
        }
        if self.roll.enabled {
            fields.push((
                "roll",
                format!(
                    "{}:{:.2}:{:.2}:{:.2}",
                    self.roll.interval_secs,
                    self.roll.clip_usd,
                    self.roll.weekly_volume_usd,
                    self.roll.weekly_cost_usd,
                ),
            ));
        }
        if self.symbols.len() == 1 && self.symbols[0].short_mmr_pct != self.symbols[0].mmr_pct {
            fields.push(("short_mmr", format!("{:.3}", self.symbols[0].short_mmr_pct)));
        }
        if self.symbols.len() > 1 {
            fields.push((
                "symbols",
                self.symbols
                    .iter()
                    .map(|c| c.spec(3))
                    .collect::<Vec<_>>()
                    .join(","),
            ));
        }
        config_fingerprint(&fields)
    }
}

// ------------------------------------------------------------------- state

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum Mode {
    /// Nothing held, nothing wanted.
    #[default]
    Off,
    /// Book wanted at `target_qty`; the tick loop keeps both legs there.
    On,
    /// Closed by DISARM or a guard; stays here until the next ARM.
    Exited,
}

/// One symbol's book.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
struct SymBook {
    mode: Mode,
    /// Per-leg size the book is being held at (base units).
    target_qty: f64,
    /// Per-leg notional the operator asked for (USD, at ARM).
    target_notional_usd: f64,
    /// DRY_RUN book (live reads the venues instead).
    dry_long_qty: f64,
    dry_short_qty: f64,
    armed_at: Option<u64>,
    exited_at: Option<u64>,
    exit_reason: Option<String>,
    net_breach_ticks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct State {
    /// Per-symbol books, keyed by upper-case symbol.
    #[serde(default)]
    books: std::collections::BTreeMap<String, SymBook>,
    halted: bool,
    halt_reason: Option<String>,
    /// When the current campaign was armed: the first ARM while no book
    /// was On. The equity and points baselines below belong to it.
    armed_at: Option<u64>,
    /// Sum of both venues' equity when the current campaign was armed.
    equity_at_arm_usd: Option<f64>,
    cycles: u64,
    /// Long-leg live points when the current campaign was armed (from the
    /// collector's history), so `subsidy.units_total` counts it only.
    points_at_arm: Option<f64>,
    /// UTC day the day-start equity below belongs to, and that equity.
    day_key: Option<String>,
    equity_day_start_usd: Option<f64>,
    process_started_at: Option<u64>,
    /// The mode this state was written in. A state armed under DRY_RUN
    /// must not carry its target into a live start (or vice versa).
    dry_run: Option<bool>,
    /// IOCs whose outcome is not known yet, per symbol (bot-strategy#1080,
    /// Codex on pairtrade#356). Arcus acks asynchronously (202): a fill can
    /// become visible after the settle waits, so the order is recorded here
    /// and NOTHING more is sent on that symbol until a later tick sees it
    /// terminal — re-sending is the one way the book can overshoot.
    #[serde(default)]
    uncertain: std::collections::BTreeMap<String, UncertainOrder>,
    /// Arcus-leg roll counters (bot-strategy#1080).
    #[serde(default)]
    roll: RollState,
    // Single-symbol state.json (before `books`): read once by
    // `migrate_legacy`, never written back.
    #[serde(default, rename = "mode", skip_serializing)]
    legacy_mode: Option<Mode>,
    #[serde(default, rename = "target_qty", skip_serializing)]
    legacy_target_qty: Option<f64>,
    #[serde(default, rename = "target_notional_usd", skip_serializing)]
    legacy_target_notional_usd: Option<f64>,
    #[serde(default, rename = "dry_long_qty", skip_serializing)]
    legacy_dry_long_qty: Option<f64>,
    #[serde(default, rename = "dry_short_qty", skip_serializing)]
    legacy_dry_short_qty: Option<f64>,
    #[serde(default, rename = "exited_at", skip_serializing)]
    legacy_exited_at: Option<u64>,
    #[serde(default, rename = "exit_reason", skip_serializing)]
    legacy_exit_reason: Option<String>,
    #[serde(default, rename = "net_breach_ticks", skip_serializing)]
    legacy_net_breach_ticks: Option<u32>,
}

impl State {
    fn book(&self, symbol: &str) -> SymBook {
        self.books.get(symbol).cloned().unwrap_or_default()
    }

    fn book_mut(&mut self, symbol: &str) -> &mut SymBook {
        self.books.entry(symbol.to_string()).or_default()
    }

    fn any_mode(&self, mode: Mode) -> bool {
        self.books.values().any(|b| b.mode == mode)
    }

    /// Aggregate mode for the card: On if any book is On, else Exited if
    /// any is still closing, else Off.
    fn overall_mode(&self) -> Mode {
        if self.any_mode(Mode::On) {
            Mode::On
        } else if self.any_mode(Mode::Exited) {
            Mode::Exited
        } else {
            Mode::Off
        }
    }
}

/// A pre-`books` state.json described one book, the (then only) symbol's.
/// It becomes that symbol's book — the primary one — so a BTC book armed
/// before the upgrade stays armed across it.
fn migrate_legacy(mut state: State, primary: &str) -> State {
    let legacy = SymBook {
        mode: state.legacy_mode.take().unwrap_or_default(),
        target_qty: state.legacy_target_qty.take().unwrap_or(0.0),
        target_notional_usd: state.legacy_target_notional_usd.take().unwrap_or(0.0),
        dry_long_qty: state.legacy_dry_long_qty.take().unwrap_or(0.0),
        dry_short_qty: state.legacy_dry_short_qty.take().unwrap_or(0.0),
        armed_at: state.armed_at,
        exited_at: state.legacy_exited_at.take(),
        exit_reason: state.legacy_exit_reason.take(),
        net_breach_ticks: state.legacy_net_breach_ticks.take().unwrap_or(0),
    };
    if state.books.is_empty() && legacy != SymBook::default() {
        let armed_at = legacy.armed_at;
        state.books.insert(primary.to_string(), legacy);
        state.armed_at = armed_at;
    }
    state
}

/// An IOC whose terminal state was not confirmed within the settle waits,
/// or whose submission the venue answered ambiguously
/// (`DexError::ReconciliationRequired`, then without an order id).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UncertainOrder {
    leg: Leg,
    exchange: String,
    /// Connector instance (account) the order was sent on; checked before
    /// reconciling so a venue/instance change never reads another account.
    #[serde(default)]
    instance: String,
    order_id: Option<String>,
    /// Requested size (base units).
    qty: f64,
    /// The leg's signed venue position right before the order.
    before_qty: f64,
    /// Mark the order was sent against (roll accounting: slippage of the
    /// fills booked when the order is released). 0 when unknown.
    #[serde(default)]
    mark: f64,
    sent_at: u64,
    reason: String,
    /// A ROLL leg (its in-flight reservation is settled when this entry is
    /// released / cleared); planner orders never touch roll reservations.
    #[serde(default)]
    roll: bool,
}

// ------------------------------------------------------------------- roll

/// `HEDGE_ROLL_MODE` is a leftover of the removed `maker_first` mode: the
/// roll is taker-only (IOC legs never rest, so nothing can be orphaned on
/// a restart). Unset / `taker` are fine; any other value is a config error
/// while the roll is enabled (it would silently change what the operator
/// asked for) and is only logged while the roll is off, so it can never
/// keep the delta-neutral holder from starting.
fn check_roll_mode(raw: &str, roll_enabled: bool) -> Result<()> {
    let v = raw.trim().to_ascii_lowercase();
    if v.is_empty() || v == "taker" {
        return Ok(());
    }
    if roll_enabled {
        bail!("HEDGE_ROLL_MODE={raw}: maker_first was removed (bot-strategy#1080) — the roll is taker-only; unset HEDGE_ROLL_MODE");
    }
    log::warn!("[CONFIG] HEDGE_ROLL_MODE={raw} ignored (roll disabled; the roll is taker-only)");
    Ok(())
}

/// Arcus-leg roll settings (`HEDGE_ROLL_*`). Off by default: enabling it is
/// the owner's call once the Arcus points terms are known (#1075, #1080).
#[derive(Debug, Clone, Default)]
struct RollCfg {
    enabled: bool,
    interval_secs: u64,
    clip_usd: f64,
    weekly_volume_usd: f64,
    weekly_cost_usd: f64,
}

/// Roll bookkeeping in state.json (serde default: older states load).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
struct RollState {
    /// Day index (days since 1970-01-01) of the Sunday the current roll
    /// week started on.
    week: u64,
    /// ACTUAL executed value / cost this week of every confirmed execution
    /// on the rolled (Arcus) leg: roll legs AND the planner's levelling,
    /// repairs and ARM builds — each books itself when it settles.
    week_volume_usd: f64,
    week_cost_usd: f64,
    /// Worst-case reservations (volume, cost) of roll legs sent but not yet
    /// settled, per symbol. Set before a roll leg is sent and cleared when
    /// it settles (its fills are booked where they settle); a leg that ends
    /// uncertain keeps its reservation until the uncertain entry is released
    /// (observed fills booked) or cleared by RISK_ACK (worst case booked).
    /// Not part of the week counters: it survives rollovers on its own.
    #[serde(default)]
    in_flight: std::collections::BTreeMap<String, (f64, f64)>,
    /// Rolls that sent at least one order this week (refusals before any
    /// send — price moved, book read failed — are not rolls).
    week_count: u64,
    /// When the last roll actually SENT an order; the interval counts from here.
    last_roll_at: Option<u64>,
    /// Per-SYMBOL refusal streaks: consecutive roll attempts on that symbol
    /// refused or rejected before anything was sent (reset by a roll of that
    /// symbol that sends, and by the week rollover). Each one doubles that
    /// symbol's backoff; `ROLL_MAX_REFUSALS` in a row suspend THAT symbol
    /// until the next roll week — the other books keep rolling.
    #[serde(default)]
    backoff: std::collections::BTreeMap<String, RollBackoff>,
    last_symbol: Option<String>,
    blocked_reason: Option<String>,
}

/// One symbol's refusal streak.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
struct RollBackoff {
    refusals: u32,
    /// No roll of this symbol before this time.
    retry_not_before: u64,
}

/// Sunday-00:00-UTC week of `now`, as the day index of that Sunday. The
/// Arcus points week is ASSUMED to run Sun 00:00 UTC (the points API's
/// weekly history starts on Sunday 2026-09-13, see bot-strategy#1075).
fn roll_week(now: u64) -> u64 {
    let day = now / 86_400;
    // 1970-01-01 was a Thursday: (day + 4) % 7 == 0 on Sundays.
    day - (day + 4) % 7
}

/// Resets the weekly counters when a new roll week has started.
fn roll_week_rollover(rs: &mut RollState, now: u64) -> bool {
    let week = roll_week(now);
    if rs.week != week {
        // Actuals restart at zero; in-flight reservations (uncertain roll
        // legs) are kept as they are until released.
        *rs = RollState {
            week,
            in_flight: std::mem::take(&mut rs.in_flight),
            last_roll_at: rs.last_roll_at,
            last_symbol: rs.last_symbol.clone(),
            ..RollState::default()
        };
        return true;
    }
    false
}

/// Sum of the in-flight reservations (volume, cost).
fn roll_in_flight_total(rs: &RollState) -> (f64, f64) {
    rs.in_flight
        .values()
        .fold((0.0, 0.0), |(v, c), (dv, dc)| (v + dv, c + dc))
}

/// THE booking entry point: a confirmed execution's actual (value, cost)
/// goes into the week of the BOOKING time (rollover first), so a roll that
/// started before Sunday 00:00 UTC and settles after it lands in the new
/// week, not in the old one the next tick's rollover would wipe.
fn roll_book_actual(rs: &mut RollState, now: u64, (value, cost): (f64, f64)) {
    roll_week_rollover(rs, now);
    rs.week_volume_usd += value;
    rs.week_cost_usd += cost;
}

/// One settled ROLL leg, as a single state mutation: its in-flight
/// reservation is dropped and its actual fills (if any) booked together, so
/// a crash between two persists can never leave both on disk (the restart
/// would book the stale reservation on top of the actual).
fn roll_settle_leg(rs: &mut RollState, sym: &str, now: u64, booking: Option<(f64, f64)>) {
    rs.in_flight.remove(sym);
    match booking {
        Some(b) => roll_book_actual(rs, now, b),
        None => {
            roll_week_rollover(rs, now);
        }
    }
}

/// THE cap check: week actuals + in-flight reservations + `reserve` must fit
/// under both weekly caps. `None` = fits, else the cap it would break.
fn roll_fits(cfg: &RollCfg, rs: &RollState, reserve: (f64, f64)) -> Option<&'static str> {
    let (fly_v, fly_c) = roll_in_flight_total(rs);
    if rs.week_volume_usd + fly_v + reserve.0 > cfg.weekly_volume_usd {
        Some("weekly_volume_budget")
    } else if rs.week_cost_usd + fly_c + reserve.1 > cfg.weekly_cost_usd {
        Some("weekly_cost_cap")
    } else {
        None
    }
}

/// Growth gates re-read right before the re-open (a growth order): the
/// KILL_SWITCH sentinel, and the Arcus leg's liquidation headroom and
/// leverage from a FRESH equity read. (A halt cannot be raised between the
/// close and the re-open — nothing in the roll path halts — so it is only
/// checked at the gate.) `None` = send.
fn roll_reopen_blocked(kill: bool, headroom_ok: bool, leverage_ok: bool) -> Option<&'static str> {
    if kill {
        Some("reopen_blocked_kill")
    } else if !headroom_ok {
        Some("reopen_blocked_headroom")
    } else if !leverage_ok {
        Some("reopen_blocked_leverage")
    } else {
        None
    }
}

/// With KILL_SWITCH engaged the re-open (growth) is never sent, and the
/// planner does not repair a gap inside the net tolerance while restricted:
/// the one-clip gap is closed by a REDUCTION of the other (non-rolled) leg
/// instead — reduce-only, by exactly what the close filled. `None` when that
/// cannot be one order (below the venue minimum) or the other leg holds less.
fn roll_kill_levelling(
    close_filled: f64,
    other_leg_qty: f64,
    min_qty: f64,
    size_decimals: u32,
) -> Option<f64> {
    // floored to the submitted precision: execute rounds, which could round a
    // finer-precision fill UP and close more of the other leg than was removed
    let q = floor_to_decimals(close_filled, size_decimals);
    (q > 0.0 && q >= min_qty && other_leg_qty + 1e-12 >= q).then_some(q)
}

/// Does an absolute IOC limit reach the touch (buy ≥ best ask, sell ≤ best
/// bid)? A roll IOC that cannot cross would be accepted, fill nothing and
/// still spend the interval: refuse it before the send instead. A missing,
/// one-sided or crossed book counts as not marketable. Compared EXACTLY on
/// the tick-rounded limit the venue will receive (`roll_limit_dec`) — no
/// float tolerance, which could wave through a limit one tick short.
fn roll_limit_crosses(
    is_buy: bool,
    limit: Decimal,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
) -> bool {
    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask >= bid && limit > Decimal::ZERO => {
            if is_buy {
                limit >= ask
            } else {
                limit <= bid
            }
        }
        _ => false,
    }
}

/// The absolute roll IOC limit exactly as the venue will receive it: mark
/// × (1 ± `ROLL_PRICE_MARGIN`) computed in `Decimal` (from `Decimal::from_f64`,
/// the shortest decimal of the f64 — never `from_f64_retain`, whose binary
/// expansion `700 × 1.001 = 700.6999…` lands one tick short after the
/// connector's inward rounding, pairtrade#315), then rounded INWARD to the
/// market tick (buy down, sell up) the same way the connector's
/// `create_order_taker_ioc_at` does. `None` for an unusable mark.
fn roll_limit_dec(is_buy: bool, mark: f64, tick: Option<Decimal>) -> Option<Decimal> {
    let m = Decimal::from_f64(mark).filter(|m| *m > Decimal::ZERO)?;
    let margin = Decimal::from_f64(ROLL_PRICE_MARGIN)?;
    let raw = if is_buy {
        m * (Decimal::ONE + margin)
    } else {
        m * (Decimal::ONE - margin)
    };
    let rounded = match tick.filter(|t| *t > Decimal::ZERO) {
        Some(t) if is_buy => (raw / t).floor() * t,
        Some(t) => (raw / t).ceil() * t,
        None => raw,
    };
    (rounded > Decimal::ZERO).then(|| rounded.normalize())
}

/// Booking of one settled execution on `leg`: `Some((value, cost))` when
/// `leg` is the rolled (Arcus) leg — whoever sent it (roll, planner repair,
/// levelling, ARM build) — `None` otherwise. Cost = fee + slippage vs the
/// mark the order was sent against.
fn roll_execution_booking(
    rolled_leg: Option<Leg>,
    leg: Leg,
    is_buy: bool,
    filled: f64,
    fee: f64,
    value: f64,
    mark: f64,
) -> Option<(f64, f64)> {
    (rolled_leg == Some(leg) && filled > 0.0)
        .then(|| (value, roll_cost(is_buy, filled, fee, value, mark)))
}

/// After a roll leg returned WITHOUT settling (`Err`): a leg that became
/// uncertain keeps its reservation (settled when the uncertain entry is
/// released / cleared); one that executed nothing drops it. (A settled leg
/// already dropped it in the same mutation that booked it, `roll_settle_leg`.)
/// Returns whether the state changed (the caller persists only then).
fn roll_finish_in_flight(
    rs: &mut RollState,
    sym: &str,
    settled: bool,
    still_uncertain: bool,
) -> bool {
    !settled && !still_uncertain && rs.in_flight.remove(sym).is_some()
}

/// Reserve a roll leg's worst case, immediately before its send.
fn roll_reserve_for_send(rs: &mut RollState, sym: &str, reserve: (f64, f64)) {
    rs.in_flight.insert(sym.to_string(), reserve);
}

/// An uncertain order on the rolled leg released on terminal evidence: its
/// reservation is replaced by the fills actually observed. Slippage is taken
/// as |value − filled × mark at send| (direction unknown here: the
/// conservative absolute).
#[allow(clippy::too_many_arguments)]
fn roll_release_booking(
    rs: &mut RollState,
    now: u64,
    sym: &str,
    roll_order: bool,
    filled: f64,
    fee: f64,
    value: f64,
    send_mark: f64,
) {
    // only a ROLL order's release settles the symbol's roll reservation; a
    // planner order must never consume (and so lose) a stale one
    if roll_order {
        rs.in_flight.remove(sym);
    }
    let slip = if send_mark > 0.0 {
        (value - filled * send_mark).abs()
    } else {
        0.0
    };
    roll_book_actual(rs, now, (value, fee.abs() + slip));
}

/// An uncertain order on the rolled leg cleared by RISK_ACK without
/// evidence: its reservation is replaced by the worst case of a full fill
/// at the current mark (± the price margin, fee and slippage bound).
#[allow(clippy::too_many_arguments)]
fn roll_risk_ack_booking(
    rs: &mut RollState,
    now: u64,
    sym: &str,
    roll_order: bool,
    qty: f64,
    mark: f64,
    slippage_bps: u32,
) {
    // a ROLL order: never less than what was reserved at send time (it may
    // have filled near its send-time limit, and a later lower mark or a
    // stale snapshot must not shrink the booking). A planner order leaves
    // any roll reservation alone and books its own worst case.
    let kept = if roll_order {
        rs.in_flight.remove(sym).unwrap_or((0.0, 0.0))
    } else {
        (0.0, 0.0)
    };
    let worst = roll_reserve(qty * mark, slippage_bps);
    roll_book_actual(rs, now, (kept.0.max(worst.0), kept.1.max(worst.1)));
}

/// In-flight reservations with no uncertain entry behind them can only be
/// left by a process that died between send and settle: book them (the leg
/// may have filled) and drop them. Returns the symbols settled.
fn roll_settle_stale_in_flight(
    rs: &mut RollState,
    now: u64,
    uncertain: &std::collections::BTreeMap<String, UncertainOrder>,
) -> Vec<String> {
    // only a ROLL uncertain entry holds a reservation: an unrelated planner
    // uncertain order on the symbol does not keep a stale one alive
    let stale: Vec<String> = rs
        .in_flight
        .keys()
        .filter(|s| !uncertain.get(*s).is_some_and(|u| u.roll))
        .cloned()
        .collect();
    for s in &stale {
        if let Some(r) = rs.in_flight.remove(s) {
            roll_book_actual(rs, now, r);
        }
    }
    stale
}

/// With the roll disabled nothing settles or books reservations (the stale
/// sweep and the release bookings are roll-only), so a reservation carried
/// in from an earlier roll-enabled run would sit in `in_flight` for good
/// and show as phantom in-flight volume. Dropped at startup, unbooked (the
/// disabled roll books nothing). Returns the symbols dropped.
fn roll_drop_in_flight_when_disabled(rs: &mut RollState, enabled: bool) -> Vec<String> {
    if enabled {
        return Vec::new();
    }
    std::mem::take(&mut rs.in_flight).into_keys().collect()
}

/// Largest move (fraction) of the rolled leg's mark between the gated
/// snapshot and each send; reservations are sized for it and a send is
/// refused beyond it, so executed value can never outrun what was booked.
const ROLL_PRICE_MARGIN: f64 = 0.001;

/// Next time a roll may run (the interval counts from the last roll that
/// actually sent; refusals back off per symbol, see `roll_symbol_backed_off`).
fn roll_next_at(rs: &RollState, interval_secs: u64) -> u64 {
    rs.last_roll_at.map(|t| t + interval_secs).unwrap_or(0)
}

/// True while `sym` is backing off after refusals (or suspended).
fn roll_symbol_backed_off(rs: &RollState, sym: &str, now: u64) -> bool {
    rs.backoff
        .get(sym)
        .is_some_and(|b| now < b.retry_not_before)
}

/// First wait after a roll refused before sending anything (no volume, no
/// cost); it doubles with each consecutive refusal up to
/// `ROLL_REFUSAL_BACKOFF_MAX_SECS`, so a persistent reject (margin,
/// reduce-only mode, market halt, auth) does not re-sign an order every minute.
const ROLL_REFUSAL_BACKOFF_SECS: u64 = 60;
const ROLL_REFUSAL_BACKOFF_MAX_SECS: u64 = 3_600;
/// Consecutive refusals that suspend the roll until the next roll week.
const ROLL_MAX_REFUSALS: u32 = 5;

/// Backoff after the `n`-th consecutive refusal (n ≥ 1).
fn roll_refusal_backoff(n: u32) -> u64 {
    let shift = n.saturating_sub(1).min(16);
    (ROLL_REFUSAL_BACKOFF_SECS << shift).min(ROLL_REFUSAL_BACKOFF_MAX_SECS)
}

/// Bookkeeping after a roll attempt on `sym`: a roll that SENT something
/// starts the (global) interval and clears THAT symbol's refusal streak; one
/// refused before any send backs that symbol off (doubling), and
/// `ROLL_MAX_REFUSALS` in a row suspend that symbol until the next roll week
/// (returns true when this attempt suspended it). Other symbols are not
/// affected: an equity book refused off-hours never stops the BTC roll.
fn roll_after_attempt(rs: &mut RollState, sym: &str, now: u64, sent: bool) -> bool {
    if sent {
        rs.last_roll_at = Some(now);
        rs.backoff.remove(sym);
        rs.week_count += 1;
        return false;
    }
    let b = rs.backoff.entry(sym.to_string()).or_default();
    b.refusals = b.refusals.saturating_add(1);
    if b.refusals >= ROLL_MAX_REFUSALS {
        // the rollover (a fresh RollState) lifts it
        b.retry_not_before = (roll_week(now) + 7) * 86_400;
        true
    } else {
        b.retry_not_before = now + roll_refusal_backoff(b.refusals);
        false
    }
}

/// The close stage's result: `Ok(re-open order)` — exactly what the close
/// filled, on the same leg — when it filled enough to re-open (the re-open
/// decides the final outcome), else `Err(roll_done outcome)`. Taker-only: a
/// close that reached the venue either settled (filled ≥ 0) or became
/// uncertain; one that did not reach it was refused before the send.
fn roll_close_stage(
    leg: Leg,
    close_sent: bool,
    close_uncertain: bool,
    close_filled: f64,
    min_qty: f64,
    size_decimals: u32,
) -> std::result::Result<Order, &'static str> {
    if close_uncertain {
        return Err("close_uncertain");
    }
    if !close_sent {
        return Err("close_not_sent");
    }
    if close_filled <= 0.0 {
        return Err("close_unfilled");
    }
    // filled, but below the venue minimum: it cannot be re-opened as one
    // order (the planner levels it later; that execution books itself)
    // The re-open is the quantity that will actually be SUBMITTED: the fill
    // floored to the order size decimals (execute rounds, which could round
    // a finer-precision fill UP past what the close filled).
    let qty = floor_to_decimals(close_filled, size_decimals);
    if close_filled < min_qty || qty < min_qty || qty <= 0.0 {
        return Err("close_partial_below_min");
    }
    Ok(Order { leg, qty })
}

/// `q` floored to `decimals` places (never rounds up).
fn floor_to_decimals(q: f64, decimals: u32) -> f64 {
    let scale = 10f64.powi(decimals as i32);
    // tiny epsilon so a value like 0.3 (0.29999…) is not floored a whole step
    ((q * scale) + 1e-9).floor() / scale
}

/// Arcus-leg notionals AFTER the re-open, from FRESH positions and the
/// FRESH mark of the rolled symbol (other symbols at their snapshot mark):
/// the growth gates must judge the account as it is when the re-open goes
/// out, not as it was at the start of the tick.
fn reopen_legs_after<Q, M>(
    symbols: &[SymbolCfg],
    leg: Leg,
    qty_of: Q,
    mark_of: M,
    sym: &str,
    reopen_qty: f64,
    fresh_mark: f64,
) -> Vec<(f64, f64)>
where
    Q: Fn(&str) -> f64,
    M: Fn(&str) -> f64,
{
    symbols
        .iter()
        .map(|c| {
            let (qty, mark) = if c.symbol == sym {
                (qty_of(&c.symbol).abs() + reopen_qty, fresh_mark)
            } else {
                (qty_of(&c.symbol).abs(), mark_of(&c.symbol))
            };
            (qty * mark, c.mmr(leg))
        })
        .collect()
}

/// A roll IOC the venue accepted that filled nothing although the price the
/// connector ACTUALLY sent (`ordered_price`, after its own tick rounding —
/// which can differ from ours at a tick-tier boundary) did not reach the
/// pre-send touch: a send-time refusal (backoff, no interval, no week
/// count), not a roll that ran. A zero fill at a limit that did reach the
/// touch is a genuine `close_unfilled` (the book moved).
fn roll_unfilled_is_refusal(
    is_buy: bool,
    filled: f64,
    sent_price: Decimal,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
) -> bool {
    filled <= 0.0 && !roll_limit_crosses(is_buy, sent_price, best_bid, best_ask)
}

/// A roll leg counts as SENT (it starts the interval and counts as a roll)
/// only when the venue accepted it (it settled) or it became uncertain (it
/// reached the venue and may have filled). A synchronous reject or any
/// pre-send failure (price moved, a read failed) executed nothing: that is
/// a refusal, which only backs off briefly.
fn roll_leg_sent(settled: bool, became_uncertain: bool) -> bool {
    settled || became_uncertain
}

/// Everything a roll must not run through, as read on this tick.
#[derive(Debug, Clone, Copy)]
struct RollGate {
    /// This symbol is backing off after refusals (or suspended this week).
    backed_off: bool,
    /// Net within tolerance NOW and after the close (`roll_balanced`).
    balanced: bool,
    halted: bool,
    kill: bool,
    uncertain: bool,
    feed_ok: bool,
    headroom_ok: bool,
    leverage_ok: bool,
    /// The configured clip is at least the venue minimum (never enlarged).
    clip_ok: bool,
    leg_holds_clip: bool,
}

/// Why the roll does not run now (`None`: go). `round_trip_usd` /
/// `round_trip_cost_usd` are the two leg reservations of the next roll
/// (exactly what `roll_reserve` will book): actual week + in-flight +
/// that projection must fit under both weekly caps.
fn roll_blocker(
    cfg: &RollCfg,
    rs: &RollState,
    now: u64,
    g: &RollGate,
    round_trip_usd: f64,
    round_trip_cost_usd: f64,
) -> Option<&'static str> {
    if now < roll_next_at(rs, cfg.interval_secs) {
        return Some("interval");
    }
    let checks = [
        (g.backed_off, "refusal_backoff"),
        (g.halted, "halted"),
        (g.kill, "kill_switch"),
        (g.uncertain, "uncertain_order"),
        (!g.feed_ok, "feed"),
        (!g.balanced, "unbalanced"),
        (!g.headroom_ok, "liq_headroom"),
        (!g.leverage_ok, "leverage"),
        (!g.clip_ok, "clip_below_venue_min"),
        (!g.leg_holds_clip, "leg_smaller_than_clip"),
    ];
    checks
        .iter()
        .find(|(hit, _)| *hit)
        .map(|(_, why)| *why)
        .or_else(|| roll_fits(cfg, rs, (round_trip_usd, round_trip_cost_usd)))
}

/// Round robin: the first symbol (in `order`) whose gates pass — with what
/// the gate computed for it (so the winner is not recomputed) — else the
/// first candidate's blocker. A blocked book must not starve the others.
fn pick_roll_symbol<T, F>(
    order: &[String],
    mut gate: F,
) -> Result<(String, T), Option<(String, &'static str)>>
where
    F: FnMut(&str) -> Result<T, &'static str>,
{
    let mut first_blocked = None;
    for sym in order {
        match gate(sym) {
            Ok(v) => return Ok((sym.clone(), v)),
            Err(why) => {
                first_blocked.get_or_insert((sym.clone(), why));
            }
        }
    }
    Err(first_blocked)
}

/// The book must still be within the net tolerance AFTER the close (before
/// the re-open), in the direction the close moves it: closing short raises
/// long − short by the clip, closing long lowers it.
fn roll_post_close_net_ok(
    net_qty: f64,
    leg: Leg,
    clip_qty: f64,
    long_mark: f64,
    tol_usd: f64,
) -> bool {
    let after = match leg {
        Leg::Short => net_qty + clip_qty,
        Leg::Long => net_qty - clip_qty,
    };
    (after * long_mark).abs() <= tol_usd
}

/// The roll's balance gate: the book must be within the net tolerance NOW
/// (a book already in breach is not rolled — the re-open would restore the
/// breach) AND still within it after the close.
fn roll_balanced(net_qty: f64, leg: Leg, clip_qty: f64, long_mark: f64, tol_usd: f64) -> bool {
    (net_qty * long_mark).abs() <= tol_usd
        && roll_post_close_net_ok(net_qty, leg, clip_qty, long_mark, tol_usd)
}

/// The simulated (DRY_RUN) quantity of `leg` in a book.
fn roll_dry_leg_qty(b: &SymBook, leg: Leg) -> f64 {
    match leg {
        Leg::Long => b.dry_long_qty,
        Leg::Short => b.dry_short_qty,
    }
}

/// Whether the re-open may go out, decided on the FRESH mark read after
/// the close settled: `None` = go, else the `roll_done` outcome. It is
/// refused when the mark moved beyond the price margin from the gated mark
/// (the re-open would trade outside the band the roll was approved for),
/// or when its fresh reservation no longer fits the weekly caps. Either way
/// the planner repairs the one-clip gap on a later tick — that execution is
/// booked when it settles but, by design, is never blocked by the caps
/// (they gate rolls only).
fn roll_reopen_decision(
    cfg: &RollCfg,
    rs: &RollState,
    gated_mark: f64,
    fresh_mark: f64,
    reserve: (f64, f64),
) -> Option<&'static str> {
    if !roll_price_ok(gated_mark, fresh_mark) {
        return Some("reopen_price_moved");
    }
    roll_fits(cfg, rs, reserve).map(|_| "reopen_capped")
}

/// Mark the roll clip is sized at: the higher of the rolled leg's mark and
/// the long mark the net guard uses, so clip × either mark ≤ the clip USD.
fn roll_sizing_mark(leg_mark: f64, long_mark: f64) -> f64 {
    leg_mark.max(long_mark)
}

/// Worst-case Arcus taker fee (Base tier) used to pre-book a roll leg's
/// cost before it is sent.
const ROLL_RESERVE_FEE_BPS: f64 = 2.25;

/// What a roll leg books against the weekly caps BEFORE it is sent: its
/// full notional and a worst-case cost (fee + the IOC slippage bound).
fn roll_reserve(notional_usd: f64, slippage_bps: u32) -> (f64, f64) {
    // valued at the worst price a send is allowed at (mark ± margin), and
    // costed with the fee, the IOC slippage bound and that price move
    let worst = notional_usd * (1.0 + ROLL_PRICE_MARGIN);
    (
        worst,
        worst * (ROLL_RESERVE_FEE_BPS + slippage_bps as f64) / 10_000.0
            + notional_usd * ROLL_PRICE_MARGIN,
    )
}

/// A roll leg may only be sent while the leg's fresh mark is within
/// `ROLL_PRICE_MARGIN` of the mark its reservation was sized at.
fn roll_price_ok(reserved_mark: f64, fresh_mark: f64) -> bool {
    reserved_mark > 0.0 && ((fresh_mark / reserved_mark) - 1.0).abs() <= ROLL_PRICE_MARGIN
}

/// Outcome of one settled roll leg (an `Err` executed nothing, or became
/// uncertain — see `roll_leg_sent`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct RollLeg {
    filled: f64,
    cost: f64,
}

/// What one `execute` did: the size the venue reports filled, and the fee
/// and filled value of the order's own fills when they were read (Arcus
/// settle), else 0 and filled × the leg's mark. Returned only for an order
/// the venue accepted (DRY_RUN: simulated); failures before or at the send,
/// and uncertain orders, are `Err`.
#[derive(Debug, Clone, PartialEq, Default)]
struct ExecResult {
    filled: f64,
    fee: f64,
    value: f64,
    order_id: String,
    /// Limit the connector actually sent (after its own tick rounding).
    ordered_price: Decimal,
}

/// A ROLL leg's execution context: the only caller that sends an absolute
/// limit and whose settle drops the roll's in-flight reservation and books
/// at the leg's anchor mark. Explicit, so a future non-roll caller with a
/// bounded price can never be mistaken for a roll leg.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RollCtx {
    /// Tick-rounded absolute IOC limit (`roll_limit_dec`).
    limit: Decimal,
}

/// The venue side and reduce-only flag an order is sent with: the ONE
/// mapping (the send, the roll limit's direction and the roll cost's sign
/// all derive from it, so they cannot disagree).
fn order_side(order: &Order) -> (OrderSide, bool) {
    match (order.leg, order.qty > 0.0) {
        (Leg::Long, true) => (OrderSide::Long, false),
        (Leg::Long, false) => (OrderSide::Short, true),
        (Leg::Short, true) => (OrderSide::Short, false),
        (Leg::Short, false) => (OrderSide::Long, true),
    }
}

/// Roll cost of one execution: the fee plus the ADVERSE slippage against the
/// mark at send (paid above mark on a buy, received below it on a sell).
/// Never negative: a fee reported debit-signed still raises it (Arcus
/// reports it positive — testnet 2026-09-28: 0.038 on $84.5), and a price
/// better than the (possibly stale) mark counts as zero slippage, never as a
/// credit that would loosen the weekly cost cap.
fn roll_cost(is_buy: bool, filled: f64, fee: f64, value: f64, mark: f64) -> f64 {
    let at_mark = filled * mark;
    let slip = if is_buy {
        value - at_mark
    } else {
        at_mark - value
    };
    fee.abs() + slip.max(0.0)
}

/// One read of an Arcus order: book presence, cancel, and its fills.
#[derive(Debug, Clone, Default)]
struct OrderView {
    open: bool,
    canceled: bool,
    filled: f64,
    fee: f64,
    value: f64,
    trades: Vec<String>,
}

/// Outcome of one look at a just-sent IOC.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Settle {
    /// Terminal; the size it filled.
    Terminal(f64),
    Pending,
}

/// Decides from one read whether an Arcus IOC is final: it must be out of
/// the open orders, either completely filled or seen canceled (the IOC's
/// unfilled rest), and the fills reported for it must agree with the
/// position change. Anything short of that is `Pending` — a fill that is
/// only partly visible must not size the paired leg.
fn settle_decision(
    sent: f64,
    open: bool,
    canceled: bool,
    fills_sum: f64,
    pos_delta: f64,
    tol: f64,
) -> Settle {
    if open {
        return Settle::Pending;
    }
    let complete = fills_sum >= sent - tol;
    if !(complete || canceled) {
        return Settle::Pending;
    }
    if (fills_sum - pos_delta).abs() > tol {
        return Settle::Pending;
    }
    Settle::Terminal(fills_sum)
}

/// The Lighter account whose points rows feed the subsidy block: only when
/// the LONG leg is on Lighter (an Arcus long must never be credited with a
/// stray LIGHTER_ACCOUNT_INDEX_<instance>'s points).
fn points_account_index(long_venue: Option<VenueKind>, raw: Option<String>) -> Option<u64> {
    if long_venue != Some(VenueKind::Lighter) {
        return None;
    }
    raw.and_then(|v| v.trim().parse().ok())
}

/// What to do with an uncertain order this tick.
#[derive(Debug, Clone, PartialEq)]
enum UncertainAction {
    /// Terminal evidence seen: release the symbol.
    Release,
    /// Still unknown, within the grace window: keep the symbol blocked.
    Keep,
    /// Cannot be settled automatically: keep it blocked AND halt; only the
    /// operator (after checking the venue) clears it with RISK_ACK.
    Escalate(String),
}

/// An uncertain order is released ONLY on terminal evidence (its own
/// fills / cancel agreeing with the position on Arcus; on Lighter, whose
/// IOC is final on ack, a known order id that is no longer open). Time alone
/// never releases it: absence from the open orders proves nothing while the
/// venue's fill feed may lag. An order sent on another venue / instance than
/// the leg is configured for now cannot be checked here at all.
fn uncertain_action(
    u: &UncertainOrder,
    current_exchange: &str,
    current_instance: &str,
    terminal_evidence: bool,
    now: u64,
    grace_secs: u64,
) -> UncertainAction {
    let other_account = u.exchange != current_exchange
        || (!u.instance.is_empty() && u.instance != current_instance);
    if other_account {
        return UncertainAction::Escalate(format!(
            "uncertain order was sent on {}/{} but the leg is now {current_exchange}/{current_instance} — check that account by hand, then RISK_ACK",
            u.exchange, u.instance
        ));
    }
    if terminal_evidence {
        return UncertainAction::Release;
    }
    if now.saturating_sub(u.sent_at) >= grace_secs {
        return UncertainAction::Escalate(format!(
            "uncertain order {:?} still unsettled after {grace_secs}s — check the venue by hand, then RISK_ACK",
            u.order_id
        ));
    }
    UncertainAction::Keep
}

/// The plan that may be sent this tick: nothing at all when an order was
/// reconciled during the tick (the risk snapshot predates it), otherwise
/// the plan without the symbols that still have an uncertain order.
fn tick_plan_after_reconcile(
    plan: Vec<(String, Vec<Order>)>,
    uncertain: &std::collections::BTreeMap<String, UncertainOrder>,
    released_now: &std::collections::BTreeSet<String>,
) -> Vec<(String, Vec<Order>)> {
    if !released_now.is_empty() {
        return Vec::new();
    }
    drop_uncertain(plan, uncertain, released_now)
}

/// The tick's plan without the symbols that have an uncertain order: no
/// order of any kind goes out on them until it is reconciled.
fn drop_uncertain(
    plan: Vec<(String, Vec<Order>)>,
    uncertain: &std::collections::BTreeMap<String, UncertainOrder>,
    released_this_tick: &std::collections::BTreeSet<String>,
) -> Vec<(String, Vec<Order>)> {
    plan.into_iter()
        .filter(|(sym, _)| !uncertain.contains_key(sym) && !released_this_tick.contains(sym))
        .collect()
}

// --------------------------------------------------------------- planning

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    Long,
    Short,
}

impl Serialize for Leg {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(match self {
            Leg::Long => "Long",
            Leg::Short => "Short",
        })
    }
}

impl<'de> Deserialize<'de> for Leg {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        match String::deserialize(d)?.as_str() {
            "Long" => Ok(Leg::Long),
            "Short" => Ok(Leg::Short),
            other => Err(serde::de::Error::custom(format!("unknown leg {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Order {
    leg: Leg,
    /// Positive = grow the leg (open more), negative = reduce it.
    qty: f64,
}

/// Held sizes: `long` and `short` are both magnitudes (≥ 0).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Book {
    long: f64,
    short: f64,
}

impl Book {
    fn net(&self) -> f64 {
        self.long - self.short
    }
}

/// The orders one tick may send, in order, given the wanted size and the
/// held sizes. Pure so the invariants can be tested without a venue.
///
/// - A lopsided book (|net| over `net_tol_qty`) is levelled before anything
///   else, with ONE order: while building (the smaller leg is under
///   `target`) the smaller leg is grown up to the larger one; otherwise, or
///   whenever `restricted`, the larger leg is reduced down to the smaller
///   one. A leg the venue just liquidated cannot be grown (the leverage
///   guard halts on it), and the halt is what flips this to the reduction.
/// - With the book level, both legs move toward `target` in the same
///   direction, one clip each, growth long-first and unwinding short-leg
///   first (so an unfilled second order leaves the book long the points
///   leg, never short it).
/// - `restricted` (halt or KILL_SWITCH) permits reductions only.
/// - Sizes under `min_qty` are ignored (dust the venue would reject).
fn plan_orders(
    target: f64,
    book: Book,
    clip_qty: f64,
    min_qty: f64,
    net_tol_qty: f64,
    restricted: bool,
) -> Vec<Order> {
    let clip = |q: f64| q.min(clip_qty);
    let net = book.net();
    if net.abs() > net_tol_qty.max(min_qty) {
        let (larger, smaller, small_held, gap) = if net > 0.0 {
            (Leg::Long, Leg::Short, book.short, net)
        } else {
            (Leg::Short, Leg::Long, book.long, -net)
        };
        let order = if !restricted && small_held < target {
            Order {
                leg: smaller,
                qty: clip(gap.min(target - small_held)),
            }
        } else {
            Order {
                leg: larger,
                qty: -clip(gap),
            }
        };
        return if order.qty.abs() < min_qty {
            vec![]
        } else {
            vec![order]
        };
    }
    let mut out = Vec::new();
    let d_long = target - book.long;
    let d_short = target - book.short;
    if d_long > 0.0 || d_short > 0.0 {
        if restricted {
            return out;
        }
        // Growth: long first, then short, so a partial tick is long-heavy.
        for (leg, d) in [(Leg::Long, d_long), (Leg::Short, d_short)] {
            let q = clip(d.max(0.0));
            if q >= min_qty {
                out.push(Order { leg, qty: q });
            }
        }
    } else {
        // Unwind: short leg first, then long.
        for (leg, d) in [(Leg::Short, d_short), (Leg::Long, d_long)] {
            let q = clip((-d).max(0.0));
            if q >= min_qty {
                out.push(Order { leg, qty: -q });
            }
        }
    }
    out
}

/// A persisted book from the other mode (DRY_RUN ↔ live) is dropped to
/// `Off`: the live venues never held a DRY_RUN book, and a live book must
/// be re-adopted by an explicit ARM after a flip back. Nothing is traded
/// by this — `Off` sends no orders.
fn reconcile_state_mode(mut state: State, dry_run: bool) -> State {
    if let Some(was) = state.dry_run {
        if was != dry_run && state.books.values().any(|b| b.mode != Mode::Off) {
            for (sym, b) in state.books.iter_mut() {
                if b.mode != Mode::Off {
                    log::warn!(
                        "[STARTUP] state.json was written with dry_run={was}, now dry_run={dry_run}: dropping {sym} mode {:?} / target {} to Off — ARM again to (re)build or adopt the book",
                        b.mode, b.target_qty
                    );
                }
                b.mode = Mode::Off;
                b.target_qty = 0.0;
            }
            state.halted = false;
            state.halt_reason = None;
        }
    }
    // Roll bookkeeping belongs to one mode: simulated DRY_RUN rolls must not
    // eat the live week's budget (or delay the first live roll), nor live
    // counters a DRY_RUN run.
    if state.dry_run.is_some_and(|was| was != dry_run) && state.roll != RollState::default() {
        log::warn!(
            "[STARTUP] dry_run flip: resetting roll bookkeeping (was week vol ${:.0} / cost ${:.2}, {} in flight)",
            state.roll.week_volume_usd,
            state.roll.week_cost_usd,
            state.roll.in_flight.len()
        );
        state.roll = RollState::default();
    }
    // Uncertain orders are live venue orders: DRY_RUN never sends and never
    // reconciles, so an entry carried into a dry-run process would block its
    // symbol for good (and keep ARM from rebuilding the dry book). Drop them;
    // the live venue positions are re-read when the process next runs live.
    if dry_run && !state.uncertain.is_empty() {
        for (sym, u) in &state.uncertain {
            log::warn!(
                "[STARTUP] dry_run: dropping live uncertain order on {sym} ({:?} {:?} qty {}) — check the venue by hand before going live again",
                u.leg, u.order_id, u.qty
            );
        }
        state.uncertain.clear();
    }
    state.dry_run = Some(dry_run);
    state
}

/// The second order of a pair, cut to the first leg's actual fill so the
/// book is never more lopsided than what really traded.
fn size_second_leg(next: &Order, first_filled: f64) -> Order {
    let q = next.qty.abs().min(first_filled.max(0.0));
    Order {
        leg: next.leg,
        qty: if next.qty < 0.0 { -q } else { q },
    }
}

/// Growth guard: the venue's gross notional after the tick's growth must
/// stay within `max_leverage` × its equity.
fn leverage_ok(gross_after_usd: f64, equity_usd: f64, max_leverage: f64) -> bool {
    gross_after_usd <= max_leverage * equity_usd
}

/// USD notional the tick's growth orders add to one venue, over every
/// symbol (each at that venue's own mark). Reductions do not count.
fn planned_growth_usd(plan: &[(String, Vec<Order>)], snap: &Snapshot, leg: Leg) -> f64 {
    plan.iter()
        .flat_map(|(sym, os)| os.iter().map(move |o| (sym, o)))
        .filter(|(_, o)| o.leg == leg && o.qty > 0.0)
        .map(|(sym, o)| {
            o.qty
                * match leg {
                    Leg::Long => snap.long.mark(sym),
                    Leg::Short => snap.short.mark(sym),
                }
        })
        .sum()
}

/// Per-symbol (notional, mmr) of one venue AFTER the tick's growth orders:
/// the held notional plus each growth order's qty at that venue's mark.
fn legs_after_growth(
    plan: &[(String, Vec<Order>)],
    snap: &Snapshot,
    cfg: &Config,
    leg: Leg,
) -> Vec<(f64, f64)> {
    let mut legs = snap.legs(cfg, leg);
    for (c, l) in cfg.symbols.iter().zip(legs.iter_mut()) {
        let mark = snap.mark_of(leg, &c.symbol);
        let grow: f64 = plan
            .iter()
            .filter(|(sym, _)| *sym == c.symbol)
            .flat_map(|(_, os)| os.iter())
            .filter(|o| o.leg == leg && o.qty > 0.0)
            .map(|o| o.qty)
            .sum();
        l.0 += grow * mark;
    }
    legs
}

/// Growth guard on liquidation headroom: after the tick's growth, the
/// venue's headroom must stay at or above `liq_guard_pct` (the bound the
/// roll's re-open is gated on). Nothing held after growth = ok.
/// The post-growth headroom halt is part of the roll feature (it repairs a
/// refused re-open by levelling down): it applies only while the roll is
/// enabled, so a default-off deployment keeps its pre-roll planner.
fn growth_headroom_guard_applies(roll_enabled: bool, growth_usd: f64) -> bool {
    roll_enabled && growth_usd > 0.0
}

fn growth_headroom_ok(equity_usd: f64, legs_after: &[(f64, f64)], liq_guard_pct: f64) -> bool {
    liq_headroom_pct(equity_usd, legs_after).is_none_or(|h| h >= liq_guard_pct)
}

/// The tick's plan with every growth order removed (reductions — DISARM
/// unwinds, levelling down — still run while halted).
fn drop_growth(plan: Vec<(String, Vec<Order>)>) -> Vec<(String, Vec<Order>)> {
    plan.into_iter()
        .map(|(sym, os)| {
            (
                sym,
                os.into_iter().filter(|o| o.qty < 0.0).collect::<Vec<_>>(),
            )
        })
        .filter(|(_, os)| !os.is_empty())
        .collect()
}

/// Whether an accepted ARM restarts the campaign baselines (armed_at,
/// equity / points at ARM, cycles).
fn resets_baseline(symbols: usize, any_book_on: bool) -> bool {
    symbols == 1 || !any_book_on
}

/// Whether a book's feed must be readable for the tick to proceed.
fn gates_feed(mode: Mode) -> bool {
    mode != Mode::Off
}

/// Both venues' quote for `symbol`, reduced to the size metadata a book
/// needs: the coarser decimals and the larger minimum, so every order is
/// representable on both legs. Returns the long-leg mark alongside.
async fn fetch_meta(long: &Venue, short: &Venue, symbol: &str) -> Result<(f64, SymMeta)> {
    let (mark, sd1, min1) = long
        .mark(symbol)
        .await
        .with_context(|| format!("long-leg quote {symbol}"))?;
    let (_, sd2, min2) = short
        .mark(symbol)
        .await
        .with_context(|| format!("short-leg quote {symbol}"))?;
    Ok((
        mark,
        SymMeta {
            size_decimals: sd1.min(sd2),
            min_qty: min1.max(min2),
        },
    ))
}

/// Base-unit size for `notional_usd` at `mark`, floored to `size_decimals`.
fn qty_for_notional(notional_usd: f64, mark: f64, size_decimals: u32) -> f64 {
    if !positive(mark) || !positive(notional_usd) {
        return 0.0;
    }
    let scale = 10f64.powi(size_decimals as i32);
    ((notional_usd / mark) * scale).floor() / scale
}

/// Distance (percentage points of gross notional) between an account's
/// equity and its maintenance requirement over every hedged position:
/// `(equity − Σ notional_i × mmr_i) / Σ notional_i`. With one symbol this
/// is `equity / notional − mmr`. `None` when nothing is held.
fn liq_headroom_pct(equity_usd: f64, legs: &[(f64, f64)]) -> Option<f64> {
    let gross: f64 = legs.iter().map(|(n, _)| n).sum();
    if !positive(gross) {
        return None;
    }
    let maint: f64 = legs.iter().map(|(n, mmr)| n * mmr).sum();
    Some((equity_usd * 100.0 - maint) / gross)
}

/// Per-leg notional (USD) per symbol, as an ARM file asks for it.
type ArmRequest = Vec<(String, f64)>;

/// What an ARM file asks for: per-leg notional (USD) per symbol.
///
/// - one symbol configured: empty → `HEDGE_TARGET_NOTIONAL_USD`, a bare
///   number → that notional (the single-symbol format, unchanged);
/// - otherwise one `SYMBOL USD` line per book (`SYMBOL=USD` and
///   `SYMBOL:USD` also accepted; a bare `SYMBOL` takes the default).
///
/// Any bad line rejects the whole file: a half-applied ARM would build a
/// book the operator did not ask for.
fn parse_arm(body: &str, cfg: &Config) -> std::result::Result<ArmRequest, String> {
    let lines: Vec<&str> = body
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    let single = cfg.symbols.len() == 1;
    if lines.is_empty() {
        return if single {
            Ok(vec![(cfg.primary().to_string(), cfg.target_notional_usd)])
        } else {
            Err("empty ARM with several symbols configured — write `SYMBOL USD` lines".into())
        };
    }
    let mut out: Vec<(String, f64)> = Vec::new();
    for line in lines {
        if let Ok(usd) = line.parse::<f64>() {
            if !single {
                return Err(format!(
                    "bare notional '{line}' is ambiguous with several symbols"
                ));
            }
            if !positive(usd) {
                return Err(format!("notional '{line}' must be > 0"));
            }
            if !out.is_empty() {
                return Err(format!(
                    "'{line}': {} appears twice (two bare notionals)",
                    cfg.primary()
                ));
            }
            out.push((cfg.primary().to_string(), usd));
            continue;
        }
        let parts: Vec<&str> = line
            .split(|c: char| c.is_whitespace() || c == '=' || c == ':')
            .filter(|p| !p.is_empty())
            .collect();
        let Some(first) = parts.first() else {
            return Err(format!("'{line}': want `SYMBOL USD`"));
        };
        let sym = first.to_ascii_uppercase();
        if !cfg.symbols.iter().any(|c| c.symbol == sym) {
            return Err(format!("{sym} is not in HEDGE_SYMBOLS"));
        }
        let usd = match parts.get(1) {
            None => cfg.target_notional_usd,
            Some(v) => v
                .parse::<f64>()
                .map_err(|_| format!("'{line}': notional is not a number"))?,
        };
        if !positive(usd) || parts.len() > 2 {
            return Err(format!("'{line}': want `SYMBOL USD` with USD > 0"));
        }
        if out.iter().any(|(s, _)| *s == sym) {
            return Err(format!("{sym} appears twice"));
        }
        out.push((sym, usd));
    }
    Ok(out)
}

/// Which books a DISARM file closes: empty → all; else one symbol per
/// line. A line naming no configured symbol closes ALL books — DISARM is
/// the protective direction, so an unreadable one errs towards flat.
fn parse_disarm(body: &str, cfg: &Config) -> Vec<String> {
    let all = cfg.symbol_names();
    let mut out = Vec::new();
    for line in body
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let sym = line.to_ascii_uppercase();
        if !all.contains(&sym) {
            log::warn!("[DISARM] '{line}' is not a configured symbol: closing ALL books");
            return all;
        }
        if !out.contains(&sym) {
            out.push(sym);
        }
    }
    if out.is_empty() {
        all
    } else {
        out
    }
}

/// One account's live-points series out of the collector's JSONL
/// (`robinhood_points_collector.py`): `(ts_unix, live_points_total)` in
/// file order. Rows of other accounts and unparsable lines are skipped.
fn points_series(lines: &str, account_index: u64) -> Vec<(u64, f64)> {
    lines
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v.get("account_index").and_then(|a| a.as_u64()) == Some(account_index))
        .filter_map(|v| {
            Some((
                v.get("ts_unix")?.as_u64()?,
                v.get("live_points_total")?.as_f64()?,
            ))
        })
        .collect()
}

/// Latest value, and the value in force at `at` (last row at or before
/// it), of a points series.
fn points_latest_and_at(series: &[(u64, f64)], at: u64) -> (Option<(u64, f64)>, Option<f64>) {
    let latest = series.last().copied();
    let at_val = series
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= at)
        .map(|(_, v)| *v);
    (latest, at_val)
}

/// The dashboard's `subsidy` block (debot-dashboard `deploy/subsidy-kpi.md`):
/// units and cost since ARM, both counted for this campaign only. Cost is
/// positive when money was given up. `None` until armed with a points
/// baseline.
fn subsidy_block(
    series: &[(u64, f64)],
    points_at_arm: Option<f64>,
    armed_at: Option<u64>,
    pnl_since_arm_usd: Option<f64>,
    now: u64,
) -> Option<serde_json::Value> {
    let (latest, _) = points_latest_and_at(series, now);
    let (as_of, latest_pts) = latest?;
    let base = points_at_arm?;
    let armed_at = armed_at?;
    let week_ago = now.saturating_sub(7 * 86_400).max(armed_at);
    let (_, at_week) = points_latest_and_at(series, week_ago);
    let units_7d = at_week.map(|v| latest_pts - v.max(base));
    Some(serde_json::json!({
        "unit": "points",
        "units_total": latest_pts - base,
        "units_7d": units_7d,
        "cost_total_usd": pnl_since_arm_usd.map(|p| -p),
        "as_of_ts": as_of,
    }))
}

fn utc_day_key(now: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_secs(now))
        .format("%Y-%m-%d")
        .to_string()
}

fn decimal(v: f64, field: &str) -> Result<Decimal> {
    Decimal::from_f64(v).ok_or_else(|| anyhow!("{field} {v} is not representable"))
}

// ---------------------------------------------------------------- venues

/// One venue read: account equity, and per hedged symbol the signed held
/// size (+ long, − short) and the mark.
#[derive(Debug, Clone, Default, Serialize)]
struct VenueSnapshot {
    equity_usd: f64,
    qty: std::collections::BTreeMap<String, f64>,
    mark: std::collections::BTreeMap<String, f64>,
}

impl VenueSnapshot {
    fn qty(&self, symbol: &str) -> f64 {
        self.qty.get(symbol).copied().unwrap_or(0.0)
    }

    fn mark(&self, symbol: &str) -> f64 {
        self.mark.get(symbol).copied().unwrap_or(0.0)
    }
}

/// Both venues, read in one go.
#[derive(Debug, Clone, Default)]
struct Snapshot {
    long: VenueSnapshot,
    short: VenueSnapshot,
}

impl Snapshot {
    /// Held sizes of one symbol's book. The long venue must hold ≥ 0, the
    /// short venue ≤ 0: anything else is a foreign position on the account,
    /// treated as zero for the hedge and never traded against.
    fn book(&self, symbol: &str) -> Book {
        Book {
            long: self.long.qty(symbol).max(0.0),
            short: (-self.short.qty(symbol)).max(0.0),
        }
    }

    /// Per-venue (notional, mmr) of every hedged book, for the guards.
    fn legs(&self, cfg: &Config, leg: Leg) -> Vec<(f64, f64)> {
        cfg.symbols
            .iter()
            .map(|c| {
                let b = self.book(&c.symbol);
                match leg {
                    Leg::Long => (b.long * self.long.mark(&c.symbol), c.mmr(Leg::Long)),
                    Leg::Short => (b.short * self.short.mark(&c.symbol), c.mmr(Leg::Short)),
                }
            })
            .collect()
    }

    fn gross(&self, cfg: &Config, leg: Leg) -> f64 {
        self.legs(cfg, leg).iter().map(|(n, _)| n).sum()
    }

    fn equity(&self, leg: Leg) -> f64 {
        match leg {
            Leg::Long => self.long.equity_usd,
            Leg::Short => self.short.equity_usd,
        }
    }

    /// `sym`'s mark on `leg`'s own venue (0 when unknown).
    fn mark_of(&self, leg: Leg, sym: &str) -> f64 {
        match leg {
            Leg::Long => self.long.mark(sym),
            Leg::Short => self.short.mark(sym),
        }
    }
}

struct Venue {
    name: &'static str,
    kind: VenueKind,
    instance: String,
    dex: Arc<DexConnectorBox>,
}

impl Venue {
    async fn mark(&self, symbol: &str) -> Result<(f64, u32, f64)> {
        let t = self
            .dex
            .get_ticker(symbol, None)
            .await
            .map_err(|e| anyhow!("{} get_ticker {symbol}: {e:?}", self.name))?;
        let price = t.price.to_f64().unwrap_or(0.0);
        if !positive(price) {
            bail!("{} ticker {symbol}: non-positive price", self.name);
        }
        Ok((
            price,
            t.size_decimals.unwrap_or(5),
            t.min_order.and_then(|d| d.to_f64()).unwrap_or(0.0),
        ))
    }

    /// Signed held size per symbol (upper-case), every market on the account.
    async fn positions(&self) -> Result<std::collections::BTreeMap<String, f64>> {
        let positions = self
            .dex
            .get_positions()
            .await
            .map_err(|e| anyhow!("{} get_positions: {e:?}", self.name))?;
        let mut out = std::collections::BTreeMap::new();
        for p in positions {
            let q = p.size.to_f64().unwrap_or(0.0).abs() * if p.sign < 0 { -1.0 } else { 1.0 };
            *out.entry(book_symbol(&p.symbol)).or_insert(0.0) += q;
        }
        Ok(out)
    }

    async fn signed_qty(&self, symbol: &str) -> Result<f64> {
        Ok(self.positions().await?.get(symbol).copied().unwrap_or(0.0))
    }

    async fn equity(&self) -> Result<f64> {
        let b = self
            .dex
            .get_balance(None)
            .await
            .map_err(|e| anyhow!("{} get_balance: {e:?}", self.name))?;
        let e = b.equity.to_f64().unwrap_or(f64::NAN);
        if !e.is_finite() {
            bail!("{} equity {} not finite", self.name, b.equity);
        }
        Ok(e)
    }
}

// ---------------------------------------------------------------- engine

/// Per-symbol order sizing, the coarser of the two venues.
#[derive(Debug, Clone, Copy)]
struct SymMeta {
    size_decimals: u32,
    min_qty: f64,
}

struct Engine {
    cfg: Config,
    state: State,
    sentinels: Sentinels,
    long: Venue,
    short: Venue,
    meta: std::collections::BTreeMap<String, SymMeta>,
    last_s3_mirror: u64,
    last_snapshot: Snapshot,
    /// When `last_snapshot` was read from the venues; `None` until the
    /// first successful read. Status carries it so marks frozen by an
    /// outage are datable from the card.
    last_snapshot_at: Option<u64>,
    /// Set while a venue is unreachable or the two marks disagree;
    /// reported in status (`hedge_holder.feed_problem`).
    feed_problem: Option<String>,
}

impl Engine {
    fn persist(&self) {
        if let Err(e) = persist_json(&self.cfg.state_path, &self.state) {
            log::error!("[STATE] persist failed: {e:?}");
        }
    }

    fn event(&self, kind: &str, fields: serde_json::Value) {
        let mut rec =
            serde_json::json!({ "ts": now_secs(), "event": kind, "dry_run": self.cfg.dry_run });
        if let (Some(dst), Some(src)) = (rec.as_object_mut(), fields.as_object()) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
        if let Err(e) = append_jsonl(&self.cfg.events_path, &rec) {
            log::warn!("[EVENT] append failed: {e:?}");
        }
    }

    fn halt(&mut self, reason: String) {
        if self.state.halted {
            return;
        }
        log::error!(
            "[HALT] {reason} — growth blocked until RISK_ACK at {}",
            self.cfg.risk_ack_path.display()
        );
        self.state.halted = true;
        self.state.halt_reason = Some(reason.clone());
        self.event("halt", serde_json::json!({ "reason": reason }));
        self.persist();
    }

    fn meta(&self, symbol: &str) -> SymMeta {
        self.meta.get(symbol).copied().unwrap_or(SymMeta {
            size_decimals: 5,
            min_qty: 0.0,
        })
    }

    /// Size metadata for a symbol that was unpriced at startup (book Off
    /// then): fetched from both venues the first time it is armed. Errors
    /// leave the ARM unapplied — sizing a book on default decimals and a
    /// zero minimum is exactly what the startup quote protects against.
    async fn ensure_meta(&mut self, symbol: &str) -> Result<()> {
        if self.meta.contains_key(symbol) {
            return Ok(());
        }
        let (mark, m) = fetch_meta(&self.long, &self.short, symbol).await?;
        log::info!(
            "[ARM] {symbol} mark={mark:.2} size_decimals={} min_qty={} (metadata fetched on ARM)",
            m.size_decimals,
            m.min_qty
        );
        self.meta.insert(symbol.to_string(), m);
        Ok(())
    }

    async fn venue_snapshot(&self, venue: &Venue, leg: Leg) -> Result<VenueSnapshot> {
        let mut snap = VenueSnapshot::default();
        // Only books the bot manages gate the tick on their feed: an Off
        // symbol whose ticker is unavailable is left unpriced (mark absent)
        // instead of failing the whole read — otherwise one dead unused
        // market would stall DISARM, the guards and every armed book. An
        // Off symbol that turns out to be HELD is refused below: margin on
        // an unpriced position cannot be measured.
        let mut unpriced: Vec<&str> = Vec::new();
        for c in &self.cfg.symbols {
            match venue.mark(&c.symbol).await {
                Ok((mark, _, _)) => {
                    snap.mark.insert(c.symbol.clone(), mark);
                }
                Err(e) if !gates_feed(self.state.book(&c.symbol).mode) => {
                    log::warn!("[FEED] {} unpriced (book Off): {e}", c.symbol);
                    unpriced.push(&c.symbol);
                }
                Err(e) => return Err(e),
            }
        }
        let refuse_held_unpriced = |snap: &VenueSnapshot| -> Result<()> {
            for sym in &unpriced {
                if snap.qty(sym) != 0.0 {
                    bail!(
                        "{} holds {} {sym} but its ticker is unavailable",
                        venue.name,
                        snap.qty(sym)
                    );
                }
            }
            Ok(())
        };
        if self.cfg.dry_run {
            snap.equity_usd = self.cfg.dry_run_equity_usd;
            for c in &self.cfg.symbols {
                let b = self.state.book(&c.symbol);
                let q = match leg {
                    Leg::Long => b.dry_long_qty,
                    Leg::Short => -b.dry_short_qty,
                };
                snap.qty.insert(c.symbol.clone(), q);
            }
            refuse_held_unpriced(&snap)?;
            return Ok(snap);
        }
        let held = venue.positions().await?;
        for c in &self.cfg.symbols {
            snap.qty.insert(
                c.symbol.clone(),
                held.get(&c.symbol).copied().unwrap_or(0.0),
            );
        }
        refuse_held_unpriced(&snap)?;
        snap.equity_usd = venue.equity().await?;
        Ok(snap)
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let snap = Snapshot {
            long: self.venue_snapshot(&self.long, Leg::Long).await?,
            short: self.venue_snapshot(&self.short, Leg::Short).await?,
        };
        for c in &self.cfg.symbols {
            let (l, s) = (snap.long.qty(&c.symbol), snap.short.qty(&c.symbol));
            if l < 0.0 {
                log::warn!(
                    "[BOOK] long venue holds a SHORT {l} {}: not part of the hedge",
                    c.symbol
                );
            }
            if s > 0.0 {
                log::warn!(
                    "[BOOK] short venue holds a LONG {s} {}: not part of the hedge",
                    c.symbol
                );
            }
        }
        Ok(snap)
    }

    /// One taker IOC on one leg of one symbol. `roll`: a ROLL leg — sent at
    /// its ABSOLUTE tick-rounded limit (bound to the reserved mark) instead of
    /// the touch ± `HEDGE_TAKER_SLIPPAGE_BPS` the planner uses, booked at that
    /// anchor mark, and settling the roll's in-flight reservation. Returns
    /// the size the venue reports filled (position delta — what the book is
    /// re-planned from) with the fee / value of its own fills when read.
    async fn execute(
        &mut self,
        symbol: &str,
        order: &Order,
        mark: f64,
        roll: Option<RollCtx>,
    ) -> Result<ExecResult> {
        // Roll accounting values slippage against THIS leg's venue mark (the
        // planner passes the long mark; an Arcus leg must not book the venue
        // basis as cost or credit). A roll leg books against the mark its
        // reservation and limit were anchored at.
        let leg_mark = self.last_snapshot.mark_of(order.leg, symbol);
        let book_mark = if roll.is_some() || leg_mark <= 0.0 {
            mark
        } else {
            leg_mark
        };
        let roll_sym = roll.is_some().then_some(symbol);
        let (side, reduce_only) = order_side(order);
        let is_buy = side == OrderSide::Long;
        let venue = self.venue(order.leg);
        let qty = order.qty.abs();
        if self.cfg.dry_run {
            log::info!(
                "[DRY_RUN] {} {side} {symbol} qty={qty} reduce_only={reduce_only} @~{mark:.2}",
                venue.name
            );
            let instance = venue.instance.clone();
            let exchange = venue.kind.dex_name();
            let signed = if order.qty > 0.0 { qty } else { -qty };
            let b = self.state.book_mut(symbol);
            match order.leg {
                Leg::Long => b.dry_long_qty = (b.dry_long_qty + signed).max(0.0),
                Leg::Short => b.dry_short_qty = (b.dry_short_qty + signed).max(0.0),
            }
            self.event(
                "fill",
                serde_json::json!({
                "symbol": symbol, "leg": format!("{:?}", order.leg), "venue": instance,
                "exchange": exchange,
                "side": format!("{side}"), "qty": qty, "reduce_only": reduce_only,
                "price": mark, "dry_run": true, "roll": roll.is_some() }),
            );
            self.roll_book_execution(
                roll_sym,
                order.leg,
                is_buy,
                qty,
                0.0,
                qty * book_mark,
                book_mark,
            );
            self.persist();
            return Ok(ExecResult {
                filled: qty,
                fee: 0.0,
                value: qty * book_mark,
                order_id: "dry_run".to_string(),
                ordered_price: roll
                    .map(|r| r.limit)
                    .or_else(|| Decimal::from_f64(mark))
                    .unwrap_or_default(),
            });
        }
        let before = venue.signed_qty(symbol).await?;
        let size = decimal(qty, "qty")?.round_dp(self.meta(symbol).size_decimals);
        let exchange = venue.kind;
        let sent = match roll.map(|r| r.limit) {
            Some(limit) => {
                venue
                    .dex
                    .create_order_taker_ioc_at(symbol, size, side, limit, reduce_only)
                    .await
            }
            None => {
                venue
                    .dex
                    .create_order_taker_ioc(
                        symbol,
                        size,
                        side,
                        self.cfg.taker_slippage_bps,
                        reduce_only,
                    )
                    .await
            }
        };
        let resp = match sent {
            Ok(r) => r,
            Err(DexError::ReconciliationRequired { detail, .. }) => {
                // The venue may have taken it: never re-send before the
                // position says what happened.
                let reason = format!("submission ambiguous: {detail}");
                self.mark_uncertain(
                    symbol,
                    order.leg,
                    exchange,
                    None,
                    qty,
                    before,
                    book_mark,
                    roll.is_some(),
                    &reason,
                );
                bail!(
                    "{} IOC {side} {symbol} {size}: {reason}",
                    self.venue(order.leg).name
                );
            }
            Err(e) => bail!("{} IOC {side} {symbol} {size}: {e:?}", venue.name),
        };
        let order_id = resp.order_id.clone();
        let (filled, fills) = if exchange == VenueKind::Arcus {
            // Arcus acks with 202: the fill is final only once the order is
            // out of the open orders AND its fills (or its cancel) are
            // visible and agree with the position change.
            match self
                .settle_arcus(order.leg, symbol, &order_id, qty, before)
                .await
            {
                Some((f, fee, value)) => (f, Some((fee, value))),
                None => {
                    let reason = format!(
                        "not terminal after {:?}s (order {order_id})",
                        self.cfg.fill_wait_secs
                    );
                    self.mark_uncertain(
                        symbol,
                        order.leg,
                        exchange,
                        Some(order_id.clone()),
                        qty,
                        before,
                        book_mark,
                        roll.is_some(),
                        &reason,
                    );
                    bail!(
                        "{} IOC {side} {symbol} {size}: {reason}",
                        self.venue(order.leg).name
                    );
                }
            }
        } else {
            // Lighter IOC is final on ack; read more than once so a
            // not-yet-visible fill is not mistaken for none (re-requesting
            // it is the one way to overshoot).
            let mut filled = 0.0;
            for &wait in &self.cfg.fill_wait_secs {
                tokio::time::sleep(Duration::from_secs(wait)).await;
                let after = venue.signed_qty(symbol).await?;
                filled = (after - before).abs();
                if filled > 0.0 {
                    break;
                }
            }
            (filled, None)
        };
        // Every settled execution on the rolled (Arcus) leg books its actual
        // value and cost into the roll week — roll legs and the planner's
        // levelling / repairs / ARM builds alike.
        if let Some((fee, value)) = fills {
            self.roll_book_execution(roll_sym, order.leg, is_buy, filled, fee, value, book_mark);
            self.persist();
        }
        let venue = self.venue(order.leg);
        log::info!(
            "[FILL] {} {side} {symbol} req={qty} filled={filled:.5} reduce_only={reduce_only} limit={} order_id={}",
            venue.name, resp.ordered_price, resp.order_id
        );
        self.event(
            "fill",
            serde_json::json!({
            "symbol": symbol, "leg": format!("{:?}", order.leg), "venue": venue.instance,
            "exchange": venue.kind.dex_name(),
            "side": format!("{side}"), "req": qty, "filled": filled, "reduce_only": reduce_only,
            "limit": resp.ordered_price.to_string(), "order_id": resp.order_id, "mark": mark,
            "roll": roll.is_some() }),
        );
        let (fee, value) = fills.unwrap_or((0.0, filled * book_mark));
        Ok(ExecResult {
            filled,
            fee,
            value,
            order_id,
            ordered_price: resp.ordered_price,
        })
    }

    fn venue(&self, leg: Leg) -> &Venue {
        match leg {
            Leg::Long => &self.long,
            Leg::Short => &self.short,
        }
    }

    /// Half a size step: two sizes closer than this are the same order size.
    fn size_tol(&self, symbol: &str) -> f64 {
        0.5 * 10f64.powi(-(self.meta(symbol).size_decimals as i32))
    }

    #[allow(clippy::too_many_arguments)]
    fn mark_uncertain(
        &mut self,
        symbol: &str,
        leg: Leg,
        exchange: VenueKind,
        order_id: Option<String>,
        qty: f64,
        before_qty: f64,
        mark: f64,
        roll: bool,
        reason: &str,
    ) {
        log::error!(
            "[UNCERTAIN] {symbol} {leg:?} ({}) {reason} — no order on {symbol} until reconciled",
            exchange.dex_name()
        );
        let u = UncertainOrder {
            leg,
            exchange: exchange.dex_name().to_string(),
            instance: self.venue(leg).instance.clone(),
            order_id,
            qty,
            before_qty,
            mark,
            sent_at: now_secs(),
            reason: reason.to_string(),
            roll,
        };
        self.event(
            "uncertain",
            serde_json::json!({ "symbol": symbol, "order": u }),
        );
        self.state.uncertain.insert(symbol.to_string(), u);
        self.persist();
    }

    /// One read of an Arcus order's state. `None` when a read failed.
    async fn arcus_order_view(&self, leg: Leg, symbol: &str, order_id: &str) -> Option<OrderView> {
        let dex = &self.venue(leg).dex;
        let open = dex
            .get_open_orders(symbol)
            .await
            .ok()?
            .orders
            .iter()
            .any(|o| o.order_id == order_id);
        let canceled = dex
            .get_canceled_orders(symbol)
            .await
            .ok()?
            .orders
            .iter()
            .any(|o| o.order_id == order_id);
        let mut view = OrderView {
            open,
            canceled,
            ..OrderView::default()
        };
        for f in dex.get_filled_orders(symbol).await.ok()?.orders {
            if f.order_id == order_id {
                view.filled += f.filled_size.and_then(|d| d.to_f64()).unwrap_or(0.0).abs();
                view.fee += f.filled_fee.and_then(|d| d.to_f64()).unwrap_or(0.0);
                view.value += f.filled_value.and_then(|d| d.to_f64()).unwrap_or(0.0).abs();
                view.trades.push(f.trade_id);
            }
        }
        Some(view)
    }

    /// Settle an Arcus IOC within `HEDGE_FILL_WAIT_SECS`: `Some((filled,
    /// fee, value))` once terminal, `None` when still unknown after the last
    /// wait.
    async fn settle_arcus(
        &mut self,
        leg: Leg,
        symbol: &str,
        order_id: &str,
        qty: f64,
        before: f64,
    ) -> Option<(f64, f64, f64)> {
        let tol = self.size_tol(symbol);
        for &wait in &self.cfg.fill_wait_secs.clone() {
            tokio::time::sleep(Duration::from_secs(wait)).await;
            let Some(v) = self.arcus_order_view(leg, symbol, order_id).await else {
                continue;
            };
            let Ok(after) = self.venue(leg).signed_qty(symbol).await else {
                continue;
            };
            if let Settle::Terminal(f) = settle_decision(
                qty,
                v.open,
                v.canceled,
                v.filled,
                (after - before).abs(),
                tol,
            ) {
                self.forget_arcus_activity(leg, symbol, order_id, &v.trades)
                    .await;
                return Some((f, v.fee, v.value));
            }
        }
        None
    }

    /// Drop consumed fill / cancel records from the connector's pending
    /// lists, so they are not re-read (and do not pile up) later.
    async fn forget_arcus_activity(
        &self,
        leg: Leg,
        symbol: &str,
        order_id: &str,
        trades: &[String],
    ) {
        let dex = &self.venue(leg).dex;
        for t in trades {
            let _ = dex.clear_filled_order(symbol, t).await;
        }
        let _ = dex.clear_canceled_order(symbol, order_id).await;
    }

    /// One roll leg (reduce when `order.qty < 0`, re-open when > 0) on the
    /// Arcus leg: a taker IOC at an ABSOLUTE limit bound to the reserved
    /// mark and rounded to the tick exactly as the venue will receive it
    /// (`roll_limit_dec`), refused before the send if the leg's mark has moved
    /// beyond the margin the reservation covers or that limit cannot reach
    /// the touch. Terminal before it returns (it settles through `execute`,
    /// which also books its fills); every roll IOC that reached the venue
    /// emits `roll_leg` (settled, uncertain, or accepted-but-unmarketable).
    /// `Err` = nothing executed, or the order became uncertain (the caller
    /// tells the two apart with `roll_leg_sent`). `fresh_tick`: the caller has
    /// just read the ticker and `mark` IS that fresh mark (the re-open), so
    /// the price guard's own ticker read is skipped; carries its tick.
    async fn roll_one(
        &mut self,
        symbol: &str,
        order: &Order,
        mark: f64,
        kind: &'static str,
        fresh_tick: Option<Option<Decimal>>,
        reserve: (f64, f64),
    ) -> Result<RollLeg> {
        // buys = long grows / short shrinks, from the send's own mapping
        let is_buy = order_side(order).0 == OrderSide::Long;
        let tick = match fresh_tick {
            Some(t) => t,
            None => self.roll_price_guard(symbol, order.leg, mark).await?,
        };
        let limit = roll_limit_dec(is_buy, mark, tick).ok_or_else(|| {
            anyhow!("roll IOC {symbol}: no usable limit from mark {mark} — not sent")
        })?;
        // An IOC that cannot reach the touch would be accepted, fill nothing
        // and still spend the interval: refuse it before the send.
        let book = self
            .venue(order.leg)
            .dex
            .get_order_book(symbol, 1)
            .await
            .map_err(|e| anyhow!("roll book {symbol}: {e:?}"))?;
        let bid = book.bids.first().map(|l| l.price);
        let ask = book.asks.first().map(|l| l.price);
        if !roll_limit_crosses(is_buy, limit, bid, ask) {
            bail!("roll IOC {symbol}: limit {limit} does not reach the touch (bid {bid:?} / ask {ask:?}) — not sent");
        }
        // Reserve only now, every pre-send refusal above having passed: a
        // crash before this point leaves no reservation for the restart to
        // book as a phantom roll; one after it is booked conservatively.
        roll_reserve_for_send(&mut self.state.roll, symbol, reserve);
        self.persist();
        let r = match self
            .execute(symbol, order, mark, Some(RollCtx { limit }))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                // It reached the venue and may fill: the per-leg record the
                // #1075 reconciliation needs most (a roll gate never runs on
                // an uncertain symbol, so a roll entry here is this send's).
                if let Some(u) = self.state.uncertain.get(symbol).filter(|u| u.roll) {
                    let oid = u.order_id.clone();
                    self.event(
                        "roll_leg",
                        serde_json::json!({ "symbol": symbol, "leg": format!("{:?}", order.leg),
                            "kind": kind, "status": "uncertain", "order_id": oid,
                            "req": order.qty.abs(), "mark": mark, "limit": limit.to_string() }),
                    );
                }
                return Err(e);
            }
        };
        let cost = roll_cost(is_buy, r.filled, r.fee, r.value, mark);
        let unmarketable = roll_unfilled_is_refusal(is_buy, r.filled, r.ordered_price, bid, ask);
        self.event(
            "roll_leg",
            serde_json::json!({ "symbol": symbol, "leg": format!("{:?}", order.leg),
                "kind": kind,
                "status": if unmarketable { "sent_unmarketable" } else { "settled" },
                "order_id": r.order_id, "req": order.qty.abs(),
                "filled": r.filled, "fee": r.fee, "value": r.value, "mark": mark,
                "limit": limit.to_string(), "sent_price": r.ordered_price.to_string(),
                "cost_usd": cost }),
        );
        if unmarketable {
            // The connector rounded our limit to a different tick (a tier
            // boundary) and it did not reach the pre-send touch: nothing
            // executed — a send-time refusal, not a roll that ran.
            bail!(
                "roll IOC {symbol}: sent at {} (ours {limit}) did not reach the touch (bid {bid:?} / ask {ask:?}) — refusal",
                r.ordered_price
            );
        }
        Ok(RollLeg {
            filled: r.filled,
            cost,
        })
    }

    /// (mark, clip qty, gates) for rolling `sym` on `leg`. The clip is the
    /// configured notional floored to the size decimals and is NEVER raised
    /// to the venue minimum (that would exceed the clip / net-tolerance
    /// bound the config validated); a clip below the minimum blocks.
    fn roll_gate(
        &self,
        sym: &str,
        leg: Leg,
        snap: &Snapshot,
        kill: bool,
        now: u64,
    ) -> (f64, f64, RollGate) {
        let mark = snap.mark_of(leg, sym);
        let m = self.meta(sym);
        // Size the clip at the HIGHER of the two venues' marks: the net
        // guard values |long − short| at the long mark, so a clip sized at a
        // cheaper Arcus mark could exceed the net tolerance it was validated
        // against (the feed gate allows up to 2 % divergence).
        let sizing_mark = roll_sizing_mark(mark, snap.long.mark(sym));
        let clip_qty = qty_for_notional(self.cfg.roll.clip_usd, sizing_mark, m.size_decimals);
        let held = snap.book(sym);
        let leg_qty = match leg {
            Leg::Long => held.long,
            Leg::Short => held.short,
        };
        let headroom = liq_headroom_pct(snap.equity(leg), &snap.legs(&self.cfg, leg));
        // (only armed books reach here: maybe_roll pre-filters on Mode::On)
        let gate = RollGate {
            backed_off: roll_symbol_backed_off(&self.state.roll, sym, now),
            balanced: roll_balanced(
                held.net(),
                leg,
                clip_qty,
                snap.long.mark(sym),
                self.cfg.net_tolerance_usd,
            ),
            halted: self.state.halted,
            kill,
            uncertain: self.state.uncertain.contains_key(sym),
            feed_ok: self.feed_problem.is_none(),
            headroom_ok: headroom.is_none_or(|h| h >= self.cfg.liq_guard_pct),
            leverage_ok: self.cfg.dry_run
                || leverage_ok(
                    snap.gross(&self.cfg, leg),
                    snap.equity(leg),
                    self.cfg.max_leverage,
                ),
            clip_ok: clip_qty > 0.0 && clip_qty >= m.min_qty,
            leg_holds_clip: clip_qty > 0.0 && leg_qty >= clip_qty,
        };
        (mark, clip_qty, gate)
    }

    /// Book one settled execution into the roll week (of the booking time)
    /// when it is on the rolled (Arcus) leg. For a ROLL leg (`roll_sym`) the
    /// booking and the drop of its in-flight reservation are one mutation.
    /// Does NOT persist: `execute` persists once per settled execution.
    #[allow(clippy::too_many_arguments)]
    fn roll_book_execution(
        &mut self,
        roll_sym: Option<&str>,
        leg: Leg,
        is_buy: bool,
        filled: f64,
        fee: f64,
        value: f64,
        mark: f64,
    ) {
        let booking = roll_execution_booking(
            self.cfg.roll_accounting_leg(),
            leg,
            is_buy,
            filled,
            fee,
            value,
            mark,
        );
        let now = now_secs();
        match roll_sym {
            Some(sym) => roll_settle_leg(&mut self.state.roll, sym, now, booking),
            None => {
                if let Some(b) = booking {
                    roll_book_actual(&mut self.state.roll, now, b);
                }
            }
        }
    }

    /// Refuse to send a roll leg once the leg's mark has moved beyond the
    /// margin its reservation covers (not an uncertain failure: nothing sent).
    /// Returns the venue's tick at that price (for the limit's rounding).
    async fn roll_price_guard(
        &self,
        symbol: &str,
        leg: Leg,
        reserved_mark: f64,
    ) -> Result<Option<Decimal>> {
        let venue = self.venue(leg);
        let t = venue
            .dex
            .get_ticker(symbol, None)
            .await
            .map_err(|e| anyhow!("{} get_ticker {symbol}: {e:?}", venue.name))?;
        let fresh = t.price.to_f64().unwrap_or(0.0);
        if !roll_price_ok(reserved_mark, fresh) {
            bail!(
                "roll price moved: {symbol} mark {fresh} vs reserved {reserved_mark} (> {} %) — not sent",
                ROLL_PRICE_MARGIN * 100.0
            );
        }
        Ok(t.min_tick)
    }

    /// Roll one clip of the Arcus leg when due and every gate is open.
    /// Strictly sequential: the re-open starts only after the close is
    /// terminal, sized to what the close filled. A failed or uncertain
    /// re-open leaves the book one clip lopsided (within the net tolerance by
    /// config): the uncertain guard holds the symbol until reconciled, then
    /// the normal levelling grows the smaller leg back.
    ///
    /// Runs only on a tick that sent nothing and reconciled nothing (the
    /// caller guarantees it), so `snap` is fresh for every symbol; every
    /// booking rolls the week over to its own time (`roll_book_actual`). It
    /// blocks the tick for at most three IOC settles — close, re-open, or
    /// close + the KILL_SWITCH levelling IOC — (≈ 3 × the sum of
    /// `HEDGE_FILL_WAIT_SECS`), plus the ticker / book / equity reads.
    /// Returns the number of roll orders it sent (for `orders_this_tick`).
    async fn maybe_roll(&mut self, now: u64, snap: &Snapshot, kill: bool) -> usize {
        if !self.cfg.roll.enabled {
            return 0;
        }
        let Some(leg) = self.cfg.roll_leg() else {
            return 0;
        };
        // Round robin over the armed books; a blocked book is skipped so it
        // cannot starve the others.
        let syms: Vec<String> = self.cfg.symbol_names();
        let start = self
            .state
            .roll
            .last_symbol
            .as_ref()
            .and_then(|l| syms.iter().position(|s| s == l))
            .map(|i| i + 1)
            .unwrap_or(0);
        let order: Vec<String> = (0..syms.len())
            .map(|k| syms[(start + k) % syms.len()].clone())
            .filter(|s| self.state.book(s).mode == Mode::On)
            .collect();
        if order.is_empty() {
            return 0;
        }
        let picked = pick_roll_symbol(&order, |sym| {
            let (mark, clip_qty, gate) = self.roll_gate(sym, leg, snap, kill, now);
            // exactly what the two legs will reserve
            let (leg_v, leg_c) = roll_reserve(clip_qty * mark, self.cfg.taker_slippage_bps);
            match roll_blocker(
                &self.cfg.roll,
                &self.state.roll,
                now,
                &gate,
                2.0 * leg_v,
                2.0 * leg_c,
            ) {
                None => Ok((mark, clip_qty)),
                Some(why) => Err(why),
            }
        });
        let (sym, (mark, clip_qty)) = match picked {
            Ok(v) => v,
            Err(first) => {
                // a symbol backing off keeps its refusal's reason (set when it
                // was refused) instead of overwriting it with "refusal_backoff"
                if first.as_ref().is_some_and(|(_, w)| *w == "refusal_backoff") {
                    return 0;
                }
                let why = first
                    .as_ref()
                    .filter(|(_, w)| *w != "interval")
                    .map(|(s, w)| format!("{s}: {w}"));
                if self.state.roll.blocked_reason != why {
                    if let Some(w) = &why {
                        log::info!("[ROLL] blocked: {w}");
                        self.event("roll_blocked", serde_json::json!({ "reason": w }));
                    }
                    self.state.roll.blocked_reason = why;
                    self.persist();
                }
                return 0;
            }
        };
        let m = self.meta(&sym);
        log::info!("[ROLL] {sym} {leg:?} clip {clip_qty} @ {mark:.2}");
        self.event(
            "roll_start",
            serde_json::json!({ "symbol": sym, "leg": format!("{leg:?}"), "clip_qty": clip_qty,
                "mark": mark }),
        );
        self.state.roll.last_symbol = Some(sym.clone());
        self.state.roll.blocked_reason = None;
        let close = Order {
            leg,
            qty: -clip_qty,
        };
        // Each leg is reserved (worst case) before it is sent; its fills
        // book themselves where they settle and the reservation is dropped,
        // unless the leg ends uncertain (then it stays until released).
        let slip = self.cfg.taker_slippage_bps;
        let (close_leg, close_sent) = self
            .roll_leg_send(&sym, &close, mark, clip_qty * mark, slip, "close", None)
            .await;
        let close_filled = close_leg.as_ref().map(|l| l.filled).unwrap_or(0.0);
        let close_cost = close_leg.as_ref().map(|l| l.cost).unwrap_or(0.0);
        let close_err = close_leg.as_ref().err().map(|e| format!("{e:#}"));
        let (mut open_filled, mut open_cost) = (0.0, 0.0);
        let mut reopen_sent = false;
        // a KILL_SWITCH levelling reduction of the other leg (not a roll leg)
        let mut levelling_sent = false;
        let mut outcome = "done";
        if let Some(e) = &close_err {
            log::warn!("[ROLL] {sym} close: {e}");
        }
        match roll_close_stage(
            leg,
            close_sent,
            self.state.uncertain.contains_key(&sym),
            close_filled,
            m.min_qty,
            m.size_decimals,
        ) {
            // close_partial_below_min: the planner repairs the shrunken leg
            // later and that execution books itself when it settles
            Err(o) => outcome = o,
            // re-open exactly what the close filled
            Ok(reopen) => {
                // The re-open goes out only after the close settled (seconds
                // later): re-read the growth gates (KILL_SWITCH; Arcus-leg
                // headroom and leverage from a FRESH equity read) and re-anchor
                // it on a FRESH mark — refused when that mark moved beyond the
                // reservation margin from the gated one, or when its fresh
                // reservation no longer fits the weekly caps.
                // A refused re-open leaves the leg one clip short:
                // the planner grows it back on the next unrestricted tick (that
                // execution books itself) — except under KILL_SWITCH, where the
                // planner makes no growth, so the gap is levelled here by a
                // REDUCTION of the other leg.
                let kill_now = self.sentinels.kill_switch_engaged();
                // one ticker read gives the fresh mark AND its tick; roll_one
                // then skips its own guard read (the mark is that read)
                let fresh = if kill_now {
                    None
                } else {
                    match self.venue(leg).dex.get_ticker(&sym, None).await {
                        Ok(t) => {
                            let p = t.price.to_f64().unwrap_or(0.0);
                            (p > 0.0).then_some((p, t.min_tick))
                        }
                        Err(e) => {
                            log::warn!("[ROLL] {sym} re-open: fresh mark read failed: {e:?}");
                            None
                        }
                    }
                };
                // FRESH equity and FRESH positions (live) — the snapshot's are
                // from the start of the tick, before the close settled.
                let fresh_account: Option<(f64, std::collections::BTreeMap<String, f64>)> =
                    if kill_now || fresh.is_none() {
                        None
                    } else if self.cfg.dry_run {
                        // DRY_RUN never reads the venue account: the simulated
                        // equity and book the snapshot and the roll gate use.
                        let dry: std::collections::BTreeMap<String, f64> = self
                            .cfg
                            .symbols
                            .iter()
                            .map(|c| {
                                (
                                    c.symbol.clone(),
                                    roll_dry_leg_qty(&self.state.book(&c.symbol), leg),
                                )
                            })
                            .collect();
                        Some((self.cfg.dry_run_equity_usd, dry))
                    } else {
                        match (
                            self.venue(leg).equity().await,
                            self.venue(leg).positions().await,
                        ) {
                            (Ok(e), Ok(p)) => Some((e, p)),
                            (e, p) => {
                                log::warn!(
                                    "[ROLL] {sym} re-open: fresh account read failed (equity ok={} positions ok={})",
                                    e.is_ok(),
                                    p.is_ok()
                                );
                                None
                            }
                        }
                    };
                // FRESH marks for every OTHER held symbol too: the gates are
                // account-wide, and a stale tick-start mark would understate a
                // market that moved while the close settled.
                let mut fresh_marks: Option<std::collections::BTreeMap<String, f64>> =
                    fresh_account.as_ref().map(|_| Default::default());
                if let (Some(marks), Some((_, pos))) = (fresh_marks.as_mut(), &fresh_account) {
                    for (s, q) in pos {
                        if *s == sym || q.abs() <= 0.0 {
                            continue;
                        }
                        match self.venue(leg).dex.get_ticker(s, None).await {
                            Ok(t) if t.price.to_f64().is_some_and(|p| p > 0.0) => {
                                marks.insert(s.clone(), t.price.to_f64().unwrap_or(0.0));
                            }
                            other => {
                                log::warn!(
                                    "[ROLL] {sym} re-open: fresh {s} mark read failed ({})",
                                    if other.is_ok() { "no price" } else { "error" }
                                );
                                fresh_marks = None;
                                break;
                            }
                        }
                    }
                }
                let gate = match (kill_now, &fresh, &fresh_account, &fresh_marks) {
                    (true, _, _, _) => roll_reopen_blocked(true, true, true),
                    (false, Some((f, _)), Some((eq, pos)), Some(marks)) => {
                        let legs_after = reopen_legs_after(
                            &self.cfg.symbols,
                            leg,
                            |s| pos.get(s).copied().unwrap_or(0.0),
                            |s| marks.get(s).copied().unwrap_or(0.0),
                            &sym,
                            reopen.qty,
                            *f,
                        );
                        let gross_after: f64 = legs_after.iter().map(|(n, _)| n).sum();
                        let headroom = liq_headroom_pct(*eq, &legs_after);
                        roll_reopen_blocked(
                            false,
                            headroom.is_none_or(|h| h >= self.cfg.liq_guard_pct),
                            self.cfg.dry_run
                                || leverage_ok(gross_after, *eq, self.cfg.max_leverage),
                        )
                    }
                    _ => Some("reopen_failed"),
                };
                let fresh = if gate.is_some() { None } else { fresh };
                // price moved beyond the band, or the fresh reservation no
                // longer fits the caps: the re-open is refused
                let reopen_refused = match (gate, fresh) {
                    (None, Some((f, _))) => roll_reopen_decision(
                        &self.cfg.roll,
                        &self.state.roll,
                        mark,
                        f,
                        roll_reserve(reopen.qty * f, slip),
                    ),
                    _ => None,
                };
                match (gate, fresh) {
                    (Some("reopen_blocked_kill"), _) => {
                        outcome = "reopen_blocked_kill_unlevelled";
                        let other = match leg {
                            Leg::Long => Leg::Short,
                            Leg::Short => Leg::Long,
                        };
                        // DRY_RUN rehearses on the simulated book, live on the venue
                        let other_qty = if self.cfg.dry_run {
                            roll_dry_leg_qty(&self.state.book(&sym), other)
                        } else {
                            match self.venue(other).signed_qty(&sym).await {
                                Ok(q) => q.abs(),
                                Err(e) => {
                                    log::error!("[ROLL] {sym} KILL_SWITCH: other-leg read failed: {e:#} — one clip stays unlevelled");
                                    0.0
                                }
                            }
                        };
                        if let Some(q) =
                            roll_kill_levelling(close_filled, other_qty, m.min_qty, m.size_decimals)
                        {
                            let reduce = Order {
                                leg: other,
                                qty: -q,
                            };
                            let other_mark = snap.mark_of(other, &sym);
                            let res = self.execute(&sym, &reduce, other_mark, None).await;
                            // counted only once it reached the venue: accepted,
                            // or it became uncertain (a pre-send failure or a
                            // synchronous reject sent nothing)
                            levelling_sent = roll_leg_sent(
                                res.is_ok(),
                                self.state
                                    .uncertain
                                    .get(&sym)
                                    .is_some_and(|u| u.leg == other),
                            );
                            match res {
                                Ok(r) if r.filled + self.size_tol(&sym) >= q => {
                                    outcome = "reopen_blocked_kill_levelled";
                                }
                                Ok(r) => log::error!(
                                    "[ROLL] {sym} KILL_SWITCH levelling filled {} of {q} — the rest stays unlevelled",
                                    r.filled
                                ),
                                Err(e) => log::error!(
                                    "[ROLL] {sym} KILL_SWITCH levelling failed: {e:#} — one clip stays unlevelled"
                                ),
                            }
                        }
                        log::warn!("[ROLL] {sym} re-open not sent (KILL_SWITCH): {outcome}");
                    }
                    (Some(blocked), _) => {
                        log::warn!(
                            "[ROLL] {sym} re-open not sent: {blocked} — the planner / guards take it from here"
                        );
                        outcome = blocked;
                    }
                    (None, None) => outcome = "reopen_failed",
                    (None, Some((fresh, _))) if reopen_refused.is_some() => {
                        let why = reopen_refused.unwrap_or("reopen_failed");
                        log::warn!(
                            "[ROLL] {sym} re-open not sent: {why} (gated mark {mark}, fresh {fresh}) — the planner repairs the gap"
                        );
                        outcome = why;
                    }
                    (None, Some((fresh, fresh_tick))) => {
                        let (res, sent) = self
                            .roll_leg_send(
                                &sym,
                                &reopen,
                                fresh,
                                reopen.qty * fresh,
                                slip,
                                "reopen",
                                Some(fresh_tick),
                            )
                            .await;
                        reopen_sent = sent;
                        match res {
                            Ok(l) => {
                                open_filled = l.filled;
                                open_cost = l.cost;
                                if l.filled + self.size_tol(&sym) < close_filled {
                                    outcome = "reopen_partial";
                                }
                            }
                            Err(e) => {
                                log::error!("[ROLL] {sym} re-open failed: {e:?} — levelling repairs it (booked when it settles)");
                                outcome = "reopen_failed";
                            }
                        }
                        if self.state.uncertain.contains_key(&sym) {
                            outcome = "reopen_uncertain";
                        }
                    }
                }
            }
        }
        let roll_orders = usize::from(close_sent) + usize::from(reopen_sent);
        let sent_orders = roll_orders + usize::from(levelling_sent);
        let suspended = roll_after_attempt(&mut self.state.roll, &sym, now, roll_orders > 0);
        if roll_orders == 0 {
            // refused before any send, or rejected synchronously (price
            // moved, a read failed, the venue refused it): nothing executed;
            // back THIS symbol off (doubling; suspended after
            // ROLL_MAX_REFUSALS), and say why — once per distinct reason
            let sb = self
                .state
                .roll
                .backoff
                .get(&sym)
                .copied()
                .unwrap_or_default();
            let refusals = sb.refusals;
            let why = if suspended {
                format!(
                    "{sym}: suspended until the next roll week after {refusals} consecutive refusals: {}",
                    close_err.clone().unwrap_or_default()
                )
            } else {
                format!("{sym}: not sent: {}", close_err.clone().unwrap_or_default())
            };
            if self.state.roll.blocked_reason.as_deref() != Some(why.as_str()) {
                log::warn!("[ROLL] blocked: {why}");
                self.event(
                    "roll_blocked",
                    serde_json::json!({ "symbol": sym, "reason": why, "refusals": refusals,
                        "retry_not_before": sb.retry_not_before, "suspended": suspended }),
                );
            }
            self.state.roll.blocked_reason = Some(why);
        }
        let rs = &mut self.state.roll;
        let (vol, cost) = (rs.week_volume_usd, rs.week_cost_usd);
        let (fly_v, fly_c) = roll_in_flight_total(rs);
        self.event(
            "roll_done",
            serde_json::json!({ "symbol": sym, "leg": format!("{leg:?}"), "outcome": outcome,
                "close_filled": close_filled, "reopen_filled": open_filled, "close_error": close_err,
                "sent_orders": sent_orders,
                "cost_usd": close_cost + open_cost, "week_volume_usd": vol, "week_cost_usd": cost,
                "in_flight_volume_usd": fly_v, "in_flight_cost_usd": fly_c }),
        );
        log::info!("[ROLL] {sym} {outcome}: close {close_filled} / reopen {open_filled}, cost ${:.2}, week ${vol:.0} / ${cost:.2} (+ in flight ${fly_v:.0} / ${fly_c:.2})", close_cost + open_cost);
        self.persist();
        sent_orders
    }

    /// Reserve, send and finish one roll leg: the reservation is persisted
    /// right before the send (`roll_one`, after every pre-send refusal) and
    /// dropped afterwards unless the leg ended uncertain.
    /// Returns the leg and whether it counts as SENT (`roll_leg_sent`).
    async fn roll_leg_send(
        &mut self,
        sym: &str,
        order: &Order,
        mark: f64,
        notional_usd: f64,
        slip: u32,
        kind: &'static str,
        fresh_tick: Option<Option<Decimal>>,
    ) -> (Result<RollLeg>, bool) {
        let was_uncertain = self.state.uncertain.contains_key(sym);
        let reserve = roll_reserve(notional_usd, slip);
        let result = self
            .roll_one(sym, order, mark, kind, fresh_tick, reserve)
            .await;
        let uncertain = self.state.uncertain.contains_key(sym);
        if roll_finish_in_flight(&mut self.state.roll, sym, result.is_ok(), uncertain) {
            self.persist();
        }
        let sent = roll_leg_sent(result.is_ok(), uncertain && !was_uncertain);
        (result, sent)
    }

    /// Re-check every uncertain order; release the ones that are now known
    /// terminal. Runs before planning: while an entry remains, its symbol
    /// gets no order at all. Returns the symbols released in THIS call: the
    /// tick's snapshot predates the release (a delayed fill may only have
    /// become visible now), so those stay blocked until the next tick plans
    /// them from a fresh snapshot.
    async fn reconcile_uncertain(&mut self, now: u64) -> std::collections::BTreeSet<String> {
        let mut released_now = std::collections::BTreeSet::new();
        let pending: Vec<(String, UncertainOrder)> = self
            .state
            .uncertain
            .iter()
            .map(|(s, u)| (s.clone(), u.clone()))
            .collect();
        for (sym, u) in pending {
            let (cur_exchange, cur_instance) = {
                let v = self.venue(u.leg);
                (v.kind.dex_name().to_string(), v.instance.clone())
            };
            // Never read another account's orders / fills / position as if
            // they were this order's evidence.
            let same_account =
                u.exchange == cur_exchange && (u.instance.is_empty() || u.instance == cur_instance);
            let mut terminal = false;
            let mut delta = f64::NAN;
            // (filled, fee, value) of the order as last observed (Arcus)
            let mut observed: Option<(f64, f64, f64)> = None;
            // Evidence reads may fail independently of the tick snapshot; a
            // failed read is simply "no evidence" — the grace / account
            // escalation below must still run, or an unreadable endpoint
            // would block the symbol for ever without the operator halt.
            let evidence = if same_account {
                let dex = self.venue(u.leg).dex.clone();
                match (
                    dex.get_open_orders(&sym).await,
                    self.venue(u.leg).signed_qty(&sym).await,
                ) {
                    (Ok(open_orders), Ok(after)) => Some((open_orders, after)),
                    (open_orders, after) => {
                        log::warn!(
                            "[UNCERTAIN] {sym}: evidence read failed (open_orders ok={} position ok={})",
                            open_orders.is_ok(),
                            after.is_ok()
                        );
                        None
                    }
                }
            } else {
                None
            };
            if let Some((open_orders, after)) = evidence {
                delta = (after - u.before_qty).abs();
                match (&u.order_id, u.exchange.as_str()) {
                    // Arcus: the order's own fills / cancel must settle it.
                    (Some(id), "arcus") => {
                        if let Some(v) = self.arcus_order_view(u.leg, &sym, id).await {
                            if let Settle::Terminal(_) = settle_decision(
                                u.qty,
                                v.open,
                                v.canceled,
                                v.filled,
                                delta,
                                self.size_tol(&sym),
                            ) {
                                self.forget_arcus_activity(u.leg, &sym, id, &v.trades).await;
                                observed = Some((v.filled, v.fee, v.value));
                                terminal = true;
                            }
                        }
                    }
                    // Lighter IOC is final on ack: a known order that is no
                    // longer open is terminal and the position read is final.
                    (Some(id), _) => {
                        terminal = !open_orders.orders.iter().any(|o| &o.order_id == id);
                    }
                    // No order id (an ambiguous submission): no evidence.
                    (None, _) => {}
                }
            }
            match uncertain_action(
                &u,
                &cur_exchange,
                &cur_instance,
                terminal,
                now,
                self.cfg.uncertain_grace_secs,
            ) {
                UncertainAction::Release => {
                    log::warn!(
                        "[UNCERTAIN] {sym} {:?} order {:?} reconciled: position moved {delta} of {} requested",
                        u.leg, u.order_id, u.qty
                    );
                    self.event(
                        "uncertain_resolved",
                        serde_json::json!({ "symbol": sym, "order": u, "position_delta": delta }),
                    );
                    // roll accounting: the order's observed fills replace its
                    // reservation (any execution on the rolled leg counts)
                    if self.cfg.roll_accounting_leg() == Some(u.leg) {
                        let (f, fee, value) = observed.unwrap_or((0.0, 0.0, 0.0));
                        roll_release_booking(
                            &mut self.state.roll,
                            now,
                            &sym,
                            u.roll,
                            f,
                            fee,
                            value,
                            u.mark,
                        );
                    }
                    self.state.uncertain.remove(&sym);
                    released_now.insert(sym);
                    self.persist();
                }
                UncertainAction::Keep => {}
                UncertainAction::Escalate(why) => {
                    if !self.state.halted {
                        self.event(
                            "uncertain_escalated",
                            serde_json::json!({ "symbol": sym, "order": u, "reason": why }),
                        );
                    }
                    self.halt(format!("{sym}: {why}"));
                }
            }
        }
        released_now
    }

    /// (ARM request, DISARM symbols). DISARM beats ARM when both are present.
    fn take_operator_files(&mut self) -> (Option<ArmRequest>, Option<Vec<String>>) {
        let disarm = if self.cfg.disarm_path.exists() {
            let body = std::fs::read_to_string(&self.cfg.disarm_path).unwrap_or_default();
            let _ = std::fs::remove_file(&self.cfg.disarm_path);
            Some(parse_disarm(&body, &self.cfg))
        } else {
            None
        };
        let mut arm = None;
        if self.cfg.arm_path.exists() {
            let body = std::fs::read_to_string(&self.cfg.arm_path).unwrap_or_default();
            let _ = std::fs::remove_file(&self.cfg.arm_path);
            match parse_arm(&body, &self.cfg) {
                Ok(v) => arm = Some(v),
                Err(e) => log::warn!("[ARM] ignored: {e}"),
            }
        }
        if disarm.is_some() && arm.is_some() {
            log::warn!("[OPERATOR] ARM and DISARM both present: DISARM wins, ARM discarded");
            arm = None;
        }
        (arm, disarm)
    }

    fn close_book(&mut self, symbol: &str, now: u64, reason: &str) {
        let b = self.state.book_mut(symbol);
        b.mode = Mode::Exited;
        b.target_qty = 0.0;
        b.exited_at = Some(now);
        b.exit_reason = Some(reason.to_string());
    }

    async fn tick(&mut self) -> Result<()> {
        let now = now_secs();
        // Roll the roll week over FIRST, before anything this tick can book
        // an execution: otherwise fills after Sunday 00:00 land in the old
        // week and the first idle tick wipes them.
        if self.cfg.roll.enabled && roll_week_rollover(&mut self.state.roll, now) {
            self.persist();
        }
        let kill = self.sentinels.kill_switch_engaged();
        if self.state.halted && self.sentinels.take_risk_ack() {
            log::warn!("[RISK_ACK] halt cleared ({:?})", self.state.halt_reason);
            self.state.halted = false;
            self.state.halt_reason = None;
            // The operator has checked the venue by hand: unresolvable
            // uncertain orders are cleared with the halt, and every book is
            // replanned from the real positions on the next tick.
            if !self.state.uncertain.is_empty() {
                log::warn!(
                    "[RISK_ACK] clearing {} uncertain order(s) on operator acknowledgement: {:?}",
                    self.state.uncertain.len(),
                    self.state.uncertain.keys().collect::<Vec<_>>()
                );
                // roll accounting: no evidence was seen, so an uncertain
                // order on the rolled leg is booked as a full fill at the
                // current mark (worst case) in place of its reservation.
                let rolled = self.cfg.roll_accounting_leg();
                let slip = self.cfg.taker_slippage_bps;
                let cleared: Vec<(String, UncertainOrder)> =
                    std::mem::take(&mut self.state.uncertain)
                        .into_iter()
                        .collect();
                for (sym, u) in cleared {
                    if rolled == Some(u.leg) {
                        let now_mark = self.last_snapshot.mark_of(u.leg, &sym);
                        let m = if now_mark > 0.0 { now_mark } else { u.mark };
                        roll_risk_ack_booking(
                            &mut self.state.roll,
                            now,
                            &sym,
                            u.roll,
                            u.qty,
                            m,
                            slip,
                        );
                    }
                }
            }
            for b in self.state.books.values_mut() {
                b.net_breach_ticks = 0;
            }
            self.event("risk_ack", serde_json::json!({}));
            self.persist();
        }

        // A venue that cannot be read (REST 5xx, WS down — Lighter Core
        // was 502/503 for ten minutes on 2026-09-20) leaves every book
        // untouched: no guard can be evaluated, so nothing is sent. Status
        // is still written, from the last good snapshot and with the
        // reason, so the card says why the bot is idle instead of going
        // stale. Operator files stay on disk for the next readable tick.
        let snap = match self.snapshot().await {
            Ok(v) => v,
            Err(e) => {
                let reason = format!("venue unreachable: {e}");
                log::error!("[FEED] {reason}");
                self.feed_problem = Some(reason);
                // Only from a real snapshot: before the first successful
                // read the default (all-zero) legs would publish a flat
                // book and a PnL of −equity as fresh figures, over the
                // previous process's last honest status.json. Leaving that
                // file alone (stale) is the truthful option there.
                if self.last_snapshot_at.is_some() {
                    self.write_status(now, kill, 0);
                }
                return Ok(());
            }
        };
        self.last_snapshot = snap.clone();
        self.last_snapshot_at = Some(now);
        let day = utc_day_key(now);
        if self.state.day_key.as_deref() != Some(day.as_str()) {
            self.state.day_key = Some(day);
            self.state.equity_day_start_usd = Some(snap.long.equity_usd + snap.short.equity_usd);
            self.persist();
        }

        // Operator files first: a DISARM must register even on a tick that
        // ends up sending nothing (feed check below), so it is acted on as
        // soon as the venues are readable again.
        let (arm, disarm) = self.take_operator_files();
        if let Some(syms) = disarm {
            for sym in syms {
                if self.state.book(&sym).mode != Mode::Off {
                    log::warn!("[DISARM] closing both {sym} legs");
                    self.close_book(&sym, now, "disarm");
                    self.event("disarm", serde_json::json!({ "symbol": sym }));
                }
            }
            self.persist();
        } else if let Some(reqs) = arm {
            if self.state.halted || kill {
                log::warn!(
                    "[ARM] ignored: halted={} kill_switch={kill}",
                    self.state.halted
                );
            } else if let Some((sym, usd)) = reqs
                .iter()
                .find(|(_, usd)| *usd > self.cfg.max_notional_usd)
            {
                log::warn!(
                    "[ARM] ignored: {sym} ${usd:.0} exceeds HEDGE_MAX_NOTIONAL_USD ${:.0}",
                    self.cfg.max_notional_usd
                );
            } else if let Some((sym, e)) = {
                let mut missing = None;
                for (sym, _) in &reqs {
                    if let Err(e) = self.ensure_meta(sym).await {
                        missing = Some((sym.clone(), e));
                        break;
                    }
                }
                missing
            } {
                log::warn!("[ARM] ignored: {sym} size metadata unavailable ({e}) — ARM again");
            } else {
                let sized: Vec<(String, f64, f64)> = reqs
                    .iter()
                    .map(|(sym, usd)| {
                        let q = qty_for_notional(
                            *usd,
                            snap.long.mark(sym),
                            self.meta(sym).size_decimals,
                        );
                        (sym.clone(), *usd, q)
                    })
                    .collect();
                if let Some((sym, usd, _)) = sized
                    .iter()
                    .find(|(sym, _, q)| *q < self.meta(sym).min_qty || *q <= 0.0)
                {
                    if snap.long.mark(sym) > 0.0 {
                        log::warn!("[ARM] ignored: {sym} ${usd:.0} is below the venue minimum");
                    } else {
                        log::warn!("[ARM] ignored: {sym} has no price on the long venue right now — ARM again");
                    }
                } else {
                    // A new campaign (no book On) resets the baselines the
                    // subsidy / PnL-since-ARM figures are measured from; with
                    // one symbol every ARM does, as before (a resize starts
                    // a new measurement).
                    if resets_baseline(self.cfg.symbols.len(), self.state.any_mode(Mode::On)) {
                        self.state.armed_at = Some(now);
                        self.state.equity_at_arm_usd =
                            Some(snap.long.equity_usd + snap.short.equity_usd);
                        self.state.points_at_arm = points_latest_and_at(&self.points_series(), now)
                            .0
                            .map(|(_, v)| v);
                        self.state.cycles += 1;
                    }
                    for (sym, usd, qty) in sized {
                        // A book the venues already hold (e.g. one opened
                        // by hand) is adopted: the legs are only topped up
                        // or trimmed to `qty` from here on.
                        let held = snap.book(&sym);
                        log::info!(
                            "[ARM] {sym} target ${usd:.0}/leg = {qty} at {:.2} (venues hold long {} / short {})",
                            snap.long.mark(&sym), held.long, held.short
                        );
                        let b = self.state.book_mut(&sym);
                        b.mode = Mode::On;
                        b.target_qty = qty;
                        b.target_notional_usd = usd;
                        b.armed_at = Some(now);
                        b.exited_at = None;
                        b.exit_reason = None;
                        self.event(
                            "arm",
                            serde_json::json!({ "symbol": sym, "notional_usd": usd, "qty": qty,
                                "mark": snap.long.mark(&sym),
                                "adopted_long": held.long, "adopted_short": held.short }),
                        );
                    }
                    self.persist();
                }
            }
        }

        // Feed sanity: with one venue's mark off (REST fallback, placeholder
        // ticker) neither the guards nor the sizes can be trusted, so no
        // order goes out on any book. Status still gets written, with the
        // reason, so the card says why the bot is idle instead of going
        // stale.
        for c in &self.cfg.symbols {
            if !gates_feed(self.state.book(&c.symbol).mode)
                && (snap.long.mark(&c.symbol) == 0.0 || snap.short.mark(&c.symbol) == 0.0)
            {
                continue;
            }
            let (lm, sm) = (snap.long.mark(&c.symbol), snap.short.mark(&c.symbol));
            let divergence = (lm / sm - 1.0).abs();
            // NaN (a zero mark) counts as divergent.
            if divergence.is_nan() || divergence > 0.02 {
                let reason = format!(
                    "venue marks diverge {:.2}% on {}: long {lm} vs short {sm} — no orders on a broken feed",
                    divergence * 100.0,
                    c.symbol
                );
                log::error!("[FEED] {reason}");
                self.feed_problem = Some(reason);
                self.write_status(now, kill, 0);
                return Ok(());
            }
        }
        self.feed_problem = None;

        // `Off` never trades. Whatever the venues hold on an Off symbol is
        // not the bot's book: an operator's own position, or a live book
        // whose state.json was lost — either way it waits for an ARM
        // (adopt) or is closed by hand, never unwound because a default
        // target is 0.
        for c in &self.cfg.symbols {
            let held = snap.book(&c.symbol);
            if self.state.book(&c.symbol).mode == Mode::Off
                && (held.long >= self.meta(&c.symbol).min_qty
                    || held.short >= self.meta(&c.symbol).min_qty)
                && held.long + held.short > 0.0
            {
                log::info!(
                    "[OFF] venues hold long {} / short {} {}; not touched while Off (ARM adopts it)",
                    held.long, held.short, c.symbol
                );
            }
        }
        if !self.state.books.values().any(|b| b.mode != Mode::Off) {
            self.write_status(now, kill, 0);
            return Ok(());
        }

        // Liquidation guard, per account over every hedged position (the
        // venues margin cross-collateral: one symbol's loss eats the
        // headroom of all). Either venue short of headroom → close every
        // armed book.
        let armed = self.state.books.values().any(|b| b.target_qty > 0.0);
        for leg in [Leg::Long, Leg::Short] {
            let equity = snap.equity(leg);
            let legs = snap.legs(&self.cfg, leg);
            let gross: f64 = legs.iter().map(|(n, _)| n).sum();
            if let Some(h) = liq_headroom_pct(equity, &legs) {
                if h < self.cfg.liq_guard_pct && armed {
                    let reason = format!(
                        "liq_guard: {leg:?} venue headroom {h:.2}% < {:.2}% (equity ${equity:.2} vs gross ${gross:.0}) — closing every book",
                        self.cfg.liq_guard_pct
                    );
                    let syms: Vec<String> = self
                        .state
                        .books
                        .iter()
                        .filter(|(_, b)| b.mode != Mode::Off)
                        .map(|(s, _)| s.clone())
                        .collect();
                    for sym in syms {
                        self.close_book(&sym, now, "liq_guard");
                    }
                    self.halt(reason);
                    break;
                } else if h < self.cfg.liq_guard_pct * 2.0 {
                    log::warn!(
                        "[MARGIN] {leg:?} venue headroom {h:.2}% (guard {:.2}%): top up the account",
                        self.cfg.liq_guard_pct
                    );
                }
            }
        }

        // Net exposure watch, per symbol.
        let mut breach: Option<String> = None;
        for c in &self.cfg.symbols {
            if self.state.book(&c.symbol).mode == Mode::Off {
                continue;
            }
            let held = snap.book(&c.symbol);
            let net_usd = held.net() * snap.long.mark(&c.symbol);
            let limit = self.cfg.net_breach_ticks;
            let b = self.state.book_mut(&c.symbol);
            if net_usd.abs() > self.cfg.net_tolerance_usd {
                b.net_breach_ticks += 1;
                log::warn!(
                    "[NET] {} |long − short| = {:.5} (${net_usd:.0}) over tolerance ${:.0}, tick {}/{limit}",
                    c.symbol,
                    held.net(),
                    self.cfg.net_tolerance_usd,
                    b.net_breach_ticks,
                );
                if b.net_breach_ticks >= limit && breach.is_none() {
                    breach = Some(format!(
                        "net exposure {} ${net_usd:.0} over tolerance for {} ticks",
                        c.symbol, b.net_breach_ticks
                    ));
                }
            } else {
                b.net_breach_ticks = 0;
            }
        }
        if let Some(reason) = breach {
            self.halt(reason);
        }

        // Orders whose outcome was unknown: re-check them first; a symbol
        // that still has one gets no order this tick (below).
        let released_now = if !self.cfg.dry_run && !self.state.uncertain.is_empty() {
            self.reconcile_uncertain(now).await
        } else {
            std::collections::BTreeSet::new()
        };

        // Roll reservations left by a process that died between send and
        // settle: booked every tick, before planning (a planner order that
        // later goes uncertain on the symbol must not keep one alive).
        if self.cfg.roll.enabled {
            let stale =
                roll_settle_stale_in_flight(&mut self.state.roll, now, &self.state.uncertain);
            if !stale.is_empty() {
                log::warn!("[ROLL] in-flight reservations without an uncertain roll order (process restart mid-roll) booked: {stale:?}");
                self.persist();
            }
        }

        // Plan (at most one clip per leg per symbol per tick).
        let restricted = self.state.halted || kill;
        let mut plan: Vec<(String, Vec<Order>)> = Vec::new();
        for c in &self.cfg.symbols {
            let b = self.state.book(&c.symbol);
            if b.mode == Mode::Off {
                continue;
            }
            let mark = snap.long.mark(&c.symbol);
            let m = self.meta(&c.symbol);
            let clip_qty =
                qty_for_notional(self.cfg.clip_usd, mark, m.size_decimals).max(m.min_qty);
            let orders = plan_orders(
                b.target_qty,
                snap.book(&c.symbol),
                clip_qty,
                m.min_qty,
                self.cfg.net_tolerance_usd / mark,
                restricted,
            );
            if !orders.is_empty() {
                plan.push((c.symbol.clone(), orders));
            }
        }

        // Uncertain symbols go out of the plan BEFORE the leverage guard (an
        // order that will never be sent must not trip it). And when any order
        // was reconciled during this tick, every guard above ran on a
        // snapshot that predates it (a late fill may have changed equity /
        // gross / net): send nothing this tick; the next one re-reads.
        for sym in self.state.uncertain.keys() {
            log::warn!("[UNCERTAIN] {sym}: an order is still unreconciled — nothing sent on {sym}");
        }
        let mut plan = tick_plan_after_reconcile(plan, &self.state.uncertain, &released_now);
        if !released_now.is_empty() {
            log::info!(
                "[UNCERTAIN] {:?} reconciled this tick — nothing sent until the next tick re-reads the venues",
                released_now
            );
        }

        // Leverage guard over the whole tick BEFORE anything is sent: each
        // venue's gross notional after every growth order of every symbol
        // must stay within max_leverage × its equity. A pair whose second
        // leg would be refused must not have its first leg bought.
        if !self.cfg.dry_run {
            for leg in [Leg::Long, Leg::Short] {
                let add = planned_growth_usd(&plan, &snap, leg);
                let after = snap.gross(&self.cfg, leg) + add;
                if add > 0.0 && !leverage_ok(after, snap.equity(leg), self.cfg.max_leverage) {
                    self.halt(format!(
                        "leverage: {leg:?} venue gross ${after:.0} after this tick > {}x equity ${:.2} — deposit, then RISK_ACK",
                        self.cfg.max_leverage,
                        snap.equity(leg)
                    ));
                    plan = drop_growth(plan);
                    break;
                }
                // Growth that fits the leverage cap can still take the venue
                // below its liquidation-headroom floor (a high-MMR symbol,
                // equity drawn down). Same handling as leverage: halt and
                // drop the growth — the halt restricts the next tick to
                // reductions, so a lopsided book (a refused roll re-open's
                // repair) is levelled DOWN instead of left one leg short.
                // Only while the roll is enabled: with the roll off the
                // planner behaves exactly as before the roll feature.
                let legs_after = legs_after_growth(&plan, &snap, &self.cfg, leg);
                if growth_headroom_guard_applies(self.cfg.roll.enabled, add)
                    && !growth_headroom_ok(snap.equity(leg), &legs_after, self.cfg.liq_guard_pct)
                {
                    self.halt(format!(
                        "growth headroom: {leg:?} venue liq headroom {:.2}% after this tick's growth ${add:.0} < {}% — deposit, then RISK_ACK",
                        liq_headroom_pct(snap.equity(leg), &legs_after).unwrap_or(f64::NAN),
                        self.cfg.liq_guard_pct
                    ));
                    plan = drop_growth(plan);
                    break;
                }
            }
        }

        let plan = plan;

        // Execute, symbol by symbol. The second order of a pair is cut to
        // what the first actually filled, so a partial on the thin RH book
        // never leaves a full-clip naked leg on the other venue. A venue
        // error ends the tick (the next one re-reads everything).
        let mut sent = 0usize;
        'symbols: for (sym, mut orders) in plan {
            let mark = snap.long.mark(&sym);
            let min_qty = self.meta(&sym).min_qty;
            let mut i = 0;
            while i < orders.len() {
                let order = orders[i].clone();
                sent += 1;
                match self
                    .execute(&sym, &order, mark, None)
                    .await
                    .map(|r| r.filled)
                {
                    Ok(filled) if filled < min_qty || filled <= 0.0 => {
                        log::warn!(
                            "[EXEC] {sym} {:?} {} unfilled (got {filled}) — no further orders this tick",
                            order.leg,
                            order.qty
                        );
                        // Stop every symbol, not just this one: an unfilled second
                        // leg means the other venue is not taking orders, and
                        // carrying on would open one naked first leg per
                        // remaining symbol.
                        break 'symbols;
                    }
                    Ok(filled) => {
                        if let Some(next) = orders.get_mut(i + 1) {
                            *next = size_second_leg(next, filled);
                            if next.qty.abs() < min_qty {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("[EXEC] {sym} {:?} {} failed: {e:?}", order.leg, order.qty);
                        break 'symbols;
                    }
                }
                i += 1;
            }
        }

        // Settle Exited → Off once flat. The re-read can fail like the
        // first one; the orders already sent this tick are still reported
        // and the settle simply waits for the next readable tick.
        let exited: Vec<String> = self
            .state
            .books
            .iter()
            .filter(|(_, b)| b.mode == Mode::Exited && b.target_qty == 0.0)
            .map(|(s, _)| s.clone())
            .collect();
        if !exited.is_empty() {
            match self.snapshot().await {
                Ok(s2) => {
                    for sym in exited {
                        let b2 = s2.book(&sym);
                        let min_qty = self.meta(&sym).min_qty;
                        if b2.long < min_qty.max(1e-12) && b2.short < min_qty.max(1e-12) {
                            let reason = self.state.book(&sym).exit_reason;
                            log::info!("[EXIT] {sym} flat on both venues ({reason:?})");
                            self.event(
                                "flat",
                                serde_json::json!({ "symbol": sym, "reason": reason }),
                            );
                            self.state.book_mut(&sym).mode = Mode::Off;
                        }
                    }
                    self.persist();
                }
                Err(e) => {
                    let reason = format!("venue unreachable: {e}");
                    log::error!("[FEED] {reason}");
                    self.feed_problem = Some(reason);
                }
            }
        }
        // The Arcus-leg roll runs only on a tick that sent nothing else.
        // Not on a tick that reconciled an order either: its snapshot (and
        // so every roll gate) predates the late fill.
        let mut orders_this_tick = sent;
        if sent == 0 && released_now.is_empty() {
            orders_this_tick += self.maybe_roll(now, &snap, kill).await;
        }
        self.persist();
        self.write_status(now, kill, orders_this_tick);
        Ok(())
    }

    fn points_series(&self) -> Vec<(u64, f64)> {
        let Some(idx) = self.cfg.long_account_index else {
            return vec![];
        };
        match std::fs::read_to_string(&self.cfg.points_history_path) {
            Ok(text) => points_series(&text, idx),
            Err(_) => vec![],
        }
    }

    fn write_status(&mut self, now: u64, kill: bool, orders_this_tick: usize) {
        let series = self.points_series();
        let feed = FeedStatus {
            problem: self.feed_problem.as_deref(),
            snapshot_at: self.last_snapshot_at,
        };
        let status = status_value(
            &self.cfg,
            &self.state,
            &self.last_snapshot,
            &series,
            feed,
            now,
            kill,
            orders_this_tick,
        );
        if let Err(e) = persist_json(&self.cfg.status_path, &status) {
            log::warn!("[STATUS] write failed: {e:?}");
            return;
        }
        if !self.cfg.status_s3_uri.is_empty()
            && now.saturating_sub(self.last_s3_mirror) >= self.cfg.status_s3_every_secs
        {
            self.last_s3_mirror = now;
            let out = std::process::Command::new("aws")
                .args([
                    "s3",
                    "cp",
                    &self.cfg.status_path.to_string_lossy(),
                    &self.cfg.status_s3_uri,
                    "--only-show-errors",
                ])
                .output();
            match out {
                Ok(o) if o.status.success() => {}
                Ok(o) => log::warn!(
                    "[STATUS] s3 mirror failed: {}",
                    String::from_utf8_lossy(&o.stderr).trim()
                ),
                Err(e) => log::warn!("[STATUS] s3 mirror spawn failed: {e}"),
            }
        }
    }
}

/// Whether the venue reads behind the snapshot are current: `problem` is
/// set while a venue is unreachable or the marks diverge (no orders go
/// out), `snapshot_at` dates the snapshot the legs/marks come from.
#[derive(Debug, Clone, Copy, Default)]
struct FeedStatus<'a> {
    problem: Option<&'a str>,
    snapshot_at: Option<u64>,
}

/// The monitoring projection. Top level follows the pairtrade-like shape
/// debot-dashboard already renders (`ts`/`updated_at`, `pnl_total` =
/// venue equity, `positions`, `subsidy`); everything hedge-specific sits
/// under `hedge_holder`.
///
/// The single-symbol fields the card reads (`target_qty`, `net_qty`,
/// `net_usd`, `basis_bps`, `legs.*.qty`, `legs.*.mark`) describe the
/// primary symbol; `legs.*.notional_usd` / `equity_usd` /
/// `liq_headroom_pct` are per account over every hedged symbol (what the
/// guards act on). `books` carries every symbol's own figures.
#[allow(clippy::too_many_arguments)] // one projection, all of its inputs
fn status_value(
    cfg: &Config,
    state: &State,
    snap: &Snapshot,
    points: &[(u64, f64)],
    feed: FeedStatus<'_>,
    now: u64,
    kill: bool,
    orders_this_tick: usize,
) -> serde_json::Value {
    let primary = cfg.primary();
    let pb = snap.book(primary);
    let pbook = state.book(primary);
    let equity_total = snap.long.equity_usd + snap.short.equity_usd;
    let pnl_since_arm = state.equity_at_arm_usd.map(|e0| equity_total - e0);
    let basis = |sym: &str| {
        let (l, s) = (snap.long.mark(sym), snap.short.mark(sym));
        if l > 0.0 && s > 0.0 {
            (l / s - 1.0) * 1e4
        } else {
            0.0
        }
    };
    let leg = |name: &str, instance: &str, which: Leg| {
        let exchange = cfg.venue(which).dex_name();
        let (qty, vs) = match which {
            Leg::Long => (pb.long, &snap.long),
            Leg::Short => (pb.short, &snap.short),
        };
        serde_json::json!({
            "instance": instance,
            "exchange": exchange,
            "side": name,
            "qty": qty,
            "notional_usd": snap.gross(cfg, which),
            "mark": vs.mark(primary),
            "equity_usd": vs.equity_usd,
            "liq_headroom_pct": liq_headroom_pct(vs.equity_usd, &snap.legs(cfg, which)),
        })
    };
    let mut positions = Vec::new();
    let mut books = serde_json::Map::new();
    for c in &cfg.symbols {
        let held = snap.book(&c.symbol);
        if held.long > 0.0 {
            positions.push(serde_json::json!({
                "symbol": format!("{} ({})", c.symbol, cfg.long_instance), "side": "long",
                "size": format!("{}", held.long), "entry_price": serde_json::Value::Null }));
        }
        if held.short > 0.0 {
            positions.push(serde_json::json!({
                "symbol": format!("{} ({})", c.symbol, cfg.short_instance), "side": "short",
                "size": format!("{}", held.short), "entry_price": serde_json::Value::Null }));
        }
        let b = state.book(&c.symbol);
        books.insert(
            c.symbol.clone(),
            serde_json::json!({
                "mode": b.mode,
                "target_qty": b.target_qty,
                "target_notional_usd": b.target_notional_usd,
                "armed_at": b.armed_at,
                "exit_reason": b.exit_reason,
                "long_qty": held.long,
                "short_qty": held.short,
                "net_qty": held.net(),
                "net_usd": held.net() * snap.long.mark(&c.symbol),
                "mark_long": snap.long.mark(&c.symbol),
                "mark_short": snap.short.mark(&c.symbol),
                "basis_bps": basis(&c.symbol),
                "mmr_pct": c.mmr_pct,
                "short_mmr_pct": c.short_mmr_pct,
            }),
        );
    }
    // Split from the top-level literal: one `json!` this deep trips the
    // macro recursion limit.
    let (roll_fly_v, roll_fly_c) = roll_in_flight_total(&state.roll);
    let hedge_holder = serde_json::json!({
        "mode": state.overall_mode(),
        "halted": state.halted,
        "halt_reason": state.halt_reason,
        "uncertain_orders": state.uncertain,
        "roll": {
            "enabled": cfg.roll.enabled,
            "mode": "taker",
            "leg": cfg.roll_leg().map(|l| format!("{l:?}")),
            "clip_usd": cfg.roll.clip_usd,
            "week_start_day": state.roll.week,
            "week_volume_usd": state.roll.week_volume_usd,
            "week_cost_usd": state.roll.week_cost_usd,
            "week_count": state.roll.week_count,
            "in_flight_volume_usd": roll_fly_v,
            "in_flight_cost_usd": roll_fly_c,
            "weekly_volume_budget_usd": cfg.roll.weekly_volume_usd,
            "weekly_cost_cap_usd": cfg.roll.weekly_cost_usd,
            "last_roll_at": state.roll.last_roll_at,
            "next_roll_at": cfg.roll.enabled.then(|| roll_next_at(&state.roll, cfg.roll.interval_secs)),
            "blocked_reason": state.roll.blocked_reason,
            // per symbol: consecutive refusals and when it may retry
            "refusal_backoff": state.roll.backoff,
        },
        "kill_switch": kill,
        "symbols": cfg.symbol_names(),
        "target_qty": pbook.target_qty,
        "target_notional_usd": state
            .books
            .values()
            .filter(|b| b.mode != Mode::Off)
            .map(|b| b.target_notional_usd)
            .sum::<f64>(),
        "armed_at": state.armed_at,
        "exited_at": pbook.exited_at,
        "exit_reason": pbook.exit_reason,
        "cycles": state.cycles,
        "net_qty": pb.net(),
        "net_usd": pb.net() * snap.long.mark(primary),
        "net_tolerance_usd": cfg.net_tolerance_usd,
        "basis_bps": basis(primary),
        "equity_total_usd": equity_total,
        "equity_at_arm_usd": state.equity_at_arm_usd,
        "pnl_since_arm_usd": pnl_since_arm,
        "points_at_arm": state.points_at_arm,
        "orders_this_tick": orders_this_tick,
        "feed_problem": feed.problem,
        "snapshot_at": feed.snapshot_at,
        "config_fp": cfg.fingerprint(),
        "legs": {
            "long": leg("long", &cfg.long_instance, Leg::Long),
            "short": leg("short", &cfg.short_instance, Leg::Short),
        },
        "books": books,
    });
    serde_json::json!({
        "ts": now,
        "updated_at": chrono::DateTime::<chrono::Utc>::from(UNIX_EPOCH + Duration::from_secs(now)).to_rfc3339(),
        "process_started_at": state.process_started_at,
        "bot": BOT,
        "id": BOT,
        "dex": "Lighter (Robinhood Chain) / Lighter Core",
        "symbol": primary,
        "dry_run": cfg.dry_run,
        "kill_switch_active": kill,
        "pnl_total": equity_total,
        "pnl_today": state.equity_day_start_usd.map(|e| equity_total - e).unwrap_or(0.0),
        "pnl_source": "equity",
        "positions_ready": true,
        "position_count": positions.len(),
        "has_position": !positions.is_empty(),
        "positions": positions,
        "subsidy": subsidy_block(points, state.points_at_arm, state.armed_at, pnl_since_arm, now),
        "hedge_holder": hedge_holder,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    let cfg = Config::from_env()?;
    log::info!(
        "[CONFIG] bot={BOT} dry_run={} symbols={} long={}:{} short={}:{} target=${:.0} max=${:.0} clip=${:.0} slip={}bps net_tol=${:.0} liq_guard={}% max_lev={}x fill_waits={:?} fp={}",
        cfg.dry_run,
        cfg.symbols
            .iter()
            .map(|c| c.spec(3))
            .collect::<Vec<_>>()
            .join(","),
        cfg.long_venue.dex_name(), cfg.long_instance,
        cfg.short_venue.dex_name(), cfg.short_instance, cfg.target_notional_usd,
        cfg.max_notional_usd, cfg.clip_usd, cfg.taker_slippage_bps, cfg.net_tolerance_usd,
        cfg.liq_guard_pct, cfg.max_leverage, cfg.fill_wait_secs, cfg.fingerprint()
    );
    let symbols = cfg.symbol_names();
    let long_dex = DexConnectorBox::create(
        cfg.long_venue.dex_name(),
        cfg.dry_run,
        &symbols,
        Some(cfg.long_instance.as_str()),
    )
    .await
    .with_context(|| {
        format!(
            "init long-leg {} connector ({})",
            cfg.long_venue.dex_name(),
            cfg.long_instance
        )
    })?;
    let short_dex = DexConnectorBox::create(
        cfg.short_venue.dex_name(),
        cfg.dry_run,
        &symbols,
        Some(cfg.short_instance.as_str()),
    )
    .await
    .with_context(|| {
        format!(
            "init short-leg {} connector ({})",
            cfg.short_venue.dex_name(),
            cfg.short_instance
        )
    })?;
    long_dex.start().await.context("start long-leg connector")?;
    short_dex
        .start()
        .await
        .context("start short-leg connector")?;

    let long = Venue {
        name: "long",
        kind: cfg.long_venue,
        instance: cfg.long_instance.clone(),
        dex: Arc::new(long_dex),
    };
    let short = Venue {
        name: "short",
        kind: cfg.short_venue,
        instance: cfg.short_instance.clone(),
        dex: Arc::new(short_dex),
    };
    let mut state: State = load_json(&cfg.state_path)?.unwrap_or_default();
    state = migrate_legacy(state, cfg.primary());
    state = reconcile_state_mode(state, cfg.dry_run);
    let dropped = roll_drop_in_flight_when_disabled(&mut state.roll, cfg.roll.enabled);
    if !dropped.is_empty() {
        log::warn!("[STARTUP] roll disabled: dropping in-flight roll reservations {dropped:?} (unbooked) — check those symbols' Arcus fills by hand if a roll was mid-flight");
    }
    state.process_started_at = Some(now_secs());
    // Size metadata (decimals / venue minimum) per symbol. Only books the
    // bot manages must be quotable at startup: an Off symbol whose market
    // is unavailable is skipped and its metadata fetched when it is armed
    // (`ensure_meta`), so one dead unused market cannot keep the process
    // from managing, guarding or DISARMing the healthy books.
    let mut meta = std::collections::BTreeMap::new();
    for sym in &symbols {
        match fetch_meta(&long, &short, sym).await {
            Ok((mark, m)) => {
                log::info!(
                    "[STARTUP] {sym} mark={mark:.2} size_decimals={} min_qty={}",
                    m.size_decimals,
                    m.min_qty
                );
                meta.insert(sym.clone(), m);
            }
            Err(e) if !gates_feed(state.book(sym).mode) => {
                log::warn!("[STARTUP] {sym} unpriced (book Off): {e} — metadata is fetched when it is armed");
            }
            Err(e) => return Err(e),
        }
    }
    for (sym, b) in &state.books {
        if !symbols.contains(sym) && b.mode != Mode::Off {
            // The config dropped a symbol whose book is still armed: it is
            // no longer read or traded. Say so loudly; the operator closes
            // it by hand or puts the symbol back.
            log::error!(
                "[STARTUP] {sym} book is {:?} (target {}) but {sym} is not in HEDGE_SYMBOLS — it will not be managed",
                b.mode, b.target_qty
            );
        }
    }
    log::info!(
        "[STARTUP] mode={:?} books={} halted={}",
        state.overall_mode(),
        state
            .books
            .iter()
            .map(|(s, b)| format!("{s}:{:?}:{}", b.mode, b.target_qty))
            .collect::<Vec<_>>()
            .join(","),
        state.halted
    );
    let mut engine = Engine {
        sentinels: Sentinels::new(cfg.kill_switch_path.clone(), cfg.risk_ack_path.clone()),
        cfg,
        state,
        long,
        short,
        meta,
        last_s3_mirror: 0,
        last_snapshot: Snapshot::default(),
        last_snapshot_at: None,
        feed_problem: None,
    };
    let tick = Duration::from_secs(engine.cfg.tick_secs);
    loop {
        if let Err(e) = engine.tick().await {
            log::error!("[TICK] {e:?}");
        }
        tokio::time::sleep(tick).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(long: f64, short: f64) -> Book {
        Book { long, short }
    }

    #[test]
    fn growth_from_flat_is_long_first_then_short_one_clip_each() {
        let o = plan_orders(0.25, book(0.0, 0.0), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![
                Order {
                    leg: Leg::Long,
                    qty: 0.12
                },
                Order {
                    leg: Leg::Short,
                    qty: 0.12
                }
            ]
        );
    }

    #[test]
    fn growth_is_blocked_while_restricted_but_unwind_is_not() {
        assert!(plan_orders(0.25, book(0.0, 0.0), 0.12, 0.0002, 0.006, true).is_empty());
        let o = plan_orders(0.0, book(0.25, 0.25), 0.12, 0.0002, 0.006, true);
        assert_eq!(
            o,
            vec![
                Order {
                    leg: Leg::Short,
                    qty: -0.12
                },
                Order {
                    leg: Leg::Long,
                    qty: -0.12
                }
            ]
        );
    }

    #[test]
    fn lopsided_book_is_levelled_before_anything_else() {
        // Mid-build (first clip filled long only): grow the short, one clip.
        let o = plan_orders(0.25, book(0.12, 0.0), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Short,
                qty: 0.12
            }]
        );
        // Never past the larger leg, even if target is higher.
        let o = plan_orders(0.25, book(0.05, 0.0), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Short,
                qty: 0.05
            }]
        );
        // Restricted (halt / KILL_SWITCH): the larger leg comes down instead.
        let o = plan_orders(0.25, book(0.25, 0.0), 0.12, 0.0002, 0.006, true);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Long,
                qty: -0.12
            }]
        );
        // Over-held on one side with the other already at target: reduce.
        let o = plan_orders(0.1, book(0.25, 0.1), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Long,
                qty: -0.12
            }]
        );
        // Mirror: long leg gone while restricted → reduce the short, whole gap if within a clip.
        let o = plan_orders(0.25, book(0.0, 0.25), 0.5, 0.0002, 0.006, true);
        assert_eq!(
            o,
            vec![Order {
                leg: Leg::Short,
                qty: -0.25
            }]
        );
        // Exactly one order in every lopsided case (never a second leg).
        for (b, r) in [
            (book(0.2, 0.0), false),
            (book(0.0, 0.2), true),
            (book(0.3, 0.1), false),
        ] {
            assert_eq!(plan_orders(0.25, b, 0.12, 0.0002, 0.006, r).len(), 1);
        }
    }

    #[test]
    fn small_imbalance_inside_tolerance_is_not_repaired_and_growth_tops_up_each_leg() {
        // long 0.12, short 0.115: within 0.006 tolerance → top up both toward 0.25.
        let o = plan_orders(0.25, book(0.12, 0.115), 0.12, 0.0002, 0.006, false);
        assert_eq!(o.len(), 2);
        assert_eq!(o[0].leg, Leg::Long);
        assert!((o[0].qty - 0.12).abs() < 1e-12);
        assert_eq!(o[1].leg, Leg::Short);
        assert!((o[1].qty - 0.12).abs() < 1e-12);
    }

    #[test]
    fn dust_is_ignored_and_final_top_up_is_exact() {
        let o = plan_orders(0.25, book(0.2499, 0.25), 0.12, 0.0002, 0.006, false);
        assert!(o.is_empty(), "0.0001 is below min_qty");
        // 0.01 short of target on the long leg (over tolerance): level it up, exactly.
        let o = plan_orders(0.25, book(0.24, 0.25), 0.12, 0.0002, 0.006, false);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].leg, Leg::Long);
        assert!((o[0].qty - 0.01).abs() < 1e-12);
        // Same gap inside tolerance: ordinary growth path, still exact.
        let o = plan_orders(0.25, book(0.245, 0.25), 0.12, 0.0002, 0.006, false);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].leg, Leg::Long);
        assert!((o[0].qty - 0.005).abs() < 1e-12);
    }

    #[test]
    fn unwind_never_overshoots_and_stops_at_flat() {
        let o = plan_orders(0.0, book(0.05, 0.05), 0.12, 0.0002, 0.006, false);
        assert_eq!(
            o,
            vec![
                Order {
                    leg: Leg::Short,
                    qty: -0.05
                },
                Order {
                    leg: Leg::Long,
                    qty: -0.05
                }
            ]
        );
        assert!(plan_orders(0.0, book(0.0, 0.0), 0.12, 0.0002, 0.006, false).is_empty());
    }

    #[test]
    fn second_leg_is_cut_to_the_first_fill() {
        let short = Order {
            leg: Leg::Short,
            qty: 0.12,
        };
        assert_eq!(size_second_leg(&short, 0.12), short);
        assert_eq!(
            size_second_leg(&short, 0.001),
            Order {
                leg: Leg::Short,
                qty: 0.001
            }
        );
        assert_eq!(size_second_leg(&short, 0.5), short);
        let unwind = Order {
            leg: Leg::Long,
            qty: -0.12,
        };
        assert_eq!(
            size_second_leg(&unwind, 0.05),
            Order {
                leg: Leg::Long,
                qty: -0.05
            }
        );
        assert_eq!(size_second_leg(&unwind, -1.0).qty, 0.0);
    }

    #[test]
    fn leverage_guard_uses_gross_after_the_tick() {
        // $20k leg on $4,000: 5.0x — refused at 4.9x, allowed at 5x.
        assert!(!leverage_ok(20_000.0, 4_000.0, 4.9));
        assert!(leverage_ok(20_000.0, 4_000.0, 5.0));
        // BTC $20k + META $10k on $8k = 3.75x ok at 5x, refused at 3.5x.
        assert!(leverage_ok(30_000.0, 8_000.0, 5.0));
        assert!(!leverage_ok(30_000.0, 8_000.0, 3.5));
    }

    fn sym_book(mode: Mode, target_qty: f64) -> SymBook {
        SymBook {
            mode,
            target_qty,
            ..Default::default()
        }
    }

    #[test]
    fn state_from_the_other_mode_is_dropped_to_off() {
        let mut armed = State {
            dry_run: Some(true),
            ..Default::default()
        };
        armed.books.insert("BTC".into(), sym_book(Mode::On, 0.247));
        armed
            .books
            .insert("META".into(), sym_book(Mode::Exited, 0.0));
        let live = reconcile_state_mode(armed.clone(), false);
        assert!(live
            .books
            .values()
            .all(|b| b.mode == Mode::Off && b.target_qty == 0.0));
        assert_eq!(live.dry_run, Some(false));
        // Same mode: untouched.
        let same = reconcile_state_mode(armed.clone(), true);
        assert_eq!(same.book("BTC").mode, Mode::On);
        assert_eq!(same.book("BTC").target_qty, 0.247);
        // Pre-field state (no dry_run recorded): kept, now stamped.
        let mut legacy = armed;
        legacy.dry_run = None;
        let kept = reconcile_state_mode(legacy, false);
        assert_eq!(kept.book("BTC").mode, Mode::On);
        assert_eq!(kept.dry_run, Some(false));

        // A live uncertain entry never survives into a dry-run process
        // (nothing would reconcile it and it would block ARM for good) …
        let mut live_state = State {
            dry_run: Some(false),
            ..Default::default()
        };
        live_state.uncertain.insert(
            "BTC".into(),
            UncertainOrder {
                leg: Leg::Short,
                exchange: "arcus".into(),
                instance: "arcus".into(),
                order_id: Some("o1".into()),
                qty: 0.1,
                before_qty: -0.4,
                mark: 0.0,
                sent_at: 1,
                reason: "t".into(),
                roll: false,
            },
        );
        assert!(reconcile_state_mode(live_state.clone(), true)
            .uncertain
            .is_empty());
        // … but a live restart keeps it (it still has to be reconciled).
        assert_eq!(reconcile_state_mode(live_state, false).uncertain.len(), 1);
    }

    #[test]
    fn single_symbol_state_json_migrates_to_the_primary_book() {
        // The Tokyo holder's state.json as written by 3e78706 (one book).
        let old = r#"{"mode":"On","target_qty":0.19685,"target_notional_usd":16000.0,
            "dry_long_qty":0.19685,"dry_short_qty":0.19685,"halted":false,"halt_reason":null,
            "armed_at":1789820643,"exited_at":null,"exit_reason":null,"equity_at_arm_usd":8000.0,
            "cycles":1,"net_breach_ticks":0,"points_at_arm":72.19526,"day_key":"2026-09-25",
            "equity_day_start_usd":8000.0,"process_started_at":1790000000,"dry_run":true}"#;
        let st: State = serde_json::from_str(old).unwrap();
        let st = migrate_legacy(st, "BTC");
        let b = st.book("BTC");
        assert_eq!(b.mode, Mode::On);
        assert_eq!(b.target_qty, 0.19685);
        assert_eq!(b.dry_long_qty, 0.19685);
        assert_eq!(b.armed_at, Some(1789820643));
        assert_eq!(st.armed_at, Some(1789820643));
        assert_eq!(st.points_at_arm, Some(72.19526));
        assert_eq!(st.books.len(), 1);
        // Written back without the legacy top-level keys; reads back the same.
        let json = serde_json::to_value(&st).unwrap();
        assert!(json.get("mode").is_none() && json.get("target_qty").is_none());
        let again = migrate_legacy(serde_json::from_value::<State>(json).unwrap(), "BTC");
        assert_eq!(again.book("BTC"), b);
        // A fresh (flat, never armed) legacy file creates no book.
        let flat: State =
            serde_json::from_str(r#"{"mode":"Off","target_qty":0.0,"halted":false,"cycles":0}"#)
                .unwrap();
        assert!(migrate_legacy(flat, "BTC").books.is_empty());
    }

    #[test]
    fn qty_for_notional_floors_to_size_decimals() {
        assert!((qty_for_notional(20_000.0, 81_000.0, 5) - 0.24691).abs() < 1e-12);
        assert_eq!(qty_for_notional(20_000.0, 0.0, 5), 0.0);
        assert_eq!(qty_for_notional(-1.0, 81_000.0, 5), 0.0);
    }

    #[test]
    fn liq_headroom_is_equity_over_gross_less_weighted_mmr() {
        // One symbol: equity / notional − mmr, as before.
        assert!((liq_headroom_pct(4_000.0, &[(20_000.0, 1.2)]).unwrap() - 18.8).abs() < 1e-9);
        assert_eq!(liq_headroom_pct(4_000.0, &[]), None);
        assert_eq!(liq_headroom_pct(4_000.0, &[(0.0, 1.2), (0.0, 3.0)]), None);
        // 8% guard on a $20k BTC leg fires when equity falls to $1,840.
        assert!(liq_headroom_pct(1_839.0, &[(20_000.0, 1.2)]).unwrap() < 8.0);
        assert!(liq_headroom_pct(1_841.0, &[(20_000.0, 1.2)]).unwrap() > 8.0);
        // BTC $20k @1.2 + AMZN $10k @6 on $6k: (600000 − 24000 − 60000) / 30000 = 17.2.
        let h = liq_headroom_pct(6_000.0, &[(20_000.0, 1.2), (10_000.0, 6.0)]).unwrap();
        assert!((h - 17.2).abs() < 1e-9);
        // Venue liquidation (equity = Σ n·mmr = $840) is headroom 0.
        let h = liq_headroom_pct(840.0, &[(20_000.0, 1.2), (10_000.0, 6.0)]).unwrap();
        assert!(h.abs() < 1e-9);
    }

    fn cfg_for_test() -> Config {
        let dir = std::env::temp_dir().join(format!("hedge-test-{}", std::process::id()));
        Config {
            dry_run: true,
            live_confirm: String::new(),
            symbols: vec![SymbolCfg {
                symbol: "BTC".into(),
                mmr_pct: 1.2,
                short_mmr_pct: 1.2,
            }],
            long_venue: VenueKind::Lighter,
            short_venue: VenueKind::Lighter,
            long_instance: "rh".into(),
            short_instance: "core".into(),
            arcus_live_confirm: String::new(),
            fill_wait_secs: DEFAULT_FILL_WAIT_SECS.to_vec(),
            uncertain_grace_secs: 60,
            roll: RollCfg::default(),
            target_notional_usd: 20_000.0,
            max_notional_usd: 30_000.0,
            clip_usd: 10_000.0,
            taker_slippage_bps: 3,
            net_tolerance_usd: 500.0,
            net_breach_ticks: 3,
            liq_guard_pct: 8.0,
            max_leverage: 5.0,
            dry_run_equity_usd: 4_000.0,
            tick_secs: 30,
            status_s3_uri: String::new(),
            status_s3_every_secs: 60,
            points_history_path: dir.join("points_history.jsonl"),
            long_account_index: Some(3209),
            arm_path: dir.join("ARM"),
            disarm_path: dir.join("DISARM"),
            kill_switch_path: dir.join("KILL_SWITCH"),
            risk_ack_path: dir.join("RISK_ACK"),
            state_path: dir.join("state.json"),
            status_path: dir.join("status.json"),
            events_path: dir.join("events.jsonl"),
        }
    }

    fn multi_cfg() -> Config {
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2,META:3,AMZN:6").unwrap();
        c
    }

    #[test]
    fn live_requires_the_g0_token() {
        let mut c = cfg_for_test();
        c.dry_run = false;
        assert!(c.validate().is_err());
        c.live_confirm = LIVE_CONFIRM_TOKEN.into();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn config_rejects_same_instance_for_both_legs() {
        let mut c = cfg_for_test();
        c.short_instance = "RH".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn symbols_parse_with_a_required_mmr_each() {
        let s = parse_symbols(" btc:1.2 , META:3,amzn:6 ").unwrap();
        assert_eq!(
            s.iter()
                .map(|c| (c.symbol.as_str(), c.mmr_pct))
                .collect::<Vec<_>>(),
            vec![("BTC", 1.2), ("META", 3.0), ("AMZN", 6.0)]
        );
        assert!(parse_symbols("BTC").is_err(), "MMR is required");
        assert!(parse_symbols("BTC:1.2,btc:1.2").is_err(), "duplicate");
        assert!(parse_symbols("BTC:0").is_err());
        assert!(parse_symbols("BTC:x").is_err());
        assert!(parse_symbols(" , ").is_err());
        // Headroom already nets out the MMR, so a guard below a symbol's
        // MMR is fine (5 % guard with AMZN at 6 %); only guard <= 0 is not.
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2,AMZN:6").unwrap();
        c.liq_guard_pct = 5.0;
        assert!(c.validate().is_ok());
        c.liq_guard_pct = 0.0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn single_symbol_fingerprint_is_unchanged_by_the_multi_symbol_code() {
        // The field list 3e78706 hashed; the Tokyo DRY_RUN holder's fp must
        // not move just because the binary learned several symbols.
        let c = cfg_for_test();
        let before = config_fingerprint(&[
            ("symbol", "BTC".to_string()),
            ("long", "rh".to_string()),
            ("short", "core".to_string()),
            ("target", "20000.00".to_string()),
            ("max_notional", "30000.00".to_string()),
            ("clip", "10000.00".to_string()),
            ("slip_bps", "3".to_string()),
            ("net_tol", "500.00".to_string()),
            ("mmr", "1.200".to_string()),
            ("liq_guard", "8.000".to_string()),
            ("max_lev", "5.00".to_string()),
        ]);
        assert_eq!(c.fingerprint(), before);
        assert_ne!(multi_cfg().fingerprint(), before);
    }

    #[test]
    fn arm_file_single_symbol_format_is_unchanged() {
        let c = cfg_for_test();
        assert_eq!(parse_arm("", &c), Ok(vec![("BTC".into(), 20_000.0)]));
        assert_eq!(parse_arm("20642\n", &c), Ok(vec![("BTC".into(), 20_642.0)]));
        assert_eq!(
            parse_arm("btc 25000", &c),
            Ok(vec![("BTC".into(), 25_000.0)])
        );
        assert!(parse_arm("-5", &c).is_err());
        assert!(parse_arm("META 10000", &c).is_err(), "not configured");
        // Two bare notionals name the primary twice: rejected like `BTC 1\nBTC 2`.
        assert!(parse_arm("20000\n25000", &c).is_err());
        assert!(parse_arm("20000\nBTC 25000", &c).is_err());
        assert!(parse_arm("BTC 25000\n20000", &c).is_err());
    }

    #[test]
    fn legacy_single_symbol_mmr_is_validated_like_parse_symbols() {
        let mut c = cfg_for_test();
        assert!(c.validate().is_ok());
        c.symbols[0].mmr_pct = f64::NAN;
        assert!(
            c.validate().is_err(),
            "NaN MMR would make every headroom check false"
        );
        c.symbols[0].mmr_pct = 0.0;
        assert!(c.validate().is_err());
        c.symbols[0].mmr_pct = 1.2;
        c.symbols[0].symbol = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn arm_file_multi_symbol_needs_explicit_lines_and_rejects_any_bad_one() {
        let c = multi_cfg();
        assert_eq!(
            parse_arm("# campaign 2\nBTC 40000\nmeta=10000\nAMZN:10000\n", &c),
            Ok(vec![
                ("BTC".into(), 40_000.0),
                ("META".into(), 10_000.0),
                ("AMZN".into(), 10_000.0)
            ])
        );
        assert_eq!(parse_arm("META", &c), Ok(vec![("META".into(), 20_000.0)]));
        assert!(parse_arm("", &c).is_err(), "empty is ambiguous");
        assert!(parse_arm("10000", &c).is_err(), "bare number is ambiguous");
        assert!(
            parse_arm("META 10000\nGOOGL 10000", &c).is_err(),
            "one bad line → none"
        );
        assert!(parse_arm("META 10000\nMETA 5000", &c).is_err());
        assert!(parse_arm("META ten", &c).is_err());
        assert!(parse_arm("META 10000 extra", &c).is_err());
        // Delimiter-only lines are rejected, not a panic (Codex P2 on #352).
        for junk in [":", "=", " : = ", "META 10000\n=="] {
            assert!(parse_arm(junk, &c).is_err(), "{junk:?}");
        }
        assert!(parse_arm(":", &cfg_for_test()).is_err());
    }

    #[test]
    fn disarm_file_closes_named_books_and_errs_towards_all() {
        let c = multi_cfg();
        let all = vec!["BTC".to_string(), "META".into(), "AMZN".into()];
        assert_eq!(parse_disarm("", &c), all);
        assert_eq!(
            parse_disarm("meta\n\nAMZN\nmeta", &c),
            vec!["META".to_string(), "AMZN".into()]
        );
        assert_eq!(
            parse_disarm("META\nGOOGL", &c),
            all,
            "unknown symbol → close everything"
        );
    }

    fn snap(pairs: &[(&str, f64, f64, f64, f64)], eq_l: f64, eq_s: f64) -> Snapshot {
        let mut s = Snapshot::default();
        s.long.equity_usd = eq_l;
        s.short.equity_usd = eq_s;
        for (sym, ql, qs, ml, ms) in pairs {
            s.long.qty.insert(sym.to_string(), *ql);
            s.short.qty.insert(sym.to_string(), *qs);
            s.long.mark.insert(sym.to_string(), *ml);
            s.short.mark.insert(sym.to_string(), *ms);
        }
        s
    }

    #[test]
    fn planned_growth_sums_every_symbol_at_its_own_venue_mark() {
        let s = snap(
            &[
                ("BTC", 0.1, -0.1, 80_000.0, 80_100.0),
                ("META", 0.0, 0.0, 750.0, 752.0),
                ("AMZN", 0.0, 0.0, 250.0, 250.0),
            ],
            4_000.0,
            4_000.0,
        );
        let plan = vec![
            (
                "BTC".to_string(),
                vec![
                    Order {
                        leg: Leg::Long,
                        qty: 0.1,
                    },
                    Order {
                        leg: Leg::Short,
                        qty: 0.1,
                    },
                ],
            ),
            (
                "META".to_string(),
                vec![
                    Order {
                        leg: Leg::Long,
                        qty: 10.0,
                    },
                    Order {
                        leg: Leg::Short,
                        qty: 10.0,
                    },
                ],
            ),
            // A reduction elsewhere frees nothing up front: not counted.
            (
                "AMZN".to_string(),
                vec![Order {
                    leg: Leg::Short,
                    qty: -5.0,
                }],
            ),
        ];
        assert!((planned_growth_usd(&plan, &s, Leg::Long) - (8_000.0 + 7_500.0)).abs() < 1e-9);
        assert!((planned_growth_usd(&plan, &s, Leg::Short) - (8_010.0 + 7_520.0)).abs() < 1e-9);
        // Held $8,010 + $15,530 growth = $23,540 on $4,000: over 5x.
        let after = s.gross(&cfg_for_test_multi_btc_meta(), Leg::Short)
            + planned_growth_usd(&plan, &s, Leg::Short);
        assert!(!leverage_ok(after, 4_000.0, 5.0));
        assert!(leverage_ok(after, 4_000.0, 6.0));
    }

    #[test]
    fn leverage_trip_keeps_reductions_and_drops_growth() {
        let plan = vec![
            (
                "BTC".to_string(),
                vec![
                    Order {
                        leg: Leg::Short,
                        qty: -0.1,
                    },
                    Order {
                        leg: Leg::Long,
                        qty: -0.1,
                    },
                ],
            ),
            (
                "META".to_string(),
                vec![
                    Order {
                        leg: Leg::Long,
                        qty: 10.0,
                    },
                    Order {
                        leg: Leg::Short,
                        qty: 10.0,
                    },
                ],
            ),
            (
                "AMZN".to_string(),
                vec![Order {
                    leg: Leg::Long,
                    qty: -3.0,
                }],
            ),
        ];
        let kept = drop_growth(plan);
        assert_eq!(
            kept,
            vec![
                (
                    "BTC".to_string(),
                    vec![
                        Order {
                            leg: Leg::Short,
                            qty: -0.1
                        },
                        Order {
                            leg: Leg::Long,
                            qty: -0.1
                        }
                    ]
                ),
                (
                    "AMZN".to_string(),
                    vec![Order {
                        leg: Leg::Long,
                        qty: -3.0
                    }]
                ),
            ]
        );
    }

    #[test]
    fn single_symbol_rearm_resets_baselines_multi_only_for_a_new_campaign() {
        assert!(
            resets_baseline(1, true),
            "single-symbol resize = new measurement, as before"
        );
        assert!(resets_baseline(1, false));
        assert!(resets_baseline(3, false));
        assert!(
            !resets_baseline(3, true),
            "adding a book mid-campaign keeps the baseline"
        );
    }

    #[test]
    fn only_managed_books_gate_the_feed() {
        assert!(!gates_feed(Mode::Off));
        assert!(gates_feed(Mode::On));
        assert!(
            gates_feed(Mode::Exited),
            "a closing book must still be priced"
        );
    }

    fn cfg_for_test_multi_btc_meta() -> Config {
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2,META:3").unwrap();
        c
    }

    #[test]
    fn snapshot_books_ignore_wrong_side_positions() {
        let s = snap(
            &[
                ("BTC", 0.247, -0.247, 81_000.0, 81_000.0),
                ("META", -1.0, 2.0, 750.0, 750.0),
            ],
            4_000.0,
            4_000.0,
        );
        assert_eq!(s.book("BTC"), book(0.247, 0.247));
        assert_eq!(
            s.book("META"),
            book(0.0, 0.0),
            "long venue short / short venue long are foreign"
        );
        assert_eq!(s.book("AMZN"), book(0.0, 0.0));
    }

    #[test]
    fn status_reports_net_basis_and_pnl_since_arm() {
        let cfg = cfg_for_test();
        let mut state = State {
            equity_at_arm_usd: Some(8_494.0),
            ..Default::default()
        };
        state.books.insert(
            "BTC".into(),
            SymBook {
                mode: Mode::On,
                target_qty: 0.247,
                target_notional_usd: 20_000.0,
                ..Default::default()
            },
        );
        let s = snap(
            &[("BTC", 0.247, -0.247, 81_349.4, 81_324.2)],
            4_480.0,
            4_010.0,
        );
        let feed = FeedStatus {
            problem: None,
            snapshot_at: Some(1_789_812_142),
        };
        let v = status_value(&cfg, &state, &s, &[], feed, 1_789_812_142, false, 0);
        let h = &v["hedge_holder"];
        assert_eq!(h["mode"], "On");
        assert!((h["net_qty"].as_f64().unwrap()).abs() < 1e-12);
        assert!((h["basis_bps"].as_f64().unwrap() - 3.0987).abs() < 0.01);
        assert!((h["pnl_since_arm_usd"].as_f64().unwrap() - (-4.0)).abs() < 1e-9);
        assert_eq!(h["legs"]["short"]["qty"], 0.247);
        assert!(
            (h["legs"]["short"]["liq_headroom_pct"].as_f64().unwrap()
                - (4_010.0 / (0.247 * 81_324.2) * 100.0 - 1.2))
                .abs()
                < 1e-9
        );
        assert_eq!(h["target_qty"], 0.247);
        assert_eq!(h["books"]["BTC"]["mode"], "On");
        // Dashboard-generic top level: equity as pnl_total, two positions,
        // no subsidy block without a points baseline.
        assert!((v["pnl_total"].as_f64().unwrap() - 8_490.0).abs() < 1e-9);
        assert_eq!(v["position_count"], 2);
        assert_eq!(v["positions"][0]["symbol"], "BTC (rh)");
        assert_eq!(v["positions"][1]["side"], "short");
        assert!(v["subsidy"].is_null());
        assert_eq!(v["updated_at"], "2026-09-19T10:02:22+00:00");
        // A healthy feed: no problem, snapshot dated by this tick.
        assert!(h["feed_problem"].is_null());
        assert_eq!(h["snapshot_at"], 1_789_812_142);
    }

    #[test]
    fn multi_symbol_status_keeps_primary_fields_and_reports_account_margin() {
        let cfg = multi_cfg();
        let mut state = State::default();
        state.books.insert("BTC".into(), sym_book(Mode::On, 0.247));
        state.books.insert("META".into(), sym_book(Mode::On, 13.3));
        let s = snap(
            &[
                ("BTC", 0.247, -0.247, 81_000.0, 81_000.0),
                ("META", 13.3, -13.3, 750.0, 751.0),
                ("AMZN", 0.0, 0.0, 250.0, 250.0),
            ],
            6_000.0,
            6_000.0,
        );
        let v = status_value(
            &cfg,
            &state,
            &s,
            &[],
            FeedStatus::default(),
            1_789_812_142,
            false,
            0,
        );
        let h = &v["hedge_holder"];
        // Card fields: the primary (BTC) book.
        assert_eq!(h["target_qty"], 0.247);
        assert_eq!(h["legs"]["long"]["qty"], 0.247);
        assert_eq!(h["legs"]["long"]["mark"], 81_000.0);
        // Account figures: gross over both symbols, MMR-weighted headroom.
        let gross = 0.247 * 81_000.0 + 13.3 * 750.0;
        assert!((h["legs"]["long"]["notional_usd"].as_f64().unwrap() - gross).abs() < 1e-6);
        let want = (6_000.0 * 100.0 - 0.247 * 81_000.0 * 1.2 - 13.3 * 750.0 * 3.0) / gross;
        assert!((h["legs"]["long"]["liq_headroom_pct"].as_f64().unwrap() - want).abs() < 1e-9);
        // Every symbol in `books`, four positions (AMZN flat).
        assert_eq!(h["books"]["META"]["long_qty"], 13.3);
        assert!(
            (h["books"]["META"]["basis_bps"].as_f64().unwrap() - (750.0 / 751.0 - 1.0) * 1e4).abs()
                < 1e-9
        );
        assert_eq!(h["books"]["AMZN"]["mode"], "Off");
        assert_eq!(v["position_count"], 4);
        assert_eq!(h["symbols"], serde_json::json!(["BTC", "META", "AMZN"]));
        assert_eq!(h["mode"], "On");
    }

    #[test]
    fn status_carries_the_feed_problem_and_dates_the_frozen_snapshot() {
        // A venue outage (Core 502/503, 2026-09-20 11:02–11:12Z): the tick
        // sends nothing but still publishes, from the last good snapshot,
        // with the reason and the snapshot's own timestamp so the card can
        // say "idle: venue unreachable, marks as of 11:01Z" instead of
        // going stale without a word.
        let cfg = cfg_for_test();
        let mut state = State::default();
        state.books.insert("BTC".into(), sym_book(Mode::On, 0.247));
        let s = snap(
            &[("BTC", 0.247, -0.247, 81_349.4, 81_324.2)],
            4_480.0,
            4_010.0,
        );
        let reason =
            "venue unreachable: short get_ticker BTC: Transient(\"recentTrades HTTP 503\")";
        let feed = FeedStatus {
            problem: Some(reason),
            snapshot_at: Some(1_789_812_142),
        };
        let v = status_value(&cfg, &state, &s, &[], feed, 1_789_812_742, false, 0);
        let h = &v["hedge_holder"];
        assert_eq!(h["feed_problem"], reason);
        // The frozen snapshot keeps its own date; `ts` is the write.
        assert_eq!(h["snapshot_at"], 1_789_812_142);
        assert_eq!(v["ts"], 1_789_812_742);
        // The legs shown are the last good read, not zeros.
        assert_eq!(h["legs"]["long"]["qty"], 0.247);
        assert_eq!(h["orders_this_tick"], 0);

        // Before any successful read `tick()` does not publish at all (the
        // default snapshot would show a flat book and −equity PnL as fresh
        // figures); a healthy tick then dates itself.
        let healthy = FeedStatus {
            problem: None,
            snapshot_at: Some(1_789_812_742),
        };
        let v1 = status_value(&cfg, &state, &s, &[], healthy, 1_789_812_742, false, 0);
        assert!(v1["hedge_holder"]["feed_problem"].is_null());
        assert_eq!(v1["hedge_holder"]["snapshot_at"], 1_789_812_742);
    }

    const POINTS: &str = concat!(
        "{\"account_index\":3209,\"arm\":\"freq\",\"ts_unix\":100,\"live_points_total\":72.0}\n",
        "{\"account_index\":281474976710500,\"arm\":\"b\",\"ts_unix\":100,\"live_points_total\":4.4}\n",
        "not json\n",
        "{\"account_index\":3209,\"arm\":\"freq\",\"ts_unix\":200,\"live_points_total\":72.5}\n",
        "{\"account_index\":3209,\"arm\":\"freq\",\"ts_unix\":300,\"live_points_total\":79.0}\n",
    );

    #[test]
    fn points_series_picks_one_account_and_skips_junk() {
        let s = points_series(POINTS, 3209);
        assert_eq!(s, vec![(100, 72.0), (200, 72.5), (300, 79.0)]);
        assert_eq!(points_series(POINTS, 281474976710500), vec![(100, 4.4)]);
        assert!(points_series(POINTS, 1).is_empty());
        let (latest, at) = points_latest_and_at(&s, 250);
        assert_eq!(latest, Some((300, 79.0)));
        assert_eq!(at, Some(72.5));
        assert_eq!(points_latest_and_at(&s, 50).1, None);
    }

    #[test]
    fn subsidy_block_counts_points_and_cost_since_arm_only() {
        let s = points_series(POINTS, 3209);
        // Armed at ts 150 with baseline 72.0 (the row in force then); equity −$4 since.
        let v = subsidy_block(&s, Some(72.0), Some(150), Some(-4.0), 300).unwrap();
        assert_eq!(v["unit"], "points");
        assert!((v["units_total"].as_f64().unwrap() - 7.0).abs() < 1e-9);
        assert!((v["cost_total_usd"].as_f64().unwrap() - 4.0).abs() < 1e-9);
        assert_eq!(v["as_of_ts"], 300);
        // 7d window starts at max(now − 7d, armed_at) = armed_at here → same as total.
        assert!((v["units_7d"].as_f64().unwrap() - 7.0).abs() < 1e-9);
        // No baseline (never armed / no collector rows at ARM) → no block.
        assert!(subsidy_block(&s, None, Some(150), Some(-4.0), 300).is_none());
        assert!(subsidy_block(&[], Some(72.0), Some(150), Some(-4.0), 300).is_none());
        // A profitable book reports a negative cost.
        let v = subsidy_block(&s, Some(72.0), Some(150), Some(2.5), 300).unwrap();
        assert!((v["cost_total_usd"].as_f64().unwrap() + 2.5).abs() < 1e-9);
    }

    #[test]
    fn utc_day_key_formats_the_date() {
        assert_eq!(utc_day_key(1_789_812_142), "2026-09-19");
    }

    // ------------------------------------------------ bot-strategy#1080: Arcus leg

    fn arcus_short_cfg() -> Config {
        let mut c = cfg_for_test();
        c.short_venue = VenueKind::Arcus;
        c.short_instance = "arcus".into();
        c
    }

    #[test]
    fn roll_mode_is_taker_only_and_strict_only_when_the_roll_is_enabled() {
        // unset / taker: fine either way
        for v in ["", "  ", "taker", "TAKER"] {
            assert!(check_roll_mode(v, true).is_ok(), "{v}");
            assert!(check_roll_mode(v, false).is_ok(), "{v}");
        }
        // maker_first was removed: refused while the roll is enabled …
        let err = check_roll_mode("maker_first", true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("maker_first was removed"), "{err}");
        assert!(check_roll_mode("maker", true).is_err());
        // … but never keeps the holder from starting with the roll off
        assert!(check_roll_mode("maker_first", false).is_ok());
        assert!(check_roll_mode("maker", false).is_ok());
    }

    #[test]
    fn roll_accounting_is_inert_while_the_roll_is_disabled() {
        let off = arcus_short_cfg();
        assert!(!off.roll.enabled);
        assert_eq!(off.roll_leg(), Some(Leg::Short));
        assert_eq!(off.roll_accounting_leg(), None);
        // nothing books with the roll off, even on the Arcus leg
        assert_eq!(
            roll_execution_booking(
                off.roll_accounting_leg(),
                Leg::Short,
                false,
                0.1,
                0.02,
                10.0,
                100.0
            ),
            None
        );
        let on = roll_cfg_on();
        assert_eq!(on.roll_accounting_leg(), Some(Leg::Short));
        assert!(roll_execution_booking(
            on.roll_accounting_leg(),
            Leg::Short,
            false,
            0.1,
            0.02,
            10.0,
            100.0
        )
        .is_some());
    }

    #[test]
    fn roll_close_outcomes_match_what_happened() {
        let min = 0.0001;
        let st = |sent, unc, filled| roll_close_stage(Leg::Short, sent, unc, filled, min, 4);
        assert_eq!(st(true, true, 0.05).unwrap_err(), "close_uncertain");
        assert_eq!(st(false, false, 0.0).unwrap_err(), "close_not_sent");
        assert_eq!(st(true, false, 0.0).unwrap_err(), "close_unfilled");
        assert_eq!(
            st(true, false, 0.00005).unwrap_err(),
            "close_partial_below_min"
        );
        // exactly the minimum re-opens
        let o = st(true, false, min).unwrap();
        assert_eq!((o.leg, o.qty), (Leg::Short, min));
        // filled enough: the re-open carries exactly the close fill, same leg
        let o = roll_close_stage(Leg::Long, true, false, 0.05, min, 4).unwrap();
        assert_eq!((o.leg, o.qty), (Leg::Long, 0.05));
        // a finer-precision fill is re-opened FLOORED to the size decimals,
        // never rounded up past what the close filled
        let o = roll_close_stage(Leg::Short, true, false, 0.123456, min, 4).unwrap();
        assert_eq!(o.qty, 0.1234);
        assert!(o.qty <= 0.123456);
        // floored below the minimum -> not re-openable
        assert_eq!(
            roll_close_stage(Leg::Short, true, false, 0.00019, 0.0002, 4).unwrap_err(),
            "close_partial_below_min"
        );
        assert_eq!(floor_to_decimals(0.3, 1), 0.3);
    }

    #[test]
    fn the_reopen_gates_use_fresh_positions_and_the_fresh_mark() {
        let cfg = multi_cfg();
        let syms = &cfg.symbols;
        let sym = syms[0].symbol.clone();
        // fresh position 0.2 (after the close) + re-open 0.05 at the FRESH mark 110
        let legs = reopen_legs_after(
            syms,
            Leg::Short,
            |s| if s == sym { -0.2 } else { 0.0 },
            |_| 100.0,
            &sym,
            0.05,
            110.0,
        );
        let n: f64 = legs.iter().map(|(n, _)| n).sum();
        assert!((n - 0.25 * 110.0).abs() < 1e-9, "{n}");
        // a stale-snapshot view (0.2 at 100) would understate the exposure
        assert!(n > 0.2 * 100.0);
    }

    #[test]
    fn a_roll_leg_counts_as_sent_only_once_accepted_or_uncertain() {
        // settled (accepted by the venue) -> sent
        assert!(roll_leg_sent(true, false));
        // became uncertain (reached the venue, may have filled) -> sent
        assert!(roll_leg_sent(false, true));
        // pre-send refusal or synchronous reject -> NOT sent (60 s backoff)
        assert!(!roll_leg_sent(false, false));
        // so a refused roll takes the backoff, not the interval
        let mut rs = RollState::default();
        roll_after_attempt(&mut rs, "BTC", 10_000, roll_leg_sent(false, false));
        assert_eq!(rs.last_roll_at, None);
        assert_eq!(rs.week_count, 0);
    }

    #[test]
    fn consecutive_refusals_back_off_exponentially_then_suspend_until_next_week() {
        assert_eq!(roll_refusal_backoff(1), 60);
        assert_eq!(roll_refusal_backoff(2), 120);
        assert_eq!(roll_refusal_backoff(3), 240);
        assert_eq!(roll_refusal_backoff(7), 3_600); // capped
        assert_eq!(roll_refusal_backoff(60), 3_600);
        let now = 1_790_467_200 + 3 * 86_400; // a Wednesday
        let mut rs = RollState {
            week: roll_week(now),
            ..RollState::default()
        };
        let mut t = now;
        for n in 1..ROLL_MAX_REFUSALS {
            assert!(!roll_after_attempt(&mut rs, "META", t, false));
            assert_eq!(
                rs.backoff.get("META"),
                Some(&RollBackoff {
                    refusals: n,
                    retry_not_before: t + roll_refusal_backoff(n)
                })
            );
            assert!(roll_symbol_backed_off(&rs, "META", t));
            t += roll_refusal_backoff(n);
            assert!(!roll_symbol_backed_off(&rs, "META", t));
        }
        // the N-th consecutive refusal suspends META until next Sunday 00:00 UTC
        assert!(roll_after_attempt(&mut rs, "META", t, false));
        let next_week = (roll_week(now) + 7) * 86_400;
        assert_eq!(rs.backoff["META"].retry_not_before, next_week);
        assert!(roll_symbol_backed_off(&rs, "META", next_week - 1));
        // ... and ONLY META: another symbol is neither backed off nor
        // suspended, and the global interval is untouched
        assert!(!roll_symbol_backed_off(&rs, "BTC", t));
        assert_eq!(roll_next_at(&rs, 3_600), 0);
        assert!(!roll_after_attempt(&mut rs, "BTC", t, true));
        assert!(roll_symbol_backed_off(&rs, "META", t + 3_600));
        // the rollover lifts it
        assert!(roll_week_rollover(&mut rs, next_week));
        assert!(rs.backoff.is_empty());
        assert!(!roll_symbol_backed_off(&rs, "META", next_week));
        // a roll that sends clears THAT symbol's streak only
        let mut rs = RollState::default();
        roll_after_attempt(&mut rs, "BTC", now, false);
        roll_after_attempt(&mut rs, "BTC", now + 60, false);
        roll_after_attempt(&mut rs, "META", now + 70, false);
        assert!(!roll_after_attempt(&mut rs, "BTC", now + 200, true));
        assert!(!rs.backoff.contains_key("BTC"));
        assert_eq!(rs.backoff["META"].refusals, 1);
    }

    #[test]
    fn a_backed_off_symbol_is_skipped_by_the_picker_not_blocking_the_others() {
        let cfg = roll_cfg_on().roll;
        let now = T0;
        let mut rs = RollState::default();
        for k in 0..ROLL_MAX_REFUSALS {
            roll_after_attempt(&mut rs, "META", now + k as u64, false);
        }
        let order = vec!["META".to_string(), "BTC".to_string()];
        let picked = pick_roll_symbol(&order, |sym| {
            let g = RollGate {
                backed_off: roll_symbol_backed_off(&rs, sym, now + 60),
                ..open_gate()
            };
            match roll_blocker(&cfg, &rs, now + 60, &g, 800.0, 0.5) {
                None => Ok(()),
                Some(w) => Err(w),
            }
        });
        assert_eq!(picked.map(|(s, _)| s), Ok("BTC".to_string()));
    }

    #[test]
    fn kill_switch_levels_the_roll_gap_by_reducing_the_other_leg() {
        // re-open blocked by KILL_SWITCH: reduce the other leg by what the
        // close filled, reduce-only
        assert_eq!(roll_kill_levelling(0.05, 0.47, 0.0001, 4), Some(0.05));
        // not one orderable order, or the other leg holds less: nothing
        assert_eq!(roll_kill_levelling(0.00005, 0.47, 0.0001, 4), None);
        assert_eq!(roll_kill_levelling(0.05, 0.03, 0.0001, 4), None);
        assert_eq!(roll_kill_levelling(0.0, 0.47, 0.0001, 4), None);
        // a finer-precision fill is levelled FLOORED, never rounded up
        assert_eq!(roll_kill_levelling(0.123456, 0.47, 0.0001, 4), Some(0.1234));
    }

    #[test]
    fn a_dry_run_live_flip_resets_the_roll_bookkeeping() {
        let mut st = State {
            dry_run: Some(true),
            ..State::default()
        };
        st.roll.week_volume_usd = 40_000.0;
        st.roll.week_cost_usd = 12.0;
        st.roll.last_roll_at = Some(1);
        st.roll.backoff.insert(
            "BTC".into(),
            RollBackoff {
                refusals: 3,
                retry_not_before: 9,
            },
        );
        st.roll.in_flight.insert("BTC".into(), (5_000.0, 2.6));
        let live = reconcile_state_mode(st.clone(), false);
        assert_eq!(live.roll, RollState::default());
        // same mode: kept
        let same = reconcile_state_mode(st, true);
        assert_eq!(same.roll.week_volume_usd, 40_000.0);
    }

    #[test]
    fn mark_of_reads_the_legs_own_venue() {
        let mut s = Snapshot::default();
        s.long.mark.insert("BTC".into(), 100.0);
        s.short.mark.insert("BTC".into(), 98.0);
        assert_eq!(s.mark_of(Leg::Long, "BTC"), 100.0);
        assert_eq!(s.mark_of(Leg::Short, "BTC"), 98.0);
        assert_eq!(s.mark_of(Leg::Short, "ETH"), 0.0);
    }

    #[test]
    fn a_refused_roll_backs_off_briefly_and_a_sent_one_starts_the_interval() {
        let interval = 3_600;
        let mut rs = RollState::default();
        // refused before any send: no interval spent, no roll counted; only
        // that symbol backs off
        roll_after_attempt(&mut rs, "BTC", 10_000, false);
        assert_eq!(rs.last_roll_at, None);
        assert_eq!(rs.week_count, 0);
        assert_eq!(roll_next_at(&rs, interval), 0);
        assert_eq!(
            rs.backoff["BTC"].retry_not_before,
            10_000 + ROLL_REFUSAL_BACKOFF_SECS
        );
        // sent: the interval starts and the backoff is cleared
        roll_after_attempt(&mut rs, "BTC", 10_100, true);
        assert_eq!(rs.last_roll_at, Some(10_100));
        assert!(rs.backoff.is_empty());
        assert_eq!(rs.week_count, 1);
        assert_eq!(roll_next_at(&rs, interval), 10_100 + interval);
    }

    #[test]
    fn state_json_written_with_the_removed_post_only_map_still_loads() {
        // a state.json from the maker_first era (pending_post_only present)
        let mut v = serde_json::to_value(State::default()).unwrap();
        v.as_object_mut().unwrap().insert(
            "pending_post_only".into(),
            serde_json::json!({"BTC": {"leg": "Short", "order_id": "o1", "qty": 0.05,
                "before_qty": -0.5, "mark": 100.0, "sent_at": 1}}),
        );
        let st: State = serde_json::from_value(v).unwrap();
        assert!(st.uncertain.is_empty());
    }

    fn roll_cfg_on() -> Config {
        let mut c = arcus_short_cfg();
        c.roll = RollCfg {
            enabled: true,
            interval_secs: 3_600,
            clip_usd: 400.0,
            weekly_volume_usd: 100_000.0,
            weekly_cost_usd: 50.0,
        };
        c
    }

    /// Any fixed booking time (bookings roll the week over to it).
    const T0: u64 = 1_791_072_000 + 3_600;

    fn open_gate() -> RollGate {
        RollGate {
            backed_off: false,
            balanced: true,
            halted: false,
            kill: false,
            uncertain: false,
            feed_ok: true,
            headroom_ok: true,
            leverage_ok: true,
            clip_ok: true,
            leg_holds_clip: true,
        }
    }

    #[test]
    fn roll_is_off_by_default_and_leaves_the_fingerprint_alone() {
        let c = cfg_for_test();
        assert!(!c.roll.enabled);
        assert!(c.validate().is_ok());
        let mut on = roll_cfg_on();
        let off_fp = {
            let mut o = on.clone();
            o.roll.enabled = false;
            o.fingerprint()
        };
        assert!(on.validate().is_ok());
        assert_ne!(on.fingerprint(), off_fp);
        on.roll.enabled = false;
        assert_eq!(on.fingerprint(), off_fp);
    }

    #[test]
    fn roll_config_is_validated_when_enabled() {
        // the Lighter leg is never rolled: no Arcus leg -> refused
        let mut c = cfg_for_test();
        c.roll = roll_cfg_on().roll;
        assert!(c.validate().unwrap_err().to_string().contains("Arcus leg"));
        let bad: Vec<Box<dyn Fn(&mut Config)>> = vec![
            Box::new(|c| c.roll.clip_usd = 0.0),
            Box::new(|c| c.roll.clip_usd = c.clip_usd + 1.0),
            Box::new(|c| c.roll.clip_usd = c.net_tolerance_usd + 1.0),
            Box::new(|c| c.roll.weekly_volume_usd = 0.0),
            Box::new(|c| c.roll.weekly_cost_usd = 0.0),
            Box::new(|c| c.roll.interval_secs = c.tick_secs - 1),
        ];
        for (i, f) in bad.iter().enumerate() {
            let mut c = roll_cfg_on();
            f(&mut c);
            assert!(c.validate().is_err(), "case {i} must be rejected");
        }
    }

    #[test]
    fn only_the_arcus_leg_is_ever_rolled() {
        assert_eq!(cfg_for_test().roll_leg(), None);
        assert_eq!(arcus_short_cfg().roll_leg(), Some(Leg::Short));
        let mut c = cfg_for_test();
        c.long_venue = VenueKind::Arcus;
        c.long_instance = "arcus".into();
        assert_eq!(c.roll_leg(), Some(Leg::Long));
    }

    #[test]
    fn roll_weeks_start_sunday_utc_and_reset_the_counters() {
        // 2026-09-27 is a Sunday (day 20723)
        assert_eq!(roll_week(1_790_467_200), 20_723);
        assert_eq!(roll_week(1_790_467_200 + 6 * 86_400 + 86_399), 20_723);
        assert_eq!(roll_week(1_790_467_199), 20_716); // Sat 09-26 -> week of 09-20
        assert_eq!(roll_week(1_791_072_000), 20_730); // next Sunday
        let mut rs = RollState {
            week: 20_723,
            week_volume_usd: 9_000.0,
            week_cost_usd: 3.0,
            week_count: 5,
            last_roll_at: Some(1_791_071_000),
            last_symbol: Some("BTC".into()),
            blocked_reason: Some("weekly_cost_cap".into()),
            ..RollState::default()
        };
        assert!(!roll_week_rollover(&mut rs, 1_791_071_999));
        assert_eq!(rs.week_count, 5);
        assert!(roll_week_rollover(&mut rs, 1_791_072_000));
        assert_eq!(
            (rs.week, rs.week_volume_usd, rs.week_cost_usd, rs.week_count),
            (20_730, 0.0, 0.0, 0)
        );
        assert_eq!(rs.last_roll_at, Some(1_791_071_000));
        assert!(rs.blocked_reason.is_none());

        // Actuals restart at zero; an in-flight reservation (an uncertain
        // roll leg) survives every rollover until it is released.
        let mut rs = RollState {
            week: 20_723,
            week_volume_usd: 9_000.0,
            week_cost_usd: 3.0,
            ..RollState::default()
        };
        rs.in_flight.insert("BTC".into(), (5_005.0, 2.6));
        assert!(roll_week_rollover(&mut rs, 1_791_072_000));
        assert_eq!((rs.week_volume_usd, rs.week_cost_usd), (0.0, 0.0));
        assert!(roll_week_rollover(&mut rs, 1_791_072_000 + 7 * 86_400));
        assert_eq!(roll_in_flight_total(&rs), (5_005.0, 2.6));
    }

    #[test]
    fn roll_gates_interval_every_blocker_and_the_weekly_caps() {
        let cfg = roll_cfg_on().roll;
        let rs = RollState {
            last_roll_at: Some(10_000),
            ..RollState::default()
        };
        let g = open_gate();
        assert_eq!(
            roll_blocker(&cfg, &rs, 10_000 + 3_599, &g, 800.0, 0.5),
            Some("interval")
        );
        assert_eq!(
            roll_blocker(&cfg, &rs, 10_000 + 3_600, &g, 800.0, 0.5),
            None
        );
        let now = 20_000;
        let cases: Vec<(Box<dyn Fn(&mut RollGate)>, &str)> = vec![
            (Box::new(|g| g.backed_off = true), "refusal_backoff"),
            (Box::new(|g| g.halted = true), "halted"),
            (Box::new(|g| g.kill = true), "kill_switch"),
            (Box::new(|g| g.uncertain = true), "uncertain_order"),
            (Box::new(|g| g.feed_ok = false), "feed"),
            (Box::new(|g| g.balanced = false), "unbalanced"),
            (Box::new(|g| g.headroom_ok = false), "liq_headroom"),
            (Box::new(|g| g.leverage_ok = false), "leverage"),
            (Box::new(|g| g.clip_ok = false), "clip_below_venue_min"),
            (
                Box::new(|g| g.leg_holds_clip = false),
                "leg_smaller_than_clip",
            ),
        ];
        for (f, want) in cases {
            let mut g = open_gate();
            f(&mut g);
            assert_eq!(roll_blocker(&cfg, &rs, now, &g, 800.0, 0.5), Some(want));
        }
        // weekly volume budget: the NEXT roll must fit
        let near = RollState {
            week_volume_usd: 99_300.0,
            ..rs.clone()
        };
        assert_eq!(
            roll_blocker(&cfg, &near, now, &g, 800.0, 0.5),
            Some("weekly_volume_budget")
        );
        assert_eq!(roll_blocker(&cfg, &near, now, &g, 700.0, 0.5), None);
        // weekly cost cap: the NEXT roll's worst-case cost must fit too
        let near_cost = RollState {
            week_cost_usd: 49.6,
            ..rs.clone()
        };
        assert_eq!(
            roll_blocker(&cfg, &near_cost, now, &g, 800.0, 0.5),
            Some("weekly_cost_cap")
        );
        assert_eq!(roll_blocker(&cfg, &near_cost, now, &g, 800.0, 0.3), None);
        let spent = RollState {
            week_cost_usd: 50.0,
            ..rs.clone()
        };
        assert_eq!(
            roll_blocker(&cfg, &spent, now, &g, 800.0, 0.5),
            Some("weekly_cost_cap")
        );
    }

    #[test]
    fn roll_clip_fits_the_net_tolerance_at_both_marks() {
        // Arcus short marked 2 % below the long venue: sizing at the Arcus
        // mark alone would put clip × long mark over the clip USD.
        let (arcus, long) = (98.0, 100.0);
        let clip_usd = 5_000.0;
        let naive = qty_for_notional(clip_usd, arcus, 4);
        assert!(naive * long > clip_usd);
        let q = qty_for_notional(clip_usd, roll_sizing_mark(arcus, long), 4);
        assert!(q * long <= clip_usd && q * arcus <= clip_usd);
        // and when Arcus is the higher one, it is used
        assert_eq!(roll_sizing_mark(101.0, 100.0), 101.0);
    }

    #[test]
    fn roll_sends_only_within_the_reserved_price_margin() {
        assert!(roll_price_ok(100.0, 100.05));
        assert!(roll_price_ok(100.0, 99.95));
        assert!(!roll_price_ok(100.0, 100.2));
        assert!(!roll_price_ok(100.0, 99.8));
        assert!(!roll_price_ok(0.0, 100.0));
        // the reservation covers the worst allowed price
        let (vol, cost) = roll_reserve(5_000.0, 3);
        assert!(vol >= 5_000.0 * (1.0 + ROLL_PRICE_MARGIN) - 1e-9);
        assert!(cost >= 5_000.0 * ROLL_PRICE_MARGIN);
    }

    #[test]
    fn every_settled_execution_on_the_rolled_leg_books_its_actual_value() {
        // a PLANNER repair (not a roll order) on the Arcus short leg: buy
        // 0.05 @ 100.2 filled value 5.01 vs mark 100 → booked at its actual
        // value, halted or not, whenever it happens
        let b =
            roll_execution_booking(Some(Leg::Short), Leg::Short, true, 0.05, 0.002, 5.01, 100.0)
                .unwrap();
        assert!((b.0 - 5.01).abs() < 1e-12);
        assert!((b.1 - (0.002 + 0.01)).abs() < 1e-12);
        let mut rs = RollState::default();
        roll_book_actual(&mut rs, T0, b);
        assert!((rs.week_volume_usd - 5.01).abs() < 1e-12);
        // the Lighter long leg is not the rolled leg: never booked
        assert_eq!(
            roll_execution_booking(Some(Leg::Short), Leg::Long, true, 0.05, 0.0, 5.0, 100.0),
            None
        );
        // no roll leg configured / nothing filled: nothing booked
        assert_eq!(
            roll_execution_booking(None, Leg::Short, true, 0.05, 0.0, 5.0, 100.0),
            None
        );
        assert_eq!(
            roll_execution_booking(Some(Leg::Short), Leg::Short, true, 0.0, 0.0, 0.0, 100.0),
            None
        );
    }

    #[test]
    fn in_flight_reservations_follow_the_leg_and_are_never_double_counted() {
        let r = roll_reserve(5_000.0, 3);
        // settled leg: the reservation goes, the fill was booked at settle
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), r);
        roll_settle_leg(&mut rs, "BTC", T0, Some((4_990.0, 1.2)));
        roll_finish_in_flight(&mut rs, "BTC", true, false);
        assert!(rs.in_flight.is_empty());
        assert_eq!((rs.week_volume_usd, rs.week_cost_usd), (4_990.0, 1.2));
        // uncertain leg: reservation kept, nothing booked yet …
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), r);
        roll_finish_in_flight(&mut rs, "BTC", false, true);
        assert_eq!(roll_in_flight_total(&rs), r);
        assert_eq!(rs.week_volume_usd, 0.0);
        // … released on evidence: observed fills replace it (once)
        roll_release_booking(&mut rs, T0, "BTC", true, 0.05, 0.002, 5.01, 100.0);
        assert!(rs.in_flight.is_empty());
        assert!((rs.week_volume_usd - 5.01).abs() < 1e-12);
        assert!((rs.week_cost_usd - (0.002 + 0.01)).abs() < 1e-12);
        // … or cleared by RISK_ACK without evidence: worst case at the current mark
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), r);
        roll_risk_ack_booking(&mut rs, T0, "BTC", true, 0.05, 110.0, 3);
        assert!(rs.in_flight.is_empty());
        let now = roll_reserve(0.05 * 110.0, 3);
        assert_eq!(
            (rs.week_volume_usd, rs.week_cost_usd),
            (r.0.max(now.0), r.1.max(now.1))
        );
        // a small reservation and a risen market: the current-mark worst case wins
        let small = roll_reserve(1.0, 3);
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), small);
        roll_risk_ack_booking(&mut rs, T0, "BTC", true, 0.05, 110.0, 3);
        assert_eq!((rs.week_volume_usd, rs.week_cost_usd), now);
        // … and never LESS than the send-time reservation (market fell since)
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), r);
        roll_risk_ack_booking(&mut rs, T0, "BTC", true, 0.05, 50.0, 3);
        assert_eq!((rs.week_volume_usd, rs.week_cost_usd), r);
        // roll taker limits are absolute, bound to the reserved mark
        assert_eq!(
            roll_limit_dec(true, 100.0, None),
            Some(Decimal::new(1001, 1))
        );
        assert_eq!(
            roll_limit_dec(false, 100.0, None),
            Some(Decimal::new(999, 1))
        );
    }

    #[test]
    fn a_zero_fill_below_the_touch_after_connector_rounding_is_a_refusal() {
        let d = |v: f64| Decimal::from_f64(v).unwrap();
        let (bid, ask) = (Some(d(1699.9)), Some(d(1700.1)));
        // our limit crossed, but the connector sent it one coarser tier tick
        // inward (1700.0 < ask 1700.1) and nothing filled: refusal
        assert!(roll_unfilled_is_refusal(true, 0.0, d(1700.0), bid, ask));
        // sell side mirror: sent above the bid
        assert!(roll_unfilled_is_refusal(false, 0.0, d(1700.0), bid, ask));
        // the sent price DID reach the touch and nothing filled: the book
        // moved — a genuine unfilled roll, not a refusal
        assert!(!roll_unfilled_is_refusal(true, 0.0, d(1700.1), bid, ask));
        assert!(!roll_unfilled_is_refusal(false, 0.0, d(1699.9), bid, ask));
        // anything filled ran, whatever price was sent
        assert!(!roll_unfilled_is_refusal(true, 0.01, d(1700.0), bid, ask));
    }

    #[test]
    fn a_planner_release_never_consumes_a_stale_roll_reservation() {
        let r = roll_reserve(5_000.0, 3);
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), r);
        // a PLANNER uncertain order on BTC released: books its own fill only
        roll_release_booking(&mut rs, T0, "BTC", false, 0.01, 0.0, 1.0, 100.0);
        assert_eq!(rs.in_flight.get("BTC"), Some(&r));
        assert!((rs.week_volume_usd - 1.0).abs() < 1e-12);
        // … and a planner RISK_ACK likewise
        roll_risk_ack_booking(&mut rs, T0, "BTC", false, 0.01, 100.0, 3);
        assert_eq!(rs.in_flight.get("BTC"), Some(&r));
        // the planner entry does not keep it alive: the next tick books it
        let mut unc = std::collections::BTreeMap::new();
        unc.insert("BTC".to_string(), uncertain_btc("arcus", "a"));
        let before = rs.week_volume_usd;
        assert_eq!(
            roll_settle_stale_in_flight(&mut rs, T0, &unc),
            vec!["BTC".to_string()]
        );
        assert!(rs.in_flight.is_empty());
        assert!((rs.week_volume_usd - (before + r.0)).abs() < 1e-9);
    }

    #[test]
    fn stale_in_flight_after_a_restart_is_booked_not_lost() {
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), (5_005.0, 2.6));
        rs.in_flight.insert("META".into(), (1_000.0, 0.5));
        let mut unc = std::collections::BTreeMap::new();
        unc.insert(
            "META".to_string(),
            UncertainOrder {
                roll: true,
                ..uncertain_btc("arcus", "a")
            },
        );
        // BTC has no uncertain order behind it: booked and dropped; META stays
        assert_eq!(
            roll_settle_stale_in_flight(&mut rs, T0, &unc),
            vec!["BTC".to_string()]
        );
        assert_eq!((rs.week_volume_usd, rs.week_cost_usd), (5_005.0, 2.6));
        assert_eq!(roll_in_flight_total(&rs), (1_000.0, 0.5));
    }

    #[test]
    fn the_next_roll_is_gated_on_actual_plus_in_flight_plus_its_inflated_reservations() {
        let cfg = roll_cfg_on().roll; // 100k volume / 50 cost
        let g = open_gate();
        let (lv, lc) = roll_reserve(10_000.0, 3);
        // exactly the raw two-leg notional left: the inflated reservation does not fit
        let rs = RollState {
            week_volume_usd: cfg.weekly_volume_usd - 20_000.0,
            ..RollState::default()
        };
        assert_eq!(
            roll_blocker(&cfg, &rs, 20_000, &g, 2.0 * lv, 2.0 * lc),
            Some("weekly_volume_budget")
        );
        // in-flight reservations count too
        let mut rs = RollState::default();
        rs.in_flight
            .insert("BTC".into(), (cfg.weekly_volume_usd - 1_000.0, 0.0));
        assert_eq!(
            roll_blocker(&cfg, &rs, 20_000, &g, 2.0 * lv, 2.0 * lc),
            Some("weekly_volume_budget")
        );
        let mut rs = RollState::default();
        rs.in_flight
            .insert("BTC".into(), (0.0, cfg.weekly_cost_usd - 1.0));
        assert_eq!(
            roll_blocker(&cfg, &rs, 20_000, &g, 2.0 * lv, 2.0 * lc),
            Some("weekly_cost_cap")
        );
        assert_eq!(
            roll_blocker(&cfg, &RollState::default(), 20_000, &g, 2.0 * lv, 2.0 * lc),
            None
        );
    }

    #[test]
    fn a_roll_ioc_is_sent_only_inside_the_reserved_margin_at_an_absolute_limit() {
        // the leg's fresh mark 0.3 % away from the reserved mark: not sent
        assert!(!roll_price_ok(100.0, 99.7));
        assert!(!roll_price_ok(100.0, 100.3));
        assert!(roll_price_ok(100.0, 99.95));
        // and what IS sent is capped at the same margin, whatever the touch
        let tick = Some(Decimal::new(1, 2));
        let buy = roll_limit_dec(true, 100.0, tick).unwrap();
        let sell = roll_limit_dec(false, 100.0, tick).unwrap();
        assert!(buy <= Decimal::new(1001, 1) && buy > Decimal::new(100, 0));
        assert!(sell >= Decimal::new(999, 1) && sell < Decimal::new(100, 0));
    }

    #[test]
    fn roll_gates_on_the_net_exposure_after_the_close() {
        // long-heavy by $400 (net +4 @100), $400 short close, $500 tol:
        // the close would leave $800 of exposure -> refused
        assert!(!roll_post_close_net_ok(4.0, Leg::Short, 4.0, 100.0, 500.0));
        // short-heavy by $400: closing short moves it to 0 -> fine
        assert!(roll_post_close_net_ok(-4.0, Leg::Short, 4.0, 100.0, 500.0));
        // balanced book, clip within tolerance -> fine
        assert!(roll_post_close_net_ok(0.0, Leg::Short, 4.0, 100.0, 500.0));
        // long leg rolled: direction reverses
        assert!(!roll_post_close_net_ok(-4.0, Leg::Long, 4.0, 100.0, 500.0));
        assert!(roll_post_close_net_ok(4.0, Leg::Long, 4.0, 100.0, 500.0));
    }

    #[test]
    fn roll_round_robin_skips_blocked_books() {
        let order: Vec<String> = ["BTC", "META", "AMZN"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        // BTC is persistently blocked: META must still be rolled, and the
        // winner comes back with what its gate computed (not recomputed).
        let picked = pick_roll_symbol(&order, |s| {
            if s == "BTC" {
                Err("unbalanced")
            } else {
                Ok((s.len() as f64, 0.5))
            }
        });
        assert_eq!(picked, Ok(("META".to_string(), (4.0, 0.5))));
        // Everyone blocked: the first candidate's reason is reported.
        let none: Result<(String, ()), _> = pick_roll_symbol(&order, |_| Err("leverage"));
        assert_eq!(none, Err(Some(("BTC".to_string(), "leverage"))));
    }

    fn d(v: &str) -> Decimal {
        v.parse().unwrap()
    }

    #[test]
    fn a_roll_ioc_that_cannot_reach_the_touch_is_not_sent() {
        let s = |v: &str| Some(d(v));
        // buy limit 100.1 vs asks
        assert!(roll_limit_crosses(true, d("100.1"), s("99.9"), s("100.05")));
        // wide book
        assert!(!roll_limit_crosses(true, d("100.1"), s("99.8"), s("100.3")));
        // sell limit 99.9 vs bids
        assert!(roll_limit_crosses(false, d("99.9"), s("99.95"), s("100.1")));
        assert!(!roll_limit_crosses(false, d("99.9"), s("99.7"), s("100.1")));
        // exact touch is marketable on both sides; one tick short is not
        // (no tolerance)
        assert!(roll_limit_crosses(true, d("100.1"), s("99.9"), s("100.1")));
        assert!(roll_limit_crosses(false, d("99.9"), s("99.9"), s("100.1")));
        assert!(!roll_limit_crosses(
            false,
            d("99.91"),
            s("99.9"),
            s("100.1")
        ));
        assert!(!roll_limit_crosses(
            true,
            d("700.69"),
            s("700.0"),
            s("700.70")
        ));
        // no / one-sided / crossed book: not marketable
        assert!(!roll_limit_crosses(true, d("100.1"), None, s("100.0")));
        assert!(!roll_limit_crosses(true, d("100.1"), s("0"), s("100.0")));
        assert!(!roll_limit_crosses(
            true,
            d("100.1"),
            s("100.2"),
            s("100.0")
        ));
    }

    #[test]
    fn the_roll_limit_is_rounded_like_the_venue_and_survives_binary_inexact_marks() {
        // the pairtrade#315 trap: 700.0 * 1.001 in f64 is 700.6999999999999,
        // which the connector's inward (buy-down) tick rounding turns into
        // 700.69 — one tick short of a 700.70 ask. Decimal arithmetic does not.
        assert_eq!(700.0_f64 * 1.001, 700.6999999999999);
        let tick = Some(d("0.01"));
        let buy = roll_limit_dec(true, 700.0, tick).unwrap();
        assert_eq!(buy, d("700.7"));
        assert!(roll_limit_crosses(
            true,
            buy,
            Some(d("700.0")),
            Some(d("700.70"))
        ));
        // sell: 700 × 0.999 = 699.3 exactly, rounded UP (inward) to the tick
        assert_eq!(roll_limit_dec(false, 700.0, tick).unwrap(), d("699.3"));
        // binary-inexact marks: rounding is inward (buy never above mark ×
        // 1.001, sell never below mark × 0.999) and lands on the tick
        let buy = roll_limit_dec(true, 1700.1, tick).unwrap();
        assert_eq!(buy, d("1701.8")); // 1701.8001 floored
        let sell = roll_limit_dec(false, 1700.1, tick).unwrap();
        assert_eq!(sell, d("1698.4")); // 1698.3999 ceiled to the tick
        let buy = roll_limit_dec(true, 84537.8, Some(d("0.1"))).unwrap();
        assert_eq!(buy, d("84622.3")); // 84622.3378 floored
                                       // a binary-inexact mark whose limit lands exactly ON a tick: 0.3 is
                                       // 0.29999999999999998889… in f64, so from_f64_retain would give
                                       // 0.30029999… and floor it a whole tick short to 0.3002
        assert_eq!(
            roll_limit_dec(true, 0.3, Some(d("0.0001"))).unwrap(),
            d("0.3003")
        );
        // no tick: the exact decimal
        assert_eq!(roll_limit_dec(true, 100.0, None).unwrap(), d("100.1"));
        assert!(roll_limit_dec(true, 0.0, tick).is_none());
        assert!(roll_limit_dec(true, f64::NAN, tick).is_none());
    }

    #[test]
    fn one_cap_check_gates_the_roll_and_the_reopen_is_not_re_capped() {
        let cfg = RollCfg {
            weekly_volume_usd: 10_000.0,
            weekly_cost_usd: 20.0,
            ..roll_cfg_on().roll
        };
        let mut rs = RollState {
            week_volume_usd: 5_000.0,
            week_cost_usd: 5.0,
            ..RollState::default()
        };
        assert_eq!(roll_fits(&cfg, &rs, roll_reserve(4_000.0, 3)), None);
        assert_eq!(
            roll_fits(&cfg, &rs, roll_reserve(5_100.0, 3)),
            Some("weekly_volume_budget")
        );
        let tight = RollCfg {
            weekly_cost_usd: 10.0,
            ..cfg.clone()
        };
        assert_eq!(
            roll_fits(&tight, &rs, roll_reserve(4_000.0, 3)),
            Some("weekly_cost_cap")
        );
        // exactly at the cap still fits
        assert_eq!(roll_fits(&cfg, &rs, (5_000.0, 0.0)), None);
        // other in-flight legs count too
        rs.in_flight.insert("META".into(), (2_000.0, 1.0));
        assert_eq!(
            roll_fits(&cfg, &rs, roll_reserve(4_000.0, 3)),
            Some("weekly_volume_budget")
        );
        // the pick-time gate IS that check (no second copy to drift)
        let g = open_gate();
        for (v, c) in [(1_000.0, 1.0), (4_000.0, 1.0), (1_000.0, 20.0)] {
            assert_eq!(
                roll_blocker(&cfg, &rs, u64::MAX / 2, &g, v, c),
                roll_fits(&cfg, &rs, (v, c))
            );
        }
        // growth gates re-read right before the re-open
        assert_eq!(roll_reopen_blocked(false, true, true), None);
        assert_eq!(
            roll_reopen_blocked(true, true, true),
            Some("reopen_blocked_kill")
        );
        assert_eq!(
            roll_reopen_blocked(false, false, true),
            Some("reopen_blocked_headroom")
        );
        assert_eq!(
            roll_reopen_blocked(false, true, false),
            Some("reopen_blocked_leverage")
        );
    }

    #[test]
    fn roll_fees_always_raise_the_booked_cost() {
        let paid = roll_cost(true, 0.01, 0.02, 1.001, 100.0);
        // a fee reported with the opposite sign books the same cost
        assert!((roll_cost(true, 0.01, -0.02, 1.001, 100.0) - paid).abs() < 1e-12);
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), (1.0, 1.0));
        roll_release_booking(&mut rs, T0, "BTC", true, 0.01, -0.02, 1.0, 100.0);
        assert!((rs.week_cost_usd - 0.02).abs() < 1e-12);
    }

    #[test]
    fn bookings_land_in_the_week_of_the_booking_time() {
        let sat = 1_791_072_000 - 5; // Sat 23:59:55 UTC (next Sunday 00:00 = 1_791_072_000)
        let sun = 1_791_072_000 + 7;
        let mut rs = RollState::default();
        roll_week_rollover(&mut rs, sat);
        rs.week_volume_usd = 3_000.0;
        // a roll started Saturday settles Sunday: it books into the NEW week
        roll_book_actual(&mut rs, sun, (800.0, 0.5));
        assert_eq!(rs.week, roll_week(sun));
        assert_eq!((rs.week_volume_usd, rs.week_cost_usd), (800.0, 0.5));
        // and the next tick-start rollover does not wipe it
        assert!(!roll_week_rollover(&mut rs, sun + 30));
        assert_eq!(rs.week_volume_usd, 800.0);
        // same through the leg-settle path
        let mut rs = RollState::default();
        roll_week_rollover(&mut rs, sat);
        rs.in_flight.insert("BTC".into(), (400.4, 0.3));
        roll_settle_leg(&mut rs, "BTC", sun, Some((400.0, 0.2)));
        assert_eq!(rs.week, roll_week(sun));
        assert_eq!(rs.week_volume_usd, 400.0);
    }

    #[test]
    fn a_settled_roll_leg_never_leaves_both_its_reservation_and_its_booking() {
        let now = 1_791_072_000 + 100;
        let mut rs = RollState::default();
        roll_week_rollover(&mut rs, now);
        rs.in_flight.insert("BTC".into(), roll_reserve(400.0, 3));
        roll_settle_leg(&mut rs, "BTC", now, Some((400.0, 0.2)));
        // the persisted state (one mutation) holds the actual only
        assert!(!rs.in_flight.contains_key("BTC"));
        assert_eq!(rs.week_volume_usd, 400.0);
        // a restart then finds nothing stale to book on top
        let stale = roll_settle_stale_in_flight(&mut rs, now, &Default::default());
        assert!(stale.is_empty());
        assert_eq!(rs.week_volume_usd, 400.0);
        // an unfilled roll leg still drops its reservation
        rs.in_flight.insert("META".into(), roll_reserve(100.0, 3));
        roll_settle_leg(&mut rs, "META", now, None);
        assert!(rs.in_flight.is_empty());
        assert_eq!(rs.week_volume_usd, 400.0);
    }

    #[test]
    fn roll_reservation_covers_the_worst_allowed_price() {
        let reserved = roll_reserve(5_000.0, 3);
        let worst = 5_000.0 * (1.0 + ROLL_PRICE_MARGIN);
        assert!((reserved.0 - worst).abs() < 1e-9);
        assert!(
            (reserved.1 - (worst * 5.25 / 10_000.0 + 5_000.0 * ROLL_PRICE_MARGIN)).abs() < 1e-9
        );
    }

    #[test]
    fn roll_cost_is_fees_plus_slippage_against_the_mark() {
        // buy 0.01 @ 100.1 vs mark 100: 0.001 slippage + 0.02 fee
        assert!((roll_cost(true, 0.01, 0.02, 1.001, 100.0) - 0.021).abs() < 1e-12);
        // sell 0.01 @ 99.9 vs mark 100
        assert!((roll_cost(false, 0.01, 0.0, 0.999, 100.0) - 0.001).abs() < 1e-12);
        // price improvement never credits the cap: a buy BELOW mark and a sell
        // ABOVE mark book the fee only (never a negative cost)
        assert!((roll_cost(true, 0.01, 0.001, 0.999, 100.0) - 0.001).abs() < 1e-12);
        assert!((roll_cost(false, 0.01, 0.001, 1.001, 100.0) - 0.001).abs() < 1e-12);
        assert_eq!(roll_cost(true, 0.01, 0.0, 0.5, 100.0), 0.0);
        assert!(roll_cost(true, 0.01, -0.001, 0.999, 100.0) >= 0.0);
    }

    #[test]
    fn roll_state_loads_from_older_state_json() {
        let mut v = serde_json::to_value(State::default()).unwrap();
        v.as_object_mut().unwrap().remove("roll");
        let st: State = serde_json::from_value(v).unwrap();
        assert_eq!(st.roll, RollState::default());
    }

    #[test]
    fn venue_parses_with_lighter_as_the_default() {
        assert_eq!(VenueKind::parse("").unwrap(), VenueKind::Lighter);
        assert_eq!(VenueKind::parse(" Lighter ").unwrap(), VenueKind::Lighter);
        assert_eq!(VenueKind::parse("ARCUS").unwrap(), VenueKind::Arcus);
        assert!(VenueKind::parse("core").is_err());
        assert_eq!(VenueKind::Arcus.dex_name(), "arcus");
        assert_eq!(VenueKind::default(), VenueKind::Lighter);
    }

    #[test]
    fn default_venues_keep_the_multi_symbol_fingerprint() {
        // The running Tokyo env (HEDGE_SYMBOLS with one MMR each, both legs
        // Lighter) must hash exactly the field list 7b1849e hashed.
        let c = multi_cfg();
        let before = config_fingerprint(&[
            ("symbol", "BTC".to_string()),
            ("long", "rh".to_string()),
            ("short", "core".to_string()),
            ("target", "20000.00".to_string()),
            ("max_notional", "30000.00".to_string()),
            ("clip", "10000.00".to_string()),
            ("slip_bps", "3".to_string()),
            ("net_tol", "500.00".to_string()),
            ("mmr", "1.200".to_string()),
            ("liq_guard", "8.000".to_string()),
            ("max_lev", "5.00".to_string()),
            ("symbols", "BTC:1.200,META:3.000,AMZN:6.000".to_string()),
        ]);
        assert_eq!(c.fingerprint(), before);
        // Fill waits are operational, not a strategy parameter.
        let mut w = multi_cfg();
        w.fill_wait_secs = vec![3, 6, 10];
        assert_eq!(w.fingerprint(), before);
    }

    #[test]
    fn a_non_default_venue_or_per_leg_mmr_changes_the_fingerprint() {
        let base = cfg_for_test().fingerprint();
        assert_ne!(arcus_short_cfg().fingerprint(), base);
        let mut long_arcus = cfg_for_test();
        long_arcus.long_venue = VenueKind::Arcus;
        assert_ne!(long_arcus.fingerprint(), base);
        assert_ne!(long_arcus.fingerprint(), arcus_short_cfg().fingerprint());
        let mut split = cfg_for_test();
        split.symbols[0].short_mmr_pct = 1.667;
        assert_ne!(split.fingerprint(), base);
        let mut multi_split = multi_cfg();
        multi_split.symbols[0].short_mmr_pct = 1.667;
        assert_ne!(multi_split.fingerprint(), multi_cfg().fingerprint());
    }

    #[test]
    fn the_same_instance_is_fine_on_two_different_venues() {
        let mut c = arcus_short_cfg();
        c.short_instance = "rh".into();
        assert!(
            c.validate().is_ok(),
            "arcus:rh vs lighter:rh are two accounts"
        );
        c.short_venue = VenueKind::Lighter;
        assert!(c.validate().is_err(), "lighter:rh twice is one account");
    }

    #[test]
    fn a_live_arcus_leg_needs_its_own_token() {
        let mut c = arcus_short_cfg();
        assert!(c.validate().is_ok(), "DRY_RUN needs no token");
        c.dry_run = false;
        c.live_confirm = LIVE_CONFIRM_TOKEN.into();
        assert!(
            c.validate().is_err(),
            "G0 token alone must not go live on Arcus"
        );
        c.arcus_live_confirm = "1046-G0-PASSED".into();
        assert!(c.validate().is_err());
        c.arcus_live_confirm = ARCUS_LIVE_CONFIRM_TOKEN.into();
        assert!(c.validate().is_ok());
        // A Lighter-only live deployment is unaffected by the new gate.
        let mut l = cfg_for_test();
        l.dry_run = false;
        l.live_confirm = LIVE_CONFIRM_TOKEN.into();
        assert!(l.validate().is_ok());
        // The Arcus token never substitutes for the G0 one.
        c.live_confirm = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn symbols_parse_a_per_leg_mmr_pair() {
        let s = parse_symbols("BTC:1.2/1.667, meta:3/6.667 ,AMZN:6").unwrap();
        assert_eq!(
            s.iter()
                .map(|c| (c.symbol.as_str(), c.mmr_pct, c.short_mmr_pct))
                .collect::<Vec<_>>(),
            vec![
                ("BTC", 1.2, 1.667),
                ("META", 3.0, 6.667),
                ("AMZN", 6.0, 6.0)
            ]
        );
        assert_eq!(s[0].spec(3), "BTC:1.200/1.667");
        assert_eq!(s[2].spec(3), "AMZN:6.000");
        assert!(parse_symbols("BTC:1.2/").is_err());
        assert!(parse_symbols("BTC:1.2/0").is_err());
        assert!(parse_symbols("BTC:/1.2").is_err());
        assert!(parse_symbols("BTC:1.2/x").is_err());
        let mut c = cfg_for_test();
        c.symbols[0].short_mmr_pct = f64::NAN;
        assert!(c.validate().is_err(), "NaN short MMR");
    }

    #[test]
    fn each_leg_guard_uses_its_own_mmr() {
        let mut c = cfg_for_test();
        c.symbols = parse_symbols("BTC:1.2/1.667").unwrap();
        let mut snap = Snapshot::default();
        snap.long.qty.insert("BTC".into(), 0.5);
        snap.long.mark.insert("BTC".into(), 80_000.0);
        snap.short.qty.insert("BTC".into(), -0.5);
        snap.short.mark.insert("BTC".into(), 80_000.0);
        assert_eq!(snap.legs(&c, Leg::Long), vec![(40_000.0, 1.2)]);
        assert_eq!(snap.legs(&c, Leg::Short), vec![(40_000.0, 1.667)]);
        // $4k equity: long headroom (4000 − 480) / 40000 = 8.8 %, short
        // (4000 − 666.8) / 40000 = 8.333 %: an 8.5 % guard trips only the
        // Arcus-side account.
        let long_h = liq_headroom_pct(4_000.0, &snap.legs(&c, Leg::Long)).unwrap();
        let short_h = liq_headroom_pct(4_000.0, &snap.legs(&c, Leg::Short)).unwrap();
        assert!((long_h - 8.8).abs() < 1e-9, "{long_h}");
        assert!((short_h - 8.333).abs() < 1e-9, "{short_h}");
    }

    #[test]
    fn venue_symbols_map_to_book_keys() {
        assert_eq!(book_symbol("BTC-USD"), "BTC");
        assert_eq!(book_symbol("meta-usd"), "META");
        assert_eq!(book_symbol("BTC"), "BTC");
        assert_eq!(book_symbol(" eth "), "ETH");
        assert_eq!(book_symbol("-USD"), "-USD", "no empty key");
        assert_eq!(book_symbol("SOL-USDC"), "SOL-USDC", "only the -USD suffix");
    }

    #[test]
    fn arcus_settle_needs_terminal_evidence_that_agrees_with_the_position() {
        let tol = 0.000005;
        // still on the book
        assert_eq!(
            settle_decision(1.0, true, false, 1.0, 1.0, tol),
            Settle::Pending
        );
        // a partial fill visible, the rest not yet canceled: must not size the pair
        assert_eq!(
            settle_decision(1.0, false, false, 0.3, 0.3, tol),
            Settle::Pending
        );
        // partial, rest canceled, position agrees -> terminal at the partial
        assert_eq!(
            settle_decision(1.0, false, true, 0.3, 0.3, tol),
            Settle::Terminal(0.3)
        );
        // fully filled
        assert_eq!(
            settle_decision(1.0, false, false, 1.0, 1.0, tol),
            Settle::Terminal(1.0)
        );
        // fills seen but the position has not moved yet (or vice versa)
        assert_eq!(
            settle_decision(1.0, false, false, 1.0, 0.0, tol),
            Settle::Pending
        );
        assert_eq!(
            settle_decision(1.0, false, true, 0.0, 0.4, tol),
            Settle::Pending
        );
        // zero fill, canceled, flat -> terminal 0
        assert_eq!(
            settle_decision(1.0, false, true, 0.0, 0.0, tol),
            Settle::Terminal(0.0)
        );
        // not open, nothing visible yet -> pending (could be not placed yet)
        assert_eq!(
            settle_decision(1.0, false, false, 0.0, 0.0, tol),
            Settle::Pending
        );
    }

    fn uncertain_btc(exchange: &str, instance: &str) -> UncertainOrder {
        UncertainOrder {
            leg: Leg::Short,
            exchange: exchange.into(),
            instance: instance.into(),
            order_id: Some("o1".into()),
            qty: 0.1,
            before_qty: 0.0,
            mark: 100.0,
            sent_at: 1_000,
            reason: "t".into(),
            roll: false,
        }
    }

    #[test]
    fn uncertain_orders_release_only_on_terminal_evidence_never_on_time() {
        let u = uncertain_btc("arcus", "arcus-a");
        // evidence -> release
        assert_eq!(
            uncertain_action(&u, "arcus", "arcus-a", true, 1_010, 60),
            UncertainAction::Release
        );
        // no evidence, within grace -> keep blocked
        assert_eq!(
            uncertain_action(&u, "arcus", "arcus-a", false, 1_030, 60),
            UncertainAction::Keep
        );
        // no evidence past the grace -> NOT released: escalate (halt, operator)
        assert!(matches!(
            uncertain_action(&u, "arcus", "arcus-a", false, 1_060, 60),
            UncertainAction::Escalate(_)
        ));
        assert!(matches!(
            uncertain_action(&u, "arcus", "arcus-a", false, 99_999, 60),
            UncertainAction::Escalate(_)
        ));
    }

    #[test]
    fn uncertain_order_on_another_venue_or_instance_is_never_read_as_evidence() {
        let u = uncertain_btc("arcus", "arcus-old");
        // leg now points at a different instance / venue: escalate at once,
        // even with "evidence" (it would come from the wrong account)
        assert!(matches!(
            uncertain_action(&u, "arcus", "arcus-new", true, 1_001, 60),
            UncertainAction::Escalate(_)
        ));
        assert!(matches!(
            uncertain_action(&u, "lighter", "arcus-old", true, 1_001, 60),
            UncertainAction::Escalate(_)
        ));
        // an entry persisted before the field existed (instance "") only
        // checks the exchange
        let legacy = uncertain_btc("arcus", "");
        assert_eq!(
            uncertain_action(&legacy, "arcus", "any", true, 1_001, 60),
            UncertainAction::Release
        );
    }

    #[test]
    fn a_late_fill_blocks_the_symbol_until_reconciled_and_never_resends() {
        let mut state = State::default();
        // Tick 1: the IOC is still pending after every wait -> uncertain.
        assert_eq!(
            settle_decision(0.1, false, false, 0.0, 0.0, 0.000005),
            Settle::Pending
        );
        state
            .uncertain
            .insert("BTC".into(), uncertain_btc("arcus", "arcus-a"));
        // Tick 2: the fill is now visible, the book looks lopsided and the
        // planner wants to send the same short again — it must not.
        let plan = vec![
            (
                "BTC".to_string(),
                vec![Order {
                    leg: Leg::Short,
                    qty: 0.1,
                }],
            ),
            (
                "META".to_string(),
                vec![Order {
                    leg: Leg::Long,
                    qty: 1.0,
                }],
            ),
        ];
        let none = std::collections::BTreeSet::new();
        let kept = drop_uncertain(plan.clone(), &state.uncertain, &none);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].0, "META");
        // Released during THIS tick (the plan was built from the older
        // snapshot): still blocked until the next tick replans it.
        assert_eq!(
            uncertain_action(
                &uncertain_btc("arcus", "arcus-a"),
                "arcus",
                "arcus-a",
                true,
                1_090,
                60
            ),
            UncertainAction::Release
        );
        state.uncertain.remove("BTC");
        let released: std::collections::BTreeSet<String> = ["BTC".to_string()].into();
        let kept = drop_uncertain(plan.clone(), &state.uncertain, &released);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].0, "META");
        // Next tick: nothing released now -> BTC plans again.
        assert_eq!(drop_uncertain(plan, &state.uncertain, &none).len(), 2);
    }

    #[test]
    fn points_are_only_attributed_to_a_lighter_long_leg() {
        assert_eq!(
            points_account_index(Some(VenueKind::Lighter), Some(" 3209 ".into())),
            Some(3209)
        );
        assert_eq!(
            points_account_index(Some(VenueKind::Arcus), Some("3209".into())),
            None
        );
        assert_eq!(points_account_index(None, Some("3209".into())), None);
        assert_eq!(points_account_index(Some(VenueKind::Lighter), None), None);
    }

    #[test]
    fn a_reconcile_during_the_tick_sends_nothing_that_tick() {
        let plan = vec![
            (
                "BTC".to_string(),
                vec![Order {
                    leg: Leg::Short,
                    qty: 0.1,
                }],
            ),
            (
                "META".to_string(),
                vec![Order {
                    leg: Leg::Long,
                    qty: 1.0,
                }],
            ),
        ];
        let none = std::collections::BTreeSet::new();
        let empty = std::collections::BTreeMap::new();
        assert_eq!(
            tick_plan_after_reconcile(plan.clone(), &empty, &none).len(),
            2
        );
        // BTC reconciled this tick: META must not trade on the stale snapshot either
        let released: std::collections::BTreeSet<String> = ["BTC".to_string()].into();
        assert!(tick_plan_after_reconcile(plan.clone(), &empty, &released).is_empty());
        // still uncertain: only that symbol is dropped (before the leverage guard)
        let mut unc = std::collections::BTreeMap::new();
        unc.insert("BTC".to_string(), uncertain_btc("arcus", "a"));
        let kept = tick_plan_after_reconcile(plan, &unc, &none);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].0, "META");
    }

    #[test]
    fn uncertain_orders_persist_and_old_state_loads_without_them() {
        let mut v = serde_json::to_value(State::default()).unwrap();
        v.as_object_mut().unwrap().remove("uncertain");
        let old: State = serde_json::from_value(v).unwrap();
        assert!(old.uncertain.is_empty());
        let mut st = State::default();
        st.uncertain.insert(
            "BTC".into(),
            UncertainOrder {
                leg: Leg::Long,
                exchange: "lighter".into(),
                instance: "rh".into(),
                order_id: None,
                qty: 0.2,
                before_qty: 0.5,
                mark: 0.0,
                sent_at: 7,
                reason: "submission ambiguous".into(),
                roll: true,
            },
        );
        let back: State = serde_json::from_str(&serde_json::to_string(&st).unwrap()).unwrap();
        assert_eq!(back.uncertain, st.uncertain);
        // an entry persisted before the `roll` flag existed loads as a planner order
        let mut e = serde_json::to_value(&st.uncertain["BTC"]).unwrap();
        e.as_object_mut().unwrap().remove("roll");
        let old_entry: UncertainOrder = serde_json::from_value(e).unwrap();
        assert!(!old_entry.roll);
    }

    #[test]
    fn fill_waits_parse_and_validate() {
        assert_eq!(parse_fill_waits("2,4").unwrap(), vec![2, 4]);
        assert_eq!(parse_fill_waits(" 3, 6 ,10 ").unwrap(), vec![3, 6, 10]);
        assert!(parse_fill_waits("").is_err());
        assert!(parse_fill_waits("0,4").is_err());
        assert!(parse_fill_waits("2,x").is_err());
        assert!(parse_fill_waits("30,31").is_err(), "over 60 s");
        let mut c = cfg_for_test();
        c.fill_wait_secs = vec![];
        assert!(c.validate().is_err());
    }

    #[test]
    fn order_side_is_the_one_mapping_for_send_limit_and_cost() {
        let o = |leg, qty| Order { leg, qty };
        assert_eq!(order_side(&o(Leg::Long, 1.0)), (OrderSide::Long, false));
        assert_eq!(order_side(&o(Leg::Long, -1.0)), (OrderSide::Short, true));
        assert_eq!(order_side(&o(Leg::Short, 1.0)), (OrderSide::Short, false));
        assert_eq!(order_side(&o(Leg::Short, -1.0)), (OrderSide::Long, true));
    }

    #[test]
    fn kill_levelling_in_dry_run_reads_the_simulated_other_leg() {
        let b = SymBook {
            dry_long_qty: 0.47,
            dry_short_qty: 0.42,
            ..SymBook::default()
        };
        assert_eq!(roll_dry_leg_qty(&b, Leg::Long), 0.47);
        assert_eq!(roll_dry_leg_qty(&b, Leg::Short), 0.42);
        // the simulated long covers the gap: levelled from the dry book
        assert_eq!(
            roll_kill_levelling(0.05, roll_dry_leg_qty(&b, Leg::Long), 0.0001, 4),
            Some(0.05)
        );
    }

    #[test]
    fn the_reopen_is_refused_on_a_price_move_or_when_it_no_longer_fits_the_caps() {
        let cfg = roll_cfg_on().roll;
        let rs = RollState::default();
        let r = |m: f64| roll_reserve(4.0 * m, 3);
        // within the margin and fits: sent
        assert_eq!(
            roll_reopen_decision(&cfg, &rs, 100.0, 100.05, r(100.05)),
            None
        );
        // beyond the margin either way: refused, whatever the caps
        assert_eq!(
            roll_reopen_decision(&cfg, &rs, 100.0, 100.2, r(100.2)),
            Some("reopen_price_moved")
        );
        assert_eq!(
            roll_reopen_decision(&cfg, &rs, 100.0, 99.8, r(99.8)),
            Some("reopen_price_moved")
        );
        // the fresh reservation no longer fits: refused
        let full = RollState {
            week_volume_usd: cfg.weekly_volume_usd - 100.0,
            ..RollState::default()
        };
        assert_eq!(
            roll_reopen_decision(&cfg, &full, 100.0, 100.05, r(100.05)),
            Some("reopen_capped")
        );
        let costly = RollState {
            week_cost_usd: cfg.weekly_cost_usd - 0.01,
            ..RollState::default()
        };
        assert_eq!(
            roll_reopen_decision(&cfg, &costly, 100.0, 100.05, r(100.05)),
            Some("reopen_capped")
        );
    }

    #[test]
    fn the_balanced_gate_needs_the_book_within_tolerance_now_and_after_the_close() {
        // net −0.01 BTC @ 80k = $800 > $500 now; closing the short 0.005
        // brings it to $400 — the post-close check alone would pass it
        assert!(roll_post_close_net_ok(
            -0.01,
            Leg::Short,
            0.005,
            80_000.0,
            500.0
        ));
        assert!(!roll_balanced(-0.01, Leg::Short, 0.005, 80_000.0, 500.0));
        // flat now, $400 after the close: rolls
        assert!(roll_balanced(0.0, Leg::Short, 0.005, 80_000.0, 500.0));
        // within now, beyond after the close: refused
        assert!(!roll_balanced(0.002, Leg::Short, 0.005, 80_000.0, 500.0));
    }

    #[test]
    fn a_roll_leg_is_reserved_only_right_before_its_send() {
        let mut rs = RollState::default();
        // a pre-send refusal (price guard / limit / touch) returned before
        // the reservation: finishing it changes nothing, so nothing persists
        assert!(!roll_finish_in_flight(&mut rs, "BTC", false, false));
        assert!(rs.in_flight.is_empty());
        // reserved right before the send, dropped after a non-uncertain Err
        let r = roll_reserve(400.0, 3);
        roll_reserve_for_send(&mut rs, "BTC", r);
        assert_eq!(roll_in_flight_total(&rs), r);
        assert!(roll_finish_in_flight(&mut rs, "BTC", false, false));
        assert!(rs.in_flight.is_empty());
        // kept while uncertain; a settled leg already dropped it at booking
        roll_reserve_for_send(&mut rs, "BTC", r);
        assert!(!roll_finish_in_flight(&mut rs, "BTC", false, true));
        assert_eq!(roll_in_flight_total(&rs), r);
        roll_settle_leg(&mut rs, "BTC", T0, None);
        assert!(!roll_finish_in_flight(&mut rs, "BTC", true, false));
        assert!(rs.in_flight.is_empty());
    }

    #[test]
    fn growth_is_refused_when_it_would_take_the_venue_under_the_liq_guard() {
        let mut cfg = cfg_for_test_multi_btc_meta();
        cfg.symbols[1].short_mmr_pct = 25.0;
        let s = snap(
            &[
                ("BTC", 0.1, -0.1, 80_000.0, 80_100.0),
                ("META", 0.0, 0.0, 750.0, 752.0),
            ],
            3_200.0,
            3_200.0,
        );
        let plan = vec![
            (
                "META".to_string(),
                vec![
                    Order {
                        leg: Leg::Long,
                        qty: 10.0,
                    },
                    Order {
                        leg: Leg::Short,
                        qty: 10.0,
                    },
                ],
            ),
            // a reduction adds nothing
            (
                "BTC".to_string(),
                vec![Order {
                    leg: Leg::Short,
                    qty: -0.05,
                }],
            ),
        ];
        let after = legs_after_growth(&plan, &s, &cfg, Leg::Short);
        assert!((after[0].0 - 8_010.0).abs() < 1e-9);
        assert!((after[1].0 - 7_520.0).abs() < 1e-9);
        assert_eq!(after[1].1, 25.0);
        // within the leverage cap (15.5k on 3.2k equity < 5x) ...
        let gross: f64 = after.iter().map(|l| l.0).sum();
        assert!(leverage_ok(gross, 3_200.0, cfg.max_leverage));
        // ... yet headroom (320000 − 9612 − 188000) / 15530 = 7.9 % < 8 %
        assert!(!growth_headroom_ok(3_200.0, &after, cfg.liq_guard_pct));
        // the long venue (1.2 % / 3 % MMR) keeps it: growth passes there
        let after_l = legs_after_growth(&plan, &s, &cfg, Leg::Long);
        assert!(growth_headroom_ok(3_200.0, &after_l, cfg.liq_guard_pct));
        // nothing held after growth: ok
        assert!(growth_headroom_ok(0.0, &[], cfg.liq_guard_pct));
        // roll off: the guard never applies (pre-roll planner unchanged)
        assert!(!growth_headroom_guard_applies(false, 5_000.0));
        assert!(growth_headroom_guard_applies(true, 5_000.0));
        assert!(!growth_headroom_guard_applies(true, 0.0));
    }

    #[test]
    fn a_disabled_roll_drops_carried_in_flight_reservations_at_startup() {
        let mut rs = RollState::default();
        rs.in_flight.insert("BTC".into(), (400.4, 0.3));
        rs.week_volume_usd = 1_000.0;
        // enabled: kept (the stale sweep books it)
        assert!(roll_drop_in_flight_when_disabled(&mut rs, true).is_empty());
        assert_eq!(rs.in_flight.len(), 1);
        // disabled: dropped unbooked, nothing else touched
        assert_eq!(
            roll_drop_in_flight_when_disabled(&mut rs, false),
            vec!["BTC".to_string()]
        );
        assert!(rs.in_flight.is_empty());
        assert_eq!(rs.week_volume_usd, 1_000.0);
        assert_eq!(roll_in_flight_total(&rs), (0.0, 0.0));
    }
}
