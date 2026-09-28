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
//!    notional after the order would exceed `HEDGE_MAX_LEVERAGE` × equity.
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
                std::env::var(format!("LIGHTER_ACCOUNT_INDEX_{suffix}"))
                    .ok()
                    .and_then(|v| v.trim().parse().ok())
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
    sent_at: u64,
    reason: String,
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

/// An uncertain IOC may be released once the venue has had
/// `grace_secs` to finish it (an IOC lives milliseconds on the matching
/// engine; what is slow is only its visibility) and it is no longer open.
/// With no order id the whole symbol must show no open order.
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

    /// One taker IOC on one leg of one symbol; returns the size the venue
    /// reports filled (position delta), which is what the book is
    /// re-planned from.
    async fn execute(&mut self, symbol: &str, order: &Order, mark: f64) -> Result<f64> {
        let (venue, side, reduce_only) = match (order.leg, order.qty > 0.0) {
            (Leg::Long, true) => (&self.long, OrderSide::Long, false),
            (Leg::Long, false) => (&self.long, OrderSide::Short, true),
            (Leg::Short, true) => (&self.short, OrderSide::Short, false),
            (Leg::Short, false) => (&self.short, OrderSide::Long, true),
        };
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
                "price": mark, "dry_run": true }),
            );
            self.persist();
            return Ok(qty);
        }
        let before = venue.signed_qty(symbol).await?;
        let size = decimal(qty, "qty")?.round_dp(self.meta(symbol).size_decimals);
        let exchange = venue.kind;
        let resp = match venue
            .dex
            .create_order_taker_ioc(symbol, size, side, self.cfg.taker_slippage_bps, reduce_only)
            .await
        {
            Ok(r) => r,
            Err(DexError::ReconciliationRequired { detail, .. }) => {
                // The venue may have taken it: never re-send before the
                // position says what happened.
                let reason = format!("submission ambiguous: {detail}");
                self.mark_uncertain(symbol, order.leg, exchange, None, qty, before, &reason);
                bail!(
                    "{} IOC {side} {symbol} {size}: {reason}",
                    self.venue(order.leg).name
                );
            }
            Err(e) => bail!("{} IOC {side} {symbol} {size}: {e:?}", venue.name),
        };
        let order_id = resp.order_id.clone();
        let filled = if exchange == VenueKind::Arcus {
            // Arcus acks with 202: the fill is final only once the order is
            // out of the open orders AND its fills (or its cancel) are
            // visible and agree with the position change.
            match self
                .settle_arcus(order.leg, symbol, &order_id, qty, before)
                .await
            {
                Some(f) => f,
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
            filled
        };
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
            "limit": resp.ordered_price.to_string(), "order_id": resp.order_id, "mark": mark }),
        );
        Ok(filled)
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
            sent_at: now_secs(),
            reason: reason.to_string(),
        };
        self.event(
            "uncertain",
            serde_json::json!({ "symbol": symbol, "order": u }),
        );
        self.state.uncertain.insert(symbol.to_string(), u);
        self.persist();
    }

    /// One read of an Arcus order's state: (open, canceled, fills sum,
    /// trade ids of those fills). `None` when a read failed.
    async fn arcus_order_view(
        &self,
        leg: Leg,
        symbol: &str,
        order_id: &str,
    ) -> Option<(bool, bool, f64, Vec<String>)> {
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
        let mut sum = 0.0;
        let mut trades = Vec::new();
        for f in dex.get_filled_orders(symbol).await.ok()?.orders {
            if f.order_id == order_id {
                sum += f.filled_size.and_then(|d| d.to_f64()).unwrap_or(0.0).abs();
                trades.push(f.trade_id);
            }
        }
        Some((open, canceled, sum, trades))
    }

    /// Settle an Arcus IOC within `HEDGE_FILL_WAIT_SECS`: `Some(filled)`
    /// once terminal, `None` when it is still unknown after the last wait.
    async fn settle_arcus(
        &mut self,
        leg: Leg,
        symbol: &str,
        order_id: &str,
        qty: f64,
        before: f64,
    ) -> Option<f64> {
        let tol = self.size_tol(symbol);
        for &wait in &self.cfg.fill_wait_secs.clone() {
            tokio::time::sleep(Duration::from_secs(wait)).await;
            let Some((open, canceled, sum, trades)) =
                self.arcus_order_view(leg, symbol, order_id).await
            else {
                continue;
            };
            let Ok(after) = self.venue(leg).signed_qty(symbol).await else {
                continue;
            };
            if let Settle::Terminal(f) =
                settle_decision(qty, open, canceled, sum, (after - before).abs(), tol)
            {
                self.forget_arcus_activity(leg, symbol, order_id, &trades)
                    .await;
                return Some(f);
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
            if same_account {
                let dex = self.venue(u.leg).dex.clone();
                let Ok(open_orders) = dex.get_open_orders(&sym).await else {
                    continue;
                };
                let Ok(after) = self.venue(u.leg).signed_qty(&sym).await else {
                    continue;
                };
                delta = (after - u.before_qty).abs();
                match (&u.order_id, u.exchange.as_str()) {
                    // Arcus: the order's own fills / cancel must settle it.
                    (Some(id), "arcus") => {
                        if let Some((open, canceled, sum, trades)) =
                            self.arcus_order_view(u.leg, &sym, id).await
                        {
                            if let Settle::Terminal(_) = settle_decision(
                                u.qty,
                                open,
                                canceled,
                                sum,
                                delta,
                                self.size_tol(&sym),
                            ) {
                                self.forget_arcus_activity(u.leg, &sym, id, &trades).await;
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
                self.state.uncertain.clear();
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
            }
        }

        for sym in self.state.uncertain.keys() {
            log::warn!("[UNCERTAIN] {sym}: an order is still unreconciled — nothing sent on {sym}");
        }
        for sym in &released_now {
            log::info!(
                "[UNCERTAIN] {sym}: just reconciled — replanned next tick from a fresh snapshot"
            );
        }
        let plan = drop_uncertain(plan, &self.state.uncertain, &released_now);

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
                match self.execute(&sym, &order, mark).await {
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
        self.persist();
        self.write_status(now, kill, sent);
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
    let hedge_holder = serde_json::json!({
        "mode": state.overall_mode(),
        "halted": state.halted,
        "halt_reason": state.halt_reason,
        "uncertain_orders": state.uncertain,
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
                sent_at: 1,
                reason: "t".into(),
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
            sent_at: 1_000,
            reason: "t".into(),
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
                sent_at: 7,
                reason: "submission ambiguous".into(),
            },
        );
        let back: State = serde_json::from_str(&serde_json::to_string(&st).unwrap()).unwrap();
        assert_eq!(back.uncertain, st.uncertain);
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
}
