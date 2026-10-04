//! Env configuration (prefix `ARCUS_VOL_`). Defaults are the parameters
//! frozen on bot-strategy#1093; loosening one needs an owner decision
//! recorded there.

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use std::path::PathBuf;
use std::str::FromStr;

/// `ARCUS_VOL_LIVE_CONFIRM` must equal this for any live order.
pub const LIVE_CONFIRM_TOKEN: &str = "1093-G2";

/// Smallest re-peg band with an offset. The venue rounds a resting price
/// away from the touch by up to one tick, so a band narrower than the tick
/// (in bp of price) would see every placed quote as off target and requote
/// it each tick. 1 bp is far above the BTC tick (~0.012 bp); on a market
/// whose tick is 1 bp of price or more the band must be set above the tick.
pub const MIN_REPEG_BAND_BPS: Decimal = Decimal::ONE;

#[derive(Debug, Clone)]
pub struct Config {
    pub market: String,
    pub clip_usd: Decimal,
    pub skew_usd: Decimal,
    pub hard_cap_usd: Decimal,
    pub margin_usd: Decimal,
    pub leverage: u32,
    /// Quotes are sized so a fill keeps |inventory| ≤ effective cap × this
    /// (see `effective_cap`): with the frozen params the effective cap equals
    /// one clip, and a quote sized to land exactly on the cap would trip the
    /// cap flatten on every fill.
    pub cap_headroom: Decimal,
    pub max_hold_secs: u64,
    pub daily_stop_usd: Decimal,
    pub cum_stop_usd: Decimal,
    pub tick_ms: u64,
    pub shock_bps: Decimal,
    pub shock_window_secs: u64,
    pub cooldown_secs: u64,
    pub book_stale_secs: u64,
    pub dms_secs: u64,
    pub dms_refresh_secs: u64,
    pub reconcile_secs: u64,
    pub position_poll_secs: u64,
    pub taker_fee_bps: Decimal,
    pub maker_fee_bps: Decimal,
    pub qty_decimals: u32,
    pub min_quote_usd: Decimal,
    /// The venue's minimum order size / notional (Arcus SPY-USD: 0.001 and
    /// $5). Inventory below either is dust: it cannot be flattened (every
    /// IOC is rejected), so it is never flattened (`logic::is_dust`).
    /// `MIN_ORDER_QTY=0` turns the size rule off.
    pub min_order_qty: Decimal,
    pub min_order_usd: Decimal,
    pub flatten_slippage_bps: u32,
    /// How long a live fill with no reported fee waits before it is booked
    /// at the taker fee (`fee_estimated`).
    pub fee_wait_secs: u64,
    /// Presence quoting (bot-strategy#1093 option B): rest quotes this many
    /// bp behind the touch (0 = at the touch, the default). At an offset a
    /// quote fills only when a sweep reaches it, so volume is near zero;
    /// the point is to test whether resting quotes alone earn MM points.
    pub quote_offset_bps: Decimal,
    /// With an offset, keep a resting quote while its distance from the
    /// touch stays within offset ± this many bp; re-peg only outside.
    /// Must be >= 1 (`MIN_REPEG_BAND_BPS`) and < the offset. Ignored at
    /// offset 0.
    pub repeg_band_bps: Decimal,
    pub state_dir: PathBuf,
    pub dry_run: bool,
    pub live_confirm: String,
    /// `ARCUS_ACCOUNT_INDEX` (the connector's own env): live refuses 0.
    pub account_index: Option<u8>,
    pub ws_url: String,
    /// `ARCUS_ADDRESS` (the connector's own env), for the account lock.
    pub arcus_address: Option<String>,
    /// Account-lock namespace, independent of STATE_DIR.
    pub lock_dir: PathBuf,
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn dec(name: &str, default: &str) -> Result<Decimal> {
    let raw = var(name).unwrap_or_else(|| default.to_string());
    Decimal::from_str(raw.trim()).with_context(|| format!("{name}={raw} is not a decimal"))
}

fn int<T: FromStr>(name: &str, default: T) -> Result<T> {
    match var(name) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("{name}={raw} is not an integer")),
    }
}

/// Strict boolean: only true/false/1/0/yes/no (trimmed, any case); unset
/// means `default`. Anything else is a startup error, so a typo in
/// `ARCUS_VOL_DRY_RUN` can never select live (Codex P1, pairtrade#361).
pub fn parse_bool(name: &str, raw: Option<&str>, default: bool) -> Result<bool> {
    let Some(raw) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(default);
    };
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => bail!("{name}={raw} is not a boolean (use true/false/1/0/yes/no)"),
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let p = |k: &str| format!("ARCUS_VOL_{k}");
        let cfg = Config {
            market: var(&p("MARKET")).unwrap_or_else(|| "BTC-USD".to_string()),
            clip_usd: dec(&p("CLIP_USD"), "10000")?,
            skew_usd: dec(&p("SKEW_USD"), "10000")?,
            hard_cap_usd: dec(&p("HARD_CAP_USD"), "30000")?,
            margin_usd: dec(&p("MARGIN_USD"), "2000")?,
            leverage: int(&p("LEVERAGE"), 5u32)?,
            cap_headroom: dec(&p("CAP_HEADROOM"), "0.95")?,
            max_hold_secs: int(&p("MAX_HOLD_SECS"), 300u64)?,
            daily_stop_usd: dec(&p("DAILY_STOP_USD"), "50")?,
            cum_stop_usd: dec(&p("CUM_STOP_USD"), "250")?,
            tick_ms: int(&p("TICK_MS"), 500u64)?,
            shock_bps: dec(&p("SHOCK_BPS"), "5")?,
            shock_window_secs: int(&p("SHOCK_WINDOW_SECS"), 2u64)?,
            cooldown_secs: int(&p("COOLDOWN_SECS"), 10u64)?,
            book_stale_secs: int(&p("BOOK_STALE_SECS"), 5u64)?,
            dms_secs: int(&p("DMS_SECS"), 30u64)?,
            dms_refresh_secs: int(&p("DMS_REFRESH_SECS"), 10u64)?,
            reconcile_secs: int(&p("RECONCILE_SECS"), 15u64)?,
            position_poll_secs: int(&p("POSITION_POLL_SECS"), 5u64)?,
            taker_fee_bps: dec(&p("TAKER_FEE_BPS"), "2.25")?,
            maker_fee_bps: dec(&p("MAKER_FEE_BPS"), "0")?,
            qty_decimals: int(&p("QTY_DECIMALS"), 5u32)?,
            min_quote_usd: dec(&p("MIN_QUOTE_USD"), "50")?,
            min_order_qty: dec(&p("MIN_ORDER_QTY"), "0")?,
            min_order_usd: dec(&p("MIN_ORDER_USD"), "5")?,
            flatten_slippage_bps: int(&p("FLATTEN_SLIPPAGE_BPS"), 20u32)?,
            fee_wait_secs: int(&p("FEE_WAIT_SECS"), 30u64)?,
            quote_offset_bps: dec(&p("QUOTE_OFFSET_BPS"), "0")?,
            repeg_band_bps: dec(&p("REPEG_BAND_BPS"), "0")?,
            state_dir: PathBuf::from(
                var(&p("STATE_DIR")).unwrap_or_else(|| "/opt/debot/arcus_vol".to_string()),
            ),
            dry_run: parse_bool(
                &p("DRY_RUN"),
                std::env::var(p("DRY_RUN")).ok().as_deref(),
                true,
            )?,
            live_confirm: var(&p("LIVE_CONFIRM")).unwrap_or_default(),
            account_index: match var("ARCUS_ACCOUNT_INDEX") {
                None => None,
                Some(raw) => Some(
                    raw.trim()
                        .parse()
                        .with_context(|| format!("ARCUS_ACCOUNT_INDEX={raw}"))?,
                ),
            },
            ws_url: var("ARCUS_WEBSOCKET_ENDPOINT")
                .unwrap_or_else(|| "wss://api.arcus.xyz/v1/ws".to_string()),
            arcus_address: var("ARCUS_ADDRESS"),
            lock_dir: PathBuf::from(var(&p("LOCK_DIR")).unwrap_or_else(|| {
                format!(
                    "{}/.local/state/arcus_vol/locks",
                    var("HOME").unwrap_or_else(|| "/tmp".to_string())
                )
            })),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        let positive = [
            ("CLIP_USD", self.clip_usd),
            ("SKEW_USD", self.skew_usd),
            ("HARD_CAP_USD", self.hard_cap_usd),
            ("MARGIN_USD", self.margin_usd),
            ("DAILY_STOP_USD", self.daily_stop_usd),
            ("CUM_STOP_USD", self.cum_stop_usd),
            ("SHOCK_BPS", self.shock_bps),
        ];
        for (name, v) in positive {
            if v <= Decimal::ZERO {
                bail!("ARCUS_VOL_{name} must be > 0 (got {v})");
            }
        }
        if self.leverage == 0 || self.tick_ms == 0 {
            bail!("ARCUS_VOL_LEVERAGE and ARCUS_VOL_TICK_MS must be > 0");
        }
        if self.cap_headroom <= Decimal::ZERO || self.cap_headroom > Decimal::ONE {
            bail!("ARCUS_VOL_CAP_HEADROOM must be in (0, 1]");
        }
        if self.dms_secs < 5 || self.dms_secs > 300 || self.dms_refresh_secs >= self.dms_secs {
            bail!("ARCUS_VOL_DMS_SECS must be 5..=300 and above DMS_REFRESH_SECS");
        }
        self.validate_presence()
    }

    /// Presence-quoting parameters. With an offset the band must be at least
    /// `MIN_REPEG_BAND_BPS` (a zero or sub-tick band re-pegs on every tick:
    /// no "consistent" quote, only cancel/place churn) and smaller than the
    /// offset (a quote that is kept can never sit at or through the touch).
    fn validate_presence(&self) -> Result<()> {
        let hundred = Decimal::ONE_HUNDRED;
        if self.quote_offset_bps < Decimal::ZERO || self.quote_offset_bps > hundred {
            bail!(
                "ARCUS_VOL_QUOTE_OFFSET_BPS must be in 0..=100 (got {})",
                self.quote_offset_bps
            );
        }
        if self.repeg_band_bps < Decimal::ZERO {
            bail!(
                "ARCUS_VOL_REPEG_BAND_BPS must be >= 0 (got {})",
                self.repeg_band_bps
            );
        }
        if self.quote_offset_bps > Decimal::ZERO
            && (self.repeg_band_bps < MIN_REPEG_BAND_BPS
                || self.repeg_band_bps >= self.quote_offset_bps)
        {
            bail!(
                "with ARCUS_VOL_QUOTE_OFFSET_BPS={} ARCUS_VOL_REPEG_BAND_BPS must be >= {MIN_REPEG_BAND_BPS} and < the offset (got {})",
                self.quote_offset_bps,
                self.repeg_band_bps
            );
        }
        Ok(())
    }

    /// The cap actually enforced: the frozen hard cap, or what the allocated
    /// margin supports at the configured leverage, whichever is smaller.
    pub fn effective_cap_usd(&self) -> Decimal {
        self.hard_cap_usd
            .min(self.margin_usd * Decimal::from(self.leverage))
    }

    /// The cap quotes are sized against (see `cap_headroom`).
    pub fn quote_cap_usd(&self) -> Decimal {
        self.effective_cap_usd() * self.cap_headroom
    }

    pub fn presence(&self) -> crate::logic::Presence {
        crate::logic::Presence {
            offset_bps: self.quote_offset_bps,
            band_bps: self.repeg_band_bps,
        }
    }

    pub fn mode(&self) -> &'static str {
        if self.dry_run {
            "dry_run"
        } else {
            "live"
        }
    }
}

/// Live needs all three: DRY_RUN off, the confirm token, and a dedicated
/// non-zero subaccount (subaccount 0 holds the owner's manual trading).
/// Exact value of `ARCUS_VOL_ALLOW_ACCOUNT0` that lets live run on subaccount
/// 0, which is shared with the owner's manual trading (owner decision
/// 2026-10-01, bot-strategy#1093).
pub const ALLOW_ACCOUNT0_TOKEN: &str = "I-understand-shared-account";

pub fn live_gate(
    dry_run: bool,
    confirm: &str,
    account_index: Option<u8>,
    allow_account0: &str,
) -> Result<(), String> {
    if dry_run {
        return Ok(());
    }
    if confirm != LIVE_CONFIRM_TOKEN {
        return Err(format!(
            "ARCUS_VOL_DRY_RUN=false needs ARCUS_VOL_LIVE_CONFIRM={LIVE_CONFIRM_TOKEN}"
        ));
    }
    match account_index {
        // Arcus subaccounts are 0..=9; 0 holds the owner's manual trading.
        Some(i) if (1..=9).contains(&i) => Ok(()),
        Some(0) if allow_account0 == ALLOW_ACCOUNT0_TOKEN => Ok(()),
        other => Err(format!(
            "live needs a dedicated Arcus subaccount: ARCUS_ACCOUNT_INDEX must be in 1..=9 (got {}); \
             subaccount 0 is shared with manual trading and needs ARCUS_VOL_ALLOW_ACCOUNT0={ALLOW_ACCOUNT0_TOKEN}",
            other.map_or_else(|| "unset".to_string(), |i| i.to_string())
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_gate_needs_token_and_nonzero_subaccount() {
        assert!(live_gate(true, "", None, "").is_ok());
        assert!(live_gate(false, "", Some(1), "").is_err());
        assert!(live_gate(false, "1093", Some(1), "").is_err());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, None, "").is_err());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(0), "").is_err());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(3), "").is_ok());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(1), "").is_ok());
        // Subaccount 0 (shared with manual trading) only with the exact opt-in.
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(0), "yes").is_err());
        assert!(live_gate(
            false,
            LIVE_CONFIRM_TOKEN,
            Some(0),
            "i-understand-shared-account"
        )
        .is_err());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(0), ALLOW_ACCOUNT0_TOKEN).is_ok());
        assert!(live_gate(false, "", Some(0), ALLOW_ACCOUNT0_TOKEN).is_err());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(10), ALLOW_ACCOUNT0_TOKEN).is_err());
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(255), ALLOW_ACCOUNT0_TOKEN).is_err());
        let refused = live_gate(false, LIVE_CONFIRM_TOKEN, Some(0), "").unwrap_err();
        assert!(refused.contains("ARCUS_VOL_ALLOW_ACCOUNT0"), "{refused}");
        assert!(live_gate(false, LIVE_CONFIRM_TOKEN, Some(9), "").is_ok());
        for bad in [10u8, 42, 255] {
            let err = live_gate(false, LIVE_CONFIRM_TOKEN, Some(bad), "").unwrap_err();
            assert!(err.contains("1..=9"), "{err}");
        }
    }

    #[test]
    fn dry_run_parses_strictly_and_a_typo_is_an_error_not_live() {
        let n = "ARCUS_VOL_DRY_RUN";
        assert!(parse_bool(n, None, true).unwrap());
        assert!(parse_bool(n, Some("  "), true).unwrap());
        for t in ["true", "TRUE", " 1 ", "yes", "Yes"] {
            assert!(parse_bool(n, Some(t), false).unwrap(), "{t}");
        }
        for f in ["false", "False", "0", "no", " NO "] {
            assert!(!parse_bool(n, Some(f), true).unwrap(), "{f}");
        }
        for typo in ["flase", "treu", "off", "on", "2", "y", "n"] {
            assert!(parse_bool(n, Some(typo), true).is_err(), "{typo}");
        }
    }

    #[test]
    fn presence_params_are_validated() {
        let d = |s: &str| Decimal::from_str(s).unwrap();
        // Defaults (offset 0): at the touch, any non-negative band is ignored.
        let mut cfg = test_config();
        assert!(cfg.validate().is_ok());
        assert!(!cfg.presence().on());
        cfg.repeg_band_bps = d("3");
        assert!(cfg.validate().is_ok());
        // Offset 5 with band 2 is fine.
        cfg.quote_offset_bps = d("5");
        cfg.repeg_band_bps = d("2");
        assert!(cfg.validate().is_ok());
        assert!(cfg.presence().on());
        // Band 0 with an offset is refused (review #4): it would re-peg on
        // every tick of the touch. So is any band under 1 bp (review 3 #3):
        // narrower than the venue's rounding it would churn the same way.
        for bad in ["0", "0.01", "0.99"] {
            cfg.repeg_band_bps = d(bad);
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains("REPEG_BAND_BPS"), "band {bad}: {err}");
        }
        cfg.repeg_band_bps = d("1");
        assert!(cfg.validate().is_ok());
        // Band == offset or above is refused (a kept quote could reach the
        // touch); so is a negative band.
        for bad in ["5", "7", "-1"] {
            cfg.repeg_band_bps = d(bad);
            assert!(cfg.validate().is_err(), "band {bad}");
        }
        cfg.repeg_band_bps = d("2");
        // Offset range 0..=100.
        cfg.quote_offset_bps = d("100");
        assert!(cfg.validate().is_ok());
        cfg.quote_offset_bps = d("100.5");
        assert!(cfg.validate().is_err());
        cfg.quote_offset_bps = d("-0.1");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn effective_cap_is_the_smaller_of_hard_cap_and_margin_times_leverage() {
        let mut cfg = test_config();
        assert_eq!(cfg.effective_cap_usd(), Decimal::from(10_000));
        assert_eq!(cfg.quote_cap_usd(), Decimal::from(9_500));
        cfg.margin_usd = Decimal::from(10_000);
        assert_eq!(cfg.effective_cap_usd(), Decimal::from(30_000));
    }

    pub(crate) fn test_config() -> Config {
        Config {
            market: "BTC-USD".to_string(),
            clip_usd: Decimal::from(10_000),
            skew_usd: Decimal::from(10_000),
            hard_cap_usd: Decimal::from(30_000),
            margin_usd: Decimal::from(2_000),
            leverage: 5,
            cap_headroom: Decimal::from_str("0.95").unwrap(),
            max_hold_secs: 300,
            daily_stop_usd: Decimal::from(50),
            cum_stop_usd: Decimal::from(250),
            tick_ms: 500,
            shock_bps: Decimal::from(5),
            shock_window_secs: 2,
            cooldown_secs: 10,
            book_stale_secs: 5,
            dms_secs: 30,
            dms_refresh_secs: 10,
            reconcile_secs: 15,
            position_poll_secs: 5,
            taker_fee_bps: Decimal::from_str("2.25").unwrap(),
            maker_fee_bps: Decimal::ZERO,
            qty_decimals: 5,
            min_quote_usd: Decimal::from(50),
            min_order_qty: Decimal::ZERO,
            min_order_usd: Decimal::from(5),
            flatten_slippage_bps: 20,
            fee_wait_secs: 30,
            quote_offset_bps: Decimal::ZERO,
            repeg_band_bps: Decimal::ZERO,
            state_dir: PathBuf::from("/tmp/unused"),
            dry_run: true,
            live_confirm: String::new(),
            account_index: None,
            ws_url: String::new(),
            arcus_address: None,
            lock_dir: PathBuf::from("/tmp/unused"),
        }
    }
}
