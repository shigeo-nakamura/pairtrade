//! Resolved `risk:` config block and its YAML resolver, split out of
//! `config/mod.rs` (bot-strategy#502). `RiskConfig` / `DailyLossAction` are
//! re-exported from the parent so the public `config::RiskConfig` path is
//! unchanged.

use anyhow::{anyhow, Result};

use super::schema::RiskYaml;
use crate::pairtrade::defaults::*;

///
/// `max_daily_loss_bps` and `max_session_loss_bps` are stored as the raw
/// 1x-equivalent values from YAML; the comparison sites multiply by
/// `PairTradeConfig::max_leverage` at evaluation time so the gates stay
/// leverage-invariant (bot-strategy#185 amendment).
#[derive(Debug, Clone)]
pub struct RiskConfig {
    /// 1x-equivalent (market-move) bps; effective threshold at runtime is
    /// this × `PairTradeConfig::max_leverage`.
    pub max_daily_loss_bps: u32,
    pub max_daily_loss_action: DailyLossAction,
    pub daily_reset_utc_hour: u32,
    /// Phase 3-1: 1x-equivalent (market-move) bps; effective threshold at
    /// runtime is this × `PairTradeConfig::max_leverage`. 0 = disabled.
    pub max_session_loss_bps: u32,
    /// Phase 3-1: rolling peak window in seconds.
    pub session_dd_lookback_secs: u64,
    /// Phase 3-1: equity sampling cadence in seconds.
    pub session_dd_sample_secs: u64,
    /// bot-strategy#575 ①: minimum unexplained equity jump (USD), observed
    /// while flat and settled, that rebaselines the rolling session-DD peak
    /// to the new equity. 0.0 = disabled.
    pub session_dd_capital_event_min_usd: f64,
    /// bot-strategy#575 ①: continuous-flat dwell (seconds) required before
    /// capital-event detection trusts the equity reading.
    pub session_dd_capital_settle_secs: u64,
    /// Phase 3-4: per-leg notional cap multiplier. 0.0 = disabled.
    /// Resolved cap = `equity_reference_usd × max_leverage × headroom`
    /// at sizing time, so the dollar threshold tracks per-instance
    /// equity and per-host leverage automatically.
    pub max_notional_headroom: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DailyLossAction {
    /// Block new entries only, let existing positions exit normally.
    Block,
    // Note: `Flatten` (force-close on threshold trip) is Phase 3. The
    // YAML parser rejects the value today so operators cannot set it
    // expecting Phase-2 behaviour to match.
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_daily_loss_bps: DEFAULT_MAX_DAILY_LOSS_BPS,
            max_daily_loss_action: DailyLossAction::Block,
            daily_reset_utc_hour: DEFAULT_DAILY_RESET_UTC_HOUR,
            max_session_loss_bps: DEFAULT_MAX_SESSION_LOSS_BPS,
            session_dd_lookback_secs: DEFAULT_SESSION_DD_LOOKBACK_SECS,
            session_dd_sample_secs: DEFAULT_SESSION_DD_SAMPLE_SECS,
            session_dd_capital_event_min_usd: DEFAULT_SESSION_DD_CAPITAL_EVENT_MIN_USD,
            session_dd_capital_settle_secs: DEFAULT_SESSION_DD_CAPITAL_SETTLE_SECS,
            max_notional_headroom: DEFAULT_MAX_NOTIONAL_HEADROOM,
        }
    }
}

pub(super) fn resolve_risk_config(yaml: Option<&RiskYaml>) -> Result<RiskConfig> {
    let Some(y) = yaml else {
        return Ok(RiskConfig::default());
    };
    let action = match y.max_daily_loss_action.as_deref().map(str::trim) {
        None | Some("") | Some("block") => DailyLossAction::Block,
        Some("flatten") => {
            return Err(anyhow!(
                "risk.max_daily_loss_action=flatten is not implemented yet (Phase 3); use 'block'"
            ));
        }
        Some(other) => {
            return Err(anyhow!(
                "risk.max_daily_loss_action: unknown value '{}'",
                other
            ));
        }
    };
    let max_notional_headroom = y
        .max_notional_headroom
        .unwrap_or(DEFAULT_MAX_NOTIONAL_HEADROOM);
    if max_notional_headroom < 0.0 || !max_notional_headroom.is_finite() {
        return Err(anyhow!(
            "risk.max_notional_headroom must be ≥ 0 and finite (got {})",
            max_notional_headroom
        ));
    }
    // 10x is well above any sane belt-and-suspenders multiplier (typical
    // 1.0–1.2) — flag config drift early. Anything above this is almost
    // certainly someone misreading the field as an absolute USD value.
    if max_notional_headroom > 10.0 {
        return Err(anyhow!(
            "risk.max_notional_headroom={} looks like an absolute USD value; \
             this field is a multiplier of equity_reference_usd × max_leverage \
             (typical 1.0–1.2)",
            max_notional_headroom
        ));
    }
    let sample_secs = y
        .session_dd_sample_secs
        .unwrap_or(DEFAULT_SESSION_DD_SAMPLE_SECS);
    if sample_secs == 0 {
        return Err(anyhow!("risk.session_dd_sample_secs must be > 0"));
    }
    let lookback_secs = y
        .session_dd_lookback_secs
        .unwrap_or(DEFAULT_SESSION_DD_LOOKBACK_SECS);
    if lookback_secs < sample_secs {
        return Err(anyhow!(
            "risk.session_dd_lookback_secs ({}) must be ≥ session_dd_sample_secs ({})",
            lookback_secs,
            sample_secs
        ));
    }
    let session_dd_capital_event_min_usd = y
        .session_dd_capital_event_min_usd
        .unwrap_or(DEFAULT_SESSION_DD_CAPITAL_EVENT_MIN_USD);
    if session_dd_capital_event_min_usd < 0.0 || !session_dd_capital_event_min_usd.is_finite() {
        return Err(anyhow!(
            "risk.session_dd_capital_event_min_usd must be ≥ 0 and finite (got {})",
            session_dd_capital_event_min_usd
        ));
    }
    let session_dd_capital_settle_secs = y
        .session_dd_capital_settle_secs
        .unwrap_or(DEFAULT_SESSION_DD_CAPITAL_SETTLE_SECS);
    let max_daily_loss_bps = y.max_daily_loss_bps.unwrap_or(DEFAULT_MAX_DAILY_LOSS_BPS);
    let max_session_loss_bps = y
        .max_session_loss_bps
        .unwrap_or(DEFAULT_MAX_SESSION_LOSS_BPS);
    // Both bps fields are interpreted as 1x-equivalent market-move bps and
    // multiplied by `max_leverage` at comparison time. Typical values are
    // 100–500 bps (1–5% of equivalent 1x equity). Anything materially above
    // that is almost certainly a leftover from the pre-leverage-neutral
    // schema where operators rescaled by leverage manually (e.g. 300 × 5 =
    // 1500). Flag those as a parse-time warning so the operator notices
    // before deploy.
    if max_daily_loss_bps > 1000 {
        log::warn!(
            "risk.max_daily_loss_bps={} is unusually high; the field is now leverage-invariant \
             (multiplied by max_leverage internally), so typical values are 100–500 bps. \
             Did you copy a pre-amendment leverage-aware value? See bot-strategy#185.",
            max_daily_loss_bps
        );
    }
    if max_session_loss_bps > 1000 {
        log::warn!(
            "risk.max_session_loss_bps={} is unusually high; the field is now leverage-invariant \
             (multiplied by max_leverage internally), so typical values are 100–500 bps. \
             Did you copy a pre-amendment leverage-aware value? See bot-strategy#185.",
            max_session_loss_bps
        );
    }
    Ok(RiskConfig {
        max_daily_loss_bps,
        max_daily_loss_action: action,
        daily_reset_utc_hour: y
            .daily_reset_utc_hour
            .unwrap_or(DEFAULT_DAILY_RESET_UTC_HOUR),
        max_session_loss_bps,
        session_dd_lookback_secs: lookback_secs,
        session_dd_sample_secs: sample_secs,
        session_dd_capital_event_min_usd,
        session_dd_capital_settle_secs,
        max_notional_headroom,
    })
}
