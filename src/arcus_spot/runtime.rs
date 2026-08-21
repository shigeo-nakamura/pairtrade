use super::{ArcusSpotInventory, ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode};
use chrono::{DateTime, Utc};
use dex_connector::{
    ArcusSpotCapture, ArcusSpotOverviewEntry, ArcusSpotRecorderSnapshot, ArcusSpotRoundTripRecord,
    ArcusSpotRouteObservation, ArcusSpotToken,
};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

const SUPPORTED_RECORDER_SCHEMA_VERSION: u32 = 3;
const PUBLIC_RECORDER_MODE: &str = "public_indicative_read_only";
const SIGNAL_FLAT_EPSILON: f64 = 1e-12;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotDirection {
    TokenAToTokenB,
    TokenBToTokenA,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotRegime {
    #[default]
    Neutral,
    RotatedAToB,
    RotatedBToA,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotRotationTrigger {
    EntrySignal,
    MeanReversionExit,
    MaxHoldExit,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotHoldCode {
    InvalidSnapshot,
    Warmup,
    NoSignal,
    RiskHalt,
    RouteUnavailable,
    StaleQuote,
    CostLimit,
    InventoryFloor,
    RotationLimit,
    InventoryImbalance,
    /// `collection_finished_at` was not strictly newer than
    /// `state.last_observation_at` -- see its doc comment.
    StaleOrDuplicateObservation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotHold {
    pub code: ArcusSpotHoldCode,
    pub detail: String,
}

impl ArcusSpotHold {
    fn new(code: ArcusSpotHoldCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotRiskHaltKind {
    DailyLoss,
    CumulativeLoss,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRiskHalt {
    pub kind: ArcusSpotRiskHaltKind,
    pub engaged_at: DateTime<Utc>,
    pub equity_usd: Decimal,
    pub loss_usd: Decimal,
    pub limit_usd: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRiskMark {
    pub equity_usd: Decimal,
    /// Loss attributable to rotating, for the current UTC day: how far
    /// actual equity sits below the day's opening basket re-priced at this
    /// tick. Zero while the bot has not traded, whatever prices did.
    pub daily_loss_usd: Decimal,
    /// The same measure taken against the basket held at probe start.
    pub cumulative_loss_usd: Decimal,
    /// How much the starting basket itself is down on price alone. Reported
    /// for visibility and never compared against a limit — see `risk_mark`.
    #[serde(default)]
    pub inventory_drawdown_usd: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRotationPlan {
    pub direction: ArcusSpotDirection,
    pub trigger: ArcusSpotRotationTrigger,
    pub sell_symbol: String,
    pub buy_symbol: String,
    /// The ERC-20 contract addresses `sell_symbol`/`buy_symbol` resolved
    /// to when this plan was built. A symbol registry can, legitimately
    /// or through compromise/misconfiguration, resolve the same symbol to
    /// a different contract by execution time; without pinning the
    /// address the operator actually approved, execution would sign and
    /// settle against whatever the *fresh* quote resolves to, with
    /// nothing to compare it against (Codex P1 follow-up, pairtrade#181).
    pub sell_token_address: String,
    pub buy_token_address: String,
    pub sell_quantity: Decimal,
    pub buy_quantity: Decimal,
    pub sell_amount_raw: String,
    pub buy_amount_raw: String,
    pub venue: String,
    pub quote_received_at: DateTime<Utc>,
    pub optimistic_round_trip_loss_bps: Decimal,
    pub gas_buffer_bps: Decimal,
    pub settlement_buffer_bps: Decimal,
    pub all_in_round_trip_cost_bps: Decimal,
    pub predicted_inventory: ArcusSpotInventory,
    pub predicted_inventory_imbalance_fraction: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ArcusSpotDecision {
    Observe { hold: ArcusSpotHold },
    WouldRotate { plan: ArcusSpotRotationPlan },
    SimulatedFill { plan: ArcusSpotRotationPlan },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRuntimeEvent {
    pub sequence: u64,
    pub observed_at: DateTime<Utc>,
    pub pair: String,
    pub mode: ArcusSpotRuntimeMode,
    pub token_a_reference_price_usd: Option<Decimal>,
    pub token_b_reference_price_usd: Option<Decimal>,
    pub relative_log_price: Option<f64>,
    pub z_score: Option<f64>,
    pub inventory_before: ArcusSpotInventory,
    pub inventory_after: ArcusSpotInventory,
    pub regime_before: ArcusSpotRegime,
    pub regime_after: ArcusSpotRegime,
    pub risk_before: Option<ArcusSpotRiskMark>,
    pub risk_after: Option<ArcusSpotRiskMark>,
    pub decision: ArcusSpotDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRuntimeState {
    pub sequence: u64,
    pub inventory: ArcusSpotInventory,
    pub regime: ArcusSpotRegime,
    pub relative_log_price_history: Vec<f64>,
    /// Reference prices from the last structurally valid observation. These
    /// make the absolute scale behind the relative-log-price signal durable,
    /// so restart/rollback acceptance can independently reapply configured
    /// USD-notional sizing instead of trusting a pending plan's quantities.
    #[serde(default)]
    pub last_token_a_reference_price_usd: Option<Decimal>,
    #[serde(default)]
    pub last_token_b_reference_price_usd: Option<Decimal>,
    /// `collection_finished_at` of the last snapshot `step_at` genuinely
    /// advanced on (not one it recognized as a repeat of this same field,
    /// and not one it rejected as structurally invalid before validating
    /// its own timestamps -- see `step_at`'s use of this field for both).
    /// Tracked here, in the checkpointed state itself, rather than by any
    /// individual caller: every writer of a shared checkpoint --
    /// `arcus-spot-execute-once`'s `live-tick` and `arcus-spot-propose-plan`
    /// alike -- calls `step_at`, and only a check inside it, under whichever
    /// lock the caller already holds around load/step_at/persist, can
    /// correctly order concurrent writers against each other. A per-caller
    /// sidecar cannot: one writer's fresher snapshot can be persisted while
    /// a second, slower writer is still fetching an older one, and that
    /// second writer's own bookkeeping would have no way to know the
    /// checkpoint had already moved past it (Codex P2 follow-up,
    /// pairtrade#186). `None` for a pre-existing checkpoint that predates
    /// this field -- the first `step_at` call after upgrade is never
    /// treated as a repeat.
    #[serde(default)]
    pub last_observation_at: Option<DateTime<Utc>>,
    pub last_rotation_at: Option<DateTime<Utc>>,
    /// Quantity of the currently-held (bought) token still open from the
    /// entry that produced the current non-Neutral `regime`, denominated in
    /// that token. `None` while `Neutral`. An exit's route offers whatever
    /// quantity the recorder's fixed-notional quote happens to propose,
    /// which is independent of what was actually acquired at entry, so this
    /// bounds each exit to the still-open amount and lets a partial fill
    /// keep the regime rotated instead of being declared closed early.
    pub rotated_quantity: Option<Decimal>,
    pub initial_equity_usd: Option<Decimal>,
    /// Inventory held when `initial_equity_usd` was first marked, i.e. the
    /// basket the cumulative loss stop measures the strategy against.
    ///
    /// Both loss stops ask "how much has *rotating* cost us", not "how much
    /// is the inventory worth". Marking equity against a fixed dollar
    /// baseline conflates the two: this bot pre-funds both legs and has no
    /// native short on Spot, so it carries their beta whether or not it ever
    /// trades, and a routine adverse day in the underlying names drains a
    /// budget meant to catch the *strategy* losing money. bot-strategy#813
    /// was filed after a $2 daily stop halted the probe on a 4.1% NVDA/AMD
    /// down day without a single swap having been made.
    ///
    /// Re-marking this basket at each tick's prices instead gives a
    /// buy-and-hold counterfactual: hold what we started with, do nothing.
    /// The gap between that and actual equity is exactly what rotating
    /// added or destroyed, and it is identically zero while the bot has not
    /// traded, at any price. Halting on beta was never protective anyway --
    /// stopping rotation does not shed inventory, so the exposure is the
    /// same halted or not, and shedding it is an operator decision.
    ///
    /// `None` on a checkpoint written before this field existed; seeded on
    /// the next tick (see `update_risk_baselines`).
    #[serde(default)]
    pub initial_baseline_inventory: Option<ArcusSpotInventory>,
    pub daily_baseline_day: Option<String>,
    pub daily_baseline_equity_usd: Option<Decimal>,
    /// Inventory held at the day's opening mark -- the daily counterpart of
    /// `initial_baseline_inventory`, and what `daily_loss_usd` is measured
    /// against. `None` on a pre-existing checkpoint; seeded on the next tick.
    #[serde(default)]
    pub daily_baseline_inventory: Option<ArcusSpotInventory>,
    /// Equity as of the most recently evaluated snapshot, updated on every
    /// tick regardless of day boundary. `daily_baseline_equity_usd` is
    /// fixed at the day's *opening* mark, so on the first tick of a new
    /// day it is a full day stale by the time of rollover; assessing the
    /// overnight gap against it instead of this field can miss a loss that
    /// occurs after an intraday gain (e.g. day opens $100, rises to $110,
    /// then drops to $105 by the next day's open — a real $5 overnight
    /// loss from the $110 peak that a $100-baseline comparison reports as
    /// a gain). See `risk_mark`'s overnight-gap handling.
    pub last_equity_usd: Option<Decimal>,
    pub risk_halt: Option<ArcusSpotRiskHalt>,
    #[cfg(feature = "arcus-spot-live")]
    #[serde(default)]
    pub last_live_execution_idempotency_key: Option<String>,
}

impl ArcusSpotRuntimeState {
    fn new(inventory: ArcusSpotInventory) -> Self {
        Self {
            sequence: 0,
            inventory,
            regime: ArcusSpotRegime::Neutral,
            relative_log_price_history: Vec::new(),
            last_token_a_reference_price_usd: None,
            last_token_b_reference_price_usd: None,
            last_observation_at: None,
            last_rotation_at: None,
            rotated_quantity: None,
            initial_equity_usd: None,
            initial_baseline_inventory: None,
            daily_baseline_day: None,
            daily_baseline_equity_usd: None,
            daily_baseline_inventory: None,
            last_equity_usd: None,
            risk_halt: None,
            #[cfg(feature = "arcus-spot-live")]
            last_live_execution_idempotency_key: None,
        }
    }
}

pub struct ArcusSpotRuntime {
    config: ArcusSpotRuntimeConfig,
    state: ArcusSpotRuntimeState,
}

/// The subset of `SnapshotContext` that depends only on token metadata and
/// reference prices, never on the recorder row's route data. Mark-to-market
/// equity valuation and risk-halt engagement only need this, and must run
/// even when the round-trip row itself is unavailable, errored, or stale:
/// a loss-limit breach during a route outage would otherwise never engage
/// the (sticky) halt if the route recovers before the next valid snapshot.
struct PriceContext {
    token_a: ArcusSpotToken,
    token_b: ArcusSpotToken,
    token_a_price_usd: Decimal,
    token_b_price_usd: Decimal,
}

struct SnapshotContext {
    token_a: ArcusSpotToken,
    token_b: ArcusSpotToken,
    token_a_price_usd: Decimal,
    token_b_price_usd: Decimal,
    row: ArcusSpotRoundTripRecord,
    /// Round-trip cost in bps. For an entry (or any tick with no exit
    /// executing), independently recomputed from the forward and reverse
    /// recommended quote amounts and cross-checked against the recorded
    /// `optimistic_round_trip_loss_bps`/`optimistic_return_amount` fields --
    /// `build_plan`'s entry cost gate must use this instead of the row's
    /// self-reported string, since a row whose two legs do not actually
    /// chain (reverse sellAmount != forward buyAmount) can report an
    /// arbitrarily cheap loss while the real recommended amounts imply a
    /// much larger one. For an exit, the cost gate does not apply (see
    /// `build_plan`), so this is simply the row's as-reported value: exits
    /// only validate the leg they actually execute, and cross-checking it
    /// against the unused leg would reintroduce the staleness-blocks-exits
    /// problem `snapshot_context` exists to avoid.
    verified_round_trip_loss_bps: Decimal,
}

struct RuntimeEventInput {
    sequence: u64,
    observed_at: DateTime<Utc>,
    inventory_before: ArcusSpotInventory,
    regime_before: ArcusSpotRegime,
    token_a_reference_price_usd: Option<Decimal>,
    token_b_reference_price_usd: Option<Decimal>,
    relative_log_price: Option<f64>,
    z_score: Option<f64>,
    risk_before: Option<ArcusSpotRiskMark>,
    decision: ArcusSpotDecision,
}

impl ArcusSpotRuntime {
    pub fn new(mut config: ArcusSpotRuntimeConfig) -> Result<Self, String> {
        config.normalize();
        config.validate()?;
        let state = ArcusSpotRuntimeState::new(config.initial_inventory);
        Ok(Self { config, state })
    }

    pub fn from_state(
        mut config: ArcusSpotRuntimeConfig,
        state: ArcusSpotRuntimeState,
    ) -> Result<Self, String> {
        config.normalize();
        config.validate()?;
        if state.inventory.token_a < config.inventory_floors.token_a
            || state.inventory.token_b < config.inventory_floors.token_b
        {
            return Err("restored Arcus inventory is below a configured floor".to_string());
        }
        if state
            .relative_log_price_history
            .iter()
            .any(|value| !value.is_finite())
        {
            return Err("restored Arcus price history contains a non-finite value".to_string());
        }
        match (
            state.last_token_a_reference_price_usd,
            state.last_token_b_reference_price_usd,
        ) {
            (None, None) => {}
            (Some(token_a), Some(token_b))
                if token_a > Decimal::ZERO && token_b > Decimal::ZERO => {}
            (Some(_), Some(_)) => {
                return Err("restored Arcus reference prices must be positive".to_string())
            }
            _ => return Err("restored Arcus reference prices must be present together".to_string()),
        }
        match state.regime {
            ArcusSpotRegime::Neutral if state.rotated_quantity.is_some() => {
                return Err("neutral restored Arcus state has a rotated quantity".to_string())
            }
            ArcusSpotRegime::RotatedAToB | ArcusSpotRegime::RotatedBToA => {
                if state
                    .rotated_quantity
                    .is_none_or(|quantity| quantity <= Decimal::ZERO)
                    || state.last_rotation_at.is_none()
                {
                    return Err(
                        "rotated restored Arcus state lacks a positive quantity or timestamp"
                            .to_string(),
                    );
                }
            }
            ArcusSpotRegime::Neutral => {}
        }
        Ok(Self { config, state })
    }

    pub fn config(&self) -> &ArcusSpotRuntimeConfig {
        &self.config
    }

    pub fn state(&self) -> &ArcusSpotRuntimeState {
        &self.state
    }

    /// The risk mark implied by the state as it stands, valued at the marks
    /// of the last observation it processed. `None` before any observation
    /// has supplied reference prices, or if the valuation overflows.
    ///
    /// For an operator asking "does the condition that halted this bot still
    /// hold" without waiting for, or fabricating, a fresh tick. Deliberately
    /// routed through the same `risk_mark` the halt itself uses so the answer
    /// cannot drift from the rule.
    pub fn last_risk_mark(&self) -> Option<ArcusSpotRiskMark> {
        let price_a = self.state.last_token_a_reference_price_usd?;
        let price_b = self.state.last_token_b_reference_price_usd?;
        let equity = self.state.inventory.checked_value_usd(price_a, price_b)?;
        Some(self.risk_mark(equity, price_a, price_b))
    }

    /// Disarm an engaged risk halt, returning what was cleared.
    ///
    /// Never called by the runtime itself: a halt is sticky by design, and
    /// nothing about a later tick is evidence that whatever caused it was
    /// handled. Only an operator, through the explicit `clear-risk-halt`
    /// command, decides that.
    ///
    /// Refuses while the halt's own condition still holds, so this cannot
    /// become a way to trade *through* a live breach rather than resume
    /// after one: the next tick would re-engage, a clean result would read
    /// as handled, and repeating would walk straight past the limit. The
    /// check lives here rather than in the calling command so that every
    /// caller gets it -- a future tool or test reaching for this method
    /// would otherwise have been able to lift a halt unconditionally
    /// (review of pairtrade#212).
    ///
    /// The daily basket is deliberately left frozen on success. Clearing
    /// says the halt need not stand, not that the day's budget is refilled:
    /// a halt lifted at a partially-remediated loss should re-engage
    /// promptly if the rest of the budget goes too. It unfreezes at the next
    /// rollover, now that no halt stands on it.
    pub fn clear_risk_halt(&mut self) -> Result<ArcusSpotRiskHalt, String> {
        let halt = self
            .state
            .risk_halt
            .clone()
            .ok_or_else(|| "Arcus runtime has no engaged risk halt to clear".to_string())?;
        let mark = self.last_risk_mark().ok_or_else(|| {
            "Arcus runtime has no reference prices to re-check the halt condition against"
                .to_string()
        })?;
        if mark.daily_loss_usd >= self.config.daily_loss_limit_usd
            || mark.cumulative_loss_usd >= self.config.cumulative_loss_limit_usd
        {
            return Err(format!(
                "Arcus risk halt condition still holds (daily {} / limit {}, cumulative {} / \
                 limit {}); it would re-engage on the next tick",
                mark.daily_loss_usd,
                self.config.daily_loss_limit_usd,
                mark.cumulative_loss_usd,
                self.config.cumulative_loss_limit_usd,
            ));
        }
        self.state.risk_halt = None;
        Ok(halt)
    }

    /// Re-evaluate the neutral-regime entry direction for one prospective
    /// relative-log-price sample without mutating the runtime. Rollback
    /// continuity verification uses this to prove that an archived entry
    /// fill was preceded by the same signal crossing the live planner uses.
    #[cfg(feature = "arcus-spot-live")]
    pub fn entry_direction_for_signal_sample(
        &self,
        relative_log_price: f64,
    ) -> Option<ArcusSpotDirection> {
        if self.state.regime != ArcusSpotRegime::Neutral || self.state.risk_halt.is_some() {
            return None;
        }
        let score = z_score(
            &self.state.relative_log_price_history,
            relative_log_price,
            self.config.min_signal_samples,
        )?;
        if score >= self.config.entry_z_score {
            Some(ArcusSpotDirection::TokenAToTokenB)
        } else if score <= -self.config.entry_z_score {
            Some(ArcusSpotDirection::TokenBToTokenA)
        } else {
            None
        }
    }

    /// Reject a plan whose trigger/direction can't possibly be committed
    /// against the runtime's *current* regime, before it is ever signed or
    /// submitted.
    ///
    /// `apply_confirmed_live_fill` already enforces this same consistency,
    /// but only after the swap has already executed on-chain and balances
    /// were reconciled -- by which point rejecting a stale plan (approved
    /// against an earlier regime, then submitted after the checkpoint moved
    /// on, e.g. from a second in-flight approval or an operator running a
    /// leftover approved plan file) leaves the wallet already swapped with
    /// nowhere for the fill to go. Calling this first lets a caller refuse
    /// before dispatch instead (Codex P1 follow-up, pairtrade#181).
    ///
    /// Regime/trigger/direction agreement alone is not sufficient: a plan
    /// can still describe a swap `apply_confirmed_live_fill` would later
    /// reject outright (Codex P1 follow-up). Two further invariants that
    /// commit path already enforces are checked here too, before dispatch
    /// rather than after:
    /// - an exit plan must not sell more than the remaining tracked open
    ///   quantity from a prior partial exit;
    /// - an entry plan must not be dispatched while a sticky risk halt is
    ///   engaged, matching the planning path's own hard block.
    #[cfg(feature = "arcus-spot-live")]
    pub fn validate_plan_consistent_with_state(
        &self,
        plan: &ArcusSpotRotationPlan,
    ) -> Result<(), String> {
        require_fill_consistent_with_regime(self.state.regime, plan.trigger, plan.direction)?;
        match plan.trigger {
            ArcusSpotRotationTrigger::EntrySignal => {
                if let Some(halt) = &self.state.risk_halt {
                    return Err(format!(
                        "cannot dispatch an entry plan while the risk halt is active: {halt:?}"
                    ));
                }
            }
            ArcusSpotRotationTrigger::MeanReversionExit | ArcusSpotRotationTrigger::MaxHoldExit => {
                let open = self
                    .state
                    .rotated_quantity
                    .ok_or("rotated regime has no tracked open quantity")?;
                if plan.sell_quantity > open {
                    return Err(format!(
                        "plan sell_quantity {} exceeds the remaining rotated quantity {open}",
                        plan.sell_quantity
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "arcus-spot-live")]
    pub fn apply_confirmed_live_fill_once(
        &mut self,
        plan: &ArcusSpotRotationPlan,
        actual_sell_quantity: Decimal,
        actual_buy_quantity: Decimal,
        filled_at: DateTime<Utc>,
        idempotency_key: &str,
    ) -> Result<bool, String> {
        if idempotency_key.trim().is_empty() {
            return Err("confirmed live fill idempotency key must not be empty".to_string());
        }
        if self.state.last_live_execution_idempotency_key.as_deref() == Some(idempotency_key) {
            return Ok(false);
        }
        self.apply_confirmed_live_fill(plan, actual_sell_quantity, actual_buy_quantity, filled_at)?;
        self.state.last_live_execution_idempotency_key = Some(idempotency_key.to_string());
        Ok(true)
    }

    /// Commit a wallet-balance-reconciled live fill. Planning never mutates
    /// live inventory; callers invoke this only after the durable execution
    /// ledger reaches Confirmed and exact balance reconciliation succeeds.
    #[cfg(feature = "arcus-spot-live")]
    pub fn apply_confirmed_live_fill(
        &mut self,
        plan: &ArcusSpotRotationPlan,
        actual_sell_quantity: Decimal,
        actual_buy_quantity: Decimal,
        filled_at: DateTime<Utc>,
    ) -> Result<(), String> {
        if self.config.mode != ArcusSpotRuntimeMode::Live {
            return Err("confirmed live fills require mode=live".to_string());
        }
        if actual_sell_quantity <= Decimal::ZERO || actual_buy_quantity <= Decimal::ZERO {
            return Err("confirmed live fill quantities must be positive".to_string());
        }
        if actual_sell_quantity != plan.sell_quantity {
            return Err(format!(
                "confirmed sell quantity {} does not equal planned exact quantity {}",
                actual_sell_quantity, plan.sell_quantity
            ));
        }
        if filled_at < plan.quote_received_at {
            return Err("confirmed fill predates its quote receipt".to_string());
        }
        require_fill_consistent_with_regime(self.state.regime, plan.trigger, plan.direction)?;

        let mut next = self.state.clone();
        let mut after = next.inventory;
        match plan.direction {
            ArcusSpotDirection::TokenAToTokenB => {
                after.token_a = after
                    .token_a
                    .checked_sub(actual_sell_quantity)
                    .ok_or("confirmed fill token A subtraction overflow")?;
                after.token_b = after
                    .token_b
                    .checked_add(actual_buy_quantity)
                    .ok_or("confirmed fill token B addition overflow")?;
            }
            ArcusSpotDirection::TokenBToTokenA => {
                after.token_b = after
                    .token_b
                    .checked_sub(actual_sell_quantity)
                    .ok_or("confirmed fill token B subtraction overflow")?;
                after.token_a = after
                    .token_a
                    .checked_add(actual_buy_quantity)
                    .ok_or("confirmed fill token A addition overflow")?;
            }
        }
        if after.token_a < self.config.inventory_floors.token_a
            || after.token_b < self.config.inventory_floors.token_b
        {
            return Err(
                "confirmed fill would place inventory below a configured floor".to_string(),
            );
        }
        next.inventory = after;
        match plan.trigger {
            ArcusSpotRotationTrigger::EntrySignal => {
                next.regime = match plan.direction {
                    ArcusSpotDirection::TokenAToTokenB => ArcusSpotRegime::RotatedAToB,
                    ArcusSpotDirection::TokenBToTokenA => ArcusSpotRegime::RotatedBToA,
                };
                next.last_rotation_at = Some(filled_at);
                next.rotated_quantity = Some(actual_buy_quantity);
            }
            ArcusSpotRotationTrigger::MeanReversionExit | ArcusSpotRotationTrigger::MaxHoldExit => {
                let open = next
                    .rotated_quantity
                    .ok_or("rotated regime has no tracked open quantity")?;
                if actual_sell_quantity > open {
                    return Err("confirmed exit sold more than tracked open quantity".to_string());
                }
                let remaining = open
                    .checked_sub(actual_sell_quantity)
                    .ok_or("confirmed exit quantity subtraction overflow")?;
                if remaining.is_zero() {
                    next.regime = ArcusSpotRegime::Neutral;
                    next.last_rotation_at = None;
                    next.rotated_quantity = None;
                } else {
                    next.rotated_quantity = Some(remaining);
                }
            }
        }
        self.state = next;
        Ok(())
    }

    /// Deterministic replay step: freshness is evaluated at the snapshot event time.
    pub fn step(&mut self, snapshot: &ArcusSpotRecorderSnapshot) -> ArcusSpotRuntimeEvent {
        self.step_at(snapshot, snapshot.collection_finished_at)
    }

    /// Read-side step with an explicit clock. A live observer should pass Utc::now().
    pub fn step_at(
        &mut self,
        snapshot: &ArcusSpotRecorderSnapshot,
        evaluation_time: DateTime<Utc>,
    ) -> ArcusSpotRuntimeEvent {
        // Checked and updated together, before anything else mutates --
        // see `last_observation_at`'s doc comment for why this must live
        // here rather than in any individual caller. A repeat leaves
        // sequence, the signal-window history, and every other field
        // completely untouched: re-evaluating it would artificially
        // consume warm-up samples and reweight the z-score history.
        if let Some(last_observation_at) = self.state.last_observation_at {
            if snapshot.collection_finished_at <= last_observation_at {
                return self.event(RuntimeEventInput {
                    sequence: self.state.sequence,
                    observed_at: evaluation_time,
                    inventory_before: self.state.inventory,
                    regime_before: self.state.regime,
                    token_a_reference_price_usd: None,
                    token_b_reference_price_usd: None,
                    relative_log_price: None,
                    z_score: None,
                    risk_before: None,
                    decision: ArcusSpotDecision::Observe {
                        hold: ArcusSpotHold::new(
                            ArcusSpotHoldCode::StaleOrDuplicateObservation,
                            format!(
                                "snapshot collection_finished_at {} is not newer than the last \
                                 observation this runtime already advanced ({last_observation_at})",
                                snapshot.collection_finished_at
                            ),
                        ),
                    },
                });
            }
        }
        self.state.sequence = self.state.sequence.saturating_add(1);
        let sequence = self.state.sequence;
        let inventory_before = self.state.inventory;
        let regime_before = self.state.regime;

        // Resolved independently of the round-trip row: mark-to-market
        // equity valuation and risk-halt engagement must run even when the
        // row itself is missing, errored, or stale, so a loss-limit breach
        // during a route outage still engages the (sticky) halt instead of
        // silently recovering before the next valid route.
        let price = match self.price_context(snapshot, evaluation_time) {
            Ok(price) => price,
            Err(hold) => {
                // Deliberately NOT advancing last_observation_at here: a
                // structurally invalid snapshot (bad schema, wrong chain,
                // corrupt/inverted timestamps, unresolvable tokens) proved
                // nothing trustworthy about collection_finished_at, and
                // committing it to the watermark before validation would
                // let a single corrupt record (e.g. a bad bootstrap
                // archive entry with a far-future timestamp) make every
                // subsequent legitimate observation look stale or
                // duplicate until wall time caught up to the bad value --
                // silently halting signal evaluation (Codex P2 follow-up,
                // pairtrade#186).
                return self.event(RuntimeEventInput {
                    sequence,
                    observed_at: evaluation_time,
                    inventory_before,
                    regime_before,
                    token_a_reference_price_usd: None,
                    token_b_reference_price_usd: None,
                    relative_log_price: None,
                    z_score: None,
                    risk_before: None,
                    decision: ArcusSpotDecision::Observe { hold },
                });
            }
        };
        // price_context validated schema/mode/chain/timestamps (and
        // resolved both tokens), so this observation is genuine even if a
        // later check in this same call still rejects it for an unrelated
        // reason (e.g. RouteUnavailable) -- advance the watermark now,
        // not conditioned on anything past this point.
        self.state.last_observation_at = Some(snapshot.collection_finished_at);
        self.state.last_token_a_reference_price_usd = Some(price.token_a_price_usd);
        self.state.last_token_b_reference_price_usd = Some(price.token_b_price_usd);

        let equity_before = match inventory_before
            .checked_value_usd(price.token_a_price_usd, price.token_b_price_usd)
        {
            Some(value) => value,
            None => {
                return self.event(RuntimeEventInput {
                    sequence,
                    observed_at: evaluation_time,
                    inventory_before,
                    regime_before,
                    token_a_reference_price_usd: Some(price.token_a_price_usd),
                    token_b_reference_price_usd: Some(price.token_b_price_usd),
                    relative_log_price: None,
                    z_score: None,
                    risk_before: None,
                    decision: ArcusSpotDecision::Observe {
                        hold: ArcusSpotHold::new(
                            ArcusSpotHoldCode::InvalidSnapshot,
                            "inventory valuation exceeds Decimal range",
                        ),
                    },
                })
            }
        };
        // Marked against the *prior* daily basket before it can be reset
        // below: on the first snapshot of a new UTC day, resetting first
        // would measure the outgoing day's rotations against the basket
        // they already produced, reporting zero and letting whatever they
        // cost go unassessed. Marking first keeps the outgoing day's basket
        // as the reference for one last tick.
        let risk_before = self.risk_mark(
            equity_before,
            price.token_a_price_usd,
            price.token_b_price_usd,
        );
        // Engaged before the baselines move, so that on a rollover tick the
        // halt already exists when `update_risk_baselines` decides whether
        // to rebase the basket this mark was taken against. Reversed, the
        // basket is gone by the time anything can tell a halt now stands on
        // it. `engage_risk_halt` reads only the config limits and this mark,
        // so nothing here depends on the baselines being current.
        self.engage_risk_halt(evaluation_time, risk_before);
        self.update_risk_baselines(evaluation_time, equity_before, inventory_before);
        self.state.last_equity_usd = Some(equity_before);

        // Computed and appended to the signal window from `price` (token
        // metadata + reference prices only) before the route-availability
        // gate below, so an outage that drops the recorder's route rows
        // does not also stall the signal history. Otherwise the first
        // route recovered after an outage would be scored against
        // pre-outage prices and could produce a spurious entry or fill
        // even though the ratio was stable throughout.
        let relative_log_price =
            match relative_log_price(price.token_a_price_usd, price.token_b_price_usd) {
                Ok(value) => value,
                Err(detail) => {
                    return self.event(RuntimeEventInput {
                        sequence,
                        observed_at: evaluation_time,
                        inventory_before,
                        regime_before,
                        token_a_reference_price_usd: Some(price.token_a_price_usd),
                        token_b_reference_price_usd: Some(price.token_b_price_usd),
                        relative_log_price: None,
                        z_score: None,
                        risk_before: Some(risk_before),
                        decision: ArcusSpotDecision::Observe {
                            hold: ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail),
                        },
                    })
                }
            };
        let informative_signal_samples =
            informative_signal_sample_count(&self.state.relative_log_price_history);
        let total_signal_samples = self.state.relative_log_price_history.len();
        let z_score = z_score(
            &self.state.relative_log_price_history,
            relative_log_price,
            self.config.min_signal_samples,
        );
        self.state
            .relative_log_price_history
            .push(relative_log_price);
        if self.state.relative_log_price_history.len() > self.config.signal_window_samples {
            let excess =
                self.state.relative_log_price_history.len() - self.config.signal_window_samples;
            self.state.relative_log_price_history.drain(0..excess);
        }

        // A max-hold exit must fire even when the signal window is flat
        // (z_score() returns None once its standard deviation collapses to
        // zero), so rotation_signal is consulted with the raw Option instead
        // of bailing out to Warmup before it ever sees a rotated regime.
        // Resolved *before* snapshot_context so it can validate only the
        // leg an exit will actually execute (see snapshot_context's doc
        // comment): a stale but unused leg must not be able to block a
        // mean-reversion or max-hold exit, defeating max_hold_secs.
        let signal = self.rotation_signal(z_score, evaluation_time, regime_before);

        let context = match self.snapshot_context(snapshot, evaluation_time, &price, signal) {
            Ok(context) => context,
            Err(hold) => {
                return self.event(RuntimeEventInput {
                    sequence,
                    observed_at: evaluation_time,
                    inventory_before,
                    regime_before,
                    token_a_reference_price_usd: Some(price.token_a_price_usd),
                    token_b_reference_price_usd: Some(price.token_b_price_usd),
                    relative_log_price: Some(relative_log_price),
                    z_score,
                    risk_before: Some(risk_before),
                    decision: ArcusSpotDecision::Observe { hold },
                })
            }
        };

        // A halt blocks new entries, but an existing rotated position must
        // still be able to exit via mean-reversion or max-hold: rotation_signal
        // is keyed on `regime_before` and can only ever produce an EntrySignal
        // from Neutral, never from a rotated regime, so falling through to it
        // here cannot open a new position while halted. Blocking
        // unconditionally instead made a halt engaged mid-rotation permanent,
        // defeating the configured maximum hold indefinitely.
        if let Some(halt) = &self.state.risk_halt {
            if regime_before == ArcusSpotRegime::Neutral {
                return self.event(RuntimeEventInput {
                    sequence,
                    observed_at: evaluation_time,
                    inventory_before,
                    regime_before,
                    token_a_reference_price_usd: Some(context.token_a_price_usd),
                    token_b_reference_price_usd: Some(context.token_b_price_usd),
                    relative_log_price: Some(relative_log_price),
                    z_score,
                    risk_before: Some(risk_before),
                    decision: ArcusSpotDecision::Observe {
                        hold: ArcusSpotHold::new(
                            ArcusSpotHoldCode::RiskHalt,
                            format!(
                                "{:?} halt engaged at {}: loss {} >= limit {}",
                                halt.kind, halt.engaged_at, halt.loss_usd, halt.limit_usd
                            ),
                        ),
                    },
                });
            }
        }

        let Some((direction, trigger)) = signal else {
            let hold = match z_score {
                Some(z) => ArcusSpotHold::new(
                    ArcusSpotHoldCode::NoSignal,
                    format!("z={z:.6}, regime={regime_before:?}"),
                ),
                None => ArcusSpotHold::new(
                    ArcusSpotHoldCode::Warmup,
                    format!(
                        "signal not ready: need {} informative prior samples; have {} across {} total prior observations",
                        self.config.min_signal_samples,
                        informative_signal_samples,
                        total_signal_samples,
                    ),
                ),
            };
            return self.event(RuntimeEventInput {
                sequence,
                observed_at: evaluation_time,
                inventory_before,
                regime_before,
                token_a_reference_price_usd: Some(context.token_a_price_usd),
                token_b_reference_price_usd: Some(context.token_b_price_usd),
                relative_log_price: Some(relative_log_price),
                z_score,
                risk_before: Some(risk_before),
                decision: ArcusSpotDecision::Observe { hold },
            });
        };

        let plan = match self.build_plan(
            &context,
            direction,
            trigger,
            evaluation_time,
            inventory_before,
        ) {
            Ok(plan) => plan,
            Err(hold) => {
                return self.event(RuntimeEventInput {
                    sequence,
                    observed_at: evaluation_time,
                    inventory_before,
                    regime_before,
                    token_a_reference_price_usd: Some(context.token_a_price_usd),
                    token_b_reference_price_usd: Some(context.token_b_price_usd),
                    relative_log_price: Some(relative_log_price),
                    z_score,
                    risk_before: Some(risk_before),
                    decision: ArcusSpotDecision::Observe { hold },
                })
            }
        };

        let decision = match self.config.mode {
            ArcusSpotRuntimeMode::ReadOnly => ArcusSpotDecision::WouldRotate { plan },
            #[cfg(feature = "arcus-spot-live")]
            ArcusSpotRuntimeMode::Live => ArcusSpotDecision::WouldRotate { plan },
            ArcusSpotRuntimeMode::ReplaySimulation => {
                self.state.inventory = plan.predicted_inventory;
                match trigger {
                    ArcusSpotRotationTrigger::EntrySignal => {
                        self.state.regime = match direction {
                            ArcusSpotDirection::TokenAToTokenB => ArcusSpotRegime::RotatedAToB,
                            ArcusSpotDirection::TokenBToTokenA => ArcusSpotRegime::RotatedBToA,
                        };
                        self.state.last_rotation_at = Some(evaluation_time);
                        self.state.rotated_quantity = Some(plan.buy_quantity);
                    }
                    ArcusSpotRotationTrigger::MeanReversionExit
                    | ArcusSpotRotationTrigger::MaxHoldExit => {
                        // build_plan() bounded plan.sell_quantity to at most
                        // the tracked open quantity, so this is >= 0; only
                        // clear the regime once the whole open amount has
                        // actually been unwound, otherwise stay rotated
                        // with the remaining open quantity so the next step
                        // keeps trying to close it out.
                        let remaining = self
                            .state
                            .rotated_quantity
                            .and_then(|open| open.checked_sub(plan.sell_quantity))
                            .unwrap_or(Decimal::ZERO)
                            .max(Decimal::ZERO);
                        if remaining.is_zero() {
                            self.state.regime = ArcusSpotRegime::Neutral;
                            self.state.last_rotation_at = None;
                            self.state.rotated_quantity = None;
                        } else {
                            self.state.rotated_quantity = Some(remaining);
                        }
                    }
                }
                ArcusSpotDecision::SimulatedFill { plan }
            }
        };

        let equity_after = self
            .state
            .inventory
            .checked_value_usd(context.token_a_price_usd, context.token_b_price_usd)
            .unwrap_or(equity_before);
        // Overwrites the pre-fill mark set earlier in this tick so
        // `last_equity_usd` records this tick's actual post-fill close.
        self.state.last_equity_usd = Some(equity_after);
        // Re-marked after the fill, against the same (still un-reset) daily
        // basket: this is the tick where a value-destroying rotation
        // actually shows up, since only a rotation can move equity away
        // from the re-priced benchmark.
        let risk_after = self.risk_mark(
            equity_after,
            context.token_a_price_usd,
            context.token_b_price_usd,
        );
        self.engage_risk_halt(evaluation_time, risk_after);
        self.event(RuntimeEventInput {
            sequence,
            observed_at: evaluation_time,
            inventory_before,
            regime_before,
            token_a_reference_price_usd: Some(context.token_a_price_usd),
            token_b_reference_price_usd: Some(context.token_b_price_usd),
            relative_log_price: Some(relative_log_price),
            z_score,
            risk_before: Some(risk_before),
            decision,
        })
    }

    fn event(&self, input: RuntimeEventInput) -> ArcusSpotRuntimeEvent {
        let risk_after = input
            .token_a_reference_price_usd
            .zip(input.token_b_reference_price_usd)
            .and_then(|(price_a, price_b)| {
                self.state
                    .inventory
                    .checked_value_usd(price_a, price_b)
                    .map(|equity| self.risk_mark(equity, price_a, price_b))
            });
        ArcusSpotRuntimeEvent {
            sequence: input.sequence,
            observed_at: input.observed_at,
            pair: format!(
                "{}/{}",
                self.config.pair.sell_symbol, self.config.pair.buy_symbol
            ),
            mode: self.config.mode,
            token_a_reference_price_usd: input.token_a_reference_price_usd,
            token_b_reference_price_usd: input.token_b_reference_price_usd,
            relative_log_price: input.relative_log_price,
            z_score: input.z_score,
            inventory_before: input.inventory_before,
            inventory_after: self.state.inventory,
            regime_before: input.regime_before,
            regime_after: self.state.regime,
            risk_before: input.risk_before,
            risk_after,
            decision: input.decision,
        }
    }

    fn price_context(
        &self,
        snapshot: &ArcusSpotRecorderSnapshot,
        evaluation_time: DateTime<Utc>,
    ) -> Result<PriceContext, ArcusSpotHold> {
        if snapshot.schema_version != SUPPORTED_RECORDER_SCHEMA_VERSION {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!(
                    "unsupported recorder schema {}; expected {}",
                    snapshot.schema_version, SUPPORTED_RECORDER_SCHEMA_VERSION
                ),
            ));
        }
        if snapshot.mode != PUBLIC_RECORDER_MODE {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!("unsupported recorder mode {:?}", snapshot.mode),
            ));
        }
        if snapshot.chain_id != self.config.chain_id {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!(
                    "snapshot chain {} does not match configured chain {}",
                    snapshot.chain_id, self.config.chain_id
                ),
            ));
        }
        if snapshot.collection_finished_at < snapshot.collection_started_at {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "snapshot finishes before it starts",
            ));
        }

        let tokens = capture_payload(&snapshot.token_metadata, "token_metadata")?;
        let token_a = find_token(tokens, &self.config.pair.sell_symbol, self.config.chain_id)?;
        let token_b = find_token(tokens, &self.config.pair.buy_symbol, self.config.chain_id)?;
        // Both lookups can independently pass verification yet still resolve
        // to the same contract (e.g. a mislabeled wrapped-token entry), which
        // would let the relative-price signal and inventory accounting treat
        // one asset as two distinct ones.
        if token_a.address.eq_ignore_ascii_case(&token_b.address) {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!(
                    "token {} and {} resolve to the same contract {}",
                    token_a.symbol, token_b.symbol, token_a.address
                ),
            ));
        }
        let overview = capture_payload(&snapshot.reference_overview, "reference_overview")?;
        // A fresh route response says nothing about how old the separately
        // captured reference-price observation is; an old or future-dated
        // overview can otherwise poison the signal history, engage a sticky
        // loss halt incorrectly, and drive notional/imbalance validation on
        // stale prices even though the route itself passes its own
        // freshness check.
        let overview_age_ms = evaluation_time
            .signed_duration_since(overview.received_at)
            .num_milliseconds();
        if overview_age_ms < 0 {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "reference_overview receipt is later than evaluation time",
            ));
        }
        let max_overview_age_ms = self.config.max_quote_age_secs.saturating_mul(1_000);
        if overview_age_ms > max_overview_age_ms {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::StaleQuote,
                format!(
                    "reference_overview age {overview_age_ms}ms exceeds {max_overview_age_ms}ms"
                ),
            ));
        }
        let token_a_price_usd = find_reference_price(overview, &token_a)?;
        let token_b_price_usd = find_reference_price(overview, &token_b)?;

        Ok(PriceContext {
            token_a,
            token_b,
            token_a_price_usd,
            token_b_price_usd,
        })
    }

    /// Selects and validates the independently quoted round-trip cycle that
    /// corresponds to `signal`. Entries consume the selected row's forward
    /// leg; exits consume its reverse leg. With no signal, the current regime
    /// identifies which cycle could eventually be exited.
    ///
    /// An exit only ever consumes one leg (see `build_plan`'s trigger
    /// match), and unlike an entry it does not need a verified round-trip
    /// cost -- the cost gate is entry-only (see `build_plan`). Requiring the
    /// *other*, unused leg to also be fresh and internally consistent here
    /// used to block every exit until that unused leg's freshness happened
    /// to recover, which can defeat `max_hold_secs`'s hard guarantee
    /// indefinitely if the row's forward/reverse legs are refreshed out of
    /// step (Codex P1 follow-up, pairtrade#177).
    fn snapshot_context(
        &self,
        snapshot: &ArcusSpotRecorderSnapshot,
        evaluation_time: DateTime<Utc>,
        price: &PriceContext,
        signal: Option<(ArcusSpotDirection, ArcusSpotRotationTrigger)>,
    ) -> Result<SnapshotContext, ArcusSpotHold> {
        let token_a = &price.token_a;
        let token_b = &price.token_b;
        let token_a_price_usd = price.token_a_price_usd;
        let token_b_price_usd = price.token_b_price_usd;

        let cycle_forward_direction = match signal {
            Some((direction, ArcusSpotRotationTrigger::EntrySignal)) => direction,
            Some((ArcusSpotDirection::TokenAToTokenB, _)) => ArcusSpotDirection::TokenBToTokenA,
            Some((ArcusSpotDirection::TokenBToTokenA, _)) => ArcusSpotDirection::TokenAToTokenB,
            None => match self.state.regime {
                ArcusSpotRegime::Neutral | ArcusSpotRegime::RotatedAToB => {
                    ArcusSpotDirection::TokenAToTokenB
                }
                ArcusSpotRegime::RotatedBToA => ArcusSpotDirection::TokenBToTokenA,
            },
        };
        let (
            cycle_sell_symbol,
            cycle_buy_symbol,
            cycle_sell_token,
            cycle_buy_token,
            cycle_sell_price_usd,
            cycle_buy_price_usd,
        ) = match cycle_forward_direction {
            ArcusSpotDirection::TokenAToTokenB => (
                &self.config.pair.sell_symbol,
                &self.config.pair.buy_symbol,
                token_a,
                token_b,
                token_a_price_usd,
                token_b_price_usd,
            ),
            ArcusSpotDirection::TokenBToTokenA => (
                &self.config.pair.buy_symbol,
                &self.config.pair.sell_symbol,
                token_b,
                token_a,
                token_b_price_usd,
                token_a_price_usd,
            ),
        };

        let matching_rows = snapshot
            .round_trips
            .iter()
            .filter(|row| {
                row.pair.sell_symbol.eq_ignore_ascii_case(cycle_sell_symbol)
                    && row.pair.buy_symbol.eq_ignore_ascii_case(cycle_buy_symbol)
                    && Decimal::from_str(&row.notional_usd)
                        .is_ok_and(|notional| notional == self.config.notional_usd)
            })
            .collect::<Vec<_>>();
        if matching_rows.len() != 1 {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                format!(
                    "expected one {}/{} row at USD {}; found {}",
                    cycle_sell_symbol,
                    cycle_buy_symbol,
                    self.config.notional_usd,
                    matching_rows.len()
                ),
            ));
        }
        let row = matching_rows[0];
        if !row.errors.is_empty() {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                format!("recorder row contains {} error(s)", row.errors.len()),
            ));
        }
        validate_recorded_reference(
            "cycle sell token",
            row.sell_reference_price_usd.as_deref(),
            cycle_sell_price_usd,
        )?;
        validate_recorded_reference(
            "cycle buy token",
            row.buy_reference_price_usd.as_deref(),
            cycle_buy_price_usd,
        )?;
        if row.forward.is_none() || row.reverse.is_none() {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                "both direct route directions are required",
            ));
        }
        if row.optimistic_round_trip_loss_bps.is_none() {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                "round-trip cost is absent",
            ));
        }
        let forward_route = row.forward.as_ref().expect("checked above");
        let reverse_route = row.reverse.as_ref().expect("checked above");

        let is_exit = signal.is_some_and(|(_, trigger)| {
            trigger == ArcusSpotRotationTrigger::MeanReversionExit
                || trigger == ArcusSpotRotationTrigger::MaxHoldExit
        });

        let verified_round_trip_loss_bps = if is_exit {
            // The selected cycle's reverse leg is the only leg an exit
            // consumes. The entry-only cost gate must not let the unused
            // forward leg's freshness block a position close.
            validate_route_leg(
                reverse_route,
                cycle_buy_token,
                cycle_sell_token,
                evaluation_time,
                self.config.max_quote_age_secs,
            )?;
            verify_reverse_notional_bound(
                reverse_route,
                self.config.notional_usd,
                cycle_buy_price_usd,
                cycle_buy_token,
            )?;
            parse_positive_or_zero(
                "optimistic_round_trip_loss_bps",
                row.optimistic_round_trip_loss_bps.as_deref(),
            )?
        } else {
            // An entry (or a tick with no exit) must independently validate
            // both legs of the selected cycle before trusting its cost.
            validate_route_leg(
                forward_route,
                cycle_sell_token,
                cycle_buy_token,
                evaluation_time,
                self.config.max_quote_age_secs,
            )?;
            validate_route_leg(
                reverse_route,
                cycle_buy_token,
                cycle_sell_token,
                evaluation_time,
                self.config.max_quote_age_secs,
            )?;
            verify_requested_notional_amount(
                row,
                forward_route,
                self.config.notional_usd,
                cycle_sell_price_usd,
                cycle_sell_token,
            )?;
            verify_reverse_notional_bound(
                reverse_route,
                self.config.notional_usd,
                cycle_buy_price_usd,
                cycle_buy_token,
            )?;
            verify_round_trip_linkage_and_loss(row)?
        };

        Ok(SnapshotContext {
            token_a: token_a.clone(),
            token_b: token_b.clone(),
            token_a_price_usd,
            token_b_price_usd,
            row: row.clone(),
            verified_round_trip_loss_bps,
        })
    }

    /// `z_score` is `None` once the signal window is flat enough that its
    /// standard deviation collapses to zero. A rotated position must still
    /// be able to time out on `max_hold_secs` in that case, so the max-hold
    /// branches are checked before the z-score is required; only the
    /// entry and mean-reversion-exit paths need an actual score.
    fn rotation_signal(
        &self,
        z_score: Option<f64>,
        evaluation_time: DateTime<Utc>,
        regime: ArcusSpotRegime,
    ) -> Option<(ArcusSpotDirection, ArcusSpotRotationTrigger)> {
        match regime {
            ArcusSpotRegime::Neutral => {
                let z_score = z_score?;
                if z_score >= self.config.entry_z_score {
                    Some((
                        ArcusSpotDirection::TokenAToTokenB,
                        ArcusSpotRotationTrigger::EntrySignal,
                    ))
                } else if z_score <= -self.config.entry_z_score {
                    Some((
                        ArcusSpotDirection::TokenBToTokenA,
                        ArcusSpotRotationTrigger::EntrySignal,
                    ))
                } else {
                    None
                }
            }
            ArcusSpotRegime::RotatedAToB => {
                if self.max_hold_elapsed(evaluation_time) {
                    return Some((
                        ArcusSpotDirection::TokenBToTokenA,
                        ArcusSpotRotationTrigger::MaxHoldExit,
                    ));
                }
                let z_score = z_score?;
                if z_score <= self.config.exit_z_score {
                    Some((
                        ArcusSpotDirection::TokenBToTokenA,
                        ArcusSpotRotationTrigger::MeanReversionExit,
                    ))
                } else {
                    None
                }
            }
            ArcusSpotRegime::RotatedBToA => {
                if self.max_hold_elapsed(evaluation_time) {
                    return Some((
                        ArcusSpotDirection::TokenAToTokenB,
                        ArcusSpotRotationTrigger::MaxHoldExit,
                    ));
                }
                let z_score = z_score?;
                if z_score >= -self.config.exit_z_score {
                    Some((
                        ArcusSpotDirection::TokenAToTokenB,
                        ArcusSpotRotationTrigger::MeanReversionExit,
                    ))
                } else {
                    None
                }
            }
        }
    }

    fn max_hold_elapsed(&self, evaluation_time: DateTime<Utc>) -> bool {
        self.state.last_rotation_at.is_some_and(|started_at| {
            evaluation_time
                .signed_duration_since(started_at)
                .num_seconds()
                >= self.config.max_hold_secs
        })
    }

    fn build_plan(
        &self,
        context: &SnapshotContext,
        direction: ArcusSpotDirection,
        trigger: ArcusSpotRotationTrigger,
        evaluation_time: DateTime<Utc>,
        inventory: ArcusSpotInventory,
    ) -> Result<ArcusSpotRotationPlan, ArcusSpotHold> {
        let route_loss = context.verified_round_trip_loss_bps;
        let all_in_cost = route_loss
            .checked_add(self.config.gas_buffer_bps)
            .and_then(|cost| cost.checked_add(self.config.settlement_buffer_bps))
            .ok_or_else(|| {
                ArcusSpotHold::new(
                    ArcusSpotHoldCode::CostLimit,
                    "all-in route cost exceeds Decimal range",
                )
            })?;
        // Entry-only, like max_rotation_fraction and the inventory-imbalance
        // cap above/below: if this were also enforced on exits, a round-trip
        // cost that rises above the limit while already rotated would keep
        // rejecting both MeanReversionExit and MaxHoldExit on every later
        // snapshot until costs fell back under it, making max_hold_secs not
        // actually a maximum.
        if trigger == ArcusSpotRotationTrigger::EntrySignal
            && all_in_cost > self.config.max_all_in_round_trip_cost_bps
        {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::CostLimit,
                format!(
                    "all-in round-trip cost {} bps exceeds {} bps",
                    all_in_cost, self.config.max_all_in_round_trip_cost_bps
                ),
            ));
        }

        let route = match trigger {
            ArcusSpotRotationTrigger::EntrySignal => {
                context.row.forward.as_ref().expect("validated forward")
            }
            ArcusSpotRotationTrigger::MeanReversionExit | ArcusSpotRotationTrigger::MaxHoldExit => {
                context.row.reverse.as_ref().expect("validated reverse")
            }
        };
        let (sell_token, buy_token, sell_balance, sell_floor) = match direction {
            ArcusSpotDirection::TokenAToTokenB => (
                &context.token_a,
                &context.token_b,
                inventory.token_a,
                self.config.inventory_floors.token_a,
            ),
            ArcusSpotDirection::TokenBToTokenA => (
                &context.token_b,
                &context.token_a,
                inventory.token_b,
                self.config.inventory_floors.token_b,
            ),
        };
        // snapshot_context() already validated both legs' identity, echoed
        // sell amount, and freshness before accepting this row; re-running
        // the same check on the selected leg here keeps build_plan callable
        // (and independently testable) without relying on that upstream
        // gate having run first.
        validate_route_leg(
            route,
            sell_token,
            buy_token,
            evaluation_time,
            self.config.max_quote_age_secs,
        )?;
        let quote = route
            .response
            .payload
            .recommended_quote()
            .map_err(|error| {
                ArcusSpotHold::new(ArcusSpotHoldCode::RouteUnavailable, error.to_string())
            })?;

        let sell_quantity = raw_amount_to_quantity(&route.sell_amount, sell_token.decimals)
            .map_err(|detail| ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail))?;
        let buy_quantity = raw_amount_to_quantity(&quote.buy_amount, buy_token.decimals)
            .map_err(|detail| ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail))?;
        let sell_amount_raw = route.sell_amount.clone();
        let buy_amount_raw = quote.buy_amount.clone();
        let sellable = sell_balance.checked_sub(sell_floor).ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InventoryFloor,
                "sell balance is below its configured floor",
            )
        })?;
        if sell_quantity > sellable {
            if trigger == ArcusSpotRotationTrigger::EntrySignal {
                return Err(ArcusSpotHold::new(
                    ArcusSpotHoldCode::InventoryFloor,
                    format!(
                        "selling {} {} would cross floor {}; balance={}",
                        sell_quantity, sell_token.symbol, sell_floor, sell_balance
                    ),
                ));
            }
            // An exit's route offers whatever amount the recorder's
            // fixed-notional quote happens to propose, which is
            // independent of how much is actually sellable above the
            // floor. Scaling buy_quantity linearly down to the sellable
            // amount was tried here, but that synthesizes a fill price the
            // venue never actually quoted: under price impact, fixed fees,
            // minimum amounts, or tiered pricing, the real executable
            // result for a smaller size can differ materially, corrupting
            // replay inventory, equity, and PnL (Codex P1 follow-up,
            // pairtrade#177). Remain unfilled and wait for a snapshot whose
            // quote actually fits above the floor instead of inventing
            // one; this accepts that a rotation can stay open longer near
            // the floor, which is preferred over recording an
            // unexecutable fill.
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InventoryFloor,
                format!(
                    "selling {} {} would cross floor {}; sellable={}, no quote at that residual \
                     size is available",
                    sell_quantity, sell_token.symbol, sell_floor, sellable
                ),
            ));
        }
        // An exit's route offers whatever quantity the recorder's
        // fixed-notional quote happens to propose at this snapshot, which is
        // independent of the quantity actually acquired at entry: quote
        // movement between entry and later snapshots routinely makes it
        // smaller OR larger than the tracked open rotation quantity. An
        // undersized quote is an ordinary partial exit — the state
        // transition below keeps the remainder tracked as still open.
        // Rejecting an oversized quote outright (an earlier version of this
        // check did) is unnecessary and actively harmful: once a smaller
        // quote has partially unwound the position, every later
        // fixed-notional quote is ordinarily larger than the shrinking
        // remainder, so a hard reject here would leave the runtime
        // permanently rotated with a residual amount no future quote could
        // ever satisfy. But letting it through at full size would instead
        // sell more than was ever acquired for this rotation, consuming
        // pre-existing (pre-rotation) inventory and misattributing it to
        // this exit (bot-strategy#755 review round13); prorate it down to
        // exactly the tracked open quantity instead, the same way the
        // floor-crossing case above is prorated.
        if trigger != ArcusSpotRotationTrigger::EntrySignal {
            if let Some(open_quantity) = self.state.rotated_quantity {
                if sell_quantity > open_quantity {
                    // Scaling buy_quantity linearly down to open_quantity
                    // was tried here, but that synthesizes a fill price the
                    // venue never actually quoted for that smaller size --
                    // the same soundness problem as the floor-crossing case
                    // above (Codex P1 follow-up, pairtrade#177). Remain
                    // unfilled and wait for a snapshot whose quote fits
                    // within the tracked open quantity instead of
                    // inventing one.
                    return Err(ArcusSpotHold::new(
                        ArcusSpotHoldCode::RotationLimit,
                        format!(
                            "exit selling {} {} exceeds the open rotation quantity {}; no quote \
                             at that size is available",
                            sell_quantity, sell_token.symbol, open_quantity
                        ),
                    ));
                }
            }
        }
        // The cap limits how large a single new entry may be relative to
        // available inventory; it must not also apply to exits. The
        // fraction is recomputed against whatever balance remains after the
        // entry, which is smaller than the balance the entry itself was
        // capped against, so applying the same cap to the reverse leg can
        // reject the exit outright and leave the position permanently
        // stuck in a rotated regime with no way to unwind.
        if trigger == ArcusSpotRotationTrigger::EntrySignal {
            let max_rotation = sellable
                .checked_mul(self.config.max_rotation_fraction)
                .ok_or_else(|| {
                    ArcusSpotHold::new(
                        ArcusSpotHoldCode::RotationLimit,
                        "rotation cap exceeds Decimal range",
                    )
                })?;
            if sell_quantity > max_rotation {
                return Err(ArcusSpotHold::new(
                    ArcusSpotHoldCode::RotationLimit,
                    format!(
                        "selling {} {} exceeds per-action cap {}",
                        sell_quantity, sell_token.symbol, max_rotation
                    ),
                ));
            }
        }

        let predicted_inventory = match direction {
            ArcusSpotDirection::TokenAToTokenB => ArcusSpotInventory {
                token_a: inventory
                    .token_a
                    .checked_sub(sell_quantity)
                    .ok_or_else(|| inventory_math_error("subtract token A"))?,
                token_b: inventory
                    .token_b
                    .checked_add(buy_quantity)
                    .ok_or_else(|| inventory_math_error("add token B"))?,
            },
            ArcusSpotDirection::TokenBToTokenA => ArcusSpotInventory {
                token_a: inventory
                    .token_a
                    .checked_add(buy_quantity)
                    .ok_or_else(|| inventory_math_error("add token A"))?,
                token_b: inventory
                    .token_b
                    .checked_sub(sell_quantity)
                    .ok_or_else(|| inventory_math_error("subtract token B"))?,
            },
        };
        let imbalance = inventory_imbalance_fraction(
            predicted_inventory,
            context.token_a_price_usd,
            context.token_b_price_usd,
        )?;
        if imbalance > self.config.max_inventory_imbalance_fraction {
            // The hard cap applies unconditionally to entries. A market move
            // can push an already-rotated portfolio's imbalance above the
            // cap between snapshots, and a single reverse quote may not
            // bring it fully back under the cap in one fill; rejecting
            // every such exit would block both mean-reversion and max-hold
            // exits and leave the runtime permanently rotated even though
            // repeated partial exits would reduce exposure. So an exit is
            // only rejected here if it would not even improve on the
            // current (pre-trade) imbalance.
            let blocks = if trigger == ArcusSpotRotationTrigger::EntrySignal {
                true
            } else {
                let current_imbalance = inventory_imbalance_fraction(
                    inventory,
                    context.token_a_price_usd,
                    context.token_b_price_usd,
                )?;
                imbalance >= current_imbalance
            };
            if blocks {
                return Err(ArcusSpotHold::new(
                    ArcusSpotHoldCode::InventoryImbalance,
                    format!(
                        "predicted USD inventory imbalance {} exceeds {}",
                        imbalance, self.config.max_inventory_imbalance_fraction
                    ),
                ));
            }
        }

        Ok(ArcusSpotRotationPlan {
            direction,
            trigger,
            sell_symbol: sell_token.symbol.clone(),
            buy_symbol: buy_token.symbol.clone(),
            sell_token_address: sell_token.address.clone(),
            buy_token_address: buy_token.address.clone(),
            sell_quantity,
            buy_quantity,
            sell_amount_raw,
            buy_amount_raw,
            venue: quote.venue.clone(),
            quote_received_at: route.response.received_at,
            optimistic_round_trip_loss_bps: route_loss,
            gas_buffer_bps: self.config.gas_buffer_bps,
            settlement_buffer_bps: self.config.settlement_buffer_bps,
            all_in_round_trip_cost_bps: all_in_cost,
            predicted_inventory,
            predicted_inventory_imbalance_fraction: imbalance,
        })
    }

    fn update_risk_baselines(
        &mut self,
        at: DateTime<Utc>,
        equity_usd: Decimal,
        inventory: ArcusSpotInventory,
    ) {
        if self.state.initial_equity_usd.is_none() {
            self.state.initial_equity_usd = Some(equity_usd);
        }
        if self.state.initial_baseline_inventory.is_none() {
            self.state.initial_baseline_inventory = Some(inventory);
        }
        let day = at.format("%Y-%m-%d").to_string();
        let rolling_over = self.state.daily_baseline_day.as_deref() != Some(day.as_str());
        if rolling_over {
            self.state.daily_baseline_day = Some(day);
            self.state.daily_baseline_equity_usd = Some(equity_usd);
        }
        // The basket is the *evidence of what is owed*, not just a per-day
        // convenience, so a rollover must not overwrite it while a halt
        // stands on it. It used to: the caller engages the halt from a mark
        // taken against the outgoing day's basket, and this then rebased
        // that basket to the current -- still impaired -- inventory in the
        // same tick. Everything that later re-derived the loss from the
        // persisted state read back ~0, so `clear-risk-halt` would lift an
        // unremediated halt for no better reason than a day boundary having
        // passed, and `require_risk_state_continuity` would reject the very
        // checkpoint where the halt correctly fired (review of
        // pairtrade#211/#212).
        //
        // Freezing only the basket, not the day or the equity mark, is
        // deliberate: those two keep their ordinary meaning for the
        // rollover-matching and continuity checks that read them, while the
        // basket goes on answering "down against what?" for as long as the
        // answer still matters. It unfreezes on the first rollover after the
        // halt is lifted.
        let basket_missing = self.state.daily_baseline_inventory.is_none();
        if basket_missing || (rolling_over && self.state.risk_halt.is_none()) {
            // `basket_missing` also covers a checkpoint written before the
            // baskets existed and loaded part-way through a day -- including
            // one loaded while halted, which must still get a basket or its
            // daily stop stays unmeasurable forever. Adopting this tick's
            // inventory only misprices rotations that already happened
            // earlier today, and self-corrects at the next rollover.
            self.state.daily_baseline_inventory = Some(inventory);
        }
    }

    /// What a basket would be worth at these prices — the buy-and-hold
    /// counterfactual the loss stops measure the strategy against.
    /// `None` only when there is no basket recorded yet, which genuinely
    /// means "not measurable" and is read downstream as no loss.
    ///
    /// A basket that exists but cannot be valued is a different thing and
    /// must not collapse into the same answer: silently reporting no loss is
    /// the one outcome a risk metric may never produce by accident. It
    /// returns `Decimal::MAX` instead, which reads downstream as an
    /// unbounded loss and halts. Unreachable at any realistic size -- the
    /// live basket is ~0.2 tokens at ~$200 against a 96-bit type -- so the
    /// point is the direction it fails in, not the case arising.
    ///
    /// `require_risk_state_continuity` re-derives this independently and
    /// treats the same condition as a hard error. The two stop differently
    /// because that is what each *can* do -- a runtime mid-tick has no way
    /// to return an error, and a verifier has no way to halt -- but both
    /// refuse to continue, which is the property that matters (review of
    /// pairtrade#211).
    fn benchmark_equity_usd(
        basket: Option<ArcusSpotInventory>,
        token_a_price_usd: Decimal,
        token_b_price_usd: Decimal,
    ) -> Option<Decimal> {
        let basket = basket?;
        Some(
            basket
                .checked_value_usd(token_a_price_usd, token_b_price_usd)
                .unwrap_or(Decimal::MAX),
        )
    }

    /// Marks both loss stops against the baseline baskets re-priced at this
    /// tick, so they measure what rotating cost rather than what the market
    /// did (bot-strategy#813; see `initial_baseline_inventory`).
    ///
    /// A useful consequence: the result no longer depends on the price
    /// *path*. The old equity-based marks did, which is why assessing the
    /// outgoing day needed a separate pre-rollover variant that referenced
    /// the previous close — an intraday gain followed by an overnight
    /// decline would otherwise net out and hide the decline. Re-pricing the
    /// basket removes that failure mode at the source: prices move the
    /// benchmark and actual equity by the same amount, so only a rotation
    /// can move the difference between them. Assessing before
    /// `update_risk_baselines` still matters, and the caller still does it,
    /// but only so the outgoing day's basket is the one being measured.
    fn risk_mark(
        &self,
        equity_usd: Decimal,
        token_a_price_usd: Decimal,
        token_b_price_usd: Decimal,
    ) -> ArcusSpotRiskMark {
        let daily_benchmark = Self::benchmark_equity_usd(
            self.state.daily_baseline_inventory,
            token_a_price_usd,
            token_b_price_usd,
        );
        let cumulative_benchmark = Self::benchmark_equity_usd(
            self.state.initial_baseline_inventory,
            token_a_price_usd,
            token_b_price_usd,
        );
        ArcusSpotRiskMark {
            equity_usd,
            daily_loss_usd: positive_loss(daily_benchmark, equity_usd),
            cumulative_loss_usd: positive_loss(cumulative_benchmark, equity_usd),
            // Reported, never halted on. This is the beta the bot carries by
            // construction; halting cannot shed it, so the number exists to
            // be watched by whoever *can* (see #772's market-beta metric).
            inventory_drawdown_usd: cumulative_benchmark
                .map(|benchmark| positive_loss(self.state.initial_equity_usd, benchmark))
                .unwrap_or(Decimal::ZERO),
        }
    }

    fn engage_risk_halt(&mut self, at: DateTime<Utc>, mark: ArcusSpotRiskMark) {
        if self.state.risk_halt.is_some() {
            return;
        }
        let candidate = if mark.daily_loss_usd >= self.config.daily_loss_limit_usd {
            Some((
                ArcusSpotRiskHaltKind::DailyLoss,
                mark.daily_loss_usd,
                self.config.daily_loss_limit_usd,
            ))
        } else if mark.cumulative_loss_usd >= self.config.cumulative_loss_limit_usd {
            Some((
                ArcusSpotRiskHaltKind::CumulativeLoss,
                mark.cumulative_loss_usd,
                self.config.cumulative_loss_limit_usd,
            ))
        } else {
            None
        };
        if let Some((kind, loss_usd, limit_usd)) = candidate {
            self.state.risk_halt = Some(ArcusSpotRiskHalt {
                kind,
                engaged_at: at,
                equity_usd: mark.equity_usd,
                loss_usd,
                limit_usd,
            });
        }
    }
}

fn capture_payload<'a, T>(
    capture: &'a ArcusSpotCapture<T>,
    name: &str,
) -> Result<&'a T, ArcusSpotHold> {
    match capture {
        ArcusSpotCapture::Success { observation } => Ok(observation),
        ArcusSpotCapture::Error { error } => Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{name} failed: {}", error.message),
        )),
    }
}

fn find_token(
    tokens: &dex_connector::ArcusSpotObservation<Vec<ArcusSpotToken>>,
    symbol: &str,
    chain_id: u64,
) -> Result<ArcusSpotToken, ArcusSpotHold> {
    let token = tokens
        .payload
        .iter()
        .find(|token| token.symbol.eq_ignore_ascii_case(symbol))
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!("verified token metadata missing for {symbol}"),
            )
        })?;
    if token.chain_id != chain_id || !token.verified {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "token {} is not verified on chain {}",
                token.symbol, chain_id
            ),
        ));
    }
    if token.decimals > 28 {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "token {} decimals {} exceed replay precision",
                token.symbol, token.decimals
            ),
        ));
    }
    Ok(token.clone())
}

fn find_reference_price(
    overview: &dex_connector::ArcusSpotObservation<Vec<ArcusSpotOverviewEntry>>,
    token: &ArcusSpotToken,
) -> Result<Decimal, ArcusSpotHold> {
    let entry = overview
        .payload
        .iter()
        .find(|entry| entry.ticker.eq_ignore_ascii_case(&token.symbol))
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!("reference price missing for {}", token.symbol),
            )
        })?;
    if !entry.contract_address.eq_ignore_ascii_case(&token.address) {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("reference address mismatch for {}", token.symbol),
        ));
    }
    entry
        .quote
        .price
        .filter(|price| *price > Decimal::ZERO)
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!(
                    "reference price is absent or non-positive for {}",
                    token.symbol
                ),
            )
        })
}

fn validate_recorded_reference(
    label: &str,
    recorded: Option<&str>,
    expected: Decimal,
) -> Result<(), ArcusSpotHold> {
    let recorded = recorded.ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{label} recorder reference is absent"),
        )
    })?;
    let recorded = Decimal::from_str(recorded).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("invalid {label} recorder reference: {error}"),
        )
    })?;
    if recorded != expected {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{label} recorder reference {recorded} does not match {expected}"),
        ));
    }
    Ok(())
}

fn validate_route(
    route: &ArcusSpotRouteObservation,
    sell_token: &ArcusSpotToken,
    buy_token: &ArcusSpotToken,
) -> Result<(), ArcusSpotHold> {
    if route.chain_id != sell_token.chain_id
        || !route.sell_symbol.eq_ignore_ascii_case(&sell_token.symbol)
        || !route.buy_symbol.eq_ignore_ascii_case(&buy_token.symbol)
        || !route.sell_token.eq_ignore_ascii_case(&sell_token.address)
        || !route.buy_token.eq_ignore_ascii_case(&buy_token.address)
    {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::RouteUnavailable,
            "route token identity does not match verified metadata",
        ));
    }
    Ok(())
}

/// Validates one route leg's token identity, that its recommended quote
/// actually echoes the requested sell amount, and that its quote receipt is
/// neither in the future nor stale relative to `evaluation_time`. Used both
/// to pre-validate every leg of a recorder row up front and to re-check the
/// specific leg a rotation plan ends up selecting.
fn validate_route_leg(
    route: &ArcusSpotRouteObservation,
    sell_token: &ArcusSpotToken,
    buy_token: &ArcusSpotToken,
    evaluation_time: DateTime<Utc>,
    max_quote_age_secs: i64,
) -> Result<(), ArcusSpotHold> {
    validate_route(route, sell_token, buy_token)?;
    let quote = route
        .response
        .payload
        .recommended_quote()
        .map_err(|error| {
            ArcusSpotHold::new(ArcusSpotHoldCode::RouteUnavailable, error.to_string())
        })?;
    if quote.sell_amount != route.sell_amount {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::RouteUnavailable,
            "recommended quote sell amount does not match route request",
        ));
    }
    let quote_age_ms = evaluation_time
        .signed_duration_since(route.response.received_at)
        .num_milliseconds();
    if quote_age_ms < 0 {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "quote receipt is later than evaluation time",
        ));
    }
    let max_quote_age_ms = max_quote_age_secs.saturating_mul(1_000);
    if quote_age_ms > max_quote_age_ms {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::StaleQuote,
            format!("quote age {quote_age_ms}ms exceeds {max_quote_age_ms}ms"),
        ));
    }
    Ok(())
}

/// The raw token amount the recorder should have requested for `notional_usd`
/// at `reference_price_usd`, replicating dex-connector's
/// `notional_to_raw_amount` (USD / price, scaled to `decimals`, truncated
/// toward zero) exactly. Used to cross-check a row's self-reported
/// `requested_sell_amount` rather than trusting it outright.
fn expected_raw_notional_amount(
    notional_usd: Decimal,
    reference_price_usd: Decimal,
    decimals: u32,
) -> Result<Decimal, ArcusSpotHold> {
    if notional_usd <= Decimal::ZERO || reference_price_usd <= Decimal::ZERO {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "notional and reference price must be positive",
        ));
    }
    let raw_scale = 10_i128.checked_pow(decimals).ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("token decimals {decimals} exceed the replay Decimal range"),
        )
    })?;
    let scale = Decimal::try_from_i128_with_scale(raw_scale, 0).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("token decimals {decimals} exceed the replay Decimal range: {error}"),
        )
    })?;
    notional_usd
        .checked_div(reference_price_usd)
        .and_then(|quantity| quantity.checked_mul(scale))
        .map(|value| value.round_dp_with_strategy(0, RoundingStrategy::ToZero))
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "USD notional exceeds the replay Decimal range",
            )
        })
}

/// A row is selected from the snapshot solely by its `notional_usd` label
/// (see `snapshot_context`), but the quantity actually traded comes from
/// `forward.sell_amount`. A malformed or mis-joined row can carry a label
/// that does not match its own embedded route, defeating the configured
/// notional limit; cross-check the row's self-reported requested amount
/// against both the forward route's actual sell amount and the amount
/// independently recomputed from the notional and reference price.
fn verify_requested_notional_amount(
    row: &ArcusSpotRoundTripRecord,
    forward: &ArcusSpotRouteObservation,
    notional_usd: Decimal,
    sell_reference_price_usd: Decimal,
    sell_token: &ArcusSpotToken,
) -> Result<(), ArcusSpotHold> {
    let requested = row.requested_sell_amount.as_deref().ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "requested_sell_amount is absent",
        )
    })?;
    if requested != forward.sell_amount {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "requested_sell_amount does not match the forward route sellAmount",
        ));
    }
    let requested_decimal = Decimal::from_str(requested).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("requested_sell_amount is invalid: {error}"),
        )
    })?;
    let expected =
        expected_raw_notional_amount(notional_usd, sell_reference_price_usd, sell_token.decimals)?;
    if requested_decimal != expected {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "requested_sell_amount {requested_decimal} does not match {expected} raw units \
                 expected for notional {notional_usd} at price {sell_reference_price_usd}"
            ),
        ));
    }
    Ok(())
}

/// A gross data-sanity bound on the reverse leg's USD value, not a slippage
/// tolerance: normal round-trip costs are already capped in bps by
/// `max_all_in_round_trip_cost_bps`. This only needs to be loose enough to
/// tolerate ordinary price movement between the forward and reverse legs
/// while catching amounts wrong by an order of magnitude (wrong token,
/// decimal error, unit mismatch) that would otherwise size a rotation far
/// outside the configured notional while still linking correctly to the
/// forward leg.
fn verify_reverse_notional_bound(
    reverse: &ArcusSpotRouteObservation,
    notional_usd: Decimal,
    reference_price_usd: Decimal,
    sell_token: &ArcusSpotToken,
) -> Result<(), ArcusSpotHold> {
    let reverse_quantity = raw_amount_to_quantity(&reverse.sell_amount, sell_token.decimals)
        .map_err(|detail| ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail))?;
    let reverse_notional_usd = reverse_quantity
        .checked_mul(reference_price_usd)
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "reverse leg notional exceeds Decimal range",
            )
        })?;
    let deviation = Decimal::new(5, 1); // 50%
                                        // `Mul` panics on overflow rather than returning an error; a
                                        // pathologically large (but otherwise validly-configured) notional_usd
                                        // must not be able to crash a replay this way (Codex P2 follow-up,
                                        // pairtrade#177).
    let floor = notional_usd
        .checked_mul(Decimal::ONE - deviation)
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "reverse notional floor exceeds Decimal range",
            )
        })?;
    let ceiling = notional_usd
        .checked_mul(Decimal::ONE + deviation)
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "reverse notional ceiling exceeds Decimal range",
            )
        })?;
    if reverse_notional_usd < floor || reverse_notional_usd > ceiling {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "reverse leg notional {reverse_notional_usd} USD is outside [{floor}, {ceiling}] \
                 for configured notional {notional_usd}"
            ),
        ));
    }
    Ok(())
}

/// Independently derives the round-trip cost from the forward and reverse
/// recommended quote amounts, and rejects the row unless that recomputation
/// agrees with what the recorder self-reported. `row.forward`/`row.reverse`
/// are assumed present (callers already gate on that).
///
/// A row's `optimistic_round_trip_loss_bps` is only a valid cost signal if
/// its two legs actually chain: the reverse route must have been sized off
/// the forward leg's recommended output, and the reported return amount must
/// match what the reverse leg's recommended quote actually returns. Without
/// checking this, a row with a mismatched (e.g. stale or mis-joined) reverse
/// leg can report an arbitrarily cheap loss while the real recommended
/// amounts imply a much larger one, letting both read-only plans and replay
/// fills pass the cost gate on incorrect risk numbers.
fn verify_round_trip_linkage_and_loss(
    row: &ArcusSpotRoundTripRecord,
) -> Result<Decimal, ArcusSpotHold> {
    let forward = row
        .forward
        .as_ref()
        .expect("caller has already verified forward is present");
    let reverse = row
        .reverse
        .as_ref()
        .expect("caller has already verified reverse is present");
    let forward_quote = forward
        .response
        .payload
        .recommended_quote()
        .map_err(|error| {
            ArcusSpotHold::new(ArcusSpotHoldCode::RouteUnavailable, error.to_string())
        })?;
    let reverse_quote = reverse
        .response
        .payload
        .recommended_quote()
        .map_err(|error| {
            ArcusSpotHold::new(ArcusSpotHoldCode::RouteUnavailable, error.to_string())
        })?;
    if forward_quote.buy_amount != reverse.sell_amount {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "reverse route sellAmount does not match the forward recommended buyAmount",
        ));
    }
    if let Some(recorded_return) = row.optimistic_return_amount.as_deref() {
        if recorded_return != reverse_quote.buy_amount {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "recorded optimistic return amount does not match the reverse recommended buyAmount",
            ));
        }
    }
    let start = Decimal::from_str(&forward.sell_amount).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("forward sellAmount is invalid: {error}"),
        )
    })?;
    if start <= Decimal::ZERO {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "forward sellAmount must be positive",
        ));
    }
    let returned = Decimal::from_str(&reverse_quote.buy_amount).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("reverse recommended buyAmount is invalid: {error}"),
        )
    })?;
    let recomputed = start
        .checked_sub(returned)
        .and_then(|loss| loss.checked_div(start))
        .and_then(|ratio| ratio.checked_mul(Decimal::from(10_000)))
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "round-trip loss exceeds Decimal range",
            )
        })?;
    let recorded = parse_positive_or_zero(
        "optimistic_round_trip_loss_bps",
        row.optimistic_round_trip_loss_bps.as_deref(),
    )?;
    if recomputed != recorded {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "recorded round-trip loss {recorded} bps does not match {recomputed} bps \
                 recomputed from the forward/reverse route amounts"
            ),
        ));
    }
    Ok(recomputed)
}

/// Shared by `apply_confirmed_live_fill` (post-fill commit) and
/// `ArcusSpotRuntime::validate_plan_consistent_with_state` (pre-dispatch
/// check): only these regime/trigger/direction combinations can ever be
/// committed without corrupting the runtime's entry/exit state machine.
fn require_fill_consistent_with_regime(
    regime: ArcusSpotRegime,
    trigger: ArcusSpotRotationTrigger,
    direction: ArcusSpotDirection,
) -> Result<(), String> {
    match (regime, trigger, direction) {
        (
            ArcusSpotRegime::Neutral,
            ArcusSpotRotationTrigger::EntrySignal,
            ArcusSpotDirection::TokenAToTokenB | ArcusSpotDirection::TokenBToTokenA,
        )
        | (
            ArcusSpotRegime::RotatedAToB,
            ArcusSpotRotationTrigger::MeanReversionExit | ArcusSpotRotationTrigger::MaxHoldExit,
            ArcusSpotDirection::TokenBToTokenA,
        )
        | (
            ArcusSpotRegime::RotatedBToA,
            ArcusSpotRotationTrigger::MeanReversionExit | ArcusSpotRotationTrigger::MaxHoldExit,
            ArcusSpotDirection::TokenAToTokenB,
        ) => Ok(()),
        other => Err(format!(
            "fill is inconsistent with runtime state: {other:?}"
        )),
    }
}

fn parse_positive_or_zero(field: &str, value: Option<&str>) -> Result<Decimal, ArcusSpotHold> {
    let value = value.ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{field} is absent"),
        )
    })?;
    let parsed = Decimal::from_str(value).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{field} is invalid: {error}"),
        )
    })?;
    if parsed < Decimal::ZERO {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{field} cannot be negative"),
        ));
    }
    Ok(parsed)
}

pub(crate) fn raw_amount_to_quantity(raw: &str, decimals: u32) -> Result<Decimal, String> {
    let raw = raw.trim();
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "raw token amount is not an unsigned integer: {raw:?}"
        ));
    }
    let digits = raw.trim_start_matches('0');
    if digits.is_empty() {
        return Err("raw token amount must be positive".to_string());
    }
    let decimals = decimals as usize;
    let rendered = if decimals == 0 {
        digits.to_string()
    } else if digits.len() <= decimals {
        format!("0.{}{}", "0".repeat(decimals - digits.len()), digits)
    } else {
        let split = digits.len() - decimals;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Decimal::from_str(&rendered)
        .map_err(|error| format!("raw token amount {raw:?} exceeds replay precision: {error}"))
}

/// Inverse of `raw_amount_to_quantity`: renders a token quantity back to the
/// integer raw-unit string a route would carry. Used by
/// `live_executor::require_raw_matches_decimal_quantity` to cross-check a
/// plan's raw and decimal amounts against each other before dispatch
/// (Codex P1 follow-up, pairtrade#181).
///
/// Rejects a quantity that is not exactly representable at `decimals`
/// instead of rounding it: silently truncating (e.g. quantity `1.9` at
/// `decimals=0`) would let a raw amount that discards the fractional part
/// pass the cross-check it exists to enforce, corrupting inventory by the
/// truncated amount once the wallet actually sells the raw units (Codex P1
/// follow-up, pairtrade#181).
pub(crate) fn quantity_to_raw_amount(quantity: Decimal, decimals: u32) -> Result<String, String> {
    if quantity < Decimal::ZERO {
        return Err(format!("quantity {quantity} must be non-negative"));
    }
    let raw_scale = 10_i128
        .checked_pow(decimals)
        .ok_or_else(|| format!("token decimals {decimals} exceed the replay Decimal range"))?;
    let scale = Decimal::try_from_i128_with_scale(raw_scale, 0).map_err(|error| {
        format!("token decimals {decimals} exceed the replay Decimal range: {error}")
    })?;
    let raw = quantity
        .checked_mul(scale)
        .ok_or_else(|| format!("quantity {quantity} exceeds the replay Decimal range"))?;
    if raw.fract() != Decimal::ZERO {
        return Err(format!(
            "quantity {quantity} is not exactly representable at {decimals} decimals \
             (scaled value {raw} has a fractional raw unit)"
        ));
    }
    Ok(raw.trunc().to_string())
}

fn relative_log_price(price_a: Decimal, price_b: Decimal) -> Result<f64, String> {
    let price_a = price_a
        .to_f64()
        .ok_or_else(|| "token A reference cannot be represented as f64".to_string())?;
    let price_b = price_b
        .to_f64()
        .ok_or_else(|| "token B reference cannot be represented as f64".to_string())?;
    let value = (price_a / price_b).ln();
    if !value.is_finite() {
        return Err("relative log price is non-finite".to_string());
    }
    Ok(value)
}

/// Counts observations that add a meaningfully different price ratio to the
/// signal window. Closed markets can emit the exact same reference prices on
/// every scheduled collection; treating all of those repeats as independent
/// samples lets a 96-element window collapse to one value and makes the
/// second post-reopen tick look like an extreme outlier. Requiring the normal
/// minimum sample count in price *changes* keeps entry fail-closed until the
/// market has supplied enough fresh information again, while the raw rolling
/// window remains time-based and continues to age out pre-close observations.
fn informative_signal_sample_count(history: &[f64]) -> usize {
    let Some(first) = history.first() else {
        return 0;
    };
    let mut count = 1;
    let mut last_informative = *first;
    for value in &history[1..] {
        if (*value - last_informative).abs() > SIGNAL_FLAT_EPSILON {
            count += 1;
            last_informative = *value;
        }
    }
    count
}

fn z_score(history: &[f64], current: f64, minimum_samples: usize) -> Option<f64> {
    if history.len() < minimum_samples || informative_signal_sample_count(history) < minimum_samples
    {
        return None;
    }
    let mean = history.iter().sum::<f64>() / history.len() as f64;
    let variance = history
        .iter()
        .map(|value| {
            let deviation = value - mean;
            deviation * deviation
        })
        .sum::<f64>()
        / history.len() as f64;
    let standard_deviation = variance.sqrt();
    if !standard_deviation.is_finite() || standard_deviation <= SIGNAL_FLAT_EPSILON {
        return None;
    }
    let score = (current - mean) / standard_deviation;
    score.is_finite().then_some(score)
}

fn inventory_imbalance_fraction(
    inventory: ArcusSpotInventory,
    price_a: Decimal,
    price_b: Decimal,
) -> Result<Decimal, ArcusSpotHold> {
    let value_a = inventory.token_a.checked_mul(price_a).ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InventoryImbalance,
            "token A valuation exceeds Decimal range",
        )
    })?;
    let value_b = inventory.token_b.checked_mul(price_b).ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InventoryImbalance,
            "token B valuation exceeds Decimal range",
        )
    })?;
    let total = value_a.checked_add(value_b).ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InventoryImbalance,
            "total valuation exceeds Decimal range",
        )
    })?;
    if total <= Decimal::ZERO {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InventoryImbalance,
            "total inventory value must be positive",
        ));
    }
    let difference = if value_a >= value_b {
        value_a - value_b
    } else {
        value_b - value_a
    };
    difference.checked_div(total).ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InventoryImbalance,
            "inventory imbalance exceeds Decimal range",
        )
    })
}

fn inventory_math_error(operation: &str) -> ArcusSpotHold {
    ArcusSpotHold::new(
        ArcusSpotHoldCode::InvalidSnapshot,
        format!("inventory operation exceeds Decimal range: {operation}"),
    )
}

fn positive_loss(baseline: Option<Decimal>, current: Decimal) -> Decimal {
    baseline
        .and_then(|baseline| baseline.checked_sub(current))
        .filter(|loss| *loss > Decimal::ZERO)
        .unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use dex_connector::ArcusSpotPair;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn config() -> ArcusSpotRuntimeConfig {
        ArcusSpotRuntimeConfig {
            mode: ArcusSpotRuntimeMode::ReplaySimulation,
            chain_id: 4663,
            pair: ArcusSpotPair {
                sell_symbol: "NVDA".to_string(),
                buy_symbol: "AMD".to_string(),
            },
            notional_usd: Decimal::from(5),
            initial_inventory: ArcusSpotInventory {
                token_a: Decimal::ONE,
                token_b: Decimal::ONE,
            },
            inventory_floors: ArcusSpotInventory {
                token_a: Decimal::new(1, 1),
                token_b: Decimal::new(1, 1),
            },
            max_rotation_fraction: Decimal::ONE,
            signal_window_samples: 3,
            min_signal_samples: 2,
            entry_z_score: 1.0,
            exit_z_score: 0.25,
            max_quote_age_secs: 30,
            max_hold_secs: 3_600,
            max_all_in_round_trip_cost_bps: Decimal::from(100),
            gas_buffer_bps: Decimal::from(5),
            settlement_buffer_bps: Decimal::from(5),
            max_inventory_imbalance_fraction: Decimal::ONE,
            daily_loss_limit_usd: Decimal::from(2),
            cumulative_loss_limit_usd: Decimal::from(10),
        }
    }

    fn context(received_at: DateTime<Utc>, loss_bps: Decimal) -> SnapshotContext {
        let token_a = ArcusSpotToken {
            chain_id: 4663,
            symbol: "NVDA".to_string(),
            name: "NVIDIA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
            source: Some("server".to_string()),
            category: Some("stock".to_string()),
            verified: true,
            wrapped_token_address: None,
            extra: BTreeMap::new(),
        };
        let token_b = ArcusSpotToken {
            chain_id: 4663,
            symbol: "AMD".to_string(),
            name: "AMD".to_string(),
            address: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            decimals: 18,
            source: Some("server".to_string()),
            category: Some("stock".to_string()),
            verified: true,
            wrapped_token_address: None,
            extra: BTreeMap::new(),
        };
        let forward_requested_at = received_at - Duration::seconds(1);
        let reverse_received_at = received_at + Duration::seconds(1);
        let reverse_requested_at = received_at;
        let row: ArcusSpotRoundTripRecord = serde_json::from_value(json!({
            "pair": {"sell_symbol": "NVDA", "buy_symbol": "AMD"},
            "notional_usd": "5",
            "sell_reference_price_usd": "200",
            "buy_reference_price_usd": "100",
            "requested_sell_amount": "25000000000000000",
            "forward": {
                "chain_id": 4663,
                "sell_symbol": "NVDA",
                "buy_symbol": "AMD",
                "sell_token": token_a.address,
                "buy_token": token_b.address,
                "sell_amount": "25000000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": "49000000000000000",
                            "sellAmount": "25000000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": forward_requested_at,
                    "received_at": received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "reverse": {
                "chain_id": 4663,
                "sell_symbol": "AMD",
                "buy_symbol": "NVDA",
                "sell_token": token_b.address,
                "buy_token": token_a.address,
                "sell_amount": "49000000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": "24800000000000000",
                            "sellAmount": "49000000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": reverse_requested_at,
                    "received_at": reverse_received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "optimistic_return_amount": "24800000000000000",
            "optimistic_round_trip_loss_bps": loss_bps.to_string(),
            "errors": []
        }))
        .unwrap();
        SnapshotContext {
            token_a,
            token_b,
            token_a_price_usd: Decimal::from(200),
            token_b_price_usd: Decimal::from(100),
            row,
            verified_round_trip_loss_bps: loss_bps,
        }
    }

    fn reverse_context(received_at: DateTime<Utc>, loss_bps: Decimal) -> SnapshotContext {
        let base = context(received_at, loss_bps);
        let forward_requested_at = received_at - Duration::seconds(1);
        let reverse_requested_at = received_at;
        let reverse_received_at = received_at + Duration::seconds(1);
        let row = serde_json::from_value(json!({
            "pair": {"sell_symbol": "AMD", "buy_symbol": "NVDA"},
            "notional_usd": "5",
            "sell_reference_price_usd": "100",
            "buy_reference_price_usd": "200",
            "requested_sell_amount": "50000000000000000",
            "forward": {
                "chain_id": 4663,
                "sell_symbol": "AMD",
                "buy_symbol": "NVDA",
                "sell_token": base.token_b.address,
                "buy_token": base.token_a.address,
                "sell_amount": "50000000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": "24500000000000000",
                            "sellAmount": "50000000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": forward_requested_at,
                    "received_at": received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "reverse": {
                "chain_id": 4663,
                "sell_symbol": "NVDA",
                "buy_symbol": "AMD",
                "sell_token": base.token_a.address,
                "buy_token": base.token_b.address,
                "sell_amount": "24500000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": "49600000000000000",
                            "sellAmount": "24500000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": reverse_requested_at,
                    "received_at": reverse_received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "optimistic_return_amount": "49600000000000000",
            "optimistic_round_trip_loss_bps": loss_bps.to_string(),
            "errors": []
        }))
        .unwrap();
        SnapshotContext { row, ..base }
    }

    fn event_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 27, 0, 0, 10).unwrap()
    }

    #[test]
    fn plan_uses_exact_recorded_amounts_and_preserves_floor() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let context = context(event_time() - Duration::seconds(2), Decimal::from(20));
        let plan = runtime
            .build_plan(
                &context,
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert_eq!(plan.sell_quantity, Decimal::new(25, 3));
        assert_eq!(plan.buy_quantity, Decimal::new(49, 3));
        assert_eq!(plan.predicted_inventory.token_a, Decimal::new(975, 3));
        assert_eq!(plan.predicted_inventory.token_b, Decimal::new(1049, 3));
        assert_eq!(plan.all_in_round_trip_cost_bps, Decimal::from(30));
    }

    #[test]
    fn reverse_direction_entry_rejects_a_mismatched_forward_cycle() {
        // context() only quotes A-to-B-to-A. Even though build_plan now
        // accepts reverse entries, route identity must prevent an A-to-B
        // forward leg from being reused as the B-to-A entry.
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::RouteUnavailable);
    }

    #[test]
    fn reverse_direction_entry_uses_its_own_forward_cycle_and_cost() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let plan = runtime
            .build_plan(
                &reverse_context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert_eq!(plan.sell_symbol, "AMD");
        assert_eq!(plan.buy_symbol, "NVDA");
        assert_eq!(plan.sell_quantity, Decimal::new(5, 2));
        assert_eq!(plan.buy_quantity, Decimal::new(245, 4));
        assert_eq!(plan.predicted_inventory.token_a, Decimal::new(10245, 4));
        assert_eq!(plan.predicted_inventory.token_b, Decimal::new(95, 2));
        assert_eq!(plan.all_in_round_trip_cost_bps, Decimal::from(30));
    }

    #[test]
    fn reverse_direction_exit_still_uses_the_row_round_trip_cost() {
        // Unlike the entry case above, exiting a TokenAToTokenB position via
        // the reverse leg (regime RotatedAToB -> exit direction
        // TokenBToTokenA) completes exactly the A-to-B-to-A cycle this row
        // quotes, so it must not be refused.
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MeanReversionExit,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert_eq!(plan.all_in_round_trip_cost_bps, Decimal::from(30));
    }

    #[test]
    fn reverse_cycle_exit_uses_that_cycles_reverse_leg() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let plan = runtime
            .build_plan(
                &reverse_context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::MeanReversionExit,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert_eq!(plan.sell_symbol, "NVDA");
        assert_eq!(plan.buy_symbol, "AMD");
        assert_eq!(plan.sell_quantity, Decimal::new(245, 4));
        assert_eq!(plan.buy_quantity, Decimal::new(496, 4));
    }

    #[test]
    fn token_floor_blocks_before_inventory_mutation() {
        let mut cfg = config();
        cfg.inventory_floors.token_a = Decimal::new(99, 2);
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InventoryFloor);
        assert_eq!(runtime.state.inventory, runtime.config.initial_inventory);
    }

    #[test]
    fn exit_exceeding_the_sellable_residual_above_the_floor_is_rejected() {
        // A prior partial unwind has shrunk token_b's sellable balance
        // (0.02 above the 0.1 floor) below what this snapshot's
        // fixed-notional reverse quote offers to sell (0.049). An earlier
        // version scaled buy_quantity linearly down to the sellable amount,
        // but that synthesizes a fill price the venue never actually
        // quoted for that smaller size -- under price impact, fixed fees,
        // minimum amounts, or tiered pricing the real executable result
        // can differ materially (Codex P1 follow-up, pairtrade#177). The
        // exit must instead remain unfilled rather than invent one.
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let inventory = ArcusSpotInventory {
            token_a: Decimal::ONE,
            token_b: Decimal::new(12, 2), // 0.12: sellable = 0.12 - 0.1 = 0.02
        };
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MeanReversionExit,
                event_time(),
                inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InventoryFloor);
    }

    #[test]
    fn exit_at_exactly_the_floor_with_no_residual_is_rejected() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let inventory = ArcusSpotInventory {
            token_a: Decimal::ONE,
            token_b: Decimal::new(1, 1), // exactly at the 0.1 floor: sellable = 0
        };
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MeanReversionExit,
                event_time(),
                inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InventoryFloor);
    }

    #[test]
    fn stale_quote_and_non_trading_buffers_are_hard_gates() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let stale = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(31), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap_err();
        assert_eq!(stale.code, ArcusSpotHoldCode::StaleQuote);

        let costly = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(95)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap_err();
        assert_eq!(costly.code, ArcusSpotHoldCode::CostLimit);
    }

    #[test]
    fn cost_limit_is_not_applied_to_exits() {
        // Unlike the EntrySignal case just above (loss_bps=95 exceeds
        // max_all_in_round_trip_cost_bps=100 once gas/settlement buffers are
        // added), the same costly context must not block an exit: otherwise
        // a round-trip cost that rises above the limit while already
        // rotated would keep rejecting MaxHoldExit on every later snapshot,
        // making max_hold_secs not actually a maximum.
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(95)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MaxHoldExit,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert_eq!(plan.all_in_round_trip_cost_bps, Decimal::from(105));
    }

    #[test]
    fn loss_halt_is_sticky() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let baseline_inventory = runtime.state.inventory;
        runtime.update_risk_baselines(event_time(), Decimal::from(300), baseline_inventory);
        let mark = runtime.risk_mark(Decimal::from(297), Decimal::from(200), Decimal::from(100));
        runtime.engage_risk_halt(event_time(), mark);
        let halt = runtime.state.risk_halt.clone().unwrap();
        assert_eq!(halt.kind, ArcusSpotRiskHaltKind::DailyLoss);

        runtime.engage_risk_halt(
            event_time() + Duration::seconds(1),
            ArcusSpotRiskMark {
                equity_usd: Decimal::from(100),
                daily_loss_usd: Decimal::from(200),
                cumulative_loss_usd: Decimal::from(200),
                inventory_drawdown_usd: Decimal::ZERO,
            },
        );
        assert_eq!(runtime.state.risk_halt.unwrap(), halt);
    }

    #[test]
    fn max_hold_exit_fires_even_without_a_z_score() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.last_rotation_at = Some(event_time());
        let signal = runtime.rotation_signal(
            None,
            event_time() + Duration::seconds(runtime.config.max_hold_secs),
            ArcusSpotRegime::RotatedAToB,
        );
        assert_eq!(
            signal,
            Some((
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MaxHoldExit
            ))
        );

        let signal = runtime.rotation_signal(
            None,
            event_time() + Duration::seconds(runtime.config.max_hold_secs),
            ArcusSpotRegime::RotatedBToA,
        );
        assert_eq!(
            signal,
            Some((
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::MaxHoldExit
            ))
        );
    }

    #[test]
    fn rotated_regime_without_a_z_score_stays_in_warmup_before_max_hold() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let signal = runtime.rotation_signal(None, event_time(), ArcusSpotRegime::RotatedAToB);
        assert_eq!(signal, None);
    }

    #[test]
    fn neutral_regime_without_a_z_score_never_enters() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let signal = runtime.rotation_signal(None, event_time(), ArcusSpotRegime::Neutral);
        assert_eq!(signal, None);
    }

    #[test]
    fn current_tick_is_scored_against_prior_history_only() {
        let history = [1.0, 1.1];
        let score = z_score(&history, 1.5, 2).unwrap();
        assert!(score > 5.0);
        assert_eq!(history, [1.0, 1.1]);
    }

    #[test]
    fn closed_market_flat_window_blocks_first_and_second_reopen_ticks() {
        let mut cfg = config();
        cfg.signal_window_samples = 96;
        cfg.min_signal_samples = 32;
        cfg.entry_z_score = 2.5;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();

        // This is the live checkpoint shape observed after a closed market:
        // every scheduled collection contributed the exact same ratio.
        let flat_price = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![flat_price; 96];

        let first_time = event_time();
        let first = runtime.step_at(
            &snapshot_with_route_unavailable(first_time, "201", "100"),
            first_time,
        );
        assert_eq!(first.z_score, None);
        assert!(matches!(
            first.decision,
            ArcusSpotDecision::Observe { hold }
                if hold.code == ArcusSpotHoldCode::RouteUnavailable
        ));
        assert_eq!(
            informative_signal_sample_count(&runtime.state.relative_log_price_history),
            2
        );

        let second_time = first_time + Duration::seconds(1);
        let second = runtime.step_at(
            &snapshot_with_route_unavailable(second_time, "202", "100"),
            second_time,
        );
        assert_eq!(second.z_score, None);
        assert!(matches!(
            second.decision,
            ArcusSpotDecision::Observe { hold }
                if hold.code == ArcusSpotHoldCode::RouteUnavailable
        ));
        assert_eq!(
            informative_signal_sample_count(&runtime.state.relative_log_price_history),
            3
        );
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn normal_low_volatility_history_remains_eligible_for_scoring() {
        // The total move is only 0.0032 bps in log-price terms, but every
        // sample adds real information above the numerical flatness floor.
        let history = (0..32)
            .map(|index| 2.0 + f64::from(index) * 1e-8)
            .collect::<Vec<_>>();
        assert_eq!(informative_signal_sample_count(&history), 32);

        let score = z_score(&history, 2.0 + 32.0e-8, 32)
            .expect("normal low-volatility history must remain scoreable");
        assert!(score.is_finite());
        assert!(score > 0.0);
    }

    #[test]
    fn informative_history_guard_preserves_mean_reversion_exit() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let current = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![current - 0.01, current + 0.01];
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at = Some(event_time() - Duration::seconds(1));

        let event = runtime.step_at(&snapshot_with_valid_row(event_time()), event_time());
        assert!(event
            .z_score
            .is_some_and(|score| score.abs() < f64::EPSILON));
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MeanReversionExit);
            }
            other => panic!("expected mean-reversion exit, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn negative_entry_signal_uses_the_independent_reverse_cycle() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let current = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![current + 0.01, current + 0.02];

        let event = runtime.step_at(
            &snapshot_with_bidirectional_rows(event_time()),
            event_time(),
        );
        assert!(event.z_score.is_some_and(|score| score < -1.0));
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.direction, ArcusSpotDirection::TokenBToTokenA);
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::EntrySignal);
                assert_eq!(plan.sell_symbol, "AMD");
                assert_eq!(plan.buy_symbol, "NVDA");
                assert_eq!(plan.sell_quantity, Decimal::new(5, 2));
                assert_eq!(plan.buy_quantity, Decimal::new(245, 4));
            }
            other => panic!("expected a reverse-direction simulated fill, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedBToA);
    }

    #[test]
    fn negative_entry_signal_holds_when_the_reverse_cycle_is_missing() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let current = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![current + 0.01, current + 0.02];

        let event = runtime.step_at(&snapshot_with_valid_row(event_time()), event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::RouteUnavailable);
                assert!(hold.detail.contains("AMD/NVDA"));
            }
            other => panic!("expected missing reverse-cycle hold, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn negative_entry_signal_holds_when_the_reverse_cycle_is_stale() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let current = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![current + 0.01, current + 0.02];
        let mut snapshot = snapshot_with_valid_row(event_time());
        snapshot.round_trips.push(
            reverse_context(
                event_time() - Duration::seconds(runtime.config.max_quote_age_secs + 2),
                Decimal::from(80),
            )
            .row,
        );

        let event = runtime.step_at(&snapshot, event_time());
        assert!(matches!(
            event.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::StaleQuote
        ));
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn reverse_cycle_entry_still_obeys_the_all_in_cost_limit() {
        let mut cfg = config();
        cfg.max_all_in_round_trip_cost_bps = Decimal::from(85);
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let current = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![current + 0.01, current + 0.02];

        let event = runtime.step_at(
            &snapshot_with_bidirectional_rows(event_time()),
            event_time(),
        );
        assert!(matches!(
            event.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::CostLimit
        ));
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn flat_history_guard_preserves_max_hold_exit_without_a_z_score() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let flat_price = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![flat_price; 3];
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));

        let event = runtime.step_at(&snapshot_with_valid_row(event_time()), event_time());
        assert_eq!(event.z_score, None);
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MaxHoldExit);
            }
            other => panic!("expected max-hold exit with a flat history, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn raw_amount_conversion_is_exact() {
        assert_eq!(
            raw_amount_to_quantity("23969319271332694", 18).unwrap(),
            Decimal::from_str("0.023969319271332694").unwrap()
        );
        assert!(raw_amount_to_quantity("0", 18).is_err());
    }

    #[test]
    fn quantity_to_raw_amount_round_trips() {
        let quantity = raw_amount_to_quantity("23969319271332694", 18).unwrap();
        assert_eq!(
            quantity_to_raw_amount(quantity, 18).unwrap(),
            "23969319271332694"
        );
        assert_eq!(quantity_to_raw_amount(Decimal::ZERO, 6).unwrap(), "0");
        assert!(quantity_to_raw_amount(Decimal::from(-1), 6).is_err());
    }

    #[test]
    fn quantity_to_raw_amount_rejects_a_fractional_raw_unit() {
        let error = quantity_to_raw_amount(Decimal::from_str("1.9").unwrap(), 0).unwrap_err();
        assert!(
            error.contains("not exactly representable"),
            "unexpected error: {error}"
        );
    }

    fn round_trip_row(
        forward_buy_amount: &str,
        reverse_sell_amount: &str,
        reverse_buy_amount: &str,
        optimistic_return_amount: &str,
        optimistic_round_trip_loss_bps: &str,
    ) -> ArcusSpotRoundTripRecord {
        serde_json::from_value(json!({
            "pair": {"sell_symbol": "NVDA", "buy_symbol": "AMD"},
            "notional_usd": "5",
            "sell_reference_price_usd": "200",
            "buy_reference_price_usd": "100",
            "requested_sell_amount": "25000000000000000",
            "forward": {
                "chain_id": 4663,
                "sell_symbol": "NVDA",
                "buy_symbol": "AMD",
                "sell_token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                "buy_token": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                "sell_amount": "25000000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": forward_buy_amount,
                            "sellAmount": "25000000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": event_time(),
                    "received_at": event_time(),
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "reverse": {
                "chain_id": 4663,
                "sell_symbol": "AMD",
                "buy_symbol": "NVDA",
                "sell_token": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                "buy_token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                "sell_amount": reverse_sell_amount,
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": reverse_buy_amount,
                            "sellAmount": reverse_sell_amount,
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": event_time(),
                    "received_at": event_time(),
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "optimistic_return_amount": optimistic_return_amount,
            "optimistic_round_trip_loss_bps": optimistic_round_trip_loss_bps,
            "errors": []
        }))
        .unwrap()
    }

    #[test]
    fn round_trip_linkage_accepts_a_consistent_row() {
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        assert_eq!(
            verify_round_trip_linkage_and_loss(&row).unwrap(),
            Decimal::from(80)
        );
    }

    #[test]
    fn round_trip_linkage_rejects_a_reverse_leg_not_sized_off_the_forward_output() {
        // The reverse route was requested with a sellAmount that does not
        // match what the forward leg's recommended quote actually produces,
        // so the two legs do not chain into one real round trip.
        let row = round_trip_row(
            "49000000000000000",
            "48000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        let error = verify_round_trip_linkage_and_loss(&row).unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("reverse route sellAmount"));
    }

    #[test]
    fn round_trip_linkage_rejects_a_stale_recorded_return_amount() {
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24900000000000000",
            "80",
        );
        let error = verify_round_trip_linkage_and_loss(&row).unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("optimistic return amount"));
    }

    #[test]
    fn round_trip_linkage_rejects_a_loss_bps_that_understates_the_real_cost() {
        // The forward/reverse amounts imply an 80 bps loss, but the row
        // self-reports 20 bps; a cost gate trusting the reported number
        // alone would pass a round trip that is really 4x costlier.
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "20",
        );
        let error = verify_round_trip_linkage_and_loss(&row).unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("does not match"));
    }

    fn nvda_token() -> ArcusSpotToken {
        ArcusSpotToken {
            chain_id: 4663,
            symbol: "NVDA".to_string(),
            name: "NVIDIA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
            source: Some("server".to_string()),
            category: Some("stock".to_string()),
            verified: true,
            wrapped_token_address: None,
            extra: BTreeMap::new(),
        }
    }

    #[test]
    fn requested_notional_amount_accepts_a_row_whose_amount_matches_its_label() {
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        let forward = row.forward.as_ref().unwrap();
        verify_requested_notional_amount(
            &row,
            forward,
            Decimal::from(5),
            Decimal::from(200),
            &nvda_token(),
        )
        .unwrap();
    }

    #[test]
    fn requested_notional_amount_rejects_a_row_labeled_smaller_than_its_route() {
        // The row is labeled (and passed here as) a $5 notional, but its
        // requested_sell_amount reflects the $50 route a mis-joined or
        // malformed row could actually carry.
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        row.requested_sell_amount = Some("250000000000000000".to_string());
        row.forward.as_mut().unwrap().sell_amount = "250000000000000000".to_string();
        let forward = row.forward.as_ref().unwrap();
        let error = verify_requested_notional_amount(
            &row,
            forward,
            Decimal::from(5),
            Decimal::from(200),
            &nvda_token(),
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("does not match"));
    }

    #[test]
    fn requested_notional_amount_rejects_a_requested_amount_disjoint_from_the_route() {
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        row.requested_sell_amount = Some("1".to_string());
        let forward = row.forward.as_ref().unwrap();
        let error = verify_requested_notional_amount(
            &row,
            forward,
            Decimal::from(5),
            Decimal::from(200),
            &nvda_token(),
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("forward route sellAmount"));
    }

    #[test]
    fn requested_notional_amount_rejects_a_missing_requested_amount() {
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        row.requested_sell_amount = None;
        let forward = row.forward.as_ref().unwrap();
        let error = verify_requested_notional_amount(
            &row,
            forward,
            Decimal::from(5),
            Decimal::from(200),
            &nvda_token(),
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("absent"));
    }

    fn amd_token() -> ArcusSpotToken {
        ArcusSpotToken {
            chain_id: 4663,
            symbol: "AMD".to_string(),
            name: "AMD".to_string(),
            address: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            decimals: 18,
            source: Some("server".to_string()),
            category: Some("stock".to_string()),
            verified: true,
            wrapped_token_address: None,
            extra: BTreeMap::new(),
        }
    }

    #[test]
    fn reverse_notional_bound_accepts_an_ordinary_round_trip() {
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        let reverse = row.reverse.as_ref().unwrap();
        // 0.049 units at $100 = $4.90, within 50% of the $5 notional.
        verify_reverse_notional_bound(reverse, Decimal::from(5), Decimal::from(100), &amd_token())
            .unwrap();
    }

    #[test]
    fn reverse_notional_bound_rejects_an_order_of_magnitude_mismatch() {
        // 0.49 units at $100 = $49, far outside 50% of the $5 notional.
        let row = round_trip_row(
            "490000000000000000",
            "490000000000000000",
            "248000000000000000",
            "248000000000000000",
            "80",
        );
        let reverse = row.reverse.as_ref().unwrap();
        let error = verify_reverse_notional_bound(
            reverse,
            Decimal::from(5),
            Decimal::from(100),
            &amd_token(),
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("reverse leg notional"));
    }

    #[test]
    fn reverse_notional_bound_rejects_overflowing_ceiling_instead_of_panicking() {
        // `Decimal::MAX / 1.5` sits right at the edge where `notional_usd *
        // 1.5` would panic via the `Mul` operator (Codex P2 follow-up,
        // pairtrade#177); the checked_mul-based ceiling must instead return
        // an InvalidSnapshot hold.
        let row = round_trip_row(
            "490000000000000000",
            "490000000000000000",
            "248000000000000000",
            "248000000000000000",
            "80",
        );
        let reverse = row.reverse.as_ref().unwrap();
        let error =
            verify_reverse_notional_bound(reverse, Decimal::MAX, Decimal::from(100), &amd_token())
                .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(
            error.detail.contains("exceeds Decimal range"),
            "{}",
            error.detail
        );
    }

    fn snapshot_with_route_unavailable(
        collected_at: DateTime<Utc>,
        nvda_price: &str,
        amd_price: &str,
    ) -> ArcusSpotRecorderSnapshot {
        serde_json::from_value(json!({
            "schema_version": 3,
            "mode": "public_indicative_read_only",
            "chain_id": 4663,
            "collection_started_at": collected_at,
            "collection_finished_at": collected_at,
            "indexer_stats": {
                "status": "error",
                "error": {"stage": "indexer_stats", "classification": "http", "retryable": false, "message": "x"}
            },
            "token_metadata": {
                "status": "success",
                "observation": {
                    "payload": [
                        {
                            "chainId": 4663,
                            "symbol": "NVDA",
                            "name": "NVIDIA",
                            "address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                            "decimals": 18,
                            "verified": true
                        },
                        {
                            "chainId": 4663,
                            "symbol": "AMD",
                            "name": "AMD",
                            "address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                            "decimals": 18,
                            "verified": true
                        }
                    ],
                    "requested_at": collected_at,
                    "received_at": collected_at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "reference_overview": {
                "status": "success",
                "observation": {
                    "payload": [
                        {
                            "ticker": "NVDA",
                            "contractAddress": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                            "name": "NVIDIA",
                            "category": "stock",
                            "quote": {"price": nvda_price}
                        },
                        {
                            "ticker": "AMD",
                            "contractAddress": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                            "name": "AMD",
                            "category": "stock",
                            "quote": {"price": amd_price}
                        }
                    ],
                    "requested_at": collected_at,
                    "received_at": collected_at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "round_trips": []
        }))
        .unwrap()
    }

    #[test]
    fn risk_halt_engages_even_when_the_route_is_unavailable() {
        // The recorder row is entirely absent (round_trips is empty), so
        // snapshot_context() fails with RouteUnavailable. Token metadata and
        // reference prices are otherwise valid, so the marks can still be
        // taken; a loss already on the books must engage the halt here
        // rather than only on a later snapshot where a route happens to be
        // available again.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let baseline_inventory = runtime.state.inventory;
        runtime.update_risk_baselines(event_time(), Decimal::from(300), baseline_inventory);
        // An earlier rotation destroyed value: the basket now holds 0.97 of
        // token A where the baseline basket holds 1.0. At the snapshot's
        // prices (150/50) that benchmark is worth 200 and the actual
        // inventory 195.5, a $4.50 attributed loss against the $2 limit.
        // Stated as an inventory difference rather than a price move on
        // purpose -- a price move is precisely what must *not* halt.
        runtime.state.inventory.token_a = Decimal::new(97, 2);
        let snapshot = snapshot_with_route_unavailable(event_time(), "150", "50");
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::RouteUnavailable);
            }
            other => panic!("expected Observe/RouteUnavailable, got {other:?}"),
        }
        assert!(
            runtime.state().risk_halt.is_some(),
            "an attributed loss during a route outage must still engage the halt"
        );
    }

    #[test]
    fn overnight_loss_is_assessed_against_the_prior_days_baseline() {
        // Equity starts at 1*200 + 1*100 = 300 (config()'s initial
        // inventory, snapshot_with_valid_row's reference prices). Resetting
        // the daily baseline to the day's opening equity *before* marking
        // this first snapshot of the new day would compare it against
        // itself (zero loss) and silently absorb whatever was lost
        // overnight; it must instead be marked against the still-active
        // prior baseline first.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day1 = event_time();
        runtime.step_at(&snapshot_with_valid_row(day1), day1);
        assert_eq!(runtime.state().risk_halt, None);

        // Simulate an overnight loss: 200*0.985 + 100*1 = 297, a $3 loss
        // against the $300 baseline, exceeding daily_loss_limit_usd=2 (but
        // not cumulative_loss_limit_usd=10).
        runtime.state.inventory.token_a = Decimal::new(985, 3);
        let day2 = day1 + Duration::days(1);
        runtime.step_at(&snapshot_with_valid_row(day2), day2);

        let halt = runtime
            .state()
            .risk_halt
            .clone()
            .expect("overnight loss breaching the prior day's baseline must engage the halt");
        assert_eq!(halt.kind, ArcusSpotRiskHaltKind::DailyLoss);
    }

    /// The bot-strategy#813 case, and the whole point of the change: on
    /// 2026-08-18 a 4.1% NVDA/AMD down day drove the live probe's equity
    /// from $100.58 to $96.46 and engaged a $2 daily-loss halt, having
    /// never made a single swap. Holding the basket is not a strategy loss.
    #[test]
    fn a_price_collapse_without_trading_is_not_a_loss_at_any_magnitude() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day = event_time();
        runtime.step_at(&snapshot_with_valid_row(day), day);
        assert_eq!(runtime.state().risk_halt, None);

        // Halve both legs. Equity goes $300 -> $150, twenty-five times the
        // $2 daily limit and fifteen times the $10 cumulative one.
        let later = day + Duration::seconds(1);
        let crashed = snapshot_with_valid_row_at_prices(later, "100", "50");
        let event = runtime.step_at(&crashed, later);

        let risk = event.risk_before.expect("prices were markable");
        assert_eq!(risk.daily_loss_usd, Decimal::ZERO);
        assert_eq!(risk.cumulative_loss_usd, Decimal::ZERO);
        // The beta is not hidden, just not halted on.
        assert_eq!(risk.inventory_drawdown_usd, Decimal::from(150));
        assert_eq!(
            runtime.state().risk_halt,
            None,
            "market beta on an untraded basket must never engage a halt",
        );
    }

    /// A checkpoint written before the baseline baskets existed must not
    /// halt on the stale equity baselines it still carries, and must adopt
    /// baskets on its first tick so the stops work from then on.
    #[test]
    fn a_checkpoint_without_baseline_baskets_seeds_them_without_halting() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day = event_time();
        runtime.step_at(&snapshot_with_valid_row(day), day);
        // Exactly the shape a pre-#813 checkpoint deserializes into: equity
        // baselines present, baskets absent, and a price collapse since.
        runtime.state.initial_baseline_inventory = None;
        runtime.state.daily_baseline_inventory = None;

        let later = day + Duration::seconds(1);
        let crashed = snapshot_with_valid_row_at_prices(later, "100", "50");
        runtime.step_at(&crashed, later);

        assert_eq!(
            runtime.state().risk_halt,
            None,
            "an upgraded checkpoint must not halt on its stale equity baselines",
        );
        assert_eq!(
            runtime.state().daily_baseline_inventory,
            Some(runtime.state().inventory),
        );
        assert_eq!(
            runtime.state().initial_baseline_inventory,
            Some(runtime.state().inventory),
        );
    }

    #[test]
    fn a_day_that_gives_back_part_of_a_gain_is_not_a_daily_loss() {
        // Deliberate behaviour change from bot-strategy#755 review round13,
        // which compared the rollover tick against the previous day's *last
        // mark* so that a $310 peak falling to $305 registered as a $5 loss
        // even though the day was $5 up on its own opening. That guard
        // existed because equity marks are path-dependent; attributed marks
        // are not, and `daily_loss_limit_usd` is a loss limit, not a
        // drawdown limit. A day that ends net positive is not a loss, so no
        // halt engages here. A genuine peak-to-trough drawdown control would
        // be a separate limit, deliberately not introduced here.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day1 = event_time();
        runtime.step_at(&snapshot_with_valid_row(day1), day1);
        assert_eq!(runtime.state().risk_halt, None);

        // Rotations take the basket to $310: 200*1.05 + 100*1.
        runtime.state.inventory.token_a = Decimal::new(105, 2);
        let day1_later = day1 + Duration::hours(6);
        runtime.step_at(&snapshot_with_valid_row(day1_later), day1_later);
        assert_eq!(runtime.state().risk_halt, None);

        // Day 2 opens at $305, still $5 above the $300 basket it started
        // from: 200*1.025 + 100*1.
        runtime.state.inventory.token_a = Decimal::new(1025, 3);
        let day2 = day1 + Duration::days(1);
        runtime.step_at(&snapshot_with_valid_row(day2), day2);

        assert_eq!(
            runtime.state().risk_halt,
            None,
            "a net-positive day must not engage a daily-*loss* halt",
        );
    }

    /// The review finding on pairtrade#211/#212, driven through `step_at`
    /// rather than a hand-built state — which is the whole point, since the
    /// bypass only existed on the real rollover path and every guard test
    /// had built its halt by hand.
    ///
    /// A rotation destroys $3 during day 1; the day-2 rollover tick engages
    /// the halt against day 1's basket and, in that same tick, used to rebase
    /// that basket onto the still-impaired inventory. Everything that later
    /// re-derived the loss then read ~0, so the halt could be lifted having
    /// remediated nothing.
    #[test]
    fn a_rollover_halt_keeps_the_basket_its_loss_was_measured_against() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day1 = event_time();
        runtime.step_at(&snapshot_with_valid_row(day1), day1);
        let day1_basket = runtime.state().daily_baseline_inventory.unwrap();

        // 200*0.985 + 100*1 = 297 against a $300 basket.
        runtime.state.inventory.token_a = Decimal::new(985, 3);
        let day2 = day1 + Duration::days(1);
        runtime.step_at(&snapshot_with_valid_row(day2), day2);

        assert!(runtime.state().risk_halt.is_some(), "the halt must engage");
        assert_eq!(
            runtime.state().daily_baseline_inventory,
            Some(day1_basket),
            "the basket the halt was measured against must survive the rollover",
        );
        // The day itself still rolls: the rollover-matching and continuity
        // checks read it, and the halt records its own engagement date.
        assert_eq!(
            runtime.state().daily_baseline_day,
            Some(day2.format("%Y-%m-%d").to_string()),
        );

        // Which is what keeps the loss visible to anything re-deriving it.
        let mark = runtime.last_risk_mark().expect("prices were marked");
        assert_eq!(mark.daily_loss_usd, Decimal::from(3));
        assert!(runtime.clear_risk_halt().is_err(), "nothing was remediated");
    }

    #[test]
    fn a_frozen_basket_unfreezes_once_the_halt_is_lifted() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day1 = event_time();
        runtime.step_at(&snapshot_with_valid_row(day1), day1);
        runtime.state.inventory.token_a = Decimal::new(985, 3);
        let day2 = day1 + Duration::days(1);
        runtime.step_at(&snapshot_with_valid_row(day2), day2);
        assert!(runtime.state().risk_halt.is_some());

        // Remediate: the rotation is reversed, so nothing is owed against
        // day 1's basket any more and the halt becomes liftable.
        runtime.state.inventory.token_a = Decimal::ONE;
        runtime.clear_risk_halt().expect("nothing is owed now");

        // Still frozen for the rest of this day -- lifting a halt does not
        // refill the day's budget -- and rebased on the next rollover.
        let day3 = day1 + Duration::days(2);
        runtime.step_at(&snapshot_with_valid_row(day3), day3);
        assert_eq!(
            runtime.state().daily_baseline_inventory,
            Some(runtime.state().inventory),
        );
    }

    #[test]
    fn a_halted_legacy_checkpoint_still_gets_its_baskets_seeded() {
        // The live host's exact shape when #211 lands: already halted, and
        // carrying no baskets. Freezing must not mean never seeding, or its
        // daily stop stays unmeasurable forever and the halt unliftable.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day1 = event_time();
        runtime.step_at(&snapshot_with_valid_row(day1), day1);
        runtime.state.risk_halt = Some(ArcusSpotRiskHalt {
            kind: ArcusSpotRiskHaltKind::DailyLoss,
            engaged_at: day1,
            equity_usd: Decimal::from(300),
            loss_usd: Decimal::from(2),
            limit_usd: Decimal::from(2),
        });
        runtime.state.daily_baseline_inventory = None;
        runtime.state.initial_baseline_inventory = None;

        let day2 = day1 + Duration::days(1);
        runtime.step_at(&snapshot_with_valid_row(day2), day2);

        assert_eq!(
            runtime.state().daily_baseline_inventory,
            Some(runtime.state().inventory),
        );
        assert_eq!(
            runtime.state().initial_baseline_inventory,
            Some(runtime.state().inventory),
        );
        runtime
            .clear_risk_halt()
            .expect("a beta-only halt owes nothing once baskets exist");
    }

    #[test]
    fn clearing_a_halt_is_refused_while_its_condition_still_holds() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day = event_time();
        runtime.step_at(&snapshot_with_valid_row(day), day);
        runtime.state.inventory.token_a = Decimal::new(985, 3);
        let later = day + Duration::hours(1);
        runtime.step_at(&snapshot_with_valid_row(later), later);

        let error = runtime
            .clear_risk_halt()
            .expect_err("a live breach must not be clearable");
        assert!(
            error.contains("would re-engage on the next tick"),
            "{error}",
        );
        assert!(runtime.state().risk_halt.is_some(), "and stays engaged");
    }

    #[test]
    fn an_attributed_loss_is_assessed_before_the_daily_basket_resets() {
        // The ordering guard that does survive: the outgoing day's loss is
        // marked against the outgoing day's basket, on the rollover tick,
        // before `update_risk_baselines` adopts a new one. Reset first and
        // the day's damage would be measured against the basket it already
        // produced, reporting zero forever.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let day1 = event_time();
        runtime.step_at(&snapshot_with_valid_row(day1), day1);
        assert_eq!(runtime.state().risk_halt, None);

        // Rotations destroy $3 of the $300 basket, under the $2 limit only
        // until the rollover tick marks it: 200*0.985 + 100*1 = 297.
        runtime.state.inventory.token_a = Decimal::new(985, 3);
        let day2 = day1 + Duration::days(1);
        runtime.step_at(&snapshot_with_valid_row(day2), day2);

        let halt = runtime
            .state()
            .risk_halt
            .clone()
            .expect("the outgoing day's attributed loss must engage the halt at rollover");
        assert_eq!(halt.kind, ArcusSpotRiskHaltKind::DailyLoss);
    }

    #[test]
    fn last_equity_mark_reflects_the_post_fill_close_not_the_pre_fill_open() {
        // A max-hold exit fires on this tick and changes marked equity
        // within the same step_at call (inventory_before != inventory_after).
        // last_equity_usd must record the *closing* (post-fill) value so
        // the next day's overnight-loss rollover check compares against
        // what equity actually was at day 1's close, not what it was
        // before the fill executed.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        assert!(matches!(
            event.decision,
            ArcusSpotDecision::SimulatedFill { .. }
        ));
        assert_ne!(
            event.inventory_before, event.inventory_after,
            "test setup must actually exercise a fill"
        );

        let reference_price_a = Decimal::from(200);
        let reference_price_b = Decimal::from(100);
        let equity_before = event
            .inventory_before
            .checked_value_usd(reference_price_a, reference_price_b)
            .unwrap();
        let equity_after = event
            .inventory_after
            .checked_value_usd(reference_price_a, reference_price_b)
            .unwrap();
        assert_ne!(equity_before, equity_after);
        assert_eq!(runtime.state().last_equity_usd, Some(equity_after));
    }

    #[test]
    fn price_sampling_continues_during_a_route_outage() {
        // A route outage must not stall the signal window: otherwise the
        // first route recovered after an outage is scored against
        // pre-outage prices and can produce a spurious entry even if the
        // ratio was stable throughout.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        assert_eq!(runtime.state().relative_log_price_history.len(), 0);
        let snapshot = snapshot_with_route_unavailable(event_time(), "150", "50");
        let event = runtime.step_at(&snapshot, event_time());
        assert!(matches!(
            event.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::RouteUnavailable
        ));
        assert_eq!(event.relative_log_price, Some((150.0_f64 / 50.0_f64).ln()));
        assert_eq!(runtime.state().relative_log_price_history.len(), 1);
    }

    #[test]
    fn step_at_rejects_a_repeated_observation() {
        // Codex P2 follow-up, pairtrade#186: re-evaluating the exact same
        // observation twice (a retried invocation, or two writers of the
        // same checkpoint racing each other) must not mutate sequence or
        // the signal-window history a second time.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let t1 = event_time();
        let snapshot = snapshot_with_route_unavailable(t1, "150", "50");

        let first = runtime.step_at(&snapshot, t1);
        assert!(matches!(
            first.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::RouteUnavailable
        ));
        let sequence_after_first = runtime.state().sequence;
        let history_len_after_first = runtime.state().relative_log_price_history.len();

        let second = runtime.step_at(&snapshot, t1);
        assert!(matches!(
            second.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::StaleOrDuplicateObservation
        ));
        assert_eq!(runtime.state().sequence, sequence_after_first);
        assert_eq!(
            runtime.state().relative_log_price_history.len(),
            history_len_after_first
        );
        assert_eq!(second.sequence, first.sequence);
    }

    #[test]
    fn step_at_rejects_an_out_of_order_observation() {
        // A late writer holding an older snapshot (e.g. a concurrent
        // arcus-spot-propose-plan that fetched before this runtime's more
        // recent tick already advanced) must not be able to append an
        // older observation after a newer one already landed.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let older = event_time();
        let newer = older + chrono::Duration::seconds(5);

        runtime.step_at(&snapshot_with_route_unavailable(newer, "150", "50"), newer);
        let sequence_after_newer = runtime.state().sequence;

        let late = runtime.step_at(&snapshot_with_route_unavailable(older, "150", "50"), newer);
        assert!(matches!(
            late.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::StaleOrDuplicateObservation
        ));
        assert_eq!(runtime.state().sequence, sequence_after_newer);
        assert_eq!(
            runtime.state().last_observation_at,
            Some(newer),
            "the late, older observation must not overwrite the newer one already recorded"
        );
    }

    #[test]
    fn step_at_accepts_a_strictly_newer_observation() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let t1 = event_time();
        let t2 = t1 + chrono::Duration::seconds(5);

        runtime.step_at(&snapshot_with_route_unavailable(t1, "150", "50"), t1);
        let sequence_after_first = runtime.state().sequence;
        let history_len_after_first = runtime.state().relative_log_price_history.len();

        let second = runtime.step_at(&snapshot_with_route_unavailable(t2, "150", "50"), t2);
        assert!(matches!(
            second.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::RouteUnavailable
        ));
        assert_eq!(runtime.state().sequence, sequence_after_first + 1);
        assert_eq!(
            runtime.state().relative_log_price_history.len(),
            history_len_after_first + 1
        );
        assert_eq!(runtime.state().last_observation_at, Some(t2));
    }

    #[test]
    fn step_at_does_not_advance_the_watermark_on_a_structurally_invalid_snapshot() {
        // Codex P2 follow-up, pairtrade#186: a corrupt/invalid snapshot
        // (here, a wrong chain_id -- price_context's InvalidSnapshot path)
        // must not commit its collection_finished_at to
        // last_observation_at. Otherwise a single bad record (e.g. a
        // far-future timestamp from a corrupt bootstrap archive entry)
        // would make every subsequent legitimate observation look stale
        // or duplicate forever, silently halting signal evaluation.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let bad_time = event_time() + chrono::Duration::days(365);
        let invalid_snapshot: ArcusSpotRecorderSnapshot = serde_json::from_value(json!({
            "schema_version": 3,
            "mode": "public_indicative_read_only",
            "chain_id": 4664,
            "collection_started_at": bad_time,
            "collection_finished_at": bad_time,
            "indexer_stats": {
                "status": "error",
                "error": {"stage": "indexer_stats", "classification": "http", "retryable": false, "message": "x"}
            },
            "token_metadata": {
                "status": "error",
                "error": {"stage": "token_metadata", "classification": "http", "retryable": false, "message": "x"}
            },
            "reference_overview": {
                "status": "error",
                "error": {"stage": "reference_overview", "classification": "http", "retryable": false, "message": "x"}
            },
            "round_trips": []
        }))
        .unwrap();

        let event = runtime.step_at(&invalid_snapshot, bad_time);
        assert!(matches!(
            event.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::InvalidSnapshot
        ));
        assert_eq!(
            runtime.state().last_observation_at,
            None,
            "an invalid snapshot must not advance the watermark"
        );

        // A genuinely valid, much-earlier observation must still be
        // accepted -- proving the bad far-future timestamp above never
        // became the watermark.
        let valid_time = event_time();
        let valid_event = runtime.step_at(
            &snapshot_with_route_unavailable(valid_time, "150", "50"),
            valid_time,
        );
        assert!(matches!(
            valid_event.decision,
            ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::RouteUnavailable
        ));
        assert_eq!(runtime.state().last_observation_at, Some(valid_time));
    }

    fn snapshot_with_valid_row(collected_at: DateTime<Utc>) -> ArcusSpotRecorderSnapshot {
        snapshot_with_valid_row_at_prices(collected_at, "200", "100")
    }

    fn snapshot_with_bidirectional_rows(collected_at: DateTime<Utc>) -> ArcusSpotRecorderSnapshot {
        let mut snapshot = snapshot_with_valid_row(collected_at);
        snapshot
            .round_trips
            .push(reverse_context(collected_at - Duration::seconds(2), Decimal::from(80)).row);
        snapshot
    }

    fn snapshot_with_valid_row_at_prices(
        collected_at: DateTime<Utc>,
        token_a_price: &str,
        token_b_price: &str,
    ) -> ArcusSpotRecorderSnapshot {
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        serde_json::from_value(json!({
            "schema_version": 3,
            "mode": "public_indicative_read_only",
            "chain_id": 4663,
            "collection_started_at": collected_at,
            "collection_finished_at": collected_at,
            "indexer_stats": {
                "status": "error",
                "error": {"stage": "indexer_stats", "classification": "http", "retryable": false, "message": "x"}
            },
            "token_metadata": {
                "status": "success",
                "observation": {
                    "payload": [
                        {
                            "chainId": 4663,
                            "symbol": "NVDA",
                            "name": "NVIDIA",
                            "address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                            "decimals": 18,
                            "verified": true
                        },
                        {
                            "chainId": 4663,
                            "symbol": "AMD",
                            "name": "AMD",
                            "address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                            "decimals": 18,
                            "verified": true
                        }
                    ],
                    "requested_at": collected_at,
                    "received_at": collected_at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "reference_overview": {
                "status": "success",
                "observation": {
                    "payload": [
                        {
                            "ticker": "NVDA",
                            "contractAddress": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                            "name": "NVIDIA",
                            "category": "stock",
                            "quote": {"price": token_a_price}
                        },
                        {
                            "ticker": "AMD",
                            "contractAddress": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                            "name": "AMD",
                            "category": "stock",
                            "quote": {"price": token_b_price}
                        }
                    ],
                    "requested_at": collected_at,
                    "received_at": collected_at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "round_trips": [row]
        }))
        .unwrap()
    }

    fn snapshot_with_overview_received_at(
        collected_at: DateTime<Utc>,
        overview_received_at: DateTime<Utc>,
    ) -> ArcusSpotRecorderSnapshot {
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        serde_json::from_value(json!({
            "schema_version": 3,
            "mode": "public_indicative_read_only",
            "chain_id": 4663,
            "collection_started_at": collected_at,
            "collection_finished_at": collected_at,
            "indexer_stats": {
                "status": "error",
                "error": {"stage": "indexer_stats", "classification": "http", "retryable": false, "message": "x"}
            },
            "token_metadata": {
                "status": "success",
                "observation": {
                    "payload": [
                        {
                            "chainId": 4663,
                            "symbol": "NVDA",
                            "name": "NVIDIA",
                            "address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                            "decimals": 18,
                            "verified": true
                        },
                        {
                            "chainId": 4663,
                            "symbol": "AMD",
                            "name": "AMD",
                            "address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                            "decimals": 18,
                            "verified": true
                        }
                    ],
                    "requested_at": collected_at,
                    "received_at": collected_at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "reference_overview": {
                "status": "success",
                "observation": {
                    "payload": [
                        {
                            "ticker": "NVDA",
                            "contractAddress": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                            "name": "NVIDIA",
                            "category": "stock",
                            "quote": {"price": "200"}
                        },
                        {
                            "ticker": "AMD",
                            "contractAddress": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                            "name": "AMD",
                            "category": "stock",
                            "quote": {"price": "100"}
                        }
                    ],
                    "requested_at": overview_received_at,
                    "received_at": overview_received_at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "round_trips": [row]
        }))
        .unwrap()
    }

    #[test]
    fn stale_reference_overview_is_rejected_even_with_a_fresh_route() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let stale_overview_at =
            event_time() - Duration::seconds(runtime.config.max_quote_age_secs + 1);
        let snapshot = snapshot_with_overview_received_at(event_time(), stale_overview_at);
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::StaleQuote);
            }
            other => panic!("expected Observe/StaleQuote, got {other:?}"),
        }
    }

    #[test]
    fn future_dated_reference_overview_is_rejected() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let future_overview_at = event_time() + Duration::seconds(5);
        let snapshot = snapshot_with_overview_received_at(event_time(), future_overview_at);
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::InvalidSnapshot);
            }
            other => panic!("expected Observe/InvalidSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn same_contract_token_pair_is_rejected() {
        // Both symbol lookups can independently pass verification while
        // resolving to the same contract (bot-strategy#755 review round12,
        // e.g. a mislabeled wrapped-token entry) — this must not be treated
        // as two distinct assets.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let mut snapshot = snapshot_with_valid_row(event_time());
        if let ArcusSpotCapture::Success { observation } = &mut snapshot.token_metadata {
            let nvda_address = observation.payload[0].address.clone();
            observation.payload[1].address = nvda_address;
        } else {
            panic!("expected successful token_metadata capture");
        }
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::InvalidSnapshot);
                assert!(hold.detail.contains("same contract"));
            }
            other => panic!("expected Observe/InvalidSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn risk_halted_rotated_regime_can_still_exit_on_max_hold() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let baseline_inventory = runtime.state.inventory;
        runtime.update_risk_baselines(event_time(), Decimal::from(300), baseline_inventory);
        runtime.engage_risk_halt(
            event_time(),
            ArcusSpotRiskMark {
                equity_usd: Decimal::from(100),
                daily_loss_usd: Decimal::from(200),
                cumulative_loss_usd: Decimal::from(200),
                inventory_drawdown_usd: Decimal::ZERO,
            },
        );
        assert!(runtime.state.risk_halt.is_some());
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MaxHoldExit);
            }
            other => panic!("expected a max-hold exit despite the halt, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn stale_unused_leg_does_not_block_a_max_hold_exit() {
        // RotatedAToB only ever exits via the reverse (B-to-A) leg (see
        // build_plan's direction match); the forward leg is not consulted
        // at all for this exit. An earlier version required *both* legs to
        // be fresh before any rotation could be evaluated, so a forward
        // leg the recorder happened not to refresh this cycle could block
        // an otherwise-ready max-hold exit indefinitely, defeating
        // max_hold_secs (Codex P1 follow-up, pairtrade#177).
        let stale_forward_received_at = event_time() - Duration::seconds(1_000);
        let row: ArcusSpotRoundTripRecord = serde_json::from_value(json!({
            "pair": {"sell_symbol": "NVDA", "buy_symbol": "AMD"},
            "notional_usd": "5",
            "sell_reference_price_usd": "200",
            "buy_reference_price_usd": "100",
            "requested_sell_amount": "25000000000000000",
            "forward": {
                "chain_id": 4663,
                "sell_symbol": "NVDA",
                "buy_symbol": "AMD",
                "sell_token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                "buy_token": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                "sell_amount": "25000000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": "49000000000000000",
                            "sellAmount": "25000000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": stale_forward_received_at,
                    "received_at": stale_forward_received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "reverse": {
                "chain_id": 4663,
                "sell_symbol": "AMD",
                "buy_symbol": "NVDA",
                "sell_token": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                "buy_token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                "sell_amount": "49000000000000000",
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": "24800000000000000",
                            "sellAmount": "49000000000000000",
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": event_time(),
                    "received_at": event_time(),
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "optimistic_return_amount": "24800000000000000",
            "optimistic_round_trip_loss_bps": "80",
            "errors": []
        }))
        .unwrap();
        let snapshot: ArcusSpotRecorderSnapshot = serde_json::from_value(json!({
            "schema_version": 3,
            "mode": "public_indicative_read_only",
            "chain_id": 4663,
            "collection_started_at": event_time(),
            "collection_finished_at": event_time(),
            "indexer_stats": {
                "status": "error",
                "error": {"stage": "indexer_stats", "classification": "http", "retryable": false, "message": "x"}
            },
            "token_metadata": {
                "status": "success",
                "observation": {
                    "payload": [
                        {"chainId": 4663, "symbol": "NVDA", "name": "NVIDIA", "address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC", "decimals": 18, "verified": true},
                        {"chainId": 4663, "symbol": "AMD", "name": "AMD", "address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC", "decimals": 18, "verified": true}
                    ],
                    "requested_at": event_time(),
                    "received_at": event_time(),
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "reference_overview": {
                "status": "success",
                "observation": {
                    "payload": [
                        {"ticker": "NVDA", "contractAddress": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC", "name": "NVIDIA", "category": "stock", "quote": {"price": "200"}},
                        {"ticker": "AMD", "contractAddress": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC", "name": "AMD", "category": "stock", "quote": {"price": "100"}}
                    ],
                    "requested_at": event_time(),
                    "received_at": event_time(),
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "round_trips": [row]
        }))
        .unwrap();

        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MaxHoldExit);
            }
            other => panic!(
                "expected the max-hold exit to fire despite the stale unused forward leg, got {other:?}"
            ),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn exit_exceeding_the_open_rotation_quantity_is_rejected() {
        // The row's reverse leg wants to sell 0.049 AMD, but only 0.04 AMD
        // is tracked as open from the entry. An earlier version scaled
        // buy_quantity linearly down to the open quantity, but that
        // synthesizes a fill price the venue never actually quoted for
        // that smaller size -- the same soundness problem as the
        // floor-crossing case (Codex P1 follow-up, pairtrade#177). The
        // exit must instead remain unfilled (staying rotated) rather than
        // invent one; a future snapshot whose quote fits within the open
        // quantity can still close it.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(40, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::RotationLimit);
            }
            other => panic!("expected the oversized exit to be rejected, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
        assert_eq!(runtime.state().rotated_quantity, Some(Decimal::new(40, 3)));
    }

    #[test]
    fn partial_exit_keeps_the_regime_rotated_with_the_remaining_open_quantity() {
        // 0.06 AMD is tracked as open, but this snapshot's reverse leg only
        // unwinds 0.049 of it; the position must stay rotated with 0.011
        // AMD still open rather than being declared closed.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(60, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MaxHoldExit);
                assert_eq!(plan.sell_quantity, Decimal::new(49, 3));
            }
            other => panic!("expected a partial max-hold exit, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
        assert_eq!(runtime.state().rotated_quantity, Some(Decimal::new(11, 3)));
    }

    #[test]
    fn exit_matching_the_open_rotation_quantity_clears_the_regime() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MaxHoldExit);
            }
            other => panic!("expected a closing max-hold exit, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::Neutral);
        assert_eq!(runtime.state().rotated_quantity, None);
    }

    #[test]
    fn entry_plan_ignores_the_rotation_fraction_cap_is_not_applied_to_exits() {
        // The per-action rotation-fraction cap is entry-only (see
        // `max_rotation_fraction` handling in build_plan); the open-quantity
        // cap tested above is what protects exits instead. A tight fraction
        // cap must still reject an oversized entry.
        let mut cfg = config();
        cfg.max_rotation_fraction = Decimal::new(1, 2); // 1%
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::RotationLimit);
    }

    #[test]
    fn entry_plan_still_rejects_a_predicted_imbalance_above_the_hard_cap() {
        let mut cfg = config();
        cfg.max_inventory_imbalance_fraction = Decimal::new(1, 1); // 10%
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InventoryImbalance);
    }

    #[test]
    fn exit_plan_is_allowed_above_the_imbalance_cap_when_it_improves_the_current_imbalance() {
        // token_a=200usd/unit, token_b=100usd/unit (see `context()`). Before
        // this exit: 0.5 NVDA (100usd) / 2.0 AMD (200usd), imbalance 0.333.
        // The reverse leg sells 0.049 AMD for 0.0248 NVDA, moving toward
        // balance (0.300) but not under a cap this tight; it must still be
        // allowed since it's a risk-reducing exit, not an entry.
        let mut cfg = config();
        cfg.max_inventory_imbalance_fraction = Decimal::new(5, 2); // 5%
        cfg.inventory_floors = ArcusSpotInventory {
            token_a: Decimal::ZERO,
            token_b: Decimal::ZERO,
        };
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let inventory = ArcusSpotInventory {
            token_a: Decimal::new(5, 1),
            token_b: Decimal::new(20, 1),
        };
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MeanReversionExit,
                event_time(),
                inventory,
            )
            .unwrap();
        assert!(
            plan.predicted_inventory_imbalance_fraction
                > runtime.config().max_inventory_imbalance_fraction,
            "test setup should keep the exit above the cap: {}",
            plan.predicted_inventory_imbalance_fraction
        );
    }

    #[test]
    fn exit_plan_is_still_rejected_above_the_cap_when_it_worsens_the_current_imbalance() {
        // Same tokens/prices as above, but starting heavily skewed toward
        // token_a (5.0 NVDA vs 0.06 AMD): selling AMD to buy more NVDA here
        // pushes further away from balance, so the exit gains no exception
        // and the hard cap applies.
        let mut cfg = config();
        cfg.max_inventory_imbalance_fraction = Decimal::new(5, 2); // 5%
        cfg.inventory_floors = ArcusSpotInventory {
            token_a: Decimal::ZERO,
            token_b: Decimal::ZERO,
        };
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let inventory = ArcusSpotInventory {
            token_a: Decimal::new(5, 0),
            token_b: Decimal::new(6, 2),
        };
        let error = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MeanReversionExit,
                event_time(),
                inventory,
            )
            .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InventoryImbalance);
    }

    #[test]
    fn risk_halted_neutral_regime_still_blocks_new_entries() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        let baseline_inventory = runtime.state.inventory;
        runtime.update_risk_baselines(event_time(), Decimal::from(300), baseline_inventory);
        runtime.engage_risk_halt(
            event_time(),
            ArcusSpotRiskMark {
                equity_usd: Decimal::from(100),
                daily_loss_usd: Decimal::from(200),
                cumulative_loss_usd: Decimal::from(200),
                inventory_drawdown_usd: Decimal::ZERO,
            },
        );
        assert!(runtime.state.risk_halt.is_some());
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::RiskHalt);
            }
            other => panic!("expected Observe/RiskHalt, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::Neutral);
    }
    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn confirmed_live_fill_commits_only_after_reconciliation_seam() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        let before = runtime.state().inventory;
        runtime
            .apply_confirmed_live_fill(&plan, plan.sell_quantity, plan.buy_quantity, event_time())
            .unwrap();
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
        assert_eq!(runtime.state().rotated_quantity, Some(plan.buy_quantity));
        assert_eq!(
            runtime.state().inventory.token_a,
            before.token_a - plan.sell_quantity
        );
        assert_eq!(
            runtime.state().inventory.token_b,
            before.token_b + plan.buy_quantity
        );
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn confirmed_live_fill_is_idempotent_by_execution_key() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert!(runtime
            .apply_confirmed_live_fill_once(
                &plan,
                plan.sell_quantity,
                plan.buy_quantity,
                event_time(),
                "arcus-spot-00000000000000000001-aabbccddeeff0011",
            )
            .unwrap());
        let committed = runtime.state().clone();
        assert!(!runtime
            .apply_confirmed_live_fill_once(
                &plan,
                plan.sell_quantity,
                plan.buy_quantity,
                event_time(),
                "arcus-spot-00000000000000000001-aabbccddeeff0011",
            )
            .unwrap());
        assert_eq!(runtime.state(), &committed);
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn plan_consistent_with_state_is_accepted_before_any_fill() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        runtime.validate_plan_consistent_with_state(&plan).unwrap();
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn stale_plan_from_a_prior_regime_is_rejected_before_dispatch() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        // Approve an entry plan while still Neutral...
        let stale_entry_plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        // ...but the checkpoint has since rotated (e.g. a prior fill
        // already committed), so re-dispatching that same approved plan
        // must now be refused rather than reach a second submission.
        runtime
            .apply_confirmed_live_fill(
                &stale_entry_plan,
                stale_entry_plan.sell_quantity,
                stale_entry_plan.buy_quantity,
                event_time(),
            )
            .unwrap();
        assert!(runtime
            .validate_plan_consistent_with_state(&stale_entry_plan)
            .is_err());
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn exit_plan_larger_than_the_remaining_rotated_quantity_is_rejected_before_dispatch() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(60, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        let snapshot = snapshot_with_valid_row(event_time());
        let event = runtime.step_at(&snapshot, event_time());
        let plan = match event.decision {
            ArcusSpotDecision::WouldRotate { plan } => plan,
            other => panic!("expected a partial max-hold exit, got {other:?}"),
        };
        // Commit that partial exit, leaving less open than this same
        // approved plan's own sell_quantity -- re-dispatching it (e.g. a
        // leftover approval file, or a second in-flight approval racing the
        // first) must now be refused rather than overselling the remaining
        // rotated position.
        runtime
            .apply_confirmed_live_fill(&plan, plan.sell_quantity, plan.buy_quantity, event_time())
            .unwrap();
        assert!(runtime.validate_plan_consistent_with_state(&plan).is_err());
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn entry_plan_is_rejected_before_dispatch_while_the_risk_halt_is_active() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        let baseline_inventory = runtime.state.inventory;
        runtime.update_risk_baselines(event_time(), Decimal::from(300), baseline_inventory);
        let mark = runtime.risk_mark(Decimal::from(297), Decimal::from(200), Decimal::from(100));
        runtime.engage_risk_halt(event_time(), mark);
        assert!(runtime.validate_plan_consistent_with_state(&plan).is_err());
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn mismatched_confirmed_sell_does_not_mutate_live_inventory() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        let before = runtime.state().clone();
        assert!(runtime
            .apply_confirmed_live_fill(
                &plan,
                plan.sell_quantity / Decimal::from(2),
                plan.buy_quantity,
                event_time(),
            )
            .is_err());
        assert_eq!(runtime.state(), &before);
    }
    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn failed_live_exit_does_not_partially_mutate_inventory() {
        let mut cfg = config();
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let entry = runtime
            .build_plan(
                &context(event_time() - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        runtime
            .apply_confirmed_live_fill(
                &entry,
                entry.sell_quantity,
                entry.buy_quantity,
                event_time(),
            )
            .unwrap();

        let mut invalid_exit = entry;
        invalid_exit.direction = ArcusSpotDirection::TokenBToTokenA;
        invalid_exit.trigger = ArcusSpotRotationTrigger::MeanReversionExit;
        invalid_exit.sell_quantity = runtime.state.rotated_quantity.unwrap() + Decimal::new(1, 6);
        invalid_exit.buy_quantity = Decimal::new(1, 3);
        let before = runtime.state.clone();
        assert!(runtime
            .apply_confirmed_live_fill(
                &invalid_exit,
                invalid_exit.sell_quantity,
                invalid_exit.buy_quantity,
                event_time(),
            )
            .is_err());
        assert_eq!(runtime.state(), &before);
    }
}
