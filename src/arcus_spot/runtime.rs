use super::{
    ArcusSpotCorporateActionEvent, ArcusSpotInventory, ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode,
};
use chrono::{DateTime, Duration, Utc};
use dex_connector::{
    ArcusSpotCapture, ArcusSpotOverviewEntry, ArcusSpotRecorderSnapshot, ArcusSpotRoundTripRecord,
    ArcusSpotRouteObservation, ArcusSpotSelectedQuote, ArcusSpotToken,
};
use dex_connector::{ArcusSpotFixedSellAmountRow, ArcusSpotPair, ArcusSpotRecorderStage};
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
    /// Unwind forced by a declared corporate-action window reaching its
    /// `reduce_exit_at` (bot-strategy#853). Sized, quoted, gated and
    /// reconciled exactly like the other two exits -- it only differs in
    /// what made it fire, which is worth keeping in the audit trail rather
    /// than borrowing `MaxHoldExit`'s name for it.
    CorporateActionExit,
}

impl ArcusSpotRotationTrigger {
    /// True for every trigger that unwinds an open rotation. Kept as one
    /// predicate so a fourth trigger cannot be added to the enum and
    /// silently miss a site that tests for "is this an exit".
    pub fn is_exit(self) -> bool {
        match self {
            Self::EntrySignal => false,
            Self::MeanReversionExit | Self::MaxHoldExit | Self::CorporateActionExit => true,
        }
    }
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
    /// Inside a declared corporate-action window: new rotations are blocked
    /// while exits stay available (bot-strategy#853).
    CorporateActionBlock,
    /// Past `resume_not_before`, but the operator has not yet supplied the
    /// reconciled `post_event_inventory` the resume depends on.
    CorporateActionResumePending,
    /// Past `resume_not_before` with a condition the runtime must not
    /// resolve on its own -- the affected token's contract or decimals
    /// changed, or the pre-event rotation could not be unwound. Exits stay
    /// available; entries need operator action.
    CorporateActionUnresolved,
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

/// The identity half of a resolved pair token: what the runtime compares
/// across a corporate-action window to decide whether the instrument it was
/// trading before the event is the same one it would trade after it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArcusSpotTokenIdentity {
    pub symbol: String,
    pub address: String,
    pub decimals: u32,
}

impl ArcusSpotTokenIdentity {
    fn from_token(token: &ArcusSpotToken) -> Self {
        Self {
            symbol: token.symbol.clone(),
            address: token.address.clone(),
            decimals: token.decimals,
        }
    }

    /// Whether two recorded identities describe the same contract, by the
    /// same rule `matches` uses against a live token. The continuity
    /// verifier compares stored identities and has to reach the same answer
    /// the runtime did, so this is the one definition (Codex P1,
    /// pairtrade#309).
    pub fn same_contract(&self, other: &Self) -> bool {
        self.address.eq_ignore_ascii_case(&other.address) && self.decimals == other.decimals
    }

    /// Addresses are compared case-insensitively: the same contract is
    /// legitimately rendered checksummed or lower-case by different
    /// responses, and treating that as a change would fail closed on a
    /// non-event.
    fn matches(&self, token: &ArcusSpotToken) -> bool {
        self.address.eq_ignore_ascii_case(&token.address) && self.decimals == token.decimals
    }
}

/// Progress through the one corporate-action window that is currently open.
/// Persisted in the checkpoint so a restart mid-window, and a replay of the
/// same observations, reach identical decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotCorporateActionProgress {
    pub event_id: String,
    /// When this runtime first blocked on the window.
    pub blocked_at: DateTime<Utc>,
    /// Token identity as of the last observation *before* the block. `None`
    /// when the runtime never observed the pre-event side of the window
    /// (the window opened while it was down, or on its very first tick), in
    /// which case there is nothing to compare and the resume says so
    /// instead of inventing a comparison.
    #[serde(default)]
    pub pre_event_token_a: Option<ArcusSpotTokenIdentity>,
    #[serde(default)]
    pub pre_event_token_b: Option<ArcusSpotTokenIdentity>,
    /// Set once, when `effective_at` was first crossed and the pre-event
    /// relative-price history was discarded.
    #[serde(default)]
    pub history_invalidated_at: Option<DateTime<Utc>>,
    /// `ArcusSpotCorporateActionEvent::fingerprint` of the declaration this
    /// progress belongs to. Matching on it rather than on `event_id` is what
    /// lets an operator rename an *active* entry without the runtime
    /// discarding the pinned pre-event identity -- and what stops a
    /// different declaration under the same label from inheriting it.
    /// Empty on records written before it existed; those match by id.
    #[serde(default)]
    pub fingerprint: String,
    /// The declaration's `effective_at`, copied when the window opened, so
    /// the record can cross into the stale-unit phase on its own -- and
    /// stamp `history_invalidated_at` at the right instant -- even if the
    /// declaration is deleted or replaced before then. Without it a record
    /// orphaned in the reduce phase never became stale, and the old
    /// quantities kept being valued at post-event prices (Codex P1,
    /// pairtrade#309). `None` on records that predate the field.
    #[serde(default)]
    pub effective_at: Option<DateTime<Utc>>,
    /// The declaration's affected symbols, copied at window open, so that
    /// what the window is *about* survives the declaration being deleted:
    /// `reset-window` may only discard an open window when the new `pair`
    /// no longer names any of them (Codex P1, pairtrade#309). Empty on
    /// records that predate the field, which the reset treats as unknown.
    #[serde(default)]
    pub symbols: Vec<String>,
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
    /// When those reference prices were received (`PriceContext::priced_at`).
    /// The runtime decides stale-unit halt suppression from it, so the
    /// verifier has to be able to re-derive that decision from the
    /// checkpoint rather than infer it from the discard stamp, which the
    /// gate writes with the later evaluation clock (Codex P1,
    /// pairtrade#309).
    #[serde(default)]
    pub last_reference_price_at: Option<DateTime<Utc>>,
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
    /// Set the first time a legacy handled record is resolved against the
    /// calendar. The resolution uses the observation watermark, which keeps
    /// advancing, so re-running it on a later load would eventually confirm
    /// a *different* action that has since passed its own resume time --
    /// permanently marking it handled. Resolving once and recording that it
    /// happened is what freezes the answer (Codex P1, pairtrade#309).
    #[serde(default)]
    pub handled_corporate_actions_resolved: bool,
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
    /// Identity of the two pair tokens as of the last structurally valid
    /// observation. Kept beside the reference prices for the same reason
    /// they are: it makes the pre-event side of a corporate-action window
    /// durable, so a window that opens while the runtime is down can still
    /// be compared against something real on resume.
    #[serde(default)]
    pub last_token_a_identity: Option<ArcusSpotTokenIdentity>,
    #[serde(default)]
    pub last_token_b_identity: Option<ArcusSpotTokenIdentity>,
    /// `collection_finished_at` of the observation the two identities above
    /// were read from. Without it the corporate-action guard cannot tell a
    /// genuinely pre-event identity from one it happened to record *inside*
    /// the window -- which is the normal case when a calendar is installed
    /// after `entry_block_at` while the runtime kept ticking, and which
    /// would make the resume compare the new contract against itself and
    /// report no drift (Codex P2, pairtrade#309).
    #[serde(default)]
    pub last_token_identity_at: Option<DateTime<Utc>>,
    /// The corporate-action window currently being applied, if any.
    #[serde(default)]
    pub corporate_action: Option<ArcusSpotCorporateActionProgress>,
    /// Event IDs already resumed from. An event stays in the config
    /// indefinitely; this is what keeps it from being applied twice.
    #[serde(default)]
    pub handled_corporate_action_ids: Vec<String>,
    /// `ArcusSpotCorporateActionEvent::fingerprint` of each handled event,
    /// appended alongside its id. The id is the operator's mutable label;
    /// this is what the event *was*, so renaming a completed entry does not
    /// resurrect it (Codex P1, pairtrade#309).
    #[serde(default)]
    pub handled_corporate_action_fingerprints: Vec<String>,
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
            last_reference_price_at: None,
            last_token_b_reference_price_usd: None,
            last_observation_at: None,
            // A fresh runtime has no handled records, so there is nothing
            // for the legacy resolution to do -- and it must not run later
            // against a watermark that has since advanced.
            handled_corporate_actions_resolved: true,
            last_rotation_at: None,
            rotated_quantity: None,
            initial_equity_usd: None,
            initial_baseline_inventory: None,
            daily_baseline_day: None,
            daily_baseline_equity_usd: None,
            daily_baseline_inventory: None,
            last_equity_usd: None,
            risk_halt: None,
            last_token_a_identity: None,
            last_token_b_identity: None,
            last_token_identity_at: None,
            corporate_action: None,
            handled_corporate_action_ids: Vec::new(),
            handled_corporate_action_fingerprints: Vec::new(),
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
    /// The earliest instant anything in this context describes: the
    /// reference overview's own receipt time, or the start of the snapshot
    /// collection that captured the token identities, whichever is older.
    /// A consumer that needs "data from after T" must compare against this,
    /// not against when the snapshot finished -- the overview is captured
    /// separately and validated on its own `received_at`, and can predate
    /// the collection that later wrapped it (Codex P1, pairtrade#309).
    observed_at: DateTime<Utc>,
    /// When the *prices* in this context were received. `observed_at` is the
    /// earliest instant anything here describes, which is the right question
    /// for the resume and the identity pins; a decision made purely of
    /// prices has to ask when the prices arrived, or a collection that
    /// started before a cutoff would license a mark built from prices
    /// received after it (Codex P2, pairtrade#309).
    priced_at: DateTime<Utc>,
}

/// Which leg of `SnapshotContext::row` an exit executes, and therefore how
/// its sell quantity was chosen (see `snapshot_context`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitLegSizing {
    /// The row was requested at exactly the tracked open rotation quantity
    /// (`ArcusSpotRoundTripRecord::fixed_sell_amount`, bot-strategy#906);
    /// the exit executes its *forward* leg, which sells that quantity.
    OpenQuantityRow,
    /// The row is the entry-direction, notional-sized cycle; the exit
    /// executes its *reverse* leg, whose size is whatever the fresh
    /// forward quote happened to produce. Only used when the snapshot
    /// carries no open-quantity row (archival recorder replays, snapshots
    /// collected before #906).
    EntryCycleReverseLeg,
}

struct SnapshotContext {
    token_a: ArcusSpotToken,
    token_b: ArcusSpotToken,
    token_a_price_usd: Decimal,
    token_b_price_usd: Decimal,
    row: ArcusSpotRoundTripRecord,
    exit_leg_sizing: ExitLegSizing,
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
        // `validate` allows a floor above `initial_inventory` when a declared
        // reconciliation reaches it -- that is what lets a floor be raised
        // after a completed split. A *fresh* runtime starts from
        // `initial_inventory`, though, so the same config would build state
        // that is already below its floors and wedge on the next
        // `load_or_create`, which reaches `from_state` with no pending
        // reconciliation to excuse it (Codex P2, pairtrade#309).
        if config.initial_inventory.token_a < config.inventory_floors.token_a
            || config.initial_inventory.token_b < config.inventory_floors.token_b
        {
            return Err(
                "a fresh Arcus runtime starts at initial_inventory, which is below \
                 inventory_floors; a floor raised to match a declared post_event_inventory can \
                 only be adopted by a runtime that already holds it"
                    .to_string(),
            );
        }
        let state = ArcusSpotRuntimeState::new(config.initial_inventory);
        Ok(Self { config, state })
    }

    /// A fresh window whose durable event numbering continues from
    /// `last_event_sequence` (the event stream's tail), rather than from
    /// zero as `new` does.
    ///
    /// The event stream is append-only and hash-chained: it "must never be
    /// deleted, truncated, or restored" (docs/arcus-spot-state-rollback.md),
    /// and `validate_event_continuity` refuses any append that is not the
    /// tail's successor. So a state-invalidating config change -- a new
    /// pair, a re-funded inventory -- cannot be handled by discarding the
    /// checkpoint and letting the next tick renumber from 1: that tick
    /// fails to commit, and every later one fails on the pending event it
    /// left behind (bot-strategy#903). Starting the fresh window at the
    /// tail keeps one contiguous audit chain across the strategy change,
    /// with the event's own `pair`/`mode` fields marking the boundary.
    pub fn new_continuing_event_sequence(
        config: ArcusSpotRuntimeConfig,
        last_event_sequence: u64,
    ) -> Result<Self, String> {
        let mut runtime = Self::new(config)?;
        runtime.state.sequence = last_event_sequence;
        Ok(runtime)
    }

    /// Carry the handled corporate-action record into a fresh runtime. The
    /// record is append-only for the life of an account, not of a window:
    /// `reset-window` discards the signal window and re-anchors the risk
    /// baselines, and neither of those makes a completed split un-happen.
    pub fn with_handled_corporate_actions(
        mut self,
        ids: Vec<String>,
        fingerprints: Vec<String>,
        observed_at: Option<DateTime<Utc>>,
    ) -> Self {
        self.state.handled_corporate_action_ids = ids;
        self.state.handled_corporate_action_fingerprints = fingerprints;
        // A fresh state is already marked resolved, so a carried legacy
        // record would never get its backfill and a still-declared completed
        // action would read as a reused label, blocking entries forever.
        // Resolve it here instead, against the watermark of the checkpoint
        // it came from (Codex P2, pairtrade#309).
        resolve_handled_corporate_action_fingerprints(&mut self.state, &self.config, observed_at);
        self
    }

    pub fn from_state(
        mut config: ArcusSpotRuntimeConfig,
        mut state: ArcusSpotRuntimeState,
    ) -> Result<Self, String> {
        config.normalize();
        config.validate()?;
        backfill_handled_corporate_action_fingerprints(&mut state, &config);
        if state.inventory.token_a < config.inventory_floors.token_a
            || state.inventory.token_b < config.inventory_floors.token_b
        {
            // Unless the state is inside a declared window whose reconciled
            // holding *does* satisfy the floors: the operator raised a floor
            // past the stale pre-event quantity and below the post-event
            // one (1 -> 5 across a 4 -> 40 split), and the resume that
            // applies it can only run if this load succeeds. Refusing here
            // made the advertised resume-time check unreachable (Codex P2,
            // pairtrade#309). Trading stays blocked by the window itself.
            let pending_reconciliation_satisfies_floors =
                state.corporate_action.as_ref().is_some_and(|progress| {
                    config.corporate_actions.iter().any(|event| {
                        Self::progress_matches(progress, event)
                            && event.post_event_inventory.is_some_and(|reconciled| {
                                reconciled.token_a >= config.inventory_floors.token_a
                                    && reconciled.token_b >= config.inventory_floors.token_b
                            })
                    })
                });
            if !pending_reconciliation_satisfies_floors {
                return Err("restored Arcus inventory is below a configured floor".to_string());
            }
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

    /// The direction a currently rotated position sells to exit, with the
    /// exact quantity still open, or `None` when neutral. A caller that
    /// collects the snapshot itself should request an extra recorder row at
    /// exactly this quantity (`ArcusSpotRecorderConfig::fixed_sell_amount_rows`)
    /// so `step_at` can size the exit to it (bot-strategy#906).
    pub fn open_exit_leg(&self) -> Option<(ArcusSpotDirection, Decimal)> {
        let open = self.state.rotated_quantity?;
        match self.state.regime {
            ArcusSpotRegime::Neutral => None,
            ArcusSpotRegime::RotatedAToB => Some((ArcusSpotDirection::TokenBToTokenA, open)),
            ArcusSpotRegime::RotatedBToA => Some((ArcusSpotDirection::TokenAToTokenB, open)),
        }
    }

    /// The extra recorder row a snapshot collector should request so the
    /// next `step_at` can exit the current rotated position at exactly its
    /// open quantity: `None` while neutral. `sell_token_decimals` is the
    /// pinned decimals of the token that exit sells (the first symbol of
    /// `direction_symbols(open_exit_leg().0)`); a quantity that is not an
    /// exact raw amount at those decimals is an error, never rounded.
    pub fn open_exit_fixed_sell_amount_row(
        &self,
        sell_token_decimals: u32,
    ) -> Result<Option<ArcusSpotFixedSellAmountRow>, String> {
        open_exit_fixed_sell_amount_row_for(
            self.state.regime,
            self.state.rotated_quantity,
            &self.config.pair,
            sell_token_decimals,
        )
    }

    /// `(sell_symbol, buy_symbol)` for `direction` under the configured pair.
    pub fn direction_symbols(&self, direction: ArcusSpotDirection) -> (&str, &str) {
        match direction {
            ArcusSpotDirection::TokenAToTokenB => {
                (&self.config.pair.sell_symbol, &self.config.pair.buy_symbol)
            }
            ArcusSpotDirection::TokenBToTokenA => {
                (&self.config.pair.buy_symbol, &self.config.pair.sell_symbol)
            }
        }
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
        self.clear_risk_halt_at(Utc::now())
    }

    /// `clear_risk_halt` with the clearance clock supplied.
    pub fn clear_risk_halt_at(&mut self, now: DateTime<Utc>) -> Result<ArcusSpotRiskHalt, String> {
        // The "condition still holds" check below reads `last_risk_mark`,
        // and while a corporate action is effective that mark values
        // pre-event quantities at post-event prices -- meaningless in both
        // directions. After a forward split it can shrink a genuine loss
        // below the limit and let a halt engaged *before* the event be
        // cleared through it; the resume then re-anchors the baselines and
        // the halt never returns. Decide it after the resume, on marks that
        // mean something (Codex P2, pairtrade#309).
        // At the clearance time, not the observation watermark: the
        // watermark does not advance while the bot is down, so an operator
        // clearing after a window became effective would otherwise be judged
        // against a pre-event mark, and the next tick's resume would
        // re-anchor the baskets and lose the halt for good (Codex P2,
        // pairtrade#309).
        let clock = self
            .state
            .last_observation_at
            .map(|observed_at| observed_at.max(now))
            .unwrap_or(now);
        if self.corporate_action_units_are_stale(clock) {
            return Err(
                "refusing to clear the risk halt while a corporate action is effective and the \
                 tracked inventory is in units the venue no longer quotes; resume first"
                    .to_string(),
            );
        }
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
    ///
    /// `dispatched_at` is the clock at the moment of submission, not the
    /// plan's. A plan made in the reduce phase of a declared corporate
    /// action is still consistent with the regime and the tracked quantity
    /// after `effective_at` -- neither changed -- but the venue's units
    /// did, and `max_plan_age_secs` does not know where the cutoff is. The
    /// phase is therefore re-read here against the dispatch clock: no exit
    /// once the tracked units are stale, no entry once a window has opened
    /// (Codex P1, pairtrade#309).
    #[cfg(feature = "arcus-spot-live")]
    pub fn validate_plan_consistent_with_state(
        &self,
        plan: &ArcusSpotRotationPlan,
        dispatched_at: DateTime<Utc>,
    ) -> Result<(), String> {
        require_fill_consistent_with_regime(self.state.regime, plan.trigger, plan.direction)?;
        match plan.trigger {
            ArcusSpotRotationTrigger::EntrySignal => {
                if let Some(halt) = &self.state.risk_halt {
                    return Err(format!(
                        "cannot dispatch an entry plan while the risk halt is active: {halt:?}"
                    ));
                }
                if let Some(reason) = self.corporate_action_blocks_entries(dispatched_at) {
                    return Err(format!("cannot dispatch an entry plan: {reason}"));
                }
            }
            ArcusSpotRotationTrigger::MeanReversionExit
            | ArcusSpotRotationTrigger::MaxHoldExit
            | ArcusSpotRotationTrigger::CorporateActionExit => {
                if self.corporate_action_units_are_stale(dispatched_at) {
                    return Err(
                        "cannot dispatch an exit plan: a corporate action is effective and the \
                         tracked open quantity is in units the venue no longer quotes"
                            .to_string(),
                    );
                }
                // Submission is not execution. An order sent just before the
                // cutoff can be mined after it, and no in-process re-check
                // can cover the venue round trip -- so exits stop a margin
                // early instead (Codex P1, pairtrade#309).
                if let Some(event) = self.corporate_action_within_settlement_margin(dispatched_at) {
                    return Err(format!(
                        "cannot dispatch an exit plan within {}s of corporate action {}'s \
                         effective time {}: it could settle after the venue changes units",
                        self.config.corporate_action_settlement_margin_secs,
                        event.event_id,
                        event.effective_at,
                    ));
                }
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
        // Not exactly the planned quantity, but within dust of it: a
        // settlement transaction may take slightly less of the signed sell
        // amount than it pulled and refund the remainder to the taker in the
        // same transaction (bot-strategy#979). The caller has already
        // required this figure to equal the settled input derived from that
        // transaction's own transfers, so the shortfall is evidence-backed
        // rather than asserted -- but this is the last seam before inventory
        // moves, so it keeps its own bound rather than trusting the caller:
        // anything more than dust short is a partial fill this design never
        // performs, and more than planned remains impossible. Committing the
        // planned quantity when a refund did occur would book inventory the
        // wallet still holds (the bot-strategy#869 drift class).
        let sell_shortfall_allowance = plan
            .sell_quantity
            .checked_mul(ROTATION_DUST_FRACTION)
            .ok_or("confirmed sell dust allowance overflow")?;
        let minimum_confirmed_sell = plan
            .sell_quantity
            .checked_sub(sell_shortfall_allowance)
            .ok_or("confirmed sell dust allowance overflow")?;
        if actual_sell_quantity > plan.sell_quantity
            || actual_sell_quantity < minimum_confirmed_sell
        {
            return Err(format!(
                "confirmed sell quantity {} is not within settlement dust of planned exact \
                 quantity {}",
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
            ArcusSpotRotationTrigger::MeanReversionExit
            | ArcusSpotRotationTrigger::MaxHoldExit
            | ArcusSpotRotationTrigger::CorporateActionExit => {
                let open = next
                    .rotated_quantity
                    .ok_or("rotated regime has no tracked open quantity")?;
                if actual_sell_quantity > open {
                    return Err("confirmed exit sold more than tracked open quantity".to_string());
                }
                match rotation_remaining_after_exit(open, actual_sell_quantity)? {
                    Some(remaining) => next.rotated_quantity = Some(remaining),
                    None => {
                        next.regime = ArcusSpotRegime::Neutral;
                        next.last_rotation_at = None;
                        next.rotated_quantity = None;
                    }
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
        self.state.last_reference_price_at = Some(price.priced_at);
        self.state.last_token_a_reference_price_usd = Some(price.token_a_price_usd);
        self.state.last_token_b_reference_price_usd = Some(price.token_b_price_usd);
        // Captured before the corporate-action gate runs, but *read* by it
        // from the value this line is about to overwrite -- see
        // `corporate_action_gate`, which pins the pre-event identity on the
        // first blocked tick from the previous observation's values.
        let observed_token_a_identity = ArcusSpotTokenIdentity::from_token(&price.token_a);
        let observed_token_b_identity = ArcusSpotTokenIdentity::from_token(&price.token_b);

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
        // ... except while a declared corporate-action window has passed
        // its `effective_at` and its resume has not been committed. The
        // venue is already quoting the post-event instrument by then while
        // `state.inventory` still holds the pre-event quantities, so
        // `equity_before` above multiplies old units by new prices: a
        // 4-for-1 split reads as a 75% loss, a 1-for-4 reverse split as a
        // 4x gain. Neither is evidence of anything, and a halt is sticky --
        // it would survive the resume that repairs the units, keep the
        // rebased runtime blocked until an operator cleared it by hand, and
        // make `state-verify-continuity` reject the authorized transition as
        // an unexplained halt. The mark itself is still taken and still
        // recorded on the event; it just may not engage a halt. Entry is
        // blocked and the exit forced throughout this window regardless, so
        // nothing accumulates exposure while the limits are unenforceable
        // (Codex, PR #309).
        //
        // Judged at `price.priced_at`, not at the processing clock and not
        // at the composite `observed_at`: the question is whether *this
        // mark* mixes units, and the mark is made of prices alone. A
        // snapshot whose prices arrived before the cutoff and was processed
        // after it is a valid pre-event mark, and suppressing it would drop
        // a genuine breach for good -- every later mark is stale, and the
        // resume re-anchors both baskets. Symmetrically, a collection that
        // *started* before the cutoff but whose prices arrived after it is
        // already a post-event mark and must stay suppressed (Codex P2 x2,
        // pairtrade#309).
        // Identity drift makes the mark mixed-unit *before* `effective_at`
        // too: the tracked quantity is the old contract's while the price is
        // the replacement's. A halt engaged from that is artificial and
        // sticky, and it wedges the documented relisting recovery --
        // `reset-window` refuses to discard a halt, `clear-risk-halt` sees
        // the same artificial breach before the cutoff and refuses on stale
        // units after it (Codex P1, pairtrade#309).
        if !self.corporate_action_units_are_stale(price.priced_at)
            && !self.corporate_action_identity_drifted(evaluation_time, &price)
        {
            self.engage_risk_halt(evaluation_time, risk_before);
        }
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
        // Declared corporate-action windows (bot-strategy#853). Placed here
        // deliberately: after the risk marks, so a blocked bot still accounts
        // for the inventory it is holding and a loss halt still engages
        // through a window; after `relative_log_price` proved computable, so
        // a price this tick is about to reject cannot be the one a resume
        // re-anchors its risk baskets on; and before the z-score is read, so
        // a window that discards the signal history has already done it by
        // the time the score is taken from it.
        let corporate_action = self.corporate_action_gate(evaluation_time, &price);
        self.state.last_token_a_identity = Some(observed_token_a_identity);
        self.state.last_token_b_identity = Some(observed_token_b_identity);
        self.state.last_token_identity_at = Some(snapshot.collection_finished_at);

        let informative_signal_samples =
            informative_signal_sample_count(&self.state.relative_log_price_history);
        let total_signal_samples = self.state.relative_log_price_history.len();
        let z_score = z_score(
            &self.state.relative_log_price_history,
            relative_log_price,
            self.config.min_signal_samples,
        );
        if !corporate_action.suppress_history {
            self.state
                .relative_log_price_history
                .push(relative_log_price);
            if self.state.relative_log_price_history.len() > self.config.signal_window_samples {
                let excess =
                    self.state.relative_log_price_history.len() - self.config.signal_window_samples;
                self.state.relative_log_price_history.drain(0..excess);
            }
        }

        // A max-hold exit must fire even when the signal window is flat
        // (z_score() returns None once its standard deviation collapses to
        // zero), so rotation_signal is consulted with the raw Option instead
        // of bailing out to Warmup before it ever sees a rotated regime.
        // Resolved *before* snapshot_context so it can validate only the
        // leg an exit will actually execute (see snapshot_context's doc
        // comment): a stale but unused leg must not be able to block a
        // mean-reversion or max-hold exit, defeating max_hold_secs.
        let gated = corporate_action.apply(
            self.rotation_signal(z_score, evaluation_time, regime_before),
            regime_before,
        );
        let signal = gated.signal();

        // A blocked tick will not trade, so validating a route cannot change
        // its outcome -- and letting it run would let an unrelated route
        // outage report `RouteUnavailable` on every tick of a declared
        // window, hiding the one condition an operator needs to see. Returned
        // here rather than at the shared no-signal branch below for that
        // reason. While a window is open its hold outranks a `RiskHalt` hold
        // for a flat runtime: both block exactly the same thing, the halt is
        // untouched, sticky, and still on the event's risk marks, and it is
        // reported again as soon as the window closes.
        if let Some(hold) = gated.blocked_by() {
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
                decision: ArcusSpotDecision::Observe { hold: hold.clone() },
            });
        }

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
                    | ArcusSpotRotationTrigger::MaxHoldExit
                    | ArcusSpotRotationTrigger::CorporateActionExit => {
                        // build_plan() bounded plan.sell_quantity to at most
                        // the tracked open quantity, so this is >= 0; only
                        // clear the regime once the whole open amount has
                        // actually been unwound, otherwise stay rotated
                        // with the remaining open quantity so the next step
                        // keeps trying to close it out. Shares the live
                        // path's dust rule so replay and live cannot
                        // disagree about when a rotation is closed.
                        let remaining = match self.state.rotated_quantity {
                            // `.min(open)` and the overflow-free subtraction
                            // inside leave no reachable error here.
                            Some(open) => {
                                rotation_remaining_after_exit(open, plan.sell_quantity.min(open))
                                    .unwrap_or(None)
                            }
                            None => None,
                        };
                        match remaining {
                            Some(remaining) => self.state.rotated_quantity = Some(remaining),
                            None => {
                                self.state.regime = ArcusSpotRegime::Neutral;
                                self.state.last_rotation_at = None;
                                self.state.rotated_quantity = None;
                            }
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
            observed_at: overview.received_at.min(snapshot.collection_started_at),
            priced_at: overview.received_at,
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

        // A rotated position exits by selling exactly what it acquired.
        // Prefer a row quoted at that exact quantity when the snapshot
        // carries one; the notional-sized cycle below is only a fallback
        // (see `ExitLegSizing`).
        let exit_direction = match signal {
            Some((direction, ArcusSpotRotationTrigger::MeanReversionExit))
            | Some((direction, ArcusSpotRotationTrigger::MaxHoldExit))
            | Some((direction, ArcusSpotRotationTrigger::CorporateActionExit)) => Some(direction),
            Some((_, ArcusSpotRotationTrigger::EntrySignal)) => None,
            None => self.open_exit_leg().map(|(direction, _)| direction),
        };
        if let (Some(direction), Some(open_quantity)) =
            (exit_direction, self.state.rotated_quantity)
        {
            if let Some(context) = self.open_quantity_exit_context(
                snapshot,
                evaluation_time,
                price,
                direction,
                open_quantity,
            )? {
                return Ok(context);
            }
        }

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

        let is_exit = signal.is_some_and(|(_, trigger)| trigger.is_exit());

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
                self.config.max_favourable_quote_deviation_bps,
                self.config.max_reference_price_age_secs,
            )?;
            verify_reverse_notional_bound(
                reverse_route,
                self.config.notional_usd,
                cycle_buy_price_usd,
                cycle_buy_token,
            )?;
            parse_round_trip_loss_bps(
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
                self.config.max_favourable_quote_deviation_bps,
                self.config.max_reference_price_age_secs,
            )?;
            validate_route_leg(
                reverse_route,
                cycle_buy_token,
                cycle_sell_token,
                evaluation_time,
                self.config.max_quote_age_secs,
                self.config.max_favourable_quote_deviation_bps,
                self.config.max_reference_price_age_secs,
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
            verify_round_trip_linkage_and_loss(
                row,
                cycle_sell_token,
                cycle_buy_token,
                self.config.max_favourable_quote_deviation_bps,
                self.config.max_reference_price_age_secs,
            )?
        };

        Ok(SnapshotContext {
            token_a: token_a.clone(),
            token_b: token_b.clone(),
            token_a_price_usd,
            token_b_price_usd,
            row: row.clone(),
            exit_leg_sizing: ExitLegSizing::EntryCycleReverseLeg,
            verified_round_trip_loss_bps,
        })
    }

    /// Selects the row quoted at exactly `open_quantity` in `direction`
    /// (`fixed_sell_amount` == the open quantity in raw units), validating
    /// only the forward leg an exit would execute. `Ok(None)` when the
    /// snapshot carries no such row at all, so the caller can fall back to
    /// the notional-sized cycle; any row that exists but is unusable is a
    /// hold, never a silent fallback -- a live tick always requests this
    /// row when rotated, so its absence there would itself be the problem.
    ///
    /// Why this exists (bot-strategy#906): the notional-sized cycle's
    /// reverse leg sells `notional_usd` worth of the held token at *today's*
    /// price, while the entry acquired `notional_usd` worth minus costs at
    /// the *entry* price. Bounding that leg to the open quantity therefore
    /// admits an exit only once the held token's USD price has risen past
    /// the entry's cost basis -- a hidden "exit only at a gain on the held
    /// leg" gate that blocked both mean-reversion and max-hold exits.
    fn open_quantity_exit_context(
        &self,
        snapshot: &ArcusSpotRecorderSnapshot,
        evaluation_time: DateTime<Utc>,
        price: &PriceContext,
        direction: ArcusSpotDirection,
        open_quantity: Decimal,
    ) -> Result<Option<SnapshotContext>, ArcusSpotHold> {
        let (sell_symbol, buy_symbol) = self.direction_symbols(direction);
        let (sell_token, buy_token, sell_price_usd, buy_price_usd) = match direction {
            ArcusSpotDirection::TokenAToTokenB => (
                &price.token_a,
                &price.token_b,
                price.token_a_price_usd,
                price.token_b_price_usd,
            ),
            ArcusSpotDirection::TokenBToTokenA => (
                &price.token_b,
                &price.token_a,
                price.token_b_price_usd,
                price.token_a_price_usd,
            ),
        };
        let open_raw =
            quantity_to_raw_amount(open_quantity, sell_token.decimals).map_err(|detail| {
                ArcusSpotHold::new(
                    ArcusSpotHoldCode::RotationLimit,
                    format!(
                        "open rotation quantity {open_quantity} {}: {detail}",
                        sell_token.symbol
                    ),
                )
            })?;
        let matching_rows = snapshot
            .round_trips
            .iter()
            .filter(|row| {
                row.pair.sell_symbol.eq_ignore_ascii_case(sell_symbol)
                    && row.pair.buy_symbol.eq_ignore_ascii_case(buy_symbol)
                    && row.fixed_sell_amount.as_deref() == Some(open_raw.as_str())
            })
            .collect::<Vec<_>>();
        if matching_rows.is_empty() {
            // A fixed row exists for this pair but at a different amount:
            // the collector requested the wrong size (e.g. its decimals
            // pin disagrees with this snapshot's token metadata), and
            // silently falling back to the legacy notional-cycle path
            // would look identical to a collector that never requested an
            // exit-sized row at all -- surface it distinctly instead.
            if let Some(mismatched) = snapshot.round_trips.iter().find(|row| {
                row.pair.sell_symbol.eq_ignore_ascii_case(sell_symbol)
                    && row.pair.buy_symbol.eq_ignore_ascii_case(buy_symbol)
                    && row.fixed_sell_amount.is_some()
            }) {
                return Err(ArcusSpotHold::new(
                    ArcusSpotHoldCode::InvalidSnapshot,
                    format!(
                        "{sell_symbol}/{buy_symbol} fixed-amount row was quoted at {:?}, not the \
                         open rotation quantity {open_raw}; the requesting collector's decimals \
                         pin likely disagrees with this snapshot's token metadata",
                        mismatched.fixed_sell_amount
                    ),
                ));
            }
            return Ok(None);
        }
        if matching_rows.len() != 1 {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                format!(
                    "expected one {sell_symbol}/{buy_symbol} row at the open quantity {open_raw}; \
                     found {}",
                    matching_rows.len()
                ),
            ));
        }
        let row = matching_rows[0];
        // Only the forward leg is executed by this exit (see the freshness
        // comment below); a failure recorded against the reverse leg or
        // its round-trip-cost derivation must not block a position close
        // that the forward leg itself quoted successfully -- the same
        // reasoning `max_hold_secs`'s guarantee already relies on for the
        // legacy entry-cycle exit path.
        if let Some(blocking) = row.errors.iter().find(|error| {
            error.stage != ArcusSpotRecorderStage::ReversePrice
                && error.stage != ArcusSpotRecorderStage::RoundTripCalculation
        }) {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                format!(
                    "open-quantity exit row's forward leg failed: {}",
                    blocking.message
                ),
            ));
        }
        validate_recorded_reference(
            "exit sell token",
            row.sell_reference_price_usd.as_deref(),
            sell_price_usd,
        )?;
        validate_recorded_reference(
            "exit buy token",
            row.buy_reference_price_usd.as_deref(),
            buy_price_usd,
        )?;
        let Some(forward_route) = row.forward.as_ref() else {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                "open-quantity exit row has no forward route",
            ));
        };
        // Only the leg the exit executes is validated for freshness; the
        // chained reverse leg exists so the row carries a round-trip cost
        // figure, but a stale unused leg must not block a position close
        // (same reasoning as the entry-cycle exit path).
        validate_route_leg(
            forward_route,
            sell_token,
            buy_token,
            evaluation_time,
            self.config.max_quote_age_secs,
            self.config.max_favourable_quote_deviation_bps,
            self.config.max_reference_price_age_secs,
        )?;
        if row.requested_sell_amount.as_deref() != Some(open_raw.as_str())
            || forward_route.sell_amount != open_raw
        {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                format!(
                    "open-quantity exit row was not quoted at {open_raw}: requested {:?}, route \
                     sellAmount {}",
                    row.requested_sell_amount, forward_route.sell_amount
                ),
            ));
        }
        // Informational only for an exit (the cost cap is entry-only, see
        // build_plan): a reverse-leg failure already tolerated above can
        // leave this absent, so default rather than hold on it.
        let verified_round_trip_loss_bps = row
            .optimistic_round_trip_loss_bps
            .as_deref()
            .map(|value| parse_round_trip_loss_bps("optimistic_round_trip_loss_bps", Some(value)))
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        Ok(Some(SnapshotContext {
            token_a: price.token_a.clone(),
            token_b: price.token_b.clone(),
            token_a_price_usd: price.token_a_price_usd,
            token_b_price_usd: price.token_b_price_usd,
            row: row.clone(),
            exit_leg_sizing: ExitLegSizing::OpenQuantityRow,
            verified_round_trip_loss_bps,
        }))
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

        let route = match (trigger, context.exit_leg_sizing) {
            (ArcusSpotRotationTrigger::EntrySignal, _)
            | (
                ArcusSpotRotationTrigger::MeanReversionExit
                | ArcusSpotRotationTrigger::MaxHoldExit
                | ArcusSpotRotationTrigger::CorporateActionExit,
                ExitLegSizing::OpenQuantityRow,
            ) => context.row.forward.as_ref().expect("validated forward"),
            (
                ArcusSpotRotationTrigger::MeanReversionExit
                | ArcusSpotRotationTrigger::MaxHoldExit
                | ArcusSpotRotationTrigger::CorporateActionExit,
                ExitLegSizing::EntryCycleReverseLeg,
            ) => context.row.reverse.as_ref().expect("validated reverse"),
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
            self.config.max_favourable_quote_deviation_bps,
            self.config.max_reference_price_age_secs,
        )?;
        let quote = select_route_quote(
            route,
            sell_token,
            buy_token,
            self.config.max_favourable_quote_deviation_bps,
            self.config.max_reference_price_age_secs,
        )?
        .quote;

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
                // An open-quantity row was requested at exactly this
                // quantity, so anything else means the row does not
                // describe the exit it was selected for.
                if context.exit_leg_sizing == ExitLegSizing::OpenQuantityRow
                    && sell_quantity != open_quantity
                {
                    return Err(ArcusSpotHold::new(
                        ArcusSpotHoldCode::RotationLimit,
                        format!(
                            "open-quantity exit row sells {} {} but {} is open",
                            sell_quantity, sell_token.symbol, open_quantity
                        ),
                    ));
                }
                if context.exit_leg_sizing == ExitLegSizing::EntryCycleReverseLeg
                    && sell_quantity > open_quantity
                {
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

/// Standalone form of `ArcusSpotRuntime::open_exit_fixed_sell_amount_row`
/// One part per billion of the rotation being closed. What is left of an
/// exit below this is settlement dust, not exposure.
///
/// A route may take slightly less of the signed sell amount than it pulled
/// and refund the remainder inside the settlement transaction
/// (bot-strategy#979) -- 16 wei of an 18-decimal token, in the live case
/// that motivated this. Carrying that residue as a still-open rotation
/// would strand the bot: exits are sized at exactly the tracked open
/// quantity, so the next tick would ask the venue to quote a few wei
/// forever. A billionth of a ~0.5-unit rotation is ~5e-10 units (well under
/// a millionth of a cent at these prices), far below any position this
/// strategy can hold deliberately and far above the wei-scale residue a
/// refund leaves.
const ROTATION_DUST_FRACTION: Decimal = Decimal::from_parts(1, 0, 0, false, 9);

/// What remains open after an exit sold `sold` of a `open`-sized rotation:
/// `None` once nothing but dust is left, so the caller closes the regime.
fn rotation_remaining_after_exit(open: Decimal, sold: Decimal) -> Result<Option<Decimal>, String> {
    if sold > open {
        return Err("confirmed exit sold more than tracked open quantity".to_string());
    }
    let remaining = open
        .checked_sub(sold)
        .ok_or("confirmed exit quantity subtraction overflow")?;
    let dust_ceiling = open
        .checked_mul(ROTATION_DUST_FRACTION)
        .ok_or("rotation dust ceiling overflow")?;
    Ok((remaining > dust_ceiling).then_some(remaining))
}

/// for a caller that only has the checkpointed regime/rotated_quantity and
/// the configured pair, not a full `ArcusSpotRuntime` -- e.g. live-tick's
/// unlocked pre-fetch peek at the checkpoint (bot-strategy#906), which must
/// not pay for `ArcusSpotRuntimeCheckpointStore::load_existing`'s config
/// drift comparison and journal logging twice on top of the locked load
/// the actual decision is made against.
pub fn open_exit_fixed_sell_amount_row_for(
    regime: ArcusSpotRegime,
    rotated_quantity: Option<Decimal>,
    pair: &ArcusSpotPair,
    sell_token_decimals: u32,
) -> Result<Option<ArcusSpotFixedSellAmountRow>, String> {
    let Some(open_quantity) = rotated_quantity else {
        return Ok(None);
    };
    let (sell_symbol, buy_symbol) = match regime {
        ArcusSpotRegime::Neutral => return Ok(None),
        ArcusSpotRegime::RotatedAToB => (pair.buy_symbol.clone(), pair.sell_symbol.clone()),
        ArcusSpotRegime::RotatedBToA => (pair.sell_symbol.clone(), pair.buy_symbol.clone()),
    };
    let sell_amount_raw =
        quantity_to_raw_amount(open_quantity, sell_token_decimals).map_err(|detail| {
            format!("open rotation quantity {open_quantity} {sell_symbol}: {detail}")
        })?;
    Ok(Some(ArcusSpotFixedSellAmountRow {
        pair: ArcusSpotPair {
            sell_symbol,
            buy_symbol,
        },
        sell_amount_raw,
    }))
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

/// The one place a leg's venue quote is chosen for planning, costing and
/// dispatch: `plausible_best_quote` against the router's own referencePrice,
/// never the router's bare `recommended` (bot-strategy#1001). A leg with no
/// usable reference, or no venue inside the band, is a route this tick
/// cannot use -- a hold, not an invalid snapshot.
fn select_route_quote<'a>(
    route: &'a ArcusSpotRouteObservation,
    sell_token: &ArcusSpotToken,
    buy_token: &ArcusSpotToken,
    max_favourable_quote_deviation_bps: Decimal,
    max_reference_price_age_secs: i64,
) -> Result<ArcusSpotSelectedQuote<'a>, ArcusSpotHold> {
    route
        .response
        .payload
        .plausible_best_quote(
            sell_token.decimals,
            buy_token.decimals,
            max_favourable_quote_deviation_bps,
            route.response.received_at,
            max_reference_price_age_secs,
        )
        .map_err(|error| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                format!(
                    "{}->{} leg has no plausible venue quote: {error}",
                    sell_token.symbol, buy_token.symbol
                ),
            )
        })
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
    max_favourable_quote_deviation_bps: Decimal,
    max_reference_price_age_secs: i64,
) -> Result<(), ArcusSpotHold> {
    validate_route(route, sell_token, buy_token)?;
    let selected = select_route_quote(
        route,
        sell_token,
        buy_token,
        max_favourable_quote_deviation_bps,
        max_reference_price_age_secs,
    )?;
    if selected.quote.sell_amount != route.sell_amount {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::RouteUnavailable,
            "selected quote sell amount does not match route request",
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
    sell_token: &ArcusSpotToken,
    buy_token: &ArcusSpotToken,
    max_favourable_quote_deviation_bps: Decimal,
    max_reference_price_age_secs: i64,
) -> Result<Decimal, ArcusSpotHold> {
    let forward = row
        .forward
        .as_ref()
        .expect("caller has already verified forward is present");
    let reverse = row
        .reverse
        .as_ref()
        .expect("caller has already verified reverse is present");
    let forward_quote = select_route_quote(
        forward,
        sell_token,
        buy_token,
        max_favourable_quote_deviation_bps,
        max_reference_price_age_secs,
    )?;
    let reverse_quote = select_route_quote(
        reverse,
        buy_token,
        sell_token,
        max_favourable_quote_deviation_bps,
        max_reference_price_age_secs,
    )?;
    // The recorder chained the reverse leg off the venue *it* selected. If
    // this runtime's band selects a different venue for the same payload,
    // the row's return amount and loss describe a different round trip than
    // the one about to be costed.
    for (leg, recorded, selected) in [
        (
            "forward",
            row.forward_venue.as_deref(),
            forward_quote.quote.venue.as_str(),
        ),
        (
            "reverse",
            row.reverse_venue.as_deref(),
            reverse_quote.quote.venue.as_str(),
        ),
    ] {
        if let Some(recorded) = recorded {
            if !recorded.eq_ignore_ascii_case(selected) {
                return Err(ArcusSpotHold::new(
                    ArcusSpotHoldCode::InvalidSnapshot,
                    format!(
                        "recorder selected {recorded} for the {leg} leg but this runtime selects \
                         {selected}; the plausibility bands disagree"
                    ),
                ));
            }
        }
    }
    if forward_quote.quote.buy_amount != reverse.sell_amount {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            "reverse route sellAmount does not match the forward selected buyAmount",
        ));
    }
    if let Some(recorded_return) = row.optimistic_return_amount.as_deref() {
        if recorded_return != reverse_quote.quote.buy_amount {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "recorded optimistic return amount does not match the reverse selected buyAmount",
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
    let returned = Decimal::from_str(&reverse_quote.quote.buy_amount).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("reverse selected buyAmount is invalid: {error}"),
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
    // Each leg was judged against its own referencePrice, and those are
    // supplied independently by the router. They must be reciprocals (the
    // same market seen from both sides; measured 0.0 bps apart live), or a
    // pair that disagrees by more than the band could pass both leg checks
    // while together implying an arbitrarily favourable -- and fictitious
    // -- round trip (Codex P1, pairtrade#323).
    let forward_reference = reference_price_of(forward, "forward")?;
    let reverse_reference = reference_price_of(reverse, "reverse")?;
    let reference_gap_bps = forward_reference
        .checked_mul(reverse_reference)
        .and_then(|product| product.checked_sub(Decimal::ONE))
        .and_then(|gap| gap.checked_mul(Decimal::from(10_000)))
        .map(|gap| gap.abs())
        .ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InvalidSnapshot,
                "reference reciprocity exceeds Decimal range",
            )
        })?;
    if reference_gap_bps > max_favourable_quote_deviation_bps {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "forward and reverse referencePrices disagree by {} bps, more than the {} bps \
                 band; they do not describe one market",
                reference_gap_bps.round_dp(3).normalize(),
                max_favourable_quote_deviation_bps.normalize()
            ),
        ));
    }
    let recorded = parse_signed_round_trip_loss_bps(
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
    // A genuine round trip never pays the taker; a favourable one is the
    // reference lagging a moving market between the two legs' quotes, and
    // the reciprocity check above already caps how far the two references
    // may disagree at one band. So one band is also the most a lag artefact
    // can be worth: a gain beyond it is not lag but an inconsistency the
    // per-leg checks cannot see, and it is refused rather than costed as
    // free (Codex P1, pairtrade#323).
    if recomputed < -max_favourable_quote_deviation_bps {
        return Err(ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!(
                "round trip returns {} bps more than it started with, beyond the {} bps a \
                 lagging reference can explain",
                (-recomputed).round_dp(3).normalize(),
                max_favourable_quote_deviation_bps.normalize()
            ),
        ));
    }
    // Signed agreement is what proves the legs chain; the cost gate itself
    // never credits a favourable round trip (see parse_round_trip_loss_bps).
    Ok(recomputed.max(Decimal::ZERO))
}

fn reference_price_of(
    route: &ArcusSpotRouteObservation,
    leg: &str,
) -> Result<Decimal, ArcusSpotHold> {
    route.response.payload.reference_price().map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::RouteUnavailable,
            format!("{leg} leg: {error}"),
        )
    })
}

/// Shared by `apply_confirmed_live_fill` (post-fill commit) and
/// `ArcusSpotRuntime::validate_plan_consistent_with_state` (pre-dispatch
/// check): only these regime/trigger/direction combinations can ever be
/// committed without corrupting the runtime's entry/exit state machine.
/// What the corporate-action guard decided for one tick.
#[derive(Debug, Clone, Default)]
struct CorporateActionGate {
    /// Present when no new rotation may be opened. Exits are never blocked
    /// by this guard: a window that could trap an open position through the
    /// event it exists to protect against would be worse than no guard.
    block_entry: Option<ArcusSpotHold>,
    /// `reduce_exit_at` has passed and any open rotation must be unwound
    /// now, whatever the signal says.
    force_exit: bool,
    /// The pre-event signal window has been discarded and no further
    /// samples are accumulated until `resume_not_before`. The interval
    /// between the effective time and the operator's resume time is
    /// exactly the stretch whose prints cannot be trusted to describe
    /// either the old or the new instrument, so feeding them into the
    /// window would just rebuild the same contamination the discard
    /// removed.
    suppress_history: bool,
    /// From `effective_at` until the resume: the tracked open quantity is
    /// in units the venue no longer quotes, so *no* runtime-generated exit
    /// -- forced, max-hold or mean-reversion -- may be sized from it. The
    /// reduce phase before `effective_at` is where exits happen; past it the
    /// position is the operator's to reconcile (Codex P1, pairtrade#309).
    suppress_exits: bool,
}

impl ArcusSpotRuntime {
    /// Applies the declared corporate-action calendar to this tick.
    ///
    /// Runs after prices, risk marks and halt engagement, and before the
    /// rotation signal: marks and risk accounting must continue through a
    /// window (a blocked bot still holds inventory and still carries its
    /// beta), while entry selection must not.
    fn corporate_action_gate(
        &mut self,
        evaluation_time: DateTime<Utc>,
        price: &PriceContext,
    ) -> CorporateActionGate {
        let mut gate = self.corporate_action_gate_inner(evaluation_time, price);
        // A new declaration wearing a handled id is never active (see
        // `active_corporate_action`), so its window would be skipped
        // entirely. It is refused here -- as an overlay on whatever the
        // live progress already decided, never in place of it: returning
        // early with both suppressions off let a reused id re-open the
        // window and the exits of an event already past `effective_at`
        // (Codex P1, pairtrade#309).
        if let Some(reused) = self
            .config
            .corporate_actions
            .iter()
            .find(|event| self.handled_record_for(event) == Some(HandledMatch::ReusedId))
            .cloned()
        {
            gate.block_entry.get_or_insert_with(|| {
                ArcusSpotHold::new(
                    ArcusSpotHoldCode::CorporateActionBlock,
                    format!(
                        "corporate action {} reuses the id of an already handled event for a \
                         different declaration; give the new action its own event_id",
                        reused.event_id,
                    ),
                )
            });
            // Record the window durably the first time it opens. Without
            // this the refusal is derived from the config alone, so removing
            // the declaration erased the fail-closed state and the next tick
            // resumed accumulating history and trading on unreconciled
            // pre-event inventory (Codex P1, pairtrade#309).
            if self.state.corporate_action.is_none() && evaluation_time >= reused.entry_block_at {
                self.record_corporate_action_progress(&reused, evaluation_time);
                // The same tick must also apply the window's phases: after
                // downtime this can be the only dispatchable observation in
                // the reduce phase, and leaving it to the next tick strands
                // an open rotation past the settlement cutoff (Codex P1,
                // pairtrade#309).
                return self.refused_declaration_gate(&reused, evaluation_time, price);
            }
            // Its effective phase is honoured here too, for the tick that
            // opens the record: once the cutoff is reached the venue may be
            // quoting new units, and the same rule as for a declared event
            // applies -- no exit sized from the tracked quantity, no
            // post-event prints into the window.
            if evaluation_time >= reused.effective_at {
                gate.force_exit = false;
                gate.suppress_history = true;
                gate.suppress_exits = true;
            }
        }
        // A window that opens within the settlement margin blocks entries at
        // dispatch, so the planner must not produce a plan its own dispatch
        // will refuse: `live-tick` would persist pending-plan evidence for
        // it, and `ReplaySimulation` -- which never runs the live validator
        // -- would record a fill production will not send (Codex P2,
        // pairtrade#309).
        if gate.block_entry.is_none() {
            if let Some(reason) = self.corporate_action_blocks_entries(evaluation_time) {
                gate.block_entry = Some(ArcusSpotHold::new(
                    ArcusSpotHoldCode::CorporateActionBlock,
                    format!("entries are already blocked at dispatch: {reason}"),
                ));
            }
        }
        gate
    }

    fn corporate_action_gate_inner(
        &mut self,
        evaluation_time: DateTime<Utc>,
        price: &PriceContext,
    ) -> CorporateActionGate {
        // Cheapest possible path for the overwhelmingly common case: an
        // empty calendar cannot change any decision, and must not cost one.
        // Only when there is no open window to resolve, though -- emptying
        // the calendar is one of the ways an operator can delete a live
        // declaration, and this path used to erase its progress and return
        // an unrestricted gate.
        if self.config.corporate_actions.is_empty() && self.state.corporate_action.is_none() {
            return CorporateActionGate::default();
        }

        let Some(event) = self.active_corporate_action(evaluation_time) else {
            // Retiring a declaration whose window never opened is ordinary
            // housekeeping: nothing was pinned, nothing was discarded,
            // nothing needs resolving. Once a window *has* opened, its
            // progress record is the only thing that remembers the
            // pre-event identity and the discarded signal window, and past
            // `effective_at` the tracked inventory is denominated in units
            // the venue no longer quotes. Dropping the record because the
            // declaration disappeared would resume sampling -- and
            // eventually trading -- on that inventory, with the event never
            // reconciled and never marked handled. Deleting or renaming a
            // live entry is not a way to cancel a window (Codex P1,
            // pairtrade#309).
            if let Some(progress) = self.state.corporate_action.clone() {
                // Its declaration may still be there and merely refused (a
                // handled id reused for a different action): that has known
                // instants, so it keeps the ordinary phases and only the
                // resume is impossible.
                if let Some(refused) = self
                    .config
                    .corporate_actions
                    .iter()
                    .find(|event| {
                        Self::progress_matches(&progress, event)
                            && self.handled_record_for(event) == Some(HandledMatch::ReusedId)
                    })
                    .cloned()
                {
                    return self.refused_declaration_gate(&refused, evaluation_time, price);
                }
                return self.undeclared_progress_gate(
                    evaluation_time,
                    &progress,
                    "is no longer declared in",
                );
            }
            self.state.corporate_action = None;
            return CorporateActionGate::default();
        };
        let event = event.clone();

        // Entering the window: pin the pre-event token identity now, from
        // the last observation taken *before* this one. Reading it from the
        // current snapshot instead would pin whatever the event may already
        // have changed.
        match self.state.corporate_action.as_mut() {
            Some(progress) if Self::progress_matches(progress, &event) => {
                // Same declaration, possibly under a new label: keep the
                // pins and the discard stamp, adopt the name (Codex P1,
                // pairtrade#309).
                if progress.event_id != event.event_id {
                    progress.event_id = event.event_id.clone();
                }
                if progress.fingerprint.is_empty() {
                    progress.fingerprint = event.fingerprint();
                }
            }
            Some(progress) => {
                // A different declaration under this label (other symbols
                // or other instants). Replacing the record would hand the
                // new event pins taken inside the old window -- or none --
                // and drop the discard stamp; the old one has to be
                // resolved first.
                let progress = progress.clone();
                return self.undeclared_progress_gate(
                    evaluation_time,
                    &progress,
                    "was replaced by a different declaration in",
                );
            }
            None => {}
        }
        if self.state.corporate_action.is_none() {
            // Only an observation taken strictly before the window opened
            // describes the pre-event instrument. A calendar installed after
            // `entry_block_at` on a runtime that kept ticking has a
            // `last_token_*_identity` from inside -- or after -- the event,
            // and pinning that would compare the new contract against itself
            // and report no drift (Codex P2, pairtrade#309). Recording the
            // pre-event side as unavailable is the honest answer there; the
            // operator's reconciled `post_event_inventory` is what carries
            // the resume.
            self.record_corporate_action_progress(&event, evaluation_time);
        }

        if evaluation_time >= event.effective_at {
            let already_invalidated = self
                .state
                .corporate_action
                .as_ref()
                .is_some_and(|progress| progress.history_invalidated_at.is_some());
            if !already_invalidated {
                self.state.relative_log_price_history.clear();
                if let Some(progress) = self.state.corporate_action.as_mut() {
                    progress.history_invalidated_at = Some(evaluation_time);
                }
            }
            // A rotation still open here is not unwound by the runtime.
            // `rotated_quantity` is in pre-event units -- the same units this
            // phase already refuses to value for a loss halt -- and an exit
            // sized from it sells one old unit as one new unit after a split
            // (marking the rotation flat with the split-adjusted remainder
            // still held) or submits an unfillable oversized order after a
            // reverse split. The forced exit belongs to the reduce phase,
            // before `effective_at`; past it, the position is the operator's
            // to reconcile (Codex P1, pairtrade#309).
            if self.state.regime != ArcusSpotRegime::Neutral {
                return CorporateActionGate {
                    block_entry: Some(ArcusSpotHold::new(
                        ArcusSpotHoldCode::CorporateActionUnresolved,
                        format!(
                            "corporate action {} is effective with the rotation still open \
                             ({:?}); the tracked open quantity is in pre-event units and will \
                             not be sized into an exit -- reconcile the position and the \
                             runtime state before the resume",
                            event.event_id, self.state.regime,
                        ),
                    )),
                    force_exit: false,
                    suppress_history: true,
                    suppress_exits: true,
                };
            }
        }

        // Identity drift, for the *whole* window rather than from
        // `effective_at`. `effective_at` is the operator's declared date;
        // the issuer's ticker can be repointed before it, and the reduce
        // phase is precisely where the runtime forces an exit -- which would
        // route the old instrument's `rotated_quantity` to the new contract,
        // selling the wrong asset if the wallet holds any of the replacement
        // and submitting an unfillable amount otherwise. Every action below
        // plans against tracked pre-event state, so nothing may pass this
        // point once the pair no longer names the same instrument (Codex P1,
        // pairtrade#309).
        if let Some(hold) = self.corporate_action_identity_drift(&event, price) {
            return CorporateActionGate {
                block_entry: Some(hold),
                force_exit: false,
                suppress_history: true,
                suppress_exits: true,
            };
        }

        // A pin the runtime never took is not a passed check. The window may
        // have been declared after it opened, or the first observation may
        // have arrived after downtime -- either way the ticker could already
        // have been repointed, and an exit sized from `rotated_quantity`
        // would route the old instrument's quantity to whatever the symbol
        // now resolves to. The resume is still allowed without a pin (the
        // operator's reconciled holding carries it, and it requires a flat
        // wallet anyway); only exits are refused (Codex P1, pairtrade#309).
        if self.state.regime != ArcusSpotRegime::Neutral {
            if let Some(symbol) = self.corporate_action_unpinned_symbol(&event) {
                return CorporateActionGate {
                    block_entry: Some(ArcusSpotHold::new(
                        ArcusSpotHoldCode::CorporateActionUnresolved,
                        format!(
                            "corporate action {} has no pre-event identity for {symbol}, so an \
                             exit cannot be checked against the instrument the tracked open \
                             quantity refers to; reconcile the position",
                            event.event_id,
                        ),
                    )),
                    force_exit: false,
                    suppress_history: evaluation_time >= event.effective_at,
                    suppress_exits: true,
                };
            }
        }

        // The evaluation clock reaching `resume_not_before` is not enough.
        // The resume values the reconciled holding at *this* observation's
        // prices and seeds the emptied window with their ratio, so the
        // observation itself must come from after the cutoff. A snapshot
        // collected shortly before it and still inside `max_quote_age_secs`
        // describes exactly the interval the calendar declares
        // untrustworthy -- the same prints the discard at `effective_at`
        // threw away. Judged on the earliest thing the context describes
        // (the overview's own receipt, or the collection start that
        // captured the identities), not on when the collection finished
        // (Codex P1 x2, pairtrade#309).
        let observed_at = price.observed_at;
        let observation_is_post_cutoff = observed_at >= event.resume_not_before;
        if evaluation_time < event.resume_not_before || !observation_is_post_cutoff {
            let hold = ArcusSpotHold::new(
                ArcusSpotHoldCode::CorporateActionBlock,
                if observation_is_post_cutoff {
                    format!(
                        "corporate action {} ({}) blocks entries until {}; source: {}",
                        event.event_id,
                        event.symbols.join("+"),
                        event.resume_not_before,
                        event.source,
                    )
                } else {
                    format!(
                        "corporate action {} ({}) reached its resume time, but the latest \
                         observation was collected at {}, before {}; the resume waits for an \
                         observation taken after the event. source: {}",
                        event.event_id,
                        event.symbols.join("+"),
                        observed_at,
                        event.resume_not_before,
                        event.source,
                    )
                },
            );
            let settlement_cutoff =
                self.corporate_action_settlement_cutoff_reached(&event, evaluation_time);
            return CorporateActionGate {
                block_entry: Some(hold),
                // Only in the reduce phase, and only while an exit could
                // still settle before the cutoff. Past `effective_at` an
                // open rotation has already returned above with its own
                // hold.
                force_exit: evaluation_time >= event.reduce_exit_at
                    && !settlement_cutoff
                    && self.state.regime != ArcusSpotRegime::Neutral,
                suppress_history: evaluation_time >= event.effective_at,
                suppress_exits: settlement_cutoff,
            };
        }

        // At or past resume_not_before, with the event still unhandled and
        // the position flat (an open rotation past `effective_at` returned
        // above and stays there until the operator reconciles it).
        let Some(post_event_inventory) = event.post_event_inventory else {
            return CorporateActionGate {
                block_entry: Some(ArcusSpotHold::new(
                    ArcusSpotHoldCode::CorporateActionResumePending,
                    format!(
                        "corporate action {} reached its resume time without a reconciled \
                         post_event_inventory; the tracked inventory still describes the \
                         pre-event holding",
                        event.event_id,
                    ),
                )),
                force_exit: false,
                suppress_history: true,
                suppress_exits: true,
            };
        };

        // Resume. The reconciled holding replaces the tracked one, and both
        // risk baskets are re-anchored to it: they are buy-and-hold
        // counterfactuals (see `initial_baseline_inventory`), and a basket
        // that no longer exists would report the corporate action itself as
        // a rotation loss and engage the sticky stop on it -- bot-strategy
        // #813's failure mode, reached by a different road.
        // Priced before anything is written. A reconciled quantity that
        // overflows at the current reference prices cannot re-anchor the
        // baskets, and a resume that lands the inventory while leaving the
        // three equity marks on their pre-event values is worse than no
        // resume: the checkpoint loads, every later tick fails its initial
        // valuation, and the event is already in
        // `handled_corporate_action_ids`, so corrected quantities can never
        // be applied. Keep it pending instead.
        // Floors are checked here, on the pending reconciliation, rather
        // than in config validation: there the check also ran against every
        // *handled* event's historical holding, so raising a floor later
        // invalidated the whole config over a quantity that will never be
        // applied again (Codex P2, pairtrade#309). A reconciled holding
        // under a floor is still refused -- it would persist a holding no
        // later checkpoint load accepts -- but as a pending hold the
        // operator can act on, not a wedge one tick later.
        if post_event_inventory.token_a < self.config.inventory_floors.token_a
            || post_event_inventory.token_b < self.config.inventory_floors.token_b
        {
            return CorporateActionGate {
                block_entry: Some(ArcusSpotHold::new(
                    ArcusSpotHoldCode::CorporateActionResumePending,
                    format!(
                        "corporate action {} has a reconciled post_event_inventory (token_a={}, \
                         token_b={}) below inventory_floors (token_a={}, token_b={}); the resume \
                         waits for the floors to be lowered or the quantities corrected",
                        event.event_id,
                        post_event_inventory.token_a,
                        post_event_inventory.token_b,
                        self.config.inventory_floors.token_a,
                        self.config.inventory_floors.token_b,
                    ),
                )),
                force_exit: false,
                suppress_history: true,
                suppress_exits: true,
            };
        }
        let Some(equity) = post_event_inventory
            .checked_value_usd(price.token_a_price_usd, price.token_b_price_usd)
        else {
            return CorporateActionGate {
                block_entry: Some(ArcusSpotHold::new(
                    ArcusSpotHoldCode::CorporateActionResumePending,
                    format!(
                        "corporate action {} has a reconciled post_event_inventory with no \
                         representable USD value at the current reference prices; the resume \
                         waits for the reconciled quantities to be corrected",
                        event.event_id,
                    ),
                )),
                force_exit: false,
                suppress_history: true,
                suppress_exits: true,
            };
        };
        self.state.inventory = post_event_inventory;
        self.state.initial_baseline_inventory = Some(post_event_inventory);
        self.state.daily_baseline_inventory = Some(post_event_inventory);
        self.state.initial_equity_usd = Some(equity);
        self.state.daily_baseline_equity_usd = Some(equity);
        self.state.last_equity_usd = Some(equity);
        self.state
            .handled_corporate_action_ids
            .push(event.event_id.clone());
        // The two lists are read side by side by index. A checkpoint from
        // before fingerprints existed has ids and no fingerprints, so the
        // first fingerprint pushed after loading it would sit at index 0,
        // beside the *oldest* legacy id -- which `handled_record_for` would
        // then read as that id having been reused, blocking entries for
        // good. Pad the legacy entries first so the new pair lines up
        // (Codex P1, pairtrade#309).
        let ids = self.state.handled_corporate_action_ids.len();
        self.state
            .handled_corporate_action_fingerprints
            .resize(ids - 1, String::new());
        self.state
            .handled_corporate_action_fingerprints
            .push(event.fingerprint());
        self.state.corporate_action = None;
        // The signal window was emptied at `effective_at` and nothing was
        // added to it since, so the ordinary warm-up gate now supplies the
        // "min_signal_samples fresh informative post-event observations
        // before a new entry" requirement without a second counter that
        // could disagree with it.
        CorporateActionGate::default()
    }

    /// True while the tracked inventory is denominated in units the venue
    /// no longer quotes: a declared window has passed its `effective_at`
    /// and its resume has not been committed (either because
    /// `post_event_inventory` is still missing, or because the forced exit
    /// has not unwound). Any equity derived from `state.inventory` is
    /// meaningless until the reconciled holding replaces it.
    fn corporate_action_units_are_stale(&self, evaluation_time: DateTime<Utc>) -> bool {
        // The stamp is the durable half: it is written at `effective_at`
        // and cleared only by the resume, so it survives a restart and an
        // operator deleting the declaration out from under an open window.
        if let Some(progress) = self.state.corporate_action.as_ref() {
            if progress.history_invalidated_at.is_some() {
                // ... but the stamp is a fact about the *calendar*, not about
                // this mark. An observation whose prices predate the cutoff
                // is still a pre-event mark, however late it is processed --
                // an ordinary overview lag on the tick after the stamp would
                // otherwise drop a genuine breach for good, and disagree
                // with the continuity verifier, which already applies this
                // escape (independent review, pairtrade#309).
                return corporate_action_effective_cutoff(progress, &self.config)
                    .is_none_or(|effective_at| evaluation_time >= effective_at);
            }
            // An orphaned record (its declaration deleted or replaced) past
            // its own recorded cutoff is stale before the gate has run this
            // tick and stamped it; one with no recorded cutoff cannot say
            // and is treated as stale rather than halted on.
            let declared = self
                .config
                .corporate_actions
                .iter()
                .any(|event| Self::progress_matches(progress, event));
            if !declared {
                // "Orphaned" covers two shapes, and only one of them is a
                // deletion. An amendment that moves `effective_at` *earlier*
                // -- a widening the checkpoint's own removal scan accepts --
                // no longer matches the stamped fingerprint either, but it is
                // the same window, and the venue re-denominates the units
                // when the calendar says so, not when the stamp does. Reading
                // only the superseded cutoff leaves the span between the two
                // instants looking like valid units, where `step_at` can
                // engage a sticky loss halt off post-event prices -- and that
                // halt then wedges recovery, because the unresolved window
                // blocks trading while `reset-window` and post-cutoff halt
                // clearance both refuse it. Take the earliest cutoff any
                // surviving declaration of this window names (Codex P1,
                // pairtrade#309).
                return corporate_action_effective_cutoff(progress, &self.config)
                    .is_none_or(|effective_at| evaluation_time >= effective_at);
            }
        }
        // ... and the calendar covers the tick that first crosses
        // `effective_at`, where the halt is evaluated before the gate has
        // had a chance to write the stamp.
        if self
            .active_corporate_action(evaluation_time)
            .is_some_and(|event| evaluation_time >= event.effective_at)
        {
            return true;
        }
        // A refused declaration (a handled id reused for a different action)
        // never becomes active and never gets a progress record, yet its
        // cutoff is as real as any other: past it the venue may quote new
        // units. The gate's overlay already suppresses its exits; this is the
        // same fact for everything that asks the predicate directly -- the
        // dispatch validator above all, which otherwise accepts an exit
        // planned before the cutoff and dispatched after it (Codex P1,
        // pairtrade#309).
        self.config.corporate_actions.iter().any(|event| {
            self.handled_record_for(event) == Some(HandledMatch::ReusedId)
                && evaluation_time >= event.effective_at
        })
    }

    /// The one declared window this tick falls in, if any: unhandled, and
    /// either still open or past its resume time and waiting to be closed
    /// out. `validate_corporate_actions` guarantees the windows are
    /// disjoint, so at most one can match.
    fn active_corporate_action(
        &self,
        evaluation_time: DateTime<Utc>,
    ) -> Option<&ArcusSpotCorporateActionEvent> {
        self.config
            .corporate_actions
            .iter()
            .filter(|event| !self.corporate_action_is_handled(event))
            .find(|event| evaluation_time >= event.entry_block_at)
    }

    /// Opens the durable record for `event`: what the window is, when its
    /// cutoff is, and the token identities as of the last observation
    /// *before* it opened. Written for a refused (reused-id) declaration as
    /// well as a declared one, so that deleting the declaration cannot erase
    /// the fail-closed state (Codex P1, pairtrade#309).
    fn record_corporate_action_progress(
        &mut self,
        event: &ArcusSpotCorporateActionEvent,
        evaluation_time: DateTime<Utc>,
    ) {
        let pre_event_observed = self
            .state
            .last_token_identity_at
            .is_some_and(|observed_at| observed_at < event.entry_block_at);
        self.state.corporate_action = Some(ArcusSpotCorporateActionProgress {
            event_id: event.event_id.clone(),
            blocked_at: evaluation_time,
            pre_event_token_a: pre_event_observed
                .then(|| self.state.last_token_a_identity.clone())
                .flatten(),
            pre_event_token_b: pre_event_observed
                .then(|| self.state.last_token_b_identity.clone())
                .flatten(),
            history_invalidated_at: None,
            fingerprint: event.fingerprint(),
            effective_at: Some(event.effective_at),
            symbols: event.symbols.clone(),
        });
    }

    /// The gate for a window whose declaration is present but *refused* --
    /// a handled id reused for a different action. Its instants are known,
    /// so it behaves exactly like a declared window except that it can never
    /// resume: the operator has to give the new action its own `event_id`,
    /// after which the same record becomes an ordinary active window.
    fn refused_declaration_gate(
        &mut self,
        event: &ArcusSpotCorporateActionEvent,
        evaluation_time: DateTime<Utc>,
        price: &PriceContext,
    ) -> CorporateActionGate {
        if evaluation_time >= event.effective_at {
            let already = self
                .state
                .corporate_action
                .as_ref()
                .is_some_and(|progress| progress.history_invalidated_at.is_some());
            if !already {
                self.state.relative_log_price_history.clear();
                if let Some(progress) = self.state.corporate_action.as_mut() {
                    progress.history_invalidated_at = Some(evaluation_time);
                }
            }
        }
        // The same identity gates the declared path applies: a refused
        // declaration is still a window, and an exit sized from
        // `rotated_quantity` must not be routed to a contract the symbol was
        // repointed to -- nor submitted at all when there is no pinned
        // identity to check it against (Codex P1 x2, pairtrade#309).
        if let Some(hold) = self.corporate_action_identity_drift(event, price) {
            return CorporateActionGate {
                block_entry: Some(hold),
                force_exit: false,
                suppress_history: true,
                suppress_exits: true,
            };
        }
        if self.state.regime != ArcusSpotRegime::Neutral {
            if let Some(symbol) = self.corporate_action_unpinned_symbol(event) {
                return CorporateActionGate {
                    block_entry: Some(ArcusSpotHold::new(
                        ArcusSpotHoldCode::CorporateActionUnresolved,
                        format!(
                            "corporate action {} reuses a handled id and has no pre-event \
                             identity for {symbol}, so an exit cannot be checked against the \
                             instrument the tracked open quantity refers to; reconcile the \
                             position",
                            event.event_id,
                        ),
                    )),
                    force_exit: false,
                    suppress_history: evaluation_time >= event.effective_at,
                    suppress_exits: true,
                };
            }
        }
        let effective = evaluation_time >= event.effective_at;
        let settlement_cutoff =
            self.corporate_action_settlement_cutoff_reached(event, evaluation_time);
        CorporateActionGate {
            block_entry: Some(ArcusSpotHold::new(
                ArcusSpotHoldCode::CorporateActionBlock,
                format!(
                    "corporate action {} reuses the id of an already handled event for a \
                     different declaration; give the new action its own event_id",
                    event.event_id,
                ),
            )),
            force_exit: !settlement_cutoff
                && evaluation_time >= event.reduce_exit_at
                && self.state.regime != ArcusSpotRegime::Neutral,
            suppress_history: effective,
            suppress_exits: settlement_cutoff,
        }
    }

    /// The fail-closed gate for a progress record whose declaration is gone
    /// or has been replaced. It forces nothing and suppresses every exit,
    /// whatever phase the record was in when the declaration vanished.
    /// Without the declaration there is no `effective_at` left to say when
    /// the tracked units stop being the quoted ones, so a record deleted in
    /// the reduce phase could never transition to stale and would keep
    /// forcing exits from `rotated_quantity` straight through the event --
    /// the split under-sale / reverse-split over-sale by another road. An
    /// open position under an undeclared window is the operator's to
    /// reconcile, full stop (Codex P1 x3, pairtrade#309).
    fn undeclared_progress_gate(
        &mut self,
        evaluation_time: DateTime<Utc>,
        progress: &ArcusSpotCorporateActionProgress,
        because: &str,
    ) -> CorporateActionGate {
        // The one thing the declared path does at `effective_at` that this
        // record still needs done: discard the pre-event window and stamp
        // it, so halt suppression, the continuity verifier and anything
        // else keyed on the stamp see the same transition at the same
        // instant whether or not the declaration survived.
        if progress.history_invalidated_at.is_none()
            && progress
                .effective_at
                .is_some_and(|effective_at| evaluation_time >= effective_at)
        {
            self.state.relative_log_price_history.clear();
            if let Some(live) = self.state.corporate_action.as_mut() {
                live.history_invalidated_at = Some(evaluation_time);
            }
        }
        CorporateActionGate {
            block_entry: Some(ArcusSpotHold::new(
                ArcusSpotHoldCode::CorporateActionBlock,
                format!(
                    "corporate action {} opened at {} and {} the approved config; restore \
                     that declaration and its reconciled post_event_inventory to resume -- \
                     the tracked inventory is still the pre-event holding",
                    progress.event_id, progress.blocked_at, because,
                ),
            )),
            force_exit: false,
            suppress_history: true,
            suppress_exits: true,
        }
    }

    /// Whether `progress` was written for `event`: by fingerprint when the
    /// record carries one, by id for records that predate it.
    /// Public form of `progress_matches` for the admin commands: does this
    /// declaration describe the window this progress record belongs to?
    /// (bot-strategy#977's `reconcile-position` has to read the pending
    /// resume's declared holdings.)
    pub fn progress_names_event(
        progress: &ArcusSpotCorporateActionProgress,
        event: &ArcusSpotCorporateActionEvent,
    ) -> bool {
        Self::progress_matches(progress, event)
    }

    fn progress_matches(
        progress: &ArcusSpotCorporateActionProgress,
        event: &ArcusSpotCorporateActionEvent,
    ) -> bool {
        if progress.fingerprint.is_empty() {
            progress.event_id.eq_ignore_ascii_case(&event.event_id)
        } else {
            progress.fingerprint == event.fingerprint()
        }
    }

    /// Handled by label *or* by identity. A completed entry an operator
    /// renames keeps its fingerprint, so it stays handled; without this a
    /// past window whose `entry_block_at` is long gone would become active
    /// again on the next tick, clear the live signal history and re-apply
    /// its `post_event_inventory` over every trade made since.
    /// True once this window's settlement cutoff is reached: exits planned
    /// from here could settle after the venue changes units, so the planning
    /// gate stops producing them at the same instant
    /// `validate_plan_consistent_with_state` starts refusing them. Without
    /// that agreement `live-tick` writes a pending plan its own dispatch
    /// rejects, and `ReplaySimulation` -- which never runs the live-only
    /// validator -- records a fill production would not send (Codex P1,
    /// pairtrade#309).
    fn corporate_action_settlement_cutoff_reached(
        &self,
        event: &ArcusSpotCorporateActionEvent,
        at: DateTime<Utc>,
    ) -> bool {
        at + Duration::seconds(self.config.corporate_action_settlement_margin_secs)
            >= event.effective_at
    }

    /// A declared window whose `effective_at` is inside the settlement
    /// margin (or already past it, with the units not yet observed as
    /// stale). Reused-id declarations count too: they never become active,
    /// but their cutoff is as real as any other.
    fn corporate_action_within_settlement_margin(
        &self,
        at: DateTime<Utc>,
    ) -> Option<&ArcusSpotCorporateActionEvent> {
        let margin = Duration::seconds(self.config.corporate_action_settlement_margin_secs);
        self.config
            .corporate_actions
            .iter()
            .filter(|event| !matches!(self.handled_record_for(event), Some(HandledMatch::Same)))
            .find(|event| at + margin >= event.effective_at)
    }

    /// Every reason the planning gate would refuse to open a rotation right
    /// now, for the dispatch validator to refuse the same plan: a declared
    /// window that has opened, a persisted progress record whose declaration
    /// was removed or replaced (the planner keeps it and fails closed, so the
    /// dispatch of a still-fresh signed entry must not be the way around it),
    /// and a reused handled id (Codex P1, pairtrade#309).
    fn corporate_action_blocks_entries(&self, at: DateTime<Utc>) -> Option<String> {
        // A window that opens within the settlement margin blocks entries
        // already: submission is not execution, and between this guard and
        // `submit_signed_quote_once` the ledger's dispatch marker still has
        // to be persisted. Rather than re-check after each intervening step
        // -- the venue round trip is outside this process and no re-check
        // reaches it -- entries stop the same margin early that exits do
        // (Codex P1, pairtrade#309).
        let margin = Duration::seconds(self.config.corporate_action_settlement_margin_secs);
        if let Some(event) = self.active_corporate_action(at + margin) {
            return Some(format!(
                "inside corporate action {}'s window (entries blocked from {})",
                event.event_id, event.entry_block_at
            ));
        }
        if let Some(progress) = self.state.corporate_action.as_ref() {
            return Some(format!(
                "corporate action {} opened at {} and is unresolved",
                progress.event_id, progress.blocked_at
            ));
        }
        self.config
            .corporate_actions
            .iter()
            .find(|event| self.handled_record_for(event) == Some(HandledMatch::ReusedId))
            .map(|event| {
                format!(
                    "corporate action {} reuses the id of an already handled event",
                    event.event_id
                )
            })
    }

    /// Anything with a handled record is excluded from becoming active --
    /// including a reused id, which is refused by the gate's overlay rather
    /// than allowed to open a window under an old name.
    fn corporate_action_is_handled(&self, event: &ArcusSpotCorporateActionEvent) -> bool {
        self.handled_record_for(event).is_some()
    }

    /// How `event` relates to the handled record, if at all. The id and the
    /// fingerprint are recorded side by side, so a handled id whose recorded
    /// fingerprint differs from this event's is a *reused label* -- a new,
    /// distinct action declared under an old name -- and must not be read as
    /// "already handled", or its window is skipped entirely (Codex P1,
    /// pairtrade#309). Records that predate fingerprints match by id alone.
    fn handled_record_for(&self, event: &ArcusSpotCorporateActionEvent) -> Option<HandledMatch> {
        handled_corporate_action_record(&self.state, event)
    }

    /// The window this tick is inside, whichever way it got there: a
    /// declared unhandled event, or a *refused* declaration reusing a
    /// handled id -- which `active_corporate_action` deliberately excludes,
    /// so anything asking "is there a window here" has to resolve it too
    /// (Codex P1, pairtrade#309).
    fn corporate_action_window_in_force(
        &self,
        evaluation_time: DateTime<Utc>,
    ) -> Option<ArcusSpotCorporateActionEvent> {
        if let Some(event) = self.active_corporate_action(evaluation_time) {
            return Some(event.clone());
        }
        self.config
            .corporate_actions
            .iter()
            .find(|event| {
                self.handled_record_for(event) == Some(HandledMatch::ReusedId)
                    && evaluation_time >= event.entry_block_at
                    && self
                        .state
                        .corporate_action
                        .as_ref()
                        .is_none_or(|progress| Self::progress_matches(progress, event))
            })
            .cloned()
    }

    /// Whether an affected symbol has already been repointed away from the
    /// identity pinned when the window opened. The mark this tick would
    /// engage a halt on mixes the old contract's quantity with the new
    /// contract's price, so it is not evidence of anything.
    fn corporate_action_identity_drifted(
        &self,
        evaluation_time: DateTime<Utc>,
        price: &PriceContext,
    ) -> bool {
        let Some(event) = self.corporate_action_window_in_force(evaluation_time) else {
            return false;
        };
        if self
            .corporate_action_identity_drift(&event, price)
            .is_some()
        {
            return true;
        }
        if self.state.corporate_action.is_some() {
            // A pin that was never taken is not a passed comparison: the
            // marks are unverifiable, so they may not engage a halt either
            // (Codex P1, pairtrade#309).
            return self.corporate_action_unpinned_symbol(&event).is_some();
        }
        // The first tick inside a window has no progress record yet -- the
        // gate writes it after the risk marks. The pin it is about to take
        // is only meaningful if the last observation predates the window;
        // otherwise the cached identity may already be the replacement and
        // comparing it with itself proves nothing.
        let pre_event_observed = self
            .state
            .last_token_identity_at
            .is_some_and(|observed_at| observed_at < event.entry_block_at);
        if !pre_event_observed {
            return true;
        }
        event.symbols.iter().any(|symbol| {
            let (last, token) = if symbol.eq_ignore_ascii_case(&self.config.pair.sell_symbol) {
                (self.state.last_token_a_identity.as_ref(), &price.token_a)
            } else {
                (self.state.last_token_b_identity.as_ref(), &price.token_b)
            };
            last.is_none_or(|last| !last.matches(token))
        })
    }

    /// An affected symbol whose pre-event identity was never observed, if
    /// any: `corporate_action_identity_drift` skips those, so on its own it
    /// reports "no drift" for a comparison it could not make.
    fn corporate_action_unpinned_symbol(
        &self,
        event: &ArcusSpotCorporateActionEvent,
    ) -> Option<String> {
        let progress = self.state.corporate_action.as_ref()?;
        event
            .symbols
            .iter()
            .find(|symbol| {
                let pinned = if symbol.eq_ignore_ascii_case(&self.config.pair.sell_symbol) {
                    progress.pre_event_token_a.as_ref()
                } else {
                    progress.pre_event_token_b.as_ref()
                };
                pinned.is_none()
            })
            .cloned()
    }

    /// Compares each affected symbol's contract and decimals against what
    /// they were before the window opened. A symbol change, merger or
    /// redemption that re-points the ticker at a different contract is
    /// exactly what this catches, and it is not something a runtime may
    /// decide to trade through.
    fn corporate_action_identity_drift(
        &self,
        event: &ArcusSpotCorporateActionEvent,
        price: &PriceContext,
    ) -> Option<ArcusSpotHold> {
        let progress = self.state.corporate_action.as_ref()?;
        for symbol in &event.symbols {
            let (pinned, token) = if symbol.eq_ignore_ascii_case(&self.config.pair.sell_symbol) {
                (progress.pre_event_token_a.as_ref(), &price.token_a)
            } else {
                (progress.pre_event_token_b.as_ref(), &price.token_b)
            };
            // No pinned identity means the runtime never saw the pre-event
            // side of this window (it was down, or the window was declared
            // after the fact). There is nothing to compare against, and
            // inventing a comparison would be worse than admitting it: the
            // operator's reconciled post_event_inventory is what carries
            // the resume in that case.
            let Some(pinned) = pinned else { continue };
            if !pinned.matches(token) {
                return Some(ArcusSpotHold::new(
                    ArcusSpotHoldCode::CorporateActionUnresolved,
                    format!(
                        "corporate action {}: {symbol} resolved to {} ({} decimals) before the \
                         window and {} ({} decimals) after it; the configured pair no longer \
                         names the same instrument",
                        event.event_id,
                        pinned.address,
                        pinned.decimals,
                        token.address,
                        token.decimals,
                    ),
                ));
            }
        }
        None
    }
}

/// How a declaration relates to the handled record. The one implementation
/// of the rule: the checkpoint store applies it too, so "already handled"
/// means the same thing at load time as it does inside a tick (Codex P1,
/// pairtrade#309).
/// Resolves legacy handled records once, while the observation watermark
/// still means something: a checkpoint written before fingerprints existed
/// holds ids alone, and the declaration bearing an id can be confirmed as
/// the handled event only if its window had completed by the last
/// observation. Anything still unresolved after this stays refused, so a
/// later action wearing an old id can never inherit "handled" from it --
/// the flaw in judging that by the evaluation clock, which flips back to
/// "handled" the moment the clock passes the new action's resume time
/// (Codex P1 x2, pairtrade#309).
/// The instant past which the venue re-denominates this window's units.
///
/// The stamped `progress.effective_at` is the durable half -- it survives
/// the declaration being deleted -- but it is a copy of the cutoff as it
/// stood when the window opened, and an amendment may since have moved the
/// cutoff *earlier* (a widening the checkpoint's removal scan accepts). The
/// venue changes the units when the calendar says so, not when the copy
/// says, so the answer is the earliest instant any surviving declaration of
/// this window names. Every reader of "is this mark pre- or post-event?"
/// goes through here, so the runtime and the continuity verifier cannot
/// disagree about a window that has been amended (Codex P1, pairtrade#309).
pub fn corporate_action_effective_cutoff(
    progress: &ArcusSpotCorporateActionProgress,
    config: &ArcusSpotRuntimeConfig,
) -> Option<DateTime<Utc>> {
    let declared = config
        .corporate_actions
        .iter()
        .filter(|event| {
            (!progress.fingerprint.is_empty() && progress.fingerprint == event.fingerprint())
                || event.event_id.eq_ignore_ascii_case(&progress.event_id)
        })
        .map(|event| event.effective_at)
        .min();
    [progress.effective_at, declared]
        .into_iter()
        .flatten()
        .min()
}

pub(crate) fn backfill_handled_corporate_action_fingerprints(
    state: &mut ArcusSpotRuntimeState,
    config: &ArcusSpotRuntimeConfig,
) {
    if state.handled_corporate_actions_resolved {
        return;
    }
    resolve_handled_corporate_action_fingerprints(state, config, state.last_observation_at);
    state.handled_corporate_actions_resolved = true;
}

/// The resolution itself, with the watermark supplied. Continuity
/// verification runs it on both sides with the *baseline's* watermark, so
/// the two are judged with the same information.
pub fn resolve_handled_corporate_action_fingerprints(
    state: &mut ArcusSpotRuntimeState,
    config: &ArcusSpotRuntimeConfig,
    watermark: Option<DateTime<Utc>>,
) {
    let ids = state.handled_corporate_action_ids.clone();
    state
        .handled_corporate_action_fingerprints
        .resize(ids.len(), String::new());
    for (index, id) in ids.iter().enumerate() {
        if !state.handled_corporate_action_fingerprints[index].is_empty() {
            continue;
        }
        if let Some(event) = config.corporate_actions.iter().find(|event| {
            event.event_id.eq_ignore_ascii_case(id)
                && watermark.is_some_and(|observed_at| event.resume_not_before <= observed_at)
        }) {
            state.handled_corporate_action_fingerprints[index] = event.fingerprint();
        }
    }
}

pub(crate) fn handled_corporate_action_record(
    state: &ArcusSpotRuntimeState,
    event: &ArcusSpotCorporateActionEvent,
) -> Option<HandledMatch> {
    let fingerprint = event.fingerprint();
    if state
        .handled_corporate_action_fingerprints
        .iter()
        .any(|handled| *handled == fingerprint)
    {
        return Some(HandledMatch::Same);
    }
    let index = state
        .handled_corporate_action_ids
        .iter()
        .position(|handled| handled.eq_ignore_ascii_case(&event.event_id))?;
    match state.handled_corporate_action_fingerprints.get(index) {
        Some(recorded) if *recorded == fingerprint => Some(HandledMatch::Same),
        // A different event's fingerprint, or a legacy record that
        // `backfill_handled_corporate_action_fingerprints` could not confirm
        // as this declaration: either way this is a reused label, refused
        // rather than treated as handled.
        _ => Some(HandledMatch::ReusedId),
    }
}

/// See `handled_corporate_action_record`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandledMatch {
    /// This exact declaration (by fingerprint) was handled.
    Same,
    /// A handled id whose recorded fingerprint is not this declaration's --
    /// a different event wearing the label, or a legacy record that could
    /// not be confirmed as this one.
    ReusedId,
}

/// The rotation signal after the corporate-action calendar has had its say.
#[derive(Debug, Clone)]
enum GatedRotationSignal {
    /// The window (if any) had nothing to say about this tick.
    Unchanged(Option<(ArcusSpotDirection, ArcusSpotRotationTrigger)>),
    /// An open rotation must be unwound now.
    ForcedExit(ArcusSpotDirection, ArcusSpotRotationTrigger),
    /// No new rotation may be opened, and there is no exit to run instead.
    Blocked(ArcusSpotHold),
}

impl GatedRotationSignal {
    fn signal(&self) -> Option<(ArcusSpotDirection, ArcusSpotRotationTrigger)> {
        match self {
            Self::Unchanged(signal) => *signal,
            Self::ForcedExit(direction, trigger) => Some((*direction, *trigger)),
            Self::Blocked(_) => None,
        }
    }

    fn blocked_by(&self) -> Option<&ArcusSpotHold> {
        match self {
            Self::Blocked(hold) => Some(hold),
            _ => None,
        }
    }
}

impl CorporateActionGate {
    /// Before `effective_at` exits are never suppressed: a window exists to
    /// get the bot *out* before an event, so one that could trap it in is
    /// self-defeating. From `effective_at` the opposite holds (see
    /// `suppress_exits`). Everything else -- an entry, or simply having
    /// nothing to do -- is reported as blocked while the window stands.
    fn apply(
        &self,
        signal: Option<(ArcusSpotDirection, ArcusSpotRotationTrigger)>,
        regime: ArcusSpotRegime,
    ) -> GatedRotationSignal {
        // Stale units: every signal, exits included, is replaced by the
        // gate's own hold. `force_exit` is never set alongside this.
        if self.suppress_exits {
            if let Some(hold) = self.block_entry.as_ref() {
                return GatedRotationSignal::Blocked(hold.clone());
            }
        }
        if self.force_exit {
            if let Some(direction) = unwind_direction(regime) {
                return GatedRotationSignal::ForcedExit(
                    direction,
                    ArcusSpotRotationTrigger::CorporateActionExit,
                );
            }
        }
        let Some(hold) = self.block_entry.as_ref() else {
            return GatedRotationSignal::Unchanged(signal);
        };
        match signal {
            Some((direction, trigger)) if trigger.is_exit() => {
                GatedRotationSignal::Unchanged(Some((direction, trigger)))
            }
            _ => GatedRotationSignal::Blocked(hold.clone()),
        }
    }
}

/// The direction that closes `regime`, or `None` when there is nothing open.
fn unwind_direction(regime: ArcusSpotRegime) -> Option<ArcusSpotDirection> {
    match regime {
        ArcusSpotRegime::Neutral => None,
        ArcusSpotRegime::RotatedAToB => Some(ArcusSpotDirection::TokenBToTokenA),
        ArcusSpotRegime::RotatedBToA => Some(ArcusSpotDirection::TokenAToTokenB),
    }
}

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
            ArcusSpotRotationTrigger::MeanReversionExit
            | ArcusSpotRotationTrigger::MaxHoldExit
            | ArcusSpotRotationTrigger::CorporateActionExit,
            ArcusSpotDirection::TokenBToTokenA,
        )
        | (
            ArcusSpotRegime::RotatedBToA,
            ArcusSpotRotationTrigger::MeanReversionExit
            | ArcusSpotRotationTrigger::MaxHoldExit
            | ArcusSpotRotationTrigger::CorporateActionExit,
            ArcusSpotDirection::TokenAToTokenB,
        ) => Ok(()),
        other => Err(format!(
            "fill is inconsistent with runtime state: {other:?}"
        )),
    }
}

/// A recorder row's `optimistic_round_trip_loss_bps` as recorded, sign
/// included. The recorder reports exactly what its two legs imply, and a
/// round trip can come out slightly favourable when the reference the
/// router prices against lags a moving market by a tick.
fn parse_signed_round_trip_loss_bps(
    field: &str,
    value: Option<&str>,
) -> Result<Decimal, ArcusSpotHold> {
    let value = value.ok_or_else(|| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{field} is absent"),
        )
    })?;
    Decimal::from_str(value).map_err(|error| {
        ArcusSpotHold::new(
            ArcusSpotHoldCode::InvalidSnapshot,
            format!("{field} is invalid: {error}"),
        )
    })
}

/// The same value as the cost gate consumes it: a favourable round trip is
/// costed at zero, never credited and never rejected.
///
/// Before bot-strategy#1001 a negative value here rejected the whole row as
/// an invalid snapshot. The sign was standing in for "one leg's quote is
/// not believable", and one venue's +212 bps quote made it discard 62% of
/// a day's ticks -- including the honest quote in the same response. That
/// judgement now happens per venue, against the router's own reference,
/// in `select_route_quote`, and the linkage check proves the legs chain; a
/// residual negative is the bounded lag artefact described above.
fn parse_round_trip_loss_bps(field: &str, value: Option<&str>) -> Result<Decimal, ArcusSpotHold> {
    Ok(parse_signed_round_trip_loss_bps(field, value)?.max(Decimal::ZERO))
}

pub fn raw_amount_to_quantity(raw: &str, decimals: u32) -> Result<Decimal, String> {
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
            max_favourable_quote_deviation_bps: Decimal::from(25),
            max_reference_price_age_secs: 120,
            corporate_actions: Vec::new(),
            // The corporate-action fixtures use second-scale windows, so
            // the 300s production default would leave no dispatchable reduce
            // phase. Tests that exercise the margin set it explicitly.
            corporate_action_settlement_margin_secs: 0,
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
                        "referencePrice": reference_price("49000000000000000", "25000000000000000"),
                        "referencePriceTimestamp": received_at.timestamp_millis(),
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
                        "referencePrice": reciprocal_reference("49000000000000000", "25000000000000000"),
                        "referencePriceTimestamp": reverse_received_at.timestamp_millis(),
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
            exit_leg_sizing: ExitLegSizing::EntryCycleReverseLeg,
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
                        "referencePrice": reference_price("24500000000000000", "50000000000000000"),
                        "referencePriceTimestamp": received_at.timestamp_millis(),
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
                        "referencePrice": reciprocal_reference("24500000000000000", "50000000000000000"),
                        "referencePriceTimestamp": reverse_received_at.timestamp_millis(),
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
    fn the_all_in_cost_gate_is_inclusive_at_the_cap() {
        // `hash-config` prints the residual budget an operator sizes
        // against, and the wording has to match the comparison: build_plan
        // rejects on `all_in_cost > cap`, so a quote landing exactly on the
        // cap clears. This matters most when the buffers equal the cap and
        // the residual is 0 bps (Codex, bot-strategy#903).
        let plan_at = |cap: Decimal| {
            let mut cfg = config();
            cfg.max_all_in_round_trip_cost_bps = cap;
            let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
            let current = (200.0_f64 / 100.0_f64).ln();
            runtime.state.relative_log_price_history = vec![current + 0.01, current + 0.02];
            runtime
                .step_at(
                    &snapshot_with_bidirectional_rows(event_time()),
                    event_time(),
                )
                .decision
        };

        // Read the quote's own all-in cost off a plan built under a cap
        // that cannot bind, so the boundary below is the real one.
        let cost = match plan_at(Decimal::from(10_000)) {
            ArcusSpotDecision::SimulatedFill { plan } => plan.all_in_round_trip_cost_bps,
            other => panic!("expected a plan under an unbinding cap, got {other:?}"),
        };

        // Exactly at the cap: accepted.
        assert!(
            matches!(plan_at(cost), ArcusSpotDecision::SimulatedFill { .. }),
            "a quote at exactly {cost} bps must clear a cap of {cost} bps",
        );

        // One tick under it: held, so the boundary is where it is claimed.
        let just_under = cost.checked_sub(Decimal::new(1, cost.scale())).unwrap();
        assert!(
            matches!(
                plan_at(just_under),
                ArcusSpotDecision::Observe { hold } if hold.code == ArcusSpotHoldCode::CostLimit
            ),
            "a cap of {just_under} bps must hold a {cost} bps quote",
        );
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

    /// A referencePrice 5 bps *above* the quote's implied price, i.e. the
    /// quote sits an honest 5 bps below the reference (both tokens are
    /// 18-decimal in these fixtures, so raw ratio == human ratio).
    fn reference_price(buy_amount: &str, sell_amount: &str) -> String {
        let implied =
            Decimal::from_str(buy_amount).unwrap() / Decimal::from_str(sell_amount).unwrap();
        (implied / Decimal::new(9995, 4)).normalize().to_string()
    }

    /// The reverse leg's reference for the same market: the exact
    /// reciprocal of the forward leg's (`reference_price(forward_buy,
    /// forward_sell)`), so the two legs describe one price.
    fn reciprocal_reference(forward_buy_amount: &str, forward_sell_amount: &str) -> String {
        let forward =
            Decimal::from_str(&reference_price(forward_buy_amount, forward_sell_amount)).unwrap();
        (Decimal::ONE / forward).normalize().to_string()
    }

    fn with_implausible_rialto_recommended(route: &mut ArcusSpotRouteObservation) {
        // The live 2026-09-10 shape: rialto's buyAmount is ~2.1% larger
        // than arcus's, the router recommends it on that alone, and it
        // sits ~+206 bps over the router's own referencePrice.
        let arcus = route.response.payload.quotes[0].clone();
        let inflated = (Decimal::from_str(&arcus.buy_amount).unwrap() * Decimal::new(10212, 4))
            .round()
            .to_string();
        let mut rialto = arcus.clone();
        rialto.venue = "rialto".to_string();
        rialto.buy_amount = inflated;
        route.response.payload.quotes.push(rialto);
        route.response.payload.recommended = "rialto".to_string();
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
                        "referencePrice": reference_price(forward_buy_amount, "25000000000000000"),
                        "referencePriceTimestamp": event_time().timestamp_millis(),
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
                        "referencePrice": reciprocal_reference(reverse_sell_amount, "25000000000000000"),
                        "referencePriceTimestamp": event_time().timestamp_millis(),
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
            verify_round_trip_linkage_and_loss(
                &row,
                &nvda_token(),
                &amd_token(),
                Decimal::from(25),
                120,
            )
            .unwrap(),
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
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
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
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
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
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("does not match"));
    }

    #[test]
    fn round_trip_linkage_selects_the_plausible_venue_over_the_recommended_one() {
        // bot-strategy#1001: the forward payload recommends an implausibly
        // rich rialto quote; the reverse leg and the recorded loss were
        // built off arcus. The row must cost as the arcus round trip (80
        // bps), not be rejected for a negative one.
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        with_implausible_rialto_recommended(row.forward.as_mut().unwrap());
        row.forward_venue = Some("arcus".to_string());
        assert_eq!(
            verify_round_trip_linkage_and_loss(
                &row,
                &nvda_token(),
                &amd_token(),
                Decimal::from(25),
                120,
            )
            .unwrap(),
            Decimal::from(80)
        );
        // A band wide enough to admit rialto selects it, and then the
        // reverse leg (sized off arcus) no longer chains: the row is
        // refused as inconsistent rather than costed on the wrong venue.
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(300),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(
            error.detail.contains("recorder selected arcus"),
            "{error:?}"
        );
    }

    #[test]
    fn round_trip_linkage_rejects_a_row_whose_recorded_venue_disagrees_with_the_selection() {
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        row.reverse_venue = Some("rialto".to_string());
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(error.detail.contains("reverse leg"), "{error:?}");
    }

    #[test]
    fn a_slightly_favourable_round_trip_within_the_band_is_costed_at_zero() {
        // Both legs sit honestly below their references (the fixture
        // helper places every quote 5 bps under), yet the chained amounts
        // return 4 bps more than they started with -- the reference-lag
        // case. Signed agreement with the recorder still holds, and the
        // cost gate sees zero, not a rejection and not a credit.
        let row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "25010000000000000",
            "25010000000000000",
            "-4",
        );
        assert_eq!(
            verify_round_trip_linkage_and_loss(
                &row,
                &nvda_token(),
                &amd_token(),
                Decimal::from(25),
                120,
            )
            .unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            parse_round_trip_loss_bps("x", Some("-4")).unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            parse_round_trip_loss_bps("x", Some("4")).unwrap(),
            Decimal::from(4)
        );
        assert!(parse_round_trip_loss_bps("x", None).is_err());
    }

    #[test]
    fn round_trip_linkage_rejects_references_that_are_not_reciprocal() {
        // Both legs sit 5 bps under their own reference, but the reverse
        // reference has drifted 1% from the forward one's reciprocal: the
        // two legs were not judged against one market.
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        let reverse = row.reverse.as_mut().unwrap();
        let drifted = reverse.response.payload.reference_price().unwrap() * Decimal::new(101, 2);
        // Keep the reverse quote in band against its drifted reference.
        reverse.response.payload.quotes[0].buy_amount =
            (Decimal::from_str("24800000000000000").unwrap() * Decimal::new(101, 2))
                .round()
                .to_string();
        reverse.response.payload.extra.insert(
            "referencePrice".to_string(),
            json!(drifted.normalize().to_string()),
        );
        row.optimistic_return_amount = Some(reverse.response.payload.quotes[0].buy_amount.clone());
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(
            error.detail.contains("referencePrices disagree"),
            "{error:?}"
        );
    }

    /// A row whose legs sit `forward_bps` / `reverse_bps` above exactly
    /// reciprocal references, so only the round-trip bound decides.
    fn row_with_in_band_legs(forward_bps: i64, reverse_bps: i64) -> ArcusSpotRoundTripRecord {
        let forward_sell = Decimal::from_str("25000000000000000").unwrap();
        let forward_reference = Decimal::new(2, 0);
        let reverse_reference = Decimal::ONE / forward_reference;
        let up = |bps: i64| Decimal::ONE + Decimal::new(bps, 4);
        let forward_buy = (forward_sell * forward_reference * up(forward_bps)).round();
        let reverse_buy = (forward_buy * reverse_reference * up(reverse_bps)).round();
        let loss = ((forward_sell - reverse_buy) / forward_sell * Decimal::from(10_000))
            .normalize()
            .to_string();
        let mut row = round_trip_row(
            &forward_buy.to_string(),
            &forward_buy.to_string(),
            &reverse_buy.to_string(),
            &reverse_buy.to_string(),
            &loss,
        );
        row.forward.as_mut().unwrap().response.payload.extra.insert(
            "referencePrice".to_string(),
            json!(forward_reference.normalize().to_string()),
        );
        row.reverse.as_mut().unwrap().response.payload.extra.insert(
            "referencePrice".to_string(),
            json!(reverse_reference.normalize().to_string()),
        );
        row
    }

    #[test]
    fn a_favourable_round_trip_is_costed_at_zero_up_to_one_band_and_refused_beyond() {
        let band = Decimal::from(25);
        // Forward exactly one band over its reference, reverse on its
        // reference: the chained amounts return 25 bps more than they
        // started with -- the most a lagging reference can explain -- and
        // the cost gate sees zero.
        let row = row_with_in_band_legs(25, 0);
        assert_eq!(
            Decimal::from_str(row.optimistic_round_trip_loss_bps.as_deref().unwrap()).unwrap(),
            Decimal::from(-25)
        );
        assert_eq!(
            verify_round_trip_linkage_and_loss(&row, &nvda_token(), &amd_token(), band, 120)
                .unwrap(),
            Decimal::ZERO
        );
        // Both legs still inside their bands and the references still
        // exactly reciprocal, yet together 26 bps favourable: nothing the
        // per-leg checks can see, so the bound must be what refuses it.
        let row = row_with_in_band_legs(25, 1);
        let error =
            verify_round_trip_linkage_and_loss(&row, &nvda_token(), &amd_token(), band, 120)
                .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::InvalidSnapshot);
        assert!(
            error.detail.contains("more than it started with"),
            "{error:?}"
        );
    }

    #[test]
    fn a_leg_with_a_stale_reference_holds_as_route_unavailable() {
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        let received_at = row.forward.as_ref().unwrap().response.received_at;
        row.forward.as_mut().unwrap().response.payload.extra.insert(
            "referencePriceTimestamp".to_string(),
            json!((received_at - Duration::seconds(121)).timestamp_millis()),
        );
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::RouteUnavailable);
        assert!(error.detail.contains("121s old"), "{error:?}");
    }

    #[test]
    fn a_leg_with_no_plausible_venue_holds_as_route_unavailable() {
        let mut row = round_trip_row(
            "49000000000000000",
            "49000000000000000",
            "24800000000000000",
            "24800000000000000",
            "80",
        );
        // A reference far below every quote makes all of them implausible.
        row.forward
            .as_mut()
            .unwrap()
            .response
            .payload
            .extra
            .insert("referencePrice".to_string(), json!("0.5"));
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::RouteUnavailable);
        assert!(
            error
                .detail
                .contains("NVDA->AMD leg has no plausible venue quote"),
            "{error:?}"
        );
        // And without any reference the leg fails closed the same way.
        row.forward
            .as_mut()
            .unwrap()
            .response
            .payload
            .extra
            .remove("referencePrice");
        let error = verify_round_trip_linkage_and_loss(
            &row,
            &nvda_token(),
            &amd_token(),
            Decimal::from(25),
            120,
        )
        .unwrap_err();
        assert_eq!(error.code, ArcusSpotHoldCode::RouteUnavailable);
        assert!(error.detail.contains("referencePrice"), "{error:?}");
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
                        "referencePrice": reference_price("49000000000000000", "25000000000000000"),
                        "referencePriceTimestamp": stale_forward_received_at.timestamp_millis(),
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
                        "referencePrice": reciprocal_reference("49000000000000000", "25000000000000000"),
                        "referencePriceTimestamp": event_time().timestamp_millis(),
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

    /// A recorder row requested at an exact raw sell amount (see
    /// `ArcusSpotRecorderConfig::fixed_sell_amount_rows`): `sell_symbol`
    /// -> `buy_symbol` at `sell_amount` raw, reverse leg chained off the
    /// forward output as the recorder does.
    fn fixed_exit_row(
        sell_symbol: &str,
        buy_symbol: &str,
        sell_amount: &str,
        forward_buy_amount: &str,
        reverse_buy_amount: &str,
        received_at: DateTime<Utc>,
    ) -> ArcusSpotRoundTripRecord {
        let address = |symbol: &str| match symbol {
            "NVDA" => "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
            "AMD" => "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
            other => panic!("unknown fixture symbol {other}"),
        };
        let price = |symbol: &str| match symbol {
            "NVDA" => "200",
            "AMD" => "100",
            other => panic!("unknown fixture symbol {other}"),
        };
        serde_json::from_value(json!({
            "pair": {"sell_symbol": sell_symbol, "buy_symbol": buy_symbol},
            "notional_usd": "4.0",
            "fixed_sell_amount": sell_amount,
            "sell_reference_price_usd": price(sell_symbol),
            "buy_reference_price_usd": price(buy_symbol),
            "requested_sell_amount": sell_amount,
            "forward": {
                "chain_id": 4663,
                "sell_symbol": sell_symbol,
                "buy_symbol": buy_symbol,
                "sell_token": address(sell_symbol),
                "buy_token": address(buy_symbol),
                "sell_amount": sell_amount,
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "referencePrice": reference_price(forward_buy_amount, sell_amount),
                        "referencePriceTimestamp": received_at.timestamp_millis(),
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": forward_buy_amount,
                            "sellAmount": sell_amount,
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": received_at,
                    "received_at": received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "reverse": {
                "chain_id": 4663,
                "sell_symbol": buy_symbol,
                "buy_symbol": sell_symbol,
                "sell_token": address(buy_symbol),
                "buy_token": address(sell_symbol),
                "sell_amount": forward_buy_amount,
                "response": {
                    "payload": {
                        "recommended": "arcus",
                        "referencePrice": reciprocal_reference(forward_buy_amount, sell_amount),
                        "referencePriceTimestamp": received_at.timestamp_millis(),
                        "all": [{
                            "venue": "arcus",
                            "buyAmount": reverse_buy_amount,
                            "sellAmount": forward_buy_amount,
                            "fees": []
                        }],
                        "errors": []
                    },
                    "requested_at": received_at,
                    "received_at": received_at,
                    "latency_ms": 1000,
                    "attempts": 1
                }
            },
            "optimistic_return_amount": reverse_buy_amount,
            "optimistic_round_trip_loss_bps": "50",
            "errors": []
        }))
        .unwrap()
    }

    /// The live scenario behind bot-strategy#906: 0.04 AMD is open, the
    /// notional cycle's reverse leg would sell 0.049 AMD (oversized, so the
    /// legacy path holds on `rotation_limit` forever), but the snapshot
    /// also carries a row quoted at exactly 0.04 AMD.
    fn rotated_runtime_with_open_amd(open_raw_units: i64) -> ArcusSpotRuntime {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(open_raw_units, 3));
        runtime.state.last_rotation_at =
            Some(event_time() - Duration::seconds(runtime.config.max_hold_secs));
        runtime
    }

    #[test]
    fn open_exit_leg_reports_the_exit_direction_and_open_quantity() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        assert_eq!(runtime.open_exit_leg(), None);
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(40, 3));
        assert_eq!(
            runtime.open_exit_leg(),
            Some((ArcusSpotDirection::TokenBToTokenA, Decimal::new(40, 3)))
        );
        assert_eq!(
            runtime.direction_symbols(ArcusSpotDirection::TokenBToTokenA),
            ("AMD", "NVDA")
        );
        runtime.state.regime = ArcusSpotRegime::RotatedBToA;
        assert_eq!(
            runtime.open_exit_leg(),
            Some((ArcusSpotDirection::TokenAToTokenB, Decimal::new(40, 3)))
        );
        assert_eq!(
            runtime.direction_symbols(ArcusSpotDirection::TokenAToTokenB),
            ("NVDA", "AMD")
        );
        runtime.state.rotated_quantity = None;
        assert_eq!(runtime.open_exit_leg(), None);
    }

    #[test]
    fn open_exit_fixed_sell_amount_row_requests_the_exact_open_quantity() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        assert_eq!(runtime.open_exit_fixed_sell_amount_row(18).unwrap(), None);
        runtime.state.regime = ArcusSpotRegime::RotatedBToA;
        runtime.state.rotated_quantity =
            Some(Decimal::from_str("0.323268605206725905000").unwrap());
        assert_eq!(
            runtime.open_exit_fixed_sell_amount_row(18).unwrap(),
            Some(ArcusSpotFixedSellAmountRow {
                pair: ArcusSpotPair {
                    sell_symbol: "NVDA".to_string(),
                    buy_symbol: "AMD".to_string(),
                },
                sell_amount_raw: "323268605206725905".to_string(),
            })
        );
        // Not representable at 6 decimals: refuse rather than round.
        let error = runtime.open_exit_fixed_sell_amount_row(6).unwrap_err();
        assert!(
            error.contains("not exactly representable"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn open_quantity_exit_row_sizes_the_exit_to_exactly_the_open_quantity() {
        let mut runtime = rotated_runtime_with_open_amd(40);
        let mut snapshot = snapshot_with_valid_row(event_time());
        snapshot.round_trips.push(fixed_exit_row(
            "AMD",
            "NVDA",
            "40000000000000000",
            "19900000000000000",
            "39800000000000000",
            event_time(),
        ));
        let inventory_before = runtime.state.inventory;
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::MaxHoldExit);
                assert_eq!(plan.direction, ArcusSpotDirection::TokenBToTokenA);
                assert_eq!(plan.sell_symbol, "AMD");
                assert_eq!(plan.buy_symbol, "NVDA");
                assert_eq!(plan.sell_quantity, Decimal::new(40, 3));
                assert_eq!(plan.sell_amount_raw, "40000000000000000");
                assert_eq!(plan.buy_amount_raw, "19900000000000000");
                assert_eq!(plan.buy_quantity, Decimal::new(199, 4));
                assert_eq!(plan.optimistic_round_trip_loss_bps, Decimal::from(50));
            }
            other => panic!("expected an exit sized to the open quantity, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::Neutral);
        assert_eq!(runtime.state().rotated_quantity, None);
        assert_eq!(
            runtime.state().inventory.token_b,
            inventory_before.token_b - Decimal::new(40, 3)
        );
        assert_eq!(
            runtime.state().inventory.token_a,
            inventory_before.token_a + Decimal::new(199, 4)
        );
    }

    #[test]
    fn open_quantity_exit_row_is_matched_by_exact_raw_amount_only() {
        // A fixed row at a *different* amount is not "close enough": a
        // collector that requested the wrong size (e.g. its decimals pin
        // disagrees with this snapshot's token metadata) must surface
        // distinctly, not blend into the legacy notional-cycle path as if
        // no exit-sized row had ever been requested at all (2026-09-04
        // review).
        let mut runtime = rotated_runtime_with_open_amd(40);
        let mut snapshot = snapshot_with_valid_row(event_time());
        snapshot.round_trips.push(fixed_exit_row(
            "AMD",
            "NVDA",
            "41000000000000000",
            "20400000000000000",
            "40800000000000000",
            event_time(),
        ));
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::InvalidSnapshot);
                assert!(
                    hold.detail.contains("not the open rotation quantity"),
                    "unexpected hold detail: {}",
                    hold.detail
                );
            }
            other => panic!("expected a mismatch hold, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn open_quantity_exit_row_with_errors_holds_instead_of_falling_back() {
        let mut runtime = rotated_runtime_with_open_amd(40);
        let mut snapshot = snapshot_with_valid_row(event_time());
        let mut row = fixed_exit_row(
            "AMD",
            "NVDA",
            "40000000000000000",
            "19900000000000000",
            "39800000000000000",
            event_time(),
        );
        row.forward = None;
        row.reverse = None;
        row.optimistic_return_amount = None;
        row.optimistic_round_trip_loss_bps = None;
        row.errors = serde_json::from_value(json!([{
            "stage": "forward_price",
            "classification": "http",
            "retryable": true,
            "message": "router timeout"
        }]))
        .unwrap();
        snapshot.round_trips.push(row);
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::RouteUnavailable);
                assert!(
                    hold.detail.contains("router timeout"),
                    "unexpected hold detail: {}",
                    hold.detail
                );
            }
            other => panic!("expected a route-unavailable hold, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
        assert_eq!(runtime.state().rotated_quantity, Some(Decimal::new(40, 3)));
    }

    #[test]
    fn open_quantity_exit_row_must_be_quoted_at_the_requested_amount() {
        // fixed_sell_amount says 0.04 AMD but the route was actually
        // requested at 0.039: the row is internally inconsistent.
        let mut runtime = rotated_runtime_with_open_amd(40);
        let mut snapshot = snapshot_with_valid_row(event_time());
        let mut row = fixed_exit_row(
            "AMD",
            "NVDA",
            "39000000000000000",
            "19400000000000000",
            "38800000000000000",
            event_time(),
        );
        row.fixed_sell_amount = Some("40000000000000000".to_string());
        snapshot.round_trips.push(row);
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::InvalidSnapshot);
            }
            other => panic!("expected an invalid-snapshot hold, got {other:?}"),
        }
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn stale_open_quantity_exit_row_holds_on_quote_age() {
        let mut runtime = rotated_runtime_with_open_amd(40);
        let mut snapshot = snapshot_with_valid_row(event_time());
        snapshot.round_trips.push(fixed_exit_row(
            "AMD",
            "NVDA",
            "40000000000000000",
            "19900000000000000",
            "39800000000000000",
            event_time() - Duration::seconds(runtime.config.max_quote_age_secs + 1),
        ));
        let event = runtime.step_at(&snapshot, event_time());
        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::StaleQuote);
            }
            other => panic!("expected a stale-quote hold, got {other:?}"),
        }
    }

    #[test]
    fn open_quantity_exit_row_is_ignored_while_neutral() {
        // A leftover fixed row (e.g. collected for a position that has
        // since closed) must not influence an entry decision: a selector
        // that matched by pair alone, ignoring fixed_sell_amount, would let
        // this row's amount leak into the entry plan instead of the
        // notional cycle's own row.
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        // Warm the signal window with a tight cluster so this snapshot's
        // relative_log_price (ln(200/100) ~= 0.693, from
        // snapshot_with_valid_row's 200/100 prices) is many standard
        // deviations away and reliably crosses entry_z_score (1.0),
        // driving a real EntrySignal rather than a Warmup hold.
        runtime.state.relative_log_price_history = vec![0.0, 0.1];
        let mut snapshot = snapshot_with_valid_row(event_time());
        snapshot.round_trips.push(fixed_exit_row(
            "AMD",
            "NVDA",
            "40000000000000000",
            "19900000000000000",
            "39800000000000000",
            event_time(),
        ));
        let event = runtime.step_at(&snapshot, event_time());
        let plan = match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => plan,
            other => panic!("expected the entry to fire, got {other:?}"),
        };
        assert_eq!(plan.trigger, ArcusSpotRotationTrigger::EntrySignal);
        assert_eq!(plan.direction, ArcusSpotDirection::TokenAToTokenB);
        assert_eq!(plan.sell_symbol, "NVDA");
        assert_eq!(plan.buy_symbol, "AMD");
        // The notional row's own requested amount ($5 at 200 -> 18
        // decimals), not the leftover fixed row's 40000000000000000: proof
        // the entry was selected by (pair, notional_usd), not by pair
        // alone.
        assert_eq!(plan.sell_amount_raw, "25000000000000000");
        assert_eq!(runtime.state().regime, ArcusSpotRegime::RotatedAToB);
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
        runtime
            .validate_plan_consistent_with_state(&plan, event_time())
            .unwrap();
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn an_exit_plan_is_refused_at_dispatch_once_the_units_are_stale() {
        // Planned in the reduce phase, dispatched after effective_at: the
        // regime and the tracked quantity are unchanged, the venue's units
        // are not, and max_plan_age_secs does not know where the cutoff is.
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor);
        cfg.mode = ArcusSpotRuntimeMode::Live;
        // The settlement margin has its own test; isolate the cutoff rule.
        cfg.corporate_action_settlement_margin_secs = 0;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor);
        let planned_at = anchor + Duration::seconds(3);
        let plan = runtime
            .build_plan(
                &context(planned_at - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::CorporateActionExit,
                planned_at,
                runtime.state.inventory,
            )
            .unwrap();
        runtime
            .validate_plan_consistent_with_state(&plan, planned_at)
            .unwrap();
        let error = runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(4))
            .unwrap_err();
        assert!(error.contains("no longer quotes"), "{error}");
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn an_entry_plan_is_refused_at_dispatch_once_a_window_has_opened() {
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor + Duration::seconds(10));
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(anchor - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                anchor,
                runtime.state.inventory,
            )
            .unwrap();
        runtime
            .validate_plan_consistent_with_state(&plan, anchor)
            .unwrap();
        let error = runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(10))
            .unwrap_err();
        assert!(error.contains("entries blocked"), "{error}");
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
            .validate_plan_consistent_with_state(&stale_entry_plan, event_time())
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
        assert!(runtime
            .validate_plan_consistent_with_state(&plan, event_time())
            .is_err());
    }

    #[test]
    fn build_plan_dispatches_on_the_plausible_venue_not_the_router_recommendation() {
        let runtime = ArcusSpotRuntime::new(config()).unwrap();
        let mut context = context(event_time() - Duration::seconds(2), Decimal::from(20));
        let arcus_buy_amount = context
            .row
            .forward
            .as_ref()
            .unwrap()
            .response
            .payload
            .quotes[0]
            .buy_amount
            .clone();
        with_implausible_rialto_recommended(context.row.forward.as_mut().unwrap());
        let plan = runtime
            .build_plan(
                &context,
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                event_time(),
                runtime.state.inventory,
            )
            .unwrap();
        assert_eq!(plan.venue, "arcus");
        assert_eq!(plan.buy_amount_raw, arcus_buy_amount);
        assert_ne!(
            context
                .row
                .forward
                .as_ref()
                .unwrap()
                .response
                .payload
                .recommended,
            plan.venue,
            "the router's recommendation was rialto and must not have been dispatched"
        );
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
        assert!(runtime
            .validate_plan_consistent_with_state(&plan, event_time())
            .is_err());
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
    /// bot-strategy#979: the live incident's shape. The settlement refunded
    /// 16 wei of an 18-decimal sell, so the wallet parted with slightly less
    /// than planned and the runtime has to commit exactly that -- otherwise
    /// it books inventory the wallet still holds.
    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn a_dust_short_confirmed_sell_commits_what_actually_moved() {
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
        let refunded = Decimal::new(16, 18);
        let sold = plan.sell_quantity - refunded;

        runtime
            .apply_confirmed_live_fill(&plan, sold, plan.buy_quantity, event_time())
            .unwrap();

        assert_eq!(runtime.state().inventory.token_a, before.token_a - sold);
        assert_eq!(
            runtime.state().inventory.token_a,
            before.token_a - plan.sell_quantity + refunded
        );
    }

    /// The dust that a refund leaves is not an open position: exits are
    /// sized at exactly the tracked open quantity, so carrying a few wei
    /// forward would leave the bot asking the venue to quote dust forever
    /// (bot-strategy#979).
    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn a_dust_short_confirmed_exit_closes_the_rotation() {
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
        let open = runtime.state.rotated_quantity.unwrap();

        let mut exit = entry;
        exit.direction = ArcusSpotDirection::TokenBToTokenA;
        exit.trigger = ArcusSpotRotationTrigger::MeanReversionExit;
        exit.sell_quantity = open;
        exit.buy_quantity = Decimal::new(1, 3);
        runtime
            .apply_confirmed_live_fill(
                &exit,
                open - Decimal::new(16, 18),
                exit.buy_quantity,
                event_time(),
            )
            .unwrap();

        assert_eq!(runtime.state().regime, ArcusSpotRegime::Neutral);
        assert_eq!(runtime.state().rotated_quantity, None);
        assert_eq!(runtime.state().last_rotation_at, None);
    }

    /// The dust band is the whole licence: a shortfall bigger than that is a
    /// partial fill this design never performs, and the last seam before
    /// inventory moves refuses it on its own rather than trusting whatever
    /// derived the quantity (bot-strategy#979).
    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn a_materially_short_confirmed_sell_is_still_refused() {
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
        // One part per million short -- a thousand times the dust band.
        let sold = plan.sell_quantity - plan.sell_quantity * Decimal::new(1, 6);

        let error = runtime
            .apply_confirmed_live_fill(&plan, sold, plan.buy_quantity, event_time())
            .unwrap_err();

        assert!(error.contains("settlement dust"));
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

    // ---- corporate-action / token-lifecycle guard (bot-strategy#853) ----

    fn corporate_action_event(anchor: DateTime<Utc>) -> ArcusSpotCorporateActionEvent {
        ArcusSpotCorporateActionEvent {
            event_id: "NVDA-2026-10-4FOR1".to_string(),
            symbols: vec!["NVDA".to_string()],
            entry_block_at: anchor,
            reduce_exit_at: anchor + Duration::seconds(2),
            effective_at: anchor + Duration::seconds(4),
            resume_not_before: anchor + Duration::seconds(12),
            source: "issuer notice 2026-09-20".to_string(),
            post_event_inventory: None,
        }
    }

    /// A history whose mean sits far from `ln(200/100)`, so the next tick on
    /// the standard 200/100 fixture is a strong positive z and an ordinary
    /// entry signal.
    fn seed_entry_signal_history(runtime: &mut ArcusSpotRuntime) {
        runtime.state.relative_log_price_history = vec![0.10, 0.11, 0.12];
    }

    /// A rotated regime holding well inside `max_hold_secs`, on a perfectly
    /// flat history so nothing but the corporate-action window can produce
    /// an exit.
    /// The identities a runtime that has been observing carries, pinned a
    /// minute before `at`. Fixtures that want the "never observed" shape
    /// clear them explicitly.
    fn seed_observed_identities(runtime: &mut ArcusSpotRuntime, at: DateTime<Utc>) {
        runtime.state.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        });
        runtime.state.last_token_b_identity = Some(ArcusSpotTokenIdentity {
            symbol: "AMD".to_string(),
            address: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            decimals: 18,
        });
        runtime.state.last_token_identity_at = Some(at - Duration::minutes(1));
    }

    fn seed_open_rotation(runtime: &mut ArcusSpotRuntime, at: DateTime<Utc>) {
        // A runtime holding an open rotation has necessarily been observing,
        // so it carries the token identities a live one would. Fixtures that
        // want the "never observed" shape clear them explicitly.
        runtime.state.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        });
        runtime.state.last_token_b_identity = Some(ArcusSpotTokenIdentity {
            symbol: "AMD".to_string(),
            address: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            decimals: 18,
        });
        runtime.state.last_token_identity_at = Some(at - Duration::minutes(1));
        let flat_price = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![flat_price; 3];
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at = Some(at);
    }

    /// `snapshot_with_valid_row`, with NVDA re-pointed at a different
    /// contract -- a symbol change, merger or redemption as the recorder
    /// actually reports one.
    const RELISTED_TOKEN_A_ADDRESS: &str = "0x00000000000000000000000000000000DeaDBeeF";

    fn snapshot_with_relisted_token_a(collected_at: DateTime<Utc>) -> ArcusSpotRecorderSnapshot {
        snapshot_with_relisted_token_a_at_prices(collected_at, "200", "100")
    }

    fn snapshot_with_relisted_token_a_at_prices(
        collected_at: DateTime<Utc>,
        token_a_price: &str,
        token_b_price: &str,
    ) -> ArcusSpotRecorderSnapshot {
        let mut snapshot =
            snapshot_with_valid_row_at_prices(collected_at, token_a_price, token_b_price);
        let ArcusSpotCapture::Success { observation } = &mut snapshot.token_metadata else {
            panic!("token metadata fixture is a success capture");
        };
        for token in &mut observation.payload {
            if token.symbol == "NVDA" {
                token.address = RELISTED_TOKEN_A_ADDRESS.to_string();
            }
        }
        let ArcusSpotCapture::Success { observation } = &mut snapshot.reference_overview else {
            panic!("reference overview fixture is a success capture");
        };
        for entry in &mut observation.payload {
            if entry.ticker == "NVDA" {
                entry.contract_address = RELISTED_TOKEN_A_ADDRESS.to_string();
            }
        }
        snapshot
    }

    #[test]
    fn an_unopened_window_leaves_every_decision_untouched() {
        let anchor = event_time();
        let mut without = ArcusSpotRuntime::new(config()).unwrap();
        let mut with = {
            let mut cfg = config();
            // Declared, but its window opens long after this run.
            cfg.corporate_actions = vec![corporate_action_event(anchor + Duration::days(30))];
            ArcusSpotRuntime::new(cfg).unwrap()
        };
        seed_entry_signal_history(&mut without);
        seed_entry_signal_history(&mut with);

        let plain = without.step_at(&snapshot_with_valid_row(anchor), anchor);
        let guarded = with.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert!(
            matches!(plain.decision, ArcusSpotDecision::SimulatedFill { .. }),
            "fixture must produce a real entry, got {:?}",
            plain.decision,
        );
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            serde_json::to_value(&guarded).unwrap(),
            "an unopened window must not change a single field of the event",
        );
        assert_eq!(with.state.corporate_action, None);
        assert_eq!(with.state, without.state);
    }

    #[test]
    fn a_declared_window_blocks_an_entry_the_signal_would_otherwise_take() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor - Duration::seconds(1))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);

        let event = runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(
                    hold.code,
                    ArcusSpotHoldCode::CorporateActionBlock,
                    "detail={}",
                    hold.detail,
                );
                assert!(
                    hold.detail.contains("NVDA-2026-10-4FOR1"),
                    "{}",
                    hold.detail
                );
                assert!(hold.detail.contains("issuer notice"), "{}", hold.detail);
            }
            other => panic!("expected a corporate-action hold, got {other:?}"),
        }
        assert!(
            event
                .z_score
                .is_some_and(|z| z >= runtime.config.entry_z_score),
            "the entry signal must still be present -- the window is what blocks it",
        );
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
        assert_eq!(
            runtime
                .state
                .corporate_action
                .as_ref()
                .map(|progress| progress.event_id.as_str()),
            Some("NVDA-2026-10-4FOR1"),
        );
    }

    #[test]
    fn a_window_blocks_entries_before_it_forces_anything_to_exit() {
        let anchor = event_time();
        let mut cfg = config();
        // Between entry_block_at and reduce_exit_at.
        cfg.corporate_actions = vec![corporate_action_event(anchor - Duration::seconds(1))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor);

        let event = runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock)
            }
            other => panic!("expected a hold before reduce_exit_at, got {other:?}"),
        }
        assert_eq!(
            runtime.state.regime,
            ArcusSpotRegime::RotatedAToB,
            "the rotation is held, not unwound, until the declared exit time",
        );
    }

    #[test]
    fn a_window_never_blocks_an_exit_the_signal_already_wants() {
        let anchor = event_time();
        let mut cfg = config();
        // Phase A: entries blocked, nothing forced yet.
        cfg.corporate_actions = vec![corporate_action_event(anchor - Duration::seconds(1))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let current = (200.0_f64 / 100.0_f64).ln();
        runtime.state.relative_log_price_history = vec![current - 0.01, current + 0.01];
        runtime.state.regime = ArcusSpotRegime::RotatedAToB;
        runtime.state.rotated_quantity = Some(Decimal::new(49, 3));
        runtime.state.last_rotation_at = Some(anchor - Duration::seconds(1));
        // A runtime holding a rotation has been observing, so the window
        // opening pins the identity it saw beforehand.
        runtime.state.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        });
        runtime.state.last_token_b_identity = Some(ArcusSpotTokenIdentity {
            symbol: "AMD".to_string(),
            address: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string(),
            decimals: 18,
        });
        runtime.state.last_token_identity_at = Some(anchor - Duration::minutes(1));

        let event = runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => assert_eq!(
                plan.trigger,
                ArcusSpotRotationTrigger::MeanReversionExit,
                "a window that could trap an open position would be worse than no window",
            ),
            other => {
                panic!("expected the mean-reversion exit to survive the window, got {other:?}")
            }
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn the_window_forces_an_open_rotation_to_unwind_at_its_exit_time() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor - Duration::seconds(3))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor);

        let event = runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        assert_eq!(
            event.z_score, None,
            "the flat history must offer no exit of its own, so only the window can",
        );
        match event.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::CorporateActionExit);
                assert_eq!(plan.direction, ArcusSpotDirection::TokenBToTokenA);
            }
            other => panic!("expected a forced corporate-action exit, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
    }

    #[test]
    fn a_forced_exit_still_fails_closed_when_the_venue_cannot_quote_it() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor - Duration::seconds(3))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor);

        let event = runtime.step_at(
            &snapshot_with_route_unavailable(anchor, "200", "100"),
            anchor,
        );

        match event.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::RouteUnavailable,
                "a corporate-action exit clears every ordinary gate, it never bypasses one",
            ),
            other => panic!("expected a hold, got {other:?}"),
        }
        assert_eq!(
            runtime.state.regime,
            ArcusSpotRegime::RotatedAToB,
            "an unquotable forced exit leaves the position open, it does not pretend to close it",
        );
    }

    #[test]
    fn the_pre_event_history_is_discarded_once_and_not_refilled_before_the_resume() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.signal_window_samples = 16;
        cfg.corporate_actions = vec![corporate_action_event(anchor)];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);

        // Inside the window but before the effective time: the pre-event
        // history is intact and still accumulating.
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert_eq!(runtime.state.relative_log_price_history.len(), 4);
        assert_eq!(
            runtime
                .state
                .corporate_action
                .as_ref()
                .unwrap()
                .history_invalidated_at,
            None,
        );

        // Crossing the effective time discards it.
        let effective_at = anchor + Duration::seconds(4);
        runtime.step_at(&snapshot_with_valid_row(effective_at), effective_at);
        assert!(runtime.state.relative_log_price_history.is_empty());
        assert_eq!(
            runtime
                .state
                .corporate_action
                .as_ref()
                .unwrap()
                .history_invalidated_at,
            Some(effective_at),
        );

        // And nothing refills it until the operator's resume time: those
        // prints describe neither the old instrument nor the new one.
        for offset in [5, 6, 11] {
            let at = anchor + Duration::seconds(offset);
            runtime.step_at(&snapshot_with_valid_row(at), at);
            assert!(
                runtime.state.relative_log_price_history.is_empty(),
                "a sample from inside the transition window must not rebuild the signal",
            );
            assert_eq!(
                runtime
                    .state
                    .corporate_action
                    .as_ref()
                    .unwrap()
                    .history_invalidated_at,
                Some(effective_at),
                "the discard happens exactly once",
            );
        }
    }

    #[test]
    fn the_resume_waits_for_the_operators_reconciled_inventory() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor)];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        let resumed_at = anchor + Duration::seconds(12);
        let event = runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);

        match event.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionResumePending,
                "detail={}",
                hold.detail,
            ),
            other => panic!("expected a resume-pending hold, got {other:?}"),
        }
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        assert_eq!(runtime.state.inventory, config().initial_inventory);
    }

    #[test]
    fn the_resume_adopts_the_reconciled_inventory_and_rebaselines_both_risk_baskets() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.signal_window_samples = 16;
        let mut event = corporate_action_event(anchor);
        // A 4-for-1 split: four times the shares, and a reference price the
        // recorder now reports at a quarter of what it was.
        let reconciled = ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        };
        event.post_event_inventory = Some(reconciled);
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let pre_event_baseline = runtime.state.initial_baseline_inventory;
        assert!(pre_event_baseline.is_some());

        let resumed_at = anchor + Duration::seconds(12);
        let resumed = runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);

        assert_eq!(runtime.state.inventory, reconciled);
        assert_eq!(runtime.state.initial_baseline_inventory, Some(reconciled));
        assert_eq!(runtime.state.daily_baseline_inventory, Some(reconciled));
        assert_ne!(
            runtime.state.initial_baseline_inventory, pre_event_baseline,
            "a basket that no longer exists would report the split itself as a rotation loss",
        );
        assert_eq!(
            runtime.state.handled_corporate_action_ids,
            vec!["NVDA-2026-10-4FOR1".to_string()],
        );
        assert_eq!(runtime.state.corporate_action, None);
        assert_eq!(runtime.state.risk_halt, None);

        // The window was emptied at the effective time, so the ordinary
        // warm-up gate -- not a second counter that could disagree with it --
        // is what holds entries until fresh samples accumulate.
        assert_eq!(runtime.state.relative_log_price_history.len(), 1);
        match resumed.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::Warmup)
            }
            other => panic!("expected warm-up after the resume, got {other:?}"),
        }
    }

    #[test]
    fn the_resume_waits_for_an_observation_taken_after_the_cutoff() {
        // The evaluation clock is not the evidence. A snapshot collected
        // just before `resume_not_before` is still fresh enough to be
        // evaluated after it, but its prints come from the interval the
        // calendar declares untrustworthy -- and the resume would value the
        // reconciled holding at those prices and seed the emptied window
        // with their ratio.
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        let reconciled = ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        };
        event.post_event_inventory = Some(reconciled);
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        // resume_not_before is anchor + 12s. Collected at +11s, evaluated
        // at +13s: past the cutoff by the clock, before it by the data.
        let collected_before = anchor + Duration::seconds(11);
        let evaluated_after = anchor + Duration::seconds(13);
        let held = runtime.step_at(&snapshot_with_valid_row(collected_before), evaluated_after);
        match held.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock);
                assert!(
                    hold.detail.contains("before"),
                    "the hold must say why: {}",
                    hold.detail
                );
            }
            other => panic!("expected the resume to wait, got {other:?}"),
        }
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        assert_ne!(runtime.state.inventory, reconciled);
        assert!(runtime.state.relative_log_price_history.is_empty());

        // An observation from after the cutoff resumes.
        let post_cutoff = anchor + Duration::seconds(14);
        runtime.step_at(&snapshot_with_valid_row(post_cutoff), post_cutoff);
        assert_eq!(runtime.state.inventory, reconciled);
        assert_eq!(
            runtime.state.handled_corporate_action_ids,
            vec!["NVDA-2026-10-4FOR1".to_string()],
        );
    }

    #[test]
    fn the_resume_cutoff_is_judged_by_the_price_observation_time() {
        // The overview is captured separately from the snapshot that wraps
        // it and validated on its own receipt time. A collection that
        // finishes after `resume_not_before` can still carry an overview
        // received before it -- and those are the prices the resume would
        // re-anchor on.
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        let reconciled = ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        };
        event.post_event_inventory = Some(reconciled);
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        // Collected and evaluated at +13s (past the +12s cutoff); overview
        // received at +11s.
        let collected = anchor + Duration::seconds(13);
        let held = runtime.step_at(
            &snapshot_with_overview_received_at(collected, anchor + Duration::seconds(11)),
            collected,
        );
        match held.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock);
                assert!(hold.detail.contains("before"), "{}", hold.detail);
            }
            other => panic!("expected the resume to wait, got {other:?}"),
        }
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        assert_ne!(runtime.state.inventory, reconciled);

        // The same collection with an overview from after the cutoff resumes.
        let later = anchor + Duration::seconds(14);
        runtime.step_at(&snapshot_with_overview_received_at(later, later), later);
        assert_eq!(runtime.state.inventory, reconciled);
    }

    #[test]
    fn planning_blocks_entries_inside_the_pre_window_margin() {
        // The planner and the dispatch validator must agree, or live-tick
        // writes a plan its own dispatch refuses and replay records a fill
        // production would not send.
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor + Duration::seconds(10));
        cfg.corporate_action_settlement_margin_secs = 1;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);

        // entry_block_at is +10s; at +8s the 1s margin has not reached it.
        let early = anchor + Duration::seconds(8);
        assert!(matches!(
            runtime
                .step_at(&snapshot_with_valid_row(early), early)
                .decision,
            ArcusSpotDecision::SimulatedFill { .. }
        ));

        let mut inside =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(10))).unwrap();
        inside.config.corporate_action_settlement_margin_secs = 1;
        seed_entry_signal_history(&mut inside);
        let at = anchor + Duration::seconds(9);
        match inside.step_at(&snapshot_with_valid_row(at), at).decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock);
                assert!(
                    hold.detail.contains("already blocked at dispatch"),
                    "{}",
                    hold.detail
                );
            }
            other => panic!("no entry may be planned once dispatch would refuse it, got {other:?}"),
        }
    }

    #[test]
    fn an_unpinned_identity_refuses_the_forced_exit() {
        // The window was declared after it opened, so no pre-event identity
        // was ever observed. The drift check has nothing to compare, which
        // is not the same as a passed comparison.
        let anchor = event_time();
        let mut runtime =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        seed_open_rotation(&mut runtime, anchor);
        // The window was declared after it opened: no identity was observed
        // before it, so nothing can be pinned.
        runtime.state.last_token_a_identity = None;
        runtime.state.last_token_b_identity = None;
        runtime.state.last_token_identity_at = None;
        let reduce = anchor + Duration::seconds(4);
        let outcome = runtime.step_at(&snapshot_with_valid_row(reduce), reduce);
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .is_some_and(|p| p.pre_event_token_a.is_none()));
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "{}",
                hold.detail
            ),
            other => panic!("expected no exit without an identity to check, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);

        // Control: one observation before the window pins the identity, and
        // the reduce phase then exits as declared.
        let mut control =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        control.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        control.step_at(&snapshot_with_valid_row(anchor), anchor);
        seed_open_rotation(&mut control, anchor);
        let reduce = anchor + Duration::seconds(4);
        assert!(matches!(
            control
                .step_at(&snapshot_with_valid_row(reduce), reduce)
                .decision,
            ArcusSpotDecision::SimulatedFill { .. }
        ));
    }

    #[test]
    fn a_repointed_ticker_does_not_engage_an_artificial_halt() {
        // Before `effective_at` the units are not yet "stale" by the
        // calendar, but a repointed ticker already makes the mark mixed:
        // old quantity, new contract's price. A halt from that is artificial
        // and sticky, and it wedges the relisting recovery.
        let anchor = event_time();
        let mut runtime =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        // A prior rotation left the wallet 0.005 NVDA short of its basket:
        // $1.00 at 200, but $4.00 at the replacement's 800.
        runtime.state.inventory.token_a = Decimal::new(995, 3);
        // One observation before the window, so the identity is pinned.
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert_eq!(
            runtime.state.risk_halt, None,
            "$4 at 200 is over the $2 limit only later"
        );

        // Inside the window, before `effective_at` (+5s), the ticker is
        // repointed and the replacement quotes at 800.
        let inside = anchor + Duration::seconds(3);
        let outcome = runtime.step_at(
            &snapshot_with_relisted_token_a_at_prices(inside, "800", "100"),
            inside,
        );
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "{}",
                hold.detail
            ),
            other => panic!("expected the relisting to hold, got {other:?}"),
        }
        assert_eq!(
            runtime.state.risk_halt, None,
            "the mark mixes the old quantity with the replacement's price",
        );

        // Control: the same price move without a relisting is a real breach.
        let mut control =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        control.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        control.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut control, anchor);
        control.state.inventory.token_a = Decimal::new(995, 3);
        control.step_at(&snapshot_with_valid_row(anchor), anchor);
        control.step_at(
            &snapshot_with_valid_row_at_prices(inside, "800", "100"),
            inside,
        );
        assert!(
            control.state.risk_halt.is_some(),
            "a real shortfall still halts"
        );
    }

    #[test]
    fn an_unpinned_identity_also_suppresses_halt_engagement() {
        // A window declared after it opened leaves the identity unpinned.
        // The marks are then unverifiable, and a halt engaged from one is
        // artificial *and* sticky -- it would wedge the relisting recovery,
        // since the window refuses the exit and clear-risk-halt refuses the
        // clearance.
        let anchor = event_time();
        let mut runtime =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        runtime.state.inventory.token_a = Decimal::new(995, 3);
        // No observation before the window: nothing can be pinned.
        let inside = anchor + Duration::seconds(3);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(inside, "800", "100"),
            inside,
        );
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .is_some_and(|p| p.pre_event_token_a.is_none()));
        assert_eq!(
            runtime.state.risk_halt, None,
            "unverifiable marks may not halt"
        );

        // A later tick, with the record now in place, still suppresses.
        let later = anchor + Duration::seconds(4);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(later, "800", "100"),
            later,
        );
        assert_eq!(runtime.state.risk_halt, None);
    }

    #[test]
    fn a_token_repointed_before_the_effective_time_is_not_force_exited() {
        // `effective_at` is the operator's declared date; the issuer can
        // repoint the ticker earlier. The reduce phase is exactly where the
        // runtime forces an exit, so drift has to be caught from the moment
        // the window opens, not from the declared cutoff.
        let anchor = event_time();
        // entry_block +1s, reduce_exit +3s, effective +5s.
        let mut runtime =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        // One observation strictly before the window, so a pin exists.
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        seed_open_rotation(&mut runtime, anchor);
        let inside = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(inside), inside);
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .and_then(|p| p.pre_event_token_a.as_ref())
            .is_some());

        // Reduce phase (+4s), still before `effective_at`, relisted.
        let reduce = anchor + Duration::seconds(4);
        let outcome = runtime.step_at(&snapshot_with_relisted_token_a(reduce), reduce);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "{}",
                hold.detail
            ),
            other => panic!("expected an unresolved hold instead of a forced exit, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn a_repointed_token_is_not_force_exited() {
        // Past `effective_at` the ticker may already name a different
        // contract. A forced exit planned there uses the old contract's
        // tracked quantity against the new address: it sells the wrong
        // instrument if the wallet holds any of the replacement, or
        // resubmits an impossible order every tick. Identity drift must hold
        // before any exit is forced, not only before the resume.
        let anchor = event_time();
        let mut cfg = config();
        // Window opens at +1s so the tick at `anchor` is a pre-event
        // observation the guard can pin: reduce_exit at +3s, effective +5s.
        let mut event = corporate_action_event(anchor + Duration::seconds(1));
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        seed_open_rotation(&mut runtime, anchor);
        let inside = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(inside), inside);
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .and_then(|p| p.pre_event_token_a.as_ref())
            .is_some());

        let effective = anchor + Duration::seconds(6);
        let outcome = runtime.step_at(&snapshot_with_relisted_token_a(effective), effective);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "{}",
                hold.detail
            ),
            other => panic!("expected an unresolved hold instead of a forced exit, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
        assert!(
            outcome_detail_mentions_relisting(&runtime),
            "the identity drift, not the open rotation, is what is reported first",
        );

        // Control: in the reduce phase, identity intact, the exit is forced.
        let mut control =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        control.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        control.step_at(&snapshot_with_valid_row(anchor), anchor);
        seed_open_rotation(&mut control, anchor);
        let reduce = anchor + Duration::seconds(4);
        let unwound = control.step_at(&snapshot_with_valid_row(reduce), reduce);
        match unwound.decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::CorporateActionExit)
            }
            other => panic!("expected the forced exit in the reduce phase, got {other:?}"),
        }
        assert_eq!(control.state.regime, ArcusSpotRegime::Neutral);
    }

    fn cfg_with_window_at(entry_block_at: DateTime<Utc>) -> ArcusSpotRuntimeConfig {
        let mut cfg = config();
        let mut event = corporate_action_event(entry_block_at);
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        cfg
    }

    fn outcome_detail_mentions_relisting(runtime: &ArcusSpotRuntime) -> bool {
        // The drift hold names the relisted address; the open-rotation hold
        // does not. Checked through the pinned identity rather than the
        // event text so the assertion does not depend on hold wording.
        runtime
            .state
            .corporate_action
            .as_ref()
            .and_then(|p| p.pre_event_token_a.as_ref())
            .is_some_and(|pinned| {
                !pinned
                    .address
                    .eq_ignore_ascii_case(RELISTED_TOKEN_A_ADDRESS)
            })
    }

    #[test]
    fn a_max_hold_exit_is_not_taken_from_stale_units() {
        // The forced exit is gone past `effective_at`; the ordinary
        // max-hold exit must not slip through the gate's exit pass-through
        // in its place -- it sizes from the same stale `rotated_quantity`.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        // Opened two hours ago against a 3,600s max hold: overdue.
        seed_open_rotation(&mut runtime, anchor - Duration::hours(2));
        let effective = anchor + Duration::seconds(6);
        let outcome = runtime.step_at(&snapshot_with_valid_row(effective), effective);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "{}",
                hold.detail
            ),
            other => panic!("expected no exit from stale units, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);

        // Control: the same overdue rotation before `effective_at` exits
        // (forced by the reduce phase here, at +2s).
        let mut control = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_open_rotation(&mut control, anchor - Duration::hours(2));
        let reduce = anchor + Duration::seconds(2);
        assert!(matches!(
            control
                .step_at(&snapshot_with_valid_row(reduce), reduce)
                .decision,
            ArcusSpotDecision::SimulatedFill { .. }
        ));
    }

    #[test]
    fn deleting_a_declaration_after_the_effective_time_does_not_force_an_exit() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_open_rotation(&mut runtime, anchor);
        // Reduce phase, no route: the exit was attempted and blocked.
        let reduce = anchor + Duration::seconds(2);
        runtime.step_at(
            &snapshot_with_route_unavailable(reduce, "200", "100"),
            reduce,
        );
        // Past `effective_at` the window is invalidated; the operator then
        // deletes the entry.
        let effective = anchor + Duration::seconds(6);
        runtime.step_at(
            &snapshot_with_route_unavailable(effective, "200", "100"),
            effective,
        );
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .is_some_and(|p| p.history_invalidated_at.is_some()));
        runtime.config.corporate_actions.clear();
        let later = anchor + Duration::seconds(8);
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        assert!(
            matches!(outcome.decision, ArcusSpotDecision::Observe { .. }),
            "an undeclared window with stale units must not size an exit: {:?}",
            outcome.decision,
        );
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn deleting_a_declaration_in_the_reduce_phase_stops_exits_for_good() {
        // Deleted before `effective_at`: the record carries no discard stamp
        // and, with the declaration gone, never will. If the gate kept
        // forcing exits "until stale", it would force them straight through
        // the real event from the pre-event quantity.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_open_rotation(&mut runtime, anchor);
        let reduce = anchor + Duration::seconds(2);
        runtime.step_at(
            &snapshot_with_route_unavailable(reduce, "200", "100"),
            reduce,
        );
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .is_some_and(|p| p.history_invalidated_at.is_none()));
        runtime.config.corporate_actions.clear();
        // Well past where the deleted declaration's effective_at (+4s) was,
        // with a route available.
        for offset in [3, 6, 20] {
            let at = anchor + Duration::seconds(offset);
            let outcome = runtime.step_at(&snapshot_with_valid_row(at), at);
            assert!(
                matches!(outcome.decision, ArcusSpotDecision::Observe { .. }),
                "no exit may be sized under an undeclared window (+{offset}s): {:?}",
                outcome.decision,
            );
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
        assert!(runtime.state.corporate_action.is_some(), "progress is kept");
    }

    #[test]
    fn an_orphaned_window_still_goes_stale_at_its_own_effective_time() {
        // Deleted in the reduce phase; the record carries the cutoff, so it
        // still discards the window and stops halting on stale units at the
        // original effective_at -- the reverse-split shortfall from the
        // round-3 test, valued at $800, must not halt here either.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        runtime.state.inventory.token_a = Decimal::new(995, 3);
        let reduce = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(reduce), reduce);
        assert_eq!(runtime.state.risk_halt, None);
        runtime.config.corporate_actions.clear();

        let effective = anchor + Duration::seconds(6);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(effective, "800", "100"),
            effective,
        );
        let progress = runtime.state.corporate_action.clone().unwrap();
        assert_eq!(progress.effective_at, Some(anchor + Duration::seconds(4)));
        assert!(
            progress.history_invalidated_at.is_some(),
            "the orphan still stamps"
        );
        assert!(runtime.state.relative_log_price_history.is_empty());
        assert_eq!(
            runtime.state.risk_halt, None,
            "old units at new prices are not a loss"
        );
        assert!(runtime.corporate_action_units_are_stale(effective));
    }

    #[test]
    fn a_handled_id_reused_for_a_different_action_is_refused() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);

        // A later, distinct action declared under the old id.
        let later = resumed_at + Duration::days(30);
        let mut reused = corporate_action_event(later);
        reused.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(8),
            token_b: Decimal::ONE,
        });
        runtime.config.corporate_actions = vec![reused];
        runtime.state.relative_log_price_history = vec![0.10, 0.11, 0.12];
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock);
                assert!(hold.detail.contains("reuses the id"), "{}", hold.detail);
            }
            other => panic!("a reused id must not trade through its window, got {other:?}"),
        }
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);
        // The refused window is recorded, so deleting the declaration cannot
        // erase the refusal (see the deletion test below).
        let progress = runtime.state.corporate_action.clone().unwrap();
        assert_eq!(
            progress.fingerprint,
            runtime.config.corporate_actions[0].fingerprint()
        );
    }

    #[test]
    fn a_resume_after_a_legacy_checkpoint_keeps_the_records_aligned() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        // The supported legacy shape: an id with no fingerprint beside it,
        // and the historical event still declared.
        let mut legacy = corporate_action_event(anchor - Duration::days(60));
        legacy.event_id = "NVDA-2026-06-SPLIT".to_string();
        legacy.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::ONE,
            token_b: Decimal::ONE,
        });
        runtime.config.corporate_actions.insert(0, legacy.clone());
        runtime.state.handled_corporate_action_ids = vec!["NVDA-2026-06-SPLIT".to_string()];
        assert!(runtime
            .state
            .handled_corporate_action_fingerprints
            .is_empty());
        runtime.state.handled_corporate_actions_resolved = false;
        // Loaded the way production loads it: `from_state` resolves the
        // legacy id against the declaration whose window completed before
        // the last observation, so it is handled rather than ambiguous.
        runtime.state.last_observation_at = Some(anchor - Duration::days(1));
        let mut runtime =
            ArcusSpotRuntime::from_state(runtime.config.clone(), runtime.state.clone()).unwrap();
        assert_eq!(
            runtime.handled_record_for(&legacy),
            Some(HandledMatch::Same),
            "the legacy id resolves to the event it was written for",
        );
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);

        assert_eq!(
            runtime.state.handled_corporate_action_ids,
            vec![
                "NVDA-2026-06-SPLIT".to_string(),
                "NVDA-2026-10-4FOR1".to_string()
            ],
        );
        assert_eq!(
            runtime.state.handled_corporate_action_fingerprints,
            vec![
                legacy.fingerprint(),
                runtime.config.corporate_actions[1].fingerprint()
            ],
        );
        assert_eq!(
            runtime.handled_record_for(&legacy),
            Some(HandledMatch::Same),
            "the resolved legacy id stays handled after the next resume",
        );
        assert_eq!(
            runtime.handled_record_for(&runtime.config.corporate_actions[1]),
            Some(HandledMatch::Same),
        );
        // And nothing blocks: no overlay from a phantom reuse.
        runtime.state.relative_log_price_history = vec![0.25, 0.26, 0.27];
        let later = resumed_at + Duration::seconds(60);
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        if let ArcusSpotDecision::Observe { hold } = &outcome.decision {
            assert_ne!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionBlock,
                "{}",
                hold.detail
            );
        }
    }

    #[test]
    fn a_pre_cutoff_observation_processed_late_still_halts() {
        // The overview was received before `effective_at`, so the mark is
        // made of pre-event prices against pre-event inventory -- a genuine
        // breach. Suppressing it because the tick happens to be processed
        // after the cutoff would lose it permanently: every later mark is
        // stale, and the resume re-anchors both baskets.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        // 0.02 NVDA short: $4.00 at the pre-event price, over the $2 limit.
        runtime.state.inventory.token_a = Decimal::new(98, 2);

        // effective_at is anchor + 4s. Observed at +3s, processed at +5s.
        let observed_at = anchor + Duration::seconds(3);
        let evaluated_at = anchor + Duration::seconds(5);
        runtime.step_at(
            &snapshot_with_overview_received_at(observed_at, observed_at),
            evaluated_at,
        );
        assert!(
            runtime.state.risk_halt.is_some(),
            "a pre-cutoff mark is measured in the units the venue was quoting",
        );

        // Prices that arrived after the cutoff stay suppressed even when the
        // collection that wrapped them started before it.
        let mut straddling = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut straddling);
        straddling.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut straddling, anchor);
        straddling.state.inventory.token_a = Decimal::new(98, 2);
        straddling.step_at(
            &snapshot_with_overview_received_at(
                anchor + Duration::seconds(3),
                anchor + Duration::seconds(5),
            ),
            anchor + Duration::seconds(6),
        );
        assert_eq!(
            straddling.state.risk_halt, None,
            "the prices are post-event even though the collection started before the cutoff",
        );

        // A mark whose observation is itself past the cutoff stays suppressed.
        let mut later = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut later);
        later.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut later, anchor);
        later.state.inventory.token_a = Decimal::new(98, 2);
        let post = anchor + Duration::seconds(5);
        later.step_at(&snapshot_with_overview_received_at(post, post), post);
        assert_eq!(later.state.risk_halt, None);
    }

    #[test]
    fn a_lagging_pre_cutoff_mark_still_halts_after_the_stamp() {
        // The discard stamp is a fact about the calendar, not about a
        // particular mark. Once it exists, an ordinary overview lag still
        // produces a pre-event mark, and suppressing it would drop a genuine
        // breach for good -- and disagree with the continuity verifier,
        // which applies the price clock.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        // $4.00 short of the basket at 200, against the $2 daily limit.
        runtime.state.inventory.token_a = Decimal::new(98, 2);

        // effective_at is +4s. This tick is priced *after* it, so the units
        // are stale, no halt is owed -- and the gate writes the stamp.
        let stamping = anchor + Duration::seconds(5);
        runtime.step_at(
            &snapshot_with_overview_received_at(stamping, stamping),
            stamping,
        );
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .is_some_and(|p| p.history_invalidated_at.is_some()));
        assert_eq!(
            runtime.state.risk_halt, None,
            "post-cutoff prices are stale units"
        );

        // The next collection lags: its overview was received before the
        // cutoff, so this is a pre-event mark and the shortfall is real.
        let lagging = anchor + Duration::seconds(6);
        runtime.step_at(
            &snapshot_with_overview_received_at(lagging, anchor + Duration::seconds(3)),
            lagging,
        );
        assert!(
            runtime.state.risk_halt.is_some(),
            "a pre-cutoff mark is measured in the units the venue was quoting",
        );
    }

    #[test]
    fn a_halt_is_not_cleared_on_stale_unit_marks() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        // A real $4 shortfall, halted before the event.
        runtime.state.inventory.token_a = Decimal::new(98, 2);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert!(runtime.state.risk_halt.is_some());
        // Past effective_at at a post-split price that shrinks the mark.
        let effective = anchor + Duration::seconds(6);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(effective, "50", "100"),
            effective,
        );
        let error = runtime
            .clear_risk_halt_at(effective + Duration::seconds(1))
            .unwrap_err();
        assert!(error.contains("no longer quotes"), "{error}");
        assert!(runtime.state.risk_halt.is_some());
    }

    #[test]
    fn a_halt_is_not_cleared_after_downtime_past_the_cutoff() {
        // The bot stopped before `effective_at`, so the watermark is still
        // pre-event; the operator clears after the action became effective.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        runtime.state.inventory.token_a = Decimal::new(98, 2);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert!(runtime.state.risk_halt.is_some());
        assert_eq!(
            runtime.state.last_observation_at,
            Some(anchor),
            "the watermark stays before the cutoff",
        );

        let error = runtime
            .clear_risk_halt_at(anchor + Duration::seconds(10))
            .unwrap_err();
        assert!(error.contains("no longer quotes"), "{error}");
        assert!(runtime.state.risk_halt.is_some());
    }

    #[test]
    fn a_restored_runtime_may_hold_more_than_the_declared_funding() {
        // A completed split raised the holding to 40 and its declaration was
        // retired; a floor of 5 is then perfectly reachable, and the config
        // alone cannot tell -- only the restored state can.
        let mut cfg = config();
        cfg.inventory_floors.token_a = Decimal::from(5);
        cfg.validate().unwrap();
        let mut state = ArcusSpotRuntime::new(config()).unwrap().state().clone();
        state.inventory.token_a = Decimal::from(40);
        state.initial_baseline_inventory = Some(state.inventory);
        state.daily_baseline_inventory = Some(state.inventory);
        ArcusSpotRuntime::from_state(cfg.clone(), state.clone()).unwrap();

        // A restored holding that does not reach the floor is still refused.
        let mut short = state;
        short.inventory.token_a = Decimal::new(4, 0);
        assert!(ArcusSpotRuntime::from_state(cfg, short).is_err());
    }

    #[test]
    fn a_fresh_runtime_must_start_above_its_floors() {
        // The config is valid -- a declared reconciliation reaches the floor
        // -- but a fresh runtime starts at initial_inventory and would
        // persist state its own next load rejects.
        let mut cfg = cfg_with_window_at(event_time());
        cfg.corporate_actions[0].post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(40),
            token_b: Decimal::ONE,
        });
        cfg.inventory_floors.token_a = Decimal::from(5);
        cfg.validate().unwrap();
        let error = match ArcusSpotRuntime::new(cfg) {
            Ok(_) => panic!("a fresh runtime below its floors must be refused"),
            Err(error) => error,
        };
        assert!(error.contains("below"), "{error}");
    }

    #[test]
    fn a_raised_floor_defers_to_a_pending_reconciliation_that_satisfies_it() {
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor);
        cfg.corporate_actions[0].post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(40),
            token_b: Decimal::ONE,
        });
        let mut runtime = ArcusSpotRuntime::new(cfg.clone()).unwrap();
        seed_entry_signal_history(&mut runtime);
        // Prior rotations left the wallet below the declared figure.
        runtime.state.inventory.token_a = Decimal::new(5, 1);
        let inside = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(inside), inside);
        assert!(runtime.state.corporate_action.is_some());
        let state = runtime.state.clone();

        // The floor is raised past the stale holding (0.5) -- still within
        // initial_inventory (1), so the config is valid and the change is
        // state-preserving -- but below the reconciled one (40): the load
        // must succeed so the resume can run.
        let mut raised = cfg.clone();
        raised.inventory_floors.token_a = Decimal::new(8, 1);
        ArcusSpotRuntime::from_state(raised.clone(), state.clone()).unwrap();

        // Without a satisfying reconciliation the floor still refuses.
        let mut unsatisfied = raised.clone();
        unsatisfied.corporate_actions[0].post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::new(6, 1),
            token_b: Decimal::ONE,
        });
        assert!(ArcusSpotRuntime::from_state(unsatisfied, state.clone()).is_err());
        let mut no_window = state.clone();
        no_window.corporate_action = None;
        assert!(ArcusSpotRuntime::from_state(raised, no_window).is_err());
    }

    #[test]
    fn a_reconciled_holding_under_a_floor_stays_pending() {
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        // Floors are 0.1/0.1; a partial redemption leaves token_b under.
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::new(5, 2),
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let before = runtime.state.inventory;
        let resumed_at = anchor + Duration::seconds(12);
        let outcome = runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionResumePending);
                assert!(
                    hold.detail.contains("below inventory_floors"),
                    "{}",
                    hold.detail
                );
            }
            other => panic!("expected the resume to stay pending, got {other:?}"),
        }
        assert_eq!(runtime.state.inventory, before);
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn an_exit_plan_is_refused_inside_the_settlement_margin() {
        // Submission is not execution: an exit sent seconds before the
        // cutoff can be mined after it. The margin stops exits early.
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor);
        cfg.mode = ArcusSpotRuntimeMode::Live;
        cfg.corporate_action_settlement_margin_secs = 1;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor - Duration::hours(2));
        let planned_at = anchor + Duration::seconds(1);
        let plan = runtime
            .build_plan(
                &context(planned_at - Duration::seconds(1), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::CorporateActionExit,
                planned_at,
                runtime.state.inventory,
            )
            .unwrap();
        // effective_at is anchor + 4s; at +1s the margin (1s) has not bitten.
        runtime
            .validate_plan_consistent_with_state(&plan, planned_at)
            .unwrap();
        // At +3s it has: 3s + 1s margin reaches the cutoff.
        let error = runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(3))
            .unwrap_err();
        assert!(
            error.contains("settle after the venue changes units"),
            "{error}"
        );
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn an_exit_plan_is_refused_at_dispatch_past_a_reused_ids_cutoff() {
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor);
        cfg.mode = ArcusSpotRuntimeMode::Live;
        // The settlement margin has its own test; isolate the cutoff rule.
        cfg.corporate_action_settlement_margin_secs = 0;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        // The first action is handled; a distinct later one reuses its id,
        // with effective_at at +17s.
        runtime.state.handled_corporate_action_ids = vec!["NVDA-2026-10-4FOR1".to_string()];
        runtime.state.handled_corporate_action_fingerprints =
            vec![runtime.config.corporate_actions[0].fingerprint()];
        let mut reused = corporate_action_event(anchor + Duration::seconds(13));
        reused.post_event_inventory = None;
        runtime.config.corporate_actions = vec![reused];
        assert_eq!(
            runtime.handled_record_for(&runtime.config.corporate_actions[0]),
            Some(HandledMatch::ReusedId)
        );
        seed_open_rotation(&mut runtime, anchor - Duration::hours(2));
        // A legitimate max-hold exit planned before the cutoff...
        let planned_at = anchor + Duration::seconds(10);
        let plan = runtime
            .build_plan(
                &context(planned_at - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenBToTokenA,
                ArcusSpotRotationTrigger::MaxHoldExit,
                planned_at,
                runtime.state.inventory,
            )
            .unwrap();
        runtime
            .validate_plan_consistent_with_state(&plan, planned_at)
            .unwrap();
        // ...dispatched after it.
        let error = runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(17))
            .unwrap_err();
        assert!(error.contains("no longer quotes"), "{error}");
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn an_entry_plan_is_refused_inside_the_settlement_margin() {
        // Between the submit guard and the client call the ledger's dispatch
        // marker still has to be persisted, and the venue round trip is
        // outside this process entirely -- so entries stop a margin before
        // the window opens rather than being re-checked after each step.
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor + Duration::seconds(10));
        cfg.mode = ArcusSpotRuntimeMode::Live;
        cfg.corporate_action_settlement_margin_secs = 1;
        let runtime = ArcusSpotRuntime::new(cfg).unwrap();
        let plan = runtime
            .build_plan(
                &context(anchor - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                anchor,
                runtime.state.inventory,
            )
            .unwrap();
        // entry_block_at is +10s: at +8s the 1s margin has not reached it.
        runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(8))
            .unwrap();
        let error = runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(9))
            .unwrap_err();
        assert!(error.contains("entries blocked"), "{error}");
    }

    #[cfg(feature = "arcus-spot-live")]
    #[test]
    fn an_entry_plan_is_refused_at_dispatch_while_progress_is_unresolved() {
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor + Duration::seconds(5));
        cfg.mode = ArcusSpotRuntimeMode::Live;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        // A fresh signed entry plan from before the window.
        let plan = runtime
            .build_plan(
                &context(anchor - Duration::seconds(2), Decimal::from(20)),
                ArcusSpotDirection::TokenAToTokenB,
                ArcusSpotRotationTrigger::EntrySignal,
                anchor,
                runtime.state.inventory,
            )
            .unwrap();
        // The window opens, then the declaration is removed: the planner
        // keeps the record and fails closed; so must dispatch.
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        let inside = anchor + Duration::seconds(6);
        runtime.step_at(&snapshot_with_valid_row(inside), inside);
        assert!(runtime.state.corporate_action.is_some());
        runtime.config.corporate_actions.clear();
        let error = runtime
            .validate_plan_consistent_with_state(&plan, anchor + Duration::seconds(7))
            .unwrap_err();
        assert!(error.contains("is unresolved"), "{error}");
    }

    #[test]
    fn a_reused_id_past_its_cutoff_is_fail_closed_for_history_and_exits() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);

        // A distinct later action under the old id, already effective, with
        // an overdue rotation open. Kept within the fixture quote's 30s
        // freshness of `anchor`: otherwise StaleQuote blocks the exit on its
        // own and the test proves nothing about the overlay.
        let mut reused = corporate_action_event(anchor + Duration::seconds(13));
        reused.post_event_inventory = None;
        runtime.config.corporate_actions = vec![reused];
        seed_open_rotation(&mut runtime, anchor - Duration::hours(2));
        let later = anchor + Duration::seconds(18); // past effective_at (+17s)
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        assert!(
            matches!(outcome.decision, ArcusSpotDecision::Observe { .. }),
            "no exit from stale units under a refused declaration: {:?}",
            outcome.decision
        );
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
        assert!(
            runtime.state.relative_log_price_history.is_empty(),
            "past its cutoff the refused window discards the pre-event window and adds nothing"
        );
    }

    #[test]
    fn a_future_action_reusing_a_legacy_id_is_not_handled() {
        // The checkpoint predates fingerprints: an id, no fingerprint. A
        // later, distinct action declared under that id must not inherit
        // "handled" from it.
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.state.handled_corporate_action_ids = vec!["NVDA-2026-10-4FOR1".to_string()];
        runtime.state.handled_corporate_action_fingerprints.clear();
        runtime.state.handled_corporate_actions_resolved = false;
        // The declaration has not reached its own resume time, so no resume
        // can have written that record for it.
        let inside = anchor + Duration::seconds(2);
        assert_eq!(
            runtime.handled_record_for(&runtime.config.corporate_actions[0]),
            Some(HandledMatch::ReusedId),
        );

        let outcome = runtime.step_at(&snapshot_with_valid_row(inside), inside);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock);
                assert!(hold.detail.contains("reuses the id"), "{}", hold.detail);
            }
            other => panic!("a legacy id must not license a new action, got {other:?}"),
        }
        assert!(
            runtime.state.corporate_action.is_some(),
            "the window is recorded"
        );

        // A legacy record whose window had completed by the last
        // observation is resolved to that event and stays handled.
        let mut past = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        past.state.handled_corporate_action_ids = vec!["NVDA-2026-10-4FOR1".to_string()];
        past.state.handled_corporate_action_fingerprints.clear();
        past.state.handled_corporate_actions_resolved = false;
        past.state.last_observation_at = Some(anchor + Duration::seconds(13));
        let resolved =
            ArcusSpotRuntime::from_state(past.config.clone(), past.state.clone()).unwrap();
        assert_eq!(
            resolved.handled_record_for(&resolved.config.corporate_actions[0]),
            Some(HandledMatch::Same),
        );

        // Offline through the whole later window: the record cannot be
        // confirmed as that declaration, so it stays refused rather than
        // flipping to handled once the clock passes the resume time.
        let mut offline = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        offline.state.handled_corporate_action_ids = vec!["NVDA-2026-10-4FOR1".to_string()];
        offline.state.handled_corporate_action_fingerprints.clear();
        offline.state.handled_corporate_actions_resolved = false;
        offline.state.last_observation_at = Some(anchor - Duration::seconds(1));
        let resolved =
            ArcusSpotRuntime::from_state(offline.config.clone(), offline.state.clone()).unwrap();
        assert_eq!(
            resolved.handled_record_for(&resolved.config.corporate_actions[0]),
            Some(HandledMatch::ReusedId),
        );
    }

    #[test]
    fn a_resolved_legacy_record_does_not_change_on_a_later_load() {
        // The resolution uses the observation watermark, which keeps
        // advancing. Re-running it on a later load would eventually confirm
        // a *different* action that has since passed its own resume time,
        // marking it handled for good -- so it runs once and is frozen.
        let anchor = event_time();
        let cfg = cfg_with_window_at(anchor);
        let mut state = ArcusSpotRuntime::new(cfg.clone()).unwrap().state().clone();
        state.handled_corporate_action_ids = vec!["NVDA-2026-10-4FOR1".to_string()];
        state.handled_corporate_action_fingerprints.clear();
        state.handled_corporate_actions_resolved = false;
        // The declaration has not completed its window, so it cannot be
        // confirmed as the event this record was written for.
        state.last_observation_at = Some(anchor - Duration::seconds(1));

        let first = ArcusSpotRuntime::from_state(cfg.clone(), state).unwrap();
        assert!(first.state.handled_corporate_actions_resolved);
        assert_eq!(
            first.handled_record_for(&first.config.corporate_actions[0]),
            Some(HandledMatch::ReusedId),
        );

        // Ticks carry the watermark past that declaration's resume time.
        let mut later = first.state.clone();
        later.last_observation_at = Some(anchor + Duration::seconds(60));
        let second = ArcusSpotRuntime::from_state(cfg, later).unwrap();
        assert_eq!(
            second.handled_record_for(&second.config.corporate_actions[0]),
            Some(HandledMatch::ReusedId),
            "a frozen resolution must not be reconsidered",
        );
    }

    /// A runtime whose handled record names this window's id under a
    /// *different* event's fingerprint: the declaration is refused, and the
    /// window is still a window.
    fn runtime_with_reused_id_window(anchor: DateTime<Utc>) -> ArcusSpotRuntime {
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        runtime.state.handled_corporate_action_ids =
            vec![runtime.config.corporate_actions[0].event_id.clone()];
        runtime.state.handled_corporate_action_fingerprints = vec!["an-older-event".to_string()];
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        runtime
    }

    #[test]
    fn a_reused_id_window_unwinds_on_the_tick_that_opens_its_record() {
        // After downtime the first observation can land in the reduce phase
        // and be the only dispatchable one. The tick that creates the
        // progress record must therefore apply the window's phases too.
        let anchor = event_time();
        let mut runtime = runtime_with_reused_id_window(anchor);
        seed_open_rotation(&mut runtime, anchor);
        assert_eq!(runtime.state.corporate_action, None);

        // reduce_exit_at is +2s and effective_at +4s.
        let reduce = anchor + Duration::seconds(3);
        match runtime
            .step_at(&snapshot_with_valid_row(reduce), reduce)
            .decision
        {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::CorporateActionExit)
            }
            other => panic!("expected the reduce phase to unwind on this tick, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
        assert!(
            runtime.state.corporate_action.is_some(),
            "the window is recorded"
        );
    }

    #[test]
    fn a_reused_id_window_does_not_engage_an_artificial_halt() {
        // `active_corporate_action` excludes a refused declaration, so the
        // halt-suppression path has to resolve it separately -- otherwise a
        // repointed ticker inside a reused-id window engages an artificial
        // sticky halt, and renaming the event cannot recover because
        // reset-window refuses an active halt.
        let anchor = event_time();
        let mut runtime = runtime_with_reused_id_window(anchor);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        runtime.state.inventory.token_a = Decimal::new(995, 3);

        let inside = anchor + Duration::seconds(3);
        runtime.step_at(
            &snapshot_with_relisted_token_a_at_prices(inside, "800", "100"),
            inside,
        );
        assert_eq!(
            runtime.state.risk_halt, None,
            "the mark mixes the old quantity with the replacement's price",
        );

        // Control: the same price move without a relisting is a real breach.
        let mut control = runtime_with_reused_id_window(anchor);
        control.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut control, anchor);
        control.state.inventory.token_a = Decimal::new(995, 3);
        control.step_at(
            &snapshot_with_valid_row_at_prices(inside, "800", "100"),
            inside,
        );
        assert!(
            control.state.risk_halt.is_some(),
            "a real shortfall still halts"
        );
    }

    #[test]
    fn a_reused_id_window_will_not_exit_against_a_repointed_ticker() {
        let anchor = event_time();
        let mut runtime = runtime_with_reused_id_window(anchor);
        // A pre-window observation pins the identity.
        runtime.step_at(
            &snapshot_with_valid_row(anchor - Duration::seconds(1)),
            anchor - Duration::seconds(1),
        );
        seed_open_rotation(&mut runtime, anchor);

        let reduce = anchor + Duration::seconds(3);
        let outcome = runtime.step_at(&snapshot_with_relisted_token_a(reduce), reduce);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "{}",
                hold.detail
            ),
            other => panic!("expected the relisting to hold, got {other:?}"),
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn deleting_a_refused_reused_id_declaration_does_not_erase_the_guard() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);

        // A distinct later action under the old id, already effective, with
        // an overdue rotation open.
        let mut reused = corporate_action_event(anchor + Duration::seconds(13));
        reused.post_event_inventory = None;
        runtime.config.corporate_actions = vec![reused];
        seed_open_rotation(&mut runtime, anchor - Duration::hours(2));
        // The tick that opens the record, then one that stamps its cutoff.
        let effective = anchor + Duration::seconds(18);
        runtime.step_at(&snapshot_with_valid_row(effective), effective);
        assert!(
            runtime.state.corporate_action.is_some(),
            "the window is recorded"
        );
        let stamped_at = anchor + Duration::seconds(19);
        runtime.step_at(&snapshot_with_valid_row(stamped_at), stamped_at);
        let progress = runtime.state.corporate_action.clone().unwrap();
        assert!(progress.history_invalidated_at.is_some());
        assert_eq!(progress.effective_at, Some(anchor + Duration::seconds(17)));

        // The operator deletes the offending entry instead of renaming it.
        runtime.config.corporate_actions.clear();
        let later = anchor + Duration::seconds(20);
        let samples_before = runtime.state.relative_log_price_history.len();
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        assert!(
            matches!(outcome.decision, ArcusSpotDecision::Observe { .. }),
            "the guard must survive the deletion: {:?}",
            outcome.decision
        );
        assert_eq!(
            runtime.state.regime,
            ArcusSpotRegime::RotatedAToB,
            "no exit"
        );
        assert_eq!(
            runtime.state.relative_log_price_history.len(),
            samples_before,
            "no post-event prints",
        );
        assert!(runtime.state.corporate_action.is_some());
    }

    #[test]
    fn a_reused_id_does_not_lift_an_effective_windows_suppression() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);

        // A second, distinct action, now past its effective_at with a
        // rotation open: stamped, exits suppressed.
        let second_at = anchor + Duration::seconds(20);
        let mut second = corporate_action_event(second_at);
        second.event_id = "NVDA-2026-11-2FOR1".to_string();
        second.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(2),
            token_b: Decimal::ONE,
        });
        runtime.config.corporate_actions = vec![second];
        seed_open_rotation(&mut runtime, second_at);
        let effective = second_at + Duration::seconds(6);
        runtime.step_at(&snapshot_with_valid_row(effective), effective);
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .is_some_and(|p| p.history_invalidated_at.is_some()));
        assert!(runtime.state.relative_log_price_history.is_empty());

        // The operator adds a third entry under the *first* event's id.
        let mut reused = corporate_action_event(anchor + Duration::days(30));
        runtime.config.corporate_actions.push({
            reused.post_event_inventory = None;
            reused
        });
        let later = effective + Duration::seconds(1);
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        assert!(
            matches!(outcome.decision, ArcusSpotDecision::Observe { .. }),
            "{:?}",
            outcome.decision
        );
        assert!(
            runtime.state.relative_log_price_history.is_empty(),
            "the effective window's history suppression must survive the reused id",
        );
        assert_eq!(
            runtime.state.regime,
            ArcusSpotRegime::RotatedAToB,
            "no exit"
        );
    }

    #[test]
    fn renaming_an_active_event_keeps_its_identity_pins() {
        let anchor = event_time();
        let mut runtime =
            ArcusSpotRuntime::new(cfg_with_window_at(anchor + Duration::seconds(1))).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let inside = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(inside), inside);
        let pinned = runtime.state.corporate_action.clone().unwrap();
        assert!(pinned.pre_event_token_a.is_some());

        runtime.config.corporate_actions[0].event_id = "NVDA-2026-10-4FOR1-v2".to_string();
        let resumed_at = anchor + Duration::seconds(14);
        let outcome = runtime.step_at(&snapshot_with_relisted_token_a(resumed_at), resumed_at);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionUnresolved,
                "the rename must not have dropped the pins: {}",
                hold.detail
            ),
            other => panic!("expected the relisting to be caught, got {other:?}"),
        }
        let progress = runtime.state.corporate_action.clone().unwrap();
        assert_eq!(progress.event_id, "NVDA-2026-10-4FOR1-v2");
        assert_eq!(progress.pre_event_token_a, pinned.pre_event_token_a);
        assert_eq!(progress.fingerprint, pinned.fingerprint);
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
    }

    #[test]
    fn a_different_declaration_under_the_same_label_fails_closed() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        let inside = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(inside), inside);
        let before = runtime.state.corporate_action.clone().unwrap();

        // Same id, other instants: not the event this progress was written for.
        runtime.config.corporate_actions[0].effective_at = anchor + Duration::seconds(30);
        runtime.config.corporate_actions[0].resume_not_before = anchor + Duration::seconds(40);
        let later = anchor + Duration::seconds(3);
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::CorporateActionBlock);
                assert!(
                    hold.detail.contains("replaced by a different declaration"),
                    "the ordinary window hold would mean the new declaration was \
                     adopted onto the old record: {}",
                    hold.detail
                );
            }
            other => panic!("expected the replaced window to fail closed, got {other:?}"),
        }
        assert_eq!(
            runtime.state.corporate_action,
            Some(before),
            "progress must survive"
        );
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        // The new declaration's history discard (at its +30s) is not taken
        // on the old record: the window is still the pre-effective one.
        assert!(runtime
            .state
            .corporate_action
            .as_ref()
            .unwrap()
            .history_invalidated_at
            .is_none());
    }

    #[test]
    fn a_renamed_handled_event_is_not_applied_again() {
        let anchor = event_time();
        let mut runtime = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);
        assert_eq!(runtime.state.handled_corporate_action_fingerprints.len(), 1);

        // Later trades move the wallet, and the window refills.
        let traded_to = ArcusSpotInventory {
            token_a: Decimal::from(3),
            token_b: Decimal::from(2),
        };
        runtime.state.inventory = traded_to;
        runtime.state.relative_log_price_history = vec![0.25, 0.26, 0.27];

        // The operator renames the completed entry.
        runtime.config.corporate_actions[0].event_id = "NVDA-2026-10-4FOR1-renamed".to_string();
        let later = resumed_at + Duration::seconds(60);
        runtime.step_at(&snapshot_with_valid_row(later), later);

        assert_eq!(
            runtime.state.inventory, traded_to,
            "post_event_inventory re-applied"
        );
        assert!(
            !runtime.state.relative_log_price_history.is_empty(),
            "window re-discarded"
        );
        assert_eq!(runtime.state.corporate_action, None, "window re-opened");
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);
    }

    #[test]
    fn an_amendment_to_an_earlier_effective_at_makes_units_stale_at_the_new_instant() {
        // The checkpoint's removal scan accepts a *widening* amendment, an
        // earlier `effective_at` among them, so the window stays declared.
        // The stamped progress still carries the superseded fingerprint and
        // cutoff, and reading only those leaves the span between the amended
        // and stored instants looking like valid units -- where `step_at`
        // values the old quantities at post-event prices and can engage a
        // sticky loss halt that then wedges recovery, since the unresolved
        // window blocks trading while `reset-window` and post-cutoff halt
        // clearance both refuse the halt (Codex P1, pairtrade#309).
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor)];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);

        // Open the window; the record copies the declaration's own cutoff.
        let during = anchor + Duration::seconds(1);
        runtime.step_at(&snapshot_with_valid_row(during), during);
        let progress = runtime
            .state
            .corporate_action
            .clone()
            .expect("the window is open");
        assert_eq!(progress.effective_at, Some(anchor + Duration::seconds(4)));
        assert!(progress.history_invalidated_at.is_none());
        assert!(!progress.fingerprint.is_empty());

        // The operator amends it to take effect sooner: same id, and every
        // cutoff moving the way that only widens the guard.
        let amended = anchor + Duration::seconds(3);
        runtime.config.corporate_actions[0].effective_at = amended;
        assert!(
            !runtime
                .config
                .corporate_actions
                .iter()
                .any(|event| ArcusSpotRuntime::progress_matches(&progress, event)),
            "the amendment no longer matches the stamped fingerprint",
        );

        // Past the amended cutoff the venue quotes the new units, whatever
        // the superseded stamp says.
        assert!(runtime.corporate_action_units_are_stale(amended));
        assert!(runtime.corporate_action_units_are_stale(anchor + Duration::seconds(4)));
        // Before it, they are still the old ones.
        assert!(!runtime.corporate_action_units_are_stale(anchor + Duration::seconds(2)));

        // ...and the same holds once the discard has been stamped. The
        // stamp is written when a tick reaches the *stored* cutoff, so a
        // later mark priced between the amended and stored instants would
        // otherwise read as a pre-event mark and let a halt engage from
        // post-amendment prices applied to pre-event quantities (Codex P1,
        // pairtrade#309).
        let past_stored = anchor + Duration::seconds(5);
        runtime.step_at(&snapshot_with_valid_row(past_stored), past_stored);
        let stamped = runtime
            .state
            .corporate_action
            .clone()
            .expect("the window is still open");
        assert!(stamped.history_invalidated_at.is_some(), "discard stamped");
        assert_eq!(
            stamped.effective_at,
            Some(anchor + Duration::seconds(4)),
            "the record still carries the superseded cutoff",
        );
        assert!(runtime.corporate_action_units_are_stale(amended));
        assert!(!runtime.corporate_action_units_are_stale(anchor + Duration::seconds(2)));
    }

    #[test]
    fn deleting_a_live_declaration_does_not_cancel_its_window() {
        // The progress record is the only thing that remembers the
        // pre-event identity and the discarded window, and past
        // `effective_at` the tracked inventory is in units the venue no
        // longer quotes. Dropping it because the operator removed the entry
        // would resume sampling -- and eventually trading -- on that
        // inventory, with the event never reconciled and never handled.
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        let inventory_before = runtime.state.inventory;

        // Open the window and cross `effective_at` (anchor + 4s).
        let during = anchor + Duration::seconds(6);
        runtime.step_at(&snapshot_with_valid_row(during), during);
        assert!(runtime.state.corporate_action.is_some());

        // The operator removes (or renames) the entry.
        runtime.config.corporate_actions.clear();
        let later = anchor + Duration::seconds(20);
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);

        assert!(
            matches!(outcome.decision, ArcusSpotDecision::Observe { .. }),
            "the window stays fail-closed: {:?}",
            outcome.decision,
        );
        // The distinguishing evidence, and what dropping the record would
        // undo: the progress survives and the window is still suppressed,
        // so no post-event print rebuilds the contamination the discard
        // removed. (The surfaced hold is `Warmup` here, as it is for any
        // suppressed window -- there is no signal left for the gate's own
        // hold to outrank.)
        let even_later = anchor + Duration::seconds(40);
        runtime.step_at(&snapshot_with_valid_row(even_later), even_later);
        assert!(runtime.state.corporate_action.is_some(), "progress is kept");
        assert!(runtime.state.relative_log_price_history.is_empty());
        assert_eq!(runtime.state.inventory, inventory_before);
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        // Still stale units, so still no halt derived from them.
        assert!(runtime.corporate_action_units_are_stale(even_later));
    }

    #[test]
    fn retiring_a_declaration_whose_window_never_opened_is_ordinary() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor + Duration::days(30))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert_eq!(runtime.state.corporate_action, None);

        runtime.config.corporate_actions.clear();
        let later = anchor + Duration::seconds(5);
        let outcome = runtime.step_at(&snapshot_with_valid_row(later), later);
        assert!(
            !matches!(
                outcome.decision,
                ArcusSpotDecision::Observe { ref hold }
                    if hold.code == ArcusSpotHoldCode::CorporateActionBlock
            ),
            "nothing was pinned or discarded, so there is nothing to resolve",
        );
        assert_eq!(runtime.state.corporate_action, None);
    }

    #[test]
    fn the_reverse_split_price_may_not_engage_a_loss_halt() {
        // The risk mark reprices the buy-and-hold basket at this tick, so a
        // price move alone cannot halt -- only the *gap* between the wallet
        // and its basket, left by prior rotations, can. From `effective_at`
        // the venue quotes the post-split instrument while both the wallet
        // and its basket are still in pre-split units, so that gap is priced
        // in the wrong denomination: a 1-for-4 reverse split multiplies the
        // same shortfall by four. A halt is sticky, so engaging one here
        // would survive the resume that repairs the units, leave the
        // authorized rebase blocked until an operator cleared it by hand,
        // and make `state-verify-continuity` reject the transition as an
        // unexplained halt (Codex, PR #309).
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        let reconciled = ArcusSpotInventory {
            token_a: Decimal::new(25, 2),
            token_b: Decimal::ONE,
        };
        event.post_event_inventory = Some(reconciled);
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        // A prior rotation left the wallet 0.005 NVDA short of its basket.
        runtime.state.inventory.token_a = Decimal::new(995, 3);

        // Pre-split, that shortfall is $1.00 against the $2 daily limit.
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert_eq!(runtime.state.risk_halt, None);

        // Past `effective_at`, at the post-split price: the same 0.005 NVDA
        // gap is now priced at $800 -- $4.00, twice the limit -- purely
        // because the units are stale.
        let during = anchor + Duration::seconds(6);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(during, "800", "100"),
            during,
        );
        assert_eq!(
            runtime.state.risk_halt, None,
            "the split's own unit change is not a loss",
        );

        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(resumed_at, "800", "100"),
            resumed_at,
        );

        assert_eq!(runtime.state.risk_halt, None);
        assert_eq!(runtime.state.inventory, reconciled);
        assert_eq!(runtime.state.initial_baseline_inventory, Some(reconciled));
        assert_eq!(
            runtime.state.handled_corporate_action_ids,
            vec!["NVDA-2026-10-4FOR1".to_string()],
        );
    }

    #[test]
    fn a_halt_engaged_before_the_effective_time_still_stands() {
        // The suppression is scoped to the ticks whose units are stale. The
        // same shortfall, large enough to breach in the units the venue is
        // still quoting, halts as usual before `effective_at` -- and the
        // halt is sticky, so the resume does not clear it.
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::new(25, 2),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        let basket = runtime.state.inventory;
        runtime.update_risk_baselines(anchor - Duration::seconds(1), Decimal::from(300), basket);
        seed_observed_identities(&mut runtime, anchor);
        // 0.02 NVDA short: $4.00 at the pre-split price, over the $2 limit.
        runtime.state.inventory.token_a = Decimal::new(98, 2);

        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert!(
            runtime.state.risk_halt.is_some(),
            "a real shortfall in the quoted units must still halt",
        );

        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(
            &snapshot_with_valid_row_at_prices(resumed_at, "800", "100"),
            resumed_at,
        );
        assert!(
            runtime.state.risk_halt.is_some(),
            "a halt is sticky; the resume does not clear it",
        );
    }

    #[test]
    fn a_resume_whose_valuation_overflows_stays_pending() {
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        // A representable `Decimal` that cannot be priced: an over-scaled
        // operator quantity is exactly how this arrives in practice.
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::MAX,
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        let inventory_before = runtime.state.inventory;
        let marks_before = (
            runtime.state.initial_equity_usd,
            runtime.state.daily_baseline_equity_usd,
            runtime.state.last_equity_usd,
        );

        let resumed_at = anchor + Duration::seconds(12);
        let outcome = runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);

        // Nothing is committed. A partial resume -- inventory rebased, marks
        // left behind, the id already handled -- would load fine and then
        // fail every later valuation with no way to apply corrected
        // quantities.
        assert_eq!(runtime.state.inventory, inventory_before);
        assert_eq!(
            runtime.state.initial_baseline_inventory,
            Some(inventory_before)
        );
        assert_eq!(
            (
                runtime.state.initial_equity_usd,
                runtime.state.daily_baseline_equity_usd,
                runtime.state.last_equity_usd,
            ),
            marks_before,
        );
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        match outcome.decision {
            ArcusSpotDecision::Observe { hold } => assert_eq!(
                hold.code,
                ArcusSpotHoldCode::CorporateActionResumePending,
                "{}",
                hold.detail
            ),
            other => panic!("expected the resume to stay pending, got {other:?}"),
        }
    }

    #[test]
    fn a_handled_window_is_never_applied_a_second_time() {
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_valid_row(resumed_at), resumed_at);

        // Whatever the runtime trades to afterwards must stand: re-applying
        // the reconciled holding would silently undo every later fill.
        let traded_to = ArcusSpotInventory {
            token_a: Decimal::from(3),
            token_b: Decimal::from(2),
        };
        runtime.state.inventory = traded_to;
        let later = resumed_at + Duration::seconds(1);
        runtime.step_at(&snapshot_with_valid_row(later), later);

        assert_eq!(runtime.state.inventory, traded_to);
        assert_eq!(runtime.state.handled_corporate_action_ids.len(), 1);
        assert_eq!(runtime.state.corporate_action, None);
    }

    #[test]
    fn a_token_relisted_across_the_window_is_left_to_the_operator() {
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor + Duration::seconds(1));
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        // A flat history: this test needs the pre-window tick to observe,
        // not to rotate.
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        // One observation before the window opens, so the guard has a real
        // pre-event identity to pin -- and one inside it, which is the tick
        // that pins it.
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert_eq!(runtime.state.regime, ArcusSpotRegime::Neutral);
        let blocked_at = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(blocked_at), blocked_at);

        let resumed_at = anchor + Duration::seconds(14);
        let event = runtime.step_at(&snapshot_with_relisted_token_a(resumed_at), resumed_at);

        match event.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(
                    hold.code,
                    ArcusSpotHoldCode::CorporateActionUnresolved,
                    "detail={}",
                    hold.detail,
                );
                assert!(
                    hold.detail.to_ascii_lowercase().contains("deadbeef"),
                    "{}",
                    hold.detail,
                );
            }
            other => panic!("expected an unresolved hold, got {other:?}"),
        }
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        assert_eq!(
            runtime.state.inventory,
            config().initial_inventory,
            "a relisted token must not have a reconciled inventory adopted on top of it",
        );
    }

    #[test]
    fn planning_stops_exits_at_the_settlement_cutoff() {
        // The planning gate and the dispatch validator must agree on when
        // exits stop: otherwise live-tick writes a plan its own dispatch
        // rejects, and ReplaySimulation -- which never runs the live-only
        // validator -- records a fill production would not send.
        let anchor = event_time();
        let mut cfg = cfg_with_window_at(anchor);
        cfg.corporate_action_settlement_margin_secs = 1;
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor);

        // reduce_exit_at is +2s and effective_at +4s, so with a 1s margin
        // the last dispatchable tick is +2s.
        let at = anchor + Duration::seconds(2);
        match runtime.step_at(&snapshot_with_valid_row(at), at).decision {
            ArcusSpotDecision::SimulatedFill { plan } => {
                assert_eq!(plan.trigger, ArcusSpotRotationTrigger::CorporateActionExit)
            }
            other => panic!("expected the forced exit while it can still settle, got {other:?}"),
        }

        // At +3s the margin reaches the cutoff: no plan is produced at all --
        // not the forced unwind, and not the max-hold exit an overdue
        // rotation would otherwise take.
        let mut late = ArcusSpotRuntime::new(cfg_with_window_at(anchor)).unwrap();
        late.config.corporate_action_settlement_margin_secs = 1;
        seed_open_rotation(&mut late, anchor - Duration::hours(2));
        let at = anchor + Duration::seconds(3);
        assert!(
            matches!(
                late.step_at(&snapshot_with_valid_row(at), at).decision,
                ArcusSpotDecision::Observe { .. }
            ),
            "no exit may be planned once dispatch would refuse it",
        );
        assert_eq!(late.state.regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn a_rotation_still_open_at_the_effective_time_is_left_to_the_operator() {
        let anchor = event_time();
        let mut cfg = config();
        let mut event = corporate_action_event(anchor);
        let reconciled = ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        };
        event.post_event_inventory = Some(reconciled);
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        seed_open_rotation(&mut runtime, anchor);

        // Reduce phase (+2s to +4s): the exit is forced, and the gate that
        // actually blocked it is the one reported.
        let reduce = anchor + Duration::seconds(2);
        let observed = runtime.step_at(
            &snapshot_with_route_unavailable(reduce, "200", "100"),
            reduce,
        );
        match observed.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::RouteUnavailable)
            }
            other => panic!("expected a hold with no route, got {other:?}"),
        }

        // From `effective_at` the tracked quantity is in pre-event units:
        // one old unit sized as one new unit after a split, an unfillable
        // oversized order after a reverse split. No exit is planned from it,
        // route or no route -- even past the resume time.
        for offset in [6, 13] {
            let at = anchor + Duration::seconds(offset);
            let outcome = runtime.step_at(&snapshot_with_valid_row(at), at);
            match outcome.decision {
                ArcusSpotDecision::Observe { hold } => assert_eq!(
                    hold.code,
                    ArcusSpotHoldCode::CorporateActionUnresolved,
                    "{}",
                    hold.detail
                ),
                other => panic!("expected an unresolved hold at +{offset}s, got {other:?}"),
            }
        }
        assert_eq!(runtime.state.regime, ArcusSpotRegime::RotatedAToB);
        assert!(runtime.state.handled_corporate_action_ids.is_empty());
        assert_eq!(
            runtime.state.inventory,
            config().initial_inventory,
            "the reconciled holding describes a flat wallet, so it must not be adopted \
             over a position the tracked open quantity still refers to",
        );
    }

    #[test]
    fn an_identity_first_seen_inside_the_window_is_not_pinned_as_pre_event() {
        let anchor = event_time();
        // The runtime ticks normally with no calendar, recording an identity
        // whose observation lands *inside* what will later be declared as the
        // window. Installing the calendar afterwards must not let that
        // identity pass as the pre-event side: it would compare the
        // post-event contract against itself and report no drift (Codex P2,
        // pairtrade#309).
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];
        runtime.step_at(&snapshot_with_relisted_token_a(anchor), anchor);
        assert_eq!(runtime.state.last_token_identity_at, Some(anchor));
        assert!(runtime.state.last_token_a_identity.is_some());

        let mut cfg = config();
        let mut event = corporate_action_event(anchor - Duration::seconds(1));
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        });
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::from_state(cfg, runtime.state.clone()).unwrap();

        let blocked_at = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_relisted_token_a(blocked_at), blocked_at);
        let progress = runtime.state.corporate_action.as_ref().unwrap();
        assert_eq!(
            progress.pre_event_token_a,
            None,
            "an identity observed at {anchor} cannot describe the side of a window that opened \
             at {}",
            anchor - Duration::seconds(1),
        );
        assert_eq!(progress.pre_event_token_b, None);

        // With no pre-event side to compare, the resume rests on the
        // reconciled holding alone rather than claiming a drift check it
        // could not make.
        let resumed_at = anchor + Duration::seconds(12);
        runtime.step_at(&snapshot_with_relisted_token_a(resumed_at), resumed_at);
        assert_eq!(
            runtime.state.handled_corporate_action_ids,
            vec!["NVDA-2026-10-4FOR1".to_string()],
        );
    }

    #[test]
    fn a_pre_window_identity_is_pinned_with_its_observation_time() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.corporate_actions = vec![corporate_action_event(anchor + Duration::seconds(1))];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();
        runtime.state.relative_log_price_history = vec![(200.0_f64 / 100.0_f64).ln(); 3];

        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        assert_eq!(runtime.state.last_token_identity_at, Some(anchor));
        assert_eq!(runtime.state.corporate_action, None);

        let blocked_at = anchor + Duration::seconds(2);
        runtime.step_at(&snapshot_with_valid_row(blocked_at), blocked_at);
        let progress = runtime.state.corporate_action.as_ref().unwrap();
        assert_eq!(
            progress
                .pre_event_token_a
                .as_ref()
                .map(|id| id.symbol.as_str()),
            Some("NVDA"),
        );
        assert_eq!(
            progress
                .pre_event_token_b
                .as_ref()
                .map(|id| id.symbol.as_str()),
            Some("AMD"),
        );
    }

    #[test]
    fn a_restart_inside_the_window_resumes_the_same_phase() {
        let anchor = event_time();
        let mut cfg = config();
        cfg.signal_window_samples = 16;
        cfg.corporate_actions = vec![corporate_action_event(anchor)];
        let mut runtime = ArcusSpotRuntime::new(cfg.clone()).unwrap();
        seed_entry_signal_history(&mut runtime);
        runtime.step_at(&snapshot_with_valid_row(anchor), anchor);
        // Cross the effective time, so the discard has already happened.
        let effective_at = anchor + Duration::seconds(4);
        runtime.step_at(&snapshot_with_valid_row(effective_at), effective_at);

        // A checkpoint round-trip, exactly as the store performs it.
        let persisted: ArcusSpotRuntimeState =
            serde_json::from_value(serde_json::to_value(&runtime.state).unwrap()).unwrap();
        let mut restarted = ArcusSpotRuntime::from_state(cfg, persisted).unwrap();
        assert_eq!(
            restarted.state.corporate_action,
            runtime.state.corporate_action
        );

        let next = anchor + Duration::seconds(5);
        let live = runtime.step_at(&snapshot_with_valid_row(next), next);
        let replayed = restarted.step_at(&snapshot_with_valid_row(next), next);
        assert_eq!(
            serde_json::to_value(&live).unwrap(),
            serde_json::to_value(&replayed).unwrap(),
        );
        assert!(restarted.state.relative_log_price_history.is_empty());
        assert_eq!(
            restarted
                .state
                .corporate_action
                .as_ref()
                .unwrap()
                .history_invalidated_at,
            Some(effective_at),
            "the discard must not run a second time after a restart",
        );
    }

    #[test]
    fn a_window_declared_after_the_fact_still_resumes_on_the_reconciled_holding() {
        let anchor = event_time();
        let mut cfg = config();
        // The whole window is already in the past when the operator declares
        // it: the runtime never saw the pre-event side, so there is no token
        // identity to compare and it says so instead of inventing one.
        let mut event = corporate_action_event(anchor - Duration::days(1));
        let reconciled = ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        };
        event.post_event_inventory = Some(reconciled);
        cfg.corporate_actions = vec![event];
        let mut runtime = ArcusSpotRuntime::new(cfg).unwrap();

        let observed = runtime.step_at(&snapshot_with_valid_row(anchor), anchor);

        assert_eq!(runtime.state.inventory, reconciled);
        assert_eq!(
            runtime.state.handled_corporate_action_ids,
            vec!["NVDA-2026-10-4FOR1".to_string()],
        );
        match observed.decision {
            ArcusSpotDecision::Observe { hold } => {
                assert_eq!(hold.code, ArcusSpotHoldCode::Warmup)
            }
            other => panic!("expected warm-up after a retroactive resume, got {other:?}"),
        }
    }
}
