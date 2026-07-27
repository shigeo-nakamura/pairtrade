use super::{ArcusSpotInventory, ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode};
use chrono::{DateTime, Utc};
use dex_connector::{
    ArcusSpotCapture, ArcusSpotOverviewEntry, ArcusSpotRecorderSnapshot, ArcusSpotRoundTripRecord,
    ArcusSpotRouteObservation, ArcusSpotToken,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

const SUPPORTED_RECORDER_SCHEMA_VERSION: u32 = 3;
const PUBLIC_RECORDER_MODE: &str = "public_indicative_read_only";

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
    pub daily_loss_usd: Decimal,
    pub cumulative_loss_usd: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRotationPlan {
    pub direction: ArcusSpotDirection,
    pub trigger: ArcusSpotRotationTrigger,
    pub sell_symbol: String,
    pub buy_symbol: String,
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
    pub last_rotation_at: Option<DateTime<Utc>>,
    pub initial_equity_usd: Option<Decimal>,
    pub daily_baseline_day: Option<String>,
    pub daily_baseline_equity_usd: Option<Decimal>,
    pub risk_halt: Option<ArcusSpotRiskHalt>,
}

impl ArcusSpotRuntimeState {
    fn new(inventory: ArcusSpotInventory) -> Self {
        Self {
            sequence: 0,
            inventory,
            regime: ArcusSpotRegime::Neutral,
            relative_log_price_history: Vec::new(),
            last_rotation_at: None,
            initial_equity_usd: None,
            daily_baseline_day: None,
            daily_baseline_equity_usd: None,
            risk_halt: None,
        }
    }
}

pub struct ArcusSpotRuntime {
    config: ArcusSpotRuntimeConfig,
    state: ArcusSpotRuntimeState,
}

struct SnapshotContext {
    token_a: ArcusSpotToken,
    token_b: ArcusSpotToken,
    token_a_price_usd: Decimal,
    token_b_price_usd: Decimal,
    row: ArcusSpotRoundTripRecord,
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

    pub fn config(&self) -> &ArcusSpotRuntimeConfig {
        &self.config
    }

    pub fn state(&self) -> &ArcusSpotRuntimeState {
        &self.state
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
        self.state.sequence = self.state.sequence.saturating_add(1);
        let sequence = self.state.sequence;
        let inventory_before = self.state.inventory;
        let regime_before = self.state.regime;

        let context = match self.snapshot_context(snapshot) {
            Ok(context) => context,
            Err(hold) => {
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
                })
            }
        };

        let equity_before = match inventory_before
            .checked_value_usd(context.token_a_price_usd, context.token_b_price_usd)
        {
            Some(value) => value,
            None => {
                return self.event(RuntimeEventInput {
                    sequence,
                    observed_at: evaluation_time,
                    inventory_before,
                    regime_before,
                    token_a_reference_price_usd: Some(context.token_a_price_usd),
                    token_b_reference_price_usd: Some(context.token_b_price_usd),
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
        self.update_risk_baselines(evaluation_time, equity_before);
        let risk_before = self.risk_mark(equity_before);
        self.engage_risk_halt(evaluation_time, risk_before);

        let relative_log_price =
            match relative_log_price(context.token_a_price_usd, context.token_b_price_usd) {
                Ok(value) => value,
                Err(detail) => {
                    return self.event(RuntimeEventInput {
                        sequence,
                        observed_at: evaluation_time,
                        inventory_before,
                        regime_before,
                        token_a_reference_price_usd: Some(context.token_a_price_usd),
                        token_b_reference_price_usd: Some(context.token_b_price_usd),
                        relative_log_price: None,
                        z_score: None,
                        risk_before: Some(risk_before),
                        decision: ArcusSpotDecision::Observe {
                            hold: ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail),
                        },
                    })
                }
            };
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

        if let Some(halt) = &self.state.risk_halt {
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

        let Some(z_score) = z_score else {
            return self.event(RuntimeEventInput {
                sequence,
                observed_at: evaluation_time,
                inventory_before,
                regime_before,
                token_a_reference_price_usd: Some(context.token_a_price_usd),
                token_b_reference_price_usd: Some(context.token_b_price_usd),
                relative_log_price: Some(relative_log_price),
                z_score: None,
                risk_before: Some(risk_before),
                decision: ArcusSpotDecision::Observe {
                    hold: ArcusSpotHold::new(
                        ArcusSpotHoldCode::Warmup,
                        format!(
                            "need {} prior samples; have {}",
                            self.config.min_signal_samples,
                            self.state
                                .relative_log_price_history
                                .len()
                                .saturating_sub(1)
                        ),
                    ),
                },
            });
        };

        let Some((direction, trigger)) =
            self.rotation_signal(z_score, evaluation_time, regime_before)
        else {
            return self.event(RuntimeEventInput {
                sequence,
                observed_at: evaluation_time,
                inventory_before,
                regime_before,
                token_a_reference_price_usd: Some(context.token_a_price_usd),
                token_b_reference_price_usd: Some(context.token_b_price_usd),
                relative_log_price: Some(relative_log_price),
                z_score: Some(z_score),
                risk_before: Some(risk_before),
                decision: ArcusSpotDecision::Observe {
                    hold: ArcusSpotHold::new(
                        ArcusSpotHoldCode::NoSignal,
                        format!("z={z_score:.6}, regime={regime_before:?}"),
                    ),
                },
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
                    z_score: Some(z_score),
                    risk_before: Some(risk_before),
                    decision: ArcusSpotDecision::Observe { hold },
                })
            }
        };

        let decision = match self.config.mode {
            ArcusSpotRuntimeMode::ReadOnly => ArcusSpotDecision::WouldRotate { plan },
            ArcusSpotRuntimeMode::ReplaySimulation => {
                self.state.inventory = plan.predicted_inventory;
                match trigger {
                    ArcusSpotRotationTrigger::EntrySignal => {
                        self.state.regime = match direction {
                            ArcusSpotDirection::TokenAToTokenB => ArcusSpotRegime::RotatedAToB,
                            ArcusSpotDirection::TokenBToTokenA => ArcusSpotRegime::RotatedBToA,
                        };
                        self.state.last_rotation_at = Some(evaluation_time);
                    }
                    ArcusSpotRotationTrigger::MeanReversionExit
                    | ArcusSpotRotationTrigger::MaxHoldExit => {
                        self.state.regime = ArcusSpotRegime::Neutral;
                        self.state.last_rotation_at = None;
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
        let risk_after = self.risk_mark(equity_after);
        self.engage_risk_halt(evaluation_time, risk_after);
        self.event(RuntimeEventInput {
            sequence,
            observed_at: evaluation_time,
            inventory_before,
            regime_before,
            token_a_reference_price_usd: Some(context.token_a_price_usd),
            token_b_reference_price_usd: Some(context.token_b_price_usd),
            relative_log_price: Some(relative_log_price),
            z_score: Some(z_score),
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
                    .map(|equity| self.risk_mark(equity))
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

    fn snapshot_context(
        &self,
        snapshot: &ArcusSpotRecorderSnapshot,
    ) -> Result<SnapshotContext, ArcusSpotHold> {
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
        let overview = capture_payload(&snapshot.reference_overview, "reference_overview")?;
        let token_a_price_usd = find_reference_price(overview, &token_a)?;
        let token_b_price_usd = find_reference_price(overview, &token_b)?;

        let matching_rows = snapshot
            .round_trips
            .iter()
            .filter(|row| {
                row.pair
                    .sell_symbol
                    .eq_ignore_ascii_case(&self.config.pair.sell_symbol)
                    && row
                        .pair
                        .buy_symbol
                        .eq_ignore_ascii_case(&self.config.pair.buy_symbol)
                    && Decimal::from_str(&row.notional_usd)
                        .is_ok_and(|notional| notional == self.config.notional_usd)
            })
            .collect::<Vec<_>>();
        if matching_rows.len() != 1 {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::RouteUnavailable,
                format!(
                    "expected one {}/{} row at USD {}; found {}",
                    self.config.pair.sell_symbol,
                    self.config.pair.buy_symbol,
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
            "token A",
            row.sell_reference_price_usd.as_deref(),
            token_a_price_usd,
        )?;
        validate_recorded_reference(
            "token B",
            row.buy_reference_price_usd.as_deref(),
            token_b_price_usd,
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

        Ok(SnapshotContext {
            token_a,
            token_b,
            token_a_price_usd,
            token_b_price_usd,
            row: row.clone(),
        })
    }

    fn rotation_signal(
        &self,
        z_score: f64,
        evaluation_time: DateTime<Utc>,
        regime: ArcusSpotRegime,
    ) -> Option<(ArcusSpotDirection, ArcusSpotRotationTrigger)> {
        match regime {
            ArcusSpotRegime::Neutral => {
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
                    Some((
                        ArcusSpotDirection::TokenBToTokenA,
                        ArcusSpotRotationTrigger::MaxHoldExit,
                    ))
                } else if z_score <= self.config.exit_z_score {
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
                    Some((
                        ArcusSpotDirection::TokenAToTokenB,
                        ArcusSpotRotationTrigger::MaxHoldExit,
                    ))
                } else if z_score >= -self.config.exit_z_score {
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
        let route_loss = parse_positive_or_zero(
            "optimistic_round_trip_loss_bps",
            context.row.optimistic_round_trip_loss_bps.as_deref(),
        )?;
        let all_in_cost = route_loss
            .checked_add(self.config.gas_buffer_bps)
            .and_then(|cost| cost.checked_add(self.config.settlement_buffer_bps))
            .ok_or_else(|| {
                ArcusSpotHold::new(
                    ArcusSpotHoldCode::CostLimit,
                    "all-in route cost exceeds Decimal range",
                )
            })?;
        if all_in_cost > self.config.max_all_in_round_trip_cost_bps {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::CostLimit,
                format!(
                    "all-in round-trip cost {} bps exceeds {} bps",
                    all_in_cost, self.config.max_all_in_round_trip_cost_bps
                ),
            ));
        }

        let (route, sell_token, buy_token, sell_balance, sell_floor) = match direction {
            ArcusSpotDirection::TokenAToTokenB => (
                context.row.forward.as_ref().expect("validated forward"),
                &context.token_a,
                &context.token_b,
                inventory.token_a,
                self.config.inventory_floors.token_a,
            ),
            ArcusSpotDirection::TokenBToTokenA => (
                context.row.reverse.as_ref().expect("validated reverse"),
                &context.token_b,
                &context.token_a,
                inventory.token_b,
                self.config.inventory_floors.token_b,
            ),
        };
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
        let max_quote_age_ms = self.config.max_quote_age_secs.saturating_mul(1_000);
        if quote_age_ms > max_quote_age_ms {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::StaleQuote,
                format!(
                    "quote age {}ms exceeds {}ms",
                    quote_age_ms, max_quote_age_ms
                ),
            ));
        }

        let sell_quantity = raw_amount_to_quantity(&route.sell_amount, sell_token.decimals)
            .map_err(|detail| ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail))?;
        let buy_quantity = raw_amount_to_quantity(&quote.buy_amount, buy_token.decimals)
            .map_err(|detail| ArcusSpotHold::new(ArcusSpotHoldCode::InvalidSnapshot, detail))?;
        let sellable = sell_balance.checked_sub(sell_floor).ok_or_else(|| {
            ArcusSpotHold::new(
                ArcusSpotHoldCode::InventoryFloor,
                "sell balance is below its configured floor",
            )
        })?;
        if sell_quantity > sellable {
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InventoryFloor,
                format!(
                    "selling {} {} would cross floor {}; balance={}",
                    sell_quantity, sell_token.symbol, sell_floor, sell_balance
                ),
            ));
        }
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
            return Err(ArcusSpotHold::new(
                ArcusSpotHoldCode::InventoryImbalance,
                format!(
                    "predicted USD inventory imbalance {} exceeds {}",
                    imbalance, self.config.max_inventory_imbalance_fraction
                ),
            ));
        }

        Ok(ArcusSpotRotationPlan {
            direction,
            trigger,
            sell_symbol: sell_token.symbol.clone(),
            buy_symbol: buy_token.symbol.clone(),
            sell_quantity,
            buy_quantity,
            sell_amount_raw: route.sell_amount.clone(),
            buy_amount_raw: quote.buy_amount.clone(),
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

    fn update_risk_baselines(&mut self, at: DateTime<Utc>, equity_usd: Decimal) {
        if self.state.initial_equity_usd.is_none() {
            self.state.initial_equity_usd = Some(equity_usd);
        }
        let day = at.format("%Y-%m-%d").to_string();
        if self.state.daily_baseline_day.as_deref() != Some(day.as_str()) {
            self.state.daily_baseline_day = Some(day);
            self.state.daily_baseline_equity_usd = Some(equity_usd);
        }
    }

    fn risk_mark(&self, equity_usd: Decimal) -> ArcusSpotRiskMark {
        ArcusSpotRiskMark {
            equity_usd,
            daily_loss_usd: positive_loss(self.state.daily_baseline_equity_usd, equity_usd),
            cumulative_loss_usd: positive_loss(self.state.initial_equity_usd, equity_usd),
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

fn raw_amount_to_quantity(raw: &str, decimals: u32) -> Result<Decimal, String> {
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

fn z_score(history: &[f64], current: f64, minimum_samples: usize) -> Option<f64> {
    if history.len() < minimum_samples {
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
    if !standard_deviation.is_finite() || standard_deviation <= 1e-12 {
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
        }
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
    fn loss_halt_is_sticky() {
        let mut runtime = ArcusSpotRuntime::new(config()).unwrap();
        runtime.update_risk_baselines(event_time(), Decimal::from(300));
        let mark = runtime.risk_mark(Decimal::from(297));
        runtime.engage_risk_halt(event_time(), mark);
        let halt = runtime.state.risk_halt.clone().unwrap();
        assert_eq!(halt.kind, ArcusSpotRiskHaltKind::DailyLoss);

        runtime.engage_risk_halt(
            event_time() + Duration::seconds(1),
            ArcusSpotRiskMark {
                equity_usd: Decimal::from(100),
                daily_loss_usd: Decimal::from(200),
                cumulative_loss_usd: Decimal::from(200),
            },
        );
        assert_eq!(runtime.state.risk_halt.unwrap(), halt);
    }

    #[test]
    fn current_tick_is_scored_against_prior_history_only() {
        let history = [1.0, 1.1];
        let score = z_score(&history, 1.5, 2).unwrap();
        assert!(score > 5.0);
        assert_eq!(history, [1.0, 1.1]);
    }

    #[test]
    fn raw_amount_conversion_is_exact() {
        assert_eq!(
            raw_amount_to_quantity("23969319271332694", 18).unwrap(),
            Decimal::from_str("0.023969319271332694").unwrap()
        );
        assert!(raw_amount_to_quantity("0", 18).is_err());
    }
}
