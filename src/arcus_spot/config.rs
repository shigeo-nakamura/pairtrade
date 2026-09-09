use chrono::{DateTime, Utc};
use dex_connector::ArcusSpotPair;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotRuntimeMode {
    /// Evaluate public observations and emit plans without changing inventory.
    #[default]
    ReadOnly,
    /// Apply indicative fills to isolated in-memory inventory for deterministic replay.
    ReplaySimulation,
    /// Emit plans for an external, durable one-shot executor. Runtime state
    /// changes only after apply_confirmed_live_fill reconciles wallet balances.
    #[cfg(feature = "arcus-spot-live")]
    Live,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotInventory {
    /// Human token quantity for pair.sell_symbol.
    pub token_a: Decimal,
    /// Human token quantity for pair.buy_symbol.
    pub token_b: Decimal,
}

impl ArcusSpotInventory {
    pub fn checked_value_usd(
        self,
        token_a_price_usd: Decimal,
        token_b_price_usd: Decimal,
    ) -> Option<Decimal> {
        self.token_a
            .checked_mul(token_a_price_usd)?
            .checked_add(self.token_b.checked_mul(token_b_price_usd)?)
    }
}

/// One operator-supplied corporate-action or token-lifecycle window.
///
/// The recorder payload carries token identity, `addedTimestamp`, prices and
/// financial fields, but no corporate-action calendar, effective time, halt
/// flag or settlement status, so the runtime cannot infer a split, merger,
/// symbol change or redemption on its own (bot-strategy#853). A price-jump
/// threshold cannot substitute: it cannot separate a split from a legitimate
/// gap, and it sees nothing at all for the lifecycle events that leave price
/// untouched. So each window is declared by an operator, from a named
/// authoritative source, and the runtime only ever *applies* it.
///
/// The four timestamps are non-decreasing and mark the phases:
///
/// - `entry_block_at` -- stop opening new rotations. Exits stay available.
/// - `reduce_exit_at` -- if a rotation is still open, exit it (the exit is
///   still subject to every quote, venue, cost, floor, gas, signing and
///   reconciliation gate; a failure holds, it never bypasses one).
/// - `effective_at` -- the pre-event relative-price history stops describing
///   the same instrument, so it is discarded exactly once and no further
///   samples are accumulated until `resume_not_before`.
/// - `resume_not_before` -- the earliest the operator asserts post-event data
///   is trustworthy. Resuming additionally requires unchanged token
///   identity, a flat regime, and `post_event_inventory`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ArcusSpotCorporateActionEvent {
    /// Stable identifier. The runtime records it once handled, so an event
    /// is applied exactly once across restart and replay even if it stays
    /// in the config forever.
    pub event_id: String,
    /// Affected pair symbol(s). Either leg invalidates the *relative* price,
    /// so the window blocks the pair regardless of which side is named; the
    /// list is what the post-event token-identity comparison checks.
    pub symbols: Vec<String>,
    pub entry_block_at: DateTime<Utc>,
    pub reduce_exit_at: DateTime<Utc>,
    pub effective_at: DateTime<Utc>,
    pub resume_not_before: DateTime<Utc>,
    /// Where the operator read this event. Required, and never inferred --
    /// an event with no citable source is a guess, and a guess that blocks
    /// trading or rebases inventory is worse than no guard at all.
    pub source: String,
    /// Wallet holdings the operator verified on-chain at or after
    /// `effective_at`.
    ///
    /// This is the reconciliation step, and it is deliberately a config
    /// field rather than a chain read inside the runtime. A split rewrites
    /// balances with no swap, so the tracked inventory -- which only ever
    /// moves through `apply_confirmed_live_fill` -- is wrong afterwards and
    /// nothing in the recorder snapshot can correct it. Putting the observed
    /// numbers in the config keeps the decision identical between replay and
    /// live-tick (the acceptance criterion this whole guard is measured on),
    /// keeps it under the same administrator-approved policy digest as every
    /// other config change, and leaves the runtime with no privileged input
    /// a replay cannot reproduce. Absent, the runtime holds at
    /// `resume_not_before` rather than resuming on stale quantities.
    #[serde(default)]
    pub post_event_inventory: Option<ArcusSpotInventory>,
}

impl ArcusSpotCorporateActionEvent {
    /// What this event *is*, independent of what it is called: the affected
    /// symbols and the four window instants. `event_id` is the operator's
    /// mutable label; the runtime records both once handled, so renaming a
    /// completed entry cannot turn it back into a new one and re-apply its
    /// `post_event_inventory` over later trades (Codex P1, pairtrade#309).
    /// `source` and `post_event_inventory` are deliberately excluded: the
    /// former is a citation, the latter is the reconciliation an operator
    /// may correct while the event is pending.
    pub fn fingerprint(&self) -> String {
        let mut symbols: Vec<String> = self
            .symbols
            .iter()
            .map(|symbol| symbol.to_ascii_lowercase())
            .collect();
        symbols.sort();
        let canonical = format!(
            "v1|{}|{}|{}|{}|{}",
            symbols.join("+"),
            self.entry_block_at.to_rfc3339(),
            self.reduce_exit_at.to_rfc3339(),
            self.effective_at.to_rfc3339(),
            self.resume_not_before.to_rfc3339(),
        );
        format!("{:x}", Sha256::digest(canonical.as_bytes()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// A key serde does not recognise here is a key that silently does nothing.
// That is tolerable for a retune and not tolerable for `corporate_actions`,
// where a mistyped list is indistinguishable from a declared window that
// simply never engages -- the guard would report nothing and trade straight
// through the event it was installed for. Refusing the config outright at
// install time is the only failure mode that cannot be missed. Verified
// against the live 2026-09-09 `/etc/arcus-spot/config.yaml`, whose
// `runtime:` section carries exactly these keys.
#[serde(deny_unknown_fields)]
pub struct ArcusSpotRuntimeConfig {
    #[serde(default)]
    pub mode: ArcusSpotRuntimeMode,
    pub chain_id: u64,
    pub pair: ArcusSpotPair,
    pub notional_usd: Decimal,
    pub initial_inventory: ArcusSpotInventory,
    pub inventory_floors: ArcusSpotInventory,
    /// Maximum fraction of inventory above the applicable floor that one plan may sell.
    pub max_rotation_fraction: Decimal,
    pub signal_window_samples: usize,
    pub min_signal_samples: usize,
    pub entry_z_score: f64,
    pub exit_z_score: f64,
    pub max_quote_age_secs: i64,
    pub max_hold_secs: i64,
    /// Recorder route loss plus the two explicit buffers must not exceed this.
    pub max_all_in_round_trip_cost_bps: Decimal,
    pub gas_buffer_bps: Decimal,
    pub settlement_buffer_bps: Decimal,
    pub max_inventory_imbalance_fraction: Decimal,
    pub daily_loss_limit_usd: Decimal,
    pub cumulative_loss_limit_usd: Decimal,
    /// Operator-declared corporate-action windows, ordered and
    /// non-overlapping. Empty -- the default, and what every existing
    /// deployment deserializes to -- leaves behaviour exactly as it was.
    #[serde(default)]
    pub corporate_actions: Vec<ArcusSpotCorporateActionEvent>,
    /// How long before a declared `effective_at` the runtime stops
    /// submitting exits.
    ///
    /// Submission is not execution: an order sent seconds before the cutoff
    /// can still be mined after it, selling the pre-event `rotated_quantity`
    /// at the post-event denomination. No amount of re-checking closes that
    /// -- the window is the venue round trip, outside this process -- so the
    /// guard is a margin instead: exits stop this many seconds early and the
    /// reduce phase is expected to be sized accordingly (Codex P1,
    /// pairtrade#309). Defaults to 300s; deployments deserialize without it.
    #[serde(default = "default_corporate_action_settlement_margin_secs")]
    pub corporate_action_settlement_margin_secs: i64,
}

fn default_corporate_action_settlement_margin_secs() -> i64 {
    300
}

impl ArcusSpotRuntimeConfig {
    pub fn normalize(&mut self) {
        self.pair.sell_symbol = self.pair.sell_symbol.trim().to_ascii_uppercase();
        self.pair.buy_symbol = self.pair.buy_symbol.trim().to_ascii_uppercase();
        for event in &mut self.corporate_actions {
            event.event_id = event.event_id.trim().to_string();
            event.source = event.source.trim().to_string();
            for symbol in &mut event.symbols {
                *symbol = symbol.trim().to_ascii_uppercase();
            }
        }
    }

    /// Recorder pairs required to independently cost-gate entries in both
    /// rotation directions. Each CSV row describes its own forward and
    /// reverse cycle, so the reverse entry must start from a separately
    /// requested B-to-A-to-B row rather than reusing the A-to-B-to-A cost.
    pub fn bidirectional_recorder_pairs_csv(&self) -> String {
        format!(
            "{}/{}, {}/{}",
            self.pair.sell_symbol,
            self.pair.buy_symbol,
            self.pair.buy_symbol,
            self.pair.sell_symbol
        )
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.chain_id == 0 {
            return Err("chain_id must be non-zero".to_string());
        }
        if self.pair.sell_symbol.trim().is_empty()
            || self.pair.buy_symbol.trim().is_empty()
            || self
                .pair
                .sell_symbol
                .eq_ignore_ascii_case(&self.pair.buy_symbol)
        {
            return Err("pair must contain two distinct non-empty symbols".to_string());
        }
        if self.notional_usd <= Decimal::ZERO {
            return Err("notional_usd must be positive".to_string());
        }
        validate_inventory("initial_inventory", self.initial_inventory)?;
        validate_inventory("inventory_floors", self.inventory_floors)?;
        if self.inventory_floors.token_a > self.initial_inventory.token_a
            || self.inventory_floors.token_b > self.initial_inventory.token_b
        {
            return Err("inventory floors cannot exceed initial inventory".to_string());
        }
        if self.max_rotation_fraction <= Decimal::ZERO || self.max_rotation_fraction > Decimal::ONE
        {
            return Err("max_rotation_fraction must be in (0, 1]".to_string());
        }
        if self.signal_window_samples < 2 {
            return Err("signal_window_samples must be at least 2".to_string());
        }
        if self.min_signal_samples < 2 || self.min_signal_samples > self.signal_window_samples {
            return Err("min_signal_samples must be in [2, signal_window_samples]".to_string());
        }
        if !self.entry_z_score.is_finite() || self.entry_z_score <= 0.0 {
            return Err("entry_z_score must be finite and positive".to_string());
        }
        if !self.exit_z_score.is_finite()
            || self.exit_z_score < 0.0
            || self.exit_z_score >= self.entry_z_score
        {
            return Err("exit_z_score must be finite and in [0, entry_z_score)".to_string());
        }
        if self.corporate_action_settlement_margin_secs < 0 {
            return Err("corporate_action_settlement_margin_secs must not be negative".to_string());
        }
        if self.max_quote_age_secs <= 0 || self.max_hold_secs <= 0 {
            return Err("quote age and hold limits must be positive".to_string());
        }
        for (name, value) in [
            (
                "max_all_in_round_trip_cost_bps",
                self.max_all_in_round_trip_cost_bps,
            ),
            ("gas_buffer_bps", self.gas_buffer_bps),
            ("settlement_buffer_bps", self.settlement_buffer_bps),
        ] {
            if value < Decimal::ZERO {
                return Err(format!("{name} cannot be negative"));
            }
        }
        if self.max_all_in_round_trip_cost_bps <= Decimal::ZERO {
            return Err("max_all_in_round_trip_cost_bps must be positive".to_string());
        }
        let fixed_buffers = self
            .gas_buffer_bps
            .checked_add(self.settlement_buffer_bps)
            .ok_or("cost buffers exceed Decimal range")?;
        if fixed_buffers > self.max_all_in_round_trip_cost_bps {
            // Named in full because the cap is routinely misread as a limit
            // on the quoted round-trip loss alone: it is compared against
            // that loss *plus* both fixed buffers (bot-strategy#903).
            return Err(format!(
                "gas_buffer_bps + settlement_buffer_bps ({fixed_buffers}) exceed                  max_all_in_round_trip_cost_bps ({}), which is the all-in limit those buffers are                  charged against, leaving no room for any quoted round-trip loss",
                self.max_all_in_round_trip_cost_bps
            ));
        }
        if self.max_inventory_imbalance_fraction < Decimal::ZERO
            || self.max_inventory_imbalance_fraction > Decimal::ONE
        {
            return Err("max_inventory_imbalance_fraction must be in [0, 1]".to_string());
        }
        if self.daily_loss_limit_usd <= Decimal::ZERO
            || self.cumulative_loss_limit_usd <= Decimal::ZERO
        {
            return Err("daily and cumulative loss limits must be positive".to_string());
        }
        self.validate_corporate_actions()?;
        Ok(())
    }

    /// Ordering, uniqueness, pair membership and monotonic timestamps for
    /// `corporate_actions`.
    ///
    /// Windows are required to be strictly ordered *and* disjoint so that at
    /// most one event is ever active. Two genuinely simultaneous events --
    /// say a split in one leg and a symbol change in the other -- are
    /// declared as one window naming both symbols; that is the same guard,
    /// and it keeps "which event is this tick in" from being a question with
    /// two answers.
    fn validate_corporate_actions(&self) -> Result<(), String> {
        let mut previous: Option<&ArcusSpotCorporateActionEvent> = None;
        for event in &self.corporate_actions {
            if event.event_id.trim().is_empty() {
                return Err("corporate_actions entries need a non-empty event_id".to_string());
            }
            if event.source.trim().is_empty() {
                return Err(format!(
                    "corporate action {} needs a source; the runtime never infers an event",
                    event.event_id
                ));
            }
            if self
                .corporate_actions
                .iter()
                .filter(|other| other.event_id.eq_ignore_ascii_case(&event.event_id))
                .count()
                > 1
            {
                return Err(format!(
                    "corporate action event_id {} is not unique",
                    event.event_id
                ));
            }
            if event.symbols.is_empty() {
                return Err(format!(
                    "corporate action {} names no symbol",
                    event.event_id
                ));
            }
            for (index, symbol) in event.symbols.iter().enumerate() {
                if !symbol.eq_ignore_ascii_case(&self.pair.sell_symbol)
                    && !symbol.eq_ignore_ascii_case(&self.pair.buy_symbol)
                {
                    return Err(format!(
                        "corporate action {} names {symbol}, which is not in the configured pair",
                        event.event_id
                    ));
                }
                if event.symbols[..index]
                    .iter()
                    .any(|earlier| earlier.eq_ignore_ascii_case(symbol))
                {
                    return Err(format!(
                        "corporate action {} names {symbol} twice",
                        event.event_id
                    ));
                }
            }
            if !(event.entry_block_at <= event.reduce_exit_at
                && event.reduce_exit_at <= event.effective_at
                && event.effective_at <= event.resume_not_before)
            {
                return Err(format!(
                    "corporate action {} timestamps must be non-decreasing: entry_block_at <= \
                     reduce_exit_at <= effective_at <= resume_not_before",
                    event.event_id
                ));
            }
            // The reduce phase must have a dispatchable interval. Exits are
            // refused within `corporate_action_settlement_margin_secs` of
            // `effective_at` (submission is not execution), so a window whose
            // reduce phase starts inside that margin advertises a forced
            // unwind that can never be submitted -- and an open rotation then
            // reaches `effective_at` and is stranded on
            // `corporate_action_unresolved` (Codex P1, pairtrade#309).
            let margin = chrono::Duration::seconds(self.corporate_action_settlement_margin_secs);
            let last_dispatchable =
                event
                    .reduce_exit_at
                    .checked_add_signed(margin)
                    .ok_or_else(|| {
                        format!(
                            "corporate action {}: reduce_exit_at plus the settlement margin \
                         overflows the representable range",
                            event.event_id
                        )
                    })?;
            if last_dispatchable >= event.effective_at {
                return Err(format!(
                    "corporate action {} leaves no dispatchable reduce phase: reduce_exit_at \
                     ({}) plus corporate_action_settlement_margin_secs ({}s) reaches \
                     effective_at ({}), so a forced unwind could never be submitted and an \
                     open rotation would be stranded. Move reduce_exit_at earlier or lower \
                     the margin",
                    event.event_id,
                    event.reduce_exit_at.to_rfc3339(),
                    self.corporate_action_settlement_margin_secs,
                    event.effective_at.to_rfc3339(),
                ));
            }
            if let Some(previous) = previous {
                if event.entry_block_at <= previous.entry_block_at {
                    return Err(format!(
                        "corporate actions must be ordered by entry_block_at; {} does not follow {}",
                        event.event_id, previous.event_id
                    ));
                }
                if event.entry_block_at <= previous.resume_not_before {
                    return Err(format!(
                        "corporate action {} overlaps {}; declare simultaneous events as one \
                         window naming both symbols",
                        event.event_id, previous.event_id
                    ));
                }
            }
            if let Some(inventory) = event.post_event_inventory {
                validate_inventory(
                    &format!("corporate action {} post_event_inventory", event.event_id),
                    inventory,
                )?;
                // Floors are compared at the resume, where handled state is
                // known: a completed event's historical holding must not
                // invalidate a later floor increase (Codex P2, pairtrade#309).
            }
            previous = Some(event);
        }
        Ok(())
    }
}

fn validate_inventory(name: &str, inventory: ArcusSpotInventory) -> Result<(), String> {
    if inventory.token_a < Decimal::ZERO || inventory.token_b < Decimal::ZERO {
        return Err(format!("{name} quantities cannot be negative"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dex_connector::ArcusSpotRecorderConfig;

    fn valid_config() -> ArcusSpotRuntimeConfig {
        ArcusSpotRuntimeConfig {
            mode: ArcusSpotRuntimeMode::ReadOnly,
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
            signal_window_samples: 20,
            min_signal_samples: 10,
            entry_z_score: 2.0,
            exit_z_score: 0.25,
            max_quote_age_secs: 30,
            max_hold_secs: 86_400,
            corporate_action_settlement_margin_secs:
                default_corporate_action_settlement_margin_secs(),
            max_all_in_round_trip_cost_bps: Decimal::from(100),
            gas_buffer_bps: Decimal::from(5),
            settlement_buffer_bps: Decimal::from(5),
            max_inventory_imbalance_fraction: Decimal::new(8, 1),
            daily_loss_limit_usd: Decimal::from(2),
            cumulative_loss_limit_usd: Decimal::from(10),
            corporate_actions: Vec::new(),
        }
    }

    #[test]
    fn rejects_floor_above_inventory() {
        let mut config = valid_config();
        config.inventory_floors.token_a = Decimal::from(2);
        assert!(config.validate().unwrap_err().contains("floors"));
    }

    #[test]
    fn rejects_buffers_above_cost_limit() {
        let mut config = valid_config();
        config.gas_buffer_bps = Decimal::from(60);
        config.settlement_buffer_bps = Decimal::from(41);
        // The message has to name the all-in comparison, not just "buffers":
        // the cap being charged the buffers *plus* the quoted round-trip loss
        // is exactly what operators misread (bot-strategy#903).
        let error = config.validate().unwrap_err();
        assert!(
            error.contains("gas_buffer_bps + settlement_buffer_bps (101)"),
            "{error}"
        );
        assert!(
            error.contains("max_all_in_round_trip_cost_bps (100), which is the all-in limit"),
            "{error}"
        );
    }

    #[test]
    fn recorder_pairs_cover_both_round_trip_cycles() {
        let config = valid_config();
        assert_eq!(
            config.bidirectional_recorder_pairs_csv(),
            "NVDA/AMD, AMD/NVDA"
        );
        let recorder =
            ArcusSpotRecorderConfig::from_csv(&config.bidirectional_recorder_pairs_csv(), "5")
                .unwrap();
        assert_eq!(recorder.pairs.len(), 2);
        assert_eq!(recorder.pairs[0], config.pair);
        assert_eq!(recorder.pairs[1].sell_symbol, config.pair.buy_symbol);
        assert_eq!(recorder.pairs[1].buy_symbol, config.pair.sell_symbol);
    }

    fn config_with_events(events: Vec<ArcusSpotCorporateActionEvent>) -> ArcusSpotRuntimeConfig {
        let mut config = valid_config();
        config.corporate_actions = events;
        config.normalize();
        config
    }

    fn split_event(event_id: &str, anchor: DateTime<Utc>) -> ArcusSpotCorporateActionEvent {
        ArcusSpotCorporateActionEvent {
            event_id: event_id.to_string(),
            symbols: vec!["NVDA".to_string()],
            entry_block_at: anchor,
            reduce_exit_at: anchor + chrono::Duration::hours(1),
            effective_at: anchor + chrono::Duration::hours(2),
            resume_not_before: anchor + chrono::Duration::hours(3),
            source: "issuer notice".to_string(),
            post_event_inventory: None,
        }
    }

    fn anchor() -> DateTime<Utc> {
        "2026-10-01T00:00:00Z".parse().unwrap()
    }

    #[test]
    fn a_mistyped_corporate_action_key_is_refused_rather_than_ignored() {
        let error = serde_yaml::from_str::<ArcusSpotRuntimeConfig>(
            r#"mode: read_only
chain_id: 4663
pair:
  sell_symbol: NVDA
  buy_symbol: AMD
notional_usd: "5"
initial_inventory: {token_a: "1", token_b: "1"}
inventory_floors: {token_a: "0.1", token_b: "0.1"}
max_rotation_fraction: "1"
signal_window_samples: 20
min_signal_samples: 10
entry_z_score: 2.0
exit_z_score: 0.25
max_quote_age_secs: 30
max_hold_secs: 86400
max_all_in_round_trip_cost_bps: "100"
gas_buffer_bps: "5"
settlement_buffer_bps: "5"
max_inventory_imbalance_fraction: "0.8"
daily_loss_limit_usd: "2"
cumulative_loss_limit_usd: "10"
corporate_action:
  - event_id: a
"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("corporate_action"), "{error}");
    }

    #[test]
    fn an_empty_calendar_is_valid_and_is_the_default() {
        let config = valid_config();
        assert!(config.corporate_actions.is_empty());
        config.validate().unwrap();
    }

    #[test]
    fn accepts_ordered_disjoint_windows() {
        let config = config_with_events(vec![
            split_event("a", anchor()),
            split_event("b", anchor() + chrono::Duration::days(1)),
        ]);
        config.validate().unwrap();
    }

    #[test]
    fn normalize_upper_cases_event_symbols_so_pair_membership_is_comparable() {
        let mut event = split_event("a", anchor());
        event.symbols = vec![" nvda ".to_string()];
        let config = config_with_events(vec![event]);
        assert_eq!(
            config.corporate_actions[0].symbols,
            vec!["NVDA".to_string()]
        );
        config.validate().unwrap();
    }

    #[test]
    fn rejects_a_symbol_outside_the_configured_pair() {
        let mut event = split_event("a", anchor());
        event.symbols = vec!["TSLA".to_string()];
        let error = config_with_events(vec![event]).validate().unwrap_err();
        assert!(error.contains("not in the configured pair"), "{error}");
    }

    #[test]
    fn rejects_a_duplicate_event_id() {
        let error = config_with_events(vec![
            split_event("a", anchor()),
            split_event("A", anchor() + chrono::Duration::days(1)),
        ])
        .validate()
        .unwrap_err();
        assert!(error.contains("not unique"), "{error}");
    }

    #[test]
    fn rejects_a_window_with_no_dispatchable_reduce_phase() {
        // Exits are refused within the settlement margin of effective_at, so
        // a reduce phase that starts inside it advertises a forced unwind
        // that can never be submitted, stranding an open rotation.
        let mut config = valid_config();
        config.corporate_action_settlement_margin_secs = 3600;
        config.corporate_actions = vec![split_event("NVDA-2026-08-SPLIT", anchor())];
        let error = config.validate().unwrap_err();
        assert!(error.contains("no dispatchable reduce phase"), "{error}");

        // An hour of reduce phase with a 30-minute margin is fine.
        config.corporate_action_settlement_margin_secs = 1800;
        config.validate().unwrap();

        // A zero-length reduce phase is refused even with no margin.
        config.corporate_action_settlement_margin_secs = 0;
        config.corporate_actions[0].reduce_exit_at = config.corporate_actions[0].effective_at;
        let error = config.validate().unwrap_err();
        assert!(error.contains("no dispatchable reduce phase"), "{error}");
    }

    #[test]
    fn rejects_out_of_order_timestamps_inside_one_event() {
        let mut event = split_event("a", anchor());
        event.effective_at = event.reduce_exit_at - chrono::Duration::minutes(1);
        let error = config_with_events(vec![event]).validate().unwrap_err();
        assert!(error.contains("non-decreasing"), "{error}");
    }

    #[test]
    fn rejects_unordered_events() {
        let error = config_with_events(vec![
            split_event("a", anchor() + chrono::Duration::days(1)),
            split_event("b", anchor()),
        ])
        .validate()
        .unwrap_err();
        assert!(error.contains("ordered by entry_block_at"), "{error}");
    }

    #[test]
    fn rejects_overlapping_windows() {
        let error = config_with_events(vec![
            split_event("a", anchor()),
            // Opens one hour before "a" resumes.
            split_event("b", anchor() + chrono::Duration::hours(2)),
        ])
        .validate()
        .unwrap_err();
        assert!(error.contains("overlaps"), "{error}");
    }

    #[test]
    fn rejects_an_event_with_no_source() {
        let mut event = split_event("a", anchor());
        event.source = "   ".to_string();
        let error = config_with_events(vec![event]).validate().unwrap_err();
        assert!(error.contains("needs a source"), "{error}");
    }

    #[test]
    fn accepts_a_reconciled_inventory_exactly_at_the_floors() {
        let mut event = split_event("a", anchor());
        let floors = valid_config().inventory_floors;
        event.post_event_inventory = Some(floors);
        config_with_events(vec![event]).validate().unwrap();
    }

    #[test]
    fn rejects_a_negative_reconciled_inventory() {
        let mut event = split_event("a", anchor());
        event.post_event_inventory = Some(ArcusSpotInventory {
            token_a: Decimal::from(-1),
            token_b: Decimal::ONE,
        });
        let error = config_with_events(vec![event]).validate().unwrap_err();
        assert!(error.contains("cannot be negative"), "{error}");
    }
}
