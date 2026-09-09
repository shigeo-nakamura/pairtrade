//! Durable checkpoint for `ArcusSpotRuntime` state (bot-strategy#772,
//! pairtrade#181). Shared by every binary that needs the runtime's current
//! signal window / regime / risk state to survive a restart: the live
//! executor (`arcus-spot-execute-once`) and the live plan proposer
//! (`arcus-spot-propose-plan`) both read and write the same checkpoint file
//! at `runtime_state_path`, so this is the single implementation of its
//! atomic-write and validated-restore logic rather than two independently
//! maintained copies of it.

use super::{
    runtime::{
        backfill_handled_corporate_action_fingerprints, handled_corporate_action_record,
        HandledMatch,
    },
    ArcusSpotInventory, ArcusSpotRegime, ArcusSpotRiskHalt, ArcusSpotRuntime,
    ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode, ArcusSpotRuntimeState,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use dex_connector::ArcusSpotPair;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

const RUNTIME_CHECKPOINT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct ArcusSpotRuntimeCheckpoint {
    schema_version: u32,
    config: ArcusSpotRuntimeConfig,
    state: ArcusSpotRuntimeState,
}

/// What a checkpoint says about itself, read without comparing it against
/// any config (see `peek_summary`).
#[derive(Debug, Clone, PartialEq)]
pub struct ArcusSpotCheckpointSummary {
    /// The pair and mode the stored state was accumulated under -- the two
    /// state-invalidating fields an operator inspecting a checkpoint before
    /// resetting it actually needs to see, since the checkpoint's own copy
    /// of the config is otherwise private to this module.
    pub pair: ArcusSpotPair,
    pub mode: ArcusSpotRuntimeMode,
    pub sequence: u64,
    pub regime: ArcusSpotRegime,
    pub rotated_quantity: Option<Decimal>,
    pub risk_halt: Option<ArcusSpotRiskHalt>,
    pub relative_log_price_samples: usize,
    /// What the bot is actually holding, as reconciled fills left it --
    /// not what the config declared at funding. `reset-window` compares
    /// the two, because building a fresh runtime takes the *declared*
    /// figure and would otherwise overwrite realized trading deltas
    /// (Codex P1 follow-up, bot-strategy#903).
    pub inventory: ArcusSpotInventory,
    /// Corporate actions this state already resumed from. A fresh window
    /// built by `reset-window` must carry these forward: the completed
    /// declarations may still be in the config, and a fresh state that has
    /// forgotten them treats each as unhandled, re-applies its obsolete
    /// `post_event_inventory` over the newly declared holding and clears
    /// the window again (Codex P1, pairtrade#309).
    pub handled_corporate_action_ids: Vec<String>,
    pub handled_corporate_action_fingerprints: Vec<String>,
    /// The corporate-action window the state is currently inside, if any.
    /// `reset-window` refuses to discard it (Codex P1, pairtrade#309).
    pub corporate_action_event_id: Option<String>,
    /// The symbols that window is about (empty when the record predates
    /// the field).
    pub corporate_action_symbols: Vec<String>,
    /// The observation watermark the stored state reached. `reset-window`
    /// carries the handled record forward and resolves any legacy entry
    /// against it (Codex P2, pairtrade#309).
    pub last_observation_at: Option<DateTime<Utc>>,
}

/// How a config change since the checkpoint was written relates to the state
/// stored alongside it, split by whether the stored state still *means* what
/// it meant when written (bot-strategy#809).
#[derive(Debug, Default, PartialEq, Eq)]
struct ArcusSpotCheckpointConfigDrift {
    /// Fields whose change makes the stored state describe something else:
    /// reusing it would silently reinterpret an accumulated signal window,
    /// regime, or inventory under assumptions it was never built under.
    state_invalidating: Vec<&'static str>,
    /// Fields that only re-aim future decisions. No stored value's meaning
    /// depends on them, so the state carries over unchanged.
    state_preserving: Vec<&'static str>,
}

/// Classifies every difference between the config a checkpoint was written
/// under and the config being loaded now.
///
/// This is deliberately *not* an authorization check. By the time any live
/// path reaches the checkpoint, the whole config has already been
/// authenticated: `execute`/`resume` verify an offline Ed25519 signature over
/// config+plan, and `auto-execute`/`auto-resume`/`live-tick` verify the
/// administrator-owned whole-config sha256 pin in
/// `/etc/arcus-spot/auto_execute_policy.json`. Those are the gates that decide
/// *whether this config may run at all*; the runtime also always executes
/// against that authenticated config rather than the checkpoint's stored copy,
/// which serves only as the witness of what the state was built under. What is
/// left for this function is the separate question of *state coherence*: given
/// that the new config is legitimate, does the accumulated state still describe
/// it?
///
/// Treating those two questions as one (a byte-for-byte `!=` on the whole
/// struct) meant every approved tuning change also demanded discarding the
/// signal window, regime, and risk baselines -- on the live probe that cost
/// days of warmup to move one forward-looking cap, which is what
/// bot-strategy#809 was filed about.
///
/// The exhaustive destructuring below has no `..` rest pattern, so adding a
/// field to `ArcusSpotRuntimeConfig` fails to compile until it is explicitly
/// classified here. That is what keeps "classify every field correctly" from
/// degrading into "classify every field that existed when this was written".
fn classify_config_drift(
    stored: &ArcusSpotRuntimeConfig,
    current: &ArcusSpotRuntimeConfig,
) -> ArcusSpotCheckpointConfigDrift {
    let ArcusSpotRuntimeConfig {
        mode: stored_mode,
        chain_id: stored_chain_id,
        pair: stored_pair,
        notional_usd: stored_notional_usd,
        initial_inventory: stored_initial_inventory,
        inventory_floors: stored_inventory_floors,
        max_rotation_fraction: stored_max_rotation_fraction,
        signal_window_samples: stored_signal_window_samples,
        min_signal_samples: stored_min_signal_samples,
        entry_z_score: stored_entry_z_score,
        exit_z_score: stored_exit_z_score,
        max_quote_age_secs: stored_max_quote_age_secs,
        max_hold_secs: stored_max_hold_secs,
        max_all_in_round_trip_cost_bps: stored_max_all_in_round_trip_cost_bps,
        gas_buffer_bps: stored_gas_buffer_bps,
        settlement_buffer_bps: stored_settlement_buffer_bps,
        max_inventory_imbalance_fraction: stored_max_inventory_imbalance_fraction,
        daily_loss_limit_usd: stored_daily_loss_limit_usd,
        cumulative_loss_limit_usd: stored_cumulative_loss_limit_usd,
        corporate_actions: stored_corporate_actions,
        corporate_action_settlement_margin_secs: stored_corporate_action_settlement_margin_secs,
    } = stored;
    let ArcusSpotRuntimeConfig {
        mode: current_mode,
        chain_id: current_chain_id,
        pair: current_pair,
        notional_usd: current_notional_usd,
        initial_inventory: current_initial_inventory,
        inventory_floors: current_inventory_floors,
        max_rotation_fraction: current_max_rotation_fraction,
        signal_window_samples: current_signal_window_samples,
        min_signal_samples: current_min_signal_samples,
        entry_z_score: current_entry_z_score,
        exit_z_score: current_exit_z_score,
        max_quote_age_secs: current_max_quote_age_secs,
        max_hold_secs: current_max_hold_secs,
        max_all_in_round_trip_cost_bps: current_max_all_in_round_trip_cost_bps,
        gas_buffer_bps: current_gas_buffer_bps,
        settlement_buffer_bps: current_settlement_buffer_bps,
        max_inventory_imbalance_fraction: current_max_inventory_imbalance_fraction,
        daily_loss_limit_usd: current_daily_loss_limit_usd,
        cumulative_loss_limit_usd: current_cumulative_loss_limit_usd,
        corporate_actions: current_corporate_actions,
        corporate_action_settlement_margin_secs: current_corporate_action_settlement_margin_secs,
    } = current;

    let mut drift = ArcusSpotCheckpointConfigDrift::default();

    // -- State-invalidating -------------------------------------------------
    // `mode` decides how inventory moves at all: replay applies indicative
    // fills to in-memory inventory, live only ever moves it after an on-chain
    // fill reconciles. Carrying one's state into the other reinterprets every
    // inventory number in it.
    if stored_mode != current_mode {
        drift.state_invalidating.push("mode");
    }
    // A different chain is a different pair of token contracts behind the same
    // symbols, so neither the price history nor the inventory refers to the
    // same assets.
    if stored_chain_id != current_chain_id {
        drift.state_invalidating.push("chain_id");
    }
    // `relative_log_price_history` is the log price ratio of exactly this
    // pair, and `inventory.token_a`/`token_b` are denominated in its two
    // symbols. Both become meaningless under a different pair.
    if stored_pair != current_pair {
        drift.state_invalidating.push("pair");
    }
    // The funding baseline the tracked inventory and `initial_equity_usd`
    // descend from. Changing it (e.g. after an operator tops the wallet up)
    // means the stored inventory no longer traces to the declared start, so
    // the state has to be rebuilt from the new baseline.
    if stored_initial_inventory != current_initial_inventory {
        drift.state_invalidating.push("initial_inventory");
    }
    // The window length is part of the definition of the z-score the stored
    // history feeds. `step_at` only drains an over-long history on its next
    // push, so a shrink would score at least one tick over a window wider
    // than the one now configured.
    if stored_signal_window_samples != current_signal_window_samples {
        drift.state_invalidating.push("signal_window_samples");
    }

    // -- State-preserving ---------------------------------------------------
    // Everything below re-aims future decisions only. Each is still reported
    // so an adoption is never silent, and each is still gated by the
    // administrator approval described above.
    if stored_notional_usd != current_notional_usd {
        drift.state_preserving.push("notional_usd");
    }
    // Raising a floor above the currently tracked inventory is not silently
    // absorbed here: `ArcusSpotRuntime::from_state` independently rejects a
    // restored inventory below any configured floor, with a message that
    // names the real problem.
    if stored_inventory_floors != current_inventory_floors {
        drift.state_preserving.push("inventory_floors");
    }
    if stored_max_rotation_fraction != current_max_rotation_fraction {
        drift.state_preserving.push("max_rotation_fraction");
    }
    // Only gates when scoring may begin; it reweights nothing already stored.
    if stored_min_signal_samples != current_min_signal_samples {
        drift.state_preserving.push("min_signal_samples");
    }
    // Exact inequality is the intent here -- any change at all is reported,
    // and `validate` has already rejected non-finite thresholds.
    if stored_entry_z_score != current_entry_z_score {
        drift.state_preserving.push("entry_z_score");
    }
    if stored_exit_z_score != current_exit_z_score {
        drift.state_preserving.push("exit_z_score");
    }
    if stored_max_quote_age_secs != current_max_quote_age_secs {
        drift.state_preserving.push("max_quote_age_secs");
    }
    // Compared against `last_rotation_at`, whose meaning ("when the open
    // rotation started") is unchanged; shortening it can only bring a
    // risk-reducing max-hold exit forward.
    if stored_max_hold_secs != current_max_hold_secs {
        drift.state_preserving.push("max_hold_secs");
    }
    if stored_max_all_in_round_trip_cost_bps != current_max_all_in_round_trip_cost_bps {
        drift
            .state_preserving
            .push("max_all_in_round_trip_cost_bps");
    }
    if stored_gas_buffer_bps != current_gas_buffer_bps {
        drift.state_preserving.push("gas_buffer_bps");
    }
    if stored_settlement_buffer_bps != current_settlement_buffer_bps {
        drift.state_preserving.push("settlement_buffer_bps");
    }
    if stored_max_inventory_imbalance_fraction != current_max_inventory_imbalance_fraction {
        drift
            .state_preserving
            .push("max_inventory_imbalance_fraction");
    }
    // The stored risk baselines keep their meaning; only the threshold they
    // are compared against moves. An already-engaged halt records the limit
    // it fired on in its own state, so its history stays accurate.
    if stored_daily_loss_limit_usd != current_daily_loss_limit_usd {
        drift.state_preserving.push("daily_loss_limit_usd");
    }
    if stored_cumulative_loss_limit_usd != current_cumulative_loss_limit_usd {
        drift.state_preserving.push("cumulative_loss_limit_usd");
    }
    // Declaring, amending or reconciling a corporate-action window must not
    // require a window reset: the whole point of the calendar is that an
    // operator can add the event they just learned about while the runtime
    // keeps marking, keeps risk-accounting, and keeps the ability to exit.
    // The one state change a window does make -- discarding the pre-event
    // signal history -- is performed by the guard itself, once, at the
    // declared effective time, and recorded in the checkpoint; a blanket
    // reset here would instead discard it at config-install time, which is
    // the wrong moment and would also drop the regime and risk baselines
    // that the forced exit still needs.
    // How early exits stop before a declared cutoff. A pure forward-looking
    // guard: it re-aims the next dispatch decision and reinterprets nothing
    // that is stored.
    if stored_corporate_action_settlement_margin_secs
        != current_corporate_action_settlement_margin_secs
    {
        drift
            .state_preserving
            .push("corporate_action_settlement_margin_secs");
    }
    if stored_corporate_actions != current_corporate_actions {
        drift.state_preserving.push("corporate_actions");
    }

    drift
}

/// A private, administrator-legible regular file, rejecting symlinks and
/// group/other access. Every caller of this store deals in either the
/// runtime checkpoint itself or the config used to validate it, both of
/// which describe live trading state and must never be world-readable.
fn read_private_regular_file(path: &Path, label: &str) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        );
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "{label} {} must not be readable or writable by group/other",
            path.display()
        );
    }
    fs::read(path).with_context(|| format!("failed to read {label} {}", path.display()))
}

pub struct ArcusSpotRuntimeCheckpointStore {
    path: PathBuf,
}

impl ArcusSpotRuntimeCheckpointStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn load_or_create(&self, config: &ArcusSpotRuntimeConfig) -> Result<ArcusSpotRuntime> {
        if !self.path.exists() {
            return ArcusSpotRuntime::new(config.clone()).map_err(anyhow::Error::msg);
        }
        self.load_existing(config)
    }

    /// Read just the persisted regime and open rotation quantity, `None` if
    /// no checkpoint exists yet. Unlike `load_existing`/`load_or_create`,
    /// this does not compare the checkpoint against any config (so it does
    /// nothing on a state-preserving config change -- see
    /// `classify_config_drift` -- and cannot itself detect a
    /// state-invalidating one) or construct a runtime; it exists for a
    /// caller that only needs to decide, cheaply and without the
    /// once-per-tick drift-check log noise a full load produces, whether a
    /// snapshot collector should request an exit-sized quote before the
    /// locked, config-validated load that the actual decision is made
    /// against (bot-strategy#906: live-tick's snapshot fetch happens before
    /// it takes the checkpoint lock, so this peek necessarily precedes that
    /// lock too).
    pub fn peek_regime_and_rotated_quantity(
        &self,
    ) -> Result<Option<(ArcusSpotRegime, Option<Decimal>)>> {
        Ok(self
            .peek_summary()?
            .map(|summary| (summary.regime, summary.rotated_quantity)))
    }

    /// The same config-independent read, reported in full. `reset-window`
    /// needs it because the whole point of a reset is that the current
    /// config no longer describes the stored state, so `load_existing`
    /// refuses to read it at all -- and the checks that decide whether a
    /// reset is safe (flat regime, no open rotation, no engaged halt) are
    /// about what the *stored* state says, not what the new config would
    /// make of it (bot-strategy#903).
    pub fn peek_summary(&self) -> Result<Option<ArcusSpotCheckpointSummary>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = read_private_regular_file(&self.path, "runtime checkpoint")?;
        let checkpoint: ArcusSpotRuntimeCheckpoint = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid runtime checkpoint {}", self.path.display()))?;
        if checkpoint.schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION {
            bail!("unsupported Arcus runtime checkpoint schema");
        }
        Ok(Some(ArcusSpotCheckpointSummary {
            pair: checkpoint.config.pair,
            mode: checkpoint.config.mode,
            sequence: checkpoint.state.sequence,
            regime: checkpoint.state.regime,
            rotated_quantity: checkpoint.state.rotated_quantity,
            risk_halt: checkpoint.state.risk_halt,
            relative_log_price_samples: checkpoint.state.relative_log_price_history.len(),
            handled_corporate_action_ids: checkpoint.state.handled_corporate_action_ids.clone(),
            handled_corporate_action_fingerprints: checkpoint
                .state
                .handled_corporate_action_fingerprints
                .clone(),
            inventory: checkpoint.state.inventory,
            corporate_action_event_id: checkpoint
                .state
                .corporate_action
                .as_ref()
                .map(|progress| progress.event_id.clone()),
            last_observation_at: checkpoint.state.last_observation_at,
            corporate_action_symbols: checkpoint
                .state
                .corporate_action
                .as_ref()
                .map(|progress| progress.symbols.clone())
                .unwrap_or_default(),
        }))
    }

    /// Which state-invalidating fields differ between the config the
    /// checkpoint was written under and `current`. `None` when no
    /// checkpoint exists.
    ///
    /// `reset-window` needs this to hold itself to its own purpose. A reset
    /// discards the accumulated signal window and re-anchors the risk
    /// baselines -- `initial_equity_usd` and the buy-and-hold basket are
    /// re-marked on the next tick, so cumulative-loss accounting starts
    /// over. That is acceptable precisely *because* it accompanies a change
    /// that already invalidated the stored state. Run with an unchanged
    /// config it is not a reset at all, just an erasure of the loss
    /// accounting the cumulative halt is measured against -- repeatable
    /// before the limit ever engages (Codex P1 follow-up,
    /// bot-strategy#903).
    pub fn state_invalidating_drift(
        &self,
        current: &ArcusSpotRuntimeConfig,
    ) -> Result<Option<Vec<&'static str>>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = read_private_regular_file(&self.path, "runtime checkpoint")?;
        let checkpoint: ArcusSpotRuntimeCheckpoint = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid runtime checkpoint {}", self.path.display()))?;
        if checkpoint.schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION {
            bail!("unsupported Arcus runtime checkpoint schema");
        }
        Ok(Some(
            classify_config_drift(&checkpoint.config, current).state_invalidating,
        ))
    }

    /// Load and validate an already-persisted checkpoint without creating
    /// or modifying anything. Operator backup/rollback checks must never
    /// turn a missing checkpoint into a successful first-run state: absence
    /// is precisely the reset condition those checks are meant to detect.
    pub fn load_existing(&self, config: &ArcusSpotRuntimeConfig) -> Result<ArcusSpotRuntime> {
        self.load_existing_at(config, Utc::now())
    }

    /// `load_existing` with the clock supplied. Window liveness is judged
    /// against *now*, not against the checkpoint's own watermark: the
    /// watermark does not advance while the bot is down, so a process that
    /// was stopped before `entry_block_at` and started after it would
    /// otherwise accept a config that dropped an already-open window (Codex
    /// P1, pairtrade#309).
    pub fn load_existing_at(
        &self,
        config: &ArcusSpotRuntimeConfig,
        now: DateTime<Utc>,
    ) -> Result<ArcusSpotRuntime> {
        if !self.path.exists() {
            bail!(
                "Arcus runtime checkpoint {} does not exist",
                self.path.display()
            );
        }
        let bytes = read_private_regular_file(&self.path, "runtime checkpoint")?;
        let checkpoint: ArcusSpotRuntimeCheckpoint = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid runtime checkpoint {}", self.path.display()))?;
        if checkpoint.schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION {
            bail!("unsupported Arcus runtime checkpoint schema");
        }
        // `corporate_actions` drift is state-preserving, but *removing* a
        // window the stored config declared is not a tuning change: it drops
        // a live guard. Before any tick has written progress there is
        // nothing else for the runtime to fail closed on, so an older but
        // still-validly-signed config could be loaded and dispatch an entry
        // or a stale-unit exit straight through the cutoff. Retiring a
        // declaration whose window had not opened by the last observation is
        // still ordinary housekeeping (Codex P1, pairtrade#309).
        // The handled record is resolved the same way `from_state` will
        // resolve it, so "already handled" reads identically here and in the
        // runtime this load is about to build.
        let mut resolved = checkpoint.state.clone();
        backfill_handled_corporate_action_fingerprints(&mut resolved, &checkpoint.config);
        let live_by = checkpoint
            .state
            .last_observation_at
            .map(|observed_at| observed_at.max(now))
            .unwrap_or(now);
        // The window need not have opened yet: this load builds the runtime
        // that a submit guard closes over, and quote/preflight/signing can
        // cross `entry_block_at` before the order goes out -- at which point
        // the guard cannot see a declaration the stripped config never had.
        // The margin that bounds how long a submission takes is the same one
        // exits already stop for, so removal is only housekeeping for a
        // window that cannot open within it (Codex P1, pairtrade#309).
        let submit_margin = Duration::seconds(
            checkpoint
                .config
                .corporate_action_settlement_margin_secs
                .max(config.corporate_action_settlement_margin_secs),
        );
        let live_by = live_by + submit_margin;
        if let Some(dropped) = checkpoint.config.corporate_actions.iter().find(|stored| {
            live_by >= stored.entry_block_at
                // "Handled" by the same rule a tick applies: an id whose
                // recorded fingerprint belongs to a *different* event is a
                // reused label, not a completed window, and dropping it
                // would remove a guard that was never resolved.
                && !matches!(
                    handled_corporate_action_record(&resolved, stored),
                    Some(HandledMatch::Same)
                )
                && !config
                    .corporate_actions
                    .iter()
                    .any(|event| event.fingerprint() == stored.fingerprint())
        }) {
            bail!(
                "Arcus runtime checkpoint {} was written under a config declaring corporate \
                 action {} (window opening at {}, within the submission margin of now and not \
                 yet handled), which the supplied config does not declare. Removing a live \
                 window drops the guard it exists to be; restore the declaration, or resolve \
                 the window first",
                self.path.display(),
                dropped.event_id,
                dropped.entry_block_at.to_rfc3339(),
            );
        }
        let drift = classify_config_drift(&checkpoint.config, config);
        if !drift.state_invalidating.is_empty() {
            bail!(
                "Arcus runtime checkpoint {} was written under a different {} -- its accumulated \
                 signal window, regime, and risk baselines no longer describe this configuration, \
                 so reusing them would silently reinterpret them. Start a fresh window \
                 deliberately instead: `arcus-spot-execute-once reset-window CONFIG_YAML` (see \
                 docs/arcus-spot-runtime.md).",
                self.path.display(),
                drift.state_invalidating.join(", "),
            );
        }
        if !drift.state_preserving.is_empty() {
            // Never silent: these are administrator-approved changes to how a
            // live, KMS-signing bot sizes and gates its next swap, and the
            // operator making one has to be able to see from the journal that
            // the running process picked it up, and against which retained
            // state, rather than inferring it. `log` is deliberately not used
            // -- no binary that loads this checkpoint installs a logger, so a
            // `log::warn!` here would go nowhere.
            eprintln!(
                "[arcus-checkpoint] {} was written under a different {}; the change is \
                 state-preserving, so its state is being reused as-is ({} price samples, regime \
                 {:?}).",
                self.path.display(),
                drift.state_preserving.join(", "),
                checkpoint.state.relative_log_price_history.len(),
                checkpoint.state.regime,
            );
        }
        // Note the direction: the runtime is built from the config passed in
        // (the authenticated one), never from the checkpoint's stored copy.
        // That copy is only the witness `classify_config_drift` compares
        // against, and the next `persist` overwrites it with this one.
        ArcusSpotRuntime::from_state(config.clone(), checkpoint.state)
            .map_err(anyhow::Error::msg)
            .context("invalid Arcus runtime checkpoint state")
    }

    pub fn persist(&self, runtime: &ArcusSpotRuntime) -> Result<()> {
        let parent = self
            .path
            .parent()
            .context("Arcus runtime_state_path has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock precedes Unix epoch")?
            .as_nanos();
        let temp = parent.join(format!(
            ".{}.tmp.{}.{}",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("runtime-state"),
            std::process::id(),
            stamp,
        ));
        let checkpoint = ArcusSpotRuntimeCheckpoint {
            schema_version: RUNTIME_CHECKPOINT_SCHEMA_VERSION,
            config: runtime.config().clone(),
            state: runtime.state().clone(),
        };
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temp)
                .with_context(|| format!("failed to create {}", temp.display()))?;
            serde_json::to_writer_pretty(&mut file, &checkpoint)
                .context("failed to serialize Arcus runtime checkpoint")?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temp, &self.path).with_context(|| {
                format!(
                    "failed to atomically replace {} with {}",
                    self.path.display(),
                    temp.display(),
                )
            })?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arcus_spot::ArcusSpotRuntimeMode;
    use dex_connector::ArcusSpotPair;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    /// A config in which *every* field differs from `live_runtime_config`,
    /// for `every_config_field_is_classified`. Values only have to be
    /// different, not jointly valid: `classify_config_drift` is a pure
    /// comparison and never validates.
    fn maximally_different_config() -> ArcusSpotRuntimeConfig {
        ArcusSpotRuntimeConfig {
            mode: ArcusSpotRuntimeMode::ReplaySimulation,
            chain_id: 1,
            pair: ArcusSpotPair {
                sell_symbol: "SPY".to_string(),
                buy_symbol: "QQQ".to_string(),
            },
            notional_usd: Decimal::from(11),
            initial_inventory: super::super::ArcusSpotInventory {
                token_a: Decimal::new(26, 2),
                token_b: Decimal::new(11, 2),
            },
            inventory_floors: super::super::ArcusSpotInventory {
                token_a: Decimal::new(6, 2),
                token_b: Decimal::new(3, 2),
            },
            max_rotation_fraction: Decimal::new(30, 2),
            signal_window_samples: 97,
            min_signal_samples: 33,
            entry_z_score: 2.6,
            exit_z_score: 0.26,
            max_quote_age_secs: 31,
            max_hold_secs: 86_401,
            max_all_in_round_trip_cost_bps: Decimal::from(76),
            gas_buffer_bps: Decimal::from(11),
            settlement_buffer_bps: Decimal::from(12),
            max_inventory_imbalance_fraction: Decimal::new(76, 2),
            daily_loss_limit_usd: Decimal::from(3),
            cumulative_loss_limit_usd: Decimal::from(11),
            corporate_actions: vec![super::super::ArcusSpotCorporateActionEvent {
                event_id: "SPY-2026-SPLIT".to_string(),
                symbols: vec!["SPY".to_string()],
                entry_block_at: "2026-10-01T00:00:00Z".parse().unwrap(),
                reduce_exit_at: "2026-10-01T12:00:00Z".parse().unwrap(),
                effective_at: "2026-10-02T00:00:00Z".parse().unwrap(),
                resume_not_before: "2026-10-03T00:00:00Z".parse().unwrap(),
                source: "issuer notice".to_string(),
                post_event_inventory: None,
            }],
            corporate_action_settlement_margin_secs: 42,
        }
    }

    fn live_runtime_config() -> ArcusSpotRuntimeConfig {
        ArcusSpotRuntimeConfig {
            mode: ArcusSpotRuntimeMode::Live,
            chain_id: 4663,
            pair: ArcusSpotPair {
                sell_symbol: "NVDA".to_string(),
                buy_symbol: "AMD".to_string(),
            },
            notional_usd: Decimal::from(5),
            initial_inventory: super::super::ArcusSpotInventory {
                token_a: Decimal::new(25, 2),
                token_b: Decimal::new(10, 2),
            },
            inventory_floors: super::super::ArcusSpotInventory {
                token_a: Decimal::new(5, 2),
                token_b: Decimal::new(2, 2),
            },
            max_rotation_fraction: Decimal::new(25, 2),
            signal_window_samples: 96,
            min_signal_samples: 32,
            entry_z_score: 2.5,
            exit_z_score: 0.25,
            max_quote_age_secs: 30,
            max_hold_secs: 86_400,
            max_all_in_round_trip_cost_bps: Decimal::from(75),
            gas_buffer_bps: Decimal::from(10),
            settlement_buffer_bps: Decimal::from(10),
            max_inventory_imbalance_fraction: Decimal::new(75, 2),
            daily_loss_limit_usd: Decimal::from(2),
            cumulative_loss_limit_usd: Decimal::from(10),
            corporate_actions: Vec::new(),
            corporate_action_settlement_margin_secs: 300,
        }
    }

    #[test]
    fn runtime_checkpoint_round_trip_is_private_and_validated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        let store = ArcusSpotRuntimeCheckpointStore::new(path.clone());
        let config = live_runtime_config();
        let runtime = store.load_or_create(&config).unwrap();
        store.persist(&runtime).unwrap();
        let restored = store.load_or_create(&config).unwrap();
        assert_eq!(restored.config(), &config);
        assert_eq!(restored.state(), runtime.state());
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn checkpoint_rejects_a_state_invalidating_config_change() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        let store = ArcusSpotRuntimeCheckpointStore::new(path);
        let config = live_runtime_config();
        let runtime = store.load_or_create(&config).unwrap();
        store.persist(&runtime).unwrap();

        let mut different = live_runtime_config();
        different.pair = ArcusSpotPair {
            sell_symbol: "SPY".to_string(),
            buy_symbol: "QQQ".to_string(),
        };
        match store.load_or_create(&different) {
            Ok(_) => panic!("expected a state-invalidating config error"),
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("was written under a different pair"),
                    "{message}"
                );
                assert!(
                    message.contains("arcus-spot-execute-once reset-window CONFIG_YAML"),
                    "{message}"
                );
            }
        }
    }

    /// The bot-strategy#809 case: an administrator-approved forward-looking
    /// cap changes, and the accumulated signal window must survive it rather
    /// than costing another warmup period to re-earn.
    #[test]
    fn checkpoint_adopts_a_state_preserving_config_change_and_keeps_state() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        let store = ArcusSpotRuntimeCheckpointStore::new(path);
        let config = live_runtime_config();
        let fresh = store.load_or_create(&config).unwrap();
        let mut state = fresh.state().clone();
        state.relative_log_price_history = vec![0.1, 0.2, 0.3];
        state.sequence = 42;
        let seeded = ArcusSpotRuntime::from_state(config.clone(), state).unwrap();
        store.persist(&seeded).unwrap();

        let mut retuned = live_runtime_config();
        retuned.max_rotation_fraction = Decimal::new(30, 2);
        let restored = store.load_or_create(&retuned).unwrap();

        assert_eq!(
            restored.state().relative_log_price_history,
            vec![0.1, 0.2, 0.3],
            "the signal window must survive a state-preserving retune",
        );
        assert_eq!(restored.state().sequence, 42);
        // The authenticated config wins over the checkpoint's stored copy.
        assert_eq!(restored.config().max_rotation_fraction, Decimal::new(30, 2));

        // And the next persist writes the new config through, so the drift is
        // reported once rather than on every subsequent load.
        store.persist(&restored).unwrap();
        let drift = {
            let bytes = fs::read(&store.path).unwrap();
            let checkpoint: ArcusSpotRuntimeCheckpoint = serde_json::from_slice(&bytes).unwrap();
            classify_config_drift(&checkpoint.config, &retuned)
        };
        assert!(drift.state_preserving.is_empty());
        assert!(drift.state_invalidating.is_empty());
    }

    /// Guards the one hole the exhaustive destructuring in
    /// `classify_config_drift` cannot close on its own: a field can be
    /// *mentioned* in the pattern (satisfying the compiler) while never being
    /// *compared*, which would silently let it drift unreported. Every field
    /// differs between these two configs, so the two buckets together must
    /// account for all of them.
    #[test]
    fn every_config_field_is_classified() {
        let stored = live_runtime_config();
        let current = maximally_different_config();

        // Counted from `current`, not `stored`: fields whose value is the
        // default are skipped in the canonical serialization (so an
        // unchanged YAML keeps its auto-execute digest across a binary
        // upgrade), and `maximally_different_config` populates every one of
        // them, so its object has a key per field.
        let field_count = serde_json::to_value(&current)
            .unwrap()
            .as_object()
            .expect("runtime config serializes as an object")
            .len();

        let drift = classify_config_drift(&stored, &current);
        let classified = drift.state_invalidating.len() + drift.state_preserving.len();
        assert_eq!(
            classified, field_count,
            "every ArcusSpotRuntimeConfig field must be compared exactly once; classified \
             {:?} / {:?} out of {field_count} fields",
            drift.state_invalidating, drift.state_preserving,
        );

        // Pin the split so reclassifying a field is a deliberate, reviewable
        // change rather than a side effect.
        assert_eq!(
            drift.state_invalidating,
            vec![
                "mode",
                "chain_id",
                "pair",
                "initial_inventory",
                "signal_window_samples",
            ],
        );
        assert!(drift.state_preserving.contains(&"corporate_actions"));
        assert!(drift
            .state_preserving
            .contains(&"corporate_action_settlement_margin_secs"));
    }

    #[test]
    fn load_refuses_a_config_that_dropped_a_live_window() {
        use super::super::ArcusSpotCorporateActionEvent;
        let dir = tempdir().unwrap();
        let store = ArcusSpotRuntimeCheckpointStore::new(dir.path().join("runtime.json"));
        let anchor: chrono::DateTime<chrono::Utc> = "2026-08-16T00:00:00Z".parse().unwrap();
        let mut declared = live_runtime_config();
        declared.corporate_actions = vec![ArcusSpotCorporateActionEvent {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            symbols: vec![declared.pair.sell_symbol.clone()],
            entry_block_at: anchor,
            reduce_exit_at: anchor + chrono::Duration::hours(1),
            effective_at: anchor + chrono::Duration::hours(2),
            resume_not_before: anchor + chrono::Duration::hours(3),
            source: "issuer notice".to_string(),
            post_event_inventory: None,
        }];
        // The window has opened as of the last observation, and no tick has
        // written progress yet -- the exact gap this guard closes.
        let mut state = ArcusSpotRuntime::new(declared.clone())
            .unwrap()
            .state()
            .clone();
        state.last_observation_at = Some(anchor + chrono::Duration::minutes(1));
        assert!(state.corporate_action.is_none());
        let runtime = ArcusSpotRuntime::from_state(declared.clone(), state.clone()).unwrap();
        store.persist(&runtime).unwrap();

        let mut dropped = declared.clone();
        dropped.corporate_actions.clear();
        let inside = anchor + chrono::Duration::minutes(2);
        let error = match store.load_existing_at(&dropped, inside) {
            Ok(_) => panic!("expected the dropped window to be refused"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Removing a live window"), "{error}");

        // Renaming it is not removing it: same fingerprint, still declared.
        let mut renamed = declared.clone();
        renamed.corporate_actions[0].event_id = "NVDA-2026-08-SPLIT-v2".to_string();
        assert!(store.load_existing_at(&renamed, inside).is_ok());

        // Downtime does not make a window un-live: the watermark stays before
        // `entry_block_at` while the clock moves past it.
        let mut early_state = state.clone();
        early_state.last_observation_at = Some(anchor - chrono::Duration::hours(1));
        store
            .persist(&ArcusSpotRuntime::from_state(declared.clone(), early_state.clone()).unwrap())
            .unwrap();
        let error = match store.load_existing_at(&dropped, inside) {
            Ok(_) => panic!("a window open by the clock is live even after downtime"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Removing a live window"), "{error}");

        // Retiring one that cannot open within the submission margin is
        // housekeeping. The default margin is 300s, so a minute before the
        // window is still refused -- the runtime this load builds could
        // submit after it opened.
        let error = match store.load_existing_at(&dropped, anchor - chrono::Duration::minutes(1)) {
            Ok(_) => panic!("a window that can open mid-submission is still live"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Removing a live window"), "{error}");
        assert!(store
            .load_existing_at(&dropped, anchor - chrono::Duration::hours(1))
            .is_ok());

        // A reused id is not "handled": the recorded fingerprint is another
        // event's, so the declaration is still an unresolved window.
        let mut reused_state = state.clone();
        reused_state.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        reused_state.handled_corporate_action_fingerprints = vec!["an-older-event".to_string()];
        store
            .persist(&ArcusSpotRuntime::from_state(declared.clone(), reused_state).unwrap())
            .unwrap();
        let error = match store.load_existing_at(&dropped, inside) {
            Ok(_) => panic!("a reused id must not read as handled"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("Removing a live window"), "{error}");

        // Genuinely handled (same fingerprint): removal is fine.
        let mut handled_state = state.clone();
        handled_state.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        handled_state.handled_corporate_action_fingerprints =
            vec![declared.corporate_actions[0].fingerprint()];
        store
            .persist(&ArcusSpotRuntime::from_state(declared.clone(), handled_state).unwrap())
            .unwrap();
        assert!(store.load_existing_at(&dropped, inside).is_ok());
    }

    #[test]
    fn an_identical_config_reports_no_drift() {
        let config = live_runtime_config();
        assert_eq!(
            classify_config_drift(&config, &config),
            ArcusSpotCheckpointConfigDrift::default(),
        );
    }

    #[test]
    fn load_existing_refuses_missing_checkpoint_without_creating_it() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        let store = ArcusSpotRuntimeCheckpointStore::new(path.clone());

        let error = match store.load_existing(&live_runtime_config()) {
            Ok(_) => panic!("expected missing checkpoint error"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("does not exist"));
        assert!(!path.exists());
    }
}
