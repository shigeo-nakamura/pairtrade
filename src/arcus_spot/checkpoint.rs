//! Durable checkpoint for `ArcusSpotRuntime` state (bot-strategy#772,
//! pairtrade#181). Shared by every binary that needs the runtime's current
//! signal window / regime / risk state to survive a restart: the live
//! executor (`arcus-spot-execute-once`) and the live plan proposer
//! (`arcus-spot-propose-plan`) both read and write the same checkpoint file
//! at `runtime_state_path`, so this is the single implementation of its
//! atomic-write and validated-restore logic rather than two independently
//! maintained copies of it.

use super::{ArcusSpotRegime, ArcusSpotRuntime, ArcusSpotRuntimeConfig, ArcusSpotRuntimeState};
use anyhow::{bail, Context, Result};
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
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = read_private_regular_file(&self.path, "runtime checkpoint")?;
        let checkpoint: ArcusSpotRuntimeCheckpoint = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid runtime checkpoint {}", self.path.display()))?;
        if checkpoint.schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION {
            bail!("unsupported Arcus runtime checkpoint schema");
        }
        Ok(Some((
            checkpoint.state.regime,
            checkpoint.state.rotated_quantity,
        )))
    }

    /// Load and validate an already-persisted checkpoint without creating
    /// or modifying anything. Operator backup/rollback checks must never
    /// turn a missing checkpoint into a successful first-run state: absence
    /// is precisely the reset condition those checks are meant to detect.
    pub fn load_existing(&self, config: &ArcusSpotRuntimeConfig) -> Result<ArcusSpotRuntime> {
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
        let drift = classify_config_drift(&checkpoint.config, config);
        if !drift.state_invalidating.is_empty() {
            bail!(
                "Arcus runtime checkpoint {} was written under a different {} -- its accumulated \
                 signal window, regime, and risk baselines no longer describe this configuration, \
                 so reusing them would silently reinterpret them. Reset the checkpoint \
                 deliberately instead (see docs/arcus-spot-runtime.md).",
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
                assert!(message.contains("Reset the checkpoint"), "{message}");
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

        let field_count = serde_json::to_value(&stored)
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
