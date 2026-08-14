//! Durable checkpoint for `ArcusSpotRuntime` state (bot-strategy#772,
//! pairtrade#181). Shared by every binary that needs the runtime's current
//! signal window / regime / risk state to survive a restart: the live
//! executor (`arcus-spot-execute-once`) and the live plan proposer
//! (`arcus-spot-propose-plan`) both read and write the same checkpoint file
//! at `runtime_state_path`, so this is the single implementation of its
//! atomic-write and validated-restore logic rather than two independently
//! maintained copies of it.

use super::{ArcusSpotRuntime, ArcusSpotRuntimeConfig, ArcusSpotRuntimeState};
use anyhow::{bail, Context, Result};
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
        let bytes = read_private_regular_file(&self.path, "runtime checkpoint")?;
        let checkpoint: ArcusSpotRuntimeCheckpoint = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid runtime checkpoint {}", self.path.display()))?;
        if checkpoint.schema_version != RUNTIME_CHECKPOINT_SCHEMA_VERSION {
            bail!("unsupported Arcus runtime checkpoint schema");
        }
        if checkpoint.config != *config {
            bail!("Arcus runtime checkpoint config does not match approved config");
        }
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
    fn checkpoint_rejects_a_config_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        let store = ArcusSpotRuntimeCheckpointStore::new(path);
        let config = live_runtime_config();
        let runtime = store.load_or_create(&config).unwrap();
        store.persist(&runtime).unwrap();

        let mut different = live_runtime_config();
        different.entry_z_score = 3.0;
        match store.load_or_create(&different) {
            Ok(_) => panic!("expected a config-mismatch error"),
            Err(error) => {
                assert!(error.to_string().contains("does not match approved config"))
            }
        }
    }
}
