//! Explicitly approved one-shot Arcus Spot execution.
//!
//! This binary is built only with arcus-spot-live. It has no loop or service
//! activation path: one invocation can consume exactly one fresh plan after
//! the validated config-and-plan SHA-256 digest is supplied again.

use anyhow::{bail, Context, Result};
use debot::arcus_spot::{
    build_arcus_spot_kms_signer, ArcusSpotChainClient, ArcusSpotChainConfig,
    ArcusSpotExecutionAttempt, ArcusSpotExecutionLedgerStore, ArcusSpotExecutionPhase,
    ArcusSpotKmsConfig, ArcusSpotKmsSigner, ArcusSpotLiveExecutor, ArcusSpotLiveExecutorConfig,
    ArcusSpotRotationPlan, ArcusSpotRuntime, ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode,
    ArcusSpotRuntimeState,
};
use dex_connector::{ArcusSpotClient, ArcusSpotConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    fs::{File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

#[derive(Debug, Serialize, Deserialize)]
struct ArcusSpotExecuteOnceConfig {
    router: ArcusSpotConfig,
    chain: ArcusSpotChainConfig,
    kms: ArcusSpotKmsConfig,
    executor: ArcusSpotLiveExecutorConfig,
    ledger_path: PathBuf,
    runtime: ArcusSpotRuntimeConfig,
    runtime_state_path: PathBuf,
}

#[derive(Serialize)]
struct ArcusSpotApprovalEnvelope<'a, C, P> {
    config: &'a C,
    plan: &'a P,
}

fn approval_digest<C: Serialize, P: Serialize>(config: &C, plan: &P) -> Result<String> {
    let canonical = serde_json::to_vec(&ArcusSpotApprovalEnvelope { config, plan })
        .context("failed to serialize approval payload")?;
    Ok(format!("sha256:{:x}", Sha256::digest(canonical)))
}

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

fn usage() -> &'static str {
    "usage:
  arcus-spot-execute-once hash CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once execute CONFIG_YAML PLAN_JSON APPROVAL_SHA256
  arcus-spot-execute-once resume CONFIG_YAML PLAN_JSON APPROVAL_SHA256"
}

fn validate_config(config: &mut ArcusSpotExecuteOnceConfig) -> Result<()> {
    if !config.ledger_path.is_absolute() || !config.runtime_state_path.is_absolute() {
        bail!("Arcus ledger_path and runtime_state_path must be absolute");
    }
    if config.ledger_path == config.runtime_state_path {
        bail!("Arcus ledger_path and runtime_state_path must be distinct");
    }
    config.runtime.normalize();
    config
        .runtime
        .validate()
        .map_err(anyhow::Error::msg)
        .context("invalid Arcus runtime configuration")?;
    if config.runtime.mode != ArcusSpotRuntimeMode::Live {
        bail!("Arcus one-shot execution requires runtime mode=live");
    }
    if config.router.chain_id != config.chain.chain_id
        || config.router.chain_id != config.kms.chain_id
        || config.router.chain_id != config.runtime.chain_id
    {
        bail!("Arcus router, chain RPC, KMS, and runtime chain IDs must match");
    }
    ArcusSpotClient::new(config.router.clone()).context("invalid Arcus router configuration")?;
    config.chain.validate()?;
    let (_, kms_address) = config.kms.validate()?;
    let (executor_taker, _) = config.executor.validate()?;
    if kms_address != executor_taker {
        bail!("Arcus KMS expected_address must match executor taker");
    }
    Ok(())
}

fn parse_config(bytes: &[u8], path: &Path) -> Result<ArcusSpotExecuteOnceConfig> {
    let mut config: ArcusSpotExecuteOnceConfig = serde_yaml::from_slice(bytes)
        .with_context(|| format!("invalid config {}", path.display()))?;
    validate_config(&mut config)?;
    Ok(config)
}

async fn executor_from_config(
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<ArcusSpotLiveExecutor<ArcusSpotKmsSigner>> {
    let client = ArcusSpotClient::new(config.router.clone())
        .context("invalid Arcus router configuration")?;
    let chain = ArcusSpotChainClient::new(config.chain.clone())
        .context("invalid Arcus chain configuration")?;
    let signer = build_arcus_spot_kms_signer(&config.kms).await?;
    let store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    ArcusSpotLiveExecutor::new(
        config.executor.clone(),
        config.runtime.pair.clone(),
        client,
        chain,
        signer,
        store,
    )
}

const RUNTIME_CHECKPOINT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct ArcusSpotRuntimeCheckpoint {
    schema_version: u32,
    config: ArcusSpotRuntimeConfig,
    state: ArcusSpotRuntimeState,
}

struct ArcusSpotRuntimeCheckpointStore {
    path: PathBuf,
}

impl ArcusSpotRuntimeCheckpointStore {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn load_or_create(&self, config: &ArcusSpotRuntimeConfig) -> Result<ArcusSpotRuntime> {
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

    fn persist(&self, runtime: &ArcusSpotRuntime) -> Result<()> {
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

fn finalize_reconciled_attempt(
    config: &ArcusSpotExecuteOnceConfig,
    executor: &mut ArcusSpotLiveExecutor<ArcusSpotKmsSigner>,
    plan: &ArcusSpotRotationPlan,
    plan_config_digest: &str,
    attempt: ArcusSpotExecutionAttempt,
) -> Result<ArcusSpotExecutionAttempt> {
    if attempt.phase != ArcusSpotExecutionPhase::Reconciled {
        return Ok(attempt);
    }
    let fill = executor.reconciled_runtime_fill(plan, plan_config_digest)?;
    let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    let mut runtime = store.load_or_create(&config.runtime)?;
    runtime
        .apply_confirmed_live_fill_once(
            plan,
            fill.actual_sell_quantity,
            fill.actual_buy_quantity,
            fill.reconciled_at,
            &fill.idempotency_key,
        )
        .map_err(anyhow::Error::msg)
        .context("failed to commit reconciled Arcus fill to runtime state")?;
    store.persist(&runtime)?;
    executor.archive_reconciled_after_runtime_commit()?;
    Ok(attempt)
}

fn write_attempt(attempt: &ArcusSpotExecutionAttempt) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, attempt)
        .context("failed to serialize execution result")?;
    stdout.write_all(b"\n")?;
    Ok(())
}

fn load_config_and_plan(
    config_path: &Path,
    plan_path: &Path,
) -> Result<(ArcusSpotExecuteOnceConfig, ArcusSpotRotationPlan)> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let plan_bytes = read_private_regular_file(plan_path, "plan")?;
    let config = parse_config(&config_bytes, config_path)?;
    let plan: ArcusSpotRotationPlan = serde_json::from_slice(&plan_bytes)
        .with_context(|| format!("invalid plan {}", plan_path.display()))?;
    Ok((config, plan))
}

fn require_approval_digest(
    config: &ArcusSpotExecuteOnceConfig,
    plan: &ArcusSpotRotationPlan,
    approved_digest: &str,
) -> Result<()> {
    let computed_digest = approval_digest(config, plan)?;
    if approved_digest != computed_digest {
        bail!("approval digest mismatch: supplied {approved_digest}, computed {computed_digest}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [command, config_path, plan_path] if command == "hash" => {
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            println!("{}", approval_digest(&config, &plan)?);
            Ok(())
        }
        [command, config_path, plan_path, approved_digest] if command == "execute" => {
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            require_approval_digest(&config, &plan, approved_digest)?;
            let mut executor = executor_from_config(&config).await?;
            let attempt = executor.execute_plan_once(&plan, approved_digest).await?;
            let attempt =
                finalize_reconciled_attempt(&config, &mut executor, &plan, approved_digest, attempt)?;
            write_attempt(&attempt)
        }
        [command, config_path, plan_path, approved_digest] if command == "resume" => {
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            require_approval_digest(&config, &plan, approved_digest)?;
            let mut executor = executor_from_config(&config).await?;
            let attempt = executor.resume_status_and_reconcile().await?;
            let attempt =
                finalize_reconciled_attempt(&config, &mut executor, &plan, approved_digest, attempt)?;
            write_attempt(&attempt)
        }
        _ => bail!(usage()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn usage_exposes_explicit_resume_command() {
        assert!(usage().contains("resume CONFIG_YAML PLAN_JSON APPROVAL_SHA256"));
    }

    fn live_runtime_config() -> ArcusSpotRuntimeConfig {
        serde_yaml::from_str(
            r#"
mode: live
chain_id: 4663
pair:
  sell_symbol: NVDA
  buy_symbol: AMD
notional_usd: "5"
initial_inventory:
  token_a: "0.25"
  token_b: "0.10"
inventory_floors:
  token_a: "0.05"
  token_b: "0.02"
max_rotation_fraction: "0.25"
signal_window_samples: 96
min_signal_samples: 32
entry_z_score: 2.5
exit_z_score: 0.25
max_quote_age_secs: 30
max_hold_secs: 86400
max_all_in_round_trip_cost_bps: "75"
gas_buffer_bps: "10"
settlement_buffer_bps: "10"
max_inventory_imbalance_fraction: "0.75"
daily_loss_limit_usd: "2"
cumulative_loss_limit_usd: "10"
"#,
        )
        .unwrap()
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
    fn approval_digest_binds_canonical_config_and_plan() {
        let config = json!({"chain_id":4663,"taker":"0x01"});
        let same_config = json!({"taker":"0x01","chain_id":4663});
        let changed_config = json!({"chain_id":4663,"taker":"0x02"});
        let plan = json!({"sell_amount_raw":"1000","venue":"arcus"});
        let same_plan = json!({"venue":"arcus","sell_amount_raw":"1000"});
        let changed_plan = json!({"sell_amount_raw":"1001","venue":"arcus"});
        assert_eq!(
            approval_digest(&config, &plan).unwrap(),
            approval_digest(&same_config, &same_plan).unwrap()
        );
        assert_ne!(
            approval_digest(&config, &plan).unwrap(),
            approval_digest(&changed_config, &plan).unwrap()
        );
        assert_ne!(
            approval_digest(&config, &plan).unwrap(),
            approval_digest(&config, &changed_plan).unwrap()
        );
        assert_eq!(approval_digest(&config, &plan).unwrap().len(), 71);
    }
}
