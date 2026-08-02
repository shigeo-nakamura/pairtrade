//! Explicitly approved one-shot Arcus Spot execution.
//!
//! This binary is built only with arcus-spot-live. It has no loop or service
//! activation path: one invocation can consume exactly one fresh plan after
//! its canonical SHA-256 digest is supplied again on the command line.

use anyhow::{bail, Context, Result};
use debot::arcus_spot::{
    build_arcus_spot_kms_signer, ArcusSpotChainClient, ArcusSpotChainConfig,
    ArcusSpotExecutionLedgerStore, ArcusSpotKmsConfig, ArcusSpotLiveExecutor,
    ArcusSpotLiveExecutorConfig, ArcusSpotRotationPlan,
};
use dex_connector::{ArcusSpotClient, ArcusSpotConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    io::{self, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
struct ArcusSpotExecuteOnceConfig {
    router: ArcusSpotConfig,
    chain: ArcusSpotChainConfig,
    kms: ArcusSpotKmsConfig,
    executor: ArcusSpotLiveExecutorConfig,
    ledger_path: PathBuf,
}

fn approval_digest<T: Serialize>(value: &T) -> Result<String> {
    let canonical = serde_json::to_vec(value).context("failed to serialize approval payload")?;
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
  arcus-spot-execute-once hash PLAN_JSON
  arcus-spot-execute-once execute CONFIG_YAML PLAN_JSON APPROVAL_SHA256"
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [command, plan_path] if command == "hash" => {
            let plan_path = Path::new(plan_path);
            let bytes = read_private_regular_file(plan_path, "plan")?;
            let plan: ArcusSpotRotationPlan = serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid plan {}", plan_path.display()))?;
            println!("{}", approval_digest(&plan)?);
            Ok(())
        }
        [command, config_path, plan_path, approved_digest] if command == "execute" => {
            let config_path = Path::new(config_path);
            let plan_path = Path::new(plan_path);
            let config_bytes = read_private_regular_file(config_path, "config")?;
            let plan_bytes = read_private_regular_file(plan_path, "plan")?;
            let config: ArcusSpotExecuteOnceConfig = serde_yaml::from_slice(&config_bytes)
                .with_context(|| format!("invalid config {}", config_path.display()))?;
            let plan: ArcusSpotRotationPlan = serde_json::from_slice(&plan_bytes)
                .with_context(|| format!("invalid plan {}", plan_path.display()))?;
            let computed_digest = approval_digest(&plan)?;
            if approved_digest != &computed_digest {
                bail!(
                    "approval digest mismatch: supplied {approved_digest}, computed {computed_digest}"
                );
            }
            if !config.ledger_path.is_absolute() {
                bail!("Arcus execution ledger_path must be absolute");
            }
            if config.router.chain_id != config.chain.chain_id
                || config.router.chain_id != config.kms.chain_id
            {
                bail!("Arcus router, chain RPC, and KMS chain IDs must match");
            }

            let client = ArcusSpotClient::new(config.router)
                .context("invalid Arcus router configuration")?;
            let chain = ArcusSpotChainClient::new(config.chain)
                .context("invalid Arcus chain configuration")?;
            let signer = build_arcus_spot_kms_signer(&config.kms).await?;
            let store = ArcusSpotExecutionLedgerStore::new(config.ledger_path);
            let mut executor =
                ArcusSpotLiveExecutor::new(config.executor, client, chain, signer, store)?;
            let attempt = executor.execute_plan_once(&plan).await?;

            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            serde_json::to_writer_pretty(&mut stdout, &attempt)
                .context("failed to serialize execution result")?;
            stdout.write_all(b"\n")?;
            Ok(())
        }
        _ => bail!(usage()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn approval_digest_is_canonical_and_amount_sensitive() {
        let first = json!({"sell_amount_raw":"1000","venue":"arcus"});
        let same = json!({"venue":"arcus","sell_amount_raw":"1000"});
        let changed = json!({"sell_amount_raw":"1001","venue":"arcus"});
        assert_eq!(
            approval_digest(&first).unwrap(),
            approval_digest(&same).unwrap()
        );
        assert_ne!(
            approval_digest(&first).unwrap(),
            approval_digest(&changed).unwrap()
        );
        assert_eq!(approval_digest(&first).unwrap().len(), 71);
    }
}
