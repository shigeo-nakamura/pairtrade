//! One-shot live plan proposal for Arcus Spot.
//!
//! `arcus-spot-runtime` (docs/arcus-spot-runtime.md) only replays a
//! pre-recorded recorder archive, evaluating freshness at each record's own
//! `collection_finished_at` rather than the current wall clock -- it can
//! never itself produce a plan fresh enough for `arcus-spot-execute-once`'s
//! `execute` to dispatch within its 60s HARD_MAX_PLAN_AGE_SECS window. This
//! binary closes that gap: it takes exactly one live snapshot from the same
//! public, read-only recorder client the archival collector uses, evaluates
//! it against the runtime's current (checkpointed) signal window with
//! `step_at(..., Utc::now())`, and -- if that produces a rotation decision
//! -- writes the resulting plan to a file `hash`/`sign-approval`/`execute`
//! can consume.
//!
//! This binary has no wallet, signing, approval, or submission surface --
//! only the read-only recorder client plus the runtime's own signal/risk
//! evaluation. It shares the runtime checkpoint file with
//! `arcus-spot-execute-once` (both must be given the same `runtime:`
//! config), and takes the same checkpoint-namespace lock `execute`/`resume`
//! do, so the two binaries cannot race on that shared state.

use anyhow::{bail, Context, Result};
use chrono::Utc;
use debot::arcus_spot::{
    replay_jsonl, ArcusSpotDecision, ArcusSpotExecutionLedgerStore, ArcusSpotRuntime,
    ArcusSpotRuntimeCheckpointStore, ArcusSpotRuntimeConfig, ArcusSpotRuntimeMode,
};
use dex_connector::{
    ArcusSpotClient, ArcusSpotConfig, ArcusSpotRecorder, ArcusSpotRecorderConfig,
    ArcusSpotRecorderSnapshot,
};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    fs::OpenOptions,
    io::{self, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Writes `bytes` to `path` as a private (mode 0600), non-symlink regular
/// file -- every subsequent `hash`/`execute`/`resume` invocation rejects a
/// plan file that isn't already mode 0600 (`read_private_regular_file` in
/// arcus_spot_execute_once.rs), and under the common `umask 022` a plain
/// `fs::write` would create a brand new file as 0644, silently breaking the
/// advertised propose-to-execute workflow. `.mode(0o600)` on `OpenOptions`
/// only takes effect when the open call actually creates the inode, so a
/// stale plan file left over at this exact path from a prior run (already
/// 0644, or looser) would keep its old permissions across an overwrite;
/// `set_permissions` after opening fixes that regardless of whether this
/// call created the file or reused an existing one.
fn write_private_plan_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to set permissions on {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))
}

/// A strict subset of `arcus-spot-execute-once`'s config schema: only the
/// fields a read-only plan proposal needs. Deliberately *not*
/// `#[serde(deny_unknown_fields)]` so the exact same CONFIG_YAML given to
/// `hash`/`execute` (which also carries `chain`/`kms`/`executor`/
/// `ledger_path`) can be reused here unmodified -- pointing propose and
/// execute at two independently maintained config files would risk their
/// `runtime:` sections drifting apart, which breaks the shared checkpoint's
/// exact-match validation.
#[derive(Debug, Deserialize)]
struct ArcusSpotProposeConfig {
    router: ArcusSpotConfig,
    runtime: ArcusSpotRuntimeConfig,
    runtime_state_path: PathBuf,
}

const OBSERVATION_EVIDENCE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotObservationEvidence {
    schema_version: u32,
    evaluation_time: chrono::DateTime<Utc>,
    snapshot: ArcusSpotRecorderSnapshot,
    resulting_runtime: ArcusSpotObservationBoundary,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotObservationBoundary {
    sequence: u64,
    last_observation_at: Option<chrono::DateTime<Utc>>,
}

fn observation_evidence_path(runtime_state_path: &Path) -> Result<PathBuf> {
    let parent = runtime_state_path
        .parent()
        .context("Arcus runtime_state_path has no parent")?;
    Ok(parent.join("live-tick-observation-evidence.json"))
}

/// Atomically persist the recorder boundary shared with rollback verification.
/// This mirrors the checkpoint store's temp-file/rename/fsync sequence so a
/// reader holding the same namespace lock never sees a partial document.
fn write_observation_evidence(
    config: &ArcusSpotProposeConfig,
    snapshot: ArcusSpotRecorderSnapshot,
    evaluation_time: chrono::DateTime<Utc>,
    resulting_runtime: ArcusSpotObservationBoundary,
) -> Result<()> {
    let path = observation_evidence_path(&config.runtime_state_path)?;
    let evidence = ArcusSpotObservationEvidence {
        schema_version: OBSERVATION_EVIDENCE_SCHEMA_VERSION,
        evaluation_time,
        snapshot,
        resulting_runtime,
    };
    let mut bytes = serde_json::to_vec_pretty(&evidence)
        .context("failed to serialize Arcus observation evidence")?;
    bytes.push(b'\n');

    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent directory", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    let temp = parent.join(format!(
        ".{}.tmp.{}.{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("observation-evidence"),
        std::process::id(),
        stamp,
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temp)
            .with_context(|| format!("failed to create {}", temp.display()))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp, &path).with_context(|| {
            format!(
                "failed to atomically replace {} with {}",
                path.display(),
                temp.display(),
            )
        })?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn usage() -> &'static str {
    "usage:
  arcus-spot-propose-plan bootstrap CONFIG_YAML SAMPLES_JSONL
  arcus-spot-propose-plan propose CONFIG_YAML [PLAN_JSON_OUT]

bootstrap replays a recorder archive (e.g. the collector's accumulated
samples.jsonl) to warm up a fresh runtime checkpoint's signal window before
propose can ever produce a real signal (min_signal_samples/
signal_window_samples in the runtime config). It refuses to run against a
runtime_state_path that already exists -- delete it explicitly first if you
really want to discard live inventory/regime/risk state and start over.

propose takes exactly one live snapshot from the configured router and
evaluates it against the checkpoint with the current wall clock. It always
persists the checkpoint afterward (even on a mere Observe), since step_at
advances the signal window and risk marks on every call regardless of
whether it decides to rotate -- skipping that persist would make every
future propose call start from the same stale, underwarmed window. If a
rotation decision resulted and PLAN_JSON_OUT was given, the plan is written
there; it must be run through `hash`/`sign-approval`/`execute` well within
the 60s HARD_MAX_PLAN_AGE_SECS window, since every step in between
(operator review, offline signing) eats into it.

Both subcommands take the same lock `arcus-spot-execute-once`'s execute/
resume take on runtime_state_path, so they cannot race against each other."
}

fn parse_config(path: &Path) -> Result<ArcusSpotProposeConfig> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read config {}", path.display()))?;
    let mut config: ArcusSpotProposeConfig = serde_yaml::from_slice(&bytes)
        .with_context(|| format!("invalid config {}", path.display()))?;
    if !config.runtime_state_path.is_absolute() {
        bail!("Arcus runtime_state_path must be absolute");
    }
    config.runtime.normalize();
    config
        .runtime
        .validate()
        .map_err(anyhow::Error::msg)
        .context("invalid Arcus runtime configuration")?;
    if config.runtime.mode != ArcusSpotRuntimeMode::Live {
        bail!("Arcus plan proposal requires runtime mode=live, matching what execute requires");
    }
    if config.router.chain_id != config.runtime.chain_id {
        bail!("Arcus router and runtime chain IDs must match");
    }
    ArcusSpotClient::new(config.router.clone()).context("invalid Arcus router configuration")?;
    Ok(config)
}

/// Locks on the same namespace `executor_from_config` in
/// arcus_spot_execute_once.rs uses (`runtime_state_path`), so a `propose`
/// invocation and a concurrent `execute`/`resume`/another `propose` cannot
/// both read-modify-write the checkpoint at once. The lock is keyed purely
/// by path, not by any ledger content, so reusing this store type here only
/// for its locking side effect (propose touches no ledger) is safe.
fn lock_runtime_checkpoint(
    runtime_state_path: &Path,
) -> Result<debot::arcus_spot::ArcusSpotExecutionLedgerLock> {
    let lock_store = ArcusSpotExecutionLedgerStore::new(runtime_state_path.to_path_buf());
    lock_store
        .acquire_exclusive_lock(runtime_state_path)
        .context("failed to acquire the Arcus runtime checkpoint lock -- is execute/resume/another propose already running?")
}

fn bootstrap(config_path: &str, samples_path: &str) -> Result<()> {
    let config = parse_config(Path::new(config_path))?;
    let _lock = lock_runtime_checkpoint(&config.runtime_state_path)?;
    if config.runtime_state_path.exists() {
        bail!(
            "runtime checkpoint {} already exists; bootstrap only initializes a fresh \
             checkpoint. Delete it explicitly first if you really want to replay from \
             scratch, discarding whatever live inventory/regime/risk state it holds.",
            config.runtime_state_path.display()
        );
    }
    let mut runtime = ArcusSpotRuntime::new(config.runtime.clone()).map_err(anyhow::Error::msg)?;
    let file =
        fs::File::open(samples_path).with_context(|| format!("failed to open {samples_path}"))?;
    let summary = replay_jsonl(&mut runtime, io::BufReader::new(file), io::sink())
        .context("failed to replay the warm-up archive")?;
    let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    store.persist(&runtime)?;
    eprintln!(
        "bootstrapped {} from {} archive record(s) ({} evaluated) -- final sequence={}, \
         regime={:?}, signal samples={}",
        config.runtime_state_path.display(),
        summary.input_records,
        summary.emitted_events,
        summary.final_state.sequence,
        summary.final_state.regime,
        summary.final_state.relative_log_price_history.len(),
    );
    Ok(())
}

async fn propose(config_path: &str, out_path: Option<&str>) -> Result<()> {
    let config = parse_config(Path::new(config_path))?;
    let _lock = lock_runtime_checkpoint(&config.runtime_state_path)?;
    let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    let mut runtime = store.load_or_create(&config.runtime)?;

    let client = ArcusSpotClient::new(config.router.clone())
        .context("invalid Arcus router configuration")?;
    let recorder_config = ArcusSpotRecorderConfig::from_csv(
        &format!(
            "{}/{}",
            config.runtime.pair.sell_symbol, config.runtime.pair.buy_symbol
        ),
        &config.runtime.notional_usd.normalize().to_string(),
    )
    .context("failed to build a single-pair recorder config from the runtime pair/notional")?;
    let recorder = ArcusSpotRecorder::new(client, recorder_config)
        .context("invalid Arcus recorder configuration")?;

    let snapshot = recorder.collect_once().await;
    let previous_sequence = runtime.state().sequence;
    let evaluation_time = Utc::now();
    let event = runtime.step_at(&snapshot, evaluation_time);

    // `propose` and `live-tick` are both checkpoint writers. Keep the shared
    // recovery boundary coherent for either writer so a successful proposal
    // cannot leave state-backup/state-verify-* rejecting a stale sidecar.
    if runtime.state().sequence != previous_sequence {
        write_observation_evidence(
            &config,
            snapshot.clone(),
            evaluation_time,
            ArcusSpotObservationBoundary {
                sequence: runtime.state().sequence,
                last_observation_at: runtime.state().last_observation_at,
            },
        )?;
    }

    // step_at mutates sequence/signal-window/risk state on every call, even
    // when it decides not to rotate. Persisting unconditionally -- not just
    // on a WouldRotate decision -- is what lets the signal window actually
    // warm up and stay warm across repeated propose invocations, and lets a
    // real risk-halt engagement observed here survive to the next
    // invocation instead of being silently discarded.
    store.persist(&runtime)?;

    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, &event).context("failed to serialize event")?;
    stdout.write_all(b"\n")?;

    match &event.decision {
        ArcusSpotDecision::WouldRotate { plan } => {
            if let Some(out_path) = out_path {
                let bytes = serde_json::to_vec_pretty(plan).context("failed to serialize plan")?;
                write_private_plan_file(Path::new(out_path), &bytes)
                    .with_context(|| format!("failed to write plan to {out_path}"))?;
                eprintln!(
                    "wrote a fresh plan to {out_path} -- run hash/sign-approval/execute well \
                     within the 60s HARD_MAX_PLAN_AGE_SECS window; every step from here \
                     (operator review, offline signing) eats into it"
                );
            }
            Ok(())
        }
        ArcusSpotDecision::Observe { hold } => {
            eprintln!("no rotation this tick: {:?} -- {}", hold.code, hold.detail);
            if out_path.is_some() {
                bail!(
                    "refusing to write a plan file: this tick observed rather than proposing \
                     a rotation"
                );
            }
            Ok(())
        }
        ArcusSpotDecision::SimulatedFill { .. } => {
            bail!("unexpected SimulatedFill decision from a mode=live runtime")
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [command, config_path, samples_path] if command == "bootstrap" => {
            bootstrap(config_path, samples_path)
        }
        [command, config_path] if command == "propose" => propose(config_path, None).await,
        [command, config_path, out_path] if command == "propose" => {
            propose(config_path, Some(out_path)).await
        }
        _ => bail!(usage()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_documents_both_subcommands() {
        assert!(usage().contains("bootstrap CONFIG_YAML SAMPLES_JSONL"));
        assert!(usage().contains("propose CONFIG_YAML [PLAN_JSON_OUT]"));
    }

    #[test]
    fn propose_config_ignores_unrelated_execute_once_fields() {
        // The whole point: the same CONFIG_YAML given to hash/execute (which
        // also carries chain/kms/executor/ledger_path) must parse here too,
        // picking out only router/runtime/runtime_state_path.
        let yaml = r#"
router:
  chain_id: 4663
chain:
  chain_id: 4663
  rpc_urls: ["https://rpc.example"]
kms:
  chain_id: 4663
  key_id: "test"
  region: "eu-central-1"
  expected_address: "0x0000000000000000000000000000000000000001"
executor:
  taker: "0x0000000000000000000000000000000000000001"
  permit2: "0x0000000000000000000000000000000000000002"
  slippage_bps: 50
  max_plan_age_secs: 30
  inventory_floor_raw: {}
  minimum_gas_balance_wei: "0"
ledger_path: /tmp/does-not-matter-ledger.json
runtime:
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
runtime_state_path: /tmp/does-not-matter-runtime.json
"#;
        let config: ArcusSpotProposeConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.runtime.pair.sell_symbol, "NVDA");
        assert_eq!(
            config.runtime_state_path,
            PathBuf::from("/tmp/does-not-matter-runtime.json")
        );
    }

    #[test]
    fn propose_config_rejects_a_relative_runtime_state_path() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        fs::write(
            &config_path,
            r#"
router:
  chain_id: 4663
runtime:
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
runtime_state_path: relative/path.json
"#,
        )
        .unwrap();
        match parse_config(&config_path) {
            Ok(_) => panic!("expected a relative-path rejection"),
            Err(error) => assert!(error.to_string().contains("must be absolute")),
        }
    }

    #[test]
    fn observation_evidence_is_stored_next_to_the_runtime_checkpoint() {
        let path = observation_evidence_path(Path::new("/var/lib/arcus/runtime.json")).unwrap();
        assert_eq!(
            path,
            PathBuf::from("/var/lib/arcus/live-tick-observation-evidence.json")
        );
    }
}
