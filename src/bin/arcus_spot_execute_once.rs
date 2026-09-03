//! Explicitly approved one-shot Arcus Spot execution.
//!
//! This binary is built only with arcus-spot-live. It has no loop or service
//! activation path: one invocation can consume exactly one fresh plan after
//! the validated config-and-plan SHA-256 digest is supplied again.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{bail, Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use chrono::{DateTime, NaiveDate, Utc};
#[cfg(test)]
use debot::arcus_spot::event_record;
use debot::arcus_spot::{
    build_arcus_spot_kms_signer, is_supported_live_route,
    manual_reconciled_runtime_fill_for_attempt, verify_archive_events, verify_record,
    ArcusSpotChainClient, ArcusSpotChainConfig, ArcusSpotDecision, ArcusSpotDirection,
    ArcusSpotExecutionAttempt, ArcusSpotExecutionLedger, ArcusSpotExecutionLedgerStore,
    ArcusSpotExecutionPhase, ArcusSpotInventory, ArcusSpotKmsConfig, ArcusSpotKmsSigner,
    ArcusSpotLiveExecutor, ArcusSpotLiveExecutorConfig, ArcusSpotLiveTickEventPublisher,
    ArcusSpotLiveTickEventRecord, ArcusSpotLiveTickEventStream, ArcusSpotRegime,
    ArcusSpotRiskHaltKind, ArcusSpotRotationPlan, ArcusSpotRotationTrigger, ArcusSpotRuntime,
    ArcusSpotRuntimeCheckpointStore, ArcusSpotRuntimeConfig, ArcusSpotRuntimeEvent,
    ArcusSpotRuntimeMode, ArcusSpotRuntimeState,
};
#[cfg(test)]
use debot::arcus_spot::{ArcusSpotBalanceSnapshot, ArcusSpotExecutionIntent, ArcusSpotHold};
use dex_connector::{
    ArcusSpotClient, ArcusSpotConfig, ArcusSpotRecorder, ArcusSpotRecorderConfig,
    ArcusSpotRecorderSnapshot,
};
use ed25519_dalek::{
    Signature, Signer, SigningKey, VerifyingKey, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH,
    SIGNATURE_LENGTH,
};
use ethers::types::{H256, U256};
use rand::RngCore;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    str::FromStr,
};
use zeroize::{Zeroize, Zeroizing};

// approval_public_key is deliberately NOT a field on this struct. It used
// to be, but that made it part of the exact payload the executor itself
// controls and includes in the approval digest: a host that can write
// CONFIG_YAML (the normal, routine deploy path for the rest of this
// config) could generate its own Ed25519 keypair, put the public half
// here, compute the resulting digest, sign it with the matching private
// key it also holds, and `execute` would accept that self-issued
// "approval" -- completely defeating the point of requiring a signature
// (Codex P1 follow-up, pairtrade#181, refining the initial signed-approval
// design). An environment variable turned out to be no better: anything
// invoking this binary directly controls its own process environment, so
// the same "executor identity" that can run `execute` at all could set
// its own value there too (Codex P1 follow-up, refining that fix again).
// The trust anchor now comes from a fixed file path this process cannot
// itself have written -- verified by ownership/permission bits the kernel
// enforces regardless of how this binary is invoked, unlike an inherited
// environment -- provisioned by an administrator (e.g. via SSM) separate
// from the routine config/plan deploy path (see docs/arcus-spot-runtime.md).
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotExecuteOnceConfig {
    router: ArcusSpotConfig,
    chain: ArcusSpotChainConfig,
    kms: ArcusSpotKmsConfig,
    executor: ArcusSpotLiveExecutorConfig,
    ledger_path: PathBuf,
    runtime: ArcusSpotRuntimeConfig,
    runtime_state_path: PathBuf,
}

const APPROVAL_PUBLIC_KEY_PATH: &str = "/etc/arcus-spot/approval_public_key";
const AUTO_EXECUTE_POLICY_PATH: &str = "/etc/arcus-spot/auto_execute_policy.json";

/// Administrator-approved digest binding *the entire* CONFIG_YAML for
/// `auto-execute`/`auto-resume`/`live-tick`, enforced independently of
/// whatever config the executor identity actually supplies. Read from the
/// same fixed, non-self-writable path pattern as `approval_public_key`.
///
/// An earlier version of this policy enumerated three fields
/// (`ledger_path`, `runtime_state_path`, `maximum_sell_amount_raw`)
/// individually. Codex correctly flagged that as insufficient (P1 follow-up,
/// pairtrade#186): every field the enumeration *didn't* cover --
/// `inventory_floor_raw`, `max_swaps_per_utc_day`, router/chain/token
/// identities, gas/slippage buffers, and any future field -- stayed fully
/// executor-controlled, so e.g. a lowered `inventory_floor_raw` could let an
/// unsigned plan violate the real floor, discoverable only after the
/// on-chain swap. A whole-config digest closes that class of gap by
/// construction: `auto-execute`/`auto-resume`/`live-tick` only ever run
/// against the byte-for-byte exact configuration an administrator approved,
/// the same trust model `execute`'s Ed25519 signature uses over
/// config+plan, just without the plan (which legitimately varies per swap
/// with fresh quotes) and without requiring a human in the loop per swap.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotAutoExecutePolicy {
    approved_config_sha256: String,
}

fn auto_execute_policy_from_admin_file() -> Result<ArcusSpotAutoExecutePolicy> {
    auto_execute_policy_from_file(Path::new(AUTO_EXECUTE_POLICY_PATH))
}

/// Opens `path` exactly once, refusing to follow a symlink at the final
/// path component (`O_NOFOLLOW`), and returns that same open file
/// alongside its `fstat`-sourced metadata. Callers validate that metadata
/// and then read from this exact file handle -- never a second, separate
/// path-based `stat`+`read`, which is racy whenever the identity running
/// this process can write the file's parent directory: between the check
/// and a later path-based read, that identity could delete the
/// already-validated trust anchor and put a symlink to attacker-controlled
/// content in its place, and the read would silently follow it (Codex P1
/// follow-up, pairtrade#186). Binding validation and read to the same
/// open file description closes that race by construction: `open()`
/// resolves the path exactly once, and everything after operates on the
/// resulting inode regardless of what happens to the path afterward.
fn open_regular_file_no_follow(path: &Path, label: &str) -> Result<(File, fs::Metadata)> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(err) if err.raw_os_error() == Some(libc::ELOOP) => {
            bail!(
                "{label} {} must be a regular non-symlink file",
                path.display()
            );
        }
        Err(err) => {
            return Err(err).with_context(|| format!("failed to open {label} {}", path.display()));
        }
    };
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {label} {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        );
    }
    Ok((file, metadata))
}

fn auto_execute_policy_from_file(path: &Path) -> Result<ArcusSpotAutoExecutePolicy> {
    let (mut file, metadata) = open_regular_file_no_follow(path, "auto-execute policy file")?;
    if metadata.permissions().mode() & 0o022 != 0 {
        bail!(
            "auto-execute policy file {} must not be group- or other-writable",
            path.display()
        );
    }
    // SAFETY: geteuid() takes no arguments, performs no memory access, and
    // cannot fail.
    let current_uid = unsafe { libc::geteuid() };
    if metadata.uid() == current_uid {
        bail!(
            "auto-execute policy file {} is owned by this process's own uid ({current_uid}) -- it must be administrator-owned, not writable by the identity running auto-execute/auto-resume",
            path.display()
        );
    }
    let mut raw = String::new();
    file.read_to_string(&mut raw)
        .with_context(|| format!("failed to read auto-execute policy file {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("invalid auto-execute policy file {}", path.display()))
}

/// Computes the same canonical digest form `approval_digest` uses for
/// config+plan, but over CONFIG_YAML alone -- this is what an administrator
/// hashes once to populate `approved_config_sha256`, and what every
/// `auto-execute`/`auto-resume`/`live-tick` invocation recomputes to compare
/// against it.
fn auto_execute_config_digest(config: &ArcusSpotExecuteOnceConfig) -> Result<String> {
    let canonical =
        serde_json::to_vec(config).context("failed to serialize config for policy digest")?;
    Ok(format!("sha256:{:x}", Sha256::digest(canonical)))
}

/// Rejects any config that is not byte-for-byte the one an administrator
/// approved, closing exactly the gap `execute`'s signature used to close
/// (Codex P1 follow-up, pairtrade#186) -- without this, the executor
/// identity could freely edit any field of its own CONFIG_YAML (ledger/
/// checkpoint paths to reset accumulated state, sell ceilings, inventory
/// floors, swap-per-day caps, router/chain/token identities, ...) and
/// `auto-execute`/`auto-resume`/`live-tick` would run against it unchecked.
fn require_config_within_auto_execute_policy(
    config: &ArcusSpotExecuteOnceConfig,
    policy: &ArcusSpotAutoExecutePolicy,
) -> Result<()> {
    let actual_digest = auto_execute_config_digest(config)?;
    if actual_digest != policy.approved_config_sha256 {
        bail!(
            "auto-execute config does not match the administrator-approved configuration (expected {}, got {actual_digest})",
            policy.approved_config_sha256
        );
    }
    Ok(())
}

/// Refuses a fresh-entry plan on the standalone `auto-execute` path.
///
/// `auto_execute_policy.json`'s config digest, `validate_plan_consistent_
/// with_state`'s regime/trigger check, and `execute_plan_once`'s preflight
/// (fresh-quote matching, inventory floors, slippage, staleness) all
/// authenticate *the execution*, not *the strategy decision*: none of them
/// re-derive whether entry_z_score was genuinely crossed, or re-check the
/// round-trip-cost, rotation-fraction, or inventory-imbalance gates
/// `ArcusSpotRuntime::step_at` itself enforces when it proposes a plan.
/// `execute`'s offline Ed25519 signature used to be what vouched for the
/// strategy decision underneath those numbers; `auto-execute` drops that
/// signature entirely, so a plan supplied here has *no* authenticated
/// provenance at all -- the executor identity could hand-craft one within
/// every check above and dispatch an entry the strategy never decided on
/// (Codex P1 follow-up, pairtrade#186).
///
/// `live-tick` does not go through this path: it builds its own plan from
/// `step_at` under the checkpoint lock immediately before dispatch, so that
/// provenance is inherent rather than merely asserted. A `MeanReversionExit`/
/// `MaxHoldExit` plan supplied here is still risk-reducing and already
/// bounded by `validate_plan_consistent_with_state` (cannot exceed the
/// genuinely open rotated quantity), so only entries are refused.
fn require_auto_execute_plan_is_not_a_fresh_entry(plan: &ArcusSpotRotationPlan) -> Result<()> {
    if plan.trigger == ArcusSpotRotationTrigger::EntrySignal {
        bail!(
            "auto-execute refuses an entry_signal plan: entries have no cryptographically or \
             checkpoint-provable link to a genuine strategy decision on this signatureless path. \
             Use `execute` with an offline-signed approval, or let `live-tick` dispatch the entry \
             it evaluates and builds itself."
        );
    }
    Ok(())
}

/// Read the trust anchor from a fixed, administrator-owned file this
/// process cannot itself have written. A caller-controlled input (a
/// config field, an inherited environment variable) can always be set to
/// whatever the caller wants by definition -- only file ownership/mode,
/// enforced by the kernel independent of how this binary was invoked,
/// can prove the *current* process lacks write access to it.
fn approval_public_key_from_admin_file() -> Result<VerifyingKey> {
    approval_public_key_from_file(Path::new(APPROVAL_PUBLIC_KEY_PATH))
}

fn approval_public_key_from_file(path: &Path) -> Result<VerifyingKey> {
    let (mut file, metadata) = open_regular_file_no_follow(path, "approval public key file")?;
    if metadata.permissions().mode() & 0o022 != 0 {
        bail!(
            "approval public key file {} must not be group- or other-writable",
            path.display()
        );
    }
    // SAFETY: geteuid() takes no arguments, performs no memory access, and
    // cannot fail.
    let current_uid = unsafe { libc::geteuid() };
    if metadata.uid() == current_uid {
        bail!(
            "approval public key file {} is owned by this process's own uid ({current_uid}) -- it must be administrator-owned, not writable by the identity running execute/resume",
            path.display()
        );
    }
    let mut raw = String::new();
    file.read_to_string(&mut raw)
        .with_context(|| format!("failed to read approval public key file {}", path.display()))?;
    parse_approval_public_key(raw.trim())
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
    let (mut file, metadata) = open_regular_file_no_follow(path, label)?;
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "{label} {} must not be readable or writable by group/other",
            path.display()
        );
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {label} {}", path.display()))?;
    Ok(bytes)
}

/// Atomically writes `bytes` to `path` at mode 0600 (temp file + rename +
/// parent-dir fsync), mirroring `ArcusSpotRuntimeCheckpointStore::persist`'s
/// pattern so a reader never observes a partially-written file.
fn write_private_regular_file_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
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
            .unwrap_or("write"),
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
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path).with_context(|| {
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

/// Fixed, deterministic path -- next to the runtime checkpoint it describes
/// -- where `live-tick` durably records the plan it is about to dispatch,
/// before dispatching it. `execute`/`auto-execute` always take PLAN_JSON as
/// an argument the caller already possesses; `live-tick` instead builds the
/// plan itself from a fresh strategy evaluation, so without this there is
/// nothing on disk for `auto-resume` to recover with if the process exits
/// after a `Submitted`-but-unconfirmed dispatch (Codex P2 follow-up,
/// pairtrade#186).
fn live_tick_pending_plan_path(config: &ArcusSpotExecuteOnceConfig) -> Result<PathBuf> {
    let parent = config
        .runtime_state_path
        .parent()
        .context("Arcus runtime_state_path has no parent")?;
    Ok(parent.join("live-tick-pending-plan.json"))
}

/// Recover an unattended live-tick attempt before accepting another market
/// observation. The plan file is the immutable strategy evidence that the
/// active ledger digest commits to; a later tick must never replace it while
/// the prior swap is still unresolved.
fn live_tick_active_recovery_plan(
    config: &ArcusSpotExecuteOnceConfig,
    ledger: &ArcusSpotExecutionLedger,
) -> Result<Option<(ArcusSpotRotationPlan, String)>> {
    let Some(active) = ledger.active.as_ref() else {
        return Ok(None);
    };
    let path = live_tick_pending_plan_path(config)?;
    let bytes = read_private_regular_file(&path, "Arcus active live-tick pending plan")?;
    let plan = plan_from_document(
        &bytes,
        &format!("Arcus active live-tick pending plan {}", path.display()),
    )?;
    let digest = approval_digest(config, &plan)?;
    if active.intent.plan_config_digest != digest {
        bail!("Arcus active execution attempt does not match its live-tick pending-plan evidence");
    }
    Ok(Some((plan, digest)))
}

fn load_live_tick_active_recovery_plan(
    config: &ArcusSpotExecuteOnceConfig,
    ledger_store: &ArcusSpotExecutionLedgerStore,
) -> Result<Option<(ArcusSpotRotationPlan, String)>> {
    let ledger = ledger_store.load_or_create(Utc::now())?;
    live_tick_active_recovery_plan(config, &ledger)
}

async fn resume_live_tick_attempt(
    config: &ArcusSpotExecuteOnceConfig,
    plan: ArcusSpotRotationPlan,
    plan_config_digest: String,
) -> Result<ArcusSpotExecutionAttempt> {
    let mut executor = executor_from_config(config).await?;
    let attempt = executor.resume_status_and_reconcile().await?;
    finalize_reconciled_attempt(config, &mut executor, &plan, &plan_config_digest, attempt)
}

async fn resume_active_live_tick_attempt(
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<Option<ArcusSpotExecutionAttempt>> {
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let lock = ledger_store.acquire_exclusive_lock(&config.runtime_state_path)?;
    let recovery = load_live_tick_active_recovery_plan(config, &ledger_store)?;
    drop(lock);

    let Some((plan, plan_config_digest)) = recovery else {
        return Ok(None);
    };
    Ok(Some(
        resume_live_tick_attempt(config, plan, plan_config_digest).await?,
    ))
}

/// Read-only diagnostic for an active execution attempt whose live-tick
/// pending-plan evidence was lost or overwritten (bot-strategy#869): scans an
/// operator-supplied, already fetch-and-verified durable event export
/// (`scripts/fetch_arcus_live_tick_events.sh`) for the `WouldRotate` plan
/// that produced the attempt, and reports it if -- and only if -- recomputing
/// `approval_digest` against the *current* config reproduces exactly the
/// digest the ledger recorded at dispatch time. That digest match is the same
/// check `live_tick_active_recovery_plan` performs; passing it here is proof
/// this is genuinely the plan that was signed and dispatched, not a
/// same-shaped guess.
///
/// This command never writes the ledger, the runtime checkpoint, or the
/// pending-plan file. On a confirmed match it prints the plan JSON an
/// operator can choose to write to the pending-plan path themselves, after
/// review, to let the ordinary resume path (auto-resume / next live-tick)
/// finish reconciliation on its own.
/// Read-only preview for `archive-rejected-apply` (bot-strategy#898):
/// reports the ledger's active attempt and whether it is eligible to be
/// archived, without mutating anything. Run this first, then pass the
/// sequence it reports to `archive-rejected-apply` to confirm you are
/// archiving the attempt you just reviewed and not a different one that
/// appeared in the meantime.
fn archive_rejected_report(config_path: &Path) -> Result<()> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;

    // archive-rejected-apply always checks CONFIG_YAML against
    // auto_execute_policy.json before doing anything else (same reasoning
    // as manual-reconcile-apply, pairtrade#241: nothing else here proves
    // CONFIG_YAML's ledger_path/runtime_state_path point at the genuine
    // production paths rather than a caller-fabricated redirect) -- check
    // it here too, first, so this report never claims eligibility for a
    // CONFIG_YAML apply would actually refuse outright.
    if let Err(error) = auto_execute_policy_from_admin_file()
        .and_then(|policy| require_config_within_auto_execute_policy(&config, &policy))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "policy_rejected",
                "detail": format!(
                    "archive-rejected-apply would refuse this CONFIG_YAML before doing anything \
                     else (auto_execute_policy.json check): {error:#}"
                ),
            }))
            .context("failed to serialize Arcus archive-rejected report")?
        );
        return Ok(());
    }

    // Pass the already-parsed, already-approved config object -- not
    // config_path -- so build_archive_rejected_report cannot re-read
    // CONFIG_YAML from disk a second time (same TOCTOU reasoning as
    // manual-reconcile-report, pairtrade#241).
    let report = build_archive_rejected_report(&config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .context("failed to serialize Arcus archive-rejected report")?
    );
    Ok(())
}

fn build_archive_rejected_report(config: &ArcusSpotExecuteOnceConfig) -> Result<serde_json::Value> {
    // Same exclusive lock a dispatching tick takes, so this read cannot
    // interleave with one committing a fill (mirrors build_repair_report).
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;
    let ledger = ledger_store.load_existing()?;

    let Some(active) = ledger.active.clone() else {
        return Ok(serde_json::json!({
            "status": "no_active_attempt",
            "detail": "the ledger has no active attempt; there is nothing to archive",
        }));
    };

    let eligible = active.phase == ArcusSpotExecutionPhase::Rejected && active.tx_hash.is_none();
    Ok(serde_json::json!({
        "status": if eligible { "eligible_to_archive" } else { "not_eligible" },
        "sequence": active.sequence,
        "phase": format!("{:?}", active.phase),
        "tx_hash": active.tx_hash,
        "detail": active.detail,
        "intent": active.intent,
        "reason_if_ineligible": if eligible {
            None
        } else {
            Some(
                "only a Rejected attempt with no tx_hash (nothing ever dispatched on-chain) can \
                 be archived by archive-rejected-apply; Dispatching/Unknown/Failed attempts, or \
                 a Rejected one with a tx_hash, need repair-report/manual-reconcile instead"
                    .to_string(),
            )
        },
    }))
}

/// Archives the ledger's active attempt once an operator has reviewed it
/// with `archive-rejected-report` and confirmed it is safe to clear
/// (bot-strategy#898). SEQUENCE must match the report's `sequence` exactly,
/// so a concurrent tick that started a new attempt between the report and
/// this call is refused rather than silently archiving the wrong one.
fn archive_rejected_apply(config_path: &Path, sequence: &str) -> Result<()> {
    let sequence: u64 = sequence
        .parse()
        .context("SEQUENCE must be the ledger's active attempt sequence number")?;

    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;
    // Same administrator-approval gate as auto-execute/auto-resume/
    // clear-risk-halt/manual-reconcile-apply: this path skips the offline
    // Ed25519 signature entirely, so nothing else here proves CONFIG_YAML's
    // ledger_path/runtime_state_path are the genuine production paths
    // rather than a caller-fabricated redirect.
    let policy = auto_execute_policy_from_admin_file()?;
    require_config_within_auto_execute_policy(&config, &policy)?;

    let result = commit_archive_rejected(&config, sequence)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result)
            .context("failed to serialize Arcus archive-rejected result")?
    );
    Ok(())
}

fn commit_archive_rejected(
    config: &ArcusSpotExecuteOnceConfig,
    sequence: u64,
) -> Result<serde_json::Value> {
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;
    let mut ledger = ledger_store.load_existing()?;

    let active = ledger
        .active
        .clone()
        .context("Arcus execution ledger has no active attempt")?;
    if active.sequence != sequence {
        bail!(
            "refusing to archive: caller expected sequence {sequence} but the ledger's active \
             attempt is sequence {} -- re-run archive-rejected-report and confirm before retrying",
            active.sequence
        );
    }

    ledger.archive_rejected()?;
    ledger_store.persist(&ledger)?;

    // Printed rather than merely done: this is the audit record of a
    // stuck, operator-reviewed attempt being cleared, and it lands in the
    // journal (mirrors clear-risk-halt's audit print).
    Ok(serde_json::json!({
        "archived": {
            "sequence": active.sequence,
            "phase": format!("{:?}", active.phase),
            "detail": active.detail,
            "intent": active.intent,
        },
        "ledger_path": config.ledger_path,
    }))
}

fn repair_report(config_path: &Path, events_jsonl_path: &Path) -> Result<()> {
    let report = build_repair_report(config_path, events_jsonl_path)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).context("failed to serialize Arcus repair report")?
    );
    Ok(())
}

fn build_repair_report(config_path: &Path, events_jsonl_path: &Path) -> Result<serde_json::Value> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;

    // Held for the rest of this function, not just the read: a
    // resume/execute/live-tick invocation racing an in-progress archive
    // scan could otherwise reconcile or archive this exact attempt (and
    // possibly start a new one) while this report still describes it as
    // active, then hand the operator instructions that would overwrite the
    // *new* attempt's pending-plan evidence with the stale plan recovered
    // here (Codex P2 follow-up, pairtrade#240).
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;
    let ledger = ledger_store.load_existing()?;

    let Some(active) = ledger.active.clone() else {
        return Ok(serde_json::json!({
            "status": "no_active_attempt",
            "detail": "the ledger has no active attempt; there is nothing to repair",
        }));
    };

    let events_bytes = fs::read(events_jsonl_path).with_context(|| {
        format!(
            "failed to read Arcus event export {}",
            events_jsonl_path.display()
        )
    })?;
    let events_text = String::from_utf8(events_bytes).with_context(|| {
        format!(
            "Arcus event export {} is not valid UTF-8",
            events_jsonl_path.display()
        )
    })?;

    #[derive(Serialize)]
    struct Candidate {
        sequence: u64,
        observed_at: DateTime<Utc>,
        venue: String,
        sell_symbol: String,
        buy_symbol: String,
        sell_amount_raw: String,
        plan_config_digest: String,
        digest_matches_ledger: bool,
    }

    let mut candidates = Vec::new();
    let mut matches = Vec::new();
    for (line_no, line) in events_text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let record: ArcusSpotLiveTickEventRecord =
            serde_json::from_str(line).with_context(|| {
                format!(
                    "{} line {} is not a valid Arcus event record",
                    events_jsonl_path.display(),
                    line_no + 1
                )
            })?;
        let event = verify_record(&record).with_context(|| {
            format!(
                "{} line {} failed event-record verification",
                events_jsonl_path.display(),
                line_no + 1
            )
        })?;
        let ArcusSpotDecision::WouldRotate { plan } = event.decision else {
            continue;
        };
        if !plan.venue.eq_ignore_ascii_case(&active.intent.venue)
            || !plan
                .sell_symbol
                .eq_ignore_ascii_case(&active.intent.sell_symbol)
            || !plan
                .buy_symbol
                .eq_ignore_ascii_case(&active.intent.buy_symbol)
            || plan.sell_amount_raw != active.intent.sell_amount_raw
        {
            continue;
        }
        let digest = approval_digest(&config, &plan)?;
        let digest_matches_ledger = digest == active.intent.plan_config_digest;
        candidates.push(Candidate {
            sequence: event.sequence,
            observed_at: event.observed_at,
            venue: plan.venue.clone(),
            sell_symbol: plan.sell_symbol.clone(),
            buy_symbol: plan.buy_symbol.clone(),
            sell_amount_raw: plan.sell_amount_raw.clone(),
            plan_config_digest: digest.clone(),
            digest_matches_ledger,
        });
        if digest_matches_ledger {
            matches.push(plan);
        }
    }

    let active_summary = serde_json::json!({
        "sequence": active.sequence,
        "phase": active.phase,
        "tx_hash": active.tx_hash,
        "dispatched_at": active.dispatched_at,
        "idempotency_key": active.idempotency_key,
        "plan_config_digest": active.intent.plan_config_digest,
    });

    let report = match matches.as_slice() {
        [plan] => {
            // resume_status_and_reconcile only accepts Submitted, Confirmed,
            // or Reconciled (live_executor.rs); every other phase bails.
            // Restoring the pending-plan file and pointing an operator at
            // auto-resume for e.g. Prepared/Dispatching/Unknown/OperatorHold/
            // Rejected/Failed would just fail again, and OperatorHold/Unknown
            // in particular need a human judgement call this tool cannot make
            // (Codex P2 follow-up, pairtrade#240).
            let resumable = matches!(
                active.phase,
                ArcusSpotExecutionPhase::Submitted
                    | ArcusSpotExecutionPhase::Confirmed
                    | ArcusSpotExecutionPhase::Reconciled
            );
            let next_steps = if resumable {
                vec![
                    "Confirm arcus-spot-live-tick.timer (and any manual execute/auto-execute/auto-resume/live-tick invocation) stays stopped from now through the write below -- this report's exclusive lock is released once it prints, so a concurrent invocation could otherwise reconcile/archive this exact attempt (and start a new one) before the file is restored.".to_string(),
                    format!(
                        "Immediately before writing, re-run repair-report and confirm `active_attempt` still has the same sequence ({}), idempotency_key, and tx_hash printed here; abort if any differ -- that means this attempt already moved on and the plan above is stale.",
                        active_summary["sequence"],
                    ),
                    format!(
                        "Then write the exact bytes of `recovered_plan` above to {} (mode 0600, owner `arcus`).",
                        live_tick_pending_plan_path(&config)?.display(),
                    ),
                    "Then either restart arcus-spot-live-tick.timer, or run `auto-resume CONFIG_YAML <that path>` directly, to let the existing resume path reconcile this attempt.".to_string(),
                    "`recovered_plan` is a bare ArcusSpotRotationPlan, not a full live-tick evidence envelope -- this tool cannot reconstruct the original recorder snapshot from the compact event archive. auto-resume/live-tick accept a bare plan (plan_from_document's legacy fallback), but state-verify-continuity requires the full envelope for any on-disk pending-plan file and will reject this one until the next live-tick dispatch overwrites it with a fresh full-envelope version -- expect that, don't treat it as a new incident.".to_string(),
                    "This command did not write anything itself; review the plan above against the production evidence in bot-strategy#869 before restoring it.".to_string(),
                ]
            } else {
                vec![
                    format!(
                        "The active attempt is in phase {:?}, which resume_status_and_reconcile does not accept (only Submitted/Confirmed/Reconciled) -- restoring the pending-plan file and running auto-resume would fail again.",
                        active.phase,
                    ),
                    "`recovered_plan` is still the digest-proven plan behind this attempt; use it as evidence for a manual decision (e.g. clear-risk-halt-style administrator action) rather than the ordinary resume path.".to_string(),
                    "This command did not write anything itself.".to_string(),
                ]
            };
            serde_json::json!({
                "status": "recovered",
                "active_attempt": active_summary,
                "candidates_scanned": candidates,
                "recovered_plan": plan,
                "resumable_via_auto_resume": resumable,
                "next_steps": next_steps,
            })
        }
        [] => serde_json::json!({
            "status": "no_digest_match",
            "active_attempt": active_summary,
            "candidates_scanned": candidates,
            "detail": "no WouldRotate event matching this attempt's venue/symbols/sell_amount_raw reproduced the ledger's plan_config_digest under the current config. Either the event export does not cover the dispatch time, or the config has changed since dispatch -- do not hand-construct a plan to force a match.",
        }),
        many => serde_json::json!({
            "status": "ambiguous",
            "active_attempt": active_summary,
            "candidates_scanned": candidates,
            "digest_matching_candidate_count": many.len(),
            "detail": "more than one candidate plan reproduced the ledger digest; refusing to pick one. This should not happen and needs manual review.",
        }),
    };
    Ok(report)
}

#[derive(Serialize)]
struct ManualReconcileCandidate {
    sequence: u64,
    observed_at: DateTime<Utc>,
    plan: ArcusSpotRotationPlan,
}

/// Coarse-match `WouldRotate` events for `manual-reconcile-*`
/// (bot-strategy#869), identically to `build_repair_report`'s own scan but
/// without computing or requiring a `plan_config_digest` match --
/// `manual-reconcile-*` exists precisely because that digest cannot be
/// reproduced for this incident class. Deliberately not shared code with
/// `build_repair_report`: the two paths must stay independently reviewable,
/// and neither should change behavior as a side effect of editing the
/// other.
fn scan_manual_reconcile_candidates(
    active: &ArcusSpotExecutionAttempt,
    events_jsonl_path: &Path,
) -> Result<Vec<ManualReconcileCandidate>> {
    let events_bytes = fs::read(events_jsonl_path).with_context(|| {
        format!(
            "failed to read Arcus event export {}",
            events_jsonl_path.display()
        )
    })?;
    if events_bytes.is_empty() {
        return Ok(Vec::new());
    }
    // Unlike repair-report's per-line verify_record scan, this must prove
    // the whole file is an unbroken, genuine slice of the real event
    // stream, not just that each line's own hashes are self-consistent:
    // there is no plan_config_digest downstream here to catch a spliced or
    // partially-forged file the way repair-report's digest match would
    // (Codex P2 follow-up, pairtrade#241). verify_archive_events requires a
    // continuous hash chain and a monotonic, gap-free sequence across every
    // record in the file, exactly like the on-host event stream's own
    // segment verification.
    let events = verify_archive_events(events_jsonl_path, &events_bytes).with_context(|| {
        format!(
            "{} failed archive verification",
            events_jsonl_path.display()
        )
    })?;
    let mut candidates = Vec::new();
    for event in events {
        let ArcusSpotDecision::WouldRotate { plan } = event.decision else {
            continue;
        };
        if !plan.venue.eq_ignore_ascii_case(&active.intent.venue)
            || !plan
                .sell_symbol
                .eq_ignore_ascii_case(&active.intent.sell_symbol)
            || !plan
                .buy_symbol
                .eq_ignore_ascii_case(&active.intent.buy_symbol)
            || plan.sell_amount_raw != active.intent.sell_amount_raw
        {
            continue;
        }
        candidates.push(ManualReconcileCandidate {
            sequence: event.sequence,
            observed_at: event.observed_at,
            plan,
        });
    }
    Ok(candidates)
}

/// `plan.direction`'s only other cross-check (`validate_plan`, at ordinary
/// dispatch time) never runs for a plan recovered from the archive after
/// the fact -- this is the manual-reconcile-only equivalent, re-derived
/// here rather than exposed from the library, so a hand-typed or
/// mis-scanned candidate whose direction is inconsistent with the
/// configured pair is rejected before it can flip the runtime regime the
/// wrong way.
fn require_plan_direction_matches_configured_pair(
    plan: &ArcusSpotRotationPlan,
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<()> {
    let pair = &config.runtime.pair;
    let (expected_sell, expected_buy) = match plan.direction {
        ArcusSpotDirection::TokenAToTokenB => (pair.sell_symbol.as_str(), pair.buy_symbol.as_str()),
        ArcusSpotDirection::TokenBToTokenA => (pair.buy_symbol.as_str(), pair.sell_symbol.as_str()),
    };
    if !plan.sell_symbol.eq_ignore_ascii_case(expected_sell)
        || !plan.buy_symbol.eq_ignore_ascii_case(expected_buy)
    {
        bail!(
            "candidate plan symbols {}/{} do not match the configured runtime pair {}/{} for \
             direction {:?}",
            plan.sell_symbol,
            plan.buy_symbol,
            pair.sell_symbol,
            pair.buy_symbol,
            plan.direction,
        );
    }
    Ok(())
}

/// Resolve exactly one archive-matching `WouldRotate` plan for `active`.
/// Refuses on zero or more than one match, exactly like `repair-report`'s
/// digest-based resolution refuses ambiguity, and additionally requires the
/// resolved plan's direction to agree with the configured runtime pair.
fn require_single_manual_reconcile_candidate(
    config: &ArcusSpotExecuteOnceConfig,
    active: &ArcusSpotExecutionAttempt,
    events_jsonl_path: &Path,
) -> Result<ArcusSpotRotationPlan> {
    let candidates = scan_manual_reconcile_candidates(active, events_jsonl_path)?;
    let plan = match candidates.as_slice() {
        [candidate] => candidate.plan.clone(),
        [] => bail!(
            "no WouldRotate event matching this attempt's venue/symbols/sell_amount_raw was found \
             in {} -- fetch a wider archive window before proceeding",
            events_jsonl_path.display(),
        ),
        many => bail!(
            "{} candidate WouldRotate events matched this attempt's venue/symbols/sell_amount_raw \
             in {}; refusing to pick one -- this needs manual review, not this tool",
            many.len(),
            events_jsonl_path.display(),
        ),
    };
    require_plan_direction_matches_configured_pair(&plan, config)?;
    Ok(plan)
}

fn manual_reconcile_report(
    config_path: &Path,
    events_jsonl_path: &Path,
    expected_sell_amount_raw: &str,
    expected_buy_amount_raw: &str,
) -> Result<()> {
    // manual-reconcile-apply always checks CONFIG_YAML against
    // auto_execute_policy.json before doing anything else (Codex P1
    // follow-up, pairtrade#241) -- check it here too, first, so this
    // report never claims "ready" (or any other status implying apply
    // would proceed) for a CONFIG_YAML apply would actually refuse
    // outright (Codex P2 follow-up, same PR).
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;
    if let Err(error) = auto_execute_policy_from_admin_file()
        .and_then(|policy| require_config_within_auto_execute_policy(&config, &policy))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "policy_rejected",
                "detail": format!(
                    "manual-reconcile-apply would refuse this CONFIG_YAML before doing anything \
                     else (auto_execute_policy.json check): {error:#}"
                ),
            }))
            .context("failed to serialize Arcus manual-reconcile report")?
        );
        return Ok(());
    }

    // Pass the already-parsed, already-approved config object -- not
    // config_path -- so build_manual_reconcile_report cannot re-read
    // CONFIG_YAML from disk a second time. A second path-based read would
    // reopen exactly the TOCTOU window auto_execute_policy.json exists to
    // close: whoever can write config_path could replace it between the
    // check above and that second read, and this report could then
    // evaluate (and print "ready" for) a CONFIG_YAML that was never
    // policy-approved (Codex P2 follow-up, pairtrade#241).
    let report = build_manual_reconcile_report(
        &config,
        events_jsonl_path,
        expected_sell_amount_raw,
        expected_buy_amount_raw,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .context("failed to serialize Arcus manual-reconcile report")?
    );
    Ok(())
}

/// Read-only preview for `manual-reconcile-apply` (bot-strategy#869): loads
/// the ledger under its exclusive lock (released once this prints, same
/// caveat as `repair-report` -- re-run immediately before `apply` and
/// confirm `active_attempt` is unchanged), resolves the one archive-matching
/// `WouldRotate` candidate for the active attempt, and -- if the attempt has
/// already reached `Reconciled` -- runs the exact pure computation
/// `manual-reconcile-apply` would commit, without writing anything. Never
/// polls chain status and never touches the runtime checkpoint or ledger.
///
/// Assumes the `auto_execute_policy.json` check has already passed --
/// `manual_reconcile_report` (the CLI wrapper) checks that first and
/// short-circuits before ever calling this, so every status this function
/// can return is one `manual-reconcile-apply` would actually reach. Takes
/// the already-parsed `config` object, not a path, so it can never re-read
/// CONFIG_YAML itself and reopen the TOCTOU window the policy check exists
/// to close (Codex P2 follow-up, pairtrade#241) -- tests construct/parse
/// `config` the same way `manual_reconcile_report` does and pass it in
/// directly, so this stays just as tempdir-testable as before.
fn build_manual_reconcile_report(
    config: &ArcusSpotExecuteOnceConfig,
    events_jsonl_path: &Path,
    expected_sell_amount_raw: &str,
    expected_buy_amount_raw: &str,
) -> Result<serde_json::Value> {
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;
    let ledger = ledger_store.load_existing()?;

    let Some(active) = ledger.active.clone() else {
        return Ok(serde_json::json!({
            "status": "no_active_attempt",
            "detail": "the ledger has no active attempt; there is nothing to reconcile",
        }));
    };
    let active_summary = serde_json::json!({
        "sequence": active.sequence,
        "phase": active.phase,
        "tx_hash": active.tx_hash,
        "idempotency_key": active.idempotency_key,
    });

    let plan = require_single_manual_reconcile_candidate(config, &active, events_jsonl_path)?;

    if active.phase != ArcusSpotExecutionPhase::Reconciled {
        // resume_status_and_reconcile (called by manual-reconcile-apply)
        // only accepts Submitted/Confirmed/Reconciled; every other phase
        // bails immediately without advancing or committing anything
        // (Codex P2 follow-up, pairtrade#241, mirroring the same
        // resumable-phase distinction build_repair_report already makes).
        let resumable = matches!(
            active.phase,
            ArcusSpotExecutionPhase::Submitted | ArcusSpotExecutionPhase::Confirmed
        );
        let detail = if resumable {
            format!(
                "the active attempt is in phase {:?}; manual-reconcile-apply will first call \
                 resume_status_and_reconcile (pure on-chain status/balance reads -- never signs or \
                 submits a transaction) to advance it, then commit using this candidate plan and \
                 the amounts you supply. This report cannot cross-check your expected amounts yet \
                 because post-swap balances are not recorded until the attempt reaches Reconciled; \
                 re-run this report after apply's resume step lands there.",
                active.phase,
            )
        } else {
            format!(
                "the active attempt is in phase {:?}, which resume_status_and_reconcile does not \
                 accept (only Submitted/Confirmed/Reconciled) -- manual-reconcile-apply would fail \
                 immediately without advancing or committing anything for this phase. This needs a \
                 manual operator decision (e.g. clear-risk-halt-style administrator action), not \
                 this tool.",
                active.phase,
            )
        };
        return Ok(serde_json::json!({
            "status": if resumable { "not_yet_reconciled" } else { "not_resumable" },
            "active_attempt": active_summary,
            "candidate_plan": plan,
            "detail": detail,
        }));
    }

    let sell_token_decimals =
        trusted_token_decimals_for_address(config, &plan.sell_symbol, &active.intent.sell_token)?;
    let buy_token_decimals =
        trusted_token_decimals_for_address(config, &plan.buy_symbol, &active.intent.buy_token)?;
    let fill = match manual_reconciled_runtime_fill_for_attempt(
        &active,
        &plan,
        expected_sell_amount_raw,
        expected_buy_amount_raw,
        sell_token_decimals,
        buy_token_decimals,
    ) {
        Ok(fill) => fill,
        Err(error) => {
            return Ok(serde_json::json!({
                "status": "would_fail",
                "active_attempt": active_summary,
                "candidate_plan": plan,
                "detail": format!("manual-reconcile-apply would refuse this: {error:#}"),
            }));
        }
    };
    // manual_reconciled_runtime_fill_for_attempt only derives quantities and
    // checks ledger/balance deltas -- it does not run
    // apply_confirmed_live_fill_once's own further checks (exact
    // sell-quantity equality against the candidate plan, fill-predates-quote
    // ordering, regime/trigger consistency, open-quantity for an exit,
    // inventory floors). Run the real commit function here too, against a
    // throwaway clone of the actual runtime checkpoint that is never
    // persisted, so "ready" means apply's commit step would actually
    // succeed, not just that a fill could be computed.
    //
    // Deliberately the *only* call dry-run here, exactly matching what
    // finalize_manual_reconciled_attempt itself calls for real. An earlier
    // round of this fix also pre-checked validate_plan_consistent_with_state,
    // which is wrong: that function's risk-halt guard exists to block a
    // *new* EntrySignal dispatch while a halt is engaged, but this attempt
    // already executed on-chain -- a halt engaged afterward (while the
    // checkpoint still shows Neutral because the fill was never committed)
    // must not block reconciling it, and finalize_manual_reconciled_attempt
    // never checked for that halt either. apply_confirmed_live_fill_once
    // already enforces every check that legitimately applies to a commit
    // (regime/trigger consistency and open-quantity-for-an-exit included)
    // on its own, and separately short-circuits safely to Ok(false) without
    // any further validation when last_live_execution_idempotency_key
    // already equals this fill's key -- the crashed-invocation recovery
    // case a prior round of this fix needed its own extra check for is
    // handled by that short-circuit alone now (Codex P2 follow-up,
    // pairtrade#241, correcting the over-restrictive check added in an
    // earlier round).
    //
    // load_existing, not load_or_create: this attempt was dispatched
    // against a checkpoint that must already exist. Silently constructing
    // a fresh one from initial_inventory on a missing/lost checkpoint file
    // would validate and (in apply) persist a commit against the wrong
    // starting state, discarding whatever real tracked inventory/regime/
    // signal history/risk state the checkpoint held -- checkpoint loss
    // must surface as an explicit recovery condition, never an implicit
    // reset (Codex P1 follow-up, pairtrade#241).
    let mut dry_run_runtime =
        ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .load_existing(&config.runtime)?;
    if let Err(error) = dry_run_runtime.apply_confirmed_live_fill_once(
        &plan,
        fill.actual_sell_quantity,
        fill.actual_buy_quantity,
        fill.reconciled_at,
        &fill.idempotency_key,
    ) {
        return Ok(serde_json::json!({
            "status": "would_fail",
            "active_attempt": active_summary,
            "candidate_plan": plan,
            "detail": format!(
                "manual-reconcile-apply would refuse this: failed to commit the reconciled fill \
                 to runtime state: {error}"
            ),
        }));
    }
    // dry_run_runtime committed cleanly above (or was already committed by
    // an earlier crashed invocation) and is discarded here without ever
    // being persisted -- apply's real commit (finalize_manual_reconciled_attempt)
    // runs the identical calls against the real, persisted checkpoint.
    Ok(serde_json::json!({
        "status": "ready",
        "active_attempt": active_summary,
        "candidate_plan": plan,
        "proposed_fill": {
            "actual_sell_quantity": fill.actual_sell_quantity,
            "actual_buy_quantity": fill.actual_buy_quantity,
            "reconciled_at": fill.reconciled_at,
            "idempotency_key": fill.idempotency_key,
        },
        "next_steps": [
            "This command wrote nothing -- the ledger, runtime checkpoint, and pending-plan \
             file are all untouched.",
            format!(
                "Confirm arcus-spot-live-tick.timer (and any manual execute/auto-execute/\
                 auto-resume/live-tick/manual-reconcile-apply invocation) stays stopped until \
                 you run manual-reconcile-apply -- a concurrent invocation could otherwise \
                 archive this exact attempt before apply runs.",
            ),
            format!(
                "Immediately before running manual-reconcile-apply, re-run this report and \
                 confirm active_attempt still has the same sequence ({}), idempotency_key, and \
                 tx_hash printed here; abort if any differ.",
                active_summary["sequence"],
            ),
            "Then run manual-reconcile-apply with this attempt's exact sequence/idempotency_key/\
             tx_hash and the same EVENTS_JSONL/EXPECTED_*_AMOUNT_RAW to commit proposed_fill \
             above and archive the attempt.".to_string(),
        ],
    }))
}

/// Commit an already-`Reconciled` attempt's runtime fill via
/// `manual_reconciled_runtime_fill` (the digest-bypass path), mirroring
/// `finalize_reconciled_attempt` exactly except for that one substitution.
/// Deliberately not shared with `finalize_reconciled_attempt`: nothing in
/// the automated `execute`/`auto-execute`/`resume`/`auto-resume`/`live-tick`
/// flow should be able to reach the digest bypass by construction, not just
/// by which arguments happen to be passed.
/// The administrator-pinned decimals for `symbol`, from
/// `CONFIG_YAML.router.trusted_token_decimals` -- covered by the same
/// `auto_execute_policy.json` digest `manual-reconcile-apply`/
/// `manual-reconcile-report` already require, so it is trustworthy
/// independent of anything an archive candidate plan claims (Codex P2
/// follow-up, pairtrade#241).
/// `symbol` alone is not enough to trust `config`'s decimals pin for it:
/// if CONFIG_YAML has been legitimately updated (a new
/// auto_execute_policy.json-approved config) since this attempt was
/// dispatched -- e.g. the symbol registry now resolves `symbol` to a
/// different ERC-20 contract with different decimals -- a symbol-only
/// lookup would silently return the *new* contract's decimals while
/// `active.intent`/`plan` still describe the swap against the *old* one,
/// converting the real settled raw amount at the wrong scale.
/// `apply_confirmed_live_fill_once` does not itself catch this (it never
/// re-derives quantities from raw amounts), so this must be checked here:
/// require the symbol's *currently configured* address to match the
/// address the attempt was actually signed and dispatched against before
/// trusting its decimals pin (Codex P1 follow-up, pairtrade#241).
fn trusted_token_decimals_for_address(
    config: &ArcusSpotExecuteOnceConfig,
    symbol: &str,
    expected_address: &str,
) -> Result<u32> {
    let configured_address = config
        .router
        .trusted_token_addresses
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
        .map(|(_, address)| address)
        .with_context(|| format!("Arcus manual-reconcile has no address pin for {symbol}"))?;
    if !configured_address.eq_ignore_ascii_case(expected_address) {
        bail!(
            "Arcus manual-reconcile decimals pin for {symbol} ({configured_address}) does not \
             match the address this attempt was actually dispatched against \
             ({expected_address}) -- the symbol registry has moved since this attempt was \
             signed; refusing to guess its decimals"
        );
    }
    config
        .router
        .trusted_token_decimals
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
        .map(|(_, decimals)| *decimals)
        .with_context(|| format!("Arcus manual-reconcile has no decimals pin for {symbol}"))
}

fn finalize_manual_reconciled_attempt(
    config: &ArcusSpotExecuteOnceConfig,
    executor: &mut ArcusSpotLiveExecutor<ArcusSpotKmsSigner>,
    plan: &ArcusSpotRotationPlan,
    expected_sell_amount_raw: &str,
    expected_buy_amount_raw: &str,
    attempt: ArcusSpotExecutionAttempt,
) -> Result<ArcusSpotExecutionAttempt> {
    if attempt.phase != ArcusSpotExecutionPhase::Reconciled {
        return Ok(attempt);
    }
    let sell_token_decimals =
        trusted_token_decimals_for_address(config, &plan.sell_symbol, &attempt.intent.sell_token)?;
    let buy_token_decimals =
        trusted_token_decimals_for_address(config, &plan.buy_symbol, &attempt.intent.buy_token)?;
    let fill = executor.manual_reconciled_runtime_fill(
        plan,
        expected_sell_amount_raw,
        expected_buy_amount_raw,
        sell_token_decimals,
        buy_token_decimals,
    )?;
    // load_existing, not load_or_create (Codex P1 follow-up, pairtrade#241):
    // see build_manual_reconcile_report's identical dry-run for why.
    let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    let mut runtime = store.load_existing(&config.runtime)?;
    runtime
        .apply_confirmed_live_fill_once(
            plan,
            fill.actual_sell_quantity,
            fill.actual_buy_quantity,
            fill.reconciled_at,
            &fill.idempotency_key,
        )
        .map_err(anyhow::Error::msg)
        .context("failed to commit manually reconciled Arcus fill to runtime state")?;
    store.persist(&runtime)?;
    executor.archive_reconciled_after_runtime_commit()?;
    Ok(attempt)
}

/// Requires `active` to be exactly the attempt the caller intends to touch,
/// pinned by sequence/idempotency_key/tx_hash, before `manual-reconcile-apply`
/// resumes or commits anything -- a stale invocation against an attempt that
/// already moved on (archived and replaced by a fresh one, for instance)
/// must fail closed instead of acting on the wrong attempt.
fn require_active_attempt_matches_pins(
    active: &ArcusSpotExecutionAttempt,
    expected_sequence: u64,
    expected_idempotency_key: &str,
    expected_tx_hash: &str,
) -> Result<()> {
    let tx_hash_matches = active
        .tx_hash
        .as_deref()
        .map(|hash| hash.eq_ignore_ascii_case(expected_tx_hash.trim()))
        .unwrap_or(false);
    if active.sequence != expected_sequence
        || active.idempotency_key != expected_idempotency_key.trim()
        || !tx_hash_matches
    {
        bail!(
            "the ledger's active attempt (sequence {}, idempotency_key {}, tx_hash {:?}) does not \
             match the SEQUENCE/IDEMPOTENCY_KEY/TX_HASH given on the command line -- this attempt \
             already moved on, or the wrong attempt was targeted; re-run manual-reconcile-report \
             and confirm before retrying",
            active.sequence,
            active.idempotency_key,
            active.tx_hash,
        );
    }
    Ok(())
}

/// The write path for `manual-reconcile-apply` (bot-strategy#869). Requires
/// CONFIG_YAML to match `auto_execute_policy.json`'s administrator-approved
/// digest -- the same gate `auto-execute`/`auto-resume`/`clear-risk-halt`
/// enforce, and load-bearing here for the same reason: this path skips both
/// the offline signature and the plan_config_digest match, so nothing else
/// proves `ledger_path`/`runtime_state_path` are the genuine production
/// paths rather than a caller-fabricated ledger paired with the real
/// checkpoint (Codex P1 follow-up, pairtrade#241). Also requires the caller
/// to pin the exact attempt (`expected_sequence`/`expected_idempotency_key`/
/// `expected_tx_hash`) so a stale invocation against an attempt that
/// already moved on fails closed instead of acting on the wrong one. Calls
/// `resume_status_and_reconcile` (pure on-chain status/balance reads, never
/// a new signature or submission) to advance Submitted/Confirmed toward
/// Reconciled exactly like `auto-resume` does, then -- only once Reconciled
/// -- commits via the digest-bypass path instead of the ordinary
/// digest-checked one.
async fn manual_reconcile_apply(
    config_path: &Path,
    events_jsonl_path: &Path,
    expected_sell_amount_raw: &str,
    expected_buy_amount_raw: &str,
    expected_sequence: &str,
    expected_idempotency_key: &str,
    expected_tx_hash: &str,
) -> Result<()> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;
    // Same administrator-approval gate as auto-execute/auto-resume/
    // clear-risk-halt (Codex P1 follow-up, pairtrade#241): this path skips
    // both the offline Ed25519 signature *and* the plan_config_digest match
    // by design, so nothing else here proves CONFIG_YAML itself -- in
    // particular ledger_path and runtime_state_path -- is the genuine
    // production config rather than one redirecting ledger_path at a
    // caller-fabricated, already-Reconciled ledger while keeping the real
    // production runtime_state_path, which would let this command commit a
    // wholly fictitious fill to the real checkpoint without ever reading
    // the chain.
    let policy = auto_execute_policy_from_admin_file()?;
    require_config_within_auto_execute_policy(&config, &policy)?;
    let expected_sequence: u64 = expected_sequence
        .trim()
        .parse()
        .context("SEQUENCE must be a non-negative integer")?;

    let mut executor = executor_from_config(&config).await?;
    let active = executor
        .ledger()
        .active
        .clone()
        .context("Arcus execution ledger has no active attempt to manually reconcile")?;
    require_active_attempt_matches_pins(
        &active,
        expected_sequence,
        expected_idempotency_key,
        expected_tx_hash,
    )?;
    let plan = require_single_manual_reconcile_candidate(&config, &active, events_jsonl_path)?;

    let attempt = executor.resume_status_and_reconcile().await?;
    let attempt = finalize_manual_reconciled_attempt(
        &config,
        &mut executor,
        &plan,
        expected_sell_amount_raw,
        expected_buy_amount_raw,
        attempt,
    )?;
    write_attempt(&attempt)
}

fn declined_route_log_path(config: &ArcusSpotExecuteOnceConfig) -> Result<PathBuf> {
    let parent = config
        .runtime_state_path
        .parent()
        .context("Arcus runtime_state_path has no parent")?;
    Ok(parent.join("declined-routes.jsonl"))
}

/// Append one record of an entry the router priced onto a venue this
/// executor may not take (bot-strategy#818).
///
/// The constraint costs roughly two thirds of entries, and counting them
/// does not say what they were worth: if the declined signals were the weak
/// ones the surviving third flatters the strategy, and if they were the
/// strong ones it understates it. Nothing in the live path can tell those
/// apart, and a shadow position tracker inside a signing bot is far more
/// machinery than the question deserves. So record what a decline *was* --
/// when, which way, how strong, at what size and price -- and price the
/// counterfactual offline against the recorder archive, which is already
/// collected and shipped to S3 continuously.
///
/// Failure to write is reported and then ignored. This file is analysis,
/// not safety: failing a tick over it would recreate, for a strictly less
/// important reason, exactly the "correct behaviour reported as a fault"
/// problem that #817 just removed.
fn decline_unsupported_route(
    config: &ArcusSpotExecuteOnceConfig,
    event: &ArcusSpotRuntimeEvent,
    plan: &ArcusSpotRotationPlan,
) -> Result<()> {
    record_declined_route(config, event, plan);
    eprintln!(
        "[arcus-route] declined a would-rotate plan: recommended venue {:?} is not one of the \
         validated Arcus/Rialto routes this executor may dispatch; nothing was submitted",
        plan.venue,
    );
    write_live_tick_event(event)
}

fn record_declined_route(
    config: &ArcusSpotExecuteOnceConfig,
    event: &ArcusSpotRuntimeEvent,
    plan: &ArcusSpotRotationPlan,
) {
    let record = serde_json::json!({
        "declined_at": event.observed_at,
        "sequence": event.sequence,
        "pair": event.pair,
        "z_score": event.z_score,
        "trigger": plan.trigger,
        "direction": plan.direction,
        "recommended_venue": plan.venue,
        "sell_symbol": plan.sell_symbol,
        "buy_symbol": plan.buy_symbol,
        "sell_quantity": plan.sell_quantity.to_string(),
        "buy_quantity": plan.buy_quantity.to_string(),
        "sell_amount_raw": plan.sell_amount_raw,
        "buy_amount_raw": plan.buy_amount_raw,
        "quote_received_at": plan.quote_received_at,
        "optimistic_round_trip_loss_bps": plan.optimistic_round_trip_loss_bps.to_string(),
        "all_in_round_trip_cost_bps": plan.all_in_round_trip_cost_bps.to_string(),
        "token_a_reference_price_usd": event.token_a_reference_price_usd.map(|p| p.to_string()),
        "token_b_reference_price_usd": event.token_b_reference_price_usd.map(|p| p.to_string()),
    });
    if let Err(error) = append_declined_route(config, &record) {
        eprintln!("[arcus-route] failed to record the declined route: {error:#}");
    }
}

fn append_declined_route(
    config: &ArcusSpotExecuteOnceConfig,
    record: &serde_json::Value,
) -> Result<()> {
    let path = declined_route_log_path(config)?;
    let mut line = serde_json::to_vec(record).context("failed to serialize the declined route")?;
    line.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.write_all(&line)
        .with_context(|| format!("failed to append to {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to flush {}", path.display()))
}

fn live_tick_observation_evidence_path(config: &ArcusSpotExecuteOnceConfig) -> Result<PathBuf> {
    let parent = config
        .runtime_state_path
        .parent()
        .context("Arcus runtime_state_path has no parent")?;
    Ok(parent.join("live-tick-observation-evidence.json"))
}

fn live_tick_event_stream(
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<ArcusSpotLiveTickEventStream> {
    let parent = config
        .runtime_state_path
        .parent()
        .context("Arcus runtime_state_path has no parent")?;
    Ok(ArcusSpotLiveTickEventStream::new(
        parent.join("live-tick-events"),
    ))
}

fn live_tick_pending_event_path(config: &ArcusSpotExecuteOnceConfig) -> Result<PathBuf> {
    let parent = config
        .runtime_state_path
        .parent()
        .context("Arcus runtime_state_path has no parent")?;
    Ok(parent.join("live-tick-event-pending.json"))
}

fn live_tick_event_publisher(
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<ArcusSpotLiveTickEventPublisher> {
    Ok(ArcusSpotLiveTickEventPublisher::new(
        live_tick_event_stream(config)?,
        live_tick_pending_event_path(config)?,
    ))
}

const LIVE_TICK_EVIDENCE_SCHEMA_VERSION: u32 = 1;
const OBSERVATION_EVIDENCE_SCHEMA_VERSION: u32 = 2;

/// Atomic recovery document written by `live-tick`. Keeping the raw recorder
/// snapshot beside the resulting plan lets continuity verification replay the
/// planner from the pre-tick checkpoint instead of trusting strategy fields
/// (especially the round-trip loss) written by a rollback candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotLiveTickEvidence {
    schema_version: u32,
    evaluation_time: DateTime<Utc>,
    snapshot: ArcusSpotRecorderSnapshot,
    plan: ArcusSpotRotationPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotLiveTickObservationEvidence {
    schema_version: u32,
    evaluation_time: DateTime<Utc>,
    snapshot: ArcusSpotRecorderSnapshot,
    /// Schema 2 binds the sidecar to the checkpoint state produced by the
    /// same `step_at` call. Schema-1 files omit this field and remain readable
    /// for rolling upgrades.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resulting_runtime: Option<ArcusSpotObservationBoundary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArcusSpotObservationBoundary {
    sequence: u64,
    last_observation_at: Option<DateTime<Utc>>,
}

fn validate_live_tick_evidence_schema(evidence: &ArcusSpotLiveTickEvidence) -> Result<()> {
    if evidence.schema_version != LIVE_TICK_EVIDENCE_SCHEMA_VERSION {
        bail!(
            "unsupported Arcus live-tick evidence schema {}; expected {}",
            evidence.schema_version,
            LIVE_TICK_EVIDENCE_SCHEMA_VERSION
        );
    }
    Ok(())
}

/// Recovery commands also accept legacy standalone plan JSON supplied by an
/// operator. Newly generated live-tick recovery files use the evidence
/// envelope above, while continuity acceptance deliberately requires it.
fn plan_from_document(bytes: &[u8], label: &str) -> Result<ArcusSpotRotationPlan> {
    if let Ok(evidence) = serde_json::from_slice::<ArcusSpotLiveTickEvidence>(bytes) {
        validate_live_tick_evidence_schema(&evidence)?;
        return Ok(evidence.plan);
    }
    serde_json::from_slice::<ArcusSpotRotationPlan>(bytes)
        .with_context(|| format!("invalid {label}"))
}

fn live_tick_evidence_from_document(
    bytes: &[u8],
    label: &str,
) -> Result<ArcusSpotLiveTickEvidence> {
    let evidence: ArcusSpotLiveTickEvidence = serde_json::from_slice(bytes)
        .with_context(|| format!("invalid {label}: recorder evidence is required"))?;
    validate_live_tick_evidence_schema(&evidence)?;
    Ok(evidence)
}

fn observation_evidence_from_document(
    bytes: &[u8],
    label: &str,
) -> Result<ArcusSpotLiveTickObservationEvidence> {
    let evidence: ArcusSpotLiveTickObservationEvidence =
        serde_json::from_slice(bytes).with_context(|| format!("invalid {label}"))?;
    match (evidence.schema_version, &evidence.resulting_runtime) {
        (LIVE_TICK_EVIDENCE_SCHEMA_VERSION, None)
        | (OBSERVATION_EVIDENCE_SCHEMA_VERSION, Some(_)) => {}
        (LIVE_TICK_EVIDENCE_SCHEMA_VERSION, Some(_)) => {
            bail!("Arcus observation evidence schema 1 must not contain a runtime boundary")
        }
        (OBSERVATION_EVIDENCE_SCHEMA_VERSION, None) => {
            bail!("Arcus observation evidence schema 2 requires a runtime boundary")
        }
        (version, _) => bail!(
            "unsupported Arcus live-tick observation evidence schema {version}; expected 1 or {}",
            OBSERVATION_EVIDENCE_SCHEMA_VERSION
        ),
    }
    Ok(evidence)
}

fn observation_evidence_matches_runtime(
    evidence: &ArcusSpotLiveTickObservationEvidence,
    runtime: &ArcusSpotRuntimeState,
) -> bool {
    match &evidence.resulting_runtime {
        Some(boundary) => {
            boundary.sequence == runtime.sequence
                && boundary.last_observation_at == runtime.last_observation_at
        }
        None => runtime.last_observation_at == Some(evidence.snapshot.collection_finished_at),
    }
}

fn require_current_observation_evidence_schema(
    evidence: &ArcusSpotLiveTickObservationEvidence,
) -> Result<()> {
    if evidence.schema_version != OBSERVATION_EVIDENCE_SCHEMA_VERSION {
        bail!(
            "Arcus current sequence-advancing observation evidence must use schema {}",
            OBSERVATION_EVIDENCE_SCHEMA_VERSION
        );
    }
    Ok(())
}

/// Evidence is published before its checkpoint. If the checkpoint write then
/// fails or the process exits, exactly one newer schema-2 boundary can remain.
/// Treat only that narrowly identified case as an ignorable orphan; every
/// other mismatch remains a hard error.
fn observation_evidence_is_newer_orphan(
    evidence: &ArcusSpotLiveTickObservationEvidence,
    runtime: &ArcusSpotRuntimeState,
) -> bool {
    let Some(boundary) = &evidence.resulting_runtime else {
        return false;
    };
    if runtime.sequence.checked_add(1) != Some(boundary.sequence) {
        return false;
    }
    match (runtime.last_observation_at, boundary.last_observation_at) {
        (Some(current), Some(boundary)) => boundary >= current,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

const STATE_BACKUP_SCHEMA_VERSION: u32 = 3;
const STATE_BACKUP_MANIFEST: &str = "manifest.json";
const STATE_BACKUP_CHECKPOINT: &str = "runtime_state.json";
const STATE_BACKUP_LEDGER: &str = "ledger.json";
const STATE_BACKUP_PENDING_PLAN: &str = "live-tick-pending-plan.json";
const STATE_BACKUP_OBSERVATION_EVIDENCE: &str = "live-tick-observation-evidence.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArcusSpotStateBackupFile {
    sha256: String,
    size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ArcusSpotRuntimeStateSummary {
    sequence: u64,
    relative_log_price_history_len: usize,
    last_observation_at: Option<DateTime<Utc>>,
    inventory: ArcusSpotInventory,
    regime: ArcusSpotRegime,
    last_rotation_at: Option<DateTime<Utc>>,
    rotated_quantity: Option<Decimal>,
    last_live_execution_idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArcusSpotLedgerStateSummary {
    next_sequence: u64,
    history_len: usize,
    active_sequence: Option<u64>,
    active_phase: Option<ArcusSpotExecutionPhase>,
    active_idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ArcusSpotStateBackupManifest {
    schema_version: u32,
    captured_at: DateTime<Utc>,
    config_sha256: String,
    runtime_checkpoint: ArcusSpotStateBackupFile,
    execution_ledger: ArcusSpotStateBackupFile,
    pending_plan: Option<ArcusSpotStateBackupFile>,
    observation_evidence: Option<ArcusSpotStateBackupFile>,
    runtime: ArcusSpotRuntimeStateSummary,
    ledger: ArcusSpotLedgerStateSummary,
}

struct ArcusSpotStateImage {
    checkpoint_bytes: Vec<u8>,
    ledger_bytes: Vec<u8>,
    pending_plan_bytes: Option<Vec<u8>>,
    observation_evidence_bytes: Option<Vec<u8>>,
    runtime: ArcusSpotRuntime,
    ledger: ArcusSpotExecutionLedger,
}

#[derive(Debug, Serialize)]
struct ArcusSpotStateVerificationReport {
    status: &'static str,
    mode: &'static str,
    config_sha256: String,
    runtime_checkpoint_sha256: String,
    execution_ledger_sha256: String,
    pending_plan_sha256: Option<String>,
    observation_evidence_sha256: Option<String>,
    runtime: ArcusSpotRuntimeStateSummary,
    ledger: ArcusSpotLedgerStateSummary,
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn state_backup_file(bytes: &[u8]) -> ArcusSpotStateBackupFile {
    ArcusSpotStateBackupFile {
        sha256: sha256_prefixed(bytes),
        size_bytes: bytes.len() as u64,
    }
}

fn runtime_state_summary(runtime: &ArcusSpotRuntime) -> ArcusSpotRuntimeStateSummary {
    let state = runtime.state();
    ArcusSpotRuntimeStateSummary {
        sequence: state.sequence,
        relative_log_price_history_len: state.relative_log_price_history.len(),
        last_observation_at: state.last_observation_at,
        inventory: state.inventory,
        regime: state.regime,
        last_rotation_at: state.last_rotation_at,
        rotated_quantity: state.rotated_quantity,
        last_live_execution_idempotency_key: state.last_live_execution_idempotency_key.clone(),
    }
}

fn ledger_state_summary(ledger: &ArcusSpotExecutionLedger) -> ArcusSpotLedgerStateSummary {
    ArcusSpotLedgerStateSummary {
        next_sequence: ledger.next_sequence,
        history_len: ledger.history.len(),
        active_sequence: ledger.active.as_ref().map(|attempt| attempt.sequence),
        active_phase: ledger.active.as_ref().map(|attempt| attempt.phase),
        active_idempotency_key: ledger
            .active
            .as_ref()
            .map(|attempt| attempt.idempotency_key.clone()),
    }
}

fn require_private_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {label} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} {} must be a non-symlink directory", path.display());
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "{label} {} must not be readable or writable by group/other",
            path.display()
        );
    }
    Ok(())
}

fn read_optional_private_plan(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            let bytes = read_private_regular_file(path, "Arcus pending plan")?;
            plan_from_document(&bytes, &format!("Arcus pending plan {}", path.display()))?;
            Ok(Some(bytes))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error)
            .with_context(|| format!("failed to inspect Arcus pending plan {}", path.display())),
    }
}

fn read_optional_private_observation_evidence(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            let bytes = read_private_regular_file(path, "Arcus observation evidence")?;
            observation_evidence_from_document(
                &bytes,
                &format!("Arcus observation evidence {}", path.display()),
            )?;
            Ok(Some(bytes))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect Arcus observation evidence {}",
                path.display()
            )
        }),
    }
}

/// Capture the files that form the live-tick recovery boundary. The
/// caller must hold the runtime checkpoint namespace lock so checkpoint,
/// ledger, pending-plan and observation-evidence reads cannot interleave with
/// a legitimate writer.
fn capture_arcus_state(config: &ArcusSpotExecuteOnceConfig) -> Result<ArcusSpotStateImage> {
    let checkpoint_store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let runtime = checkpoint_store.load_existing(&config.runtime)?;
    let ledger = ledger_store.load_existing()?;
    match fs::symlink_metadata(live_tick_pending_event_path(config)?) {
        Ok(_) => bail!(
            "Arcus pending durable event must be recovered by live-tick/propose before state backup"
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to inspect Arcus pending durable event"),
    }
    let checkpoint_bytes =
        read_private_regular_file(&config.runtime_state_path, "Arcus runtime checkpoint")?;
    let ledger_bytes = read_private_regular_file(&config.ledger_path, "Arcus execution ledger")?;
    let pending_plan_bytes = read_optional_private_plan(&live_tick_pending_plan_path(config)?)?;
    let observation_evidence_bytes = match read_optional_private_observation_evidence(
        &live_tick_observation_evidence_path(config)?,
    )? {
        Some(bytes) => {
            let evidence =
                observation_evidence_from_document(&bytes, "Arcus observation evidence")?;
            if observation_evidence_matches_runtime(&evidence, runtime.state()) {
                Some(bytes)
            } else if observation_evidence_is_newer_orphan(&evidence, runtime.state()) {
                // The writer publishes evidence first. A crash before the
                // checkpoint rename leaves the sidecar one sequence ahead;
                // omit it from the captured boundary instead of requiring a
                // new trade-capable tick or manual deletion to recover.
                None
            } else {
                bail!("Arcus observation evidence does not match the runtime boundary");
            }
        }
        None => None,
    };
    Ok(ArcusSpotStateImage {
        checkpoint_bytes,
        ledger_bytes,
        pending_plan_bytes,
        observation_evidence_bytes,
        runtime,
        ledger,
    })
}

fn manifest_for_state(
    config: &ArcusSpotExecuteOnceConfig,
    state: &ArcusSpotStateImage,
    captured_at: DateTime<Utc>,
) -> Result<ArcusSpotStateBackupManifest> {
    Ok(ArcusSpotStateBackupManifest {
        schema_version: STATE_BACKUP_SCHEMA_VERSION,
        captured_at,
        config_sha256: auto_execute_config_digest(config)?,
        runtime_checkpoint: state_backup_file(&state.checkpoint_bytes),
        execution_ledger: state_backup_file(&state.ledger_bytes),
        pending_plan: state.pending_plan_bytes.as_deref().map(state_backup_file),
        observation_evidence: state
            .observation_evidence_bytes
            .as_deref()
            .map(state_backup_file),
        runtime: runtime_state_summary(&state.runtime),
        ledger: ledger_state_summary(&state.ledger),
    })
}

fn write_new_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to fsync {}", path.display()))
}

/// Create a complete immutable backup directory using a hidden staging
/// directory and final rename. The live checkpoint/ledger are only read;
/// nothing here is a restore operation.
fn create_arcus_state_backup(
    config: &ArcusSpotExecuteOnceConfig,
    backup_dir: &Path,
) -> Result<ArcusSpotStateBackupManifest> {
    create_arcus_state_backup_with_capture(config, backup_dir, None)
}

#[cfg(test)]
fn create_arcus_state_backup_at(
    config: &ArcusSpotExecuteOnceConfig,
    backup_dir: &Path,
    captured_at: DateTime<Utc>,
) -> Result<ArcusSpotStateBackupManifest> {
    create_arcus_state_backup_with_capture(config, backup_dir, Some(captured_at))
}

fn create_arcus_state_backup_with_capture(
    config: &ArcusSpotExecuteOnceConfig,
    backup_dir: &Path,
    captured_at: Option<DateTime<Utc>>,
) -> Result<ArcusSpotStateBackupManifest> {
    if !backup_dir.is_absolute() {
        bail!("Arcus state backup directory must be absolute");
    }
    if fs::symlink_metadata(backup_dir).is_ok() {
        bail!(
            "Arcus state backup destination {} already exists",
            backup_dir.display()
        );
    }
    let parent = backup_dir
        .parent()
        .context("Arcus state backup directory has no parent")?;
    let backup_name = backup_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("Arcus state backup directory has no valid file name")?;
    let parent_metadata = fs::symlink_metadata(parent)
        .with_context(|| format!("failed to inspect backup parent {}", parent.display()))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        bail!(
            "Arcus state backup parent {} must be a non-symlink directory",
            parent.display()
        );
    }

    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;
    let state = capture_arcus_state(config)?;
    let manifest = manifest_for_state(config, &state, captured_at.unwrap_or_else(Utc::now))?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    let staging = parent.join(format!(
        ".{backup_name}.tmp.{}.{}",
        std::process::id(),
        stamp
    ));
    let result = (|| -> Result<()> {
        fs::create_dir(&staging)
            .with_context(|| format!("failed to create {}", staging.display()))?;
        fs::set_permissions(&staging, fs::Permissions::from_mode(0o700))?;
        write_new_private_file(
            &staging.join(STATE_BACKUP_CHECKPOINT),
            &state.checkpoint_bytes,
        )?;
        write_new_private_file(&staging.join(STATE_BACKUP_LEDGER), &state.ledger_bytes)?;
        if let Some(bytes) = &state.pending_plan_bytes {
            write_new_private_file(&staging.join(STATE_BACKUP_PENDING_PLAN), bytes)?;
        }
        if let Some(bytes) = &state.observation_evidence_bytes {
            write_new_private_file(&staging.join(STATE_BACKUP_OBSERVATION_EVIDENCE), bytes)?;
        }
        let mut manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .context("failed to serialize Arcus state backup manifest")?;
        manifest_bytes.push(b'\n');
        write_new_private_file(&staging.join(STATE_BACKUP_MANIFEST), &manifest_bytes)?;
        File::open(&staging)?.sync_all()?;
        if fs::symlink_metadata(backup_dir).is_ok() {
            bail!(
                "Arcus state backup destination {} appeared while staging",
                backup_dir.display()
            );
        }
        fs::rename(&staging, backup_dir).with_context(|| {
            format!(
                "failed to atomically publish Arcus state backup {}",
                backup_dir.display()
            )
        })?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&staging);
    }
    result?;
    Ok(manifest)
}

fn require_file_matches_manifest(
    bytes: &[u8],
    expected: &ArcusSpotStateBackupFile,
    label: &str,
) -> Result<()> {
    let actual = state_backup_file(bytes);
    if actual != *expected {
        bail!(
            "{label} does not match backup manifest (expected {} / {} bytes, got {} / {} bytes)",
            expected.sha256,
            expected.size_bytes,
            actual.sha256,
            actual.size_bytes
        );
    }
    Ok(())
}

fn load_arcus_state_backup(
    config: &ArcusSpotExecuteOnceConfig,
    backup_dir: &Path,
) -> Result<(ArcusSpotStateBackupManifest, ArcusSpotStateImage)> {
    require_private_directory(backup_dir, "Arcus state backup directory")?;
    let manifest_bytes =
        read_private_regular_file(&backup_dir.join(STATE_BACKUP_MANIFEST), "backup manifest")?;
    let manifest: ArcusSpotStateBackupManifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| {
            format!(
                "invalid Arcus state backup manifest {}",
                backup_dir.join(STATE_BACKUP_MANIFEST).display()
            )
        })?;
    if manifest.schema_version != STATE_BACKUP_SCHEMA_VERSION {
        bail!(
            "unsupported Arcus state backup schema {}; expected {}",
            manifest.schema_version,
            STATE_BACKUP_SCHEMA_VERSION
        );
    }
    let config_sha256 = auto_execute_config_digest(config)?;
    if manifest.config_sha256 != config_sha256 {
        bail!("Arcus state backup config does not match the supplied config");
    }

    let checkpoint_path = backup_dir.join(STATE_BACKUP_CHECKPOINT);
    let ledger_path = backup_dir.join(STATE_BACKUP_LEDGER);
    let checkpoint_bytes =
        read_private_regular_file(&checkpoint_path, "backup runtime checkpoint")?;
    let ledger_bytes = read_private_regular_file(&ledger_path, "backup execution ledger")?;
    require_file_matches_manifest(
        &checkpoint_bytes,
        &manifest.runtime_checkpoint,
        "backup runtime checkpoint",
    )?;
    require_file_matches_manifest(
        &ledger_bytes,
        &manifest.execution_ledger,
        "backup execution ledger",
    )?;
    let pending_plan_path = backup_dir.join(STATE_BACKUP_PENDING_PLAN);
    let pending_plan_bytes = match &manifest.pending_plan {
        Some(expected) => {
            let bytes = read_private_regular_file(&pending_plan_path, "backup pending plan")?;
            require_file_matches_manifest(&bytes, expected, "backup pending plan")?;
            plan_from_document(&bytes, "backup pending plan")?;
            Some(bytes)
        }
        None => {
            if fs::symlink_metadata(&pending_plan_path).is_ok() {
                bail!("backup has an unrecorded pending-plan file");
            }
            None
        }
    };
    let observation_evidence_path = backup_dir.join(STATE_BACKUP_OBSERVATION_EVIDENCE);
    let observation_evidence_bytes = match &manifest.observation_evidence {
        Some(expected) => {
            let bytes = read_private_regular_file(
                &observation_evidence_path,
                "backup observation evidence",
            )?;
            require_file_matches_manifest(&bytes, expected, "backup observation evidence")?;
            observation_evidence_from_document(&bytes, "backup observation evidence")?;
            Some(bytes)
        }
        None => {
            if fs::symlink_metadata(&observation_evidence_path).is_ok() {
                bail!("backup has an unrecorded observation-evidence file");
            }
            None
        }
    };
    let runtime = ArcusSpotRuntimeCheckpointStore::new(checkpoint_path)
        .load_existing(&config.runtime)
        .context("backup runtime checkpoint failed canonical validation")?;
    if let Some(bytes) = &observation_evidence_bytes {
        let evidence = observation_evidence_from_document(bytes, "backup observation evidence")?;
        if !observation_evidence_matches_runtime(&evidence, runtime.state()) {
            bail!("backup observation evidence does not match its runtime boundary");
        }
    }
    let ledger = ArcusSpotExecutionLedgerStore::new(ledger_path)
        .load_existing()
        .context("backup execution ledger failed canonical validation")?;
    if manifest.runtime != runtime_state_summary(&runtime) {
        bail!("backup runtime summary does not match its checkpoint");
    }
    if manifest.ledger != ledger_state_summary(&ledger) {
        bail!("backup ledger summary does not match its ledger");
    }
    Ok((
        manifest,
        ArcusSpotStateImage {
            checkpoint_bytes,
            ledger_bytes,
            pending_plan_bytes,
            observation_evidence_bytes,
            runtime,
            ledger,
        },
    ))
}

fn daily_risk_baseline(
    state: &ArcusSpotRuntimeState,
    label: &str,
) -> Result<Option<(NaiveDate, Decimal)>> {
    match (
        state.daily_baseline_day.as_deref(),
        state.daily_baseline_equity_usd,
    ) {
        (None, None) => {
            if state.initial_equity_usd.is_some()
                || state.last_equity_usd.is_some()
                || state.risk_halt.is_some()
            {
                bail!("{label} Arcus risk state has values without a daily baseline");
            }
            Ok(None)
        }
        (Some(day), Some(equity)) => {
            if state.initial_equity_usd.is_none() || state.last_equity_usd.is_none() {
                bail!("{label} Arcus risk state has an incomplete equity baseline");
            }
            let day = NaiveDate::parse_from_str(day, "%Y-%m-%d")
                .with_context(|| format!("{label} Arcus daily baseline day is invalid"))?;
            Ok(Some((day, equity)))
        }
        _ => bail!("{label} Arcus risk state has mismatched daily baseline fields"),
    }
}

fn require_rollover_matches_observation(
    day: NaiveDate,
    current: &ArcusSpotRuntimeState,
    acceptance_not_before: DateTime<Utc>,
    acceptance_not_after: DateTime<Utc>,
) -> Result<()> {
    let observation_day = current
        .last_observation_at
        .context("Arcus daily baseline advanced without a last observation")?
        .date_naive();
    if day != observation_day && Some(day) != observation_day.succ_opt() {
        bail!("Arcus daily baseline rollover does not match the accepted observation");
    }
    if day < acceptance_not_before.date_naive() || day > acceptance_not_after.date_naive() {
        bail!("Arcus daily baseline rollover is outside the approved tick window");
    }
    Ok(())
}

fn positive_loss_from_mark(reference: Option<Decimal>, equity: Option<Decimal>) -> Result<Decimal> {
    match reference.zip(equity) {
        Some((reference, equity)) => Ok(reference
            .checked_sub(equity)
            .context("Arcus risk loss calculation exceeds Decimal range")?
            .max(Decimal::ZERO)),
        None => Ok(Decimal::ZERO),
    }
}

fn require_risk_state_continuity(
    config: &ArcusSpotRuntimeConfig,
    baseline: &ArcusSpotRuntimeState,
    current: &ArcusSpotRuntimeState,
    sequence_advance: u64,
    acceptance_not_before: DateTime<Utc>,
    acceptance_not_after: DateTime<Utc>,
) -> Result<()> {
    let baseline_daily = daily_risk_baseline(baseline, "backup")?;
    if baseline.initial_equity_usd.is_some()
        && current.initial_equity_usd != baseline.initial_equity_usd
    {
        bail!("Arcus runtime cumulative equity baseline changed across restart/rollback");
    }
    if baseline.risk_halt.is_some() && current.risk_halt != baseline.risk_halt {
        bail!("Arcus runtime lost or changed its sticky risk halt across restart/rollback");
    }
    if baseline.last_equity_usd.is_some() && current.last_equity_usd.is_none() {
        bail!("Arcus runtime lost its last equity mark across restart/rollback");
    }
    let current_daily = daily_risk_baseline(current, "current")?;
    if sequence_advance == 0 {
        if current.initial_equity_usd != baseline.initial_equity_usd
            || current_daily != baseline_daily
            || current.last_equity_usd != baseline.last_equity_usd
            || current.risk_halt != baseline.risk_halt
        {
            bail!("Arcus runtime risk state changed without a new observation");
        }
    }
    match (baseline_daily, current_daily) {
        (None, None) => {}
        (None, Some((current_day, current_equity))) => {
            require_rollover_matches_observation(
                current_day,
                current,
                acceptance_not_before,
                acceptance_not_after,
            )?;
            if current.initial_equity_usd != Some(current_equity)
                || current.last_equity_usd != Some(current_equity)
            {
                bail!(
                    "Arcus runtime initialized mismatched cumulative, daily, and last equity marks"
                );
            }
        }
        (Some(_), None) => {
            bail!("Arcus runtime lost its daily loss baseline across restart/rollback")
        }
        (Some((baseline_day, baseline_equity)), Some((current_day, current_equity))) => {
            if current_day == baseline_day {
                if current_equity != baseline_equity {
                    bail!("Arcus runtime daily equity baseline changed without a UTC rollover");
                }
            } else {
                if current_day < baseline_day {
                    bail!("Arcus runtime daily baseline day regressed across restart/rollback");
                }
                require_rollover_matches_observation(
                    current_day,
                    current,
                    acceptance_not_before,
                    acceptance_not_after,
                )?;
                if current.last_equity_usd != Some(current_equity) {
                    bail!("Arcus runtime UTC rollover baseline does not match its equity mark");
                }
            }
        }
    }
    if baseline.risk_halt.is_none() {
        // Re-derived here rather than read off the runtime, so a checkpoint
        // cannot assert its own innocence -- but it has to re-derive the
        // *same* measure the runtime halts on, or the two disagree and every
        // ordinary down day makes verification demand a halt the runtime was
        // right not to engage. Both sides therefore price the baseline
        // baskets at the marks the current state was last valued on
        // (bot-strategy#813).
        let prices = current
            .last_token_a_reference_price_usd
            .zip(current.last_token_b_reference_price_usd);
        // Absent baskets (a checkpoint predating them) or absent marks leave
        // the expectation unassessable, which `positive_loss_from_mark`
        // already renders as no expected loss. Requiring a halt the runtime
        // had no information to engage would fail every such checkpoint.
        let benchmark = |basket: Option<ArcusSpotInventory>| -> Result<Option<Decimal>> {
            match basket.zip(prices) {
                Some((basket, (price_a, price_b))) => {
                    Ok(Some(basket.checked_value_usd(price_a, price_b).context(
                        "Arcus risk basket valuation exceeds Decimal range",
                    )?))
                }
                None => Ok(None),
            }
        };
        let daily_loss = positive_loss_from_mark(
            benchmark(current.daily_baseline_inventory)?,
            current.last_equity_usd,
        )?;
        let cumulative_loss = positive_loss_from_mark(
            benchmark(current.initial_baseline_inventory)?,
            current.last_equity_usd,
        )?;
        let expected = if daily_loss >= config.daily_loss_limit_usd {
            Some((
                ArcusSpotRiskHaltKind::DailyLoss,
                daily_loss,
                config.daily_loss_limit_usd,
            ))
        } else if cumulative_loss >= config.cumulative_loss_limit_usd {
            Some((
                ArcusSpotRiskHaltKind::CumulativeLoss,
                cumulative_loss,
                config.cumulative_loss_limit_usd,
            ))
        } else {
            None
        };
        match (expected, current.risk_halt.as_ref()) {
            (None, None) => {}
            (None, Some(_)) => {
                bail!("Arcus runtime engaged an unexpected risk halt across restart/rollback")
            }
            (Some(_), None) => {
                bail!("Arcus runtime omitted a newly triggered loss halt across restart/rollback")
            }
            (Some((kind, loss, limit)), Some(halt)) => {
                let current_day = current_daily
                    .map(|(day, _)| day)
                    .context("Arcus runtime engaged a risk halt without a daily baseline")?;
                let current_equity = current
                    .last_equity_usd
                    .context("Arcus runtime engaged a risk halt without a last equity mark")?;
                if halt.kind != kind
                    || halt.equity_usd != current_equity
                    || halt.loss_usd != loss
                    || halt.limit_usd != limit
                    || halt.engaged_at.date_naive() != current_day
                {
                    bail!("Arcus runtime newly triggered loss halt does not match its risk mark");
                }
            }
        }
    }
    Ok(())
}

fn parse_raw_amount(label: &str, raw: &str) -> Result<U256> {
    U256::from_dec_str(raw.trim()).with_context(|| format!("invalid Arcus {label}"))
}

fn quantity_to_raw_for_continuity(quantity: Decimal, decimals: u32) -> Result<String> {
    if quantity < Decimal::ZERO {
        bail!("Arcus acceptance plan quantity must not be negative");
    }
    let raw_scale = 10_i128
        .checked_pow(decimals)
        .context("Arcus acceptance token decimals exceed Decimal range")?;
    let scale = Decimal::try_from_i128_with_scale(raw_scale, 0)
        .context("Arcus acceptance token decimals exceed Decimal range")?;
    let raw = quantity
        .checked_mul(scale)
        .context("Arcus acceptance plan quantity exceeds Decimal range")?;
    if raw.fract() != Decimal::ZERO {
        bail!("Arcus acceptance plan quantity has a fractional raw unit");
    }
    Ok(raw.trunc().to_string())
}

fn require_acceptance_plan_matches_config(
    config: &ArcusSpotExecuteOnceConfig,
    plan: &ArcusSpotRotationPlan,
) -> Result<()> {
    let (expected_sell, expected_buy) = match plan.direction {
        ArcusSpotDirection::TokenAToTokenB => (
            config.runtime.pair.sell_symbol.as_str(),
            config.runtime.pair.buy_symbol.as_str(),
        ),
        ArcusSpotDirection::TokenBToTokenA => (
            config.runtime.pair.buy_symbol.as_str(),
            config.runtime.pair.sell_symbol.as_str(),
        ),
    };
    if !plan.sell_symbol.eq_ignore_ascii_case(expected_sell)
        || !plan.buy_symbol.eq_ignore_ascii_case(expected_buy)
    {
        bail!("Arcus acceptance plan direction does not match the configured pair");
    }
    let sell_amount = parse_raw_amount("plan sell amount", &plan.sell_amount_raw)?;
    let maximum_sell = config
        .executor
        .maximum_sell_amount_raw
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&plan.sell_symbol))
        .map(|(_, raw)| raw)
        .with_context(|| {
            format!(
                "Arcus acceptance has no maximum sell amount for {}",
                plan.sell_symbol
            )
        })?;
    let maximum_sell = parse_raw_amount("maximum sell amount", maximum_sell)?;
    if sell_amount > maximum_sell {
        bail!(
            "Arcus acceptance sell amount {sell_amount} exceeds configured maximum {maximum_sell}"
        );
    }
    for (symbol, address, quantity, raw) in [
        (
            plan.sell_symbol.as_str(),
            plan.sell_token_address.as_str(),
            plan.sell_quantity,
            plan.sell_amount_raw.as_str(),
        ),
        (
            plan.buy_symbol.as_str(),
            plan.buy_token_address.as_str(),
            plan.buy_quantity,
            plan.buy_amount_raw.as_str(),
        ),
    ] {
        let trusted_address = config
            .router
            .trusted_token_addresses
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
            .map(|(_, address)| address)
            .with_context(|| format!("Arcus acceptance has no address pin for {symbol}"))?;
        if !address.eq_ignore_ascii_case(trusted_address) {
            bail!("Arcus acceptance plan token address does not match its configured pin");
        }
        let decimals = config
            .router
            .trusted_token_decimals
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
            .map(|(_, decimals)| *decimals)
            .with_context(|| format!("Arcus acceptance has no decimals pin for {symbol}"))?;
        if quantity_to_raw_for_continuity(quantity, decimals)? != raw {
            bail!("Arcus acceptance plan raw amount does not match its decimal quantity");
        }
    }
    Ok(())
}

fn reconciled_fill_for_continuity(
    config: &ArcusSpotExecuteOnceConfig,
    plan: &ArcusSpotRotationPlan,
    attempt: &ArcusSpotExecutionAttempt,
    evaluation_time: DateTime<Utc>,
) -> Result<(Decimal, DateTime<Utc>)> {
    require_acceptance_plan_matches_config(config, plan)?;
    if attempt.phase != ArcusSpotExecutionPhase::Reconciled {
        bail!("Arcus acceptance attempt did not finish reconciled");
    }
    if !attempt
        .router_status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case("confirmed"))
    {
        bail!("Arcus acceptance attempt has no confirmed router status");
    }
    if attempt.chain_id != config.runtime.chain_id
        || attempt.chain_id != config.chain.chain_id
        || attempt.chain_id != config.router.chain_id
        || !attempt.taker.eq_ignore_ascii_case(&config.executor.taker)
        || attempt.tx_hash.is_none()
    {
        bail!("Arcus acceptance attempt does not match the configured chain/taker");
    }
    let tx_hash = attempt
        .tx_hash
        .as_deref()
        .context("reconciled Arcus acceptance attempt omitted its transaction hash")?;
    let tx_hash =
        H256::from_str(tx_hash.trim()).context("invalid Arcus acceptance transaction hash")?;
    if tx_hash == H256::zero() {
        bail!("Arcus acceptance transaction hash must not be zero");
    }
    if !is_supported_live_route(plan) {
        bail!("Arcus acceptance requires an Arcus or Rialto plan");
    }
    let plan_config_digest = approval_digest(config, plan)?;
    if attempt.intent.plan_config_digest != plan_config_digest
        || !attempt.intent.venue.eq_ignore_ascii_case(&plan.venue)
        || !attempt
            .intent
            .sell_symbol
            .eq_ignore_ascii_case(&plan.sell_symbol)
        || !attempt
            .intent
            .buy_symbol
            .eq_ignore_ascii_case(&plan.buy_symbol)
        || !attempt
            .intent
            .sell_token
            .eq_ignore_ascii_case(&plan.sell_token_address)
        || !attempt
            .intent
            .buy_token
            .eq_ignore_ascii_case(&plan.buy_token_address)
        || attempt.intent.sell_amount_raw != plan.sell_amount_raw
    {
        bail!("Arcus acceptance attempt does not match its pending runtime plan");
    }
    let post = attempt
        .post_balances
        .as_ref()
        .context("reconciled Arcus acceptance attempt omitted post balances")?;
    let pre_sell = parse_raw_amount("pre sell balance", &attempt.pre_balances.sell_balance_raw)?;
    let pre_buy = parse_raw_amount("pre buy balance", &attempt.pre_balances.buy_balance_raw)?;
    let pre_gas = parse_raw_amount("pre gas balance", &attempt.pre_balances.gas_balance_wei)?;
    let post_sell = parse_raw_amount("post sell balance", &post.sell_balance_raw)?;
    let post_buy = parse_raw_amount("post buy balance", &post.buy_balance_raw)?;
    let sell_floor = config
        .executor
        .inventory_floor_raw
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&plan.sell_symbol))
        .map(|(_, raw)| raw)
        .with_context(|| {
            format!(
                "Arcus acceptance has no inventory floor for {}",
                plan.sell_symbol
            )
        })?;
    let buy_floor = config
        .executor
        .inventory_floor_raw
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&plan.buy_symbol))
        .map(|(_, raw)| raw)
        .with_context(|| {
            format!(
                "Arcus acceptance has no inventory floor for {}",
                plan.buy_symbol
            )
        })?;
    let sell_floor = parse_raw_amount("sell inventory floor", sell_floor)?;
    let buy_floor = parse_raw_amount("buy inventory floor", buy_floor)?;
    let gas_floor = parse_raw_amount(
        "minimum gas balance",
        &config.executor.minimum_gas_balance_wei,
    )?;
    if post_sell < sell_floor {
        bail!("Arcus acceptance post-swap sell balance is below its configured floor");
    }
    if pre_buy < buy_floor {
        bail!("Arcus acceptance pre-swap buy balance is below its configured floor");
    }
    if pre_gas < gas_floor {
        bail!("Arcus acceptance pre-swap gas balance is below its configured minimum");
    }
    let sold_raw = pre_sell
        .checked_sub(post_sell)
        .context("reconciled Arcus acceptance sell balance increased")?;
    let bought_raw = post_buy
        .checked_sub(pre_buy)
        .context("reconciled Arcus acceptance buy balance decreased")?;
    if sold_raw != parse_raw_amount("intent sell amount", &attempt.intent.sell_amount_raw)? {
        bail!("Arcus acceptance sell delta does not match its intent");
    }
    let planned_buy_raw = parse_raw_amount("plan buy amount", &plan.buy_amount_raw)?;
    if planned_buy_raw.is_zero() || plan.buy_quantity <= Decimal::ZERO {
        bail!("Arcus acceptance pending plan has an invalid buy quantity");
    }
    let intent_minimum = parse_raw_amount(
        "intent minimum buy amount",
        &attempt.intent.minimum_buy_amount_raw,
    )?;
    let retained_bps = 10_000_u32
        .checked_sub(config.executor.slippage_bps)
        .context("Arcus acceptance slippage exceeds 10000 bps")?;
    let approved_minimum = planned_buy_raw
        .checked_mul(U256::from(retained_bps))
        .context("Arcus acceptance approved minimum calculation overflow")?
        .checked_add(U256::from(9_999_u32))
        .context("Arcus acceptance approved minimum rounding overflow")?
        / U256::from(10_000_u32);
    if intent_minimum < approved_minimum {
        bail!("Arcus acceptance signed minimum undercuts the pending plan's approved buy floor");
    }
    if bought_raw < intent_minimum {
        bail!("Arcus acceptance buy delta is below its signed minimum");
    }
    let bought_decimal = Decimal::from_str(&bought_raw.to_string())
        .context("Arcus acceptance buy amount exceeds Decimal range")?;
    let planned_buy_decimal = Decimal::from_str(&planned_buy_raw.to_string())
        .context("Arcus acceptance planned buy amount exceeds Decimal range")?;
    let actual_buy_quantity = plan
        .buy_quantity
        .checked_mul(bought_decimal)
        .and_then(|value| value.checked_div(planned_buy_decimal))
        .context("Arcus acceptance buy quantity exceeds Decimal range")?;
    if plan.sell_quantity <= Decimal::ZERO || actual_buy_quantity <= Decimal::ZERO {
        bail!("Arcus acceptance runtime quantities must be positive");
    }
    let filled_at = attempt
        .dispatched_at
        .context("reconciled Arcus acceptance attempt omitted its dispatch time")?;
    let planning_age_ms = evaluation_time
        .signed_duration_since(plan.quote_received_at)
        .num_milliseconds();
    let max_quote_age_ms = config.runtime.max_quote_age_secs.saturating_mul(1_000);
    if planning_age_ms < 0 || planning_age_ms > max_quote_age_ms {
        bail!("Arcus acceptance quote was stale or future-dated at strategy planning");
    }
    let plan_age = filled_at.signed_duration_since(plan.quote_received_at);
    let plan_age_ms = plan_age.num_milliseconds();
    let max_plan_age_ms = i64::try_from(config.executor.max_plan_age_secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000);
    if plan_age_ms < 0 || plan_age_ms > max_plan_age_ms {
        bail!("Arcus acceptance plan was stale or future-dated at dispatch");
    }
    Ok((actual_buy_quantity, filled_at))
}

fn position_state_matches(left: &ArcusSpotRuntimeState, right: &ArcusSpotRuntimeState) -> bool {
    left.inventory == right.inventory
        && left.regime == right.regime
        && left.last_rotation_at == right.last_rotation_at
        && left.rotated_quantity == right.rotated_quantity
        && left.last_live_execution_idempotency_key == right.last_live_execution_idempotency_key
}

fn runtime_state_matches_replay(
    replayed: &ArcusSpotRuntimeState,
    persisted: &ArcusSpotRuntimeState,
) -> bool {
    if replayed.relative_log_price_history.len() != persisted.relative_log_price_history.len()
        || replayed
            .relative_log_price_history
            .iter()
            .zip(&persisted.relative_log_price_history)
            .any(|(left, right)| (left - right).abs() > 1e-12)
    {
        return false;
    }
    let mut replayed_without_floats = replayed.clone();
    let mut persisted_without_floats = persisted.clone();
    replayed_without_floats.relative_log_price_history.clear();
    persisted_without_floats.relative_log_price_history.clear();
    replayed_without_floats == persisted_without_floats
}

/// Read the sample from the runtime produced by the preserved recorder replay.
/// Do not infer whether a sample was appended by comparing final history bytes:
/// a bounded rolling window is strategy state, not the authoritative evidence.
fn acceptance_signal_sample(replayed: &ArcusSpotRuntimeState) -> Result<f64> {
    replayed
        .relative_log_price_history
        .last()
        .copied()
        .context("Arcus acceptance attempt has no signal sample")
}

fn require_acceptance_quote_belongs_to_observation(
    plan: &ArcusSpotRotationPlan,
    accepted_observation_at: DateTime<Utc>,
) -> Result<()> {
    if plan.quote_received_at > accepted_observation_at {
        bail!("Arcus acceptance quote was received after its accepted observation");
    }
    Ok(())
}

fn acceptance_reference_prices(
    baseline: &ArcusSpotRuntimeState,
    current: &ArcusSpotRuntimeState,
    signal_sample: f64,
) -> Result<(Decimal, Decimal)> {
    let token_a = current
        .last_token_a_reference_price_usd
        .context("Arcus acceptance attempt omitted its token A reference price")?;
    let token_b = current
        .last_token_b_reference_price_usd
        .context("Arcus acceptance attempt omitted its token B reference price")?;
    if token_a <= Decimal::ZERO || token_b <= Decimal::ZERO {
        bail!("Arcus acceptance reference prices must be positive");
    }
    let token_a_f64 = token_a
        .to_string()
        .parse::<f64>()
        .context("Arcus acceptance token A reference price exceeds f64 range")?;
    let token_b_f64 = token_b
        .to_string()
        .parse::<f64>()
        .context("Arcus acceptance token B reference price exceeds f64 range")?;
    let recorded_signal = (token_a_f64 / token_b_f64).ln();
    if !recorded_signal.is_finite() || (recorded_signal - signal_sample).abs() > 1e-12 {
        bail!("Arcus acceptance reference prices do not match its accepted signal sample");
    }
    let marked_equity = baseline
        .inventory
        .checked_value_usd(token_a, token_b)
        .context("Arcus acceptance reference-price valuation exceeds Decimal range")?;
    if current.last_equity_usd != Some(marked_equity) {
        bail!("Arcus acceptance reference prices do not match its accepted equity mark");
    }
    Ok((token_a, token_b))
}

fn require_acceptance_entry_within_strategy_limits(
    config: &ArcusSpotExecuteOnceConfig,
    baseline: &ArcusSpotRuntimeState,
    plan: &ArcusSpotRotationPlan,
    signal_sample: f64,
    token_a_reference_price_usd: Decimal,
    token_b_reference_price_usd: Decimal,
) -> Result<()> {
    if plan.trigger != ArcusSpotRotationTrigger::EntrySignal {
        bail!("Arcus acceptance from a neutral backup must be an entry signal");
    }
    if plan.optimistic_round_trip_loss_bps < Decimal::ZERO {
        bail!("Arcus acceptance optimistic round-trip loss must not be negative");
    }
    let all_in_cost = plan
        .optimistic_round_trip_loss_bps
        .checked_add(config.runtime.gas_buffer_bps)
        .and_then(|cost| cost.checked_add(config.runtime.settlement_buffer_bps))
        .context("Arcus acceptance all-in cost exceeds Decimal range")?;
    if plan.gas_buffer_bps != config.runtime.gas_buffer_bps
        || plan.settlement_buffer_bps != config.runtime.settlement_buffer_bps
        || plan.all_in_round_trip_cost_bps != all_in_cost
    {
        bail!("Arcus acceptance all-in cost does not match the configured buffer arithmetic");
    }
    if all_in_cost > config.runtime.max_all_in_round_trip_cost_bps {
        bail!(
            "Arcus acceptance all-in cost {} exceeds configured maximum {}",
            all_in_cost,
            config.runtime.max_all_in_round_trip_cost_bps
        );
    }
    let sell_decimals = config
        .router
        .trusted_token_decimals
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&plan.sell_symbol))
        .map(|(_, decimals)| *decimals)
        .with_context(|| {
            format!(
                "Arcus acceptance has no decimals pin for {}",
                plan.sell_symbol
            )
        })?;
    let raw_scale = 10_i128
        .checked_pow(sell_decimals)
        .context("Arcus acceptance token decimals exceed Decimal range")?;
    let raw_scale = Decimal::try_from_i128_with_scale(raw_scale, 0)
        .context("Arcus acceptance token decimals exceed Decimal range")?;
    let sell_reference_price_usd = match plan.direction {
        ArcusSpotDirection::TokenAToTokenB => token_a_reference_price_usd,
        ArcusSpotDirection::TokenBToTokenA => token_b_reference_price_usd,
    };
    let expected_sell_raw = config
        .runtime
        .notional_usd
        .checked_div(sell_reference_price_usd)
        .and_then(|quantity| quantity.checked_mul(raw_scale))
        .context("Arcus acceptance configured notional exceeds Decimal range")?
        .trunc();
    if parse_raw_amount("plan sell amount", &plan.sell_amount_raw)?
        != parse_raw_amount(
            "configured-notional sell amount",
            &expected_sell_raw.to_string(),
        )?
    {
        bail!("Arcus acceptance sell amount does not match the configured USD notional");
    }
    let (sellable, predicted_inventory) = match plan.direction {
        ArcusSpotDirection::TokenAToTokenB => (
            baseline
                .inventory
                .token_a
                .checked_sub(config.runtime.inventory_floors.token_a)
                .context("Arcus acceptance backup inventory is below its token A floor")?,
            ArcusSpotInventory {
                token_a: baseline
                    .inventory
                    .token_a
                    .checked_sub(plan.sell_quantity)
                    .context("Arcus acceptance predicted token A inventory underflow")?,
                token_b: baseline
                    .inventory
                    .token_b
                    .checked_add(plan.buy_quantity)
                    .context("Arcus acceptance predicted token B inventory overflow")?,
            },
        ),
        ArcusSpotDirection::TokenBToTokenA => (
            baseline
                .inventory
                .token_b
                .checked_sub(config.runtime.inventory_floors.token_b)
                .context("Arcus acceptance backup inventory is below its token B floor")?,
            ArcusSpotInventory {
                token_a: baseline
                    .inventory
                    .token_a
                    .checked_add(plan.buy_quantity)
                    .context("Arcus acceptance predicted token A inventory overflow")?,
                token_b: baseline
                    .inventory
                    .token_b
                    .checked_sub(plan.sell_quantity)
                    .context("Arcus acceptance predicted token B inventory underflow")?,
            },
        ),
    };
    let maximum_rotation = sellable
        .checked_mul(config.runtime.max_rotation_fraction)
        .context("Arcus acceptance rotation cap exceeds Decimal range")?;
    if plan.sell_quantity > maximum_rotation {
        bail!(
            "Arcus acceptance sell quantity {} exceeds strategy per-action rotation cap {}",
            plan.sell_quantity,
            maximum_rotation
        );
    }
    if plan.predicted_inventory != predicted_inventory {
        bail!("Arcus acceptance predicted inventory does not match the backup and plan quantities");
    }
    // The price scale cancels from the USD imbalance fraction. Recover the
    // A/B ratio from the accepted relative-log-price sample and compare both
    // the planner-recorded value and configured hard cap. The tolerance only
    // covers the runtime's Decimal -> f64 -> ln/exp round trip.
    let price_ratio = signal_sample.exp();
    let token_a = predicted_inventory
        .token_a
        .to_string()
        .parse::<f64>()
        .context("Arcus acceptance token A inventory exceeds f64 range")?;
    let token_b = predicted_inventory
        .token_b
        .to_string()
        .parse::<f64>()
        .context("Arcus acceptance token B inventory exceeds f64 range")?;
    let value_a = token_a * price_ratio;
    let total = value_a + token_b;
    if !price_ratio.is_finite()
        || price_ratio <= 0.0
        || !value_a.is_finite()
        || !total.is_finite()
        || total <= 0.0
    {
        bail!("Arcus acceptance inventory imbalance cannot be reconstructed");
    }
    let imbalance = (value_a - token_b).abs() / total;
    let recorded_imbalance = plan
        .predicted_inventory_imbalance_fraction
        .to_string()
        .parse::<f64>()
        .context("Arcus acceptance recorded imbalance exceeds f64 range")?;
    let maximum_imbalance = config
        .runtime
        .max_inventory_imbalance_fraction
        .to_string()
        .parse::<f64>()
        .context("Arcus acceptance configured imbalance exceeds f64 range")?;
    if (recorded_imbalance - imbalance).abs() > 1e-9 {
        bail!("Arcus acceptance recorded inventory imbalance does not match its accepted price");
    }
    if imbalance > maximum_imbalance {
        bail!("Arcus acceptance predicted inventory exceeds the configured imbalance cap");
    }
    Ok(())
}

fn require_acceptance_daily_swap_capacity(
    config: &ArcusSpotExecuteOnceConfig,
    baseline: &ArcusSpotExecutionLedger,
    attempt: &ArcusSpotExecutionAttempt,
) -> Result<()> {
    let execution_day = attempt.prepared_at.date_naive();
    let completed_before_acceptance = baseline
        .history
        .iter()
        .filter(|archived| archived.updated_at.date_naive() == execution_day)
        .count();
    if completed_before_acceptance >= config.executor.max_swaps_per_utc_day as usize {
        bail!("Arcus acceptance attempt exceeds the configured UTC daily swap cap");
    }
    Ok(())
}

fn require_acceptance_ledger_and_position_continuity(
    config: &ArcusSpotExecuteOnceConfig,
    baseline: &ArcusSpotStateImage,
    current: &ArcusSpotStateImage,
    runtime_sequence_advance: u64,
    acceptance_not_before: DateTime<Utc>,
    acceptance_not_after: DateTime<Utc>,
) -> Result<()> {
    let baseline_runtime = baseline.runtime.state();
    let current_runtime = current.runtime.state();
    if baseline.ledger.active.is_some() || baseline_runtime.regime != ArcusSpotRegime::Neutral {
        bail!("Arcus continuity backup is not the required neutral/no-active acceptance baseline");
    }
    let ledger_advance = current
        .ledger
        .next_sequence
        .checked_sub(baseline.ledger.next_sequence)
        .context("Arcus ledger next_sequence regressed across restart/rollback")?;
    if ledger_advance > 1 {
        bail!("Arcus ledger advanced by more than one acceptance attempt");
    }
    if current.ledger.history.len() < baseline.ledger.history.len()
        || current.ledger.history[..baseline.ledger.history.len()] != baseline.ledger.history
    {
        bail!("Arcus ledger lost or changed archived attempts across restart/rollback");
    }
    match ledger_advance {
        0 => {
            if current.ledger.active.is_some()
                || current.ledger.history.len() != baseline.ledger.history.len()
            {
                bail!("Arcus ledger changed without advancing its attempt sequence");
            }
            if current.pending_plan_bytes != baseline.pending_plan_bytes {
                bail!("Arcus pending recovery plan changed without an acceptance attempt");
            }
            if runtime_sequence_advance == 0 {
                if current.observation_evidence_bytes != baseline.observation_evidence_bytes {
                    bail!("Arcus observation evidence changed without a new observation");
                }
            } else {
                let bytes = current
                    .observation_evidence_bytes
                    .as_deref()
                    .context("Arcus accepted no-swap observation has no recorder evidence")?;
                let evidence = observation_evidence_from_document(
                    bytes,
                    "Arcus accepted no-swap observation evidence",
                )?;
                require_current_observation_evidence_schema(&evidence)?;
                if evidence.evaluation_time < acceptance_not_before
                    || evidence.evaluation_time > acceptance_not_after
                {
                    bail!("Arcus no-swap recorder evaluation is outside the approved tick window");
                }
                let mut replayed =
                    ArcusSpotRuntime::from_state(config.runtime.clone(), baseline_runtime.clone())
                        .map_err(anyhow::Error::msg)
                        .context("failed to reconstruct the Arcus no-swap replay baseline")?;
                let replayed_event = replayed.step_at(&evidence.snapshot, evidence.evaluation_time);
                if !matches!(replayed_event.decision, ArcusSpotDecision::Observe { .. }) {
                    bail!("Arcus no-swap recorder evidence reproduced a rotation decision");
                }
                if !runtime_state_matches_replay(replayed.state(), current_runtime) {
                    bail!("Arcus no-swap runtime state does not match its recorder evidence");
                }
            }
            if !position_state_matches(baseline_runtime, current_runtime) {
                bail!("Arcus position state changed without a reconciled acceptance attempt");
            }
        }
        1 => {
            if runtime_sequence_advance != 1 {
                bail!("Arcus ledger advanced without the single approved observation");
            }
            let accepted_observation_at = match (
                baseline_runtime.last_observation_at,
                current_runtime.last_observation_at,
            ) {
                (Some(baseline_at), Some(current_at)) if current_at > baseline_at => current_at,
                (None, Some(current_at)) => current_at,
                _ => bail!("Arcus acceptance attempt has no newly accepted observation"),
            };
            if current.ledger.active.is_some()
                || current.ledger.history.len() != baseline.ledger.history.len() + 1
            {
                bail!("Arcus acceptance attempt is unresolved or was not archived exactly once");
            }
            let attempt = current.ledger.history.last().expect("length checked above");
            if attempt.sequence != baseline.ledger.next_sequence {
                bail!("Arcus acceptance attempt has an unexpected sequence");
            }
            let dispatched_at = attempt
                .dispatched_at
                .context("reconciled Arcus acceptance attempt omitted its dispatch time")?;
            if attempt.prepared_at < accepted_observation_at
                || attempt.prepared_at > acceptance_not_after
                || dispatched_at < attempt.prepared_at
                || dispatched_at > attempt.updated_at
                || attempt.updated_at > acceptance_not_after
            {
                bail!("Arcus acceptance attempt chronology is outside the approved tick window");
            }
            // The live executor checks the archived-attempt count for the
            // UTC day immediately before it prepares a new attempt. Rebuild
            // that same point-in-time guard from the immutable backup and
            // the preparation day. The chronology checks above bind this
            // durable timestamp to the accepted tick, and it is the closest
            // persisted equivalent of validate_plan's Utc::now() day.
            require_acceptance_daily_swap_capacity(config, &baseline.ledger, attempt)?;
            let plan_bytes = current
                .pending_plan_bytes
                .as_deref()
                .context("Arcus reconciled acceptance attempt has no pending runtime plan")?;
            let evidence = live_tick_evidence_from_document(
                plan_bytes,
                "Arcus acceptance pending runtime plan",
            )?;
            let observation_bytes = current
                .observation_evidence_bytes
                .as_deref()
                .context("Arcus acceptance attempt has no shared observation evidence")?;
            let observation_evidence = observation_evidence_from_document(
                observation_bytes,
                "Arcus acceptance observation evidence",
            )?;
            require_current_observation_evidence_schema(&observation_evidence)?;
            if observation_evidence.evaluation_time != evidence.evaluation_time
                || serde_json::to_value(&observation_evidence.snapshot)?
                    != serde_json::to_value(&evidence.snapshot)?
            {
                bail!("Arcus acceptance plan and observation evidence do not match");
            }
            let plan = evidence.plan.clone();
            if evidence.snapshot.collection_finished_at != accepted_observation_at {
                bail!("Arcus acceptance recorder evidence does not match the accepted observation");
            }
            if evidence.evaluation_time < accepted_observation_at
                || evidence.evaluation_time > attempt.prepared_at
            {
                bail!("Arcus acceptance recorder evaluation is outside the approved tick window");
            }
            require_acceptance_quote_belongs_to_observation(&plan, accepted_observation_at)?;

            // Re-run the exact planner from the immutable pre-rollback
            // checkpoint and the raw recorder snapshot captured by
            // live-tick. This independently derives route linkage/loss,
            // quote freshness, signal, sizing, and inventory projections;
            // a rollback candidate cannot gain acceptance by merely
            // writing a self-consistent but understated nonnegative cost
            // into its pending plan.
            let mut replayed_runtime =
                ArcusSpotRuntime::from_state(config.runtime.clone(), baseline_runtime.clone())
                    .map_err(anyhow::Error::msg)
                    .context("failed to reconstruct the Arcus acceptance replay baseline")?;
            let replayed_event =
                replayed_runtime.step_at(&evidence.snapshot, evidence.evaluation_time);
            let replayed_plan = match &replayed_event.decision {
                ArcusSpotDecision::WouldRotate { plan } => plan,
                ArcusSpotDecision::Observe { hold } => bail!(
                    "Arcus acceptance recorder evidence did not reproduce a live rotation plan: \
                     {}",
                    hold.detail
                ),
                ArcusSpotDecision::SimulatedFill { .. } => bail!(
                    "Arcus acceptance recorder evidence unexpectedly produced a simulated fill"
                ),
            };
            if replayed_plan != &plan {
                bail!(
                    "Arcus acceptance pending plan does not match its independently replayed \
                     recorder evidence"
                );
            }

            let signal_sample = acceptance_signal_sample(replayed_runtime.state())?;
            let signal_runtime =
                ArcusSpotRuntime::from_state(config.runtime.clone(), baseline_runtime.clone())
                    .map_err(anyhow::Error::msg)
                    .context("failed to reconstruct the Arcus acceptance signal baseline")?;
            let signal_direction = signal_runtime
                .entry_direction_for_signal_sample(signal_sample)
                .context("Arcus acceptance attempt has no valid entry signal")?;
            if signal_direction != plan.direction {
                bail!("Arcus acceptance entry signal direction does not match its pending plan");
            }
            let (token_a_reference_price_usd, token_b_reference_price_usd) =
                acceptance_reference_prices(baseline_runtime, current_runtime, signal_sample)?;
            require_acceptance_entry_within_strategy_limits(
                config,
                baseline_runtime,
                &plan,
                signal_sample,
                token_a_reference_price_usd,
                token_b_reference_price_usd,
            )?;
            let (actual_buy_quantity, filled_at) =
                reconciled_fill_for_continuity(config, &plan, attempt, evidence.evaluation_time)?;
            replayed_runtime
                .validate_plan_consistent_with_state(&plan)
                .map_err(anyhow::Error::msg)
                .context("Arcus acceptance plan is inconsistent with the backup position")?;
            let applied = replayed_runtime
                .apply_confirmed_live_fill_once(
                    &plan,
                    plan.sell_quantity,
                    actual_buy_quantity,
                    filled_at,
                    &attempt.idempotency_key,
                )
                .map_err(anyhow::Error::msg)
                .context("failed to derive the Arcus acceptance position transition")?;
            if !applied || !runtime_state_matches_replay(replayed_runtime.state(), current_runtime)
            {
                bail!("Arcus position state does not match the reconciled acceptance attempt");
            }
        }
        _ => unreachable!("ledger advance bounded above"),
    }
    Ok(())
}

fn require_signal_history_continuity(
    baseline: &[f64],
    current: &[f64],
    sequence_advance: u64,
    signal_window_samples: usize,
) -> Result<()> {
    if baseline.len() > signal_window_samples || current.len() > signal_window_samples {
        bail!("Arcus runtime signal history exceeds the configured window");
    }
    match sequence_advance {
        0 => {
            if current != baseline {
                bail!("Arcus runtime signal history changed without a new observation");
            }
        }
        1 => {
            // A newer tick increments sequence before validating the snapshot,
            // so a structurally invalid observation may leave the history
            // untouched. If it appends a sample, every retained baseline value
            // must remain byte-for-byte equal and in order; a full window drops
            // exactly its oldest value.
            if current == baseline {
                return Ok(());
            }
            let expected_len = baseline.len().saturating_add(1).min(signal_window_samples);
            let dropped = baseline
                .len()
                .saturating_add(1)
                .saturating_sub(expected_len);
            let retained = &baseline[dropped..];
            if current.len() != expected_len || !current.starts_with(retained) {
                bail!("Arcus runtime retained signal history changed across restart/rollback");
            }
        }
        _ => {
            bail!("Arcus runtime advanced by {sequence_advance} observations; expected at most one")
        }
    }
    Ok(())
}

fn require_arcus_state_continuity(
    config: &ArcusSpotExecuteOnceConfig,
    baseline: &ArcusSpotStateImage,
    current: &ArcusSpotStateImage,
    acceptance_not_before: DateTime<Utc>,
    acceptance_not_after: DateTime<Utc>,
) -> Result<()> {
    let baseline_runtime = baseline.runtime.state();
    let current_runtime = current.runtime.state();
    let sequence_advance = current_runtime
        .sequence
        .checked_sub(baseline_runtime.sequence)
        .context("Arcus runtime sequence regressed across restart/rollback")?;
    require_signal_history_continuity(
        &baseline_runtime.relative_log_price_history,
        &current_runtime.relative_log_price_history,
        sequence_advance,
        config.runtime.signal_window_samples,
    )?;
    require_risk_state_continuity(
        &config.runtime,
        baseline_runtime,
        current_runtime,
        sequence_advance,
        acceptance_not_before,
        acceptance_not_after,
    )?;
    if sequence_advance == 1
        && current_runtime.last_observation_at != baseline_runtime.last_observation_at
    {
        let accepted_at = current_runtime
            .last_observation_at
            .context("Arcus runtime advanced its observation watermark to an empty value")?;
        if accepted_at < acceptance_not_before || accepted_at > acceptance_not_after {
            bail!("Arcus runtime last observation is outside the approved tick window");
        }
    }
    match (
        baseline_runtime.last_observation_at,
        current_runtime.last_observation_at,
    ) {
        (baseline_at, current_at) if sequence_advance == 0 && current_at != baseline_at => {
            bail!("Arcus runtime last observation changed without a new observation")
        }
        (Some(baseline_at), Some(current_at)) if current_at < baseline_at => {
            bail!("Arcus runtime last observation regressed across restart/rollback")
        }
        (Some(_), None) => {
            bail!("Arcus runtime lost its last observation across restart/rollback")
        }
        _ => {}
    }
    require_acceptance_ledger_and_position_continuity(
        config,
        baseline,
        current,
        sequence_advance,
        acceptance_not_before,
        acceptance_not_after,
    )
}

fn verify_arcus_state_backup(
    config: &ArcusSpotExecuteOnceConfig,
    backup_dir: &Path,
    exact: bool,
) -> Result<ArcusSpotStateVerificationReport> {
    verify_arcus_state_backup_at(config, backup_dir, exact, Utc::now())
}

/// `verify_arcus_state_backup` with the verification clock supplied rather
/// than read from the system, mirroring `create_arcus_state_backup_at` on the
/// capture side.
///
/// `verified_at` is the upper bound of the acceptance window every continuity
/// check is judged against, so a fixture that pins its capture time but lets
/// this end run on the wall clock describes a window that widens every day it
/// is not run. Two tests asserting rejection *past* that bound silently
/// stopped testing anything once real time overtook their hardcoded dates
/// (bot-strategy#810); pinning both ends keeps the scenario the fixture
/// describes fixed.
fn verify_arcus_state_backup_at(
    config: &ArcusSpotExecuteOnceConfig,
    backup_dir: &Path,
    exact: bool,
    verified_at: DateTime<Utc>,
) -> Result<ArcusSpotStateVerificationReport> {
    let (manifest, baseline) = load_arcus_state_backup(config, backup_dir)?;
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;
    let current = capture_arcus_state(config)?;
    if exact {
        require_file_matches_manifest(
            &current.checkpoint_bytes,
            &manifest.runtime_checkpoint,
            "live runtime checkpoint",
        )?;
        require_file_matches_manifest(
            &current.ledger_bytes,
            &manifest.execution_ledger,
            "live execution ledger",
        )?;
        match (&manifest.pending_plan, &current.pending_plan_bytes) {
            (Some(expected), Some(bytes)) => {
                require_file_matches_manifest(bytes, expected, "live pending plan")?
            }
            (None, None) => {}
            _ => bail!("live pending-plan presence changed since the backup"),
        }
        match (
            &manifest.observation_evidence,
            &current.observation_evidence_bytes,
        ) {
            (Some(expected), Some(bytes)) => {
                require_file_matches_manifest(bytes, expected, "live observation evidence")?
            }
            (None, None) => {}
            _ => bail!("live observation-evidence presence changed since the backup"),
        }
    } else {
        require_arcus_state_continuity(
            config,
            &baseline,
            &current,
            manifest.captured_at,
            verified_at,
        )?;
    }
    Ok(ArcusSpotStateVerificationReport {
        status: "verified",
        mode: if exact { "exact" } else { "continuity" },
        config_sha256: manifest.config_sha256,
        runtime_checkpoint_sha256: sha256_prefixed(&current.checkpoint_bytes),
        execution_ledger_sha256: sha256_prefixed(&current.ledger_bytes),
        pending_plan_sha256: current.pending_plan_bytes.as_deref().map(sha256_prefixed),
        observation_evidence_sha256: current
            .observation_evidence_bytes
            .as_deref()
            .map(sha256_prefixed),
        runtime: runtime_state_summary(&current.runtime),
        ledger: ledger_state_summary(&current.ledger),
    })
}

fn usage() -> &'static str {
    "usage:
  arcus-spot-execute-once keygen PRIVATE_KEY_FILE
  arcus-spot-execute-once hash CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once hash-config CONFIG_YAML
  arcus-spot-execute-once state-backup CONFIG_YAML BACKUP_DIR
  arcus-spot-execute-once state-verify-exact CONFIG_YAML BACKUP_DIR
  arcus-spot-execute-once state-verify-continuity CONFIG_YAML BACKUP_DIR
  arcus-spot-execute-once sign-approval DIGEST PRIVATE_KEY_FILE
  arcus-spot-execute-once execute CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
  arcus-spot-execute-once auto-execute CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once resume CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
  arcus-spot-execute-once auto-resume CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once live-tick CONFIG_YAML
  arcus-spot-execute-once clear-risk-halt CONFIG_YAML
  arcus-spot-execute-once repair-report CONFIG_YAML EVENTS_JSONL
  arcus-spot-execute-once manual-reconcile-report CONFIG_YAML EVENTS_JSONL \
      EXPECTED_SELL_AMOUNT_RAW EXPECTED_BUY_AMOUNT_RAW
  arcus-spot-execute-once manual-reconcile-apply CONFIG_YAML EVENTS_JSONL \
      EXPECTED_SELL_AMOUNT_RAW EXPECTED_BUY_AMOUNT_RAW SEQUENCE IDEMPOTENCY_KEY TX_HASH
  arcus-spot-execute-once archive-rejected-report CONFIG_YAML
  arcus-spot-execute-once archive-rejected-apply CONFIG_YAML SEQUENCE

archive-rejected-report/archive-rejected-apply are the recovery path for an
active attempt stuck in phase Rejected (bot-strategy#898): the router
refused a submission (or a prepared plan aged out before dispatch) before
any transaction was ever sent, so there is nothing to reconcile financially,
but nothing previously moved it out of the ledger's single `active` slot
either -- every later tick's resume/require_non_terminal_failure check
correctly (and permanently) refuses to proceed past it, by design, since a
rejected outcome must never be silently retried. -report only ever loads
the ledger and never mutates anything; it also refuses (as `not_eligible`)
a Rejected attempt that somehow carries a tx_hash, since that would mean a
transaction may actually have reached the chain and needs
repair-report/manual-reconcile's heavier on-chain verification instead of a
plain archive. Run -report first, then -apply with the exact sequence it
reported, so a new attempt that started between the two calls is refused
rather than silently archived. Both require CONFIG_YAML to match
auto_execute_policy.json's administrator-approved digest (same gate as
auto-execute/auto-resume/clear-risk-halt/manual-reconcile-report/apply)
before doing anything else.

manual-reconcile-report/manual-reconcile-apply are the last-resort recovery
path for exactly the incident class repair-report's own report describes as
no_digest_match: an active Submitted/Confirmed/Reconciled attempt whose
dispatched plan cannot be reproduced byte-exact from the durable event
archive (a fresher quote at dispatch time diverged it from its logged
WouldRotate observation), so the ordinary digest-checked resume path can
never resolve it. They resolve the same single archive-matching WouldRotate
candidate repair-report would (refusing on zero or more than one match,
verified as a hash-chain-continuous, monotonic-sequence slice of the real
event stream, not just each record's own self-consistency) for its
direction/trigger only. Committed sell/buy quantities never come from that
candidate's own sell_quantity/buy_quantity/buy_amount_raw fields -- instead
the caller independently attests the settled sell/buy raw amounts (from the
caller's own chain verification, e.g. eth_getTransactionReceipt logs plus
balanceOf deltas), cross-checked against the deltas the ledger's own
EIP-1898-pinned reconciliation already computed, and those raw amounts are
converted to quantities using CONFIG_YAML's own router.trusted_token_decimals
pin -- so nothing about the committed quantities can be steered by a forged
or spliced archive candidate, only by CONFIG_YAML itself (administrator-gated,
see below) and the caller's own attestation (cross-checked against the
ledger). -report only ever loads the ledger file and never mutates anything,
including when the preview looks correct; run it first, then -apply with the
exact same arguments plus this attempt's sequence/idempotency_key/tx_hash
pinned explicitly. Both require CONFIG_YAML to match
auto_execute_policy.json's administrator-approved digest (same gate as
auto-execute/auto-resume/clear-risk-halt) before doing anything else. -apply
then resumes the attempt toward Reconciled (pure on-chain status/balance
reads, exactly like auto-resume) and only then commits, archiving the
attempt afterward. Neither command is reachable from
execute/auto-execute/resume/auto-resume/live-tick.

state-backup and state-verify-* are offline operator commands. They never
construct an RPC/router client, KMS signer, approval policy, or executor and
cannot submit a swap. state-backup takes the same exclusive checkpoint lock
as live-tick, requires an already-existing valid checkpoint and ledger, and
publishes a mode-0700 backup directory atomically with mode-0600 copies and a
SHA-256/config-bound manifest. state-verify-exact proves byte identity while
the timer remains stopped (including recovery/observation evidence, if present).
state-verify-continuity is the post-start check: it permits normal checkpoint
and ledger advancement but refuses sequence/history regression, lost attempts,
or a position-state change without a corresponding ledger change. Neither
command restores or deletes live state.

live-tick is the unattended-probe entry point. Before accepting a new market
snapshot, it resumes any active ledger attempt from the digest-bound original
pending-plan evidence; unresolved or mismatched evidence fails closed without
advancing the signal checkpoint. With no active attempt, it fetches exactly one
live snapshot itself (the same public, read-only recorder client
arcus-spot-propose-plan and the archival collector use -- never a
caller-supplied file, which would have no authenticated origin), evaluates
the strategy signal (ArcusSpotRuntime::step_at) against it, always persists
the resulting runtime checkpoint under an exclusive lock, and only when
that genuinely decides WouldRotate does it build and dispatch a plan --
through the same policy-gated, signatureless path as auto-execute. Meant to
be invoked on a timer; most ticks decide Observe and touch neither the KMS
signer nor the submission network. step_at itself (in the shared runtime,
tracked in the checkpointed state so every writer of it -- live-tick and
arcus-spot-propose-plan alike -- is covered) rejects a snapshot whose
collection_finished_at is not strictly newer than the last one it actually
advanced on: re-consuming or reordering an observation would artificially
reweight the z-score history. Every accepted observation is durably bound to
its recorder snapshot and evaluation time in
<runtime_state_path's directory>/live-tick-observation-evidence.json. Before
dispatching, it also writes the plan-bearing recovery envelope to
<runtime_state_path's directory>/live-tick-pending-plan.json (both mode 0600);
while an attempt is active that file cannot be replaced by a later signal. If the
process exits after Submitted but before confirmation, the next live-tick
resumes it automatically; auto-resume CONFIG_YAML <that path> remains the
manual recovery command.

auto-execute/auto-resume/live-tick skip the offline human approval signature
(explicit owner decision while total inventory at risk stays small -- see
the comment at their call sites). Every other gate execute/resume enforce
is unchanged: plan/config validation, staleness, on-chain preflight,
exact-value Permit2, slippage, and loss stops. In place of the signature,
CONFIG_YAML must byte-for-byte match the exact configuration an
administrator approved by sha256 digest, recorded in an administrator-owned
policy file at /etc/arcus-spot/auto_execute_policy.json (same ownership/mode
trust model as approval_public_key, see docs/arcus-spot-runtime.md for its
schema) -- otherwise the executor identity could bypass the daily swap cap,
stakes ceiling, or any other config field by supplying fresh values itself.

The config digest only authenticates *the execution*, not *the strategy
decision* a plan claims to represent -- it says nothing about whether
entry_z_score was genuinely crossed, or whether the round-trip-cost,
rotation-fraction, and inventory-imbalance gates step_at itself enforces
actually held. auto-execute therefore refuses a caller-supplied
entry_signal-triggered PLAN_JSON outright: only execute's offline
signature, or live-tick's own checkpoint-lock-provenanced plan, may
dispatch an entry. A mean-reversion-exit/max-hold-exit plan is still
accepted -- it is risk-reducing and already bounded by the runtime
checkpoint's own genuinely-open rotated quantity.

keygen/sign-approval are meant to run on a separate, offline machine: the
resulting private key file must never be copied to the host that runs
execute/resume. execute/resume require keygen's printed public key at
/etc/arcus-spot/approval_public_key, deployed by an administrator and
owned by a different uid than the one running this binary -- never in
CONFIG_YAML or an inherited environment variable, either of which the
executor identity itself could set.

keygen writes the private key passphrase-encrypted (Argon2id + AES-256-GCM)
to PRIVATE_KEY_FILE, and prompts interactively for the passphrase (twice,
to confirm) -- it is never accepted as a command-line argument or read from
an environment variable, both of which would leak into shell history or
the process list. sign-approval prompts for the same passphrase once to
decrypt. There is no passphrase recovery: losing it makes that key file
permanently undecryptable, but the wallet itself holds no funds tied to
this key -- regenerate a fresh keypair with keygen and have an
administrator redeploy the new public key."
}

/// Resolves `.`/`..` components purely lexically -- no filesystem access,
/// so this works even before any of these files exist (the common case for
/// a fresh deployment) unlike `fs::canonicalize`. Sufficient for collision
/// detection between the absolute paths `validate_config` compares:
/// without it, `runtime_state_path=/var/lib/x/sub/../runtime.json` and
/// `ledger_path=/var/lib/x/live-tick-pending-plan.json` compare unequal by
/// raw `PathBuf` even though the derived pending-plan path (built from
/// `runtime_state_path`'s parent) actually resolves to the ledger (Codex
/// P2 follow-up, pairtrade#186).
fn lexically_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                if !matches!(
                    normalized.components().next_back(),
                    None | Some(std::path::Component::RootDir)
                        | Some(std::path::Component::Prefix(_))
                ) {
                    normalized.pop();
                }
            }
            std::path::Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

/// Resolves symlinks in `path`'s parent directory (via `fs::canonicalize`,
/// which needs that directory to already exist) and rejoins the file name
/// -- closing the gap `lexically_normalize` alone leaves open: a symlinked
/// directory component makes two paths that ultimately name the same file
/// on disk compare unequal lexically (e.g. `/var/lib/arcus/alias ->
/// /var/lib/arcus/state` makes `alias/x.json` and `state/x.json` look like
/// different files even though they are the same one) (Codex P2 follow-up,
/// pairtrade#186). Falls back to the lexically-normalized path unchanged
/// when the parent doesn't exist yet -- config validation must keep
/// working before an operator has created these directories, and an
/// as-yet-nonexistent parent cannot itself be a symlink pointing somewhere
/// collision-relevant.
fn resolve_path_for_collision_check(path: &Path) -> PathBuf {
    // Walk up from the path itself (deliberately *not* lexically
    // normalized first) toward the root, canonicalizing the longest
    // existing prefix and reappending whatever trailing components don't
    // exist yet on top of it. Pre-collapsing `..` as plain text before
    // canonicalizing is wrong whenever it crosses a symlink boundary:
    // with `/var/lib/base/alias -> /var/lib/other/child`,
    // `alias/../sibling` actually names `/var/lib/other/sibling` (go
    // through the symlink, then up from *its target's* parent), not
    // `/var/lib/base/sibling` -- a textual collapse of `alias/..` can't
    // know that, and only `fs::canonicalize`, given the still-embedded
    // `..` and symlink together, resolves it correctly (Codex P2
    // follow-up, pairtrade#186). `fs::canonicalize` only accepts a path
    // that exists in full, so this still has to walk up from the leaf
    // for the (common) case where the file itself doesn't exist yet;
    // `lexically_normalize` is applied once at the very end, to the
    // combined (by-then symlink-free) result, purely to collapse a `..`
    // that landed in the not-yet-existing trailing suffix.
    let mut ancestor = path;
    let mut pending_components: Vec<&std::ffi::OsStr> = Vec::new();
    let canonical_ancestor = loop {
        match fs::canonicalize(ancestor) {
            Ok(canonical) => break canonical,
            Err(_) => match (ancestor.file_name(), ancestor.parent()) {
                (Some(name), Some(next)) => {
                    pending_components.push(name);
                    ancestor = next;
                }
                // No existing ancestor anywhere in the prefix (or we
                // walked off the top of the path) -- nothing left to
                // canonicalize against.
                _ => return lexically_normalize(path),
            },
        }
    };
    let mut combined = canonical_ancestor;
    for component in pending_components.into_iter().rev() {
        combined.push(component);
    }
    lexically_normalize(&combined)
}

fn validate_config(config: &mut ArcusSpotExecuteOnceConfig) -> Result<()> {
    if !config.ledger_path.is_absolute() || !config.runtime_state_path.is_absolute() {
        bail!("Arcus ledger_path and runtime_state_path must be absolute");
    }
    let ledger_path = resolve_path_for_collision_check(&config.ledger_path);
    let runtime_state_path = resolve_path_for_collision_check(&config.runtime_state_path);
    if ledger_path == runtime_state_path {
        bail!("Arcus ledger_path and runtime_state_path must be distinct");
    }
    // live-tick's fixed, derived pending-plan path must not alias either
    // durable state file: it atomically replaces whatever sits at that
    // path with plan JSON before constructing the executor, so if
    // ledger_path or runtime_state_path happened to resolve there, that
    // write would destroy the checkpoint or ledger outright and the
    // subsequent fresh load would fail (Codex P2 follow-up, pairtrade#186).
    let pending_plan_path = resolve_path_for_collision_check(&live_tick_pending_plan_path(config)?);
    if pending_plan_path == ledger_path || pending_plan_path == runtime_state_path {
        bail!(
            "Arcus ledger_path/runtime_state_path must not resolve to the derived live-tick pending-plan path {}",
            pending_plan_path.display()
        );
    }
    let observation_evidence_path =
        resolve_path_for_collision_check(&live_tick_observation_evidence_path(config)?);
    let pending_event_path =
        resolve_path_for_collision_check(&live_tick_pending_event_path(config)?);
    let event_stream_path =
        resolve_path_for_collision_check(live_tick_event_stream(config)?.directory());
    if ledger_path == event_stream_path
        || ledger_path.starts_with(&event_stream_path)
        || runtime_state_path == event_stream_path
        || runtime_state_path.starts_with(&event_stream_path)
    {
        bail!(
            "Arcus ledger_path/runtime_state_path must not resolve to or beneath the derived live-tick event-stream directory {}",
            event_stream_path.display()
        );
    }
    if observation_evidence_path == ledger_path
        || observation_evidence_path == runtime_state_path
        || observation_evidence_path == pending_plan_path
    {
        bail!(
            "Arcus durable state paths must not resolve to the derived live-tick observation-evidence path {}",
            observation_evidence_path.display()
        );
    }
    if pending_event_path == ledger_path
        || pending_event_path == runtime_state_path
        || pending_event_path == pending_plan_path
        || pending_event_path == observation_evidence_path
    {
        bail!(
            "Arcus durable state paths must not resolve to the derived live-tick pending-event path {}",
            pending_event_path.display()
        );
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

fn parse_approval_public_key(hex_key: &str) -> Result<VerifyingKey> {
    let bytes = hex::decode(hex_key.trim()).context("approval_public_key must be hex-encoded")?;
    let bytes: [u8; PUBLIC_KEY_LENGTH] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("approval_public_key must be {PUBLIC_KEY_LENGTH} bytes"))?;
    VerifyingKey::from_bytes(&bytes).context("approval_public_key is not a valid Ed25519 point")
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
    // Lock on runtime_state_path, not ledger_path: the runtime checkpoint is
    // the single shared source of truth two racing invocations could
    // otherwise both dispatch against, while ledger_path is only where this
    // particular invocation happens to persist its own attempt history
    // (Codex P1 follow-up, pairtrade#181).
    ArcusSpotLiveExecutor::new(
        config.executor.clone(),
        config.runtime.pair.clone(),
        client,
        chain,
        signer,
        store,
        &config.runtime_state_path,
    )
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

fn write_live_tick_event(event: &ArcusSpotRuntimeEvent) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer_pretty(&mut stdout, event)
        .context("failed to serialize live-tick event")?;
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
    let plan = plan_from_document(&plan_bytes, &format!("plan {}", plan_path.display()))?;
    Ok((config, plan))
}

/// Verify `approval_signature_hex` is a genuine Ed25519 signature, under
/// `approval_public_key` (sourced from `ARCUS_APPROVAL_PUBLIC_KEY`, never
/// from the config/plan files this digest itself covers -- see the comment
/// on `ArcusSpotExecuteOnceConfig`), over the canonical config+plan digest.
/// Returns that digest for the caller to bind into the execution ledger.
/// Unlike comparing the digest directly against a caller-supplied copy of
/// itself, this cannot be satisfied by anything the executor could compute
/// on its own: only whoever holds the matching private key (never present
/// on this host) can produce a valid signature.
fn require_approval_signature(
    config: &ArcusSpotExecuteOnceConfig,
    plan: &ArcusSpotRotationPlan,
    approval_public_key: &VerifyingKey,
    approval_signature_hex: &str,
) -> Result<String> {
    let computed_digest = approval_digest(config, plan)?;
    verify_approval_signature(
        &computed_digest,
        approval_public_key,
        approval_signature_hex,
    )?;
    Ok(computed_digest)
}

fn verify_approval_signature(
    digest: &str,
    public_key: &VerifyingKey,
    approval_signature_hex: &str,
) -> Result<()> {
    let signature_bytes = hex::decode(approval_signature_hex.trim())
        .context("approval signature must be hex-encoded")?;
    let signature_bytes: [u8; SIGNATURE_LENGTH] = signature_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("approval signature must be {SIGNATURE_LENGTH} bytes"))?;
    let signature = Signature::from_bytes(&signature_bytes);
    public_key
        .verify_strict(digest.as_bytes(), &signature)
        .context(
        "approval signature does not verify against approval_public_key for this exact config+plan",
    )
}

// Argon2id parameters (OWASP-recommended minimum as of this writing: 19 MiB
// memory, 2 iterations, 1 degree of parallelism). Recorded in every
// encrypted key file rather than re-derived from a compile-time constant,
// so a future change to these defaults never breaks decrypting an
// already-written file.
const ARGON2ID_M_COST_KIB: u32 = 19_456;
const ARGON2ID_T_COST: u32 = 2;
const ARGON2ID_P_COST: u32 = 1;
const APPROVAL_KEY_FILE_VERSION: u32 = 1;
const KDF_SALT_LEN: usize = 16;
const AES_GCM_NONCE_LEN: usize = 12;

/// On-disk format for a passphrase-encrypted Ed25519 approval private key
/// (bot-strategy#772). The raw key never touches disk in plaintext; only
/// this struct, serialized as JSON, does. Every parameter needed to
/// reproduce the exact symmetric key is stored alongside the ciphertext,
/// so this file is self-describing and never depends on a compile-time
/// default that could silently change between the keygen and
/// sign-approval invocations (e.g. across a binary upgrade).
#[derive(Serialize, Deserialize)]
struct EncryptedApprovalKey {
    version: u32,
    kdf: String,
    kdf_salt_hex: String,
    kdf_m_cost_kib: u32,
    kdf_t_cost: u32,
    kdf_p_cost: u32,
    cipher: String,
    nonce_hex: String,
    ciphertext_hex: String,
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    buf
}

fn hex_decode_exact<const N: usize>(hex_str: &str, label: &str) -> Result<[u8; N]> {
    let bytes = hex::decode(hex_str).with_context(|| format!("{label} is not valid hex"))?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow::anyhow!("{label} must be {N} bytes, got {}", bytes.len()))
}

fn derive_symmetric_key(
    passphrase: &[u8],
    salt: &[u8],
    m_cost_kib: u32,
    t_cost: u32,
    p_cost: u32,
) -> Result<Zeroizing<[u8; 32]>> {
    let params = Params::new(m_cost_kib, t_cost, p_cost, Some(32))
        .map_err(|error| anyhow::anyhow!("invalid Argon2id parameters: {error}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = Zeroizing::new([0u8; 32]);
    argon2
        .hash_password_into(passphrase, salt, key.as_mut())
        .map_err(|error| anyhow::anyhow!("Argon2id key derivation failed: {error}"))?;
    Ok(key)
}

/// Prompts twice (and requires the two entries to match) so a typo when
/// setting the passphrase can't silently lock the key behind a passphrase
/// the user didn't intend and has no record of.
fn read_new_passphrase() -> Result<Zeroizing<Vec<u8>>> {
    let first = Zeroizing::new(
        rpassword::prompt_password("Passphrase for the new approval key: ")
            .context("failed to read passphrase")?,
    );
    let second = Zeroizing::new(
        rpassword::prompt_password("Confirm passphrase: ")
            .context("failed to read passphrase confirmation")?,
    );
    if *first != *second {
        bail!("passphrases did not match");
    }
    if first.is_empty() {
        bail!("passphrase must not be empty");
    }
    Ok(Zeroizing::new(first.as_bytes().to_vec()))
}

fn read_existing_passphrase(prompt: &str) -> Result<Zeroizing<Vec<u8>>> {
    let passphrase =
        Zeroizing::new(rpassword::prompt_password(prompt).context("failed to read passphrase")?);
    Ok(Zeroizing::new(passphrase.as_bytes().to_vec()))
}

fn encrypt_signing_key(
    signing_key: &SigningKey,
    passphrase: &[u8],
) -> Result<EncryptedApprovalKey> {
    let salt = random_bytes::<KDF_SALT_LEN>();
    let symmetric_key = derive_symmetric_key(
        passphrase,
        &salt,
        ARGON2ID_M_COST_KIB,
        ARGON2ID_T_COST,
        ARGON2ID_P_COST,
    )?;
    let cipher = Aes256Gcm::new_from_slice(symmetric_key.as_ref())
        .context("failed to initialize AES-256-GCM")?;
    let nonce_bytes = random_bytes::<AES_GCM_NONCE_LEN>();
    let nonce = Nonce::from_slice(&nonce_bytes);
    let mut seed = signing_key.to_bytes();
    let ciphertext = cipher
        .encrypt(nonce, seed.as_ref())
        .map_err(|_| anyhow::anyhow!("AES-256-GCM encryption failed"))?;
    seed.zeroize();
    Ok(EncryptedApprovalKey {
        version: APPROVAL_KEY_FILE_VERSION,
        kdf: "argon2id".to_string(),
        kdf_salt_hex: hex::encode(salt),
        kdf_m_cost_kib: ARGON2ID_M_COST_KIB,
        kdf_t_cost: ARGON2ID_T_COST,
        kdf_p_cost: ARGON2ID_P_COST,
        cipher: "aes-256-gcm".to_string(),
        nonce_hex: hex::encode(nonce_bytes),
        ciphertext_hex: hex::encode(ciphertext),
    })
}

fn decrypt_signing_key(encrypted: &EncryptedApprovalKey, passphrase: &[u8]) -> Result<SigningKey> {
    if encrypted.version != APPROVAL_KEY_FILE_VERSION {
        bail!(
            "unsupported approval key file version {} (expected {APPROVAL_KEY_FILE_VERSION})",
            encrypted.version
        );
    }
    if encrypted.kdf != "argon2id" {
        bail!("unsupported approval key file kdf {:?}", encrypted.kdf);
    }
    if encrypted.cipher != "aes-256-gcm" {
        bail!(
            "unsupported approval key file cipher {:?}",
            encrypted.cipher
        );
    }
    let salt: [u8; KDF_SALT_LEN] = hex_decode_exact(&encrypted.kdf_salt_hex, "kdf_salt_hex")?;
    let nonce_bytes: [u8; AES_GCM_NONCE_LEN] = hex_decode_exact(&encrypted.nonce_hex, "nonce_hex")?;
    let ciphertext =
        hex::decode(&encrypted.ciphertext_hex).context("ciphertext_hex is not valid hex")?;
    let symmetric_key = derive_symmetric_key(
        passphrase,
        &salt,
        encrypted.kdf_m_cost_kib,
        encrypted.kdf_t_cost,
        encrypted.kdf_p_cost,
    )?;
    let cipher = Aes256Gcm::new_from_slice(symmetric_key.as_ref())
        .context("failed to initialize AES-256-GCM")?;
    let nonce = Nonce::from_slice(&nonce_bytes);
    let mut plaintext = cipher.decrypt(nonce, ciphertext.as_ref()).map_err(|_| {
        anyhow::anyhow!("failed to decrypt the approval key -- wrong passphrase or corrupted file")
    })?;
    let seed: [u8; SECRET_KEY_LENGTH] = plaintext.as_slice().try_into().map_err(|_| {
        anyhow::anyhow!(
            "decrypted approval key must be exactly {SECRET_KEY_LENGTH} bytes, got {}",
            plaintext.len()
        )
    })?;
    plaintext.zeroize();
    let signing_key = SigningKey::from_bytes(&seed);
    let mut seed = seed;
    seed.zeroize();
    Ok(signing_key)
}

fn read_ed25519_signing_key(path: &Path) -> Result<SigningKey> {
    let bytes = read_private_regular_file(path, "private key")?;
    let encrypted: EncryptedApprovalKey = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse encrypted approval key {}", path.display()))?;
    let passphrase = read_existing_passphrase("Passphrase for the approval key: ")?;
    decrypt_signing_key(&encrypted, &passphrase)
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [command, key_path] if command == "keygen" => {
            let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
            let passphrase = read_new_passphrase()?;
            let encrypted = encrypt_signing_key(&signing_key, &passphrase)?;
            let payload = serde_json::to_vec_pretty(&encrypted)
                .context("failed to serialize the encrypted approval key")?;
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(key_path)
                .with_context(|| format!("failed to create {key_path}"))?;
            file.write_all(&payload)?;
            eprintln!("wrote passphrase-encrypted private key to {key_path} -- copy it to an offline machine ONLY, never to the execute/resume host. The passphrase is not stored anywhere; losing it makes this file permanently undecryptable (regenerate a fresh key with keygen if that happens -- the wallet itself is unaffected, see docs/arcus-spot-runtime.md).");
            println!("{}", hex::encode(signing_key.verifying_key().to_bytes()));
            Ok(())
        }
        [command, config_path, plan_path] if command == "hash" => {
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            println!("{}", approval_digest(&config, &plan)?);
            Ok(())
        }
        [command, config_path] if command == "hash-config" => {
            // What an administrator runs once, against the exact CONFIG_YAML
            // being deployed, to populate auto_execute_policy.json's
            // approved_config_sha256 -- see docs/arcus-spot-runtime.md.
            let config_bytes = read_private_regular_file(Path::new(config_path), "config")?;
            let config = parse_config(&config_bytes, Path::new(config_path))?;
            println!("{}", auto_execute_config_digest(&config)?);
            Ok(())
        }
        [command, config_path] if command == "clear-risk-halt" => {
            // The only way an engaged halt is ever lifted. A halt is sticky
            // on purpose -- no later tick is evidence that whatever caused
            // it was dealt with -- so lifting it is an operator judgement,
            // taken deliberately, never a thing the runtime talks itself
            // into (bot-strategy#813).
            let config_bytes = read_private_regular_file(Path::new(config_path), "config")?;
            let config = parse_config(&config_bytes, Path::new(config_path))?;
            // Same administrator-approval gate as auto-execute/live-tick:
            // resuming a halted bot re-enables exactly the dispatch path
            // that gate governs, so it is held to the same standard. Not the
            // offline Ed25519 signature, deliberately -- requiring more to
            // *resume* dispatching than to dispatch would be theatre.
            let policy = auto_execute_policy_from_admin_file()?;
            require_config_within_auto_execute_policy(&config, &policy)?;

            let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
            // Same exclusive lock a dispatching tick takes, so this
            // read-modify-write cannot interleave with one committing a fill.
            let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;

            let mut runtime = store.load_existing(&config.runtime)?;
            // Captured before clearing, purely so the record below can show
            // what the marks were at that moment. The refusal itself lives
            // in `clear_risk_halt`, so it cannot be bypassed by reaching for
            // the runtime directly.
            let mark = runtime.last_risk_mark().context(
                "Arcus runtime has no reference prices to re-check the halt condition against",
            )?;
            let halt = runtime.clear_risk_halt().map_err(anyhow::Error::msg)?;
            store.persist(&runtime)?;
            // Printed rather than merely done: this is the audit record of a
            // risk control being disarmed, and it lands in the journal.
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "cleared": {
                        "kind": halt.kind,
                        "engaged_at": halt.engaged_at,
                        "equity_usd": halt.equity_usd.to_string(),
                        "loss_usd": halt.loss_usd.to_string(),
                        "limit_usd": halt.limit_usd.to_string(),
                    },
                    "mark_at_clear": {
                        "equity_usd": mark.equity_usd.to_string(),
                        "daily_loss_usd": mark.daily_loss_usd.to_string(),
                        "cumulative_loss_usd": mark.cumulative_loss_usd.to_string(),
                        "inventory_drawdown_usd": mark.inventory_drawdown_usd.to_string(),
                    },
                    "runtime_state_path": config.runtime_state_path,
                }))?
            );
            eprintln!(
                "[arcus-risk] cleared a {:?} halt engaged at {}; take a fresh state-backup, as \
                 backups from before this no longer verify",
                halt.kind, halt.engaged_at,
            );
            Ok(())
        }
        [command, config_path, backup_dir] if command == "state-backup" => {
            let config_bytes = read_private_regular_file(Path::new(config_path), "config")?;
            let config = parse_config(&config_bytes, Path::new(config_path))?;
            let manifest = create_arcus_state_backup(&config, Path::new(backup_dir))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&manifest)
                    .context("failed to serialize Arcus state backup result")?
            );
            Ok(())
        }
        [command, config_path, backup_dir]
            if command == "state-verify-exact" || command == "state-verify-continuity" =>
        {
            let config_bytes = read_private_regular_file(Path::new(config_path), "config")?;
            let config = parse_config(&config_bytes, Path::new(config_path))?;
            let report = verify_arcus_state_backup(
                &config,
                Path::new(backup_dir),
                command == "state-verify-exact",
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .context("failed to serialize Arcus state verification report")?
            );
            Ok(())
        }
        [command, config_path, events_jsonl_path] if command == "repair-report" => {
            repair_report(Path::new(config_path), Path::new(events_jsonl_path))
        }
        [command, config_path] if command == "archive-rejected-report" => {
            archive_rejected_report(Path::new(config_path))
        }
        [command, config_path, sequence] if command == "archive-rejected-apply" => {
            archive_rejected_apply(Path::new(config_path), sequence)
        }
        [command, config_path, events_jsonl_path, expected_sell_amount_raw, expected_buy_amount_raw]
            if command == "manual-reconcile-report" =>
        {
            manual_reconcile_report(
                Path::new(config_path),
                Path::new(events_jsonl_path),
                expected_sell_amount_raw,
                expected_buy_amount_raw,
            )
        }
        [command, config_path, events_jsonl_path, expected_sell_amount_raw, expected_buy_amount_raw, sequence, idempotency_key, tx_hash]
            if command == "manual-reconcile-apply" =>
        {
            manual_reconcile_apply(
                Path::new(config_path),
                Path::new(events_jsonl_path),
                expected_sell_amount_raw,
                expected_buy_amount_raw,
                sequence,
                idempotency_key,
                tx_hash,
            )
            .await
        }
        [command, digest, key_path] if command == "sign-approval" => {
            let signing_key = read_ed25519_signing_key(Path::new(key_path))?;
            let signature = signing_key.sign(digest.as_bytes());
            println!("{}", hex::encode(signature.to_bytes()));
            Ok(())
        }
        [command, config_path, plan_path, approval_signature] if command == "execute" => {
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            let approval_public_key = approval_public_key_from_admin_file()?;
            let plan_config_digest = require_approval_signature(
                &config,
                &plan,
                &approval_public_key,
                approval_signature,
            )?;
            // executor_from_config acquires the exclusive ledger lock
            // (inside ArcusSpotLiveExecutor::new); the runtime-checkpoint
            // consistency check must happen only *after* that, and must
            // re-read the checkpoint fresh rather than reuse anything
            // loaded earlier. A plan can pass every check above (venue,
            // symbols/direction, a genuinely signed approval) while still
            // being stale relative to the checkpoint another overlapping
            // `execute` invocation commits and archives while this one is
            // still constructing its client/chain/KMS signer -- checking
            // before the lock (or reusing a pre-lock read) leaves that
            // window open, letting a plan valid against the old regime
            // still be signed and dispatched against the now-stale state
            // (Codex P1 follow-up, pairtrade#181, refining an earlier
            // fix in the same area).
            let mut executor = executor_from_config(&config).await?;
            let runtime_store =
                ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            let runtime = runtime_store.load_or_create(&config.runtime)?;
            runtime
                .validate_plan_consistent_with_state(&plan)
                .map_err(anyhow::Error::msg)
                .context("Arcus plan is inconsistent with the current runtime checkpoint")?;
            let attempt = executor
                .execute_plan_once(&plan, &plan_config_digest)
                .await?;
            let attempt = finalize_reconciled_attempt(
                &config,
                &mut executor,
                &plan,
                &plan_config_digest,
                attempt,
            )?;
            write_attempt(&attempt)
        }
        [command, config_path, plan_path] if command == "auto-execute" => {
            // Skips the offline Ed25519 approval signature required by
            // `execute`. Explicit owner decision (bot-strategy#772,
            // 2026-08-12): while total inventory at risk stays small, the
            // per-swap human-signing round trip is pure friction with no
            // safety benefit proportionate to the amount at stake, and the
            // approval gate's original purpose -- proving this brand-new
            // execution path actually works against the real Arcus API
            // before trusting it unattended -- was already served by the
            // one-swap acceptance test's earlier signed attempts (which
            // exercised every other gate below: config/plan structural
            // validation, on-chain preflight, exact-value permit
            // construction, slippage, staleness). Every other safety gate
            // is unchanged and still enforced identically to `execute`:
            // plan/config structural validation, `max_plan_age_secs`/
            // `max_quote_age_secs`, inventory floors, daily/cumulative
            // loss stops, exact-value-only Permit2, and the runtime
            // checkpoint consistency check below. Revisit this decision
            // (return to requiring `execute` with a human signature, or
            // add a scale-dependent threshold) before any inventory
            // scale-up beyond what is currently approved on #772.
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            require_auto_execute_plan_is_not_a_fresh_entry(&plan)?;
            let policy = auto_execute_policy_from_admin_file()?;
            require_config_within_auto_execute_policy(&config, &policy)?;
            let plan_config_digest = approval_digest(&config, &plan)?;
            let mut executor = executor_from_config(&config).await?;
            let runtime_store =
                ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            let runtime = runtime_store.load_or_create(&config.runtime)?;
            runtime
                .validate_plan_consistent_with_state(&plan)
                .map_err(anyhow::Error::msg)
                .context("Arcus plan is inconsistent with the current runtime checkpoint")?;
            let attempt = executor
                .execute_plan_once(&plan, &plan_config_digest)
                .await?;
            let attempt = finalize_reconciled_attempt(
                &config,
                &mut executor,
                &plan,
                &plan_config_digest,
                attempt,
            )?;
            write_attempt(&attempt)
        }
        [command, config_path, plan_path] if command == "auto-resume" => {
            // Signatureless counterpart to `resume`, gated the same way as
            // `auto-execute`: an attempt dispatched via `auto-execute` that
            // comes back `Submitted` (not yet confirmed) or crashes before
            // runtime commit otherwise has no recovery path that doesn't
            // require the offline signature this command family exists to
            // skip -- an unattended flow that can start unattended but
            // then dead-ends waiting for a human is not actually unattended
            // (Codex P2 follow-up, pairtrade#186).
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            let policy = auto_execute_policy_from_admin_file()?;
            require_config_within_auto_execute_policy(&config, &policy)?;
            let plan_config_digest = approval_digest(&config, &plan)?;
            let mut executor = executor_from_config(&config).await?;
            let attempt = executor.resume_status_and_reconcile().await?;
            let attempt = finalize_reconciled_attempt(
                &config,
                &mut executor,
                &plan,
                &plan_config_digest,
                attempt,
            )?;
            write_attempt(&attempt)
        }
        [command, config_path] if command == "live-tick" => {
            // The unattended-probe entry point: evaluate the strategy
            // signal against one fresh recorder snapshot and, only if it
            // genuinely fires, dispatch through the exact same
            // policy-gated, signatureless path as `auto-execute`. Most
            // ticks decide `Observe` (no position warranted) and never
            // touch the KMS signer or the network beyond the one read-only
            // snapshot fetch below -- this is the "future read-only daemon
            // [that] must call step_at with the current UTC time" flagged
            // as not-yet-built in this same doc (bot-strategy#772/#775,
            // 7-day activity probe).
            let config_bytes = read_private_regular_file(Path::new(config_path), "config")?;
            let config = parse_config(&config_bytes, Path::new(config_path))?;
            let policy = auto_execute_policy_from_admin_file()?;
            require_config_within_auto_execute_policy(&config, &policy)?;

            if let Some(attempt) = resume_active_live_tick_attempt(&config).await? {
                return write_attempt(&attempt);
            }

            // Fetch the snapshot live, from the same public, read-only
            // recorder client the archival collector and
            // arcus-spot-propose-plan use -- never from a caller-supplied
            // file. An earlier version took RECORDER_SNAPSHOT_JSON as a
            // second argument; read_private_regular_file only checks its
            // mode/type, not its origin, so the executor identity could
            // fabricate an internally-consistent snapshot (prices, route
            // records) that drives step_at to EntrySignal even though the
            // real market never crossed the threshold, dispatched through
            // this exact signatureless path (Codex P1 follow-up,
            // pairtrade#186). Fetching it here, the same way propose-plan
            // does, means the snapshot's provenance is inherent rather
            // than merely asserted.
            let client = ArcusSpotClient::new(config.router.clone())
                .context("invalid Arcus router configuration")?;
            let recorder_config = ArcusSpotRecorderConfig::from_csv(
                &config.runtime.bidirectional_recorder_pairs_csv(),
                &config.runtime.notional_usd.normalize().to_string(),
            )
            .context(
                "failed to build a bidirectional recorder config from the runtime pair/notional",
            )?;
            let recorder = ArcusSpotRecorder::new(client, recorder_config)
                .context("invalid Arcus recorder configuration")?;
            let snapshot: ArcusSpotRecorderSnapshot = recorder.collect_once().await;

            let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            // Hold the same exclusive lock `execute`/`auto-execute` take
            // before dispatch, but scoped to just this read-modify-write: a
            // concurrent live-tick, auto-execute, or auto-resume racing this
            // one could otherwise commit a reconciled fill (new inventory,
            // regime, and idempotency key) between this tick's load and
            // persist, and this tick's persist would then silently replace
            // that newer state with one computed from the pre-fill
            // snapshot, letting a later tick re-plan against a position
            // that no longer exists. Released before executor_from_config
            // acquires its own fresh lock on the same namespace below --
            // holding it across dispatch too would make that acquisition
            // conflict with this one from inside the same process, since
            // flock is scoped per open file description, not per process
            // (Codex P1 follow-up, pairtrade#186).
            let ledger_store_for_checkpoint =
                ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
            let checkpoint_lock =
                ledger_store_for_checkpoint.acquire_exclusive_lock(&config.runtime_state_path)?;

            // The optimistic check before the public snapshot fetch is not
            // sufficient by itself: another live-tick can dispatch while
            // this invocation is collecting that snapshot. Re-read the
            // ledger under the same lock that guards the checkpoint before
            // advancing the runtime or replacing pending-plan evidence.
            if let Some((plan, plan_config_digest)) =
                load_live_tick_active_recovery_plan(&config, &ledger_store_for_checkpoint)?
            {
                drop(checkpoint_lock);
                let attempt = resume_live_tick_attempt(&config, plan, plan_config_digest).await?;
                return write_attempt(&attempt);
            }

            let mut runtime = store.load_or_create(&config.runtime)?;
            let event_publisher = live_tick_event_publisher(&config)?;
            event_publisher
                .recover(runtime.state().sequence)
                .context("failed to recover Arcus pending durable event")?;
            let previous_sequence = runtime.state().sequence;
            // step_at itself rejects a snapshot whose collection_finished_at
            // is not strictly newer than the last one it genuinely advanced
            // on -- tracked in the checkpointed state, under this same
            // lock, rather than in any caller-local bookkeeping, so it
            // correctly orders this invocation against a concurrent
            // arcus-spot-propose-plan (or another live-tick) writing the
            // same checkpoint (Codex P2 follow-up, pairtrade#186; see
            // ArcusSpotRuntimeState::last_observation_at's doc comment).
            let evaluation_time = Utc::now();
            let event = runtime.step_at(&snapshot, evaluation_time);
            // Captured now, under the lock, so dispatch below can bind the
            // plan to the exact observation it was computed from -- see
            // its use after the lock is re-acquired.
            let plan_observation_at = runtime.state().last_observation_at;
            if runtime.state().sequence != previous_sequence {
                let observation_evidence = ArcusSpotLiveTickObservationEvidence {
                    schema_version: OBSERVATION_EVIDENCE_SCHEMA_VERSION,
                    evaluation_time,
                    snapshot: snapshot.clone(),
                    resulting_runtime: Some(ArcusSpotObservationBoundary {
                        sequence: runtime.state().sequence,
                        last_observation_at: runtime.state().last_observation_at,
                    }),
                };
                let bytes = serde_json::to_vec_pretty(&observation_evidence)
                    .context("failed to serialize Arcus live-tick observation evidence")?;
                write_private_regular_file_atomic(
                    &live_tick_observation_evidence_path(&config)?,
                    &bytes,
                )?;
            }
            // Persisted unconditionally, independent of the decision below:
            // the accumulated price-history window is exactly what next
            // tick's signal depends on, and losing a tick's contribution
            // because this run happened not to rotate would silently widen
            // gaps in the very history the entry/exit z-score needs.
            event_publisher
                .stage(&event)
                .context("failed to stage Arcus durable event before checkpoint")?;
            store.persist(&runtime)?;
            // Journald is not the replay source of truth. Persist every
            // checkpointed decision, including WouldRotate ticks whose stdout
            // later becomes an execution attempt, to the private hash-chained
            // stream. This happens while the checkpoint lock is still held so
            // concurrent state writers cannot reorder events. The exact event
            // was staged before the checkpoint rename, so a later invocation
            // can finish an append interrupted after checkpoint publication.
            event_publisher
                .commit(&event)
                .context("failed to commit Arcus live-tick durable event")?;
            let plan = match event.decision.clone() {
                ArcusSpotDecision::WouldRotate { plan } => plan,
                ArcusSpotDecision::Observe { .. } | ArcusSpotDecision::SimulatedFill { .. } => {
                    drop(checkpoint_lock);
                    return write_live_tick_event(&event);
                }
            };
            // The router recommends whichever venue prices best, and that is
            // The executor supports the two venue paths whose typed data,
            // prepared transaction, canonical contracts, and reconciliation
            // semantics are explicitly validated: Arcus and Rialto. Any
            // other router result (notably LI.FI) is an ordinary market
            // outcome rather than a service fault.
            //
            // Treated as a fault until bot-strategy#817: the plan was built,
            // written, and only refused deep inside the executor, so the
            // unit exited non-zero. Twelve consecutive ticks on 2026-08-19
            // therefore looked like a failing service while the bot was in
            // fact behaving correctly, which is precisely the signal a real
            // fault would have needed to stand out from. Declining here
            // keeps the run successful and leaves no pending-plan file for a
            // dispatch that never happened. `validate_plan` still refuses
            // the same route independently.
            if !is_supported_live_route(&plan) {
                // Held until the record is written so two concurrent ticks
                // cannot interleave their declines out of order in the file.
                let declined = decline_unsupported_route(&config, &event, &plan);
                drop(checkpoint_lock);
                return declined;
            }
            let plan_config_digest = approval_digest(&config, &plan)?;
            // Durably record the plan and the recorder evidence from which
            // it was derived before dispatching it: unlike
            // execute/auto-execute, live-tick builds this plan itself
            // rather than receiving it as an argument the caller already
            // holds a copy of, so without this write, a crash or exit
            // between submission and confirmation leaves nothing for
            // `auto-resume` to recover with (Codex P2 follow-up,
            // pairtrade#186).
            let pending_plan_path = live_tick_pending_plan_path(&config)?;
            let evidence = ArcusSpotLiveTickEvidence {
                schema_version: LIVE_TICK_EVIDENCE_SCHEMA_VERSION,
                evaluation_time,
                snapshot,
                plan: plan.clone(),
            };
            let plan_bytes = serde_json::to_vec_pretty(&evidence)
                .context("failed to serialize Arcus live-tick evidence")?;
            write_private_regular_file_atomic(&pending_plan_path, &plan_bytes)?;
            // Keep checkpoint + pending-plan backup capture coherent: the
            // pending plan is part of Submitted-crash recovery, so publish
            // it while the same checkpoint namespace lock is still held.
            // Backup/verify takes this lock too and therefore observes
            // either the old complete state or this new complete state,
            // never a checkpoint from one tick with a plan from another.
            drop(checkpoint_lock);
            let mut executor = executor_from_config(&config).await?;
            // Re-read fresh, same reasoning as `execute`'s own comment
            // above: the plan above was computed before the ledger lock
            // (acquired inside executor_from_config) was held, so another
            // overlapping invocation could have advanced the checkpoint in
            // between.
            let runtime_store =
                ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            let fresh_runtime = runtime_store.load_or_create(&config.runtime)?;
            // validate_plan_consistent_with_state only checks regime/
            // trigger/direction/open-quantity structural consistency, not
            // that this plan corresponds to the checkpoint's *current*
            // observation -- a concurrent live-tick or propose-plan
            // processing a newer snapshot after this invocation's own
            // checkpoint lock was dropped (above) could persist a new
            // signal state whose regime happens to still be structurally
            // consistent even though the newer observation itself now
            // says Observe, or favors the opposite direction. Reject
            // outright if the checkpoint has moved past the exact
            // observation this plan was computed from, rather than
            // dispatching an entry based on a signal state that no longer
            // reflects the runtime's own most recent evaluation (Codex P1
            // follow-up, pairtrade#186).
            if fresh_runtime.state().last_observation_at != plan_observation_at {
                bail!(
                    "Arcus live-tick plan is stale: the runtime checkpoint has advanced to a \
                     newer observation ({:?}) since this plan was computed from ({:?})",
                    fresh_runtime.state().last_observation_at,
                    plan_observation_at
                );
            }
            fresh_runtime
                .validate_plan_consistent_with_state(&plan)
                .map_err(anyhow::Error::msg)
                .context("Arcus plan is inconsistent with the current runtime checkpoint")?;
            let attempt = executor
                .execute_plan_once(&plan, &plan_config_digest)
                .await?;
            let attempt = finalize_reconciled_attempt(
                &config,
                &mut executor,
                &plan,
                &plan_config_digest,
                attempt,
            )?;
            write_attempt(&attempt)
        }
        [command, config_path, plan_path, approval_signature] if command == "resume" => {
            let (config, plan) =
                load_config_and_plan(Path::new(config_path), Path::new(plan_path))?;
            let approval_public_key = approval_public_key_from_admin_file()?;
            let plan_config_digest = require_approval_signature(
                &config,
                &plan,
                &approval_public_key,
                approval_signature,
            )?;
            let mut executor = executor_from_config(&config).await?;
            let attempt = executor.resume_status_and_reconcile().await?;
            let attempt = finalize_reconciled_attempt(
                &config,
                &mut executor,
                &plan,
                &plan_config_digest,
                attempt,
            )?;
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

    /// The instant these fixtures treat as "now".
    ///
    /// Every timestamp in this module is part of one hand-built scenario
    /// (backup captured 2026-08-15T23:58:00Z, observations through
    /// 2026-08-16T12:01:00Z), and the relationships between them are what the
    /// continuity assertions are about. Reading the real clock for any single
    /// one of them puts that timestamp an ever-growing distance from the rest,
    /// so a scenario that held when it was written quietly stops describing
    /// what its assertions claim -- which is exactly how bot-strategy#810
    /// happened. Anchor every fixture clock here instead; it sits just after
    /// the last fixture observation so a live tick reads as freshly accepted.
    fn fixture_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-16T12:05:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn create_arcus_state_backup(
        config: &ArcusSpotExecuteOnceConfig,
        backup_dir: &Path,
    ) -> Result<ArcusSpotStateBackupManifest> {
        let captured_at = DateTime::parse_from_rfc3339("2026-08-15T23:58:00Z")
            .unwrap()
            .with_timezone(&Utc);
        super::create_arcus_state_backup_at(config, backup_dir, captured_at)
    }

    /// Shadows the production entry point so every test in this module is
    /// verified against `fixture_now` rather than the wall clock. See
    /// `verify_arcus_state_backup_at`.
    fn verify_arcus_state_backup(
        config: &ArcusSpotExecuteOnceConfig,
        backup_dir: &Path,
        exact: bool,
    ) -> Result<ArcusSpotStateVerificationReport> {
        super::verify_arcus_state_backup_at(config, backup_dir, exact, fixture_now())
    }

    /// Keeps bot-strategy#810 from recurring.
    ///
    /// Its whole failure mode was silent: one fixture timestamp read the wall
    /// clock while the rest were hardcoded, so the scenario drifted apart over
    /// days until two tests asserted rejections that could no longer happen --
    /// and they kept "failing for the wrong reason" rather than pointing at
    /// the clock. Nothing structural stopped that, so this does: fixtures in
    /// this module get their time from `fixture_now`, never from the system.
    ///
    /// If a test genuinely needs the real clock, it needs its own deliberate
    /// justification -- move that call behind a named helper here and exempt
    /// the helper explicitly, rather than reintroducing a bare `Utc::now()`.
    #[test]
    fn test_fixtures_never_read_the_wall_clock() {
        let source = include_str!("arcus_spot_execute_once.rs");
        let tests_module = source
            .split_once("\nmod tests {")
            .expect("this file has a tests module")
            .1;
        // Split so this scanner's own source line is not a match for itself.
        let needle = concat!("Utc::", "now()");
        let offenders = tests_module
            .lines()
            .filter(|line| line.contains(needle))
            .filter(|line| !line.trim_start().starts_with("///"))
            .collect::<Vec<_>>();
        assert!(
            offenders.is_empty(),
            "test fixtures must anchor time to fixture_now(), not the wall clock \
             (bot-strategy#810); offending lines: {offenders:#?}",
        );
    }

    #[test]
    fn usage_exposes_explicit_resume_command() {
        assert!(usage().contains("resume CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX"));
        assert!(usage().contains("keygen"));
        assert!(usage().contains("sign-approval"));
        assert!(usage().contains("auto-execute CONFIG_YAML PLAN_JSON"));
        assert!(usage().contains("auto-resume CONFIG_YAML PLAN_JSON"));
        assert!(usage().contains("live-tick CONFIG_YAML"));
        assert!(!usage().contains("live-tick CONFIG_YAML RECORDER_SNAPSHOT_JSON"));
        assert!(usage().contains("hash-config CONFIG_YAML"));
        assert!(usage().contains("state-backup CONFIG_YAML BACKUP_DIR"));
        assert!(usage().contains("state-verify-exact CONFIG_YAML BACKUP_DIR"));
        assert!(usage().contains("state-verify-continuity CONFIG_YAML BACKUP_DIR"));
        assert!(usage().contains("clear-risk-halt CONFIG_YAML"));
        assert!(usage().contains("repair-report CONFIG_YAML EVENTS_JSONL"));
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

    #[test]
    fn encrypted_approval_key_round_trips_with_the_correct_passphrase() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let encrypted = encrypt_signing_key(&signing_key, b"correct horse battery staple").unwrap();
        let decrypted = decrypt_signing_key(&encrypted, b"correct horse battery staple").unwrap();
        assert_eq!(decrypted.to_bytes(), signing_key.to_bytes());
    }

    #[test]
    fn encrypted_approval_key_rejects_the_wrong_passphrase() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let encrypted = encrypt_signing_key(&signing_key, b"correct horse battery staple").unwrap();
        assert!(decrypt_signing_key(&encrypted, b"wrong passphrase").is_err());
    }

    #[test]
    fn encrypted_approval_key_rejects_tampered_ciphertext() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let mut encrypted =
            encrypt_signing_key(&signing_key, b"correct horse battery staple").unwrap();
        let mut raw = hex::decode(&encrypted.ciphertext_hex).unwrap();
        raw[0] ^= 0xFF;
        encrypted.ciphertext_hex = hex::encode(raw);
        // AES-GCM is authenticated: any bit flip in the ciphertext must be
        // detected and rejected, not silently decrypted into garbage that
        // then fails a downstream length/format check instead.
        assert!(decrypt_signing_key(&encrypted, b"correct horse battery staple").is_err());
    }

    #[test]
    fn encrypted_approval_key_rejects_an_unsupported_file_version() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let mut encrypted = encrypt_signing_key(&signing_key, b"passphrase").unwrap();
        encrypted.version = APPROVAL_KEY_FILE_VERSION + 1;
        assert!(decrypt_signing_key(&encrypted, b"passphrase").is_err());
    }

    #[test]
    fn encrypted_approval_key_file_round_trips_through_json() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let encrypted = encrypt_signing_key(&signing_key, b"passphrase").unwrap();
        let json = serde_json::to_vec(&encrypted).unwrap();
        let parsed: EncryptedApprovalKey = serde_json::from_slice(&json).unwrap();
        let decrypted = decrypt_signing_key(&parsed, b"passphrase").unwrap();
        assert_eq!(decrypted.to_bytes(), signing_key.to_bytes());
    }

    #[test]
    fn genuine_approval_signature_verifies() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let digest = "sha256:abc123";
        let signature_hex = hex::encode(signing_key.sign(digest.as_bytes()).to_bytes());

        verify_approval_signature(digest, &signing_key.verifying_key(), &signature_hex).unwrap();
    }

    #[test]
    fn approval_signature_from_a_different_key_is_rejected() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let other_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let digest = "sha256:abc123";
        // Signed by a different key than the trusted one -- exactly what
        // a host without the real private key would be stuck with if it
        // tried to mint its own "approval".
        let signature_hex = hex::encode(other_key.sign(digest.as_bytes()).to_bytes());

        assert!(
            verify_approval_signature(digest, &signing_key.verifying_key(), &signature_hex)
                .is_err()
        );
    }

    #[test]
    fn approval_signature_over_a_different_digest_is_rejected() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let signed_digest = "sha256:abc123";
        let presented_digest = "sha256:def456";
        let signature_hex = hex::encode(signing_key.sign(signed_digest.as_bytes()).to_bytes());

        assert!(verify_approval_signature(
            presented_digest,
            &signing_key.verifying_key(),
            &signature_hex
        )
        .is_err());
    }

    #[test]
    fn malformed_approval_public_key_is_rejected() {
        assert!(parse_approval_public_key("not-hex").is_err());
        assert!(parse_approval_public_key("aabbcc").is_err());
    }

    #[test]
    fn approval_public_key_file_owned_by_this_process_is_rejected() {
        // The exact scenario this file-based trust anchor exists to
        // defend against: this test process is the only uid available to
        // write the file with, so a file it owns is precisely what a
        // compromised/self-approving executor identity would produce.
        let dir = tempdir().unwrap();
        let path = dir.path().join("approval_public_key");
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        fs::write(&path, hex::encode(signing_key.verifying_key().to_bytes())).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let error = approval_public_key_from_file(&path).unwrap_err();
        assert!(error
            .to_string()
            .contains("owned by this process's own uid"));
    }

    #[test]
    fn approval_public_key_file_that_is_group_writable_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("approval_public_key");
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        fs::write(&path, hex::encode(signing_key.verifying_key().to_bytes())).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o660)).unwrap();

        let error = approval_public_key_from_file(&path).unwrap_err();
        assert!(error.to_string().contains("group- or other-writable"));
    }

    #[test]
    fn approval_public_key_symlink_is_rejected() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("real_key");
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        fs::write(&target, hex::encode(signing_key.verifying_key().to_bytes())).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("approval_public_key");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let error = approval_public_key_from_file(&link).unwrap_err();
        assert!(error.to_string().contains("non-symlink"));
    }

    #[test]
    fn approval_public_key_is_not_a_field_of_the_executed_config() {
        // The whole point of the fix: config/plan files (the routine,
        // automatable deploy path) cannot carry or influence the trust
        // anchor at all -- there is no field for it to occupy.
        let value = serde_json::to_value(&json!({
            "router": {}, "chain": {}, "kms": {}, "executor": {},
            "ledger_path": "/tmp/x", "runtime": {}, "runtime_state_path": "/tmp/y",
            "approval_public_key": "ff".repeat(32),
        }))
        .unwrap();
        let error = serde_json::from_value::<ArcusSpotExecuteOnceConfig>(value).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    fn execute_once_config(
        ledger_path: &str,
        runtime_state_path: &str,
        max_sell_nvda: &str,
    ) -> ArcusSpotExecuteOnceConfig {
        execute_once_config_with_daily_cap(ledger_path, runtime_state_path, max_sell_nvda, 10)
    }

    fn execute_once_config_with_daily_cap(
        ledger_path: &str,
        runtime_state_path: &str,
        max_sell_nvda: &str,
        max_swaps_per_utc_day: u32,
    ) -> ArcusSpotExecuteOnceConfig {
        serde_yaml::from_str(&format!(
            r#"
router:
  router_base_url: "https://router.spot.arcus.xyz"
  meta_base_url: "https://api.arcus.xyz"
  indexer_base_url: "https://indexer.spot.arcus.xyz"
  chain_id: 4663
  request_timeout_ms: 30000
  min_request_interval_ms: 250
  max_attempts: 3
  retry_base_delay_ms: 500
  max_retry_delay_ms: 30000
  user_agent: "test"
  trusted_permit2_spenders:
    arcus:
      - "0x006102b16A04c20306A28b652745D3973D7D24fa"
    rialto:
      - "0xC94135b63772b91D79d0A2DaAb2a8801f32359bD"
  trusted_token_addresses:
    NVDA: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC"
    AMD: "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC"
  trusted_token_decimals:
    NVDA: 18
    AMD: 18
chain:
  rpc_urls:
    - "https://rpc.mainnet.chain.robinhood.com"
  chain_id: 4663
  request_interval_ms: 200
kms:
  region: "eu-central-1"
  key_id: "alias/test"
  chain_id: 4663
  expected_address: "0x812B6A6da8E0dF1fBCA7939ae32089Cf85c5DF05"
executor:
  taker: "0x812B6A6da8E0dF1fBCA7939ae32089Cf85c5DF05"
  permit2: "0x000000000022D473030F116dDEE9F6B43aC78BA3"
  slippage_bps: 50
  minimum_gas_balance_wei: "1000000000000000"
  inventory_floor_raw:
    NVDA: "50000000000000000"
    AMD: "20000000000000000"
  maximum_sell_amount_raw:
    NVDA: "{max_sell_nvda}"
    AMD: "21084353605755395"
  max_swaps_per_utc_day: {max_swaps_per_utc_day}
  max_plan_age_secs: 60
ledger_path: {ledger_path}
runtime_state_path: {runtime_state_path}
runtime:
  mode: live
  chain_id: 4663
  pair:
    sell_symbol: NVDA
    buy_symbol: AMD
  notional_usd: "10"
  initial_inventory:
    token_a: "0.35"
    token_b: "0.16"
  inventory_floors:
    token_a: "0.05"
    token_b: "0.02"
  max_rotation_fraction: "0.25"
  signal_window_samples: 96
  min_signal_samples: 32
  entry_z_score: 2.5
  exit_z_score: 0.25
  max_quote_age_secs: 60
  max_hold_secs: 86400
  max_all_in_round_trip_cost_bps: "120"
  gas_buffer_bps: "10"
  settlement_buffer_bps: "10"
  max_inventory_imbalance_fraction: "0.75"
  daily_loss_limit_usd: "2"
  cumulative_loss_limit_usd: "10"
"#
        ))
        .unwrap()
    }

    fn persist_initial_operator_state(config: &ArcusSpotExecuteOnceConfig) {
        let runtime = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .persist(&runtime)
            .unwrap();
        ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone())
            .persist(&ArcusSpotExecutionLedger::default())
            .unwrap();
        let store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
        drop(
            store
                .acquire_exclusive_lock(&config.runtime_state_path)
                .unwrap(),
        );
    }

    fn prepare_signal_ready_acceptance_baseline(config: &ArcusSpotExecuteOnceConfig) {
        let history: Vec<f64> = (0..32)
            .map(|index| if index % 2 == 0 { -0.01 } else { 0.01 })
            .collect();
        rewrite_checkpoint_state(&config.runtime_state_path, |state| {
            state["sequence"] = json!(32);
            state["relative_log_price_history"] = json!(history);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
        });
    }

    fn accepted_entry_snapshot(at: DateTime<Utc>) -> ArcusSpotRecorderSnapshot {
        serde_json::from_value(json!({
            "schema_version": 3,
            "mode": "public_indicative_read_only",
            "chain_id": 4663,
            "collection_started_at": at,
            "collection_finished_at": at,
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
                    "requested_at": at,
                    "received_at": at,
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
                            "quote": {"price": "176.49938051691913"}
                        }
                    ],
                    "requested_at": at,
                    "received_at": at,
                    "latency_ms": 10,
                    "attempts": 1
                }
            },
            "round_trips": [{
                "pair": {"sell_symbol": "NVDA", "buy_symbol": "AMD"},
                "notional_usd": "10",
                "sell_reference_price_usd": "200",
                "buy_reference_price_usd": "176.49938051691913",
                "requested_sell_amount": "50000000000000000",
                "forward": {
                    "chain_id": 4663,
                    "sell_symbol": "NVDA",
                    "buy_symbol": "AMD",
                    "sell_token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                    "buy_token": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                    "sell_amount": "50000000000000000",
                    "response": {
                        "payload": {
                            "recommended": "arcus",
                            "all": [{"venue": "arcus", "buyAmount": "50000000000000000", "sellAmount": "50000000000000000", "fees": []}],
                            "errors": []
                        },
                        "requested_at": at,
                        "received_at": at,
                        "latency_ms": 10,
                        "attempts": 1
                    }
                },
                "reverse": {
                    "chain_id": 4663,
                    "sell_symbol": "AMD",
                    "buy_symbol": "NVDA",
                    "sell_token": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
                    "buy_token": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
                    "sell_amount": "50000000000000000",
                    "response": {
                        "payload": {
                            "recommended": "arcus",
                            "all": [{"venue": "arcus", "buyAmount": "49617500000000000", "sellAmount": "50000000000000000", "fees": []}],
                            "errors": []
                        },
                        "requested_at": at,
                        "received_at": at,
                        "latency_ms": 10,
                        "attempts": 1
                    }
                },
                "optimistic_return_amount": "49617500000000000",
                "optimistic_round_trip_loss_bps": "76.5",
                "errors": []
            }]
        }))
        .unwrap()
    }

    fn no_swap_snapshot(
        at: DateTime<Utc>,
        token_a_price: &str,
        token_b_price: &str,
    ) -> ArcusSpotRecorderSnapshot {
        let mut value = serde_json::to_value(accepted_entry_snapshot(at)).unwrap();
        let overview = value["reference_overview"]["observation"]["payload"]
            .as_array_mut()
            .unwrap();
        overview[0]["quote"]["price"] = json!(token_a_price);
        overview[1]["quote"]["price"] = json!(token_b_price);
        value["round_trips"] = json!([]);
        serde_json::from_value(value).unwrap()
    }

    fn structurally_invalid_snapshot(at: DateTime<Utc>) -> ArcusSpotRecorderSnapshot {
        let mut value = serde_json::to_value(accepted_entry_snapshot(at)).unwrap();
        value["schema_version"] = json!(999);
        serde_json::from_value(value).unwrap()
    }

    fn persist_observation_evidence(
        config: &ArcusSpotExecuteOnceConfig,
        snapshot: ArcusSpotRecorderSnapshot,
        evaluation_time: DateTime<Utc>,
    ) {
        let runtime = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .load_existing(&config.runtime)
            .unwrap();
        let evidence = ArcusSpotLiveTickObservationEvidence {
            schema_version: OBSERVATION_EVIDENCE_SCHEMA_VERSION,
            evaluation_time,
            snapshot,
            resulting_runtime: Some(ArcusSpotObservationBoundary {
                sequence: runtime.state().sequence,
                last_observation_at: runtime.state().last_observation_at,
            }),
        };
        write_private_regular_file_atomic(
            &live_tick_observation_evidence_path(config).unwrap(),
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
    }

    fn rewrite_checkpoint_state(path: &Path, edit: impl FnOnce(&mut serde_json::Value)) {
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        edit(&mut value["state"]);
        let mut bytes = serde_json::to_vec_pretty(&value).unwrap();
        bytes.push(b'\n');
        write_private_regular_file_atomic(path, &bytes).unwrap();
    }

    fn reconciled_entry_attempt(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
        sequence: u64,
    ) -> ArcusSpotExecutionAttempt {
        let at = DateTime::parse_from_rfc3339("2026-08-16T12:00:02Z")
            .unwrap()
            .with_timezone(&Utc);
        reconciled_entry_attempt_at(config, plan, sequence, at)
    }

    fn reconciled_entry_attempt_at(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
        sequence: u64,
        at: DateTime<Utc>,
    ) -> ArcusSpotExecutionAttempt {
        let payload_hash = format!("sha256:{}", "a".repeat(64));
        ArcusSpotExecutionAttempt {
            sequence,
            idempotency_key: format!(
                "arcus-spot-{sequence:020}-{}",
                &payload_hash["sha256:".len()..][..16]
            ),
            payload_hash,
            chain_id: config.runtime.chain_id,
            taker: config.executor.taker.clone(),
            prepared_at: at,
            dispatched_at: Some(at),
            updated_at: at,
            phase: ArcusSpotExecutionPhase::Reconciled,
            intent: ArcusSpotExecutionIntent {
                venue: plan.venue.clone(),
                sell_symbol: plan.sell_symbol.clone(),
                buy_symbol: plan.buy_symbol.clone(),
                sell_token: plan.sell_token_address.clone(),
                buy_token: plan.buy_token_address.clone(),
                sell_amount_raw: plan.sell_amount_raw.clone(),
                minimum_buy_amount_raw: "49750000000000000".to_string(),
                plan_config_digest: approval_digest(config, plan).unwrap(),
            },
            pre_balances: ArcusSpotBalanceSnapshot {
                observed_at: at,
                sell_token: plan.sell_token_address.clone(),
                buy_token: plan.buy_token_address.clone(),
                sell_balance_raw: "1000000000000000000".to_string(),
                buy_balance_raw: "100000000000000000".to_string(),
                gas_balance_wei: "1000000000000000".to_string(),
            },
            post_balances: Some(ArcusSpotBalanceSnapshot {
                observed_at: at,
                sell_token: plan.sell_token_address.clone(),
                buy_token: plan.buy_token_address.clone(),
                sell_balance_raw: "950000000000000000".to_string(),
                buy_balance_raw: "150000000000000000".to_string(),
                gas_balance_wei: "1000000000000000".to_string(),
            }),
            tx_hash: Some(format!("0x{sequence:064x}")),
            router_status: Some("confirmed".to_string()),
            detail: None,
        }
    }

    fn persist_reconciled_entry_transition(
        config: &ArcusSpotExecuteOnceConfig,
        ledger_path: &Path,
        runtime_path: &Path,
    ) -> ArcusSpotExecutionAttempt {
        let accepted_at = fixture_now();
        let snapshot = accepted_entry_snapshot(accepted_at);
        let baseline = ArcusSpotRuntimeCheckpointStore::new(runtime_path.to_path_buf())
            .load_existing(&config.runtime)
            .unwrap();

        // Build the authentic plan under the ordinary approved limits. A
        // few negative tests deliberately tighten one limit in `config`;
        // they still need the same candidate artifact so the verifier can
        // demonstrate that replaying under the supplied config rejects it.
        let mut planning_config = config.runtime.clone();
        planning_config.max_rotation_fraction = Decimal::new(25, 2);
        planning_config.max_all_in_round_trip_cost_bps = Decimal::from(120);
        planning_config.max_inventory_imbalance_fraction = Decimal::new(75, 2);
        let mut planner =
            ArcusSpotRuntime::from_state(planning_config, baseline.state().clone()).unwrap();
        let event = planner.step_at(&snapshot, accepted_at);
        let plan = match event.decision {
            ArcusSpotDecision::WouldRotate { plan } => plan,
            other => panic!("acceptance fixture did not produce a rotation plan: {other:?}"),
        };
        let evidence = ArcusSpotLiveTickEvidence {
            schema_version: LIVE_TICK_EVIDENCE_SCHEMA_VERSION,
            evaluation_time: accepted_at,
            snapshot: snapshot.clone(),
            plan: plan.clone(),
        };
        let plan_bytes = serde_json::to_vec_pretty(&evidence).unwrap();
        write_private_regular_file_atomic(
            &live_tick_pending_plan_path(config).unwrap(),
            &plan_bytes,
        )
        .unwrap();
        let ledger_store = ArcusSpotExecutionLedgerStore::new(ledger_path);
        let mut ledger = ledger_store.load_existing().unwrap();
        let attempt = reconciled_entry_attempt_at(config, &plan, ledger.next_sequence, accepted_at);
        ledger.next_sequence += 1;
        ledger.history.push(attempt.clone());
        ArcusSpotExecutionLedgerStore::new(ledger_path)
            .persist(&ledger)
            .unwrap();

        let mut runtime = baseline;
        runtime.step_at(&snapshot, accepted_at);
        runtime
            .apply_confirmed_live_fill_once(
                &plan,
                plan.sell_quantity,
                plan.buy_quantity,
                accepted_at,
                &attempt.idempotency_key,
            )
            .unwrap();
        ArcusSpotRuntimeCheckpointStore::new(runtime_path.to_path_buf())
            .persist(&runtime)
            .unwrap();
        persist_observation_evidence(config, snapshot, accepted_at);
        attempt
    }

    #[test]
    fn state_backup_and_exact_verification_are_private_and_atomic() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("pre-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);

        let manifest = create_arcus_state_backup(&config, &backup_dir).unwrap();
        let report = verify_arcus_state_backup(&config, &backup_dir, true).unwrap();

        assert_eq!(manifest.schema_version, STATE_BACKUP_SCHEMA_VERSION);
        assert_eq!(report.status, "verified");
        assert_eq!(report.mode, "exact");
        assert_eq!(
            fs::metadata(&backup_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for name in [
            STATE_BACKUP_MANIFEST,
            STATE_BACKUP_CHECKPOINT,
            STATE_BACKUP_LEDGER,
        ] {
            assert_eq!(
                fs::metadata(backup_dir.join(name))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        let error = create_arcus_state_backup(&config, &backup_dir).unwrap_err();
        assert!(error.to_string().contains("already exists"));
    }

    #[test]
    fn state_backup_refuses_to_race_an_executor_lock() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("blocked-backup");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let ledger_before = fs::read(&ledger_path).unwrap();
        let runtime_before = fs::read(&runtime_path).unwrap();
        let store = ArcusSpotExecutionLedgerStore::new(ledger_path.clone());
        let _held = store.acquire_exclusive_lock(&runtime_path).unwrap();

        let error = create_arcus_state_backup(&config, &backup_dir).unwrap_err();

        assert!(error
            .to_string()
            .contains("another Arcus executor already holds"));
        assert!(!backup_dir.exists());
        assert_eq!(fs::read(ledger_path).unwrap(), ledger_before);
        assert_eq!(fs::read(runtime_path).unwrap(), runtime_before);
    }

    #[test]
    fn exact_verification_detects_a_valid_advance_that_continuity_accepts() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(1);
            state["relative_log_price_history"] = json!([0.125]);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            state["initial_equity_usd"] = json!("98.2399008827070608");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("98.2399008827070608");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("98.2399008827070608");
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "200", "176.49938051691913"),
            accepted_at,
        );

        let exact_error = verify_arcus_state_backup(&config, &backup_dir, true).unwrap_err();
        let continuity = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();

        assert!(exact_error
            .to_string()
            .contains("does not match backup manifest"));
        assert_eq!(continuity.mode, "continuity");
        assert_eq!(continuity.runtime.sequence, 1);
    }

    #[test]
    fn continuity_verification_rejects_rewritten_signal_history() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.125, 0.25]);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!([9.0, 10.0]);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("retained signal history changed"));
    }

    #[test]
    fn continuity_verification_accepts_one_sample_full_window_shift() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let baseline_history: Vec<f64> = (0..96).map(|sample| f64::from(sample) / 100.0).collect();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!(baseline_history.clone());
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        let mut shifted_history = baseline_history[1..].to_vec();
        shifted_history.push(1.25);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!(shifted_history);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("57.300959372038022");
            state["initial_equity_usd"] = json!("79.16815349952608352");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("79.16815349952608352");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("79.16815349952608352");
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "200", "57.300959372038022"),
            accepted_at,
        );

        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();

        assert_eq!(report.mode, "continuity");
        assert_eq!(report.runtime.sequence, 8);
    }

    #[test]
    fn continuity_verification_accepts_an_identical_full_window_rotation_without_a_swap() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let unchanged_full_window = vec![0.125; config.runtime.signal_window_samples];
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!(unchanged_full_window.clone());
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!(unchanged_full_window);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            state["initial_equity_usd"] = json!("98.2399008827070608");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("98.2399008827070608");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("98.2399008827070608");
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "200", "176.49938051691913"),
            accepted_at,
        );

        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();

        assert_eq!(report.mode, "continuity");
        assert_eq!(report.runtime.sequence, 8);
    }

    #[test]
    fn continuity_verification_accepts_an_evidenced_sequence_only_invalid_tick() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();

        let evaluation_time = fixture_now();
        let snapshot = structurally_invalid_snapshot(evaluation_time);
        let store = ArcusSpotRuntimeCheckpointStore::new(runtime_path);
        let mut runtime = store.load_existing(&config.runtime).unwrap();
        let event = runtime.step_at(&snapshot, evaluation_time);
        assert!(matches!(
            event.decision,
            ArcusSpotDecision::Observe {
                hold: ArcusSpotHold {
                    code: debot::arcus_spot::ArcusSpotHoldCode::InvalidSnapshot,
                    ..
                }
            }
        ));
        assert_eq!(runtime.state().sequence, 1);
        assert_eq!(runtime.state().last_observation_at, None);
        store.persist(&runtime).unwrap();
        persist_observation_evidence(&config, snapshot, evaluation_time);

        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();
        assert_eq!(report.mode, "continuity");
        assert_eq!(report.runtime.sequence, 1);
        assert_eq!(report.runtime.last_observation_at, None);
    }

    #[test]
    fn state_backup_ignores_a_one_sequence_newer_orphaned_evidence_sidecar() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("after-crash");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let evaluation_time = fixture_now();
        let evidence = ArcusSpotLiveTickObservationEvidence {
            schema_version: OBSERVATION_EVIDENCE_SCHEMA_VERSION,
            evaluation_time,
            snapshot: structurally_invalid_snapshot(evaluation_time),
            resulting_runtime: Some(ArcusSpotObservationBoundary {
                sequence: 1,
                last_observation_at: None,
            }),
        };
        write_private_regular_file_atomic(
            &live_tick_observation_evidence_path(&config).unwrap(),
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();

        let manifest = create_arcus_state_backup(&config, &backup_dir).unwrap();
        assert!(manifest.observation_evidence.is_none());
        assert!(!backup_dir.join(STATE_BACKUP_OBSERVATION_EVIDENCE).exists());

        let report = verify_arcus_state_backup(&config, &backup_dir, true).unwrap();
        assert_eq!(report.mode, "exact");
        assert!(report.observation_evidence_sha256.is_none());
    }

    #[test]
    fn state_backup_rejects_a_nonadjacent_observation_evidence_boundary() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("mismatched-evidence");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let evaluation_time = fixture_now();
        let evidence = ArcusSpotLiveTickObservationEvidence {
            schema_version: OBSERVATION_EVIDENCE_SCHEMA_VERSION,
            evaluation_time,
            snapshot: structurally_invalid_snapshot(evaluation_time),
            resulting_runtime: Some(ArcusSpotObservationBoundary {
                sequence: 2,
                last_observation_at: None,
            }),
        };
        write_private_regular_file_atomic(
            &live_tick_observation_evidence_path(&config).unwrap(),
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();

        let error = create_arcus_state_backup(&config, &backup_dir).unwrap_err();
        assert!(error
            .to_string()
            .contains("observation evidence does not match the runtime boundary"));
        assert!(!backup_dir.exists());
    }

    #[test]
    fn state_backup_accepts_a_coherent_legacy_schema_one_observation_sidecar() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("legacy-evidence");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let observed_at = fixture_now();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(1);
            state["last_observation_at"] = json!(observed_at);
        });
        let evidence = ArcusSpotLiveTickObservationEvidence {
            schema_version: LIVE_TICK_EVIDENCE_SCHEMA_VERSION,
            evaluation_time: observed_at,
            snapshot: no_swap_snapshot(observed_at, "200", "176.49938051691913"),
            resulting_runtime: None,
        };
        write_private_regular_file_atomic(
            &live_tick_observation_evidence_path(&config).unwrap(),
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();

        let manifest = create_arcus_state_backup(&config, &backup_dir).unwrap();
        assert!(manifest.observation_evidence.is_some());
        verify_arcus_state_backup(&config, &backup_dir, true).unwrap();
    }

    #[test]
    fn continuity_verification_rejects_legacy_evidence_for_the_current_tick() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();

        let evaluation_time = fixture_now();
        let snapshot = no_swap_snapshot(evaluation_time, "200", "176.49938051691913");
        let store = ArcusSpotRuntimeCheckpointStore::new(runtime_path);
        let mut runtime = store.load_existing(&config.runtime).unwrap();
        let event = runtime.step_at(&snapshot, evaluation_time);
        assert!(matches!(event.decision, ArcusSpotDecision::Observe { .. }));
        store.persist(&runtime).unwrap();
        let evidence = ArcusSpotLiveTickObservationEvidence {
            schema_version: LIVE_TICK_EVIDENCE_SCHEMA_VERSION,
            evaluation_time,
            snapshot,
            resulting_runtime: None,
        };
        write_private_regular_file_atomic(
            &live_tick_observation_evidence_path(&config).unwrap(),
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();
        assert!(error
            .to_string()
            .contains("current sequence-advancing observation evidence must use schema 2"));
    }

    #[test]
    fn continuity_verification_rejects_a_same_day_loss_baseline_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
            state["initial_equity_usd"] = json!("300");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("300");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("299");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["daily_baseline_equity_usd"] = json!("250");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("250");
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("daily equity baseline changed without a UTC rollover"));
    }

    #[test]
    fn continuity_verification_rejects_a_last_equity_mark_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
            state["initial_equity_usd"] = json!("300");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("300");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("299");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_equity_usd"] = serde_json::Value::Null;
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("lost its last equity mark"));
    }

    #[test]
    fn continuity_verification_accepts_a_matching_utc_day_rollover() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.0]);
            state["last_observation_at"] = json!("2026-08-15T23:59:00Z");
            state["initial_equity_usd"] = json!("98.2399008827070608");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-15");
            state["daily_baseline_equity_usd"] = json!("98.2399008827070608");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("98.2399008827070608");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!([0.0, 0.125]);
            state["last_observation_at"] = json!("2026-08-16T00:00:01Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("98.2399008827070608");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("98.2399008827070608");
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "200", "176.49938051691913"),
            accepted_at,
        );

        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();

        assert_eq!(report.mode, "continuity");
    }

    /// The review finding on pairtrade#211: a halt engaged on a rollover
    /// tick used to be rejected here as "unexpected", because the basket its
    /// loss was measured against had already been rebased in that same tick
    /// and the re-derivation below then read back ~0. No test combined a
    /// genuine rollover with a genuine halt, so nothing caught it.
    #[test]
    fn continuity_verification_accepts_a_loss_halt_engaged_on_a_rollover() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.0]);
            state["last_observation_at"] = json!("2026-08-15T23:59:00Z");
            state["initial_equity_usd"] = json!("300");
            state["daily_baseline_day"] = json!("2026-08-15");
            state["daily_baseline_equity_usd"] = json!("300");
            state["last_equity_usd"] = json!("300");
            // A rotation on 2026-08-15 gave up a hundredth of token A for
            // nothing, so the day's basket sits above the inventory held.
            state["initial_baseline_inventory"] = json!({"token_a": "0.36", "token_b": "0.16"});
            state["daily_baseline_inventory"] = json!({"token_a": "0.36", "token_b": "0.16"});
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!([0.0, 0.125]);
            state["last_observation_at"] = json!("2026-08-16T00:00:01Z");
            state["last_token_a_reference_price_usd"] = json!("600");
            state["last_token_b_reference_price_usd"] = json!("529.49814155075739");
            // The day and its equity mark roll, as they always did...
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("294.7197026481211824");
            state["last_equity_usd"] = json!("294.7197026481211824");
            // ...but the basket does not, because a halt now stands on it.
            state["risk_halt"] = json!({
                "kind": "daily_loss",
                "engaged_at": "2026-08-16T00:00:01Z",
                "equity_usd": "294.7197026481211824",
                "loss_usd": "6.0000000000000000",
                "limit_usd": "2",
            });
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T00:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "600", "529.49814155075739"),
            accepted_at,
        );

        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();

        assert_eq!(report.mode, "continuity");
    }

    #[test]
    fn continuity_verification_rejects_a_future_day_baseline_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.0]);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
            state["initial_equity_usd"] = json!("98.2399008827070608");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("98.2399008827070608");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("98.2399008827070608");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!([0.0, 0.125]);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            state["daily_baseline_day"] = json!("2026-08-17");
            state["daily_baseline_equity_usd"] = json!("98.2399008827070608");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("98.2399008827070608");
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("rollover is outside the approved tick window"));
    }

    #[test]
    fn continuity_verification_rejects_a_mismatched_first_cumulative_baseline() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(1);
            state["last_observation_at"] = json!("2026-08-16T12:00:01Z");
            state["initial_equity_usd"] = json!("250");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("300");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("300");
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("mismatched cumulative, daily, and last equity marks"));
    }

    #[test]
    fn continuity_verification_requires_a_newly_triggered_loss_halt() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.0]);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
            state["initial_equity_usd"] = json!("300");
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("300");
            state["last_equity_usd"] = json!("300");
            // Baskets one hundredth of token A larger than the inventory
            // actually held: an earlier rotation gave that up and got
            // nothing back. Priced at this state's marks (600 /
            // 529.49814155075739) the baskets are worth 300.7197… against
            // an actual 294.7197…, a $6 attributed loss past the $2 daily
            // limit. Stated as a basket difference rather than the price
            // move this fixture used before #813, because a price move is
            // exactly what must no longer require a halt.
            state["initial_baseline_inventory"] = json!({"token_a": "0.36", "token_b": "0.16"});
            state["daily_baseline_inventory"] = json!({"token_a": "0.36", "token_b": "0.16"});
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!([0.0, 0.125]);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_token_a_reference_price_usd"] = json!("600");
            state["last_token_b_reference_price_usd"] = json!("529.49814155075739");
            state["last_equity_usd"] = json!("294.7197026481211824");
        });
        let observed_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let evaluated_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:01Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(observed_at, "600", "529.49814155075739"),
            evaluated_at,
        );

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("omitted a newly triggered loss halt"));

        rewrite_checkpoint_state(&runtime_path, |state| {
            state["risk_halt"] = json!({
                "kind": "daily_loss",
                "engaged_at": "2026-08-16T12:01:01Z",
                "equity_usd": "294.7197026481211824",
                "loss_usd": "6.0000000000000000",
                "limit_usd": "2",
            });
        });
        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();
        assert_eq!(report.mode, "continuity");
    }

    #[test]
    fn continuity_verification_rejects_a_false_equity_mark_without_a_swap() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.0]);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
            state["initial_equity_usd"] = json!("90");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("90");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("90");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(8);
            state["relative_log_price_history"] = json!([0.0, 0.125]);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            // Inflated rather than deflated (it was "95" before #813): the
            // basket is genuinely worth 98.2399… at these marks, so a mark
            // *below* it now reads as a real attributed loss and trips the
            // halt expectation before the replay ever runs. Overstating
            // equity is the adversarial direction anyway -- it is how a
            // loss would be hidden -- and the replay must still catch it.
            state["last_equity_usd"] = json!("105");
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "200", "176.49938051691913"),
            accepted_at,
        );

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("does not match its recorder evidence"));
    }

    #[test]
    fn continuity_verification_rejects_self_consistent_forged_no_swap_state() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(1);
            state["relative_log_price_history"] = json!([0.2]);
            state["last_observation_at"] = json!("2026-08-16T12:01:00Z");
            state["last_token_a_reference_price_usd"] = json!("200");
            state["last_token_b_reference_price_usd"] = json!("163.7461506155964");
            state["initial_equity_usd"] = json!("96.199384098495424");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("96.199384098495424");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("96.199384098495424");
        });
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        persist_observation_evidence(
            &config,
            no_swap_snapshot(accepted_at, "200", "176.49938051691913"),
            accepted_at,
        );

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("does not match its recorder evidence"));
    }

    #[test]
    fn continuity_verification_rejects_multiple_acceptance_attempts() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        let plan = rotation_plan("entry_signal");
        let ledger = ArcusSpotExecutionLedger {
            schema_version: 2,
            next_sequence: 3,
            active: None,
            history: vec![
                reconciled_entry_attempt(&config, &plan, 1),
                reconciled_entry_attempt(&config, &plan, 2),
            ],
        };
        ArcusSpotExecutionLedgerStore::new(&ledger_path)
            .persist(&ledger)
            .unwrap();

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("more than one acceptance attempt"));
    }

    #[test]
    fn continuity_verification_accepts_one_reconciled_entry_transition() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let report = verify_arcus_state_backup(&config, &backup_dir, false).unwrap();

        assert_eq!(report.mode, "continuity");
        assert_eq!(report.ledger.next_sequence, 2);
        assert_eq!(report.runtime.regime, ArcusSpotRegime::RotatedAToB);
    }

    #[test]
    fn acceptance_signal_sample_comes_from_the_independent_replay() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let mut replayed_state = ArcusSpotRuntime::new(config.runtime)
            .unwrap()
            .state()
            .clone();
        replayed_state.relative_log_price_history = vec![0.125; 96];

        assert_eq!(acceptance_signal_sample(&replayed_state).unwrap(), 0.125);
    }

    #[test]
    fn continuity_verification_rejects_position_unrelated_to_the_reconciled_attempt() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["inventory"] = json!({"token_a": "0.26", "token_b": "0.21"});
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("position state does not match the reconciled acceptance attempt"));
    }

    #[test]
    fn continuity_verification_rejects_an_attempt_below_the_plan_buy_floor() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);
        let store = ArcusSpotExecutionLedgerStore::new(&ledger_path);
        let mut ledger = store.load_existing().unwrap();
        ledger.history[0].intent.minimum_buy_amount_raw = "1".to_string();
        store.persist(&ledger).unwrap();

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("undercuts the pending plan's approved buy floor"));
    }

    #[test]
    fn continuity_verification_rejects_an_attempt_above_the_sell_ceiling() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "49999999999999999",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("exceeds configured maximum"));
    }

    #[test]
    fn continuity_verification_rejects_an_attempt_above_the_daily_swap_cap() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config_with_daily_cap(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
            1,
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        let mut plan = rotation_plan("entry_signal");
        plan.quote_received_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let prior_attempt = reconciled_entry_attempt(&config, &plan, 1);
        ArcusSpotExecutionLedgerStore::new(&ledger_path)
            .persist(&ArcusSpotExecutionLedger {
                schema_version: 2,
                next_sequence: 2,
                active: None,
                history: vec![prior_attempt.clone()],
            })
            .unwrap();
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("UTC daily swap cap"));
    }

    #[test]
    fn continuity_daily_cap_uses_the_preparation_day_across_midnight() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config_with_daily_cap(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
            1,
        );
        let plan = rotation_plan("entry_signal");
        let prior_attempt = reconciled_entry_attempt(&config, &plan, 1);
        let baseline = ArcusSpotExecutionLedger {
            schema_version: 2,
            next_sequence: 2,
            active: None,
            history: vec![prior_attempt],
        };
        let mut acceptance_attempt = reconciled_entry_attempt(&config, &plan, 2);
        acceptance_attempt.prepared_at = DateTime::parse_from_rfc3339("2026-08-17T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        require_acceptance_daily_swap_capacity(&config, &baseline, &acceptance_attempt).unwrap();
    }

    #[test]
    fn continuity_verification_rejects_an_entry_without_a_signal_crossing() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);
        rewrite_checkpoint_state(&runtime_path, |state| {
            *state["relative_log_price_history"]
                .as_array_mut()
                .unwrap()
                .last_mut()
                .unwrap() = json!(0.0);
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("position state does not match the reconciled acceptance attempt"));
    }

    #[test]
    fn continuity_verification_accepts_a_valid_reverse_direction_entry() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let runtime = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        let mut plan = rotation_plan("entry_signal");
        plan.direction = ArcusSpotDirection::TokenBToTokenA;
        plan.sell_symbol = "AMD".to_string();
        plan.buy_symbol = "NVDA".to_string();
        plan.sell_token_address = "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC".to_string();
        plan.buy_token_address = "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string();
        plan.sell_quantity = Decimal::new(2, 2);
        plan.buy_quantity = Decimal::new(4, 2);
        plan.sell_amount_raw = "20000000000000000".to_string();
        plan.buy_amount_raw = "40000000000000000".to_string();
        plan.predicted_inventory = ArcusSpotInventory {
            token_a: Decimal::new(39, 2),
            token_b: Decimal::new(14, 2),
        };
        plan.predicted_inventory_imbalance_fraction =
            Decimal::from_str("0.05405405405405406").unwrap();

        require_acceptance_entry_within_strategy_limits(
            &config,
            runtime.state(),
            &plan,
            0.4_f64.ln(),
            Decimal::from(200),
            Decimal::from(500),
        )
        .unwrap();
    }

    #[test]
    fn continuity_verification_rejects_a_negative_route_cost() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let runtime = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        let mut plan = rotation_plan("entry_signal");
        plan.optimistic_round_trip_loss_bps = Decimal::NEGATIVE_ONE;
        plan.all_in_round_trip_cost_bps = Decimal::from(19);

        let error = require_acceptance_entry_within_strategy_limits(
            &config,
            runtime.state(),
            &plan,
            0.125,
            Decimal::from(200),
            Decimal::from(100),
        )
        .unwrap_err();

        assert!(error.to_string().contains("must not be negative"));
    }

    #[test]
    fn continuity_verification_rejects_an_understated_nonnegative_route_cost() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let pending_path = live_tick_pending_plan_path(&config).unwrap();
        let bytes = read_private_regular_file(&pending_path, "test pending plan").unwrap();
        let mut evidence = live_tick_evidence_from_document(&bytes, "test pending plan").unwrap();
        evidence.plan.optimistic_round_trip_loss_bps = Decimal::ONE;
        evidence.plan.all_in_round_trip_cost_bps = Decimal::from(21);
        write_private_regular_file_atomic(
            &pending_path,
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("does not match its independently replayed recorder evidence"));
    }

    #[test]
    fn continuity_verification_accepts_rialto_and_rejects_an_unknown_venue() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let mut rialto_plan = rotation_plan("entry_signal");
        rialto_plan.venue = "rialto".to_string();
        rialto_plan.quote_received_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        let rialto_attempt = reconciled_entry_attempt(&config, &rialto_plan, 1);
        reconciled_fill_for_continuity(
            &config,
            &rialto_plan,
            &rialto_attempt,
            rialto_attempt.prepared_at,
        )
        .unwrap();

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "other".to_string();
        let attempt = reconciled_entry_attempt(&config, &plan, 1);

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("Arcus or Rialto"));
    }

    #[test]
    fn continuity_verification_rejects_an_unconfirmed_router_status() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        for router_status in [None, Some("submitted".to_string())] {
            let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
            attempt.router_status = router_status;

            let error =
                reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
                    .unwrap_err();
            assert!(error.to_string().contains("no confirmed router status"));
        }
    }

    #[test]
    fn continuity_verification_rejects_a_zero_transaction_hash() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.tx_hash = Some(format!("0x{}", "0".repeat(64)));

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("must not be zero"));
    }

    #[test]
    fn continuity_verification_rejects_an_entry_above_the_configured_notional() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let runtime = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        let plan = rotation_plan("entry_signal");

        let error = require_acceptance_entry_within_strategy_limits(
            &config,
            runtime.state(),
            &plan,
            0.125,
            Decimal::from(250),
            Decimal::from(100),
        )
        .unwrap_err();

        assert!(error.to_string().contains("configured USD notional"));
    }

    #[test]
    fn continuity_verification_rejects_a_post_sell_balance_below_the_raw_floor() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.pre_balances.sell_balance_raw = "90000000000000000".to_string();
        attempt.post_balances.as_mut().unwrap().sell_balance_raw = "40000000000000000".to_string();

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("post-swap sell balance"));
    }

    #[test]
    fn continuity_verification_rejects_a_pre_buy_balance_below_the_raw_floor() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.pre_balances.buy_balance_raw = "10000000000000000".to_string();
        attempt.post_balances.as_mut().unwrap().buy_balance_raw = "60000000000000000".to_string();

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("pre-swap buy balance"));
    }

    #[test]
    fn continuity_verification_rejects_a_pre_swap_gas_balance_below_the_minimum() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.pre_balances.gas_balance_wei = "999999999999999".to_string();

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("pre-swap gas balance"));
    }

    #[test]
    fn continuity_verification_reapplies_the_runtime_quote_freshness_limit() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let mut config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        config.runtime.max_quote_age_secs = 1;
        let mut plan = rotation_plan("entry_signal");
        plan.quote_received_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let attempt = reconciled_entry_attempt(&config, &plan, 1);

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("strategy planning"));
    }

    #[test]
    fn continuity_verification_measures_quote_freshness_at_evaluation_time() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let mut config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        config.runtime.max_quote_age_secs = 1;
        let mut plan = rotation_plan("entry_signal");
        plan.quote_received_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let attempt = reconciled_entry_attempt(&config, &plan, 1);
        let evaluation_time = DateTime::parse_from_rfc3339("2026-08-16T12:00:00.500Z")
            .unwrap()
            .with_timezone(&Utc);

        reconciled_fill_for_continuity(&config, &plan, &attempt, evaluation_time).unwrap();
    }

    #[test]
    fn continuity_verification_rejects_a_quote_after_the_accepted_observation() {
        let mut plan = rotation_plan("entry_signal");
        plan.quote_received_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:02Z")
            .unwrap()
            .with_timezone(&Utc);
        let accepted_observation_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:01Z")
            .unwrap()
            .with_timezone(&Utc);

        let error = require_acceptance_quote_belongs_to_observation(&plan, accepted_observation_at)
            .unwrap_err();

        assert!(error.to_string().contains("after its accepted observation"));
    }

    #[test]
    fn continuity_verification_rejects_an_entry_above_the_rotation_fraction_cap() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let mut config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        config.runtime.max_rotation_fraction = Decimal::new(1, 1);
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("per-action cap"), "{error:#}");
    }

    #[test]
    fn continuity_verification_rejects_an_entry_above_the_all_in_cost_limit() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let mut config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        config.runtime.max_all_in_round_trip_cost_bps = Decimal::from(90);
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(
            error.to_string().contains("all-in round-trip cost"),
            "{error:#}"
        );
    }

    #[test]
    fn continuity_verification_rejects_an_entry_above_the_inventory_imbalance_cap() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let mut config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        config.runtime.max_inventory_imbalance_fraction = Decimal::new(2, 1);
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(
            error.to_string().contains("inventory imbalance"),
            "{error:#}"
        );
    }

    #[test]
    fn continuity_verification_rejects_a_backdated_acceptance_attempt() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        prepare_signal_ready_acceptance_baseline(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        persist_reconciled_entry_transition(&config, &ledger_path, &runtime_path);
        let store = ArcusSpotExecutionLedgerStore::new(&ledger_path);
        let mut ledger = store.load_existing().unwrap();
        ledger.history[0].prepared_at = DateTime::parse_from_rfc3339("2026-08-16T11:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        store.persist(&ledger).unwrap();

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("chronology"));
    }

    #[test]
    fn continuity_verification_rejects_a_future_observation_watermark() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-start");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(1);
            state["last_observation_at"] = json!("2099-01-01T00:00:00Z");
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("approved tick window"));
    }

    #[test]
    fn continuity_verification_rejects_a_cumulative_equity_baseline_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["initial_equity_usd"] = json!("300");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("300");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("300");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["initial_equity_usd"] = serde_json::Value::Null;
            state["initial_baseline_inventory"] = state["inventory"].clone();
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error
            .to_string()
            .contains("cumulative equity baseline changed"));
    }

    #[test]
    fn continuity_verification_rejects_a_sticky_risk_halt_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["initial_equity_usd"] = json!("300");
            state["initial_baseline_inventory"] = state["inventory"].clone();
            state["daily_baseline_day"] = json!("2026-08-16");
            state["daily_baseline_equity_usd"] = json!("300");
            state["daily_baseline_inventory"] = state["inventory"].clone();
            state["last_equity_usd"] = json!("297");
            state["risk_halt"] = json!({
                "kind": "daily_loss",
                "engaged_at": "2026-08-16T12:00:00Z",
                "equity_usd": "297",
                "loss_usd": "3",
                "limit_usd": "2",
            });
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["risk_halt"] = serde_json::Value::Null;
        });

        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("sticky risk halt"));
    }

    #[test]
    fn continuity_verification_rejects_a_checkpoint_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        rewrite_checkpoint_state(&runtime_path, |state| {
            state["sequence"] = json!(7);
            state["relative_log_price_history"] = json!([0.125, 0.25]);
            state["last_observation_at"] = json!("2026-08-16T12:00:00Z");
        });
        create_arcus_state_backup(&config, &backup_dir).unwrap();

        let fresh = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        ArcusSpotRuntimeCheckpointStore::new(runtime_path)
            .persist(&fresh)
            .unwrap();
        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("runtime sequence regressed"));
    }

    #[test]
    fn continuity_verification_rejects_a_ledger_sequence_reset() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("before-rollback");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let ledger_store = ArcusSpotExecutionLedgerStore::new(ledger_path);
        let mut advanced = ArcusSpotExecutionLedger::default();
        advanced.next_sequence = 7;
        ledger_store.persist(&advanced).unwrap();
        create_arcus_state_backup(&config, &backup_dir).unwrap();

        ledger_store
            .persist(&ArcusSpotExecutionLedger::default())
            .unwrap();
        let error = verify_arcus_state_backup(&config, &backup_dir, false).unwrap_err();

        assert!(error.to_string().contains("ledger next_sequence regressed"));
    }

    #[test]
    fn state_verification_rejects_a_tampered_backup_before_comparison() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let backup_dir = dir.path().join("tampered-backup");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        create_arcus_state_backup(&config, &backup_dir).unwrap();
        OpenOptions::new()
            .append(true)
            .open(backup_dir.join(STATE_BACKUP_LEDGER))
            .unwrap()
            .write_all(b" \n")
            .unwrap();

        let error = verify_arcus_state_backup(&config, &backup_dir, true).unwrap_err();

        assert!(error
            .to_string()
            .contains("backup execution ledger does not match backup manifest"));
    }

    #[test]
    fn state_backup_includes_and_exactly_verifies_both_evidence_sidecars() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let pending_path = dir.path().join(STATE_BACKUP_PENDING_PLAN);
        let backup_dir = dir.path().join("with-pending-plan");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);
        let accepted_at = DateTime::parse_from_rfc3339("2026-08-16T12:01:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let snapshot = no_swap_snapshot(accepted_at, "200", "176.49938051691913");
        let runtime_store = ArcusSpotRuntimeCheckpointStore::new(runtime_path.clone());
        let mut runtime = runtime_store.load_existing(&config.runtime).unwrap();
        runtime.step_at(&snapshot, accepted_at);
        runtime_store.persist(&runtime).unwrap();
        persist_observation_evidence(&config, snapshot, accepted_at);
        let observation_bytes =
            fs::read(live_tick_observation_evidence_path(&config).unwrap()).unwrap();
        let plan_bytes = serde_json::to_vec_pretty(&rotation_plan("entry_signal")).unwrap();
        write_private_regular_file_atomic(&pending_path, &plan_bytes).unwrap();

        let manifest = create_arcus_state_backup(&config, &backup_dir).unwrap();
        verify_arcus_state_backup(&config, &backup_dir, true).unwrap();

        assert_eq!(
            manifest.pending_plan.unwrap().sha256,
            sha256_prefixed(&plan_bytes)
        );
        assert_eq!(
            manifest.observation_evidence.unwrap().sha256,
            sha256_prefixed(&observation_bytes)
        );
        assert_eq!(
            fs::read(backup_dir.join(STATE_BACKUP_PENDING_PLAN)).unwrap(),
            plan_bytes
        );
        assert_eq!(
            fs::read(backup_dir.join(STATE_BACKUP_OBSERVATION_EVIDENCE)).unwrap(),
            observation_bytes
        );
    }

    #[test]
    fn config_rejects_a_runtime_state_path_colliding_with_the_live_tick_pending_plan_path() {
        // live-tick derives its pending-plan path from runtime_state_path's
        // directory; if runtime_state_path itself resolved there, live-tick
        // would atomically overwrite the checkpoint with plan JSON right
        // before the subsequent fresh checkpoint load, destroying it
        // (Codex P2 follow-up, pairtrade#186).
        let mut config = execute_once_config(
            "/var/lib/x/ledger.json",
            "/var/lib/x/live-tick-pending-plan.json",
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error.to_string().contains("live-tick pending-plan path"));
    }

    #[test]
    fn config_rejects_a_ledger_path_colliding_with_the_live_tick_pending_plan_path() {
        let mut config = execute_once_config(
            "/var/lib/x/live-tick-pending-plan.json",
            "/var/lib/x/runtime.json",
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error.to_string().contains("live-tick pending-plan path"));
    }

    #[test]
    fn config_rejects_a_ledger_path_colliding_with_observation_evidence() {
        let mut config = execute_once_config(
            "/var/lib/x/live-tick-observation-evidence.json",
            "/var/lib/x/runtime.json",
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error
            .to_string()
            .contains("live-tick observation-evidence path"));
    }

    #[test]
    fn config_rejects_a_ledger_path_colliding_with_the_pending_event() {
        let mut config = execute_once_config(
            "/var/lib/x/live-tick-event-pending.json",
            "/var/lib/x/runtime.json",
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error.to_string().contains("live-tick pending-event path"));
    }

    #[test]
    fn config_rejects_state_files_in_the_event_stream_directory() {
        for ledger_path in [
            "/var/lib/x/live-tick-events",
            "/var/lib/x/live-tick-events/ledger.json",
        ] {
            let mut config = execute_once_config(ledger_path, "/var/lib/x/runtime.json", "1000");
            let error = validate_config(&mut config).unwrap_err();
            assert!(error.to_string().contains("event-stream directory"));
        }

        let mut config = execute_once_config(
            "/var/lib/x/ledger.json",
            "/var/lib/x/live-tick-events",
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error.to_string().contains("event-stream directory"));
    }

    #[test]
    fn state_backup_refuses_an_unresolved_pending_event() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "1000",
        );
        persist_initial_operator_state(&config);
        fs::write(live_tick_pending_event_path(&config).unwrap(), b"pending").unwrap();

        let error = create_arcus_state_backup(&config, &dir.path().join("backup")).unwrap_err();
        assert!(error
            .to_string()
            .contains("pending durable event must be recovered"));
    }

    #[test]
    fn lexically_normalize_resolves_parent_dir_components() {
        assert_eq!(
            lexically_normalize(Path::new("/var/lib/x/sub/../runtime.json")),
            Path::new("/var/lib/x/runtime.json")
        );
        assert_eq!(
            lexically_normalize(Path::new("/var/lib/./x/runtime.json")),
            Path::new("/var/lib/x/runtime.json")
        );
        // A `..` at the root has nothing left to pop -- stays put rather
        // than escaping above the root or panicking.
        assert_eq!(
            lexically_normalize(Path::new("/../runtime.json")),
            Path::new("/runtime.json")
        );
    }

    #[test]
    fn resolve_path_for_collision_check_sees_through_a_symlinked_parent() {
        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real");
        fs::create_dir(&real_dir).unwrap();
        let alias_dir = dir.path().join("alias");
        std::os::unix::fs::symlink(&real_dir, &alias_dir).unwrap();

        let via_alias = resolve_path_for_collision_check(&alias_dir.join("x.json"));
        let via_real = resolve_path_for_collision_check(&real_dir.join("x.json"));
        assert_eq!(
            via_alias, via_real,
            "a symlinked directory component must resolve to the same path as its target"
        );
    }

    #[test]
    fn resolve_path_for_collision_check_falls_back_when_parent_does_not_exist_yet() {
        let path = Path::new("/nonexistent-parent-dir-arcus-test-xyz/runtime.json");
        assert_eq!(
            resolve_path_for_collision_check(path),
            lexically_normalize(path)
        );
    }

    #[test]
    fn resolve_path_for_collision_check_sees_through_a_symlinked_grandparent() {
        // Codex P2 follow-up, pairtrade#186: the immediate parent itself
        // doesn't exist here (only the symlinked grandparent does), which
        // the single-level canonicalize(parent) attempt alone cannot see
        // through.
        let dir = tempdir().unwrap();
        let real_dir = dir.path().join("real");
        fs::create_dir(&real_dir).unwrap();
        let alias_dir = dir.path().join("alias");
        std::os::unix::fs::symlink(&real_dir, &alias_dir).unwrap();

        let via_alias = resolve_path_for_collision_check(&alias_dir.join("new").join("x.json"));
        let via_real = resolve_path_for_collision_check(&real_dir.join("new").join("x.json"));
        assert_eq!(
            via_alias, via_real,
            "a symlinked grandparent must resolve to the same path as its target even when the \
             immediate parent directory doesn't exist yet"
        );
    }

    #[test]
    fn resolve_path_for_collision_check_resolves_parent_dir_traversal_across_a_symlink() {
        // Codex P2 follow-up, pairtrade#186: `..` must be resolved using
        // filesystem semantics together with any symlink it crosses, not
        // collapsed as plain text first. alias -> target_parent/child, so
        // alias/../sibling actually names target_parent/sibling (go
        // through the symlink, then up from *its target's* parent) --
        // not a sibling of `alias` itself.
        let dir = tempdir().unwrap();
        let target_parent = dir.path().join("target_parent");
        fs::create_dir(&target_parent).unwrap();
        fs::create_dir(target_parent.join("child")).unwrap();
        let sibling = target_parent.join("sibling");
        fs::create_dir(&sibling).unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(target_parent.join("child"), &alias).unwrap();

        let via_traversal =
            resolve_path_for_collision_check(&alias.join("..").join("sibling").join("x.json"));
        let via_direct = resolve_path_for_collision_check(&sibling.join("x.json"));
        assert_eq!(
            via_traversal, via_direct,
            "a `..` crossing a symlink must resolve relative to the symlink's target, not the \
             symlink's own location"
        );
    }

    #[test]
    fn config_rejects_a_symlinked_parent_disguised_collision() {
        // Codex P2 follow-up, pairtrade#186: lexical normalization alone
        // can't see through a symlinked directory component -- `alias`
        // and `state` compare unequal lexically even when `alias` really
        // points at `state`, so ledger_path=alias/live-tick-pending-plan.json
        // and runtime_state_path=state/runtime.json alias the same file on
        // disk despite passing the lexical-only check.
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        fs::create_dir(&state_dir).unwrap();
        let alias_dir = dir.path().join("alias");
        std::os::unix::fs::symlink(&state_dir, &alias_dir).unwrap();

        let ledger_path = alias_dir.join("live-tick-pending-plan.json");
        let runtime_state_path = state_dir.join("runtime.json");

        let mut config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_state_path.to_str().unwrap(),
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error.to_string().contains("live-tick pending-plan path"));
    }

    #[test]
    fn config_rejects_a_traversal_disguised_collision_with_the_pending_plan_path() {
        // Codex P2 follow-up, pairtrade#186: raw PathBuf equality doesn't
        // catch a ledger_path that is textually different from, but
        // lexically resolves to, the same file as the derived pending-plan
        // path -- exactly Codex's own example.
        let mut config = execute_once_config(
            "/var/lib/x/live-tick-pending-plan.json",
            "/var/lib/x/sub/../runtime.json",
            "1000",
        );
        let error = validate_config(&mut config).unwrap_err();
        assert!(error.to_string().contains("live-tick pending-plan path"));
    }

    /// bot-strategy#818 option C: counting declines does not say what they
    /// were worth, so each one records enough to price the counterfactual
    /// offline against the recorder archive — when, which way, how strong,
    /// at what size and price.
    #[test]
    fn a_declined_route_is_recorded_with_enough_to_price_it_later() {
        let dir = tempdir().unwrap();
        let ledger_path = dir.path().join("ledger.json");
        let runtime_path = dir.path().join("runtime.json");
        let config = execute_once_config(
            ledger_path.to_str().unwrap(),
            runtime_path.to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "rialto".to_string();
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let runtime = store.load_existing(&config.runtime).unwrap();
        let event = ArcusSpotRuntimeEvent {
            sequence: 41,
            observed_at: fixture_now(),
            pair: "NVDA/AMD".to_string(),
            mode: ArcusSpotRuntimeMode::Live,
            token_a_reference_price_usd: Some(Decimal::from(200)),
            token_b_reference_price_usd: Some(Decimal::from(100)),
            relative_log_price: Some(0.5),
            z_score: Some(2.9),
            inventory_before: runtime.state().inventory,
            inventory_after: runtime.state().inventory,
            regime_before: ArcusSpotRegime::Neutral,
            regime_after: ArcusSpotRegime::Neutral,
            risk_before: None,
            risk_after: None,
            decision: ArcusSpotDecision::WouldRotate { plan: plan.clone() },
        };

        // Through the same entry point the live-tick arm returns, so a
        // decline that stopped recording would fail here rather than
        // silently produce an empty file at readout time.
        decline_unsupported_route(&config, &event, &plan).unwrap();
        decline_unsupported_route(&config, &event, &plan).unwrap();

        let path = declined_route_log_path(&config).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600,
        );
        let lines: Vec<&str> = {
            let raw = fs::read_to_string(&path).unwrap();
            Box::leak(raw.into_boxed_str()).lines().collect()
        };
        assert_eq!(lines.len(), 2, "each decline appends, none overwrite");

        let row: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        // Signal strength, so the weak-vs-strong question is answerable.
        assert_eq!(row["z_score"], serde_json::json!(2.9));
        // Which venue took it away from us.
        assert_eq!(row["recommended_venue"], "rialto");
        // Direction and size, to reconstruct the position.
        assert_eq!(row["sell_symbol"], "NVDA");
        assert_eq!(row["buy_symbol"], "AMD");
        assert_eq!(row["sell_quantity"], "0.05");
        // Marks, to price entry against the archive.
        assert_eq!(row["token_a_reference_price_usd"], "200");
        assert_eq!(row["token_b_reference_price_usd"], "100");
        assert_eq!(row["sequence"], serde_json::json!(41));
    }

    fn persist_test_live_tick_plan(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
    ) {
        let at = fixture_now();
        let evidence = ArcusSpotLiveTickEvidence {
            schema_version: LIVE_TICK_EVIDENCE_SCHEMA_VERSION,
            evaluation_time: at,
            snapshot: accepted_entry_snapshot(at),
            plan: plan.clone(),
        };
        write_private_regular_file_atomic(
            &live_tick_pending_plan_path(config).unwrap(),
            &serde_json::to_vec_pretty(&evidence).unwrap(),
        )
        .unwrap();
    }

    fn ledger_with_active_plan(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
    ) -> ArcusSpotExecutionLedger {
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger.next_sequence = 2;
        ledger.active = Some(reconciled_entry_attempt(config, plan, 1));
        ledger
    }

    #[test]
    fn live_tick_active_recovery_loads_the_digest_bound_pending_plan() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        persist_test_live_tick_plan(&config, &plan);
        let ledger = ledger_with_active_plan(&config, &plan);

        let (recovered, digest) = live_tick_active_recovery_plan(&config, &ledger)
            .unwrap()
            .unwrap();

        assert_eq!(recovered, plan);
        assert_eq!(digest, ledger.active.unwrap().intent.plan_config_digest);
    }

    #[test]
    fn live_tick_active_recovery_rejects_overwritten_pending_plan() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let ledger = ledger_with_active_plan(&config, &plan);
        let mut later_plan = plan;
        later_plan.buy_quantity = Decimal::new(49, 2);
        persist_test_live_tick_plan(&config, &later_plan);

        let error = live_tick_active_recovery_plan(&config, &ledger).unwrap_err();

        assert!(error
            .to_string()
            .contains("does not match its live-tick pending-plan evidence"));
    }

    #[test]
    fn live_tick_without_an_active_attempt_ignores_stale_pending_evidence() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let pending_path = live_tick_pending_plan_path(&config).unwrap();
        write_private_regular_file_atomic(&pending_path, b"stale-not-json").unwrap();

        let recovery =
            live_tick_active_recovery_plan(&config, &ArcusSpotExecutionLedger::default()).unwrap();

        assert!(recovery.is_none());
    }

    #[test]
    fn live_tick_recheck_observes_an_attempt_created_after_the_initial_check() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
        let plan = rotation_plan("entry_signal");
        persist_test_live_tick_plan(&config, &plan);

        let initial_lock = store
            .acquire_exclusive_lock(&config.runtime_state_path)
            .unwrap();
        assert!(load_live_tick_active_recovery_plan(&config, &store)
            .unwrap()
            .is_none());
        drop(initial_lock);

        let competing_lock = store
            .acquire_exclusive_lock(&config.runtime_state_path)
            .unwrap();
        store
            .persist(&ledger_with_active_plan(&config, &plan))
            .unwrap();
        drop(competing_lock);

        let checkpoint_lock = store
            .acquire_exclusive_lock(&config.runtime_state_path)
            .unwrap();
        let recovery = load_live_tick_active_recovery_plan(&config, &store).unwrap();
        drop(checkpoint_lock);

        assert!(recovery.is_some());
    }

    /// bot-strategy#817/#818: a plan on an unvalidated venue is an ordinary
    /// market outcome, not a fault, so live-tick declines it before building
    /// anything rather than letting the executor fail the run.
    ///
    /// `ArcusSpotLiveExecutor::validate_plan` calls this same predicate, so
    /// the pre-dispatch check and the enforcement cannot drift apart into
    /// live-tick dispatching something the executor then refuses.
    #[test]
    fn only_validated_arcus_and_rialto_routes_are_dispatchable() {
        let arcus = rotation_plan("entry_signal");
        assert_eq!(arcus.venue, "arcus");
        assert!(is_supported_live_route(&arcus));

        // Case-insensitively: the venue string comes off the wire.
        let mut shouty = rotation_plan("entry_signal");
        shouty.venue = "ARCUS".to_string();
        assert!(is_supported_live_route(&shouty));

        // The venue that actually wins most routes in practice -- about two
        // thirds of them in the recorder archive on 2026-08-19.
        let mut rialto = rotation_plan("entry_signal");
        rialto.venue = "rialto".to_string();
        assert!(is_supported_live_route(&rialto));

        let mut lifi = rotation_plan("entry_signal");
        lifi.venue = "lifi".to_string();
        assert!(!is_supported_live_route(&lifi));

        let mut empty = rotation_plan("entry_signal");
        empty.venue = String::new();
        assert!(!is_supported_live_route(&empty));
    }

    fn rotation_plan(trigger: &str) -> ArcusSpotRotationPlan {
        serde_json::from_value(serde_json::json!({
            "direction": "token_a_to_token_b",
            "trigger": trigger,
            "sell_symbol": "NVDA",
            "buy_symbol": "AMD",
            "sell_token_address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
            "buy_token_address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
            "sell_quantity": "0.05",
            "buy_quantity": "0.05",
            "sell_amount_raw": "50000000000000000",
            "buy_amount_raw": "50000000000000000",
            "venue": "arcus",
            "quote_received_at": "2026-08-14T00:00:00Z",
            "optimistic_round_trip_loss_bps": "76.5",
            "gas_buffer_bps": "10",
            "settlement_buffer_bps": "10",
            "all_in_round_trip_cost_bps": "96.5",
            "predicted_inventory": {"token_a": "0.30", "token_b": "0.21"},
            "predicted_inventory_imbalance_fraction": "0.23628662061830083",
        }))
        .unwrap()
    }

    #[test]
    fn auto_execute_refuses_a_fresh_entry_signal_plan() {
        // The gap Codex flagged (P1 follow-up, pairtrade#186): nothing on
        // the signatureless path authenticates that an entry plan's
        // strategy fields (z-score crossing, round-trip cost, rotation
        // fraction, inventory imbalance) came from a genuine step_at
        // evaluation rather than being hand-crafted within every other
        // check's tolerance.
        let plan = rotation_plan("entry_signal");
        let error = require_auto_execute_plan_is_not_a_fresh_entry(&plan).unwrap_err();
        assert!(error.to_string().contains("entry_signal"));
    }

    #[test]
    fn auto_execute_allows_a_mean_reversion_exit_plan() {
        // Exits are risk-reducing and already bounded by the checkpoint's
        // own genuinely-open rotated quantity (validate_plan_consistent_
        // with_state), so they are not restricted to execute/live-tick.
        let plan = rotation_plan("mean_reversion_exit");
        require_auto_execute_plan_is_not_a_fresh_entry(&plan).unwrap();
    }

    #[test]
    fn auto_execute_allows_a_max_hold_exit_plan() {
        let plan = rotation_plan("max_hold_exit");
        require_auto_execute_plan_is_not_a_fresh_entry(&plan).unwrap();
    }

    fn auto_execute_policy_for(config: &ArcusSpotExecuteOnceConfig) -> ArcusSpotAutoExecutePolicy {
        ArcusSpotAutoExecutePolicy {
            approved_config_sha256: auto_execute_config_digest(config).unwrap(),
        }
    }

    #[test]
    fn auto_execute_policy_accepts_a_config_matching_the_approved_digest() {
        let config =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&config);
        require_config_within_auto_execute_policy(&config, &policy).unwrap();
    }

    #[test]
    fn auto_execute_policy_rejects_a_redirected_ledger_path() {
        // Without this, the executor identity could point ledger_path at a
        // fresh, empty file to silently reset the daily swap count and
        // prior attempt history that the real ledger accumulates.
        let approved =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config(
            "/tmp/attacker-chosen/ledger.json",
            "/var/lib/x/runtime.json",
            "1000",
        );
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match the administrator-approved configuration"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_redirected_runtime_state_path() {
        let approved =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config(
            "/var/lib/x/ledger.json",
            "/tmp/attacker-chosen/runtime.json",
            "1000",
        );
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match the administrator-approved configuration"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_sell_ceiling_raised_past_the_administrator_approved_value() {
        let approved =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config(
            "/var/lib/x/ledger.json",
            "/var/lib/x/runtime.json",
            "999999999999",
        );
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match the administrator-approved configuration"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_field_the_old_field_by_field_check_never_covered() {
        // Regression guard for the exact gap Codex flagged (P1 follow-up,
        // pairtrade#186): the earlier policy shape only compared
        // ledger_path/runtime_state_path/maximum_sell_amount_raw, so a
        // change to any other field -- like max_swaps_per_utc_day here --
        // would have passed silently. Digest-binding the whole config
        // closes that regardless of which field changes.
        let approved = execute_once_config_with_daily_cap(
            "/var/lib/x/ledger.json",
            "/var/lib/x/runtime.json",
            "1000",
            10,
        );
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config_with_daily_cap(
            "/var/lib/x/ledger.json",
            "/var/lib/x/runtime.json",
            "1000",
            999,
        );
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match the administrator-approved configuration"));
    }

    #[test]
    fn auto_execute_policy_file_owned_by_this_process_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auto_execute_policy.json");
        fs::write(&path, r#"{"approved_config_sha256":"sha256:00"}"#).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let error = auto_execute_policy_from_file(&path).unwrap_err();
        assert!(error.to_string().contains("administrator-owned"));
    }

    #[test]
    fn auto_execute_policy_file_that_is_group_writable_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auto_execute_policy.json");
        fs::write(&path, r#"{"approved_config_sha256":"sha256:00"}"#).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o664)).unwrap();
        let error = auto_execute_policy_from_file(&path).unwrap_err();
        // Group-writable AND owned by this process -- the ownership check
        // fires first, which is still a correct rejection; assert on
        // whichever this environment's fs::write() ownership produces.
        assert!(
            error.to_string().contains("group- or other-writable")
                || error.to_string().contains("administrator-owned")
        );
    }

    #[test]
    fn live_tick_persists_the_checkpoint_even_on_an_observe_decision() {
        // The behavior live-tick relies on: unlike execute/auto-execute
        // (which only ever touch the checkpoint via a confirmed fill),
        // live-tick must persist after *every* tick so the accumulated
        // price-history window survives even ticks that decide Observe --
        // otherwise a probe running mostly-Observe (duty-cycle-starved, as
        // this style of signal generally is) would never actually build up
        // the history its own entry/exit z-score needs.
        let dir = tempdir().unwrap();
        let state_path = dir.path().join("runtime.json");
        let config = live_runtime_config();
        let store = ArcusSpotRuntimeCheckpointStore::new(state_path.clone());
        let mut runtime = store.load_or_create(&config).unwrap();

        let snapshot: ArcusSpotRecorderSnapshot = serde_json::from_str(
            r#"{"schema_version":2,"mode":"public_indicative_read_only","chain_id":4663,"collection_started_at":"2026-07-27T00:00:00Z","collection_finished_at":"2026-07-27T00:00:01Z","indexer_stats":{"status":"error","error":{"stage":"indexer_stats","classification":"http","retryable":false,"message":"x"}},"token_metadata":{"status":"error","error":{"stage":"token_metadata","classification":"http","retryable":false,"message":"x"}},"reference_overview":{"status":"error","error":{"stage":"reference_overview","classification":"http","retryable":false,"message":"x"}},"round_trips":[]}"#,
        )
        .unwrap();

        let event = runtime.step_at(&snapshot, fixture_now());
        assert!(matches!(event.decision, ArcusSpotDecision::Observe { .. }));
        store.persist(&runtime).unwrap();

        assert!(state_path.exists());
        assert_eq!(
            fs::metadata(&state_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let reloaded = store.load_or_create(&config).unwrap();
        assert_eq!(reloaded.state().sequence, runtime.state().sequence);
    }

    fn write_private_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn repair_report_active_submitted_attempt(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
        at: DateTime<Utc>,
    ) -> ArcusSpotExecutionAttempt {
        let payload_hash = format!("sha256:{}", "b".repeat(64));
        ArcusSpotExecutionAttempt {
            sequence: 2,
            idempotency_key: format!(
                "arcus-spot-{:020}-{}",
                2,
                &payload_hash["sha256:".len()..][..16]
            ),
            payload_hash,
            chain_id: config.runtime.chain_id,
            taker: config.executor.taker.clone(),
            prepared_at: at,
            dispatched_at: Some(at),
            updated_at: at,
            phase: ArcusSpotExecutionPhase::Submitted,
            intent: ArcusSpotExecutionIntent {
                venue: plan.venue.clone(),
                sell_symbol: plan.sell_symbol.clone(),
                buy_symbol: plan.buy_symbol.clone(),
                sell_token: plan.sell_token_address.clone(),
                buy_token: plan.buy_token_address.clone(),
                sell_amount_raw: plan.sell_amount_raw.clone(),
                minimum_buy_amount_raw: "1".to_string(),
                plan_config_digest: approval_digest(config, plan).unwrap(),
            },
            pre_balances: ArcusSpotBalanceSnapshot {
                observed_at: at,
                sell_token: plan.sell_token_address.clone(),
                buy_token: plan.buy_token_address.clone(),
                sell_balance_raw: "1000000000000000000".to_string(),
                buy_balance_raw: "100000000000000000".to_string(),
                gas_balance_wei: "1000000000000000".to_string(),
            },
            post_balances: None,
            tx_hash: Some(format!("0x{:064x}", 7)),
            router_status: Some("submitted".to_string()),
            detail: None,
        }
    }

    /// Persists a runtime checkpoint plus a ledger whose only attempt is
    /// `active`, and provisions the executor lock file so
    /// `acquire_existing_exclusive_lock` (the read-only primitive
    /// `repair_report` uses) succeeds exactly as it would against a real
    /// deployment.
    fn persist_repair_report_ledger_state(
        config: &ArcusSpotExecuteOnceConfig,
        active: Option<ArcusSpotExecutionAttempt>,
    ) {
        let runtime = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .persist(&runtime)
            .unwrap();
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger.next_sequence = 3;
        ledger.active = active;
        let store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
        store.persist(&ledger).unwrap();
        drop(
            store
                .acquire_exclusive_lock(&config.runtime_state_path)
                .unwrap(),
        );
    }

    fn repair_report_would_rotate_event(
        sequence: u64,
        observed_at: DateTime<Utc>,
        plan: ArcusSpotRotationPlan,
    ) -> ArcusSpotRuntimeEvent {
        ArcusSpotRuntimeEvent {
            sequence,
            observed_at,
            pair: "NVDA/AMD".to_string(),
            mode: ArcusSpotRuntimeMode::Live,
            token_a_reference_price_usd: Some(Decimal::from(200)),
            token_b_reference_price_usd: Some(Decimal::from(100)),
            relative_log_price: Some(0.5),
            z_score: Some(2.9),
            inventory_before: ArcusSpotInventory {
                token_a: Decimal::new(30, 2),
                token_b: Decimal::new(21, 2),
            },
            inventory_after: ArcusSpotInventory {
                token_a: Decimal::new(30, 2),
                token_b: Decimal::new(21, 2),
            },
            regime_before: ArcusSpotRegime::Neutral,
            regime_after: ArcusSpotRegime::Neutral,
            risk_before: None,
            risk_after: None,
            decision: ArcusSpotDecision::WouldRotate { plan },
        }
    }

    fn write_repair_report_event_archive(path: &Path, events: &[ArcusSpotRuntimeEvent]) {
        let mut previous = None;
        let mut lines = Vec::new();
        for event in events {
            let record = event_record(event, previous.clone()).unwrap();
            previous = Some(record.chain_sha256.clone());
            lines.push(serde_json::to_string(&record).unwrap());
        }
        // verify_archive_events (manual-reconcile-*'s stricter archive
        // check, unlike repair-report's own per-line scan) requires a
        // trailing newline, matching a genuine on-host segment file.
        let mut content = lines.join("\n");
        if !content.is_empty() {
            content.push('\n');
        }
        write_private_file(path, content.as_bytes());
    }

    #[test]
    fn repair_report_recovers_a_plan_that_reproduces_the_ledger_digest() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "rialto".to_string();
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active.clone()));

        // A decoy at an unrelated sell_amount_raw must not match the coarse
        // intent filter, and so must never even reach a digest comparison.
        let mut decoy = plan.clone();
        decoy.sell_amount_raw = "999999999999999999".to_string();
        decoy.sell_quantity = Decimal::new(999999, 6);
        write_repair_report_event_archive(
            &events_path,
            &[
                repair_report_would_rotate_event(100, at - chrono::Duration::seconds(10), decoy),
                repair_report_would_rotate_event(101, at, plan.clone()),
            ],
        );

        let report = build_repair_report(&config_path, &events_path).unwrap();
        assert_eq!(report["status"], "recovered");
        assert_eq!(
            report["recovered_plan"]["sell_amount_raw"],
            serde_json::json!(plan.sell_amount_raw)
        );
        assert_eq!(
            report["active_attempt"]["tx_hash"],
            serde_json::json!(active.tx_hash)
        );
        // Submitted is one of the phases resume_status_and_reconcile accepts,
        // so it is fine to point the operator at auto-resume here.
        assert_eq!(report["resumable_via_auto_resume"], true);
        let steps: Vec<&str> = report["next_steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|step| step.as_str().unwrap())
            .collect();
        assert!(steps.iter().any(|step| step.contains("auto-resume")));
        // Codex P2 follow-up, pairtrade#240: the report's own exclusive
        // lock is released once this call returns, so the guidance must
        // tell the operator to revalidate attempt identity immediately
        // before restoring the file rather than trusting this snapshot
        // indefinitely.
        assert!(steps
            .iter()
            .any(|step| step.contains("re-run repair-report")));
        assert!(steps
            .iter()
            .any(|step| step.contains(&active.sequence.to_string())));
        // Codex P2 follow-up, pairtrade#240: a restored bare plan is not a
        // full live-tick evidence envelope, so state-verify-continuity will
        // reject it until overwritten by a genuine dispatch. The report must
        // say so up front rather than let it surprise the operator later.
        assert!(steps
            .iter()
            .any(|step| step.contains("state-verify-continuity")));
        // Exactly the intent-matching, digest-matching candidate -- the
        // decoy never appears because it fails the coarse filter first.
        assert_eq!(report["candidates_scanned"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn repair_report_flags_a_recovered_plan_in_a_non_resumable_phase_for_manual_review() {
        // Codex P2 follow-up, pairtrade#240: resume_status_and_reconcile only
        // accepts Submitted/Confirmed/Reconciled (live_executor.rs); pointing
        // an operator at auto-resume for e.g. an OperatorHold attempt would
        // just fail again and, worse, imply the tool has a routine answer
        // for a state that specifically needs a human decision.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "rialto".to_string();
        let at = fixture_now();
        let mut active = repair_report_active_submitted_attempt(&config, &plan, at);
        active.phase = ArcusSpotExecutionPhase::OperatorHold;
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report = build_repair_report(&config_path, &events_path).unwrap();
        assert_eq!(report["status"], "recovered");
        assert_eq!(report["resumable_via_auto_resume"], false);
        let steps: Vec<&str> = report["next_steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|step| step.as_str().unwrap())
            .collect();
        assert!(!steps.iter().any(|step| step.contains("run `auto-resume")));
        assert!(steps.iter().any(|step| step.contains("OperatorHold")));
    }

    #[test]
    fn repair_report_refuses_a_same_shaped_plan_that_does_not_reproduce_the_digest() {
        // Regression coverage for the real bot-strategy#869 incident: a
        // later live-tick had already overwritten `live-tick-pending-plan.json`
        // by the time this ran, and the nearest same-venue/same-symbols/
        // same-sell_amount_raw `WouldRotate` event in the durable archive
        // turned out to carry different quote-derived fields (a fresh quote
        // was pulled between evaluation and dispatch) and so did not
        // reproduce the ledger's `plan_config_digest`. The report must say
        // so plainly rather than ever recommending a same-shaped plan it
        // cannot prove is the one that was actually signed and dispatched.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "rialto".to_string();
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));

        // Same venue/symbols/sell_amount_raw (passes the coarse filter) but
        // a different quote -- exactly what a re-quote between the logged
        // evaluation and the actual dispatch produces.
        let mut near_miss = plan.clone();
        near_miss.buy_quantity = plan.buy_quantity + Decimal::new(1, 6);
        near_miss.buy_amount_raw = "44954909625073291".to_string();
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(
                101,
                at - chrono::Duration::seconds(2),
                near_miss,
            )],
        );

        let report = build_repair_report(&config_path, &events_path).unwrap();
        assert_eq!(report["status"], "no_digest_match");
        let candidates = report["candidates_scanned"].as_array().unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0]["digest_matches_ledger"], false);
        assert!(report.get("recovered_plan").is_none());
    }

    #[test]
    fn repair_report_reports_no_active_attempt_when_the_ledger_is_flat() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        persist_repair_report_ledger_state(&config, None);
        write_private_file(&events_path, b"");

        let report = build_repair_report(&config_path, &events_path).unwrap();
        assert_eq!(report["status"], "no_active_attempt");
    }

    fn rejected_attempt_no_tx(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
        at: DateTime<Utc>,
    ) -> ArcusSpotExecutionAttempt {
        let mut attempt = repair_report_active_submitted_attempt(config, plan, at);
        attempt.phase = ArcusSpotExecutionPhase::Rejected;
        attempt.tx_hash = None;
        attempt.router_status = None;
        attempt.detail = Some("HTTP 422 SHELL_SUBMIT_FAILED".to_string());
        attempt
    }

    #[test]
    fn archive_rejected_report_reports_no_active_attempt_when_the_ledger_is_flat() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        persist_repair_report_ledger_state(&config, None);

        let report = build_archive_rejected_report(&config).unwrap();
        assert_eq!(report["status"], "no_active_attempt");
    }

    #[test]
    fn archive_rejected_report_flags_eligible_for_a_rejected_attempt_with_no_tx_hash() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let at = fixture_now();
        let plan = rotation_plan("entry_signal");
        persist_repair_report_ledger_state(
            &config,
            Some(rejected_attempt_no_tx(&config, &plan, at)),
        );

        let report = build_archive_rejected_report(&config).unwrap();
        assert_eq!(report["status"], "eligible_to_archive");
        assert_eq!(report["reason_if_ineligible"], serde_json::Value::Null);
    }

    #[test]
    fn archive_rejected_report_flags_not_eligible_for_a_submitted_attempt() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let at = fixture_now();
        let plan = rotation_plan("entry_signal");
        persist_repair_report_ledger_state(
            &config,
            Some(repair_report_active_submitted_attempt(&config, &plan, at)),
        );

        let report = build_archive_rejected_report(&config).unwrap();
        assert_eq!(report["status"], "not_eligible");
        assert!(report["reason_if_ineligible"].is_string());
    }

    #[test]
    fn archive_rejected_report_flags_not_eligible_for_a_rejected_attempt_with_a_tx_hash() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let at = fixture_now();
        let plan = rotation_plan("entry_signal");
        let mut attempt = rejected_attempt_no_tx(&config, &plan, at);
        attempt.tx_hash = Some(format!("0x{:064x}", 7));
        persist_repair_report_ledger_state(&config, Some(attempt));

        let report = build_archive_rejected_report(&config).unwrap();
        assert_eq!(report["status"], "not_eligible");
    }

    #[test]
    fn commit_archive_rejected_archives_a_matching_sequence() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let at = fixture_now();
        let plan = rotation_plan("entry_signal");
        let attempt = rejected_attempt_no_tx(&config, &plan, at);
        let sequence = attempt.sequence;
        persist_repair_report_ledger_state(&config, Some(attempt));

        let result = commit_archive_rejected(&config, sequence).unwrap();
        assert_eq!(result["archived"]["sequence"], sequence);

        let ledger = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone())
            .load_existing()
            .unwrap();
        assert!(ledger.active.is_none());
        assert_eq!(ledger.history.len(), 1);
    }

    #[test]
    fn commit_archive_rejected_refuses_a_sequence_mismatch() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let at = fixture_now();
        let plan = rotation_plan("entry_signal");
        let attempt = rejected_attempt_no_tx(&config, &plan, at);
        let real_sequence = attempt.sequence;
        persist_repair_report_ledger_state(&config, Some(attempt));

        let error = commit_archive_rejected(&config, real_sequence + 1).unwrap_err();
        assert!(error.to_string().contains("refusing to archive"));

        // Refusing must leave the ledger untouched.
        let ledger = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone())
            .load_existing()
            .unwrap();
        assert!(ledger.active.is_some());
    }

    #[test]
    fn commit_archive_rejected_refuses_a_non_rejected_active_attempt() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let at = fixture_now();
        let plan = rotation_plan("entry_signal");
        let attempt = repair_report_active_submitted_attempt(&config, &plan, at);
        let sequence = attempt.sequence;
        persist_repair_report_ledger_state(&config, Some(attempt));

        assert!(commit_archive_rejected(&config, sequence).is_err());
    }

    /// `repair_report_active_submitted_attempt` plus a `Reconciled` phase
    /// and real post-swap balances: sells `plan.sell_amount_raw` and buys
    /// `plan.buy_amount_raw` exactly, so `sell_balance_raw`/`buy_balance_raw`
    /// deltas equal those two raw amounts precisely.
    fn manual_reconcile_reconciled_attempt(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
        at: DateTime<Utc>,
    ) -> ArcusSpotExecutionAttempt {
        let mut active = repair_report_active_submitted_attempt(config, plan, at);
        active.phase = ArcusSpotExecutionPhase::Reconciled;
        active.post_balances = Some(ArcusSpotBalanceSnapshot {
            observed_at: at,
            sell_token: plan.sell_token_address.clone(),
            buy_token: plan.buy_token_address.clone(),
            sell_balance_raw: "950000000000000000".to_string(),
            buy_balance_raw: "150000000000000000".to_string(),
            gas_balance_wei: "1000000000000000".to_string(),
        });
        active
    }

    #[test]
    fn manual_reconcile_report_previews_a_not_yet_reconciled_attempt_from_a_digest_mismatching_candidate(
    ) {
        // The gap manual-reconcile-* exists for: exactly the same
        // digest-mismatching event that
        // repair_report_refuses_a_same_shaped_plan_that_does_not_reproduce_the_digest
        // makes repair-report refuse must still resolve to a usable
        // candidate here, since this path never computes or checks a
        // plan_config_digest at all.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );

        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active.clone()));

        let mut near_miss = plan.clone();
        near_miss.buy_quantity = plan.buy_quantity + Decimal::new(1, 6);
        near_miss.buy_amount_raw = "50000000000001000".to_string();
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(
                101,
                at - chrono::Duration::seconds(2),
                near_miss.clone(),
            )],
        );

        let report = build_manual_reconcile_report(&config, &events_path, "1", "1").unwrap();
        assert_eq!(report["status"], "not_yet_reconciled");
        assert_eq!(
            report["active_attempt"]["sequence"],
            serde_json::json!(active.sequence)
        );
        assert_eq!(
            report["candidate_plan"]["buy_amount_raw"],
            serde_json::json!(near_miss.buy_amount_raw)
        );
    }

    #[test]
    fn manual_reconcile_report_flags_a_non_resumable_phase_instead_of_pointing_at_apply() {
        // Codex P2 follow-up, pairtrade#241: resume_status_and_reconcile
        // only accepts Submitted/Confirmed/Reconciled. For every other
        // phase (Prepared/Dispatching/Rejected/Failed/Unknown/
        // OperatorHold), manual-reconcile-apply's first call would bail
        // immediately -- the report must say so plainly instead of telling
        // the operator apply will "advance" it.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );

        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let mut active = repair_report_active_submitted_attempt(&config, &plan, at);
        active.phase = ArcusSpotExecutionPhase::OperatorHold;
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report = build_manual_reconcile_report(&config, &events_path, "1", "1").unwrap();
        assert_eq!(report["status"], "not_resumable");
        assert!(report["detail"]
            .as_str()
            .unwrap()
            .contains("manual operator decision"));
    }

    #[test]
    fn manual_reconcile_report_errs_when_no_would_rotate_event_matches() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));
        write_private_file(&events_path, b"");

        let error = build_manual_reconcile_report(&config, &events_path, "1", "1").unwrap_err();
        assert!(
            error.to_string().contains("no WouldRotate event matching"),
            "{error}"
        );
    }

    #[test]
    fn manual_reconcile_report_rejects_an_archive_with_a_broken_hash_chain() {
        // Codex P2 follow-up, pairtrade#241: verify_record alone proves
        // only that a single record's own hashes are self-consistent -- it
        // says nothing about whether the record is a genuine, unmodified
        // part of the real event stream. A forged record can carry
        // perfectly self-consistent hashes of its own while breaking the
        // chain to its neighbor. repair-report catches this downstream via
        // plan_config_digest; manual-reconcile-* has no such backstop, so
        // it must catch it here instead (verify_archive_events).
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));

        // A genuine two-event chain, plus a third record whose own hashes
        // are internally self-consistent (it would pass verify_record on
        // its own) but whose previous_chain_sha256 does not chain from the
        // second event -- exactly what splicing a forged record into an
        // otherwise real export looks like.
        let genuine = [
            repair_report_would_rotate_event(101, at, plan.clone()),
            repair_report_would_rotate_event(102, at + chrono::Duration::seconds(1), plan.clone()),
        ];
        let mut lines = Vec::new();
        let mut previous = None;
        for event in &genuine {
            let record = event_record(event, previous.clone()).unwrap();
            previous = Some(record.chain_sha256.clone());
            lines.push(serde_json::to_string(&record).unwrap());
        }
        let forged_event =
            repair_report_would_rotate_event(103, at + chrono::Duration::seconds(2), plan.clone());
        let forged_record = event_record(&forged_event, None).unwrap();
        lines.push(serde_json::to_string(&forged_record).unwrap());
        let mut content = lines.join("\n");
        content.push('\n');
        write_private_file(&events_path, content.as_bytes());

        let error = build_manual_reconcile_report(&config, &events_path, "1", "1").unwrap_err();
        assert!(
            format!("{error:#}").contains("hash-chain break"),
            "{error:#}"
        );
    }

    #[test]
    fn manual_reconcile_report_errs_when_multiple_would_rotate_events_match() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[
                repair_report_would_rotate_event(101, at, plan.clone()),
                repair_report_would_rotate_event(
                    102,
                    at + chrono::Duration::seconds(1),
                    plan.clone(),
                ),
            ],
        );

        let error = build_manual_reconcile_report(&config, &events_path, "1", "1").unwrap_err();
        assert!(
            error.to_string().contains("refusing to pick one"),
            "{error}"
        );
    }

    #[test]
    fn manual_reconcile_report_errs_when_the_candidate_direction_disagrees_with_the_configured_pair(
    ) {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = repair_report_active_submitted_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));

        // sell_symbol/buy_symbol (NVDA/AMD) still coarse-match the active
        // intent, but the flipped direction is inconsistent with those
        // symbols under the configured pair (sell_symbol=NVDA/buy_symbol=AMD):
        // TokenBToTokenA requires selling AMD and buying NVDA.
        let mut wrong_direction = plan.clone();
        wrong_direction.direction = ArcusSpotDirection::TokenBToTokenA;
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, wrong_direction)],
        );

        let error = build_manual_reconcile_report(&config, &events_path, "1", "1").unwrap_err();
        assert!(
            error.to_string().contains("configured runtime pair"),
            "{error}"
        );
    }

    #[test]
    fn manual_reconcile_report_is_ready_for_a_reconciled_attempt_with_correct_expected_amounts() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report = build_manual_reconcile_report(
            &config,
            &events_path,
            "50000000000000000",
            "50000000000000000",
        )
        .unwrap();
        assert_eq!(report["status"], "ready");
        // Computed from the operator-attested raw amounts and the
        // config-pinned decimals (18 for both NVDA/AMD in execute_once_config),
        // not copied from plan.sell_quantity/buy_quantity -- so it is
        // numerically equal but not necessarily the identical Decimal scale
        // (Codex P2 follow-up, pairtrade#241).
        let actual_sell_quantity: Decimal = report["proposed_fill"]["actual_sell_quantity"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let actual_buy_quantity: Decimal = report["proposed_fill"]["actual_buy_quantity"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(actual_sell_quantity, plan.sell_quantity);
        assert_eq!(actual_buy_quantity, plan.buy_quantity);
        assert!(report["next_steps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|step| step.as_str().unwrap().contains("manual-reconcile-apply")));
    }

    #[test]
    fn manual_reconcile_report_refuses_a_decimals_pin_whose_address_moved_since_dispatch() {
        // Codex P1 follow-up, pairtrade#241: if CONFIG_YAML's symbol->address
        // pin for the buy symbol has changed since this attempt was
        // dispatched (a legitimate, later administrator-approved config
        // update, unrelated to this specific attempt), its decimals must
        // not be trusted for an attempt signed against the *old* address --
        // converting the real raw amount at the new contract's decimals
        // could silently produce the wrong quantity.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let mut config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);
        // The config now resolves AMD (the buy symbol) to a different
        // contract than active.intent.buy_token / plan.buy_token_address.
        config.router.trusted_token_addresses.insert(
            "AMD".to_string(),
            "0x0000000000000000000000000000000000000099".to_string(),
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let error = build_manual_reconcile_report(
            &config,
            &events_path,
            "50000000000000000",
            "50000000000000000",
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("does not match the address"),
            "{error:#}"
        );
    }

    #[test]
    fn manual_reconcile_report_dry_runs_the_commit_before_reporting_ready() {
        // Codex P2 follow-up, pairtrade#241: manual_reconciled_runtime_fill_for_attempt
        // only derives quantities and checks ledger/balance deltas; it does
        // not run apply_confirmed_live_fill_once's own checks (regime/
        // trigger consistency, in this case). A mean_reversion_exit plan
        // against a checkpoint that is still Neutral (no open rotated
        // position) would compute a fine-looking fill here but fail at
        // apply's real commit step -- the report must catch that and say
        // would_fail, not ready.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("mean_reversion_exit");
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);
        // persist_repair_report_ledger_state always persists a fresh,
        // Neutral-regime checkpoint -- inconsistent with a
        // mean_reversion_exit plan, which requires an already-rotated
        // regime with tracked open quantity.
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report = build_manual_reconcile_report(
            &config,
            &events_path,
            "50000000000000000",
            "50000000000000000",
        )
        .unwrap();
        assert_eq!(report["status"], "would_fail", "{report}");
        assert!(report["detail"]
            .as_str()
            .unwrap()
            .contains("failed to commit the reconciled fill"));
    }

    #[test]
    fn manual_reconcile_report_refuses_when_the_runtime_checkpoint_is_missing() {
        // Codex P1 follow-up, pairtrade#241: load_or_create would silently
        // construct a fresh runtime from initial_inventory on a
        // missing/lost checkpoint file, discarding whatever real tracked
        // inventory/regime/signal history/risk state it held. Checkpoint
        // loss must surface as an explicit error, not an implicit reset.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);

        // Persist only the ledger (with its lock); never write the runtime
        // checkpoint file -- simulating checkpoint loss.
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger.next_sequence = 3;
        ledger.active = Some(active);
        let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
        ledger_store.persist(&ledger).unwrap();
        drop(
            ledger_store
                .acquire_exclusive_lock(&config.runtime_state_path)
                .unwrap(),
        );
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let error = build_manual_reconcile_report(
            &config,
            &events_path,
            "50000000000000000",
            "50000000000000000",
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not exist"), "{error}");
    }

    #[test]
    fn manual_reconcile_report_is_ready_when_the_fill_was_already_committed_by_a_crashed_invocation(
    ) {
        // Codex P2 follow-up, pairtrade#241: if a prior invocation
        // persisted the runtime fill but crashed before archiving the
        // ledger attempt, apply_confirmed_live_fill_once short-circuits to
        // Ok(false) on the matching idempotency key without re-validating
        // regime consistency -- the checkpoint has already moved on from
        // what the plan describes (Neutral -> RotatedAToB for this
        // entry_signal plan), which would otherwise make
        // validate_plan_consistent_with_state fail. The report must
        // recognize this as already-safe, not would_fail.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);

        let mut runtime = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        runtime
            .apply_confirmed_live_fill_once(
                &plan,
                plan.sell_quantity,
                plan.buy_quantity,
                at,
                &active.idempotency_key,
            )
            .unwrap();
        ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .persist(&runtime)
            .unwrap();

        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger.next_sequence = 3;
        ledger.active = Some(active);
        let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
        ledger_store.persist(&ledger).unwrap();
        drop(
            ledger_store
                .acquire_exclusive_lock(&config.runtime_state_path)
                .unwrap(),
        );
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report = build_manual_reconcile_report(
            &config,
            &events_path,
            "50000000000000000",
            "50000000000000000",
        )
        .unwrap();
        assert_eq!(report["status"], "ready", "{report}");
    }

    #[test]
    fn manual_reconcile_report_ignores_the_candidate_plans_own_quantities() {
        // Codex P2 follow-up, pairtrade#241: proposed_fill must come from
        // the operator-attested raw amounts and the config-pinned
        // trusted_token_decimals, never from the archived candidate's own
        // sell_quantity/buy_quantity/buy_amount_raw -- a forged or spliced
        // candidate claiming wildly different quantities for the same real
        // settled amounts must produce the identical proposed_fill.
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let mut plan = rotation_plan("entry_signal");
        plan.buy_quantity = Decimal::new(999_999, 0);
        plan.buy_amount_raw = "1".to_string();
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report = build_manual_reconcile_report(
            &config,
            &events_path,
            "50000000000000000",
            "50000000000000000",
        )
        .unwrap();
        assert_eq!(report["status"], "ready");
        let actual_buy_quantity: Decimal = report["proposed_fill"]["actual_buy_quantity"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        // Not plan.buy_quantity (999999) -- the real settled amount at the
        // config-pinned 18 decimals.
        assert_eq!(actual_buy_quantity, Decimal::new(5, 2));
    }

    #[test]
    fn manual_reconcile_report_would_fail_when_the_expected_buy_amount_is_wrong() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        let events_path = dir.path().join("events.jsonl");
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        write_private_file(
            &config_path,
            serde_yaml::to_string(&config).unwrap().as_bytes(),
        );
        let plan = rotation_plan("entry_signal");
        let at = fixture_now();
        let active = manual_reconcile_reconciled_attempt(&config, &plan, at);
        persist_repair_report_ledger_state(&config, Some(active));
        write_repair_report_event_archive(
            &events_path,
            &[repair_report_would_rotate_event(101, at, plan.clone())],
        );

        let report =
            build_manual_reconcile_report(&config, &events_path, "50000000000000000", "1").unwrap();
        assert_eq!(report["status"], "would_fail");
        assert!(report["detail"]
            .as_str()
            .unwrap()
            .contains("reconciled buy delta"));
    }

    #[test]
    fn manual_reconcile_apply_pins_reject_a_sequence_mismatch() {
        let plan = rotation_plan("entry_signal");
        let config =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let active = repair_report_active_submitted_attempt(&config, &plan, fixture_now());
        let error = require_active_attempt_matches_pins(
            &active,
            active.sequence + 1,
            &active.idempotency_key,
            active.tx_hash.as_deref().unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn manual_reconcile_apply_pins_reject_a_tx_hash_mismatch() {
        let plan = rotation_plan("entry_signal");
        let config =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let active = repair_report_active_submitted_attempt(&config, &plan, fixture_now());
        let error = require_active_attempt_matches_pins(
            &active,
            active.sequence,
            &active.idempotency_key,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn manual_reconcile_apply_pins_accept_the_exact_active_attempt() {
        let plan = rotation_plan("entry_signal");
        let config =
            execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let active = repair_report_active_submitted_attempt(&config, &plan, fixture_now());
        require_active_attempt_matches_pins(
            &active,
            active.sequence,
            &active.idempotency_key,
            active.tx_hash.as_deref().unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn auto_execute_policy_symlink_is_rejected() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("real_policy.json");
        fs::write(&target, r#"{"approved_config_sha256":"sha256:00"}"#).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("auto_execute_policy.json");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = auto_execute_policy_from_file(&link).unwrap_err();
        assert!(error.to_string().contains("non-symlink"));
    }
}
