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
use debot::arcus_spot::corporate_action_effective_cutoff;
#[cfg(test)]
use debot::arcus_spot::event_record;
use debot::arcus_spot::resolve_handled_corporate_action_fingerprints;
use debot::arcus_spot::{
    build_arcus_spot_kms_signer, is_supported_live_route,
    manual_reconciled_runtime_fill_for_attempt, open_exit_fixed_sell_amount_row_for,
    verify_archive_events, verify_record, ArcusSpotChainClient, ArcusSpotChainConfig,
    ArcusSpotCorporateActionProgress, ArcusSpotDecision, ArcusSpotDirection,
    ArcusSpotExecutionAttempt, ArcusSpotExecutionLedger, ArcusSpotExecutionLedgerStore,
    ArcusSpotExecutionPhase, ArcusSpotInventory, ArcusSpotKmsConfig, ArcusSpotKmsSigner,
    ArcusSpotLiveExecutor, ArcusSpotLiveExecutorConfig, ArcusSpotLiveTickEventPublisher,
    ArcusSpotLiveTickEventRecord, ArcusSpotLiveTickEventStream, ArcusSpotQuoteUnavailable,
    ArcusSpotRegime, ArcusSpotRiskHaltKind, ArcusSpotRotationPlan, ArcusSpotRotationTrigger,
    ArcusSpotRuntime, ArcusSpotRuntimeCheckpointStore, ArcusSpotRuntimeConfig,
    ArcusSpotRuntimeEvent, ArcusSpotRuntimeMode, ArcusSpotRuntimeState,
};
// Test-only since bot-strategy#853's cutoff rule moved to a shared helper:
// nothing outside the tests names these types here any more.
#[cfg(test)]
use debot::arcus_spot::{
    ArcusSpotBalanceSnapshot, ArcusSpotCorporateActionEvent, ArcusSpotExecutionIntent,
    ArcusSpotHold, ArcusSpotHoldCode, ArcusSpotRiskHalt, ArcusSpotTokenIdentity,
};
use dex_connector::{
    ArcusSpotClient, ArcusSpotConfig, ArcusSpotPair, ArcusSpotRecorder, ArcusSpotRecorderConfig,
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
/// `MaxHoldExit`/`CorporateActionExit` plan supplied here is still
/// risk-reducing and already bounded by `validate_plan_consistent_with_state`
/// (cannot exceed the genuinely open rotated quantity), so only entries are
/// refused.
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

/// How many router rejections in a row `live-tick` will clear on its own
/// before it stops and waits for an operator.
///
/// A rejection is cheap to clear once (nothing reached the chain), but a
/// venue returning 422 to everything is a different situation: clearing
/// forever would rebuild and re-sign a plan every tick against a router
/// that is refusing them, and the operator would never see it.
const MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS: usize = 3;

/// Count the rejections at the tail of `history`, i.e. how many attempts in
/// a row ended rejected with nothing succeeding since.
fn consecutive_tail_rejections(history: &[ArcusSpotExecutionAttempt]) -> usize {
    history
        .iter()
        .rev()
        .take_while(|attempt| attempt.phase == ArcusSpotExecutionPhase::Rejected)
        .count()
}

/// Clear a router rejection that never reached the chain, so the tick can
/// go on to evaluate a fresh observation (bot-strategy#986).
///
/// bot-strategy#898 gave `Rejected` a recovery path, but only a manual one:
/// every tick after a rejection exits 1 on `resume is not allowed in phase
/// Some(Rejected)` until an operator runs `archive-rejected-apply`. On
/// 2026-09-10 that cost seven hours of downtime (bot-strategy#985) for a
/// rejection that carried no transaction at all.
///
/// The narrow case this clears is exactly the one `archive-rejected-report`
/// already decides mechanically: phase `Rejected` with no `tx_hash`, so the
/// router refused before anything existed to reconcile. It is *not* a
/// retry -- the plan is discarded, and the tick that follows builds a new
/// one from a fresh observation, or decides not to trade at all. Everything
/// else (a `tx_hash` that may have reached the chain, `Unknown`,
/// `OperatorHold`, `Failed`) still stops and waits, as does a run of
/// rejections that reaches `MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS`.
fn auto_archive_router_rejection(ledger: &mut ArcusSpotExecutionLedger) -> Result<Option<u64>> {
    let Some(active) = ledger.active.as_ref() else {
        return Ok(None);
    };
    if active.phase != ArcusSpotExecutionPhase::Rejected || active.tx_hash.is_some() {
        return Ok(None);
    }
    let sequence = active.sequence;
    let detail = active.detail.clone().unwrap_or_default();
    // This attempt included: a run that reaches the cap stops here rather
    // than clearing the one that would reach it.
    let run = consecutive_tail_rejections(&ledger.history) + 1;
    if run >= MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS {
        eprintln!(
            "[arcus-rejected] sequence={sequence} run={run} not cleared: \
             {run} router rejections in a row have reached the cap of \
             {MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS}; leaving it for an operator \
             (archive-rejected-report/-apply). detail: {detail}"
        );
        return Ok(None);
    }
    // Delegates the phase/tx_hash invariants rather than restating them, so
    // this path can never be looser than the manual command's.
    ledger.archive_rejected()?;
    eprintln!(
        "[arcus-rejected] sequence={sequence} run={run} cleared automatically: the router \
         refused the submission and no transaction was sent, so there is nothing to reconcile; \
         this tick evaluates a fresh observation and never re-sends the refused plan. \
         detail: {detail}"
    );
    Ok(Some(sequence))
}

async fn resume_active_live_tick_attempt(
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<Option<ArcusSpotExecutionAttempt>> {
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    let lock = ledger_store.acquire_exclusive_lock(&config.runtime_state_path)?;
    let mut ledger = ledger_store.load_or_create(Utc::now())?;
    if auto_archive_router_rejection(&mut ledger)?.is_some() {
        ledger_store.persist(&ledger)?;
        drop(lock);
        // Nothing to resume: the tick continues into an ordinary evaluation.
        return Ok(None);
    }
    let recovery = live_tick_active_recovery_plan(config, &ledger)?;
    drop(lock);

    let Some((plan, plan_config_digest)) = recovery else {
        return Ok(None);
    };
    Ok(Some(
        resume_live_tick_attempt(config, plan, plan_config_digest).await?,
    ))
}

/// Read-only preview for `archive-rejected-apply` (bot-strategy#898):
/// reports the ledger's active attempt and whether it is eligible to be
/// archived, without mutating anything. Run this first, then pass the
/// sequence it reports to `archive-rejected-apply` to confirm you are
/// archiving the attempt you just reviewed and not a different one that
/// appeared in the meantime.
fn archive_rejected_report(config_path: &Path) -> Result<()> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;
    if print_policy_rejected_report_if_needed(&config, "archive-rejected-apply")? {
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

    // Dry-run the real guard on a throwaway clone rather than hand-duplicating
    // archive_rejected()'s phase/tx_hash conditions here: if that method's
    // eligibility rule ever changes, this report (both the status and the
    // reason) follows automatically instead of silently drifting into
    // telling an operator something is "eligible_to_archive" that apply
    // would then refuse (matching how manual_reconcile_report runs the real
    // commit logic against a clone to derive "ready", not a hand-written
    // approximation of it).
    let ineligible_reason = ledger.clone().archive_rejected().err();
    let eligible = ineligible_reason.is_none();
    Ok(serde_json::json!({
        "status": if eligible { "eligible_to_archive" } else { "not_eligible" },
        "sequence": active.sequence,
        "phase": active.phase,
        "tx_hash": active.tx_hash,
        "detail": active.detail,
        "intent": active.intent,
        "reason_if_ineligible": ineligible_reason.map(|error| format!("{error:#}")),
    }))
}

/// Archives the ledger's active attempt once an operator has reviewed it
/// with `archive-rejected-report` and confirmed it is safe to clear
/// (bot-strategy#898). SEQUENCE must match the report's `sequence` exactly,
/// so a concurrent tick that started a new attempt between the report and
/// this call is refused rather than silently archiving the wrong one.
fn archive_rejected_apply(config_path: &Path, sequence: &str) -> Result<()> {
    let sequence: u64 = sequence
        .trim()
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
            "idempotency_key": active.idempotency_key,
            "phase": active.phase,
            "prepared_at": active.prepared_at,
            "dispatched_at": active.dispatched_at,
            "updated_at": active.updated_at,
            "detail": active.detail,
            "intent": active.intent,
        },
        "ledger_path": config.ledger_path,
    }))
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

/// Shared by every `*-report` command that previews a `*-apply` command
/// gated on `auto_execute_policy.json` (`manual-reconcile-report` ->
/// `manual-reconcile-apply`, `archive-rejected-report` ->
/// `archive-rejected-apply`): if CONFIG_YAML would be refused, prints a
/// `policy_rejected` report naming `apply_command_name` and returns `true`
/// so the caller stops here, rather than letting a report ever claim
/// "ready"/"eligible" for a CONFIG_YAML the apply command would actually
/// refuse outright (Codex P1/P2 follow-up, pairtrade#241). Returns `false`
/// if the config passed the check and the caller should proceed.
fn print_policy_rejected_report_if_needed(
    config: &ArcusSpotExecuteOnceConfig,
    apply_command_name: &str,
) -> Result<bool> {
    if let Err(error) = auto_execute_policy_from_admin_file()
        .and_then(|policy| require_config_within_auto_execute_policy(config, &policy))
    {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "policy_rejected",
                "detail": format!(
                    "{apply_command_name} would refuse this CONFIG_YAML before doing anything \
                     else (auto_execute_policy.json check): {error:#}"
                ),
            }))
            .context("failed to serialize Arcus policy-rejected report")?
        );
        return Ok(true);
    }
    Ok(false)
}

fn manual_reconcile_report(
    config_path: &Path,
    events_jsonl_path: &Path,
    expected_sell_amount_raw: &str,
    expected_buy_amount_raw: &str,
) -> Result<()> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;
    if print_policy_rejected_report_if_needed(&config, "manual-reconcile-apply")? {
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
/// The administrator-pinned decimals for `symbol` alone, from
/// `CONFIG_YAML.router.trusted_token_decimals` -- shared by every call site
/// that only needs the pin itself (one that must also cross-check the
/// pinned token address is `trusted_token_decimals_for_address`, below).
/// `caller` names the calling context for the error message (e.g.
/// "manual-reconcile", "acceptance", "live-tick").
fn trusted_token_decimals_for_symbol(
    config: &ArcusSpotExecuteOnceConfig,
    symbol: &str,
    caller: &str,
) -> Result<u32> {
    config
        .router
        .trusted_token_decimals
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
        .map(|(_, decimals)| *decimals)
        .with_context(|| format!("Arcus {caller} has no decimals pin for {symbol}"))
}

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
    trusted_token_decimals_for_symbol(config, symbol, "manual-reconcile")
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
    record_undispatched_plan(config, event, plan, UNSUPPORTED_ROUTE_REASON, None);
    eprintln!(
        "[arcus-route] declined a would-rotate plan: recommended venue {:?} is not one of the \
         validated Arcus/Rialto routes this executor may dispatch; nothing was submitted",
        plan.venue,
    );
    write_live_tick_event(event)
}

/// Hold a would-rotate plan whose venue could not quote it, and exit
/// successfully.
///
/// Mirrors `decline_unsupported_route`: record what the undispatched plan
/// was, say so on stderr, emit the tick's event on stdout, and leave the
/// checkpoint alone so the next observation re-evaluates from the same
/// history. Nothing was signed or submitted, so there is no ledger state to
/// unwind -- only this invocation's own pending-plan file, which describes a
/// dispatch that never happened.
fn hold_on_unavailable_quote(
    config: &ArcusSpotExecuteOnceConfig,
    event: &ArcusSpotRuntimeEvent,
    plan: &ArcusSpotRotationPlan,
    pending_plan_path: &Path,
    pending_plan_bytes: &[u8],
    unavailable: &ArcusSpotQuoteUnavailable,
) -> Result<()> {
    let detail = unavailable.to_string();
    record_undispatched_plan(
        config,
        event,
        plan,
        QUOTE_UNAVAILABLE_REASON,
        Some(detail.as_str()),
    );
    remove_own_pending_plan(pending_plan_path, pending_plan_bytes);
    eprintln!(
        "[arcus-quote] held a would-rotate plan: {detail}; nothing was submitted, and the next \
         tick re-evaluates from a fresh observation",
    );
    write_live_tick_event(event)
}

/// Remove the pending-plan file this invocation wrote -- and only that file.
///
/// The plan is written while the checkpoint lock is held and this hold
/// happens after that lock is dropped, so an overlapping tick may already
/// have replaced the file with a plan of its own. That newer plan may still
/// be dispatched, and it is the only evidence `auto-resume` would have for a
/// `Submitted`-but-unconfirmed swap, so deleting it would destroy exactly
/// what the file exists for. Compare the bytes first and leave anything this
/// invocation did not write alone. A failure to clean up is reported and
/// ignored: an orphaned plan makes `state-verify-exact` disagree with an
/// older backup, which is worth a line of output but is not worth failing an
/// otherwise clean hold over.
fn remove_own_pending_plan(path: &Path, expected: &[u8]) {
    match fs::read(path) {
        Ok(current) if current == expected => {
            if let Err(error) = fs::remove_file(path) {
                eprintln!(
                    "[arcus-quote] failed to remove the undispatched pending plan {}: {error}",
                    path.display(),
                );
            }
        }
        Ok(_) => eprintln!(
            "[arcus-quote] left {} in place: it no longer holds this tick's plan",
            path.display(),
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => eprintln!(
            "[arcus-quote] failed to read the pending plan {} before cleaning it up: {error}",
            path.display(),
        ),
    }
}

/// Why a would-rotate plan was built and then not dispatched.
///
/// Records written before bot-strategy#967 carry no `reason` field at all,
/// and every one of them is an `unsupported_route`: that was the only way a
/// plan reached this log. Offline readers should treat a missing `reason`
/// as `unsupported_route` rather than as unknown.
const UNSUPPORTED_ROUTE_REASON: &str = "unsupported_route";
const QUOTE_UNAVAILABLE_REASON: &str = "quote_unavailable";

fn record_undispatched_plan(
    config: &ArcusSpotExecuteOnceConfig,
    event: &ArcusSpotRuntimeEvent,
    plan: &ArcusSpotRotationPlan,
    reason: &str,
    detail: Option<&str>,
) {
    let record = serde_json::json!({
        "declined_at": event.observed_at,
        "reason": reason,
        "detail": detail,
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
        eprintln!("[arcus-route] failed to record the undispatched {reason} plan: {error:#}");
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

/// The extra recorder row live-tick requests while the checkpointed
/// position is rotated: the exit direction at exactly the open rotation
/// quantity, in raw units of the token that exit sells, using the
/// administrator-pinned `router.trusted_token_decimals` (a missing pin is
/// a configuration error -- the live path could not execute that exit
/// anyway). `None` when there is no checkpoint yet or it is neutral.
fn live_tick_open_quantity_exit_row(
    config: &ArcusSpotExecuteOnceConfig,
) -> Result<Option<dex_connector::ArcusSpotFixedSellAmountRow>> {
    // A cheap peek at the checkpoint file, not the full config-validated
    // `load_or_create`/`load_existing`: this only decides what to request
    // before the network fetch below, the locked load further down is what
    // the actual decision is made against, and a full load here would pay
    // for (and, on a state-preserving config change, log) the same drift
    // comparison twice per tick for no benefit.
    let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    let Some((regime, rotated_quantity)) = store.peek_regime_and_rotated_quantity()? else {
        return Ok(None);
    };
    let sell_symbol = match (regime, rotated_quantity) {
        (_, None) | (ArcusSpotRegime::Neutral, _) => return Ok(None),
        (ArcusSpotRegime::RotatedAToB, Some(_)) => &config.runtime.pair.buy_symbol,
        (ArcusSpotRegime::RotatedBToA, Some(_)) => &config.runtime.pair.sell_symbol,
    };
    let decimals = trusted_token_decimals_for_symbol(config, sell_symbol, "live-tick")?;
    open_exit_fixed_sell_amount_row_for(regime, rotated_quantity, &config.runtime.pair, decimals)
        .map_err(anyhow::Error::msg)
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

/// Start a fresh signal window under a state-invalidating config change,
/// keeping the durable event stream contiguous (bot-strategy#903).
///
/// The documented procedure used to be "remove the checkpoint file and let
/// the next tick start a fresh window". That predates the hash-chained
/// event stream (#825): a fresh checkpoint numbers its first event 1, the
/// stream tail is at N, and `append` refuses the discontinuity -- so the
/// tick exits non-zero *after* staging a pending event, and every later
/// tick then refuses that incompatible pending event too. Recovering it
/// took hand-editing executor state (the #902 pair change did exactly
/// that), which is the one thing the rollback runbook says never to do.
///
/// So this is the sanctioned operation instead: same administrator policy
/// digest gate and same exclusive lock as `clear-risk-halt`, refusing
/// unless the bot is genuinely idle, and writing a fresh checkpoint whose
/// sequence continues from the verified stream tail. The stream is neither
/// truncated nor renumbered; the event's own `pair`/`mode` fields mark the
/// boundary, and the replaced checkpoint is copied aside first.
fn reset_runtime_window(config_path: &Path) -> Result<serde_json::Value> {
    let config_bytes = read_private_regular_file(config_path, "config")?;
    let config = parse_config(&config_bytes, config_path)?;
    // A reset re-arms exactly the dispatch path this gate governs, under a
    // config the checkpoint itself can no longer vouch for, so it is held
    // to the same administrator approval as clear-risk-halt.
    let policy = auto_execute_policy_from_admin_file()?;
    require_config_within_auto_execute_policy(&config, &policy)?;

    // Pass the already-parsed, already-approved config object -- not
    // config_path -- so the committing half cannot re-read CONFIG_YAML from
    // disk a second time (same TOCTOU reasoning as archive-rejected-apply).
    commit_runtime_window_reset(&config)
}

/// Everything `reset-window` does once its administrator gate has passed.
fn commit_runtime_window_reset(config: &ArcusSpotExecuteOnceConfig) -> Result<serde_json::Value> {
    let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
    let ledger_store = ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone());
    // Same exclusive lock a dispatching tick takes: this read-modify-write
    // must not interleave with one committing a fill.
    let _lock = ledger_store.acquire_existing_exclusive_lock(&config.runtime_state_path)?;

    let publisher = live_tick_event_publisher(config)?;
    match fs::symlink_metadata(publisher.pending_path()) {
        Ok(_) => bail!(
            "Arcus pending durable event {} must be recovered by a live-tick run before the \
             window can be reset -- resetting around it would strand an event the stream still \
             expects",
            publisher.pending_path().display()
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to inspect Arcus pending durable event"),
    }

    let ledger = ledger_store.load_existing()?;
    if let Some(active) = &ledger.active {
        bail!(
            "Arcus execution attempt {} is still active in phase {:?}; resolve it (auto-resume, \
             archive-rejected-apply, or manual-reconcile-apply) before resetting the window",
            active.sequence,
            active.phase,
        );
    }
    let pending_plan_path = live_tick_pending_plan_path(config)?;
    match fs::symlink_metadata(&pending_plan_path) {
        Ok(_) => bail!(
            "Arcus live-tick pending plan {} still exists; it is the evidence of a dispatch this \
             reset would orphan",
            pending_plan_path.display()
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("failed to inspect Arcus live-tick pending plan"),
    }

    // Read the checkpoint without comparing it to this config: under a
    // state-invalidating change load_existing refuses outright, and that
    // refusal is the very situation being resolved here.
    // Everything this command promises to check -- flat regime, no open
    // rotation, no engaged halt, and a config change worth resetting for --
    // is read out of the checkpoint. Without one, none of them can be
    // established, so the reset would be an unconditional re-anchoring of
    // the loss baselines rather than a reset of anything.
    //
    // An earlier revision allowed it while the execution ledger showed no
    // fund-moving attempt, on the theory that a bot which never swapped can
    // hold no position. That covers positions and nothing else: the risk
    // marks are computed from prices against the baseline inventory, so
    // daily and cumulative loss accrue -- and a halt can engage -- with
    // zero swaps ever dispatched. "Never traded" is therefore not evidence
    // that a reset is safe, and the missing-checkpoint case is refused
    // outright (Codex P1 follow-up, bot-strategy#903).
    let previous = match store.peek_summary()? {
        Some(previous) => previous,
        None => bail!(
            "Arcus runtime checkpoint {} is missing, so none of this command's preconditions \
             can be checked: the regime, any open rotation, any engaged risk halt and the \
             config a fresh window would differ from all live in it. Resetting would only \
             re-anchor the initial-equity and buy-and-hold loss baselines the cumulative halt \
             is measured against, which is what this command must never be a way to do. Put \
             the checkpoint back first, from the `.pre-reset` copy beside it or from a \
             state-backup directory, and make sure it is the one matching the stream's current \
             tail. Do not run a live-tick to rebuild it: against a non-empty stream it stages a \
             sequence-1 event and checkpoints it before the append rejects the discontinuity, \
             leaving a pending event no later tick can recover -- the wedged state this command \
             exists to avoid",
            config.runtime_state_path.display(),
        ),
    };
    {
        if previous.regime != ArcusSpotRegime::Neutral || previous.rotated_quantity.is_some() {
            bail!(
                "Arcus runtime checkpoint holds an open rotation (regime {:?}, rotated quantity \
                 {}); exit it under the config it was entered under before resetting the window -- \
                 a fresh window has no record of what is still held",
                previous.regime,
                previous
                    .rotated_quantity
                    .map(|quantity| quantity.normalize().to_string())
                    .unwrap_or_else(|| "none".to_string()),
            );
        }
        if let Some(halt) = &previous.risk_halt {
            bail!(
                "Arcus runtime checkpoint carries a {:?} risk halt engaged at {}; a reset would \
                 silently discard it. Decide it deliberately with clear-risk-halt first \
                 (bot-strategy#813)",
                halt.kind,
                halt.engaged_at,
            );
        }
    }

    // A reset is for a change that already invalidated the stored state.
    // With the config unchanged it discards an accumulated signal window
    // and re-anchors the risk baselines for nothing -- and re-anchoring is
    // the part that matters: `initial_equity_usd` and the buy-and-hold
    // basket are re-marked on the next tick, so cumulative-loss accounting
    // starts over. Repeated before the limit engages, that is a way to
    // never reach the cumulative halt at all, available to anyone who can
    // invoke the executor with the already-approved production config. The
    // approval gate authorises *this config*, not an unlimited number of
    // baseline erasures under it (Codex P1 follow-up, bot-strategy#903).
    let changed = store
        .state_invalidating_drift(&config.runtime)?
        .unwrap_or_default();
    {
        if changed.is_empty() {
            bail!(
                "Arcus runtime checkpoint {} was written under a config whose state-invalidating \
                 fields (mode, chain_id, pair, initial_inventory, signal_window_samples) all \
                 match the one supplied, so there is nothing here for a fresh window to be \
                 about. Resetting anyway would only discard the accumulated signal window and \
                 restart the initial-equity and buy-and-hold loss baselines the cumulative halt \
                 is measured against. Deploy the changed CONFIG_YAML first",
                config.runtime_state_path.display(),
            );
        }
    }

    let tail = publisher.stream().latest_committed()?;
    let tail_sequence = tail.map(|(sequence, _)| sequence).unwrap_or(0);
    // Building a fresh runtime takes its inventory from
    // `config.initial_inventory` -- the figure declared at funding -- while
    // confirmed fills have been permanently adjusting `state.inventory`
    // ever since. So a reset whose only state-invalidating change is
    // `signal_window_samples` (the one field of the five that leaves the
    // inventory's meaning intact) would silently roll realized trading
    // deltas back to the declaration, and every later size and
    // inventory-floor check would then reason about balances the wallet
    // does not have.
    //
    // This does not guess which figure is right. The operator declares it:
    // if the holdings have moved, `initial_inventory` has to be updated to
    // the reconciled figure before the reset, which is also the change that
    // makes the reset's re-anchoring of the risk baselines correct rather
    // than arbitrary. A reset that *does* change `initial_inventory` is the
    // re-funding case and is left alone -- there the declaration is meant
    // to differ from what was held, and it is a deliberate act rather than
    // a silent overwrite (Codex P1 follow-up, bot-strategy#903).
    if !changed.contains(&"initial_inventory")
        && previous.inventory != config.runtime.initial_inventory
    {
        bail!(
            "Arcus runtime checkpoint holds inventory token_a={} token_b={} but this config \
             declares initial_inventory token_a={} token_b={}; a reset builds the fresh runtime \
             from the declared figure, so it would discard the difference and size later swaps \
             against balances the wallet does not have. Set initial_inventory to the reconciled \
             holdings first",
            previous.inventory.token_a.normalize(),
            previous.inventory.token_b.normalize(),
            config.runtime.initial_inventory.token_a.normalize(),
            config.runtime.initial_inventory.token_b.normalize(),
        );
    }

    // The two must be exactly in step. Ahead of the stream was already
    // refused; behind it is the more dangerous direction and was not,
    // because the checkpoint reads as valid while the events it has not
    // seen may hold a completed entry fill or an engaged halt whose
    // attempt the ledger has since archived -- so every other guard here
    // passes on stale state, and the reset replaces the authoritative
    // record with a neutral checkpoint at the tail. That is reachable by
    // restoring an older `.pre-reset` copy or an older state-backup, which
    // is exactly what this command's own refusals tell an operator to do
    // (Codex P1 follow-up, bot-strategy#903).
    if previous.sequence != tail_sequence {
        bail!(
            "Arcus runtime checkpoint is at sequence {} but the event stream tail is {}; the two \
             must be in step before a reset. {} Reconcile them with repair-report first -- a \
             checkpoint that has not seen every committed event cannot show whether the bot is \
             idle, and a reset would replace that record rather than continue it",
            previous.sequence,
            tail_sequence,
            if previous.sequence > tail_sequence {
                "The stream is behind its own checkpoint, which is a recovery case, not a reset."
            } else {
                "The checkpoint has not seen the stream's later events, which may hold a fill or \
                 a halt this reset would discard."
            },
        );
    }

    // The handled corporate-action record outlives the window: the
    // completed declarations may still be in the config, and a fresh state
    // that had forgotten them would treat each as unhandled on the next
    // tick and resume it again -- clearing the new window and replacing the
    // newly declared initial_inventory with the old post_event_inventory
    // (Codex P1, pairtrade#309).
    // An open corporate-action window is state this reset would silently
    // discard: the fresh runtime has no progress record, so the next tick
    // trades without the fail-closed guard while the inventory may still be
    // in pre-event units. Refused -- unless the change *is* the instrument
    // being replaced (`pair`), which is the documented recovery for a
    // relisted ticker and makes the old window moot (Codex P1, pairtrade#309).
    if let Some(event_id) = &previous.corporate_action_event_id {
        // "The instrument is being replaced" means the affected symbol is
        // gone from the new pair -- not merely that `pair` changed. Swapping
        // the legs, or changing only the other leg, keeps the affected token
        // and its unreconciled pre-event inventory in play (Codex P1,
        // pairtrade#309). A record that predates `symbols` cannot say what
        // it is about and is refused too.
        let new_pair = [
            config.runtime.pair.sell_symbol.as_str(),
            config.runtime.pair.buy_symbol.as_str(),
        ];
        let affected_still_traded = previous.corporate_action_symbols.is_empty()
            || previous
                .corporate_action_symbols
                .iter()
                .any(|symbol| new_pair.iter().any(|leg| leg.eq_ignore_ascii_case(symbol)));
        if !changed.contains(&"pair") || affected_still_traded {
            bail!(
                "Arcus runtime checkpoint is inside corporate action {event_id}'s window \
                 (symbols {:?}); a reset would discard that progress and the next tick would \
                 trade without the guard. Resolve the window first (restore its declaration \
                 and let it resume, or reconcile per the runbook), or replace the affected \
                 instrument with a `pair` that no longer names it",
                previous.corporate_action_symbols,
            );
        }
    }
    let runtime =
        ArcusSpotRuntime::new_continuing_event_sequence(config.runtime.clone(), tail_sequence)
            .map_err(anyhow::Error::msg)
            .context("the configuration being reset to is itself invalid")?
            .with_handled_corporate_actions(
                previous.handled_corporate_action_ids.clone(),
                previous.handled_corporate_action_fingerprints.clone(),
                previous.last_observation_at,
            );

    // The evidence sidecar describes the state being discarded, so it is
    // retired before the checkpoint is replaced: the reverse order could
    // leave a fresh checkpoint beside evidence that contradicts it, which
    // state-backup refuses to capture.
    let mut retired = Vec::new();
    let evidence_path = live_tick_observation_evidence_path(config)?;
    if let Some(path) = retire_replaced_state_file(&evidence_path, "pre-reset")? {
        retired.push(path.display().to_string());
    }
    {
        // Copied, not moved: a rename followed by a failed persist would
        // leave the state directory with no checkpoint at all, and the next
        // tick would then start a fresh window at sequence 1 -- exactly the
        // discontinuity this command exists to prevent.
        let checkpoint_bytes =
            read_private_regular_file(&config.runtime_state_path, "Arcus runtime checkpoint")?;
        retired.push(
            copy_replaced_state_file(&config.runtime_state_path, &checkpoint_bytes, "pre-reset")?
                .display()
                .to_string(),
        );
    }
    store.persist(&runtime)?;

    eprintln!(
        "[arcus-reset] fresh window for {} continuing the event stream at sequence {}; take a \
         fresh state-backup, as backups from before this no longer verify",
        pair_label(&config.runtime.pair),
        tail_sequence,
    );
    Ok(serde_json::json!({
        "reset": {
            "pair": pair_label(&config.runtime.pair),
            "mode": config.runtime.mode,
            "checkpoint_sequence": tail_sequence,
            "next_event_sequence": tail_sequence.saturating_add(1),
        },
        "previous_checkpoint": serde_json::json!({
            "pair": pair_label(&previous.pair),
            "mode": previous.mode,
            "sequence": previous.sequence,
            "relative_log_price_samples": previous.relative_log_price_samples,
        }),
        "event_stream": {
            "directory": publisher.stream().directory().display().to_string(),
            "tail_sequence": tail_sequence,
            "tail_observed_at": tail.map(|(_, observed_at)| observed_at),
        },
        "runtime_state_path": config.runtime_state_path,
        "retired": retired,
    }))
}

/// Move a state file being replaced aside, under a suffixed name in its own
/// directory, and report where it went. `None` if there was nothing there.
///
/// Deliberately kept rather than deleted: this and `copy_replaced_state_file`
/// leave the only trace of the replaced state that survives an operator who
/// skipped the `state-backup` the runbook asks for. Neither is a backup --
/// no manifest, nothing verifies them -- so nothing reads them back; they
/// exist to be inspected.
fn retire_replaced_state_file(path: &Path, reason: &str) -> Result<Option<PathBuf>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => bail!(
            "Arcus state file {} must be a regular non-symlink file",
            path.display()
        ),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()))
        }
    }
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent directory", path.display()))?;
    let destination = parent.join(replaced_state_file_name(path, reason)?);
    fs::rename(path, &destination).with_context(|| {
        format!(
            "failed to move {} aside to {}",
            path.display(),
            destination.display()
        )
    })?;
    File::open(parent)?.sync_all()?;
    Ok(Some(destination))
}

/// The same suffixed destination as `retire_replaced_state_file`, written
/// as a private copy of `bytes` while the original stays in place.
fn copy_replaced_state_file(path: &Path, bytes: &[u8], reason: &str) -> Result<PathBuf> {
    let parent = path
        .parent()
        .with_context(|| format!("{} has no parent directory", path.display()))?;
    let destination = parent.join(replaced_state_file_name(path, reason)?);
    write_new_private_file(&destination, bytes)?;
    File::open(parent)?.sync_all()?;
    Ok(destination)
}

/// `<name>.<reason>.<nanoseconds>`: unique per invocation, sorts by time,
/// and stays inside the state directory the executor already owns.
fn replaced_state_file_name(path: &Path, reason: &str) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("{} has no valid file name", path.display()))?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    Ok(format!("{name}.{reason}.{stamp}"))
}

/// Symbols as the durable event's own `pair` field spells them.
fn pair_label(pair: &ArcusSpotPair) -> String {
    format!("{}/{}", pair.sell_symbol, pair.buy_symbol)
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

/// The single equity number a corporate-action resume writes to all three
/// marks, re-derived from the reconciled holding and this tick's reference
/// prices. Requiring the marks to *be* it is what keeps the resume from
/// being an exemption: a checkpoint that moved them anywhere else fails.
fn require_corporate_action_rebase_marks(
    current: &ArcusSpotRuntimeState,
    inventory: ArcusSpotInventory,
) -> Result<Decimal> {
    let (price_a, price_b) = current
        .last_token_a_reference_price_usd
        .zip(current.last_token_b_reference_price_usd)
        .context("resumed without the reference marks it valued the holding at")?;
    let equity = inventory
        .checked_value_usd(price_a, price_b)
        .context("reconciled holding valuation exceeds Decimal range")?;
    if current.initial_equity_usd != Some(equity)
        || current.daily_baseline_equity_usd != Some(equity)
        || current.last_equity_usd != Some(equity)
    {
        bail!(
            "cumulative, daily and last equity marks must all be the reconciled holding priced at \
             this tick ({equity}); found {:?} / {:?} / {:?}",
            current.initial_equity_usd,
            current.daily_baseline_equity_usd,
            current.last_equity_usd,
        );
    }
    Ok(equity)
}

/// True when the checkpoint sits inside a declared corporate action's
/// stale-unit phase: past its `effective_at`, resume not yet committed.
///
/// The runtime deliberately declines to engage a loss halt there -- the
/// venue quotes the post-event instrument while the tracked inventory and
/// its buy-and-hold basket are still in pre-event units, so the gap between
/// them is priced in the wrong denomination and a reverse split multiplies
/// an otherwise sub-limit shortfall (see `corporate_action_units_are_stale`
/// in the runtime). This verifier re-derives that same loss, so without the
/// matching exception it demands a halt the runtime was right not to engage
/// and rejects every valid backup spanning such a tick.
///
/// The stamp alone is not enough to earn the exception: the progress record
/// must name an event the *approved config* declares and that the runtime
/// has not yet handled, so a checkpoint cannot mint the exemption for
/// itself (bot-strategy#853).
fn corporate_action_units_are_stale(
    config: &ArcusSpotRuntimeConfig,
    current: &ArcusSpotRuntimeState,
) -> bool {
    // A distinct later action reusing a handled id never gets a progress
    // record, yet the runtime treats its units as stale from its cutoff and
    // declines to engage a halt there. Without the same case here, a valid
    // checkpoint from that phase is rejected for "omitting" the halt (Codex
    // P2, pairtrade#309).
    // Judged on the price clock, like the progress-record path: on the tick
    // that first crosses the cutoff the runtime may have engaged a genuine
    // halt from pre-cutoff prices, and classifying the units stale from the
    // later observation watermark would let that halt be removed and still
    // verify (Codex P1, pairtrade#309). A checkpoint predating the field
    // keeps the observation watermark.
    let priced_at = current
        .last_reference_price_at
        .or(current.last_observation_at);
    if let Some(priced_at) = priced_at {
        let reused_and_effective = config.corporate_actions.iter().any(|event| {
            priced_at >= event.effective_at
                && current
                    .handled_corporate_action_ids
                    .iter()
                    .position(|handled| handled.eq_ignore_ascii_case(&event.event_id))
                    // An empty or missing slot is an unresolved legacy
                    // record, which the runtime classifies as a reused label
                    // -- so it must read the same way here (Codex P2,
                    // pairtrade#309).
                    .is_some_and(|index| {
                        current
                            .handled_corporate_action_fingerprints
                            .get(index)
                            .is_none_or(|recorded| *recorded != event.fingerprint())
                    })
        });
        if reused_and_effective {
            return true;
        }
    }
    let Some(progress) = current.corporate_action.as_ref() else {
        return false;
    };
    // Identity drift makes the mark mixed-unit even before the cutoff -- old
    // quantity, replacement's price -- and the runtime declines to halt on
    // it, so a checkpoint from that tick must not be required to carry one
    // (Codex P1, pairtrade#309).
    let declared = config
        .corporate_actions
        .iter()
        .find(|event| {
            (!progress.fingerprint.is_empty() && progress.fingerprint == event.fingerprint())
                || event.event_id.eq_ignore_ascii_case(&progress.event_id)
        })
        .cloned();
    if let Some(event) = declared.as_ref() {
        let drifted = event.symbols.iter().any(|symbol| {
            let (pinned, observed) = if symbol.eq_ignore_ascii_case(&config.pair.sell_symbol) {
                (
                    progress.pre_event_token_a.as_ref(),
                    current.last_token_a_identity.as_ref(),
                )
            } else {
                (
                    progress.pre_event_token_b.as_ref(),
                    current.last_token_b_identity.as_ref(),
                )
            };
            match (pinned, observed) {
                // The runtime's rule: address case-insensitive, decimals
                // exact, symbol irrelevant. A derived `!=` disagreed with it
                // on checksum casing (verifier says drift, runtime halts)
                // and on a missing pin (runtime suppresses, verifier demanded
                // a halt) -- both let a rollback drop a genuine sticky halt
                // (Codex P1, pairtrade#309).
                (Some(pinned), Some(observed)) => !pinned.same_contract(observed),
                // No pin: the runtime cannot verify the mark and does not
                // engage, so neither may this demand one.
                (None, _) => true,
                (Some(_), None) => false,
            }
        });
        if drifted {
            return true;
        }
    }
    let Some(stamped_at) = progress.history_invalidated_at else {
        return false;
    };
    // The stamp is written with the evaluation clock, but the runtime
    // decides halt suppression from the *price* clock -- so on the tick that
    // first crosses the cutoff a genuine halt may have engaged from
    // pre-cutoff prices. Granting the exemption on the stamp alone would let
    // that halt be removed and still verify. Re-derive the decision from the
    // clock the runtime actually used (Codex P1, pairtrade#309); a
    // checkpoint predating the field cannot say, and keeps the old
    // stamp-only reading.
    if let Some(priced_at) = current.last_reference_price_at {
        // Shared with the runtime's own stale-unit predicate: an amendment
        // that moved the cutoff earlier is the one that counts, and the two
        // must not disagree about a window that has been amended (Codex P1,
        // pairtrade#309).
        let cutoff = corporate_action_effective_cutoff(progress, config);
        if cutoff.is_some_and(|cutoff| priced_at < cutoff) {
            return false;
        }
    }
    if current
        .handled_corporate_action_ids
        .iter()
        .any(|handled| handled.eq_ignore_ascii_case(&progress.event_id))
    {
        return false;
    }
    // Declared, or an orphaned record that carries its own cutoff and was
    // stamped at or after it -- the runtime stamps those itself when the
    // declaration is gone (Codex P1, pairtrade#309). A stamp with neither
    // is not something the runtime writes.
    config
        .corporate_actions
        .iter()
        .any(|event| event.event_id.eq_ignore_ascii_case(&progress.event_id))
        || progress
            .effective_at
            .is_some_and(|effective_at| stamped_at >= effective_at)
}

fn require_risk_state_continuity(
    config: &ArcusSpotRuntimeConfig,
    baseline: &ArcusSpotRuntimeState,
    current: &ArcusSpotRuntimeState,
    sequence_advance: u64,
    acceptance_not_before: DateTime<Utc>,
    acceptance_not_after: DateTime<Utc>,
    corporate_action: &ArcusSpotCorporateActionContinuity,
) -> Result<()> {
    // A resume re-anchors both risk baskets and the cumulative baseline onto
    // the reconciled holding, because they are buy-and-hold counterfactuals
    // and the old basket no longer exists (bot-strategy#813/#853). The three
    // equity marks it writes are all the same number -- that holding priced
    // at this tick's reference marks -- so rather than exempting the fields,
    // this re-derives the number and requires them to be it.
    let rebased_equity = match corporate_action.resumed_inventory {
        Some(inventory) => Some(
            require_corporate_action_rebase_marks(current, inventory)
                .context("Arcus corporate-action resume marks are inconsistent")?,
        ),
        None => None,
    };
    let baseline_daily = daily_risk_baseline(baseline, "backup")?;
    if rebased_equity.is_none()
        && baseline.initial_equity_usd.is_some()
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
                if current_equity != baseline_equity && rebased_equity != Some(current_equity) {
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
                // The one phase in which the runtime is *supposed* to omit
                // it. Scoped to that phase only: an unexpected halt is
                // still rejected above, and once the resume commits the
                // baskets are re-anchored and the ordinary expectation
                // applies again (Codex P1, pairtrade#309).
                if !corporate_action_units_are_stale(config, current) {
                    bail!(
                        "Arcus runtime omitted a newly triggered loss halt across restart/rollback"
                    )
                }
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
        let decimals = trusted_token_decimals_for_symbol(config, symbol, "acceptance")?;
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
) -> Result<(Decimal, Decimal, DateTime<Utc>)> {
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
    // A settlement transaction can refund part of the signed sell amount
    // inside the same transaction, so the wallet may part with less than
    // was signed for (bot-strategy#979). More than the signed amount
    // remains impossible under Permit2 and stays fatal.
    let signed_sell_raw = parse_raw_amount("intent sell amount", &attempt.intent.sell_amount_raw)?;
    if sold_raw > signed_sell_raw {
        bail!("Arcus acceptance sell delta exceeds its intent");
    }
    // What that shortfall is allowed to be is not this function's judgement
    // call: it must equal the settled input the reconciliation read derived
    // from the transaction's own transfers, exactly as the live commit path
    // requires (Codex P1 follow-up, bot-strategy#979). Accepting any delta
    // below the signed amount would let altered post-balances -- with a
    // runtime state altered to match -- pass continuity verification.
    //
    // An attempt reconciled before that evidence existed keeps the former
    // exact-equality invariant instead: those binaries could not have
    // reconciled a refund at all, so a short delta there is unexplained,
    // not merely unproven.
    match attempt.settled_sell_amount_raw.as_deref() {
        Some(settled_sell_raw) => {
            let settled_sell_raw = parse_raw_amount("settled sell amount", settled_sell_raw)?;
            if settled_sell_raw > signed_sell_raw {
                bail!("Arcus acceptance settled swap input exceeds its intent");
            }
            if sold_raw != settled_sell_raw {
                bail!("Arcus acceptance sell delta does not match its settled swap input");
            }
        }
        None => {
            if sold_raw != signed_sell_raw {
                bail!("Arcus acceptance sell delta does not match its intent");
            }
        }
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
    // Scaled from what actually left the wallet, the same way the buy side
    // is scaled from what actually arrived: committing the plan's own
    // sell_quantity would book inventory the wallet still holds whenever the
    // settlement refunded part of the signed amount (bot-strategy#979).
    let planned_sell_raw = parse_raw_amount("plan sell amount", &plan.sell_amount_raw)?;
    if planned_sell_raw.is_zero() || plan.sell_quantity <= Decimal::ZERO {
        bail!("Arcus acceptance pending plan has an invalid sell quantity");
    }
    let sold_decimal = Decimal::from_str(&sold_raw.to_string())
        .context("Arcus acceptance sell amount exceeds Decimal range")?;
    let planned_sell_decimal = Decimal::from_str(&planned_sell_raw.to_string())
        .context("Arcus acceptance planned sell amount exceeds Decimal range")?;
    let actual_sell_quantity = plan
        .sell_quantity
        .checked_mul(sold_decimal)
        .and_then(|value| value.checked_div(planned_sell_decimal))
        .context("Arcus acceptance sell quantity exceeds Decimal range")?;
    if actual_sell_quantity <= Decimal::ZERO || actual_buy_quantity <= Decimal::ZERO {
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
    Ok((actual_sell_quantity, actual_buy_quantity, filled_at))
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
    let sell_decimals = trusted_token_decimals_for_symbol(config, &plan.sell_symbol, "acceptance")?;
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
    corporate_action: &ArcusSpotCorporateActionContinuity,
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
            // A corporate-action resume moves inventory with no swap and no
            // ledger attempt -- that is the whole point of it -- so the one
            // transition the approved config declares is compared against
            // the reconciled holding instead of against the backup. The
            // replay directly above has already reproduced this same state
            // from the recorder evidence; this keeps the independent check
            // meaningful rather than skipping it (bot-strategy#853).
            match corporate_action.resumed_inventory {
                Some(inventory) => {
                    let mut expected = baseline_runtime.clone();
                    expected.inventory = inventory;
                    if !position_state_matches(&expected, current_runtime) {
                        bail!(
                            "Arcus position state changed beyond the declared corporate-action \
                             resume"
                        );
                    }
                }
                None => {
                    if !position_state_matches(baseline_runtime, current_runtime) {
                        bail!(
                            "Arcus position state changed without a reconciled acceptance attempt"
                        );
                    }
                }
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
            let (actual_sell_quantity, actual_buy_quantity, filled_at) =
                reconciled_fill_for_continuity(config, &plan, attempt, evidence.evaluation_time)?;
            replayed_runtime
                .validate_plan_consistent_with_state(&plan, evidence.evaluation_time)
                .map_err(anyhow::Error::msg)
                .context("Arcus acceptance plan is inconsistent with the backup position")?;
            let applied = replayed_runtime
                .apply_confirmed_live_fill_once(
                    &plan,
                    actual_sell_quantity,
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

/// A corporate-action transition (bot-strategy#853) that the approved config
/// itself authorizes, for the continuity checks below.
///
/// A resume and the history discard that precedes it both change state with
/// no ledger attempt and no swap: inventory, both risk baskets, the
/// cumulative equity baseline and the whole signal window move on an
/// ordinary observation tick. Every one of those is, correctly, a violation
/// for any *other* reason, so rather than loosening the rules this derives
/// the one transition the config declares and hands the checks its exact
/// shape. Nothing here trusts the current checkpoint's say-so: the event has
/// to be declared, with a reconciled holding, and every field the resume
/// touches has to have landed on the value that holding implies.
#[derive(Debug, Clone, Default)]
struct ArcusSpotCorporateActionContinuity {
    /// The pre-event signal window was discarded at a declared
    /// `effective_at`, or by the resume that followed it.
    history_discarded: bool,
    /// The runtime resumed onto the operator's reconciled holding.
    resumed_inventory: Option<ArcusSpotInventory>,
}

fn corporate_action_continuity(
    config: &ArcusSpotRuntimeConfig,
    baseline: &ArcusSpotRuntimeState,
    current: &ArcusSpotRuntimeState,
    sequence_advance: u64,
    verified_at: DateTime<Utc>,
) -> Result<ArcusSpotCorporateActionContinuity> {
    let mut authorized = ArcusSpotCorporateActionContinuity::default();

    // A legacy record is resolved once, at load, from the observation
    // watermark. A backup taken before that resolution and the state after
    // it therefore differ in slots neither side changed deliberately, so the
    // append-only prefix -- and only that comparison -- is made on copies
    // resolved with the *baseline's* watermark: the same information, one
    // answer. Every other check below still reads what the runtime actually
    // wrote (Codex P1, pairtrade#309).
    let (resolved_baseline, resolved_current) = {
        let mut resolved_baseline = baseline.clone();
        let mut resolved_current = current.clone();
        let watermark = baseline.last_observation_at;
        resolve_handled_corporate_action_fingerprints(&mut resolved_baseline, config, watermark);
        resolve_handled_corporate_action_fingerprints(&mut resolved_current, config, watermark);
        (resolved_baseline, resolved_current)
    };

    if current.handled_corporate_action_ids.len() < baseline.handled_corporate_action_ids.len()
        || current.handled_corporate_action_ids[..baseline.handled_corporate_action_ids.len()]
            != baseline.handled_corporate_action_ids[..]
    {
        bail!(
            "Arcus runtime lost or reordered its handled corporate actions across restart/rollback"
        );
    }
    // The fingerprints are the identity half of the same record and are
    // append-only in exactly the same way: dropping one would let a renamed
    // entry be applied again after a restore (Codex P1, pairtrade#309).
    let fingerprints_before = baseline.handled_corporate_action_fingerprints.len();
    // Resolution is a first-load event. Once the backup says it happened,
    // the raw slots are what must match: comparing resolved copies would
    // otherwise let a candidate blank a slot, claim the marker, and look
    // identical -- after which `from_state` skips the backfill and the
    // declaration silently becomes a reused id (Codex P1, pairtrade#309).
    let (compare_baseline, compare_current) = if baseline.handled_corporate_actions_resolved {
        (baseline, current)
    } else {
        (&resolved_baseline, &resolved_current)
    };
    if compare_current.handled_corporate_action_fingerprints.len()
        < compare_baseline.handled_corporate_action_fingerprints.len()
        || compare_current.handled_corporate_action_fingerprints
            [..compare_baseline.handled_corporate_action_fingerprints.len()]
            != compare_baseline.handled_corporate_action_fingerprints[..]
    {
        bail!(
            "Arcus runtime lost or reordered its handled corporate-action fingerprints across \
             restart/rollback"
        );
    }
    // Raw slicing below: a checkpoint whose fingerprint vector is shorter
    // than its id vector is exactly the adversarially-shaped state this
    // function exists to reject, so say so rather than panicking on the
    // index (independent review, pairtrade#309).
    if current.handled_corporate_action_fingerprints.len() < fingerprints_before
        || current.handled_corporate_action_fingerprints.len()
            != current.handled_corporate_action_ids.len()
    {
        bail!(
            "Arcus runtime lost or reordered its handled corporate-action fingerprints across \
             restart/rollback"
        );
    }
    let resumed =
        &current.handled_corporate_action_ids[baseline.handled_corporate_action_ids.len()..];
    let resumed_fingerprints =
        &current.handled_corporate_action_fingerprints[fingerprints_before..];
    match resumed {
        [] => {
            if !resumed_fingerprints.is_empty() {
                bail!("Arcus runtime recorded a corporate-action fingerprint without a resume");
            }
        }
        [event_id] => {
            if sequence_advance != 1 {
                bail!("Arcus corporate action {event_id} resumed without a single new observation");
            }
            // By id, or by the fingerprint recorded beside it: a completed
            // entry the operator renamed afterwards is the same event, which
            // is the whole point of the fingerprint (Codex P2,
            // pairtrade#309).
            // The appended fingerprint is the *last* of the newly added
            // entries: a legacy record (ids with no fingerprints) is padded
            // with empty strings first, so `first()` would read padding
            // (Codex P2, pairtrade#309).
            let resumed_fingerprint = resumed_fingerprints.last().filter(|it| !it.is_empty());
            let event = config
                .corporate_actions
                .iter()
                .find(|event| {
                    event.event_id.eq_ignore_ascii_case(event_id)
                        || resumed_fingerprint
                            .is_some_and(|recorded| *recorded == event.fingerprint())
                })
                .with_context(|| {
                    format!("Arcus runtime resumed corporate action {event_id}, which the approved config does not declare")
                })?;
            let inventory = event.post_event_inventory.with_context(|| {
                format!("Arcus corporate action {event_id} resumed without a reconciled post_event_inventory")
            })?;
            if current.inventory != inventory
                || current.initial_baseline_inventory != Some(inventory)
                || current.daily_baseline_inventory != Some(inventory)
            {
                bail!("Arcus corporate action {event_id} did not land on its reconciled holding");
            }
            if current.regime != ArcusSpotRegime::Neutral
                || current.rotated_quantity.is_some()
                || current.last_rotation_at.is_some()
            {
                bail!("Arcus corporate action {event_id} resumed with a rotation still open");
            }
            if current.corporate_action.is_some() {
                bail!("Arcus corporate action {event_id} resumed without clearing its progress");
            }
            // The pair is appended in step. A legacy record (ids without
            // fingerprints) is padded with empty entries first so the new
            // fingerprint lands beside its own id, never beside an older one
            // (Codex P1, pairtrade#309).
            let ids = current.handled_corporate_action_ids.len();
            let fingerprints = &current.handled_corporate_action_fingerprints;
            let aligned = fingerprints.len() == ids
                && fingerprints[fingerprints_before..ids - 1]
                    .iter()
                    .all(String::is_empty)
                && fingerprints[ids - 1] == event.fingerprint();
            if !aligned {
                bail!(
                    "Arcus corporate action {event_id} resumed without recording the fingerprint \
                     of the declared event beside its id"
                );
            }
            authorized.resumed_inventory = Some(inventory);
            authorized.history_discarded = true;
        }
        _ => bail!("Arcus runtime resumed more than one corporate action in a single observation"),
    }

    // The progress record itself may only change the way an observation
    // changes it. Anything else -- above all a pinned pre-event identity
    // replaced with the post-event one, which makes the runtime's drift
    // check compare the replacement contract to itself -- is an edit, not a
    // transition (Codex P1, pairtrade#309).
    require_corporate_action_progress_transition(
        config,
        baseline,
        current,
        sequence_advance,
        !resumed.is_empty(),
        verified_at,
    )?;

    // The discard is its own tick, at `effective_at`, and leaves the window
    // empty with the progress record stamped. `resumed` above covers the
    // degenerate case where one tick does both.
    let baseline_invalidated = baseline
        .corporate_action
        .as_ref()
        .and_then(|progress| progress.history_invalidated_at);
    if let Some(progress) = &current.corporate_action {
        if progress.history_invalidated_at.is_some() && baseline_invalidated.is_none() {
            if sequence_advance != 1 {
                bail!(
                    "Arcus corporate action {} discarded its signal window without a new observation",
                    progress.event_id
                );
            }
            if !current.relative_log_price_history.is_empty() {
                bail!(
                    "Arcus corporate action {} stamped a discard it did not perform",
                    progress.event_id
                );
            }
            authorized.history_discarded = true;
        }
    }

    Ok(authorized)
}

fn require_corporate_action_progress_transition(
    config: &ArcusSpotRuntimeConfig,
    baseline: &ArcusSpotRuntimeState,
    current: &ArcusSpotRuntimeState,
    sequence_advance: u64,
    resumed: bool,
    verified_at: DateTime<Utc>,
) -> Result<()> {
    if sequence_advance == 0 {
        if current.corporate_action != baseline.corporate_action {
            bail!("Arcus corporate-action progress changed without a new observation");
        }
        // The cached identities are what a window opening copies into its
        // pre-event pins, so a forged cache would let a relisting compare
        // the replacement contract against itself and resume. Nothing else
        // compares them (Codex P1, pairtrade#309).
        if current.last_token_a_identity != baseline.last_token_a_identity
            || current.last_token_b_identity != baseline.last_token_b_identity
            || current.last_token_identity_at != baseline.last_token_identity_at
        {
            bail!("Arcus cached token identities changed without a new observation");
        }
        // The price clock decides the stale-unit exemption, so it is
        // forgeable evidence and gets the same treatment.
        if current.last_reference_price_at != baseline.last_reference_price_at {
            bail!("Arcus reference-price timestamp changed without a new observation");
        }
        // Resolution may run at load without advancing the sequence, so the
        // marker is allowed to go false -> true; the reverse, and any change
        // to an already-resolved record, is not.
        if baseline.handled_corporate_actions_resolved
            && (!current.handled_corporate_actions_resolved
                || current.handled_corporate_action_fingerprints
                    != baseline.handled_corporate_action_fingerprints)
        {
            bail!("Arcus handled corporate-action resolution changed without a new observation");
        }
        return Ok(());
    }
    // A discard stamp may only sit at or after the cutoff it claims to mark
    // and no later than the observation that produced it. Applied wherever a
    // stamp can appear -- a window opening already stamped counts (Codex P1,
    // pairtrade#309).
    let stamp_within_bounds = |progress: &ArcusSpotCorporateActionProgress,
                               stamped_at: DateTime<Utc>| {
        // Shared with the runtime's own stale-unit predicate: an amendment
        // that moved the cutoff earlier is the one that counts, and the two
        // must not disagree about a window that has been amended (Codex P1,
        // pairtrade#309).
        let cutoff = corporate_action_effective_cutoff(progress, config);
        // Lower bound: the cutoff it claims to mark. Upper bound: the clock
        // this verification runs at -- *not* `last_observation_at`. The
        // runtime stamps with `evaluation_time`, which `live-tick` takes
        // with `Utc::now()` after fetching the snapshot, so the stamp is
        // normally later than the observation watermark and bounding it
        // there rejected every genuine discard (Codex P1, pairtrade#309).
        cutoff.is_some_and(|cutoff| stamped_at >= cutoff) && stamped_at <= verified_at
    };
    match (&baseline.corporate_action, &current.corporate_action) {
        (None, None) => {}
        (Some(_), None) => {
            if !resumed {
                bail!("Arcus corporate-action progress was cleared without a resume");
            }
        }
        (None, Some(opened)) => {
            // A window opened on this observation. Its declaration must be
            // in the approved config, its cutoff copied from it, and its
            // pins must be exactly what the runtime had observed *before*
            // `entry_block_at` -- or absent when nothing had been.
            let event = config
                .corporate_actions
                .iter()
                .find(|event| {
                    (!opened.fingerprint.is_empty() && opened.fingerprint == event.fingerprint())
                        || event.event_id.eq_ignore_ascii_case(&opened.event_id)
                })
                .with_context(|| {
                    format!(
                        "Arcus corporate action {} opened a window the approved config does not declare",
                        opened.event_id
                    )
                })?;
            if opened.effective_at != Some(event.effective_at)
                || opened.symbols != event.symbols
                || opened.fingerprint != event.fingerprint()
            {
                bail!(
                    "Arcus corporate action {} opened with a record that does not match its declaration",
                    opened.event_id
                );
            }
            let pre_event_observed = baseline
                .last_token_identity_at
                .is_some_and(|observed_at| observed_at < event.entry_block_at);
            let (expected_a, expected_b) = if pre_event_observed {
                (
                    baseline.last_token_a_identity.clone(),
                    baseline.last_token_b_identity.clone(),
                )
            } else {
                (None, None)
            };
            if opened.pre_event_token_a != expected_a || opened.pre_event_token_b != expected_b {
                bail!(
                    "Arcus corporate action {} pinned a pre-event identity the backup never observed",
                    opened.event_id
                );
            }
            // The runtime only writes this record once the evaluation clock
            // reaches `entry_block_at`, so a record opened earlier is not a
            // transition -- and restoring one makes pre-window ticks treat
            // it as unresolved progress, blocking entries and suppressing
            // history before the window exists (Codex P2, pairtrade#309).
            if opened.blocked_at < event.entry_block_at || opened.blocked_at > verified_at {
                bail!(
                    "Arcus corporate action {} opened at {}, outside its declared window",
                    opened.event_id,
                    opened.blocked_at
                );
            }
            if let Some(stamped_at) = opened.history_invalidated_at {
                if !stamp_within_bounds(opened, stamped_at) {
                    bail!(
                        "Arcus corporate action {} opened with a discard stamp no observation \
                         produces",
                        opened.event_id
                    );
                }
            }
        }
        (Some(before), Some(after)) => {
            let same_event = after.fingerprint == before.fingerprint
                || (before.fingerprint.is_empty()
                    && after.event_id.eq_ignore_ascii_case(&before.event_id));
            // A stamp may appear, but only at or after the cutoff it claims
            // to mark and no later than the observation that produced it.
            // Otherwise a modified checkpoint could discard its window early
            // and -- because the stamp is what makes the units read as stale
            // -- suppress exits before the declared cutoff (Codex P1,
            // pairtrade#309).
            let stamp_ok = match (before.history_invalidated_at, after.history_invalidated_at) {
                (Some(was), Some(now)) => was == now,
                (None, None) => true,
                (Some(_), None) => false,
                (None, Some(stamped_at)) => stamp_within_bounds(after, stamped_at),
            };
            if !same_event
                || !stamp_ok
                || after.blocked_at != before.blocked_at
                || after.pre_event_token_a != before.pre_event_token_a
                || after.pre_event_token_b != before.pre_event_token_b
                || after.effective_at != before.effective_at
                || after.symbols != before.symbols
                || (after.event_id != before.event_id && before.fingerprint.is_empty())
            {
                bail!(
                    "Arcus corporate action {} progress changed in a way no observation produces",
                    before.event_id
                );
            }
        }
    }
    Ok(())
}

fn require_signal_history_continuity(
    baseline: &[f64],
    current: &[f64],
    sequence_advance: u64,
    signal_window_samples: usize,
    corporate_action: &ArcusSpotCorporateActionContinuity,
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
            // A declared `effective_at` discards the window outright and
            // accumulates nothing until the resume, so an empty history is
            // the expected shape on exactly that tick -- and only when
            // `corporate_action_continuity` proved the config declares it
            // (bot-strategy#853).
            if corporate_action.history_discarded {
                if current.is_empty() {
                    return Ok(());
                }
                // The degenerate one-tick path: no tick ran between
                // `effective_at` and `resume_not_before`, so the same
                // observation discards the pre-event window *and* resumes.
                // The resume clears the gate, so that tick's own post-event
                // sample is appended to the emptied window and the shape is
                // one fresh value -- unrelated to the discarded baseline,
                // which is why the ordinary `starts_with` rule cannot see it.
                if corporate_action.resumed_inventory.is_some() && current.len() == 1 {
                    return Ok(());
                }
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
    // Derived once, before anything relaxes: an unexplained move in any of
    // these fields is still a violation, and this is what separates the
    // transition the approved config declares from one that merely looks
    // like it (bot-strategy#853).
    let corporate_action = corporate_action_continuity(
        &config.runtime,
        baseline_runtime,
        current_runtime,
        sequence_advance,
        acceptance_not_after,
    )?;
    require_signal_history_continuity(
        &baseline_runtime.relative_log_price_history,
        &current_runtime.relative_log_price_history,
        sequence_advance,
        config.runtime.signal_window_samples,
        &corporate_action,
    )?;
    require_risk_state_continuity(
        &config.runtime,
        baseline_runtime,
        current_runtime,
        sequence_advance,
        acceptance_not_before,
        acceptance_not_after,
        &corporate_action,
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
        &corporate_action,
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
  arcus-spot-execute-once reset-window CONFIG_YAML
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

reset-window starts a fresh signal window when a state-invalidating
`runtime:` field changed (`mode`, `chain_id`, `pair`, `initial_inventory`,
`signal_window_samples`), which the checkpoint otherwise refuses to load
under. It never deletes or renumbers the hash-chained event stream: the new
checkpoint continues from the stream's verified tail, so the audit chain
stays contiguous across the strategy change and the events' own `pair`/`mode`
fields mark the boundary (bot-strategy#903). It refuses unless the bot is
idle -- no pending durable event, no active ledger attempt, no pending-plan
evidence, no open rotation, and no engaged risk halt (clear-risk-halt is the
deliberate decision for that one, never a side effect of a reset). A missing
checkpoint is refused outright: every one of those checks reads the
checkpoint, and losses accrue against the baseline inventory even on a bot
that has never swapped, so nothing else on the host can stand in for it. The
replaced checkpoint is copied aside and the stale observation-evidence
sidecar is moved aside, both as `<name>.pre-reset.<nanos>`; neither is a
verified backup, so still take a `state-backup` before the config swap (it
verifies against the *old* config) and another once the reset is live.

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
            // stdout stays exactly the digest -- callers pipe it into the
            // policy file. The cost budget goes to stderr: the cap is
            // compared against the *all-in* figure (quoted round-trip loss
            // plus both fixed buffers), and an operator who reads it as a
            // cap on the quoted loss alone sizes it too low and then cannot
            // tell why every tick holds on `cost_limit` (bot-strategy#903).
            eprintln!(
                "[arcus-config] cost budget: max_all_in_round_trip_cost_bps {} bps is compared \
                 against quoted round-trip loss + gas_buffer_bps {} + settlement_buffer_bps {}, \
                 so a quote clears the gate only at or below {} bps",
                config.runtime.max_all_in_round_trip_cost_bps.normalize(),
                config.runtime.gas_buffer_bps.normalize(),
                config.runtime.settlement_buffer_bps.normalize(),
                config
                    .runtime
                    .max_all_in_round_trip_cost_bps
                    .checked_sub(
                        config
                            .runtime
                            .gas_buffer_bps
                            .checked_add(config.runtime.settlement_buffer_bps)
                            .context("cost buffers exceed Decimal range")?
                    )
                    .context("cost buffers exceed Decimal range")?
                    .normalize(),
            );
            // The same reasoning for the corporate-action calendar
            // (bot-strategy#853): a window that was meant to be declared and
            // silently is not looks exactly like no window at all, and the
            // one moment an operator can still catch that is while
            // installing the config they just edited. `deny_unknown_fields`
            // catches a mistyped key; this catches an event that landed
            // somewhere other than where it was meant to.
            if config.runtime.corporate_actions.is_empty() {
                eprintln!("[arcus-config] corporate actions: none declared");
            } else {
                eprintln!(
                    "[arcus-config] corporate actions: {} declared",
                    config.runtime.corporate_actions.len()
                );
                for event in &config.runtime.corporate_actions {
                    eprintln!(
                        "[arcus-config]   {} ({}) block {} -> exit {} -> effective {} -> resume {}                          [{}] source: {}",
                        event.event_id,
                        event.symbols.join("+"),
                        event.entry_block_at,
                        event.reduce_exit_at,
                        event.effective_at,
                        event.resume_not_before,
                        match event.post_event_inventory {
                            Some(inventory) => format!(
                                "reconciled token_a={} token_b={}",
                                inventory.token_a.normalize(),
                                inventory.token_b.normalize()
                            ),
                            None => "NOT RECONCILED -- the resume will hold".to_string(),
                        },
                        event.source,
                    );
                }
            }
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
        [command, config_path] if command == "reset-window" => {
            // The sanctioned way to start a fresh signal window under a
            // state-invalidating config change (a new pair, a re-funded
            // inventory), replacing the JSON surgery #902 needed
            // (bot-strategy#903). Printed rather than merely done: this
            // discards an accumulated window on a live bot, so the record
            // of what was discarded lands in the journal.
            println!(
                "{}",
                serde_json::to_string_pretty(&reset_runtime_window(Path::new(config_path))?)?
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
                .validate_plan_consistent_with_state(&plan, Utc::now())
                .map_err(anyhow::Error::msg)
                .context("Arcus plan is inconsistent with the current runtime checkpoint")?;
            let attempt = executor
                .execute_plan_once(&plan, &plan_config_digest, &|at| {
                    runtime.validate_plan_consistent_with_state(&plan, at)
                })
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
                .validate_plan_consistent_with_state(&plan, Utc::now())
                .map_err(anyhow::Error::msg)
                .context("Arcus plan is inconsistent with the current runtime checkpoint")?;
            let attempt = executor
                .execute_plan_once(&plan, &plan_config_digest, &|at| {
                    runtime.validate_plan_consistent_with_state(&plan, at)
                })
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
            let mut recorder_config = ArcusSpotRecorderConfig::from_csv(
                &config.runtime.bidirectional_recorder_pairs_csv(),
                &config.runtime.notional_usd.normalize().to_string(),
            )
            .context(
                "failed to build a bidirectional recorder config from the runtime pair/notional",
            )?;
            // A rotated position exits by selling exactly the quantity it
            // acquired, so the snapshot must carry a quote at that exact
            // size (bot-strategy#906). This unlocked peek at the checkpoint
            // only decides which rows to *request*; the locked load below
            // is what the decision is made against, and step_at matches
            // the row by exact raw amount, so a checkpoint that moves in
            // between (a concurrent fill) simply leaves this row unused.
            if let Some(row) = live_tick_open_quantity_exit_row(&config)? {
                recorder_config = recorder_config.with_fixed_sell_amount_row(row);
            }
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
                .validate_plan_consistent_with_state(&plan, Utc::now())
                .map_err(anyhow::Error::msg)
                .context("Arcus plan is inconsistent with the current runtime checkpoint")?;
            let attempt = match executor
                .execute_plan_once(&plan, &plan_config_digest, &|at| {
                    // The re-read instance, like the pre-dispatch check
                    // above: the guard exists to judge the submission seam
                    // against current state, and closing over the
                    // pre-lock `runtime` silently defeats that for anything
                    // added to the validator later (independent review,
                    // pairtrade#309).
                    fresh_runtime.validate_plan_consistent_with_state(&plan, at)
                })
                .await
            {
                Ok(attempt) => attempt,
                // The venue could not quote and said so before this dispatch
                // touched anything (bot-strategy#967). Like an unsupported
                // recommended route (#817), that is an ordinary outcome of
                // asking a market for a price, not a fault of this bot, and
                // reporting it as a failed unit is exactly the noise a real
                // fault would have to stand out from. Every other error --
                // including a retryable one raised after submission, where an
                // attempt exists to reconcile -- still fails the run.
                Err(error) => {
                    return match error.downcast_ref::<ArcusSpotQuoteUnavailable>() {
                        Some(unavailable) => hold_on_unavailable_quote(
                            &config,
                            &event,
                            &plan,
                            &pending_plan_path,
                            &plan_bytes,
                            unavailable,
                        ),
                        None => Err(error),
                    };
                }
            };
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
        assert!(usage().contains("reset-window CONFIG_YAML"));
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

    /// The token identities, and the observation they were read from, that
    /// `step_at` now records on every structurally valid observation
    /// (bot-strategy#853). A hand-written checkpoint has to carry them for
    /// exactly the reason it already carries `last_token_*_reference_price_usd`:
    /// the continuity replay reproduces them from the recorder evidence, and
    /// a fixture without them is not a checkpoint the runtime would ever
    /// have written.
    fn set_observed_token_identities(state: &mut serde_json::Value, observed_at: &str) {
        state["last_token_a_identity"] = json!({
            "symbol": "NVDA",
            "address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
            "decimals": 18,
        });
        state["last_token_b_identity"] = json!({
            "symbol": "AMD",
            "address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
            "decimals": 18,
        });
        state["last_token_identity_at"] = json!(observed_at);
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
            // The buy-balance delta above, as the settlement receipt's own
            // `SwapExecuted.amount_out` would have reported it
            // (bot-strategy#883).
            settled_buy_amount_raw: Some("50000000000000000".to_string()),
            // The sell-balance delta above, as the settlement
            // transaction's own transfers would have reported it, with no
            // refund leg (bot-strategy#979).
            settled_sell_amount_raw: Some("50000000000000000".to_string()),
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            set_observed_token_identities(state, "2026-08-16T12:00:00Z");
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
            state["last_token_b_reference_price_usd"] = json!("57.300959372038022");
            set_observed_token_identities(state, "2026-08-16T12:01:00Z");
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            set_observed_token_identities(state, "2026-08-16T12:01:00Z");
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
            state["last_token_b_reference_price_usd"] = json!("176.49938051691913");
            set_observed_token_identities(state, "2026-08-16T00:00:01Z");
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
            state["last_token_b_reference_price_usd"] = json!("529.49814155075739");
            set_observed_token_identities(state, "2026-08-16T00:00:01Z");
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
            state["last_token_b_reference_price_usd"] = json!("529.49814155075739");
            set_observed_token_identities(state, "2026-08-16T12:01:00Z");
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
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
            state["last_reference_price_at"] = state["last_observation_at"].clone();
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

    /// An entry plan quoted one second before `reconciled_entry_attempt`'s
    /// fixed dispatch time, so the continuity path's plan/quote freshness
    /// checks pass.
    fn continuity_plan() -> ArcusSpotRotationPlan {
        let mut plan = rotation_plan("entry_signal");
        plan.quote_received_at = DateTime::parse_from_rfc3339("2026-08-16T12:00:01Z")
            .unwrap()
            .with_timezone(&Utc);
        plan
    }

    /// bot-strategy#979: the continuity path carries the same invariant as
    /// the live one. A settlement that refunded 16 wei of the signed sell
    /// amount must reconcile, and the quantity it hands the runtime must be
    /// the 16-wei-smaller amount the wallet actually parted with -- not the
    /// plan's own sell_quantity.
    #[test]
    fn continuity_fill_scales_the_sell_quantity_when_the_settlement_refunded() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = continuity_plan();
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        // pre 1000000000000000000 - post 950000000000000016 = 49999999999999984,
        // i.e. the signed 50000000000000000 less a 16-wei refund, which is
        // what the settlement read derived from the transaction's transfers.
        attempt.post_balances.as_mut().unwrap().sell_balance_raw = "950000000000000016".to_string();
        attempt.settled_sell_amount_raw = Some("49999999999999984".to_string());

        let (actual_sell_quantity, _actual_buy_quantity, _filled_at) =
            reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at).unwrap();

        assert_eq!(
            actual_sell_quantity,
            Decimal::from_str_exact("0.049999999999999984").unwrap()
        );
        assert!(actual_sell_quantity < plan.sell_quantity);
    }

    /// More than the signed amount can never have left the wallet under
    /// Permit2, so that stays fatal on this path too (bot-strategy#979).
    #[test]
    fn continuity_fill_refuses_a_sell_delta_above_the_signed_amount() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = continuity_plan();
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.post_balances.as_mut().unwrap().sell_balance_raw = "949999999999999999".to_string();

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error.to_string().contains("sell delta exceeds its intent"));
    }

    /// Codex P1 follow-up (bot-strategy#979): a short sell delta is only
    /// acceptable because the settlement's own transfers say so. Post
    /// balances edited to fabricate a shortfall -- with runtime state
    /// edited to match -- must not pass continuity verification just
    /// because the delta stays under the signed amount.
    #[test]
    fn continuity_fill_binds_a_short_sell_delta_to_the_settled_input() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = continuity_plan();
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.post_balances.as_mut().unwrap().sell_balance_raw = "950000000000000016".to_string();

        // The recorded settlement still says the full signed amount left.
        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match its settled swap input"));

        // A settled input above the signed amount is impossible under
        // Permit2 and is refused before the delta is even consulted.
        attempt.settled_sell_amount_raw = Some("50000000000000001".to_string());
        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("settled swap input exceeds its intent"));
    }

    /// An attempt reconciled before the settled input was recorded keeps
    /// the former exact-equality invariant: such a binary could not have
    /// reconciled a refund, so a short delta there is unexplained
    /// (bot-strategy#979).
    #[test]
    fn continuity_fill_refuses_a_short_sell_delta_on_a_legacy_attempt() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = continuity_plan();
        let mut attempt = reconciled_entry_attempt(&config, &plan, 1);
        attempt.post_balances.as_mut().unwrap().sell_balance_raw = "950000000000000016".to_string();
        attempt.settled_sell_amount_raw = None;

        let error = reconciled_fill_for_continuity(&config, &plan, &attempt, attempt.prepared_at)
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("sell delta does not match its intent"));
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

    /// The same evidence, for the other reason a would-rotate plan never
    /// gets dispatched (bot-strategy#967): the venue could not quote it.
    /// `reason` is what tells the two apart at readout time, so a hold that
    /// stopped stamping it would silently be counted as an unsupported
    /// route.
    #[test]
    fn a_quote_unavailable_hold_is_recorded_and_clears_its_own_pending_plan() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "rialto".to_string();
        let event = undispatched_plan_event(&config, &plan);
        let pending_plan_path = live_tick_pending_plan_path(&config).unwrap();
        let plan_bytes = b"this invocation's own pending plan".to_vec();
        write_private_regular_file_atomic(&pending_plan_path, &plan_bytes).unwrap();

        let unavailable = ArcusSpotQuoteUnavailable {
            venue: "rialto",
            detail: "Arcus Spot HTTP 422 (http, retryable=true): NO_QUOTES".to_string(),
        };
        hold_on_unavailable_quote(
            &config,
            &event,
            &plan,
            &pending_plan_path,
            &plan_bytes,
            &unavailable,
        )
        .unwrap();

        assert!(
            !pending_plan_path.exists(),
            "a plan that was never dispatched must not be left as recovery evidence",
        );
        let raw = fs::read_to_string(declined_route_log_path(&config).unwrap()).unwrap();
        let row: serde_json::Value = serde_json::from_str(raw.lines().next().unwrap()).unwrap();
        assert_eq!(row["reason"], "quote_unavailable");
        assert_eq!(row["detail"], unavailable.to_string());
        assert_eq!(row["recommended_venue"], "rialto");
        assert_eq!(row["z_score"], serde_json::json!(2.9));
    }

    /// An unsupported route keeps its own reason, so the two undispatched
    /// families stay separable in one file.
    #[test]
    fn a_declined_route_is_stamped_with_its_own_reason() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);

        let mut plan = rotation_plan("entry_signal");
        plan.venue = "lifi".to_string();
        let event = undispatched_plan_event(&config, &plan);

        decline_unsupported_route(&config, &event, &plan).unwrap();

        let raw = fs::read_to_string(declined_route_log_path(&config).unwrap()).unwrap();
        let row: serde_json::Value = serde_json::from_str(raw.lines().next().unwrap()).unwrap();
        assert_eq!(row["reason"], "unsupported_route");
        assert_eq!(row["detail"], serde_json::Value::Null);
    }

    /// A concurrent tick writes its plan to the same fixed path after this
    /// invocation dropped the checkpoint lock. That plan may still be
    /// dispatched and is the only `auto-resume` evidence for it, so a hold
    /// here must not delete it.
    #[test]
    fn a_hold_leaves_a_pending_plan_it_did_not_write() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        persist_initial_operator_state(&config);

        let plan = rotation_plan("entry_signal");
        let event = undispatched_plan_event(&config, &plan);
        let pending_plan_path = live_tick_pending_plan_path(&config).unwrap();
        let newer = b"a later tick's pending plan".to_vec();
        write_private_regular_file_atomic(&pending_plan_path, &newer).unwrap();

        hold_on_unavailable_quote(
            &config,
            &event,
            &plan,
            &pending_plan_path,
            b"this invocation's own pending plan",
            &ArcusSpotQuoteUnavailable {
                venue: "rialto",
                detail: "NO_QUOTES".to_string(),
            },
        )
        .unwrap();

        assert_eq!(fs::read(&pending_plan_path).unwrap(), newer);
    }

    /// A would-rotate event for a plan that reached the dispatch seam.
    fn undispatched_plan_event(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
    ) -> ArcusSpotRuntimeEvent {
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let runtime = store.load_existing(&config.runtime).unwrap();
        ArcusSpotRuntimeEvent {
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
        }
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

    /// bot-strategy#986: a router rejection that never reached the chain is
    /// cleared by the tick itself, so a 422 no longer costs the seven hours
    /// bot-strategy#985 did.
    fn rejected_attempt(
        config: &ArcusSpotExecuteOnceConfig,
        plan: &ArcusSpotRotationPlan,
        sequence: u64,
    ) -> ArcusSpotExecutionAttempt {
        let mut attempt = reconciled_entry_attempt(config, plan, sequence);
        attempt.phase = ArcusSpotExecutionPhase::Rejected;
        attempt.tx_hash = None;
        attempt.post_balances = None;
        attempt.settled_buy_amount_raw = None;
        attempt.settled_sell_amount_raw = None;
        attempt.detail = Some(
            "Arcus Spot submission was rejected by HTTP 422 from              https://router.spot.arcus.xyz/v1/submit: {\"code\":\"SHELL_SUBMIT_FAILED\"}"
                .to_string(),
        );
        attempt
    }

    fn ledger_with(
        active: Option<ArcusSpotExecutionAttempt>,
        history: Vec<ArcusSpotExecutionAttempt>,
    ) -> ArcusSpotExecutionLedger {
        let mut ledger = ArcusSpotExecutionLedger::default();
        ledger.next_sequence = history.len() as u64 + 2;
        ledger.history = history;
        ledger.active = active;
        ledger
    }

    #[test]
    fn a_router_rejection_with_no_transaction_is_cleared_by_the_tick() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let mut ledger = ledger_with(Some(rejected_attempt(&config, &plan, 1)), vec![]);

        assert_eq!(auto_archive_router_rejection(&mut ledger).unwrap(), Some(1));
        assert!(ledger.active.is_none(), "the active slot is free again");
        assert_eq!(ledger.history.len(), 1, "the rejection is kept in history");
        assert_eq!(ledger.history[0].phase, ArcusSpotExecutionPhase::Rejected);
    }

    #[test]
    fn a_rejection_carrying_a_tx_hash_still_waits_for_an_operator() {
        // It may have reached the chain, so it needs
        // repair-report/manual-reconcile, not a plain archive -- the same
        // boundary archive-rejected-report already draws.
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let mut attempt = rejected_attempt(&config, &plan, 1);
        attempt.tx_hash = Some(format!("0x{}", "1".repeat(64)));
        let mut ledger = ledger_with(Some(attempt), vec![]);

        assert_eq!(auto_archive_router_rejection(&mut ledger).unwrap(), None);
        assert!(ledger.active.is_some(), "still held for an operator");
    }

    #[test]
    fn phases_other_than_rejected_still_wait_for_an_operator() {
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        for phase in [
            ArcusSpotExecutionPhase::Unknown,
            ArcusSpotExecutionPhase::OperatorHold,
            ArcusSpotExecutionPhase::Failed,
        ] {
            let mut attempt = rejected_attempt(&config, &plan, 1);
            attempt.phase = phase;
            let mut ledger = ledger_with(Some(attempt), vec![]);
            assert_eq!(
                auto_archive_router_rejection(&mut ledger).unwrap(),
                None,
                "{phase:?} is not a router rejection"
            );
            assert!(ledger.active.is_some(), "{phase:?} stays active");
        }
    }

    #[test]
    fn a_run_of_rejections_stops_at_the_cap() {
        // A venue refusing everything is not the cheap case: clearing
        // forever would re-plan and re-sign against a router that is saying
        // no, with nobody told about it.
        let dir = tempdir().unwrap();
        let config = execute_once_config(
            dir.path().join("ledger.json").to_str().unwrap(),
            dir.path().join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        );
        let plan = rotation_plan("entry_signal");
        let history: Vec<_> = (1..MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS as u64)
            .map(|sequence| rejected_attempt(&config, &plan, sequence))
            .collect();
        assert_eq!(
            consecutive_tail_rejections(&history),
            MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS - 1
        );
        let sequence = MAX_CONSECUTIVE_AUTO_ARCHIVED_REJECTIONS as u64;
        let mut ledger = ledger_with(
            Some(rejected_attempt(&config, &plan, sequence)),
            history.clone(),
        );

        assert_eq!(auto_archive_router_rejection(&mut ledger).unwrap(), None);
        assert!(ledger.active.is_some(), "the cap hands it to an operator");

        // A success in between resets the run: this is about a venue
        // refusing everything, not about a lifetime total.
        let mut interrupted = history.clone();
        interrupted.push(reconciled_entry_attempt(&config, &plan, 99));
        assert_eq!(consecutive_tail_rejections(&interrupted), 0);
        let mut ledger = ledger_with(
            Some(rejected_attempt(&config, &plan, sequence + 1)),
            interrupted,
        );
        assert_eq!(
            auto_archive_router_rejection(&mut ledger).unwrap(),
            Some(sequence + 1),
        );
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
            settled_buy_amount_raw: None,
            settled_sell_amount_raw: None,
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
        // Must serialize via ArcusSpotExecutionPhase's own snake_case
        // Serialize impl, not Debug's PascalCase spelling, to match every
        // other report's "phase" convention (build_repair_report et al.).
        assert_eq!(report["phase"], "rejected");
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

    // -- reset-window (bot-strategy#903) ---------------------------------

    fn reset_window_config(dir: &Path) -> ArcusSpotExecuteOnceConfig {
        execute_once_config(
            dir.join("ledger.json").to_str().unwrap(),
            dir.join("runtime.json").to_str().unwrap(),
            "100000000000000000",
        )
    }

    fn reset_window_observe_event(
        sequence: u64,
        observed_at: DateTime<Utc>,
    ) -> ArcusSpotRuntimeEvent {
        let inventory = ArcusSpotInventory {
            token_a: Decimal::new(35, 2),
            token_b: Decimal::new(16, 2),
        };
        ArcusSpotRuntimeEvent {
            sequence,
            observed_at,
            pair: "NVDA/AMD".to_string(),
            mode: ArcusSpotRuntimeMode::Live,
            token_a_reference_price_usd: Some(Decimal::from(200)),
            token_b_reference_price_usd: Some(Decimal::from(100)),
            relative_log_price: Some(0.5),
            z_score: Some(0.1),
            inventory_before: inventory,
            inventory_after: inventory,
            regime_before: ArcusSpotRegime::Neutral,
            regime_after: ArcusSpotRegime::Neutral,
            risk_before: None,
            risk_after: None,
            decision: ArcusSpotDecision::Observe {
                hold: ArcusSpotHold {
                    code: ArcusSpotHoldCode::NoSignal,
                    detail: "z below entry threshold".to_string(),
                },
            },
        }
    }

    /// A live host mid-probe: three committed events, and a checkpoint whose
    /// accumulated window is at the same sequence.
    fn seed_reset_window_host(config: &ArcusSpotExecuteOnceConfig, tail: u64) {
        persist_initial_operator_state(config);
        let stream = live_tick_event_stream(config).unwrap();
        for sequence in 1..=tail {
            stream
                .append(&reset_window_observe_event(
                    sequence,
                    fixture_now() + chrono::Duration::seconds(sequence as i64),
                ))
                .unwrap();
        }
        let base = ArcusSpotRuntime::new(config.runtime.clone()).unwrap();
        let mut state = base.state().clone();
        state.sequence = tail;
        state.relative_log_price_history = vec![0.11, 0.12, 0.13];
        state.last_observation_at = Some(fixture_now());
        ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .persist(&ArcusSpotRuntime::from_state(config.runtime.clone(), state).unwrap())
            .unwrap();
    }

    /// The same config with a state-invalidating change: a re-funded
    /// inventory baseline, which `classify_config_drift` refuses to carry
    /// state across.
    fn reset_window_next_config(dir: &Path) -> ArcusSpotExecuteOnceConfig {
        let mut next = reset_window_config(dir);
        next.runtime.initial_inventory.token_a = Decimal::new(50, 2);
        next
    }

    #[test]
    fn reset_window_starts_a_fresh_window_that_continues_the_event_stream() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        let next = reset_window_next_config(dir.path());
        let store = ArcusSpotRuntimeCheckpointStore::new(next.runtime_state_path.clone());
        // The situation being resolved: the accumulated state cannot be
        // loaded under the new config at all.
        assert!(store.load_existing(&next.runtime).is_err());

        let report = commit_runtime_window_reset(&next).unwrap();

        assert_eq!(report["reset"]["checkpoint_sequence"], 3);
        assert_eq!(report["reset"]["next_event_sequence"], 4);
        assert_eq!(report["previous_checkpoint"]["sequence"], 3);
        assert_eq!(
            report["previous_checkpoint"]["relative_log_price_samples"],
            3
        );
        let runtime = store.load_existing(&next.runtime).unwrap();
        assert_eq!(runtime.state().sequence, 3);
        assert!(runtime.state().relative_log_price_history.is_empty());
        assert_eq!(runtime.state().inventory.token_a, Decimal::new(50, 2));
        assert_eq!(runtime.state().last_observation_at, None);

        // The invariant the whole command exists for: the next tick's event
        // is the stream tail's successor, and the renumbering the old
        // "remove the checkpoint" procedure produced is still refused.
        let stream = live_tick_event_stream(&next).unwrap();
        let at = fixture_now() + chrono::Duration::seconds(60);
        assert!(stream.append(&reset_window_observe_event(1, at)).is_err());
        stream.append(&reset_window_observe_event(4, at)).unwrap();
    }

    #[test]
    fn reset_window_carries_the_handled_corporate_actions_forward() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 2);
        // The state being reset already resumed from a split.
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let mut state = store
            .load_existing(&config.runtime)
            .unwrap()
            .state()
            .clone();
        state.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        state.handled_corporate_action_fingerprints = vec!["fp-split".to_string()];
        store
            .persist(&ArcusSpotRuntime::from_state(config.runtime.clone(), state).unwrap())
            .unwrap();
        let next = reset_window_next_config(dir.path());

        commit_runtime_window_reset(&next).unwrap();

        let fresh = ArcusSpotRuntimeCheckpointStore::new(next.runtime_state_path.clone())
            .load_existing(&next.runtime)
            .unwrap();
        assert!(fresh.state().relative_log_price_history.is_empty());
        assert_eq!(
            fresh.state().handled_corporate_action_ids,
            vec!["NVDA-2026-08-SPLIT".to_string()],
        );
        assert_eq!(
            fresh.state().handled_corporate_action_fingerprints,
            vec!["fp-split".to_string()],
        );
    }

    #[test]
    fn reset_window_resolves_a_carried_legacy_handled_record() {
        // A legacy checkpoint carries ids with no fingerprints. The fresh
        // state a reset builds is already marked resolved, so without
        // resolving them here the still-declared completed action would read
        // as a reused label and block entries forever.
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 2);
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let mut state = store
            .load_existing(&config.runtime)
            .unwrap()
            .state()
            .clone();
        state.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        state.handled_corporate_action_fingerprints.clear();
        state.handled_corporate_actions_resolved = false;
        state.last_observation_at = Some("2026-08-17T00:00:00Z".parse().unwrap());
        store
            .persist(&ArcusSpotRuntime::from_state(config.runtime.clone(), state).unwrap())
            .unwrap();

        let mut next = reset_window_next_config(dir.path());
        next.runtime.corporate_actions = vec![ArcusSpotCorporateActionEvent {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            symbols: vec![next.runtime.pair.sell_symbol.clone()],
            entry_block_at: "2026-08-16T00:00:00Z".parse().unwrap(),
            reduce_exit_at: "2026-08-16T01:00:00Z".parse().unwrap(),
            effective_at: "2026-08-16T02:00:00Z".parse().unwrap(),
            resume_not_before: "2026-08-16T03:00:00Z".parse().unwrap(),
            source: "issuer notice".to_string(),
            post_event_inventory: None,
        }];
        commit_runtime_window_reset(&next).unwrap();

        let fresh = ArcusSpotRuntimeCheckpointStore::new(next.runtime_state_path.clone())
            .load_existing(&next.runtime)
            .unwrap();
        assert_eq!(
            fresh.state().handled_corporate_action_fingerprints,
            vec![next.runtime.corporate_actions[0].fingerprint()],
            "the carried legacy record is resolved, not left ambiguous",
        );
    }

    #[test]
    fn reset_window_refuses_to_drop_an_open_corporate_action_window() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 2);
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let mut state = store
            .load_existing(&config.runtime)
            .unwrap()
            .state()
            .clone();
        state.corporate_action = Some(stale_unit_progress());
        store
            .persist(&ArcusSpotRuntime::from_state(config.runtime.clone(), state).unwrap())
            .unwrap();

        // An unrelated state-invalidating change: refused.
        let mut unrelated = reset_window_config(dir.path());
        unrelated.runtime.signal_window_samples += 1;
        let error = commit_runtime_window_reset(&unrelated)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("inside corporate action NVDA-2026-08-SPLIT"),
            "{error}"
        );

        // A `pair` edit that keeps the affected token -- swapped legs, or
        // only the other leg changed -- is not a replacement.
        let mut swapped = reset_window_config(dir.path());
        let (a, b) = (
            swapped.runtime.pair.sell_symbol.clone(),
            swapped.runtime.pair.buy_symbol.clone(),
        );
        swapped.runtime.pair.sell_symbol = b;
        swapped.runtime.pair.buy_symbol = a;
        let error = commit_runtime_window_reset(&swapped)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("inside corporate action NVDA-2026-08-SPLIT"),
            "{error}"
        );

        // Replacing the instrument is the documented recovery for a relisted
        // ticker, and makes the old window moot.
        let mut replaced = reset_window_config(dir.path());
        replaced.runtime.pair.sell_symbol = "NVDB".to_string();
        commit_runtime_window_reset(&replaced).unwrap();
        let fresh = ArcusSpotRuntimeCheckpointStore::new(replaced.runtime_state_path.clone())
            .load_existing(&replaced.runtime)
            .unwrap();
        assert_eq!(fresh.state().corporate_action, None);
    }

    #[test]
    fn reset_window_keeps_the_replaced_checkpoint_beside_the_new_one() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 2);
        let replaced = fs::read(&config.runtime_state_path).unwrap();
        let next = reset_window_next_config(dir.path());

        let report = commit_runtime_window_reset(&next).unwrap();

        let retired: Vec<String> = serde_json::from_value(report["retired"].clone()).unwrap();
        let copy = retired
            .iter()
            .find(|path| path.contains("runtime.json.pre-reset."))
            .expect("the replaced checkpoint is reported");
        assert_eq!(fs::read(copy).unwrap(), replaced);
        assert_ne!(fs::read(&next.runtime_state_path).unwrap(), replaced);
    }

    #[test]
    fn reset_window_refuses_to_roll_reconciled_inventory_back_to_the_declaration() {
        // Widening the signal window is the one state-invalidating change
        // that leaves the inventory's meaning intact -- and building the
        // fresh runtime takes inventory from `initial_inventory`, so a
        // bot that has traded would have its realized deltas rolled back
        // to the funding declaration, and every later size and floor check
        // would reason about balances the wallet does not have (Codex P1
        // follow-up).
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        // A completed rotation left the wallet holding something other
        // than what was declared at funding.
        rewrite_checkpoint_state(&config.runtime_state_path, |state| {
            state["inventory"]["token_a"] = serde_json::json!("0.20");
        });
        // Only the window length changes.
        let mut next = reset_window_config(dir.path());
        next.runtime.signal_window_samples += 8;

        let error = commit_runtime_window_reset(&next).unwrap_err().to_string();

        assert!(error.contains("holds inventory token_a=0.2"), "{error}");
        assert!(
            error.contains("Set initial_inventory to the reconciled holdings"),
            "{error}"
        );

        // Re-declaring the holdings is what makes it proceed: the operator
        // states the truth rather than the command inferring it.
        next.runtime.initial_inventory.token_a = Decimal::new(20, 2);
        commit_runtime_window_reset(&next).unwrap();
    }

    #[test]
    fn reset_window_refuses_a_checkpoint_that_trails_the_stream() {
        // Restoring an older `.pre-reset` copy or state-backup is exactly
        // what this command's other refusals tell an operator to do, so a
        // checkpoint behind the stream is reachable by following them. It
        // reads as valid while the events it has not seen may hold a fill
        // or an engaged halt, and every other guard here then passes on
        // stale state (Codex P1 follow-up).
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 5);
        // Roll the checkpoint back to an earlier sequence, leaving the
        // append-only stream at 5.
        rewrite_checkpoint_state(&config.runtime_state_path, |state| {
            state["sequence"] = serde_json::json!(2);
        });

        let error = commit_runtime_window_reset(&reset_window_next_config(dir.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("must be in step"), "{error}");
        assert!(
            error.contains("has not seen the stream's later events"),
            "{error}"
        );
    }

    #[test]
    fn reset_window_refuses_a_missing_checkpoint() {
        // Every precondition this command promises -- flat regime, no open
        // rotation, no engaged halt, a config change worth resetting for --
        // is read out of the checkpoint, so without one the reset is just
        // an unconditional re-anchoring of the loss baselines.
        //
        // This replaces an earlier allowance for the #902 case (the old
        // runbook removed the checkpoint). Gating it on "the ledger shows
        // no fund-moving attempt" was not enough: the risk marks are priced
        // against the baseline inventory, so losses accrue and a halt can
        // engage with zero swaps ever dispatched (Codex P1 follow-up).
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        fs::remove_file(&config.runtime_state_path).unwrap();

        let error = commit_runtime_window_reset(&reset_window_next_config(dir.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("is missing"), "{error}");
        // Refusing must not leave a checkpoint behind: the operator still
        // has to put the real one back.
        assert!(!config.runtime_state_path.exists());
    }

    #[test]
    fn reset_window_refuses_an_unchanged_config() {
        // A reset re-anchors initial_equity_usd and the buy-and-hold
        // basket, so cumulative-loss accounting starts over. With nothing
        // state-invalidating actually changed that is not a reset, it is a
        // repeatable erasure of the accounting the cumulative halt is
        // measured against, available to anyone who can run the executor
        // with the approved production config (Codex P1 follow-up).
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);

        let error = commit_runtime_window_reset(&config)
            .unwrap_err()
            .to_string();

        assert!(error.contains("all match the one supplied"), "{error}");
        // The checkpoint is untouched by a refusal.
        let summary = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
            .peek_summary()
            .unwrap()
            .unwrap();
        assert_eq!(summary.sequence, 3);
        assert_eq!(summary.relative_log_price_samples, 3);
    }

    #[test]
    fn reset_window_refuses_a_pending_durable_event() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        live_tick_event_publisher(&config)
            .unwrap()
            .stage(&reset_window_observe_event(
                4,
                fixture_now() + chrono::Duration::seconds(30),
            ))
            .unwrap();

        let error = commit_runtime_window_reset(&reset_window_next_config(dir.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("pending durable event"), "{error}");
        // Nothing was reset: the old checkpoint is still the live one.
        assert_eq!(
            ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone())
                .peek_summary()
                .unwrap()
                .unwrap()
                .relative_log_price_samples,
            3
        );
    }

    #[test]
    fn reset_window_refuses_an_unresolved_ledger_attempt() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        let plan = rotation_plan("entry_signal");
        let ledger = ArcusSpotExecutionLedger {
            next_sequence: 3,
            active: Some(repair_report_active_submitted_attempt(
                &config,
                &plan,
                fixture_now(),
            )),
            ..Default::default()
        };
        ArcusSpotExecutionLedgerStore::new(config.ledger_path.clone())
            .persist(&ledger)
            .unwrap();

        let error = commit_runtime_window_reset(&reset_window_next_config(dir.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("still active in phase"), "{error}");
    }

    #[test]
    fn reset_window_refuses_an_open_rotation() {
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let mut state = store
            .load_existing(&config.runtime)
            .unwrap()
            .state()
            .clone();
        state.regime = ArcusSpotRegime::RotatedAToB;
        state.rotated_quantity = Some(Decimal::new(4, 2));
        state.last_rotation_at = Some(fixture_now());
        store
            .persist(&ArcusSpotRuntime::from_state(config.runtime.clone(), state).unwrap())
            .unwrap();

        let error = commit_runtime_window_reset(&reset_window_next_config(dir.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("open rotation"), "{error}");
    }

    #[test]
    fn reset_window_refuses_an_engaged_risk_halt() {
        // A reset builds a fresh state, and a fresh state has no halt: left
        // unchecked, reset-window would be a second, undocumented way to
        // disarm the sticky risk stop clear-risk-halt exists to gate.
        let dir = tempdir().unwrap();
        let config = reset_window_config(dir.path());
        seed_reset_window_host(&config, 3);
        let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
        let mut state = store
            .load_existing(&config.runtime)
            .unwrap()
            .state()
            .clone();
        state.risk_halt = Some(ArcusSpotRiskHalt {
            kind: ArcusSpotRiskHaltKind::DailyLoss,
            engaged_at: fixture_now(),
            equity_usd: Decimal::from(90),
            loss_usd: Decimal::from(3),
            limit_usd: Decimal::from(2),
        });
        store
            .persist(&ArcusSpotRuntime::from_state(config.runtime.clone(), state).unwrap())
            .unwrap();

        let error = commit_runtime_window_reset(&reset_window_next_config(dir.path()))
            .unwrap_err()
            .to_string();

        assert!(error.contains("risk halt"), "{error}");
        assert!(error.contains("clear-risk-halt"), "{error}");
    }

    // ---- corporate-action continuity (bot-strategy#853, Codex P1) ----

    fn continuity_state(sequence: u64, inventory: (&str, &str)) -> ArcusSpotRuntimeState {
        serde_json::from_value(json!({
            "sequence": sequence,
            "inventory": {"token_a": inventory.0, "token_b": inventory.1},
            "regime": "neutral",
            "relative_log_price_history": [0.25],
            "last_token_a_reference_price_usd": "200",
            "last_token_b_reference_price_usd": "100",
            "last_observation_at": "2026-08-16T12:00:00Z",
            "last_rotation_at": null,
            "rotated_quantity": null,
            "initial_equity_usd": "300",
            "initial_baseline_inventory": {"token_a": inventory.0, "token_b": inventory.1},
            "daily_baseline_day": "2026-08-16",
            "daily_baseline_equity_usd": "300",
            "daily_baseline_inventory": {"token_a": inventory.0, "token_b": inventory.1},
            "last_equity_usd": "300",
            "risk_halt": null,
        }))
        .unwrap()
    }

    fn fixture_event() -> ArcusSpotCorporateActionEvent {
        config_with_corporate_action(None).corporate_actions[0].clone()
    }

    fn config_with_corporate_action(
        post_event_inventory: Option<(&str, &str)>,
    ) -> ArcusSpotRuntimeConfig {
        let dir = tempdir().unwrap();
        let mut config = execute_once_config(
            dir.path().join("l.json").to_str().unwrap(),
            dir.path().join("r.json").to_str().unwrap(),
            "100000000000000000",
        )
        .runtime;
        config.corporate_actions = vec![ArcusSpotCorporateActionEvent {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            symbols: vec!["NVDA".to_string()],
            entry_block_at: "2026-08-16T00:00:00Z".parse().unwrap(),
            reduce_exit_at: "2026-08-16T01:00:00Z".parse().unwrap(),
            effective_at: "2026-08-16T02:00:00Z".parse().unwrap(),
            resume_not_before: "2026-08-16T03:00:00Z".parse().unwrap(),
            source: "issuer notice".to_string(),
            post_event_inventory: post_event_inventory.map(|(a, b)| ArcusSpotInventory {
                token_a: a.parse().unwrap(),
                token_b: b.parse().unwrap(),
            }),
        }];
        config
    }

    /// baseline: mid-window, history already discarded. current: resumed onto
    /// the reconciled holding, with all three equity marks re-derived from it.
    fn resume_pair() -> (ArcusSpotRuntimeState, ArcusSpotRuntimeState) {
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.relative_log_price_history.clear();
        baseline.corporate_action = Some(ArcusSpotCorporateActionProgress {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            blocked_at: "2026-08-16T00:00:01Z".parse().unwrap(),
            pre_event_token_a: None,
            pre_event_token_b: None,
            history_invalidated_at: Some("2026-08-16T02:00:01Z".parse().unwrap()),
            fingerprint: String::new(),
            effective_at: None,
            symbols: vec!["NVDA".to_string()],
        });

        let mut current = continuity_state(8, ("4", "1"));
        current.relative_log_price_history = vec![0.25];
        current.corporate_action = None;
        current.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        current.handled_corporate_action_fingerprints = vec![fixture_event().fingerprint()];
        // 4 NVDA at 200 + 1 AMD at 100.
        for mark in [
            &mut current.initial_equity_usd,
            &mut current.daily_baseline_equity_usd,
            &mut current.last_equity_usd,
        ] {
            *mark = Some(Decimal::from(900));
        }
        (baseline, current)
    }

    #[test]
    fn a_declared_corporate_action_resume_is_authorized() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (baseline, current) = resume_pair();
        let authorized =
            corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();
        assert_eq!(
            authorized.resumed_inventory,
            Some(ArcusSpotInventory {
                token_a: Decimal::from(4),
                token_b: Decimal::ONE,
            }),
        );
        assert!(authorized.history_discarded);
    }

    #[test]
    fn a_resume_of_an_undeclared_event_is_rejected() {
        let mut config = config_with_corporate_action(Some(("4", "1")));
        config.corporate_actions.clear();
        let (baseline, current) = resume_pair();
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not declare"), "{error}");
    }

    #[test]
    fn a_resume_without_a_reconciled_holding_is_rejected() {
        let config = config_with_corporate_action(None);
        let (baseline, current) = resume_pair();
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("post_event_inventory"), "{error}");
    }

    #[test]
    fn a_resume_onto_a_holding_the_config_did_not_declare_is_rejected() {
        // The checkpoint claims the resume but landed somewhere else.
        let config = config_with_corporate_action(Some(("4", "1")));
        let (baseline, mut current) = resume_pair();
        current.inventory.token_a = Decimal::from(5);
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("did not land on its reconciled holding"),
            "{error}"
        );
    }

    #[test]
    fn a_resume_that_left_a_rotation_open_is_rejected() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (baseline, mut current) = resume_pair();
        current.regime = ArcusSpotRegime::RotatedAToB;
        current.rotated_quantity = Some(Decimal::ONE);
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("rotation still open"), "{error}");
    }

    #[test]
    fn a_resume_without_a_new_observation_is_rejected() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (baseline, current) = resume_pair();
        let error = corporate_action_continuity(&config, &baseline, &current, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("single new observation"), "{error}");
    }

    #[test]
    fn a_resume_must_record_the_declared_events_fingerprint() {
        let config = config_with_corporate_action(Some(("4", "1")));
        // A fingerprint vector shorter than the id vector is a malformed
        // record, caught by the length guard before anything slices it.
        let (baseline, mut current) = resume_pair();
        current.handled_corporate_action_fingerprints.clear();
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("lost or reordered"), "{error}");

        // And it must be *this* event's: a fingerprint of some other
        // declaration does not make the rename guard's record.
        let (baseline, mut current) = resume_pair();
        current.handled_corporate_action_fingerprints = vec!["deadbeef".to_string()];
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("without recording the fingerprint"),
            "{error}"
        );
    }

    #[test]
    fn progress_edits_that_no_observation_produces_are_rejected() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.corporate_action = Some(stale_unit_progress());
        let pinned = ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        };
        baseline
            .corporate_action
            .as_mut()
            .unwrap()
            .pre_event_token_a = Some(pinned.clone());

        // Same sequence, pin replaced with the post-event identity: the
        // drift check would then compare the replacement to itself.
        let mut edited = baseline.clone();
        edited.corporate_action.as_mut().unwrap().pre_event_token_a =
            Some(ArcusSpotTokenIdentity {
                address: "0xdeadbeef00000000000000000000000000000000".to_string(),
                ..pinned.clone()
            });
        let error = corporate_action_continuity(&config, &baseline, &edited, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("changed without a new observation"),
            "{error}"
        );

        // One observation later, the same edit is still not a transition.
        let mut edited = baseline.clone();
        edited.sequence = 8;
        edited.corporate_action.as_mut().unwrap().pre_event_token_a = None;
        let error = corporate_action_continuity(&config, &baseline, &edited, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("no observation produces"), "{error}");

        // ...whereas the discard stamp appearing is exactly what one does.
        let mut stamped = baseline.clone();
        stamped.sequence = 8;
        stamped.relative_log_price_history.clear();
        stamped
            .corporate_action
            .as_mut()
            .unwrap()
            .history_invalidated_at = None;
        let mut before = baseline.clone();
        before
            .corporate_action
            .as_mut()
            .unwrap()
            .history_invalidated_at = None;
        stamped
            .corporate_action
            .as_mut()
            .unwrap()
            .history_invalidated_at = Some("2026-08-16T02:00:01Z".parse().unwrap());
        corporate_action_continuity(&config, &before, &stamped, 1, verified_now()).unwrap();

        // Cleared without a resume: not a transition either.
        let mut cleared = baseline.clone();
        cleared.sequence = 8;
        cleared.corporate_action = None;
        let error = corporate_action_continuity(&config, &baseline, &cleared, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("cleared without a resume"), "{error}");
    }

    #[test]
    fn cached_token_identities_may_not_change_without_an_observation() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let observed = ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        };
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.last_token_a_identity = Some(observed.clone());
        baseline.last_token_identity_at = Some("2026-08-15T23:00:00Z".parse().unwrap());
        corporate_action_continuity(&config, &baseline, &baseline.clone(), 0, verified_now())
            .unwrap();

        // The post-event contract, dropped into the cache at the same
        // sequence: a later window would pin it as the *pre-event* identity.
        let mut forged = baseline.clone();
        forged.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            address: "0xdeadbeef00000000000000000000000000000000".to_string(),
            ..observed
        });
        let error = corporate_action_continuity(&config, &baseline, &forged, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("cached token identities"), "{error}");

        let mut restamped = baseline.clone();
        restamped.last_token_identity_at = Some("2026-08-16T01:00:00Z".parse().unwrap());
        let error = corporate_action_continuity(&config, &baseline, &restamped, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("cached token identities"), "{error}");

        // The price clock decides the stale-unit exemption, so it is
        // forgeable evidence and gets the same treatment.
        let mut repriced = baseline.clone();
        repriced.last_reference_price_at = Some("2026-08-16T01:00:00Z".parse().unwrap());
        let error = corporate_action_continuity(&config, &baseline, &repriced, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("reference-price timestamp"), "{error}");
    }

    #[test]
    fn a_window_may_not_open_before_its_declared_entry_block() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let event = fixture_event();
        let observed = ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        };
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.last_token_a_identity = Some(observed.clone());
        baseline.last_token_identity_at = Some("2026-08-15T23:00:00Z".parse().unwrap());
        let opened_at = |at: &str| {
            let mut current = continuity_state(8, ("1", "1"));
            current.corporate_action = Some(ArcusSpotCorporateActionProgress {
                event_id: event.event_id.clone(),
                blocked_at: at.parse().unwrap(),
                pre_event_token_a: Some(observed.clone()),
                pre_event_token_b: None,
                history_invalidated_at: None,
                fingerprint: event.fingerprint(),
                effective_at: Some(event.effective_at),
                symbols: event.symbols.clone(),
            });
            current
        };
        // entry_block_at is 2026-08-16T00:00:00Z.
        corporate_action_continuity(
            &config,
            &baseline,
            &opened_at("2026-08-16T00:00:01Z"),
            1,
            verified_now(),
        )
        .unwrap();

        // Opened before the window exists: restoring this blocks entries and
        // suppresses history early.
        let error = corporate_action_continuity(
            &config,
            &baseline,
            &opened_at("2026-08-15T20:00:00Z"),
            1,
            verified_now(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("outside its declared window"), "{error}");

        // Opened after the verification clock.
        let error = corporate_action_continuity(
            &config,
            &baseline,
            &opened_at("2026-08-18T00:00:00Z"),
            1,
            verified_now(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("outside its declared window"), "{error}");
    }

    #[test]
    fn an_opened_window_may_not_arrive_already_stamped_early() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let event = fixture_event();
        let observed = ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        };
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.last_token_a_identity = Some(observed.clone());
        baseline.last_token_identity_at = Some("2026-08-15T23:00:00Z".parse().unwrap());
        let opened = |stamp: Option<&str>| {
            let mut current = continuity_state(8, ("1", "1"));
            current.relative_log_price_history.clear();
            current.corporate_action = Some(ArcusSpotCorporateActionProgress {
                event_id: event.event_id.clone(),
                blocked_at: "2026-08-16T00:00:01Z".parse().unwrap(),
                pre_event_token_a: Some(observed.clone()),
                pre_event_token_b: None,
                history_invalidated_at: stamp.map(|at| at.parse().unwrap()),
                fingerprint: event.fingerprint(),
                effective_at: Some(event.effective_at),
                symbols: event.symbols.clone(),
            });
            current
        };
        // Unstamped, and stamped at the cutoff: both are transitions.
        corporate_action_continuity(&config, &baseline, &opened(None), 1, verified_now()).unwrap();
        corporate_action_continuity(
            &config,
            &baseline,
            &opened(Some("2026-08-16T02:00:01Z")),
            1,
            verified_now(),
        )
        .unwrap();

        // Stamped before the cutoff: after a restore this would suppress
        // halts and refuse exits before effective_at ever arrived.
        let error = corporate_action_continuity(
            &config,
            &baseline,
            &opened(Some("2026-08-16T00:30:00Z")),
            1,
            verified_now(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("discard stamp no observation produces"),
            "{error}"
        );
    }

    #[test]
    fn a_discard_stamp_must_sit_at_or_after_its_cutoff() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        let mut open = stale_unit_progress();
        open.history_invalidated_at = None;
        baseline.corporate_action = Some(open.clone());
        let stamped = |at: &str| {
            let mut current = continuity_state(8, ("1", "1"));
            current.relative_log_price_history.clear();
            let mut progress = open.clone();
            progress.history_invalidated_at = Some(at.parse().unwrap());
            current.corporate_action = Some(progress);
            current
        };

        // The cutoff is 02:00Z and the observation 12:00Z.
        corporate_action_continuity(
            &config,
            &baseline,
            &stamped("2026-08-16T02:00:01Z"),
            1,
            verified_now(),
        )
        .unwrap();

        // Stamped before the cutoff: a premature discard, which would also
        // make dispatch treat the units as stale early.
        let error = corporate_action_continuity(
            &config,
            &baseline,
            &stamped("2026-08-16T01:00:00Z"),
            1,
            verified_now(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("no observation produces"), "{error}");

        // The runtime stamps with `evaluation_time`, which live-tick takes
        // after fetching the snapshot, so a stamp later than the observation
        // watermark is normal and must verify.
        corporate_action_continuity(
            &config,
            &baseline,
            &stamped("2026-08-16T13:00:00Z"),
            1,
            verified_now(),
        )
        .unwrap();

        // Stamped after the verification clock: not a transition this backup
        // can have produced.
        let error = corporate_action_continuity(
            &config,
            &baseline,
            &stamped("2026-08-18T00:00:00Z"),
            1,
            verified_now(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("no observation produces"), "{error}");
    }

    #[test]
    fn an_opened_window_must_pin_what_the_backup_observed() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let event = fixture_event();
        let observed = ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        };
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.last_token_a_identity = Some(observed.clone());
        baseline.last_token_identity_at = Some("2026-08-15T23:00:00Z".parse().unwrap());
        let mut current = continuity_state(8, ("1", "1"));
        let opened = ArcusSpotCorporateActionProgress {
            event_id: event.event_id.clone(),
            blocked_at: "2026-08-16T00:00:01Z".parse().unwrap(),
            pre_event_token_a: Some(observed.clone()),
            pre_event_token_b: None,
            history_invalidated_at: None,
            fingerprint: event.fingerprint(),
            effective_at: Some(event.effective_at),
            symbols: event.symbols.clone(),
        };
        current.corporate_action = Some(opened.clone());
        corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();

        // A pin the backup never observed.
        let mut forged = current.clone();
        forged.corporate_action.as_mut().unwrap().pre_event_token_a =
            Some(ArcusSpotTokenIdentity {
                address: "0xdeadbeef00000000000000000000000000000000".to_string(),
                ..observed.clone()
            });
        let error = corporate_action_continuity(&config, &baseline, &forged, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("never observed"), "{error}");

        // The backup's identity came from inside the window: nothing may be
        // pinned.
        let mut late = baseline.clone();
        late.last_token_identity_at = Some("2026-08-16T00:00:00Z".parse().unwrap());
        let error = corporate_action_continuity(&config, &late, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("never observed"), "{error}");
    }

    #[test]
    fn a_renamed_completed_event_is_matched_by_fingerprint() {
        // The backup predates the resume; afterwards the operator renamed
        // the completed entry. Same fingerprint, new id.
        let mut config = config_with_corporate_action(Some(("4", "1")));
        let (baseline, current) = resume_pair();
        config.corporate_actions[0].event_id = "NVDA-2026-08-SPLIT-v2".to_string();
        let authorized =
            corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();
        assert!(authorized.resumed_inventory.is_some());

        // ... including across legacy padding: the backup has an id with no
        // fingerprint, so the resume pads before appending and the resumed
        // event's fingerprint is the *last* new entry, not the first.
        let (mut legacy_baseline, mut legacy_current) = resume_pair();
        legacy_baseline.handled_corporate_action_ids = vec!["OLDER".to_string()];
        legacy_baseline
            .handled_corporate_action_fingerprints
            .clear();
        legacy_current.handled_corporate_action_ids =
            vec!["OLDER".to_string(), "NVDA-2026-08-SPLIT".to_string()];
        legacy_current.handled_corporate_action_fingerprints =
            vec![String::new(), fixture_event().fingerprint()];
        let authorized = corporate_action_continuity(
            &config,
            &legacy_baseline,
            &legacy_current,
            1,
            verified_now(),
        )
        .unwrap();
        assert!(authorized.resumed_inventory.is_some());

        // A genuinely undeclared resume is still refused.
        let mut gone = config.clone();
        gone.corporate_actions.clear();
        let error = corporate_action_continuity(&gone, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not declare"), "{error}");
    }

    #[test]
    fn a_resume_after_a_legacy_record_pads_before_appending() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (mut baseline, mut current) = resume_pair();
        baseline.handled_corporate_action_ids = vec!["OLDER".to_string()];
        baseline.handled_corporate_action_fingerprints.clear();
        current.handled_corporate_action_ids =
            vec!["OLDER".to_string(), "NVDA-2026-08-SPLIT".to_string()];
        current.handled_corporate_action_fingerprints =
            vec![String::new(), fixture_event().fingerprint()];
        corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();

        // Appended without padding: the fingerprint sits beside OLDER, so
        // the resolved prefix no longer matches the backup's.
        let mut misaligned = current.clone();
        misaligned.handled_corporate_action_fingerprints = vec![fixture_event().fingerprint()];
        let error = corporate_action_continuity(&config, &baseline, &misaligned, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("lost or reordered"), "{error}");

        // Padded correctly but the appended fingerprint is not the declared
        // event's: that is what the alignment check is for.
        let mut wrong = current.clone();
        wrong.handled_corporate_action_fingerprints =
            vec![String::new(), "not-the-declared-event".to_string()];
        let error = corporate_action_continuity(&config, &baseline, &wrong, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("beside its id"), "{error}");
    }

    #[test]
    fn dropped_handled_fingerprints_are_rejected() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (mut baseline, mut current) = resume_pair();
        baseline.handled_corporate_action_ids = vec!["OLDER".to_string()];
        baseline.handled_corporate_action_fingerprints = vec!["older-fp".to_string()];
        current.handled_corporate_action_ids =
            vec!["OLDER".to_string(), "NVDA-2026-08-SPLIT".to_string()];
        // The older fingerprint is gone.
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("fingerprints"), "{error}");
    }

    #[test]
    fn dropped_or_reordered_handled_events_are_rejected() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (mut baseline, current) = resume_pair();
        baseline.handled_corporate_action_ids = vec!["SOMETHING-ELSE".to_string()];
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("lost or reordered"), "{error}");
    }

    #[test]
    fn a_stamped_discard_that_did_not_empty_the_window_is_rejected() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.corporate_action = Some(ArcusSpotCorporateActionProgress {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            blocked_at: "2026-08-16T00:00:01Z".parse().unwrap(),
            pre_event_token_a: None,
            pre_event_token_b: None,
            history_invalidated_at: Some("2026-08-16T02:00:01Z".parse().unwrap()),
            fingerprint: fixture_event().fingerprint(),
            effective_at: Some(fixture_event().effective_at),
            symbols: vec!["NVDA".to_string()],
        });
        assert!(!current.relative_log_price_history.is_empty());
        let error = corporate_action_continuity(&config, &baseline, &current, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("stamped a discard it did not perform"),
            "{error}"
        );
    }

    #[test]
    fn a_genuine_discard_authorizes_the_emptied_window() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.relative_log_price_history.clear();
        current.corporate_action = Some(ArcusSpotCorporateActionProgress {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            blocked_at: "2026-08-16T00:00:01Z".parse().unwrap(),
            pre_event_token_a: None,
            pre_event_token_b: None,
            history_invalidated_at: Some("2026-08-16T02:00:01Z".parse().unwrap()),
            fingerprint: fixture_event().fingerprint(),
            effective_at: Some(fixture_event().effective_at),
            symbols: vec!["NVDA".to_string()],
        });
        let authorized =
            corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();
        assert!(authorized.history_discarded);
        assert_eq!(authorized.resumed_inventory, None);
        // And that is exactly what lets the history check accept it.
        require_signal_history_continuity(
            &baseline.relative_log_price_history,
            &current.relative_log_price_history,
            1,
            96,
            &authorized,
        )
        .unwrap();
        require_signal_history_continuity(
            &baseline.relative_log_price_history,
            &current.relative_log_price_history,
            1,
            96,
            &ArcusSpotCorporateActionContinuity::default(),
        )
        .unwrap_err();
    }

    /// A verification clock comfortably after every fixture timestamp.
    fn verified_now() -> DateTime<Utc> {
        "2026-08-17T00:00:00Z".parse().unwrap()
    }

    fn stale_unit_progress() -> ArcusSpotCorporateActionProgress {
        ArcusSpotCorporateActionProgress {
            event_id: "NVDA-2026-08-SPLIT".to_string(),
            blocked_at: "2026-08-16T00:00:01Z".parse().unwrap(),
            // Pinned: an unpinned window is the "cannot verify" shape, which
            // the runtime treats as suppressing halt engagement, and these
            // fixtures are about the ordinary stale-unit phase.
            pre_event_token_a: Some(ArcusSpotTokenIdentity {
                symbol: "NVDA".to_string(),
                address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
                decimals: 18,
            }),
            pre_event_token_b: None,
            history_invalidated_at: Some("2026-08-16T02:00:01Z".parse().unwrap()),
            fingerprint: fixture_event().fingerprint(),
            effective_at: Some(fixture_event().effective_at),
            symbols: vec!["NVDA".to_string()],
        }
    }

    #[test]
    fn the_stale_unit_phase_does_not_demand_a_halt() {
        // The runtime declines to engage a halt between `effective_at` and
        // the resume, because the wallet-vs-basket gap is priced in units
        // the venue no longer quotes. This verifier re-derives that same
        // loss, so without the matching exception every valid backup
        // spanning such a tick is rejected for "omitting" a halt the
        // runtime was right not to engage.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        // A $10 daily loss against the $2 limit -- measured in pre-event
        // units at post-event prices.
        current.last_equity_usd = Some(Decimal::from(290));
        current.corporate_action = Some(stale_unit_progress());
        // The transition rules are covered elsewhere; here the window is
        // open on both sides so only the halt expectation is under test.
        let mut baseline = baseline;
        baseline.corporate_action = current.corporate_action.clone();
        let none = ArcusSpotCorporateActionContinuity::default();
        require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap();

        // An ordinary tick with the same loss still has to have halted.
        let mut ordinary = continuity_state(8, ("1", "1"));
        ordinary.last_equity_usd = Some(Decimal::from(290));
        let error = require_risk_state_continuity(
            &config, &baseline, &ordinary, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );

        // And the exemption is not something a checkpoint can mint for
        // itself: the progress must name an event the approved config
        // declares, and one the runtime has not already handled.
        let mut undeclared = config.clone();
        undeclared.corporate_actions.clear();
        // (Without a recorded cutoff: an orphan *with* one is the shape the
        // runtime writes itself and is exempt -- see below.)
        let mut no_cutoff = current.clone();
        no_cutoff.corporate_action.as_mut().unwrap().effective_at = None;
        let mut no_cutoff_baseline = baseline.clone();
        no_cutoff_baseline.corporate_action = no_cutoff.corporate_action.clone();
        let error = require_risk_state_continuity(
            &undeclared,
            &no_cutoff_baseline,
            &no_cutoff,
            1,
            not_before,
            not_after,
            &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );
        // ... unless the orphaned record carries its own cutoff and a stamp
        // at or after it -- the one shape the runtime writes without a
        // declaration -- and is not already handled.
        let mut orphaned = current.clone();
        orphaned.corporate_action.as_mut().unwrap().effective_at =
            Some("2026-08-16T02:00:00Z".parse().unwrap());
        require_risk_state_continuity(
            &undeclared,
            &baseline,
            &orphaned,
            1,
            not_before,
            not_after,
            &none,
        )
        .unwrap();
        let mut inconsistent = orphaned.clone();
        inconsistent.corporate_action.as_mut().unwrap().effective_at =
            Some("2026-08-16T02:30:00Z".parse().unwrap());
        require_risk_state_continuity(
            &undeclared,
            &baseline,
            &inconsistent,
            1,
            not_before,
            not_after,
            &none,
        )
        .unwrap_err();

        // A genuinely handled record -- resolved, with the declared event's
        // fingerprint beside its id -- gets no exemption. (An id with no
        // fingerprint is an unresolved legacy record, which the runtime and
        // this verifier both read as a reused label.)
        let mut handled = current.clone();
        handled.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        handled.handled_corporate_action_fingerprints = vec![fixture_event().fingerprint()];
        let error = require_risk_state_continuity(
            &config, &baseline, &handled, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );
    }

    #[test]
    fn a_reused_ids_stale_phase_does_not_demand_a_halt_either() {
        // A distinct later action under a handled id never gets a progress
        // record, but the runtime treats its units as stale from its cutoff
        // and declines to engage a halt. The verifier must agree, or every
        // backup from that phase is rejected for "omitting" the halt.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.last_equity_usd = Some(Decimal::from(290));
        current.corporate_action = None;
        // The id is handled, but under a different fingerprint: the config
        // entry is a new action wearing the old label.
        current.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        current.handled_corporate_action_fingerprints = vec!["an-older-event".to_string()];
        let mut baseline = baseline;
        baseline.handled_corporate_action_ids = current.handled_corporate_action_ids.clone();
        baseline.handled_corporate_action_fingerprints =
            current.handled_corporate_action_fingerprints.clone();
        // last_observation_at (12:00Z) is past the fixture's effective_at.
        let none = ArcusSpotCorporateActionContinuity::default();
        require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap();

        // Before that cutoff the halt is still demanded.
        let mut early = current.clone();
        early.last_observation_at = Some("2026-08-16T01:00:00Z".parse().unwrap());
        let error = require_risk_state_continuity(
            &config, &baseline, &early, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );
    }

    #[test]
    fn an_unresolved_legacy_record_reads_as_reused_in_the_stale_check() {
        // The runtime classifies a handled id with no aligned fingerprint as
        // a reused label, records progress and suppresses the halt after the
        // cutoff. This verifier has to read it the same way, or a valid
        // checkpoint from that phase is rejected for omitting the halt.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.last_equity_usd = Some(Decimal::from(290));
        current.corporate_action = None;
        for state in [&mut baseline, &mut current] {
            state.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
            state.handled_corporate_action_fingerprints.clear();
        }
        require_risk_state_continuity(
            &config,
            &baseline,
            &current,
            1,
            not_before,
            not_after,
            &ArcusSpotCorporateActionContinuity::default(),
        )
        .unwrap();
    }

    #[test]
    fn a_pre_cutoff_priced_mark_still_owes_its_halt() {
        // The tick that first crosses the cutoff stamps with the evaluation
        // clock, but the runtime judged the halt on the price clock. If the
        // prices predate the cutoff a genuine halt may have engaged, so the
        // exemption must not be granted on the stamp alone.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.last_equity_usd = Some(Decimal::from(290));
        current.corporate_action = Some(stale_unit_progress());
        baseline.corporate_action = current.corporate_action.clone();
        let none = ArcusSpotCorporateActionContinuity::default();

        // Prices from before the cutoff (02:00Z): the halt is still owed.
        for state in [&mut baseline, &mut current] {
            state.last_reference_price_at = Some("2026-08-16T01:59:00Z".parse().unwrap());
        }
        let error = require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );

        // Prices from after it: the runtime suppressed, and so does this.
        for state in [&mut baseline, &mut current] {
            state.last_reference_price_at = Some("2026-08-16T02:01:00Z".parse().unwrap());
        }
        require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap();
    }

    #[test]
    fn a_reused_ids_stale_phase_is_judged_on_the_price_clock() {
        // Same rule as the progress-record path: on the tick that first
        // crosses the cutoff a pre-cutoff mark may have engaged a genuine
        // halt, so the exemption cannot come from the observation watermark.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.last_equity_usd = Some(Decimal::from(290));
        current.corporate_action = None;
        for state in [&mut baseline, &mut current] {
            state.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
            state.handled_corporate_action_fingerprints = vec!["an-older-event".to_string()];
        }
        let none = ArcusSpotCorporateActionContinuity::default();

        // Prices from before the cutoff (02:00Z): the halt is still owed.
        for state in [&mut baseline, &mut current] {
            state.last_reference_price_at = Some("2026-08-16T01:59:00Z".parse().unwrap());
        }
        let error = require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );

        // Prices from after it: the runtime suppressed, and so does this.
        for state in [&mut baseline, &mut current] {
            state.last_reference_price_at = Some("2026-08-16T02:01:00Z".parse().unwrap());
        }
        require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap();
    }

    #[test]
    fn a_drifted_identity_does_not_owe_a_halt() {
        // The runtime declines to halt on a mark whose quantity and price
        // belong to different contracts, so this must not demand one.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let pinned = ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 18,
        };
        let mut baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.last_equity_usd = Some(Decimal::from(290));
        let mut progress = stale_unit_progress();
        progress.history_invalidated_at = None;
        progress.pre_event_token_a = Some(pinned.clone());
        current.corporate_action = Some(progress.clone());
        baseline.corporate_action = Some(progress);
        for state in [&mut baseline, &mut current] {
            state.last_token_a_identity = Some(pinned.clone());
        }
        let none = ArcusSpotCorporateActionContinuity::default();

        // Identity intact: the halt is owed.
        let error = require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );

        // Repointed: the mark is mixed-unit and no halt is owed.
        current.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            address: "0xdeadbeef00000000000000000000000000000000".to_string(),
            ..pinned
        });
        require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap();
    }

    #[test]
    fn identity_comparison_matches_the_runtimes_rule() {
        // The runtime compares address case-insensitively and ignores the
        // symbol; a derived `!=` disagreed on checksum casing, and on a
        // missing pin the runtime suppresses while this used to demand a
        // halt. Both let a rollback drop a genuine sticky halt.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.last_equity_usd = Some(Decimal::from(290));
        let mut progress = stale_unit_progress();
        progress.history_invalidated_at = None;
        current.corporate_action = Some(progress.clone());
        baseline.corporate_action = Some(progress);
        let none = ArcusSpotCorporateActionContinuity::default();

        // Same contract, lower-cased and with a different symbol string:
        // the runtime sees no drift and halts, so this must demand one.
        current.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            symbol: "nvda.us".to_string(),
            address: "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec".to_string(),
            decimals: 18,
        });
        let error = require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("omitted a newly triggered loss halt"),
            "{error}"
        );

        // Different decimals is a different instrument.
        current.last_token_a_identity = Some(ArcusSpotTokenIdentity {
            symbol: "NVDA".to_string(),
            address: "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC".to_string(),
            decimals: 6,
        });
        require_risk_state_continuity(
            &config, &baseline, &current, 1, not_before, not_after, &none,
        )
        .unwrap();

        // No pin: the runtime cannot verify the mark and does not engage.
        let mut unpinned = current.clone();
        unpinned
            .corporate_action
            .as_mut()
            .unwrap()
            .pre_event_token_a = None;
        let mut unpinned_baseline = baseline.clone();
        unpinned_baseline.corporate_action = unpinned.corporate_action.clone();
        require_risk_state_continuity(
            &config,
            &unpinned_baseline,
            &unpinned,
            1,
            not_before,
            not_after,
            &none,
        )
        .unwrap();
    }

    #[test]
    fn a_resolved_record_may_not_be_blanked_at_the_same_sequence() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let mut baseline = continuity_state(7, ("1", "1"));
        baseline.handled_corporate_action_ids = vec!["NVDA-2026-08-SPLIT".to_string()];
        baseline.handled_corporate_action_fingerprints = vec![fixture_event().fingerprint()];
        baseline.handled_corporate_actions_resolved = true;
        corporate_action_continuity(&config, &baseline, &baseline.clone(), 0, verified_now())
            .unwrap();

        // Blank the slot but keep the marker: after a restore `from_state`
        // skips the backfill and the declaration becomes a reused id. The
        // raw prefix comparison catches it, because the backup says the
        // record is already resolved.
        let mut blanked = baseline.clone();
        blanked.handled_corporate_action_fingerprints = vec![String::new()];
        let error = corporate_action_continuity(&config, &baseline, &blanked, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("lost or reordered"), "{error}");

        // Un-resolving a resolved record is not a transition either.
        let mut unresolved = baseline.clone();
        unresolved.handled_corporate_actions_resolved = false;
        let error = corporate_action_continuity(&config, &baseline, &unresolved, 0, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("resolution changed"), "{error}");

        // The same blanking one observation later is caught by the
        // append-only prefix, which now compares raw slots once the backup
        // says the record is resolved.
        let mut later = blanked.clone();
        later.sequence = 8;
        let error = corporate_action_continuity(&config, &baseline, &later, 1, verified_now())
            .unwrap_err()
            .to_string();
        assert!(error.contains("lost or reordered"), "{error}");
    }

    #[test]
    fn the_stale_unit_phase_still_rejects_an_unexpected_halt() {
        // Only the omission is excused. A halt that appeared without a
        // derivable loss is still an unexplained change.
        let not_before: DateTime<Utc> = "2026-08-16T11:00:00Z".parse().unwrap();
        let not_after: DateTime<Utc> = "2026-08-16T13:00:00Z".parse().unwrap();
        let config = config_with_corporate_action(Some(("4", "1")));
        let baseline = continuity_state(7, ("1", "1"));
        let mut current = continuity_state(8, ("1", "1"));
        current.corporate_action = Some(stale_unit_progress());
        current.risk_halt = Some(ArcusSpotRiskHalt {
            kind: ArcusSpotRiskHaltKind::DailyLoss,
            engaged_at: "2026-08-16T12:00:00Z".parse().unwrap(),
            equity_usd: Decimal::from(300),
            loss_usd: Decimal::from(5),
            limit_usd: Decimal::from(2),
        });
        let error = require_risk_state_continuity(
            &config,
            &baseline,
            &current,
            1,
            not_before,
            not_after,
            &ArcusSpotCorporateActionContinuity::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unexpected risk halt"), "{error}");
    }

    #[test]
    fn a_same_tick_discard_and_resume_authorizes_its_one_fresh_sample() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (mut baseline, current) = resume_pair();
        // The backup was taken after the reconciled config landed but before
        // any tick ran inside the window, so it still holds the pre-event
        // history and carries no progress stamp. The resume tick then does
        // both: it discards that window and appends its own post-event
        // sample.
        baseline.relative_log_price_history = vec![0.10, 0.11, 0.12];
        baseline.corporate_action = None;
        let authorized =
            corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();
        assert!(authorized.history_discarded);
        assert!(authorized.resumed_inventory.is_some());
        assert_eq!(current.relative_log_price_history.len(), 1);
        require_signal_history_continuity(
            &baseline.relative_log_price_history,
            &current.relative_log_price_history,
            1,
            96,
            &authorized,
        )
        .unwrap();
        // Only the resume earns that sample: a discard-only tick that
        // carries one is still the "stamped a discard it did not perform"
        // shape, and an undeclared transition is rejected outright.
        require_signal_history_continuity(
            &baseline.relative_log_price_history,
            &current.relative_log_price_history,
            1,
            96,
            &ArcusSpotCorporateActionContinuity {
                history_discarded: true,
                resumed_inventory: None,
            },
        )
        .unwrap_err();
        require_signal_history_continuity(
            &baseline.relative_log_price_history,
            &current.relative_log_price_history,
            1,
            96,
            &ArcusSpotCorporateActionContinuity::default(),
        )
        .unwrap_err();
    }

    #[test]
    fn a_resume_may_not_carry_more_than_one_fresh_sample() {
        let config = config_with_corporate_action(Some(("4", "1")));
        let (mut baseline, mut current) = resume_pair();
        baseline.relative_log_price_history = vec![0.10, 0.11, 0.12];
        baseline.corporate_action = None;
        current.relative_log_price_history = vec![0.25, 0.26];
        let authorized =
            corporate_action_continuity(&config, &baseline, &current, 1, verified_now()).unwrap();
        require_signal_history_continuity(
            &baseline.relative_log_price_history,
            &current.relative_log_price_history,
            1,
            96,
            &authorized,
        )
        .unwrap_err();
    }

    #[test]
    fn rebase_marks_must_be_the_reconciled_holding_priced_at_this_tick() {
        let (_, current) = resume_pair();
        let inventory = ArcusSpotInventory {
            token_a: Decimal::from(4),
            token_b: Decimal::ONE,
        };
        assert_eq!(
            require_corporate_action_rebase_marks(&current, inventory).unwrap(),
            Decimal::from(900),
        );

        let mut drifted = current.clone();
        drifted.daily_baseline_equity_usd = Some(Decimal::from(901));
        let error = require_corporate_action_rebase_marks(&drifted, inventory)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("must all be the reconciled holding"),
            "{error}"
        );
    }
}
