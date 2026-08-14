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
use chrono::Utc;
use debot::arcus_spot::{
    build_arcus_spot_kms_signer, ArcusSpotChainClient, ArcusSpotChainConfig, ArcusSpotDecision,
    ArcusSpotExecutionAttempt, ArcusSpotExecutionLedgerStore, ArcusSpotExecutionPhase,
    ArcusSpotKmsConfig, ArcusSpotKmsSigner, ArcusSpotLiveExecutor, ArcusSpotLiveExecutorConfig,
    ArcusSpotRotationPlan, ArcusSpotRotationTrigger, ArcusSpotRuntimeCheckpointStore,
    ArcusSpotRuntimeConfig, ArcusSpotRuntimeEvent, ArcusSpotRuntimeMode,
};
use dex_connector::{
    ArcusSpotClient, ArcusSpotConfig, ArcusSpotRecorder, ArcusSpotRecorderConfig,
    ArcusSpotRecorderSnapshot,
};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SIGNATURE_LENGTH};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
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
            return Err(err)
                .with_context(|| format!("failed to open {label} {}", path.display()));
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
    let (mut file, metadata) =
        open_regular_file_no_follow(path, "auto-execute policy file")?;
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
    let (mut file, metadata) =
        open_regular_file_no_follow(path, "approval public key file")?;
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

fn usage() -> &'static str {
    "usage:
  arcus-spot-execute-once keygen PRIVATE_KEY_FILE
  arcus-spot-execute-once hash CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once hash-config CONFIG_YAML
  arcus-spot-execute-once sign-approval DIGEST PRIVATE_KEY_FILE
  arcus-spot-execute-once execute CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
  arcus-spot-execute-once auto-execute CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once resume CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
  arcus-spot-execute-once auto-resume CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once live-tick CONFIG_YAML

live-tick is the unattended-probe entry point: it fetches exactly one live
snapshot itself (the same public, read-only recorder client
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
reweight the z-score history. Before dispatching, it durably writes the
plan it built to
<runtime_state_path's directory>/live-tick-pending-plan.json (mode 0600);
if the process exits after Submitted but before confirmation, recover with
auto-resume CONFIG_YAML <that path>.

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
    let normalized = lexically_normalize(path);
    let (Some(parent), Some(file_name)) = (normalized.parent(), normalized.file_name()) else {
        return normalized;
    };
    // Walk up from `parent` toward the root, canonicalizing the longest
    // existing ancestor and reappending whatever components don't exist
    // yet on top of it. Trying only the immediate parent (and giving up
    // entirely if that one doesn't exist yet) misses a symlink further up
    // the tree whenever the leaf directory itself hasn't been created --
    // e.g. `/var/lib/alias -> real`, an absent `real/new`, and paths under
    // `alias/new/...` vs. `real/new/...`: the immediate parent (`.../new`)
    // doesn't exist under either name, but `alias` itself does and
    // resolves to `real` (Codex P2 follow-up, pairtrade#186).
    let mut ancestor = parent;
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
                _ => return normalized,
            },
        }
    };
    let mut resolved = canonical_ancestor;
    for component in pending_components.into_iter().rev() {
        resolved.push(component);
    }
    resolved.push(file_name);
    resolved
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
    let pending_plan_path =
        resolve_path_for_collision_check(&live_tick_pending_plan_path(config)?);
    if pending_plan_path == ledger_path || pending_plan_path == runtime_state_path {
        bail!(
            "Arcus ledger_path/runtime_state_path must not resolve to the derived live-tick pending-plan path {}",
            pending_plan_path.display()
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
    let bytes = hex::decode(hex_key.trim())
        .context("approval_public_key must be hex-encoded")?;
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
    let plan: ArcusSpotRotationPlan = serde_json::from_slice(&plan_bytes)
        .with_context(|| format!("invalid plan {}", plan_path.display()))?;
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
    verify_approval_signature(&computed_digest, approval_public_key, approval_signature_hex)?;
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
        .context("approval signature does not verify against approval_public_key for this exact config+plan")
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
        bail!("unsupported approval key file cipher {:?}", encrypted.cipher);
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
            let plan_config_digest =
                require_approval_signature(&config, &plan, &approval_public_key, approval_signature)?;
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
            let runtime_store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
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
            let runtime_store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
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
                &format!(
                    "{}/{}",
                    config.runtime.pair.sell_symbol, config.runtime.pair.buy_symbol
                ),
                &config.runtime.notional_usd.normalize().to_string(),
            )
            .context(
                "failed to build a single-pair recorder config from the runtime pair/notional",
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

            let mut runtime = store.load_or_create(&config.runtime)?;
            // step_at itself rejects a snapshot whose collection_finished_at
            // is not strictly newer than the last one it genuinely advanced
            // on -- tracked in the checkpointed state, under this same
            // lock, rather than in any caller-local bookkeeping, so it
            // correctly orders this invocation against a concurrent
            // arcus-spot-propose-plan (or another live-tick) writing the
            // same checkpoint (Codex P2 follow-up, pairtrade#186; see
            // ArcusSpotRuntimeState::last_observation_at's doc comment).
            let event = runtime.step_at(&snapshot, Utc::now());
            // Persisted unconditionally, independent of the decision below:
            // the accumulated price-history window is exactly what next
            // tick's signal depends on, and losing a tick's contribution
            // because this run happened not to rotate would silently widen
            // gaps in the very history the entry/exit z-score needs.
            store.persist(&runtime)?;
            drop(checkpoint_lock);

            let plan = match event.decision.clone() {
                ArcusSpotDecision::WouldRotate { plan } => plan,
                ArcusSpotDecision::Observe { .. } | ArcusSpotDecision::SimulatedFill { .. } => {
                    return write_live_tick_event(&event);
                }
            };
            let plan_config_digest = approval_digest(&config, &plan)?;
            // Durably record the plan before dispatching it: unlike
            // execute/auto-execute, live-tick builds this plan itself
            // rather than receiving it as an argument the caller already
            // holds a copy of, so without this write, a crash or exit
            // between submission and confirmation leaves nothing for
            // `auto-resume` to recover with (Codex P2 follow-up,
            // pairtrade#186).
            let pending_plan_path = live_tick_pending_plan_path(&config)?;
            let plan_bytes = serde_json::to_vec_pretty(&plan)
                .context("failed to serialize Arcus live-tick plan")?;
            write_private_regular_file_atomic(&pending_plan_path, &plan_bytes)?;
            let mut executor = executor_from_config(&config).await?;
            // Re-read fresh, same reasoning as `execute`'s own comment
            // above: the plan above was computed before the ledger lock
            // (acquired inside executor_from_config) was held, so another
            // overlapping invocation could have advanced the checkpoint in
            // between.
            let runtime_store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            let fresh_runtime = runtime_store.load_or_create(&config.runtime)?;
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
            let plan_config_digest =
                require_approval_signature(&config, &plan, &approval_public_key, approval_signature)?;
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
        let decrypted =
            decrypt_signing_key(&encrypted, b"correct horse battery staple").unwrap();
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
        assert!(error.to_string().contains("owned by this process's own uid"));
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


    fn rotation_plan(trigger: &str) -> ArcusSpotRotationPlan {
        serde_json::from_value(serde_json::json!({
            "direction": "token_a_to_token_b",
            "trigger": trigger,
            "sell_symbol": "NVDA",
            "buy_symbol": "AMD",
            "sell_token_address": "0xd0601CE157Db5bdC3162BbaC2a2C8aF5320D9EEC",
            "buy_token_address": "0x86923f96303D656E4aa86D9d42D1e57ad2023fdC",
            "sell_quantity": "0.1",
            "buy_quantity": "0.05",
            "sell_amount_raw": "100000000000000000",
            "buy_amount_raw": "50000000000000000",
            "venue": "arcus",
            "quote_received_at": "2026-08-14T00:00:00Z",
            "optimistic_round_trip_loss_bps": "76.5",
            "gas_buffer_bps": "10",
            "settlement_buffer_bps": "10",
            "all_in_round_trip_cost_bps": "96.5",
            "predicted_inventory": {"token_a": "0.25", "token_b": "0.21"},
            "predicted_inventory_imbalance_fraction": "0.1",
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
        let config = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&config);
        require_config_within_auto_execute_policy(&config, &policy).unwrap();
    }

    #[test]
    fn auto_execute_policy_rejects_a_redirected_ledger_path() {
        // Without this, the executor identity could point ledger_path at a
        // fresh, empty file to silently reset the daily swap count and
        // prior attempt history that the real ledger accumulates.
        let approved = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config("/tmp/attacker-chosen/ledger.json", "/var/lib/x/runtime.json", "1000");
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match the administrator-approved configuration"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_redirected_runtime_state_path() {
        let approved = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config("/var/lib/x/ledger.json", "/tmp/attacker-chosen/runtime.json", "1000");
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match the administrator-approved configuration"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_sell_ceiling_raised_past_the_administrator_approved_value() {
        let approved = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy_for(&approved);
        let config = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "999999999999");
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
        let approved =
            execute_once_config_with_daily_cap("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000", 10);
        let policy = auto_execute_policy_for(&approved);
        let config =
            execute_once_config_with_daily_cap("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000", 999);
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

        let event = runtime.step_at(&snapshot, Utc::now());
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
