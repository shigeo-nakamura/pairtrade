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
    ArcusSpotRotationPlan, ArcusSpotRuntime, ArcusSpotRuntimeConfig, ArcusSpotRuntimeEvent,
    ArcusSpotRuntimeMode, ArcusSpotRuntimeState,
};
use dex_connector::{ArcusSpotClient, ArcusSpotConfig, ArcusSpotRecorderSnapshot};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SIGNATURE_LENGTH};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    fs::{File, OpenOptions},
    io::{self, Write},
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

/// Administrator-set ceiling for `auto-execute`/`auto-resume`, enforced
/// independently of whatever CONFIG_YAML the executor identity supplies.
/// Read from the same fixed, non-self-writable path pattern as
/// `approval_public_key` (Codex P1 follow-up, pairtrade#186): without
/// this, the executor identity could point `ledger_path`/
/// `runtime_state_path` at a fresh location to reset the daily swap count
/// and prior history, or raise `maximum_sell_amount_raw` past what was
/// actually approved on bot-strategy#772 -- the offline signature
/// `execute` requires is exactly what used to make those caller-controlled
/// config values safe.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArcusSpotAutoExecutePolicy {
    ledger_path: PathBuf,
    runtime_state_path: PathBuf,
    max_sell_amount_raw: std::collections::BTreeMap<String, String>,
}

fn auto_execute_policy_from_admin_file() -> Result<ArcusSpotAutoExecutePolicy> {
    auto_execute_policy_from_file(Path::new(AUTO_EXECUTE_POLICY_PATH))
}

fn auto_execute_policy_from_file(path: &Path) -> Result<ArcusSpotAutoExecutePolicy> {
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!("failed to inspect auto-execute policy file {}", path.display())
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "auto-execute policy file {} must be a regular non-symlink file",
            path.display()
        );
    }
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
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read auto-execute policy file {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("invalid auto-execute policy file {}", path.display()))
}

/// Rejects a config whose caller-controlled `ledger_path`/
/// `runtime_state_path`/`maximum_sell_amount_raw` fall outside what the
/// administrator-owned policy actually approved, closing exactly the gap
/// `execute`'s signature used to close (Codex P1 follow-up, pairtrade#186).
fn require_config_within_auto_execute_policy(
    config: &ArcusSpotExecuteOnceConfig,
    policy: &ArcusSpotAutoExecutePolicy,
) -> Result<()> {
    if config.ledger_path != policy.ledger_path {
        bail!(
            "auto-execute config ledger_path {} does not match the administrator-approved path {}",
            config.ledger_path.display(),
            policy.ledger_path.display()
        );
    }
    if config.runtime_state_path != policy.runtime_state_path {
        bail!(
            "auto-execute config runtime_state_path {} does not match the administrator-approved path {}",
            config.runtime_state_path.display(),
            policy.runtime_state_path.display()
        );
    }
    for (symbol, configured_max) in &config.executor.maximum_sell_amount_raw {
        let ceiling_raw = policy.max_sell_amount_raw.get(symbol).ok_or_else(|| {
            anyhow::anyhow!(
                "auto-execute config sets maximum_sell_amount_raw for {symbol}, which has no administrator-approved ceiling"
            )
        })?;
        let configured_value: u128 = configured_max.parse().with_context(|| {
            format!("auto-execute config maximum_sell_amount_raw for {symbol} is not a valid raw amount")
        })?;
        let ceiling_value: u128 = ceiling_raw.parse().with_context(|| {
            format!("auto-execute policy max_sell_amount_raw for {symbol} is not a valid raw amount")
        })?;
        if configured_value > ceiling_value {
            bail!(
                "auto-execute config maximum_sell_amount_raw for {symbol} ({configured_value}) exceeds the administrator-approved ceiling ({ceiling_value})"
            );
        }
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
    let metadata = fs::symlink_metadata(path).with_context(|| {
        format!("failed to inspect approval public key file {}", path.display())
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "approval public key file {} must be a regular non-symlink file",
            path.display()
        );
    }
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
    let raw = fs::read_to_string(path)
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
  arcus-spot-execute-once keygen PRIVATE_KEY_FILE
  arcus-spot-execute-once hash CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once sign-approval DIGEST PRIVATE_KEY_FILE
  arcus-spot-execute-once execute CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
  arcus-spot-execute-once auto-execute CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once resume CONFIG_YAML PLAN_JSON APPROVAL_SIGNATURE_HEX
  arcus-spot-execute-once auto-resume CONFIG_YAML PLAN_JSON
  arcus-spot-execute-once live-tick CONFIG_YAML RECORDER_SNAPSHOT_JSON

live-tick is the unattended-probe entry point: it evaluates the strategy
signal (ArcusSpotRuntime::step_at) against one fresh recorder snapshot,
always persists the resulting runtime checkpoint, and only when that
genuinely decides WouldRotate does it build and dispatch a plan -- through
the same policy-gated, signatureless path as auto-execute. Meant to be
invoked on a timer shortly after each recorder snapshot lands; most ticks
decide Observe and touch neither the KMS signer nor the network.

auto-execute/auto-resume/live-tick skip the offline human approval signature
(explicit owner decision while total inventory at risk stays small -- see
the comment at their call sites). Every other gate execute/resume enforce
is unchanged: plan/config validation, staleness, on-chain preflight,
exact-value Permit2, slippage, and loss stops. In place of the signature,
CONFIG_YAML's ledger_path, runtime_state_path, and maximum_sell_amount_raw
must match an administrator-owned policy file at
/etc/arcus-spot/auto_execute_policy.json (same ownership/mode trust model
as approval_public_key) -- otherwise the executor identity could bypass
the daily swap cap and stakes ceiling by supplying fresh values itself.

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
        [command, config_path, snapshot_path] if command == "live-tick" => {
            // The unattended-probe entry point: evaluate the strategy
            // signal against one fresh recorder snapshot and, only if it
            // genuinely fires, dispatch through the exact same
            // policy-gated, signatureless path as `auto-execute`. Most
            // ticks decide `Observe` (no position warranted) and never
            // touch the KMS signer or the network beyond the on-disk
            // snapshot already written by the recorder timer -- this is
            // the "future read-only daemon [that] must call step_at with
            // the current UTC time" flagged as not-yet-built in this same
            // doc (bot-strategy#772/#775, 7-day activity probe).
            let config_bytes = read_private_regular_file(Path::new(config_path), "config")?;
            let config = parse_config(&config_bytes, Path::new(config_path))?;
            let policy = auto_execute_policy_from_admin_file()?;
            require_config_within_auto_execute_policy(&config, &policy)?;

            let snapshot_bytes = read_private_regular_file(Path::new(snapshot_path), "recorder snapshot")?;
            let snapshot: ArcusSpotRecorderSnapshot = serde_json::from_slice(&snapshot_bytes)
                .with_context(|| format!("invalid recorder snapshot {snapshot_path}"))?;

            let store = ArcusSpotRuntimeCheckpointStore::new(config.runtime_state_path.clone());
            let mut runtime = store.load_or_create(&config.runtime)?;
            let event = runtime.step_at(&snapshot, Utc::now());
            // Persisted unconditionally, independent of the decision below:
            // the accumulated price-history window is exactly what next
            // tick's signal depends on, and losing a tick's contribution
            // because this run happened not to rotate would silently widen
            // gaps in the very history the entry/exit z-score needs.
            store.persist(&runtime)?;

            let plan = match event.decision.clone() {
                ArcusSpotDecision::WouldRotate { plan } => plan,
                ArcusSpotDecision::Observe { .. } | ArcusSpotDecision::SimulatedFill { .. } => {
                    return write_live_tick_event(&event);
                }
            };
            let plan_config_digest = approval_digest(&config, &plan)?;
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
        assert!(usage().contains("live-tick CONFIG_YAML RECORDER_SNAPSHOT_JSON"));
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
  max_swaps_per_utc_day: 10
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

    fn auto_execute_policy(ledger_path: &str, runtime_state_path: &str, max_sell_nvda: &str) -> ArcusSpotAutoExecutePolicy {
        ArcusSpotAutoExecutePolicy {
            ledger_path: PathBuf::from(ledger_path),
            runtime_state_path: PathBuf::from(runtime_state_path),
            max_sell_amount_raw: std::collections::BTreeMap::from([
                ("NVDA".to_string(), max_sell_nvda.to_string()),
                ("AMD".to_string(), "21084353605755395".to_string()),
            ]),
        }
    }

    #[test]
    fn auto_execute_policy_accepts_a_config_within_limits() {
        let config = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        require_config_within_auto_execute_policy(&config, &policy).unwrap();
    }

    #[test]
    fn auto_execute_policy_rejects_a_redirected_ledger_path() {
        // Without this, the executor identity could point ledger_path at a
        // fresh, empty file to silently reset the daily swap count and
        // prior attempt history that the real ledger accumulates.
        let config = execute_once_config("/tmp/attacker-chosen/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error.to_string().contains("ledger_path"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_redirected_runtime_state_path() {
        let config = execute_once_config("/var/lib/x/ledger.json", "/tmp/attacker-chosen/runtime.json", "1000");
        let policy = auto_execute_policy("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error.to_string().contains("runtime_state_path"));
    }

    #[test]
    fn auto_execute_policy_rejects_a_sell_ceiling_raised_past_the_administrator_limit() {
        let config = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "999999999999");
        let policy = auto_execute_policy("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error.to_string().contains("maximum_sell_amount_raw"));
    }

    #[test]
    fn auto_execute_policy_accepts_a_sell_ceiling_at_or_below_the_limit() {
        let config = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = auto_execute_policy("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        require_config_within_auto_execute_policy(&config, &policy).unwrap();
    }

    #[test]
    fn auto_execute_policy_rejects_a_symbol_with_no_administrator_ceiling() {
        let config = execute_once_config("/var/lib/x/ledger.json", "/var/lib/x/runtime.json", "1000");
        let policy = ArcusSpotAutoExecutePolicy {
            ledger_path: PathBuf::from("/var/lib/x/ledger.json"),
            runtime_state_path: PathBuf::from("/var/lib/x/runtime.json"),
            max_sell_amount_raw: std::collections::BTreeMap::new(),
        };
        let error = require_config_within_auto_execute_policy(&config, &policy).unwrap_err();
        assert!(error.to_string().contains("no administrator-approved ceiling"));
    }

    #[test]
    fn auto_execute_policy_file_owned_by_this_process_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auto_execute_policy.json");
        fs::write(&path, r#"{"ledger_path":"/x","runtime_state_path":"/y","max_sell_amount_raw":{}}"#).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let error = auto_execute_policy_from_file(&path).unwrap_err();
        assert!(error.to_string().contains("administrator-owned"));
    }

    #[test]
    fn auto_execute_policy_file_that_is_group_writable_is_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auto_execute_policy.json");
        fs::write(&path, r#"{"ledger_path":"/x","runtime_state_path":"/y","max_sell_amount_raw":{}}"#).unwrap();
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
        fs::write(&target, r#"{"ledger_path":"/x","runtime_state_path":"/y","max_sell_amount_raw":{}}"#).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("auto_execute_policy.json");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = auto_execute_policy_from_file(&link).unwrap_err();
        assert!(error.to_string().contains("non-symlink"));
    }
}
