//! Durable, hash-chained Arcus live-tick runtime-event stream.
//!
//! Journald is operational evidence, not a replay archive: file-granular
//! retention once removed the first 3.5 days of the Arcus probe
//! (bot-strategy#825/#854). This store writes every live-tick decision to
//! private daily JSONL segments next to the runtime checkpoint.

use super::{ArcusSpotDecision, ArcusSpotHoldCode, ArcusSpotRuntimeEvent};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

const EVENT_STREAM_SCHEMA_VERSION: u32 = 1;
const PENDING_EVENT_SCHEMA_VERSION: u32 = 1;
const EVENT_STREAM_DOMAIN: &[u8] = b"arcus-live-tick-event-stream-v1\0";
const SHA256_PREFIX: &str = "sha256:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArcusSpotLiveTickEventRecord {
    pub schema_version: u32,
    pub previous_chain_sha256: Option<String>,
    pub event_sha256: String,
    pub chain_sha256: String,
    /// Exact compact JSON bytes hashed by event_sha256. Keeping the canonical
    /// payload as a string lets non-Rust verifiers recompute the hash without
    /// depending on their own float serializer.
    pub event_json: String,
}

#[derive(Debug, Clone)]
struct VerifiedRecord {
    record: ArcusSpotLiveTickEventRecord,
    event: ArcusSpotRuntimeEvent,
}

#[derive(Debug, Clone)]
pub struct ArcusSpotLiveTickEventStream {
    directory: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ArcusSpotPendingEventDocument {
    schema_version: u32,
    event_sha256: String,
    event_json: String,
}

#[derive(Debug, Clone)]
pub struct ArcusSpotLiveTickEventPublisher {
    stream: ArcusSpotLiveTickEventStream,
    pending_path: PathBuf,
}

impl ArcusSpotLiveTickEventStream {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    fn latest_verified_record(&self) -> Result<Option<VerifiedRecord>> {
        self.ensure_private_directory()?;
        let segments = self.segment_paths()?;
        match segments.last() {
            Some(path) => Ok(self.verify_segment(path)?.pop()),
            None => Ok(None),
        }
    }

    /// Append one checkpointed event and fsync both data and a newly-created
    /// segment's directory entry.
    ///
    /// The caller holds the checkpoint lock. Normal writers use
    /// `ArcusSpotLiveTickEventPublisher`, which stages this exact payload before
    /// the checkpoint rename and can recover it if this append is interrupted.
    pub fn append(&self, event: &ArcusSpotRuntimeEvent) -> Result<ArcusSpotLiveTickEventRecord> {
        self.ensure_private_directory()?;
        let segment_name = segment_name(event.observed_at);
        let segment_path = self.directory.join(&segment_name);
        let segments = self.segment_paths()?;

        let previous = if let Some(latest) = segments.last() {
            let latest_name = latest
                .file_name()
                .and_then(|name| name.to_str())
                .context("Arcus event-stream segment name is not UTF-8")?;
            if latest_name > segment_name.as_str() {
                bail!(
                    "Arcus live-tick event time {} precedes latest stream segment {latest_name}",
                    event.observed_at
                );
            }
            self.verify_segment(latest)?.pop()
        } else {
            None
        };

        if let Some(previous) = &previous {
            validate_event_continuity(&previous.event, event)?;
        }

        let previous_chain_sha256 = previous.map(|item| item.record.chain_sha256);
        let record = event_record(event, previous_chain_sha256)?;

        let existed = segment_path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&segment_path)
            .with_context(|| {
                format!(
                    "failed to open Arcus event-stream segment {}",
                    segment_path.display()
                )
            })?;
        validate_private_regular_file(&file, &segment_path)?;
        let mut bytes =
            serde_json::to_vec(&record).context("failed to serialize Arcus event-stream record")?;
        bytes.push(b'\n');
        file.write_all(&bytes)?;
        file.sync_all()?;
        if !existed {
            File::open(&self.directory)?.sync_all()?;
        }
        Ok(record)
    }

    fn ensure_private_directory(&self) -> Result<()> {
        if self.directory.exists() {
            let metadata = fs::symlink_metadata(&self.directory).with_context(|| {
                format!(
                    "failed to inspect Arcus event-stream directory {}",
                    self.directory.display()
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!(
                    "Arcus event-stream path {} must be a non-symlink directory",
                    self.directory.display()
                );
            }
            if metadata.permissions().mode() & 0o077 != 0 {
                bail!(
                    "Arcus event-stream directory {} must not be accessible by group/other",
                    self.directory.display()
                );
            }
            return Ok(());
        }
        let parent = self
            .directory
            .parent()
            .context("Arcus event-stream directory has no parent")?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&self.directory)
            .with_context(|| {
                format!(
                    "failed to create Arcus event-stream directory {}",
                    self.directory.display()
                )
            })?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }

    fn segment_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("Arcus event-stream segment name is not UTF-8"))?;
            if !is_segment_name(&name) {
                bail!(
                    "unexpected file in Arcus event-stream directory: {}",
                    path.display()
                );
            }
            paths.push(path);
        }
        paths.sort();
        Ok(paths)
    }

    fn verify_segment(&self, path: &Path) -> Result<Vec<VerifiedRecord>> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open Arcus event stream {}", path.display()))?;
        validate_private_regular_file(&file, path)?;
        let mut bytes = Vec::new();
        (&file)
            .read_to_end(&mut bytes)
            .with_context(|| format!("failed to read {}", path.display()))?;
        verify_segment_bytes(path, &bytes)
    }

    /// Remove only a provably incomplete prefix of the exact staged record.
    /// Any other unterminated content remains a hard error.
    fn repair_incomplete_pending_append(
        &self,
        event: &ArcusSpotRuntimeEvent,
        pending_event_sha256: &str,
    ) -> Result<()> {
        self.ensure_private_directory()?;
        let segment_path = self.directory.join(segment_name(event.observed_at));
        let segments = self.segment_paths()?;
        let Some(segment_index) = segments.iter().position(|path| path == &segment_path) else {
            return Ok(());
        };
        if segment_index + 1 != segments.len() {
            bail!(
                "Arcus incomplete pending append is not in the latest stream segment {}",
                segment_path.display()
            );
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&segment_path)
            .with_context(|| {
                format!(
                    "failed to open Arcus event-stream segment {} for pending recovery",
                    segment_path.display()
                )
            })?;
        validate_private_regular_file(&file, &segment_path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if bytes.ends_with(b"\n") {
            // The interrupted append may have written the complete record but
            // failed during sync. Re-establish durability before recovery is
            // allowed to clear the only staged copy.
            file.sync_all()?;
            return Ok(());
        }

        let complete_len = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let complete_records = if complete_len == 0 {
            Vec::new()
        } else {
            verify_segment_bytes(&segment_path, &bytes[..complete_len])?
        };
        let previous = if let Some(previous) = complete_records.last() {
            Some(previous.clone())
        } else if segment_index > 0 {
            self.verify_segment(&segments[segment_index - 1])?.pop()
        } else {
            None
        };
        if let Some(previous) = &previous {
            validate_event_continuity(&previous.event, event)?;
        }
        let expected = event_record(event, previous.map(|item| item.record.chain_sha256))?;
        if expected.event_sha256 != pending_event_sha256 {
            bail!("Arcus incomplete append does not match the pending event hash");
        }
        let mut expected_bytes = serde_json::to_vec(&expected)
            .context("failed to serialize expected Arcus pending stream record")?;
        expected_bytes.push(b'\n');
        let incomplete = &bytes[complete_len..];
        if !expected_bytes.starts_with(incomplete) {
            bail!(
                "Arcus unterminated stream tail does not match the pending event at {}",
                segment_path.display()
            );
        }

        file.set_len(complete_len as u64)?;
        file.sync_all()?;
        drop(file);
        if complete_len == 0 {
            fs::remove_file(&segment_path)?;
            File::open(&self.directory)?.sync_all()?;
        }
        Ok(())
    }
}

fn verify_segment_bytes(path: &Path, bytes: &[u8]) -> Result<Vec<VerifiedRecord>> {
    if !bytes.ends_with(b"\n") {
        bail!(
            "Arcus event-stream segment {} has an unterminated final record",
            path.display()
        );
    }
    let mut verified: Vec<VerifiedRecord> = Vec::new();
    for (index, line) in bytes[..bytes.len() - 1]
        .split(|byte| *byte == b'\n')
        .enumerate()
    {
        if line.is_empty() {
            bail!(
                "empty Arcus event-stream record at {}:{}",
                path.display(),
                index + 1
            );
        }
        let record: ArcusSpotLiveTickEventRecord =
            serde_json::from_slice(line).with_context(|| {
                format!(
                    "invalid Arcus event-stream record at {}:{}",
                    path.display(),
                    index + 1
                )
            })?;
        let event = verify_record(&record).with_context(|| {
            format!(
                "invalid Arcus event-stream record at {}:{}",
                path.display(),
                index + 1
            )
        })?;
        if let Some(previous) = verified.last() {
            if record.previous_chain_sha256.as_deref()
                != Some(previous.record.chain_sha256.as_str())
            {
                bail!(
                    "Arcus event-stream hash-chain break at {}:{}",
                    path.display(),
                    index + 1
                );
            }
            validate_event_continuity(&previous.event, &event)?;
        }
        verified.push(VerifiedRecord { record, event });
    }
    Ok(verified)
}

impl ArcusSpotLiveTickEventPublisher {
    pub fn new(stream: ArcusSpotLiveTickEventStream, pending_path: PathBuf) -> Self {
        Self {
            stream,
            pending_path,
        }
    }

    pub fn stream(&self) -> &ArcusSpotLiveTickEventStream {
        &self.stream
    }

    pub fn pending_path(&self) -> &Path {
        &self.pending_path
    }

    /// Finish or discard a publication interrupted while the caller held the
    /// checkpoint namespace lock. This must run before processing a new
    /// recorder snapshot.
    pub fn recover(&self, checkpoint_sequence: u64) -> Result<()> {
        let Some((document, event)) = self.load_pending()? else {
            return Ok(());
        };
        if event.sequence == checkpoint_sequence {
            self.stream
                .repair_incomplete_pending_append(&event, &document.event_sha256)?;
        }
        let tail = self.stream.latest_verified_record()?;
        if let Some(tail) = &tail {
            if tail.event == event && tail.record.event_sha256 == document.event_sha256 {
                self.clear_pending()?;
                return Ok(());
            }
        }

        if event.sequence == checkpoint_sequence {
            self.stream
                .append(&event)
                .context("failed to recover checkpointed Arcus pending event")?;
            self.clear_pending()?;
            return Ok(());
        }

        if checkpoint_sequence.checked_add(1) == Some(event.sequence) {
            if tail
                .as_ref()
                .is_some_and(|tail| tail.event.sequence > checkpoint_sequence)
            {
                bail!(
                    "Arcus event stream advanced beyond checkpoint while a pre-checkpoint event was pending"
                );
            }
            // The pending write precedes the checkpoint rename. If the
            // checkpoint still has the previous sequence, no state advance was
            // committed and this staged event must not enter the replay stream.
            self.clear_pending()?;
            return Ok(());
        }

        bail!(
            "Arcus pending event sequence {} is incompatible with checkpoint sequence {checkpoint_sequence}",
            event.sequence
        )
    }

    /// Durably stage the exact event before publishing its checkpoint.
    pub fn stage(&self, event: &ArcusSpotRuntimeEvent) -> Result<()> {
        match fs::symlink_metadata(&self.pending_path) {
            Ok(_) => bail!(
                "Arcus pending event {} already exists; recover it before staging another event",
                self.pending_path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to inspect Arcus pending event {}",
                        self.pending_path.display()
                    )
                });
            }
        }
        let event_json = serde_json::to_string(event)
            .context("failed to serialize Arcus pending event payload")?;
        let document = ArcusSpotPendingEventDocument {
            schema_version: PENDING_EVENT_SCHEMA_VERSION,
            event_sha256: sha256_prefixed(event_json.as_bytes()),
            event_json,
        };
        let mut bytes = serde_json::to_vec_pretty(&document)
            .context("failed to serialize Arcus pending event document")?;
        bytes.push(b'\n');
        write_private_atomic(&self.pending_path, &bytes)
    }

    /// Append the already-staged event and remove the recovery sidecar only
    /// after the stream fsync succeeds.
    pub fn commit(&self, event: &ArcusSpotRuntimeEvent) -> Result<ArcusSpotLiveTickEventRecord> {
        let (_, pending) = self
            .load_pending()?
            .context("Arcus pending event is missing before stream commit")?;
        if pending != *event {
            bail!("Arcus pending event does not match the checkpointed event");
        }
        let record = self.stream.append(event)?;
        self.clear_pending()?;
        Ok(record)
    }

    fn load_pending(
        &self,
    ) -> Result<Option<(ArcusSpotPendingEventDocument, ArcusSpotRuntimeEvent)>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&self.pending_path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
                bail!(
                    "Arcus pending event {} must be a non-symlink regular file",
                    self.pending_path.display()
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to open Arcus pending event {}",
                        self.pending_path.display()
                    )
                });
            }
        };
        let metadata = file.metadata().with_context(|| {
            format!(
                "failed to inspect Arcus pending event {}",
                self.pending_path.display()
            )
        })?;
        if !metadata.is_file() {
            bail!(
                "Arcus pending event {} must be a non-symlink regular file",
                self.pending_path.display()
            );
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "Arcus pending event {} must not be accessible by group/other",
                self.pending_path.display()
            );
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let document: ArcusSpotPendingEventDocument =
            serde_json::from_slice(&bytes).context("invalid Arcus pending event document")?;
        if document.schema_version != PENDING_EVENT_SCHEMA_VERSION {
            bail!(
                "unsupported Arcus pending event schema {}",
                document.schema_version
            );
        }
        if sha256_prefixed(document.event_json.as_bytes()) != document.event_sha256 {
            bail!("Arcus pending event payload hash mismatch");
        }
        let event = serde_json::from_str(&document.event_json)
            .context("invalid Arcus pending event payload")?;
        Ok(Some((document, event)))
    }

    fn clear_pending(&self) -> Result<()> {
        let parent = self
            .pending_path
            .parent()
            .context("Arcus pending event path has no parent")?;
        fs::remove_file(&self.pending_path).with_context(|| {
            format!(
                "failed to remove Arcus pending event {}",
                self.pending_path.display()
            )
        })?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("Arcus pending event path has no parent")?;
    fs::create_dir_all(parent)?;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock precedes Unix epoch")?
        .as_nanos();
    let temp = parent.join(format!(
        ".{}.tmp.{}.{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("pending-event"),
        std::process::id(),
        stamp
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Build a self-consistent record for `event`, chained onto
/// `previous_chain_sha256`. `verify_record` is this function's inverse.
/// Exposed alongside it so tests/tools building a standalone or exported
/// record don't have to duplicate the hashing scheme.
pub fn event_record(
    event: &ArcusSpotRuntimeEvent,
    previous_chain_sha256: Option<String>,
) -> Result<ArcusSpotLiveTickEventRecord> {
    let event_json = serde_json::to_string(event)
        .context("failed to serialize Arcus live-tick event payload")?;
    let event_sha256 = sha256_prefixed(event_json.as_bytes());
    let chain_sha256 = chain_sha256(previous_chain_sha256.as_deref(), &event_sha256);
    Ok(ArcusSpotLiveTickEventRecord {
        schema_version: EVENT_STREAM_SCHEMA_VERSION,
        previous_chain_sha256,
        event_sha256,
        chain_sha256,
        event_json,
    })
}

fn segment_name(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d.jsonl").to_string()
}

fn is_segment_name(name: &str) -> bool {
    name.len() == 16
        && name.ends_with(".jsonl")
        && NaiveDate::parse_from_str(&name[..10], "%Y-%m-%d").is_ok()
}

fn validate_private_regular_file(file: &File, path: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if !metadata.is_file() {
        bail!(
            "Arcus event-stream segment {} must be a regular file",
            path.display()
        );
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "Arcus event-stream segment {} must not be accessible by group/other",
            path.display()
        );
    }
    Ok(())
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("{SHA256_PREFIX}{:x}", Sha256::digest(bytes))
}

fn chain_sha256(previous: Option<&str>, event_sha256: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(EVENT_STREAM_DOMAIN);
    digest.update(previous.unwrap_or("-").as_bytes());
    digest.update(b"\0");
    digest.update(event_sha256.as_bytes());
    format!("{SHA256_PREFIX}{:x}", digest.finalize())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == SHA256_PREFIX.len() + 64
        && value.starts_with(SHA256_PREFIX)
        && value[SHA256_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Verify one record's self-consistency (event hash, chain-link hash, schema
/// version) and parse its payload. Exposed for standalone verification of an
/// exported/archived record outside the append-only on-host stream, where
/// there is no adjacent record to check chain continuity against -- callers
/// needing full continuity across a range still go through
/// `ArcusSpotLiveTickEventStream`/the archive fetch-and-verify pipeline.
pub fn verify_record(record: &ArcusSpotLiveTickEventRecord) -> Result<ArcusSpotRuntimeEvent> {
    if record.schema_version != EVENT_STREAM_SCHEMA_VERSION {
        bail!(
            "unsupported Arcus event-stream schema {}",
            record.schema_version
        );
    }
    if !valid_sha256(&record.event_sha256) || !valid_sha256(&record.chain_sha256) {
        bail!("Arcus event-stream hashes must be lowercase sha256 values");
    }
    if record
        .previous_chain_sha256
        .as_deref()
        .is_some_and(|value| !valid_sha256(value))
    {
        bail!("Arcus previous event-stream hash is invalid");
    }
    let actual_event_sha256 = sha256_prefixed(record.event_json.as_bytes());
    if actual_event_sha256 != record.event_sha256 {
        bail!(
            "Arcus event payload hash mismatch: expected {}, got {actual_event_sha256}",
            record.event_sha256
        );
    }
    let actual_chain_sha256 = chain_sha256(
        record.previous_chain_sha256.as_deref(),
        &record.event_sha256,
    );
    if actual_chain_sha256 != record.chain_sha256 {
        bail!(
            "Arcus event chain hash mismatch: expected {}, got {actual_chain_sha256}",
            record.chain_sha256
        );
    }
    serde_json::from_str(&record.event_json)
        .context("Arcus event-stream payload is not a runtime event")
}

fn validate_event_continuity(
    previous: &ArcusSpotRuntimeEvent,
    current: &ArcusSpotRuntimeEvent,
) -> Result<()> {
    if current.observed_at < previous.observed_at {
        bail!("Arcus event-stream timestamps are not monotonic");
    }
    if previous.sequence.checked_add(1) == Some(current.sequence) {
        return Ok(());
    }
    let stale_same_sequence = current.sequence == previous.sequence
        && matches!(
            current.decision,
            ArcusSpotDecision::Observe { ref hold }
                if hold.code == ArcusSpotHoldCode::StaleOrDuplicateObservation
        );
    if stale_same_sequence {
        return Ok(());
    }
    bail!(
        "Arcus event-stream sequence discontinuity: {} -> {}",
        previous.sequence,
        current.sequence
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arcus_spot::{
        ArcusSpotHold, ArcusSpotInventory, ArcusSpotRegime, ArcusSpotRuntimeMode,
    };
    use rust_decimal::Decimal;
    use std::os::unix::fs::symlink;
    use tempfile::tempdir;

    fn event(sequence: u64, at: &str) -> ArcusSpotRuntimeEvent {
        let inventory = ArcusSpotInventory {
            token_a: Decimal::ONE,
            token_b: Decimal::ONE,
        };
        ArcusSpotRuntimeEvent {
            sequence,
            observed_at: DateTime::parse_from_rfc3339(at)
                .unwrap()
                .with_timezone(&Utc),
            pair: "NVDA/AMD".to_string(),
            mode: ArcusSpotRuntimeMode::Live,
            token_a_reference_price_usd: Some(Decimal::from(200)),
            token_b_reference_price_usd: Some(Decimal::from(100)),
            relative_log_price: Some(0.5),
            z_score: Some(1.0),
            inventory_before: inventory,
            inventory_after: inventory,
            regime_before: ArcusSpotRegime::Neutral,
            regime_after: ArcusSpotRegime::Neutral,
            risk_before: None,
            risk_after: None,
            decision: ArcusSpotDecision::Observe {
                hold: ArcusSpotHold {
                    code: ArcusSpotHoldCode::NoSignal,
                    detail: "test".to_string(),
                },
            },
        }
    }

    fn stale_event(sequence: u64, at: &str) -> ArcusSpotRuntimeEvent {
        let mut event = event(sequence, at);
        event.relative_log_price = None;
        event.z_score = None;
        event.decision = ArcusSpotDecision::Observe {
            hold: ArcusSpotHold {
                code: ArcusSpotHoldCode::StaleOrDuplicateObservation,
                detail: "duplicate".to_string(),
            },
        };
        event
    }

    fn publisher(root: &Path) -> ArcusSpotLiveTickEventPublisher {
        ArcusSpotLiveTickEventPublisher::new(
            ArcusSpotLiveTickEventStream::new(root.join("events")),
            root.join("live-tick-event-pending.json"),
        )
    }

    #[test]
    fn appends_private_hash_chained_daily_segments() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events");
        let store = ArcusSpotLiveTickEventStream::new(path.clone());
        let first = store.append(&event(41, "2026-08-24T23:47:00Z")).unwrap();
        let second = store.append(&event(42, "2026-08-25T00:02:00Z")).unwrap();

        assert_eq!(second.previous_chain_sha256, Some(first.chain_sha256));
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for day in ["2026-08-24.jsonl", "2026-08-25.jsonl"] {
            let segment = path.join(day);
            assert_eq!(
                fs::metadata(&segment).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(fs::read_to_string(segment).unwrap().lines().count(), 1);
        }
    }

    #[test]
    fn rejects_a_sequence_gap() {
        let dir = tempdir().unwrap();
        let store = ArcusSpotLiveTickEventStream::new(dir.path().join("events"));
        store.append(&event(10, "2026-08-25T00:02:00Z")).unwrap();
        let error = store
            .append(&event(12, "2026-08-25T00:17:00Z"))
            .unwrap_err();
        assert!(error.to_string().contains("sequence discontinuity"));
    }

    #[test]
    fn accepts_only_a_stale_hold_at_the_same_sequence() {
        let dir = tempdir().unwrap();
        let store = ArcusSpotLiveTickEventStream::new(dir.path().join("events"));
        store.append(&event(10, "2026-08-25T00:02:00Z")).unwrap();

        let error = store
            .append(&event(10, "2026-08-25T00:17:00Z"))
            .unwrap_err();
        assert!(error.to_string().contains("sequence discontinuity"));

        let stale = stale_event(10, "2026-08-25T00:17:00Z");
        store.append(&stale).unwrap();
    }

    #[test]
    fn recovers_the_exact_event_after_checkpoint_publication() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher
            .stream()
            .append(&event(10, "2026-08-25T00:02:00Z"))
            .unwrap();
        let pending = event(11, "2026-08-25T00:17:00Z");
        publisher.stage(&pending).unwrap();

        publisher.recover(11).unwrap();

        assert!(!publisher.pending_path().exists());
        let tail = publisher
            .stream()
            .latest_verified_record()
            .unwrap()
            .unwrap();
        assert_eq!(tail.event, pending);
        assert_eq!(
            fs::read_to_string(dir.path().join("events/2026-08-25.jsonl"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }

    #[test]
    fn recovery_after_append_before_clear_does_not_duplicate_the_event() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher
            .stream()
            .append(&event(10, "2026-08-25T00:02:00Z"))
            .unwrap();
        let pending = event(11, "2026-08-25T00:17:00Z");
        publisher.stage(&pending).unwrap();
        publisher.stream().append(&pending).unwrap();

        publisher.recover(11).unwrap();

        assert!(!publisher.pending_path().exists());
        assert_eq!(
            fs::read_to_string(dir.path().join("events/2026-08-25.jsonl"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }

    #[test]
    fn recovers_a_partial_record_append_in_an_existing_segment() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher
            .stream()
            .append(&event(10, "2026-08-25T00:02:00Z"))
            .unwrap();
        let pending = event(11, "2026-08-25T00:17:00Z");
        publisher.stage(&pending).unwrap();
        let previous = publisher
            .stream()
            .latest_verified_record()
            .unwrap()
            .unwrap();
        let record = event_record(&pending, Some(previous.record.chain_sha256)).unwrap();
        let record_bytes = serde_json::to_vec(&record).unwrap();
        let segment = dir.path().join("events/2026-08-25.jsonl");
        OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap()
            .write_all(&record_bytes[..record_bytes.len() / 2])
            .unwrap();

        publisher.recover(11).unwrap();

        assert!(!publisher.pending_path().exists());
        assert_eq!(fs::read_to_string(segment).unwrap().lines().count(), 2);
        assert_eq!(
            publisher
                .stream()
                .latest_verified_record()
                .unwrap()
                .unwrap()
                .event,
            pending
        );
    }

    #[test]
    fn recovers_an_empty_new_daily_segment_and_preserves_the_chain() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        let first = publisher
            .stream()
            .append(&event(10, "2026-08-25T23:47:00Z"))
            .unwrap();
        let pending = event(11, "2026-08-26T00:02:00Z");
        publisher.stage(&pending).unwrap();
        let empty_segment = dir.path().join("events/2026-08-26.jsonl");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&empty_segment)
            .unwrap()
            .sync_all()
            .unwrap();

        publisher.recover(11).unwrap();

        let line = fs::read_to_string(&empty_segment).unwrap();
        assert_eq!(line.lines().count(), 1);
        let record: ArcusSpotLiveTickEventRecord = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(record.previous_chain_sha256, Some(first.chain_sha256));
        assert_eq!(verify_record(&record).unwrap(), pending);
    }

    #[test]
    fn refuses_to_truncate_an_unterminated_tail_that_is_not_the_pending_record() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher
            .stream()
            .append(&event(10, "2026-08-25T00:02:00Z"))
            .unwrap();
        publisher.stage(&event(11, "2026-08-25T00:17:00Z")).unwrap();
        let segment = dir.path().join("events/2026-08-25.jsonl");
        OpenOptions::new()
            .append(true)
            .open(&segment)
            .unwrap()
            .write_all(b"{\"schema_version\":9")
            .unwrap();
        let before = fs::read(&segment).unwrap();

        let error = publisher.recover(11).unwrap_err();

        assert!(error
            .to_string()
            .contains("does not match the pending event"));
        assert_eq!(fs::read(segment).unwrap(), before);
        assert!(publisher.pending_path().exists());
    }

    #[test]
    fn discards_an_advancing_event_if_the_checkpoint_did_not_advance() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher
            .stream()
            .append(&event(10, "2026-08-25T00:02:00Z"))
            .unwrap();
        publisher.stage(&event(11, "2026-08-25T00:17:00Z")).unwrap();

        publisher.recover(10).unwrap();

        assert!(!publisher.pending_path().exists());
        assert_eq!(
            fs::read_to_string(dir.path().join("events/2026-08-25.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[test]
    fn recovers_an_exact_stale_event_at_the_checkpoint_sequence() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher
            .stream()
            .append(&event(10, "2026-08-25T00:02:00Z"))
            .unwrap();
        let stale = stale_event(10, "2026-08-25T00:17:00Z");
        publisher.stage(&stale).unwrap();

        publisher.recover(10).unwrap();

        let tail = publisher
            .stream()
            .latest_verified_record()
            .unwrap()
            .unwrap();
        assert_eq!(tail.event, stale);
        assert!(tail.event.relative_log_price.is_none());
        assert!(tail.event.z_score.is_none());
    }

    #[test]
    fn pending_event_is_private_and_tampering_fails_closed() {
        let dir = tempdir().unwrap();
        let publisher = publisher(dir.path());
        publisher.stage(&event(11, "2026-08-25T00:17:00Z")).unwrap();
        assert_eq!(
            fs::metadata(publisher.pending_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let raw = fs::read_to_string(publisher.pending_path()).unwrap();
        fs::write(publisher.pending_path(), raw.replace("NVDA/AMD", "SPY/QQQ")).unwrap();

        let error = publisher.recover(11).unwrap_err();
        assert!(format!("{error:#}").contains("payload hash mismatch"));
    }

    #[test]
    fn rejects_a_tampered_payload_before_appending() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events");
        let store = ArcusSpotLiveTickEventStream::new(path.clone());
        store.append(&event(10, "2026-08-25T00:02:00Z")).unwrap();
        let segment = path.join("2026-08-25.jsonl");
        let raw = fs::read_to_string(&segment).unwrap();
        fs::write(&segment, raw.replace("NVDA/AMD", "SPY/QQQ")).unwrap();

        let error = store
            .append(&event(11, "2026-08-25T00:17:00Z"))
            .unwrap_err();
        assert!(format!("{error:#}").contains("payload hash mismatch"));
    }

    #[test]
    fn rejects_an_unterminated_final_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("events");
        let store = ArcusSpotLiveTickEventStream::new(path.clone());
        store.append(&event(10, "2026-08-25T00:02:00Z")).unwrap();
        let segment = path.join("2026-08-25.jsonl");
        let mut raw = fs::read(&segment).unwrap();
        assert_eq!(raw.pop(), Some(b'\n'));
        fs::write(&segment, raw).unwrap();

        let error = store
            .append(&event(11, "2026-08-25T00:17:00Z"))
            .unwrap_err();
        assert!(error.to_string().contains("unterminated final record"));
    }

    #[test]
    fn rejects_a_symlinked_stream_directory() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = dir.path().join("events");
        symlink(&target, &link).unwrap();
        let error = ArcusSpotLiveTickEventStream::new(link)
            .append(&event(1, "2026-08-25T00:02:00Z"))
            .unwrap_err();
        assert!(error.to_string().contains("non-symlink directory"));
    }
}
