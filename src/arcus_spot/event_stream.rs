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

impl ArcusSpotLiveTickEventStream {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Append one checkpointed event and fsync both data and a newly-created
    /// segment's directory entry.
    ///
    /// The caller holds the checkpoint lock and persists the checkpoint
    /// immediately before this call. An append failure therefore fails the
    /// oneshot loudly; a later append refuses the resulting sequence gap
    /// instead of silently claiming an incomplete replay is authoritative.
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

        let event_json = serde_json::to_string(event)
            .context("failed to serialize Arcus live-tick event payload")?;
        let event_sha256 = sha256_prefixed(event_json.as_bytes());
        let previous_chain_sha256 = previous.map(|item| item.record.chain_sha256);
        let chain_sha256 = chain_sha256(previous_chain_sha256.as_deref(), &event_sha256);
        let record = ArcusSpotLiveTickEventRecord {
            schema_version: EVENT_STREAM_SCHEMA_VERSION,
            previous_chain_sha256,
            event_sha256,
            chain_sha256,
            event_json,
        };

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

fn verify_record(record: &ArcusSpotLiveTickEventRecord) -> Result<ArcusSpotRuntimeEvent> {
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

        let mut stale = event(10, "2026-08-25T00:17:00Z");
        stale.relative_log_price = None;
        stale.z_score = None;
        stale.decision = ArcusSpotDecision::Observe {
            hold: ArcusSpotHold {
                code: ArcusSpotHoldCode::StaleOrDuplicateObservation,
                detail: "duplicate".to_string(),
            },
        };
        store.append(&stale).unwrap();
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
