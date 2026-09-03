//! Shared plumbing for standalone directional bots (bot-strategy#816 dip-grid,
//! bot-strategy#893 bull-mode holder).
//!
//! pairtrade's own `risk_io` / `status` / `pnl_log` modules are private to the
//! `pairtrade` module tree, so a separate `src/bin/` crate cannot reach them.
//! Rather than widening that surface, the small on-disk conventions the
//! directional bots share live here as a `pub mod`:
//!
//! - atomic tmp+rename JSON writes (state / status snapshots)
//! - append-only JSONL logs
//! - KILL_SWITCH / RISK_ACK sentinel files (same semantics as pairtrade: the
//!   ack file is consumed so a stale ack never clears the *next* halt)
//! - the "DRY_RUN=false is refused until reviewed" startup guard
//! - a short config fingerprint for the `[CONFIG] ... fp=` startup line
//!
//! `robinhood_dipgrid.rs` still carries its own copy of these helpers; moving
//! it onto this module is a follow-up (bot-strategy#894 scope note).

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write as _;
use std::path::{Path, PathBuf};

/// Write `contents` to `path` via a same-directory temp file + rename so a
/// crash mid-write never leaves a truncated file behind.
pub fn atomic_write(path: &Path, contents: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "state".to_string());
    let tmp = dir.join(format!(".{name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp, contents)?;
    std::fs::rename(&tmp, path)
}

/// Serialize `value` as pretty JSON and write it atomically.
pub fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value).context("serialize state")?;
    atomic_write(path, &json).with_context(|| format!("write {}", path.display()))
}

/// Load JSON from `path`; a missing or unparsable file yields `T::default()`.
/// Callers that must distinguish "missing" from "corrupt" should use
/// [`load_json`] instead.
pub fn load_json_or_default<T: DeserializeOwned + Default>(path: &Path) -> T {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => T::default(),
    }
}

/// Load JSON from `path`. `Ok(None)` when the file does not exist; an error
/// when it exists but cannot be parsed (never silently reset a corrupt file).
pub fn load_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let v =
                serde_json::from_str(&s).with_context(|| format!("parse {}", path.display()))?;
            Ok(Some(v))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Append one JSON record as a line to a JSONL file.
pub fn append_jsonl(path: &Path, record: &serde_json::Value) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{record}")
}

/// KILL_SWITCH / RISK_ACK sentinel files.
#[derive(Debug, Clone)]
pub struct Sentinels {
    pub kill_switch: PathBuf,
    pub risk_ack: PathBuf,
}

impl Sentinels {
    pub fn new(kill_switch: PathBuf, risk_ack: PathBuf) -> Self {
        Self {
            kill_switch,
            risk_ack,
        }
    }

    /// Operator kill switch: while the file exists the bot must not open or
    /// add exposure. Protective exits stay allowed.
    pub fn kill_switch_engaged(&self) -> bool {
        self.kill_switch.exists()
    }

    /// Consume a RISK_ACK file. Returns `true` when an ack was present. The
    /// file is removed unconditionally so a stale ack from an earlier incident
    /// can never re-arm and clear the next halt.
    pub fn take_risk_ack(&self) -> bool {
        if !self.risk_ack.exists() {
            return false;
        }
        if let Err(e) = std::fs::remove_file(&self.risk_ack) {
            log::warn!(
                "[RISK_ACK] failed to remove {} after ack: {e:?}",
                self.risk_ack.display()
            );
        }
        true
    }
}

/// Startup guard for prototypes: refuse `DRY_RUN=false` until the rollout
/// issue explicitly lifts it (the lift is a code change, not an env flip).
pub fn refuse_live(dry_run: bool, bot: &str, reason: &str) -> Result<()> {
    if dry_run {
        return Ok(());
    }
    bail!("{bot}: DRY_RUN=false refused: {reason}");
}

/// Short sha256 fingerprint over ordered `(key, value)` config fields, for the
/// `[CONFIG] ... fp=` startup line (same 12-hex convention as pairtrade).
pub fn config_fingerprint(fields: &[(&str, String)]) -> String {
    let mut h = Sha256::new();
    for (k, v) in fields {
        h.update(k.as_bytes());
        h.update(b"=");
        h.update(v.as_bytes());
        h.update(b"\n");
    }
    let digest = h.finalize();
    digest.iter().take(6).map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
    struct S {
        a: u32,
        b: String,
    }

    #[test]
    fn persist_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nested").join("state.json");
        let s = S {
            a: 7,
            b: "x".into(),
        };
        persist_json(&p, &s).unwrap();
        assert_eq!(load_json::<S>(&p).unwrap(), Some(s));
        // no temp file left behind
        let leftovers: Vec<_> = std::fs::read_dir(p.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with('.'))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn load_json_distinguishes_missing_from_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s.json");
        assert!(load_json::<S>(&p).unwrap().is_none());
        std::fs::write(&p, "{not json").unwrap();
        assert!(load_json::<S>(&p).is_err());
        assert_eq!(load_json_or_default::<S>(&p), S::default());
    }

    #[test]
    fn risk_ack_is_consumed_once() {
        let dir = tempfile::tempdir().unwrap();
        let s = Sentinels::new(dir.path().join("KILL"), dir.path().join("ACK"));
        assert!(!s.kill_switch_engaged());
        assert!(!s.take_risk_ack());
        std::fs::write(&s.risk_ack, "").unwrap();
        assert!(s.take_risk_ack());
        assert!(!s.risk_ack.exists());
        assert!(!s.take_risk_ack());
        std::fs::write(&s.kill_switch, "").unwrap();
        assert!(s.kill_switch_engaged());
    }

    #[test]
    fn refuse_live_guard() {
        assert!(refuse_live(true, "bot", "r").is_ok());
        let e = refuse_live(false, "bot", "not reviewed").unwrap_err();
        assert!(e.to_string().contains("not reviewed"));
    }

    #[test]
    fn fingerprint_is_stable_and_order_sensitive() {
        let a = config_fingerprint(&[("x", "1".into()), ("y", "2".into())]);
        let b = config_fingerprint(&[("x", "1".into()), ("y", "2".into())]);
        let c = config_fingerprint(&[("y", "2".into()), ("x", "1".into())]);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.len(), 12);
    }
}
