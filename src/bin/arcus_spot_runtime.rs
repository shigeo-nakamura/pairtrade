//! Deterministic Arcus Spot recorder replay.
//!
//! This binary has no wallet, signing, approval, quote-submission, or swap
//! surface. The only state mutation available is isolated replay simulation.

use anyhow::{bail, Context, Result};
use debot::arcus_spot::{replay_jsonl, ArcusSpotRuntime, ArcusSpotRuntimeConfig};
use std::{
    env,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Write},
    os::unix::fs::MetadataExt,
    path::Path,
};

/// Whether `a` and `b` name the same underlying file, via a symlink,
/// relative-path alias, or hard link. `File::create` on the output path
/// truncates it before `replay_jsonl` gets a chance to read the already
/// opened input handle, so an aliased pair silently zeroes the recorder
/// archive instead of failing loudly.
fn same_file(a: &Path, b: &Path) -> bool {
    if let (Ok(canonical_a), Ok(canonical_b)) = (fs::canonicalize(a), fs::canonicalize(b)) {
        if canonical_a == canonical_b {
            return true;
        }
    }
    if let (Ok(meta_a), Ok(meta_b)) = (fs::metadata(a), fs::metadata(b)) {
        if meta_a.dev() == meta_b.dev() && meta_a.ino() == meta_b.ino() {
            return true;
        }
    }
    false
}

fn main() -> Result<()> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if !(2..=3).contains(&arguments.len()) {
        bail!("usage: arcus-spot-runtime CONFIG_YAML RECORDER_JSONL [EVENTS_JSONL]");
    }
    let config_path = Path::new(&arguments[0]);
    let input_path = Path::new(&arguments[1]);
    let config_bytes = std::fs::read(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: ArcusSpotRuntimeConfig = serde_yaml::from_slice(&config_bytes)
        .with_context(|| format!("invalid config {}", config_path.display()))?;
    let mut runtime = ArcusSpotRuntime::new(config).map_err(anyhow::Error::msg)?;
    let input = File::open(input_path)
        .with_context(|| format!("failed to open input {}", input_path.display()))?;

    let summary = if let Some(output_path) = arguments.get(2) {
        let output_path = Path::new(output_path);
        if same_file(input_path, output_path) {
            bail!(
                "EVENTS_JSONL {} must not alias RECORDER_JSONL {}: creating the \
                 output would truncate the input before it is replayed",
                output_path.display(),
                input_path.display()
            );
        }
        let output = File::create(output_path)
            .with_context(|| format!("failed to create output {}", output_path.display()))?;
        replay_jsonl(&mut runtime, BufReader::new(input), BufWriter::new(output))?
    } else {
        let stdout = io::stdout();
        replay_jsonl(
            &mut runtime,
            BufReader::new(input),
            BufWriter::new(stdout.lock()),
        )?
    };
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    serde_json::to_writer(&mut stderr, &summary).context("failed to serialize replay summary")?;
    stderr.write_all(b"\n").context("failed to write summary")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn unique_temp_dir(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "arcus-spot-runtime-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn same_file_detects_identical_paths() {
        let dir = unique_temp_dir("identical");
        let path = dir.join("events.jsonl");
        fs::write(&path, b"{}").unwrap();
        assert!(same_file(&path, &path));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_file_detects_a_symlink_alias() {
        let dir = unique_temp_dir("symlink");
        let recorder = dir.join("recorder.jsonl");
        fs::write(&recorder, b"{}").unwrap();
        let events = dir.join("events.jsonl");
        std::os::unix::fs::symlink(&recorder, &events).unwrap();
        assert!(same_file(&recorder, &events));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_file_detects_a_hard_link_alias() {
        let dir = unique_temp_dir("hardlink");
        let recorder = dir.join("recorder.jsonl");
        fs::write(&recorder, b"{}").unwrap();
        let events = dir.join("events.jsonl");
        fs::hard_link(&recorder, &events).unwrap();
        assert!(same_file(&recorder, &events));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn same_file_rejects_distinct_paths() {
        let dir = unique_temp_dir("distinct");
        let recorder = dir.join("recorder.jsonl");
        fs::write(&recorder, b"{}").unwrap();
        let events = dir.join("events.jsonl");
        fs::write(&events, b"{}").unwrap();
        assert!(!same_file(&recorder, &events));
        fs::remove_dir_all(dir).unwrap();
    }
}
