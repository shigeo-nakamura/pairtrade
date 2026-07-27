//! Deterministic Arcus Spot recorder replay.
//!
//! This binary has no wallet, signing, approval, quote-submission, or swap
//! surface. The only state mutation available is isolated replay simulation.

use anyhow::{bail, Context, Result};
use debot::arcus_spot::{replay_jsonl, ArcusSpotRuntime, ArcusSpotRuntimeConfig};
use std::{
    env,
    fs::File,
    io::{self, BufReader, BufWriter, Write},
    path::Path,
};

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
