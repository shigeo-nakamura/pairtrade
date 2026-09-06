//! Append-only JSONL outputs: `ledger.jsonl` (decisions, intents, fills,
//! summaries, halts) and `pnl.jsonl` (daily marks, leg exits). See
//! `docs/book-runtime.md` §8.

use std::path::PathBuf;

use serde_json::{json, Value};

use crate::directional::append_jsonl;

#[derive(Debug, Clone)]
pub struct Ledger {
    path: PathBuf,
    instance_id: String,
}

impl Ledger {
    pub fn new(path: PathBuf, instance_id: &str) -> Self {
        Self {
            path,
            instance_id: instance_id.to_string(),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Append one row. `now` is unix seconds (rendered as `ts_ms`);
    /// `payload` must be a JSON object and is merged after the envelope so
    /// callers cannot accidentally override `event` / `instance_id`.
    pub fn write(&self, now: i64, event: &str, decision_key: Option<&str>, payload: Value) {
        let mut row = json!({
            "event": event,
            "ts_ms": now * 1000,
            "instance_id": self.instance_id,
            "decision_key": decision_key,
        });
        if let (Some(dst), Some(src)) = (row.as_object_mut(), payload.as_object()) {
            for (k, v) in src {
                dst.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        if let Err(e) = append_jsonl(&self.path, &row) {
            log::warn!("[LEDGER] append to {} failed: {e}", self.path.display());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_fields_win_over_payload() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("l.jsonl");
        let l = Ledger::new(p.clone(), "inst");
        l.write(
            1_700_000_000,
            "fill",
            Some("k"),
            json!({"event": "x", "qty": 1.5}),
        );
        let text = std::fs::read_to_string(&p).unwrap();
        let v: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["event"], "fill");
        assert_eq!(v["instance_id"], "inst");
        assert_eq!(v["decision_key"], "k");
        assert_eq!(v["ts_ms"], 1_700_000_000_000i64);
        assert_eq!(v["qty"], 1.5);
    }
}
