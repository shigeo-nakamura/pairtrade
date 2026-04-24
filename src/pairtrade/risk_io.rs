//! On-disk persistence for runtime risk state (circuit breaker counters
//! and cool-down deadlines). Without this, a restart/crash during an
//! active cool-down silently clears the counter and lets the bot re-enter
//! immediately after N consecutive losses. See bot-strategy#185 Phase 1-3.
//!
//! File lives next to `history_file` (typically `/opt/debot/`) and is
//! written atomically via tmpfile + rename, same pattern as `history_io`.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub(super) struct InstanceRiskState {
    #[serde(default)]
    pub consecutive_losses: u32,
    #[serde(default)]
    pub circuit_breaker_until_ts: Option<i64>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub(super) struct RiskStateSnapshot {
    #[serde(rename = "_v")]
    pub version: u32,
    #[serde(default)]
    pub instances: HashMap<String, InstanceRiskState>,
}

pub(super) fn persist_risk_state(
    path: &Path,
    instances: &HashMap<String, InstanceRiskState>,
) {
    let snapshot = RiskStateSnapshot {
        version: 1,
        instances: instances.clone(),
    };
    let Ok(json) = serde_json::to_string(&snapshot) else {
        log::warn!("[RISK_STATE] serialize failed");
        return;
    };
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "risk_state.json".to_string());
    let tmp = dir.join(format!(".{}.tmp.{}", file_name, std::process::id()));
    if let Err(e) = fs::write(&tmp, json) {
        log::warn!("[RISK_STATE] tmp write failed: {:?}", e);
        return;
    }
    if let Err(e) = fs::rename(&tmp, path) {
        log::warn!("[RISK_STATE] rename failed: {:?}", e);
        let _ = fs::remove_file(&tmp);
    }
}

pub(super) fn load_risk_state(path: &Path) -> HashMap<String, InstanceRiskState> {
    let content = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(e) => {
            log::warn!("[RISK_STATE] read failed ({}): {:?}", path.display(), e);
            return HashMap::new();
        }
    };
    match serde_json::from_str::<RiskStateSnapshot>(&content) {
        Ok(snap) => snap.instances,
        Err(e) => {
            log::warn!("[RISK_STATE] parse failed ({}): {:?}", path.display(), e);
            HashMap::new()
        }
    }
}
