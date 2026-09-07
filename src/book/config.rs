//! Book runtime configuration (`configs/book/<instance>.yaml`), see
//! `docs/book-runtime.md` §2.

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::directional::config_fingerprint;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BookConfig {
    pub schema_version: u32,
    pub instance_id: String,
    pub venue: String,
    pub dry_run: bool,
    pub universe: UniverseConfig,
    pub schedule: ScheduleConfig,
    pub signal: SignalConfig,
    pub sizing: SizingConfig,
    pub execution: ExecutionConfig,
    pub risk: RiskConfig,
    pub paths: PathsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UniverseConfig {
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    IntervalDays,
    Daily,
    Calendar,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    pub kind: ScheduleKind,
    #[serde(default)]
    pub anchor_date: Option<NaiveDate>,
    #[serde(default)]
    pub every_days: Option<u32>,
    /// `HH:MM` UTC.
    #[serde(default)]
    pub decision_time_utc: Option<String>,
    pub signal_grace_secs: i64,
    #[serde(default)]
    pub flatten_after_secs: Option<i64>,
    #[serde(default)]
    pub calendar_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    pub path: PathBuf,
    pub producer_id: String,
    pub max_age_secs: i64,
    #[serde(default = "default_true")]
    pub require_dollar_neutral: bool,
    #[serde(default = "default_net_tolerance")]
    pub net_tolerance: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SizingConfig {
    pub gross_notional_usd: f64,
    pub max_symbol_weight: f64,
    pub max_gross_usd: f64,
    pub max_net_usd: f64,
    pub min_order_usd: f64,
    pub rebalance_deadband_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub slippage_bps: u32,
    pub max_attempts: u32,
    pub fill_confirm_timeout_secs: i64,
    /// Lighter has no price-capped IOC in dex-connector v4.7.20
    /// (`create_order_taker_ioc` is `Permanent`; bot-strategy#918): when
    /// true, a live order falls back to `create_order(price=None)`, i.e.
    /// the venue's own ±20 % protection price instead of `slippage_bps`.
    /// Default false = fail closed (the order is not sent).
    #[serde(default)]
    pub allow_venue_protection_fallback: bool,
    pub paper_slippage_bps: f64,
    #[serde(default)]
    pub paper_fee_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    pub equity_reference_usd: f64,
    pub max_session_loss_bps: f64,
    pub max_daily_loss_bps: f64,
    pub kill_switch_path: PathBuf,
    pub risk_ack_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
    pub state: PathBuf,
    pub ledger: PathBuf,
    pub pnl: PathBuf,
    pub status: PathBuf,
}

fn default_true() -> bool {
    true
}

fn default_net_tolerance() -> f64 {
    0.05
}

/// Parse `HH:MM` into seconds after midnight.
pub fn parse_hhmm(s: &str) -> Result<i64> {
    let (h, m) = s
        .trim()
        .split_once(':')
        .with_context(|| format!("decision_time_utc must be HH:MM, got {s:?}"))?;
    let h: i64 = h.parse().with_context(|| format!("bad hour in {s:?}"))?;
    let m: i64 = m.parse().with_context(|| format!("bad minute in {s:?}"))?;
    if !(0..24).contains(&h) || !(0..60).contains(&m) {
        bail!("decision_time_utc out of range: {s:?}");
    }
    Ok(h * 3600 + m * 60)
}

impl BookConfig {
    /// The exclusive per-instance lock `book_runtime` acquires before
    /// loading state or connecting to the venue (bot-strategy#937), derived
    /// from `paths.state` so it needs no config field of its own. Exposed
    /// here (not computed separately in the binary) so `validate_impl`
    /// below can reserve it: a config whose `risk_ack_path` (or any other
    /// runtime path) happened to equal it would let the engine's normal
    /// stray-ack cleanup unlink the lock file's directory entry out from
    /// under the running process's held flock (which stays valid only on
    /// the now-unlinked inode), letting a second instance recreate the
    /// pathname and lock a different inode -- defeating the exclusion the
    /// lock exists for. Derived from `resolved_path`, not the raw
    /// `paths.state` spelling, so two configs that name the same state
    /// through different existing leaf aliases (a symlink or hard link)
    /// still land on the same lock path instead of each locking a
    /// different file while loading the same state.
    pub fn instance_lock_path(&self) -> PathBuf {
        resolved_path(&self.paths.state).with_extension("lock")
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read book config {}", path.display()))?;
        let cfg: BookConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("parse book config {}", path.display()))?;
        cfg.validate_impl(Some(path))?;
        Ok(cfg)
    }

    pub fn from_yaml_str(text: &str) -> Result<Self> {
        let cfg: BookConfig = serde_yaml::from_str(text).context("parse book config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_impl(None)
    }

    fn validate_impl(&self, config_path: Option<&Path>) -> Result<()> {
        if self.schema_version != SCHEMA_VERSION {
            bail!(
                "book config schema_version {} unsupported (expected {})",
                self.schema_version,
                SCHEMA_VERSION
            );
        }
        if self.instance_id.trim().is_empty() {
            bail!("instance_id must not be empty");
        }
        // Validate against what this binary can actually reach:
        // `DexConnectorBox`'s arms are feature-gated, so accepting a venue
        // the release build has no connector for would pass validation and
        // then fail at startup with `Unsupported dex`.
        let supported: &[&str] = &[
            #[cfg(feature = "lighter-sdk")]
            "lighter",
            #[cfg(feature = "hyperliquid-sdk")]
            "hyperliquid",
            #[cfg(feature = "hyperliquid-sdk")]
            "hyperliquid-account",
        ];
        if !supported.contains(&self.venue.as_str()) {
            bail!(
                "venue {:?} is not available in this build (compiled connectors: {}); rebuild with the matching dex-connector feature or pick another venue",
                self.venue,
                if supported.is_empty() {
                    "none".to_string()
                } else {
                    supported.join(", ")
                }
            );
        }
        if self.universe.symbols.is_empty() {
            bail!("universe.symbols must not be empty");
        }
        let mut seen = std::collections::HashSet::new();
        for s in &self.universe.symbols {
            if s.trim().is_empty() || s != s.trim() {
                bail!("universe symbol {s:?} is empty or has surrounding whitespace");
            }
            if !seen.insert(s.clone()) {
                bail!("universe symbol {s:?} listed twice");
            }
        }
        let sc = &self.schedule;
        match sc.kind {
            ScheduleKind::IntervalDays => {
                if sc.anchor_date.is_none() {
                    bail!("schedule.anchor_date is required for interval_days");
                }
                match sc.every_days {
                    Some(d) if d >= 1 => {}
                    _ => bail!("schedule.every_days must be >= 1 for interval_days"),
                }
                parse_hhmm(
                    sc.decision_time_utc
                        .as_deref()
                        .context("schedule.decision_time_utc is required for interval_days")?,
                )?;
            }
            ScheduleKind::Daily => {
                parse_hhmm(
                    sc.decision_time_utc
                        .as_deref()
                        .context("schedule.decision_time_utc is required for daily")?,
                )?;
            }
            ScheduleKind::Calendar => {
                if sc.calendar_path.is_none() {
                    bail!("schedule.calendar_path is required for calendar");
                }
                if sc.flatten_after_secs.is_some() {
                    bail!("schedule.flatten_after_secs is not used with calendar (flatten_at comes from the calendar)");
                }
            }
        }
        if sc.signal_grace_secs <= 0 {
            bail!("schedule.signal_grace_secs must be > 0");
        }
        if let Some(f) = sc.flatten_after_secs {
            if f <= sc.signal_grace_secs {
                bail!("schedule.flatten_after_secs must exceed signal_grace_secs");
            }
            // A flatten that falls after the next decision would overlap two
            // decisions' obligations; the runtime keeps one decision record.
            let cadence_secs = match sc.kind {
                ScheduleKind::Daily => 86_400,
                ScheduleKind::IntervalDays => sc.every_days.unwrap_or(1) as i64 * 86_400,
                ScheduleKind::Calendar => i64::MAX,
            };
            if f >= cadence_secs {
                bail!(
                    "schedule.flatten_after_secs ({f}) must be shorter than the decision cadence ({cadence_secs}s)"
                );
            }
        }
        if sc.signal_grace_secs
            >= match sc.kind {
                ScheduleKind::Daily => 86_400,
                ScheduleKind::IntervalDays => sc.every_days.unwrap_or(1) as i64 * 86_400,
                ScheduleKind::Calendar => i64::MAX,
            }
        {
            bail!("schedule.signal_grace_secs must be shorter than the decision cadence");
        }
        // YAML accepts `.nan` / `.inf`; a NaN limit compares false against
        // everything and would silently disable a rail. Every numeric field
        // must be finite before the range checks below mean anything.
        for (name, v) in [
            ("signal.net_tolerance", self.signal.net_tolerance),
            ("sizing.gross_notional_usd", self.sizing.gross_notional_usd),
            ("sizing.max_symbol_weight", self.sizing.max_symbol_weight),
            ("sizing.max_gross_usd", self.sizing.max_gross_usd),
            ("sizing.max_net_usd", self.sizing.max_net_usd),
            ("sizing.min_order_usd", self.sizing.min_order_usd),
            (
                "sizing.rebalance_deadband_usd",
                self.sizing.rebalance_deadband_usd,
            ),
            (
                "execution.paper_slippage_bps",
                self.execution.paper_slippage_bps,
            ),
            ("execution.paper_fee_bps", self.execution.paper_fee_bps),
            ("risk.equity_reference_usd", self.risk.equity_reference_usd),
            ("risk.max_session_loss_bps", self.risk.max_session_loss_bps),
            ("risk.max_daily_loss_bps", self.risk.max_daily_loss_bps),
        ] {
            if !v.is_finite() {
                bail!("{name} must be a finite number, got {v}");
            }
        }
        if self.signal.producer_id.trim().is_empty() {
            bail!("signal.producer_id must not be empty");
        }
        if self.signal.max_age_secs <= 0 {
            bail!("signal.max_age_secs must be > 0");
        }
        if !(0.0..=1.0).contains(&self.signal.net_tolerance) {
            bail!("signal.net_tolerance must be within [0, 1]");
        }
        let sz = &self.sizing;
        if sz.gross_notional_usd <= 0.0 {
            bail!("sizing.gross_notional_usd must be > 0");
        }
        if !(0.0 < sz.max_symbol_weight && sz.max_symbol_weight <= 1.0) {
            bail!("sizing.max_symbol_weight must be within (0, 1]");
        }
        if sz.max_gross_usd < sz.gross_notional_usd {
            bail!("sizing.max_gross_usd must be >= gross_notional_usd");
        }
        if sz.max_net_usd < 0.0 || sz.min_order_usd < 0.0 || sz.rebalance_deadband_usd < 0.0 {
            bail!("sizing caps must be >= 0");
        }
        let ex = &self.execution;
        if ex.max_attempts == 0 {
            bail!("execution.max_attempts must be >= 1");
        }
        if ex.fill_confirm_timeout_secs <= 0 {
            bail!("execution.fill_confirm_timeout_secs must be > 0");
        }
        if ex.paper_slippage_bps < 0.0 || ex.paper_fee_bps < 0.0 {
            bail!("execution.paper_* bps must be >= 0");
        }
        // The paper fill is `mid * (1 -/+ slip)`: at 100% a sell fills at
        // zero and beyond it the price and the fee go negative, which
        // would corrupt the basis, realized PnL and equity of the whole
        // replay instead of failing here.
        if ex.paper_slippage_bps >= 10_000.0 {
            bail!("execution.paper_slippage_bps must be < 10000 (100%)");
        }
        // The live budget guards the same adverse-price direction as the
        // paper one (LiveExecutor's within_slippage / send_capped price
        // cap): at 100% or more it stops meaningfully bounding a sell
        // (any positive mid passes), so a live order could clear far
        // outside its sizing reference instead of being rejected.
        if ex.slippage_bps >= 10_000 {
            bail!("execution.slippage_bps must be < 10000 (100%)");
        }
        // paths.state itself must not be a symlink: its atomic persist
        // (rename a temp file onto this exact pathname) replaces whatever
        // is there rather than writing through it, so a leaf symlink is
        // destroyed -- and diverges from its former target -- on the very
        // first persist. instance_lock_path derives from this path
        // resolved through symlinks precisely so two configs naming the
        // same state via an alias share one lock; that guarantee would
        // silently stop holding after the first restart following that
        // first persist, once the alias and target have become two
        // separate files. An ancestor *directory* symlink is fine (the
        // kernel resolves it transparently on every read/write/rename, so
        // it is never replaced), only the final path component is unsafe.
        if let Ok(meta) = std::fs::symlink_metadata(&self.paths.state) {
            if meta.file_type().is_symlink() {
                bail!(
                    "paths.state ({}) must not itself be a symlink -- its first atomic persist \
                     would replace the link (not its target), silently diverging from whatever \
                     it used to alias; point every config directly at the real file instead",
                    self.paths.state.display()
                );
            }
        }
        // Every runtime file must be its own. Sharing one would have each
        // ledger append leave the state unparsable and the end-of-tick
        // persist overwrite the ledger; a path that collided with a flag
        // file would be worse still, since the kill switch and RISK_ACK
        // are read as "this file exists".
        let lock_path = self.instance_lock_path();
        let mut named: Vec<(&str, &Path)> = vec![
            ("paths.state", &self.paths.state),
            ("paths.ledger", &self.paths.ledger),
            ("paths.pnl", &self.paths.pnl),
            ("paths.status", &self.paths.status),
            ("risk.kill_switch_path", &self.risk.kill_switch_path),
            ("risk.risk_ack_path", &self.risk.risk_ack_path),
            ("signal.path", &self.signal.path),
            ("<instance-lock>", &lock_path),
        ];
        if let Some(calendar_path) = &self.schedule.calendar_path {
            named.push(("schedule.calendar_path", calendar_path));
        }
        if let Some(config_path) = config_path {
            named.push(("<--config>", config_path));
        }
        let mut seen: Vec<(&str, PathBuf, Option<(u64, u64)>)> = Vec::new();
        for (name, path) in named {
            let key = resolved_path(path);
            let identity = file_identity(path);
            if let Some((other, _, _)) = seen
                .iter()
                .find(|(_, k, id)| *k == key || (identity.is_some() && *id == identity))
            {
                bail!("{name} and {other} are the same file ({})", path.display());
            }
            seen.push((name, key, identity));
        }
        let r = &self.risk;
        if r.equity_reference_usd <= 0.0 {
            bail!("risk.equity_reference_usd must be > 0");
        }
        if r.max_session_loss_bps <= 0.0 || r.max_daily_loss_bps <= 0.0 {
            bail!("risk.max_*_loss_bps must be > 0");
        }
        Ok(())
    }

    /// Seconds after midnight of the configured decision time.
    pub fn decision_time_secs(&self) -> i64 {
        self.schedule
            .decision_time_utc
            .as_deref()
            .and_then(|s| parse_hhmm(s).ok())
            .unwrap_or(0)
    }

    /// sha256-12 over the canonical JSON of the whole effective config.
    pub fn fingerprint(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        config_fingerprint(&[("book_config", json)])
    }

    /// One-line `[CONFIG]` summary for the startup log.
    pub fn log_line(&self) -> String {
        format!(
            "[CONFIG] instance={} venue={} dry_run={} schedule={:?} universe={} gross=${:.0} max_w={} max_gross=${:.0} max_net=${:.0} session_loss_bps={} daily_loss_bps={} producer={} fp={}",
            self.instance_id,
            self.venue,
            self.dry_run,
            self.schedule.kind,
            self.universe.symbols.len(),
            self.sizing.gross_notional_usd,
            self.sizing.max_symbol_weight,
            self.sizing.max_gross_usd,
            self.sizing.max_net_usd,
            self.risk.max_session_loss_bps,
            self.risk.max_daily_loss_bps,
            self.signal.producer_id,
            self.fingerprint()
        )
    }
}

/// `.` and `..` folded away textually. Used when the real directory is
/// not on disk yet, so `run/state.json` and `run/../run/state.json` are
/// still recognised as one file. A leading `..` that cannot be folded is
/// kept, and no symlink is followed -- there is nothing to follow.
fn lexical_path(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(
                    out.components().next_back(),
                    None | Some(Component::ParentDir) | Some(Component::RootDir)
                ) {
                    out.pop();
                } else if out.components().next_back() != Some(Component::RootDir) {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// An absolute form of a path whose directory does not exist yet: folded
/// lexically and, when still relative, anchored to the working directory
/// the process will actually write from. Without the anchor
/// `run/state.json` and `$PWD/run/state.json` would compare as two files
/// and both could be handed to state and ledger.
fn anchored_path(p: &Path) -> PathBuf {
    let lex = lexical_path(p);
    if lex.is_absolute() {
        return lex;
    }
    match std::env::current_dir() {
        Ok(cwd) => lexical_path(&cwd.join(lex)),
        Err(_) => lex,
    }
}

/// `(device, inode)` of an existing path, so two hard links to one file --
/// distinct directory entries that canonicalize to distinct path strings --
/// are still caught. `None` when the path does not exist yet or the
/// platform has no such identity (hard links are unix-only).
#[cfg(unix)]
fn file_identity(p: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(p).ok().map(|m| (m.dev(), m.ino()))
}

#[cfg(not(unix))]
fn file_identity(_p: &Path) -> Option<(u64, u64)> {
    None
}

/// A path in a form two configured paths can be compared by: the whole
/// path resolved through symlinks and `..` when it exists, so a leaf alias
/// (`ledger.jsonl -> state.json`) and not just a `..` in the directory
/// (`a/../a/b.json`) is caught. Before that file exists -- the common case
/// on first run -- each iteration canonicalizes the *longest existing
/// ancestor directory* of the current candidate (so a symlinked ancestor
/// with not-yet-created subdirectories below it, e.g. `link/new/state.json`
/// with only `link` existing, still resolves through the symlink) and
/// appends the missing components lexically; if that yields a leaf that is
/// itself an existing symlink (dangling or not), it is followed and the
/// whole process repeats -- so a chain whose next hop re-enters another
/// symlinked-but-incomplete ancestor (`ledger.jsonl -> link/new/state.json`
/// with `link -> real` and `real/new/` not created yet) still resolves
/// through every hop, not just the first. A hop that revisits an
/// already-seen candidate (a symlink cycle) stops and falls back to the
/// lexical form of where it landed.
fn resolved_path(p: &Path) -> PathBuf {
    let mut current = p.to_path_buf();
    let mut seen = std::collections::HashSet::new();
    loop {
        if let Ok(full) = current.canonicalize() {
            return full;
        }
        if !seen.insert(current.clone()) {
            return anchored_path(&current);
        }
        let (Some(dir), Some(name)) = (current.parent(), current.file_name()) else {
            return anchored_path(&current);
        };
        if dir.as_os_str().is_empty() {
            return anchored_path(&current);
        }
        let (resolved_dir, missing) = resolve_longest_existing_ancestor(dir);
        if !missing.is_empty() {
            // Some directory between the closest existing ancestor and
            // this candidate's own parent does not exist yet, so the
            // candidate itself cannot exist (or be a symlink) either --
            // this is as far as resolution can go.
            let mut base = resolved_dir;
            for comp in missing.into_iter().rev() {
                base.push(comp);
            }
            base.push(name);
            return base;
        }
        let candidate = resolved_dir.join(name);
        let Ok(meta) = std::fs::symlink_metadata(&candidate) else {
            return candidate;
        };
        if !meta.file_type().is_symlink() {
            return candidate;
        }
        let Ok(target) = std::fs::read_link(&candidate) else {
            return candidate;
        };
        current = if target.is_absolute() {
            target
        } else {
            resolved_dir.join(target)
        };
    }
}

/// Canonicalize the longest existing prefix of `dir` (walking up until one
/// canonicalizes), returning that canonical prefix plus the path
/// components below it that do not exist yet, nearest-first (i.e. in
/// reverse of the order they should be re-appended).
fn resolve_longest_existing_ancestor(dir: &Path) -> (PathBuf, Vec<&OsStr>) {
    let mut cur = dir;
    let mut missing = Vec::new();
    loop {
        if let Ok(canon) = cur.canonicalize() {
            return (canon, missing);
        }
        match (cur.file_name(), cur.parent()) {
            (Some(comp), Some(parent)) if !parent.as_os_str().is_empty() => {
                missing.push(comp);
                cur = parent;
            }
            // No existing ancestor at all (a relative path exhausted
            // down to its implicit cwd root, or a component ending in
            // `..` `file_name` can't name): fold *the original* `dir`
            // lexically in one shot instead of re-anchoring `cur` here
            // and separately re-appending whatever was already pushed
            // onto `missing` -- doing both would double up the last
            // component (e.g. `cwd/not-created-yet/not-created-yet`).
            _ => return (anchored_path(dir), Vec::new()),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_config_yaml() -> String {
    r#"
schema_version: 1
instance_id: test-book
venue: lighter
dry_run: true
universe:
  symbols: [BTC, ETH, SOL, DOT]
schedule:
  kind: interval_days
  anchor_date: 2026-07-03
  every_days: 5
  decision_time_utc: "00:30"
  signal_grace_secs: 3600
signal:
  path: /tmp/does-not-matter/signal.json
  producer_id: test_producer
  max_age_secs: 7200
sizing:
  gross_notional_usd: 1000
  max_symbol_weight: 0.5
  max_gross_usd: 1100
  max_net_usd: 150
  min_order_usd: 10
  rebalance_deadband_usd: 5
execution:
  slippage_bps: 50
  max_attempts: 3
  fill_confirm_timeout_secs: 15
  paper_slippage_bps: 5
risk:
  equity_reference_usd: 1000
  max_session_loss_bps: 500
  max_daily_loss_bps: 300
  kill_switch_path: /tmp/does-not-matter/KILL_SWITCH
  risk_ack_path: /tmp/does-not-matter/RISK_ACK
paths:
  state: /tmp/does-not-matter/state.json
  ledger: /tmp/does-not-matter/ledger.jsonl
  pnl: /tmp/does-not-matter/pnl.jsonl
  status: /tmp/does-not-matter/status.json
"#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_validates_the_reference_config() {
        let cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        assert_eq!(cfg.instance_id, "test-book");
        assert_eq!(cfg.decision_time_secs(), 30 * 60);
        assert!(cfg.signal.require_dollar_neutral);
        assert_eq!(cfg.fingerprint().len(), 12);
    }

    #[test]
    fn fingerprint_changes_with_any_field() {
        let a = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        let mut b = a.clone();
        b.sizing.max_net_usd += 1.0;
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn rejects_two_runtime_paths_that_are_the_same_file() {
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.ledger = c.paths.state.clone();
        let e = c.validate().unwrap_err().to_string();
        assert!(e.contains("paths.ledger and paths.state"), "{e}");
        // A runtime file that doubles as a flag file is worse: the kill
        // switch is read as "this file exists".
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.risk.kill_switch_path = c.paths.status.clone();
        assert!(c.validate().is_err());
        // The derived instance-lock path (bot-strategy#937) must be
        // reserved too: RISK_ACK aliasing it would let the engine's normal
        // stray-ack cleanup unlink the lock file's directory entry out
        // from under the running process's held flock, letting a second
        // instance relock a fresh inode at the same pathname.
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.risk.risk_ack_path = c.instance_lock_path();
        let e = c.validate().unwrap_err().to_string();
        assert!(
            e.contains("risk.risk_ack_path") && e.contains("<instance-lock>"),
            "{e}"
        );
        // Different spellings of one path are still one file.
        let dir = tempfile::tempdir().unwrap();
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = dir.path().join("state.json");
        c.paths.pnl = dir.path().join("sub").join("..").join("state.json");
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        assert!(c.validate().is_err());
        // And before the first run has created the directory, where there
        // is nothing to canonicalize against.
        let run = dir.path().join("not-created-yet");
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = run.join("state.json");
        c.paths.pnl = run.join("sub").join("..").join("state.json");
        assert!(c.validate().is_err());
        // A relative spelling of a path is the same file as its absolute
        // one, whatever the working directory writes resolve against.
        let cwd = std::env::current_dir().unwrap();
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = PathBuf::from("not-created-yet/shared.json");
        c.paths.ledger = cwd.join("not-created-yet").join("shared.json");
        assert!(c.validate().is_err());
        // An existing leaf symlink aliases two paths that look distinct
        // right down to the file name.
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("state.json"), "{}").unwrap();
            std::os::unix::fs::symlink(
                dir.path().join("state.json"),
                dir.path().join("ledger.jsonl"),
            )
            .unwrap();
            let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
            c.paths.state = dir.path().join("state.json");
            c.paths.ledger = dir.path().join("ledger.jsonl");
            assert!(c.validate().is_err());
        }
        // Two hard links to one inode canonicalize to two distinct path
        // strings -- unlike a symlink, neither is "the real path" -- so the
        // check must also compare device+inode identity, not just resolved
        // path names.
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("state.json"), "{}").unwrap();
            std::fs::hard_link(
                dir.path().join("state.json"),
                dir.path().join("ledger.jsonl"),
            )
            .unwrap();
            let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
            c.paths.state = dir.path().join("state.json");
            c.paths.ledger = dir.path().join("ledger.jsonl");
            assert!(c.validate().is_err());
        }
        // A leaf symlink whose target does not exist yet (created before
        // the file it points at, as on a fresh run) still aliases the two
        // paths -- whole-path canonicalization fails on both, but the
        // dangling link itself must still be followed rather than compared
        // by its raw, unresolved leaf name.
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(
                dir.path().join("state.json"),
                dir.path().join("ledger.jsonl"),
            )
            .unwrap();
            let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
            c.paths.state = dir.path().join("state.json");
            c.paths.ledger = dir.path().join("ledger.jsonl");
            assert!(c.validate().is_err());
        }
        // A chained dangling symlink (ledger.jsonl -> alias -> state.json,
        // none of the three existing yet) must still resolve through both
        // hops to the same path as state.json, not stop at the
        // intermediate `alias` name.
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(dir.path().join("alias"), dir.path().join("ledger.jsonl"))
                .unwrap();
            std::os::unix::fs::symlink(dir.path().join("state.json"), dir.path().join("alias"))
                .unwrap();
            let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
            c.paths.state = dir.path().join("state.json");
            c.paths.ledger = dir.path().join("ledger.jsonl");
            assert!(c.validate().is_err());
        }
        // signal.path aliasing another runtime path is just as unsafe: the
        // engine reads it directly, so an alias to the ledger makes the
        // signal unparsable, and an alias to the kill switch blocks every
        // opening.
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.signal.path = c.paths.ledger.clone();
        let e = c.validate().unwrap_err().to_string();
        assert!(e.contains("signal.path"), "{e}");
    }

    #[test]
    #[cfg(unix)]
    fn rejects_paths_state_that_is_itself_a_symlink() {
        // instance_lock_path derives from paths.state resolved through
        // symlinks, but the first atomic persist (rename onto the exact
        // paths.state pathname) replaces a leaf symlink rather than
        // writing through it -- so an alias that looked safe at startup
        // would silently stop being one after the first persist. Refusing
        // to start at all is simpler and more robust than trying to keep
        // every downstream consumer (locking, loading, persisting) in
        // sync with an alias that cannot itself survive a single write.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("real.json"), "{}").unwrap();
        std::os::unix::fs::symlink(dir.path().join("real.json"), dir.path().join("state.json"))
            .unwrap();
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = dir.path().join("state.json");
        let e = c.validate().unwrap_err().to_string();
        assert!(e.contains("paths.state") && e.contains("symlink"), "{e}");
        // A symlinked *ancestor directory* is unaffected: the kernel
        // resolves it transparently on every read/write/rename, so it is
        // never replaced the way a leaf symlink is.
        let real_dir = tempfile::tempdir().unwrap();
        std::fs::write(real_dir.path().join("state.json"), "{}").unwrap();
        let link_dir = tempfile::tempdir().unwrap();
        std::fs::remove_dir(link_dir.path()).unwrap();
        std::os::unix::fs::symlink(real_dir.path(), link_dir.path()).unwrap();
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = link_dir.path().join("state.json");
        c.validate().unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn resolve_leaf_gives_up_on_a_symlink_cycle_instead_of_hanging() {
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(dir.path().join("b"), dir.path().join("a")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("a"), dir.path().join("b")).unwrap();
        // Must terminate (the test itself is the timeout) and return some
        // deterministic path rather than looping forever.
        let resolved = resolved_path(&dir.path().join("a"));
        assert!(
            resolved.ends_with("a") || resolved.ends_with("b"),
            "{resolved:?}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn resolved_path_follows_a_symlinked_ancestor_above_a_not_yet_created_directory() {
        // link -> real, and real/new does not exist yet: link/new/state.json
        // and real/new/state.json must resolve to the same path even
        // though neither the `new` directory nor `state.json` exists.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("real")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("link")).unwrap();
        let via_link = resolved_path(&dir.path().join("link").join("new").join("state.json"));
        let via_real = resolved_path(&dir.path().join("real").join("new").join("state.json"));
        assert_eq!(via_link, via_real, "{via_link:?} vs {via_real:?}");

        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = dir.path().join("real").join("new").join("state.json");
        c.paths.ledger = dir.path().join("link").join("new").join("state.json");
        assert!(c.validate().is_err());
    }

    #[test]
    #[cfg(unix)]
    fn resolved_path_re_resolves_a_symlink_target_that_passes_through_a_symlinked_ancestor() {
        // alias -> link/new/state.json, link -> real, real exists but
        // real/new does not: following the dangling leaf `alias` lands on
        // `link/new/state.json`, and `link` itself must still be resolved
        // through to `real` (with `new` appended lexically) rather than
        // returned as the raw post-follow path.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("real")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("link")).unwrap();
        std::os::unix::fs::symlink(
            dir.path().join("link").join("new").join("state.json"),
            dir.path().join("alias"),
        )
        .unwrap();
        let via_alias = resolved_path(&dir.path().join("alias"));
        let via_real = resolved_path(&dir.path().join("real").join("new").join("state.json"));
        assert_eq!(via_alias, via_real, "{via_alias:?} vs {via_real:?}");

        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.paths.state = dir.path().join("real").join("new").join("state.json");
        c.paths.ledger = dir.path().join("alias");
        assert!(c.validate().is_err());
    }

    #[test]
    fn load_rejects_a_config_file_that_aliases_one_of_its_own_runtime_paths() {
        // `BookConfig::validate` alone can't see this collision: the
        // `--config` file itself is never one of the `named` runtime
        // paths, so only `load` (which knows the file it just parsed) can
        // catch a YAML that points an output at itself.
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("book.yaml");
        let mut cfg: BookConfig = serde_yaml::from_str(&test_config_yaml()).unwrap();
        cfg.paths.ledger = config_path.clone();
        std::fs::write(&config_path, serde_yaml::to_string(&cfg).unwrap()).unwrap();
        let e = BookConfig::load(&config_path).unwrap_err().to_string();
        assert!(
            e.contains("paths.ledger") && e.contains("<--config>"),
            "{e}"
        );
        // A config file that only shares a path with another runtime
        // output, not itself, still loads fine.
        let config_path2 = dir.path().join("ok.yaml");
        let cfg2: BookConfig = serde_yaml::from_str(&test_config_yaml()).unwrap();
        std::fs::write(&config_path2, serde_yaml::to_string(&cfg2).unwrap()).unwrap();
        assert!(BookConfig::load(&config_path2).is_ok());
    }

    #[test]
    fn rejects_paper_slippage_of_a_hundred_percent_or_more() {
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.execution.paper_slippage_bps = 9_999.0;
        assert!(c.validate().is_ok());
        c.execution.paper_slippage_bps = 10_000.0;
        let e = c.validate().unwrap_err().to_string();
        assert!(e.contains("paper_slippage_bps"), "{e}");
    }

    #[test]
    fn rejects_live_slippage_of_a_hundred_percent_or_more() {
        let mut c = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        c.execution.slippage_bps = 9_999;
        assert!(c.validate().is_ok());
        c.execution.slippage_bps = 10_000;
        let e = c.validate().unwrap_err().to_string();
        assert!(e.contains("execution.slippage_bps"), "{e}");
    }

    #[test]
    fn rejects_unknown_fields_and_bad_values() {
        let bad = test_config_yaml().replace("dry_run: true", "dry_run: true\nbogus: 1");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace("max_symbol_weight: 0.5", "max_symbol_weight: 1.5");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad =
            test_config_yaml().replace("symbols: [BTC, ETH, SOL, DOT]", "symbols: [BTC, BTC]");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace(
            "decision_time_utc: \"00:30\"",
            "decision_time_utc: \"24:30\"",
        );
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace(
            "signal_grace_secs: 3600",
            "signal_grace_secs: 3600\n  flatten_after_secs: 60",
        );
        assert!(BookConfig::from_yaml_str(&bad).is_err());
    }

    #[test]
    fn rejects_a_venue_this_build_has_no_connector_for() {
        let bad = test_config_yaml().replace("venue: lighter", "venue: bitmex");
        let err = BookConfig::from_yaml_str(&bad).unwrap_err().to_string();
        assert!(err.contains("not available in this build"), "{err}");
        // Hyperliquid is only accepted when the binary carries its SDK,
        // which the released book-runtime does not.
        let hl = test_config_yaml().replace("venue: lighter", "venue: hyperliquid");
        let parsed = BookConfig::from_yaml_str(&hl);
        if cfg!(feature = "hyperliquid-sdk") {
            assert!(parsed.is_ok());
        } else {
            assert!(parsed
                .unwrap_err()
                .to_string()
                .contains("not available in this build"));
        }
    }

    #[test]
    fn rejects_nan_limits_and_overlapping_flattens() {
        let bad =
            test_config_yaml().replace("max_session_loss_bps: 500", "max_session_loss_bps: .nan");
        assert!(BookConfig::from_yaml_str(&bad)
            .unwrap_err()
            .to_string()
            .contains("finite"));
        let bad =
            test_config_yaml().replace("equity_reference_usd: 1000", "equity_reference_usd: .inf");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        let bad = test_config_yaml().replace("max_net_usd: 150", "max_net_usd: .nan");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
        // 5-day cadence: a 6-day flatten overlaps the next decision.
        let bad = test_config_yaml().replace(
            "signal_grace_secs: 3600",
            "signal_grace_secs: 3600\n  flatten_after_secs: 518400",
        );
        assert!(BookConfig::from_yaml_str(&bad)
            .unwrap_err()
            .to_string()
            .contains("cadence"));
        let ok = test_config_yaml().replace(
            "signal_grace_secs: 3600",
            "signal_grace_secs: 3600\n  flatten_after_secs: 86400",
        );
        assert!(BookConfig::from_yaml_str(&ok).is_ok());
        let bad =
            test_config_yaml().replace("signal_grace_secs: 3600", "signal_grace_secs: 432000");
        assert!(BookConfig::from_yaml_str(&bad).is_err());
    }

    #[test]
    fn calendar_kind_requires_a_path_and_no_flatten_after() {
        let y = test_config_yaml()
            .replace("kind: interval_days", "kind: calendar")
            .replace(
                "  anchor_date: 2026-07-03\n  every_days: 5\n  decision_time_utc: \"00:30\"\n",
                "",
            );
        assert!(BookConfig::from_yaml_str(&y).is_err());
        let y = y.replace(
            "signal_grace_secs: 3600",
            "signal_grace_secs: 3600\n  calendar_path: /tmp/cal.json",
        );
        assert!(BookConfig::from_yaml_str(&y).is_ok());
    }
}
