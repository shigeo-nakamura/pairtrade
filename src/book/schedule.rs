//! Decision scheduling, see `docs/book-runtime.md` §4.
//!
//! A `Decision` is one point in time at which the runtime is allowed to read
//! a signal for a given `decision_key`, with a grace window after it and an
//! optional fixed-window `flatten_at`. Everything here is pure arithmetic
//! on unix seconds so it can be driven by the wall clock (live) or by the
//! replay driver.

use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::config::{parse_hhmm, ScheduleConfig, ScheduleKind};
use super::signal::ceil_secs;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub key: String,
    /// Unix seconds.
    pub decision_at: i64,
    /// Unix seconds; the last instant a signal is accepted for this key.
    pub window_end: i64,
    /// Unix seconds at which the book must be flat again, if fixed-window.
    pub flatten_at: Option<i64>,
}

/// One entry of a `kind: calendar` file. `deny_unknown_fields` matters
/// more here than elsewhere: a misspelled `flatten_at` (e.g.
/// `flatten_after`) would otherwise be silently dropped and defaulted to
/// `None`, and the runtime would accept the calendar but never close that
/// entry's position -- potentially leaving live exposure open
/// indefinitely instead of failing to load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarEntry {
    pub decision_key: String,
    pub decision_at: DateTime<Utc>,
    #[serde(default)]
    pub flatten_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarFile {
    #[serde(default)]
    pub calendar_version: String,
    pub entries: Vec<CalendarEntry>,
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    kind: ScheduleKind,
    anchor: Option<NaiveDate>,
    every_days: i64,
    decision_secs: i64,
    grace_secs: i64,
    flatten_after_secs: Option<i64>,
    calendar: Vec<CalendarEntry>,
}

pub fn date_key(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

fn unix(d: NaiveDate, secs_after_midnight: i64) -> i64 {
    let t = NaiveTime::from_num_seconds_from_midnight_opt(secs_after_midnight as u32, 0)
        .expect("validated HH:MM");
    Utc.from_utc_datetime(&d.and_time(t)).timestamp()
}

impl Scheduler {
    pub fn from_config(cfg: &ScheduleConfig) -> Result<Self> {
        Self::from_config_with_calendar(cfg, None)
    }

    /// `from_config`, but reading the calendar from `calendar_override`
    /// when given, for `book_runtime --validate --calendar <path>`
    /// (bot-strategy#952). `install_book_runtime.sh` must validate a
    /// *staged* calendar before promoting it to `schedule.calendar_path`,
    /// and pointing the binary at the staged copy is what lets the
    /// installer reuse this exact code -- serde's `deny_unknown_fields`,
    /// chrono's timestamp parsing, and `build`'s duplicate-key, flatten
    /// and overlap rules -- instead of re-implementing them in Python,
    /// where every mismatch between the two is a config the installer
    /// promotes and the service then refuses to start on.
    pub fn from_config_with_calendar(
        cfg: &ScheduleConfig,
        calendar_override: Option<&Path>,
    ) -> Result<Self> {
        let calendar = match cfg.kind {
            ScheduleKind::Calendar => {
                let path = calendar_override
                    .or(cfg.calendar_path.as_deref())
                    .context("calendar_path required")?;
                load_calendar(path)?
            }
            _ => {
                // Fail closed: an override for a schedule that never reads
                // a calendar would validate a file the runtime ignores.
                if let Some(p) = calendar_override {
                    bail!(
                        "calendar {} given for a {:?} schedule, which never loads one",
                        p.display(),
                        cfg.kind
                    );
                }
                Vec::new()
            }
        };
        Self::build(cfg, calendar)
    }

    /// The calendar this scheduler was built from (empty for
    /// `interval_days` / `daily`), so `--validate` can report what it read.
    pub fn calendar(&self) -> &[CalendarEntry] {
        &self.calendar
    }

    /// Construct with an explicit calendar (tests / replay).
    pub fn build(cfg: &ScheduleConfig, mut calendar: Vec<CalendarEntry>) -> Result<Self> {
        calendar.sort_by_key(|e| e.decision_at);
        let mut keys = std::collections::HashSet::new();
        for e in &calendar {
            if !keys.insert(e.decision_key.as_str()) {
                bail!("calendar has duplicate decision_key {}", e.decision_key);
            }
            if let Some(f) = e.flatten_at {
                // The flatten must clear the whole signal window: a
                // decision accepted late in the window would otherwise open
                // legs after their own mandated exit (the tick processes
                // flattens before decisions). Same invariant the config
                // enforces for `flatten_after_secs`. Validated on the same
                // `ceil_secs`-rounded seconds `calendar_decision` actually
                // runs on, not the raw fractional instants: a decision at
                // `T.1` with 10s grace and flatten at `T+10.2` looks fine
                // compared as DateTimes, but both round up to the same
                // second `T+11` at runtime, and the tick that accepts a
                // signal at that inclusive final second processes the
                // flatten before the decision, opening a leg after its own
                // mandated exit.
                let window_end = ceil_secs(e.decision_at) + cfg.signal_grace_secs.max(0);
                if ceil_secs(f) <= window_end {
                    bail!(
                        "calendar entry {} has flatten_at {} inside its signal window (ends {})",
                        e.decision_key,
                        f,
                        window_end
                    );
                }
            }
        }
        // No overlap: an entry's signal window and exit must be over before
        // the next decision starts (the runtime keeps a single decision
        // record). Same rounded seconds as above.
        for w in calendar.windows(2) {
            let grace = cfg.signal_grace_secs.max(0);
            let d0 = ceil_secs(w[0].decision_at);
            let first_end = (d0 + grace).max(w[0].flatten_at.map(ceil_secs).unwrap_or(d0));
            // Equality is overlap too: the window end is inclusive, but at
            // that exact instant `Scheduler::current` already selects the
            // next entry, so a first-entry signal arriving at its last
            // permitted second would never be read, and an absent first
            // entry could never be recorded as skipped before the second
            // key replaces it.
            let d1 = ceil_secs(w[1].decision_at);
            if first_end >= d1 {
                bail!(
                    "calendar entries {} and {} overlap: the first's window runs through {} (inclusive) but the second starts at {}",
                    w[0].decision_key,
                    w[1].decision_key,
                    first_end,
                    w[1].decision_at
                );
            }
        }
        Ok(Self {
            kind: cfg.kind,
            anchor: cfg.anchor_date,
            every_days: match cfg.kind {
                ScheduleKind::Daily => 1,
                _ => cfg.every_days.unwrap_or(1) as i64,
            },
            decision_secs: cfg
                .decision_time_utc
                .as_deref()
                .map(parse_hhmm)
                .transpose()?
                .unwrap_or(0),
            grace_secs: cfg.signal_grace_secs,
            flatten_after_secs: cfg.flatten_after_secs,
            calendar,
        })
    }

    fn decision_for_date(&self, d: NaiveDate) -> Decision {
        let at = unix(d, self.decision_secs);
        Decision {
            key: date_key(d),
            decision_at: at,
            window_end: at + self.grace_secs,
            flatten_at: self.flatten_after_secs.map(|s| at + s),
        }
    }

    fn calendar_decision(&self, e: &CalendarEntry) -> Decision {
        // Rounded up (`signal::ceil_secs`), not floored: a configured
        // instant with a fractional remainder (e.g. `T + 0.5s`) must not
        // let a whole-second tick at `T` select or flatten it half a
        // second early, submitting a prepublished signal or closing
        // before the operator's own configured decision_at/flatten_at.
        let decision_at = ceil_secs(e.decision_at);
        Decision {
            key: e.decision_key.clone(),
            decision_at,
            window_end: decision_at + self.grace_secs,
            flatten_at: e.flatten_at.map(ceil_secs),
        }
    }

    /// The most recent decision whose `decision_at <= now`, if any.
    pub fn current(&self, now: i64) -> Option<Decision> {
        match self.kind {
            ScheduleKind::Calendar => self
                .calendar
                .iter()
                .rev()
                .find(|e| ceil_secs(e.decision_at) <= now)
                .map(|e| self.calendar_decision(e)),
            ScheduleKind::Daily | ScheduleKind::IntervalDays => {
                let today = Utc.timestamp_opt(now, 0).single()?.date_naive();
                let mut d = self.aligned_on_or_before(today)?;
                if unix(d, self.decision_secs) > now {
                    d = self.aligned_on_or_before(d - Duration::days(1))?;
                }
                Some(self.decision_for_date(d))
            }
        }
    }

    /// The first decision strictly after `now`.
    pub fn next_after(&self, now: i64) -> Option<Decision> {
        match self.kind {
            ScheduleKind::Calendar => self
                .calendar
                .iter()
                .find(|e| ceil_secs(e.decision_at) > now)
                .map(|e| self.calendar_decision(e)),
            ScheduleKind::Daily | ScheduleKind::IntervalDays => {
                let today = Utc.timestamp_opt(now, 0).single()?.date_naive();
                // Before the anchor the first decision is the anchor itself.
                let mut d = match self.aligned_on_or_before(today) {
                    Some(d) => d,
                    None => self.anchor?,
                };
                while unix(d, self.decision_secs) <= now {
                    d += Duration::days(self.every_days);
                }
                Some(self.decision_for_date(d))
            }
        }
    }

    /// Latest scheduled date <= `d` (anchor-aligned for interval_days).
    fn aligned_on_or_before(&self, d: NaiveDate) -> Option<NaiveDate> {
        match self.kind {
            ScheduleKind::Daily => Some(d),
            ScheduleKind::IntervalDays => {
                let anchor = self.anchor?;
                let delta = (d - anchor).num_days();
                if delta < 0 {
                    return None;
                }
                let k = delta / self.every_days;
                Some(anchor + Duration::days(k * self.every_days))
            }
            ScheduleKind::Calendar => None,
        }
    }

    /// All decisions with `decision_at` in `[from, to]`, ascending (replay).
    pub fn decisions_between(&self, from: i64, to: i64) -> Vec<Decision> {
        let mut out = Vec::new();
        let mut cursor = from - 1;
        while let Some(d) = self.next_after(cursor) {
            if d.decision_at > to {
                break;
            }
            cursor = d.decision_at;
            out.push(d);
        }
        out
    }
}

pub fn load_calendar(path: &Path) -> Result<Vec<CalendarEntry>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read calendar {}", path.display()))?;
    let file: CalendarFile = serde_json::from_str(&text)
        .with_context(|| format!("parse calendar {}", path.display()))?;
    Ok(file.entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::config::{test_config_yaml, BookConfig};

    fn ts(s: &str) -> i64 {
        DateTime::parse_from_rfc3339(s).unwrap().timestamp()
    }

    #[test]
    fn load_calendar_rejects_a_misspelled_flatten_field_instead_of_dropping_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("calendar.json");
        // `flatten_after` is not a field: without deny_unknown_fields this
        // would silently deserialize as an entry with no flatten_at at
        // all, accepting the calendar but never closing the position.
        std::fs::write(
            &path,
            r#"{"entries":[{"decision_key":"2026-09-08","decision_at":"2026-09-08T06:30:00Z","flatten_after":"2026-09-08T13:30:00Z"}]}"#,
        )
        .unwrap();
        let e = format!("{:#}", load_calendar(&path).unwrap_err());
        assert!(e.to_lowercase().contains("flatten_after"), "{e}");
    }

    fn interval() -> Scheduler {
        let cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        Scheduler::from_config(&cfg.schedule).unwrap()
    }

    /// `--validate --calendar` has to reach the same rules the service
    /// starts on, and has to refuse a file the service would never read
    /// (bot-strategy#952).
    #[test]
    fn calendar_override_is_read_for_calendar_kinds_and_refused_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let staged = dir.path().join("staged.calendar.json");
        std::fs::write(
            &staged,
            r#"{"calendar_version":"v1","entries":[{"decision_key":"2026-09-15","decision_at":"2026-09-15T13:29:00Z","flatten_at":"2026-09-15T13:36:00Z"}]}"#,
        )
        .unwrap();

        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        cfg.schedule.kind = ScheduleKind::Calendar;
        cfg.schedule.flatten_after_secs = None;
        // The exdiv instance's grace: the 7-minute window between decision
        // and flatten only clears a grace this short.
        cfg.schedule.signal_grace_secs = 45;
        // The configured path does not exist yet -- that is the installer's
        // case, validating before it promotes the staged file.
        cfg.schedule.calendar_path = Some(dir.path().join("not-installed-yet.json"));
        let s = Scheduler::from_config_with_calendar(&cfg.schedule, Some(&staged)).unwrap();
        assert_eq!(s.calendar().len(), 1);
        assert_eq!(
            s.current(ts("2026-09-15T13:29:30Z")).unwrap().key,
            "2026-09-15"
        );
        // Without the override the missing configured path still fails.
        assert!(Scheduler::from_config(&cfg.schedule).is_err());
        // A staged calendar that breaks a build rule is rejected here, not
        // at the service's next start.
        let bad = dir.path().join("bad.calendar.json");
        std::fs::write(
            &bad,
            r#"{"calendar_version":"v1","entries":[{"decision_key":"2026-09-15","decision_at":"2026-09-15T13:29:00Z","flatten_at":"2026-09-15T13:29:10Z"}]}"#,
        )
        .unwrap();
        assert!(Scheduler::from_config_with_calendar(&cfg.schedule, Some(&bad)).is_err());

        // An interval_days config never loads a calendar, so validating
        // one against it would report on a file the runtime ignores.
        let interval_cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        let e = format!(
            "{:#}",
            Scheduler::from_config_with_calendar(&interval_cfg.schedule, Some(&staged))
                .unwrap_err()
        );
        assert!(e.contains("never loads one"), "{e}");
    }

    /// The committed exdiv-lighter calendar must build against the
    /// committed config: CI regenerates the calendar from the events file
    /// but only this check runs the runtime's own rules over the result
    /// (bot-strategy#948 / #952).
    #[test]
    fn committed_exdiv_calendar_builds_against_its_config() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let cfg = BookConfig::load(&root.join("configs/book/exdiv-lighter.yaml")).unwrap();
        let calendar = root.join("configs/book/exdiv-lighter.calendar.json");
        let s = Scheduler::from_config_with_calendar(&cfg.schedule, Some(&calendar)).unwrap();
        assert!(!s.calendar().is_empty());
    }

    #[test]
    fn interval_days_aligns_to_anchor() {
        let s = interval(); // anchor 2026-07-03, every 5d, 00:30, grace 3600
                            // 2026-09-06 is anchor + 65 days = 13 * 5 → a decision day.
        let d = s.current(ts("2026-09-06T00:31:00Z")).unwrap();
        assert_eq!(d.key, "2026-09-06");
        assert_eq!(d.decision_at, ts("2026-09-06T00:30:00Z"));
        assert_eq!(d.window_end, ts("2026-09-06T01:30:00Z"));
        assert_eq!(d.flatten_at, None);
        // Before 00:30 on a decision day the current decision is the prior one.
        let d = s.current(ts("2026-09-06T00:10:00Z")).unwrap();
        assert_eq!(d.key, "2026-09-01");
        // A non-decision day still reports the last decision.
        let d = s.current(ts("2026-09-08T12:00:00Z")).unwrap();
        assert_eq!(d.key, "2026-09-06");
        // next_after walks forward on the grid.
        let n = s.next_after(ts("2026-09-06T00:31:00Z")).unwrap();
        assert_eq!(n.key, "2026-09-11");
        let n = s.next_after(ts("2026-09-06T00:10:00Z")).unwrap();
        assert_eq!(n.key, "2026-09-06");
        // Before the anchor there is no current decision, but the next one
        // is the anchor itself.
        assert!(s.current(ts("2026-07-01T00:00:00Z")).is_none());
        assert_eq!(
            s.next_after(ts("2026-07-01T00:00:00Z")).unwrap().key,
            "2026-07-03"
        );
        assert_eq!(
            s.decisions_between(ts("2026-07-01T00:00:00Z"), ts("2026-07-09T00:00:00Z"))
                .iter()
                .map(|d| d.key.clone())
                .collect::<Vec<_>>(),
            vec!["2026-07-03", "2026-07-08"]
        );
    }

    #[test]
    fn decisions_between_lists_the_grid() {
        let s = interval();
        let list = s.decisions_between(ts("2026-09-01T00:00:00Z"), ts("2026-09-12T00:00:00Z"));
        let keys: Vec<_> = list.iter().map(|d| d.key.as_str()).collect();
        assert_eq!(keys, vec!["2026-09-01", "2026-09-06", "2026-09-11"]);
    }

    #[test]
    fn daily_with_flatten_after() {
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        cfg.schedule.kind = ScheduleKind::Daily;
        cfg.schedule.flatten_after_secs = Some(6 * 3600);
        let s = Scheduler::from_config(&cfg.schedule).unwrap();
        let d = s.current(ts("2026-09-07T05:00:00Z")).unwrap();
        assert_eq!(d.key, "2026-09-07");
        assert_eq!(d.flatten_at, Some(ts("2026-09-07T06:30:00Z")));
        assert_eq!(
            s.next_after(ts("2026-09-07T05:00:00Z")).unwrap().key,
            "2026-09-08"
        );
    }

    #[test]
    fn calendar_kind_uses_entries_and_rejects_bad_ones() {
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        cfg.schedule.kind = ScheduleKind::Calendar;
        cfg.schedule.flatten_after_secs = None;
        let entries = vec![
            CalendarEntry {
                decision_key: "2026-09-08".into(),
                decision_at: DateTime::parse_from_rfc3339("2026-09-08T06:30:00Z")
                    .unwrap()
                    .into(),
                flatten_at: Some(
                    DateTime::parse_from_rfc3339("2026-09-08T13:30:00Z")
                        .unwrap()
                        .into(),
                ),
            },
            CalendarEntry {
                decision_key: "2026-09-09".into(),
                decision_at: DateTime::parse_from_rfc3339("2026-09-09T06:30:00Z")
                    .unwrap()
                    .into(),
                flatten_at: Some(
                    DateTime::parse_from_rfc3339("2026-09-09T13:30:00Z")
                        .unwrap()
                        .into(),
                ),
            },
        ];
        let s = Scheduler::build(&cfg.schedule, entries.clone()).unwrap();
        assert!(s.current(ts("2026-09-08T06:00:00Z")).is_none());
        let d = s.current(ts("2026-09-08T07:00:00Z")).unwrap();
        assert_eq!(d.key, "2026-09-08");
        assert_eq!(d.flatten_at, Some(ts("2026-09-08T13:30:00Z")));
        assert_eq!(
            s.next_after(ts("2026-09-08T07:00:00Z")).unwrap().key,
            "2026-09-09"
        );
        assert!(s.next_after(ts("2026-09-09T07:00:00Z")).is_none());
        // a flatten inside the signal window is rejected
        let mut inside = entries.clone();
        inside[0].flatten_at = Some(
            DateTime::parse_from_rfc3339("2026-09-08T06:35:00Z")
                .unwrap()
                .into(),
        );
        assert!(Scheduler::build(&cfg.schedule, inside)
            .unwrap_err()
            .to_string()
            .contains("signal window"));
        // duplicate key rejected, also when another entry sits in between (A, B, A)
        let mut dup = entries.clone();
        dup.push(CalendarEntry {
            decision_key: "2026-09-08".into(),
            decision_at: DateTime::parse_from_rfc3339("2026-09-10T06:30:00Z")
                .unwrap()
                .into(),
            flatten_at: None,
        });
        assert!(Scheduler::build(&cfg.schedule, dup)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        // overlapping flatten rejected
        let mut over = entries.clone();
        over[0].flatten_at = Some(
            DateTime::parse_from_rfc3339("2026-09-09T07:00:00Z")
                .unwrap()
                .into(),
        );
        assert!(Scheduler::build(&cfg.schedule, over)
            .unwrap_err()
            .to_string()
            .contains("overlap"));
        // flatten before decision rejected
        let mut bad = entries.clone();
        bad[0].flatten_at = Some(bad[0].decision_at);
        assert!(Scheduler::build(&cfg.schedule, bad).is_err());
        // A first window ending *exactly* when the second decision starts
        // is overlap too: at that instant `current` already selects the
        // second entry, so the first's last permitted arrival second is
        // unreachable and an absent first entry is never recorded skipped.
        let mut touching = entries.clone();
        touching[0].flatten_at = Some(
            DateTime::parse_from_rfc3339("2026-09-09T06:30:00Z")
                .unwrap()
                .into(),
        );
        let e = Scheduler::build(&cfg.schedule, touching)
            .unwrap_err()
            .to_string();
        assert!(e.contains("overlap") && e.contains("inclusive"), "{e}");
    }

    #[test]
    fn a_fractional_calendar_boundary_is_rounded_up_not_down() {
        // decision_at/flatten_at floored via .timestamp() would let a
        // whole-second tick at the floor select the decision, or flatten,
        // half a second before the operator's configured instant --
        // submitting a prepublished signal, or closing, early.
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        cfg.schedule.kind = ScheduleKind::Calendar;
        cfg.schedule.flatten_after_secs = None;
        let entries = vec![CalendarEntry {
            decision_key: "2026-09-08".into(),
            decision_at: DateTime::parse_from_rfc3339("2026-09-08T06:30:00.5Z")
                .unwrap()
                .into(),
            flatten_at: Some(
                DateTime::parse_from_rfc3339("2026-09-08T13:30:00.5Z")
                    .unwrap()
                    .into(),
            ),
        }];
        let s = Scheduler::build(&cfg.schedule, entries).unwrap();
        // Not yet current at the floor of decision_at.
        assert!(s.current(ts("2026-09-08T06:30:00Z")).is_none());
        let d = s.current(ts("2026-09-08T06:30:01Z")).unwrap();
        assert_eq!(d.decision_at, ts("2026-09-08T06:30:01Z"));
        assert_eq!(d.flatten_at, Some(ts("2026-09-08T13:30:01Z")));
    }

    #[test]
    fn calendar_validation_uses_the_same_rounded_seconds_as_the_runtime() {
        // decision_at = T+0.1s, flatten_at = T+10.2s, grace = 10s:
        // compared as raw DateTimes the flatten (10.2s after T) looks
        // safely past the window's raw end (0.1 + 10 = 10.1s after T),
        // but calendar_decision rounds both up to the same runtime second
        // (1 and 11) -- the flatten lands inside, not after, the window
        // it is supposed to clear. Validation must reject this the same
        // way it already rejects two DateTimes that were equal to begin
        // with.
        let mut cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        cfg.schedule.kind = ScheduleKind::Calendar;
        cfg.schedule.flatten_after_secs = None;
        cfg.schedule.signal_grace_secs = 10;
        let entries = vec![CalendarEntry {
            decision_key: "2026-09-08".into(),
            decision_at: DateTime::parse_from_rfc3339("2026-09-08T00:00:00.1Z")
                .unwrap()
                .into(),
            flatten_at: Some(
                DateTime::parse_from_rfc3339("2026-09-08T00:00:10.2Z")
                    .unwrap()
                    .into(),
            ),
        }];
        let e = Scheduler::build(&cfg.schedule, entries)
            .unwrap_err()
            .to_string();
        assert!(e.contains("inside its signal window"), "{e}");
    }
}
