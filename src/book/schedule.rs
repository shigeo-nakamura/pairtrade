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

/// One entry of a `kind: calendar` file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarEntry {
    pub decision_key: String,
    pub decision_at: DateTime<Utc>,
    #[serde(default)]
    pub flatten_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        let calendar = match cfg.kind {
            ScheduleKind::Calendar => {
                let path = cfg
                    .calendar_path
                    .as_deref()
                    .context("calendar_path required")?;
                load_calendar(path)?
            }
            _ => Vec::new(),
        };
        Self::build(cfg, calendar)
    }

    /// Construct with an explicit calendar (tests / replay).
    pub fn build(cfg: &ScheduleConfig, mut calendar: Vec<CalendarEntry>) -> Result<Self> {
        calendar.sort_by_key(|e| e.decision_at);
        for w in calendar.windows(2) {
            if w[0].decision_key == w[1].decision_key {
                bail!("calendar has duplicate decision_key {}", w[0].decision_key);
            }
        }
        for e in &calendar {
            if let Some(f) = e.flatten_at {
                if f <= e.decision_at {
                    bail!(
                        "calendar entry {} has flatten_at <= decision_at",
                        e.decision_key
                    );
                }
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
        Decision {
            key: e.decision_key.clone(),
            decision_at: e.decision_at.timestamp(),
            window_end: e.decision_at.timestamp() + self.grace_secs,
            flatten_at: e.flatten_at.map(|t| t.timestamp()),
        }
    }

    /// The most recent decision whose `decision_at <= now`, if any.
    pub fn current(&self, now: i64) -> Option<Decision> {
        match self.kind {
            ScheduleKind::Calendar => self
                .calendar
                .iter()
                .rev()
                .find(|e| e.decision_at.timestamp() <= now)
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
                .find(|e| e.decision_at.timestamp() > now)
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

    fn interval() -> Scheduler {
        let cfg = BookConfig::from_yaml_str(&test_config_yaml()).unwrap();
        Scheduler::from_config(&cfg.schedule).unwrap()
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
        // duplicate key rejected
        let mut dup = entries.clone();
        dup.push(entries[0].clone());
        assert!(Scheduler::build(&cfg.schedule, dup).is_err());
        // flatten before decision rejected
        let mut bad = entries.clone();
        bad[0].flatten_at = Some(bad[0].decision_at);
        assert!(Scheduler::build(&cfg.schedule, bad).is_err());
    }
}
