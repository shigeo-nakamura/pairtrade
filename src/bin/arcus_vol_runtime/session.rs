//! The venue trading session of an RWA perp (bot-strategy#1093 session
//! offset): while the underlying trades (Arcus "RTH", 04:00–20:00 ET for the
//! US equity/ETF perps) the presence quotes rest further behind the touch
//! than off-hours, because swept quotes cost ≈ −1.4 bp/$ in-session against
//! ≈ −0.1 bp/$ off-hours (grid_report.md, 2026-10-05).
//!
//! Source of truth: the venue's own market row (`GET /v1/markets?market=`):
//! `isOutsideRth` (holiday-aware) while a read of it is fresh, else the
//! row's `regularTradingHours` evaluated on the local clock. Only the
//! `America/New_York` zone is supported (every Arcus RWA perp uses it); its
//! US daylight-saving rule is applied here so no timezone database is
//! needed. A market without hours (crypto: `regularTradingHours: null`) has
//! no session, and nothing switches.

use chrono::{Datelike, NaiveDate, Weekday};
use serde_json::Value;

/// A venue `isOutsideRth` read older than this no longer decides: the
/// local hours do (a missed boundary flips within one refresh anyway).
pub const VENUE_FLAG_TTL_MS: u64 = 180_000;

/// `regularTradingHours` of a market row, in seconds of the New York day.
/// `overnight`: the session starts one evening and ends the next morning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradingHours {
    pub start_s: u32,
    pub end_s: u32,
    pub overnight: bool,
}

/// What one market row says about the session.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketSession {
    pub hours: Option<TradingHours>,
    pub outside_rth: Option<bool>,
    /// The row's timezone when it is not one this module can evaluate.
    pub unsupported_tz: Option<String>,
}

pub const NEW_YORK: &str = "America/New_York";

fn nth_sunday(year: i32, month: u32, n: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("valid month");
    let to_sunday = (7 - first.weekday().num_days_from_sunday()) % 7;
    first + chrono::Duration::days(i64::from(to_sunday + 7 * (n - 1)))
}

/// New York's UTC offset in seconds at `utc_ms`: −4 h from the second
/// Sunday of March 02:00 EST (07:00 UTC) to the first Sunday of November
/// 02:00 EDT (06:00 UTC), −5 h otherwise (US rule since 2007).
pub fn new_york_utc_offset_secs(utc_ms: u64) -> i64 {
    let secs = (utc_ms / 1_000) as i64;
    let Some(t) = chrono::DateTime::from_timestamp(secs, 0) else {
        return -5 * 3_600;
    };
    let year = t.year();
    let start = nth_sunday(year, 3, 2)
        .and_hms_opt(7, 0, 0)
        .expect("valid time")
        .and_utc()
        .timestamp();
    let end = nth_sunday(year, 11, 1)
        .and_hms_opt(6, 0, 0)
        .expect("valid time")
        .and_utc()
        .timestamp();
    if secs >= start && secs < end {
        -4 * 3_600
    } else {
        -5 * 3_600
    }
}

/// Whether `utc_ms` falls inside `h` on New York weekdays. A same-day
/// session runs Monday–Friday from `start_s` to `end_s`. An overnight one
/// opens Sunday–Thursday evenings at `start_s` and closes the next morning
/// (Monday–Friday) at `end_s`. Holidays are not known here — the venue's
/// `isOutsideRth` covers them while it is fresh.
pub fn in_hours(utc_ms: u64, h: TradingHours) -> bool {
    let local = (utc_ms / 1_000) as i64 + new_york_utc_offset_secs(utc_ms);
    let sod = local.rem_euclid(86_400) as u32;
    // 1970-01-01 was a Thursday: day 0 → Thu.
    let weekday = match (local.div_euclid(86_400) + 3).rem_euclid(7) {
        0 => Weekday::Mon,
        1 => Weekday::Tue,
        2 => Weekday::Wed,
        3 => Weekday::Thu,
        4 => Weekday::Fri,
        5 => Weekday::Sat,
        _ => Weekday::Sun,
    };
    let weekday_mf = !matches!(weekday, Weekday::Sat | Weekday::Sun);
    if !h.overnight {
        return weekday_mf && sod >= h.start_s && sod < h.end_s;
    }
    let evening_open = matches!(
        weekday,
        Weekday::Sun | Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu
    );
    (sod >= h.start_s && evening_open) || (sod < h.end_s && weekday_mf)
}

/// Parse the session fields of one `/v1/markets` row.
pub fn parse_market_row(row: &Value) -> MarketSession {
    let outside_rth = row.get("isOutsideRth").and_then(Value::as_bool);
    let mut unsupported_tz = None;
    let hours = row
        .get("regularTradingHours")
        .filter(|v| !v.is_null())
        .and_then(|rth| {
            let tz = rth.get("timezone").and_then(Value::as_str).unwrap_or("");
            if tz != NEW_YORK {
                unsupported_tz = Some(tz.to_string());
                return None;
            }
            let secs = |k: &str| {
                rth.get(k)
                    .and_then(Value::as_u64)
                    .filter(|s| *s <= 86_400)
                    .map(|s| s as u32)
            };
            Some(TradingHours {
                start_s: secs("startSecondsOfDay")?,
                end_s: secs("endSecondsOfDay")?,
                overnight: rth
                    .get("isOvernight")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        });
    MarketSession {
        hours,
        outside_rth,
        unsupported_tz,
    }
}

/// The market's row out of a `/v1/markets` response (`{"markets": [..]}`).
pub fn market_row<'a>(body: &'a Value, market: &str) -> Option<&'a Value> {
    body.get("markets")?
        .as_array()?
        .iter()
        .find(|m| m.get("marketDisplayName").and_then(Value::as_str) == Some(market))
}

/// Session knowledge accumulated from market-row reads.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Session {
    pub hours: Option<TradingHours>,
    /// `(isOutsideRth, read at ms)` of the last successful read.
    pub venue_flag: Option<(bool, u64)>,
}

impl Session {
    pub fn apply(&mut self, read: &MarketSession, now_ms: u64) {
        self.hours = read.hours;
        self.venue_flag = read.outside_rth.map(|o| (o, now_ms));
    }

    /// `Some(true)` in-session, `Some(false)` off-hours, `None` when the
    /// market has no session (crypto, unsupported zone, never read).
    pub fn in_session(&self, now_ms: u64) -> Option<bool> {
        let h = self.hours?;
        let clock = in_hours(now_ms, h);
        if let Some((outside, at)) = self.venue_flag {
            // The venue flag describes the session at its read time. Once
            // the clock has crossed a session boundary since then, the flag
            // is about the previous session (bot-strategy#1093, 2026-10-06
            // 00:00Z: the 23:59 read kept the bot "in session" at the wider
            // offset for 23 s after the session had closed); the clock
            // decides until the next read.
            let crossed = in_hours(at, h) != clock;
            if now_ms.saturating_sub(at) <= VENUE_FLAG_TTL_MS && !crossed {
                return Some(!outside);
            }
        }
        Some(clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SPY: TradingHours = TradingHours {
        start_s: 14_400,
        end_s: 72_000,
        overnight: false,
    };

    fn ms(rfc3339: &str) -> u64 {
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .timestamp_millis() as u64
    }

    #[test]
    fn new_york_offset_follows_the_us_dst_rule() {
        // 2026: DST from Sun 03-08 07:00Z to Sun 11-01 06:00Z.
        assert_eq!(
            new_york_utc_offset_secs(ms("2026-03-08T06:59:59Z")),
            -5 * 3600
        );
        assert_eq!(
            new_york_utc_offset_secs(ms("2026-03-08T07:00:00Z")),
            -4 * 3600
        );
        assert_eq!(
            new_york_utc_offset_secs(ms("2026-10-05T12:00:00Z")),
            -4 * 3600
        );
        assert_eq!(
            new_york_utc_offset_secs(ms("2026-11-01T05:59:59Z")),
            -4 * 3600
        );
        assert_eq!(
            new_york_utc_offset_secs(ms("2026-11-01T06:00:00Z")),
            -5 * 3600
        );
        assert_eq!(
            new_york_utc_offset_secs(ms("2027-01-15T12:00:00Z")),
            -5 * 3600
        );
        // 2027: second Sunday of March is 03-14, first Sunday of Nov 11-07.
        assert_eq!(
            new_york_utc_offset_secs(ms("2027-03-14T07:00:00Z")),
            -4 * 3600
        );
        assert_eq!(
            new_york_utc_offset_secs(ms("2027-11-07T06:00:00Z")),
            -5 * 3600
        );
    }

    #[test]
    fn the_spy_session_is_04_to_20_new_york_on_weekdays_in_edt_and_est() {
        // EDT (Mon 2026-10-05): 04:00 ET = 08:00Z, 20:00 ET = 24:00Z.
        assert!(!in_hours(ms("2026-10-05T07:59:59Z"), SPY));
        assert!(in_hours(ms("2026-10-05T08:00:00Z"), SPY));
        assert!(in_hours(ms("2026-10-05T23:59:59Z"), SPY));
        assert!(!in_hours(ms("2026-10-06T00:00:00Z"), SPY));
        // EST (Mon 2026-11-02): 04:00 ET = 09:00Z, 20:00 ET = 01:00Z next day.
        assert!(!in_hours(ms("2026-11-02T08:59:59Z"), SPY));
        assert!(in_hours(ms("2026-11-02T09:00:00Z"), SPY));
        assert!(in_hours(ms("2026-11-03T00:59:59Z"), SPY));
        assert!(!in_hours(ms("2026-11-03T01:00:00Z"), SPY));
    }

    #[test]
    fn weekends_are_off_session() {
        // Sat 2026-10-03 and Sun 2026-10-04 at New York noon.
        assert!(!in_hours(ms("2026-10-03T16:00:00Z"), SPY));
        assert!(!in_hours(ms("2026-10-04T16:00:00Z"), SPY));
        // Friday 19:59 ET is in, Friday 20:00 ET out.
        assert!(in_hours(ms("2026-10-02T23:59:00Z"), SPY));
        assert!(!in_hours(ms("2026-10-03T00:00:00Z"), SPY));
    }

    #[test]
    fn an_overnight_session_opens_sunday_to_thursday_evenings() {
        let night = TradingHours {
            start_s: 18 * 3600,
            end_s: 17 * 3600,
            overnight: true,
        };
        // Sun 18:00 ET (EDT) = 22:00Z opens; Sat is closed all day.
        assert!(in_hours(ms("2026-10-04T22:00:00Z"), night));
        assert!(!in_hours(ms("2026-10-03T22:00:00Z"), night));
        // Fri 17:00 ET closes and Fri evening does not reopen.
        assert!(in_hours(ms("2026-10-09T20:59:00Z"), night));
        assert!(!in_hours(ms("2026-10-09T22:30:00Z"), night));
    }

    #[test]
    fn a_market_row_parses_hours_flag_and_zone() {
        let spy = json!({"marketDisplayName": "SPY-USD", "isOutsideRth": true,
            "regularTradingHours": {"startSecondsOfDay": 14400, "endSecondsOfDay": 72000,
                                    "timezone": "America/New_York", "isOvernight": false}});
        let s = parse_market_row(&spy);
        assert_eq!(s.hours, Some(SPY));
        assert_eq!(s.outside_rth, Some(true));
        assert_eq!(s.unsupported_tz, None);
        let btc = json!({"marketDisplayName": "BTC-USD", "isOutsideRth": false,
                         "regularTradingHours": null});
        assert_eq!(parse_market_row(&btc).hours, None);
        let tokyo = json!({"regularTradingHours": {"startSecondsOfDay": 0,
            "endSecondsOfDay": 3600, "timezone": "Asia/Tokyo", "isOvernight": false}});
        let s = parse_market_row(&tokyo);
        assert_eq!(s.hours, None);
        assert_eq!(s.unsupported_tz.as_deref(), Some("Asia/Tokyo"));
        let body = json!({"markets": [btc, spy]});
        assert_eq!(
            market_row(&body, "SPY-USD").and_then(|r| r.get("isOutsideRth")),
            Some(&json!(true))
        );
        assert!(market_row(&body, "QQQ-USD").is_none());
    }

    #[test]
    fn a_fresh_venue_flag_decides_and_a_stale_one_falls_back_to_the_clock() {
        let mut s = Session::default();
        let mon_noon = ms("2026-10-05T16:00:00Z"); // in hours by the clock
        assert_eq!(s.in_session(mon_noon), None); // never read
                                                  // A holiday: the venue says outside although the clock says in.
        s.apply(
            &MarketSession {
                hours: Some(SPY),
                outside_rth: Some(true),
                unsupported_tz: None,
            },
            mon_noon,
        );
        assert_eq!(s.in_session(mon_noon + VENUE_FLAG_TTL_MS), Some(false));
        assert_eq!(s.in_session(mon_noon + VENUE_FLAG_TTL_MS + 1), Some(true));
        // Crypto: no hours → no session, whatever the flag says.
        s.apply(
            &MarketSession {
                hours: None,
                outside_rth: Some(false),
                unsupported_tz: None,
            },
            mon_noon,
        );
        assert_eq!(s.in_session(mon_noon), None);
    }

    #[test]
    fn a_venue_flag_read_before_a_session_boundary_yields_to_the_clock() {
        // The 2026-10-06 resume: last read 23:59:37Z (in session, 19:59 ET),
        // the session closed at 00:00Z (20:00 ET).
        let mut s = Session::default();
        s.apply(
            &MarketSession {
                hours: Some(SPY),
                outside_rth: Some(false),
                unsupported_tz: None,
            },
            ms("2026-10-05T23:59:37Z"),
        );
        assert_eq!(s.in_session(ms("2026-10-05T23:59:59Z")), Some(true));
        // Within the flag's TTL, but past the close: the clock decides.
        assert_eq!(s.in_session(ms("2026-10-06T00:00:00Z")), Some(false));
        assert_eq!(s.in_session(ms("2026-10-06T00:00:20Z")), Some(false));
        // The open the same way (08:00Z = 04:00 ET in EDT).
        s.apply(
            &MarketSession {
                hours: Some(SPY),
                outside_rth: Some(true),
                unsupported_tz: None,
            },
            ms("2026-10-06T07:59:30Z"),
        );
        assert_eq!(s.in_session(ms("2026-10-06T07:59:59Z")), Some(false));
        assert_eq!(s.in_session(ms("2026-10-06T08:00:01Z")), Some(true));
        // No boundary since the read: a holiday flag still wins (unchanged).
        s.apply(
            &MarketSession {
                hours: Some(SPY),
                outside_rth: Some(true),
                unsupported_tz: None,
            },
            ms("2026-10-06T12:00:00Z"),
        );
        assert_eq!(s.in_session(ms("2026-10-06T12:01:00Z")), Some(false));
    }
}
