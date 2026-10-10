//! Adaptive quote-distance learner, SHADOW MODE ONLY (bot-strategy#1093).
//!
//! Fixed offset rules overfit in the offline sim (`adapt_report.md`), so the
//! runtime learns which quote distance pays best per market and per time
//! window from its own round trips. In this version the learner only
//! *records* what it would choose: nothing here can change the live offset.
//! The runtime keeps quoting with `ARCUS_VOL_QUOTE_OFFSET_BPS` /
//! `ARCUS_VOL_SESSION_OFFSET_BPS`; the learner's recommendation goes to the
//! journal (`kind: "learner"`) and `status.json` only.
//!
//! TODO(bot-strategy#1093): a `live` mode that applies the recommendation
//! (inside the existing stops) after the shadow readout. Out of scope here.
//!
//! Model
//! - Context = time window of the market's day, in New York time (the
//!   runtime is one market): `off` (20:00–04:00 ET on weekdays), `pre`
//!   (04:00–09:30), `cash` (09:30–16:00), `after` (16:00–20:00) and
//!   `weekend` (Friday 20:00 ET → Sunday 20:00 ET).
//! - Arms = candidate offsets (bp). A round trip (first fill out of flat →
//!   back to flat / dust) is attributed to the arm live at its entry fill
//!   and the window of that fill.
//! - Reward rate per arm = (net $ + point value × volume) per quoting hour.
//!   Points are ~proportional to volume (≈60 pt per $1M on 2026-10-07), so
//!   `point_value_per_m` turns volume into $ at an assumed $/point; a wider
//!   arm that trades rarely and a tight one that trades often but bleeds are
//!   compared on the same $/hour scale. Statistics decay exponentially in
//!   wall time (half-life `half_life_h`).
//! - Policy (deterministic UCB): arms below an exploration floor of quoting
//!   hours are recommended first; otherwise rate + `ucb_c`/√hours. Guards:
//!   when the window's recent entry markouts (60 s) average below
//!   `guard_bps`, only offsets ≥ the live one are eligible (widen-only, the
//!   QQQ lesson); an arm live when the daily stop tripped is excluded for
//!   `breach_hours`.
//! - In shadow mode only the live arm accrues data, so other arms stay
//!   "explore" until a live mode plays them. The point of the shadow phase is
//!   to check attribution, the guards and the stats against the journal.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::session::new_york_utc_offset_secs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LearnerMode {
    Off,
    Shadow,
}

impl LearnerMode {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim() {
            "off" | "" => Ok(LearnerMode::Off),
            "shadow" => Ok(LearnerMode::Shadow),
            other => Err(format!("{other} is not off|shadow")),
        }
    }
}

/// `ARCUS_VOL_LEARNER*` settings.
#[derive(Debug, Clone, PartialEq)]
pub struct LearnerSettings {
    pub mode: LearnerMode,
    /// Candidate offsets in bp, ascending.
    pub arms: Vec<f64>,
    pub half_life_h: f64,
    /// Assumed $ value of the points earned per $1M of volume.
    pub point_value_per_m: f64,
    /// An arm with fewer quoting hours than max(`min_hours`,
    /// `explore_floor` × hours / arms) in its window is explored first.
    pub min_hours: f64,
    pub explore_floor: f64,
    pub ucb_c: f64,
    pub guard_bps: f64,
    pub guard_n: usize,
    pub breach_hours: f64,
    /// Offsets that were live in the past, for attributing journal history:
    /// (from_ms, off-session offset, session offset), ascending.
    pub history: Vec<(u64, f64, f64)>,
}

impl LearnerSettings {
    pub fn off() -> Self {
        LearnerSettings {
            mode: LearnerMode::Off,
            arms: vec![2.0, 3.0, 5.0, 8.0, 12.0],
            half_life_h: 7.0 * 24.0,
            point_value_per_m: 15.0,
            min_hours: 2.0,
            explore_floor: 0.1,
            ucb_c: 1.0,
            guard_bps: -1.0,
            guard_n: 20,
            breach_hours: 24.0,
            history: Vec::new(),
        }
    }

    /// `"2026-10-03T20:36:00Z=2/2,2026-10-05T10:19:00Z=2/5"`: from each
    /// instant on, the off-session / session offsets that were live.
    pub fn parse_history(raw: &str) -> Result<Vec<(u64, f64, f64)>, String> {
        let mut out = Vec::new();
        for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (ts, offs) = part
                .split_once('=')
                .ok_or_else(|| format!("{part}: expected <RFC3339>=<off>/<session>"))?;
            let t = chrono::DateTime::parse_from_rfc3339(ts.trim())
                .map_err(|e| format!("{ts}: {e}"))?
                .timestamp_millis() as u64;
            let (o, s) = offs
                .split_once('/')
                .ok_or_else(|| format!("{offs}: expected <off>/<session>"))?;
            let o: f64 = o.trim().parse().map_err(|_| format!("{o}: not a number"))?;
            let s: f64 = s.trim().parse().map_err(|_| format!("{s}: not a number"))?;
            out.push((t, o, s));
        }
        out.sort_by_key(|x| x.0);
        Ok(out)
    }

    /// The offset live at `ts_ms` per `history`, `None` before its first entry.
    pub fn history_arm(&self, ts_ms: u64) -> Option<f64> {
        let w = window_at(ts_ms);
        self.history
            .iter()
            .rev()
            .find(|(from, _, _)| *from <= ts_ms)
            .map(|(_, off, sess)| if w.in_venue_session() { *sess } else { *off })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Window {
    Off,
    Pre,
    Cash,
    After,
    Weekend,
}

impl Window {
    /// Inside the venue's 04:00–20:00 ET session (where the session offset
    /// applies), used only to attribute journal history.
    pub fn in_venue_session(self) -> bool {
        matches!(self, Window::Pre | Window::Cash | Window::After)
    }
}

/// The window of `utc_ms` in New York time (US DST via `session`).
pub fn window_at(utc_ms: u64) -> Window {
    let local = (utc_ms / 1_000) as i64 + new_york_utc_offset_secs(utc_ms);
    let sod = local.rem_euclid(86_400);
    // 1970-01-01 was a Thursday: 0 = Thu … 4 = Mon.
    let dow = (local.div_euclid(86_400) + 3).rem_euclid(7); // 0 Mon … 6 Sun
    const H4: i64 = 4 * 3_600;
    const H930: i64 = 9 * 3_600 + 1_800;
    const H16: i64 = 16 * 3_600;
    const H20: i64 = 20 * 3_600;
    let weekend = dow == 5 || (dow == 6 && sod < H20) || (dow == 4 && sod >= H20);
    if weekend {
        return Window::Weekend;
    }
    if !(H4..H20).contains(&sod) {
        Window::Off
    } else if sod < H930 {
        Window::Pre
    } else if sod < H16 {
        Window::Cash
    } else {
        Window::After
    }
}

fn arm_key(offset: f64) -> String {
    let r = (offset * 100.0).round() / 100.0;
    if r.fract() == 0.0 {
        format!("{}", r as i64)
    } else {
        format!("{r}")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ArmStats {
    pub trips: f64,
    pub vol: f64,
    pub net: f64,
    pub taker: f64,
    pub hours: f64,
    pub last_ms: u64,
}

impl ArmStats {
    fn decay_to(&mut self, now: u64, half_life_h: f64) {
        if self.last_ms != 0 && now > self.last_ms && half_life_h > 0.0 {
            let dt_h = (now - self.last_ms) as f64 / 3_600_000.0;
            let k = 0.5f64.powf(dt_h / half_life_h);
            self.trips *= k;
            self.vol *= k;
            self.net *= k;
            self.taker *= k;
            self.hours *= k;
        }
        self.last_ms = self.last_ms.max(now);
    }

    fn decayed(&self, now: u64, half_life_h: f64) -> ArmStats {
        let mut s = self.clone();
        s.decay_to(now, half_life_h);
        s
    }

    /// (net + point value × volume) per quoting hour.
    pub fn rate(&self, point_value_per_m: f64) -> Option<f64> {
        (self.hours > 0.0).then(|| (self.net + point_value_per_m * self.vol / 1e6) / self.hours)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContextState {
    pub arms: BTreeMap<String, ArmStats>,
    /// Trips whose live offset is not known (history before `history`).
    pub unknown_trips: u64,
    pub trips: u64,
    /// Entry markouts (60 s, bp, + = favourable), newest last.
    pub markouts: VecDeque<f64>,
    /// Arm key → excluded until (ms).
    pub breaches: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trip {
    pub start_ms: u64,
    pub window: Window,
    pub arm: Option<f64>,
    pub vol: f64,
    pub net: f64,
    pub taker: u32,
}

/// Persisted learner state (`learner.json` in the state dir).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LearnerState {
    pub contexts: BTreeMap<Window, ContextState>,
    pub open: Option<Trip>,
    /// Entry fill id → window, for attributing its markout.
    pub entry_fills: VecDeque<(String, Window)>,
    /// Highest journal `seq` ingested (rows at or below are skipped).
    pub seen_seq: u64,
    pub last_tick_ms: Option<u64>,
    pub last_emit_ms: u64,
    pub last_window: Option<Window>,
    pub breach_day: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ArmView {
    pub offset_bps: f64,
    pub hours: f64,
    pub trips: f64,
    pub vol: f64,
    pub net: f64,
    pub rate: Option<f64>,
    pub eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Recommendation {
    pub window: Window,
    pub live_offset_bps: Option<f64>,
    pub recommended_offset_bps: Option<f64>,
    pub reason: &'static str,
    pub trips_in_context: u64,
    pub unknown_trips: u64,
    pub markout_mean_bps: Option<f64>,
    pub arms: Vec<ArmView>,
}

pub struct Learner {
    pub settings: LearnerSettings,
    pub state: LearnerState,
}

fn num(v: &Value, k: &str) -> Option<f64> {
    match v.get(k)? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

const MAX_ENTRY_FILLS: usize = 500;
const EMIT_EVERY_MS: u64 = 3_600_000;
/// A tick gap longer than this is not counted as quoting time.
const MAX_TICK_GAP_MS: u64 = 60_000;

impl Learner {
    pub fn new(settings: LearnerSettings, state: LearnerState) -> Self {
        Learner { settings, state }
    }

    pub fn on(&self) -> bool {
        self.settings.mode != LearnerMode::Off
    }

    /// One journal row (`fill` or `markout`), live or replayed. `arm` is the
    /// offset live at the fill (the runtime's current one when live; the
    /// `history` one when replayed). Rows at or below `seen_seq` are skipped,
    /// so bootstrapping and live ingest can overlap.
    pub fn ingest(&mut self, row: &Value, arm: Option<f64>, dust_usd: f64) {
        let seq = row.get("seq").and_then(Value::as_u64).unwrap_or(0);
        if seq != 0 && seq <= self.state.seen_seq {
            return;
        }
        if seq != 0 {
            self.state.seen_seq = seq;
        }
        match row.get("kind").and_then(Value::as_str) {
            Some("fill") => self.ingest_fill(row, arm, dust_usd),
            Some("markout") => self.ingest_markout(row),
            _ => {}
        }
    }

    fn ingest_fill(&mut self, row: &Value, arm: Option<f64>, dust_usd: f64) {
        let (Some(ts), Some(inv), Some(px), Some(notional)) = (
            row.get("ts_ms").and_then(Value::as_u64),
            num(row, "inventory"),
            num(row, "px"),
            num(row, "notional"),
        ) else {
            return;
        };
        let net = num(row, "realized").unwrap_or(0.0) - num(row, "fee").unwrap_or(0.0);
        let taker = row.get("role").and_then(Value::as_str) != Some("maker");
        if self.state.open.is_none() {
            let window = window_at(ts);
            self.state.open = Some(Trip {
                start_ms: ts,
                window,
                arm,
                vol: 0.0,
                net: 0.0,
                taker: 0,
            });
            if let Some(id) = row.get("fill_id").and_then(Value::as_str) {
                self.state.entry_fills.push_back((id.to_string(), window));
                while self.state.entry_fills.len() > MAX_ENTRY_FILLS {
                    self.state.entry_fills.pop_front();
                }
            }
        }
        let trip = self.state.open.as_mut().expect("open trip");
        trip.vol += notional;
        trip.net += net;
        trip.taker += u32::from(taker);
        if (inv * px).abs() < dust_usd {
            let trip = self.state.open.take().expect("open trip");
            self.close_trip(trip, ts);
        }
    }

    fn close_trip(&mut self, trip: Trip, now: u64) {
        let hl = self.settings.half_life_h;
        let ctx = self.state.contexts.entry(trip.window).or_default();
        ctx.trips += 1;
        match trip.arm {
            Some(a) => {
                let s = ctx.arms.entry(arm_key(a)).or_default();
                s.decay_to(now, hl);
                s.trips += 1.0;
                s.vol += trip.vol;
                s.net += trip.net;
                s.taker += f64::from(trip.taker);
            }
            None => ctx.unknown_trips += 1,
        }
    }

    fn ingest_markout(&mut self, row: &Value) {
        if row.get("horizon_s").and_then(Value::as_u64) != Some(60) {
            return;
        }
        let (Some(id), Some(bps)) = (row.get("fill_id").and_then(Value::as_str), num(row, "bps"))
        else {
            return;
        };
        let Some(window) = self
            .state
            .entry_fills
            .iter()
            .find(|(f, _)| f == id)
            .map(|(_, w)| *w)
        else {
            return;
        };
        let n = self.settings.guard_n;
        let ctx = self.state.contexts.entry(window).or_default();
        ctx.markouts.push_back(bps);
        while ctx.markouts.len() > n {
            ctx.markouts.pop_front();
        }
    }

    /// Quoting time for the arm live now (called every tick).
    pub fn on_tick(&mut self, now: u64, live: Option<f64>, quoting: bool) {
        self.on_tick_gap(now, live, quoting, MAX_TICK_GAP_MS);
    }

    fn on_tick_gap(&mut self, now: u64, live: Option<f64>, quoting: bool, max_gap_ms: u64) {
        if let (Some(prev), Some(a), true) = (self.state.last_tick_ms, live, quoting) {
            let dt = now.saturating_sub(prev);
            if dt > 0 && dt <= max_gap_ms {
                let hl = self.settings.half_life_h;
                let s = self
                    .state
                    .contexts
                    .entry(window_at(now))
                    .or_default()
                    .arms
                    .entry(arm_key(a))
                    .or_default();
                s.decay_to(now, hl);
                s.hours += dt as f64 / 3_600_000.0;
            }
        }
        self.state.last_tick_ms = Some(now);
    }

    /// The daily stop tripped while `live` was quoting: exclude that arm in
    /// the current window for `breach_hours`. Once per UTC `day`.
    pub fn note_breach(&mut self, now: u64, live: Option<f64>, day: &str) {
        if self.state.breach_day.as_deref() == Some(day) {
            return;
        }
        self.state.breach_day = Some(day.to_string());
        let Some(a) = live else { return };
        let until = now + (self.settings.breach_hours * 3_600_000.0) as u64;
        self.state
            .contexts
            .entry(window_at(now))
            .or_default()
            .breaches
            .insert(arm_key(a), until);
    }

    pub fn recommend(&self, now: u64, live: Option<f64>) -> Recommendation {
        self.recommend_in(window_at(now), now, live)
    }

    pub fn recommend_in(&self, window: Window, now: u64, live: Option<f64>) -> Recommendation {
        let s = &self.settings;
        let empty = ContextState::default();
        let ctx = self.state.contexts.get(&window).unwrap_or(&empty);
        let markout_mean = (ctx.markouts.len() >= 5)
            .then(|| ctx.markouts.iter().sum::<f64>() / ctx.markouts.len() as f64);
        let guard = markout_mean.is_some_and(|m| m < s.guard_bps);
        let mut arms: Vec<ArmView> = s
            .arms
            .iter()
            .map(|&a| {
                let st = ctx
                    .arms
                    .get(&arm_key(a))
                    .map(|x| x.decayed(now, s.half_life_h))
                    .unwrap_or_default();
                let breached = ctx.breaches.get(&arm_key(a)).is_some_and(|&u| u > now);
                let widen_ok = !guard || live.is_none_or(|l| a >= l);
                ArmView {
                    offset_bps: a,
                    hours: st.hours,
                    trips: st.trips,
                    vol: st.vol,
                    net: st.net,
                    rate: st.rate(s.point_value_per_m),
                    eligible: !breached && widen_ok,
                }
            })
            .collect();
        arms.sort_by(|a, b| a.offset_bps.total_cmp(&b.offset_bps));
        let eligible: Vec<&ArmView> = arms.iter().filter(|a| a.eligible).collect();
        let (pick, reason) = if eligible.is_empty() {
            (live, "no_eligible_arm")
        } else {
            let total: f64 = eligible.iter().map(|a| a.hours).sum();
            let floor = s
                .min_hours
                .max(s.explore_floor * total / eligible.len() as f64);
            let under: Vec<&&ArmView> = eligible.iter().filter(|a| a.hours < floor).collect();
            if let Some(a) = under.iter().min_by(|x, y| {
                let dx = live.map_or(0.0, |l| (x.offset_bps - l).abs());
                let dy = live.map_or(0.0, |l| (y.offset_bps - l).abs());
                x.hours.total_cmp(&y.hours).then(dx.total_cmp(&dy))
            }) {
                (
                    Some(a.offset_bps),
                    if guard {
                        "explore_widen_only"
                    } else {
                        "explore"
                    },
                )
            } else {
                let best = eligible
                    .iter()
                    .max_by(|x, y| {
                        let ux = x.rate.unwrap_or(f64::MIN) + s.ucb_c / x.hours.max(1e-9).sqrt();
                        let uy = y.rate.unwrap_or(f64::MIN) + s.ucb_c / y.hours.max(1e-9).sqrt();
                        ux.total_cmp(&uy)
                    })
                    .expect("non-empty");
                (
                    Some(best.offset_bps),
                    if guard { "best_widen_only" } else { "best" },
                )
            }
        };
        Recommendation {
            window,
            live_offset_bps: live,
            recommended_offset_bps: pick,
            reason,
            trips_in_context: ctx.trips,
            unknown_trips: ctx.unknown_trips,
            markout_mean_bps: markout_mean,
            arms,
        }
    }

    /// A `learner` journal row when an hour has passed or the window changed.
    pub fn due_row(&mut self, now: u64, live: Option<f64>, market: &str) -> Option<Value> {
        let w = window_at(now);
        let due = now.saturating_sub(self.state.last_emit_ms) >= EMIT_EVERY_MS
            || self.state.last_window != Some(w);
        if !due {
            return None;
        }
        self.state.last_emit_ms = now;
        self.state.last_window = Some(w);
        let r = self.recommend(now, live);
        Some(json!({
            "kind": "learner",
            "ts_ms": now,
            "market": market,
            "mode": "shadow",
            "window": r.window,
            "live_offset_bps": r.live_offset_bps,
            "recommended_offset_bps": r.recommended_offset_bps,
            "reason": r.reason,
            "trips_in_context": r.trips_in_context,
            "unknown_trips": r.unknown_trips,
            "markout_mean_bps": r.markout_mean_bps,
            "arms": r.arms,
        }))
    }

    /// The `learner` block of status.json.
    pub fn status(&self, now: u64, live: Option<f64>) -> Value {
        let r = self.recommend(now, live);
        json!({
            "mode": "shadow",
            "context": r.window,
            "live_offset": r.live_offset_bps,
            "recommended_offset": r.recommended_offset_bps,
            "reason": r.reason,
            "trips_in_context": r.trips_in_context,
        })
    }

    pub fn load(path: &std::path::Path) -> Option<LearnerState> {
        let raw = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&raw).ok()
    }

    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let body = serde_json::to_string(&self.state).map_err(std::io::Error::other)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, path)
    }

    /// Offline replay of a fills journal (`arcus_vol_runtime learner-replay`):
    /// like `replay`, plus quoting time approximated from the row timestamps
    /// (gaps ≤ 15 min between consecutive rows count as quoting with the
    /// arm `history` says was live). Returns one recommendation per window.
    pub fn replay_offline(&mut self, rows: &[Value], dust_usd: f64) -> Vec<Recommendation> {
        let mut last_ts = 0;
        for row in rows {
            let Some(ts) = row.get("ts_ms").and_then(Value::as_u64) else {
                continue;
            };
            let arm = self.settings.history_arm(ts);
            self.on_tick_gap(ts, arm, true, 15 * 60_000);
            self.ingest(row, arm, dust_usd);
            last_ts = last_ts.max(ts);
        }
        [
            Window::Off,
            Window::Pre,
            Window::Cash,
            Window::After,
            Window::Weekend,
        ]
        .into_iter()
        .map(|w| {
            // The arm the history says is live in that window now.
            let live =
                self.settings.history.last().map(
                    |(_, off, sess)| {
                        if w.in_venue_session() {
                            *sess
                        } else {
                            *off
                        }
                    },
                );
            self.recommend_in(w, last_ts, live)
        })
        .collect()
    }

    /// Replay journal rows (bootstrap / offline): arms from `history`.
    pub fn replay(&mut self, rows: &[Value], dust_usd: f64) {
        for row in rows {
            let arm = row
                .get("ts_ms")
                .and_then(Value::as_u64)
                .and_then(|t| self.settings.history_arm(t));
            self.ingest(row, arm, dust_usd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(rfc: &str) -> u64 {
        chrono::DateTime::parse_from_rfc3339(rfc)
            .unwrap()
            .timestamp_millis() as u64
    }

    fn shadow() -> LearnerSettings {
        LearnerSettings {
            mode: LearnerMode::Shadow,
            ..LearnerSettings::off()
        }
    }

    fn fill(seq: u64, ts: u64, inv: &str, notional: &str, realized: &str, role: &str) -> Value {
        json!({"kind": "fill", "seq": seq, "ts_ms": ts, "fill_id": format!("f{seq}"),
               "px": "100", "inventory": inv, "notional": notional,
               "realized": realized, "fee": if role == "maker" { "0" } else { "0.5" },
               "role": role})
    }

    #[test]
    fn windows_follow_new_york_time_and_dst() {
        // Monday 2026-10-05 (EDT, UTC−4).
        assert_eq!(window_at(ms("2026-10-05T03:00:00Z")), Window::Off); // Sun 23:00 ET → weekday overnight
        assert_eq!(window_at(ms("2026-10-05T07:59:00Z")), Window::Off); // 03:59 ET
        assert_eq!(window_at(ms("2026-10-05T08:00:00Z")), Window::Pre); // 04:00 ET
        assert_eq!(window_at(ms("2026-10-05T13:29:00Z")), Window::Pre);
        assert_eq!(window_at(ms("2026-10-05T13:30:00Z")), Window::Cash); // 09:30 ET
        assert_eq!(window_at(ms("2026-10-05T20:00:00Z")), Window::After); // 16:00 ET
        assert_eq!(window_at(ms("2026-10-06T00:00:00Z")), Window::Off); // 20:00 ET
                                                                        // Weekend: Fri 20:00 ET → Sun 20:00 ET.
        assert_eq!(window_at(ms("2026-10-10T00:00:00Z")), Window::Weekend); // Fri 20:00 ET
        assert_eq!(window_at(ms("2026-10-11T23:59:00Z")), Window::Weekend); // Sun 19:59 ET
        assert_eq!(window_at(ms("2026-10-12T00:00:00Z")), Window::Off); // Sun 20:00 ET
                                                                        // After the DST change (EST, UTC−5): cash opens 14:30Z.
        assert_eq!(window_at(ms("2026-11-02T14:29:00Z")), Window::Pre);
        assert_eq!(window_at(ms("2026-11-02T14:30:00Z")), Window::Cash);
    }

    #[test]
    fn a_round_trip_is_attributed_to_the_entry_arm_and_window() {
        let mut l = Learner::new(shadow(), LearnerState::default());
        let t0 = ms("2026-10-05T13:29:45Z"); // pre, 15 s before the cash open
        l.ingest(&fill(1, t0, "25", "2500", "0", "maker"), Some(5.0), 5.0);
        // Closed in the cash window with another arm live: the entry's
        // window and arm count.
        l.ingest(
            &fill(2, t0 + 30_000, "0", "2500", "1.5", "maker"),
            Some(3.0),
            5.0,
        );
        assert!(!l.state.contexts.contains_key(&Window::Cash));
        let ctx = &l.state.contexts[&Window::Pre];
        assert_eq!(ctx.trips, 1);
        let s = &ctx.arms["5"];
        assert_eq!(s.trips, 1.0);
        assert!((s.vol - 5000.0).abs() < 1e-9);
        assert!((s.net - 1.5).abs() < 1e-9);
        assert!(!ctx.arms.contains_key("3"));
        assert!(l.state.open.is_none());
    }

    #[test]
    fn dust_closes_a_trip_and_taker_exits_are_counted() {
        let mut l = Learner::new(shadow(), LearnerState::default());
        let t0 = ms("2026-10-05T03:00:00Z");
        l.ingest(&fill(1, t0, "25", "2500", "0", "maker"), Some(3.0), 5.0);
        // 0.04 × 100 = $4 < $5 dust: trip closed.
        l.ingest(
            &fill(2, t0 + 1_000, "0.04", "2496", "-2", "taker"),
            Some(3.0),
            5.0,
        );
        let s = &l.state.contexts[&Window::Off].arms["3"];
        assert_eq!(s.taker, 1.0);
        assert!((s.net - (-2.5)).abs() < 1e-9);
    }

    #[test]
    fn rows_already_seen_are_skipped() {
        let mut l = Learner::new(shadow(), LearnerState::default());
        let t0 = ms("2026-10-05T03:00:00Z");
        let a = fill(1, t0, "25", "2500", "0", "maker");
        let b = fill(2, t0 + 1_000, "0", "2500", "1", "maker");
        for r in [&a, &b, &a, &b] {
            l.ingest(r, Some(3.0), 5.0);
        }
        assert_eq!(l.state.contexts[&Window::Off].trips, 1);
    }

    #[test]
    fn statistics_decay_with_the_half_life() {
        let mut s = ArmStats {
            trips: 4.0,
            vol: 1000.0,
            net: 2.0,
            taker: 2.0,
            hours: 8.0,
            last_ms: 1,
        };
        s.decay_to(1 + 168 * 3_600_000, 168.0);
        assert!((s.trips - 2.0).abs() < 1e-9 && (s.hours - 4.0).abs() < 1e-9);
    }

    fn with_arm(l: &mut Learner, w: Window, a: f64, hours: f64, net: f64, vol: f64, now: u64) {
        l.state.contexts.entry(w).or_default().arms.insert(
            arm_key(a),
            ArmStats {
                trips: 10.0,
                vol,
                net,
                taker: 0.0,
                hours,
                last_ms: now,
            },
        );
    }

    #[test]
    fn the_policy_picks_the_better_arm_once_all_are_explored() {
        let now = ms("2026-10-05T03:00:00Z");
        let mut l = Learner::new(shadow(), LearnerState::default());
        for a in [2.0, 3.0, 5.0, 8.0, 12.0] {
            with_arm(&mut l, Window::Off, a, 20.0, -2.0, 50_000.0, now);
        }
        with_arm(&mut l, Window::Off, 5.0, 20.0, 10.0, 200_000.0, now);
        let r = l.recommend(now, Some(3.0));
        assert_eq!((r.recommended_offset_bps, r.reason), (Some(5.0), "best"));
    }

    #[test]
    fn an_unexplored_arm_is_recommended_first_nearest_the_live_one() {
        let now = ms("2026-10-05T03:00:00Z");
        let mut l = Learner::new(shadow(), LearnerState::default());
        with_arm(&mut l, Window::Off, 3.0, 50.0, 5.0, 100_000.0, now);
        let r = l.recommend(now, Some(3.0));
        assert_eq!(r.reason, "explore");
        assert_eq!(r.recommended_offset_bps, Some(2.0)); // 0 h, |2−3| = 1 beats 5
                                                         // Floor scales with total hours: 12 bp with 2.5 h < 10% × 100 h / 5 arms.
        for a in [2.0, 3.0, 5.0, 8.0] {
            with_arm(&mut l, Window::Off, a, 25.0, 1.0, 10_000.0, now);
        }
        with_arm(&mut l, Window::Off, 12.0, 1.9, 9.0, 10_000.0, now);
        assert_eq!(
            l.recommend(now, Some(3.0)).recommended_offset_bps,
            Some(12.0)
        );
    }

    #[test]
    fn a_bad_markout_window_only_widens() {
        let now = ms("2026-10-05T03:00:00Z");
        let mut l = Learner::new(shadow(), LearnerState::default());
        for a in [2.0, 3.0, 5.0, 8.0, 12.0] {
            with_arm(&mut l, Window::Off, a, 20.0, 0.0, 10_000.0, now);
        }
        with_arm(&mut l, Window::Off, 2.0, 20.0, 50.0, 500_000.0, now); // best by far
        assert_eq!(
            l.recommend(now, Some(5.0)).recommended_offset_bps,
            Some(2.0)
        );
        let ctx = l.state.contexts.get_mut(&Window::Off).unwrap();
        ctx.markouts = VecDeque::from(vec![-1.5; 20]);
        let r = l.recommend(now, Some(5.0));
        assert!(r.recommended_offset_bps.unwrap() >= 5.0, "{r:?}");
        assert_eq!(r.reason, "best_widen_only");
        assert!(r
            .arms
            .iter()
            .filter(|a| a.offset_bps < 5.0)
            .all(|a| !a.eligible));
    }

    #[test]
    fn markouts_of_entry_fills_feed_the_guard() {
        let mut l = Learner::new(shadow(), LearnerState::default());
        let t0 = ms("2026-10-05T03:00:00Z");
        l.ingest(&fill(1, t0, "25", "2500", "0", "maker"), Some(3.0), 5.0);
        l.ingest(
            &fill(2, t0 + 1_000, "0", "2500", "0", "maker"),
            Some(3.0),
            5.0,
        );
        for (seq, id, h) in [(3, "f1", 60), (4, "f2", 60), (5, "f1", 10)] {
            l.ingest(
                &json!({"kind": "markout", "seq": seq, "fill_id": id, "horizon_s": h, "bps": "-2.0"}),
                None,
                5.0,
            );
        }
        // Only the entry fill's 60 s markout counts.
        assert_eq!(
            l.state.contexts[&Window::Off].markouts,
            VecDeque::from(vec![-2.0])
        );
    }

    #[test]
    fn an_arm_that_breached_the_daily_stop_is_excluded_for_a_while() {
        let now = ms("2026-10-05T03:00:00Z");
        let mut l = Learner::new(shadow(), LearnerState::default());
        for a in [2.0, 3.0, 5.0, 8.0, 12.0] {
            with_arm(&mut l, Window::Off, a, 20.0, 0.0, 10_000.0, now);
        }
        with_arm(&mut l, Window::Off, 2.0, 20.0, 50.0, 500_000.0, now);
        l.note_breach(now, Some(2.0), "2026-10-05");
        let r = l.recommend(now + 1_000, Some(2.0));
        assert_ne!(r.recommended_offset_bps, Some(2.0));
        assert_eq!(
            l.recommend(now + 25 * 3_600_000, Some(2.0))
                .recommended_offset_bps,
            Some(2.0)
        );
    }

    #[test]
    fn quoting_time_accrues_to_the_live_arm_and_ignores_gaps() {
        let mut l = Learner::new(shadow(), LearnerState::default());
        let t0 = ms("2026-10-05T03:00:00Z");
        l.on_tick(t0, Some(3.0), true);
        l.on_tick(t0 + 1_800_000, Some(3.0), true); // 30 min gap: not counted
        for i in 1..=7_200u64 {
            l.on_tick(t0 + 1_800_000 + i * 500, Some(3.0), true);
        }
        l.on_tick(t0 + 1_800_000 + 7_200 * 500 + 500, Some(3.0), false);
        let h = l.state.contexts[&Window::Off].arms["3"].hours;
        // One hour of 500 ms ticks, decayed within the hour (half-life 7 d).
        assert!((h - 1.0).abs() < 5e-3, "{h}");
    }

    #[test]
    fn state_round_trips_through_the_file() {
        let dir = std::env::temp_dir().join(format!("learner-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("learner.json");
        let mut l = Learner::new(shadow(), LearnerState::default());
        let t0 = ms("2026-10-05T13:35:00Z");
        l.ingest(&fill(1, t0, "25", "2500", "0", "maker"), Some(5.0), 5.0);
        l.note_breach(t0, Some(5.0), "2026-10-05");
        l.save(&path).unwrap();
        let back = Learner::load(&path).unwrap();
        assert_eq!(back, l.state);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn bootstrap_attributes_history_and_marks_the_rest_unknown() {
        let mut s = shadow();
        s.history = LearnerSettings::parse_history("2026-10-05T10:19:00Z=2/5").unwrap();
        let mut l = Learner::new(s, LearnerState::default());
        let rows: Vec<Value> = include_str!("offset_learner_testdata/fills.jsonl")
            .lines()
            .filter(|x| !x.trim().is_empty())
            .map(|x| serde_json::from_str(x).unwrap())
            .collect();
        l.replay(&rows, 5.0);
        // Before the history: unknown. Off-session trip → 2 bp; cash → 5 bp.
        assert_eq!(l.state.contexts[&Window::Off].unknown_trips, 1);
        assert_eq!(l.state.contexts[&Window::Off].arms["2"].trips, 1.0);
        assert_eq!(l.state.contexts[&Window::Cash].arms["5"].trips, 1.0);
        assert_eq!(l.state.contexts[&Window::Cash].markouts.len(), 1);
        assert_eq!(l.state.seen_seq, 9);
    }

    /// The body of `fn name(` in main.rs (to the next method).
    fn main_body(name: &str) -> String {
        let main = include_str!("main.rs");
        let start = main
            .find(&format!("fn {name}("))
            .unwrap_or_else(|| panic!("fn {name} not found"));
        let rest = &main[start..];
        let end = [
            rest[1..].find("\n    async fn "),
            rest[1..].find("\n    fn "),
        ]
        .into_iter()
        .flatten()
        .min()
        .map_or(rest.len(), |i| i + 1);
        rest[..end].to_string()
    }

    #[test]
    fn the_shadow_learner_cannot_reach_the_live_offset() {
        let main = include_str!("main.rs");
        // Its recommendation is never read back into the runtime.
        assert!(!main.contains(".recommend("));
        assert!(!main.contains("recommended_offset_bps\"].as_f64"));
        // The quoting paths never touch the learner.
        // (`tick` only feeds it the markout rows it journals.)
        for f in ["tick", "send_places", "send_modifies"] {
            let body = main_body(f).replace("self.learner_ingest(Some(&row));", "");
            assert!(!body.contains("learner"), "fn {f} uses the learner");
        }
        // Only these methods call into it.
        let allowed = [
            "learner_live_offset",
            "learner_ingest",
            "learner_tick",
            "book_at",
            "book_spilled",
            "finish_tick",
        ];
        for line in main.lines().filter(|l| l.contains("self.learner.")) {
            let owner = allowed.iter().any(|f| main_body(f).contains(line.trim()));
            assert!(owner, "learner call outside the shadow hooks: {line}");
        }
        // The module holds no config and no quoting handle.
        let me = include_str!("offset_learner.rs");
        let code = &me[..me.find("#[cfg(test)]").unwrap()];
        for banned in ["Config", "presence", "QuotePlan", "dex"] {
            assert!(!code.contains(banned), "offset_learner mentions {banned}");
        }
    }

    #[test]
    fn with_the_gate_off_no_model_is_loaded() {
        // #1120 coexistence: the gate stays off on the live units; off must
        // never read a model file (a missing one would refuse the start).
        let mut cfg = crate::config::tests::test_config();
        cfg.gate.model = Some(std::path::PathBuf::from("/nonexistent/gate-model.json"));
        let gate = crate::build_gate(&cfg).expect("off ignores the model path");
        assert!(!gate.on());
        let body = include_str!("main.rs");
        let b = &body[body.find("fn build_gate(").unwrap()..];
        assert!(
            b.find("if !g.mode.on()").unwrap() < b.find("[GATE] loaded").unwrap(),
            "off returns before any [GATE] log or model load"
        );
    }

    #[test]
    fn offline_replay_gives_one_recommendation_per_window() {
        let mut s = shadow();
        s.history = LearnerSettings::parse_history("2026-10-05T10:19:00Z=2/5").unwrap();
        let mut l = Learner::new(s, LearnerState::default());
        let rows: Vec<Value> = include_str!("offset_learner_testdata/fills.jsonl")
            .lines()
            .filter(|x| !x.trim().is_empty())
            .map(|x| serde_json::from_str(x).unwrap())
            .collect();
        let recs = l.replay_offline(&rows, 5.0);
        assert_eq!(recs.len(), 5);
        let cash = recs.iter().find(|r| r.window == Window::Cash).unwrap();
        assert_eq!(cash.live_offset_bps, Some(5.0));
        assert!(cash
            .arms
            .iter()
            .any(|a| a.offset_bps == 5.0 && a.hours > 0.0));
    }

    #[test]
    fn history_parses_and_rejects_garbage() {
        let h =
            LearnerSettings::parse_history("2026-10-09T17:08:00Z=3/5, 2026-10-05T10:19:00Z=2/5")
                .unwrap();
        assert_eq!(h[0].1, 2.0); // sorted
        assert!(LearnerSettings::parse_history("2026-10-05=2").is_err());
        assert!(LearnerMode::parse("live").is_err());
    }
}
