#!/usr/bin/env python3
"""Learner verification readout for the SPY Arcus volume bot (bot-strategy#1093).

Scores the rules frozen in `deploy/arcus_vol/LEARNER_VERIFY_PREREG.md` from
the instance's own state files (copied down from the host first):

    arcus_vol_learner_verify.py --fills fills.jsonl --learner learner.json \\
        --t0 2026-11-05T10:00:00Z --out OUTDIR

`--t0` is the restart that put `LEARNER=live` in effect (the prereg addendum
records it). The test period is the 14 whole UTC days after the day of t0;
the run is refused before it has ended and refused a second time
(`OUTDIR/READOUT_DONE.json`, one look). The baseline period is fixed here
(10-27..11-03, fixed 3 bp off-hours / 5 bp session) and is computed the same
way from the same fills file. Stdlib only.
"""
import argparse
import json
import math
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

POINT_VALUE_PER_M = 72.0  # $3/pt x 24 pt per $1M (owner 2026-11-04)
LIVE_ARMS = [3.0, 5.0, 8.0]
BASELINE_DAYS = [f"2026-10-{d}" for d in range(27, 32)] + ["2026-11-01", "2026-11-02", "2026-11-03"]
TEST_DAYS_N = 14
M1_MIN_HOURS = 2.0  # each live arm live for >= 2 h in at least one window
M2_STUCK_SHARE = 0.8  # widest arm holding >= 80 % of a window's live hours ...
M2_MIN_WINDOW_H = 10.0  # ... in a window with >= 10 live hours ...
# ... while no fill was attributed to it = stuck without information
M3_MAX_CHANGES_PER_DAY = 24.0
NY = ZoneInfo("America/New_York")


def utc_day(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def window_of(ms):
    """Same windows as offset_learner.rs: off / pre / cash / after / weekend (NY time)."""
    t = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(NY)
    wd, m = t.weekday(), t.hour * 60 + t.minute
    if (wd == 4 and m >= 20 * 60) or wd == 5 or (wd == 6 and m < 20 * 60):
        return "weekend"
    if m < 4 * 60 or m >= 20 * 60:
        return "off"
    if m < 9 * 60 + 30:
        return "pre"
    if m < 16 * 60:
        return "cash"
    return "after"


def load_fills(path):
    out = []
    with open(path) as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get("kind") == "fill" and r.get("mode", "live") == "live":
                out.append(r)
    return out


def daily_reward(fills):
    """{utc day: (net $, volume $)}; net = realized - fee."""
    d = {}
    for r in fills:
        day = utc_day(int(r["ts_ms"]))
        net, vol = d.get(day, (0.0, 0.0))
        net += float(r.get("realized") or 0) - float(r.get("fee") or 0)
        vol += abs(float(r.get("notional") or 0))
        d[day] = (net, vol)
    return d


def reward(net_vol):
    net, vol = net_vol
    return net + POINT_VALUE_PER_M * vol / 1e6


def arm_at(live_log, ms):
    """Offset live at `ms` from the (ms, offset|None) log; None = configured fallback."""
    cur = None
    for t, o in live_log:
        if t > ms:
            break
        cur = o
    return cur


def live_hours(live_log, start_ms, end_ms, step_ms=60_000):
    """{(window, arm): hours} by sampling each minute in [start, end)."""
    h = {}
    log = sorted((int(t), o) for t, o in live_log)
    i, cur = 0, None
    for ms in range(start_ms, end_ms, step_ms):
        while i < len(log) and log[i][0] <= ms:
            cur = log[i][1]
            i += 1
        k = (window_of(ms), cur)
        h[k] = h.get(k, 0.0) + step_ms / 3_600_000
    return h


def welch_t(a, b):
    if len(a) < 2 or len(b) < 2:
        return None
    va, vb = statistics.variance(a), statistics.variance(b)
    se = math.sqrt(va / len(a) + vb / len(b))
    return None if se == 0 else (statistics.mean(a) - statistics.mean(b)) / se


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--fills", required=True)
    ap.add_argument("--learner", required=True, help="learner.json (holds live_log)")
    ap.add_argument("--t0", required=True, help="LIVE restart, UTC ISO (prereg addendum)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--now", default=None, help=argparse.SUPPRESS)  # tests only
    a = ap.parse_args(argv)

    t0 = datetime.fromisoformat(a.t0.replace("Z", "+00:00")).astimezone(timezone.utc)
    start = (t0 + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=TEST_DAYS_N)
    now = datetime.fromisoformat(a.now.replace("Z", "+00:00")) if a.now else datetime.now(timezone.utc)
    if now < end:
        print(f"test period runs to {end:%Y-%m-%dT%H:%MZ}: readout refused before it ends", file=sys.stderr)
        return 2
    os.makedirs(a.out, exist_ok=True)
    marker = os.path.join(a.out, "READOUT_DONE.json")
    if os.path.exists(marker):
        print(f"readout already done: {marker} (one look)", file=sys.stderr)
        return 2

    fills = load_fills(a.fills)
    live_log = [(int(t), (None if o is None else float(o))) for t, o in json.load(open(a.learner)).get("live_log", [])]
    s_ms, e_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    test_days = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(TEST_DAYS_N)]

    # M1-M3: mechanics
    hours = live_hours(live_log, s_ms, e_ms)
    fills_by = {}
    for r in fills:
        ms = int(r["ts_ms"])
        if s_ms <= ms < e_ms:
            k = (window_of(ms), arm_at(live_log, ms))
            fills_by[k] = fills_by.get(k, 0) + 1
    m1 = {arm: max([h for (w, o), h in hours.items() if o == arm] or [0.0]) for arm in LIVE_ARMS}
    m1_pass = all(v >= M1_MIN_HOURS for v in m1.values())
    widest = max(LIVE_ARMS)
    stuck = []
    for w in sorted({w for w, _ in hours}):
        tot = sum(h for (ww, _), h in hours.items() if ww == w)
        hw = hours.get((w, widest), 0.0)
        if tot >= M2_MIN_WINDOW_H and hw / tot >= M2_STUCK_SHARE and fills_by.get((w, widest), 0) == 0:
            stuck.append(w)
    m2_pass = not stuck
    changes = sum(1 for t, _ in live_log if s_ms <= t < e_ms)
    m3_pass = changes <= M3_MAX_CHANGES_PER_DAY * TEST_DAYS_N

    # E1: economics on the learner's own objective, per UTC day
    d = daily_reward(fills)
    base = [reward(d.get(x, (0.0, 0.0))) for x in BASELINE_DAYS]
    test = [reward(d.get(x, (0.0, 0.0))) for x in test_days]
    e1_pass = statistics.mean(test) >= statistics.mean(base)
    mech = m1_pass and m2_pass and m3_pass
    verdict = ("BROKEN" if not mech else "WORKS" if e1_pass else "MECHANICS_OK_ECONOMICS_NOT_BETTER")

    res = {
        "t0": a.t0, "test_start": start.isoformat(), "test_end": end.isoformat(),
        "M1_max_live_hours_per_arm": m1, "M1_pass": m1_pass,
        "M2_stuck_windows": stuck, "M2_pass": m2_pass,
        "M3_live_log_changes": changes, "M3_pass": m3_pass,
        "E1_baseline_mean": statistics.mean(base), "E1_test_mean": statistics.mean(test),
        "E1_welch_t_descriptive": welch_t(test, base), "E1_pass": e1_pass,
        "baseline_daily": dict(zip(BASELINE_DAYS, base)), "test_daily": dict(zip(test_days, test)),
        "test_volume": sum(d.get(x, (0.0, 0.0))[1] for x in test_days),
        "test_net": sum(d.get(x, (0.0, 0.0))[0] for x in test_days),
        "hours_by_window_arm": {f"{w}|{o}": round(h, 2) for (w, o), h in sorted(hours.items(), key=str)},
        "fills_by_window_arm": {f"{w}|{o}": n for (w, o), n in sorted(fills_by.items(), key=str)},
        "verdict": verdict,
    }
    with open(os.path.join(a.out, "learner_verify.json"), "w") as fh:
        json.dump(res, fh, indent=1)
    with open(marker, "w") as fh:
        json.dump({"done_at": now.isoformat(), "verdict": verdict}, fh)
    print(json.dumps({k: res[k] for k in ("M1_pass", "M2_pass", "M3_pass", "E1_baseline_mean", "E1_test_mean", "E1_pass", "verdict")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
