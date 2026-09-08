#!/usr/bin/env python3
"""Signal producer for the ex-dividend gap book instance (bot-strategy#948).

Two jobs, both pure functions of committed files plus the local Lighter
WS logger (`~/bot/scripts/lighter_exdiv_logger.py`, bot-strategy#681):

  calendar  configs/book/exdiv-events.json  ->  <instance>.calendar.json
            One runtime calendar entry per ex-dividend date that has at
            least one `status: declared` row: decision one minute before
            the US cash open, flatten six minutes after it (the frozen
            open-window design). Every time here is anchored to
            `America/New_York` and converted to UTC per date, so a winter
            (EST) event gets 14:29Z/14:36Z where a summer (EDT) one gets
            13:29Z/13:36Z. CI checks the committed calendar equals this
            output (`--check`).

  signal    a couple of minutes before the open on an ex-dividend date:
            read the logger rows of the last few minutes, apply the skip
            gates, size each leg from the slippage budget, and write
            schema-v1 signal.json (empty weights = valid "skip", so the
            runtime still records the decision). Optionally upload to S3
            for book_signal_fetch.sh.

            Input rows are always bounded at the decision instant, never
            at "now": a delayed cron or an operator retry inside the
            runtime's grace window must not size the book from prices
            observed after the decision (and possibly after the dividend
            step). `as_of` is then genuinely the last row used.

The runtime never computes a signal (docs/book-runtime.md §1); everything
that decides *whether* and *how much* lives here so the runtime stays a
pure executor. Rules, all from the issue (2026-09-07 comments):

  skip     spread > 30 bps, or L1 < $200, or the step already landed
           before the open (event index move since T-1 close, hedge-
           adjusted, <= -0.5 x dividend bps), or no fresh book, or the
           hedge leg itself fails the spread gate.
  size     slippage budget = 20 % of the dividend (bps). Take the widest
           logged depth band (2/3/5/10/20/50 bps from mid) inside the
           budget and use half of the median cumulative bid depth in it (a
           short hits bids); if no band fits, or the band holds no depth
           (half-spread wider than the band, or rows without the depth
           field), fall back to the frozen 25 % x L1 rule. Cap $2,000 per
           leg, drop legs under $50. Hedge = same notional, opposite sign.

Usage:
  exdiv_signal_producer.py calendar --events configs/book/exdiv-events.json \
      --out configs/book/exdiv-lighter.calendar.json [--check]
  exdiv_signal_producer.py signal --events configs/book/exdiv-events.json \
      --out ~/bot/logs/exdiv_948/signal.json --config configs/book/exdiv-lighter.yaml \
      [--date 2026-09-18] [--log-dir ~/bot/logs/lighter_exdiv] \
      [--s3-uri s3://debot-dashboard/debot/book/exdiv-lighter/signal.json]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics as st
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import book_signal_file as bsf  # noqa: E402

PRODUCER_ID = "exdiv_948_open_window_v1"
CALENDAR_SCHEMA = 1
# Every schedule instant is derived from the frozen XNYS session table
# (configs/engine-b/trading_calendar.json, the A-7 freeze CI already
# diff-checks against its own generator), never from wall-clock UTC or
# weekday arithmetic. That table carries the real open and close of each
# session, so this handles all three ways a fixed schedule goes wrong:
# daylight saving (the open is 13:30 UTC in summer, 14:30 in winter),
# market holidays (2026-09-07 is Labor Day, so T-1 for an 09-08 event is
# 09-04), and half days (2026-11-27 closes at 18:00 UTC, so a 15:59-NY
# close lookup would find nothing).
DEFAULT_TRADING_CALENDAR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "configs", "engine-b", "trading_calendar.json")
ENTRY_OFFSET_SECS = -60                 # one minute before the cash open
FLATTEN_OFFSET_SECS = 360               # six minutes after it
LOOKBACK_OFFSET_SECS = -420             # gates read the seven minutes before the open
# T-1 close reference, in preference order. Several candidates so a single
# missing logger row does not make the landed gate unevaluable (which now
# skips the event).
PREV_CLOSE_OFFSETS_SECS = (-60, -120, -180, -300, -600)
HOST_STATUS_MAX_AGE_SECS = 900          # a status older than this is a dead runtime
MIN_FRESH_ROWS = 3
FRESH_OB_SECS = 90.0
# `ob_age_secs` says how stale the book was WHEN THE ROW WAS WRITTEN, and
# the row-age bound in `fresh` cannot reject anything inside a 6-minute
# lookback window. So a logger that recorded three rows and then stalled
# would still pass MIN_FRESH_ROWS and size the order from a book minutes
# old. Require the newest sample itself to be recent.
MAX_LAST_ROW_AGE_SECS = 120.0

GROSS_USD = 8000.0                      # sizing.gross_notional_usd
MAX_LEG_USD = 2000.0
MIN_LEG_USD = 50.0
MAX_SYMBOL_WEIGHT = 0.5                 # sizing.max_symbol_weight
MAX_NET_USD = 2200.0                    # sizing.max_net_usd
# The runtime re-checks max_net_usd on the ROUNDED target notionals, and
# it rounds each leg's quantity down independently, so a mixed-sign day
# can land a few dollars above a cap the weights satisfied exactly -- and
# a net breach rejects the whole plan (`cap_net`), losing every event of
# the day. Aim below the cap by this fraction so lot rounding has room.
NET_CAP_HEADROOM = 0.05
SKIP_SPREAD_BPS = 30.0
MIN_L1_USD = 200.0
SLIPPAGE_BUDGET_FRAC = 0.20
DEPTH_FILL_FRAC = 0.50
L1_FALLBACK_FRAC = 0.25
LANDED_FRAC = 0.5
DEPTH_BANDS = (2, 3, 5, 10, 20, 50)     # must match the logger's DEPTH_BPS
EVENT_KEYS = {"symbol", "ex_date", "dividend_usd", "hedge", "status", "source"}
STATUSES = {"declared", "estimated"}
DEFAULT_LOG_DIR = os.path.expanduser("~/bot/logs/lighter_exdiv")


# ---------------------------------------------------------------- events

def load_events(path: str) -> list[dict]:
    """Parse and validate the human calendar. Every row must have exactly
    EVENT_KEYS; a typo'd key is an error, not a silently ignored field."""
    with open(path) as f:
        d = json.load(f)
    if not isinstance(d, dict) or d.get("schema_version") != CALENDAR_SCHEMA:
        raise SystemExit(f"{path}: schema_version must be {CALENDAR_SCHEMA}")
    rows = d.get("events")
    if not isinstance(rows, list):
        raise SystemExit(f"{path}: 'events' must be a list")
    seen = set()
    out = []
    for i, e in enumerate(rows):
        if not isinstance(e, dict) or set(e) != EVENT_KEYS:
            raise SystemExit(f"{path}: event {i} keys must be exactly {sorted(EVENT_KEYS)}, got {sorted(e) if isinstance(e, dict) else type(e).__name__}")
        sym = e["symbol"]
        if not isinstance(sym, str) or not re.fullmatch(r"[A-Z0-9]{1,12}", sym):
            raise SystemExit(f"{path}: event {i}: bad symbol {sym!r}")
        try:
            d0 = date.fromisoformat(e["ex_date"])
        except (TypeError, ValueError):
            raise SystemExit(f"{path}: event {i} ({sym}): bad ex_date {e['ex_date']!r}")
        div = e["dividend_usd"]
        if isinstance(div, bool) or not isinstance(div, (int, float)) or not math.isfinite(div) or div <= 0:
            raise SystemExit(f"{path}: event {i} ({sym}): dividend_usd must be a finite positive number")
        hedge = e["hedge"]
        if hedge is not None and (not isinstance(hedge, str) or not re.fullmatch(r"[A-Z0-9]{1,12}", hedge) or hedge == sym):
            raise SystemExit(f"{path}: event {i} ({sym}): bad hedge {hedge!r}")
        if e["status"] not in STATUSES:
            raise SystemExit(f"{path}: event {i} ({sym}): status must be one of {sorted(STATUSES)}")
        if not isinstance(e["source"], str) or not e["source"].strip():
            raise SystemExit(f"{path}: event {i} ({sym}): source must be a non-empty string")
        if (sym, d0) in seen:
            raise SystemExit(f"{path}: duplicate event {sym} {d0}")
        seen.add((sym, d0))
        out.append({**e, "ex_date": d0, "dividend_usd": float(div)})
    return out


def declared_on(events: list[dict], d: date) -> list[dict]:
    return [e for e in events if e["status"] == "declared" and e["ex_date"] == d]


_CAL_CACHE: dict[str, dict] = {}


def load_trading_calendar(path: str = DEFAULT_TRADING_CALENDAR) -> dict:
    """The frozen XNYS/XKRX session table; only the `us_*` fields are read."""
    if path not in _CAL_CACHE:
        with open(path) as f:
            d = json.load(f)
        sessions = d.get("sessions")
        rng = d.get("range") or {}
        if not isinstance(sessions, dict) or not sessions:
            raise SystemExit(f"{path}: no sessions table")
        if not rng.get("start") or not rng.get("end"):
            raise SystemExit(f"{path}: no range")
        _CAL_CACHE[path] = d
    return _CAL_CACHE[path]


def _us_session(d: date, cal_path: str) -> dict:
    cal = load_trading_calendar(cal_path)
    rng = cal["range"]
    if not (rng["start"] <= d.isoformat() <= rng["end"]):
        raise SystemExit(
            f"{d} is outside the frozen trading calendar ({rng['start']}..{rng['end']}); "
            "regenerate configs/engine-b/trading_calendar.json before scheduling it")
    s = cal["sessions"].get(d.isoformat())
    if s is None:
        raise SystemExit(f"{d} is missing from the frozen trading calendar")
    return s


def is_us_session(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> bool:
    return bool(_us_session(d, cal_path).get("us_is_open"))


def _session_edge(d: date, field: str, cal_path: str) -> datetime:
    s = _us_session(d, cal_path)
    if not s.get("us_is_open") or s.get(field) is None:
        raise SystemExit(f"{d} is not a US trading session; it cannot carry an ex-dividend event")
    return datetime.fromtimestamp(s[field] / 1_000_000, tz=timezone.utc)


def session_open(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> datetime:
    return _session_edge(d, "us_open_utc_us", cal_path)


def session_close(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> datetime:
    return _session_edge(d, "us_close_utc_us", cal_path)


def _ts(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def decision_at(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> datetime:
    return session_open(d, cal_path) + timedelta(seconds=ENTRY_OFFSET_SECS)


def flatten_at(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> datetime:
    return session_open(d, cal_path) + timedelta(seconds=FLATTEN_OFFSET_SECS)


def lookback_start(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> datetime:
    return session_open(d, cal_path) + timedelta(seconds=LOOKBACK_OFFSET_SECS)


def calendar_from_events(events: list[dict],
                         cal_path: str = DEFAULT_TRADING_CALENDAR) -> dict:
    """Runtime calendar (docs/book-runtime.md §2/§4): one entry per date
    with a declared event; key = the date, so several events on one day
    share a decision and never overlap."""
    dates = sorted({e["ex_date"] for e in events if e["status"] == "declared"})
    entries = [{"decision_key": d.isoformat(),
                "decision_at": _ts(decision_at(d, cal_path)),
                "flatten_at": _ts(flatten_at(d, cal_path))} for d in dates]
    canon = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    version = "exdiv-948-v1-" + hashlib.sha256(canon.encode()).hexdigest()[:12]
    return {"calendar_version": version, "entries": entries}


def render_calendar(cal: dict) -> str:
    return json.dumps(cal, indent=1, sort_keys=True) + "\n"


# ---------------------------------------------------------------- logger rows

def _f(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def load_rows(log_dir: str, d: date) -> list[dict]:
    p = os.path.join(log_dir, f"orcl_{d.strftime('%Y%m%d')}.jsonl")
    rows = []
    if not os.path.exists(p):
        return rows
    with open(p) as f:
        for line in f:
            try:
                r = json.loads(line)
                r["_t"] = datetime.strptime(r["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except (ValueError, KeyError, TypeError):
                continue
            rows.append(r)
    return rows


def mid(r):
    b, a = _f(r.get("best_bid")), _f(r.get("best_ask"))
    return (b + a) / 2 if b and a and a >= b and b > 0 else None


def spread_bps(r):
    m = mid(r)
    return (_f(r["best_ask"]) - _f(r["best_bid"])) / m * 1e4 if m else None


def l1_usd(r):
    m = mid(r)
    bs, as_ = _f(r.get("best_bid_size")), _f(r.get("best_ask_size"))
    if m is None or bs is None or as_ is None:
        return None
    return min(bs, as_) * m


def book_fresh(r) -> bool:
    """`ob_age_secs` is intrinsic to the row: how stale the book was when
    the logger wrote it. A row carrying a stalled book is not a price
    sample at all, whichever day it belongs to -- so this is the one check
    both the current-day window and the T-1 close references apply. (The
    close references used to skip it, so a book that stalled into the
    previous close could shift the landed-step baseline by however far it
    had drifted, against a gate of only a few bps.)"""
    age = _f(r.get("ob_age_secs"))
    return age is not None and age <= FRESH_OB_SECS


def fresh(r, cutoff: datetime) -> bool:
    """`book_fresh` plus the row's own age, measured against the decision
    cutoff rather than wall-clock now, so a late run reads exactly the rows
    an on-time run would have read."""
    return book_fresh(r) and (cutoff - r["_t"]).total_seconds() <= 600


def window_rows(rows, sym: str, cutoff: datetime, start: datetime) -> list[dict]:
    """Fresh rows in [09:23 NY, cutoff]. `cutoff` is min(now, decision_at):
    a run that starts late (or is retried inside the runtime's grace
    window) must not see prices published after the decision instant, and
    must not see a *smaller* window either -- everything is measured
    against the cutoff, so a retry reproduces the on-time file."""
    return [r for r in rows if r.get("symbol") == sym and start <= r["_t"] <= cutoff
            and fresh(r, cutoff) and mid(r)]


def prev_trading_day(d: date, cal_path: str = DEFAULT_TRADING_CALENDAR) -> date:
    """The previous US *session*, not merely the previous weekday: an event
    the day after a market holiday would otherwise take its T-1 close from
    a day the cash market never opened, where the venue's index is an
    internal book price and the landed-step gate reads garbage."""
    p = d - timedelta(days=1)
    for _ in range(10):
        if is_us_session(p, cal_path):
            return p
        p -= timedelta(days=1)
    raise SystemExit(f"no US trading session in the 10 days before {d}")


def close_index(rows_prev, sym: str, prev_day: date,
                cal_path: str = DEFAULT_TRADING_CALENDAR):
    """T-1 index at the last minute of the previous session (close - 1 min,
    fallback close - 5 min). Outside regular hours Lighter's index is an
    internal book price, so only these rows count, and the minute they land
    on moves with both daylight saving and half days (2026-11-27 closes at
    18:00 UTC, not 21:00)."""
    close = session_close(prev_day, cal_path)
    for off in PREV_CLOSE_OFFSETS_SECS:
        target = close + timedelta(seconds=off)
        xs = [_f(r.get("index_price")) for r in rows_prev
              if r.get("symbol") == sym and r["_t"].hour == target.hour
              and r["_t"].minute == target.minute and book_fresh(r)]
        xs = [x for x in xs if x]
        if xs:
            return xs[-1]
    return None


def latest_index(win):
    xs = [_f(r.get("index_price")) for r in win]
    xs = [x for x in xs if x]
    return xs[-1] if xs else None


def latest_mid(win):
    xs = [mid(r) for r in win]
    xs = [x for x in xs if x]
    return xs[-1] if xs else None


def _by_minute(win):
    out = {}
    for r in win:
        m = mid(r)
        if m is not None:
            out[r["_t"].replace(second=0, microsecond=0)] = m
    return out


def latest_mid_pair(win, ctl_win):
    """The event and control mids at the latest minute BOTH were sampled.

    Taking each leg's own last row would compare, say, the event at 09:27
    against its control at 09:25 whenever one leg dropped a sample, and
    the market movement in between would be read as relative premarket
    movement -- the same error the T-1 references avoid by pairing.
    Returns (event_mid, control_mid, minute) or (None, None, None)."""
    a, b = _by_minute(win), _by_minute(ctl_win)
    common = sorted(set(a) & set(b))
    if not common:
        return None, None, None
    t = common[-1]
    return a[t], b[t], t


def last_row_age_secs(win, cutoff: datetime):
    """Seconds between the newest sample in `win` and the decision."""
    return (cutoff - max(r["_t"] for r in win)).total_seconds() if win else None


def _mid_at(rows_prev, sym: str, target: datetime):
    """Mid of the last book-fresh row of `sym` in `target`'s minute. A row
    whose book had already stalled (`book_fresh`) is skipped exactly as a
    current-day row would be, so a stalled close makes the landed gate
    unavailable rather than quietly wrong."""
    xs = [mid(r) for r in rows_prev
          if r.get("symbol") == sym and r["_t"].hour == target.hour
          and r["_t"].minute == target.minute and book_fresh(r)]
    xs = [x for x in xs if x]
    return xs[-1] if xs else None


def close_mid(rows_prev, sym: str, prev_day: date,
              cal_path: str = DEFAULT_TRADING_CALENDAR):
    """The perp mid at the last minute of the previous session."""
    close = session_close(prev_day, cal_path)
    for off in PREV_CLOSE_OFFSETS_SECS:
        m = _mid_at(rows_prev, sym, close + timedelta(seconds=off))
        if m is not None:
            return m
    return None


def close_mid_pair(rows_prev, sym: str, ctl: str, prev_day: date,
                   cal_path: str = DEFAULT_TRADING_CALENDAR):
    """T-1 close mids for the event and its control, resolved on the SAME
    minute. Resolving them independently would let a logger gap on one leg
    pull its reference minutes earlier, so the differential would then
    include that leg's own drift over the gap -- up to 9 minutes with the
    widened offset list, against a gate threshold of only a few bps.
    Returns (event_mid, control_mid); the control is None when no offset
    has both, which makes the gate unevaluable and skips the event."""
    close = session_close(prev_day, cal_path)
    ev_only = None
    for off in PREV_CLOSE_OFFSETS_SECS:
        target = close + timedelta(seconds=off)
        ev, cm = _mid_at(rows_prev, sym, target), _mid_at(rows_prev, ctl, target)
        if ev is not None and cm is not None:
            return ev, cm
        if ev_only is None and ev is not None:
            ev_only = ev
    # No common minute: fall back to the event alone (the caller then uses
    # the unadjusted gate, which can only skip more often).
    return ev_only, None


def band_for_budget(budget_bps: float):
    fit = [b for b in DEPTH_BANDS if b <= budget_bps]
    return max(fit) if fit else None


def depth_median(win, band: int, side: str):
    xs = []
    for r in win:
        d = r.get("cum_depth_usd")
        if isinstance(d, dict) and str(band) in d:
            v = _f(d[str(band)].get(side))
            if v is not None:
                xs.append(v)
    return st.median(xs) if xs else None


# ---------------------------------------------------------------- decision

def evaluate_event(ev: dict, rows_t, rows_prev, cutoff: datetime, prev_day: date,
                   start: datetime, cal_path: str = DEFAULT_TRADING_CALENDAR) -> dict:
    """One event -> {"notional_usd", "skip": reason|None, diagnostics}."""
    sym, hedge = ev["symbol"], ev["hedge"]
    out = {"symbol": sym, "hedge": hedge, "ex_date": ev["ex_date"].isoformat(),
           "dividend_usd": ev["dividend_usd"], "skip": None, "notional_usd": 0.0}
    win = window_rows(rows_t, sym, cutoff, start)
    out["n_rows"] = len(win)
    if len(win) < MIN_FRESH_ROWS:
        out["skip"] = "no_fresh_book"
        return out
    age = last_row_age_secs(win, cutoff)
    out["last_row_age_secs"] = round(age, 1)
    if age > MAX_LAST_ROW_AGE_SECS:
        # Rows exist but the feed stopped: everything below would be
        # measured on a book that is minutes old.
        out["skip"] = "book_stalled"
        return out
    sp = st.median([spread_bps(r) for r in win])
    l1 = st.median([v for v in (l1_usd(r) for r in win) if v is not None] or [0.0])
    m = st.median([mid(r) for r in win])
    ref = close_index(rows_prev, sym, prev_day, cal_path)
    out["ref_index_source"] = "t-1_close" if ref else "current_mid"
    ref = ref or m
    div_bps = ev["dividend_usd"] / ref * 1e4
    out.update({"spread_bps": round(sp, 2), "l1_usd": round(l1, 2), "mid": m, "ref_index": ref,
                "dividend_bps": round(div_bps, 2)})
    if sp > SKIP_SPREAD_BPS:
        out["skip"] = "spread_gt_30bps"
        return out
    if l1 < MIN_L1_USD:
        out["skip"] = "l1_lt_200usd"
        return out
    ctl = hedge or "US500"
    ctl_win = window_rows(rows_t, ctl, cutoff, start)
    ctl_age = last_row_age_secs(ctl_win, cutoff)
    out["control_last_row_age_secs"] = round(ctl_age, 1) if ctl_age is not None else None
    if hedge:
        if len(ctl_win) < MIN_FRESH_ROWS:
            out["skip"] = "hedge_no_fresh_book"
            return out
        if ctl_age > MAX_LAST_ROW_AGE_SECS:
            out["skip"] = "hedge_book_stalled"
            return out
        hsp = st.median([spread_bps(r) for r in ctl_win])
        out["hedge_spread_bps"] = round(hsp, 2)
        if hsp > SKIP_SPREAD_BPS:
            out["skip"] = "hedge_spread_gt_30bps"
            return out

    # Landed-before-open gate: has the step already arrived while we were
    # waiting? Measured on the perp MID, not on index_price: before the
    # cash open Lighter's index for a single-name/ETF perp is an internal
    # book-derived price (see close_index), while the futures-derived
    # hedge legs quote continuously -- comparing one against the other
    # mixes two different price definitions, and a few bps of thin
    # pre-open book noise on the event leg would false-fire the gate and
    # throw away the event (QQQ's threshold is only about -6 bps). The mid
    # is also the honest measure of "can we still capture it": it is what
    # we would transact at.
    ev_ref_mid, ctl_ref_mid = close_mid_pair(rows_prev, sym, ctl, prev_day, cal_path)
    ev_now, ctl_now, paired_minute = latest_mid_pair(win, ctl_win)
    if paired_minute is not None:
        out["premarket_paired_minute"] = paired_minute.strftime("%Y-%m-%dT%H:%M:00Z")
        # The paired minute is what the gate actually measures at, so it is
        # the one that has to be recent, not merely the event's own newest
        # row (already checked above).
        if (cutoff - paired_minute).total_seconds() > MAX_LAST_ROW_AGE_SECS:
            ev_now = ctl_now = None
    if ev_ref_mid and ev_now and ctl_ref_mid and ctl_now:
        adj = (ev_now / ev_ref_mid - 1) * 1e4 - (ctl_now / ctl_ref_mid - 1) * 1e4
        out["premarket_adj_move_bps"] = round(adj, 2)
        out["premarket_basis"] = "mid_vs_t-1_close_mid"
        if adj <= -LANDED_FRAC * div_bps:
            out["skip"] = "gap_already_landed"
            return out
    else:
        # Fail CLOSED whenever any of the four inputs is missing, the
        # control included. An earlier revision fell back to the event's
        # own unadjusted move on the claim that it "can only skip more
        # often" -- that is false: an event down 3 bps while its control is
        # up 5 bps has an adjusted move of -8 bps and would skip at a -6
        # bps threshold, while the unadjusted -3 bps would trade. The
        # control adjustment is the gate, not a refinement of it (single
        # stock idio noise over this window runs 3-5x the dividend), so
        # without it there is no gate. Missing one event costs one
        # observation; trading with the check silently disabled costs a
        # loss and a corrupted capture measurement.
        out["premarket_adj_move_bps"] = None
        out["premarket_basis"] = None
        out["landed_gate_inputs"] = {
            "event_t1_mid": ev_ref_mid is not None, "control_t1_mid": ctl_ref_mid is not None,
            "event_now_mid": ev_now is not None, "control_now_mid": ctl_now is not None,
            "control": ctl}
        out["skip"] = "landed_gate_unavailable"
        return out
    budget = SLIPPAGE_BUDGET_FRAC * div_bps
    band = band_for_budget(budget)
    out["slippage_budget_bps"] = round(budget, 2)
    out["depth_band_bps"] = band
    if band is not None:
        dep = depth_median(win, band, "bid")
        out["cum_bid_depth_usd"] = dep
        if dep is None:
            # Logger predates the depth extension for these rows.
            notional = L1_FALLBACK_FRAC * l1
            out["size_rule"] = "l1_fallback_no_depth"
        elif dep <= 0:
            # Half-spread wider than the band: the touch itself sits outside
            # it, so "depth within budget" is empty. The frozen L1 rule is
            # the only size rule that still applies; the spread gate above
            # already decided the event is tradable.
            notional = L1_FALLBACK_FRAC * l1
            out["size_rule"] = "l1_fallback_touch_outside_band"
        else:
            notional = DEPTH_FILL_FRAC * dep
            out["size_rule"] = f"depth_{band}bps"
    else:
        notional = L1_FALLBACK_FRAC * l1
        out["size_rule"] = "l1_fallback_budget_lt_5bps"
    notional = min(MAX_LEG_USD, notional)
    if notional < MIN_LEG_USD:
        out["skip"] = "too_small"
        out["notional_usd"] = round(notional, 2)
        return out
    out["notional_usd"] = round(notional, 2)
    out["expected_net_bps"] = round(div_bps - sp - (band or 0), 2)
    return out


WEIGHT_PLACES = 6


def _quantize(v: float, places: int = WEIGHT_PLACES) -> float:
    """Round a weight toward zero, so |w| never grows in the last place."""
    q = 10 ** places
    return math.floor(abs(v) * q) / q * (1.0 if v >= 0 else -1.0)


def net_cap_target(max_net_usd: float) -> float:
    """What the producer aims for under the runtime's hard net cap."""
    return max_net_usd * (1.0 - NET_CAP_HEADROOM)


def _cap_excess(w: dict, gross: float, max_net_usd: float):
    """The factor every weight must be multiplied by to fit every cap, or
    None when the book already fits. Mirrors src/book/signal.rs and the
    runtime's post-rounding planner caps."""
    if not w:
        return None
    factors = []
    tot = sum(abs(v) for v in w.values())
    if tot > 1.0:
        factors.append(1.0 / tot)
    big = max(abs(v) for v in w.values())
    if big > MAX_SYMBOL_WEIGHT:
        factors.append(MAX_SYMBOL_WEIGHT / big)
    net_usd = abs(sum(w.values())) * gross
    if max_net_usd > 0 and net_usd > net_cap_target(max_net_usd):
        factors.append(net_cap_target(max_net_usd) / net_usd)
    if not factors:
        return None
    # A hair under the exact ratio so the next quantisation cannot land
    # back exactly on the boundary.
    return min(factors) * (1.0 - 1e-9)


def weights_from_evals(evals: list[dict], gross: float,
                       max_net_usd: float = MAX_NET_USD) -> tuple[dict, dict]:
    """Sum legs into weights (fractions of gross). If the day's book would
    breach sum|w| <= 1, a symbol cap, or the instance's net-dollar cap,
    scale every leg down together so the hedge ratios survive; the runtime
    rejects a whole plan that breaches any cap, which would lose every
    event of the day rather than shrink them."""
    w: dict[str, float] = {}
    for e in evals:
        if e["skip"] or e["notional_usd"] <= 0:
            continue
        w[e["symbol"]] = w.get(e["symbol"], 0.0) - e["notional_usd"] / gross
        if e["hedge"]:
            w[e["hedge"]] = w.get(e["hedge"], 0.0) + e["notional_usd"] / gross
    w = {k: v for k, v in w.items() if abs(v) > 1e-12}
    scale = 1.0
    sized = [e for e in evals if not e["skip"] and e["notional_usd"] > 0]
    tot = sum(abs(v) for v in w.values())
    if tot > 1.0:
        scale = min(scale, 1.0 / tot)
    big = max((abs(v) for v in w.values()), default=0.0)
    if big > MAX_SYMBOL_WEIGHT:
        scale = min(scale, MAX_SYMBOL_WEIGHT / big)
    # Unhedged single-stock events add up on one side: two $2,000 shorts
    # are only 0.5 gross and 0.25 per symbol, both inside their caps, yet
    # $4,000 net against a $2,200 cap -> the runtime would reject the plan
    # (cap_net) and neither event would trade.
    # The same target `_cap_excess` uses, so the first pass does not leave
    # work for the verification loop and under-report the scale.
    net_usd = abs(sum(w.values())) * gross
    if max_net_usd > 0 and net_usd > net_cap_target(max_net_usd):
        scale = min(scale, net_cap_target(max_net_usd) / net_usd)
    if scale < 1.0:
        w = {k: v * scale for k, v in w.items()}
    # Quantise toward zero, never to nearest: the runtime's caps are hard
    # limits (`sum |w| <= 1 + 1e-9`), and rounding many legs independently
    # can push the aggregate back over one after scaling put it inside --
    # three hedged legs of $1588.57/$1649.26/$1685.08 land on 1.000002 and
    # the whole signal is rejected. Truncation can only shrink |w|.
    w = {k: _quantize(v) for k, v in w.items()}
    # Truncation moves every weight toward zero, so gross and per-symbol
    # can only improve, but the *net* of opposite-signed legs can drift by
    # up to one tick per leg. Verify and shrink again if it still breaches.
    for _ in range(4):
        excess = _cap_excess(w, gross, max_net_usd)
        if excess is None:
            break
        # Fold every shrink into the reported scale: `applied_notional_usd`
        # exists to say what was actually sent, so it must not be computed
        # from a factor that a later pass has already tightened.
        scale *= excess
        w = {k: _quantize(v * excess) for k, v in w.items()}
    else:
        raise SystemExit(
            f"could not fit the day's book inside the caps (gross {sum(abs(v) for v in w.values()):.6f}, "
            f"net ${abs(sum(w.values())) * gross:.2f} vs ${max_net_usd:.2f}); refusing to publish")
    # A common scale-down changes what is actually traded, so record it on
    # each event rather than leaving the readout to report the pre-scale
    # size it never sent.
    for e in sized:
        e["applied_notional_usd"] = round(e["notional_usd"] * scale, 2)
    return w, {"scale": scale, "gross_usd": round(sum(abs(v) for v in w.values()) * gross, 2),
               "net_usd": round(sum(w.values()) * gross, 2)}


def build_signal(events, d: date, rows_t, rows_prev, now: datetime, gross: float = GROSS_USD,
                 producer_id: str = PRODUCER_ID, max_net_usd: float = MAX_NET_USD,
                 cal_path: str = DEFAULT_TRADING_CALENDAR) -> dict | None:
    todays = declared_on(events, d)
    if not todays:
        return None
    # Everything the gates and sizing see is bounded at the decision
    # instant. A late run therefore produces the same file it would have
    # produced on time (minus rows that never arrived), instead of one
    # sized from post-decision prices and labelled as if it were not.
    dec = decision_at(d, cal_path)
    cutoff = min(now, dec)
    start = lookback_start(d, cal_path)
    prev_day = prev_trading_day(d, cal_path)
    evals = [evaluate_event(e, rows_t, rows_prev, cutoff, prev_day, start, cal_path)
             for e in todays]
    weights, agg = weights_from_evals(evals, gross, max_net_usd)
    # Every row an evaluation actually consumed, control/hedge legs
    # included: their spread feeds the hedge gate and their mid feeds the
    # landed gate, so a hedge sample newer than the event's would make
    # `as_of` predate data the decision was really based on.
    used = [r["_t"]
            for e in todays
            for sym in {e["symbol"], e["hedge"] or "US500"}
            for r in window_rows(rows_t, sym, cutoff, start)]
    as_of = max(used) if used else cutoff
    meta = {"source": "lighter_exdiv_logger rows + configs/book/exdiv-events.json",
            "date": d.isoformat(), "events": evals, "gross_reference_usd": gross,
            "decision_at": _ts(dec), "input_cutoff": _ts(cutoff),
            "session_open": _ts(session_open(d, cal_path)),
            "prev_session": prev_day.isoformat(), **agg}
    return bsf.build_signal(producer_id, now, as_of, d.isoformat(), weights, meta)


def universe_from_config(path: str) -> set:
    text = open(path).read()
    block = re.search(r"^universe:\s*$(.*?)^[a-z_]+:", text, re.M | re.S)
    if not block:
        raise SystemExit(f"{path}: no universe: block")
    syms = set(re.findall(r"^\s+- ([A-Za-z0-9_]+)\s*$", block.group(1), re.M))
    if not syms:
        raise SystemExit(f"{path}: universe.symbols is empty")
    return syms


def read_host_status(uri_or_path: str) -> dict | None:
    """The runtime's published status.json (local path or s3:// URI), or
    None when it cannot be read. Unreadable is not the same as stale: a
    transient S3 error must not block a legitimate publish."""
    try:
        if uri_or_path.startswith("s3://"):
            out = subprocess.run(["aws", "s3", "cp", "--only-show-errors", uri_or_path, "-"],
                                 check=True, capture_output=True, text=True).stdout
        else:
            with open(uri_or_path) as f:
                out = f.read()
        return json.loads(out)
    except (subprocess.CalledProcessError, OSError, ValueError):
        return None


def host_schedule_drift(status: dict, key: str, dec: datetime,
                        now: datetime | None = None,
                        max_age_secs: int = HOST_STATUS_MAX_AGE_SECS) -> str | None:
    """A message when the running instance has not scheduled `key`, else None.

    The runtime loads its calendar file once at startup from
    /opt/book-runtime/, which the /opt/debot config sync cannot write. So a
    regenerated calendar (a new event, or EWY going from estimated to
    declared) reaches the host but not the process until the instance is
    reinstalled and restarted, and the decision is silently never
    scheduled.

    Staleness is checked first: a runtime that died days ago leaves a
    status whose `next_decision_key` may well be today's, so reading the
    key alone would pass exactly the case this guard exists to catch -- a
    decision nobody is going to consume."""
    book = status.get("book")
    if not isinstance(book, dict):
        return "status.json has no `book` block"
    now = now or datetime.now(timezone.utc)
    ts = status.get("ts")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        return "status.json has no numeric `ts`; cannot tell whether the runtime is alive"
    age = (now - datetime.fromtimestamp(ts, tz=timezone.utc)).total_seconds()
    if age > max_age_secs:
        return (f"the instance's status is {age / 60:.0f} min old (updated "
                f"{status.get('updated_at', '?')}); the runtime is not running, so nothing "
                f"would consume this decision")
    got_key, got_at = book.get("next_decision_key"), book.get("next_decision_at")
    if got_key is None:
        return "the running instance reports no next decision (its calendar may be empty or exhausted)"
    if got_key != key:
        return (f"the running instance's next decision is {got_key} ({got_at}), not today's {key}: "
                f"its calendar predates this events file")
    if got_at and got_at != _ts(dec):
        return (f"the running instance schedules {key} at {got_at}, not {_ts(dec)}: "
                f"its calendar was generated from a different session table")
    return None


def upload(path: str, s3_uri: str) -> None:
    subprocess.run(["aws", "s3", "cp", "--only-show-errors", "--content-type", "application/json",
                    "--cache-control", "max-age=20", path, s3_uri], check=True)


# ---------------------------------------------------------------- CLI

def cmd_calendar(a) -> int:
    cal = calendar_from_events(load_events(a.events), a.trading_calendar)
    text = render_calendar(cal)
    if a.check:
        if not os.path.exists(a.out):
            print(f"{a.out} is missing; regenerate with the calendar subcommand", file=sys.stderr)
            return 1
        if open(a.out).read() != text:
            print(f"{a.out} differs from {a.events}; regenerate with the calendar subcommand", file=sys.stderr)
            return 1
        print(f"ok {a.out} matches {a.events} ({len(cal['entries'])} entries, {cal['calendar_version']})")
        return 0
    tmp = a.out + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, a.out)
    print(f"wrote {a.out}: {len(cal['entries'])} entries, {cal['calendar_version']}")
    return 0


def cmd_signal(a) -> int:
    now = bsf.parse_ts(a.now) if a.now else datetime.now(timezone.utc).replace(microsecond=0)
    d = date.fromisoformat(a.date) if a.date else now.date()
    events = load_events(a.events)
    todays = declared_on(events, d)
    if a.config:
        # Only today's events can block today's publish. A symbol missing
        # from the universe on some future row is worth warning about --
        # it must be added before that date -- but refusing here would let
        # a row for next month silently cancel this morning's event.
        universe = universe_from_config(a.config)
        def _missing(rows):
            return sorted({x for e in rows for x in (e["symbol"], e["hedge"]) if x} - universe)
        blocking = _missing(todays)
        if blocking:
            print(f"refusing: today's events name symbols outside the deployed universe "
                  f"({a.config}): {', '.join(blocking)}", file=sys.stderr)
            return 2
        later = _missing([e for e in events
                          if e["status"] == "declared" and e["ex_date"] > d])
        if later:
            print(f"WARNING: later declared events name symbols outside the deployed universe "
                  f"({a.config}): {', '.join(later)}. Add them to universe.symbols and redeploy "
                  f"before those dates.", file=sys.stderr)
    if not todays:
        print(f"no declared ex-dividend event on {d}; nothing written")
        return 0
    dec = decision_at(d, a.trading_calendar)
    start = lookback_start(d, a.trading_calendar)
    if now < start:
        # The UTC cron carries one line per possible open hour, so on a
        # winter event the summer line fires an hour early. Without this it
        # would "succeed" with an empty input window -- every event
        # no_fresh_book -- and upload a go-flat signal for the day, which
        # the runtime may consume before the real run publishes.
        print(f"refusing: run started at {_ts(now)}, before the {_ts(start)} input window opens "
              f"(the decision is {_ts(dec)})", file=sys.stderr)
        return 5
    if now > dec:
        # Inputs are bounded at the decision either way, but the runtime
        # would still ACT on a file that lands inside its grace window --
        # entering at or after the open, i.e. after the very step this
        # strategy exists to capture. Refusing is the default; the opt-out
        # is for regenerating a file offline (replay, backfill), not for
        # the unattended cron.
        late = (now - dec).total_seconds()
        msg = (f"run started {late:.0f}s after the {_ts(dec)} decision; "
               f"entering now would be at or after the open")
        if not a.allow_after_decision:
            print(f"refusing: {msg} (pass --allow-after-decision to write anyway)", file=sys.stderr)
            return 3
        print(f"WARNING: {msg}; writing anyway (--allow-after-decision)", file=sys.stderr)
    if a.verify_host_status:
        status = read_host_status(a.verify_host_status)
        if status is None:
            print(f"WARNING: could not read {a.verify_host_status}; publishing without the "
                  f"host-schedule check", file=sys.stderr)
        else:
            drift = host_schedule_drift(status, d.isoformat(), dec, now)
            if drift:
                print(f"refusing: {drift}. Reinstall the instance (install_book_runtime.sh) and "
                      f"restart it so it loads the current calendar -- see "
                      f"docs/exdiv-book-operations.md 'Calendar updates'.", file=sys.stderr)
                return 4
    prev_day = prev_trading_day(d, a.trading_calendar)
    rows_t, rows_prev = load_rows(a.log_dir, d), load_rows(a.log_dir, prev_day)
    sig = build_signal(events, d, rows_t, rows_prev, now, a.gross, a.producer_id, a.max_net_usd,
                       a.trading_calendar)
    bsf.write_signal(a.out, sig)
    ev_summary = ", ".join(
        f"{e['symbol']}:{e['skip'] or '$' + str(e.get('applied_notional_usd', e['notional_usd']))}"
        for e in sig["meta"]["events"])
    print(f"wrote {a.out} key={sig['decision_key']} legs={len(sig['weights'])} gross=${sig['meta']['gross_usd']} "
          f"net=${sig['meta']['net_usd']} sha={sig['payload_sha256'][:12]} [{ev_summary}]")
    if a.s3_uri:
        upload(a.out, a.s3_uri)
        print(f"uploaded to {a.s3_uri}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("calendar", help="events.json -> runtime calendar json")
    c.add_argument("--events", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--trading-calendar", default=DEFAULT_TRADING_CALENDAR,
                   help="frozen XNYS session table; every instant is derived from it")
    c.add_argument("--check", action="store_true", help="exit 1 unless --out already equals the generated calendar")
    s = sub.add_parser("signal", help="write today's signal.json from the logger rows")
    s.add_argument("--events", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--config", default=None, help="deployed runtime config; refuse symbols outside its universe")
    s.add_argument("--date", default=None, help="ex-dividend date YYYY-MM-DD (default: today UTC)")
    s.add_argument("--now", default=None, help="override wall clock, YYYY-MM-DDTHH:MM:SSZ (tests / replay)")
    s.add_argument("--allow-after-decision", action="store_true",
                   help="write even when the run starts after the decision instant. Default is to "
                        "exit 3: the runtime's grace window would otherwise let a late file open "
                        "the position at or after the open, past the step being captured. For "
                        "offline regeneration only, never for the cron")
    s.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    s.add_argument("--trading-calendar", default=DEFAULT_TRADING_CALENDAR,
                   help="frozen XNYS session table; every instant is derived from it")
    s.add_argument("--gross", type=float, default=GROSS_USD)
    s.add_argument("--max-net-usd", type=float, default=MAX_NET_USD,
                   help="sizing.max_net_usd of the deployed config; the day's legs are scaled "
                        "together to stay inside it (the runtime rejects the whole plan otherwise)")
    s.add_argument("--producer-id", default=PRODUCER_ID)
    s.add_argument("--s3-uri", default=None)
    s.add_argument("--verify-host-status", default=None,
                   help="the running instance's status.json (path or s3:// URI). Refuse to publish "
                        "(exit 4) when it has not scheduled today's decision, which means its "
                        "calendar file predates this events file and the decision would be "
                        "silently skipped. An unreadable status only warns")
    a = ap.parse_args()
    if a.cmd == "calendar":
        return cmd_calendar(a)
    if not (math.isfinite(a.gross) and a.gross > 0):
        print(f"--gross must be a finite positive number, got {a.gross}", file=sys.stderr)
        return 2
    if not (math.isfinite(a.max_net_usd) and a.max_net_usd > 0):
        print(f"--max-net-usd must be a finite positive number, got {a.max_net_usd}", file=sys.stderr)
        return 2
    return cmd_signal(a)


if __name__ == "__main__":
    sys.exit(main())
