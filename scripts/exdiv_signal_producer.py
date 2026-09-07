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
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import book_signal_file as bsf  # noqa: E402

PRODUCER_ID = "exdiv_948_open_window_v1"
CALENDAR_SCHEMA = 1
# Every schedule instant is defined in US market local time and converted
# per date: the US cash open is 13:30 UTC only under daylight saving, and
# 14:30 UTC in winter (a December EWY event, say). Hard-coded UTC would
# enter an hour early and disable the T-1 close lookup for those dates.
MARKET_TZ = ZoneInfo("America/New_York")
ENTRY_NY = (9, 29, 0)                   # one minute before the cash open
FLATTEN_NY = (9, 36, 0)
LOOKBACK_START_NY = (9, 23)             # rows [09:23 NY, decision] feed the gates
PREV_CLOSE_NY = ((15, 59), (15, 55))    # T-1 close index, in preference order
MIN_FRESH_ROWS = 3
FRESH_OB_SECS = 90.0

GROSS_USD = 8000.0                      # sizing.gross_notional_usd
MAX_LEG_USD = 2000.0
MIN_LEG_USD = 50.0
MAX_SYMBOL_WEIGHT = 0.5                 # sizing.max_symbol_weight
MAX_NET_USD = 2200.0                    # sizing.max_net_usd
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


def market_instant(d: date, hms) -> datetime:
    """A market-local wall time on date `d`, as an aware UTC datetime.

    `d` is the trading date, so the NY calendar date is the same one; the
    conversion is what moves with daylight saving."""
    h, m = hms[0], hms[1]
    sec = hms[2] if len(hms) > 2 else 0
    return datetime.combine(d, dtime(h, m, sec), tzinfo=MARKET_TZ).astimezone(timezone.utc)


def _ts(d: date, hms) -> str:
    return market_instant(d, hms).strftime("%Y-%m-%dT%H:%M:%SZ")


def decision_at(d: date) -> datetime:
    return market_instant(d, ENTRY_NY)


def calendar_from_events(events: list[dict]) -> dict:
    """Runtime calendar (docs/book-runtime.md §2/§4): one entry per date
    with a declared event; key = the date, so several events on one day
    share a decision and never overlap."""
    dates = sorted({e["ex_date"] for e in events if e["status"] == "declared"})
    entries = [{"decision_key": d.isoformat(),
                "decision_at": _ts(d, ENTRY_NY),
                "flatten_at": _ts(d, FLATTEN_NY)} for d in dates]
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


def fresh(r, cutoff: datetime) -> bool:
    """`ob_age_secs` is intrinsic to the row (how stale the book was when it
    was logged); the second bound is the row's own age, measured against the
    decision cutoff rather than wall-clock now, so a late run reads exactly
    the rows an on-time run would have read."""
    age = _f(r.get("ob_age_secs"))
    return age is not None and age <= FRESH_OB_SECS and (cutoff - r["_t"]).total_seconds() <= 600


def window_rows(rows, sym: str, cutoff: datetime) -> list[dict]:
    """Fresh rows in [09:23 NY, cutoff]. `cutoff` is min(now, decision_at):
    a run that starts late (or is retried inside the runtime's grace
    window) must not see prices published after the decision instant, and
    must not see a *smaller* window either -- everything is measured
    against the cutoff, so a retry reproduces the on-time file."""
    start = market_instant(cutoff.date(), LOOKBACK_START_NY)
    return [r for r in rows if r.get("symbol") == sym and start <= r["_t"] <= cutoff
            and fresh(r, cutoff) and mid(r)]


def prev_trading_day(d: date) -> date:
    p = d - timedelta(days=1)
    while p.weekday() >= 5:
        p -= timedelta(days=1)
    return p


def close_index(rows_prev, sym: str, prev_day: date):
    """T-1 index at the last regular-session minute (NY 15:59, fallback
    15:55). Outside regular hours Lighter's index is an internal book price,
    so only these rows count; the UTC minute they land on moves with
    daylight saving, hence the per-date conversion."""
    for hms in PREV_CLOSE_NY:
        target = market_instant(prev_day, hms)
        xs = [_f(r.get("index_price")) for r in rows_prev
              if r.get("symbol") == sym and r["_t"].hour == target.hour
              and r["_t"].minute == target.minute]
        xs = [x for x in xs if x]
        if xs:
            return xs[-1]
    return None


def latest_index(win):
    xs = [_f(r.get("index_price")) for r in win]
    xs = [x for x in xs if x]
    return xs[-1] if xs else None


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

def evaluate_event(ev: dict, rows_t, rows_prev, cutoff: datetime, prev_day: date) -> dict:
    """One event -> {"notional_usd", "skip": reason|None, diagnostics}."""
    sym, hedge = ev["symbol"], ev["hedge"]
    out = {"symbol": sym, "hedge": hedge, "ex_date": ev["ex_date"].isoformat(),
           "dividend_usd": ev["dividend_usd"], "skip": None, "notional_usd": 0.0}
    win = window_rows(rows_t, sym, cutoff)
    out["n_rows"] = len(win)
    if len(win) < MIN_FRESH_ROWS:
        out["skip"] = "no_fresh_book"
        return out
    sp = st.median([spread_bps(r) for r in win])
    l1 = st.median([v for v in (l1_usd(r) for r in win) if v is not None] or [0.0])
    m = st.median([mid(r) for r in win])
    ref = close_index(rows_prev, sym, prev_day)
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
    # Landed-before-open gate: event index move since T-1 close, minus the
    # hedge's (or US500's) move over the same span. Needs a real T-1 close.
    ctl = hedge or "US500"
    ctl_win = window_rows(rows_t, ctl, cutoff)
    ctl_ref = close_index(rows_prev, ctl, prev_day)
    ev_now, ctl_now = latest_index(win), latest_index(ctl_win)
    if out["ref_index_source"] == "t-1_close" and ev_now and ctl_ref and ctl_now:
        adj = ((ev_now / ref - 1) - (ctl_now / ctl_ref - 1)) * 1e4
        out["premarket_adj_move_bps"] = round(adj, 2)
        if adj <= -LANDED_FRAC * div_bps:
            out["skip"] = "gap_already_landed"
            return out
    else:
        out["premarket_adj_move_bps"] = None
    if hedge:
        if len(ctl_win) < MIN_FRESH_ROWS:
            out["skip"] = "hedge_no_fresh_book"
            return out
        hsp = st.median([spread_bps(r) for r in ctl_win])
        out["hedge_spread_bps"] = round(hsp, 2)
        if hsp > SKIP_SPREAD_BPS:
            out["skip"] = "hedge_spread_gt_30bps"
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
    net_usd = abs(sum(w.values())) * gross
    if max_net_usd > 0 and net_usd > max_net_usd:
        scale = min(scale, max_net_usd / net_usd)
    if scale < 1.0:
        w = {k: v * scale for k, v in w.items()}
    w = {k: round(v, 6) for k, v in w.items()}
    return w, {"scale": scale, "gross_usd": round(sum(abs(v) for v in w.values()) * gross, 2),
               "net_usd": round(sum(w.values()) * gross, 2)}


def build_signal(events, d: date, rows_t, rows_prev, now: datetime, gross: float = GROSS_USD,
                 producer_id: str = PRODUCER_ID, max_net_usd: float = MAX_NET_USD) -> dict | None:
    todays = declared_on(events, d)
    if not todays:
        return None
    # Everything the gates and sizing see is bounded at the decision
    # instant. A late run therefore produces the same file it would have
    # produced on time (minus rows that never arrived), instead of one
    # sized from post-decision prices and labelled as if it were not.
    dec = decision_at(d)
    cutoff = min(now, dec)
    prev_day = prev_trading_day(d)
    evals = [evaluate_event(e, rows_t, rows_prev, cutoff, prev_day) for e in todays]
    weights, agg = weights_from_evals(evals, gross, max_net_usd)
    used = [r["_t"] for e in todays for r in window_rows(rows_t, e["symbol"], cutoff)]
    as_of = max(used) if used else cutoff
    meta = {"source": "lighter_exdiv_logger rows + configs/book/exdiv-events.json",
            "date": d.isoformat(), "events": evals, "gross_reference_usd": gross,
            "decision_at": dec.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "input_cutoff": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"), **agg}
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


def upload(path: str, s3_uri: str) -> None:
    subprocess.run(["aws", "s3", "cp", "--only-show-errors", "--content-type", "application/json",
                    "--cache-control", "max-age=20", path, s3_uri], check=True)


# ---------------------------------------------------------------- CLI

def cmd_calendar(a) -> int:
    cal = calendar_from_events(load_events(a.events))
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
    if a.config:
        outside = sorted({x for e in events for x in (e["symbol"], e["hedge"]) if x} - universe_from_config(a.config))
        if outside:
            print(f"refusing: events name symbols outside the deployed universe ({a.config}): {', '.join(outside)}", file=sys.stderr)
            return 2
    if not declared_on(events, d):
        print(f"no declared ex-dividend event on {d}; nothing written")
        return 0
    dec = decision_at(d)
    if now > dec:
        late = (now - dec).total_seconds()
        msg = (f"run started {late:.0f}s after the {dec:%Y-%m-%dT%H:%M:%SZ} decision; "
               f"inputs are bounded at the decision instant")
        if a.refuse_after_decision:
            print(f"refusing: {msg}", file=sys.stderr)
            return 3
        print(f"WARNING: {msg}", file=sys.stderr)
    rows_t, rows_prev = load_rows(a.log_dir, d), load_rows(a.log_dir, prev_trading_day(d))
    sig = build_signal(events, d, rows_t, rows_prev, now, a.gross, a.producer_id, a.max_net_usd)
    bsf.write_signal(a.out, sig)
    ev_summary = ", ".join(f"{e['symbol']}:{e['skip'] or '$' + str(e['notional_usd'])}" for e in sig["meta"]["events"])
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
    c.add_argument("--check", action="store_true", help="exit 1 unless --out already equals the generated calendar")
    s = sub.add_parser("signal", help="write today's signal.json from the logger rows")
    s.add_argument("--events", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--config", default=None, help="deployed runtime config; refuse symbols outside its universe")
    s.add_argument("--date", default=None, help="ex-dividend date YYYY-MM-DD (default: today UTC)")
    s.add_argument("--now", default=None, help="override wall clock, YYYY-MM-DDTHH:MM:SSZ (tests / replay)")
    s.add_argument("--refuse-after-decision", action="store_true",
                   help="exit 3 instead of writing when the run starts after the decision instant "
                        "(inputs are bounded there either way; this makes a late cron loud)")
    s.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    s.add_argument("--gross", type=float, default=GROSS_USD)
    s.add_argument("--max-net-usd", type=float, default=MAX_NET_USD,
                   help="sizing.max_net_usd of the deployed config; the day's legs are scaled "
                        "together to stay inside it (the runtime rejects the whole plan otherwise)")
    s.add_argument("--producer-id", default=PRODUCER_ID)
    s.add_argument("--s3-uri", default=None)
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
