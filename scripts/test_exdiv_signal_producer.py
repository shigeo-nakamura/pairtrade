#!/usr/bin/env python3
"""Tests for exdiv_signal_producer.py (synthetic logger rows, no network)."""
import json
import os
import statistics as st
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import book_signal_file as bsf  # noqa: E402
import exdiv_signal_producer as xp  # noqa: E402

D = date(2026, 9, 18)
NOW = datetime(2026, 9, 18, 13, 27, 30, tzinfo=timezone.utc)
EVENTS = [
    {"symbol": "SPY", "ex_date": D, "dividend_usd": 1.83, "hedge": "US500", "status": "declared", "source": "t"},
    {"symbol": "IBM", "ex_date": D, "dividend_usd": 1.69, "hedge": None, "status": "declared", "source": "t"},
    {"symbol": "EWY", "ex_date": date(2026, 12, 16), "dividend_usd": 1.8, "hedge": "US500", "status": "estimated", "source": "t"},
]


def row(sym, t, bid, ask, bid_sz=10.0, ask_sz=10.0, index=None, ob_age=1.0, depth=None):
    r = {"ts": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "symbol": sym, "best_bid": bid, "best_ask": ask,
         "best_bid_size": bid_sz, "best_ask_size": ask_sz, "ob_age_secs": ob_age,
         "index_price": str(index if index is not None else (bid + ask) / 2), "_t": t}
    if depth is not None:
        r["cum_depth_usd"] = depth
    return r


def series(sym, start, n, **kw):
    return [row(sym, start + timedelta(minutes=i), **kw) for i in range(n)]


def base_rows(spy_depth=None):
    t0 = NOW.replace(minute=23, second=0)
    rows = []
    # SPY: 1 bps spread, L1 $6.6k each side, bid depth within 5 bps $40k
    dep = spy_depth if spy_depth is not None else {"2": {"bid": 15000.0, "ask": 16000.0}, "3": {"bid": 25000.0, "ask": 26000.0},
                                                    "5": {"bid": 40000.0, "ask": 50000.0}, "10": {"bid": 90000.0, "ask": 80000.0},
                                                    "20": {"bid": 100000.0, "ask": 100000.0}, "50": {"bid": 4e6, "ask": 5e6}}
    rows += series("SPY", t0, 5, bid=659.97, ask=660.03, depth=dep)
    rows += series("US500", t0, 5, bid=6599.9, ask=6600.1, bid_sz=20, ask_sz=20)
    # IBM: 3 bps spread, L1 ~$490, depth within 10 bps $3k
    rows += series("IBM", t0, 5, bid=245.96, ask=246.04, bid_sz=2.0, ask_sz=2.0,
                   depth={"5": {"bid": 1500.0, "ask": 1400.0}, "10": {"bid": 3000.0, "ask": 3100.0},
                          "20": {"bid": 5000.0, "ask": 5000.0}, "50": {"bid": 9000.0, "ask": 9000.0}})
    return rows


def prev_rows(spy_close=660.0, us500_close=6600.0, ibm_close=246.0):
    tp = datetime(2026, 9, 17, 19, 59, tzinfo=timezone.utc)
    return [row("SPY", tp, spy_close - 0.03, spy_close + 0.03, index=spy_close),
            row("US500", tp, us500_close - 0.1, us500_close + 0.1, index=us500_close),
            row("IBM", tp, ibm_close - 0.04, ibm_close + 0.04, index=ibm_close)]


class CalendarTests(unittest.TestCase):
    def test_calendar_groups_declared_dates_only(self):
        cal = xp.calendar_from_events(EVENTS)
        self.assertEqual([e["decision_key"] for e in cal["entries"]], ["2026-09-18"])
        e = cal["entries"][0]
        self.assertEqual(e["decision_at"], "2026-09-18T13:29:00Z")
        self.assertEqual(e["flatten_at"], "2026-09-18T13:36:00Z")
        self.assertTrue(cal["calendar_version"].startswith("exdiv-948-v1-"))
        # deterministic: same events -> same version; different -> different
        self.assertEqual(cal["calendar_version"], xp.calendar_from_events(EVENTS)["calendar_version"])
        more = EVENTS + [dict(EVENTS[0], ex_date=date(2026, 12, 18))]
        self.assertNotEqual(cal["calendar_version"], xp.calendar_from_events(more)["calendar_version"])

    def test_committed_calendar_matches_events(self):
        repo = os.path.dirname(HERE)
        ev = os.path.join(repo, "configs", "book", "exdiv-events.json")
        cal = os.path.join(repo, "configs", "book", "exdiv-lighter.calendar.json")
        self.assertEqual(open(cal).read(), xp.render_calendar(xp.calendar_from_events(xp.load_events(ev))))
        # every declared event symbol/hedge is in the deployed universe
        uni = xp.universe_from_config(os.path.join(repo, "configs", "book", "exdiv-lighter.yaml"))
        for e in xp.load_events(ev):
            self.assertIn(e["symbol"], uni)
            if e["hedge"]:
                self.assertIn(e["hedge"], uni)

    def test_events_validation_rejects_bad_rows(self):
        def write(events):
            fd, p = tempfile.mkstemp(suffix=".json")
            with os.fdopen(fd, "w") as f:
                json.dump({"schema_version": 1, "events": events}, f)
            return p
        good = {"symbol": "SPY", "ex_date": "2026-09-18", "dividend_usd": 1.83, "hedge": "US500",
                "status": "declared", "source": "x"}
        self.assertEqual(len(xp.load_events(write([good]))), 1)
        for bad in (dict(good, flatten_at="x"),                 # unknown key
                    {k: v for k, v in good.items() if k != "source"},
                    dict(good, status="confirmed"),
                    dict(good, dividend_usd=0),
                    dict(good, dividend_usd="1.83"),
                    dict(good, ex_date="2026-9-18"),
                    dict(good, hedge="SPY"),
                    dict(good, symbol="spy")):
            with self.assertRaises(SystemExit, msg=bad):
                xp.load_events(write([bad]))
        with self.assertRaises(SystemExit):  # duplicate (symbol, date)
            xp.load_events(write([good, dict(good, dividend_usd=2.0)]))


class MarketTimezoneTests(unittest.TestCase):
    """The open is 13:30 UTC only under daylight saving (Codex P2 on #288)."""

    def test_calendar_entries_follow_the_new_york_open(self):
        summer = [dict(EVENTS[0], ex_date=date(2026, 9, 18))]
        winter = [dict(EVENTS[0], symbol="EWY", ex_date=date(2026, 12, 16))]
        e_s = xp.calendar_from_events(summer)["entries"][0]
        e_w = xp.calendar_from_events(winter)["entries"][0]
        self.assertEqual((e_s["decision_at"], e_s["flatten_at"]),
                         ("2026-09-18T13:29:00Z", "2026-09-18T13:36:00Z"))
        self.assertEqual((e_w["decision_at"], e_w["flatten_at"]),
                         ("2026-12-16T14:29:00Z", "2026-12-16T14:36:00Z"))

    def test_prev_close_lookup_follows_the_new_york_close(self):
        # NY 15:59 is 19:59 UTC in September and 20:59 UTC in December; a
        # fixed 19:59 would silently disable the landed-step gate in winter.
        sept = datetime(2026, 9, 17, 19, 59, tzinfo=timezone.utc)
        dec = datetime(2026, 12, 15, 20, 59, tzinfo=timezone.utc)
        self.assertEqual(xp.close_index([row("SPY", sept, 659.0, 661.0, index=660.0)],
                                        "SPY", date(2026, 9, 17)), 660.0)
        self.assertEqual(xp.close_index([row("EWY", dec, 59.0, 61.0, index=60.0)],
                                        "EWY", date(2026, 12, 15)), 60.0)
        # the summer-shaped row is NOT accepted for a winter date
        self.assertIsNone(xp.close_index([row("EWY", datetime(2026, 12, 15, 19, 59, tzinfo=timezone.utc),
                                              59.0, 61.0, index=60.0)], "EWY", date(2026, 12, 15)))

    def test_window_start_follows_the_new_york_lookback(self):
        winter_d = date(2026, 12, 16)
        start = xp.market_instant(winter_d, xp.LOOKBACK_START_NY)
        self.assertEqual(start.strftime("%H:%M"), "14:23")
        self.assertEqual(xp.decision_at(winter_d).strftime("%H:%M"), "14:29")


class SignalTests(unittest.TestCase):
    def test_hedged_and_unhedged_legs_sized_from_depth(self):
        sig = xp.build_signal(EVENTS, D, base_rows(), prev_rows(), NOW)
        w = sig["weights"]
        # SPY: dividend 27.7 bps -> budget 5.5 bps -> band 5 -> 0.5 * $40k = $20k -> capped $2k
        self.assertAlmostEqual(w["SPY"], -2000 / 8000, places=6)
        self.assertAlmostEqual(w["US500"], 2000 / 8000, places=6)
        # IBM: 68.7 bps -> budget 13.7 -> band 10 -> 0.5 * $3k = $1,500, unhedged
        self.assertAlmostEqual(w["IBM"], -1500 / 8000, places=6)
        ev = {e["symbol"]: e for e in sig["meta"]["events"]}
        self.assertEqual(ev["SPY"]["size_rule"], "depth_5bps")
        self.assertEqual(ev["IBM"]["size_rule"], "depth_10bps")
        self.assertIsNone(ev["SPY"]["skip"])
        self.assertEqual(sig["decision_key"], "2026-09-18")
        self.assertEqual(sig["as_of"], "2026-09-18T13:27:00Z")   # last row used, never after now
        self.assertEqual(sig["generated_at"], "2026-09-18T13:27:30Z")
        self.assertEqual(sig["payload_sha256"], bsf.payload_sha256(
            xp.PRODUCER_ID, "2026-09-18T13:27:00Z", "2026-09-18", w))
        self.assertEqual(sig["meta"]["scale"], 1.0)
        self.assertEqual(sig["meta"]["net_usd"], -1500.0)

    def test_skip_gates(self):
        # wide spread on the event leg
        rows = [r for r in base_rows() if r["symbol"] != "SPY"] + series("SPY", NOW.replace(minute=23, second=0), 5, bid=657.0, ask=663.0)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertEqual(sig["weights"], {})
        self.assertEqual(sig["meta"]["events"][0]["skip"], "spread_gt_30bps")
        # thin L1
        rows = [r for r in base_rows() if r["symbol"] != "SPY"] + series("SPY", NOW.replace(minute=23, second=0), 5, bid=659.97, ask=660.03, bid_sz=0.1, ask_sz=0.1)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertEqual(sig["meta"]["events"][0]["skip"], "l1_lt_200usd")
        # stale book (ob_age) -> no fresh rows
        rows = [r for r in base_rows() if r["symbol"] != "SPY"] + series("SPY", NOW.replace(minute=23, second=0), 5, bid=659.97, ask=660.03, ob_age=500)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertEqual(sig["meta"]["events"][0]["skip"], "no_fresh_book")
        # gap already landed: SPY index down 40 bps vs T-1 close while US500 flat
        rows = [r for r in base_rows() if r["symbol"] != "SPY"] + series("SPY", NOW.replace(minute=23, second=0), 5, bid=657.33, ask=657.39, index=657.36)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertEqual(sig["meta"]["events"][0]["skip"], "gap_already_landed")
        # same move on the hedge too (market-wide, not the dividend) -> not landed
        rows = [r for r in rows if r["symbol"] != "US500"] + series("US500", NOW.replace(minute=23, second=0), 5, bid=6573.5, ask=6573.7, bid_sz=20, ask_sz=20, index=6573.6)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertIsNone(sig["meta"]["events"][0]["skip"])
        # hedge leg wide -> skip the event
        rows = [r for r in base_rows() if r["symbol"] != "US500"] + series("US500", NOW.replace(minute=23, second=0), 5, bid=6570.0, ask=6630.0, bid_sz=20, ask_sz=20)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertEqual(sig["meta"]["events"][0]["skip"], "hedge_spread_gt_30bps")

    def test_l1_fallback_when_no_depth_or_small_budget(self):
        rows = base_rows(spy_depth={})          # depth field present but empty -> no band data
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        e = sig["meta"]["events"][0]
        self.assertEqual(e["size_rule"], "l1_fallback_no_depth")
        self.assertAlmostEqual(e["notional_usd"], round(0.25 * 10 * 660.0, 2), places=1)
        # tiny dividend (budget < 5 bps): 25 % of L1 rule
        small = [dict(EVENTS[0], dividend_usd=0.10)]
        sig = xp.build_signal(small, D, base_rows(), prev_rows(), NOW)
        self.assertEqual(sig["meta"]["events"][0]["size_rule"], "l1_fallback_budget_lt_5bps")
        # touch outside the band (20 bps spread, 5 bps band): L1 rule again
        wide = {"2": {"bid": 0.0, "ask": 0.0}, "3": {"bid": 0.0, "ask": 0.0}, "5": {"bid": 0.0, "ask": 0.0},
                "10": {"bid": 0.0, "ask": 0.0}, "20": {"bid": 6000.0, "ask": 6000.0}, "50": {"bid": 9000.0, "ask": 9000.0}}
        rows = [r for r in base_rows() if r["symbol"] != "SPY"] + series("SPY", NOW.replace(minute=23, second=0), 5, bid=659.34, ask=660.66, depth=wide)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        e = sig["meta"]["events"][0]
        self.assertIsNone(e["skip"])
        self.assertEqual(e["depth_band_bps"], 5)
        self.assertEqual(e["size_rule"], "l1_fallback_touch_outside_band")
        # depth path too small to trade -> skip (synthetic: band holds $60)
        tiny = dict(wide, **{"5": {"bid": 60.0, "ask": 60.0}})
        rows = [r for r in base_rows() if r["symbol"] != "SPY"] + series("SPY", NOW.replace(minute=23, second=0), 5, bid=659.97, ask=660.03, depth=tiny)
        sig = xp.build_signal(EVENTS[:1], D, rows, prev_rows(), NOW)
        self.assertEqual(sig["meta"]["events"][0]["skip"], "too_small")

    def test_scale_down_keeps_hedge_ratio(self):
        evals = [{"symbol": "SPY", "hedge": "US500", "skip": None, "notional_usd": 2000.0},
                 {"symbol": "IWM", "hedge": "US500", "skip": None, "notional_usd": 2000.0},
                 {"symbol": "QQQ", "hedge": "US100", "skip": None, "notional_usd": 2000.0},
                 {"symbol": "IBM", "hedge": None, "skip": None, "notional_usd": 2000.0},
                 {"symbol": "TSM", "hedge": None, "skip": "spread_gt_30bps", "notional_usd": 0.0}]
        w, agg = xp.weights_from_evals(evals, 8000.0)
        self.assertLessEqual(sum(abs(v) for v in w.values()), 1.0 + 1e-9)
        self.assertLessEqual(max(abs(v) for v in w.values()), xp.MAX_SYMBOL_WEIGHT + 1e-9)
        self.assertAlmostEqual(w["US500"], -(w["SPY"] + w["IWM"]), places=9)
        self.assertNotIn("TSM", w)
        self.assertLess(agg["scale"], 1.0)

    def test_late_run_is_bounded_at_the_decision_instant(self):
        """A delayed cron or a retry inside the grace window must not size
        from post-decision prices, nor relabel as_of to hide it (Codex P1)."""
        on_time = xp.build_signal(EVENTS[:1], D, base_rows(), prev_rows(), NOW)
        # Same day, but the book collapses right after the decision: rows at
        # 13:30-13:34 carry a fraction of the depth and a much wider spread.
        after = []
        for i in range(5):
            t = datetime(2026, 9, 18, 13, 30 + i, tzinfo=timezone.utc)
            after.append(row("SPY", t, 655.0, 665.0, bid_sz=0.05, ask_sz=0.05,
                             depth={"2": {"bid": 10.0, "ask": 10.0}, "3": {"bid": 10.0, "ask": 10.0},
                                    "5": {"bid": 10.0, "ask": 10.0}, "10": {"bid": 10.0, "ask": 10.0},
                                    "20": {"bid": 10.0, "ask": 10.0}, "50": {"bid": 10.0, "ask": 10.0}}))
            after.append(row("US500", t, 6599.9, 6600.1, bid_sz=20, ask_sz=20))
        late_now = datetime(2026, 9, 18, 13, 35, tzinfo=timezone.utc)
        late = xp.build_signal(EVENTS[:1], D, base_rows() + after, prev_rows(), late_now)
        # The post-decision rows are invisible: same weights as the on-time run.
        self.assertEqual(late["weights"], on_time["weights"])
        self.assertEqual(late["meta"]["events"][0]["spread_bps"], on_time["meta"]["events"][0]["spread_bps"])
        # as_of is a real observation time, never after the decision.
        self.assertLessEqual(late["as_of"], "2026-09-18T13:29:00Z")
        self.assertEqual(late["meta"]["input_cutoff"], "2026-09-18T13:29:00Z")
        self.assertEqual(late["meta"]["decision_at"], "2026-09-18T13:29:00Z")
        self.assertEqual(late["generated_at"], "2026-09-18T13:35:00Z")
        # Mutation check: the assertions above are only meaningful if those
        # post-decision rows would otherwise have changed the outcome. With
        # the cutoff moved back to "now" (the pre-fix behaviour) they are
        # visible, and they do change it -- 30 bps median spread trips the
        # skip gate that the bounded run passes.
        rows = base_rows() + after
        dec = xp.decision_at(D)
        bounded_win = xp.window_rows(rows, "SPY", dec)
        unbounded_win = xp.window_rows(rows, "SPY", late_now)
        self.assertEqual(len(bounded_win), 5)          # the whole 09:23-09:29 window
        self.assertGreater(len(unbounded_win), len(bounded_win))
        self.assertTrue(any(r["_t"] > dec for r in unbounded_win))
        self.assertTrue(all(r["_t"] <= dec for r in bounded_win))
        self.assertGreater(
            st.median([xp.spread_bps(r) for r in unbounded_win]),
            xp.SKIP_SPREAD_BPS,
        )
        self.assertLess(
            st.median([xp.spread_bps(r) for r in bounded_win]),
            xp.SKIP_SPREAD_BPS,
        )
        # A run with only post-decision rows has nothing admissible at all.
        wide_only = xp.build_signal(EVENTS[:1], D, after, prev_rows(), late_now)
        self.assertEqual(wide_only["meta"]["events"][0]["skip"], "no_fresh_book")

    def test_aggregate_net_cap_scales_unhedged_events_together(self):
        """Two $2,000 unhedged shorts are inside the gross and symbol caps
        but breach max_net_usd, which would make the runtime reject the
        whole plan and trade neither event (Codex P2)."""
        evals = [{"symbol": "IBM", "hedge": None, "skip": None, "notional_usd": 2000.0},
                 {"symbol": "TSM", "hedge": None, "skip": None, "notional_usd": 2000.0}]
        w, agg = xp.weights_from_evals(evals, 8000.0, 2200.0)
        self.assertLessEqual(abs(agg["net_usd"]), 2200.0 + 1e-6)
        self.assertAlmostEqual(agg["net_usd"], -2200.0, places=4)
        self.assertAlmostEqual(w["IBM"], w["TSM"], places=9)      # scaled together
        self.assertLess(agg["scale"], 1.0)
        # A hedged pair has no net exposure, so the cap never shrinks it.
        hedged = [{"symbol": "SPY", "hedge": "US500", "skip": None, "notional_usd": 2000.0}]
        w2, agg2 = xp.weights_from_evals(hedged, 8000.0, 2200.0)
        self.assertEqual(agg2["scale"], 1.0)
        self.assertAlmostEqual(agg2["net_usd"], 0.0, places=6)

    def test_producer_constants_match_the_deployed_config(self):
        """The producer mirrors the runtime caps; drift would produce files
        the runtime rejects."""
        import re
        cfg = open(os.path.join(os.path.dirname(HERE), "configs", "book", "exdiv-lighter.yaml")).read()

        def num(section, key):
            block = re.search(rf"^{section}:\s*$(.*?)^[a-z_]+:", cfg, re.M | re.S).group(1)
            return float(re.search(rf"^\s+{key}:\s*([0-9.]+)", block, re.M).group(1))

        self.assertEqual(xp.GROSS_USD, num("sizing", "gross_notional_usd"))
        self.assertEqual(xp.MAX_SYMBOL_WEIGHT, num("sizing", "max_symbol_weight"))
        self.assertEqual(xp.MAX_NET_USD, num("sizing", "max_net_usd"))
        self.assertEqual(xp.PRODUCER_ID, re.search(r"^\s+producer_id:\s*(\S+)", cfg, re.M).group(1))
        # one leg at the cap must stay inside the per-symbol weight cap
        self.assertLessEqual(xp.MAX_LEG_USD / xp.GROSS_USD, xp.MAX_SYMBOL_WEIGHT + 1e-12)

    def test_no_event_or_no_rows(self):
        self.assertIsNone(xp.build_signal(EVENTS, date(2026, 9, 17), [], [], NOW))
        sig = xp.build_signal(EVENTS, D, [], [], NOW)      # event day, logger dead
        self.assertEqual(sig["weights"], {})
        self.assertEqual(sig["as_of"], sig["generated_at"])
        self.assertTrue(all(e["skip"] == "no_fresh_book" for e in sig["meta"]["events"]))

    def test_cli_calendar_check_and_signal(self):
        with tempfile.TemporaryDirectory() as td:
            ev = os.path.join(td, "events.json")
            with open(ev, "w") as f:
                json.dump({"schema_version": 1, "events": [
                    {"symbol": "SPY", "ex_date": "2026-09-18", "dividend_usd": 1.83, "hedge": "US500",
                     "status": "declared", "source": "t"}]}, f)
            cal = os.path.join(td, "cal.json")
            script = os.path.join(HERE, "exdiv_signal_producer.py")
            r = subprocess.run([sys.executable, script, "calendar", "--events", ev, "--out", cal, "--check"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 1, r.stderr)
            r = subprocess.run([sys.executable, script, "calendar", "--events", ev, "--out", cal], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            r = subprocess.run([sys.executable, script, "calendar", "--events", ev, "--out", cal, "--check"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(json.load(open(cal))["entries"][0]["decision_key"], "2026-09-18")
            # signal: logger dir with the synthetic rows
            logdir = os.path.join(td, "log")
            os.makedirs(logdir)
            for d, rows in ((D, base_rows()), (date(2026, 9, 17), prev_rows())):
                with open(os.path.join(logdir, f"orcl_{d.strftime('%Y%m%d')}.jsonl"), "w") as f:
                    for rr in rows:
                        f.write(json.dumps({k: v for k, v in rr.items() if k != "_t"}) + "\n")
            out = os.path.join(td, "signal.json")
            cfg = os.path.join(os.path.dirname(HERE), "configs", "book", "exdiv-lighter.yaml")
            r = subprocess.run([sys.executable, script, "signal", "--events", ev, "--out", out, "--config", cfg,
                                "--date", "2026-09-18", "--now", "2026-09-18T13:27:30Z", "--log-dir", logdir],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            sig = json.load(open(out))
            self.assertAlmostEqual(sig["weights"]["SPY"], -0.25)
            self.assertAlmostEqual(sig["weights"]["US500"], 0.25)
            # non-event day writes nothing
            r = subprocess.run([sys.executable, script, "signal", "--events", ev, "--out", out + ".2",
                                "--date", "2026-09-17", "--log-dir", logdir], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertFalse(os.path.exists(out + ".2"))
            # late run: writes with a warning by default, exit 3 with the flag
            r = subprocess.run([sys.executable, script, "signal", "--events", ev, "--out", out,
                                "--date", "2026-09-18", "--now", "2026-09-18T13:35:00Z", "--log-dir", logdir],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn("WARNING", r.stderr)
            r = subprocess.run([sys.executable, script, "signal", "--events", ev, "--out", out,
                                "--date", "2026-09-18", "--now", "2026-09-18T13:35:00Z", "--log-dir", logdir,
                                "--refuse-after-decision"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 3, r.stdout + r.stderr)
            # symbol outside the deployed universe -> refuse (exit 2)
            with open(ev, "w") as f:
                json.dump({"schema_version": 1, "events": [
                    {"symbol": "ZZZ", "ex_date": "2026-09-18", "dividend_usd": 1.0, "hedge": None,
                     "status": "declared", "source": "t"}]}, f)
            r = subprocess.run([sys.executable, script, "signal", "--events", ev, "--out", out, "--config", cfg,
                                "--date", "2026-09-18", "--log-dir", logdir], capture_output=True, text=True)
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
