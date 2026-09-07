# Ex-dividend gap bot — `exdiv-lighter` book instance

bot-strategy#948. The ex-dividend open-window short is **not a new binary**:
it is a `schedule.kind: calendar` instance of the book runtime
(`docs/book-runtime.md`, bot-strategy#937), decided 2026-09-07 on the issue.
This file is the "what runs where" companion for that instance, alongside
`docs/book-runtime-operations.md` (identity, secrets, start/stop, daily
checks — all of which apply unchanged). All times UTC.

Status: **prepared, not installed, not running.** Gate G1 (ETF capture
ratio ≥ 0.5 in ≥ 2 of the 3 September ETF events: IWM 09-15, SPY 09-18,
QQQ 09-21) decides whether the DRY_RUN instance is switched on at all.

## Mechanism and frozen design (summary; the issue is the source)

Lighter's stock/ETF perp index is the raw equity price with no dividend
adjustment, so on an ex-dividend date it steps down by the dividend at the
US cash open (the first minute or two of the session) with no T-1
pre-discount (bot-strategy#681, verified live on ORCL 2026-07-10 and the
August round). The bot shorts the event symbol **one minute before the
cash open**, hedged with the same notional long in `US500` (S&P 500 ETFs)
or `US100` (QQQ) — futures-derived indexes that do not step — and covers
both **six minutes after it**. Single stocks run unhedged. Entry and exit
are **time-only**; there is no price-based take-profit or stop. Everything
that decides *whether* and *how much* is in the producer.

Every instant is derived from the frozen XNYS session table
(`configs/engine-b/trading_calendar.json`, already diff-checked in CI
against its own generator by the Engine B A-7 freeze), never from
wall-clock UTC or weekday arithmetic. That covers all three ways a fixed
schedule goes wrong:

| | why a fixed schedule breaks | example |
|---|---|---|
| daylight saving | the open is 13:30 UTC only in summer | 2026-12-16 opens 14:30 UTC, so it decides at 14:29:00Z |
| market holidays | T-1 must be the previous *session* | 2026-09-07 is Labor Day, so T-1 for an 09-08 event is 09-04 |
| half days | the close moves, so the T-1 close lookup moves | 2026-11-27 closes 18:00 UTC, not 21:00 |

A declared event on a date the US market is closed, or outside the frozen
calendar's 2026-01-01..2027-12-31 range, is refused rather than scheduled.
Extending past 2027 means regenerating that calendar first.

## Files

| file | role |
|---|---|
| `configs/book/exdiv-events.json` | human-maintained calendar: one row per (symbol, ex_date) with `dividend_usd`, `hedge`, `status` (`declared` / `estimated`), `source`. Only `declared` rows are used |
| `configs/book/exdiv-lighter.calendar.json` | runtime calendar, **generated** from the events file (`decision_at 13:29:00Z`, `flatten_at 13:36:00Z`, one entry per date). CI fails if it is stale |
| `configs/book/exdiv-lighter.yaml` | runtime config (`fp=6ba59389b7ce` at 2026-09-07): universe = event symbols + `US500`/`US100`, gross $8,000 (weights are fractions of it, one leg ≤ 0.25 = $2,000), grace 120 s, `max_age_secs 300`, `require_dollar_neutral: false` (single stocks are unhedged by design), `max_net_usd 2200` |
| `scripts/exdiv_signal_producer.py` | `calendar` (events → runtime calendar, `--check` for CI) and `signal` (pre-open cron: gates + sizing → `signal.json`, optional S3 upload). Both derive every instant from `configs/engine-b/trading_calendar.json` (`--trading-calendar` overrides) |
| `scripts/test_exdiv_signal_producer.py` | unit tests (synthetic logger rows) |
| `deploy/book-runtime-exdiv-lighter.service` | runtime unit, PROM `127.0.0.1:9475`, status → `s3://debot-dashboard/debot/status/book-exdiv-lighter/` |
| `deploy/book-signal-fetch-exdiv-lighter.{service,timer}` | S3 → local signal fetch, `Mon..Fri 09:27:00–09:31:40 America/New_York every 20 s` (`AccuracySec=1s`, so it follows US daylight saving); nothing polls outside that span |
| `scripts/install_book_runtime.sh` | now also installs `BOOK_CALENDAR_SOURCE` (default `/opt/debot/configs/book/<instance>.calendar.json`) as `/opt/book-runtime/<instance>.calendar.json` — the service cannot read `/opt/debot` (`InaccessiblePaths`) |

Observation side (bot-strategy repo, `scripts/strategy_probes/exdiv_948/`,
runs from flat copies in `~/bot/scripts/` on the workstation): the Lighter
WS-only logger `lighter_exdiv_logger.py` (14 markets, 1-min rows with L1,
`index_price`, funding, and since 2026-09-07 `cum_depth_usd` = cumulative
resting notional within 2/3/5/10/20/50 bps of mid per side) and the
Hyperliquid `hl_exdiv_poller.py`. The producer reads the Lighter logger's
`orcl_YYYYMMDD.jsonl` files directly.

## Producer rules (the signal side of the frozen design)

Run about two minutes before the open on the ex-dividend date (13:27 UTC
in summer, 14:27 in winter — the cron below fires on both and exits
immediately on non-event days). For every declared event that day, using
the fresh rows (`ob_age_secs ≤ 90`) from seven minutes before the open up
to the decision instant:

1. **Skip gates** (issue design): median spread > 30 bps; median L1 (min of
   bid/ask notional) < $200; the step already landed before the open
   (event `index_price` move since the T-1 19:59 close, minus the hedge's —
   or `US500`'s — move, ≤ −0.5 × dividend bps); no fresh book; hedge leg
   spread > 30 bps.
2. **Size** (2026-09-07 comment, replaces the bare `25 % × L1`): slippage
   budget = 20 % of the dividend in bps; take the widest logged depth band
   inside the budget and use **half the median cumulative bid depth** in
   it; cap $2,000; drop under $50. If no band fits (budget < 2 bps), or the
   band holds no depth (half-spread wider than the band, or rows without
   the field), the frozen `25 % × L1` rule applies instead.
3. Hedge = same notional, opposite sign. Several events on one day are
   summed; if the day would breach `sum |w| ≤ 1` or a symbol cap every leg
   is scaled down together so the hedge ratios survive.
4. Write `signal.json` (schema v1, `payload_sha256`) **even if every event
   was skipped** — an empty map is the runtime's valid "go flat", so the
   ledger keeps a `decision` row with the per-event diagnostics in `meta`
   (`spread_bps`, `l1_usd`, `dividend_bps`, `slippage_budget_bps`,
   `depth_band_bps`, `cum_bid_depth_usd`, `size_rule`, `skip`,
   `premarket_adj_move_bps`, `expected_net_bps`). No event on the date →
   nothing is written.

Inputs are always bounded at the decision instant, never at wall-clock
"now": a delayed run reads exactly the rows an on-time run would have
read, so it cannot size from post-open prices. On top of that, **a run
that starts after the decision refuses outright (exit 3)** — the runtime's
120 s grace window would otherwise let a late file open the position at or
after the open, past the very step the strategy exists to capture.
`--allow-after-decision` overrides that for offline regeneration (replay,
backfill) and must never appear in the cron. `as_of` = the last row
actually used (never after the decision, so never look-ahead);
`decision_key` = the date, matching the calendar entry; `meta` records
`decision_at`, `input_cutoff`, `session_open` and `prev_session`.

## Workstation cron (operator adds; agents do not edit crontab)

The workstation runs on UTC, so both possible open times get a line and
whichever one is not the market's 09:27 simply finds no event window:

```
27 13,14 * * 1-5 python3 $HOME/bot/.worktrees/pairtrade-master/scripts/exdiv_signal_producer.py signal --events $HOME/bot/.worktrees/pairtrade-master/configs/book/exdiv-events.json --config $HOME/bot/.worktrees/pairtrade-master/configs/book/exdiv-lighter.yaml --verify-host-status s3://debot-dashboard/debot/status/book-exdiv-lighter/status.json --out $HOME/bot/logs/exdiv_948/signal.json --s3-uri s3://debot-dashboard/debot/book/exdiv-lighter/signal.json >> $HOME/bot/logs/exdiv_948/producer.log 2>&1
```

`--verify-host-status` reads the running instance's published status and
refuses (exit 4) when it has not scheduled today's decision — see
"Calendar updates" below. An unreadable status only warns, so a transient
S3 error cannot block a legitimate publish.

(The off-hour run is harmless: on a non-event day it exits 0 having
written nothing, and on an event day the wrong-hour line is after the
decision and exits 3 without writing. If you prefer a single line, set
`CRON_TZ=America/New_York` on the crontab and use `27 9 * * 1-5`.)

Exit codes: 0 wrote (or nothing to do), 2 config/universe refusal, 3 the
run started after the decision, 4 the running instance has not scheduled
this decision.

It exits 0 with "no declared ex-dividend event" on every other day. The
fetch timer on the host polls S3 from 09:27:00 NY, so the upload has ~90 s
of slack before the 09:29:00 decision; the 120 s grace closes the window
two minutes later (entering after the step has landed is pointless, so a
late signal is `skipped`, not applied).

## Calendar updates (after the instance is installed)

**A regenerated calendar does not reach the running process on its own.**
`deploy-configs.yml` syncs it to `/opt/debot/configs/book/`, but the
service reads `/opt/book-runtime/exdiv-lighter.calendar.json` and cannot
see `/opt/debot` (`InaccessiblePaths`), and it loads that file once at
startup. So whenever `configs/book/exdiv-events.json` changes — a new
event, or EWY going from `estimated` to `declared` — the new decision date
is scheduled only after the instance is reinstalled **and restarted**.
This is the same deploy-is-not-restart rule as bot-strategy#580.

After the config deploy lands:

```
# 1. promote the new calendar into the runtime's own directory
sudo env BOOK_INSTANCE=exdiv-lighter \
     BOOK_BINARY_SOURCE=/opt/book-runtime/bin/book_runtime \
     BOOK_LIBSIGNER_SOURCE=/opt/book-runtime/lib/libsigner.so \
     bash /opt/debot/scripts/install_book_runtime.sh

# 2. restart so it loads (safe only while flat -- outside 09:29-09:36 NY
#    on an event day the book is always flat by design)
sudo systemctl restart book-runtime-exdiv-lighter

# 3. verify the new date is actually scheduled
sudo journalctl -u book-runtime-exdiv-lighter --since '-5min' --no-pager | grep '\[CONFIG\]'
jq '.book.next_decision_key, .book.next_decision_at' /var/lib/book-runtime/exdiv-lighter/status.json
```

Step 3 is the check that matters: `next_decision_key` must be the next
declared ex-dividend date. The producer performs the same check remotely
before every publish (`--verify-host-status`, in the cron above) and
refuses with exit 4 rather than uploading a signal for a decision the
runtime will never open a window for.

## Host install (only after G1 passes)

The Robinhood-host bootstrap in `ci.yml` / `deploy-configs.yml` installs
**only** `xsmom-695` on purpose; this instance is installed by hand once:

```
sudo env BOOK_INSTANCE=exdiv-lighter \
     BOOK_BINARY_SOURCE=/opt/book-runtime/bin/book_runtime \
     BOOK_LIBSIGNER_SOURCE=/opt/book-runtime/lib/libsigner.so \
     bash /opt/debot/scripts/install_book_runtime.sh
```

(config, calendar, units and fetch script come from the synced
`/opt/debot` tree; the binary/signer are the ones the xsmom install
already validated). Then the secrets file
`/etc/book-runtime/exdiv-lighter-secrets.env` (same keys and permissions as
xsmom-695 — a **separate** Lighter account from xsmom-695 is not required
for DRY_RUN but is for live, since two instances on one account would each
adopt the other's positions), `systemctl enable --now
book-signal-fetch-exdiv-lighter.timer`, `systemctl start
book-runtime-exdiv-lighter`, and the standard post-start checks: `[CONFIG]
… fp=6ba59389b7ce`, `status.json` `next_decision` = the next calendar
entry, no `[ADOPT]` rows.

## What to check after each event

- Ledger: one `decision` row for the date (`outcome=applied` with the
  producer's `signal_sha256`, or `skipped:window_missed` if nothing was
  fetched in time), `fill` rows at 13:29 and a `flatten` at 13:36, then a
  `rebalance_summary`.
- `pnl.jsonl` exit rows give the paper realized per leg; the capture ratio
  is still computed by `exdiv_readout.py` from the logger (control-adjusted
  index step), not from paper PnL.
- Producer log: the per-event line
  `[SPY:$1500.0, QQQ:l1_lt_200usd, …]` says what was sized or why it was
  skipped.

## Not done here (by decision)

- **Hyperliquid instance (`exdiv-hl`)**: dex-connector v4.7.20's
  Hyperliquid `create_order_taker_ioc` is spot-only, so an HL perp IOC
  path is the one real code cost of the HL leg. It is deferred until the
  2026-09-16 TSM readout shows the same index step on `xyz:TSM`; without
  that, HL is observation only.
- **Live**: needs G1 (September ETF events) and G2 (ETF L1 median 13:00–
  14:00 ≥ $2,000), the `BOOK_CONFIRM_LIVE` flow in
  `docs/book-runtime-operations.md`, and its own explicit approval.

## Unit delivery

`deploy-configs.yml` uploads the three `exdiv-lighter` units to S3 and
copies them to `/opt/debot/deploy/` on the Robinhood host, which is what
`install_book_runtime.sh` reads by default. They are **delivered only** —
CI never installs or starts this instance, so the manual install above has
its units without the runtime appearing on the host before G1.
