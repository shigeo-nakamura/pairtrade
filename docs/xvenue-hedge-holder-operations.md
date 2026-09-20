# xvenue hedge holder — operations (bot-strategy#1046)

Lighter on Robinhood Chain **BTC long** + Lighter Core **BTC short**, equal
size, held for the Robinhood-chain weekly points drop. Binary:
`src/bin/xvenue_hedge_holder.rs` (module doc = design + invariants).

## Why it exists

The Robinhood-chain pairtrade arms (freq / b) bled ~$35/day for ~8.5 points
a week ($16–33/pt against an ~$8–16/pt LIT value). Funding on the two
Lighter deployments is identical to the hour (bot-strategy#1046 Phase 0),
so a long on RH hedged short on Core holds for ~nothing; the venue itself
recommends exactly this ("hedge your Robinhood Chain longs on Lighter
Core"). Whether the weekly drop rewards a *held* hedged long is the G0
question answered by the 2026-09-25 drop; this bot goes live only on that
readout (`HEDGE_LIVE_CONFIRM=1046-G0-PASSED`).

## Host layout (Tokyo `debot-robinhood-lighter`, `i-0095af4fe0efbc5dd`)

| path | what |
|---|---|
| `/opt/debot-hedge-holder/bin/xvenue_hedge_holder` | ARM64 binary (artifact of `ci-xvenue-hedge-holder.yml`) |
| `/opt/debot-hedge-holder/lib/libsigner.so` | same artifact |
| `/opt/debot-hedge-holder/hedge/` | `ARM` `DISARM` `KILL_SWITCH` `RISK_ACK` `state.json` `status.json` `events.jsonl` |
| `/opt/debot/scripts/debot-xvenue-hedge-holder.env` | credentials + parameters (`scripts/debot-xvenue-hedge-holder.env.example`) |
| `/opt/debot/scripts/debot-xvenue-hedge-holder.sh` | launcher (`scripts/`) |
| `/etc/systemd/system/debot-xvenue-hedge-holder.service` | unit (`deploy/`) |

Both legs' Lighter API keys must be encrypted under **this host's**
`ENCRYPTED_DATA_KEY`. The RH leg reuses the freq account (3209); the Core
leg is the old canary sub-account (281474976624819), whose keys were
encrypted under the Frankfurt data key and need re-encrypting first (same
procedure as the xsmom-695 secrets, bot-strategy#937).

Instance suffixes: `_RH` / `_CORE` on `LIGHTER_*`, `REST_ENDPOINT`,
`WEB_SOCKET_ENDPOINT` (the endpoint suffixing is new in this PR,
`src/config.rs`).

## Deploy (manual, like freq/b — no CI deploy step)

1. Download `xvenue-hedge-holder-arm64-<sha>` from the workflow run.
2. Copy `bin/xvenue_hedge_holder` and `lib/libsigner.so` to the host
   (S3 `debot/status/robinhood-lighter/` is writable by the instance role
   and works as a drop box; delete after).
3. Install the launcher, env file and unit; `systemctl daemon-reload`.
4. Start in DRY_RUN (default). Check the journal for
   `[CONFIG] bot=xvenue_hedge_holder dry_run=true ... fp=` and a
   `[STARTUP] mode=Off` line, then `touch hedge/ARM` and watch two
   `[DRY_RUN]` fills per tick until `status.json` shows
   `legs.long.qty == legs.short.qty == target_qty`.

## Going live (after G0 PASS)

1. Set `HEDGE_DRY_RUN=false` **and** `HEDGE_LIVE_CONFIRM=1046-G0-PASSED`,
   restart. A `state.json` armed under DRY_RUN is dropped to `Off` at this
   start (logged `[STARTUP] state.json was written with dry_run=true ...`),
   so nothing trades until the next ARM.
2. **`Off` never sends an order.** A position the venues already hold (the
   manual G0 hedge, or a live book whose `state.json` was lost) is left
   alone and logged `[OFF] venues hold ...`. `touch ARM` **adopts** it: the
   legs are topped up or trimmed to the new target from there (the manual
   entry price is then not in `events.jsonl`; the `arm` event records the
   adopted sizes). To start clean instead, close the manual position by
   hand first.
3. `touch ARM`. First tick: `[FILL] long Long BTC ...` then
   `[FILL] short Short BTC ...`, one clip each (the second leg is cut to
   what the first actually filled); the book completes over
   `ceil(target/clip)` ticks.

## Operator files

| file | effect |
|---|---|
| `ARM` (empty or `<usd>`) | build the book at `HEDGE_TARGET_NOTIONAL_USD` (or `<usd>`) per leg |
| `DISARM` | unwind both legs (short leg first), then `mode: Off` |
| `KILL_SWITCH` (present) | no growth; reductions and DISARM still run |
| `RISK_ACK` | clear a halt |

## Halts (`status.json.halt_reason`)

| reason | what happened | remedy |
|---|---|---|
| `net exposure ...` | legs unequal by > `HEDGE_NET_TOLERANCE_USD` for `HEDGE_NET_BREACH_TICKS` ticks (an IOC keeps failing on one venue) | check the failing venue / book; while halted the bot only *reduces* the larger leg; `RISK_ACK` |
| `leverage: ...` | a growth order this tick would exceed `HEDGE_MAX_LEVERAGE` × that venue's equity; nothing was sent (checked before the first leg) — also what fires after a liquidation | deposit, `RISK_ACK`, re-`ARM` |
| `liq_guard: ...` | a venue's headroom fell under `HEDGE_LIQ_GUARD_PCT`; **both legs were closed** | rebalance collateral, `RISK_ACK`, re-`ARM` |

A halt never leaves the book lopsided on purpose: the tick loop keeps
reducing the larger leg toward the smaller one while halted.

## Status (`status.json`, mirrored to `HEDGE_STATUS_S3_URI`)

The top level is the pairtrade-like shape debot-dashboard already renders:
`ts` / `updated_at`, `pnl_total` (= both venues' equity), `pnl_today`
(equity change since 00:00 UTC), `positions` (one per leg, symbol
`BTC (rh)` / `BTC (core)`), `kill_switch_active`, and the dashboard's
`subsidy` block (`deploy/subsidy-kpi.md` there): `units_total` = the long
account's live points since ARM (from the points collector's
`points_history.jsonl`, `HEDGE_POINTS_HISTORY_PATH`), `units_7d`,
`cost_total_usd` = −(equity change since ARM), `as_of_ts` = the newest
collector row. Absent until the book is armed with a points baseline.

The points collector also reads the short leg's own tallies on Lighter
Core (`--arm core-canary:<this env>:core`, rows with `instance: "core"`;
`robinhood-points-snapshot.service`). Those rows are for the readout —
whether the Core side earns anything for the hedge changes its cost per
point — and are not part of `subsidy` (the dashboard reads the long
account's rows by `account_index`). On Core `livePoints/total` answers
403 (WAF), so those rows carry `live_points_total: null` and the reason
under `errors`; the series to difference there is `total_points`
(`robinhood_points_daily.py --instance core --tally total_points`;
`last_week_points` is the latest drop's size, not a cumulative series). The hedge env must be group-readable by `ec2-user`
(`install_robinhood_points_snapshot.sh` does this).

Everything hedge-specific is under `hedge_holder`: `mode`,
`halted`/`halt_reason`, `target_qty`, `net_qty`/`net_usd`, `basis_bps`
(RH mark vs Core mark), `equity_total_usd`, `equity_at_arm_usd`,
`pnl_since_arm_usd`, `points_at_arm`, and per leg `qty`, `notional_usd`,
`mark`, `equity_usd`, `liq_headroom_pct`.

## Feed problems (`hedge_holder.feed_problem`)

While the two venues' marks differ by more than 2 % (one feed on a REST
fallback or a placeholder ticker) the bot sends nothing, keeps writing
`status.json` with `hedge_holder.feed_problem`, and still consumes
`DISARM` (acted on once the feed agrees again).

While a venue cannot be read at all (REST 5xx, WebSocket down — Lighter
Core answered 502/503 for ten minutes on 2026-09-20 11:02–11:12Z) the
tick is skipped the same way: no order, no guard evaluation, and
`status.json` is still written from the **last good snapshot** with
`feed_problem = "venue unreachable: …"`. `hedge_holder.snapshot_at`
dates that snapshot (the top-level `ts` is the write time), so the card
can show how old the marks and headroom figures are. `ARM`/`DISARM`
files are left in place and register on the next readable tick.
`feed_problem` is `null` on a healthy tick.

## Restart / stop

The unit does **not** close positions on stop; a delta-neutral book needs
no supervision while the process is down, and the venue funding on both
sides keeps netting. `DISARM` before stopping only if the book should go.
