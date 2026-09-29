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

## Several symbols (option C, bot-strategy#1046 2026-09-25)

`HEDGE_SYMBOLS` lists every hedged market with the venue's maintenance
margin for it, primary first:

```bash
export HEDGE_SYMBOLS="BTC:1.2,META:3,AMZN:6,GOOGL:3"
```

The MMR is `maintenance_margin_fraction / 100` from
`/api/v1/orderBookDetails` (BTC/ETH 120 → 1.2; META/GOOGL/AAPL/TSLA/NVDA
300 → 3; AMZN/AMD/MU/INTC 600 → 6 as of 2026-09-25) — required per symbol,
because the guards weight each book's notional by it. Unset,
`HEDGE_SYMBOL` + `HEDGE_MMR_PCT` behave exactly as before (same config
fingerprint, same state.json; a one-book state.json from an older binary
becomes the primary symbol's book).

Each symbol is its own book (own target, own mode, own net-exposure
counter), all on the same two accounts. **Margin is per account**: Lighter
cross-margins every position, so

- `liq_headroom_pct` = `(equity − Σ notional_i × mmr_i) / Σ notional_i`
  over every hedged book on that venue; under `HEDGE_LIQ_GUARD_PCT` →
  **every armed book is closed** and the bot halts;
- the leverage guard adds up every growth order of the tick, across
  symbols, before sending any of them.

Positions on markets NOT in `HEDGE_SYMBOLS` are invisible to both guards —
keep the accounts to hedged markets only, or leave margin for them.
`HEDGE_MAX_NOTIONAL_USD` caps each symbol's per-leg notional.

Status: the single-symbol fields the card reads (`target_qty`, `net_*`,
`basis_bps`, `legs.*.qty`/`mark`) describe the primary symbol;
`legs.*.notional_usd` / `equity_usd` / `liq_headroom_pct` are the account
figures the guards use; `hedge_holder.books.<SYM>` has every book's own
target / held sizes / net / basis.

## Short leg on Arcus Perps (bot-strategy#1080)

Either leg can run on Arcus Perps: `HEDGE_SHORT_VENUE=arcus` (or
`HEDGE_LONG_VENUE`), default `lighter`. The purpose is RH-Lighter long /
Arcus short: the RH long keeps earning RH points (they come from held
hedged OI; the Core short earned 0), and the short leg can earn Arcus
points. An env without the venue variables runs exactly as before, with the
same config fingerprint.

**Env** (see the commented block at the end of
`scripts/debot-xvenue-hedge-holder.env.example`). Every name takes the
`_<INSTANCE>` suffix of `HEDGE_SHORT_INSTANCE`.

| variable | meaning |
|---|---|
| `HEDGE_SHORT_VENUE=arcus` | short leg on Arcus |
| `ARCUS_ADDRESS` | master wallet that owns the API key (required live) |
| `ARCUS_ACCOUNT_INDEX` | subaccount 0-9 (default 0) |
| `ARCUS_API_PRIVATE_KEY` | Ed25519 seed hex, KMS-encrypted (`ENCRYPTED_DATA_KEY`) — or `ARCUS_PLAIN_API_PRIVATE_KEY` for testing |
| `ARCUS_API_KEY` | public key hex, optional cross-check |
| `ARCUS_REST_ENDPOINT` / `ARCUS_WEBSOCKET_ENDPOINT` | default mainnet; testnet `api.testnet.arcus.xyz` |
| `HEDGE_SYMBOLS=BTC:1.2/1.667` | per-leg MMR `LONG/SHORT` (Arcus BTC 1.667 %, stocks 6.667 %; off-hours stock IMF is 15 %) |
| `HEDGE_FILL_WAIT_SECS=3,6,10` | Arcus acks orders asynchronously (202); re-read positions longer |
| `HEDGE_UNCERTAIN_GRACE_SECS=60` | (default) seconds an uncertain order may stay unsettled before the bot **halts** for the operator (it is never released on time alone) |

**Settling an Arcus IOC.** An Arcus order counts as final only when it has left the open orders, is completely filled or its unfilled rest shows as canceled, and its reported fills agree with the position change. A partly visible fill never sizes the paired leg. If that has not happened by the last `HEDGE_FILL_WAIT_SECS` wait, the order is marked **uncertain**: `state.json` `uncertain`, `status.json` `hedge_holder.uncertain_orders`, event `uncertain`. The same happens when the connector answers a submission with `ReconciliationRequired`. **Nothing more is sent on that symbol** until a later tick sees **terminal evidence**: on Arcus, its own fills or cancel agreeing with the position change; on Lighter (IOC final on ack), a known order id that is no longer open (event `uncertain_resolved`). The book is then re-planned from the real positions on the next tick, so a late fill is levelled, never re-sent. Time alone **never** releases it. After `HEDGE_UNCERTAIN_GRACE_SECS` without evidence (or at once, if the leg's venue/instance changed since the order was sent — its evidence would come from the wrong account) the bot **halts** (event `uncertain_escalated`). The operator checks that venue account by hand, levels it if needed, then touches `RISK_ACK`, which clears the halt **and** the uncertain entries. Lighter keeps the position-delta settle, because its IOC is final on ack.
| `HEDGE_ARCUS_LIVE_CONFIRM=1080` | second live gate for any Arcus leg, on top of `HEDGE_LIVE_CONFIRM` |

A dry-run Arcus connector never loads the signer key (it only reads public
market data). A live one refuses to start without `ARCUS_ADDRESS` and a
private key. Status and events carry `exchange` (`lighter` / `arcus`) next
to the existing `instance` / `venue` fields.

Differences from Core to keep in mind:

- **Funding is not identical** (Arcus BTC median +0.00125 %/h vs the
  Lighter +0.0012 %/h cap). The pair's carry is real and must be measured,
  not assumed to be zero.
- **Oracles differ.** Basis and liquidation timing on the two accounts are
  less correlated than RH vs Core. Keep `HEDGE_LIQ_GUARD_PCT` at least as
  wide.
- **BTC first.** Arcus single-stock books are thin ($10k taker impact of
  7–8 bp) and stock margin jumps off-hours.

### Testnet smoke (owner, before any mainnet move)

1. Create a **testnet** API key (testnet.arcus.xyz/api-keys) and fund it
   with **Testnet Deposit**.
2. Run the dex-connector ignored smoke tests first (they place a post-only
   order, cancel it, arm/disarm the dead man's switch, then IOC + close —
   connector-level; the holder's roll itself only sends IOCs):
   `ARCUS_TESTNET_ADDRESS=0x… ARCUS_TESTNET_API_PRIVATE_KEY_HEX=… cargo test --features arcus-sdk -- --ignored arcus`
   in dex-connector.
3. Then run the holder in DRY_RUN against testnet, with the short leg on
   Arcus (`ARCUS_*_ENDPOINT` = testnet) and the long leg on the usual RH
   instance. Check `[CONFIG] ... short=arcus:<instance>` and that marks and
   `basis_bps` look sane.

### Migration outline (mainnet, owner-run; keep the RH long open)

**`DISARM` closes BOTH legs. Do not use it for this move.** The goal is to
swap the short leg while the RH long stays open:

1. Pre-fund Arcus. The short needs its full notional at the chosen
   leverage + guard, sized on **initial** margin plus a buffer.
2. Open the Arcus short by hand (UI, taker or maker) at the held RH-long
   size, then **close the Core short by hand**. Do it close in time: the
   gap is unhedged exposure. The bot never trades the old Core short once
   the short venue is switched, and it stays on that account until closed.
3. Stop the bot. In the env: `HEDGE_SHORT_VENUE=arcus`,
   `HEDGE_SHORT_INSTANCE=<arcus instance>`, the `ARCUS_*` variables, the
   `LONG/SHORT` MMR, `HEDGE_FILL_WAIT_SECS`, and
   `HEDGE_ARCUS_LIVE_CONFIRM=1080`.
4. Start. The config fingerprint changes; `state.json` is kept, so a book
   that was `On` stays `On` at its `target_qty` and is re-read from the
   venues. With both legs already at the target, no order is sent. If the
   Arcus short was **not** opened by hand, the bot builds it itself (IOC
   clips, net-exposure rules as usual): this is a valid alternative to
   step 2's manual open, at taker cost. If the book was `Off`/`Exited`,
   **ARM with the held size** (`BTC <usd>` = held qty × mark + 1) to adopt
   the pair without an order. Check `status.json`:
   `legs.short.exchange == "arcus"`, net ≈ 0, headroom on both accounts.

## Operator files

| file | effect |
|---|---|
| `ARM`, one symbol (empty or `<usd>`) | build the book at `HEDGE_TARGET_NOTIONAL_USD` (or `<usd>`) per leg |
| `ARM`, several symbols (`SYMBOL USD` per line) | (re)arm each listed book at `USD` per leg; books not listed are left as they are. Empty file or a bare number is rejected (ambiguous), and so is the whole file if any line is bad |
| `DISARM` (empty) | unwind every book (short leg first per symbol), then `mode: Off` |
| `DISARM` (`SYMBOL` per line) | unwind only those books; an unknown symbol closes ALL books (protective) |
| `KILL_SWITCH` (present) | no growth; reductions and DISARM still run |
| `RISK_ACK` | clear a halt |

## Halts (`status.json.halt_reason`)

| reason | what happened | remedy |
|---|---|---|
| `net exposure ...` | legs unequal by > `HEDGE_NET_TOLERANCE_USD` for `HEDGE_NET_BREACH_TICKS` ticks (an IOC keeps failing on one venue) | check the failing venue / book; while halted the bot only *reduces* the larger leg; `RISK_ACK` |
| `leverage: ...` | a growth order this tick would exceed `HEDGE_MAX_LEVERAGE` × that venue's equity; nothing was sent (checked before the first leg) — also what fires after a liquidation | deposit, `RISK_ACK`, re-`ARM` |
| `liq_guard: ...` | a venue's account headroom (over every hedged book) fell under `HEDGE_LIQ_GUARD_PCT`; **every armed book was closed** | rebalance collateral, `RISK_ACK`, re-`ARM` |

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
`feed_problem` is `null` on a healthy tick. A process that starts *during*
an outage has no snapshot yet and writes nothing until its first
successful read (the previous process's `status.json` stays on disk and
goes stale, which is the honest picture). The Exited → Off settle re-read
fails the same way: orders already sent that tick are still reported and
the settle waits for the next readable tick.

## Restart / stop

The unit does **not** close positions on stop; a delta-neutral book needs
no supervision while the process is down, and the venue funding on both
sides keeps netting. `DISARM` before stopping only if the book should go.

## Arcus-leg roll (bot-strategy#1080) — default OFF

Arcus points are reported (unofficially) at ~100 pt per $1M of **perps volume**, while the RH points come from **holding** (#1046). The roll therefore trades **only the Arcus leg**. The Lighter/RH leg is never rolled: holding earns RH points at ~$1–2/pt, versus ~$36–92/pt from volume, and Lighter's terms say artificial trading does not earn points.

**Only enable it after** the owner's manual Arcus week has shown that volume earns points (#1075: the wallet's fills are reconciled against the weekly drop) and the official Arcus terms do not forbid it. Start with a small budget. Rolling a hedge leg is close to wash trading, which Arcus may filter or penalise.

What one roll does, strictly sequentially and only on a tick that sent nothing else:
1. A reduce-only close of `HEDGE_ROLL_CLIP_USD` on the Arcus leg, waited on until terminal (the Arcus settle above).
2. A re-open on the same leg, sized to what the close actually filled. There is nothing to re-open if the close did not fill.

The book is lopsided by at most one roll clip, and only between the two orders. There are never two orders on the account at once, so there is no self-trade. If the re-open fails or is uncertain, the uncertain guard holds the symbol until it is reconciled. The normal levelling then grows the short leg back, because the clip is at or below the net tolerance, so no breach is counted.

A roll runs only when all of these hold:
- the book is `On` and balanced within the tolerance
- the bot is not halted and there is no KILL_SWITCH
- the symbol has no uncertain order and the feed is healthy
- the Arcus account headroom is at least `HEDGE_LIQ_GUARD_PCT` and it is within `HEDGE_MAX_LEVERAGE`
- the Arcus leg holds at least one clip
- the interval has elapsed
- the weekly volume budget (including this roll's close and re-open) and the weekly cost cap are not exhausted

Books are rolled round-robin.

| env | default | meaning |
|---|---|---|
| `HEDGE_ROLL_ENABLED` | `false` | master switch; needs an Arcus leg |
| `HEDGE_ROLL_INTERVAL_SECS` | `3600` | minimum time between rolls (>= tick) |
| `HEDGE_ROLL_CLIP_USD` | — | notional per roll; must be <= `HEDGE_CLIP_USD` and <= `HEDGE_NET_TOLERANCE_USD` |
| `HEDGE_ROLL_WEEKLY_VOLUME_USD` | — | weekly volume budget that **gates rolls only** (every Arcus-leg execution counts toward it, but only rolls are ever refused by it) |
| `HEDGE_ROLL_WEEKLY_COST_USD` | — | weekly cap on fees + slippage vs mark; **gates rolls only**, like the volume budget |
| `HEDGE_ROLL_MODE` | unset | leftover of the removed `maker_first` mode: the roll is **taker-only**. Unset / `taker` are accepted; any other value is a config error while the roll is enabled and is only logged while it is off |

**Roll accounting (ground truth).** The weekly caps **gate rolls only**: they decide whether the next roll (or a roll's re-open) may be sent, and never block the planner. Levelling, repairs, ARM builds, DISARM closes and guard unwinds on the rolled Arcus leg always run; they are **booked** into the week (so they consume the roll budget) but are never refused by it — the caps are not a hard volume/cost limit on the Arcus account. Every execution on that leg books its **actual** executed value and cost (fee + slippage vs the mark it was sent against) into the week at the moment it is confirmed, so a repair is booked whenever it happens, at its real price. A roll leg is additionally **reserved** before it is sent: notional × (1 + 0.1 %), plus fee, slippage bound and that margin as cost. The reservation is dropped once the leg returns, because its fills were already booked where they settled. A leg that ends **uncertain** keeps its reservation (`in_flight`, kept across Sundays) until the uncertain entry is released on evidence, when its observed fills are booked, or cleared by `RISK_ACK`, when the larger of its send-time reservation and a full fill at the current mark is booked as the worst case. The next roll runs only if actual week + in-flight + its own two reservations fit under both caps. Before every send, the leg's fresh mark must be within 0.1 % of the reserved mark and the absolute limit must reach the touch (buy ≥ best ask, sell ≤ best bid, compared exactly with no tolerance), else nothing is sent. The limit is built in `Decimal` from `Decimal::from_f64` (never `from_f64_retain`, pairtrade#315) and rounded **inward** to the venue tick at that price (buy down, sell up), exactly as the connector rounds it, so the marketability check sees the price that is actually sent. The re-open, sent only after the close settled, is re-anchored on a fresh mark: its reservation and limit use that mark, the caps are re-checked with it, and right before it KILL_SWITCH is re-read and the Arcus headroom / leverage are re-checked on a **fresh** equity read (a halt cannot start mid-roll: it is only set by the tick's guards). Any refusal leaves the leg one clip short for the planner to repair, except KILL_SWITCH, which levels the gap itself (see the outcomes). A roll leg's booking and the drop of its reservation are one state change (one persist), and every booking lands in the week of its booking time. Fees are booked as absolute values (taker-only: always paid), and a leg's cost is never negative: price improvement over the mark books the fee only. Reservations left in flight by a process that died mid-roll are booked at the next roll pass. Counters reset every Sunday 00:00 UTC (at the start of the tick, before anything can book). Roll IOCs are sent at an **absolute** limit bound to the reserved mark (not re-anchored to the touch), and every booking uses the Arcus leg's own mark. With the roll disabled none of this books anything.

**Taker-only.** Both roll legs are IOCs (Arcus taker fee, Base tier 2.25 bp), sent at an absolute limit within 0.1 % of the reserved mark. An IOC never rests on the book, so a restart can never leave a roll order behind (the post-only `maker_first` mode and its restart reconciliation were removed, bot-strategy#1080). A roll blocks the tick for at most two IOC settles (≈ 2 × the sum of `HEDGE_FILL_WAIT_SECS`).

The roll week runs **Sunday 00:00 UTC to Sunday 00:00 UTC**. This is an assumption: the Arcus points API's weekly history starts on Sunday 2026-09-13 (#1075).

**Refusals back off.** A roll attempt that sends nothing (refused before the send, or rejected synchronously) counts as a refusal. Consecutive refusals wait 60 s, 120 s, 240 s, … doubling up to 1 h; after 5 in a row the roll is **suspended until the next roll week** (Sunday 00:00 UTC). Any attempt that sends an order resets the count. Each distinct refusal reason emits one `roll_blocked` event and stays in `blocked_reason` for the whole backoff.

**DRY_RUN flip.** Switching `DRY_RUN` on ↔ off resets the roll bookkeeping (week counters, in-flight reservations, refusals) at startup, so simulated volume never gates live rolls and vice versa.

Status: `hedge_holder.roll` (`enabled`, `mode` = `taker`, `leg`, week volume / cost / count, budgets, `last_roll_at`, `next_roll_at`, `blocked_reason`, `consecutive_refusals`). Events: `roll_start`, `roll_leg` (one per roll IOC: symbol, leg, `kind` = close / reopen, `order_id`, requested / filled qty, fee, value, mark, limit, `cost_usd`), `roll_done`, `roll_blocked` (reason, refusals, `retry_not_before`, `suspended`). Every `fill` event carries `"roll": true|false`, so roll volume can be told apart from planner volume in the log. `roll_done.outcome` is one of:

| outcome | meaning |
|---|---|
| `done` | close and re-open both filled the clip |
| `close_not_sent` | nothing executed: refused before the send (price moved beyond the margin, the limit does not reach the touch, a read failed) or rejected synchronously by the venue. No volume, no cost; counts as a refusal (backoff above), not a whole interval |
| `close_unfilled` | the close was accepted and filled nothing (e.g. the touch sat outside the price margin) |
| `close_partial_below_min` | the close filled less than the venue minimum: nothing to re-open; the planner levels it later (booked when it settles) |
| `close_uncertain` / `reopen_uncertain` | an order's outcome is unknown: the symbol is blocked until the uncertain machinery settles it (**needs attention if it escalates**) |
| `reopen_partial` / `reopen_failed` | the re-open filled less than the close / failed (or its fresh mark / the fresh Arcus equity could not be read): the planner levels the gap next tick |
| `reopen_capped` | the re-open, re-reserved at the fresh mark, would cross a weekly cap: not sent; the planner levels the gap (booked when it settles) |
| `reopen_blocked_kill_levelled` / `reopen_blocked_kill_unlevelled` | KILL_SWITCH was engaged while the close settled: the re-open is not sent, and the roll itself levels the gap with a reduce-only IOC of the closed quantity on the **other** leg (the planner does not run under KILL_SWITCH). `_unlevelled` = that reduction was not possible (other leg too small / read failed) or did not fully fill: **needs attention** |
| `reopen_blocked_headroom` / `reopen_blocked_leverage` | the fresh Arcus equity read shows headroom below `HEDGE_LIQ_GUARD_PCT` / leverage above `HEDGE_MAX_LEVERAGE`: the growth re-open is not sent; the planner levels (reductions only) |

A roll leg counts as **sent** only once the venue accepted it (it settled) or it became uncertain (it reached the venue and may have filled). `week_count` counts only rolls that sent at least one order; `last_roll_at` is when a roll last sent one (the interval counts from there). `orders_this_tick` in status includes roll orders. With the roll **disabled**, nothing books into the roll counters and nothing about the roll runs at startup: a default-off deployment behaves exactly as before the roll existed.

