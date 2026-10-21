# Arcus volume bot — shadow toxicity log (bot-strategy#1093)

`TOXICITY_LOG=on` in an instance's `config.env` makes `arcus_vol_runtime`
append feature rows to `toxicity_YYYYMMDD.jsonl` in its state dir (UTC day of
the row, no fsync, like the quote gate's `gate_YYYYMMDD.jsonl`). **Logging
only**: the tick never reads the log, the reference feed or the engine; with
the log on, quoting is byte-for-byte the baseline. Pre-registration of the
gate test: `deploy/arcus_vol/TOXICITY_PREREG.md`. Evaluation skeleton:
`scripts/arcus_vol_toxicity_eval.py`.

Config keys (`config.env`, launcher allow-list):

| key | values | meaning |
|---|---|---|
| `TOXICITY_LOG` | `off` (default) / `on` | write the log |
| `TOXICITY_REF` | Lighter Core symbol, default = `MARKET` without `-USD`; `none` | reference book. An unlisted symbol (e.g. `GLD`) leaves every `ref_*` field `null` and logs it once. `XAU` is a *proxy* for GLD if wanted (different instrument, ratio ≠ 1). |

Reference feed: the public Lighter Core `order_book/<market_id>` WebSocket
(`wss://mainnet.zklighter.elliot.ai/stream`), market id from one
`orderBookDetails` read per connect (metadata, not a poll), no credentials.
The runtime's own `trades` tape (public) is started with the log on, as it
is for the quote gate.

## Rows

Every row: `kind`, `t` (host receive / decision time, ms), `x` (the venue's
own time of the thing the row is about, ms, `null` when the venue gave none),
`market`.

### `quote` — one per placement / re-peg / replacement the venue ACCEPTED
Written after the batch returns, for each side whose resting quote (order
id, price or size) changed; a rejected or DMS-blocked send leaves no row.
`side` (`buy`/`sell`), `px`, `qty`, `dist_touch_bps` (behind the touch on
our side, as of the last own-book read ≤ `t`), `offset_bps` (presence offset
quoting with), `arm_bps` (the learner's arm at `t`, LIVE / HISTORY
attribution, same as the learner's own), `band_bps`, `session_in`, plus the
feature block.

### `fill` — one per booked fill (maker and taker; `role` says which)
`fill_id` (join key to `fills.jsonl` `markout` rows and to `markout_tox`
rows), `x` = the fill's venue time, `rx_t` = when the runtime received it,
`px`, `qty`, `side`, `dist_touch_bps` (as of the own book ≤ fill time),
`ref_mid_at_fill`, plus the feature block **as of the fill's venue time**
(never a book read after it).

### `markout_tox` — per fill, horizons 1 / 10 / 60 / 300 s
`fill_id`, `horizon_s`, `markout_mid_bps` (own mid at `t` vs fill px, signed
in our favour: buy → up is positive), `markout_ref_bps` (reference mid at
`t` vs `ref_mid_at_fill`, same sign), `late` (`true` when no fresh mid came
within 10 s of the due time; both values `null` then), `mid`, `ref_mid`.
`fills.jsonl`'s own 5 / 30 / 60 s `markout` rows are untouched. A horizon
waits for every mid it needs (the own mid, and the reference mid when the
fill had one) until the cutoff. After a restart the horizons still owed by
the fills of the last two UTC days are rebuilt from the log (`restart:
true` on the ones already past their cutoff).

### Feature block (quote and fill rows)

| field | definition |
|---|---|
| `arc_bid`, `arc_ask`, `arc_mid` | own top of book as of `t` |
| `arc_bid_sz1`, `arc_ask_sz1` | L1 sizes |
| `arc_spread_bp` | (ask − bid) / mid × 1e4 |
| `arc_top_imb` | (bidsz − asksz) / (bidsz + asksz), L1 |
| `arc_l2_imb_5lv`, `arc_l2_imb_10lv` | same over the top 5 / 10 levels |
| `arc_book_age_ms` | `t` − receive time of that book read |
| `x_book` | venue time of that book read |
| `arc_flow_1s`, `arc_flow_5s`, `arc_flow_30s` | signed taker notional on Arcus in `(t−h, t]` (taker buy +) |
| `arc_ntr_30s` | trade count in the last 30 s |
| `arc_ret_1s`, `arc_ret_5s`, `arc_ret_30s` | own mid return (bp) from `t−h` to `t`, as-of |
| `window` | `off` / `pre` / `cash` / `after` / `weekend` (learner's New York windows) |
| `prev_boundary`, `secs_since_prev`, `next_boundary`, `secs_to_next` | nearest session edges: `cash_open_0930`, `cash_close_1600`, `overnight_start_2000` (Mon–Thu), `weekend_close_2000` (Fri), `sunday_reopen_1800`, `overnight_start_2000` (Sun) — New York clock, DST-aware |
| `ref_symbol`, `ref_up` | reference symbol, feed up |
| `ref_mid` | reference mid if the last frame is ≤ 5 s old |
| `ref_t`, `ref_x` | host receive / venue time of the last reference frame |
| `ref_lead_ms` | `t` − `ref_t` (how long ago the reference last changed) |
| `ref_dev_bp` | (arc_mid − ref_mid) / ref_mid × 1e4 |
| `ref_ret_1s`, `ref_ret_5s` | reference mid return (bp) over the last 1 / 5 s, signed |
| `ref_jump_1s`, `ref_jump_5s` | max |reference mid move| (bp) over the last 1 / 5 s |

Directional features are logged **raw** (not side-flipped); the evaluation
flips by side where a rule needs "against the quote".

## Mapping to bot-strategy#1190 (A-levers, PREREG 2026-10-16)

| #1190 | here | note |
|---|---|---|
| metric 2 / B1: external price moved ≥ x bp against the quote inside the 5 s before the fill | `ref_ret_5s` with the fill's side (R3 in the PREREG); magnitude `ref_jump_5s` (R1) | #1190 uses Pyth Hermes / HL xyz; here the reference is Lighter Core (same symbol). SPY/QQQ are not on HL xyz; GLD is on no Lighter venue. |
| `markout_mid(60 s)` | `fills.jsonl` `markout` 60 s = primary; `markout_tox` `markout_mid_bps` 1/10/60/300 | the #1082 CUTOFF §4 horizons |
| `markout_ref` | `markout_tox` `markout_ref_bps` 1/10/60/300 | |
| join key `t` (host receive, ms) + source time `x` | every row `t` + `x`; reference `ref_t` + `ref_x` | |
| metric 1 (lead Δ vs Lighter's index `s.ix`) | — | not measurable here (no Lighter index feed); `ref_lead_ms` is the age of the last reference change, not a lead |
| metric 3 (HL xyz ↔ Lighter cross-correlation) | — | out of scope |

The live Arcus maker fills exported for #1190 on 2026-10-16
(`~/bot/logs/studies/2026-10-16-arcus-vol-fills-for-1190/`) are the same
`fills.jsonl` rows this log joins to.

## Enable

1. Add to `/etc/debot-arcus-vol*/config.env` (and `deploy/arcus_vol/config/*.config`):
   `TOXICITY_LOG=on` (+ `TOXICITY_REF=...` when the default is not wanted).
2. `install_arcus_vol_host.sh --apply --apply-config`, then restart the
   instance (owner-run, flat, outside the blackout holds).
3. status.json gets a `toxicity` block (`rows_today`, `pending_markouts`,
   `ref.up/age_ms/market_id/last_err`); the journal line
   `[TOX] reference <sym> (Lighter market <id>) up` confirms the feed.
