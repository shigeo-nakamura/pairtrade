# SPY offset-learner verification: pre-registration (bot-strategy#1093)

Frozen 2026-11-04, before `LEARNER=live` is in effect on SPY. Owner
2026-11-04: postpone the continue/stop decision on the Arcus volume bot until
we have confirmed that the learning framework works; prepare this at
**$3/pt**.

## Why

The toxicity gate (`TOXICITY_PREREG.md`) was KILLED at IS on 2026-11-04, so
the bot's only remaining lever is the offset learner. The last LIVE run
(2026-10-19 → 10-23, arms 5/8/12) could not be judged: it settled on 12 bp,
which got almost no fills, so no arm collected data. This run removes 12 bp
and adds 3 bp. 3 bp is the fixed off-hours offset SPY has quoted since
2026-10-26, and the shadow stats rate it as the best off-hours arm.

## What changes (this PR)

| | before | during the test |
|---|---|---|
| `LEARNER` (SPY only) | shadow | **live** |
| `LEARNER_LIVE_ARMS` | 5,8,12 | **3,5,8** |
| `LIVE_MIN_OFFSET_BPS` (code) | 5 (owner 10-19) | **3** (owner 11-04) |
| `LEARNER_POINT_VALUE_PER_M` | 72 | 72 (= $3/pt × 24 pt per $1M) |
| fallback offsets `QUOTE_OFFSET_BPS` / `SESSION_OFFSET_BPS` | 3 / 5 | 3 / 5 |
| decide every / hysteresis | 60 min / 0.5 $/h | unchanged |

Stops, DMS, the pause timers, clip size and KILL_SWITCH are unchanged and
take precedence over the learner. GLD and AAPL are not touched. Scores carry
over from shadow (`learner.json`), so the learner starts warm. This is by
design and applies the same way to every arm.

## Test period

- **t0** is the restart that loads `LEARNER=live` on `debot-arcus-vol`
  (owner-approved). It is recorded in the addendum below, together with the
  first `learner LIVE` journal line.
- The test covers the **14 whole UTC days** after t0's day. It includes two
  points drops.
- The **baseline** is SPY's fixed 3 / 5 bp period, UTC days 2026-10-27 …
  2026-11-03. It is computed from the same `fills.jsonl` by the same
  function: daily reward mean **+$1.25/day**, sd $6.2.

## Rules (scored once by `scripts/arcus_vol_learner_verify.py`)

Mechanics: does the learner do what it is built to do?

- **M1 explores:** every live arm (3, 5, 8) has ≥ 2 **quoting** hours in at least one window. Quoting hours are counted the way the runtime credits them: only while it is actually quoting, so halts, pauses and downtime add nothing. They are read from the hourly `kind: learner` rows in `learner.jsonl`, with the half-life decay undone.
- **M2 not stuck:** no window with ≥ 10 quoting hours spends ≥ 80 % of them on the widest arm (8 bp) while not one fill is attributed to that arm (fills are attributed by `live_log`). This is the 10-19..10-23 failure mode.
- **M3 no flapping:** at most 24 `live_log` changes per day on average.

Economics: the learner's own objective, per UTC day.

- **E1:** the mean daily reward over the test period, where reward = net $ (realized − fee) + 72 × volume / $1M, is **≥ the baseline mean**. A Welch t is reported but is descriptive only.

Verdict:

- **BROKEN:** any of M1–M3 fails.
- **MECHANICS_OK_ECONOMICS_NOT_BETTER:** M1–M3 pass and E1 fails.
- **WORKS:** M1–M3 pass and E1 passes.

Honest power note: with a daily sd of about $6, 14 days resolve only a
difference of about $3/day or more. E1 can say "not worse on average"; it
cannot prove a small gain. The readout also reports $/pt from the two points
drops; those figures are supplied by the owner and are disclosure only.

## Procedure

1. Run the readout after the test period ends. The script refuses earlier and refuses a second run (`READOUT_DONE.json`).
2. Copy `/var/lib/debot-arcus-vol/state/{fills.jsonl,learner.json,learner.jsonl}` down from the host, then run:
   `arcus_vol_learner_verify.py --fills fills.jsonl --learner learner.json --journal learner.jsonl --t0 <addendum t0> --out <study dir>`
3. Post the verdict on #1093. The continue/stop decision goes back to the owner.
4. The rules, arms, point value and baseline are never changed after t0.
5. A stop, a halt, or an owner-requested change during the test is disclosed in the readout, not used to rescue the result.

## Addendum (filled at t0, nothing else)

- t0: _pending owner-approved restart_
