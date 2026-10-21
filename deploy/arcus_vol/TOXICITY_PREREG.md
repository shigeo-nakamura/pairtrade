# Pre-registration — Arcus volume-bot toxicity gate (bot-strategy#1093)

Frozen 2026-10-21 before any `toxicity_*.jsonl` row exists (the log ships with
`TOXICITY_LOG=off`; it is switched on by the owner after this PR merges). The
sha256 of this file is posted on the PR and on #1093. Changes after that are
addenda with their own sha, never edits.

## Question

Can a gate evaluated at quote time, from features available on a DEX, tell
which of the bot's maker fills on Arcus stock/ETF perps will be adverse
(picked off), so that **pulling** those quotes (or **widening** them) raises
the mean 60 s markout of the remaining fills enough to make the bot net
positive before points?

## Data

- `toxicity_YYYYMMDD.jsonl` (state dir of every running instance): `quote`
  rows (every placement / re-peg / replacement), `fill` rows (every booked
  fill, `fill_id`), `markout_tox` rows (1 / 10 / 60 / 300 s against the own
  mid `markout_mid_bps` and the Lighter reference mid `markout_ref_bps`).
- `fills.jsonl`: the existing `markout` rows (5 / 30 / 60 s on the own mid),
  joined on `fill_id`. **Primary outcome = `fills.jsonl` `markout` 60 s**
  (`bps`, signed in our favour), the series every earlier readout used.
- Markets: whatever runs (SPY-USD, GLD-USD, AAPL-USD at freeze time), maker
  fills only (`role == "maker"`), blackout holds and halted periods excluded
  automatically (no fills there).

## Split (opened once)

- In-sample (IS): fills from the first log row up to **2026-11-03 23:59:59Z**
  (two weeks of logging, assuming the switch-on by 10-22).
- Out-of-sample (OOS): fills from **2026-11-04 00:00:00Z** to
  **2026-11-17 23:59:59Z**.
- `scripts/arcus_vol_toxicity_eval.py --split 2026-11-04 --oos-end 2026-11-18`
  evaluates IS (fills before the split) and refuses to select a rule with
  fewer than 150 IS maker fills; the OOS run (`--oos`) scores only fills in
  `[split, oos-end)` — later rows are never scored — and needs `--open-oos`,
  which writes `OOS_OPENED.json` into the output dir and refuses a second
  opening. The OOS is read **once**, for the single rule selected on IS. If
  fewer than 150 IS maker fills exist by 11-03, the IS window extends by
  whole weeks until it has 150 (addendum with the new split / end dates,
  before any OOS row is read).

## Gate family (fixed now)

Rules a quote-time gate can apply with the logged features. Each rule flags
a fill as "would have been pulled". The gate **only removes or widens**
quotes the baseline planned (a subset of the baseline, design D1 of
bot-strategy#1120); it never adds one.

| id | rule (flag the fill when) | grid |
|---|---|---|
| R1 | `ref_jump_5s ≥ x` (reference moved ≥ x bp in the 5 s before) | x ∈ {1, 2, 3, 5} bp |
| R2 | `ref_jump_1s ≥ x` | x ∈ {1, 2, 3, 5} bp |
| R3 | reference moved **against** the quote: buy fill and `ref_ret_5s ≤ −x`, sell fill and `ref_ret_5s ≥ +x` | x ∈ {1, 2, 3, 5} bp |
| R4 | `secs_to_next ≤ s` or `secs_since_prev ≤ s` (near a session boundary) | s ∈ {60, 300, 600} |
| R5 | `abs(arc_flow_5s) ≥ f` in the direction that hits the quote (taker buys for an ask fill, taker sells for a bid fill) | f ∈ {$5k, $25k, $100k} |

A fill with the rule's feature `null` is **not** flagged (the live gate would
fall back to baseline, bot-strategy#1120 D6).

## Metric and gates

For a rule: `kept` = maker fills not flagged, `all` = every maker fill in the
window.

- `Δ` = mean(markout60 | kept) − mean(markout60 | all), in bp.
- `t` = day-clustered: `Δ_d` per UTC day with ≥ 3 fills in both groups,
  `t = mean(Δ_d) / (sd(Δ_d) / √n_days)`, `n_days ≥ 5`.
- `keep` = |kept| / |all|.

**IS selection**: the rule with the largest `Δ` among those with
`keep ≥ 0.5` and `t ≥ 2` on IS; none → **KILL** (no OOS is opened).

**OOS verdict (one look)** for the selected rule:
- **PASS**: `Δ ≥ 1 bp`, `t ≥ 2`, `keep ≥ 0.5`.
- otherwise **KILL** — no re-sweep of the grid, no new rule on the same tapes.

Reported alongside (no gate): the same table for `markout_ref_bps` at 60 s,
`markout_mid_bps` at 1 / 10 / 300 s, and per market.

## After a PASS

A PASS is a *proposal* to run the rule live through the pluggable quote gate
(`ARCUS_VOL_GATE_*`, pull only; widen = a separate pre-registration) on the
same markets, bounded by the unchanged daily / cumulative / position stops,
for two weeks, with a live readout by the same metric. Nothing auto-advances.

## Not done

No order is changed by the log. No reference other than the public Lighter
Core `order_book` channel. No model fitting beyond the threshold grid above.
