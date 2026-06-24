# Round Attribution Readout Runbook

This runbook turns a pairtrade round readout into a repeatable attribution packet. It is diagnostic only: no YAML change, restart, or live config mutation belongs in this workflow.

## Readout Checklist

Work top to bottom; the preflight is a hard gate before any A/B/C scoring.

- [ ] **Config-drift preflight passes** — `scripts/check_config_drift.sh --round-json configs/pairtrade/round.json` exits 0 (running effective config == round expectation). Stop the readout on drift.
- [ ] **DEX realised PnL by variant** — per-variant `live_trade_attribution.py --csv-out` from DEX accounting exports, rolled up by `round_attribution_rollup.py`. The score is realised DEX PnL, not modeled JSONL/bot PnL.
- [ ] **Concentration + feature buckets** — rollup includes worst UTC day, weekly PnL concentration, and the cross-variant feature-bucket matrix (loss recall / win-kill).
- [ ] **Execution-artifact separation** — any A/B/C decision explicitly separates strategy signal from execution artifacts; attach the #613 execution-ledger report when it covers the window, otherwise record the missing-ledger caveat.
- [ ] **#510 blocker statement** — update bot-strategy#510 with the current top 1-2 blockers and follow-up issue mapping, supported by the packet or an explicit caveat.

The sections below detail each step.

## Inputs

- Per-variant DEX accounting exports for the readout window.
- Per-variant `live_trade_attribution.py --csv-out` output.
- Optional #613 execution-ledger reports from `scripts/execution_ledger_report.py`.
- The committed round expectation file, normally `configs/pairtrade/round.json`.

## Blocking Preflight

Run config-drift verification before scoring A/B/C differences:

```bash
scripts/check_config_drift.sh --round-json configs/pairtrade/round.json
```

If this reports drift, stop the readout and treat the affected window as config-contaminated until the running effective gauges match the round expectation.

## Per-Variant Attribution

Generate one realised-attribution CSV per variant from DEX accounting exports:

```bash
scripts/live_trade_attribution.py \
  --venue lighter \
  --variant a \
  --pair BTC_ETH \
  --csv-out /tmp/round-a-attribution.csv \
  <variant-specific input args>
```

Repeat for B and C. The cross-variant rollup must score DEX realised PnL, not only modeled JSONL or bot-side PnL.

## Execution Ledger Attachment

When #613 ledger files cover the same window, generate the execution report:

```bash
scripts/execution_ledger_report.py \
  --ledger '/opt/debot/debot_pnl/execution-debot-pair-btceth-*.jsonl' \
  --since 2026-06-01T00:00:00Z \
  --until 2026-06-22T00:00:00Z \
  --report-out /tmp/round-execution-ledger.md
```

Ledger reports are execution diagnostics. They explain slippage, leg-sync delay, partial fills, reissues, latency, and fee drag; they do not replace DEX realised PnL as the accounting source of truth. Missing ledger coverage is unknown execution attribution, not zero execution leakage.

## Round Rollup

Build the readout packet:

```bash
scripts/round_attribution_rollup.py \
  --csv /tmp/round-a-attribution.csv \
  --csv /tmp/round-b-attribution.csv \
  --csv /tmp/round-c-attribution.csv \
  --execution-ledger-report /tmp/round-execution-ledger.md \
  --title "Round N Attribution Rollup" \
  --report-out /tmp/round-attribution-rollup.md
```

The report must include:

- DEX realised PnL by variant.
- Worst UTC day and weekly PnL concentration.
- Cross-variant feature buckets with loss recall and win-kill.
- Execution-ledger coverage or an explicit missing-ledger caveat.
- A decision note that separates strategy signal from execution artifacts.

## #510 Blocker Statement

After the rollup is reviewed, update bot-strategy#510 with the current top one or two blockers and the follow-up mapping. A blocker is valid only when it is supported by the realised-attribution packet or by an explicit caveat that explains why the evidence is incomplete.
