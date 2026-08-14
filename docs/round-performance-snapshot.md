# Round Performance Snapshot

This runbook produces the repeatable evidence packet that was previously
assembled ad hoc during a live round: flat-to-flat collateral delta,
external-equity MDD, pre-window daily-DD `+2σ`, bot-PnL divergence, pairwise
opportunity capture, and comparable execution-ledger metrics.

It complements `round-attribution-readout.md`. DEX accounting exports processed
by `live_trade_attribution.py` remain the per-trade source of truth. The snapshot
script is diagnostic only and never fetches data or changes live state.

## 1. Commit a Round Manifest

Copy the preceding round's manifest and update the boundary timestamp, exact
post-allocation collateral, agent names, comparison direction, and known
data-quality issue:

```text
configs/pairtrade/scenarios/roundN/roundN-performance-readout.json
```

The equity spike filter is disabled unless the manifest explicitly enables it.
Enable it only for a one-sample glitch whose immediate neighbors agree. A
persistent equity step may be a real transfer and must not be filtered.

`pre_window_guard_hours` excludes boundary-preparation withdrawals/deposits
from the prior daily-DD baseline. Keep the default 24-hour guard unless the
allocation timeline proves a different clean cutoff.

## 2. Collect Read-Only Artifacts

Replace the round, timestamps, host tag, and service for the target window.
Frankfurt journal timestamps are always supplied in UTC.

```bash
READOUT_DIR=/tmp/pairtrade-round9-readout
mkdir -p "$READOUT_DIR"

# Durable full-window PnL/execution archive, followed by the current host tail
# that may not have reached S3 yet.
aws s3 sync \
  s3://debot-dashboard/debot/bt-archive/frankfurt/debot-pair-btceth/pnl/ \
  "$READOUT_DIR/"
scp 'debot:/opt/debot/debot_pnl/pnl-debot-pair-btceth-*.jsonl' "$READOUT_DIR/"
scp 'debot:/opt/debot/debot_pnl/execution-debot-pair-btceth_*.jsonl' "$READOUT_DIR/"

# External equity curves survive false in-process DD rebaselines.
for variant in a b c; do
  aws s3 cp \
    "s3://debot-dashboard/debot/status/frankfurt/debot-pair-btceth-${variant}.equity_history.jsonl" \
    "$READOUT_DIR/${variant}.equity_history.jsonl"
done

# A collateral score is provisional unless every evaluated arm is flat at the
# same snapshot.
ssh debot 'curl -fsS http://127.0.0.1:9464/metrics' >"$READOUT_DIR/metrics.prom"
```

Keep DEX accounting exports under `~/bot/logs/` and run the per-trade workflow
from `round-attribution-readout.md` alongside this snapshot.

## 3. Generate the Packet

Record an actual UTC cutoff; do not infer it from elapsed sleep/check cycles.

```bash
date -u '+%Y-%m-%dT%H:%M:%SZ'

scripts/round_performance_report.py \
  --manifest configs/pairtrade/scenarios/round9/round9-performance-readout.json \
  --data-dir "$READOUT_DIR" \
  --metrics "$READOUT_DIR/metrics.prom" \
  --until 2026-08-14T07:24:48Z \
  --report-out "$READOUT_DIR/round9-performance.md" \
  --json-out "$READOUT_DIR/round9-performance.json"
```

The JSON packet is the audit artifact. The Markdown file is the human readout.

## 4. Review Data Guards

- `flat` must be `true` for every scored arm.
- `capital_event_like_steps` must be zero, or venue transfers must be
  reconciled before collateral delta is interpreted as PnL.
- Every removed isolated spike is reported. Inspect it in the JSON packet.
- `collateral_return_bps` and external MDD come from venue-equity history.
  `bot_return_bps` is secondary while a known PnL bias is not live-verified.
- Pairwise `paired_gap_bps_secondary` separates common entries from
  `opportunity_gap_bps_secondary`. This prevents an earlier exit that enables
  an extra trade from being mis-described as direct exit-price edge.
- Missing execution-ledger fields mean unknown attribution, not zero leakage.

Current Prometheus gauges do not preserve circuit-breaker history. Join the
retained journal or a durable risk-event archive separately:

```bash
ssh debot "sudo journalctl -u debot-pair-btceth \
  --since '2026-08-07 00:00:00 UTC' --no-pager | \
  grep -E '\[CIRCUIT_BREAKER\]|session.halt|kill.switch|capital (deposit|withdrawal)'"
```

If the round exceeds journal retention, mark breaker/halt history incomplete
rather than treating missing log lines as zero events.

## 5. Record the Decision

Update the existing round tracker with:

- UTC cutoff and flatness evidence;
- DEX realised PnL plus collateral cross-check;
- normalized pairwise gaps and whether they came from paired trades or extra
  opportunity capture;
- execution size/slippage comparison;
- external MDD, `+2σ` trigger, and breaker/halt history;
- data-quality blockers and the final evaluation date.

Create a new bot-strategy issue only for a distinct actionable blocker or
automation gap. Do not create a new issue merely to duplicate the round readout.
Do not restart, deploy, transfer capital, or mutate risk state as part of this
workflow.
