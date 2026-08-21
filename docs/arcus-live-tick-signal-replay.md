# Arcus live-tick signal replay

Trade-level and threshold-tuning claims must use `arcus-spot-live-tick`'s own
observation sequence, not the independently sampled recorder archive. Export
the unit journal with `journalctl -o cat` and copy the matching
`runtime_state.json`, then run:

    scripts/arcus_live_tick_signal_replay.py \
      /path/to/arcus-live-tick-journal.log \
      --checkpoint /path/to/runtime_state.json \
      --entry-z 2.5 \
      --entry-z-change 2026-08-20T13:26:22Z=2.0 \
      --round-trip-cost-bps 35

The journaled z-score is the decision source of truth. The tool also
recomputes every score whose full rolling window is present in the export,
using the runtime's population variance and informative-sample guard. With a
checkpoint it requires the journal's final sequence to equal the checkpoint
sequence and compares every float in the checkpoint signal-history tail by
its exact binary representation. A mismatch fails closed instead of mixing
two captures.

The output includes:

- sequence gaps and checkpoint-tail verification;
- counts above selected absolute z-score thresholds;
- a chronological entry-threshold schedule;
- mean-reversion versus max-hold trade summaries;
- each simulated trade and any still-open simulated rotation.

Gross bps is the directed log-price-ratio move in the simulated strategy's
favor. Net bps only subtracts the caller-supplied fixed round-trip cost. It is
not reconciled live PnL and does not model route availability or fills.
