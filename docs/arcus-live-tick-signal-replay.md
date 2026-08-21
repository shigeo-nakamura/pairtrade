# Arcus live-tick signal replay

Trade-level and threshold-tuning claims must use `arcus-spot-live-tick`'s own
observation sequence, not the independently sampled recorder archive. Export
the unit journal with `journalctl -o cat` and copy the matching
`runtime_state.json`, then pin the journal boundary to that checkpoint and run:

    end_sequence="$(jq -r .state.sequence /path/to/runtime_state.json)"
    scripts/arcus_live_tick_signal_replay.py \
      /path/to/arcus-live-tick-journal.log \
      --checkpoint /path/to/runtime_state.json \
      --end-sequence "$end_sequence" \
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

Trust rules:

- Sequence gaps are fatal by default because a missing tick can hide an entry
  or exit. `--allow-gaps` is diagnostics-only and marks the report
  non-authoritative. Use `--start-sequence` to select a continuous suffix after
  a historical gap.
- Z-score recomputation restarts its full-window warm-up after every gap.
- Capture the checkpoint before exporting the journal, pass its sequence as
  `--end-sequence`, and keep `--checkpoint`; a later/mismatched checkpoint fails
  closed.
- The trade table is a neutral-start, signal-only counterfactual. It does not
  replay route, risk, inventory, or fill gates.

The output includes:

- sequence gaps, an authoritative verdict, and checkpoint-tail verification;
- counts above selected absolute z-score thresholds;
- a chronological entry-threshold schedule;
- mean-reversion versus max-hold trade summaries;
- each simulated trade and any still-open simulated rotation.

Gross bps is the directed log-price-ratio move in the simulated strategy's
favor. Net bps only subtracts the caller-supplied fixed round-trip cost. It is
not reconciled live PnL and does not model route availability or fills.
