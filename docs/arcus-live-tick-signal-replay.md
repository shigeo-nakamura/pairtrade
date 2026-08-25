# Arcus live-tick signal replay

Trade-level and threshold-tuning claims must use `arcus-spot-live-tick`'s own
observation sequence, not the independently sampled recorder archive. The
authoritative source for observations produced after bot-strategy #825 is the
executor-owned append-only stream:

    /var/lib/debot-arcus/spot-execute-once/live-tick-events/YYYY-MM-DD.jsonl

Every compact runtime-event payload is stored verbatim inside a schema-1
envelope. SHA-256 binds the exact payload bytes, and a domain-separated chain
binds record order across UTC-day segments. Sequence and timestamp continuity
are checked independently. The executor validates the latest segment before
every append and fsyncs each record. Files are mode 0600 under a mode-0700
directory.

At 00:12 UTC the archive timer scans every local closed UTC-day segment,
verifies and deterministically compresses each one, then publishes data before
its manifest under:

    s3://debot-dashboard/arcus-archive/live-tick-events/debot-arcus/YYYY/MM/

That bucket is private and versioned. Current local segments and current S3
versions have indefinite retention; noncurrent S3 versions expire after 90
days. The immutable writer refuses to replace a key with different bytes.
Each manifest records raw and compressed hashes, byte counts, sequence/time
bounds, and first/last chain hashes. A missing day after stream initialization,
payload/hash/chain corruption, or an S3 collision is a hard archive failure.
Rechecking an already archived segment is byte-identical and harmless. This
full closed-segment scan is required because a persistent systemd timer
coalesces a multi-day outage into one catch-up activation.

Fetch and verify a closed range before replay:

    scripts/fetch_arcus_live_tick_events.sh 2026-08-26 2026-09-25 /tmp/arcus-live-events.jsonl
    scripts/arcus_live_tick_signal_replay.py /tmp/arcus-live-events.jsonl --entry-z 2.0

The fetcher verifies every archive manifest and compressed/raw hash, then
verifies the chain and sequence across day boundaries. The replay verifies the
durable envelope again and includes `verification.durable_event_stream` in
its result. It never falls back to permissive journal parsing when an input
looks like a durable stream but fails integrity.

For a replay ending at the current checkpoint, copy `runtime_state.json`, pin
the boundary, and retain the stronger checkpoint-tail comparison:

    end_sequence="$(jq -r .state.sequence /path/to/runtime_state.json)"
    scripts/arcus_live_tick_signal_replay.py \
      /path/to/arcus-live-tick-journal.log \
      --checkpoint /path/to/runtime_state.json \
      --end-sequence "$end_sequence" \
      --entry-z 2.5 \
      --entry-z-change 2026-08-20T13:26:22Z=2.0 \
      --round-trip-cost-bps 35

The event's recorded z-score is the decision source of truth. The tool also
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
- Journald remains a retention-limited fallback for events before the durable
  stream's deployment. The lost 2026-08-19 through 2026-08-22 portion cannot
  be reconstructed byte-exactly; the independent recorder must not be
  substituted for it. Authoritative coverage begins with the first durable
  stream record.

The output includes:

- sequence gaps, an authoritative verdict, and checkpoint-tail verification;
- counts above selected absolute z-score thresholds;
- a chronological entry-threshold schedule;
- mean-reversion versus max-hold trade summaries;
- each simulated trade and any still-open simulated rotation.

Gross bps is the directed log-price-ratio move in the simulated strategy's
favor. Net bps only subtracts the caller-supplied fixed round-trip cost. It is
not reconciled live PnL and does not model route availability or fills.
