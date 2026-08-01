# Arcus Spot inventory runtime

The Arcus Spot path is intentionally separate from the leveraged BTC/ETH
engine. It consumes schema-v3 public recorder snapshots and operates in one of
two non-live modes:

- read_only emits a would-rotate plan and never changes inventory;
- replay_simulation applies the recorded indicative route only to isolated
  in-memory inventory.

There is no live mode and no wallet, signing, approval, firm-quote, submit, or
transaction-status dependency in this slice.

## Safety gates

Before a plan is emitted, the runtime verifies the chain, verified token and
contract identity, exact pair/notional row, both direct routes, matching
reference prices, route age, recommended venue amounts, and recorder schema.
It then enforces:

- a hard floor for each token;
- a per-rotation fraction of inventory above the floor;
- a maximum post-rotation USD inventory imbalance;
- optimistic round-trip loss plus explicit gas and settlement buffers;
- sticky daily and cumulative mark-to-market loss halts;
- maximum hold and mean-reversion exit behavior.

The log-price ratio signal is evaluated against prior samples only. The current
sample is appended after z-score calculation, avoiding same-tick look-ahead.
This is a runtime/replay seam, not evidence that NVDA/AMD is economically
qualified.

## Deterministic replay

Build with the pinned Arcus connector feature and replay the recorder archive:

    cargo run --no-default-features --features arcus-spot-sdk \
      --bin arcus-spot-runtime -- \
      configs/pairtrade/arcus-spot-runtime.example.yaml \
      /path/to/samples.jsonl \
      /tmp/arcus-events.jsonl

The optional third argument is the event JSONL path. Without it, events go to
stdout. A compact final state summary goes to stderr. Replay evaluates quote
freshness at each snapshot event timestamp, never at the current wall clock.
A future read-only daemon must call step_at with the current UTC time.

The example config is not wired to systemd or deployment. Enabling a service,
installing wallet material, or permitting signed swaps remains outside this
slice and blocked on bot-strategy #756, #757, #758, and explicit approval.
