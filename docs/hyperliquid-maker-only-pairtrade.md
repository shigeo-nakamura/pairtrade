# Hyperliquid Maker-Only Pairtrade Prototype

Tracking: bot-strategy#709

## Scope

This is the first Hyperliquid pairtrade slice. It is not a live trading path.
The current connector is read-only and supports enough public market data for
pairtrade signal evaluation:

- `meta` for perp market metadata and `szDecimals`.
- `allMids` for normalized mid prices.
- `l2Book` for top-of-book bid/ask state.

All trading, account, fill, cancel, and signing methods intentionally return an
explicit `DexError::Permanent` until the maker-only execution path is designed,
tested, and reviewed.

## Current Design Boundary

Hyperliquid should not be treated as a direct Lighter clone. The prototype must
assume fee-bearing perps and make the maker/taker mix measurable from day one.
The first live-capable implementation must therefore satisfy these invariants:

- Entry orders are submitted as ALO/post-only on both legs.
- ALO rejection is not retried aggressively at a crossing price.
- If neither leg fills before `entry_post_only_timeout_secs`, cancel both legs.
- If exactly one leg fills, there is a strict hedge window:
  - complete the missing leg with bounded IOC only when the signal is still valid
    and a per-session taker budget remains;
  - otherwise flatten the filled leg with bounded IOC.
- At or after the hedge deadline, flatten the filled leg even if the signal is
  still valid.
- Dead-man cancel is armed before live order placement is enabled.
- Every fill is tagged as maker/post-only or taker/recovery in the execution
  ledger before PnL attribution is trusted.

The executable spec for the asymmetric-fill branch is in
`src/pairtrade/testing/hyperliquid_maker_tests.rs`.

## Hyperliquid API Notes

Docs checked: 2026-07-06.

- Limit order TIF supports `Alo`, `Ioc`, and `Gtc`; `Alo` is add-liquidity-only
  and cancels instead of immediately matching.
- `scheduleCancel` can schedule a cancel-all operation, but the time must be at
  least 5 seconds in the future and the trigger count is capped per day.
- IP REST budget is shared across REST calls. `l2Book` and `allMids` are cheap
  info calls, while many user/history info calls are heavier.
- Address-based action limits are separate from info requests. Cancels have a
  larger limit, but cancel/replace loops still need a hard cadence cap.
- Fee rates are tier-dependent. The observer config keeps `fee_bps > 0` only to
  force post-only pricing mode; live fee attribution must read actual account
  fees or ledger fills.

Primary references:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees

## DRY_RUN / Shadow Runbook

Use this only from a local shell or a dedicated shadow host. No Frankfurt restart
is needed for this read-side slice.

Build the Hyperliquid-only pairtrade binary:

```bash
cd /home/shigeo/bot/pairtrade
cargo build --no-default-features --features hyperliquid-sdk
```

Prepare local output directories:

```bash
mkdir -p /tmp/debot-hyperliquid-observe/history_archive
```

The `/tmp` paths in the YAML are local smoke-test defaults. For a long-running
shadow collector, point the existing output overrides at a persistent
`STATE_DIR`, for example in the dedicated observer service environment:

```bash
STATE_DIR=/path/to/persistent/debot-pair-hyperliquid-observe
mkdir -p "$STATE_DIR/history_archive"
export DATA_DUMP_FILE="$STATE_DIR/market_data_hyperliquid_pairs.jsonl"
export HISTORY_ARCHIVE_DIR="$STATE_DIR/history_archive"
export PAIRTRADE_HISTORY_FILE="$STATE_DIR/pairtrade_history_BTC_ETH_SOL.json"
```

`risk_state.json` is written next to `PAIRTRADE_HISTORY_FILE`. In a systemd
`EnvironmentFile`, do not rely on shell variable interpolation; set the three
output variables above to their resolved absolute paths. Configure these values
only for the dedicated Hyperliquid observer service; do not modify an existing
trading service environment.

Run the observer:

```bash
cd /home/shigeo/bot/pairtrade
REST_ENDPOINT=https://api.hyperliquid.xyz \
WEB_SOCKET_ENDPOINT=wss://api.hyperliquid.xyz/ws \
PAIRTRADE_CONFIG_PATH=configs/pairtrade/debot-pair-hyperliquid-observe.yaml \
RUST_LOG=info \
target/debug/debot
```

Expected startup signals:

- `[CONFIG] DEX_NAME is: hyperliquid`
- `[hyperliquid] read-only connector started`
- `[CONFIG] ... post_only_supported=true post_only_enabled=true`
- `[METRICS]` lines for BTC/ETH and SOL/ETH after warm-up
- JSONL market data under `/tmp/debot-hyperliquid-observe/`

Stop criteria:

- Any attempted order/account method on Hyperliquid is a bug in observe mode.
- Any repeated `Hyperliquid allMids missing symbol` error means symbol mapping
  must be fixed before continuing.
- Any sustained REST rate-limit or timeout burst means polling cadence must be
  reduced or WebSocket market data must be implemented before extending scope.

## Next Live-Capable Slice

Do not enable live orders until these are done:

- Signing path and API wallet handling, preferably reusing the smallest safe
  subset of the old Hyperliquid implementation.
- Asset-index cache from `meta.universe` for order/cancel payloads.
- ALO `create_order` with cloid and bounded retry budget.
- `cancelByCloid`, `orderStatus`, `openOrders`, `userFillsByTime`, and dead-man
  `scheduleCancel` support.
- Execution-ledger maker/taker attribution and taker-budget kill switch.
- Unit tests for cloid mapping, ALO rejection, partial fill, cancel timeout,
  and one-leg flatten.
