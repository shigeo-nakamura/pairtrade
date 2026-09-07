# Book runtime — slow cross-sectional book on the debot operational stack

bot-strategy#937. Host runtime for XSMOM (bot-strategy#695) and, once its
Phase 1 is approved, Engine B (bot-strategy#866 / #876). This document is
the frozen spec the code under `src/book/` implements; change the spec and
the code in the same PR.

All timestamps are UTC. All monetary amounts are USD notional.

## 1. Shape

The runtime holds a **book of single-symbol perp legs** on one venue
(`lighter` or `hyperliquid`). At discrete **decision times** it reads a
**target weight vector** produced by an external signal producer, turns
the difference between the current book and the target into IOC taker
orders, confirms fills against the venue position, and records everything
on disk. Between decisions it only marks the book, accrues funding, watches
the risk rails, and (for fixed-window strategies) flattens at the scheduled
time.

The runtime **never computes a signal**. The producer is a separate process
(Python, cron) that writes a signal file; the runtime validates freshness,
schema, universe membership, caps, and hash before it acts. A signal that
fails any check is rejected and the decision is skipped, never partially
applied.

What is deliberately *not* here: continuous spread tracking, rehedging,
z-scores, Kalman betas — the BTC/ETH pairtrade engine that used to live in
this repo was sunset on 2026-09-06 and removed (same issue).

## 2. Config schema (`configs/book/<instance>.yaml`)

```yaml
schema_version: 1
instance_id: xsmom-695            # status/ledger tag, dashboard card id
venue: lighter                    # lighter | hyperliquid
dry_run: true                     # paper fills; live requires BOOK_CONFIRM_LIVE=yes-i-mean-it too

universe:
  symbols: [BTC, ETH, SOL, ...]   # whitelist; weights on other symbols reject the signal

schedule:
  kind: interval_days             # interval_days | daily | calendar
  anchor_date: 2026-07-03         # interval_days: decisions on anchor + k*every_days
  every_days: 5
  decision_time_utc: "00:30"      # interval_days / daily
  signal_grace_secs: 5400         # window after decision_time in which a valid signal is accepted
  flatten_after_secs: null        # fixed-window strategies: flatten at decision + N seconds
  calendar_path: null             # kind=calendar: JSON list of {decision_key, decision_at, flatten_at}

signal:
  path: /var/lib/book-xsmom/signal.json
  producer_id: xsmom_695_L28_H5_q20_riskadj
  max_age_secs: 7200              # now - generated_at must be below this
  require_dollar_neutral: true    # |sum(w)| <= net_tolerance
  net_tolerance: 0.05

sizing:
  gross_notional_usd: 1000        # sum |w| == 1 maps to this
  max_symbol_weight: 0.15         # |w_i| cap (fraction of gross)
  max_gross_usd: 1100             # post-rounding cap on sum |target notional|
  max_net_usd: 150                # post-rounding cap on |sum target notional|
  min_order_usd: 10               # venue minimum; smaller diffs are skipped (unless closing)
  rebalance_deadband_usd: 5       # diffs below this are not traded at all

execution:
  slippage_bps: 50                # IOC price cap; also a pre-send drift guard vs the sizing mid
  max_attempts: 3                 # per intent, on partial fill
  fill_confirm_timeout_secs: 15   # venue position must reflect the fill within this
  allow_venue_protection_fallback: false  # Lighter has no capped IOC (#918): true = send with the venue's ±20% protection instead
  paper_slippage_bps: 5           # dry_run fill = mid +/- this
  paper_fee_bps: 0

risk:
  equity_reference_usd: 1000      # paper equity base; live uses the venue equity
  max_session_loss_bps: 500       # sticky halt + flatten, cleared by RISK_ACK
  max_daily_loss_bps: 300         # daily halt (no new opens), resets at 00:00 UTC
  kill_switch_path: /var/lib/book-xsmom/KILL_SWITCH
  risk_ack_path: /var/lib/book-xsmom/RISK_ACK

paths:
  state: /var/lib/book-xsmom/state.json
  ledger: /var/lib/book-xsmom/ledger.jsonl
  pnl: /var/lib/book-xsmom/pnl.jsonl
  status: /var/lib/book-xsmom/status.json
```

The effective config is fingerprinted (`[CONFIG] instance=… fp=<sha256-12>`)
at startup and exported as `book_config_info{instance,fp}` so a
deployed-but-not-loaded config is visible on `/metrics` (same discipline as
bot-strategy#580).

## 3. Signal file contract (schema v1)

```json
{
  "schema_version": 1,
  "producer_id": "xsmom_695_L28_H5_q20_riskadj",
  "generated_at": "2026-09-06T00:20:36Z",
  "as_of": "2026-09-06T00:00:00Z",
  "decision_key": "2026-09-06",
  "weights": {"SOL": 0.10, "HYPE": 0.10, "DOT": -0.10, "DOGE": -0.10},
  "meta": {"lookback_days": 28, "universe_size": 38},
  "payload_sha256": "…"
}
```

- `as_of` is the close of the last bar the producer used. **Point-in-time
  contract**: `as_of <= generated_at` and `as_of <= decision_at` of the
  decision the file is meant for; a file whose `as_of` is after the
  decision time is rejected as look-ahead. (Same discipline as
  bot-strategy#848.)
- `decision_key` names exactly one decision (the calendar date for
  `interval_days` / `daily`, the calendar entry key for `calendar`). The
  runtime applies each key at most once; a file for a key already applied
  or skipped is ignored.
- `weights` are fractions of `sizing.gross_notional_usd`; `sum |w| <= 1`,
  each `|w| <= max_symbol_weight`, every symbol in `universe.symbols`. When
  `require_dollar_neutral`, `|sum w| <= net_tolerance`. An empty map is a
  valid "go flat" instruction.
- `payload_sha256` is the sha256 of the canonical JSON
  (`separators=(",", ":")`, `sort_keys=True`) of
  `{"as_of","decision_key","producer_id","weights"}`, floats rendered as
  Python `repr(float)` (fixed notation for decimal exponents in `[-4, 16)`,
  otherwise `1e-05` style). The runtime recomputes it with a formatter that
  matches Python for the full finite range; a mismatch rejects the file. The hash is written to the ledger and
  `state.json` so a rebalance is traceable to the exact vector that caused
  it.
- Validation order: parse → schema_version → producer_id → hash →
  freshness (`generated_at`) → look-ahead (`as_of`) → decision_key →
  universe → weight bounds → neutrality. The first failure wins and is
  logged as `[SIGNAL] rejected key=… reason=…` plus a `decision` ledger row
  with `outcome=rejected`.

## 4. Scheduler

- `interval_days`: decision dates are `anchor_date + k*every_days`
  (k >= 0); `decision_at = date + decision_time_utc`.
- `daily`: every calendar date at `decision_time_utc`.
- `calendar`: explicit list; `flatten_at` per entry (Engine B: KRX close
  and US cash open from the frozen `configs/engine-b/trading_calendar.json`,
  converted by the producer).
- The decision **window** is `[decision_at, decision_at + signal_grace_secs]`.
  Inside the window the runtime re-reads the signal file every tick until
  it validates; if the window closes without a valid file the key is marked
  `skipped` (positions stay as they are — a stale book is safer than a
  book built from a stale signal) and a WARN is logged.
- `flatten_after_secs` / `flatten_at`: when reached and the book is not
  flat, the runtime rebalances to the empty vector with `reduce_only`
  orders. This is an internal decision (`outcome=flatten`), no file
  required.
- Restart safety: `state.json` carries `last_decision` (key + outcome +
  applied hash + rounded target quantities + `flatten_at`), written and
  persisted **before the first order of a decision is sent**, so a restart
  inside a window (or mid-execution) neither re-applies nor double-trades. A `partial`
  decision is retried from the **persisted target quantities**, never by
  re-reading the producer file: one decision key stays tied to one
  accepted vector even if the file is rewritten or removed, and the
  retries are bounded by `max_attempts` inside the window.
- An overdue flatten (`flatten_at` passed, book not flat) is processed
  before any decision — including after a restart that lands past the
  *next* decision time. The schedule does not advance onto a new key while
  the previous key's flatten is still pending.

## 5. Rebalancer (pure, unit-tested)

Inputs: target weights, gross notional, current signed positions,
mid prices, venue lot metadata (`size_decimals`, `min_order`).
Output: ordered list of `OrderIntent { symbol, side, qty, notional_usd,
reduce_only }`.

1. `target_qty_i = round_down(w_i * gross / price_i, size_decimals_i)`.
2. Caps are checked on the **rounded** targets: `sum |target notional| <=
   max_gross_usd`, `|sum target notional| <= max_net_usd`, per-symbol
   `|target notional| <= max_symbol_weight * gross (+ one lot)`. Any
   violation rejects the whole plan (`outcome=rejected reason=cap_…`);
   there is no partial application.
3. `diff_i = target_qty_i - current_qty_i`. A zero target closes the whole
   leg `reduce_only` regardless of size (dust is never left behind by
   design; if the venue rejects it the residual is reported, not hidden).
   Otherwise diffs with `|diff*price| < rebalance_deadband_usd` or
   `< min_order_usd` are dropped and listed as `skipped`.
4. A sign flip becomes two intents: close the existing leg `reduce_only`,
   then open the new one.
5. Ordering: all reducing intents first (largest notional first), then
   opening intents (largest first) — margin is freed before it is used.

## 6. Execution and fill confirmation

- Live: before sending, the current mid must still be within
  `slippage_bps` of the intent's sizing price in the adverse direction,
  otherwise the intent errors out unsent and the residual is re-planned
  next tick. The order goes out as `create_order_taker_ioc(symbol, qty,
  side, slippage_bps, reduce_only)`; on a venue without a price-capped IOC
  (Lighter in dex-connector v4.7.20, bot-strategy#918) it is sent as
  `create_order(price=None)` with the venue's ±20 % protection price
  **only if** `execution.allow_venue_protection_fallback` is true, else
  it is not sent. The HTTP 200 is **not** a fill (Lighter: accepted, not
  executed — bot-strategy#875 G-2). The runtime polls `get_positions()`
  for up to `fill_confirm_timeout_secs`; the filled quantity is the change
  in the venue position. Partial fills re-plan the residual up to
  `max_attempts`; an unfilled residual after the last attempt is logged
  `[REBALANCE] residual …` and carried in `state.json` as
  `pending_residual` so the next tick within the window retries, and the
  dashboard shows it.
- Live position adoption: every tick the venue position is the source of
  truth; any leg whose venue quantity differs from the book (opened or
  changed elsewhere, or by a crashed previous run) is adopted at the
  venue's quantity **and** average entry price (mid when the venue reports
  none) and logged `[ADOPT]`; the stored leg's funding is settled up to
  that instant first. A leg the venue no longer holds is dropped. The
  runtime subscribes prices for the universe plus every persisted leg, and
  a leg adopted outside that set is priced from the venue ticker (60 s
  cache) so a reduce-only close can always be planned.
- DRY_RUN: the paper book lives in `state.json`; fills are at
  `mid * (1 +/- paper_slippage_bps)` with `paper_fee_bps`; quantities are
  rounded the same way as live, so paper and live share every code path
  except the venue call.
- Every attempt writes a `fill` ledger row (intent, requested, filled,
  price, venue/paper, latency, attempt) and a `rebalance_summary` row
  closes the decision.

## 7. Portfolio risk

- **Kill switch** (`risk.kill_switch_path` exists): no opening intents;
  reducing intents and flattens still run.
- **Venue equity unavailable** (live `get_balance` fails or returns a
  non-positive number): the tick does not anchor, roll over, or evaluate
  the rails at all (a stale number must never become an anchor) and blocks
  every opening intent (`order_blocked
  reason=equity_unavailable`, `book.equity_ready=false` in `status.json`)
  until a fresh value is read; reductions and flattens still run.
- **Session drawdown halt**: `session_start_equity - equity >
  max_session_loss_bps/1e4 * session_start_equity` (equity = venue equity
  live, `equity_reference + cum realized + unrealized` paper) engages a
  sticky halt: flatten the book with reduce-only orders, block every
  opening intent, publish `session_halted=true`, and stay halted across
  restarts until the `risk_ack_path` file is consumed (bot-strategy#932
  semantics: the flatten fills are booked as exit fills from venue fills,
  not from the mid). Consuming the ack re-anchors both the session and the
  daily window at the current equity, and therefore only happens while the
  venue equity is fresh (live) — with `equity_ready=false` the ack file is
  left in place and the halt stays. A stray ack found while not halted
  is deleted without effect, so it can never clear a *future* halt.
- **Daily loss halt**: realized + unrealized loss since 00:00 UTC beyond
  `max_daily_loss_bps` blocks opening intents until the next UTC day; no
  flatten.
- **Funding**: the runtime books an *estimated* funding accrual per leg
  from the venue's current funding rate × notional × hours since the last
  accrual, once per UTC day at the mark **and** right before any fill
  touches the leg, so a leg closed between marks (fixed-window exits) is
  not left unaccounted. Realized funding from the venue ledger is a
  producer-side reconciliation, not a runtime input.

## 8. On-disk outputs

- `state.json` (atomic write): positions, `last_decision`, session/daily
  risk state, `pending_residual`, cumulative realized / funding, peak
  equity.
- `ledger.jsonl`: `decision`, `order_intent`, `fill`, `rebalance_summary`,
  `flatten`, `halt`, `adopt` rows; every row carries `instance_id`,
  `ts_ms`, and the decision key it belongs to.
- `pnl.jsonl`: one `mark` row per UTC day (equity, realized, unrealized,
  funding estimate, per-symbol marks) plus one `exit` row per closed leg.
- `status.json`: the flat `debot-dashboard` schema (`id`, `dry_run`,
  `has_position`, `positions`, `pnl_total`, `pnl_today`,
  `kill_switch_active`, `trade_stats`) plus a nested `book` block
  (`next_decision_at`, `last_decision`, `signal_status`, `gross_usd`,
  `net_usd`, `session_halted`, `pending_residual`). Mirrored to S3 when
  `STATUS_S3_BUCKET` / `STATUS_S3_KEY_PREFIX` are set.
- Prometheus (`PROM_LISTEN`): `book_gross_usd`, `book_net_usd`,
  `book_position_count`, `book_equity_usd`, `book_session_halted`,
  `book_signal_age_seconds`, `book_decision_total{outcome}`,
  `book_order_total{result}`, `book_config_info{fp}`, plus the shared
  `debot_process_start_timestamp_seconds` / `debot_version_info`.

## 9. Replay

`book-runtime --config <yaml> --replay <dir> --out <dir>` runs the same
engine against `bars.jsonl` (`{"date","symbol","close"[,"funding_rate_hourly"]}`
rows; previous `state.json` / ledgers / status in `--out` are removed
first so a rerun never resumes or appends), an optional `lots.json` (`{"SYM": {"size_decimals", "min_order_qty"}}`,
default 4 decimals) and `signals/<key>.json` files with a synthetic clock:
for every bar date `D` the closes of `D` become the prices, each decision /
flatten scheduled inside `D` (a midnight decision belongs to the date it
starts) is ticked at its exact time (paper fills at the close of `D`), and
after the final tick at `D 23:59:59` the daily mark labelled `D` is written
(replay never marks at a decision tick, so funding intervals line up with
the dates whose rates they use). The config's paths are redirected into `--out` and `dry_run` is
forced on. Given identical inputs `ledger.jsonl`, `pnl.jsonl` and
`state.json` are byte-identical run to run (covered by a test), which is
what the shadow → live comparison for XSMOM relies on.

Signal fixtures are written with `scripts/book_signal_file.py` (the same
helper producers import), e.g.

```bash
scripts/book_signal_file.py --out replay/signals/2026-07-03.json \
  --producer xsmom_695_L28_H5_q20_riskadj --decision-key 2026-07-03 \
  --as-of 2026-07-03T00:00:00Z --generated-at 2026-07-03T00:20:00Z \
  BTC=0.0625 ETH=-0.0625 ...
```

`generated_at` must fall inside `[decision_at - max_age_secs, decision_at
+ 60 s]` for the replay clock to accept it.

## 10. Live gate and hosts

- `dry_run: false` refuses to start unless `BOOK_CONFIRM_LIVE=yes-i-mean-it`
  is also set (two-variable rule, same as `engine_b_live`).
- Live execution is **Lighter-only** for now: orders go out as
  `create_order(price=None)` (venue-native market/IOC with Lighter's 20 %
  protection price — bot-strategy#918 tracks a price-constrained IOC).
  dex-connector v4.7.20's Hyperliquid `create_order_taker_ioc` is
  spot-only, so `venue: hyperliquid` is DRY_RUN-only until a perp IOC path
  exists there; the runtime refuses `dry_run: false` on any other venue.
- SIGTERM does **not** reduce-only close the book (same as `engine_b_live`);
  the state is persisted and the next start resumes from it (live: from
  the venue position, which is adopted if it differs).
- Binary: `ci.yml` builds and uploads `bin/book_runtime` to S3 next to
  `engine_b_live`; host installation (unit, identity, secrets) is the
  follow-up PR together with the XSMOM producer.
- Building this runtime is **not** capital approval. XSMOM goes live only
  if the pre-registered 2026-10-02 readout on bot-strategy#695 passes;
  Engine B only after #876's Phase 1 gate.
- Hosts: Tokyo (`i-0095af4fe0efbc5dd`) or a new instance. Never Frankfurt.

## 11. Engine B on this runtime — decision

Engine B fits the shape: one traded symbol (`SNDK`), one decision per
session day at KRX close (`t1`), fixed-window exit at US cash open (`t2`),
target weight `{SNDK: ±1}` or `{}`. On this runtime it is
`schedule.kind: calendar` with entries generated from
`configs/engine-b/trading_calendar.json` (`decision_at = t1`,
`flatten_at = t2`), `flatten` handled by the scheduler, and a producer
that computes ε from the Phase 0 observer's SQLite (mid at `t0`/`t1`) and
writes `{SNDK: sign(ε)}` when `|ε| >= threshold`, else `{}`.

What does **not** carry over 1:1 from `engine_b_live`: the in-process WS
capture of `t0`/`t1` prices (the producer owns that), the same-day
`orderBookDetails` eligibility gate (moves to the producer, which can
emit `{}` when the symbol is `force_reduce_only`), and the exact
`entry_deadline_secs` / `exit_deadline_secs` semantics (here:
`signal_grace_secs` and the flatten retry loop). `engine_b_live` stays as
the 2026-09-10 smoke-test binary; migrating Phase 1 (#876) onto this
runtime is a follow-up decision once #876's gate is reached — the
required pieces (calendar schedule, flatten, single-symbol book) are
implemented here so that decision is not blocked on the runtime.
