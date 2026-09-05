# Engine B order-path specification (A-3 / A-8 / A-9)

Resolution record for bot-strategy#875 (requirements doc
`engine_b_requirements_0.3.md` -- an attachment on bot-strategy#866, not a
file in any repo -- §3 items A-3, A-8, A-9 and §10 TBD-6/7).
All three are pure technical-correctness items the 2026-09-02 pivot
(bot-strategy#866, "方針転換の記録") explicitly kept in scope for the
live-smoke track, so this document is written against **what
`src/bin/engine_b_live.rs` actually does today through `dex-connector`'s
Lighter implementation**, not against the Phase 1/2 design the
requirements doc describes. Where the two differ, the gap is listed in
§4 with an impact call for the `$100` smoke test.

Sources, all read 2026-09-04:

- Lighter official docs: `apidocs.lighter.xyz/docs/rate-limits`,
  `.../docs/trading`, `.../docs/websocket-reference`
- Lighter mainnet `GET /api/v1/orderBookDetails` (public, fetched
  2026-09-04 ~18:00 UTC)
- `dex-connector` at the tag `pairtrade` pins (`v4.7.18`; code paths
  read on a local checkout and then diffed against the tag — the only
  order-path difference was the `sendTx` response classification, which
  is what §2.2 describes):
  `src/lighter_connector/{dex_impl,orders,order_payload,protocol,
  market_cache,parsing,rest,ticker,ws}.rs`, `src/lighter_waf_cooldown.rs`,
  `src/ws_reconnect.rs`, `crates/lighter-ratelimit/src/{bucket,client,
  weights}.rs`
- `engine-b-live.service` journal on `debot-robinhood-lighter`
  (`i-0095af4fe0efbc5dd`), 2026-09-03 15:30 .. 2026-09-04 18:00 UTC

Use UTC throughout.

## 1. A-3: price / size decimals, minimum order size

### 1.1 Live values (Lighter mainnet, 2026-09-04)

| symbol | market_id | price_decimals | size_decimals | min_base_amount | min_quote_amount | last price | role in `engine-b-live` |
|---|---|---|---|---|---|---|---|
| SNDK | 139 | 2 | 4 | 0.0050 | 10 USD | 1715.49 | `us_primary` (the only traded symbol) |
| SKHY | 216 | 2 | 4 | 0.0500 | 10 USD | 173.41 | `kr_primary` (signal only, never ordered) |
| SKHYNIXUSD | 161 | 3 | 3 | 0.007 | 10 USD | 1257.494 | KR candidate (#872 recommends over SKHY) |
| MU | 164 | 2 | 4 | 0.0100 | 10 USD | 998.52 | US candidate |
| SOXL | 197 | 2 | 4 | 0.0500 | 10 USD | 115.77 | control symbol |
| NVDA | 110 | 3 | 3 | 0.035 | 10 USD | 230.421 | control symbol |
| EWY | 166 | 2 | 4 | 0.0350 | 10 USD | 186.06 | control symbol |
| USDKRW | 105 | 2 | 4 | 0.0050 | 10 USD | 1345.18 | control symbol |

Same values appear in the service's own startup log as
`[MARKET_INFO] Loaded market <symbol>: price_decimals=.. size_decimals=..
min_order=Some(..)` (`min_order` is `min_base_amount`), so the on-host
truth can always be re-read from `journalctl -u engine-b-live` without a
REST call.

Per the official docs, `min_base_amount` / `min_quote_amount` "apply only
to maker orders — whichever is higher is enforced". `engine-b-live` only
sends taker (IOC) orders, so neither minimum is exchange-enforced on its
path; they are still the right floor to design against.

### 1.2 How the engine sizes an order

`maybe_enter` computes `size = notional_usd / latest_price[us_primary]`
as `f64` with `notional_usd = min(lot_usd, equity_usd_reference *
leverage * 0.9)`, formats it to 8 decimals, and hands the `Decimal` to
`DexConnector::create_order`. The connector then:

1. resolves `MarketInfo` (decimals, `market_id`) from its startup cache
   (`market_cache.rs::resolve_market_info`);
2. scales size to Lighter's integer `base_amount` with
   `RoundingStrategy::ToZero` at `size_decimals` (`dex_impl.rs`,
   `scale_decimal_to_u64`) — i.e. **truncates, never rounds up**;
3. if truncation yields `0` for a non-zero size, forces `base_amount = 1`
   (one size-tick) rather than failing;
4. scales the price with `MidpointAwayFromZero` at `price_decimals`.

Worked example at today's config (`lot_usd=100`, SNDK 1715.49):
`100 / 1715.49 = 0.058298…` → truncated to 4 dp → `0.0582 SNDK`
(≈ $99.84). This is 11.6× `min_base_amount` and 10× `min_quote_amount`,
so the smoke-test lot clears both minimums with margin. The minimums
would only bind if SNDK traded above ~$20,000 (`0.005 × price > $100`).

The engine itself performs **no** `min_base_amount` / `min_quote_amount`
check and no decimals-aware rounding of its own; it relies entirely on
the connector's truncation + the exchange's rejection. That is
acceptable for a fixed `$100` lot on a `$1,715` symbol; it is not
acceptable as-is for the requirements doc's `Q_gate`-sized Phase 2
notional on a thinner symbol. See §4 G-7.

### 1.3 Dynamic re-fetch at startup (A-3's second clause)

- `DexConnectorBox::create("lighter", …)` → `LighterConnector::start` →
  `ensure_market_metadata_loaded` → `refresh_market_cache` (REST
  `orderBookDetails` + `orderBooks`, weight 300 each) on **every process
  start**, with 10 attempts and exponential backoff capped at 60 s
  (~8 min total) so a WAF cooldown at boot does not crash-loop the unit.
- There is **no periodic refresh** after startup. A Lighter-side change to
  `supported_price_decimals` / `supported_size_decimals` /
  `min_base_amount` mid-run is only picked up by a service restart.
  Lighter has not changed these for any of the eight tracked symbols
  between 2026-09-02 01:00 and 2026-09-04 18:13 UTC (every 12th hourly
  partition of the Phase 0 observer's `market_metadata` table,
  `lighter_mainnet_context` venue, decoded from `public_json`: exactly one
  distinct `(supported_price_decimals, supported_size_decimals,
  min_base_amount, min_quote_amount, status)` tuple per symbol), so a
  restart-to-refresh policy is adequate for the smoke test.
- The engine's own same-day eligibility gate (`fetch_order_book_details`,
  bot-strategy#872 / pairtrade#266) re-reads `orderBookDetails` once per
  entry window but only consumes `status` / `force_reduce_only` /
  `daily_quote_token_volume`; it does not feed decimals back into the
  connector.

**A-3 status: resolved for the smoke-test scope** (values recorded,
sizing path verified, startup re-fetch confirmed). Reopen at Phase 2
sizing (§4 G-7).

## 2. A-8: API rate limits, WS connection limits, REST retry

### 2.1 Official limits (apidocs.lighter.xyz/docs/rate-limits)

REST, per rolling minute:

| tier | limit |
|---|---|
| Standard | **60 requests** (not weighted; additionally capped where `premium_weighted / endpoint_weight < 60`) |
| Premium / Plus | 24,000 weighted requests |
| Builder | 240,000 weighted requests |

Official endpoint weights (the premium formula): `sendTx` /
`sendTxBatch` / `nextNonce` = **6**, `trades` / `recentTrades` = **600**,
`publicPools` / `txFromL1TxHash` = 50, `accountInactiveOrders` /
`deposit/latest` = 100, everything else (`account`, `orderBookDetails`,
`orderBooks`, `funding-rates`, …) = **300**.

What the connector's sidecar actually charges
(`crates/lighter-ratelimit/src/weights.rs::weight_for`, v4.7.18) is a
coarser table: paths ending in `/sendTx` / `/sendTxBatch` / `/nextNonce`
→ 6, ending in `/trades` → 600, `/changeAccountTier` → 3000, **everything
else → 300** -- so `recentTrades` (does not end in `/trades`),
`publicPools`, `deposit/latest` etc. are all charged 300 by the sidecar
regardless of the official value. Budget arithmetic in §2.3 uses the
sidecar's numbers where the sidecar is involved at all.

Enforcement: exceeding returns **HTTP 429 or HTTP 405**. Cooldown is
60 s static when the firewall trips, `weight / (total_weight / 60)` when
the API server trips (e.g. 750 ms for `account`). **REST and WebSocket
limits are coupled**: "When you're rate-limited on REST, WebSocket
connections also get rate-limited, and vice versa."

WebSocket, per IP: **255 connections**, **500 subscriptions per
connection**, **200 client messages per minute** (excluding
`sendTx`/`sendTxBatch`), **50 in-flight messages**. Keepalive: the client
must send at least one frame every **2 minutes**.

`engine-b-live`'s Lighter account is assumed **Standard** (the same tier
every other bot in this repo runs on; nothing in its provisioning changed
tier). Design against 60 req/min.

### 2.2 What the connector does

- **Host-shared sidecar** (`lighter-ratelimit.service`, UDS
  `/run/lighter-ratelimit/lighter-ratelimit.sock`, mode `0666`, active on
  the Tokyo host; `engine-b-live.service`'s `ProtectSystem=strict` does
  not block a Unix-socket `connect`). Every connector REST call acquires
  `weights::weight_for(endpoint)` from it before firing. Policy:
  `sendTx` (create / cancel / modify) and `nextNonce` use
  `Wait { max_ms: 5000 }` (nonce: 3000 ms); generic reads use `Shed`
  (fail fast with `DexError::RateLimited`). If the sidecar is unreachable
  the client logs once and falls back to an in-process bucket with the
  same parameters.
- **The sidecar bucket is 60,000 weight / min, refilling 1,000 / s**
  (`bucket.rs`, and the unit's `LIGHTER_RATELIMIT_CAPACITY=60000`). That
  models a weighted premium-style bucket, **not** the Standard tier's 60
  unweighted requests / min: at weight 6 it would admit 10,000 `sendTx`
  per minute. For a Standard account the sidecar is therefore a
  best-effort smoother, not the binding limiter. See §4 G-5.
- **Reactive protection** (`lighter_waf_cooldown.rs`): a 429 / 405 / WAF
  challenge engages a **90–120 s host-shared cooldown** (deadline stored
  on disk so every Lighter process on the host fails fast with
  `DexError::RateLimited` instead of refreshing the WAF window). This is
  the mechanism that actually protects a Standard account once a burst
  has happened. Tunables and file locations are documented once, in
  dex-connector's `README_LIGHTER.md` ("Rate Limit / WAF 対策の環境変数");
  this document deliberately does not repeat them.
- **Observability**: `[API_TRACKER] #N <METHOD> <endpoint> | Recent calls
  (60s): k | Rate: k/min` on every REST call, with an extra warning line
  above 45 calls / 60 s.
- **Retry conditions**: the connector itself does **not** retry a failed
  `sendTx` / `nextNonce` / `account` call. A `sendTx` response is
  classified (v4.7.18, `orders.rs`): 429 / 405 / WAF challenge →
  `DexError::RateLimited` and the host-shared cooldown is engaged (nonce
  cache untouched); any other 4xx except 408 / 421 / 425 →
  `DexError::ServerResponse` (a genuine rejection such as insufficient
  margin or a bad size — do not blindly resend) with the nonce cache
  invalidated; 5xx, 408 / 421 / 425 and transport errors →
  `DexError::Transient` with the nonce cache invalidated so the next
  attempt re-fetches it. Startup market metadata is the one path with a
  built-in retry loop (§1.3). Whether to retry is the caller's decision;
  `engine_b_live.rs` re-attempts entry on the next 5 s tick until
  `entry_deadline_secs` (180 s) and re-attempts exit on every tick until
  it succeeds (see §4 G-4 for why that matters).

### 2.3 What `engine-b-live` actually consumes

Measured on the host, current binary (pairtrade `7ba80fb`):

| window | REST calls | detail |
|---|---|---|
| 2026-09-04 11:27 → 14:59 UTC (3.5 h, one process lifetime) | **3** | `orderBooks`, `orderBookDetails`, `funding-rates`, all at startup |
| steady state | 0 / min | prices, positions, fills all arrive over WS |

Per-boundary REST cost of the order path, worst case:

| step | REST calls | weight | note |
|---|---|---|---|
| eligibility gate (`fetch_order_book_details`) | 1 | 300 (official) / **not charged** | raw `reqwest` in the engine, **outside the sidecar budget**; only the reactive cooldown applies; once per entry window |
| `get_ticker` inside `create_order(price=None)` | 0–1 | 600 (official) / **not charged** | served from the WS price cache when < 30 s old; REST fallback is `recentTrades` via `fetch_text_with_waf_guard`, which checks the WAF cooldown but **never calls `acquire_rest_budget`** -- so this path, like the eligibility gate, has no local pacing at all until a 429 actually comes back. It only fires when the WS price is stale, i.e. when the WS is already unhealthy: the two unbudgeted paths on this binary are exactly the ones that run under stress |
| `nextNonce` | 0–1 | 6 | cached with TTL; refetched after any `sendTx` failure |
| `sendTx` (entry) | 1 | 6 | |
| exit: same as entry minus the eligibility gate | 1–3 | | |

Total ≤ ~8 requests per trading day against a 60 / min budget. The shared
IP also carries the three Robinhood arms and the Phase 0 observer; the
sidecar's `stats 60s` line on 2026-09-04 shows `tokens_remaining≈59,430
/ 60,000` at rest, i.e. the whole host is nowhere near the weighted
budget either.

WS footprint: **one connection**, 13 subscriptions (`account_all/{idx}`
+ `order_book/{id}` × 6 + `market_stats/{id}` × 6) against 500 / conn.
Keepalive: idle client ping every 20 s, pong timeout 15 s × 2 misses,
60 s stall timeout → reconnect with exponential backoff (base 1.5,
capped 60 s, jittered; `ws_reconnect.rs::lighter()`), comfortably inside
the 2-minute rule. Observed: 4 reconnects in ~27 h, all
`ResetWithoutClosingHandshake` from Lighter's edge, all self-recovered
within seconds, positions re-snapshotted from `account_all` on resume.

**A-8 status: resolved for the smoke-test scope.** The engine's call
volume is two orders of magnitude below the Standard limit; the real risk
is not the limit but the mis-modelled sidecar (§4 G-5), which only
matters if a future change adds REST polling (e.g. a reconcile loop).

## 3. A-9: order semantics (reduce-only, post-only, IOC/FOK, cancel, client order ID)

### 3.1 Protocol enums (official) vs connector constants

| concept | Lighter value | `protocol.rs` constant | exposed by `create_order`? |
|---|---|---|---|
| ORDER_TYPE_LIMIT | 0 | `ORDER_TYPE_LIMIT = 0` | yes (`price = Some(..)`) |
| ORDER_TYPE_MARKET | 1 | `ORDER_TYPE_IOC = 1` (same value, connector's name) | yes (`price = None`) |
| STOP_LOSS / SL_LIMIT / TP / TP_LIMIT | 2 / 3 / 4 / 5 | `ORDER_TYPE_TRIGGER = 2` (+ literals 3/4/5) | only via `create_advanced_trigger_order`, unused by Engine B |
| TWAP | 6 | — | no |
| TIF IMMEDIATE_OR_CANCEL | 0 | `TIF_IOC = 0` | only together with MARKET |
| TIF GOOD_TILL_TIME | 1 | `TIF_GTT = 1` | yes (default for `price = Some`) |
| TIF POST_ONLY | 2 | `TIF_POST_ONLY = 2` | yes (`spread = Some(-2)`) |
| FOK | **does not exist on Lighter** | — | no |
| reduce_only | bool in the signed tx | `OrderPayload.reduce_only` | yes (passed through verbatim) |

### 3.2 `create_order` mapping as used by `engine_b_live.rs`

`submit_order` calls `create_order(us_primary, size, side, price=None,
spread=None, reduce_only, expiry=None)`. With `price = None` the
connector builds a **MARKET (1) + IOC (0)** order whose `price` field is
a *protection price* = last ticker price × 0.8 (sell) / × 1.2 (buy);
Lighter treats a market order's price as the worst acceptable price and
"the sequencer cancels if better terms unavailable". `order_expiry` is
`0` (nil) for IOC, form field `price_protection=false`.

Other mappings (not used by Engine B today, recorded for §5.2 / §6.3
work):

- `price = Some(p)` → LIMIT + GTT, expiry 24 h unless `expiry_secs` given.
  Lighter requires GTT expiry between **5 minutes and 30 days**.
- `spread = Some(-2)` → LIMIT + POST_ONLY (venue rejects a crossing
  order instead of executing as taker).
- `spread = Some(-1)` ("IOC" sentinel) is **degraded to GTT** by the
  connector. **A limit-price IOC with an explicit collar is not reachable
  through `create_order` for Lighter**; `create_order_taker_ioc` returns
  `Permanent("not implemented")` for this connector. Getting §6.3's
  "marketable limit, IOC, ≤ 50 bps from mid" would need a connector
  change (LIMIT + TIF_IOC is a legal Lighter combination). See §4 G-3.

### 3.3 reduce-only

The flag is signed into the tx and enforced by Lighter (an order that
would open or flip a position is rejected / clipped exchange-side; the
docs page fetched does not spell out reject-vs-clip). The engine sets
`reduce_only=true` on every exit and `false` on entry. Because the
engine's exit size is its *own* record of the entry size (see §4 G-2),
reduce-only is currently the only thing preventing an over-sized exit
from opening the opposite position — treat it as a load-bearing safety
property, not a nicety.

### 3.4 Cancel

- `cancel_order` → signed `tx_type=15` with `market_id`, `order_index`,
  fresh nonce; `Wait` budget policy. The `order_index` the connector sends
  is the numeric `order_id` it returned from `create_order`, which is the
  **`client_order_index`** (see 3.5). The official docs state a cancel may
  reference "either `order_index` or the same `client_order_index`", so
  this is valid.
- `cancel_all_orders` / `cancel_orders` are **client-side fan-outs** over
  the WS-fed `cached_open_orders` map (one `tx_type=15` per order); no
  native cancel-all tx is used. `get_open_orders` is **WS-cache only, no
  REST fallback** — after a reconnect the cache is rebuilt from the
  `account_all` snapshot.
- A cancel is acknowledged by HTTP 200 (= accepted by the API server, not
  proof the order is gone); terminal confirmation is the WS order update.
  The requirements doc's "cancel-confirm-replace" therefore needs a WS
  round-trip, not just the REST ack.
- Engine B sends **no cancels today** (IOC only, nothing rests).

### 3.5 Client order ID, nonce, response semantics

- `client_order_index` is a `uint48` the client picks. The connector uses
  `Utc::now().timestamp_millis()` (fits `uint48` until year 10889) and
  returns it as both `order_id` and `client_order_id`. It is **not
  persisted** anywhere and uniqueness across restarts is only guaranteed
  by the clock; the requirements doc's §6.5 `order_intent` journal
  (persist-before-send, UNIQUE constraint) is **not implemented**. Two
  orders in the same millisecond would collide — impossible for Engine B
  (one order per boundary), relevant for any future multi-order path.
- Nonce: "handled per API key", strictly `old + 1` (skipping ahead is
  allowed up to `2^47 - 1`). The connector fetches `nextNonce` once,
  caches `next_nonce` with a TTL, increments locally per tx, and drops the
  cache on any `sendTx` failure other than a rate-limit response (a 429 /
  405 never consumed the nonce). There is no on-disk nonce; a restart
  simply re-fetches. This satisfies A-9's "nonce は API key ごと" clause
  as long as **only one process uses this API key** — the dedicated
  `engine-b-live` account/key is not shared with the Robinhood arms
  (separate `live-secrets.env`, separate account index), so this holds.
- `sendTx` HTTP 200 means "accepted by the API servers … does not
  guarantee the execution of your order". `CreateOrderResponse.ordered_size`
  is the **requested** `base_amount`, not a fill. Fill evidence arrives on
  WS `account_all.trades` as `[FILL_DETECTION] Trade detected:
  order_id=<client_order_index> size=.. price=..` and is queryable via
  `get_filled_orders(symbol)`; the position itself via `get_positions()`
  (WS-fed; returns `Transient("positions not ready")` until the first
  `account_all` snapshot after connect).

**A-9 status: resolved as a specification** (every semantic the doc asks
for is now pinned to a concrete Lighter value and connector code path).
Two of the findings are execution gaps for the smoke test — G-2 and G-4
below.

## 4. Gaps vs the requirements doc, with smoke-test impact

| id | gap | where | impact at `$100` / SNDK | recommendation |
|---|---|---|---|---|
| G-1 | `set_leverage` is a **no-op** on the Lighter connector (`dex_impl.rs`: logs at debug, returns `Ok`). `ENGINE_B_LIVE_LEVERAGE=2` only feeds the notional cap (`equity × leverage × 0.9`); the exchange applies its per-market default margin (SNDK `default_initial_margin_fraction=666`, `maintenance=300`, raw units — interpret under A-5 / #877). | entry | none: $100 notional on ~$1,000 equity is far below any margin bound | document; do not read `leverage=2` as an exchange setting |
| G-2 | **Fill was assumed on HTTP 200.** `OpenPosition.size` = requested size, `entry_price` = last WS mid. If the IOC filled partially or not at all, the engine held a phantom position, logged a fictitious PnL, and sent a reduce-only exit sized to a position that might not exist. | entry → exit | real for the smoke test — exactly the class of bug the test is meant to surface | **Addressed in pairtrade#275 (open, not yet merged -- the running binary still has this gap)**: after an accepted IOC the engine polls the WS-fed `get_positions()` for up to `ENGINE_B_LIVE_FILL_CONFIRM_TIMEOUT_SECS` (15 s) and records the exchange's side / size / entry price; no position → treated as unfilled, no retry that day; exits are sized to the exchange's current position and only complete once it reports flat. Verify on the first live cycle. |
| G-3 | Entry/exit is MARKET+IOC with a **±20 % protection price**, not the doc's "marketable limit ≤ 50 bps from mid" (§6.3). No limit-IOC path exists in the connector for Lighter. | entry, exit | low: SNDK does ~$15 M/day with ~1.3 bps spread; observed top-5 depth ≈ $83 k vs a $100 order | accept for the smoke test; open a dex-connector item (LIMIT + TIF_IOC) before any Phase 2 sizing |
| G-4 | **No idempotency journal.** A `sendTx` that timed out after Lighter accepted it returned `Transient`; `day.entered` stayed `false`, and the next 5 s tick re-submitted → possible double entry (2 × notional). §6.5 (persist intent, then send; on timeout query by client ID) is unimplemented. A position check alone does not close this: REST and WS rate limits are coupled (§2.1), so the same stress that timed out the `sendTx` can delay the WS `account_all` update, and a single `get_positions()` read right after the timeout can be a false negative. | entry | bounded (2 × $100), but a correctness hole | **Addressed in pairtrade#275 (open, not yet merged) by construction: at most one entry `sendTx` per session day.** Any `submit_order` outcome (Ok, `Transient`, `RateLimited`, `ServerResponse`) ends the day's submitting; after an error the engine still polls the exchange position for the full confirm window and adopts a position if one appears. The exchange position is also read before the single submit (catches a position left by a crashed prior process). A persisted `order_intent` journal remains Phase 2 work. |
| G-5 | Rate limiter sidecar models a 60,000-weight/min bucket; Standard tier is 60 req/min unweighted. The connector's real Standard-tier protection is reactive (429 → 90–120 s cooldown) plus the `[API_TRACKER]` warning at 45/60 s. | REST | none at ≤ 8 req/day | do not add REST polling loops to this binary without revisiting; if `reconcile` polling is added for G-2, poll ≤ 1/s and prefer the WS-fed `get_positions()` |
| G-6 | Startup / reconnect reconcile (§6.4) is not implemented: the engine does not compare its state file with exchange positions at boot and does not cancel unknown open orders. `OpenPosition` is in-memory only (already in the binary's KNOWN GAPS). | boot | matters only if the service restarts between entry and exit | keep the documented manual rule (check the exchange before trusting `status.json`); Phase 2 item |
| G-7 | No `min_base_amount` / `min_quote_amount` / decimals guard in the engine; sizing relies on connector truncation, and the connector silently substitutes `base_amount = 1` (one size tick) when truncation yields zero rather than refusing — on a thin symbol that is a different order than the one intended, not a rejection. | entry | none at $100 / SNDK (11× the minimum) | add an explicit floor check (and reject a zero-after-truncation size) when `lot_usd` becomes `Q_gate`-driven |
| G-8 | Lighter's reduce-only semantics for an order larger than the position (reject the whole order vs clip to the position) are **not documented on the pages read and not yet observed live**. Before #275 this was the only guard against an over-sized exit flipping the position. | exit | after #275 reduce-only is no longer load-bearing (exits are sized to the exchange position), but the exchange behaviour is still unverified | observe on the first live exit; if a partial exit ever leaves a remainder, the next tick's re-read + re-send covers it either way |

G-2 and G-4 were the two worth a code change before the first
`CONFIRM_LIVE` flip; pairtrade#275 (open at the time of writing -- check
its merge state and the deployed binary's commit before relying on this)
addresses both. The rest are recorded
here so the requirements doc v0.4 (bot-strategy#879) can mark A-3/A-8/A-9
✅ with these caveats instead of ❌.

## 5. Quick reference for the operator

- Sizing sanity before a flip: `lot_usd / price(us_primary)` truncated to
  `size_decimals` must be ≥ `min_base_amount` and × price ≥ $10. Read
  both from the `[MARKET_INFO]` line for `us_primary` in the current
  journal.
- After an entry, the lines to expect, in order:
  `[ENTRY] side=… notional=$… size=…` → `[API_TRACKER] … POST
  /api/v1/sendTx` → `✅ [FILL_DETECTION] Trade detected: order_id=<ms
  timestamp> size=… price=…` → `Updated cached positions: 1 positions`.
  A missing `[FILL_DETECTION]` after a live `[ENTRY]` is G-2 happening.
- A `[Lighter rate-limit] cooldown engaged for …s` line means a 429/405
  hit somewhere on the host (shared IP); every Lighter REST call from
  this process fails fast until the deadline. WS keeps running.
- `[lighter-ratelimit] sidecar unreachable` means the process fell back
  to its in-process bucket for its lifetime; harmless at this call volume.
