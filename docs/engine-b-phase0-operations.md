# Engine B Phase 0A operations

This runbook covers the read-only observer for bot-strategy#866 / Project 8.
Use UTC for all timestamps.

## Safety boundary

`engine_b_phase0.py` has no Lighter SDK, signer, account-authentication, or
order module. Its outbound WebSocket allowlist contains only public
`subscribe`, `unsubscribe`, `ping`, and `pong` messages. The service runs as
the dedicated `engine-b-phase0` Unix identity from a root-owned runtime copy;
its systemd mount namespace makes `/opt/debot` inaccessible. It therefore
cannot read the Robinhood `freq2` credentials owned by the trading identity.
`collector_manifest.order_capability` and the Prometheus gauge
`engine_b_phase0_order_capability` must both remain `0`.

The observer collects a single venue, `lighter` (mainnet Lighter,
`mainnet.zklighter.elliot.ai`) -- the intended future execution venue and the
complete requirements-v0.3 context universe at once, including `EWY` and
`USDKRW`. This replaces an earlier two-venue plan (`robinhood` +
`lighter_mainnet_context`): Robinhood Lighter (`api.rh.lighter.xyz`) did not
list `EWY`/`USDKRW`, which would have required either combining two venues'
data into one regression (needing a new, reviewed analysis-plan version) or
waiting on Robinhood to list them. Targeting mainnet Lighter directly removes
the gap outright -- every required same-venue control is already there, so
there is no cross-venue combination to justify or defer.

A-7 (the verified KRX/US market calendar) is resolved via the frozen
calendar described below, and with mainnet Lighter as the only venue there is
no same-venue-missing-symbols reason either: a `trading_session` row's
`validity_reason` is `None` for a normal, fully-resolved open trading day.

## A-7: KRX/US cash-market trading calendar

`krx_is_open`/`us_cash_is_open` in `trading_session` come from a frozen,
pre-computed session table -- `configs/engine-b/trading_calendar.json` --
generated offline by `scripts/engine_b_trading_calendar_freeze.py` from the
`exchange_calendars` library (`XKRX` for Korea, `XNYS` for the US cash
market; pinned in `scripts/engine_b_trading_calendar_freeze_requirements.txt`).
The observer itself never imports a calendar library: it loads this JSON with
stdlib `json` at startup (`TradingCalendar.load`) and looks up each date by
ISO string. When a date resolves, `trading_session.t0_us`/`t1_us`/`t2_us`
(KRX open, KRX close, US cash open) come from that date's real
`krx_open_utc_us`/`krx_close_utc_us`/`us_open_utc_us`, not a fixed
09:00/15:30 KST or 9:30am America/New_York placeholder -- this matters on
irregular-schedule days such as the delayed open on the first trading day of
a year. `Collector.health_payload()["trading_calendar_version"]` reports the
loaded `calendar_version`, or `null` if no calendar loaded.

`health_payload()["phase0_sample_blockers"]` drops the A-7 line only when
`Collector.calendar_covers_upcoming_sessions()` is true, i.e. the loaded
calendar actually resolves both today and tomorrow (the two dates
`session_loop` writes) -- a calendar object being loaded is not enough once
its committed `range` runs out (currently 2027-12-31); re-freeze with an
extended range before then.

If the frozen file is missing, unreadable, a session entry is malformed, or a
queried date falls outside its committed `range`, the observer falls back to
the original fail-closed placeholder for that date only (`krx_is_open=0`,
`us_cash_is_open=0`, `calendar_version=UNRESOLVED_A7_zoneinfo_only`,
`validity_reason` includes `A7_UNRESOLVED_VERIFIED_KRX_US_CALENDAR`) --
never a crash.

KRX observes no DST (`Asia/Seoul` is a fixed UTC+9), so its session hours are
stable; the US cash market's `America/New_York` session shifts by an hour
across the EDT/EST boundary, which `exchange_calendars` resolves correctly
from the IANA tzdb without any special-casing here.

`exchange_calendars`' recurring-holiday rules can still miss KRX-specific
adjustments announced or scheduled separately from the library's own table.
Two kinds are known so far, each with its own override dict in
`scripts/engine_b_trading_calendar_freeze.py` (a citable primary source is
required per entry, and the generated document records which overrides
applied within a given `--start`/`--end`, in `krx_one_off_closures` /
`krx_delayed_open_one_hour` respectively):

- **Full closures** (`KRX_ONE_OFF_CLOSURES`): an administrative holiday the
  library marked open. 2026-06-03 (Local Election Day) and 2026-07-17
  (Constitution Day, reinstated for 2026) both required this.
- **One-hour delayed open** (`KRX_DELAYED_OPEN_ONE_HOUR`): KRX shifts the
  cash-market session to 10:00-16:30 KST (from the normal 09:00-15:30) on the
  day of the national CSAT exam each year, which the library does not encode
  at all -- it reports the normal hours. 2026-11-19 and 2027-11-18 (the CSAT
  dates falling in the current frozen range) both required this. Korean CSAT
  naming is offset by academic year (a "2027학년도" exam is administered in
  November 2026); resolve the actual calendar date, not the label, before
  adding a new year's entry.

Before trusting a freeze for gate evaluation (G0-8 needs A-7 resolved),
cross-check the covered years against KRX's own published holiday/session
notice and extend the relevant override dict for anything the library still
gets wrong -- both of the above were caught by review, not by this process
catching them itself, so treat the two dicts as a known-incomplete starting
point rather than an exhaustive audit.

**Re-freezing** (extend the covered date range, pick up an
`exchange_calendars` release, or after cross-checking a specific year against
KRX's own published holiday notice -- rule-based generation can miss one-off
administrative holidays):

```bash
python3 -m venv /tmp/calendar-freeze-venv
/tmp/calendar-freeze-venv/bin/pip install -r scripts/engine_b_trading_calendar_freeze_requirements.txt
/tmp/calendar-freeze-venv/bin/python scripts/engine_b_trading_calendar_freeze.py \
  --start 2026-01-01 --end 2027-12-31 \
  --out configs/engine-b/trading_calendar.json
```

Review the diff, then commit it together with the code/requirements change
that motivated it. CI ("Verify Engine B trading calendar freeze") regenerates
the artifact from its own committed `range` and diffs it byte-for-byte
against the committed file, so a hand-edit or an un-recommitted
`exchange_calendars` bump fails the build. `install_engine_b_phase0.sh`
installs it read-only alongside `phase0.json`
(`$INSTALL_DIR/trading_calendar.json`, mode 0440); the deploy workflow ships
it through the same S3 release prefix as the rest of the Phase 0 release.

## A-10: WS sequence, snapshot/delta, server timestamps, gap detection

Resolution record for bot-strategy#874 (requirements doc
`engine_b_requirements_0.3.md` §3 A-10 / §10 TBD-8, and the F0-03 /
F0-07 / F0-08 / F0-10 functional requirements that depend on it). The
observer has been in production since 2026-09-02 with this logic, so this
section pins the requirement wording to the Lighter semantics (official
`apidocs.lighter.xyz/docs/websocket-reference`, read 2026-09-04) and to
the code path in `scripts/engine_b_phase0.py` that implements it, and
states plainly which parts are **not** done online.

### What Lighter guarantees

- `order_book/{market_id}`: `subscribed/order_book` is a full snapshot
  carrying `offset`, `nonce`, `begin_nonce` and `timestamp` (µs);
  `update/order_book` is a delta carrying `begin_nonce`, `nonce`, `offset`.
  Continuity rule (verbatim intent): the current message's `begin_nonce`
  must equal the previous message's `nonce`. `offset` "will increase, but
  it's not guaranteed to be continuous" and "you can expect the offset to
  change drastically on reconnection if you're routed to a different
  server" -- so **`nonce` is the sequence, `offset` is not** (it is stored
  for provenance only). Server time is `last_updated_at`, microseconds.
- `trade/{market_id}`: per-trade `trade_id` / `trade_id_str`, `price` /
  `size` as strings, `timestamp` in **milliseconds**, `transaction_time`
  in microseconds, `ask_id` / `bid_id`, `is_maker_ask`; the message also
  carries a `nonce`. No documented continuity rule for this channel.
- `market_stats/{market_id}`: periodic stats (mark / index / mid, funding,
  volumes) with a message-level `timestamp`; no sequence.
- Keepalive: the client must send at least one frame every 2 minutes.

### How the observer implements it

| requirement | implementation (`scripts/engine_b_phase0.py`) |
|---|---|
| F0-03 receive time, server time, connection session, local sequence, exchange sequence on every event | `book_event`: `ts_recv_us` (monotonic-free wall clock at receive, `now_us()`), `ts_srv_us`, `connection_session_id`, `local_sequence` (per venue/channel/market counter, `UNIQUE(connection_session_id, market_id, local_sequence)`), `exchange_sequence` = `nonce`, plus `begin_sequence` and `exchange_offset` (present since schema v1; beyond the requirements doc's minimum). `trade`: same set with `exchange_sequence` = the message `nonce`. **Scope: book and trade rows only.** Rows derived from `market_stats` (`price_observation`, `funding`) carry `observed_ts_us` (+ `ts_srv_us` on `price_observation` only) but no `connection_session_id`, `local_sequence` or `exchange_sequence`, so they cannot be tied to a WS session -- an unresolved F0-03 obligation if A-4 / A-6 analyses ever need per-session provenance for mark/index/funding. |
| snapshot vs delta identity (F0-02) | `event_kind` ∈ {`snapshot`, `delta`, `reconstructed`}. `is_complete_snapshot = 1` on two kinds of row: a `subscribed/order_book` message that carries a `nonce` (`event_kind = snapshot`, exchange-originated), and a `reconstructed` top-N emitted from the in-memory `BookState` at most once per `reconstructed_snapshot_interval_ms` while the book is synced (both from `phase0.json`: `top_levels`, floor 5, currently 5; interval currently 1000 ms), tagged with the last applied `nonce`. `delta` rows are always `0`. Offline code that wants only exchange-originated snapshots must filter on `event_kind`, not on `is_complete_snapshot` alone. |
| sequence check (A-10) | (Mechanics also summarised in "Host and service" below; this row is the requirement mapping.) `BookState.apply_snapshot` requires `nonce` (else the book is dropped, reason `snapshot_missing_nonce`). `BookState.apply_delta` requires `synced && begin_nonce == last_nonce` and a present `nonce`; any failure clears the book, marks it unsynced, increments `engine_b_phase0_sequence_gap_total`, writes a `data_gap` row (`channel = order_book`, `expected_sequence` = last `nonce`, `observed_sequence` = offending `begin_nonce`, reason `delta_missing_begin_nonce` / `delta_missing_nonce` / `begin_nonce_mismatch_or_unsynced_delta`), and **unsubscribes + resubscribes that one channel** to force a fresh snapshot. |
| no analysis on an unsynced book (F0-08) | `book_synced` per (venue, market) drives the `engine_b_phase0_book_synced` gauge; `reconstructed` rows are only produced while synced, and the `data_gap` row stays open (`ts_end_us IS NULL`) until the next synced snapshot emits a `gap_close` event, which sets `ts_end_us` on the open rows for that (venue, market) -- there is no separate column for this; query `ts_end_us`. Offline, §4.5.2's synced-and-no-gap condition is evaluated as: no `data_gap` **and no `sealed_gap_interval`** for that (venue, market) overlapping the boundary window (a gap whose continuation landed in an already-sealed hour is recorded only as `sealed_gap_interval` in a later retained partition, with no `data_gap` row covering the sealed hour), and the boundary snapshot is an `is_complete_snapshot = 1` row (a `snapshot` or a synced `reconstructed` row -- see the F0-02 row). |
| server timestamp meaning | `normalize_exchange_timestamp_us`: integers below `10^14` are treated as milliseconds and scaled ×1000, larger ones as microseconds (Lighter mixes the two: book `last_updated_at` / `timestamp` are µs, trade `timestamp` is ms). Book: `last_updated_at` from the payload, else the message, else `timestamp`. Trade: per-trade `timestamp`, else message `timestamp`; a trade lacking an exchange timestamp, or whose timestamp is outside `[recv − 7 d, recv + 5 min]` (`trade_timestamp_in_range`), is **rejected on its own** -- counted in `engine_b_phase0_trade_rejected_total{reason=missing_timestamp|out_of_range_timestamp}`, recorded as a point `data_gap` on the `trade` channel, never raised (a raise would tear the venue feed down, bot-strategy#908 item 7) -- while the other trades of the same message are kept; `event_ts_us` (partitioning, OHLCV bucket, synthetic IDs) is the exchange time, never the receive time. `transaction_time` is not consumed but survives in `raw_public_json`. Market stats: message `timestamp`. |
| gap detection on disconnect (F0-07) | (Reconnect/resubscribe mechanics are also described in "Host and service" below.) `feed_loop`: on any exception or close, a `data_gap` row per subscribed market with `channel = connection`, `reason = connection_error:<ExceptionType>` (or `normal_stop` / `task_cancelled`), `ts_start_us` = disconnect time (or attempt start when the connect itself failed) -- **coalesced**: the insert is `INSERT OR IGNORE` against the one-open-gap-per-(venue, market, channel) index, so a market whose previous `connection` gap is still open (it never got its post-reconnect snapshot, bot-strategy#908) gets no new row for a later disconnect; the existing gap silently spans the connected-but-unsynced period and the next outage, and the later disconnect's time and reason survive only in `ws_connection` (`ended_ts_recv_us`, `end_reason`). Use `ws_connection` for per-session disconnects, `data_gap` for "book unusable" intervals; every `BookState` is marked unsynced; reconnect after exponential backoff 1 s → 60 s (doubling), `websockets.connect(ping_interval=20, ping_timeout=15, open_timeout=20, max_queue=4096)`, then resubscribe all `order_book` / `trade` / `market_stats` channels. Partial unique indexes guarantee at most one open gap per (venue, market, channel). |
| gap bounds survive a crash | Two mechanisms on the next start. `_recover_orphaned_sessions` ends a dead `ws_connection` segment at its last proven receive time (`last_activity_ts_recv_us`, bumped on **every** received frame since schema v9) with a durable `end_reason`. `_journal_stale_open_gaps` closes any `data_gap` still open in an *older* partition at that partition's hour boundary (`partition_start + 1 h`) and re-opens a continuation row in the next partition -- i.e. a crash bounds gaps at partition granularity, not at the exact last frame; read the `ws_connection` row for a tighter bound -- `last_activity_ts_recv_us` is the last *proven* frame, so after a quiet period the real disconnect/crash lies somewhere between it and the restart; it is a conservative bound, not the exact time. |
| replay / duplicate handling | trade identities are, in order of preference, the exchange's `trade_id_str`, else its numeric `trade_id`, else a versioned synthetic ID scoped by exchange `nonce` for incremental messages (an ID-less incremental message **without** a nonce is refused); reconnect snapshots deduplicate via the replay-alias multiset. See the "Host and service" section below for the sealed-partition side of this. |

### What is deliberately *not* done online (and where it lands instead)

1. **Trade-channel sequence continuity is not checked.** The message
   `nonce` is stored and used for dedup scoping, but a missed
   `update/trade` between two received ones while the socket stays up is
   **not detectable at all** -- neither online nor offline (see below:
   `trade_id`s are not contiguous, so a jump proves nothing); only a
   `connection` gap covering the window can flag it. F0-07's per-channel / per-sequence granularity is therefore fully met for
   `order_book` and only at connection granularity for `trade` /
   `market_stats`. Lighter documents no continuity rule for the trade
   channel, so there is nothing to check online. **Consequence for OHLCV
   (F0-04)**: `ohlcv_1m` is built by merging every received trade into
   its 1-minute bucket, and `is_complete` is set purely by bucket age
   (`OHLCV_FINALIZE_GRACE_US` after the bucket closes), *not* by any
   trade-completeness or book-snapshot criterion -- a trade silently
   missed while the socket stayed connected leaves a bucket marked
   complete with wrong OHLC / volume / trade count. §4.5.2's boundary
   prices are unaffected (they come from book snapshots). **Trade
   completeness within a connected session cannot be verified from the
   collected data**: Lighter gives no continuity rule for the channel,
   `trade_id`s are not guaranteed contiguous per market (100 → 102 is not
   evidence of a miss), and ID-less trades carry non-orderable synthetic
   IDs. Only the `connection`-gap overlap can invalidate a bucket; a miss
   with the socket up is undetectable. F0-04's own remedy -- a second,
   API-sourced OHLCV series for cross-checking -- is **not collected**
   by this observer (no candle channel or REST kline poll; `ohlcv_1m`
   has a single WS-trade `source`), so `ohlcv_1m` must be treated as
   best-effort and `is_complete = 1` as "finalised", never as "verified
   complete", until that cross-check source is added (tracked as a
   follow-up under bot-strategy#908).
2. **`daily_data_quality` is created by the schema but has no writer.**
   `event_count`, `missing_duration_us`, `out_of_order_count`,
   `duplicate_count`, `sequence_gap_count`, `stale_quote_duration_us`,
   `crossed_book_duration_us` (F0-10) must be derived at analysis time
   from `book_event` / `trade` / `data_gap` / **`sealed_gap_interval`** /
   `ws_connection` -- `missing_duration_us` in particular must union
   `data_gap` with `sealed_gap_interval`, since a gap whose continuation
   landed in an already-sealed hour exists only in the latter.
   `reconnect_count`, `duplicate_count` and `sequence_gap_count` are the
   exceptions (`sequence_gap_count`: repeated invalid deltas before the
   replacement snapshot are coalesced into the one open `order_book` gap
   row, so only the Prometheus counter has the per-break count).
   `duplicate_count`: replayed / duplicate trades are discarded by the
   replay-alias check, the sealed-index check or `INSERT OR IGNORE`
   without a rejection row or a durable counter, and `local_sequence`
   gaps cannot recover a duplicate that was the last message, so it is
   not derivable from SQLite. `reconnect_count`: a run of failed connection
   attempts creates no `ws_connection` row and, because of the
   one-open-gap-per-(venue, market, channel) partial unique index, only
   one coalesced `connection` gap -- the attempt count survives solely in
   the in-memory `engine_b_phase0_reconnect_total` gauge (scraped by
   Alloy), not in SQLite; `engine_b_phase0_sequence_gap_total` and
   `engine_b_phase0_reconnect_total` give the live counts in Prometheus
   meanwhile.
3. **Clock offset is not measured by the collector.** `max_clock_offset_us`
   is never populated and there is no NTP check in the process; §7's
   "offset > 250 ms → warning, > 1 s → halt" rule is a host-level
   property. The absolute offset comes only from the host's `chronyd`
   tracking. `ts_recv_us − ts_srv_us` on `order_book` deltas is a
   *signed* latency-plus-skew diagnostic: a host clock running behind the
   exchange subtracts from the network delay, so a small or even negative
   value does not exclude an offset above the 1 s threshold. Use it to
   spot drift trends, never as a bound for the §7 thresholds.

### How to verify on the host

- Host clock: `chronyc tracking` (the host is NTP-disciplined; §7's 250 ms /
  1 s thresholds are a host property, not something the collector measures).
- Gap inventory for a partition:
  `SELECT channel, reason, COUNT(*) FROM data_gap GROUP BY 1, 2`,
  `SELECT venue, symbol, channel, reason FROM data_gap WHERE ts_end_us IS NULL`
  (open gaps), and `SELECT venue, symbol, channel, sealed_partition FROM
  sealed_gap_interval` (intervals known unusable whose original gap row
  lives in an already-sealed hour -- always union these with `data_gap`). `channel = order_book` rows are sequence breaks; `channel =
  connection` rows are disconnects.
- Live sync state: `engine_b_phase0_book_synced{market_id=...}` on
  `127.0.0.1:9472/metrics` -- a market stuck at `0` while
  `engine_b_phase0_feed_connected` is `1` has not received its
  post-reconnect snapshot, and the collector has **no automatic
  resubscribe for that case**; `engine_b_phase0_sequence_gap_total` and
  `engine_b_phase0_reconnect_total` are the live counters behind the
  `data_gap` table.
- Server-time coverage: `SELECT event_kind, COUNT(*) FROM book_event WHERE
  ts_srv_us IS NULL GROUP BY 1`. A `reconstructed` row inherits the
  triggering message's server time, so it can only be NULL when that raw
  message was; a NULL on a `snapshot` or `delta` row means the exchange
  message carried no parseable `last_updated_at` / `timestamp` (the row is
  still stored) -- a non-trivial count there is a schema-drift / malformed-
  feed signal, not noise. A `recv − srv` distribution on deltas is a
  signed latency/skew diagnostic (see item 3 above), not an offset bound.
- Deploy ≠ restart: the deploy workflow does **not** restart
  `engine-b-phase0.service`. Compare `/opt/engine-b-phase0/release.env`
  with the running process start time (`systemctl show -p
  ActiveEnterTimestamp`) and `collector_manifest` before trusting that the
  running observer matches `origin/master`.

Known limitations of the running collector (tracked in bot-strategy#908,
with the 2026-09-04 measurements in bot-strategy#874): a single
non-positive `market_stats` price tears down the whole venue connection;
markets that miss their post-reconnect snapshot stay unsynced until the
next reconnect; the `task_cancelled` gap reason is not reliably
attributed.

### Resolution

For the Phase 0 logger's purpose A-10 is resolved: every stored book
snapshot can be proven synced or not, every interval in which a market's
book was unusable is covered by a bounded `data_gap` (or
`sealed_gap_interval`) row -- coalesced, not one row per disconnect: a
disconnect while a market is already unsynced extends the open gap, and
the per-disconnect record is `ws_connection` -- `order_book` sequence
breaks are likewise coalesced into one open `order_book` gap per market
until the replacement snapshot arrives (`idx_data_gap_open_order_book`;
the per-break count lives only in `engine_b_phase0_sequence_gap_total`),
and server versus receive time are both kept in a known unit. Two
intervals are **not** recorded anywhere and must be bounded offline:
(a) from a connection's start until its first complete snapshot for a
market (`BookState` starts unsynced, but a `connection` gap is opened
only on disconnect) -- bound it as `ws_connection.started_ts_recv_us` →
first `is_complete_snapshot = 1` row of that session and market; (b) an
`order_book` subscription that silently stops delivering while `trade` /
`market_stats` keep the socket alive -- the collector tracks liveness per
venue, not per market channel, so `BookState.synced` stays true and the
book looks current until the next invalid delta or disconnect. Detect it
offline as an anomalously long interval between consecutive `book_event`
rows for a market relative to that market's own cadence (or to its
`trade` / `price_observation` activity in the same interval) and treat
the interval as unusable; per-market book-channel liveness in the
collector is tracked as a follow-up under bot-strategy#908. Items 1–3 above are
analysis-time obligations (1, 2) and a host-level check (3). "Resolved"
here means *detected and recorded*, not *self-healing*: the collector has
no automatic resubscribe when a post-reconnect snapshot never arrives, so
a market can sit unsynced (its `connection` gap open, later bounded by
`_journal_stale_open_gaps`) until the next reconnect -- that recovery gap
and the other collector limitations are tracked in bot-strategy#908.

## Host and service

- EC2: `debot-robinhood-lighter` (`i-0095af4fe0efbc5dd`, `ap-northeast-1`)
- Service: `engine-b-phase0.service`
- Data: `/var/lib/engine-b-phase0/data/engine_b_phase0_YYYYMMDD_HH.sqlite3`
- Health: `/run/engine-b-phase0/status.json`
- Metrics: `127.0.0.1:9472/metrics`

Per-market book watchdog (bot-strategy#908): the collector no longer tears
the venue connection down when one market's `market_stats` carries a
non-positive price -- that message is skipped, counted in
`engine_b_phase0_market_stats_rejected_total{field=...}` and recorded as a
point `data_gap` on the `market_stats` channel. The same rule applies to
an individual trade whose exchange timestamp is missing or implausible
(older than 7 days or more than 5 minutes ahead of receipt -- a thin
market's `subscribed/trade` snapshot carries its last fills, which on a
weekend restart were 8 and 19 days old) or whose price / size is
non-positive or malformed: that row alone is dropped, counted in
`engine_b_phase0_trade_rejected_total{reason=...}` (`missing_timestamp`,
`out_of_range_timestamp`, `non_positive_price`, `non_positive_size`,
`malformed_price_or_size`) and recorded as a point `data_gap` on the
`trade` channel, while the other trades of the same message are kept.
A market still unsynced
`book_resubscribe_after_ms` (default 10 s) after its last subscribe gets
its `order_book` channel re-subscribed
(`engine_b_phase0_book_resubscribe_total{reason="unsynced"}`), and a
synced market whose book has been silent while its own trade /
market_stats have been arriving for `book_stall_after_ms` (default 60 s,
measured from the first auxiliary message after the last book message,
with at least one within the last half of that window) is declared stale
-- the gap starts where the auxiliary traffic was observably flowing: unsynced, `data_gap(channel=order_book,
reason=book_channel_stalled)` written to the detecting partition with its
start clamped to that partition's beginning (a sealed hour never receives
new rows); the portion of the stall in earlier hours is preserved as
`sealed_gap_interval` rows in the detecting partition, one per hour, so
`missing_duration_us` (union of both tables) still covers it;
re-subscribed (`engine_b_phase0_book_stall_total` counts detections,
`engine_b_phase0_book_resubscribe_total{reason="stalled"}` counts frames
actually sent). At most `book_watchdog_batch`
(default 5) subscribe frames per venue per second, and at most 100
(re)subscribe frames per venue per rolling minute across the reconnect
burst, the watchdog and sequence-break recovery (half of Lighter's 200
client messages / minute per IP); a reconnect burst subscribes market by market, waiting for budget;
10 frames/min are set aside for keepalive traffic (library protocol
pings/pongs and application-level pong replies, which are never withheld)
and 2 frames per configured market for proven sequence breaks
(`load_config` rejects a venue whose reserve would leave the watchdog
fewer than 20 frames/min). All three keys are
optional in `phase0.json`. A feed task cancelled without `stop_event`
set is now recorded as `task_cancelled_unexpected` with a logged stack.

The databases rotate hourly because the normalized public feed is too large
for the host's 20 GiB root volume. At minute 10, the archive timer checkpoints
an abandoned WAL for each closed partition, finalizes its remaining closed
OHLCV rows, and runs SQLite integrity checking. The collector and archiver use
the same per-partition `flock`, so a live writer and seal/delete operation cannot
race. The timer uploads a gzip plus SHA-256 file with AES256 S3 server-side
encryption, downloads both objects again, validates byte equality, gzip,
decompressed checksum, and remote SQLite integrity, and only then reaches the
deletion gate.

A physical WebSocket connection that spans an hourly boundary is represented
as one partition-local `ws_connection` segment in each database. Schema v9
records `last_activity_ts_recv_us` for every received application message,
including market-stats, ping/pong, malformed payloads, and delayed trades whose
exchange event belongs to another hour. Crash recovery uses that durable receive
time rather than only local book/trade rows. A delayed or tolerated future trade
retained outside its receive-time partition receives an `is_physical=0`,
`event_time_reference` row solely to satisfy provenance/FK linkage; it is never
rotated, archived, or counted as a physical session. Closed
partitions end their open segments at the hour boundary with
`end_reason=partition_rotation`; the active partition records the real close
time and reason. Every partition for one collector process preserves the same
`collector_manifest.started_ts_us`, so database rotation is not mistaken for a
collector restart. The archiver applies the same partition-boundary closure
under the shared lock before upload, covering a completely quiet feed that
produces no collector batch after the hour changes. The SQLite writer also
self-wakes just after each UTC hour boundary to close old cached handles, so
the minute-10 archiver does not mistake an idle collector connection for a
live transaction.

Local deletion is disabled by default through
`ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=false`; the active hour is never an
archive target. When deletion is explicitly enabled, a verified stable
partition is atomically marked in `/var/lib/engine-b-phase0/sealed/` before its
local database is removed. The seal includes an exact SQLite index of the
canonical archive's trade identities. The collector never recreates a sealed
canonical partition: replayed canonical trades are discarded using that
index, while genuinely new events for that exchange hour are stored in the
active partition's `late_trade` table with the original sealed partition
recorded. ID-less trades receive a versioned stable synthetic identity using
the exchange event timestamp, canonical raw trade, and its occurrence number
within identical trades in the message. Subscribed snapshots exclude message
nonce and absolute array position so overlapping reconnect snapshots retain
the same multiset identities. Incremental `update/trade` messages additionally
scope ID-less identities by exchange nonce, preserving indistinguishable
legitimate trades delivered in separate updates while deduplicating a replayed
update. A delivery-independent replay alias is also stored as a multiset in
each live database and seal sidecar: reconnect snapshots consume the existing
alias count before inserting, so update-to-snapshot replays deduplicate without
collapsing distinct nonce-scoped updates. A trade missing an exchange timestamp, or carrying an implausible one, or
whose price or size is not strictly positive (or not a finite decimal), is
rejected **row by row** (bot-strategy#908 item 7): that row is dropped, counted in
`engine_b_phase0_trade_rejected_total{reason=...}` and written as a point
`data_gap` on the `trade` channel in the receiving partition, and the
remaining rows of the same message are queued normally.
`engine_b_phase0_trade_total` counts accepted rows only, so the two series
partition the received rows.
When an older
partition contains NULL IDs or obsolete synthetic IDs, the index builder
reconstructs the current identity and replay alias from stored ordering.
For a pre-v7 seal sidecar without replay aliases, primary IDs remain usable;
an otherwise unverifiable ID-less snapshot for that sealed hour is
conservatively discarded instead of crashing the writer or duplicating data.
The committed `late_trade` row is the durable reconciliation journal: replay
checks consult all retained hourly databases, and the archiver copies every
journaled identity into its referenced sealed sidecar before the source hour
can be archived or deleted. Once copied, a replay in a later active hour is
discarded from the sidecar. There is no separate sidecar transaction on the
collector write path, so a process stop cannot strand a committed late trade
between two writes. Every older index changed by reconciliation is republished
and verified in S3 before the source journal DB can be deleted.
If the source changes or WAL sidecars reappear during upload, the seal is rolled
back and the local partition is retained.
If the archiver is interrupted after sealing but before deletion, the next run
removes the residual database only after its SHA-256, SQLite integrity, seal,
and trade-index metadata all match. A mismatched fragment is retained
fail-closed.

Before any verified local deletion, the trade-identity index and seal JSON are
also uploaded beside the canonical archive as `.trade_ids.sqlite3` and
`.seal.json` objects with AES256 encryption, downloaded again, and compared
byte-for-byte. The seal is the remote commit marker. After host/state-volume
replacement, restore both sidecars to `/var/lib/engine-b-phase0/sealed/` and
verify their binding before starting collection; do not resume from only the
hourly gzip objects.

Archive prefix:

```text
s3://debot-dashboard/debot/engine-b/phase0/raw/<host>/YYYY/MM/
```

## Install and start

The host requires Python 3.11. `install_engine_b_phase0.sh` creates the
dedicated system identity and a root-owned isolated venv/runtime, installs
exact dependencies and the three staged systemd units, and writes the required
full Git commit to `/opt/engine-b-phase0/release.env`. It runs
`daemon-reload` but never starts or restarts a service. The normal
Robinhood deploy workflow stages the units and passes `GITHUB_SHA`; for a
manual install, provide both values explicitly. Because the observer keeps
writing while the installer re-owns `/var/lib/engine-b-phase0`, a state file
that vanishes mid-walk (SQLite `-wal` / `-shm` side files, a partition being
sealed) is skipped with a `vanished during re-own` notice rather than
failing the deploy (bot-strategy#908 item 8). The re-own is a
descriptor-anchored Python walk (`os.fwalk` + `O_NOFOLLOW` + `fchown` /
`fchmod`) over directories and regular files only, so a symlink -- leaf or
an ancestor swapped in by the running observer -- is never followed by the
root-run installer; an entry that cannot be re-owned for any other reason,
or an unknown service group, still aborts the install.

The first operator-controlled restart also completes the identity handoff.
After systemd stops the legacy `ec2-user` observer, root-privileged pre-start
commands repair the group and mode of the entire state tree before config
validation runs as `engine-b-phase0`. This covers files created by the legacy
process after the installer ran; do not remove those pre-start commands until
the handoff has completed.

```bash
sudo dnf install -y python3.11 python3.11-pip
sudo env \
  ENGINE_B_PHASE0_CODE_COMMIT=<40-character-deployed-git-sha> \
  ENGINE_B_PHASE0_UNIT_SOURCE_DIR=/path/to/staged/units \
  bash /opt/debot/scripts/install_engine_b_phase0.sh
sudo systemctl enable --now engine-b-phase0.service
```

The Tokyo deployment currently keeps `engine-b-phase0-archive.timer` disabled.
Enabling verified-local deletion requires explicit operator approval. After
that approval, set `ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=true` in a systemd
override for `engine-b-phase0-archive.service`, reload systemd, and enable the
timer. Never enable deletion merely to bypass a failed archive check.

### Capacity recovery and bounded archive runs (#915)

At 2026-09-05 19:09 UTC the Tokyo root had 1,645,625,344 bytes free
(93% used); Phase 0 data occupied about 15.16 GB. The archive timer was
inactive and its configured S3 prefix had zero objects. Daily SQLite sizes
were 5,171,081,216 bytes on September 2 and 5,368,737,792 on September 3.
The observer was restored separately under #908; low throughput during its
outage is not a capacity estimate for the recovered collector.

Proposed initial hot retention is **24 hours**, pending operator approval.
At the larger observed daily rate, budget about 5.37 GB for hot DBs, 4.60 GB
for other existing root usage, 2.18 GB for a largest-partition verification
(4 × 410,894,336 bytes plus a 512 MiB reserve), and 5.37 GB for one day
of archive failure. That totals about 17.52 GB within the 21.40 GB usable
filesystem. This is a provisional budget, not evidence of sufficient capacity
under the recovered mainnet workload; remeasure through a full active session.
No volume expansion is included in this proposal. S3 storage/requests and any
cross-region transfer remain chargeable; this runbook does not quote prices.

Archive controls (environment variables):

| Variable | Default | Meaning |
|---|---:|---|
| `ENGINE_B_PHASE0_RETENTION_HOURS` | `0` | Skip partitions whose **end** is newer than now minus this many hours; also protects interrupted-seal recovery. Set `24` for the proposed hot window. |
| `ENGINE_B_PHASE0_MAX_PARTITIONS` | `0` | Maximum eligible partitions attempted per invocation, oldest first; `0` is unlimited. Busy/skipped attempts count. |
| `ENGINE_B_PHASE0_ARCHIVE_PARTITION` | empty | Restrict a verification/recovery run to one `YYYYMMDD_HH`; never bypasses hot/current-hour protection. Missing or invalid targets fail. |
| `ENGINE_B_PHASE0_MIN_FREE_BYTES` | `536870912` | Reserve left after the estimated scratch budget of four times DB plus WAL size. An insufficient full-DB preflight fails before checkpoint/upload. Every sidecar publication (including interrupted-seal recovery and reconciled-index republication) also checks the actual index plus seal download sizes against the remaining free space and this reserve. |

All values are validated before modifying a partition. The space estimate is
conservative planning, not a reservation: concurrent collection, sidecar sizes,
and compression can still exhaust space. Do not lower the reserve merely to
force a failed run. A bounded **nondeleting** run will revisit the same oldest
DBs on repetition; use an explicit partition for subsequent verification.

1. Check current free space, sizes and archive state. With only 1.65 GB free,
   the largest partition fails the 2.18 GB preflight. Start with a small closed
   hour; do not start the entire backlog without a capacity check.
2. After approval to archive to the configured private destination, run a
   **nondeleting** single-partition check with the reviewed script. For example,
   after deploying this change:

   ```bash
   sudo -u engine-b-phase0 env \
     ENGINE_B_PHASE0_ARCHIVE_PARTITION=20260901_23 \
     ENGINE_B_PHASE0_MAX_PARTITIONS=1 \
     ENGINE_B_PHASE0_RETENTION_HOURS=24 \
     ENGINE_B_PHASE0_DELETE_VERIFIED_LOCAL=false \
     bash /opt/engine-b-phase0/engine_b_phase0_archive.sh
   ```

   Destination:
   `s3://debot-dashboard/debot/engine-b/phase0/raw/debot-robinhood-lighter/2026/09/engine_b_phase0_20260901_23.sqlite3.gz`
   plus `.sha256`. The existing script downloads and checks the compressed
   bytes, decompressed SHA-256 and SQLite integrity. Keep the output and
   independently download the objects to a separate restore directory.
   Nondeleting mode does **not** create the trade index or seal; this check
   alone is not complete seal-sidecar disaster-recovery acceptance.
3. Only after explicit approval for verified local deletion, run small batches
   with `RETENTION_HOURS=24`, `MAX_PARTITIONS=24` and
   `DELETE_VERIFIED_LOCAL=true` (all with the `ENGINE_B_PHASE0_` prefix).
   Verify S3 DB/checksum/index/seal together in an independent restore directory
   using `--verify-sealed-partition` before accepting the recovery workflow.
   The canonical gzip and its original SHA must also be rechecked when
   investigating an interrupted run; local seal validation alone does not
   establish current remote availability.
4. Once the backlog has cleared and full restoration is demonstrated, enable
   the timer with an approved override retaining 24 hours. Do not leave a
   one-partition target in the recurring service. An initial batch limit of
   24 is for supervised recovery; confirm sustained throughput exceeds arrivals
   before choosing a recurring cap. Check exit status and free bytes after
   each run. Disabling the timer stops future deletion but does not restore
   removed DBs; use the verified remote artifacts for restoration.

Remaining acceptance for #915: successful real uploads and independent restore
(including sidecar/seal binding), approved deletion, at least one retention
cycle, recovered-feed write-rate measurement, and persistent monitoring/alerts
for free space, archive failure and last successful archive age. The controls
above do not by themselves complete those operational requirements.

Verification:

```bash
sudo systemctl status engine-b-phase0.service --no-pager
sudo journalctl -u engine-b-phase0.service --since '10 minutes ago' --no-pager
curl -fsS http://127.0.0.1:9472/healthz
curl -fsS http://127.0.0.1:9472/metrics | grep engine_b_phase0_order_capability
```

Confirm both venues are connected, all expected books become synchronized,
the DB queue remains bounded, SQLite `PRAGMA integrity_check` returns `ok`, and
`order_capability` is `false`/`0`.
Book snapshots without a nonce, and deltas without both begin/end nonces, are
recorded as incomplete sequence gaps and force a public-channel resubscribe;
they never produce reconstructed top-of-book rows.
Open connection/order-book gap rows are closed when a replacement snapshot
restores synchronization, including rows retained in an earlier hourly DB. The
collector and archiver both fsync a write-ahead continuation marker before
bounding an old row. The next collector batch completes any interrupted source
close, imports the marker into its fixed destination partition idempotently,
and carries the gap through every intervening hourly partition before removing
the markers. A recovery snapshot therefore cannot undercount an open gap across
a crash, long collector outage, or hourly rotation. Gap recovery only closes
rows that began at or before the snapshot timestamp, so a later disconnect in
the same database flush remains open. If a marker's fixed destination was
already sealed, recovery advances it from that hour's end rather than entering
a restart loop. The seal index proves whether that continuation was already in
the canonical archive; otherwise the full skipped interval is written
idempotently to `sealed_gap_interval` in the next retained database so quality
calculations retain its missing duration. Live WebSocket session segments are
likewise carried through idle hourly partitions even when no feed payload
arrives. Each session handoff is write-ahead journaled before the old segment
is closed. The marker records the source collector run ID: the same live
collector preserves the destination segment as open, while a different process
only preserves a destination segment that was durably created. If the marker
was fsynced but the destination write never committed, recovery creates a
zero-length segment at the handoff boundary and records a connection gap from
that boundary through the replacement snapshot with
`collector_restart_recovery`. Missing sealed-hour destinations are recorded as
`sealed_gap_interval` evidence rather than as connected session time. On a
restart, even an archived destination row is not treated as proof that the
physical socket survived until the archiver's mechanical boundary close; the
sealed hour retains conservative gap evidence.
Startup also discovers open rows left in retained databases by an earlier
process, bounds each orphaned physical session at its last durable book/trade
activity, and records a connection gap from that point through the replacement
snapshot. Recovery therefore does not count crash downtime as connected and
does not depend on the archive timer. A write-ahead continuation marker carries
every stale open gap across each intervening retained hourly partition before
the replacement snapshot closes it. Legacy duplicate open connection/order-book
gaps are coalesced before those continuation markers are created, preventing
missing-duration overcount. If a journal target was already sealed,
the sidecar proves whether that session segment exists; same-process live
handoffs carry a missing sealed-hour segment into the next retained database as
`sealed_session_interval` evidence, while restart recovery uses
`sealed_gap_interval` so crash downtime cannot look connected.
`ws_connection` starts only after the WebSocket handshake succeeds. DNS,
TCP/TLS, and handshake failures contribute connection-gap evidence but never
create a physical session row or inflate session duration. Their gap starts at
the connection-attempt timestamp, not after the handshake timeout returns.
Repeated connection failures before a successful replacement snapshot share
one open `connection` gap per venue/market, preventing retry backoff from
counting the same outage more than once.

## `freq2` account record

The retired Robinhood pairtrade arm's environment file was preserved without
decrypting or logging it:

```text
s3://debot-dashboard/debot/credentials-archive/engine-b-robinhood-freq2.env
```

- S3 VersionId: `JK34BppwJtxSYMpsJwOyOdCpQKJ_pCr6`
- SHA-256: `29f164a80657697e43988366a36b6fe5ab68a06eb1557fe6913b01fe120634c6`
- S3 encryption: `AES256`

The API key fields inside that file are already KMS-encrypted ciphertext. The
KMS-wrapped data key is deliberately not copied into this archive; recovery
uses the separately managed `debot_secrets_common.env`. Never place either
file in GitHub, logs, or the Phase 0 process environment. This account is only
for a future Phase 1/2 implementation after the requirements gates and explicit
phase-advance approval are satisfied.

## Stop and recovery

Stopping Phase 0 cannot create or close a position because it has no private
exchange capability:

```bash
sudo systemctl stop engine-b-phase0.service
```

Before restart, inspect the last journal error, disk usage, the active SQLite
WAL, and S3 archive continuity. A DB write failure or disk-full condition must
remain fail-closed; do not bypass the archive verification to reclaim space.

## Archive capacity monitoring (#915)

`scripts/engine_b_archive_monitor.py` provides independent, read-only metrics on
`127.0.0.1:9473/metrics`. It reads filesystem capacity, retained partition sizes,
local seals, and the archive service/timer's systemd properties. It does not open
SQLite databases, call an exchange, archive files, or change service state.
`--once` prints the same observations plus errors as JSON and exits nonzero when
any probe fails. Collection errors also emit `probe_success=0` over HTTP 200 so
Prometheus can record partial observations; HTTP success alone is not health.

The supplied unit uses the existing `engine-b-phase0` account. Install from a
reviewed checkout (this monitor is separate from the observer runtime installer):

```bash
sudo install -o root -g engine-b-phase0 -m 0550 scripts/engine_b_archive_monitor.py /opt/engine-b-phase0/engine_b_archive_monitor.py
sudo install -o root -g root -m 0644 deploy/engine-b-phase0-monitor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now engine-b-phase0-monitor.service
sudo -u engine-b-phase0 /usr/bin/python3 /opt/engine-b-phase0/engine_b_archive_monitor.py --once
curl -fsS http://127.0.0.1:9473/metrics
```

Only the new monitoring service is started. Match its `--retention-hours` to the
archive service's effective `ENGINE_B_PHASE0_RETENTION_HOURS` (the supplied unit
assumes the approved 24-hour retention). These metrics and the stalled alert
assume `DELETE_VERIFIED_LOCAL=true`; archive-only operation needs a separate
completion marker and must not use the verified-removal metric as success.

Add an Alloy scrape using the deployment's existing remote-write receiver:

```alloy
prometheus.scrape "engine_b_phase0_archive" {
  targets = [{ "__address__" = "127.0.0.1:9473" }]
  job_name = "engine-b-phase0-archive"
  scrape_interval = "60s"
  scrape_timeout = "30s"
  forward_to = [prometheus.remote_write.EXISTING.receiver]
}
```

Replace `EXISTING` with the actual configured component name. Import
`grafana/alerts/engine-b-archive.rules.yml` into the connected Prometheus ruler
and route warning alerts to the operator's existing contact point. The rules
cover missing scrape/probe failure, less than 5 GiB or 15% available disk,
archive failure/inactive timer, and eligible backlog without a verified removal
for three hours. Warning thresholds are initial operational defaults, not a
measured capacity guarantee. Confirm the job label in remote storage before
loading the rules; the absent-series alert assumes this job should exist.

`last_verified_removal_timestamp_seconds` is the latest local seal timestamp
whose source DB no longer exists. The archiver writes a seal before it verifies
remote sidecars, so a seal **with a retained source DB never counts as success**.
No qualifying seal yields zero, even if systemd reports a successful no-op run.
This is evidence of the normal verified-deletion path, not a fresh independent
S3 integrity check, and assumes operators do not remove source DBs manually.
The seal timestamp precedes completion by the sidecar-upload duration. Use the
independent S3 restore procedure above for archive integrity verification.

`retained_partition_bytes` and `eligible_partition_bytes` count DB file sizes,
excluding WAL/SHM, seal indexes, and scratch files. Available disk includes all
filesystem usage. Eligibility uses the partition **end** plus retention, never
mtime. A retained-byte delta is not a write-rate estimate because archival
removes files concurrently. Measure full weekday production separately from
canonical archived DB lengths, grouped by partition hour, and retained complete
hours; deduplicate by partition and report coverage before extrapolating.

Deployment acceptance remains operational work: confirm a normal hourly cycle,
exercise each rule using a test series/ruler test without stopping collection or
filling the disk, verify delivery to the contact point, and record the result in
#915. Do not mark persistent monitoring complete merely because this endpoint
or the rule file exists. Weekday recovered-feed write-rate measurement is also
still required before closing #915.
## Reproducible boundary preflight (#872)

`scripts/engine_b_boundary_quality.py` audits **closed offline SQLite copies**
from the Phase 0 collector. Restore the canonical S3 DB archives and verify
checksums first using the archive procedure above. Do not point this tool at the
live data directory or remove WAL/SHM files to make a live DB pass its guard.

```bash
python3 scripts/engine_b_boundary_quality.py \
  --data-dir /tmp/engine-b-offline \
  --calendar configs/engine-b/trading_calendar.json \
  --start 2026-09-08 --end 2026-09-09 \
  --symbols SKHYNIXUSD SNDK SOXL NVDA EWY USDKRW \
  --output /tmp/engine-b-boundary-quality.json
```

The required symbols are explicit. The example is an input set to inspect, not
a primary-symbol/model freeze. Rerun candidate comparisons with the alternative
KR/US inputs and preserve each report. The frozen calendar supplies t0 (KRX
open), t1 (KRX close), and t2 (US cash open), including DST and delayed opens.
Closed days are `market_closed`, never valid observations. Missing calendar days
or malformed inputs fail the command with exit 2. A completed report exits 0
even when boundaries fail: inspect the per-day/per-symbol status and reasons.

Each boundary selects the latest stored complete snapshot **at or before** the
boundary with receive age at most 30 seconds. It does not fill holes with later
prices or fall back to an older good snapshot when the latest is malformed.
It reports nonce presence, locked/crossed/missing-sided/zero-size/nonfinite book
errors, contiguous level order, Decimal mid/spread/top-five depth, and available
receive-minus-server timestamp diagnostics. Missing sequence numbers fail the
preflight; their presence does not prove complete sequence continuity.

The legacy `lighter_mainnet_context` and current `lighter` aliases are accepted
only when `collector_manifest.config_json` identifies HTTPS/WSS mainnet
endpoints and the event's symbol/market ID. Conflicting IDs or non-mainnet
endpoints are rejected. Robinhood rows never substitute for missing mainnet
inputs. Equal-time candidates from both aliases are ambiguous and fail.

Provide hourly partitions covering each boundary plus/minus 15 minutes; t0
requires the preceding day's final hour. Missing files fail the preflight and
are listed explicitly. Connection/order-book `data_gap` and
`sealed_gap_interval` rows in these partitions are checked for overlap with the
whole boundary window, including gaps after the selected quote. Recovery rows
stored **outside** these loaded partitions are not scanned by this preflight.
The report does not sum missing durations or treat a file's presence as proof
of continuous collection. Missing gap tables are reported as missing evidence.

Every report includes the SHA-256 and size of each loaded DB, missing input
names, calendar/code hashes, parameters and an `analysis_hash`. The analyzer
opens DBs read-only with `immutable=1`, rejects WAL/SHM companions, and rehashes
inputs after analysis to detect source mutation. Keep its input directory
immutable for the entire run; there is no live-backup or repair functionality.
Reports are atomically replaced after a successful analysis.

`boundary_preflight_pass` is a necessary-input check only. **G0-2 remains
`not_evaluated` in every report.** This tool does not determine full-session
connection/sequence coverage, clock synchronization, freshness throughout the
window, late recovery evidence in other partitions, eligibility, arrival-time
execution VWAP, funding/fees, model selection, or Phase 0A/0B acceptance. A
boundary passing this check must not be counted as a valid statistical session.
Those remaining #872 checks need separate daily analysis over the complete
archive and recovery evidence.

## Step 0 absorption test (#988)

`scripts/engine_b_step0_extract.py` + `scripts/engine_b_step0_absorption.py`
answer one question: over the KRX session, has the US perp already made the move
the KR name made, and is what is left bigger than the round trip? The kill rules
are frozen in bot-strategy#988 — `sd(fwd) < 14 bps`, or `R^2 >= 0.8` together
with `sd(eps) < 14 bps`.

This pair is deliberately cheap. It reads the 1 Hz `price_observation` mid
(`market_stats`) at t0/t1/t2 only, so it can sweep weeks of archive by pulling
one hourly partition at a time and deleting it again — the 15 GB corpus is never
materialised, and the host's free space is not put at risk (#915).

It is **not** a session-validity gate. `scripts/engine_b_boundary_quality.py`
above is the tool that decides whether a boundary is a usable statistical
observation (book snapshots, sequence/gap evidence, mainnet-alias provenance).
Step 0 numbers are dispersion scale, not validated Phase 0A sessions.

Run the extractor on the observer host (SSH does not reach it; use
`aws ssm send-command`), then the statistics anywhere:

```bash
# on i-0095af4fe0efbc5dd (ap-northeast-1), as root
python3 engine_b_step0_extract.py \
  --start 2026-09-01 --end 2026-09-10 \
  --out /var/tmp/engine-b-step0/prices.jsonl
# add --venues robinhood for the pre-pivot (#244) rows; never mix venues in one file

# anywhere, on the JSONL
python3 scripts/engine_b_step0_absorption.py \
  --prices prices.jsonl --kr-symbol SKHYNIXUSD --us-symbol SNDK \
  --price-types mid,mark,index --json-out step0.json
```

The extractor appends, and a rerun skips a partition only when the file holds an
explicit completion record for it that covered every instant it was asked for.
An interrupted write, a transient S3 failure, an hour the archive has not
uploaded yet, and today's t2 partition queried before 13:30 all stay eligible,
so rerunning the same command each day fills the holes instead of freezing them.
An archived partition is sealed and immutable, so whatever it answered is final
even when that answer is "nothing" — it is not re-fetched. `--force` re-queries
everything. Rows carry the venue and the lag between the instant and the quote
that stood for it, and the statistics step drops any leg further than
`--max-lag-secs` (default 120 s) from its instant.

Each statistic uses every day that can support it: `fwd` needs only the US
symbol at t1 and t2, the regression needs both symbols at t0 and t1, so a day
with one hole still contributes where it can.

The decision is three-valued — `KILL`, `PROCEED`, or `UNRESOLVED` — because the
rules are written on standard deviations the sample only estimates. A kill needs
the whole chi-square interval below 14 bps, a clearance needs the whole interval
above it, and an interval straddling the threshold is unresolved, which is a
different answer from "not killed". K0-b additionally needs both beta
conventions to agree before it can kill, so the formula ambiguity above can
never be what ends the experiment. No data reads as `UNRESOLVED`, never as a
pass.
