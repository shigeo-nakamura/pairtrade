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
| no analysis on an unsynced book (F0-08) | `book_synced` per (venue, market) drives the `engine_b_phase0_book_synced` gauge; `reconstructed` rows are only produced while synced, and the `data_gap` row stays open (`ts_end_us IS NULL`) until the next synced snapshot emits a `gap_close` event, which sets `ts_end_us` on the open rows for that (venue, market) -- there is no separate column for this; query `ts_end_us`. Offline, §4.5.2's synced-and-no-gap condition is evaluated as: no `data_gap` for that (venue, market) overlapping the boundary window, and the boundary snapshot is an `is_complete_snapshot = 1` row (a `snapshot` or a synced `reconstructed` row -- see the F0-02 row). |
| server timestamp meaning | `normalize_exchange_timestamp_us`: integers below `10^14` are treated as milliseconds and scaled ×1000, larger ones as microseconds (Lighter mixes the two: book `last_updated_at` / `timestamp` are µs, trade `timestamp` is ms). Book: `last_updated_at` from the payload, else the message, else `timestamp`. Trade: per-trade `timestamp`, else message `timestamp`; a trade message with any trade lacking an exchange timestamp is **rejected whole** (`RuntimeError`), as is one whose timestamp is outside `[recv − 7 d, recv + 5 min]` (`validate_trade_timestamp_us`); `event_ts_us` (partitioning, OHLCV bucket, synthetic IDs) is the exchange time, never the receive time. `transaction_time` is not consumed but survives in `raw_public_json`. Market stats: message `timestamp`. |
| gap detection on disconnect (F0-07) | (Reconnect/resubscribe mechanics are also described in "Host and service" below.) `feed_loop`: on any exception or close, a `data_gap` row per subscribed market with `channel = connection`, `reason = connection_error:<ExceptionType>` (or `normal_stop` / `task_cancelled`), `ts_start_us` = disconnect time (or attempt start when the connect itself failed) -- **coalesced**: the insert is `INSERT OR IGNORE` against the one-open-gap-per-(venue, market, channel) index, so a market whose previous `connection` gap is still open (it never got its post-reconnect snapshot, bot-strategy#908) gets no new row for a later disconnect; the existing gap silently spans the connected-but-unsynced period and the next outage, and the later disconnect's time and reason survive only in `ws_connection` (`ended_ts_recv_us`, `end_reason`). Use `ws_connection` for per-session disconnects, `data_gap` for "book unusable" intervals; every `BookState` is marked unsynced; reconnect after exponential backoff 1 s → 60 s (doubling), `websockets.connect(ping_interval=20, ping_timeout=15, open_timeout=20, max_queue=4096)`, then resubscribe all `order_book` / `trade` / `market_stats` channels. Partial unique indexes guarantee at most one open gap per (venue, market, channel). |
| gap bounds survive a crash | Two mechanisms on the next start. `_recover_orphaned_sessions` ends a dead `ws_connection` segment at its last proven receive time (`last_activity_ts_recv_us`, bumped on **every** received frame since schema v9) with a durable `end_reason`. `_journal_stale_open_gaps` closes any `data_gap` still open in an *older* partition at that partition's hour boundary (`partition_start + 1 h`) and re-opens a continuation row in the next partition -- i.e. a crash bounds gaps at partition granularity, not at the exact last frame; read the `ws_connection` row for a tighter bound -- `last_activity_ts_recv_us` is the last *proven* frame, so after a quiet period the real disconnect/crash lies somewhere between it and the restart; it is a conservative bound, not the exact time. |
| replay / duplicate handling | trade identities are `trade_id_str` when present, otherwise a versioned synthetic ID scoped by exchange `nonce` for incremental messages (an ID-less incremental message **without** a nonce is refused); reconnect snapshots deduplicate via the replay-alias multiset. See the "Host and service" section below for the sealed-partition side of this. |

### What is deliberately *not* done online (and where it lands instead)

1. **Trade-channel sequence continuity is not checked.** The message
   `nonce` is stored and used for dedup scoping, but a missed
   `update/trade` between two received ones is only detectable offline
   (gaps in `trade_id` order per market, or a `connection` gap covering
   the window). F0-07's per-channel / per-sequence granularity is therefore fully met for
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
   from `book_event` / `trade` / `data_gap` / `ws_connection`.
   `reconnect_count` is the exception: a run of failed connection
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
  `SELECT channel, reason, COUNT(*) FROM data_gap GROUP BY 1, 2` and
  `SELECT venue, symbol, channel, reason FROM data_gap WHERE ts_end_us IS NULL`
  (open gaps). `channel = order_book` rows are sequence breaks; `channel =
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
snapshot can be proven synced or not, every disconnect and every
`order_book` sequence break is a bounded `data_gap` row, and server versus
receive time are both kept in a known unit. Items 1–3 above are
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
collapsing distinct nonce-scoped updates. Any trade message missing an exchange
timestamp is rejected before any row is emitted. Trade price and size must also
both be strictly positive; one invalid element
rejects the entire received trade message before any of its rows are queued.
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
manual install, provide both values explicitly.

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
