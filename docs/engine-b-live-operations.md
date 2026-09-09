# Engine B live-experiment operations

This runbook covers `engine-b-live.service` for bot-strategy#866 / Project 8.
Use UTC for all timestamps.

## What this is (and is not)

This is the minimal-notional infrastructure smoke test the user explicitly
chose on 2026-09-02 to reach a live trade by 2026-09-10, skipping Phase
0A/0B (statistical validation of the H1 hypothesis) and Phase 1 (paper
trading) -- see bot-strategy#866's "方針転換の記録" comment and the plan at
the PR that introduced this binary. It is **not** the validated Phase 1/2
implementation the requirements doc (`engine_b_requirements_0.3.md`)
describes.

See `src/bin/engine_b_live.rs`'s module doc for the exact strategy shape
(single-symbol directional bet on the US primary, driven by an unvalidated
`epsilon = ln(kr_t1/kr_t0) - ln(us_t1/us_t0)` diff signal) and its KNOWN
GAPS section -- do not duplicate that list here; read it there.

The order path itself (Lighter decimals / minimum sizes, rate limits, IOC
/ reduce-only / cancel / client-order-ID semantics, and the gaps between
what this binary does and what the requirements doc specifies) is
documented in `docs/engine-b-order-spec.md` (bot-strategy#875, A-3 / A-8
/ A-9). Read its §4 gap table before any `CONFIRM_LIVE` flip.

## Safety boundary

- `order_capability` is real for this binary (unlike `engine-b-phase0`,
  which has none) -- it places real orders once `ENGINE_B_LIVE_DRY_RUN=false`
  **and** `ENGINE_B_LIVE_CONFIRM_LIVE=yes-i-mean-it` are both set. Setting
  only the first refuses to start (`anyhow::bail!` in `main()`). Never work
  around this by patching the binary; it exists so flipping DRY_RUN off is
  always a deliberate two-variable act, not a config-file typo.
- `InaccessiblePaths=/opt/debot /opt/engine-b-phase0` in
  `deploy/engine-b-live.service`: this identity cannot read the Robinhood
  trading bot's freq/b/freq2 credentials, nor the Phase 0 observer's own
  (credential-less) tree. Its own Lighter credentials live only in
  `/etc/engine-b-live/live-secrets.env`, owned `root:engine-b-live`, mode
  0640 or tighter, readable only by this service's own dedicated
  `engine-b-live` Unix identity.
- `lot_usd` is hard-capped in code by `EngineBLiveConfig::max_notional_usd()`
  (`equity_usd_reference * leverage * 0.9`) regardless of the configured
  `ENGINE_B_LIVE_LOT_USD` -- a config typo cannot send an oversized order.
- `ENGINE_B_LIVE_MAX_SESSION_LOSS_BPS` (default 500 = 5%) engages a sticky
  session halt on realized drawdown, cleared only by creating the file at
  `ENGINE_B_LIVE_RISK_ACK_PATH` (default
  `/var/lib/engine-b-live/RISK_ACK_ENGINE-B-LIVE`); `ENGINE_B_LIVE_KILL_SWITCH_PATH`
  (default `/var/lib/engine-b-live/KILL_SWITCH`) blocks all new entries
  while present, independent of drawdown. Same on-disk convention as
  `robinhood_dipgrid.rs` / pairtrade's own RISK_ACK/KILL_SWITCH pattern
  (see `runbook_risk_ack.md`), reimplemented locally in this binary since
  pairtrade's `risk_io` module is private to the `pairtrade` module tree.
- **Same-day eligibility gate** (bot-strategy#872): right before submitting
  an order, the engine fetches Lighter's public `orderBookDetails` REST
  endpoint (`ENGINE_B_LIVE_LIGHTER_REST_URL`, default
  `https://mainnet.zklighter.elliot.ai`) and skips today's entry if
  `kr_primary`/`us_primary` is `force_reduce_only`, not `status=active`, or
  below `ENGINE_B_LIVE_MIN_DAILY_VOLUME_USD` (default `100000`, same
  placeholder value as `engine_b_phase0.py`'s `MIN_DAILY_VOLUME_USD` --
  keep both in sync until #872 freezes a data-driven value). **Fails
  closed** (bot-strategy#916): a fetch/parse error blocks the entry and is
  retried on the next 5 s tick, bounded by
  `ENGINE_B_LIVE_MAX_ELIGIBILITY_ATTEMPTS` (default 6) and by the entry
  deadline; once the attempts are spent the day is skipped with
  `skip_reason=eligibility_unavailable`. It used to fail *open* -- a single
  REST hiccup would have traded straight through a `force_reduce_only`
  market.
- **Fail-closed signal inputs** (bot-strategy#916): every entry decision
  reads only price observations that passed ingest validation (positive mid
  inside a two-sided, uncrossed book; a venue timestamp that is either
  plausible-and-recent or discarded as a broken clock), arrived on the
  current price-feed generation (a broadcast `Lagged` bumps it, so nothing
  observed before dropped updates is reused), and are no older than
  `ENGINE_B_LIVE_MAX_PRICE_STALENESS_SECS` (default 30). Consequences:
  - `t0`/`t1` are snapshotted only once both primaries have a usable price;
    a partial or stale snapshot is never captured or persisted.
  - The `t0` capture is bounded by `ENGINE_B_LIVE_T0_CAPTURE_GRACE_SECS`
    (default 300) after KRX open, whether or not a usable snapshot exists
    by then: nothing usable ends the day with `skip_reason=no_usable_t0`,
    and a snapshot that only *becomes* usable past the bound (a feed
    recovering at t0+301 s) ends it with `skip_reason=late_t0`. Neither is
    backfilled as if it were the open.
  - The order-sizing price is re-checked immediately before the send, not
    reused from the `t1` capture (the eligibility fetch and position read in
    between are awaits). So is the signal itself: immediately before the
    send, **every leg it rests on must still be a live observation** —
    same feed generation *and* inside the staleness bound, read under one
    lock. Either a lag or a leg that simply stops reporting (the KR feed
    stalling while the US feed keeps ticking) discards the whole `t1`
    capture and recomputes epsilon on the next tick; a fresh US price
    never makes a stale KR value sendable. `t0` is exempt — it is a
    historical boundary reference, not a current price.
  - Every terminal no-entry path records a `skip_reason`, logged as
    `[SKIP] ...` and surfaced in `status.json` under `han_bridge`, alongside
    `stale_or_missing_symbols` and `price_feed_generation`. It is persisted
    to `risk_state.json` as `last_session_skip_reason` beside
    `last_session_date`, so a same-day restart restores why the day was
    settled, not only that it was. A day that is *already* settled — entered
    this run, or restored from `last_session_date` after a restart — skips
    the `t0` capture entirely, so restarting after a completed cycle cannot
    record a `no_usable_t0` over a day that actually traded
    (bot-strategy#965).
  - **These gates are entry-only.** `maybe_exit` and the unconfirmed-position
    adoption path read the last price raw, so a stale feed can never keep an
    open position from being closed or an unknown exposure from being
    adopted. The feed runs on its own task writing into a shared
    `PriceFeed`, not as an arm of the tick loop, so a `Lagged` during the
    seconds a tick spends awaiting the exchange bumps the generation while
    entry preparation is still in flight and the send-time check sees it.
    A PnL booked off a stale mid is logged as such; if *every*
    update has been rejected at ingest (e.g. a venue replaying a stale
    snapshot after a restart) the close still goes out and the PnL is
    booked off the last raw mid, or as a last resort off the entry price
    with `source=entry_price_pnl_unknown` in the log -- reconcile that one
    from the exchange fill.
- **Both legs are bounded taker IOCs** (bot-strategy#918, #978;
  dex-connector v4.7.24): `submit_order` sends
  `create_order_taker_ioc_at` at the marketable limit
  `mid * (1 ± ENGINE_B_LIVE_SLIPPAGE_BPS)` (default **50**) computed from
  this process's own observation, tick-rounded inward by the connector,
  remainder cancelled. `ENGINE_B_LIVE_SLIPPAGE_BPS` is therefore a bound
  against the **mid** and the venue does not re-anchor it on its own
  touch at submit time (which the earlier touch-relative bps send could,
  #918's residual). This replaces `create_order(price = None)`, whose
  ±20 % protection price bounded a $100 lot at $20 per leg. The value is
  still validated at startup against the connector's accepted `1..=1000`,
  because the one path left without a book of its own to price against --
  a reduce-only exit with no observed quote -- still sends it as a
  percentage: the process refuses to start outside it rather than losing
  a session day to a rejected send (only one entry `sendTx` is allowed
  per day, G-4). Two consequences at the first live
  cycle: a size that truncates to zero at the market's size decimals is
  rejected instead of being forced up to one size tick, and **freshness
  is now entirely this process's job** — the absolute-limit path
  deliberately carries no staleness gate of its own (there is no
  reference price in the connector to age-check), where the older
  percentage path failed the send on a book the connector considered
  stale. The engine's own gates stand in for it, on **both** legs: the
  observation the limit is priced from must be usable at send time --
  fresh clock, within `ENGINE_B_LIVE_MAX_PRICE_STALENESS_SECS`, current
  feed generation, not future-dated. An entry priced off anything else is
  refused; an **exit** falls back to `create_order_taker_ioc` against the
  connector's own live touch (logged as `[EXIT] no usable book ...`).
  That fallback is not cosmetic: `maybe_exit` deliberately closes on
  prices too stale to enter on, and an absolute limit off a dead quote
  does not re-anchor the way the old percentage did — a stopped feed
  keeps handing out the same observation, so every reduce-only IOC would
  come back unmarketable and the position would stay open. If a live send is ever rejected for crossing the
  bound, widen `ENGINE_B_LIVE_SLIPPAGE_BPS` deliberately with the
  observed book in hand — never back to an unbounded market order.
- **Fill confirmation against the exchange** (bot-strategy#875 G-2/G-4,
  `docs/engine-b-order-spec.md` §4 -- introduced by pairtrade#272, so the
  file is absent until that PR merges): a live entry is only recorded once
  the WS-fed `get_positions()` shows the `us_primary` position. The check
  is a small state machine advanced once per 5 s tick (never a blocking
  wait in the select loop) for up to `ENGINE_B_LIVE_FILL_CONFIRM_TIMEOUT_SECS`
  (default 15) after the sendTx -- Lighter's HTTP 200 means "accepted",
  not "executed". Outcomes: position seen → `[ENTRY] ... confirmed_by=
  exchange_position` (partial fills logged; `entry_price_estimated=true`
  when the exchange gave no `avg_entry_price` and the WS mid was used);
  none within the window → `Han Bridge ENTRY UNFILLED` (or `ENTRY FAILED`
  if the sendTx itself had errored), day marked acted, **no retry**;
  account unreadable at the end of the window (even if an earlier read was
  flat -- a fill update can land after a flat reading) → `Han Bridge ENTRY
  UNCONFIRMED`, day marked acted, no position tracked: check the exchange
  manually before the exit window. Partial exits: each observed reduction is booked as realized at the WS
  mid of the attempt that closed it, and the final flat books only the
  last remainder, so the drawdown halt sees the aggregate. An `UNCONFIRMED` entry
  sets `risk_state.json`'s `position_unconfirmed` (persisted, survives
  the day roll) and engages the session halt: `status.json` publishes
  `positions_ready=false` and `han_bridge.position_unconfirmed=true`, the
  engine keeps reading the exchange every tick and adopts the position if
  it appears (`Han Bridge ENTRY ADOPTED`; deferred while no entry price
  or WS price is available), the flag stays set even after adoption
  (the adopted position is memory-only, so a restart re-runs the
  adoption), and only RISK_ACK -- after the operator has reconciled
  against the exchange -- clears the flag and the halt. A same-side
  position that grows outside the engine's own orders re-bases the cost
  on the exchange's average entry price and halts new entries. Any
  position that is not today's own confirmed entry (adopted with unknown
  origin, recovered after `UNCONFIRMED`, or carried over midnight) is
  flattened on the next tick with emergency semantics instead of waiting
  for today's `t2` -- its intended exit window is unknown or already past. `status.json`'s position `size` is the live open quantity, not the
  entry quantity. The pre-send
  marker write is checked: if `risk_state.json` cannot be written the
  order is not sent that tick. **At most one entry `sendTx` per session day** --
  `risk_state.json`'s `last_session_date` is written *before* the send
  (so a crash/restart mid-confirmation cannot re-submit; after such a
  restart, check the exchange for a position this process no longer
  tracks), and a send error is never followed by a re-submit, only by
  the same position watch. A position the exchange already holds before submit is
  adopted (`adopted_from_exchange=true origin=unknown`) instead of
  re-ordered, and a position carried over from a previous session (exit
  kept failing) blocks today's entry entirely. **Side mismatch** between
  what was submitted and what the exchange holds (entry or exit) records
  the exchange's side and entry price, then engages the sticky session
  halt (`Han Bridge SESSION HALT`, cleared by RISK_ACK) -- same bar as
  pairtrade's SignFlip verdict. Every exit is sized to the exchange's
  current position (capped at 1.5× the tracked size against a transient
  over-report) and only counts as done when the exchange reports flat; if
  the account channel is unreadable the exit waits, except past
  `exit_deadline` where a reduce-only for the tracked size is sent anyway
  (reduce-only caps it at the real position). Known caveat: dex-connector's
  `positions_ready` is not reset on WS reconnect (bot-strategy#911), so a
  read right after a reconnect can be stale.
- **No SIGTERM-graceful-close handling exists in this prototype.** An open
  position is not reduce-only-closed on service stop/restart. Before any
  planned restart, check `status.json`'s `has_position` field and either
  wait for the scheduled exit window or manually close the position first.
  Since bot-strategy#917 the restart is at least no longer *blind*: the
  position is persisted and the next start reconciles it against the
  exchange before it is allowed to enter anything (see Stop and recovery).
  That is recovery, not graceful shutdown -- the position still rides
  through the restart unhedged.

## Host and service

- EC2: `debot-robinhood-lighter` (`i-0095af4fe0efbc5dd`, `ap-northeast-1`) --
  same host as `engine-b-phase0.service` and the Robinhood trading bot;
  Engine B's live binary is a fully separate credential-isolated identity
  on that shared host, not a new instance.
- Service: `engine-b-live.service`
- Code/config (root-owned, read-only): `/opt/engine-b-live/` (binary at
  `bin/engine_b_live`, `trading_calendar.json`, `live-config.env` generated
  from `configs/engine-b/live.json` by `scripts/install_engine_b_live.sh`,
  and this identity's own copy of the Lighter SDK signer library at
  `lib/libsigner.so` -- `InaccessiblePaths=/opt/debot` means this process
  can never load the Robinhood trading bot's `/opt/debot/lib/libsigner.so`
  at runtime, so `engine-b-live.service` sets
  `LD_LIBRARY_PATH=/opt/engine-b-live/lib` and points at this dedicated
  copy instead)
- Secrets (root:engine-b-live, this identity only): `/etc/engine-b-live/live-secrets.env`
- State (this identity, writable): `/var/lib/engine-b-live/` (`risk_state.json`,
  `status.json`, `pnl.jsonl`, `KILL_SWITCH`, `RISK_ACK_ENGINE-B-LIVE`)
- Metrics/health: none yet in this prototype (no `/metrics` endpoint) --
  operational visibility is `status.json` (written every 30s) and the
  systemd journal.

## Credential provisioning (one-time, before first start)

The installer (`scripts/install_engine_b_live.sh`) deliberately never
touches `/etc/engine-b-live/live-secrets.env` beyond creating its parent
directory -- this file must be assembled by hand, once, following this
runbook. Without it, `get_lighter_config_from_env`'s `.expect(...)` calls
make the service fail to start (fail-closed, not a silent bad default).

1. **Create the Lighter mainnet account.** New EVM wallet, fund with the
   $1000 USDC equity. Register a Lighter API key for it to obtain
   `api_key_public`, `api_key_private`, and `api_key_index`. Record the
   wallet address (`LIGHTER_WALLET_ADDRESS`); `LIGHTER_ACCOUNT_INDEX=0`
   lets `dex-connector` auto-discover the real account index from the
   wallet address at connector startup (`discover_account_index`), so it
   does not need to be looked up by hand.

2. **Determine whether the shared `ENCRYPTED_DATA_KEY` can be reused.**
   The other bots on this host (freq/b) already have a KMS-wrapped AES data
   key provisioned as `ENCRYPTED_DATA_KEY` in the common secrets file their
   launcher (`/opt/debot/scripts/debot-pair-robinhood-lighter.sh`) sources:
   `/opt/debot/scripts/debot_secrets_common.env` (mode 0600, owner
   `ec2-user:ec2-user` -- confirmed present 2026-09-03, alongside
   `GMAIL_APP_PASSWORD`/`GMAIL_TO`/`GMAIL_USER`; not under `/opt/debot/*.env`
   directly). Check only for the **existence** of that variable name on the
   host (never its value) before deciding:
   ```bash
   aws ssm send-command --region ap-northeast-1 \
     --instance-ids i-0095af4fe0efbc5dd \
     --document-name AWS-RunShellScript \
     --parameters file:///path/to/check-encrypted-data-key.json \
     --query Command.CommandId --output text
   # check-encrypted-data-key.json: {"commands": ["grep -l '^ENCRYPTED_DATA_KEY=' /opt/debot/scripts/*.env 2>/dev/null || echo none"]}
   ```
   If found, reuse that same plaintext AES key (retrieve it the same way
   it was obtained when freq/b were provisioned -- outside the scope of this
   repo, ask the operator who set that up) and its `ENCRYPTED_DATA_KEY`
   ciphertext for this account too. If not found, generate a new one via
   `aws kms generate-data-key` against this project's existing KMS key (do
   not create a new KMS key without checking with the operator first) and
   record the new `ENCRYPTED_DATA_KEY` ciphertext.

3. **Encrypt the new account's API key values.**
   ```bash
   python3 scripts/encrypt.py "<plaintext-AES-key-base64>" "<api_key_public>"
   python3 scripts/encrypt.py "<plaintext-AES-key-base64>" "<api_key_private>"
   ```
   Each prints a base64 ciphertext -- these become `LIGHTER_PUBLIC_API_KEY`
   and `LIGHTER_PRIVATE_API_KEY` respectively.

4. **Assemble `/etc/engine-b-live/live-secrets.env` on the host** (mode
   0640, owner `root:engine-b-live` -- `scripts/install_engine_b_live.sh`
   creates the parent directory with the right group already):
   ```
   ENCRYPTED_DATA_KEY=<ciphertext from step 2>
   LIGHTER_PUBLIC_API_KEY=<ciphertext from step 3>
   LIGHTER_PRIVATE_API_KEY=<ciphertext from step 3>
   LIGHTER_API_KEY_INDEX=<from step 1>
   LIGHTER_ACCOUNT_INDEX=0
   LIGHTER_WALLET_ADDRESS=<from step 1>
   ```

5. **Add the notification credentials (bot-strategy#968).** The binary's
   only push alert path is `EmailClient`, which needs all three of
   `GMAIL_USER`, `GMAIL_TO` (or `TO_ADDRESS`) and `GMAIL_APP_PASSWORD`.
   Without them every ENTRY / EXIT / SESSION HALT notification is dropped
   after a single `WARN`, which is exactly how this service ran from
   2026-09-01 to 2026-09-08 notifying nobody. `notification_gate` now
   refuses to start a **live** run without them (DRY_RUN logs an error and
   continues); an operator who really means to run live with no alert path
   sets `ENGINE_B_LIVE_ALLOW_NO_NOTIFICATIONS=yes-i-know` and it stays on
   the record in the log.

   The same three values already exist on this host in
   `/opt/debot/scripts/debot_secrets_common.env` for the Robinhood arms,
   but this unit cannot read them: `InaccessiblePaths=/opt/debot` is part
   of its isolation and stays that way. Copy them into
   `/etc/engine-b-live/live-secrets.env` instead, **stripping the `export`
   prefix and any surrounding quotes** -- systemd's `EnvironmentFile=`
   parses neither (the same trap that broke `ENCRYPTED_DATA_KEY` on
   2026-09-03):
   ```
   GMAIL_USER=<same as the Robinhood arms>
   GMAIL_TO=<same>
   GMAIL_APP_PASSWORD=<same>
   ```
   Confirm with a real send after the next restart -- an unnoticed SMTP
   auth failure looks the same as a working channel until something needs
   to alert.

6. **Verify with DRY_RUN before ever touching CONFIRM_LIVE.**
   `live-config.env` (installed from `configs/engine-b/live.json`) already
   sets `ENGINE_B_LIVE_DRY_RUN=true` by default. Start the service
   (`sudo systemctl start engine-b-live.service` -- this first start is an
   explicit operator action, same as any other service start on this
   host) and confirm via `journalctl -u engine-b-live -f` that:
   - the connector connects and subscribes without error
   - `status.json` (`/var/lib/engine-b-live/status.json`) shows
     `calendar_version` matching the committed `trading_calendar.json`
   - `[DRY_RUN] would submit ...` lines appear at the next scheduled entry
     window instead of real order errors

   Only after a clean DRY_RUN cycle, and with the user's explicit
   go-ahead, set `ENGINE_B_LIVE_DRY_RUN=false` and
   `ENGINE_B_LIVE_CONFIRM_LIVE=yes-i-mean-it` in `live-secrets.env` (not
   `live-config.env` -- keep the live-confirmation flag colocated with the
   credentials it gates, not in the git-reviewable non-secret file) and
   restart.

## Stop and recovery

- `sudo systemctl stop engine-b-live.service` does not close an open
  position (see Safety boundary above) -- check `status.json` first.
  SIGTERM/SIGINT are handled only to make the state durable and to log and
  notify exactly what stays open (`[SHUTDOWN] SIGTERM: ... is STILL OPEN
  and is NOT being closed here`); no reduce-only is sent on the way out,
  deliberately -- a close this process cannot confirm is worse than a
  documented open position (bot-strategy#917).
- The open position **is** persisted, as `RiskState.open_position` in
  `risk_state.json`, and reconciled against the exchange on the first tick
  after a start (`[RECONCILE]` lines, bot-strategy#917). No entry is sent
  before that comparison succeeds, and a `get_positions()` that keeps
  failing keeps entries blocked rather than letting one through blind.
  What the reconciliation does, live:

  | risk_state.json | exchange | outcome |
  |---|---|---|
  | no position | flat | clean start |
  | matching position | same side and size | resumed; exits at its own `t2`, or at once if that window has already passed |
  | position | *different* side or size, same symbol | the exchange's position is adopted for immediate close **and** the session halts |
  | position on a symbol `us_primary` no longer names | that symbol still open | **not** closed here: this engine only ever submits orders for `us_primary`. The record is parked in `unmanaged_positions`, the session halts, and **an operator must flatten it by hand**. It stays in `status.json`'s position list and in the shutdown alert, refreshed from the venue on every start, until the venue reports it gone |
  | position | flat | halt: it was closed at a price this process never saw, so its PnL is unbooked |
  | no position | holds one | adopted for immediate close **and** the session halts |

  Every halt above clears only via `RISK_ACK` (see the risk runbook), so
  an operator sees it before any new entry goes out.
- Under `DRY_RUN` the exchange is not the authority: the simulated
  position is resumed from `risk_state.json`, and a real position on the
  account is reported (`[RECONCILE] DRY_RUN, but the exchange holds ...`)
  but never adopted or closed by this process.
- A crash mid-day still loses the in-memory `t0`/`t1` price snapshots
  beyond what `RiskState.t0_prices` recovers, and
  `RiskState.last_session_date` remains what prevents re-entering a day
  already acted on. After a restart mid-position, the `[RECONCILE]` line
  in the journal is the record of what the account actually held -- read
  it rather than assuming the state file alone was right.
