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
  keep both in sync until #872 freezes a data-driven value). Fails open
  (proceeds without the gate, logged as a warning) on a fetch/parse error.
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
  last remainder, so the drawdown halt sees the aggregate. While an entry
  is `UNCONFIRMED`, `status.json` publishes `positions_ready=false` and
  `han_bridge.position_unconfirmed=true` until the day rolls. The pre-send
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

5. **Verify with DRY_RUN before ever touching CONFIRM_LIVE.**
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
- A crash mid-day loses in-memory `t0`/`t1` price snapshots and any
  not-yet-persisted entry state; `RiskState.last_session_date` prevents
  re-entering a day already acted on before the crash, but does not
  recover an in-flight entry/exit decision. This prototype does not persist
  `OpenPosition` to disk -- after a restart mid-position, check the real
  Lighter account balance/position via the exchange directly, not this
  service's own state file, before assuming no position is open.
