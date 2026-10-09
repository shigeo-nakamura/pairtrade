# Arcus volume / presence runtime — operations (bot-strategy#1093)

`arcus_vol_runtime` quotes one Arcus Perps market on **subaccount 0** of the
owner's wallet: post-only quotes resting `QUOTE_OFFSET_BPS` behind the touch
(presence mode; `0` = at the touch), the inventory-reducing side at the touch,
daily and cumulative stops, a 30 s per-market dead man's switch
(`scheduleCancel` with `marketId`, refreshed every 10 s). Binary: `src/bin/arcus_vol_runtime/` (module doc =
design + invariants). Feature set: `--no-default-features --features
arcus-sdk` (no lighter-sdk, no libsigner).

## Why it is on the Tokyo host

It ran on the workstation first (2026-10-01/02). Three network drops in three
days there (an ISP IPv6 prefix change, two full outages) each let the venue's
dead man's switch cancel the resting quotes, which defeats "quote
consistently". The Tokyo `debot-main` host (`i-0095af4fe0efbc5dd`,
ap-northeast-1, aarch64 AL2023) has a stable IPv4 path.

## Host layout

| path | what |
|---|---|
| `/opt/debot-arcus-vol/bin/arcus_vol_runtime` | ARM64 binary (artifact of `deploy-arcus-vol.yml`) |
| `/opt/debot-arcus-vol/manifest.json` | source sha, dex-connector ref/sha, binary sha256 |
| `/etc/debot-arcus-vol/live.env` | Arcus API credentials, **mode 600, operator-written, never deployed** |
| `/etc/debot-arcus-vol/config.env` | market / sizing / stops (optional; defaults in the launcher) |
| `/var/lib/debot-arcus-vol/state/` | `state.json` `status.json` `fills.jsonl` `KILL_SWITCH` `HALT` |
| `/var/lib/debot-arcus-vol/locks/` | per-account runtime lock |
| `/opt/debot/scripts/debot-arcus-vol.sh` | launcher (`scripts/`) |
| `/opt/debot/scripts/arcus_vol_encrypt_key.sh` | one-shot: encrypt the signing key in `live.env` under the host data key (`scripts/`) |
| `/opt/debot/scripts/debot-arcus-vol.env.example` | both env files' keys with defaults (`scripts/`) |
| `/etc/systemd/system/debot-arcus-vol.service` | unit (`deploy/`) |

## Deploy (workflow_dispatch, never starts the unit)

```bash
# build only (any branch) / deploy (master only)
gh workflow run deploy-arcus-vol.yml --repo shigeo-nakamura/pairtrade --ref master -f target=build-only
gh workflow run deploy-arcus-vol.yml --repo shigeo-nakamura/pairtrade --ref master -f target=deploy
```

The dex-connector ref comes from `Cargo.lock` through
`_resolve-dex-connector-ref.yml` (bot-strategy#899); `-f
dex_connector_ref=vX.Y.Z` overrides it for one run. The deploy job verifies
the sha256 on the host, installs binary + launcher + unit + `.env.example`,
runs `daemon-reload` and `systemctl enable`, and stops there. It prints a
NOTE when `/etc/debot-arcus-vol/live.env` or `config.env` is missing. It
never overwrites either env file.

## Credentials (operator)

```bash
sudo mkdir -p -m 700 /etc/debot-arcus-vol
sudo install -m 600 /dev/null /etc/debot-arcus-vol/live.env
sudo vi /etc/debot-arcus-vol/live.env        # ARCUS_ADDRESS / ARCUS_API_KEY / ARCUS_PLAIN_API_PRIVATE_KEY / ARCUS_ACCOUNT_INDEX=0
sudo /opt/debot/scripts/arcus_vol_encrypt_key.sh   # plain key -> ARCUS_API_PRIVATE_KEY (KMS-encrypted) + ENCRYPTED_DATA_KEY
sudo install -m 644 /opt/debot/scripts/debot-arcus-vol.env.example /etc/debot-arcus-vol/config.env  # then keep only the config.env block
```

`arcus_vol_encrypt_key.sh` (bash + openssl + aws-cli, no python packages)
decrypts the host's `ENCRYPTED_DATA_KEY` (from `live.env`, else
`/opt/debot/scripts/debot_secrets_common.env`) through KMS in **eu-central-1**
(the hosts' shared data key; this is debot-utils' default region, so leave
`AWS_REGION` unset), encrypts the 32 key bytes with AES-256-CBC under a
random IV, verifies the roundtrip, and rewrites `live.env` atomically. No
plaintext copy is kept by default; `--keep-plain-backup` leaves the previous
file as `live.env.plain.bak` (mode 600) for a manual rollback — delete it
once the service has started. The output is byte-identical to
`scripts/encrypt.py <data-key> 0x<hex>`; the runtime decrypts it with
`debot_utils::decrypt_data_with_kms(.., output_as_hex = true)`.

Both files are parsed (never sourced) against an allow-list: `live.env` may
only carry `ARCUS_ADDRESS`, `ARCUS_API_KEY`, `ARCUS_ACCOUNT_INDEX`,
`ARCUS_API_PRIVATE_KEY`, `ENCRYPTED_DATA_KEY`, `ARCUS_PLAIN_API_PRIVATE_KEY`;
`config.env` only the keys in the example file. Anything else (an endpoint
override, a second account index) refuses the start, and the account index
is re-checked after both files are loaded. The launcher refuses to start
unless `live.env` is mode 600, complete, and `ARCUS_ACCOUNT_INDEX=0`. A plain `ARCUS_PLAIN_API_PRIVATE_KEY` starts only
with `ALLOW_PLAIN_KEY=1` in `config.env` (testing); the launcher never
exports a plain and an encrypted key together (the config loader would
prefer the plain one).

## Start / stop (operator only — CI never does this)

```bash
sudo systemctl start debot-arcus-vol      # launcher: sentinels -> venue flat check -> sizing -> exec runtime
sudo systemctl status debot-arcus-vol
sudo journalctl -u debot-arcus-vol -f | grep -E '\[ARCUS_VOL\]|\[arcus\]|WARN|ERROR'
sudo cat /var/lib/debot-arcus-vol/state/status.json | jq '{plan, quotes, inventory, pnl, halt}'
sudo systemctl stop debot-arcus-vol       # SIGTERM: cancel-all, fill harvest, persist, DMS disarm
```

Before a start, the launcher checks on the venue that the market is flat
and has no open orders on subaccount 0; it refuses otherwise (log line
`REFUSED: ...`). Sizing is recomputed from **free collateral** at every
start (`clip = min(CLIP_MAX_USD, free * LEVERAGE / 2 * 0.9)` rounded down to
$100; inventory cap 2 clips), so other positions on the cross account
shrink the quotes rather than over-commit margin.

### Per-position stop (optional)

`POSITION_STOP_BPS` in `config.env` (bot-strategy#1093, 2026-10-08): once the
open position's adverse move against its average entry reaches this many bp
at a fresh mid, the runtime flattens it like any other flatten (pull quotes,
reduce-only taker IOC) and keeps quotes pulled for 60 s (at least
`ARCUS_VOL_COOLDOWN_SECS`); then it quotes again — it is not a halt. It ranks
after startup / halt / cap and before the 300 s max-hold; dust never
triggers; with no fresh mid it is not evaluated (a WARN once a minute). Each
trigger logs `POSITION STOP: ...` and appends a `position_stop` row (entry,
mark, adverse bp) to `fills.jsonl`; `status.json` shows `position_stop_bps`
and `position_stops_today`. First setting 25 bp — far enough that normal
noise around a 2-5 bp quote does not trip it; to be tuned on the recorded
tape.

### Session offset (optional)

`SESSION_OFFSET_BPS` / `SESSION_BAND_BPS` in `config.env` (both or neither)
make the quotes rest further behind the touch while the market's venue
session is open — for the US equity/ETF perps 04:00–20:00 New York on
weekdays — and at `QUOTE_OFFSET_BPS` / `REPEG_BAND_BPS` off-hours and at
weekends. Reason (grid_report.md, 2026-10-05, SPY): at 2 bp the swept quotes
cost about −1.4 bp per $ of volume in session but only −0.1 bp/$ off-hours;
5 bp in session is about break-even.

The session comes from the market's own row, `GET /v1/markets?market=…`,
read once a minute only when a session offset is set: the venue's
`isOutsideRth` flag decides while the last read is < 3 min old (it also
knows holidays); otherwise `regularTradingHours` is evaluated on the host
clock with the US daylight-saving rule (`America/New_York` only). A market
without hours (crypto) never switches. Each switch logs one line
(`[ARCUS_VOL] SPY-USD: in session → quotes rest 5 bp behind the touch`), and
`status.json` shows `session` (`in` / `off`) with the active
`quote_offset_bps` / `repeg_band_bps`. Changing the values needs a restart.

### Quote gate (bot-strategy#1120) — shadow mode

An adverse-selection gate that decides, per side and per quote cycle,
whether the baseline quote should rest or be pulled. Design:
`~/bot/logs/studies/2026-10-04-quote-gate-1120/DESIGN.md`. It sits after
`plan_quotes` and before `quote_action`, so it can only REMOVE a quote the
baseline planned (never add one, never price an unpriced side). The safety
plan (`tick_plan`: flatten / halt / stale / shock / DMS) is untouched.

`GATE_MODE` in `config.env` (launcher keys `GATE_*` → runtime
`ARCUS_VOL_GATE_*`; a restart is needed to change them):

| mode | effect |
|---|---|
| `off` (default) | nothing computed or written; the tick is as before |
| `shadow` | every quote cycle is scored and logged; **the plan is never changed** (`apply` is the identity, unit-tested) |
| `enforce` | pulls are applied. Needs `GATE_ENFORCE_CONFIRM=1120-arcus` AND `GATE_MODEL`, after a shadow readout PASS (design §10, owner decision) |

Model file: `GATE_MODEL=<path>` (design §5.2 JSON envelope, `kind`
`linear` in this build; `forest` / `lookup` are refused until their
evaluators land). At start the runtime recomputes the file's
`payload_sha256`, checks `venue` / `market` against this runtime and
`feature_names` against its feature engine, and refuses to start on any
mismatch — in shadow as in enforce. The deploy workflow installs the shipped
`configs/quote_gate/quote_gate_linear_arcus_btc.json` (the 10-01 Binance
premium-deviation rule, θ = −0.25 bp, **BTC-USD only**) at
`/opt/debot-arcus-vol/gate/quote_gate_linear_arcus_btc.json`; a model for
another market must be a separate file for that market. One `[GATE] loaded
kind=… market=… theta=…bp sha=<12>` line at start is the fingerprint.

Reference feeds (named by the model's `feature_spec.ref_feeds`): one
Binance USDT-M combined WS (`bookTicker` + `aggTrade`) and one Hyperliquid WS
(`bbo` + `trades`), reconnecting with backoff; the public Arcus `trades`
tape is subscribed in every mode while the gate is on (own-flow features).
Features use only records received strictly before the decision time
(`rx < τ`, the 37 features of #1113 `rf_common`; a golden test pins them to
the Python values on a real tape slice).

Fail-safe (`Fallback`, with a reason, never a zero-filled score):
`model_missing`, `warmup:<s>` (900 s from start and after a reference
feed's reconnect, `GATE_WARMUP_S`), `ref_stale:<feed>:<age_ms>` (no record
in 10 s, `GATE_REF_STALE_MS`), `ref_gap_cooldown:<feed>:<s>` (60 s after a
connect/gap event, `GATE_EVENT_COOLDOWN_S`), `feed_down:<feed>` (a feed
that reported Down and has not reconnected, however long ago — the own
trades tape can be quiet while connected, so its age alone is not used),
`feature_gap:<name>` (no history yet, NaN). The action a fallback takes under enforce is
`GATE_FALLBACK`: `baseline` (default: quote as planned), `pull`, or
`baseline_then_pull:<secs>`; in shadow it is only recorded.

What shadow writes (all in `STATE_DIR`):

- `gate_YYYYMMDD.jsonl` (UTC day, no fsync): one row per quote cycle —
  `{"kind":"gate","ts_ms":τ,"market","mode","forced_shadow","model_sha",
  "ref_age_ms":{own_book,own_tape,binance,hyperliquid},"features":{…37…},
  "bid":{"decision":"scored"|"fallback","action":"quote"|"pull","pred_bps",
  "reason","baseline_qty","reducing_side"},"ask":{…},"applied":false}`.
  `GATE_LOG_FEATURES=none` drops `features` (keep `all` for the shadow
  week: it is the re-calibration data). ≈ 2 Hz × ~600 B; archive it with
  `fills.jsonl`.
- `fills.jsonl`: every `fill` row gains `"gate": {ts_ms, mode, decision,
  action, pred_bps|reason, model_sha, applied}` = the gate cycle in force at
  `t_fill − 0.3 s` (the labels' τ), or `null` (gate off, or no cycle known
  after a restart). Existing keys are unchanged; replay ignores `gate`.
  The shadow readout (pull-group vs kept-group 30 s markout) needs only
  these rows plus the existing `markout` rows.
- `status.json` `gate`: `mode`, `effective_mode`, `model` (kind, sha,
  theta), `cycles`, `fallback_counts`, `fallback_active`, `pull_rate_5m`,
  `pred_bps_last`, `ref_age_ms`, `warmup_secs_left`. The 60 s summary line
  ends with `gate=shadow pull5m b=18% a=22% fb=none`.

Markets without a reference feed (SPY-USD, QQQ-USD, NVDA-USD, GLD-USD):
the BTC model is refused for them (market mismatch). Shadow WITHOUT
`GATE_MODEL` is allowed and records `fallback model_missing` on every cycle
(nothing else is computed), which only proves the plumbing. A gate for the
equity/ETF perps first needs a lead-lag study of the candidate references
(Lighter index, Polymarket idx, Nado — design §8 / Q4), a feature-engine
variant on that reference plus session flags, and its own frozen cell.

To run the shadow week as designed (D9), use a **DRY_RUN BTC-USD at-touch
instance** with its own `STATE_DIR` (never the live presence bots'
subaccount) and `GATE_MODE=shadow`, `GATE_MODEL=…arcus_btc.json`,
`GATE_LOG_FEATURES=all`. Turning the gate on, as any config change, is an
operator restart.

### Sentinels (in `STATE_DIR`)

| file | meaning |
|---|---|
| `KILL_SWITCH` | runtime pulls quotes, flattens (reduce-only IOC), halts while present. The launcher does **not** start while it exists (exit 0, so `Restart=on-failure` stays quiet): `sudo rm .../KILL_SWITCH` first. |
| `HALT` | written by the runtime on a sticky halt (cumulative stop, unrecoverable reconcile). Read the journal, then remove it by hand before the next start. |
| `GATE_OFF` | quote gate (bot-strategy#1120): while present, `GATE_MODE=enforce` is demoted to shadow from the next tick (no restart; rows carry `forced_shadow: true`). No effect in shadow / off. |

### What the dead man's switch means for manual trading

The switch is **per-market** (`scheduleCancel` with `marketId`, dex-connector
4.7.44+): if the host cannot reach the venue for ~30 s (or the runtime dies),
the venue cancels only the open orders **in the runtime's market**
(`MARKET`, e.g. SPY-USD). Manual orders in other markets survive. Before
dex-connector 4.7.44 the switch was account-wide and cancelled every open
order on subaccount 0 (a manual NEAR-USD limit was lost that way on
2026-10-01). Positions are never touched.

- Manual orders **in the runtime's own market** are still cancelled as
  stray orders (and by the switch) — keep those on another subaccount.
- On its first arm the runtime also disarms the account-wide switch once,
  in case an older build left it armed (it would otherwise fire and cancel
  every market). Nothing else on subaccount 0 may rely on the account-wide
  switch.
- The runtime only reports the switch armed when the venue echoed the
  market (`marketId`) back: a gateway that ignored the field would have
  armed the account-wide switch, so that is an error, no quote is sent, and
  the connector immediately disarms the account-wide switch again (the error
  line says whether that worked).
- At shutdown an unconfirmed per-market disarm is only a WARN ("…switch may
  still fire within 30s -- it only cancels SPY-USD orders, which are already
  cancelled; exiting normally"): the quotes were cancelled and read back
  before the disarm.
- Venue quotas: 10 auto-fires per UTC day per subaccount (shared by all of
  its switches), at most 50 armed switches per wallet.

## Moving state from another host

`state.json` carries the cumulative-stop accounting and `fills.jsonl` the
journal. To continue a run elsewhere: stop the old runtime, copy both files
into `STATE_DIR` on the new host (same market), then start. Two runtimes on
the same subaccount at once (even from different hosts) must never happen:
each believes it owns the account's orders and the dead man's switch.
