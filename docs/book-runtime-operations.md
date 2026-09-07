# Book runtime operations — `xsmom-695` on the Tokyo host

Runbook for the first `book-runtime` instance (bot-strategy#937 / #695).
Design and contracts are in `docs/book-runtime.md`; this file is the
"what runs where and how to check it" companion. All times UTC.

## Data flow

```
this workstation (cron)                     S3                          Tokyo host i-0095af4fe0efbc5dd
~/bot/scripts/xsmom_shadow_695.py  ─┐                                   book-signal-fetch-xsmom-695.timer (*/5 min)
  ledger.jsonl "rebalance" row      ├─▶ scripts/xsmom_signal_producer.py ─▶ s3://debot-dashboard/debot/book/xsmom-695/signal.json
                                    ┘   (hourly :25, only on rebalance days)          │
                                                                                      ▼
                                                             /var/lib/book-runtime/xsmom-695/signal.json
                                                                                      │
                                                             book-runtime-xsmom-695.service (5 s ticks, DRY_RUN)
                                                               state.json / ledger.jsonl / pnl.jsonl / status.json
                                                               status.json ─▶ s3://debot-dashboard/debot/status/book-xsmom-695/
```

- The producer is a thin adapter: the shadow watcher already owns the
  universe screen, ranking, and quantile membership, so the DRY_RUN book
  tracks the pre-registered shadow track 1:1 (`weight = side × notional /
  1000`). It refuses to emit off-grid dates or books that violate the
  runtime caps (exit 2), and writes nothing on non-rebalance days.
- Decision grid: anchor 2026-07-03, every 5 days, decision 00:30, grace
  90 min (`signal_grace_secs: 5400`). Next decisions: 2026-09-11,
  2026-09-16, 2026-09-21. The watcher's first successful run
  of a rebalance day is normally 00:20; if it only succeeds later the
  producer still emits within the hour, and the runtime accepts anything
  inside the window. A rebalance day with no valid file by 02:00 is
  `skipped` and the previous book is held.

## Workstation cron (operator adds; agents do not edit crontab)

```
25 * * * * $HOME/bot/scripts/.venv-lighter-collector/bin/python $HOME/bot/pairtrade/scripts/xsmom_signal_producer.py --out $HOME/bot/logs/xsmom_shadow/signal.json --config $HOME/bot/pairtrade/configs/book/xsmom-695.yaml --s3-uri s3://debot-dashboard/debot/book/xsmom-695/signal.json >> $HOME/bot/logs/xsmom_shadow/producer.log 2>&1
```

`--config` cross-checks the book against the universe the deployed runtime
will accept; see "Universe drift" below. The fetcher on the host applies
every check `signal.rs` does before it replaces the current file (hash,
schema, producer, freshness, look-ahead, decision key, universe, caps,
neutrality), all from values the installer derives from the deployed
config, and additionally refuses a decision key older than the signal
already in place, so a bad upload can never displace one the runtime
could still have used. The local `aws` CLI uses the admin
profile already on this machine.

(Point the script path at the checkout that tracks `master` once
pairtrade#285/#286 are merged. The local `aws` CLI uses the admin profile
that already exists on this machine.)

## Universe drift (expect this, roughly every few rebalances)

The shadow watcher re-screens its universe point-in-time at every
rebalance, on purpose: pinning it would put survivorship back into the
track (bot-strategy#695). So the book's membership genuinely moves. Over
2026-07-03 .. 2026-09-06 it introduced **2 to 6 new symbols per
rebalance**, and 21 of the 55 symbols it has held were never in the frozen
L38 snapshot.

`configs/book/xsmom-695.yaml` therefore lists the **union** of that
snapshot and everything the shadow book has actually held (59 symbols as
of 2026-09-06), not L38.

A symbol outside that list makes the runtime reject the whole signal
(`rejected:unknown_symbol` in `status.json`, previous book held). The
producer's `--config` check turns that into a producer-side refusal
naming the missing symbols:

```
refusing: 2026-09-11 book has 3 symbol(s) outside the deployed universe
(...xsmom-695.yaml): FOO, BAR, BAZ. Add them to universe.symbols and
redeploy the config, then re-run.
```

When that fires:

1. Add the symbols to `universe.symbols`, open a PR, merge it, and let
   `Deploy Configs` install the new config.
2. **Restart the service**: `book_runtime` reads its config once at
   startup, and both deploy paths install without restarting on purpose
   (bot-strategy#269). Until the restart the running process still
   enforces the old universe and will keep rejecting the signal.
   ```bash
   sudo systemctl restart book-runtime-xsmom-695
   sudo journalctl -u book-runtime-xsmom-695 -n 20 --no-pager | grep '\[CONFIG\]'
   ```
   Check the `fp=` in that line against `book_runtime --config … --validate`
   on the merged config. The runtime does not force-close on restart (it
   is not pairtrade), so a DRY_RUN book simply resumes from `state.json`;
   for a live book, restart outside a decision window.
3. Re-run the producer.

The window stays open for 90 minutes after 00:30 UTC, so a same-morning
fix still lands, but only if the restart happens too.

Making the bound dynamic (accept any symbol the venue lists, keeping the
per-symbol and gross/net caps as the real guard) is the proper fix,
tracked as bot-strategy#941. Until that lands this is a recurring,
expected maintenance step.

## Host install (CI, no start)

Two workflows can install, and neither depends on the other having run
first: `ci.yml` owns the binary object in S3, `deploy-configs.yml` owns
the config and unit objects, and each stages **everything** it needs from
S3 into a per-run directory before calling
`scripts/install_book_runtime.sh` with `BOOK_INSTANCE=xsmom-695`. A run
whose counterpart object is not published yet logs and skips, so the
first rollout completes whichever way the two happen to interleave.
The installer holds an exclusive lock, so the two never interleave:

- identity `book-runtime:book-runtime` (system user, no home, nologin)
- `/opt/book-runtime/{bin/book_runtime, bin/book_signal_fetch.sh,
  lib/libsigner.so, xsmom-695.yaml}` root-owned, group-readable
- `/var/lib/book-runtime/xsmom-695/` (state, ledger, pnl, status, signal,
  `KILL_SWITCH`, `RISK_ACK`) owned by the identity
- `/etc/book-runtime/` created; **`xsmom-695-secrets.env` is never written
  by CI**
- units installed + `daemon-reload`; nothing started.

The installer stages the whole bundle (binary, signer library, config,
fetch script), runs `book_runtime --validate` on the staged config with the
staged binary, and only then promotes all four files; a bundle that does
not validate fails the deploy and leaves the previous one intact. It holds
an exclusive `flock` on `/var/lock/book-runtime-install.lock` for that
sequence, so the binary deploy (`ci.yml`) and the config deploy
(`deploy-configs.yml`) can never interleave when a push fires both.

## Credentials (one-time, operator)

DRY_RUN still opens a Lighter WS/REST session through `dex-connector`, so
`get_lighter_config_from_env` needs the same variables `engine-b-live`
uses. Provision `/etc/book-runtime/xsmom-695-secrets.env` (mode 0640,
`root:book-runtime`) following `docs/engine-b-live-operations.md`
"Credential provisioning" — a dedicated read-only API key on a separate
Lighter account is the right shape for a paper book; reuse of the Engine B
account is possible but couples two identities that were deliberately
isolated. `InaccessiblePaths` in the unit keeps this identity out of
`/opt/debot`, `/opt/engine-b-*`, `/etc/engine-b-live`.

## Start / stop

```bash
sudo systemctl enable --now book-signal-fetch-xsmom-695.timer
sudo systemctl start book-runtime-xsmom-695
sudo journalctl -u book-runtime-xsmom-695 -n 50 --no-pager
```

Expected startup lines: `[CONFIG] instance=xsmom-695 … fp=<12hex>` (must
match `book_runtime --validate` on the deployed YAML), `[STARTUP]
instance=xsmom-695 mode=DRY_RUN positions=… last_decision=…`, `[PROM]
exporter listening on http://127.0.0.1:9474/metrics`.

Stopping does not close the paper book (or a live one); the state is
persisted and the next start resumes from it.

## Daily checks

- `status.json` (`/var/lib/book-runtime/xsmom-695/status.json`, mirrored to
  S3): `book.signal_status` (`waiting_for_file` / `applied:<sha>` /
  `partial:<sha>` / `rejected:<reason>` / `skipped:<reason>`),
  `book.next_decision_at`, `book.session_halted`, `position_count`,
  `book.gross_usd` ≈ shadow book gross.
- On a rebalance day, `ledger.jsonl` must have one `decision` row for the
  key with `outcome=applied` and `signal_sha256` equal to the producer's
  `payload_sha256` (printed by the producer and in the S3 file).
- `pnl.jsonl` `mark` rows once per UTC day; compare `equity_usd` with the
  shadow watcher's `cron.log` mark line — differences come from the paper
  fill model (`paper_slippage_bps: 5` vs the shadow's opposite-touch fill)
  and the funding *estimate* vs the shadow's realized `/fundings`.
- Metrics: `book_decision_total{outcome}`, `book_order_total{result}`,
  `book_signal_age_seconds`, `book_session_halted`, `book_config_info{fp}`.

## Risk rails

- `touch /var/lib/book-runtime/xsmom-695/KILL_SWITCH` — no opening intents;
  closes and flattens still run. Remove the file to release.
- Session halt (`max_session_loss_bps: 1500`, i.e. the #695 S2 gate of
  $150 on $1000): the book is flattened and `session_halted=true` sticks
  until `touch /var/lib/book-runtime/xsmom-695/RISK_ACK` (consumed on the
  next tick; re-anchors session and daily equity).
- Daily halt (`max_daily_loss_bps: 500`): opens blocked until the next UTC
  day, no flatten.

## Live transition (only after the 2026-10-02 readout passes on #695)

Not a checklist to execute now; recorded so the path is explicit:

1. Fund a dedicated Lighter account at the intended notional (small), key
   it, put the credentials in the secrets file.
2. Set `dry_run: false` in `configs/book/xsmom-695.yaml` **and**
   `Environment=BOOK_CONFIRM_LIVE=yes-i-mean-it` in a unit drop-in; either
   alone refuses to start.
3. Restart before a decision window with the book flat (the paper book
   does not carry over: delete `state.json` positions or let the venue
   reconcile adopt whatever is there — it starts flat on a new account).
4. First live decision: watch `[FILL]` lines for `venue_fills` fill-price
   source and `book_order_total{result="filled"}`; residuals show as
   `partial` with `residual_qty` in the ledger.
5. Live is Lighter-only until dex-connector has a perp IOC path for
   Hyperliquid (`docs/book-runtime.md` §10).
