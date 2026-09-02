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

The observer collects two explicitly labelled venues:

- `robinhood`: intended future execution venue (`SKHY`, `SNDK`, `MU`, `SOXL`,
  `NVDA`, `SPY`, `QQQ`). Robinhood did not list `EWY` or `USDKRW` on
  2026-09-01.
- `lighter_mainnet_context`: the complete requirements-v0.3 context universe,
  including `EWY` and `USDKRW`.

Do not combine the two venues into a v0.3 primary regression without a new,
reviewed analysis-plan version. Until A-7 has a verified KRX/US market calendar
and Robinhood's same-venue control gap is resolved, `trading_session` rows are
written fail-closed with `krx_is_open=0`, `us_cash_is_open=0`, and must not be
counted as valid Phase 0A samples.

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
as one partition-local `ws_connection` segment in each database. Closed
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
within identical trades in the message. Message nonce and absolute array
position are deliberately excluded, so overlapping reconnect snapshots retain
the same multiset identities. When an older
partition contains NULL IDs or the obsolete unversioned synthetic IDs, the
index builder reconstructs the current identity from its stored
receive/message ordering.
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
the same database flush remains open.

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
