# Engine B Phase 0A operations

This runbook covers the read-only observer for bot-strategy#866 / Project 8.
Use UTC for all timestamps.

## Safety boundary

`engine_b_phase0.py` has no Lighter SDK, signer, account-authentication, or
order module. Its outbound WebSocket allowlist contains only public
`subscribe`, `unsubscribe`, `ping`, and `pong` messages. The service does not
load `/opt/debot/scripts/*.env`, so it cannot read the Robinhood `freq2`
credentials. `collector_manifest.order_capability` and the Prometheus gauge
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
for the host's 20 GiB root volume. At minute 10, the archive timer checks each
closed SQLite partition, uploads a gzip plus SHA-256 file with AES256 S3
server-side encryption, verifies the remote objects, and only then removes the
closed local database. The active hour and any database with `-wal`/`-shm`
sidecars are never removed.

Archive prefix:

```text
s3://debot-dashboard/debot/engine-b/phase0/raw/<host>/YYYY/MM/
```

## Install and start

The host requires Python 3.11. `install_engine_b_phase0.sh` creates an isolated
venv and installs the exact dependency versions, but never starts or restarts a
service.

```bash
sudo dnf install -y python3.11 python3.11-pip
sudo bash /opt/debot/scripts/install_engine_b_phase0.sh
sudo systemctl daemon-reload
sudo systemctl enable --now engine-b-phase0.service
sudo systemctl enable --now engine-b-phase0-archive.timer
```

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
