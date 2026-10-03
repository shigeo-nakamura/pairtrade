# Arcus volume / presence runtime — operations (bot-strategy#1093)

`arcus_vol_runtime` quotes one Arcus Perps market on **subaccount 0** of the
owner's wallet: post-only quotes resting `QUOTE_OFFSET_BPS` behind the touch
(presence mode; `0` = at the touch), the inventory-reducing side at the touch,
daily and cumulative stops, a 30 s dead man's switch (`scheduleCancel`,
refreshed every 10 s). Binary: `src/bin/arcus_vol_runtime/` (module doc =
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
random IV, verifies the roundtrip, and rewrites `live.env` atomically
(previous file kept as `live.env.plain.bak`, mode 600 — delete it once the
service has started). The output is byte-identical to
`scripts/encrypt.py <data-key> 0x<hex>`; the runtime decrypts it with
`debot_utils::decrypt_data_with_kms(.., output_as_hex = true)`.

The launcher refuses to start unless `live.env` is mode 600, complete, and
`ARCUS_ACCOUNT_INDEX=0`. A plain `ARCUS_PLAIN_API_PRIVATE_KEY` starts only
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

### Sentinels (in `STATE_DIR`)

| file | meaning |
|---|---|
| `KILL_SWITCH` | runtime pulls quotes, flattens (reduce-only IOC), halts while present. The launcher does **not** start while it exists (exit 0, so `Restart=on-failure` stays quiet): `sudo rm .../KILL_SWITCH` first. |
| `HALT` | written by the runtime on a sticky halt (cumulative stop, unrecoverable reconcile). Read the journal, then remove it by hand before the next start. |

### What the dead man's switch means for manual trading

`scheduleCancel` is **account-wide**: if the host cannot reach the venue for
~30 s (or the runtime dies), the venue cancels **every** open order on
subaccount 0, including manual limit orders in other markets (this
happened to a NEAR-USD limit on 2026-10-01). Positions are not touched.
Keep manual resting orders on another subaccount while the runtime runs.
Per-market `scheduleCancel` (`marketId`) is a recorded follow-up.

## Moving state from another host

`state.json` carries the cumulative-stop accounting and `fills.jsonl` the
journal. To continue a run elsewhere: stop the old runtime, copy both files
into `STATE_DIR` on the new host (same market), then start. Two runtimes on
the same subaccount at once (even from different hosts) must never happen:
each believes it owns the account's orders and the dead man's switch.
