# Arcus volume bots — host state (bot-strategy#1093)

Everything the Tokyo `debot-main` host (`i-0095af4fe0efbc5dd`) runs for the
Arcus volume bots beyond what `deploy-arcus-vol.yml` installs (binary,
launcher, `deploy/debot-arcus-vol.service`, `.env.example`). Captured
byte-for-byte from the host on 2026-10-12; `install_arcus_vol_host.sh
--verify` reported zero drift at that point.

| path | host destination | what |
|---|---|---|
| `instances.tsv` | — | one row per market: unit, `/etc` dir, state root, enablement, wallet |
| `units/debot-arcus-vol-<m>.service` | `/etc/systemd/system/` | per-market instance units (= base unit + `DEBOT_ARCUS_VOL_ETC_DIR` + own `WorkingDirectory`) |
| `config/<m>.config` | `/etc/debot-arcus-vol[-<m>]/config.env` | market / sizing / offsets / stops / learner params (no secrets) |
| `config/qqq.config.retired` | — | QQQ retired 2026-10-10; host keeps `config.env.retired-*`, no `config.env` |
| `scripts/arcus-vol-pause.sh` | `/opt/debot/scripts/` | pause = KILL_SWITCH → wait flat → stop; resume = start |
| `scripts/arcus-vol-reopen.sh` | `/opt/debot/scripts/` | pause every ACTIVE instance (records them), resume only those |
| `units/arcus-vol-{pause,resume}-{hood,slv}.*` | `/etc/systemd/system/` | HOOD/SLV weekend pause: Sat 00:00 UTC → Sun 20:10 New York (via `arcus-vol-reopen.sh --tag weekend-<m>`: only a bot the pause actually stopped is resumed) |
| `units/arcus-vol-reopen-{pause,resume}.*` | `/etc/systemd/system/` | Sunday reopen blackout 17:55–18:05 and 19:55–20:05 New York |
| `units/arcus-vol-open-{pause,resume}.*` | `/etc/systemd/system/` | weekday cash-open blackout 09:28–09:35 New York |

The config files are named `.config` (not `.env`) because `*.env` is
gitignored repo-wide as a secrets guard. `live.env` (credentials, KMS-encrypted key) is never in the repo, never read
or written by the installer.

## Install / check

```bash
# on the host, from a checkout or an S3-staged copy of deploy/:
sudo deploy/arcus_vol/install_arcus_vol_host.sh            # --verify: drift report, changes nothing (exit 1 on drift)
sudo deploy/arcus_vol/install_arcus_vol_host.sh --apply    # install scripts/units/timers, missing config.env, enablement
sudo deploy/arcus_vol/install_arcus_vol_host.sh --apply --apply-config   # also overwrite differing config.env (backup .bak-<ts>)
```

`--apply` never starts, stops or restarts a trading unit: it installs files,
`daemon-reload`s, sets `enable`/`disable` per `instances.tsv` (no `--now`) and
`enable --now`s the timers. It prints `systemctl restart` lines for instances
whose unit or config changed — restarting is the operator's call (owner
approval for any bot restart on this host; check the instance is flat).

## Known limitations (owner-accepted 2026-10-12)

- **No catch-up after a reboot.** The pause timers are `Persistent=false`. If
  the host is down when a pause should fire (e.g. Saturday 00:00 UTC) and
  comes back inside the window, the enabled instances start through
  `multi-user.target` and quote through the rest of that window. Check the
  instances by hand after an unplanned reboot.
- **The fleet check cannot see a failed blackout.** `fleet_check.py` runs at
  minute 15 of the hour, so it never samples the weekday 09:27–09:38 or the
  Sunday :54–:08 windows; `ACTIVE_IN_PAUSE` only catches a failed HOOD/SLV
  weekend pause. A failed short blackout shows up only in the journal
  (`journalctl -u arcus-vol-open-pause -u arcus-vol-reopen-pause`, syslog tag
  `arcus-vol-reopen` / `arcus-vol-pause`).
- **Run instances enabled.** Resume treats a `disabled`/`masked` unit as an
  operator hold and leaves it stopped. An instance that was started by hand
  while disabled will therefore be paused by a blackout and never resumed.
  Keep every live instance `enabled` (as `instances.tsv` and `--apply` do);
  disable a unit only when it should stay down. A failed `is-enabled` lookup is
  an error: resume exits 6 and keeps the record for the next run (#396).

## Rule

Any change made on the host (config tweak, new market, new timer) goes into
this directory in a PR in the same session (owner, 2026-10-12: "anything
running on live bot should be saved in the repo"). Run `--verify` after.
