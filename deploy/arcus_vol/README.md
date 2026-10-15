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
| `scripts/arcus-vol-pause.sh` | `/opt/debot/scripts/` | IN PROCESS: pause = touch KILL_SWITCH → wait for the runtime's ack (halt, no quotes, flat) and keep the hold; resume = remove KILL_SWITCH → confirm the kill halt is released (starts the unit only if it died during the hold). Never stops a unit |
| `scripts/arcus-vol-reopen.sh` | `/opt/debot/scripts/` | pause every ACTIVE instance not already held (records them), resume (release) only those; a window never takes over another window's or the operator's hold |
| `units/arcus-vol-reopen-{pause,resume}.*` | `/etc/systemd/system/` | Sunday reopen blackout 17:55–18:05 and 19:55–20:05 New York |
| `units/arcus-vol-open-{pause,resume}.*` | `/etc/systemd/system/` | weekday cash-open blackout 09:28–09:35 New York |
| `units/arcus-vol-evening-{pause,resume}.*` | `/etc/systemd/system/` | weekday overnight-session-start blackout 19:55–20:05 New York, Mon–Thu (Fri 20:00 is the weekend pause) |

## Why in process (2026-10-14)

Arcus caps per-market dead man's switch auto-fires at 10 per UTC day per
subaccount. A stop that could not disarm the switch cost one fire, and the
blackouts stopped/started every instance several times a day. Holding the
running runtime in its KILL_SWITCH halt keeps the switch refreshed (the
runtime refreshes it every tick, halted or not), so a window costs no fire.
The hold also survives a crash or reboot: `KILL_SWITCH` is a file and the
launcher refuses to start while it exists, so the instance stays down (no
quotes) until resume removes it and starts it again.

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
  `multi-user.target` and quote through the rest of that window. (A reboot
  *during* a hold is fine: the `KILL_SWITCH` file persists.) Check the
  instances by hand after an unplanned reboot.
- **The fleet check cannot see a failed blackout.** `fleet_check.py` runs at
  minute 15 of the hour, so it never samples the weekday 09:27–09:38 or the
  Sunday :54–:08 windows. Units stay active through every hold, so a hold is
  not visible as an inactive unit either; `status.json` shows
  `halt: kill_switch`. A failed blackout shows up only in the journal
  (`journalctl -u arcus-vol-open-pause -u arcus-vol-reopen-pause`, syslog tag
  `arcus-vol-reopen` / `arcus-vol-pause`).
- **Run instances enabled.** Resume treats a `disabled`/`masked` unit as an
  operator hold and leaves its `KILL_SWITCH` in place. An instance that was
  started by hand while disabled will therefore be held by a blackout and
  never released.
  Keep every live instance `enabled` (as `instances.tsv` and `--apply` do);
  disable a unit only when it should stay down. A failed `is-enabled` lookup is
  an error: resume exits 6 and keeps the record for the next run (#396).
- **Resume starts a recorded instance that is no longer running** (a crash or
  reboot during the hold; the launcher refuses to start with `KILL_SWITCH`).
  To keep an instance down across a window, `disable` it (operator hold), do
  not just stop it.

## Rule

Any change made on the host (config tweak, new market, new timer) goes into
this directory in a PR in the same session (owner, 2026-10-12: "anything
running on live bot should be saved in the repo"). Run `--verify` after.
