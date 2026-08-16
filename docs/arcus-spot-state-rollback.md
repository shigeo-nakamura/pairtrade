# Arcus Spot live-tick state preservation and binary rollback

This runbook covers the `arcus-spot-live-tick` checkpoint/recovery boundary on
the dedicated Arcus host. It is deliberately separate from unit installation
and Health Watch configuration. The commands added here are offline state
inspection tools; they do not create an RPC client, load a KMS signer, invoke
the executor, submit a transaction, restore state, or alter a service/timer.

Running a live acceptance window is a different operation. Starting the
oneshot service executes one `live-tick`, which can sign and submit a swap if
the strategy produces a valid entry/exit plan. Do not perform the acceptance
section without explicit approval for that window.

## Recovery boundary

The approved config currently binds the state paths below. The config itself
is not copied into a backup because it may contain operational RPC endpoints;
instead the manifest binds its canonical SHA-256 digest.

| Artifact | Current path | Format / role |
|---|---|---|
| Runtime checkpoint | `/var/lib/debot-arcus/spot-execute-once/runtime_state.json` | schema 1; exact runtime config plus signal history, inventory, regime, risk state and last committed execution key |
| Execution ledger | `/var/lib/debot-arcus/spot-execute-once/ledger.json` | schema 2; monotonic attempt sequence, immutable archive and any active recovery state |
| Recovery plan | `/var/lib/debot-arcus/spot-execute-once/live-tick-pending-plan.json` | optional; exact plan needed by `auto-resume` after an interrupted live-tick submission |
| Namespace lock | `/var/lib/debot-arcus/spot-execute-once/.runtime_state.json.lock` | mode-0600 process lock shared by live-tick, execute/resume, proposer and state tooling |

Checkpoint and ledger stores already use create-new temporary files, file
`fsync`, atomic rename and parent-directory `fsync`. `live-tick` now writes its
pending recovery plan while holding the same checkpoint namespace lock, so a
backup observes either the previous complete boundary or the next complete
boundary, never a checkpoint from one tick and a plan from another.

The state tool uses strict `load_existing` reads. A missing file is an error;
it is never accepted as first-run state. Ledger inspection does not run the
normal restart recovery that rewrites `Dispatching` to `Unknown`. The tool
also refuses to create a missing lock, preventing a root-run inspection from
leaving behind a root-owned lock that the `arcus` service could not reopen.

## Offline commands

```text
arcus-spot-execute-once state-backup CONFIG_YAML BACKUP_DIR
arcus-spot-execute-once state-verify-exact CONFIG_YAML BACKUP_DIR
arcus-spot-execute-once state-verify-continuity CONFIG_YAML BACKUP_DIR
```

`state-backup` requires an absolute, nonexistent destination whose parent
already exists. It takes the existing executor lock, canonically validates the
checkpoint and ledger, validates the optional pending plan, and publishes a
mode-0700 directory through a hidden staging directory plus atomic rename.
Every copied file and the manifest are mode 0600 and fsynced. The manifest
records:

- canonical approved-config digest;
- byte length and SHA-256 of checkpoint, ledger and optional pending plan;
- non-secret continuity summaries (runtime sequence/history/watermark,
  inventory/regime/open quantity, ledger next sequence/archive/active phase).

`state-verify-exact` validates the backup first, then requires every current
state artifact to match its recorded bytes. Use this while the timer remains
stopped, before and after a binary-only rollback.

`state-verify-continuity` is the post-start check. It allows normal tick/fill
advancement but rejects:

- runtime sequence or last-observation regression, more than one observation
  advance, or corruption/removal/reordering of retained signal-history samples
  (a full window may shift by exactly one sample for the approved tick);
- cumulative-equity baseline changes, mismatched first equity baselines,
  sticky or newly-required risk-halt loss, same-day daily baseline resets,
  invalid UTC rollover baselines, or loss of the last equity mark;
- a backup that was not neutral with no active attempt, ledger regression,
  changed archived history, or more than one new acceptance attempt;
- an unresolved/non-reconciled acceptance attempt, or one not bound to the
  preserved pending plan, its configured-slippage buy floor, and its exact
  reconciled wallet-balance delta;
- inventory/regime/rotation/open-quantity/execution-key changes that do not
  exactly equal applying that one reconciled fill to the backup position.

Continuity is intentionally weaker than exact identity. Always obtain an
exact pass while stopped; continuity is supplementary evidence after a tick.

## Preflight (read-only)

Access the host through SSM; inbound SSH remains closed. Record all timestamps
in UTC.

1. Confirm the timer/service names and current binary provenance:

   ```bash
   sudo systemctl status arcus-spot-live-tick.timer --no-pager
   sudo systemctl status arcus-spot-live-tick.service --no-pager
   sudo systemctl cat arcus-spot-live-tick.service
   sudo sha256sum /usr/local/bin/arcus-spot-execute-once
   sudo find /usr/local/share/arcus-spot-execute-once/releases -maxdepth 2 -name manifest.json -type f -print
   ```

2. Do not open a rollback window when the ledger has an active attempt or the
   runtime is rotated. Use the most recent previously recorded state report;
   if no report exists, create the backup only after the timer stop in the
   approved procedure below and inspect `ledger.active_sequence` and
   `runtime.regime` in its JSON output. Required safe baseline:

   - `ledger.active_sequence: null`
   - `runtime.regime: "neutral"`

   If either condition is false, wait for normal reconciliation/exit. Do not
   delete or edit a checkpoint, ledger or pending plan to force the gate.

3. Identify two explicit immutable releases: the currently installed release
   and the rollback candidate. Compare each `manifest.json` with its binary and
   checksum. Do not select by `ls | tail`, mtime, a glob, or an unresolved
   symlink. The rollback candidate must support the on-disk checkpoint schema
   1 and ledger schema 2; otherwise use a forward fix.

## Approved stop / exact backup / binary rollback

These steps mutate service scheduling and the installed binary. They are not
authorized by merely merging this code or by a read-only audit.

1. Before changing scheduling, run both queries below and record their literal
   outputs outside the host as `TIMER_ENABLED_BEFORE` and
   `TIMER_ACTIVE_BEFORE`:

   ```bash
   sudo systemctl is-enabled arcus-spot-live-tick.timer
   sudo systemctl is-active arcus-spot-live-tick.timer
   ```

   Continue only when the enabled value is exactly `enabled` or `disabled` and
   the active value is exactly `active` or `inactive`. A different value such
   as `masked`, `activating`, `deactivating` or `failed` requires diagnosis;
   do not normalize it as part of this procedure.

   Stop only the timer, then poll for at most 120 seconds for any in-flight
   oneshot to become naturally inactive:

   ```bash
   sudo systemctl stop arcus-spot-live-tick.timer
   for attempt in $(seq 1 60); do
     service_state="$(sudo systemctl show arcus-spot-live-tick.service \
       -p ActiveState --value)"
     if [ "$service_state" = "inactive" ]; then
       break
     fi
     if [ "$attempt" -eq 60 ]; then
       echo "oneshot did not become inactive within 120 seconds; abort" >&2
       exit 1
     fi
     sleep 2
   done
   sudo systemctl is-active arcus-spot-live-tick.timer
   sudo systemctl show arcus-spot-live-tick.service \
     -p ActiveState -p SubState -p Result -p ExecMainStatus
   ```

   The timer and service must both report `inactive` before continuing. This
   procedure must never run `systemctl stop` or `systemctl kill` against the
   service: if the bounded wait expires, leave the timer stopped and diagnose
   the still-running oneshot without signalling it. It may be between dispatch
   and ledger persistence.

2. Create a root-owned backup parent once, then choose an explicit UTC-stamped
   destination (replace the placeholder before running):

   ```bash
   sudo install -d -o root -g root -m 0700 /var/lib/debot-arcus/spot-state-backups
   sudo /usr/local/bin/arcus-spot-execute-once state-backup \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   sudo /usr/local/bin/arcus-spot-execute-once state-verify-exact \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   ```

   Save both JSON outputs. Reconfirm the neutral/no-active baseline. A backup
   failure leaves only a hidden staging directory; it never alters live state.
   Remove a failed staging directory only after resolving its exact generated
   path and confirming no state file points there.

3. Validate the explicit rollback release before installation. Substitute the
   fully resolved immutable release directory from preflight:

   ```bash
   cd /usr/local/share/arcus-spot-execute-once/releases/<release-id>
   sudo sha256sum -c arcus-spot-execute-once.sha256
   sudo jq . manifest.json
   ```

4. Install only the binary through a same-filesystem staging file and atomic
   rename. Do not copy, delete or restore anything under the live state path:

   ```bash
   sudo install -o root -g root -m 0755 \
     /usr/local/share/arcus-spot-execute-once/releases/<release-id>/arcus-spot-execute-once \
     /usr/local/bin/arcus-spot-execute-once.rollback-new
   sudo sha256sum /usr/local/bin/arcus-spot-execute-once.rollback-new
   sudo mv -f /usr/local/bin/arcus-spot-execute-once.rollback-new \
     /usr/local/bin/arcus-spot-execute-once
   ```

5. While the timer is still stopped, prove the rollback did not touch state:

   ```bash
   sudo /usr/local/bin/arcus-spot-execute-once state-verify-exact \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   ```

   This command must be supported by the selected rollback release. If a
   legacy release predates the command, invoke the state-capable, known-good
   current binary directly from its explicit immutable release directory for
   this verification; do not start the legacy binary until the exact check
   passes. Verify that diagnostic binary against its own manifest first.

At this point binary rollback and byte-exact state preservation are proven
without invoking the executor. It is safe to stop here and leave the timer
inactive pending a separately approved live acceptance.

## Explicitly approved one-tick/start acceptance

Starting the service can trade. Reconfirm explicit approval, neutral runtime,
no active ledger attempt, current gas/risk gates, and the exact intended
binary SHA before continuing.

1. Invoke exactly one tick with the timer still stopped:

   ```bash
   sudo systemctl start arcus-spot-live-tick.service
   sudo systemctl show arcus-spot-live-tick.service \
     -p ActiveState -p SubState -p Result -p ExecMainStatus
   sudo journalctl -u arcus-spot-live-tick.service --since '<UTC-start>' --no-pager
   ```

2. If the ledger reports `Submitted`, `Unknown`, `OperatorHold`, or another
   active phase, stop. Keep the timer disabled and follow `auto-resume` using
   the preserved pending plan. Never replace the ledger/checkpoint from the
   backup to make the active attempt disappear.

3. When the tick completes with no unresolved attempt, prove continuity:

   ```bash
   sudo /usr/local/bin/arcus-spot-execute-once state-verify-continuity \
     /etc/arcus-spot/config.yaml \
     /var/lib/debot-arcus/spot-state-backups/<YYYYMMDDTHHMMSSZ>-pre-rollback
   ```

4. Only after the continuity report is saved may the timer be restored. Use
   the two values recorded before the stop; enabled state and active state are
   independent. Apply exactly one row from this table:

   | `TIMER_ENABLED_BEFORE` | `TIMER_ACTIVE_BEFORE` | Restore action |
   |---|---|---|
   | `enabled` | `active` | `sudo systemctl enable arcus-spot-live-tick.timer` then `sudo systemctl start arcus-spot-live-tick.timer` |
   | `enabled` | `inactive` | `sudo systemctl enable arcus-spot-live-tick.timer`; do not start it |
   | `disabled` | `active` | `sudo systemctl disable arcus-spot-live-tick.timer` then `sudo systemctl start arcus-spot-live-tick.timer` |
   | `disabled` | `inactive` | `sudo systemctl disable arcus-spot-live-tick.timer`; do not start it |

   Re-run both queries and require their outputs to match the recorded values
   exactly:

   ```bash
   sudo systemctl is-enabled arcus-spot-live-tick.timer
   sudo systemctl is-active arcus-spot-live-tick.timer
   ```

   If the active state was restored to `active`, observe the next natural timer
   result and Health Watch signal before closing the acceptance window. Never
   enable or start the timer merely because that is the expected production
   default.

## EC2 stop/start variant

Issue #758 also requires the fixed network identity to survive an EC2
stop/start. That is a separate production mutation from a systemd stop/start.
Use the same stopped-timer exact backup before stopping the instance. After SSM
returns, verify the Elastic IP association and instance ID read-only, run
`state-verify-exact` before starting any unit, then follow the one-tick
acceptance above. Preserve `TIMER_ENABLED_BEFORE` and `TIMER_ACTIVE_BEFORE`
outside the instance across the stop/start, and restore both values with the
same table rather than assuming the timer should be enabled and active. This
variant requires its own explicit approval and is not performed by CI or this
PR.

## Failure and restore policy

- Missing checkpoint, ledger, lock or backup file is a hard failure. Do not
  accept `load_or_create` output as rollback evidence.
- A config-digest mismatch means the backup belongs to a different approved
  runtime. Do not edit the manifest to make it pass.
- A checksum/summary mismatch means the backup or live state changed. Keep the
  timer stopped and diagnose before any invocation.
- This tooling intentionally has no state-restore command. Restoring an old
  ledger can erase evidence of an on-chain attempt or reset a daily cap;
  restoring an old checkpoint can resurrect stale inventory/regime state.
  Any state restore therefore requires a separately reviewed recovery plan,
  explicit approval, current on-chain balance reconciliation and a new backup
  of the damaged state before replacement.
- Binary rollback must be forward-compatible with checkpoint schema 1 and
  ledger schema 2. If compatibility is uncertain, forward-fix the binary.

Evidence to attach to bot-strategy #758 consists of the selected release
manifests/SHA-256 values, pre-rollback backup manifest, exact verification
before and after binary replacement, service result/journal for the approved
one tick, post-start continuity report, both pre-stop timer values and both
post-restore timer values, and (for the EC2 variant) EIP association
before/after. Never attach config contents, RPC credentials, signing material
or KMS-sensitive output.
