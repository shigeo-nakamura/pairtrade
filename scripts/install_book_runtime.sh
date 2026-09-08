#!/bin/bash
# Install/update one book runtime instance (bot-strategy#937) on a host.
# Never starts or restarts the service and never touches the host-only
# secrets file (/etc/book-runtime/<instance>-secrets.env) beyond creating
# its parent directory -- same discipline as install_engine_b_live.sh.
#
#   sudo env BOOK_INSTANCE=xsmom-695 BOOK_BINARY_SOURCE=/opt/debot/bin/book_runtime \
#        BOOK_LIBSIGNER_SOURCE=/opt/debot/deploy/engine-b-live-libsigner.so \
#        bash install_book_runtime.sh
set -euo pipefail

INSTANCE=${BOOK_INSTANCE:?BOOK_INSTANCE (e.g. xsmom-695) is required}
INSTALL_DIR=${BOOK_INSTALL_DIR:-/opt/book-runtime}
SECRETS_DIR=${BOOK_SECRETS_DIR:-/etc/book-runtime}
STATE_ROOT=${BOOK_STATE_ROOT:-/var/lib/book-runtime}
BINARY_SOURCE=${BOOK_BINARY_SOURCE:-/opt/debot/bin/book_runtime}
LIBSIGNER_SOURCE=${BOOK_LIBSIGNER_SOURCE:-/opt/debot/lib/libsigner.so}
CONFIG_SOURCE=${BOOK_CONFIG_SOURCE:-/opt/debot/configs/book/${INSTANCE}.yaml}
FETCH_SCRIPT_SOURCE=${BOOK_FETCH_SCRIPT_SOURCE:-/opt/debot/scripts/book_signal_fetch.sh}
UNIT_SOURCE_DIR=${BOOK_UNIT_SOURCE_DIR:-/opt/debot/deploy}
# Optional: a `schedule.kind: calendar` instance's calendar file, installed
# next to the config as <INSTALL_DIR>/<instance>.calendar.json (the unit's
# InaccessiblePaths hides /opt/debot from the service, so the runtime
# cannot read it from the synced configs tree). Empty = the instance has no
# calendar (interval_days / daily). A calendar-kind config with no source
# at all is rejected below, before promotion, since --validate does not
# load the calendar and the service would only fail at its next start.
CALENDAR_SOURCE=${BOOK_CALENDAR_SOURCE:-}
if [ -z "$CALENDAR_SOURCE" ] && [ -f "/opt/debot/configs/book/${INSTANCE}.calendar.json" ]; then
  CALENDAR_SOURCE=/opt/debot/configs/book/${INSTANCE}.calendar.json
fi
SERVICE_USER=book-runtime
SERVICE_GROUP=book-runtime
LOCK_FILE=${BOOK_INSTALL_LOCK:-/var/lock/book-runtime-install.lock}

# ci.yml (binary) and deploy-configs.yml (config/units) both run this
# script on the same host, and a push touching src/** and configs/** fires
# both workflows at once. A shared GitHub concurrency group is the wrong
# tool -- queuing a third job there cancels the pending one, which would
# silently drop a binary deploy -- so the mutual exclusion lives here: each
# run holds an exclusive lock for the whole stage-validate-promote
# sequence, and both contenders install a complete, self-consistent bundle
# from whatever is currently staged under /opt/debot.
install -d -m 0755 "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -w 600 9; then
  echo "another book runtime install is holding $LOCK_FILE; giving up after 600s" >&2
  exit 1
fi

for source in "$BINARY_SOURCE" "$LIBSIGNER_SOURCE" "$CONFIG_SOURCE" "$FETCH_SCRIPT_SOURCE" \
              "$UNIT_SOURCE_DIR/book-runtime-${INSTANCE}.service" \
              "$UNIT_SOURCE_DIR/book-signal-fetch-${INSTANCE}.service" \
              "$UNIT_SOURCE_DIR/book-signal-fetch-${INSTANCE}.timer"; do
  if [ ! -f "$source" ]; then
    echo "book runtime source is missing: $source" >&2
    exit 1
  fi
done

if ! getent group "$SERVICE_GROUP" >/dev/null; then
  groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_GROUP" --home-dir /nonexistent \
    --shell /sbin/nologin --no-create-home "$SERVICE_USER"
fi

install -d -o root -g "$SERVICE_GROUP" -m 0750 "$INSTALL_DIR" "$INSTALL_DIR/bin" "$INSTALL_DIR/lib"

# Stage the whole runtime bundle (binary, signer library, config, fetch
# script) and validate the config WITH the staged binary before anything
# installed is touched, so a deploy whose config does not parse -- or whose
# binary rejects it -- leaves the previous bundle intact and consistent.
STAGE=$(mktemp -d "$INSTALL_DIR/.stage.XXXXXX")
trap 'rm -rf "$STAGE"' EXIT
install -d -m 0750 "$STAGE/bin" "$STAGE/lib"
install -o root -g "$SERVICE_GROUP" -m 0550 "$BINARY_SOURCE" "$STAGE/bin/book_runtime"
install -o root -g "$SERVICE_GROUP" -m 0440 "$LIBSIGNER_SOURCE" "$STAGE/lib/libsigner.so"
install -o root -g "$SERVICE_GROUP" -m 0440 "$CONFIG_SOURCE" "$STAGE/${INSTANCE}.yaml"
install -o root -g "$SERVICE_GROUP" -m 0550 "$FETCH_SCRIPT_SOURCE" "$STAGE/bin/book_signal_fetch.sh"
# Read the schedule kind now, before the calendar block: a
# `schedule.kind: calendar` config whose calendar source is absent must
# fail HERE, not at the next service start. `book_runtime --validate`
# does not load `schedule.calendar_path`, so without this check a first
# install promotes a bundle the service cannot start, and an update
# silently keeps whatever calendar was installed before.
# Every value below comes from the staged binary parsing the staged
# config, never from awk (bot-strategy#948, pairtrade#293). awk is not a
# YAML parser: quoted scalars kept their quotes, values containing spaces
# were truncated at the first token, trailing `# comments` leaked in, and
# each fix only uncovered the next case -- while the binary that will run
# the config is the one thing guaranteed to read it the same way the
# service does. Same reasoning as bot-strategy#952 moving calendar
# validation into the runtime. This runs before the calendar is staged,
# so it deliberately does not build the scheduler.
#
# Feature-detected, because this workflow can legitimately run ahead of a
# binary that supports it: deploy-configs.yml syncs scripts/ and then
# stages whatever binary `book-runtime/current.json` currently points at,
# while ci.yml republishes that pointer only after its much slower ARM
# build -- and a config-only push never triggers a binary build at all.
# So on the deploy that first carries this installer, the staged binary
# can still be the previous one, whose argument parser rejects an unknown
# flag and would fail the whole config deployment (pairtrade#293 Codex).
FETCH_ENV_RENDERED=0
if LD_LIBRARY_PATH="$STAGE/lib" "$STAGE/bin/book_runtime" --help 2>&1 | grep -q -- '--print-fetch-env'; then
  FETCH_ENV_RENDERED=1
  if ! LD_LIBRARY_PATH="$STAGE/lib" "$STAGE/bin/book_runtime" \
        --config "$STAGE/${INSTANCE}.yaml" \
        --print-fetch-env "$STAGE/${INSTANCE}.fetch.env"; then
    echo "book runtime could not parse $CONFIG_SOURCE with $BINARY_SOURCE; installed bundle left untouched" >&2
    exit 1
  fi
elif [ -f "$INSTALL_DIR/${INSTANCE}.fetch.env" ] \
     && cmp -s "$STAGE/${INSTANCE}.yaml" "$INSTALL_DIR/${INSTANCE}.yaml"; then
  # The published binary predates the flag, but the config is byte-identical
  # to the installed one, so the installed fetch env already describes it.
  # Carrying it forward keeps the deploy green without guessing at any
  # value -- no YAML is parsed on this path either.
  echo "note: published book_runtime predates --print-fetch-env; config unchanged, carrying the installed fetch env forward" >&2
  cp -p "$INSTALL_DIR/${INSTANCE}.fetch.env" "$STAGE/${INSTANCE}.fetch.env"
else
  echo "the published book_runtime predates --print-fetch-env and there is no unchanged fetch env to carry forward; re-run Deploy Configs after this commit's binary deploy has published book-runtime/current.json" >&2
  exit 1
fi
# Bare KEY=value lines, restricted to a character set systemd's
# EnvironmentFile= parser and a shell read identically.
# Every read below carries a `:-` default: on the compatibility path the
# file was written by the *previous* installer, which did not emit
# BOOK_CALENDAR_PATH at all, and `set -u` would abort the deploy on it
# (pairtrade#293 Codex).
# shellcheck disable=SC1090
. "$STAGE/${INSTANCE}.fetch.env"
KIND=${BOOK_SCHEDULE_KIND:-}
CALENDAR_PATH=${BOOK_CALENDAR_PATH:-}
if [ -z "$KIND" ]; then
  echo "could not read schedule.kind from $CONFIG_SOURCE" >&2
  exit 1
fi
if [ "$KIND" = "calendar" ]; then
  if [ -z "$CALENDAR_SOURCE" ]; then
    echo "schedule.kind is calendar but no calendar source was found: set BOOK_CALENDAR_SOURCE or provide /opt/debot/configs/book/${INSTANCE}.calendar.json; installed bundle left untouched" >&2
    exit 1
  fi
  # The calendar is always promoted to <INSTALL_DIR>/<instance>.calendar.json
  # (the unit's ProtectSystem/InaccessiblePaths assume the install dir), so
  # the config must point exactly there -- --validate does not load it,
  # and a config naming any other path would install cleanly and then fail
  # at service start with the calendar sitting where the runtime does not
  # look.
  if [ "$FETCH_ENV_RENDERED" = 1 ]; then
    if [ "$CALENDAR_PATH" != "$INSTALL_DIR/${INSTANCE}.calendar.json" ]; then
      echo "schedule.calendar_path must be $INSTALL_DIR/${INSTANCE}.calendar.json (got '${CALENDAR_PATH}'); installed bundle left untouched" >&2
      exit 1
    fi
  else
    # Compatibility path only: the carried-forward env predates
    # BOOK_CALENDAR_PATH, and it is only taken when the staged config is
    # byte-identical to the installed one -- which the running service is
    # already loading its calendar from, so the path is whatever it was.
    echo "note: skipping the schedule.calendar_path check (carried-forward fetch env, config unchanged)" >&2
  fi
fi
if [ -n "$CALENDAR_SOURCE" ]; then
  if [ ! -f "$CALENDAR_SOURCE" ]; then
    echo "book runtime calendar source is missing: $CALENDAR_SOURCE" >&2
    exit 1
  fi
  # Staged next to the staged config so `--validate --calendar` below
  # checks the file that is about to be promoted, not whatever calendar a
  # previous install left at schedule.calendar_path.
  install -o root -g "$SERVICE_GROUP" -m 0440 "$CALENDAR_SOURCE" "$STAGE/${INSTANCE}.calendar.json"
fi
# The staged binary validates the staged config AND (for calendar kinds)
# the staged calendar, with `--calendar` pointing at the copy above
# (bot-strategy#952). Reusing the binary's own loader is the point: a
# separate re-implementation of serde's deny_unknown_fields, chrono's
# timestamp parsing and the duplicate-key/flatten/overlap rules drifts,
# and every drift is a bundle this installer promotes and the service
# then refuses to start on.
VALIDATE_ARGS=(--config "$STAGE/${INSTANCE}.yaml" --validate)
if [ -n "$CALENDAR_SOURCE" ]; then
  VALIDATE_ARGS+=(--calendar "$STAGE/${INSTANCE}.calendar.json")
fi
if ! LD_LIBRARY_PATH="$STAGE/lib" "$STAGE/bin/book_runtime" "${VALIDATE_ARGS[@]}" >/dev/null; then
  echo "book runtime bundle failed validation ($CONFIG_SOURCE with $BINARY_SOURCE); installed bundle left untouched" >&2
  exit 1
fi
# The fetch env was rendered by the binary above, before the calendar
# checks; only its required-field assertions remain here, so a config
# missing something the fetcher needs fails before promotion rather than
# silently disabling one of the fetcher's checks.
if [ -z "$BOOK_SIGNAL_PRODUCER_ID" ]; then
  echo "signal.producer_id is empty in $CONFIG_SOURCE" >&2
  exit 1
fi
case "$BOOK_SIGNAL_MAX_AGE_SECS" in
  ''|*[!0-9]*)
    echo "signal.max_age_secs must be a non-negative integer in $CONFIG_SOURCE (got '${BOOK_SIGNAL_MAX_AGE_SECS}')" >&2
    exit 1
    ;;
esac
# Only date-keyed schedules (interval_days / daily) let the fetcher derive
# a decision time from the file's own decision_key; a calendar schedule
# leaves this empty and the fetcher skips that one check.
case "$KIND" in
  interval_days|daily)
    if [ -z "$BOOK_DECISION_TIME_UTC" ]; then
      echo "schedule.decision_time_utc is required for kind=$KIND in $CONFIG_SOURCE" >&2
      exit 1
    fi
    ;;
esac
if [ -z "$BOOK_UNIVERSE" ]; then
  echo "universe.symbols is empty in $CONFIG_SOURCE" >&2
  exit 1
fi
chown root:"$SERVICE_GROUP" "$STAGE/${INSTANCE}.fetch.env"
chmod 0440 "$STAGE/${INSTANCE}.fetch.env"

mv -f "$STAGE/bin/book_runtime" "$INSTALL_DIR/bin/book_runtime"
mv -f "$STAGE/lib/libsigner.so" "$INSTALL_DIR/lib/libsigner.so"
mv -f "$STAGE/bin/book_signal_fetch.sh" "$INSTALL_DIR/bin/book_signal_fetch.sh"
mv -f "$STAGE/${INSTANCE}.yaml" "$INSTALL_DIR/${INSTANCE}.yaml"
mv -f "$STAGE/${INSTANCE}.fetch.env" "$INSTALL_DIR/${INSTANCE}.fetch.env"
if [ -n "$CALENDAR_SOURCE" ]; then
  mv -f "$STAGE/${INSTANCE}.calendar.json" "$INSTALL_DIR/${INSTANCE}.calendar.json"
fi

install -d -o root -g "$SERVICE_GROUP" -m 0750 "$SECRETS_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$STATE_ROOT" "$STATE_ROOT/${INSTANCE}"

for unit in "book-runtime-${INSTANCE}.service" "book-signal-fetch-${INSTANCE}.service" "book-signal-fetch-${INSTANCE}.timer"; do
  install -o root -g root -m 0644 "$UNIT_SOURCE_DIR/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload

echo "book runtime ${INSTANCE} installed; nothing was started or restarted."
if [ ! -f "$SECRETS_DIR/${INSTANCE}-secrets.env" ]; then
  echo "NOTE: $SECRETS_DIR/${INSTANCE}-secrets.env does not exist yet -- the service cannot start until the operator provisions it (docs/book-runtime-operations.md)."
fi
