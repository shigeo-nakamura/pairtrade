#!/bin/bash
# Install/update the isolated Phase 0A observer runtime. Does not start/restart it.
set -euo pipefail

INSTALL_DIR=${ENGINE_B_PHASE0_INSTALL_DIR:-/opt/engine-b-phase0}
PYTHON_BIN=${ENGINE_B_PHASE0_PYTHON_BIN:-/usr/bin/python3.11}
REQUIREMENTS=${ENGINE_B_PHASE0_REQUIREMENTS:-/opt/debot/scripts/engine_b_phase0_requirements.txt}
OBSERVER_SOURCE=${ENGINE_B_PHASE0_OBSERVER_SOURCE:-/opt/debot/scripts/engine_b_phase0.py}
ARCHIVER_SOURCE=${ENGINE_B_PHASE0_ARCHIVER_SOURCE:-/opt/debot/scripts/engine_b_phase0_archive.sh}
CONFIG_SOURCE=${ENGINE_B_PHASE0_CONFIG_SOURCE:-/opt/debot/configs/engine-b/phase0.json}
CALENDAR_SOURCE=${ENGINE_B_PHASE0_CALENDAR_SOURCE:-/opt/debot/configs/engine-b/trading_calendar.json}
UNIT_SOURCE_DIR=${ENGINE_B_PHASE0_UNIT_SOURCE_DIR:-/opt/debot/deploy}
CODE_COMMIT=${ENGINE_B_PHASE0_CODE_COMMIT:-}
SERVICE_USER=engine-b-phase0
SERVICE_GROUP=engine-b-phase0
STATE_DIR=/var/lib/engine-b-phase0

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Python 3.11 is required at $PYTHON_BIN" >&2
  exit 1
fi
if [ ! -f "$REQUIREMENTS" ]; then
  echo "Requirements lock is missing: $REQUIREMENTS" >&2
  exit 1
fi
for source in "$OBSERVER_SOURCE" "$ARCHIVER_SOURCE" "$CONFIG_SOURCE" "$CALENDAR_SOURCE"; do
  if [ ! -f "$source" ]; then
    echo "Engine B runtime source is missing: $source" >&2
    exit 1
  fi
done
if [[ ! "$CODE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ENGINE_B_PHASE0_CODE_COMMIT must be the deployed 40-character Git SHA" >&2
  exit 1
fi
for unit in engine-b-phase0.service engine-b-phase0-archive.service engine-b-phase0-archive.timer; do
  if [ ! -f "$UNIT_SOURCE_DIR/$unit" ]; then
    echo "Engine B systemd unit is missing: $UNIT_SOURCE_DIR/$unit" >&2
    exit 1
  fi
done

version=$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [ "$version" != "3.11" ]; then
  echo "Expected Python 3.11, got $version from $PYTHON_BIN" >&2
  exit 1
fi

if ! getent group "$SERVICE_GROUP" >/dev/null; then
  groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --gid "$SERVICE_GROUP" --home-dir /nonexistent \
    --shell /sbin/nologin --no-create-home "$SERVICE_USER"
fi

install -d -o root -g "$SERVICE_GROUP" -m 0750 "$INSTALL_DIR"
if [ ! -x "$INSTALL_DIR/venv/bin/python" ]; then
  "$PYTHON_BIN" -m venv "$INSTALL_DIR/venv"
fi
PIP_NO_CACHE_DIR=1 "$INSTALL_DIR/venv/bin/pip" install \
  --disable-pip-version-check --requirement "$REQUIREMENTS"
chown -R root:"$SERVICE_GROUP" "$INSTALL_DIR/venv"
chmod -R g+rX,o-rwx "$INSTALL_DIR/venv"
install -o root -g "$SERVICE_GROUP" -m 0550 "$OBSERVER_SOURCE" \
  "$INSTALL_DIR/engine_b_phase0.py"
install -o root -g "$SERVICE_GROUP" -m 0550 "$ARCHIVER_SOURCE" \
  "$INSTALL_DIR/engine_b_phase0_archive.sh"
install -o root -g "$SERVICE_GROUP" -m 0440 "$CONFIG_SOURCE" \
  "$INSTALL_DIR/phase0.json"
install -o root -g "$SERVICE_GROUP" -m 0440 "$CALENDAR_SOURCE" \
  "$INSTALL_DIR/trading_calendar.json"
install -o root -g "$SERVICE_GROUP" -m 0440 "$REQUIREMENTS" \
  "$INSTALL_DIR/requirements.txt"

# Preserve uninterrupted writes for an already-running legacy ec2-user
# process while making all existing state group-writable for the dedicated
# identity that takes over on the next operator-controlled restart.
#
# The observer keeps writing while this runs (deploy != restart), so SQLite
# -wal/-shm side files and partitions being sealed can vanish between the
# directory walk and the chgrp/chmod call. `chgrp -R` treated that as a hard
# failure and turned the whole Deploy Configs job red (bot-strategy#908
# item 8: engine_b_phase0_20260823_00.sqlite3-shm). A path that no longer
# exists is not an error; any other failure still aborts the install.
reown_state_tree() {
  # Descriptor-anchored walk (os.fwalk + O_NOFOLLOW + fchown/fchmod), never
  # following symlinks: the observer can rename entries under STATE_DIR
  # while this runs as root, so a pathname-based chgrp/chmod could be
  # redirected outside the tree through a swapped-in symlinked ancestor
  # (Codex review on pairtrade#280). Only directories and regular files are
  # touched; an entry that vanished mid-walk or turned into a symlink is
  # skipped, any other error aborts the install.
  "$PYTHON_BIN" - "$STATE_DIR" "$SERVICE_GROUP" <<'PY'
import errno
import grp
import os
import stat
import sys

root, group = sys.argv[1], sys.argv[2]
DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


def fix_fd(fd, gid):
    st = os.fstat(fd)
    if stat.S_ISDIR(st.st_mode):
        wanted = (stat.S_IMODE(st.st_mode) | 0o070) & ~0o007
    elif stat.S_ISREG(st.st_mode):
        wanted = (stat.S_IMODE(st.st_mode) | 0o060) & ~0o007
    else:
        return
    if st.st_gid != gid:
        os.fchown(fd, -1, gid)
    if stat.S_IMODE(st.st_mode) != wanted:
        os.fchmod(fd, wanted)


def fix_entry(dirfd, name, is_dir, gid, label):
    try:
        fd = os.open(name, DIR_FLAGS if is_dir else FILE_FLAGS, dir_fd=dirfd)
    except FileNotFoundError:
        print(f"skipping {label}: vanished during re-own", file=sys.stderr)
        return
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.ENXIO):
            print(f"skipping {label}: not a directory or regular file", file=sys.stderr)
            return
        raise
    try:
        fix_fd(fd, gid)
    finally:
        os.close(fd)


def on_walk_error(exc):
    if isinstance(exc, FileNotFoundError):
        print(f"skipping {exc.filename}: vanished during re-own", file=sys.stderr)
        return
    raise exc


def main():
    try:
        gid = grp.getgrnam(group).gr_gid
    except KeyError:
        print(f"failed to re-own {root}: unknown group {group}", file=sys.stderr)
        return 1
    try:
        for dirpath, dirnames, filenames, dirfd in os.fwalk(
            root, topdown=True, onerror=on_walk_error, follow_symlinks=False
        ):
            if dirpath == root:
                fix_fd(dirfd, gid)
            for name in dirnames:
                fix_entry(dirfd, name, True, gid, os.path.join(dirpath, name))
            for name in filenames:
                fix_entry(dirfd, name, False, gid, os.path.join(dirpath, name))
    except OSError as exc:
        print(f"failed to re-own {exc.filename or root} for {group}: {exc.strerror}", file=sys.stderr)
        return 1
    return 0


sys.exit(main())
PY
}

if [ -d "$STATE_DIR" ]; then
  reown_state_tree
else
  install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 \
    "$STATE_DIR" "$STATE_DIR/data" "$STATE_DIR/locks" "$STATE_DIR/sealed" \
    "$STATE_DIR/gap-continuations" "$STATE_DIR/session-continuations"
fi

requirements_hash=$(sha256sum "$REQUIREMENTS" | cut -d' ' -f1)
installed_version=$($INSTALL_DIR/venv/bin/python -c 'import websockets; print(websockets.__version__)')
if [ "$installed_version" != "15.0.1" ]; then
  echo "Unexpected websockets version after install: $installed_version" >&2
  exit 1
fi
systemd-analyze verify \
  "$UNIT_SOURCE_DIR/engine-b-phase0.service" \
  "$UNIT_SOURCE_DIR/engine-b-phase0-archive.service" \
  "$UNIT_SOURCE_DIR/engine-b-phase0-archive.timer"
for unit in engine-b-phase0.service engine-b-phase0-archive.service engine-b-phase0-archive.timer; do
  install -o root -g root -m 0644 "$UNIT_SOURCE_DIR/$unit" "/etc/systemd/system/$unit"
done
systemctl daemon-reload

release_tmp=$(mktemp "$INSTALL_DIR/.release.env.XXXXXX")
trap 'rm -f -- "$release_tmp"' EXIT
printf 'ENGINE_B_PHASE0_CODE_COMMIT=%s\n' "$CODE_COMMIT" > "$release_tmp"
install -o root -g "$SERVICE_GROUP" -m 0440 "$release_tmp" "$INSTALL_DIR/release.env"
rm -f -- "$release_tmp"
trap - EXIT

printf 'requirements_sha256=%s\npython_version=%s\nwebsockets_version=%s\ncode_commit=%s\n' \
  "$requirements_hash" "$version" "$installed_version" "$CODE_COMMIT" \
  > "$INSTALL_DIR/runtime-manifest.txt"
chown root:"$SERVICE_GROUP" "$INSTALL_DIR/runtime-manifest.txt"
chmod 0440 "$INSTALL_DIR/runtime-manifest.txt"

echo "Engine B Phase 0 runtime and units installed; services were not started or restarted."
