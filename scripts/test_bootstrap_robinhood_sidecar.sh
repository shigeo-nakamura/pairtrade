#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
cleanup() {
  if [ -f "$WORK/fake-sidecar-server.pids" ]; then
    while read -r pid; do
      kill "$pid" 2>/dev/null || true
    done < "$WORK/fake-sidecar-server.pids"
  fi
  rm -rf -- "$WORK"
}
trap cleanup EXIT

BUNDLE="$WORK/bundle"
PAIRTRADE_DIR="$WORK/pairtrade"
SYSTEMD_DIR="$WORK/systemd"
SIDECAR_ROOT="$WORK/sidecar"
SOCKET_PATH="$WORK/lighter-ratelimit.sock"
mkdir -p "$BUNDLE" "$PAIRTRADE_DIR" "$SYSTEMD_DIR" "$SIDECAR_ROOT"

cat > "$WORK/fake-sidecar-server.py" <<'PY'
import socket
import sys

path = sys.argv[1]
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(path)
srv.listen(5)
while True:
    conn, _ = srv.accept()
    with conn:
        data = b""
        while not data.endswith(b"\n"):
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        conn.sendall(b'{"granted": true}\n')
PY
: > "$WORK/fake-sidecar-server.log"

SOURCE_SHA=43e405cc811c318034b17bab9c3dd2b387a6f897
DEPLOYMENT_SOURCE_SHA=4b7e859a6e80224e1744fe6e0d609b8b1e1348ff
printf '#!/usr/bin/env bash\nexit 0\n' > "$BUNDLE/lighter-ratelimit"
chmod 755 "$BUNDLE/lighter-ratelimit"
cat > "$BUNDLE/lighter-ratelimit.service" <<'EOF'
[Unit]
Before=debot-pair-robinhood-lighter.service
[Service]
Type=simple
ExecStart=/opt/lighter-ratelimit/bin/lighter-ratelimit
[Install]
WantedBy=multi-user.target
EOF

BINARY_SHA=$(sha256sum "$BUNDLE/lighter-ratelimit" | awk '{print $1}')
UNIT_SHA=$(sha256sum "$BUNDLE/lighter-ratelimit.service" | awk '{print $1}')
printf '%s  lighter-ratelimit\n' "$BINARY_SHA" > "$BUNDLE/lighter-ratelimit.sha256"
jq -n \
  --arg source_sha "$SOURCE_SHA" \
  --arg deployment_source_sha "$DEPLOYMENT_SOURCE_SHA" \
  --arg binary_sha256 "$BINARY_SHA" \
  --arg unit_sha256 "$UNIT_SHA" \
  '{schema_version: 1, artifact: "lighter-ratelimit", architecture: "aarch64", source_sha: $source_sha, deployment_source_sha: $deployment_source_sha, binary_sha256: $binary_sha256, mode: "0755", unit_sha256: $unit_sha256, unit_mode: "0644"}' \
  > "$BUNDLE/manifest.json"
jq -n --arg sha "$SOURCE_SHA" '{dex_connector_sha: $sha}' > "$PAIRTRADE_DIR/manifest.json"

cat > "$WORK/aws" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
test "$1" = s3
test "$2" = cp
source=$3
destination=$4
case "$source" in
  */lighter-ratelimit) artifact=lighter-ratelimit ;;
  */lighter-ratelimit.sha256) artifact=lighter-ratelimit.sha256 ;;
  */manifest.json) artifact=manifest.json ;;
  */lighter-ratelimit.service) artifact=lighter-ratelimit.service ;;
  *) exit 2 ;;
esac
cp "$FAKE_BUNDLE/$artifact" "$destination"
EOF

cat > "$WORK/file" <<'EOF'
#!/usr/bin/env bash
printf '%s: ELF 64-bit LSB pie executable, ARM aarch64\n' "$1"
EOF

cat > "$WORK/ldd" <<'EOF'
#!/usr/bin/env bash
printf 'libc.so.6 => /lib64/libc.so.6\n'
EOF

cat > "$WORK/systemd-analyze" <<'EOF'
#!/usr/bin/env bash
test "$1" = verify
test -f "$2"
EOF

cat > "$WORK/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SYSTEMCTL_LOG"
case "$1" in
  daemon-reload|enable) ;;
  is-active)
    test -f "$SYSTEMCTL_STATE"
    ;;
  is-enabled)
    exit 0
    ;;
  start|restart)
    : > "$SYSTEMCTL_STATE"
    rm -f "$FAKE_SOCKET"
    python3 "$FAKE_SIDECAR_SERVER" "$FAKE_SOCKET" >>"$FAKE_SIDECAR_LOG" 2>&1 &
    echo "$!" >> "$FAKE_SIDECAR_PIDS_FILE"
    disown
    for _ in {1..50}; do
      [ -S "$FAKE_SOCKET" ] && break
      sleep 0.1
    done
    ;;
  show)
    printf 'debot-pair-robinhood-lighter.service network.target\n'
    ;;
  *) exit 2 ;;
esac
EOF
chmod +x "$WORK/aws" "$WORK/file" "$WORK/ldd" "$WORK/systemd-analyze" "$WORK/systemctl"
: > "$WORK/systemctl.log"

TEST_OWNER=$(id -un)
TEST_GROUP=$(id -gn)
FAKE_SIDECAR_PIDS_FILE="$WORK/fake-sidecar-server.pids"
: > "$FAKE_SIDECAR_PIDS_FILE"
run_bootstrap() {
  FAKE_BUNDLE="$BUNDLE" \
  AWS_BIN="$WORK/aws" \
  FILE_BIN="$WORK/file" \
  LDD_BIN="$WORK/ldd" \
  SYSTEMCTL="$WORK/systemctl" \
  SYSTEMCTL_LOG="$WORK/systemctl.log" \
  SYSTEMCTL_STATE="$WORK/systemctl.state" \
  FAKE_SOCKET="$SOCKET_PATH" \
  FAKE_SIDECAR_SERVER="$WORK/fake-sidecar-server.py" \
  FAKE_SIDECAR_LOG="$WORK/fake-sidecar-server.log" \
  FAKE_SIDECAR_PIDS_FILE="$FAKE_SIDECAR_PIDS_FILE" \
  SYSTEMD_ANALYZE="$WORK/systemd-analyze" \
  SYSTEMD_DIR="$SYSTEMD_DIR" \
  SIDECAR_ROOT="$SIDECAR_ROOT" \
  SOCKET_PATH="$SOCKET_PATH" \
  INSTALL_OWNER="$TEST_OWNER" \
  INSTALL_GROUP="$TEST_GROUP" \
    bash "$REPO_ROOT/scripts/bootstrap-robinhood-sidecar.sh" test-bucket "$PAIRTRADE_DIR"
}

run_bootstrap
cmp "$BUNDLE/lighter-ratelimit" "$SIDECAR_ROOT/bin/lighter-ratelimit"
cmp "$BUNDLE/lighter-ratelimit.service" "$SYSTEMD_DIR/lighter-ratelimit.service"
cmp "$BUNDLE/manifest.json" "$SIDECAR_ROOT/active-manifest.json"
test "$(grep -c '^start lighter-ratelimit.service$' "$WORK/systemctl.log")" -eq 1

# An identical deployment is idempotent: no start or restart.
run_bootstrap
test "$(grep -Ec '^(start|restart) lighter-ratelimit.service$' "$WORK/systemctl.log")" -eq 1

# Simulate interruption after updated files were installed but before the
# process was restarted and the active-release marker advanced.
printf '#!/usr/bin/env bash\nexit 1\n' > "$BUNDLE/lighter-ratelimit"
chmod 755 "$BUNDLE/lighter-ratelimit"
BINARY_SHA=$(sha256sum "$BUNDLE/lighter-ratelimit" | awk '{print $1}')
printf '%s  lighter-ratelimit\n' "$BINARY_SHA" > "$BUNDLE/lighter-ratelimit.sha256"
jq --arg sha "$BINARY_SHA" '.binary_sha256 = $sha' "$BUNDLE/manifest.json" > "$WORK/manifest.next"
mv "$WORK/manifest.next" "$BUNDLE/manifest.json"
cp "$BUNDLE/lighter-ratelimit" "$SIDECAR_ROOT/bin/lighter-ratelimit"
cp "$BUNDLE/manifest.json" "$SIDECAR_ROOT/manifest.json"
run_bootstrap
test "$(grep -c '^restart lighter-ratelimit.service$' "$WORK/systemctl.log")" -eq 1
cmp "$BUNDLE/manifest.json" "$SIDECAR_ROOT/active-manifest.json"

# A bundle built from a different dex-connector source fails before mutation.
jq '.source_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' \
  "$BUNDLE/manifest.json" > "$WORK/manifest.bad"
mv "$WORK/manifest.bad" "$BUNDLE/manifest.json"
if run_bootstrap; then
  echo "expected source provenance mismatch to fail" >&2
  exit 1
fi
cmp "$BUNDLE/lighter-ratelimit" "$SIDECAR_ROOT/bin/lighter-ratelimit"

echo "Robinhood sidecar bootstrap tests passed"
