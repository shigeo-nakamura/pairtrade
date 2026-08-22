#!/bin/bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
cleanup() {
    rm -rf -- "$WORK"
}
trap cleanup EXIT
export ROBINHOOD_RUNTIME_LOCK_FILE="$WORK/runtime.lock"

S3_ROOT="$WORK/s3"
INSTALL_DIR="$WORK/install"
mkdir -p "$S3_ROOT/debot" "$INSTALL_DIR/bin" "$INSTALL_DIR/lib" "$INSTALL_DIR/scripts"

printf 'new debot\n' > "$S3_ROOT/debot/debot"
printf 'new libsigner\n' > "$S3_ROOT/debot/libsigner.so"
DEBOT_SHA=$(sha256sum "$S3_ROOT/debot/debot" | awk '{print $1}')
LIBSIGNER_SHA=$(sha256sum "$S3_ROOT/debot/libsigner.so" | awk '{print $1}')
mkdir -p "$WORK/package/bin" "$WORK/package/lib"
cp "$S3_ROOT/debot/debot" "$WORK/package/bin/debot"
cp "$S3_ROOT/debot/libsigner.so" "$WORK/package/lib/libsigner.so"
(cd "$WORK/package" && sha256sum bin/debot lib/libsigner.so > "$S3_ROOT/debot/checksums.sha256")
jq -n \
  --arg debot_sha "$DEBOT_SHA" \
  --arg libsigner_sha "$LIBSIGNER_SHA" \
  '{schema_version: 1, architecture: "aarch64", dex_connector_sha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", artifacts: {debot: {path: "bin/debot", sha256: $debot_sha, mode: "0755"}, libsigner: {path: "lib/libsigner.so", sha256: $libsigner_sha, mode: "0644"}}}' \
  > "$S3_ROOT/debot/manifest.json"

printf 'old debot\n' > "$INSTALL_DIR/bin/debot"
printf 'old libsigner\n' > "$INSTALL_DIR/lib/libsigner.so"
printf 'old checksums\n' > "$INSTALL_DIR/checksums.sha256"
printf '{"old":true}\n' > "$INSTALL_DIR/manifest.json"
cp -a "$INSTALL_DIR" "$WORK/install.before"

cat > "$WORK/aws" <<'SCRIPT'
#!/bin/bash
set -euo pipefail
test "$1" = s3
test "$2" = cp
source=$3
destination=$4
relative=${source#s3://test-bucket/}
cp "$FAKE_S3_ROOT/$relative" "$destination"
SCRIPT

cat > "$WORK/bootstrap" <<'SCRIPT'
#!/bin/bash
set -euo pipefail
mode=activate
if [ "${1:-}" = --validate-only ]; then
    mode=validate
    shift
fi
if [ "$mode" = activate ]; then
    test "$1" = local-bundle
    test -d "$SIDECAR_BUNDLE_DIR"
else
    test "$1" = test-bucket
fi
test -f "$2/manifest.json"
jq -e '.dex_connector_sha == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"' "$2/manifest.json" >/dev/null
printf '%s\n' "$mode" >> "$BOOTSTRAP_LOG"
if [ "$mode" = validate ] && [ "${FAIL_PREFLIGHT:-0}" = 1 ]; then
    echo "simulated sidecar provenance failure" >&2
    exit 1
fi
if [ "$mode" = validate ] && [ -n "${SIDECAR_STAGE_DIR:-}" ]; then
    mkdir -p "$SIDECAR_STAGE_DIR"
    printf 'fake sidecar\n' > "$SIDECAR_STAGE_DIR/lighter-ratelimit"
    sidecar_sha=$(sha256sum "$SIDECAR_STAGE_DIR/lighter-ratelimit" | awk '{print $1}')
    printf '%s  lighter-ratelimit\n' "$sidecar_sha" > "$SIDECAR_STAGE_DIR/lighter-ratelimit.sha256"
    printf '{}\n' > "$SIDECAR_STAGE_DIR/manifest.json"
    printf '[Service]\n' > "$SIDECAR_STAGE_DIR/lighter-ratelimit.service"
fi
SCRIPT

cat > "$WORK/install-command" <<'SCRIPT'
#!/bin/bash
set -euo pipefail
if [ "${FAIL_RUNTIME_INSTALL:-0}" = 1 ] && [[ " $* " == *" $FAKE_INSTALL_DIR/robinhood-sidecar-bundle/ "* ]]; then
    echo "simulated runtime install failure" >&2
    exit 1
fi
exec "$REAL_INSTALL" "$@"
SCRIPT
chmod +x "$WORK/aws" "$WORK/bootstrap" "$WORK/install-command"

run_deploy() {
  FAKE_S3_ROOT="$S3_ROOT" \
  AWS_BIN="$WORK/aws" \
  BOOTSTRAP_BIN="$WORK/bootstrap" \
  BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  INSTALL_BIN="$WORK/install-command" \
  REAL_INSTALL="$(command -v install)" \
  FAKE_INSTALL_DIR="$INSTALL_DIR" \
  INSTALL_OWNER="$(id -un)" \
  INSTALL_GROUP="$(id -gn)" \
  RUNTIME_DIR_OWNER="$(id -un)" \
  RUNTIME_DIR_GROUP="$(id -gn)" \
    bash "$REPO_ROOT/scripts/deploy-robinhood-runtime.sh" \
      test-bucket "$INSTALL_DIR" "$DEBOT_SHA" "$LIBSIGNER_SHA"
}

: > "$WORK/bootstrap.log"

# A failed sidecar preflight must leave both runtime and sidecar untouched.
if FAIL_PREFLIGHT=1 run_deploy >"$WORK/preflight-failure.log" 2>&1; then
    echo "expected sidecar preflight failure" >&2
    exit 1
fi
diff -ru "$WORK/install.before" "$INSTALL_DIR"
grep -F "simulated sidecar provenance failure" "$WORK/preflight-failure.log"
test "$(cat "$WORK/bootstrap.log")" = validate

# A partial runtime install is rolled back before sidecar activation.
: > "$WORK/bootstrap.log"
if FAIL_RUNTIME_INSTALL=1 run_deploy >"$WORK/install-failure.log" 2>&1; then
    echo "expected runtime install failure" >&2
    exit 1
fi
diff -ru "$WORK/install.before" "$INSTALL_DIR"
grep -F "restoring the previous runtime before sidecar activation" "$WORK/install-failure.log"
test "$(cat "$WORK/bootstrap.log")" = validate

# A successful deploy commits runtime metadata but does not activate the sidecar.
: > "$WORK/bootstrap.log"
run_deploy
cmp "$S3_ROOT/debot/debot" "$INSTALL_DIR/bin/debot"
cmp "$S3_ROOT/debot/libsigner.so" "$INSTALL_DIR/lib/libsigner.so"
cmp "$S3_ROOT/debot/checksums.sha256" "$INSTALL_DIR/checksums.sha256"
cmp "$S3_ROOT/debot/manifest.json" "$INSTALL_DIR/manifest.json"
test -f "$INSTALL_DIR/robinhood-sidecar-bundle/lighter-ratelimit"
test -f "$INSTALL_DIR/robinhood-sidecar-bundle/lighter-ratelimit.sha256"
test -f "$INSTALL_DIR/robinhood-sidecar-bundle/manifest.json"
test -f "$INSTALL_DIR/robinhood-sidecar-bundle/lighter-ratelimit.service"
test "$(stat -c %U:%G:%a "$INSTALL_DIR/bin")" = "$(id -un):$(id -gn):750"
test "$(stat -c %U:%G:%a "$INSTALL_DIR/lib")" = "$(id -un):$(id -gn):750"
test "$(cat "$WORK/bootstrap.log")" = validate

# A legacy config-only host without a staged bundle remains restartable.
BOOTSTRAP_BIN="$WORK/bootstrap" BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  bash "$REPO_ROOT/scripts/activate-robinhood-sidecar.sh" "$WORK/legacy-install" \
  2>"$WORK/legacy-warning.log"
grep -F "bundle not staged yet" "$WORK/legacy-warning.log"
test "$(cat "$WORK/bootstrap.log")" = validate

# The systemd dependency activates the sidecar against committed runtime.
BOOTSTRAP_BIN="$WORK/bootstrap" BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  bash "$REPO_ROOT/scripts/activate-robinhood-sidecar.sh" "$INSTALL_DIR"
test "$(sed -n '2p' "$WORK/bootstrap.log")" = activate
test "$(wc -l < "$WORK/bootstrap.log")" -eq 2

# A bot-start activation waits while a runtime publication owns the shared
# lock, so it cannot inspect or activate a partially committed file set.
exec 8>"$ROBINHOOD_RUNTIME_LOCK_FILE"
flock -x 8
BOOTSTRAP_BIN="$WORK/bootstrap" BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  bash "$REPO_ROOT/scripts/activate-robinhood-sidecar.sh" "$INSTALL_DIR" &
ACTIVATION_PID=$!
sleep 0.2
kill -0 "$ACTIVATION_PID"
test "$(wc -l < "$WORK/bootstrap.log")" -eq 2
flock -u 8
wait "$ACTIVATION_PID"
test "$(sed -n '3p' "$WORK/bootstrap.log")" = activate
test "$(wc -l < "$WORK/bootstrap.log")" -eq 3

echo "Robinhood runtime staging tests passed"
