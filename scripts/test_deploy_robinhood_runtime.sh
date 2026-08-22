#!/bin/bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
cleanup() {
    rm -rf -- "$WORK"
}
trap cleanup EXIT

S3_ROOT="$WORK/s3"
INSTALL_DIR="$WORK/install"
mkdir -p "$S3_ROOT/debot" "$INSTALL_DIR/bin" "$INSTALL_DIR/lib" "$INSTALL_DIR/scripts"

printf "new debot\n" > "$S3_ROOT/debot/debot"
printf "new libsigner\n" > "$S3_ROOT/debot/libsigner.so"
DEBOT_SHA=$(sha256sum "$S3_ROOT/debot/debot" | awk "{print \$1}")
LIBSIGNER_SHA=$(sha256sum "$S3_ROOT/debot/libsigner.so" | awk "{print \$1}")
mkdir -p "$WORK/package/bin" "$WORK/package/lib"
cp "$S3_ROOT/debot/debot" "$WORK/package/bin/debot"
cp "$S3_ROOT/debot/libsigner.so" "$WORK/package/lib/libsigner.so"
(cd "$WORK/package" && sha256sum bin/debot lib/libsigner.so > "$S3_ROOT/debot/checksums.sha256")
jq -n \
  --arg debot_sha "$DEBOT_SHA" \
  --arg libsigner_sha "$LIBSIGNER_SHA" \
  "{schema_version: 1, architecture: \"aarch64\", dex_connector_sha: \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\", artifacts: {debot: {path: \"bin/debot\", sha256: \$debot_sha, mode: \"0755\"}, libsigner: {path: \"lib/libsigner.so\", sha256: \$libsigner_sha, mode: \"0644\"}}}" \
  > "$S3_ROOT/debot/manifest.json"

printf "old debot\n" > "$INSTALL_DIR/bin/debot"
printf "old libsigner\n" > "$INSTALL_DIR/lib/libsigner.so"
printf "old checksums\n" > "$INSTALL_DIR/checksums.sha256"
printf "{\"old\":true}\n" > "$INSTALL_DIR/manifest.json"
cp -a "$INSTALL_DIR" "$WORK/install.before"

cat > "$WORK/aws" <<"SCRIPT"
#!/bin/bash
set -euo pipefail
test "$1" = s3
test "$2" = cp
source=$3
destination=$4
relative=${source#s3://test-bucket/}
cp "$FAKE_S3_ROOT/$relative" "$destination"
SCRIPT

cat > "$WORK/bootstrap" <<"SCRIPT"
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
jq -e ".dex_connector_sha == \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"" "$2/manifest.json" >/dev/null
printf "%s %s\n" "$mode" "$2" >> "$BOOTSTRAP_LOG"
if [ "$mode" = validate ] && [ "${FAIL_PREFLIGHT:-0}" = 1 ]; then
    echo "simulated sidecar provenance failure" >&2
    exit 1
fi
if [ "$mode" = validate ] && [ -n "${SIDECAR_STAGE_DIR:-}" ]; then
    mkdir -p "$SIDECAR_STAGE_DIR"
    printf "fake sidecar\n" > "$SIDECAR_STAGE_DIR/lighter-ratelimit"
    sidecar_sha=$(sha256sum "$SIDECAR_STAGE_DIR/lighter-ratelimit" | awk "{print \$1}")
    printf "%s  lighter-ratelimit\n" "$sidecar_sha" > "$SIDECAR_STAGE_DIR/lighter-ratelimit.sha256"
    printf "{}\n" > "$SIDECAR_STAGE_DIR/manifest.json"
    printf "[Service]\n" > "$SIDECAR_STAGE_DIR/lighter-ratelimit.service"
fi
SCRIPT

cat > "$WORK/install-command" <<"SCRIPT"
#!/bin/bash
set -euo pipefail
if [ "${FAIL_RUNTIME_INSTALL:-0}" = 1 ] &&
   [[ "$*" == *"/robinhood-releases/.release."*"/robinhood-sidecar-bundle/"* ]]; then
    echo "simulated runtime install failure" >&2
    exit 1
fi
exec "$REAL_INSTALL" "$@"
SCRIPT

cat > "$WORK/mv-command" <<"SCRIPT"
#!/bin/bash
set -euo pipefail
last=${!#}
if [ "${FAIL_POINTER_SWITCH:-0}" = 1 ] &&
   [ "$last" = "$FAKE_INSTALL_DIR/robinhood-runtime-current" ]; then
    echo "simulated pointer switch failure" >&2
    exit 1
fi
exec "$REAL_MV" "$@"
SCRIPT
chmod +x "$WORK/aws" "$WORK/bootstrap" "$WORK/install-command" "$WORK/mv-command"

run_deploy() {
  FAKE_S3_ROOT="$S3_ROOT" \
  AWS_BIN="$WORK/aws" \
  BOOTSTRAP_BIN="$WORK/bootstrap" \
  BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  INSTALL_BIN="$WORK/install-command" \
  MV_BIN="$WORK/mv-command" \
  REAL_INSTALL="$(command -v install)" \
  REAL_MV="$(command -v mv)" \
  FAKE_INSTALL_DIR="$INSTALL_DIR" \
  INSTALL_OWNER="$(id -un)" \
  INSTALL_GROUP="$(id -gn)" \
  RUNTIME_DIR_OWNER="$(id -un)" \
  RUNTIME_DIR_GROUP="$(id -gn)" \
    bash "$REPO_ROOT/scripts/deploy-robinhood-runtime.sh" \
      test-bucket "$INSTALL_DIR" "$DEBOT_SHA" "$LIBSIGNER_SHA"
}

assert_legacy_runtime_unchanged() {
    cmp "$WORK/install.before/bin/debot" "$INSTALL_DIR/bin/debot"
    cmp "$WORK/install.before/lib/libsigner.so" "$INSTALL_DIR/lib/libsigner.so"
    cmp "$WORK/install.before/checksums.sha256" "$INSTALL_DIR/checksums.sha256"
    cmp "$WORK/install.before/manifest.json" "$INSTALL_DIR/manifest.json"
}

: > "$WORK/bootstrap.log"

# A failed sidecar preflight leaves the legacy runtime and current pointer untouched.
if FAIL_PREFLIGHT=1 run_deploy >"$WORK/preflight-failure.log" 2>&1; then
    echo "expected sidecar preflight failure" >&2
    exit 1
fi
assert_legacy_runtime_unchanged
test ! -e "$INSTALL_DIR/robinhood-runtime-current"
grep -F "simulated sidecar provenance failure" "$WORK/preflight-failure.log"
test "$(wc -l < "$WORK/bootstrap.log")" -eq 1

# A partial immutable-release install is removed before publication.
: > "$WORK/bootstrap.log"
if FAIL_RUNTIME_INSTALL=1 run_deploy >"$WORK/install-failure.log" 2>&1; then
    echo "expected runtime install failure" >&2
    exit 1
fi
assert_legacy_runtime_unchanged
test ! -e "$INSTALL_DIR/robinhood-runtime-current"
test -z "$(find "$INSTALL_DIR/robinhood-releases" -mindepth 1 -maxdepth 1 -print -quit)"
grep -F "current runtime pointer was not changed" "$WORK/install-failure.log"
test "$(wc -l < "$WORK/bootstrap.log")" -eq 1

# A successful deploy publishes one complete immutable release without activation.
: > "$WORK/bootstrap.log"
run_deploy
CURRENT_RELEASE=$(readlink -f "$INSTALL_DIR/robinhood-runtime-current")
case "$CURRENT_RELEASE" in
  "$INSTALL_DIR"/robinhood-releases/*) ;;
  *) echo "current runtime did not resolve inside the release directory" >&2; exit 1 ;;
esac
cmp "$S3_ROOT/debot/debot" "$CURRENT_RELEASE/bin/debot"
cmp "$S3_ROOT/debot/libsigner.so" "$CURRENT_RELEASE/lib/libsigner.so"
cmp "$S3_ROOT/debot/checksums.sha256" "$CURRENT_RELEASE/checksums.sha256"
cmp "$S3_ROOT/debot/manifest.json" "$CURRENT_RELEASE/manifest.json"
test -f "$CURRENT_RELEASE/robinhood-sidecar-bundle/lighter-ratelimit"
test -f "$CURRENT_RELEASE/robinhood-sidecar-bundle/lighter-ratelimit.sha256"
test -f "$CURRENT_RELEASE/robinhood-sidecar-bundle/manifest.json"
test -f "$CURRENT_RELEASE/robinhood-sidecar-bundle/lighter-ratelimit.service"
test "$(stat -c %U:%G:%a "$CURRENT_RELEASE/bin")" = "$(id -un):$(id -gn):755"
test "$(stat -c %U:%G:%a "$CURRENT_RELEASE/lib")" = "$(id -un):$(id -gn):755"
assert_legacy_runtime_unchanged
test "$(wc -l < "$WORK/bootstrap.log")" -eq 1

# A failed pointer rename cannot expose a partial release or change current.
if FAIL_POINTER_SWITCH=1 run_deploy >"$WORK/pointer-failure.log" 2>&1; then
    echo "expected runtime pointer switch failure" >&2
    exit 1
fi
test "$(readlink -f "$INSTALL_DIR/robinhood-runtime-current")" = "$CURRENT_RELEASE"
grep -F "previous release remains current" "$WORK/pointer-failure.log"

# Before the first runtime deploy, activation pins the legacy install and no-ops.
LEGACY_INSTALL="$WORK/legacy-install"
LEGACY_PIN="$WORK/legacy-run/runtime"
mkdir -p "$LEGACY_INSTALL/bin"
printf "legacy executable\n" > "$LEGACY_INSTALL/bin/debot"
BOOTSTRAP_BIN="$WORK/bootstrap" BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  bash "$REPO_ROOT/scripts/activate-robinhood-sidecar.sh" "$LEGACY_INSTALL" "$LEGACY_PIN" \
  2>"$WORK/legacy-warning.log"
test "$(readlink -f "$LEGACY_PIN")" = "$(readlink -f "$LEGACY_INSTALL")"
grep -F "pinned legacy runtime" "$WORK/legacy-warning.log"
test "$(wc -l < "$WORK/bootstrap.log")" -eq 2

# The dependency pins and activates exactly the same complete release.
RUNTIME_PIN="$WORK/runtime-run/runtime"
BOOTSTRAP_BIN="$WORK/bootstrap" BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  bash "$REPO_ROOT/scripts/activate-robinhood-sidecar.sh" "$INSTALL_DIR" "$RUNTIME_PIN"
test "$(readlink -f "$RUNTIME_PIN")" = "$CURRENT_RELEASE"
test "$(tail -n 1 "$WORK/bootstrap.log")" = "activate $CURRENT_RELEASE"

# Later deploy pointer changes cannot alter the already pinned bot start.
ALTERNATE_RELEASE="$INSTALL_DIR/robinhood-releases/alternate"
mkdir -p "$ALTERNATE_RELEASE"
ln -sfn "robinhood-releases/alternate" "$INSTALL_DIR/.current-next"
mv -Tf "$INSTALL_DIR/.current-next" "$INSTALL_DIR/robinhood-runtime-current"
test "$(readlink -f "$RUNTIME_PIN")" = "$CURRENT_RELEASE"
test "$(readlink -f "$INSTALL_DIR/robinhood-runtime-current")" = "$ALTERNATE_RELEASE"

echo "Robinhood atomic runtime publication tests passed"
