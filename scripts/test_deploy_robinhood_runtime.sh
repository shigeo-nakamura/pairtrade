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
test "$1" = test-bucket
test -f "$2/manifest.json"
jq -e '.dex_connector_sha == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"' "$2/manifest.json" >/dev/null
if [ "${FAIL_BOOTSTRAP:-0}" = 1 ]; then
    echo "simulated sidecar provenance failure" >&2
    exit 1
fi
printf '%s\n' "$2" > "$BOOTSTRAP_LOG"
SCRIPT
chmod +x "$WORK/aws" "$WORK/bootstrap"

run_deploy() {
  FAKE_S3_ROOT="$S3_ROOT" \
  AWS_BIN="$WORK/aws" \
  BOOTSTRAP_BIN="$WORK/bootstrap" \
  BOOTSTRAP_LOG="$WORK/bootstrap.log" \
  INSTALL_OWNER="$(id -un)" \
  INSTALL_GROUP="$(id -gn)" \
    bash "$REPO_ROOT/scripts/deploy-robinhood-runtime.sh" \
      test-bucket "$INSTALL_DIR" "$DEBOT_SHA" "$LIBSIGNER_SHA"
}

# A failed sidecar preflight must leave all installed runtime files untouched.
if FAIL_BOOTSTRAP=1 run_deploy >"$WORK/preflight-failure.log" 2>&1; then
    echo "expected sidecar preflight failure" >&2
    exit 1
fi
diff -ru "$WORK/install.before" "$INSTALL_DIR"
grep -F "simulated sidecar provenance failure" "$WORK/preflight-failure.log"

# A successful preflight installs the fully validated staged artifact set.
run_deploy
cmp "$S3_ROOT/debot/debot" "$INSTALL_DIR/bin/debot"
cmp "$S3_ROOT/debot/libsigner.so" "$INSTALL_DIR/lib/libsigner.so"
cmp "$S3_ROOT/debot/checksums.sha256" "$INSTALL_DIR/checksums.sha256"
cmp "$S3_ROOT/debot/manifest.json" "$INSTALL_DIR/manifest.json"
test -s "$WORK/bootstrap.log"

echo "Robinhood runtime staging tests passed"
