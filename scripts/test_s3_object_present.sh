#!/bin/bash
# Exercises scripts/s3_object_present.sh with a fake `aws` on PATH: a
# present object, a confirmed 404, and an ambiguous failure (403) that
# must NOT be reported as missing.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"
cat > "$T/bin/aws" <<'FAKE'
#!/bin/bash
case "$FAKE_AWS_MODE" in
  present) echo '{"ContentLength": 1}'; exit 0 ;;
  missing) echo "An error occurred (404) when calling the HeadObject operation: Not Found" >&2; exit 254 ;;
  denied)  echo "An error occurred (403) when calling the HeadObject operation: Forbidden" >&2; exit 254 ;;
esac
FAKE
chmod +x "$T/bin/aws"
export PATH="$T/bin:$PATH"

FAKE_AWS_MODE=present bash "$HERE/s3_object_present.sh" b k
rc=0; FAKE_AWS_MODE=missing bash "$HERE/s3_object_present.sh" b k || rc=$?
[ "$rc" = "10" ] || { echo "FAIL: missing object should exit 10, got $rc" >&2; exit 1; }
rc=0; FAKE_AWS_MODE=denied bash "$HERE/s3_object_present.sh" b k 2>/dev/null || rc=$?
[ "$rc" = "1" ] || { echo "FAIL: an ambiguous failure must exit 1, not 10; got $rc" >&2; exit 1; }
echo "s3_object_present tests OK"
