#!/bin/bash
# Drives fetch_book_bundle.sh against a fake `aws` backed by a local tree:
# a complete bundle verifies, a tampered artifact fails, and an absent
# pointer reports "nothing published" (10) rather than an error.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/s3/debot/book-runtime/releases/abc123"

printf 'BIN\n' > "$T/s3/debot/book-runtime/releases/abc123/book_runtime"
printf 'SIG\n' > "$T/s3/debot/book-runtime/releases/abc123/libsigner.so"
python3 - "$T" <<'PY'
import hashlib, json, sys
t = sys.argv[1]
p = f"{t}/s3/debot/book-runtime/releases/abc123"
sha = lambda f: hashlib.sha256(open(f, "rb").read()).hexdigest()
json.dump({"artifacts": {"book_runtime": {"sha256": sha(f"{p}/book_runtime")},
                         "libsigner": {"sha256": sha(f"{p}/libsigner.so")}}},
          open(f"{p}/manifest.json", "w"))
json.dump({"sha": "abc123", "prefix": "debot/book-runtime/releases/abc123"},
          open(f"{t}/s3/debot/book-runtime/current.json", "w"))
PY

cat > "$T/bin/aws" <<FAKE
#!/bin/bash
# s3 cp s3://bucket/key dest   |   s3api head-object --bucket b --key k
if [ "\$1" = "s3api" ]; then
  key=""; for a in "\$@"; do [ "\$prev" = "--key" ] && key="\$a"; prev="\$a"; done
  [ -f "$T/s3/\$key" ] && exit 0
  echo "An error occurred (404) when calling the HeadObject operation: Not Found" >&2; exit 254
fi
src=\${@: -2:1}; dst=\${@: -1}
cp "$T/s3/\${src#s3://*/}" "\$dst"
FAKE
chmod +x "$T/bin/aws"
export PATH="$T/bin:$PATH"

prefix=$(bash "$HERE/fetch_book_bundle.sh" bucket "$T/dest")
[ "$prefix" = "debot/book-runtime/releases/abc123" ] || { echo "FAIL: prefix $prefix" >&2; exit 1; }
cmp -s "$T/dest/book_runtime" "$T/s3/debot/book-runtime/releases/abc123/book_runtime"

# A signer that does not match the manifest must fail, not install.
printf 'OTHER\n' > "$T/s3/debot/book-runtime/releases/abc123/libsigner.so"
if bash "$HERE/fetch_book_bundle.sh" bucket "$T/dest2" 2>/dev/null; then
  echo "FAIL: a mismatched artifact was accepted" >&2; exit 1
fi

# Nothing published yet -> 10, distinct from an error.
rm "$T/s3/debot/book-runtime/current.json"
rc=0; bash "$HERE/fetch_book_bundle.sh" bucket "$T/dest3" >/dev/null 2>&1 || rc=$?
[ "$rc" = "10" ] || { echo "FAIL: missing pointer should exit 10, got $rc" >&2; exit 1; }
echo "fetch_book_bundle tests OK"
