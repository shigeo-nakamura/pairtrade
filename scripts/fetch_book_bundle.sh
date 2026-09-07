#!/bin/bash
# Download one immutable book runtime bundle (binary + signer + manifest)
# into DEST and verify both artifacts against that bundle's own manifest.
#
#   fetch_book_bundle.sh <bucket> <dest> [prefix]
#
# Without a prefix the pointer object `debot/book-runtime/current.json`
# names the release to take. CI writes that pointer last, after the
# versioned objects, so a reader either sees the previous release or a
# complete new one -- never a new binary paired with the previous signer,
# which is what independently downloading two unversioned keys can give.
#
# exit 0 = bundle staged and verified, 10 = nothing published yet,
# 1 = anything else (a failure here must not be mistaken for "not
# published", see s3_object_present.sh).
set -euo pipefail

BUCKET=${1:?bucket}
DEST=${2:?dest}
PREFIX=${3:-}
HERE=$(cd "$(dirname "$0")" && pwd)

install -d -m 0755 "$DEST"

if [ -z "$PREFIX" ]; then
  rc=0
  bash "$HERE/s3_object_present.sh" "$BUCKET" debot/book-runtime/current.json || rc=$?
  [ "$rc" = "10" ] && exit 10
  [ "$rc" = "0" ] || exit "$rc"
  aws s3 cp "s3://$BUCKET/debot/book-runtime/current.json" "$DEST/current.json"
  PREFIX=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['prefix'])" "$DEST/current.json")
  [ -n "$PREFIX" ] || { echo "current.json has no prefix" >&2; exit 1; }
fi

aws s3 cp "s3://$BUCKET/$PREFIX/book_runtime" "$DEST/book_runtime"
aws s3 cp "s3://$BUCKET/$PREFIX/libsigner.so" "$DEST/libsigner.so"
aws s3 cp "s3://$BUCKET/$PREFIX/manifest.json" "$DEST/manifest.json"

python3 - "$DEST" <<'PY'
import hashlib, json, sys
dest = sys.argv[1]
manifest = json.load(open(f"{dest}/manifest.json"))["artifacts"]
for name, path in (("book_runtime", "book_runtime"), ("libsigner", "libsigner.so")):
    want = manifest[name]["sha256"]
    got = hashlib.sha256(open(f"{dest}/{path}", "rb").read()).hexdigest()
    if got != want:
        raise SystemExit(f"{path}: sha256 {got} != manifest {want}")
PY

chmod 0755 "$DEST/book_runtime"
echo "$PREFIX"
