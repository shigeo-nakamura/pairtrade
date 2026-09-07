#!/bin/bash
# Fetch the producer's signal file from S3 into the book runtime's state
# directory (bot-strategy#937). Runs from book-signal-fetch-<instance>.timer
# on the host as the book-runtime identity. Atomic: the download lands in a
# temp file next to the target and is renamed only if it parses as JSON
# and its payload changed, so the runtime never reads a half-written or
# byte-identical re-upload as a new file.
#
#   book_signal_fetch.sh s3://bucket/debot/book/xsmom-695/signal.json /var/lib/book-runtime/xsmom-695/signal.json
set -euo pipefail

SRC=${1:?s3 uri}
DST=${2:?destination path}
DIR=$(dirname "$DST")
TMP="$DIR/.signal.fetch.$$"
trap 'rm -f "$TMP"' EXIT

if ! aws s3 cp --only-show-errors "$SRC" "$TMP"; then
  echo "book_signal_fetch: download failed: $SRC" >&2
  exit 1
fi
if ! python3 -c "import json,sys; d=json.load(open(sys.argv[1])); assert d['schema_version']==1 and 'payload_sha256' in d" "$TMP" 2>/dev/null; then
  echo "book_signal_fetch: $SRC is not a schema-v1 signal file; keeping the current one" >&2
  exit 1
fi
if [ -f "$DST" ] && cmp -s "$TMP" "$DST"; then
  exit 0
fi
chmod 0640 "$TMP"
mv -f "$TMP" "$DST"
echo "book_signal_fetch: updated $DST ($(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d['decision_key'], d['payload_sha256'][:12])" "$DST"))"
