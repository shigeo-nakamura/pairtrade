#!/bin/bash
# Exercises scripts/book_signal_fetch.sh with a fake `aws` on PATH: a valid
# file is promoted, a file missing a required field or with a wrong payload
# hash is refused (current file kept), an unchanged re-download is a no-op.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/dst"
cat > "$T/bin/aws" <<'FAKE'
#!/bin/bash
# fake aws s3 cp [flags] <src> <dst>: copy a local path
src=${@: -2:1}; dst=${@: -1}
cp "$src" "$dst"
FAKE
chmod +x "$T/bin/aws"
export PATH="$T/bin:$PATH"

python3 "$HERE/book_signal_file.py" --out "$T/good.json" --producer p --decision-key 2026-09-06 \
  --as-of 2026-09-06T00:00:00Z --generated-at 2026-09-06T00:20:00Z BTC=0.1 ETH=-0.1 >/dev/null
python3 - "$T" <<'PY'
import json, sys
t = sys.argv[1]
d = json.load(open(f"{t}/good.json"))
m = dict(d); del m["decision_key"]; json.dump(m, open(f"{t}/bad_missing.json", "w"))
h = json.loads(json.dumps(d)); h["weights"]["BTC"] = 0.2; json.dump(h, open(f"{t}/bad_hash.json", "w"))
open(f"{t}/bad_json.json", "w").write("{")
PY

bash "$HERE/book_signal_fetch.sh" "$T/good.json" "$T/dst/signal.json" | grep -q "updated .* 2026-09-06"
cmp -s "$T/good.json" "$T/dst/signal.json"

for bad in bad_missing bad_hash bad_json; do
  if bash "$HERE/book_signal_fetch.sh" "$T/$bad.json" "$T/dst/signal.json" 2>/dev/null; then
    echo "FAIL: $bad was promoted" >&2; exit 1
  fi
  cmp -s "$T/good.json" "$T/dst/signal.json" || { echo "FAIL: $bad displaced the current file" >&2; exit 1; }
done

out=$(bash "$HERE/book_signal_fetch.sh" "$T/good.json" "$T/dst/signal.json")
[ -z "$out" ] || { echo "FAIL: unchanged re-download should be silent, got: $out" >&2; exit 1; }
ls -A "$T/dst" | grep -q '^\.signal\.' && { echo "FAIL: temp file left behind" >&2; exit 1; }
echo "book_signal_fetch tests OK"
