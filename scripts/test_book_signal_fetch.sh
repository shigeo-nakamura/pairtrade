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
export BOOK_SIGNAL_MAX_AGE_SECS=7200
export BOOK_SIGNAL_PRODUCER_ID=p
export BOOK_DECISION_TIME_UTC=23:59
export BOOK_SCHEDULE_KIND=daily
export BOOK_UNIVERSE=BTC,ETH
export BOOK_MAX_SYMBOL_WEIGHT=0.5
export BOOK_NET_TOLERANCE=0.05
export BOOK_REQUIRE_DOLLAR_NEUTRAL=true

# Generated now: the fetcher enforces BOOK_SIGNAL_MAX_AGE_SECS, so a
# fixed past timestamp would make the "good" fixture stale over time.
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
KEY=$(date -u +%Y-%m-%d)
python3 "$HERE/book_signal_file.py" --out "$T/good.json" --producer p --decision-key "$KEY" \
  --as-of "$NOW" --generated-at "$NOW" BTC=0.1 ETH=-0.1 >/dev/null
python3 - "$T" <<'PY'
import json, sys
t = sys.argv[1]
d = json.load(open(f"{t}/good.json"))
m = dict(d); del m["decision_key"]; json.dump(m, open(f"{t}/bad_missing.json", "w"))
h = json.loads(json.dumps(d)); h["weights"]["BTC"] = 0.2; json.dump(h, open(f"{t}/bad_hash.json", "w"))
g = dict(d); g["generated_at"] = 123; json.dump(g, open(f"{t}/bad_ts_type.json", "w"))
w = json.loads(json.dumps(d)); w["weights"]["BTC"] = "0.1"; json.dump(w, open(f"{t}/bad_weight_type.json", "w"))
import datetime, hashlib
def rehash(o):
    o["payload_sha256"] = hashlib.sha256(json.dumps(
        {"as_of": o["as_of"], "decision_key": o["decision_key"],
         "producer_id": o["producer_id"], "weights": o["weights"]},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return o
gen = datetime.datetime.strptime(d["generated_at"], "%Y-%m-%dT%H:%M:%SZ")
a = json.loads(json.dumps(d))
a["as_of"] = (gen + datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
json.dump(rehash(a), open(f"{t}/bad_lookahead.json", "w"))
v = dict(d); v["schema_version"] = True; json.dump(v, open(f"{t}/bad_schema_bool.json", "w"))
f = dict(d); f["schema_version"] = 1.0; json.dump(f, open(f"{t}/bad_schema_float.json", "w"))
n = json.loads(json.dumps(d)); n["meta"] = {"k": float("nan")}; open(f"{t}/bad_nan_meta.json", "w").write(json.dumps(n))
wrong = json.loads(json.dumps(d))
wrong["producer_id"] = "someone_else"
json.dump(rehash(wrong), open(f"{t}/bad_producer.json", "w"))
# as_of after the day's decision time (23:59 in the test env).
late = json.loads(json.dumps(d))
late["as_of"] = late["generated_at"]
late["generated_at"] = (gen + datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
json.dump(rehash(late), open(f"{t}/bad_after_decision.json", "w"))
outside = json.loads(json.dumps(d))
outside["weights"] = {"BTC": 0.1, "XYZ": -0.1}
json.dump(rehash(outside), open(f"{t}/bad_universe.json", "w"))
big = json.loads(json.dumps(d))
big["weights"] = {"BTC": 0.9, "ETH": -0.9}
json.dump(rehash(big), open(f"{t}/bad_symbol_cap.json", "w"))
skew = json.loads(json.dumps(d))
skew["weights"] = {"BTC": 0.4, "ETH": -0.1}
json.dump(rehash(skew), open(f"{t}/bad_net.json", "w"))
# A date-shaped key that is neither the current nor the next decision.
oldkey = json.loads(json.dumps(d))
oldkey["decision_key"] = (datetime.datetime.strptime(d["decision_key"], "%Y-%m-%d") - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
json.dump(rehash(oldkey), open(f"{t}/bad_old_key.json", "w"))
stale = json.loads(json.dumps(d))
stale["generated_at"] = "2020-01-01T00:00:00Z"
stale["as_of"] = "2020-01-01T00:00:00Z"
json.dump(rehash(stale), open(f"{t}/bad_stale.json", "w"))
fut = json.loads(json.dumps(d))
fut["generated_at"] = "2099-01-01T00:00:00Z"
json.dump(fut, open(f"{t}/bad_future.json", "w"))
raw = open(f"{t}/good.json").read()
open(f"{t}/bad_dup_key.json", "w").write(raw.replace("{", '{"producer_id":"other",', 1))
open(f"{t}/bad_json.json", "w").write("{")
PY

bash "$HERE/book_signal_fetch.sh" "$T/good.json" "$T/dst/signal.json" | grep -q "updated .*($KEY "
cmp -s "$T/good.json" "$T/dst/signal.json"

for bad in bad_missing bad_hash bad_ts_type bad_weight_type bad_lookahead bad_schema_bool bad_schema_float bad_nan_meta bad_dup_key bad_future bad_stale bad_producer bad_after_decision bad_universe bad_symbol_cap bad_net bad_old_key bad_json; do
  if bash "$HERE/book_signal_fetch.sh" "$T/$bad.json" "$T/dst/signal.json" 2>/dev/null; then
    echo "FAIL: $bad was promoted" >&2; exit 1
  fi
  cmp -s "$T/good.json" "$T/dst/signal.json" || { echo "FAIL: $bad displaced the current file" >&2; exit 1; }
done

out=$(bash "$HERE/book_signal_fetch.sh" "$T/good.json" "$T/dst/signal.json")
[ -z "$out" ] || { echo "FAIL: unchanged re-download should be silent, got: $out" >&2; exit 1; }
ls -A "$T/dst" | grep -q '^\.signal\.' && { echo "FAIL: temp file left behind" >&2; exit 1; }
echo "book_signal_fetch tests OK"
