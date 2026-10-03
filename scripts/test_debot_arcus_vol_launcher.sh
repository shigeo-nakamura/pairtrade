#!/usr/bin/env bash
# Stub test for scripts/debot-arcus-vol.sh (bot-strategy#1093): a fake Arcus
# REST server on 127.0.0.1 and a fake binary that dumps its environment. Never
# contacts the real venue, never reads real credentials.
#   bash scripts/test_debot_arcus_vol_launcher.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LAUNCHER="$HERE/debot-arcus-vol.sh"
T="$(mktemp -d)"
trap 'kill "${SERVER_PID:-}" 2>/dev/null || true; rm -rf "$T"' EXIT

# ---- fake venue: responses come from files the test rewrites per case -----
mkdir -p "$T/venue"
cat > "$T/venue/server.py" <<'PY'
import http.server, json, os, sys
ROOT = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        name = self.path.split("?", 1)[0].rsplit("/", 1)[-1]
        path = os.path.join(ROOT, name + ".json")
        if not os.path.exists(path):
            self.send_response(404); self.end_headers(); return
        body = open(path, "rb").read()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
open(os.path.join(ROOT, "port"), "w").write(str(srv.server_address[1]))
srv.serve_forever()
PY
python3 "$T/venue/server.py" "$T/venue" &
SERVER_PID=$!
for _ in $(seq 1 50); do [ -s "$T/venue/port" ] && break; sleep 0.1; done
PORT="$(cat "$T/venue/port")"
venue() { # positions-json open-orders-json account-json
    printf '%s' "$1" > "$T/venue/positions.json"
    printf '%s' "$2" > "$T/venue/openOrders.json"
    printf '%s' "$3" > "$T/venue/account.json"
}
FLAT='{"positions":{},"total":0}'
NO_ORDERS='{"orders":[],"total":0}'
acct() { printf '{"equity":"%s","freeCollateral":"%s"}' "$1" "$2"; }

# ---- fake binary + env files ----------------------------------------------
mkdir -p "$T/bin" "$T/etc" "$T/state"
cat > "$T/bin/arcus_vol_runtime" <<'SH'
#!/usr/bin/env bash
env | grep -E '^(ARCUS_|RUST_LOG)' | sort > "$FAKE_ENV_OUT"
echo "FAKE RUNTIME STARTED"
SH
chmod 755 "$T/bin/arcus_vol_runtime"
write_live() { # mode account-index
    printf 'ARCUS_ADDRESS=0xA2C78E14DFD5586444CE4FE28FC4E36308A066D6\nARCUS_API_KEY=fake\nARCUS_PLAIN_API_PRIVATE_KEY=fake\nARCUS_ACCOUNT_INDEX=%s\n' "$2" > "$T/etc/live.env"
    chmod "$1" "$T/etc/live.env"
}
write_live 600 0
rm -f "$T/etc/config.env"

run() { # -> stdout+stderr in $OUT, exit code in $RC
    export FAKE_ENV_OUT="$T/env.out"
    rm -f "$FAKE_ENV_OUT"
    set +e
    OUT="$(DEBOT_ARCUS_VOL_ETC_DIR="$T/etc" DEBOT_ARCUS_VOL_BIN="$T/bin/arcus_vol_runtime" \
        DEBOT_ARCUS_VOL_API_BASE="http://127.0.0.1:$PORT" STATE_DIR="$T/state" LOCK_DIR="$T/locks" \
        bash "$LAUNCHER" 2>&1)"
    RC=$?
    set -e
}
fail() { echo "FAIL: $*"; echo "--- output:"; echo "$OUT"; exit 1; }
envval() { grep "^$1=" "$T/env.out" | cut -d= -f2-; }
PASS=0
ok() { PASS=$((PASS + 1)); echo "ok $PASS - $*"; }

# 1. happy path, defaults (no config.env), free collateral well above the clip
venue "$FLAT" "$NO_ORDERS" "$(acct 5557.07 5377.12)"
run
[ "$RC" -eq 0 ] || fail "happy path exit $RC"
grep -q "FAKE RUNTIME STARTED" <<< "$OUT" || fail "runtime not exec'd"
grep -q 'sizing: market SPY-USD, clip \$2500 per side (max \$2500), inventory cap \$5000, margin \$1000 at 5x (free collateral \$5377)' <<< "$OUT" || fail "sizing line"
grep -q 'presence offset 5 bp +/- 2 bp; daily stop \$5, cumulative stop \$21' <<< "$OUT" || fail "presence/stops line"
for kv in ARCUS_VOL_DRY_RUN=false ARCUS_VOL_LIVE_CONFIRM=1093-G2 ARCUS_VOL_ALLOW_ACCOUNT0=I-understand-shared-account \
          ARCUS_VOL_MARKET=SPY-USD ARCUS_VOL_CLIP_USD=2500 ARCUS_VOL_SKEW_USD=2500 ARCUS_VOL_HARD_CAP_USD=5000 \
          ARCUS_VOL_MARGIN_USD=1000 ARCUS_VOL_LEVERAGE=5 ARCUS_VOL_QUOTE_OFFSET_BPS=5 ARCUS_VOL_REPEG_BAND_BPS=2 \
          ARCUS_VOL_DAILY_STOP_USD=5 ARCUS_VOL_CUM_STOP_USD=21 ARCUS_WS_PRIVATE=1 RUST_LOG=info ARCUS_ACCOUNT_INDEX=0 \
          "ARCUS_VOL_STATE_DIR=$T/state" "ARCUS_VOL_LOCK_DIR=$T/locks"; do
    grep -qxF "$kv" "$T/env.out" || fail "runtime env missing $kv (got: $(grep "^${kv%%=*}=" "$T/env.out" || echo none))"
done
[ "$(envval ARCUS_ADDRESS)" = "0xA2C78E14DFD5586444CE4FE28FC4E36308A066D6" ] || fail "credentials not exported"
[ -d "$T/locks" ] || fail "lock dir not created"
ok "defaults: sizing, presence, stops and credentials reach the runtime"

# 2. free collateral caps the clip (free 600 -> 1300 per side; 250 -> 500, the floor)
venue "$FLAT" "$NO_ORDERS" "$(acct 900 600)"; run
[ "$RC" -eq 0 ] && [ "$(envval ARCUS_VOL_CLIP_USD)" = 1300 ] && [ "$(envval ARCUS_VOL_HARD_CAP_USD)" = 2600 ] && [ "$(envval ARCUS_VOL_MARGIN_USD)" = 520 ] || fail "free 600 -> clip 1300 cap 2600 margin 520 (got $(envval ARCUS_VOL_CLIP_USD)/$(envval ARCUS_VOL_HARD_CAP_USD)/$(envval ARCUS_VOL_MARGIN_USD))"
venue "$FLAT" "$NO_ORDERS" "$(acct 5000 250)"; run
[ "$RC" -eq 0 ] && [ "$(envval ARCUS_VOL_CLIP_USD)" = 500 ] || fail "free 250 -> clip 500 (min of equity and free)"
ok "sizing follows min(equity, free collateral)"

# 3. too little free collateral -> refused, runtime not started
venue "$FLAT" "$NO_ORDERS" "$(acct 5000 200)"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: free collateral too small" <<< "$OUT" && [ ! -f "$T/env.out" ] || fail "free 200 must refuse"
ok "refuses when free collateral cannot carry a USD 500 clip"

# 4. position open in the market -> refused; a position in another market is fine
venue '{"positions":{"5":{"marketDisplayName":"SPY-USD","size":"1.2","side":"LONG"}},"total":1}' "$NO_ORDERS" "$(acct 5557 5377)"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: SPY-USD position open" <<< "$OUT" || fail "open SPY position must refuse"
venue '{"positions":{"6":{"marketDisplayName":"HYPE-USD","size":"100","side":"LONG"}},"total":1}' "$NO_ORDERS" "$(acct 5557 4500)"; run
[ "$RC" -eq 0 ] || fail "a HYPE position must not block SPY"
ok "venue flat check is per market"

# 5. open order in the market -> refused
venue "$FLAT" '{"orders":[{"marketDisplayName":"SPY-USD","side":"BUY","price":"700","remainingSize":"1"}],"total":1}' "$(acct 5557 5377)"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: 1 open SPY-USD order" <<< "$OUT" || fail "open SPY order must refuse"
ok "refuses while an order rests in the market"

# 6. config.env overrides market and presence/stops; numbers are validated
venue "$FLAT" "$NO_ORDERS" "$(acct 5557 5377)"
printf 'MARKET=QQQ-USD\nCLIP_MAX_USD=1500\nQUOTE_OFFSET_BPS=10\nREPEG_BAND_BPS=4\nDAILY_STOP_USD=8\nCUM_STOP_USD=30\nLEVERAGE=4\n' > "$T/etc/config.env"
run
[ "$RC" -eq 0 ] || fail "config.env run exit $RC"
for kv in ARCUS_VOL_MARKET=QQQ-USD ARCUS_VOL_CLIP_USD=1500 ARCUS_VOL_HARD_CAP_USD=3000 ARCUS_VOL_MARGIN_USD=750 ARCUS_VOL_LEVERAGE=4 \
          ARCUS_VOL_QUOTE_OFFSET_BPS=10 ARCUS_VOL_REPEG_BAND_BPS=4 ARCUS_VOL_DAILY_STOP_USD=8 ARCUS_VOL_CUM_STOP_USD=30; do
    grep -qxF "$kv" "$T/env.out" || fail "config.env override missing $kv"
done
printf 'MARKET=QQQ-USD\nCLIP_MAX_USD=2500abc\n' > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: CLIP_MAX_USD must be a number" <<< "$OUT" || fail "non-numeric CLIP_MAX_USD must refuse"
# env files are parsed, never sourced: a shell metacharacter is a refusal, not a command
printf 'MARKET=QQQ-USD\nCLIP_MAX_USD=2500; touch %s/pwned\n' "$T" > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: $T/etc/config.env line 2: unexpected characters" <<< "$OUT" && [ ! -e "$T/pwned" ] || fail "shell metacharacters in config.env must refuse without executing"
printf 'MARKET=QQQ-USD\n$(touch %s/pwned2)\n' "$T" > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "expected KEY=VALUE" <<< "$OUT" && [ ! -e "$T/pwned2" ] || fail "non KEY=VALUE lines must refuse without executing"
rm -f "$T/etc/config.env"
ok "config.env overrides are applied, validated, and never executed"

# 7. sentinels: KILL_SWITCH / HALT -> exit 0 without starting (no restart loop)
venue "$FLAT" "$NO_ORDERS" "$(acct 5557 5377)"
touch "$T/state/KILL_SWITCH"; run
[ "$RC" -eq 0 ] && grep -q "NOT STARTING: KILL_SWITCH present" <<< "$OUT" && [ ! -f "$T/env.out" ] || fail "KILL_SWITCH must hold with exit 0"
rm "$T/state/KILL_SWITCH"; touch "$T/state/HALT"; run
[ "$RC" -eq 0 ] && grep -q "NOT STARTING: sticky HALT present" <<< "$OUT" && [ ! -f "$T/env.out" ] || fail "HALT must hold with exit 0"
rm "$T/state/HALT"
ok "KILL_SWITCH / HALT stop the start with exit 0"

# 8. credentials: wrong mode, missing key, wrong subaccount, missing file
write_live 644 0; run
[ "$RC" -eq 1 ] && grep -q "must be mode 600" <<< "$OUT" || fail "mode 644 must refuse"
write_live 600 1; run
[ "$RC" -eq 1 ] && grep -q "subaccount 0" <<< "$OUT" || fail "account index 1 must refuse"
printf 'ARCUS_ADDRESS=0xabc\nARCUS_ACCOUNT_INDEX=0\n' > "$T/etc/live.env"; chmod 600 "$T/etc/live.env"; run
[ "$RC" -eq 1 ] && grep -q "credentials incomplete" <<< "$OUT" || fail "incomplete credentials must refuse"
rm "$T/etc/live.env"; run
[ "$RC" -eq 1 ] && grep -q "missing $T/etc/live.env" <<< "$OUT" || fail "missing live.env must refuse"
write_live 600 0
ok "credential file checks"

# 9. venue unreachable -> refused (no start on a blind venue)
venue "$FLAT" "$NO_ORDERS" "$(acct 5557 5377)"
rm "$T/venue/positions.json"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: position read failed" <<< "$OUT" || fail "venue read failure must refuse"
ok "refuses when the venue cannot be read"

echo "all $PASS launcher checks passed"
