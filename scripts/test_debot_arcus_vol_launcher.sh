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
    printf '%s' "$MARKETS" > "$T/venue/markets.json"
}
MARKETS='{"markets":[{"marketDisplayName":"SPY-USD","markPrice":"770.5","minOrderSize":"0.001","minOrderNotional":"5"}]}'
FLAT='{"positions":{},"total":0}'
NO_ORDERS='{"orders":[],"total":0}'
acct() { printf '{"equity":"%s","freeCollateral":"%s"}' "$1" "$2"; }

# ---- fake binary + env files ----------------------------------------------
mkdir -p "$T/bin" "$T/etc" "$T/state"
cat > "$T/bin/arcus_vol_runtime" <<'SH'
#!/usr/bin/env bash
env | grep -E '^(ARCUS_|RUST_LOG|ENCRYPTED_DATA_KEY)' | sort > "$FAKE_ENV_OUT"
echo "FAKE RUNTIME STARTED"
SH
chmod 755 "$T/bin/arcus_vol_runtime"
EDK="ZmFrZS1lbmNyeXB0ZWQtZGF0YS1rZXk="
write_live() { # mode account-index [key-line]
    printf 'ARCUS_ADDRESS=0xA2C78E14DFD5586444CE4FE28FC4E36308A066D6\nARCUS_API_KEY=fake\n%s\nARCUS_ACCOUNT_INDEX=%s\n' "${3:-ARCUS_API_PRIVATE_KEY=Y2lwaGVydGV4dA==}" "$2" > "$T/etc/live.env"
    chmod "$1" "$T/etc/live.env"
}
write_live 600 0
# the host's shared file uses `export KEY="value"`
printf 'export ENCRYPTED_DATA_KEY="%s"\nexport RUST_LOG_IGNORED="debug"\n' "$EDK" > "$T/etc/secrets_common.env"
rm -f "$T/etc/config.env"

run() { # -> stdout+stderr in $OUT, exit code in $RC
    export FAKE_ENV_OUT="$T/env.out"
    rm -f "$FAKE_ENV_OUT"
    set +e
    OUT="$(DEBOT_ARCUS_VOL_ETC_DIR="$T/etc" DEBOT_ARCUS_VOL_BIN="$T/bin/arcus_vol_runtime" \
        DEBOT_ARCUS_VOL_API_BASE="http://127.0.0.1:$PORT" DEBOT_ARCUS_VOL_SECRETS_COMMON="$T/etc/secrets_common.env" STATE_DIR="$T/state" LOCK_DIR="$T/locks" \
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
[ "$(envval ARCUS_API_PRIVATE_KEY)" = "Y2lwaGVydGV4dA==" ] || fail "encrypted key not exported"
[ "$(envval ENCRYPTED_DATA_KEY)" = "$EDK" ] || fail "ENCRYPTED_DATA_KEY not taken from secrets_common (quoted export form)"
! grep -q "^ARCUS_PLAIN_API_PRIVATE_KEY=" "$T/env.out" || fail "plain key variable must not reach the runtime on the encrypted path"
! grep -q "^RUST_LOG_IGNORED=" "$T/env.out" && [ "$(envval RUST_LOG)" = info ] || fail "secrets_common must not override RUST_LOG defaults"
grep -q "signing key: encrypted" <<< "$OUT" || fail "key mode line"
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
[ "$RC" -eq 1 ] && grep -q "REFUSED: SPY-USD position open" <<< "$OUT" && grep -q "above the venue minimum" <<< "$OUT" || fail "open SPY position must refuse"
venue '{"positions":{"6":{"marketDisplayName":"HYPE-USD","size":"100","side":"LONG"}},"total":1}' "$NO_ORDERS" "$(acct 5557 4500)"; run
[ "$RC" -eq 0 ] || fail "a HYPE position must not block SPY"
ok "venue flat check is per market"

# 4b. dust position (bot-strategy#1093 live residual -0.00046 SPY = $0.35) -> carried, starts
DUST='{"positions":{"5":{"marketDisplayName":"SPY-USD","size":"-0.00046","side":"SHORT"}},"total":1}'
venue "$DUST" "$NO_ORDERS" "$(acct 5557 5377)"; run
[ "$RC" -eq 0 ] && grep -q "dust position -0.00046 (~\$0.35) carried" <<< "$OUT" && [ -f "$T/env.out" ] || fail "dust position must be carried: $OUT"
# above minOrderSize but below the $5 notional floor is still dust
venue '{"positions":{"5":{"marketDisplayName":"SPY-USD","size":"0.005","side":"LONG"}},"total":1}' "$NO_ORDERS" "$(acct 5557 5377)"; run
[ "$RC" -eq 0 ] && grep -q "dust position 0.005" <<< "$OUT" || fail "below-notional position must be carried"
# just above both minimums -> refused
venue '{"positions":{"5":{"marketDisplayName":"SPY-USD","size":"0.007","side":"LONG"}},"total":1}' "$NO_ORDERS" "$(acct 5557 5377)"; run
[ "$RC" -eq 1 ] && grep -q "above the venue minimum" <<< "$OUT" || fail "0.007 SPY (~\$5.39) must refuse"
# a market whose minOrderSize dominates the notional floor: 0.0004 BTC (~$34) is dust by SIZE only
MARKETS_SAVE="$MARKETS"
MARKETS='{"markets":[{"marketDisplayName":"SPY-USD","markPrice":"85000","minOrderSize":"0.0005","minOrderNotional":"5"}]}'
venue '{"positions":{"5":{"marketDisplayName":"SPY-USD","size":"0.0004","side":"LONG"}},"total":1}' "$NO_ORDERS" "$(acct 5557 5377)"; run
[ "$RC" -eq 0 ] && grep -q "dust position 0.0004" <<< "$OUT" || fail "below-minOrderSize position must be carried"
MARKETS="$MARKETS_SAVE"
# dust still needs the orders check: a dust position plus an open order refuses
venue "$DUST" '{"orders":[{"marketDisplayName":"SPY-USD","side":"BUY","price":"700","remainingSize":"1"}],"total":1}' "$(acct 5557 5377)"; run
[ "$RC" -eq 1 ] && grep -q "open SPY-USD order" <<< "$OUT" || fail "dust + open order must refuse"
# markets endpoint missing -> refuse with a clear line, never start blind
venue "$DUST" "$NO_ORDERS" "$(acct 5557 5377)"; rm "$T/venue/markets.json"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: markets read failed" <<< "$OUT" && [ ! -f "$T/env.out" ] || fail "missing markets must refuse"
# ...but a FLAT account does not need /v1/markets at all: it starts with the endpoint down
venue "$FLAT" "$NO_ORDERS" "$(acct 5557 5377)"; rm "$T/venue/markets.json"; run
[ "$RC" -eq 0 ] && grep -q "venue check: SPY-USD flat" <<< "$OUT" && [ -f "$T/env.out" ] || fail "flat account must start without /v1/markets: $OUT"
# market row missing from /v1/markets -> refuse
venue "$DUST" "$NO_ORDERS" "$(acct 5557 5377)"; printf '{"markets":[]}' > "$T/venue/markets.json"; run
[ "$RC" -eq 1 ] && grep -q "not found in /v1/markets" <<< "$OUT" || fail "unknown market row must refuse"
ok "a dust position below the venue minimum is carried; anything above refuses; never judged blind"

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
# allow-lists: config.env cannot move the account or redirect the venue; live.env cannot carry endpoints
printf 'MARKET=QQQ-USD\nARCUS_ACCOUNT_INDEX=1\n' > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: $T/etc/config.env line 2: key ARCUS_ACCOUNT_INDEX is not accepted here" <<< "$OUT" && [ ! -f "$T/env.out" ] || fail "config.env must not set ARCUS_ACCOUNT_INDEX"
printf 'ARCUS_WEBSOCKET_ENDPOINT=wss://evil.example/ws\n' > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "key ARCUS_WEBSOCKET_ENDPOINT is not accepted here" <<< "$OUT" || fail "config.env must refuse unknown keys"
# session offset: both keys pass through; unset = not exported; half a pair or a non-number refuses
printf 'QUOTE_OFFSET_BPS=2\nREPEG_BAND_BPS=1\nSESSION_OFFSET_BPS=5\nSESSION_BAND_BPS=2\n' > "$T/etc/config.env"; run
[ "$RC" -eq 0 ] && grep -qxF ARCUS_VOL_SESSION_OFFSET_BPS=5 "$T/env.out" && grep -qxF ARCUS_VOL_SESSION_BAND_BPS=2 "$T/env.out" \
    && grep -qxF ARCUS_VOL_QUOTE_OFFSET_BPS=2 "$T/env.out" && grep -q "in session 5 bp +/- 2 bp" <<< "$OUT" || fail "session offset must reach the runtime"
printf 'QUOTE_OFFSET_BPS=2\nREPEG_BAND_BPS=1\n' > "$T/etc/config.env"; run
[ "$RC" -eq 0 ] && ! grep -q '^ARCUS_VOL_SESSION_' "$T/env.out" || fail "no session keys must be exported when unset"
printf 'SESSION_OFFSET_BPS=5\n' > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "set both SESSION_OFFSET_BPS and SESSION_BAND_BPS" <<< "$OUT" || fail "half a session pair must refuse"
printf 'SESSION_OFFSET_BPS=5\nSESSION_BAND_BPS=two\n' > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "SESSION_BAND_BPS must be a number" <<< "$OUT" || fail "non-numeric session band must refuse"
# per-position stop: passes through when set, not exported when unset, a non-number refuses
printf 'POSITION_STOP_BPS=25\n' > "$T/etc/config.env"; run
[ "$RC" -eq 0 ] && grep -qxF ARCUS_VOL_POSITION_STOP_BPS=25 "$T/env.out" || fail "position stop must reach the runtime"
printf 'QUOTE_OFFSET_BPS=2\nREPEG_BAND_BPS=1\n' > "$T/etc/config.env"; run
[ "$RC" -eq 0 ] && ! grep -q '^ARCUS_VOL_POSITION_STOP_BPS=' "$T/env.out" || fail "no position stop must be exported when unset"
printf 'POSITION_STOP_BPS=25bp\n' > "$T/etc/config.env"; run
[ "$RC" -eq 1 ] && grep -q "POSITION_STOP_BPS must be a number" <<< "$OUT" || fail "non-numeric position stop must refuse"
rm -f "$T/etc/config.env"
printf 'ARCUS_ADDRESS=0xA2C7\nARCUS_API_KEY=fake\nARCUS_API_PRIVATE_KEY=Y2lwaGVydGV4dA==\nARCUS_ACCOUNT_INDEX=0\nARCUS_REST_ENDPOINT=https://evil.example\n' > "$T/etc/live.env"; chmod 600 "$T/etc/live.env"; run
[ "$RC" -eq 1 ] && grep -q "live.env line 5: key ARCUS_REST_ENDPOINT is not accepted here" <<< "$OUT" || fail "live.env must refuse endpoint keys"
write_live 600 0
ok "config.env overrides are applied, validated, never executed, and allow-listed (no account / endpoint keys)"

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

# 8b. signing-key modes
write_live 600 0 "ARCUS_PLAIN_API_PRIVATE_KEY=deadbeef"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: live.env carries a PLAIN signing key; encrypt it with: sudo /opt/debot/scripts/arcus_vol_encrypt_key.sh" <<< "$OUT" && [ ! -f "$T/env.out" ] || fail "plain key without ALLOW_PLAIN_KEY must refuse"
printf 'ALLOW_PLAIN_KEY=1\n' > "$T/etc/config.env"; run
[ "$RC" -eq 0 ] && [ "$(envval ARCUS_PLAIN_API_PRIVATE_KEY)" = deadbeef ] && ! grep -q "^ENCRYPTED_DATA_KEY=" "$T/env.out" && grep -q "signing key: PLAIN (ALLOW_PLAIN_KEY=1" <<< "$OUT" || fail "plain key with ALLOW_PLAIN_KEY=1 must start without the encrypted variables"
rm "$T/etc/config.env"
write_live 600 0 "ARCUS_API_PRIVATE_KEY=Y2lwaGVydGV4dA=="; mv "$T/etc/secrets_common.env" "$T/etc/sc.bak"; run
[ "$RC" -eq 1 ] && grep -q "ENCRYPTED_DATA_KEY is neither in" <<< "$OUT" || fail "encrypted key without any ENCRYPTED_DATA_KEY must refuse"
mv "$T/etc/sc.bak" "$T/etc/secrets_common.env"
printf 'ARCUS_ADDRESS=0xA2C7\nARCUS_API_KEY=fake\nARCUS_API_PRIVATE_KEY=Y2lwaGVydGV4dA==\nENCRYPTED_DATA_KEY=bGl2ZS1lbnYtZWRr\nARCUS_ACCOUNT_INDEX=0\n' > "$T/etc/live.env"; chmod 600 "$T/etc/live.env"; run
[ "$RC" -eq 0 ] && [ "$(envval ENCRYPTED_DATA_KEY)" = "bGl2ZS1lbnYtZWRr" ] || fail "ENCRYPTED_DATA_KEY in live.env must win over secrets_common"
write_live 600 0 "# no signing key line"; run
[ "$RC" -eq 1 ] && grep -q "no signing key in" <<< "$OUT" || fail "no key at all must refuse"
write_live 600 0
ok "signing key: encrypted by default, plain only with ALLOW_PLAIN_KEY=1, never both"

# 9. venue unreachable -> refused (no start on a blind venue)
venue "$FLAT" "$NO_ORDERS" "$(acct 5557 5377)"
rm "$T/venue/positions.json"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: position read failed" <<< "$OUT" || fail "venue read failure must refuse"
ok "refuses when the venue cannot be read"

echo "all $PASS launcher checks passed"
