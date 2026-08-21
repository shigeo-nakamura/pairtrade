#!/usr/bin/env bash
# Regression test for check_config_drift.sh's --round-json per-variant
# max_leverage handling (bot-strategy#814 PR review):
#   1. round.json can declare a per-variant `max_leverage` override (falling
#      back to the top-level value for a variant that doesn't declare one),
#      matching how the running process itself resolves leverage.
#   2. The round.json field parser uses '|' as its internal record
#      separator, not a tab — an empty cell adjacent to a tab (e.g. the
#      common case of `use_frozen_beta_exit_z` left undeclared) used to be
#      silently swallowed by bash's `read`, which treats a run of IFS
#      *whitespace* characters — tab is always one, regardless of the
#      current IFS value — as a single delimiter. That shifted every field
#      after the empty one left by one, so `equity_reference_usd` (and now
#      `max_leverage`) parsed as empty and the assertion silently no-opped
#      instead of verifying anything.
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
HTTP_PID=""
cleanup() {
  [ -n "$HTTP_PID" ] && kill "$HTTP_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

CONFIG="$WORK/config.yaml"
printf 'dummy pairtrade config content for drift test\n' > "$CONFIG"
DISK_SHA=$(sha256sum "$CONFIG" | cut -c1-12)
touch -d '2020-01-01' "$CONFIG"

FAKE_BIN="$WORK/bin"
mkdir -p "$FAKE_BIN"
cat > "$FAKE_BIN/systemctl" <<'EOF'
#!/usr/bin/env bash
if [ "$1" = show ] && [ "$2" = -p ] && [ "$3" = ExecMainStartTimestamp ]; then
  echo "Wed 2026-08-18 00:00:00 UTC"
  exit 0
fi
exit 1
EOF
chmod +x "$FAKE_BIN/systemctl"

ROUND_JSON="$WORK/round.json"
cat > "$ROUND_JSON" <<'EOF'
{
  "max_leverage": 20,
  "variants": {
    "freq": {"force_close_secs": 3600, "exit_z": 0.2, "stop_loss_z": 4.0, "equity_reference_usd": 2000, "max_leverage": 30},
    "b": {"force_close_secs": 10800, "exit_z": 0.2, "stop_loss_z": 8.0, "equity_reference_usd": 4000, "max_leverage": 50, "sizing_beta_floor": 0.6, "exit_on_sizing_beta_floor": true},
    "c": {"force_close_secs": 3600, "exit_z": 0.2, "stop_loss_z": 4.0, "equity_reference_usd": 1000}
  }
}
EOF

write_metrics() {
  # $1 = observed max_leverage for variant b (30/50/20 = correct scenario;
  # anything else must be reported as drift).
  # $2 = observed exit_on_sizing_beta_floor for variant b (default 1 = matches
  # round.json; bot-strategy#824 review — the new field must be asserted too).
  local b_mlev="$1"
  local b_eobf="${2:-1}"
  cat > "$WORK/metrics.txt" <<METRICS
pairtrade_config_file_info{variant="freq",sha="$DISK_SHA"} 1
pairtrade_effective_force_close_secs{variant="freq"} 3600
pairtrade_effective_force_close_secs{variant="b"} 10800
pairtrade_effective_force_close_secs{variant="c"} 3600
pairtrade_effective_exit_z{variant="freq"} 0.2
pairtrade_effective_exit_z{variant="b"} 0.2
pairtrade_effective_exit_z{variant="c"} 0.2
pairtrade_effective_stop_loss_z{variant="freq"} 4.0
pairtrade_effective_stop_loss_z{variant="b"} 8.0
pairtrade_effective_stop_loss_z{variant="c"} 4.0
pairtrade_equity_reference_usd{variant="freq"} 2000
pairtrade_equity_reference_usd{variant="b"} 4000
pairtrade_equity_reference_usd{variant="c"} 1000
pairtrade_max_leverage_config{variant="freq"} 30
pairtrade_max_leverage_config{variant="b"} $b_mlev
pairtrade_max_leverage_config{variant="c"} 20
pairtrade_effective_sizing_beta_floor{variant="freq"} 0
pairtrade_effective_sizing_beta_floor{variant="b"} 0.6
pairtrade_effective_sizing_beta_floor{variant="c"} 0
pairtrade_effective_exit_on_sizing_beta_floor{variant="freq"} 0
pairtrade_effective_exit_on_sizing_beta_floor{variant="b"} $b_eobf
pairtrade_effective_exit_on_sizing_beta_floor{variant="c"} 0
METRICS
}

PORT=$((20000 + RANDOM % 20000))
serve_metrics() {
  (cd "$WORK" && python3 -c "
import http.server, socketserver, sys
class H(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(open('metrics.txt', 'rb').read())
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(('127.0.0.1', $PORT), H) as httpd:
    httpd.serve_forever()
" ) &
  HTTP_PID=$!
  for _ in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:$PORT/metrics.txt" && return 0
    sleep 0.1
  done
  echo "fake metrics server never came up" >&2
  exit 1
}

run_check() {
  PATH="$FAKE_BIN:$PATH" "$REPO_ROOT/scripts/check_config_drift.sh" \
    --service dummy \
    --config "$CONFIG" \
    --metrics "http://127.0.0.1:$PORT/metrics.txt" \
    --round-json "$ROUND_JSON" \
    --quiet
}

fail() { echo "FAIL: $1" >&2; exit 1; }

# --- Case 1: per-variant max_leverage matches (freq=30, b=50, c inherits 20) ---
write_metrics 50
serve_metrics
if ! out=$(run_check 2>&1); then
  echo "$out" >&2
  fail "expected no drift when gauges match the per-variant round.json max_leverage (freq=30/b=50/c=20), got exit $?"
fi
kill "$HTTP_PID" 2>/dev/null || true
wait "$HTTP_PID" 2>/dev/null || true
HTTP_PID=""
echo "PASS: correct per-variant max_leverage (freq=30/b=50/c=20) reports no drift"

# --- Case 2: b's override was never applied (still at the top-level 20) -------
write_metrics 20
serve_metrics
set +e
out=$(run_check 2>&1)
rc=$?
set -e
kill "$HTTP_PID" 2>/dev/null || true
wait "$HTTP_PID" 2>/dev/null || true
HTTP_PID=""
[ "$rc" -eq 2 ] || fail "expected exit 2 (drift) when variant b's max_leverage gauge (20) doesn't match its round.json override (50), got exit $rc: $out"
echo "$out" | grep -q 'variant b effective max_leverage=20' \
  || fail "drift message did not name variant b's mismatched max_leverage; got: $out"
echo "PASS: variant b running at the wrong (non-overridden) max_leverage is correctly flagged as drift"

# --- Case 3: b's exit_on_sizing_beta_floor gauge disagrees with round.json ----
write_metrics 50 0
serve_metrics
set +e
out=$(run_check 2>&1)
rc=$?
set -e
kill "$HTTP_PID" 2>/dev/null || true
wait "$HTTP_PID" 2>/dev/null || true
HTTP_PID=""
[ "$rc" -eq 2 ] || fail "expected exit 2 (drift) when variant b's exit_on_sizing_beta_floor gauge (0) doesn't match round.json (1), got exit $rc: $out"
echo "$out" | grep -q 'variant b effective exit_on_sizing_beta_floor=0' \
  || fail "drift message did not name variant b's mismatched exit_on_sizing_beta_floor; got: $out"
echo "PASS: variant b running with the beta-floor exit disabled when round.json enables it is correctly flagged as drift"

echo "ALL CHECKS PASSED"
