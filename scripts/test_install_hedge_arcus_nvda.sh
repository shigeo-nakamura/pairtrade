#!/usr/bin/env bash
# Tests for deploy/xvenue_hedge_arcus/install_hedge_arcus_nvda.sh (bot-strategy#1293).
# Runs in a temp root with a fake systemctl/journalctl; never touches the real host
# and never uses real credentials (the "ciphertexts" are random 64-byte blobs).
#   bash scripts/test_install_hedge_arcus_nvda.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
KIT="$HERE/deploy/xvenue_hedge_arcus"
INST="$KIT/install_hedge_arcus_nvda.sh"
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
fail=0; ok() { echo "ok   $*"; }; bad() { echo "FAIL $*"; fail=1; }

A2C7_MIXED=0xA2C78E14dfD5586444ce4FE28Fc4E36308A066d6
UNIT=debot-xvenue-hedge-arcus-nvda
blob() { head -c 64 /dev/urandom | base64 -w0; }
A_PUB=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'); A_PRIV=$(blob); L_PUB=$(blob); L_PRIV=$(blob)

# ---- fake systemd -------------------------------------------------------------
ST="$T/st"; mkdir -p "$ST/act" "$ST/en" "$T/bin"
cat > "$T/bin/systemctl" <<SH
#!/bin/bash
echo "\$*" >> "$ST/calls"
u=\${@: -1}
case "\$1" in
  is-active) s=\$(cat "$ST/act/\$u" 2>/dev/null || echo inactive); [ "\$2" = --quiet ] || echo "\$s"; [ "\$s" = active ] ;;
  is-enabled) s=\$(cat "$ST/en/\$u" 2>/dev/null || echo disabled); echo "\$s"; [ "\$s" = enabled ] ;;
  enable) echo enabled > "$ST/en/\$u" ;;
  daemon-reload) ;;
  start) echo active > "$ST/act/\$u"; [ -x "$ST/onstart" ] && "$ST/onstart" ;;
  stop) echo inactive > "$ST/act/\$u" ;;
  show) echo 4242 ;;
  *) echo "unexpected systemctl \$*" >> "$ST/bad"; exit 1 ;;
esac
SH
cat > "$T/bin/journalctl" <<SH
#!/bin/bash
cat "$ST/journal.txt" 2>/dev/null
SH
chmod 755 "$T/bin/systemctl" "$T/bin/journalctl"
GOOD_JOURNAL='Nov 12 [CONFIG] bot=xvenue_hedge_holder dry_run=true symbols=NVDA:5.000/3.000 long=arcus:arcus short=lighter:core target=$20000 max=$22000 clip=$2500 slip=5bps net_tol=$3000 liq_guard=8% max_lev=4.5x fill_waits=[3, 6, 10] fp=abc
Nov 12 [STARTUP] NVDA mark=167.39 size_decimals=3 min_qty=0.025
Nov 12 [STARTUP] mode=Off books=1'

# ---- fake host root ------------------------------------------------------------
setup() {   # fresh root in $R; onstart writes a clean DRY_RUN status.json
  R="$T/root"; rm -rf "$R" "$ST/act"/* "$ST/en"/* "$ST/calls" "$ST/bad"; mkdir -p "$R/opt/debot-hedge-arcus/bin" "$R/opt/debot-hedge-arcus/lib" "$R/opt/debot/scripts" "$R/etc/systemd/system" "$R/etc/debot-arcus-vol" "$R/etc/debot-arcus-vol-gld"
  printf 'BINARY-v4.7.45\n' > "$R/opt/debot-hedge-arcus/bin/xvenue_hedge_holder"; chmod 755 "$R/opt/debot-hedge-arcus/bin/xvenue_hedge_holder"
  printf 'LIBSIGNER\n' > "$R/opt/debot-hedge-arcus/lib/libsigner.so"; echo 44787b > "$R/opt/debot-hedge-arcus/PAIRTRADE_SHA"
  printf 'export ENCRYPTED_DATA_KEY="RURLMQ=="\n' > "$R/opt/debot/scripts/debot_secrets_common.env"
  printf 'export RUST_LOG="info"\n' > "$R/opt/debot/scripts/debot.env"
  printf 'export HEDGE_SYMBOLS="AAPL:5/3"\nexport LIGHTER_ACCOUNT_INDEX_CORE="281474976574527"\n' > "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.env"; chmod 600 "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.env"
  printf 'ARCUS_ADDRESS=%s\nARCUS_ACCOUNT_INDEX=0\nARCUS_API_KEY=%s\nARCUS_API_PRIVATE_KEY=%s\nENCRYPTED_DATA_KEY=RURLMQ==\n' "$A2C7_MIXED" "$A_PUB" "$A_PRIV" > "$R/etc/debot-arcus-vol/live.env"
  chmod 600 "$R/etc/debot-arcus-vol/live.env"
  printf 'LIGHTER_ACCOUNT_INDEX_CORE=281474976624889\nLIGHTER_API_KEY_INDEX_CORE=3\nLIGHTER_PUBLIC_API_KEY_CORE=%s\nLIGHTER_PRIVATE_API_KEY_CORE=%s\n' "$L_PUB" "$L_PRIV" > "$T/fragment.env"; chmod 600 "$T/fragment.env"
  echo active > "$ST/act/debot-xvenue-hedge-arcus"
  printf '%s\n' "$GOOD_JOURNAL" > "$ST/journal.txt"
  cat > "$ST/onstart" <<SH
#!/bin/bash
mkdir -p "$R/opt/debot-hedge-arcus-nvda/hedge"
echo '{"updated_at":"2026-11-12T22:00:00Z","dry_run":true,"hedge_holder":{"mode":"Off","halted":false,"legs":{"long":{"exchange":"arcus","instance":"arcus"},"short":{"exchange":"lighter","instance":"core"}},"books":{"NVDA":{"mode":"Off"}}}}' > "$R/opt/debot-hedge-arcus-nvda/hedge/status.json"
SH
  chmod 755 "$ST/onstart"
  export HEDGE_ARCUS_ROOT="$R" HEDGE_ARCUS_SYSTEMCTL="$T/bin/systemctl" HEDGE_ARCUS_JOURNALCTL="$T/bin/journalctl" HEDGE_ARCUS_TEST_NO_CHOWN=1 HEDGE_ARCUS_WAIT_SECS=0
}
ENVF() { echo "$R/opt/debot/scripts/$UNIT.env"; }
kv() { sed -nE "s/^(export )?$2=\"?([^\"]*)\"?$/\2/p" "$1" | head -1; }
nothing_written() { [ ! -e "$R/opt/debot-hedge-arcus-nvda" ] && [ ! -e "$(ENVF)" ] && [ ! -e "$R/etc/systemd/system/$UNIT.service" ] && [ ! -e "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.sh" ]; }

# 1. verify on a fresh host: drift, exit 1, nothing written, systemd only queried
setup; out=$(bash "$INST" --verify 2>&1); rc=$?
[ $rc = 1 ] && ok "verify on a fresh host exits 1" || bad "verify rc=$rc: $out"
echo "$out" | grep -q "DRIFT missing $R/etc/systemd/system/$UNIT.service" && echo "$out" | grep -q "DRIFT missing $(ENVF)" && ok "verify reports missing unit + env" || bad "verify drift lines: $out"
nothing_written && ok "verify writes nothing" || bad "verify wrote files"
grep -qE '^(enable|start|stop|daemon-reload)' "$ST/calls" && bad "verify changed systemd state" || ok "verify only queries systemd"

# 2. argument errors
out=$(bash "$INST" --apply 2>&1); rc=$?; [ $rc = 2 ] && ok "--apply without --fragment exits 2" || bad "apply w/o fragment rc=$rc"
out=$(bash "$INST" 2>&1); rc=$?; [ $rc = 2 ] && ok "no mode exits 2" || bad "no mode rc=$rc"

# 3. preflight refusals (nothing written)
setup; echo enabled > "$ST/en/debot-arcus-vol"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "debot-arcus-vol is enabled" && nothing_written && ok "refuses while the SPY volume bot is enabled" || bad "volume bot enabled: rc=$rc $out"
setup; echo active > "$ST/act/debot-arcus-vol-gld"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "debot-arcus-vol-gld is ACTIVE" && nothing_written && ok "refuses while the GLD volume bot is active" || bad "volume bot active: rc=$rc"
setup; sed -i 's/^ENCRYPTED_DATA_KEY=.*/ENCRYPTED_DATA_KEY=T1RIRVI=/' "$R/etc/debot-arcus-vol/live.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "ENCRYPTED_DATA_KEY differs" && nothing_written && ok "refuses a live.env under another data key" || bad "data key differs: rc=$rc $out"
setup; sed -i '/^ENCRYPTED_DATA_KEY=/d' "$R/etc/debot-arcus-vol/live.env"; out=$(bash "$INST" --verify 2>&1)
echo "$out" | grep -q "live.env data key: none" && ok "live.env without its own data key is accepted (common one)" || bad "no-own-data-key path: $out"
setup; sed -i 's/^ARCUS_ADDRESS=.*/ARCUS_ADDRESS=0x812B6A6da8E0dF1fBCA7939ae32089Cf85c5DF05/' "$R/etc/debot-arcus-vol/live.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "ARCUS_ADDRESS is not wallet" && nothing_written && ok "refuses a live.env of another wallet" || bad "wrong wallet: rc=$rc"
setup; sed -i "s/^ARCUS_API_PRIVATE_KEY=.*/ARCUS_PLAIN_API_PRIVATE_KEY=deadbeef/" "$R/etc/debot-arcus-vol/live.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "not ciphertext-shaped" && nothing_written && ok "refuses a live.env without the ciphertext (plaintext only)" || bad "plain live.env: rc=$rc $out"
setup; chmod 644 "$R/etc/debot-arcus-vol/live.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "not 600" && ok "refuses a world-readable live.env" || bad "live.env mode: rc=$rc"
setup; printf 'export LIGHTER_ACCOUNT_INDEX_CORE="281474976624889"\n' > "$R/opt/debot/scripts/debot-other.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "already used by $R/opt/debot/scripts/debot-other.env" && nothing_written && ok "refuses a Core sub-account another env uses" || bad "core reuse: rc=$rc $out"
setup; echo 'LIGHTER_PLAIN_PRIVATE_API_KEY_CORE=abc' >> "$T/fragment.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "fragment must hold exactly" && nothing_written && ok "refuses a fragment with extra keys" || bad "fragment extra key: rc=$rc $out"
setup; sed -i 's/^LIGHTER_ACCOUNT_INDEX_CORE=.*/LIGHTER_ACCOUNT_INDEX_CORE=281474976574527/' "$T/fragment.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "fragment LIGHTER_ACCOUNT_INDEX_CORE is not" && ok "refuses a fragment for another sub-account" || bad "fragment account: rc=$rc"
setup; sed -i "s/^LIGHTER_PRIVATE_API_KEY_CORE=.*/LIGHTER_PRIVATE_API_KEY_CORE=$(head -c 40 /dev/urandom | od -An -tx1 | tr -d ' \n')/" "$T/fragment.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "LIGHTER_PRIVATE_API_KEY_CORE is not ciphertext-shaped" && nothing_written && ok "refuses a fragment with a hex (plaintext-shaped) key" || bad "fragment hex key: rc=$rc $out"
setup; chmod 644 "$T/fragment.env"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "fragment must be mode 600" && ok "refuses a world-readable fragment" || bad "fragment mode: rc=$rc"
setup; rm "$R/opt/debot-hedge-arcus/lib/libsigner.so"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "AAPL instance file missing" && nothing_written && ok "refuses without the AAPL instance's libsigner" || bad "missing libsigner: rc=$rc"

# 4. apply on a clean host
setup; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 0 ] && echo "$out" | grep -q "INSTALL OK" && ok "apply exits 0 with INSTALL OK" || bad "apply rc=$rc: $out"
B="$R/opt/debot-hedge-arcus-nvda"
cmp -s "$B/bin/xvenue_hedge_holder" "$R/opt/debot-hedge-arcus/bin/xvenue_hedge_holder" && [ "$(stat -c %a "$B/bin/xvenue_hedge_holder")" = 755 ] \
  && cmp -s "$B/lib/libsigner.so" "$R/opt/debot-hedge-arcus/lib/libsigner.so" && [ -f "$B/PAIRTRADE_SHA" ] && ok "binary + libsigner copied from the AAPL instance" || bad "binary/lib copy"
cmp -s "$KIT/debot-xvenue-hedge-arcus.sh" "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.sh" && [ "$(stat -c %a "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.sh")" = 755 ] && ok "shared launcher installed 755" || bad "launcher"
cmp -s "$KIT/$UNIT.service" "$R/etc/systemd/system/$UNIT.service" && [ "$(stat -c %a "$R/etc/systemd/system/$UNIT.service")" = 644 ] && ok "unit installed 644" || bad "unit"
E=$(ENVF); [ -f "$E" ] && [ "$(stat -c %a "$E")" = 600 ] && ok "env written 0600" || bad "env mode/missing"
grep -qE '^export [A-Z0-9_]+="[^"]*<' "$E" && bad "env still has a placeholder: $(grep -E '^export [A-Z0-9_]+="[^"]*<' "$E" | cut -d= -f1)" || ok "env has no placeholder values"
[ "$(kv "$E" ARCUS_API_KEY_ARCUS)" = "$A_PUB" ] && [ "$(kv "$E" ARCUS_API_PRIVATE_KEY_ARCUS)" = "$A_PRIV" ] && ok "Arcus key lines copied from live.env" || bad "Arcus key lines"
[ "$(kv "$E" LIGHTER_API_KEY_INDEX_CORE)" = 3 ] && [ "$(kv "$E" LIGHTER_PUBLIC_API_KEY_CORE)" = "$L_PUB" ] && [ "$(kv "$E" LIGHTER_PRIVATE_API_KEY_CORE)" = "$L_PRIV" ] && ok "Lighter lines taken from the fragment" || bad "Lighter lines"
[ "$(kv "$E" LIGHTER_ACCOUNT_INDEX_CORE)" = 281474976624889 ] && [ "$(kv "$E" ARCUS_ADDRESS_ARCUS)" = 0xa2c78e14dfd5586444ce4fe28fc4e36308a066d6 ] && [ "$(kv "$E" HEDGE_SYMBOLS)" = "NVDA:5/3" ] && [ "$(kv "$E" HEDGE_DRY_RUN)" = true ] && ok "env pins account, wallet, NVDA, DRY_RUN" || bad "env pins"
grep -qE '^export HEDGE_(ARCUS_)?LIVE_CONFIRM=' "$E" && bad "env has a live confirm token" || ok "live confirm tokens stay commented"
grep -q PLAIN <(grep -E '^export' "$E") && bad "env has a PLAIN variable" || ok "no plaintext key variable"
grep -q '^daemon-reload' "$ST/calls" && grep -q "^enable $UNIT" "$ST/calls" && grep -q "^start --no-block $UNIT" "$ST/calls" && ok "daemon-reload, enable, start --no-block" || bad "systemd calls: $(cat "$ST/calls")"
grep -E '^(stop|restart|start) ' "$ST/calls" | grep -v "$UNIT" | grep -q . && bad "touched another unit" || ok "no other unit started/stopped"
[ ! -e "$ST/bad" ] && ok "no unexpected systemctl call" || bad "unexpected systemctl: $(cat "$ST/bad")"
echo "$out" | grep -q "debot-xvenue-hedge-arcus untouched (pid 4242)" && ok "AAPL instance PID checked" || bad "AAPL pid check: $out"
grep -qF "$A_PRIV" <<<"$out" || grep -qF "$L_PRIV" <<<"$out" || grep -qF "$L_PUB" <<<"$out" && bad "a ciphertext was printed" || ok "no ciphertext in the output"

# 5. verify after apply is clean; re-apply with the unit active updates nothing and never restarts
sha_before=$(sha256sum "$E" | cut -c1-16)
out=$(bash "$INST" --verify 2>&1); rc=$?; [ $rc = 0 ] && echo "$out" | grep -q "VERIFY OK" && ok "verify after apply exits 0" || bad "verify after apply rc=$rc: $out"
: > "$ST/calls"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 0 ] && echo "$out" | grep -q "was already active and was not restarted" && ok "re-apply on an active unit does not restart" || bad "re-apply rc=$rc: $out"
[ "$(sha256sum "$E" | cut -c1-16)" = "$sha_before" ] && ok "re-apply keeps the env (keys written once)" || bad "re-apply rewrote the env"
grep -qE '^(start|stop|restart)' "$ST/calls" && bad "re-apply started/stopped something" || ok "re-apply: no start/stop"
# a stopped unit whose kept env is already LIVE is never started by a re-apply (Codex P1 on pairtrade#426)
echo inactive > "$ST/act/$UNIT"
sed -i 's/^export HEDGE_DRY_RUN=.*/export HEDGE_DRY_RUN="false"/; s/^# export HEDGE_LIVE_CONFIRM=/export HEDGE_LIVE_CONFIRM=/; s/^# export HEDGE_ARCUS_LIVE_CONFIRM=/export HEDGE_ARCUS_LIVE_CONFIRM=/' "$E"
: > "$ST/calls"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 0 ] && echo "$out" | grep -q "is a LIVE configuration" && echo "$out" | grep -q "NOT started" && ok "re-apply never starts a stopped LIVE instance" || bad "live env re-apply: rc=$rc $out"
grep -qE '^(start|stop|restart)' "$ST/calls" && bad "live env re-apply touched the unit: $(grep -E '^(start|stop|restart)' "$ST/calls")" || ok "live env re-apply: no start/stop call"
[ "$(cat "$ST/act/$UNIT")" = inactive ] && ok "live instance stays stopped" || bad "live instance was started"
# a kept DRY_RUN env with only one token uncommented still counts as live
sed -i 's/^export HEDGE_DRY_RUN=.*/export HEDGE_DRY_RUN="true"/; s/^export HEDGE_LIVE_CONFIRM=/# export HEDGE_LIVE_CONFIRM=/' "$E"
: > "$ST/calls"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 0 ] && echo "$out" | grep -q "NOT started" && ! grep -qE '^start' "$ST/calls" && ok "a lone confirm token also blocks the auto-start" || bad "lone token: rc=$rc $(grep -E '^start' "$ST/calls")"
# back to a clean DRY_RUN env: a stopped DRY_RUN instance IS started again by re-apply
sed -i 's/^export HEDGE_ARCUS_LIVE_CONFIRM=/# export HEDGE_ARCUS_LIVE_CONFIRM=/' "$E"
: > "$ST/calls"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 0 ] && echo "$out" | grep -q "INSTALL OK" && grep -q "^start --no-block $UNIT" "$ST/calls" && ok "re-apply starts a stopped DRY_RUN instance" || bad "dry re-apply: rc=$rc $out"
# a changed launcher in the kit → re-apply installs it (the AAPL instance picks it up at its own restart)
printf '# drifted\n' >> "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.sh"
out=$(bash "$INST" --verify 2>&1); rc=$?; [ $rc = 1 ] && echo "$out" | grep -q "DRIFT content $R/opt/debot/scripts/debot-xvenue-hedge-arcus.sh" && ok "verify reports a drifted launcher" || bad "launcher drift: $out"
out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); cmp -s "$KIT/debot-xvenue-hedge-arcus.sh" "$R/opt/debot/scripts/debot-xvenue-hedge-arcus.sh" && ok "re-apply restores the launcher" || bad "launcher not restored"

# 6. post-check failure: no [CONFIG] line → unit stopped again, exit 1
setup; printf 'Nov 12 [STARTUP] mode=Off books=1\n' > "$ST/journal.txt"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "POSTCHECK FAIL no \[CONFIG\]" && grep -q "^stop $UNIT" "$ST/calls" && [ "$(cat "$ST/act/$UNIT")" = inactive ] && ok "missing [CONFIG] → postcheck fails and the unit is stopped" || bad "postcheck config: rc=$rc $out"
setup; cat > "$ST/onstart" <<SH
#!/bin/bash
mkdir -p "$R/opt/debot-hedge-arcus-nvda/hedge"
echo '{"dry_run":false,"hedge_holder":{"mode":"Off","halted":false,"legs":{"long":{"exchange":"arcus"},"short":{"exchange":"lighter"}},"books":{}}}' > "$R/opt/debot-hedge-arcus-nvda/hedge/status.json"
SH
chmod 755 "$ST/onstart"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "status/events check" && grep -q "^stop $UNIT" "$ST/calls" && ok "status.json dry_run=false → postcheck fails, unit stopped" || bad "postcheck status: rc=$rc $out"
setup; printf '%s\nNov 12 [ERROR] init short-leg: boom\n' "$GOOD_JOURNAL" > "$ST/journal.txt"; out=$(bash "$INST" --apply --fragment "$T/fragment.env" 2>&1); rc=$?
[ $rc = 1 ] && echo "$out" | grep -q "errors in the journal" && ok "journal ERROR → postcheck fails" || bad "postcheck error: rc=$rc"

[ $fail = 0 ] && echo "ALL OK" || { echo "FAILURES"; exit 1; }
