#!/usr/bin/env bash
# Stub test for deploy/xvenue_hedge_arcus/debot-xvenue-hedge-arcus.sh (bot-strategy#1293):
# fake binaries that dump their environment, fake env files. Exercises the
# per-instance table (AAPL / nvda) and every refusal. Never touches a host.
#   bash scripts/test_xvenue_hedge_arcus_launcher.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
L="$HERE/deploy/xvenue_hedge_arcus/debot-xvenue-hedge-arcus.sh"
T=$(mktemp -d); trap 'rm -rf "${T:?}"' EXIT
fail=0; ok() { echo "ok   $*"; }; bad() { echo "FAIL $*"; fail=1; }

mkdir -p "$T/env" "$T/opt/debot-hedge-arcus/bin" "$T/opt/debot-hedge-arcus/lib" "$T/opt/debot-hedge-arcus-nvda/bin"
for d in debot-hedge-arcus debot-hedge-arcus-nvda; do
  cat > "$T/opt/$d/bin/xvenue_hedge_holder" <<SH
#!/usr/bin/env bash
echo "FAKE $d"
env | grep -E '^(HEDGE_|LIGHTER_|ARCUS_|REST_ENDPOINT|WEB_SOCKET_ENDPOINT|LD_LIBRARY_PATH|LIGHTER_GO_PATH)' | sort > "$T/env.out"
SH
  chmod 755 "$T/opt/$d/bin/xvenue_hedge_holder"
done
: > "$T/opt/debot-hedge-arcus/lib/libsigner.so"     # only the AAPL instance has a lib/ here
printf 'export ENCRYPTED_DATA_KEY="ZmFrZQ=="\n' > "$T/env/debot_secrets_common.env"
# debot.env carries other bots' UNSUFFIXED venue defaults: the launcher must drop them.
printf 'export LIGHTER_ACCOUNT_INDEX="3209"\nexport ARCUS_ADDRESS="0x0000000000000000000000000000000000000001"\nexport HEDGE_BASE_DIR="/opt/debot-hedge-holder"\nexport RUST_LOG="info"\n' > "$T/env/debot.env"

A812=0x812B6A6da8E0dF1fBCA7939ae32089Cf85c5DF05
A2C7=0xa2c78e14dfd5586444ce4fe28fc4e36308a066d6
S3=s3://debot-dashboard/debot/status/robinhood-lighter
# write_env UNIT WALLET CORE SYMBOLS STATUS [extra lines...]
write_env() {
  local unit=$1 wallet=$2 core=$3 syms=$4 status=$5; shift 5
  { printf 'export HEDGE_DRY_RUN="true"\nexport HEDGE_LONG_VENUE="arcus"\nexport HEDGE_LONG_INSTANCE="arcus"\n'
    printf 'export HEDGE_SHORT_VENUE="lighter"\nexport HEDGE_SHORT_INSTANCE="core"\n'
    printf 'export HEDGE_SYMBOLS="%s"\nexport HEDGE_TARGET_NOTIONAL_USD="20000"\nexport HEDGE_ROLL_ENABLED="false"\n' "$syms"
    printf 'export HEDGE_STATUS_S3_URI="%s"\n' "$status"
    printf 'export ARCUS_ADDRESS_ARCUS="%s"\nexport ARCUS_ACCOUNT_INDEX_ARCUS="0"\nexport ARCUS_API_KEY_ARCUS="ab"\nexport ARCUS_API_PRIVATE_KEY_ARCUS="Y2lwaGVy"\n' "$wallet"
    printf 'export LIGHTER_ACCOUNT_INDEX_CORE="%s"\nexport LIGHTER_API_KEY_INDEX_CORE="24"\nexport LIGHTER_PUBLIC_API_KEY_CORE="Y2lwaGVy"\nexport LIGHTER_PRIVATE_API_KEY_CORE="Y2lwaGVy"\n' "$core"
    for l in "$@"; do printf '%s\n' "$l"; done
  } > "$T/env/$unit.env"
}
aapl_env() { write_env debot-xvenue-hedge-arcus "$A812" 281474976574527 "AAPL:5/3" "$S3/xvenue-hedge-arcus.json" "$@"; }
nvda_env() { write_env debot-xvenue-hedge-arcus-nvda "$A2C7" 281474976624889 "NVDA:5/3" "$S3/xvenue-hedge-arcus-nvda.json" "$@"; }
run() { rm -f "$T/env.out"; OUT=$(DEBOT_ENV_DIR="$T/env" DEBOT_HEDGE_ARCUS_ROOT="$T" bash "$L" "$@" 2>&1); RC=$?; }
refused() { # DESC -- expects exit 64 and the message fragment $1
  [ "$RC" = 64 ] && echo "$OUT" | grep -qF "$1" && [ ! -e "$T/env.out" ] && ok "refused: $2" \
    || bad "expected refusal '$1' for $2 (rc=$RC): $OUT"; }

# 1. AAPL instance (no argument)
aapl_env; nvda_env; run
[ "$RC" = 0 ] && echo "$OUT" | grep -q 'FAKE debot-hedge-arcus$' && ok "AAPL instance execs its own binary" || bad "AAPL instance rc=$RC: $OUT"
grep -q "^HEDGE_BASE_DIR=$T/opt/debot-hedge-arcus$" "$T/env.out" && ok "AAPL state dir" || bad "AAPL HEDGE_BASE_DIR: $(grep HEDGE_BASE_DIR "$T/env.out")"
grep -q "^LIGHTER_GO_PATH=$T/opt/debot-hedge-arcus/lib$" "$T/env.out" && grep -q "^LD_LIBRARY_PATH=$T/opt/debot-hedge-arcus/lib:" "$T/env.out" \
  && ok "AAPL libsigner path exported" || bad "AAPL libsigner path"
grep -qE '^(LIGHTER_ACCOUNT_INDEX|ARCUS_ADDRESS)=' "$T/env.out" && bad "unsuffixed venue vars from debot.env leaked" || ok "unsuffixed venue vars dropped"
grep -q "^ARCUS_ADDRESS_ARCUS=$A812$" "$T/env.out" && grep -q '^LIGHTER_ACCOUNT_INDEX_CORE=281474976574527$' "$T/env.out" && ok "AAPL suffixed vars passed" || bad "AAPL suffixed vars"

# 2. nvda instance
run nvda
[ "$RC" = 0 ] && echo "$OUT" | grep -q 'FAKE debot-hedge-arcus-nvda$' && ok "nvda instance execs its own binary" || bad "nvda rc=$RC: $OUT"
grep -q "^HEDGE_BASE_DIR=$T/opt/debot-hedge-arcus-nvda$" "$T/env.out" && ok "nvda state dir" || bad "nvda HEDGE_BASE_DIR"
grep -q '^LIGHTER_GO_PATH=' "$T/env.out" && bad "nvda exported a libsigner path without lib/" || ok "nvda: no lib/ → no LIGHTER_GO_PATH"
grep -q "^ARCUS_ADDRESS_ARCUS=$A2C7$" "$T/env.out" && grep -q '^HEDGE_SYMBOLS=NVDA:5/3$' "$T/env.out" && ok "nvda suffixed vars passed" || bad "nvda suffixed vars"
[ -d "$T/opt/debot-hedge-arcus-nvda/hedge" ] && ok "nvda hedge/ dir created" || bad "nvda hedge dir"

# 3. HEDGE_DRY_RUN defaults to true when the env omits it
nvda_env; sed -i '/^export HEDGE_DRY_RUN=/d' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
[ "$RC" = 0 ] && grep -q '^HEDGE_DRY_RUN=true$' "$T/env.out" && ok "HEDGE_DRY_RUN defaults to true" || bad "HEDGE_DRY_RUN default (rc=$RC)"

# 4. refusals — wallet / account / market / status / roll / base dir
nvda_env; sed -i "s/^export ARCUS_ADDRESS_ARCUS=.*/export ARCUS_ADDRESS_ARCUS=\"$A812\"/" "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "ARCUS_ADDRESS_ARCUS must be this instance's wallet $A2C7" "nvda env pointing at the AAPL wallet"
aapl_env; sed -i "s/^export ARCUS_ADDRESS_ARCUS=.*/export ARCUS_ADDRESS_ARCUS=\"$A2C7\"/" "$T/env/debot-xvenue-hedge-arcus.env"; run
refused "ARCUS_ADDRESS_ARCUS must be this instance's wallet 0x812b" "AAPL env pointing at the nvda wallet"
aapl_env; sed -i 's/^export ARCUS_ADDRESS_ARCUS=.*/export ARCUS_ADDRESS_ARCUS=""/' "$T/env/debot-xvenue-hedge-arcus.env"; run
refused "ARCUS_ADDRESS_ARCUS must be" "empty wallet"
nvda_env; sed -i 's/^export LIGHTER_ACCOUNT_INDEX_CORE=.*/export LIGHTER_ACCOUNT_INDEX_CORE="281474976574527"/' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "LIGHTER_ACCOUNT_INDEX_CORE must be 281474976624889" "nvda env on the AAPL Core sub-account"
aapl_env; sed -i 's/^export LIGHTER_ACCOUNT_INDEX_CORE=.*/export LIGHTER_ACCOUNT_INDEX_CORE="281474976624819"/' "$T/env/debot-xvenue-hedge-arcus.env"; run
refused "LIGHTER_ACCOUNT_INDEX_CORE must be 281474976574527" "AAPL env on the #1046 Core account"
nvda_env; sed -i 's/^export HEDGE_SYMBOLS=.*/export HEDGE_SYMBOLS="AAPL:5\/3"/' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "HEDGE_SYMBOLS must be 'NVDA:<mmr>'" "nvda env holding AAPL (same market on both wallets)"
nvda_env; sed -i 's/^export HEDGE_SYMBOLS=.*/export HEDGE_SYMBOLS="NVDA:5\/3,AAPL:5\/3"/' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "HEDGE_SYMBOLS must list only NVDA" "nvda env with a second book"
nvda_env; sed -i 's/^export HEDGE_SYMBOLS=.*/export HEDGE_SYMBOLS="NVDAX:5\/3"/' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "HEDGE_SYMBOLS must be 'NVDA:<mmr>'" "symbol prefix trick NVDAX"
aapl_env; sed -i 's/^export HEDGE_SYMBOLS=.*/export HEDGE_SYMBOLS="NVDA:5\/3"/' "$T/env/debot-xvenue-hedge-arcus.env"; run
refused "HEDGE_SYMBOLS must be 'AAPL:<mmr>'" "AAPL env holding NVDA"
nvda_env; sed -i "s#^export HEDGE_STATUS_S3_URI=.*#export HEDGE_STATUS_S3_URI=\"$S3/xvenue-hedge-arcus.json\"#" "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "HEDGE_STATUS_S3_URI must be $S3/xvenue-hedge-arcus-nvda.json" "nvda env writing the AAPL status object"
aapl_env; sed -i "s#^export HEDGE_STATUS_S3_URI=.*#export HEDGE_STATUS_S3_URI=\"$S3/xvenue-hedge-holder.json\"#" "$T/env/debot-xvenue-hedge-arcus.env"; run
refused "HEDGE_STATUS_S3_URI must be $S3/xvenue-hedge-arcus.json" "AAPL env writing the first holder's status object"
nvda_env 'export HEDGE_ROLL_ENABLED="true"'; run nvda
refused "the roll stays off" "roll enabled"
nvda_env 'export HEDGE_BASE_DIR="/opt/debot-hedge-arcus"'; run nvda
refused "HEDGE_BASE_DIR must not be set" "HEDGE_BASE_DIR in the env"
nvda_env 'export HEDGE_POINTS_HISTORY_PATH="/tmp/x"'; run nvda
refused "HEDGE_POINTS_HISTORY_PATH must not be set" "points history path in the env"
nvda_env; sed -i 's/^export ARCUS_ACCOUNT_INDEX_ARCUS=.*/export ARCUS_ACCOUNT_INDEX_ARCUS="1"/' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "ARCUS_ACCOUNT_INDEX_ARCUS must be 0" "subaccount 1"
nvda_env; sed -i 's/^export HEDGE_SHORT_INSTANCE=.*/export HEDGE_SHORT_INSTANCE="rh"/' "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
refused "HEDGE_SHORT_INSTANCE must be core" "short leg on another Lighter instance"

# 5. unknown instance and missing env
nvda_env; run spy
refused "unknown instance 'spy'" "unknown instance argument"
rm -f "$T/env/debot-xvenue-hedge-arcus-nvda.env"; run nvda
[ "$RC" != 0 ] && [ ! -e "$T/env.out" ] && ok "missing env file → no exec (rc=$RC)" || bad "missing env file started the binary"

[ $fail = 0 ] && echo "ALL OK" || { echo "FAILURES"; exit 1; }
