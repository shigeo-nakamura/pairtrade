#!/bin/bash
# Encrypt the Arcus API signing key in /etc/debot-arcus-vol/live.env under the
# host's KMS data key (bot-strategy#1093). Host-side, run as root; needs only
# bash, openssl, base64, and aws-cli (no python packages).
#
# Reads  ARCUS_PLAIN_API_PRIVATE_KEY (64 hex chars, optional 0x) from live.env
#        ENCRYPTED_DATA_KEY from live.env, else /opt/debot/scripts/debot_secrets_common.env
# Does   aws kms decrypt -> 32-byte data key; AES-256-CBC/PKCS7 with a random
#        16-byte IV over the RAW 32 key bytes; output base64(IV || ciphertext)
#        -- byte-identical to `scripts/encrypt.py <data-key-b64> 0x<hex>`, which is
#        what debot_utils::decrypt_data_with_kms(.., output_as_hex=true) expects.
# Checks the roundtrip (openssl dec -> lowercase hex == input) BEFORE writing.
# Writes live.env atomically (mode 600 root): keeps ARCUS_ADDRESS / ARCUS_API_KEY /
#        ARCUS_ACCOUNT_INDEX and every other line, adds ARCUS_API_PRIVATE_KEY=<cipher>
#        and ENCRYPTED_DATA_KEY=<value>, drops the plain-key line. The previous file
#        is kept as live.env.plain.bak (600) unless --no-backup.
# Prints only status lines -- never a key, a ciphertext, or the data key.
#
#   sudo /opt/debot/scripts/arcus_vol_encrypt_key.sh [--dry-run] [--no-backup]
#
# The KMS key lives in eu-central-1 (the hosts' shared data key); AWS_REGION
# defaults to that here exactly as debot-utils does at decrypt time.
set -euo pipefail

DRY_RUN=0; BACKUP=1
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --no-backup) BACKUP=0 ;;
        *) echo "unknown argument: $arg" >&2; exit 2 ;;
    esac
done
ETC_DIR="${DEBOT_ARCUS_VOL_ETC_DIR:-/etc/debot-arcus-vol}"
LIVE_ENV="$ETC_DIR/live.env"
SECRETS_COMMON="${DEBOT_ARCUS_VOL_SECRETS_COMMON:-/opt/debot/scripts/debot_secrets_common.env}"
REGION="${AWS_REGION:-eu-central-1}"

say() { echo "[arcus_vol_encrypt_key] $*"; }
die() { echo "[arcus_vol_encrypt_key] REFUSED: $*" >&2; exit 1; }

# Read one KEY from a KEY=VALUE / export KEY="VALUE" file without sourcing it.
kv_get() { # file key -> value on stdout (empty if absent)
    local file="$1" key="$2" line value
    while IFS= read -r line || [ -n "$line" ]; do
        line="${line%$'\r'}"
        [[ "$line" =~ ^[[:space:]]*(export[[:space:]]+)?${key}=(.*)$ ]] || continue
        value="${BASH_REMATCH[2]}"
        if [[ "$value" =~ ^\"(.*)\"$ ]]; then value="${BASH_REMATCH[1]}"; fi
        printf '%s' "$value"; return 0
    done < "$file"
    return 0
}

[ -f "$LIVE_ENV" ] || die "missing $LIVE_ENV"
[ "$(stat -c %a "$LIVE_ENV")" = "600" ] || die "$LIVE_ENV must be mode 600 (is $(stat -c %a "$LIVE_ENV"))"
for tool in openssl base64 aws mktemp; do command -v "$tool" >/dev/null || die "$tool not found on PATH"; done

if [ -n "$(kv_get "$LIVE_ENV" ARCUS_API_PRIVATE_KEY)" ]; then
    say "live.env already carries ARCUS_API_PRIVATE_KEY (encrypted); nothing to do"
    [ -z "$(kv_get "$LIVE_ENV" ARCUS_PLAIN_API_PRIVATE_KEY)" ] || say "NOTE: a plain key line is still present too; remove it by hand (the launcher never exports both)"
    exit 0
fi

plain="$(kv_get "$LIVE_ENV" ARCUS_PLAIN_API_PRIVATE_KEY)"
[ -n "$plain" ] || die "ARCUS_PLAIN_API_PRIVATE_KEY not found in $LIVE_ENV"
plain="${plain#0x}"; plain="${plain#0X}"
[[ "$plain" =~ ^[0-9a-fA-F]{64}$ ]] || die "ARCUS_PLAIN_API_PRIVATE_KEY is not 64-hex"
plain="$(printf '%s' "$plain" | tr 'A-F' 'a-f')"

edk="$(kv_get "$LIVE_ENV" ENCRYPTED_DATA_KEY)"; edk_src="$LIVE_ENV"
if [ -z "$edk" ]; then
    [ -f "$SECRETS_COMMON" ] || die "ENCRYPTED_DATA_KEY is neither in $LIVE_ENV nor is $SECRETS_COMMON present"
    edk="$(kv_get "$SECRETS_COMMON" ENCRYPTED_DATA_KEY)"; edk_src="$SECRETS_COMMON"
    [ -n "$edk" ] || die "ENCRYPTED_DATA_KEY not found in $SECRETS_COMMON"
fi
edk="${edk// /}"
[[ "$edk" =~ ^[A-Za-z0-9+/=]+$ ]] || die "ENCRYPTED_DATA_KEY does not look like base64"
say "plain key: 64 hex; ENCRYPTED_DATA_KEY from $edk_src; KMS region $REGION"

# All secret material goes through a private temp dir; never onto the command
# line of a long-running process and never into stdout.
work="$(mktemp -d)"
chmod 700 "$work"
trap 'rm -rf "$work"' EXIT

printf '%s' "$edk" | base64 -d > "$work/edk.bin" 2>/dev/null || die "ENCRYPTED_DATA_KEY is not valid base64"
if ! aws kms decrypt --region "$REGION" --ciphertext-blob "fileb://$work/edk.bin" --query Plaintext --output text > "$work/dk.b64" 2> "$work/kms.err"; then
    die "aws kms decrypt failed in $REGION: $(head -c 300 "$work/kms.err" | tr '\n' ' ')"
fi
tr -d '\n' < "$work/dk.b64" | base64 -d > "$work/dk.bin" 2>/dev/null || die "KMS plaintext is not valid base64"
[ "$(stat -c %s "$work/dk.bin")" -eq 32 ] || die "data key is not 32 bytes (got $(stat -c %s "$work/dk.bin"))"
dk_hex="$(od -An -tx1 -v "$work/dk.bin" | tr -d ' \n')"

iv_hex="$(openssl rand -hex 16)"
printf '%s' "$plain" | xxd -r -p > "$work/key.bin" 2>/dev/null || { printf '%s' "$plain" | python3 -c 'import sys;sys.stdout.buffer.write(bytes.fromhex(sys.stdin.read()))' > "$work/key.bin"; }
[ "$(stat -c %s "$work/key.bin")" -eq 32 ] || die "internal: key bytes are not 32"
openssl enc -aes-256-cbc -K "$dk_hex" -iv "$iv_hex" -in "$work/key.bin" -out "$work/ct.bin" 2>/dev/null || die "openssl enc failed"
[ "$(stat -c %s "$work/ct.bin")" -eq 48 ] || die "internal: ciphertext is not 48 bytes (32 + PKCS7 block)"
{ printf '%s' "$iv_hex" | xxd -r -p; cat "$work/ct.bin"; } > "$work/blob.bin"
[ "$(stat -c %s "$work/blob.bin")" -eq 64 ] || die "internal: blob is not 64 bytes"
cipher_b64="$(base64 -w0 < "$work/blob.bin")"

# Roundtrip exactly the way the runtime will read it: IV = first 16 bytes.
openssl enc -d -aes-256-cbc -K "$dk_hex" -iv "$iv_hex" -in "$work/ct.bin" -out "$work/rt.bin" 2>/dev/null || die "roundtrip decrypt failed"
rt_hex="$(od -An -tx1 -v "$work/rt.bin" | tr -d ' \n')"
[ "$rt_hex" = "$plain" ] || die "roundtrip mismatch; live.env left untouched"
say "roundtrip verified (AES-256-CBC, IV-prefixed, 64-byte blob)"

if [ "$DRY_RUN" -eq 1 ]; then
    say "dry-run: would rewrite $LIVE_ENV: drop ARCUS_PLAIN_API_PRIVATE_KEY, add ARCUS_API_PRIVATE_KEY and ENCRYPTED_DATA_KEY$( [ "$BACKUP" -eq 1 ] && echo ", keep $LIVE_ENV.plain.bak")"
    exit 0
fi

new="$work/live.env.new"
: > "$new"; chmod 600 "$new"
while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    case "$line" in
        *ARCUS_PLAIN_API_PRIVATE_KEY=*|*ENCRYPTED_DATA_KEY=*|*ARCUS_API_PRIVATE_KEY=*) continue ;;
    esac
    printf '%s\n' "$line" >> "$new"
done < "$LIVE_ENV"
{
    echo "# signing key encrypted by arcus_vol_encrypt_key.sh on $(date -u +%FT%TZ) (AES-256-CBC under the host data key)"
    echo "ARCUS_API_PRIVATE_KEY=$cipher_b64"
    echo "ENCRYPTED_DATA_KEY=$edk"
} >> "$new"
grep -q '^ARCUS_ADDRESS=' "$new" && grep -q '^ARCUS_API_KEY=' "$new" && grep -q '^ARCUS_ACCOUNT_INDEX=' "$new" || die "internal: rewritten file lost a required line; live.env left untouched"
! grep -q 'ARCUS_PLAIN_API_PRIVATE_KEY=' "$new" || die "internal: plain key still present in the rewrite"

if [ "$BACKUP" -eq 1 ]; then
    cp -p "$LIVE_ENV" "$LIVE_ENV.plain.bak"
    chmod 600 "$LIVE_ENV.plain.bak"
fi
owner="$(stat -c %U:%G "$LIVE_ENV")"
install -m 600 -o "${owner%%:*}" -g "${owner##*:}" "$new" "$LIVE_ENV.tmp"
mv -f "$LIVE_ENV.tmp" "$LIVE_ENV"
say "wrote $LIVE_ENV (mode 600, owner $owner): ARCUS_API_PRIVATE_KEY + ENCRYPTED_DATA_KEY, plain key removed$( [ "$BACKUP" -eq 1 ] && echo "; previous file kept as live.env.plain.bak (600) -- delete it once the service has started with the encrypted key")"
