#!/usr/bin/env bash
# Test for scripts/arcus_vol_encrypt_key.sh (bot-strategy#1093): a fake `aws`
# on PATH returns a fixed 32-byte data key; the produced ciphertext is decrypted
# with an independent implementation (python `cryptography` when available,
# else a second openssl run) and its byte layout is asserted
# (base64 -> 16-byte IV + 48-byte ciphertext, the scripts/encrypt.py format).
#   bash scripts/test_arcus_vol_encrypt_key.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/arcus_vol_encrypt_key.sh"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/etc" "$T/opt"

# fixed data key (32 bytes) that the fake KMS "decrypts" to, base64 as aws prints it
DK_HEX="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
DK_B64="$(printf '%s' "$DK_HEX" | xxd -r -p | base64 -w0)"
EDK_B64="$(head -c 48 /dev/zero | tr '\0' 'E' | base64 -w0)"   # the "ciphertext" of the data key; opaque to the test
cat > "$T/bin/aws" <<SH
#!/usr/bin/env bash
# fake aws-cli: only kms decrypt (--query Plaintext --output text)
[ "\$1" = kms ] && [ "\$2" = decrypt ] || { echo "fake aws: unexpected \$*" >&2; exit 9; }
echo "\$*" >> "$T/aws.calls"
printf '%s\n' "\${FAKE_KMS_PLAINTEXT_B64:-$DK_B64}"
SH
chmod 755 "$T/bin/aws"
export PATH="$T/bin:$PATH"

KEY_HEX="9f8e7d6c5b4a39281706f5e4d3c2b1a0ffeeddccbbaa99887766554433221100"
write_live() {
    printf 'ARCUS_ADDRESS=0xA2C7\nARCUS_API_KEY=pub\n%s\nARCUS_ACCOUNT_INDEX=0\n' "$1" > "$T/etc/live.env"
    chmod 600 "$T/etc/live.env"
}
run() {
    set +e
    OUT="$(DEBOT_ARCUS_VOL_ETC_DIR="$T/etc" DEBOT_ARCUS_VOL_SECRETS_COMMON="$T/opt/debot_secrets_common.env" bash "$SCRIPT" "$@" 2>&1)"
    RC=$?
    set -e
}
fail() { echo "FAIL: $*"; echo "--- output:"; echo "$OUT"; exit 1; }
PASS=0; ok() { PASS=$((PASS + 1)); echo "ok $PASS - $*"; }
val() { grep "^$1=" "$T/etc/live.env" | cut -d= -f2-; }

# 1. happy path: EDK from secrets_common (export KEY="value" form), 0x-prefixed upper-case key
printf 'export ENCRYPTED_DATA_KEY="%s"\nexport OTHER="x"\n' "$EDK_B64" > "$T/opt/debot_secrets_common.env"
write_live "ARCUS_PLAIN_API_PRIVATE_KEY=0x$(printf '%s' "$KEY_HEX" | tr 'a-f' 'A-F')"
run
[ "$RC" -eq 0 ] || fail "encrypt exit $RC"
grep -q "roundtrip verified" <<< "$OUT" || fail "no roundtrip line"
grep -q "wrote $T/etc/live.env" <<< "$OUT" || fail "no write line"
! grep -qi "$KEY_HEX" <<< "$OUT" || fail "plain key leaked to output"
! grep -q "$DK_B64" <<< "$OUT" || fail "data key leaked to output"
! grep -q "ARCUS_PLAIN_API_PRIVATE_KEY=" "$T/etc/live.env" || fail "plain key still in live.env"
[ "$(stat -c %a "$T/etc/live.env")" = 600 ] || fail "live.env mode $(stat -c %a "$T/etc/live.env")"
[ "$(val ARCUS_ADDRESS)" = 0xA2C7 ] && [ "$(val ARCUS_API_KEY)" = pub ] && [ "$(val ARCUS_ACCOUNT_INDEX)" = 0 ] || fail "other lines not kept"
[ "$(val ENCRYPTED_DATA_KEY)" = "$EDK_B64" ] || fail "ENCRYPTED_DATA_KEY not added"
CIPHER="$(val ARCUS_API_PRIVATE_KEY)"; [ -n "$CIPHER" ] || fail "ARCUS_API_PRIVATE_KEY not added"
[ ! -e "$T/etc/live.env.plain.bak" ] || fail "default run must not keep a plaintext backup"
! grep -rq "$KEY_HEX" "$T/etc" || fail "plain key still present somewhere under etc/"
grep -q -- "--ciphertext-blob fileb://" "$T/aws.calls" && grep -q -- "--region eu-central-1" "$T/aws.calls" || fail "kms call shape: $(cat "$T/aws.calls")"
ok "encrypts, rewrites live.env, keeps the other lines, keeps no plaintext copy, leaks nothing"

# 2. byte layout + independent decrypt
printf '%s' "$CIPHER" | base64 -d > "$T/blob.bin"
[ "$(stat -c %s "$T/blob.bin")" -eq 64 ] || fail "blob is $(stat -c %s "$T/blob.bin") bytes, expected 16 (IV) + 48 (ciphertext)"
if python3 -c "import cryptography" 2>/dev/null; then
    DEC="$(python3 - "$T/blob.bin" "$DK_HEX" <<'PY'
import sys
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
blob = open(sys.argv[1], "rb").read(); key = bytes.fromhex(sys.argv[2])
iv, ct = blob[:16], blob[16:]
assert len(iv) == 16 and len(ct) == 48, (len(iv), len(ct))
d = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
padded = d.update(ct) + d.finalize()
u = padding.PKCS7(128).unpadder()
pt = u.update(padded) + u.finalize()
assert len(pt) == 32, len(pt)
print(pt.hex())
PY
)"
    IMPL="python cryptography (AES-256-CBC + PKCS7, IV = blob[:16])"
else
    IV_HEX="$(head -c 16 "$T/blob.bin" | od -An -tx1 -v | tr -d ' \n')"
    tail -c +17 "$T/blob.bin" > "$T/ct.bin"
    [ "$(stat -c %s "$T/ct.bin")" -eq 48 ] || fail "ciphertext part is not 48 bytes"
    DEC="$(openssl enc -d -aes-256-cbc -K "$DK_HEX" -iv "$IV_HEX" -in "$T/ct.bin" | od -An -tx1 -v | tr -d ' \n')"
    IMPL="openssl (second invocation; layout asserted 16 + 48 bytes)"
fi
[ "$DEC" = "$KEY_HEX" ] || fail "independent decrypt mismatch: $DEC"
ok "independent decrypt recovers the key via $IMPL; layout = base64(IV[16] || AES-CBC-PKCS7[48])"

# 3. byte-identical to scripts/encrypt.py: re-encrypt the same key with the IV the
#    script chose, using encrypt.py's exact algorithm (AES.new(key, MODE_CBC, iv);
#    pad(data, 16); b64(iv + ct)) -- with pycryptodome if installed, else the same
#    steps through `cryptography`. Equal blobs = same layout, padding and mode.
REF="$(python3 - "$T/blob.bin" "$DK_HEX" "$KEY_HEX" <<'PY'
import base64, sys
blob = open(sys.argv[1], "rb").read(); key = bytes.fromhex(sys.argv[2]); data = bytes.fromhex(sys.argv[3])
iv = blob[:16]
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad
    cipher = AES.new(key, AES.MODE_CBC, iv)
    ct = cipher.encrypt(pad(data, AES.block_size))
except ImportError:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives import padding
    p = padding.PKCS7(128).padder(); padded = p.update(data) + p.finalize()
    e = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor(); ct = e.update(padded) + e.finalize()
print(base64.b64encode(iv + ct).decode())
PY
)"
[ "$REF" = "$CIPHER" ] || fail "blob differs from the scripts/encrypt.py algorithm for the same IV"
ok "byte-identical to the scripts/encrypt.py algorithm (same IV -> same base64 blob)"

# 4. idempotent: already encrypted -> nothing to do, file unchanged
before="$(sha256sum "$T/etc/live.env")"; run
[ "$RC" -eq 0 ] && grep -q "already carries ARCUS_API_PRIVATE_KEY" <<< "$OUT" && [ "$before" = "$(sha256sum "$T/etc/live.env")" ] || fail "second run must be a no-op"
ok "second run is a no-op"

# 5. EDK in live.env wins over secrets_common; --dry-run writes nothing; --keep-plain-backup opt-in
printf 'ARCUS_ADDRESS=0xA2C7\nARCUS_API_KEY=pub\nARCUS_PLAIN_API_PRIVATE_KEY=%s\nENCRYPTED_DATA_KEY=%s\nARCUS_ACCOUNT_INDEX=0\n' "$KEY_HEX" "$EDK_B64" > "$T/etc/live.env"; chmod 600 "$T/etc/live.env"
rm -f "$T/opt/debot_secrets_common.env"
before="$(sha256sum "$T/etc/live.env")"; run --dry-run
[ "$RC" -eq 0 ] && grep -q "dry-run: would rewrite" <<< "$OUT" && [ "$before" = "$(sha256sum "$T/etc/live.env")" ] || fail "dry-run must not write"
grep -q "ENCRYPTED_DATA_KEY from $T/etc/live.env" <<< "$OUT" || fail "EDK source should be live.env"
run --keep-plain-backup
[ "$RC" -eq 0 ] && [ -n "$(val ARCUS_API_PRIVATE_KEY)" ] && [ "$(grep -c '^ENCRYPTED_DATA_KEY=' "$T/etc/live.env")" -eq 1 ] || fail "--keep-plain-backup run"
[ -f "$T/etc/live.env.plain.bak" ] && [ "$(stat -c %a "$T/etc/live.env.plain.bak")" = 600 ] && grep -q "ARCUS_PLAIN_API_PRIVATE_KEY=$KEY_HEX" "$T/etc/live.env.plain.bak" || fail "--keep-plain-backup must leave live.env.plain.bak (600)"
rm -f "$T/etc/live.env.plain.bak"
ok "dry-run writes nothing; EDK from live.env; --keep-plain-backup is the only way to keep a plaintext copy; one ENCRYPTED_DATA_KEY line"

# 6. refusals: not 64-hex (message names only that), data key not 32 bytes, mode, missing EDK
printf 'export ENCRYPTED_DATA_KEY="%s"\n' "$EDK_B64" > "$T/opt/debot_secrets_common.env"
write_live "ARCUS_PLAIN_API_PRIVATE_KEY=0xabc123"; run
[ "$RC" -eq 1 ] && grep -q "REFUSED: ARCUS_PLAIN_API_PRIVATE_KEY is not 64-hex" <<< "$OUT" && ! grep -q "abc123" <<< "$OUT" || fail "short key must refuse with 'not 64-hex' only"
write_live "ARCUS_PLAIN_API_PRIVATE_KEY=$KEY_HEX"
FAKE_KMS_PLAINTEXT_B64="$(head -c 16 /dev/zero | base64 -w0)" run
[ "$RC" -eq 1 ] && grep -q "data key is not 32 bytes (got 16)" <<< "$OUT" && grep -q "ARCUS_PLAIN_API_PRIVATE_KEY=" "$T/etc/live.env" || fail "16-byte data key must refuse and leave the file"
chmod 644 "$T/etc/live.env"; run
[ "$RC" -eq 1 ] && grep -q "must be mode 600" <<< "$OUT" || fail "mode 644 must refuse"
chmod 600 "$T/etc/live.env"; rm -f "$T/opt/debot_secrets_common.env"; run
[ "$RC" -eq 1 ] && grep -q "ENCRYPTED_DATA_KEY is neither" <<< "$OUT" || fail "missing EDK must refuse"
ok "refusals: not 64-hex, data key size, file mode, missing ENCRYPTED_DATA_KEY"

echo "all $PASS encrypt-key checks passed"
