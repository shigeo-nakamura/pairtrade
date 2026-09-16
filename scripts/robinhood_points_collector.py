#!/usr/bin/env python3
"""Snapshot the Robinhood Chain Lighter points tallies for each arm.

bot-strategy#938. The Robinhood arms buy points, and the subsidy KPI
(`subsidy_ledger.py`, pairtrade#297) prices them as cost per point -- but
the denominator has been an operator-supplied file because the points
routes were believed to answer 403. They do not: `/api/v1/points` does
not exist, while the two routes the Lighter web app actually calls do,
on `api.rh.lighter.xyz` as well, and answer

    {"code":20001,"message":"invalid param : auth query param and
     Authorization header are empty"}

unauthenticated. They want the same bearer token the SDK mints for the
other authenticated reads. This script mints it and reads them:

    GET /api/v1/livePoints/total?account_index=N  -> total_live_points
    GET /api/v1/referral/points?account_index=N   -> user_total_points,
        user_last_week_points, user_total_referral_reward_points,
        user_last_week_referral_reward_points, reward_point_multiplier

per docs.lighter.xyz (Lighter on Robinhood Chain Points): "points are
calculated and awarded in real time based on eligible activity, with an
additional weekly points drop ... every Friday". How the live tally and
the credited total relate is not documented, so every tally is written
as read, side by side, and `robinhood_points_daily.py` turns the series
into the per-day rows the ledger takes once the series shows which one
moves how.

Why this is a script and not the bot
------------------------------------
The Robinhood pairtrade service is frozen: its engine left this repo in
#284 and no workflow touches the binary (bot-strategy#937). A points
read therefore cannot go into status.json the way funding did; it lives
beside the bot, reading the same credential files the launcher sources.

Credentials never leave the host and never touch disk in the clear. The
KMS data key is decrypted by the instance role (`aws kms decrypt`), the
API key is AES-256-CBC-decrypted in memory exactly as
`debot_utils::decrypt_data_with_kms` does (IV = first 16 bytes, PKCS#7),
and the token is minted through the same `libsigner.so` the bot links,
via ctypes. Nothing derived from the key is printed or written; the
output rows carry account indices and tallies only.

Output
------
One JSON object per arm per run appended to `--out` (default
`/home/ec2-user/debot_status/robinhood-points/points_history.jsonl`):

    {"ts": "...Z", "ts_unix": N, "arm": "freq", "account_index": 3209,
     "live_points_total": 1234.5, "total_points": ..., "last_week_points": ...,
     "total_referral_reward_points": ..., "last_week_referral_reward_points": ...,
     "reward_point_multiplier": 1.0 | null,
     "raw": {"livePoints/total": {...}, "referral/points": {...}}}

`raw` keeps the venue's bodies verbatim (minus the per-referee list) so a
field the venue adds or renames is visible in the series before a parser
is taught about it. A tally the venue did not state is an error for that
arm, never a 0: a zero denominator reads as "traded for nothing".

An arm that fails is reported on stderr and skipped; the other arm's row
is still written; the exit status is non-zero if any arm failed so the
timer unit shows it.
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

DEFAULT_BASE_URL = "https://api.rh.lighter.xyz"
# `LIGHTER_ROBINHOOD_CHAIN_ID` in dex-connector (signing chain id for the
# api.rh.lighter.xyz deployment). The token is signed for this chain.
ROBINHOOD_SIGNING_CHAIN_ID = 466_324
DEFAULT_LIBSIGNER = "/opt/debot/lib/libsigner.so"
DEFAULT_OUT = Path("/home/ec2-user/debot_status/robinhood-points/points_history.jsonl")
TOKEN_TTL_SECS = 600
HTTP_TIMEOUT_SECS = 20

TALLY_FIELDS = (
    # (row key, endpoint, venue field)
    ("live_points_total", "livePoints/total", "total_live_points"),
    ("total_points", "referral/points", "user_total_points"),
    ("last_week_points", "referral/points", "user_last_week_points"),
    ("total_referral_reward_points", "referral/points", "user_total_referral_reward_points"),
    ("last_week_referral_reward_points", "referral/points", "user_last_week_referral_reward_points"),
)


class CollectorError(Exception):
    pass


# --- credentials ---------------------------------------------------------


def load_env(path: Path) -> dict[str, str]:
    """KEY=VALUE lines as the launcher's `source` would see them: comments
    and blanks skipped, an optional `export `, surrounding quotes
    stripped. Anything fancier (expansion, escapes) is out of scope --
    these files hold ciphertext and integers."""
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        out[key.strip()] = value
    return out


def require(env: dict[str, str], key: str, path: Path) -> str:
    value = env.get(key, "")
    if not value:
        raise CollectorError(f"{path}: {key} is not set")
    return value


def kms_decrypt_data_key(encrypted_data_key_b64: str, region: str) -> bytes:
    """The AES data key, via the instance role. AWS CLI v2 takes blob
    parameters as base64 text, which is the form the env file holds."""
    ciphertext = encrypted_data_key_b64.replace(" ", "")
    result = subprocess.run(
        ["aws", "kms", "decrypt", "--region", region, "--ciphertext-blob", ciphertext,
         "--query", "Plaintext", "--output", "text"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        # stderr from the CLI names the failure (AccessDenied, IMDS, ...)
        # and never the key.
        raise CollectorError(
            f"aws kms decrypt failed (rc={result.returncode}): {result.stderr.strip()[:300]}")
    return base64.b64decode(result.stdout.strip())


def aes_cbc_decrypt(data_key: bytes, ciphertext_b64: str) -> bytes:
    """Mirror of `debot_utils::decrypt_data_with_kms`: base64 body whose
    first 16 bytes are the IV, AES-256-CBC with PKCS#7 padding."""
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    raw = base64.b64decode(ciphertext_b64.replace(" ", ""))
    if len(raw) < 32:
        raise CollectorError("ciphertext shorter than IV + one block")
    decryptor = Cipher(algorithms.AES(data_key), modes.CBC(raw[:16])).decryptor()
    padded = decryptor.update(raw[16:]) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


# --- signer ---------------------------------------------------------------


class _StrOrErr(ctypes.Structure):
    _fields_ = [("str", ctypes.c_void_p), ("err", ctypes.c_void_p)]


class Signer:
    """`CreateClient` + `CreateAuthToken` from the Lighter Go signer.

    Same exports and argument shapes dex-connector's `ffi.rs` binds
    (lighter-go v1.0.2): `CreateClient(url, private_key_hex, chain_id,
    api_key_index, account_index) -> char* err` and
    `CreateAuthToken(deadline, api_key_index, account_index) -> StrOrErr`.
    Both C strings are freed here, as the Rust side does."""

    def __init__(self, path: str) -> None:
        self.lib = ctypes.CDLL(path)
        self.libc = ctypes.CDLL("libc.so.6")
        self.libc.free.argtypes = [ctypes.c_void_p]
        self.lib.CreateClient.restype = ctypes.c_void_p
        self.lib.CreateClient.argtypes = [
            ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int, ctypes.c_int, ctypes.c_longlong]
        self.lib.CreateAuthToken.restype = _StrOrErr
        self.lib.CreateAuthToken.argtypes = [ctypes.c_longlong, ctypes.c_int, ctypes.c_longlong]

    def _take(self, ptr: int | None) -> str | None:
        if not ptr:
            return None
        text = ctypes.string_at(ptr).decode("utf-8", "replace")
        self.libc.free(ptr)
        return text

    def create_client(self, base_url: str, private_key_hex: str, chain_id: int,
                      api_key_index: int, account_index: int) -> None:
        if len(private_key_hex) != 80:
            # 40-byte key, same check as `create_go_client`.
            raise CollectorError(
                f"api private key must be 80 hex chars, got {len(private_key_hex)}")
        err = self._take(self.lib.CreateClient(
            base_url.encode(), private_key_hex.encode(), chain_id, api_key_index, account_index))
        if err:
            raise CollectorError(f"CreateClient: {err}")

    def auth_token(self, deadline_unix: int, api_key_index: int, account_index: int) -> str:
        result = self.lib.CreateAuthToken(deadline_unix, api_key_index, account_index)
        err = self._take(result.err)
        token = self._take(result.str)
        if err:
            raise CollectorError(f"CreateAuthToken: {err}")
        if not token:
            raise CollectorError("CreateAuthToken returned no token")
        return token


# --- venue ----------------------------------------------------------------


def http_get_json(url: str, headers: dict[str, str]) -> tuple[int, Any]:
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECS) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, body = exc.code, exc.read()
    try:
        return status, json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CollectorError(f"{url.split('?')[0]}: HTTP {status}, non-JSON body: {exc}") from exc


def parse_tally(value: Any, field: str) -> Decimal:
    """A tally the venue may spell as a JSON number or a numeric string
    (its numeric fields arrive both ways on the same deployment).
    Absent, null, empty or non-numeric is an error naming the field --
    an unreadable denominator must never become a zero."""
    if value is None:
        raise CollectorError(f"points field {field} is missing")
    if isinstance(value, bool):
        raise CollectorError(f"points field {field} is a boolean")
    if isinstance(value, (int, float)):
        text = repr(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise CollectorError(f"points field {field} is an empty string")
    else:
        raise CollectorError(f"points field {field} is unexpected JSON: {value!r}")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise CollectorError(f"points field {field} is not numeric: {text!r}") from exc
    if not parsed.is_finite():
        raise CollectorError(f"points field {field} is not finite: {text!r}")
    return parsed


def check_envelope(endpoint: str, status: int, body: Any) -> dict:
    """Lighter answers application errors as HTTP 200 with `code != 200`,
    and auth failures as 400/401 with the same envelope; both are errors
    that carry the venue's message."""
    if not isinstance(body, dict):
        raise CollectorError(f"{endpoint}: HTTP {status}, body is not an object")
    code = body.get("code")
    if status != 200 or code != 200:
        raise CollectorError(
            f"{endpoint}: HTTP {status}, code {code}: {body.get('message', 'no message')}")
    return body


def tallies_from_bodies(bodies: dict[str, dict]) -> dict[str, Decimal | None]:
    out: dict[str, Decimal | None] = {}
    for key, endpoint, field in TALLY_FIELDS:
        out[key] = parse_tally(bodies[endpoint].get(field), field)
    multiplier = bodies["referral/points"].get("reward_point_multiplier")
    # Optional the way the venue means it: absent, null or an empty string
    # (the docs type it as a string) all say "no multiplier". A present,
    # non-empty, unreadable one is still an error.
    if multiplier is None or (isinstance(multiplier, str) and not multiplier.strip()):
        out["reward_point_multiplier"] = None
    else:
        out["reward_point_multiplier"] = parse_tally(multiplier, "reward_point_multiplier")
    return out


def fetch_points(base_url: str, token: str, api_key_public: str,
                 account_index: int) -> dict[str, dict]:
    headers = {"Authorization": token, "X-API-KEY": api_key_public,
               "User-Agent": "robinhood-points-collector (bot-strategy#938)"}
    bodies: dict[str, dict] = {}
    for endpoint in ("livePoints/total", "referral/points"):
        url = f"{base_url}/api/v1/{endpoint}?account_index={account_index}"
        status, body = http_get_json(url, headers)
        bodies[endpoint] = check_envelope(endpoint, status, body)
    return bodies


# --- rows -----------------------------------------------------------------


def decimal_to_json(value: Decimal | None) -> float | int | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def raw_for_row(bodies: dict[str, dict]) -> dict[str, dict]:
    """The venue bodies as read, minus the per-referee list (other
    people's addresses; not this account's tally)."""
    raw = {endpoint: dict(body) for endpoint, body in bodies.items()}
    raw.get("referral/points", {}).pop("referrals", None)
    return raw


def build_row(arm: str, account_index: int, bodies: dict[str, dict],
              now_unix: int) -> dict[str, Any]:
    tallies = tallies_from_bodies(bodies)
    row: dict[str, Any] = {
        "ts": datetime.fromtimestamp(now_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ts_unix": now_unix,
        "arm": arm,
        "account_index": account_index,
    }
    for key, _endpoint, _field in TALLY_FIELDS:
        row[key] = decimal_to_json(tallies[key])
    row["reward_point_multiplier"] = decimal_to_json(tallies["reward_point_multiplier"])
    row["raw"] = raw_for_row(bodies)
    return row


def append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")


# --- main -----------------------------------------------------------------


@dataclass(frozen=True)
class Arm:
    name: str
    env_path: Path


def parse_arm(spec: str) -> Arm:
    name, sep, path = spec.partition(":")
    if not sep or not name or not path:
        raise argparse.ArgumentTypeError(f"--arm wants NAME:ENV_PATH, got {spec!r}")
    return Arm(name=name, env_path=Path(path))


def collect_arm(arm: Arm, data_key: bytes, signer: Signer, base_url: str,
                now_unix: int) -> dict[str, Any]:
    env = load_env(arm.env_path)
    api_key_index = int(require(env, "LIGHTER_API_KEY_INDEX", arm.env_path))
    account_index = int(require(env, "LIGHTER_ACCOUNT_INDEX", arm.env_path))
    private_key_hex = aes_cbc_decrypt(
        data_key, require(env, "LIGHTER_PRIVATE_API_KEY", arm.env_path)).hex()
    api_key_public = aes_cbc_decrypt(
        data_key, require(env, "LIGHTER_PUBLIC_API_KEY", arm.env_path)).decode("utf-8")
    signer.create_client(base_url, private_key_hex, ROBINHOOD_SIGNING_CHAIN_ID,
                         api_key_index, account_index)
    token = signer.auth_token(now_unix + TOKEN_TTL_SECS, api_key_index, account_index)
    bodies = fetch_points(base_url, token, api_key_public, account_index)
    return build_row(arm.name, account_index, bodies, now_unix)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--arm", action="append", type=parse_arm, required=True,
                        help="NAME:ENV_PATH; repeatable (freq:/opt/debot/scripts/...env)")
    parser.add_argument("--common", type=Path,
                        default=Path("/opt/debot/scripts/debot_secrets_common.env"),
                        help="env file holding ENCRYPTED_DATA_KEY (and optionally AWS_REGION)")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--libsigner", default=DEFAULT_LIBSIGNER)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--s3-uri", default=os.environ.get("ROBINHOOD_POINTS_S3_URI", ""),
                        help="if set, `aws s3 cp` the history file there after appending")
    parser.add_argument("--print", action="store_true", help="also print each row to stdout")
    args = parser.parse_args(argv)

    common = load_env(args.common)
    region = common.get("AWS_REGION") or os.environ.get("AWS_REGION") or "eu-central-1"
    data_key = kms_decrypt_data_key(require(common, "ENCRYPTED_DATA_KEY", args.common), region)
    signer = Signer(args.libsigner)
    now_unix = int(time.time())

    rows: list[dict[str, Any]] = []
    failures = 0
    for arm in args.arm:
        try:
            rows.append(collect_arm(arm, data_key, signer, args.base_url, now_unix))
        except (CollectorError, OSError, ValueError) as exc:
            failures += 1
            print(f"arm {arm.name}: {exc}", file=sys.stderr)

    if rows:
        append_rows(args.out, rows)
        if args.print:
            for row in rows:
                print(json.dumps(row, sort_keys=True))
        if args.s3_uri:
            result = subprocess.run(["aws", "s3", "cp", str(args.out), args.s3_uri],
                                    capture_output=True, text=True, check=False)
            if result.returncode != 0:
                failures += 1
                print(f"s3 mirror failed (rc={result.returncode}): {result.stderr.strip()[:300]}",
                      file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
