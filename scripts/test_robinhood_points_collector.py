#!/usr/bin/env python3
"""Tests for robinhood_points_collector.py and robinhood_points_daily.py
(bot-strategy#938). The venue, KMS and the Go signer are not touched:
what is tested is everything between a venue body and a ledger row."""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import robinhood_points_collector as collector  # noqa: E402
import robinhood_points_daily as daily  # noqa: E402


def referral_body(**overrides):
    body = {
        "code": 200,
        "referrals": [{"l1_address": "0xabc", "total_points": "1"}],
        "user_total_points": 9876.25,
        "user_last_week_points": 150,
        "user_total_referral_reward_points": 0,
        "user_last_week_referral_reward_points": 0,
        "reward_point_multiplier": "1.5",
    }
    body.update(overrides)
    return body


def bodies(live=1234.5, **referral_overrides):
    return {
        "livePoints/total": {"code": 200, "total_live_points": live},
        "referral/points": referral_body(**referral_overrides),
    }


class EnvFileTests(unittest.TestCase):
    def test_reads_the_launcher_shapes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "arm.env")
            path.write_text(
                "# KMS-encrypted\n"
                "LIGHTER_PUBLIC_API_KEY=abc def==\n"
                "export LIGHTER_PRIVATE_API_KEY='xyz'\n"
                'LIGHTER_API_KEY_INDEX="4"\n'
                "\n"
                "LIGHTER_ACCOUNT_INDEX=3209  \n",
                encoding="utf-8",
            )
            env = collector.load_env(path)
        self.assertEqual(env["LIGHTER_PUBLIC_API_KEY"], "abc def==")
        self.assertEqual(env["LIGHTER_PRIVATE_API_KEY"], "xyz")
        self.assertEqual(env["LIGHTER_API_KEY_INDEX"], "4")
        self.assertEqual(env["LIGHTER_ACCOUNT_INDEX"], "3209")

    def test_require_names_the_file_and_key(self):
        with self.assertRaises(collector.CollectorError) as ctx:
            collector.require({}, "LIGHTER_ACCOUNT_INDEX", Path("/x/arm.env"))
        self.assertIn("/x/arm.env: LIGHTER_ACCOUNT_INDEX", str(ctx.exception))


class AesTests(unittest.TestCase):
    def test_mirrors_debot_utils_layout(self):
        # IV = first 16 bytes, AES-256-CBC, PKCS#7 -- encrypt with the
        # same primitives and make sure the decrypt reads it back.
        from cryptography.hazmat.primitives import padding
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        key, iv = os.urandom(32), os.urandom(16)
        padder = padding.PKCS7(128).padder()
        plain = b"\x01" * 40
        padded = padder.update(plain) + padder.finalize()
        enc = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
        blob = iv + enc.update(padded) + enc.finalize()
        # The env files carry the base64 with spaces in places.
        text = base64.b64encode(blob).decode()
        spaced = text[:10] + " " + text[10:]
        self.assertEqual(collector.aes_cbc_decrypt(key, spaced), plain)

    def test_short_blob_is_an_error(self):
        with self.assertRaises(collector.CollectorError):
            collector.aes_cbc_decrypt(b"k" * 32, base64.b64encode(b"x" * 20).decode())


class TallyTests(unittest.TestCase):
    def test_number_and_numeric_string_read_the_same(self):
        self.assertEqual(collector.parse_tally(1234.5, "f"), Decimal("1234.5"))
        self.assertEqual(collector.parse_tally("1234.5", "f"), Decimal("1234.5"))
        self.assertEqual(collector.parse_tally(7, "f"), Decimal(7))

    def test_unreadable_is_never_zero(self):
        for value in (None, "", "   ", "n/a", [1], {"a": 1}, True, "NaN", "inf"):
            with self.assertRaises(collector.CollectorError, msg=repr(value)) as ctx:
                collector.parse_tally(value, "total_live_points")
            self.assertIn("total_live_points", str(ctx.exception))

    def test_every_required_tally_is_read(self):
        tallies = collector.tallies_from_bodies(bodies())
        self.assertEqual(tallies["live_points_total"], Decimal("1234.5"))
        self.assertEqual(tallies["total_points"], Decimal("9876.25"))
        self.assertEqual(tallies["last_week_points"], Decimal(150))
        self.assertEqual(tallies["total_referral_reward_points"], Decimal(0))
        self.assertEqual(tallies["last_week_referral_reward_points"], Decimal(0))
        self.assertEqual(tallies["reward_point_multiplier"], Decimal("1.5"))

    def test_a_missing_required_tally_fails_the_arm(self):
        body = bodies()
        del body["referral/points"]["user_last_week_referral_reward_points"]
        with self.assertRaises(collector.CollectorError) as ctx:
            collector.tallies_from_bodies(body)
        self.assertIn("user_last_week_referral_reward_points", str(ctx.exception))

    def test_multiplier_is_optional_but_not_garbage(self):
        for absent in (None, "", " "):
            self.assertIsNone(
                collector.tallies_from_bodies(bodies(reward_point_multiplier=absent))
                ["reward_point_multiplier"], repr(absent))
        body = bodies(reward_point_multiplier="x2")
        with self.assertRaises(collector.CollectorError):
            collector.tallies_from_bodies(body)
        body = bodies()
        del body["referral/points"]["reward_point_multiplier"]
        self.assertIsNone(collector.tallies_from_bodies(body)["reward_point_multiplier"])


class EnvelopeTests(unittest.TestCase):
    def test_http_200_with_application_error_is_an_error(self):
        with self.assertRaises(collector.CollectorError) as ctx:
            collector.check_envelope("referral/points", 200,
                                     {"code": 21100, "message": "account not found"})
        self.assertIn("account not found", str(ctx.exception))

    def test_auth_rejection_carries_the_venue_message(self):
        with self.assertRaises(collector.CollectorError) as ctx:
            collector.check_envelope(
                "livePoints/total", 400,
                {"code": 20001,
                 "message": "invalid param : auth query param and Authorization header are empty"})
        self.assertIn("auth query param", str(ctx.exception))

    def test_http_200_without_a_code_is_a_plain_success_body(self):
        # referral/points answers without the {code, message} envelope
        # (first live run, 2026-09-16); livePoints/total answers with it.
        body = {"user_total_points": 1, "user_last_week_points": 0}
        self.assertIs(collector.check_envelope("referral/points", 200, body), body)
        with self.assertRaises(collector.CollectorError):
            collector.check_envelope("referral/points", 500, body)

    def test_non_object_is_an_error(self):
        with self.assertRaises(collector.CollectorError):
            collector.check_envelope("x", 200, [1, 2])


class RowTests(unittest.TestCase):
    def test_row_shape_and_raw_without_referees(self):
        row = collector.build_row("freq", 3209, bodies(), 1_789_534_395)
        self.assertEqual(row["ts"], "2026-09-16T04:53:15Z")
        self.assertEqual(row["arm"], "freq")
        self.assertEqual(row["account_index"], 3209)
        self.assertEqual(row["live_points_total"], 1234.5)
        self.assertEqual(row["total_points"], 9876.25)
        self.assertEqual(row["last_week_points"], 150)
        self.assertIsInstance(row["last_week_points"], int)
        self.assertEqual(row["reward_point_multiplier"], 1.5)
        self.assertNotIn("referrals", row["raw"]["referral/points"])
        self.assertEqual(row["raw"]["livePoints/total"]["total_live_points"], 1234.5)
        # JSON round trip: nothing non-serialisable slipped in.
        json.dumps(row)

    def test_append_rows_is_one_line_per_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "nested", "points_history.jsonl")
            collector.append_rows(path, [{"a": 1}, {"b": 2}])
            collector.append_rows(path, [{"c": 3}])
            lines = path.read_text().splitlines()
        self.assertEqual([json.loads(line) for line in lines], [{"a": 1}, {"b": 2}, {"c": 3}])


class MainTests(unittest.TestCase):
    """`main` with the credential and venue seams patched: one arm fails,
    the other's row is still written, the exit status says so."""

    def test_one_failing_arm_does_not_lose_the_other(self):
        with tempfile.TemporaryDirectory() as tmp:
            common = Path(tmp, "common.env")
            common.write_text("ENCRYPTED_DATA_KEY=AAAA\nAWS_REGION=ap-northeast-1\n")
            freq = Path(tmp, "freq.env")
            freq.write_text("LIGHTER_PUBLIC_API_KEY=x\nLIGHTER_PRIVATE_API_KEY=y\n"
                            "LIGHTER_API_KEY_INDEX=4\nLIGHTER_ACCOUNT_INDEX=3209\n")
            b = Path(tmp, "b.env")
            b.write_text("LIGHTER_PUBLIC_API_KEY=x\nLIGHTER_PRIVATE_API_KEY=y\n"
                         "LIGHTER_API_KEY_INDEX=5\nLIGHTER_ACCOUNT_INDEX=281474976710500\n")
            out = Path(tmp, "points_history.jsonl")

            fetch_calls = []

            def fake_fetch(base_url, token, api_key_public, account_index):
                fetch_calls.append((base_url, token, api_key_public, account_index))
                if account_index == 3209:
                    return bodies(live=10)
                raise collector.CollectorError("livePoints/total: HTTP 401, code 20013: bad")

            fake_signer = mock.Mock()
            fake_signer.auth_token.return_value = "tok"
            with mock.patch.object(collector, "kms_decrypt_data_key", return_value=b"k" * 32), \
                    mock.patch.object(collector, "aes_cbc_decrypt",
                                      side_effect=lambda key, ct: b"p" * 40), \
                    mock.patch.object(collector, "Signer", return_value=fake_signer), \
                    mock.patch.object(collector, "fetch_points", side_effect=fake_fetch), \
                    mock.patch.object(collector.time, "time", return_value=1_789_534_395.0):
                rc = collector.main([
                    "--arm", f"freq:{freq}", "--arm", f"b:{b}", "--common", str(common),
                    "--libsigner", "/nonexistent.so", "--out", str(out)])
            rows = [json.loads(line) for line in out.read_text().splitlines()]

        self.assertEqual(rc, 1)
        self.assertEqual([row["arm"] for row in rows], ["freq"])
        self.assertEqual(rows[0]["live_points_total"], 10)
        # The client was registered for freq's key pair with the Robinhood
        # signing chain id, and the token minted for that pair.
        fake_signer.create_client.assert_any_call(
            collector.DEFAULT_BASE_URL, ("p" * 40).encode().hex(),
            collector.ROBINHOOD_SIGNING_CHAIN_ID, 4, 3209)
        fake_signer.auth_token.assert_any_call(1_789_534_395 + collector.TOKEN_TTL_SECS, 4, 3209)
        # Both keys reach the venue hex-encoded, as the bot's
        # decrypt_data_with_kms(.., output_as_hex=true) produces them.
        self.assertEqual(fetch_calls[0][2], ("p" * 40).encode().hex())

    def test_region_follows_the_launcher_source_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            common = Path(tmp, "common.env"); common.write_text("AWS_REGION=eu-central-1\n")
            debot = Path(tmp, "debot.env"); debot.write_text("AWS_REGION=ap-northeast-1\n")
            arm = Path(tmp, "arm.env"); arm.write_text("LIGHTER_ACCOUNT_INDEX=1\n")
            missing = Path(tmp, "nope.env")
            self.assertEqual(collector.resolve_region([common, debot, arm], {}), "ap-northeast-1")
            self.assertEqual(collector.resolve_region([common], {}), "eu-central-1")
            self.assertEqual(collector.resolve_region([arm, missing], {}), "eu-central-1")
            self.assertEqual(collector.resolve_region([arm], {"AWS_REGION": "us-east-1"}),
                             "us-east-1")
            # A file's setting overrides the process env, as `set -a; source` does.
            self.assertEqual(collector.resolve_region([debot], {"AWS_REGION": "us-east-1"}),
                             "ap-northeast-1")


class DailyTests(unittest.TestCase):
    @staticmethod
    def snap(arm, ts_unix, live, total):
        return {"arm": arm, "ts_unix": ts_unix, "live_points_total": live,
                "total_points": total}

    def test_differences_the_last_snapshot_of_consecutive_days(self):
        day = 86_400
        d0 = 1_789_430_400  # 2026-09-15 00:00Z
        rows = [
            self.snap("freq", d0 + 100, 100, 1000),
            self.snap("freq", d0 + 3600, 110, 1000),        # later on 09-15: this one closes it
            self.snap("freq", d0 + day + 3600, 130, 1000),  # 09-16
            self.snap("freq", d0 + 2 * day + 10, 131, 1500),  # 09-17
            self.snap("b", d0 + 100, 5, 50),
            self.snap("b", d0 + day + 100, 7, 50),
        ]
        latest = daily.last_snapshot_per_day(rows, "live_points_total")
        out, notes = daily.daily_points(latest, "live_points_total")
        self.assertEqual(
            [(r["date"], r["arm"], r["points"]) for r in out],
            [("2026-09-16", "b", 2.0), ("2026-09-16", "freq", 20.0), ("2026-09-17", "freq", 1.0)])
        self.assertTrue(all(r["tally"] == "live_points_total" for r in out))
        # The first day of each arm has no opening tally and is named.
        self.assertEqual(len([n for n in notes if "2026-09-15" in n]), 2)

        latest = daily.last_snapshot_per_day(rows, "total_points")
        out, _ = daily.daily_points(latest, "total_points")
        self.assertEqual([(r["date"], r["arm"], r["points"]) for r in out],
                         [("2026-09-16", "b", 0.0), ("2026-09-16", "freq", 0.0),
                          ("2026-09-17", "freq", 500.0)])

    def test_a_gap_day_is_skipped_not_lumped(self):
        day = 86_400
        d0 = 1_789_430_400
        rows = [self.snap("freq", d0 + 10, 100, 0), self.snap("freq", d0 + 2 * day + 10, 160, 0)]
        out, notes = daily.daily_points(
            daily.last_snapshot_per_day(rows, "live_points_total"), "live_points_total")
        self.assertEqual(out, [])
        self.assertTrue(any("2026-09-17: no snapshot on 2026-09-16" in n for n in notes))

    def test_a_negative_day_is_skipped_and_flagged_and_rebaselines(self):
        # The ledger's load_points rejects a negative row and with it the
        # whole file, so a revoked day must not be written; the revised
        # tally is still the next day's opening.
        day = 86_400
        d0 = 1_789_430_400
        rows = [self.snap("freq", d0 + 10, 100, 0), self.snap("freq", d0 + day + 10, 90, 0),
                self.snap("freq", d0 + 2 * day + 10, 95, 0)]
        out, notes = daily.daily_points(
            daily.last_snapshot_per_day(rows, "live_points_total"), "live_points_total")
        self.assertEqual([(r["date"], r["points"]) for r in out], [("2026-09-17", 5.0)])
        self.assertTrue(any("2026-09-16" in n and "fell by 10" in n for n in notes))

    def test_damaged_snapshots_are_errors(self):
        with self.assertRaises(daily.PointsDailyError):
            daily.last_snapshot_per_day([{"arm": "freq", "ts_unix": 1, "total_points": 1}],
                                        "live_points_total")
        with self.assertRaises(daily.PointsDailyError):
            daily.last_snapshot_per_day([{"arm": "freq", "ts_unix": "1", "live_points_total": 1}],
                                        "live_points_total")
        with self.assertRaises(daily.PointsDailyError):
            daily.last_snapshot_per_day([{"ts_unix": 1, "live_points_total": 1}],
                                        "live_points_total")

    def test_main_writes_the_ledger_shape(self):
        day = 86_400
        d0 = 1_789_430_400
        with tempfile.TemporaryDirectory() as tmp:
            history = Path(tmp, "h.jsonl")
            history.write_text("".join(json.dumps(r) + "\n" for r in [
                self.snap("freq", d0 + 10, 100, 0), self.snap("freq", d0 + day + 10, 125, 0)]))
            out = Path(tmp, "points.jsonl")
            rc = daily.main([str(history), "--tally", "live_points_total", "--out", str(out)])
            rows = [json.loads(line) for line in out.read_text().splitlines()]
        self.assertEqual(rc, 0)
        self.assertEqual(rows, [{"arm": "freq", "closing_ts_unix": d0 + day + 10,
                                 "date": "2026-09-16", "points": 25.0,
                                 "tally": "live_points_total"}])

    def test_torn_last_line_is_tolerated_interior_damage_is_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = Path(tmp, "h.jsonl")
            history.write_text('{"arm":"freq","ts_unix":1,"live_points_total":1}\n{"arm":')
            self.assertEqual(len(daily.read_snapshots(history)), 1)
            history.write_text('{"arm":\n{"arm":"freq","ts_unix":1,"live_points_total":1}\n')
            with self.assertRaises(daily.PointsDailyError):
                daily.read_snapshots(history)


if __name__ == "__main__":
    unittest.main()
