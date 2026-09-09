#!/usr/bin/env python3
import ast
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import engine_b_boundary_quality
from engine_b_boundary_quality import analyze, Dataset, digest, HOUR, SECOND

SCRIPT_DIR = Path(__file__).resolve().parent
SCHEMA = next(ast.literal_eval(node.value) for node in ast.parse((SCRIPT_DIR / 'engine_b_phase0.py').read_text()).body
              if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'SCHEMA' for t in node.targets))
T0 = int(datetime(2026, 9, 8, tzinfo=timezone.utc).timestamp()) * SECOND
TIMES = (T0, T0 + 6 * HOUR + 1800 * SECOND, T0 + 13 * HOUR + 1800 * SECOND)


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calendar = self.root / 'calendar.json'
        self.calendar.write_text(json.dumps({'calendar_version': 'fixture', 'sessions': {'2026-09-08': {
            'krx_is_open': True, 'us_is_open': True, 'krx_open_utc_us': TIMES[0],
            'krx_close_utc_us': TIMES[1], 'us_open_utc_us': TIMES[2]}}}))
        self.config = {'venues': [{'name': 'lighter', 'rest_url': 'https://mainnet.zklighter.elliot.ai',
                                  'ws_url': 'wss://mainnet.zklighter.elliot.ai/stream?readonly=true',
                                  'markets': [{'symbol': 'SNDK', 'market_id': 139}]}]}
        for t in TIMES:
            for hour in range((t - 900 * SECOND) // HOUR, (t + 900 * SECOND) // HOUR + 1):
                self.create(hour * HOUR)
            self.event(t - SECOND)

    def path(self, t):
        return self.root / ('engine_b_phase0_' + datetime.fromtimestamp(t / SECOND, timezone.utc).strftime('%Y%m%d_%H') + '.sqlite3')

    def create(self, t):
        path = self.path(t)
        if path.exists():
            return
        db = sqlite3.connect(path)
        db.executescript(SCHEMA)
        db.execute('INSERT INTO collector_manifest VALUES (?,?,?,?,?,?,?,?,?,?)',
                   ('run', T0, '0.4', 'test', 'a' * 40, 'b' * 64, 'c' * 64, 'test', 0, json.dumps(self.config)))
        db.execute('INSERT INTO ws_connection VALUES (?,?,?,?,?,?,?,?,?)',
                   ('connection', 'lighter', 'public', T0-HOUR, TIMES[-1], None, 'test', None, 1))
        db.commit()
        db.close()

    def event(self, t, venue='lighter', bid='99', ask='101', size='1', complete=1):
        self.create(t)
        db = sqlite3.connect(self.path(t))
        local_seq = db.execute('SELECT COALESCE(MAX(local_sequence),0)+1 FROM book_event').fetchone()[0]
        event_id = db.execute('INSERT INTO book_event(connection_session_id,venue,market_id,symbol,event_kind,exchange_sequence,local_sequence,ts_recv_us,ts_srv_us,is_complete_snapshot,api_schema_version) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                             ('connection', venue, 139, 'SNDK', 'reconstructed', '123', local_seq, t, t-1000, complete, 'test')).lastrowid
        db.executemany('INSERT INTO book_level VALUES (?,?,?,?,?)', [(event_id, 'bid', 0, bid, size), (event_id, 'ask', 0, ask, size)])
        db.commit()
        db.close()

    def sql(self, t, sql, args=()):
        db = sqlite3.connect(self.path(t))
        db.execute(sql, args)
        db.commit()
        db.close()

    def report(self, symbols=None):
        return analyze(self.root, self.calendar, '2026-09-08', '2026-09-08', symbols or ['SNDK'])

    def cell(self, report, label='t0'):
        return report['days'][0]['boundaries'][label]['symbols']['SNDK']

    def test_complete_boundaries_are_not_g0_pass(self):
        report = self.report()
        self.assertEqual(report['days'][0]['status'], 'boundary_preflight_pass')
        self.assertEqual(report['g0_2'], 'not_evaluated')
        self.assertEqual(self.cell(report)['selected']['mid'], '100')
        self.assertEqual(self.cell(report)['selected']['age_us'], SECOND)
        self.assertEqual(report['analysis_hash'], self.report()['analysis_hash'])
        self.assertTrue(all('sha256' in v for v in report['inputs'].values()))

    def test_never_uses_post_boundary_price(self):
        self.event(T0 + SECOND, bid='999', ask='1001')
        self.assertEqual(self.cell(self.report())['selected']['mid'], '100')

    def test_stale_snapshot_is_not_backfilled(self):
        self.sql(T0-SECOND, 'DELETE FROM book_event')
        self.event(T0 - 31*SECOND)
        self.event(T0 + SECOND)
        self.assertIn('missing_or_stale_complete_snapshot', self.cell(self.report())['reasons'])

    def test_latest_bad_book_does_not_fall_back_to_older_good_book(self):
        self.event(T0, bid='102', ask='101')
        self.assertTrue(any(r.startswith('invalid_book:') for r in self.cell(self.report())['reasons']))

    def test_zero_and_nonfinite_sizes_rejected(self):
        for size in ('0', 'NaN', '-1'):
            with self.subTest(size=size):
                self.event(T0, size=size)
                self.assertFalse(self.cell(self.report())['boundary_preflight_pass'])

    def test_missing_control_does_not_count_a_valid_day(self):
        self.assertEqual(self.report(['SNDK', 'USDKRW'])['days'][0]['status'], 'boundary_preflight_fail')

    def test_old_alias_verified_from_manifest(self):
        old = json.loads(json.dumps(self.config))
        old['venues'][0]['name'] = 'lighter_mainnet_context'
        for path in self.root.glob('*.sqlite3'):
            db = sqlite3.connect(path)
            db.execute('UPDATE collector_manifest SET config_json=?', (json.dumps(old),))
            db.execute("UPDATE book_event SET venue='lighter_mainnet_context'")
            db.commit()
            db.close()
        self.assertEqual(self.report()['days'][0]['status'], 'boundary_preflight_pass')

    def test_robinhood_endpoint_cannot_be_relabelled_mainnet(self):
        wrong = json.loads(json.dumps(self.config))
        wrong['venues'][0]['ws_url'] = 'wss://robinhood.example/stream'
        self.sql(T0, 'UPDATE collector_manifest SET config_json=?', (json.dumps(wrong),))
        with self.assertRaisesRegex(ValueError, 'unverified mainnet'):
            self.report()

    def test_robinhood_rows_are_not_mainnet_candidates(self):
        self.sql(T0-SECOND, "UPDATE book_event SET venue='lighter_robinhood'")
        self.assertIn('missing_or_stale_complete_snapshot', self.cell(self.report())['reasons'])

    def test_gap_after_quote_invalidates_window(self):
        self.sql(T0, 'INSERT INTO data_gap(venue,symbol,market_id,channel,ts_start_us,ts_end_us,reason) VALUES (?,?,?,?,?,?,?)',
                 ('lighter', 'SNDK', 139, 'order_book', T0, None, 'disconnect'))
        self.assertIn('known_gap_in_window', self.cell(self.report())['reasons'])

    def test_sealed_gap_evidence_is_checked(self):
        self.sql(T0, 'INSERT INTO sealed_gap_interval(interval_id,sealed_partition,venue,market_id,symbol,channel,ts_start_us,ts_end_us,reason) VALUES (?,?,?,?,?,?,?,?,?)',
                 ('gap', '20260908_00', 'lighter', 139, 'SNDK', 'connection', T0, T0+SECOND, 'recovery'))
        self.assertIn('known_gap_in_window', self.cell(self.report())['reasons'])

    def test_missing_window_file_is_visible(self):
        self.path(T0).unlink()
        self.assertIn('missing_window_partition', self.cell(self.report())['reasons'])
        self.assertTrue(any(v.get('missing') for v in self.report()['inputs'].values()))

    def test_exact_staleness_limit_is_inclusive(self):
        self.sql(T0-SECOND, 'DELETE FROM book_event')
        self.event(T0-30*SECOND)
        self.assertTrue(self.cell(self.report())['boundary_preflight_pass'])

    def test_missing_sequence_and_side_rejected(self):
        self.sql(T0-SECOND, 'UPDATE book_event SET exchange_sequence=NULL')
        self.sql(T0-SECOND, "DELETE FROM book_level WHERE side='ask'")
        reasons = self.cell(self.report())['reasons']
        self.assertIn('missing_sequence', reasons)
        self.assertTrue(any(r.startswith('invalid_book:') for r in reasons))

    def test_extreme_finite_book_values_are_an_invalid_book_not_a_crash(self):
        # Finite, and accepted by the collector's canonical_decimal(), but
        # far past what the arithmetic can hold: mid/spread/depth overflow.
        self.sql(T0-SECOND, "UPDATE book_level SET price='9e999999', size='9e999999' WHERE side='ask' AND level=0")
        reasons = self.cell(self.report())['reasons']
        self.assertTrue(any(r.startswith('invalid_book:') for r in reasons), reasons)

    def test_wrong_market_id_is_not_accepted_by_symbol(self):
        self.sql(T0-SECOND, 'UPDATE book_event SET market_id=999')
        self.assertIn('unverified_event_market', self.cell(self.report())['reasons'])

    def test_calendar_closed_day_is_not_a_sample(self):
        calendar = json.loads(self.calendar.read_text())
        calendar['sessions']['2026-09-08']['us_is_open'] = False
        self.calendar.write_text(json.dumps(calendar))
        report = self.report()
        self.assertEqual(report['days'][0]['status'], 'market_closed')
        self.assertEqual(report['inputs'], {})

    def test_non_boolean_session_flags_are_rejected(self):
        # The collector's own TradingCalendar.load refuses these, so a report
        # that quietly read 0/1 or "true" as a session state would be built
        # from a calendar the producer would not accept -- and would render
        # as a plausible `market_closed` or even a pass.
        for value in (0, 1, 'true', None):
            with self.subTest(value=value):
                calendar = json.loads(self.calendar.read_text())
                calendar['sessions']['2026-09-08']['us_is_open'] = value
                self.calendar.write_text(json.dumps(calendar))
                with self.assertRaisesRegex(ValueError, 'must be booleans'):
                    self.report()

    def test_boundary_timestamps_must_satisfy_the_producer_rule(self):
        # The same predicate the collector applies
        # (TradingCalendar._valid_timestamp_us): not merely "an int in
        # order". Ordered pre-epoch values would otherwise search 1969
        # partitions and exit 0 with an ordinary boundary_preflight_fail,
        # and an out-of-range value is not storable in the collector's own
        # SQLite column.
        for times in (
            [-3_000_000, -2_000_000, -1_000_000],
            [0, 1, 2**63],
            [1.0, 2.0, 3.0],
            [True, 2, 3],
        ):
            with self.subTest(times=times):
                calendar = json.loads(self.calendar.read_text())
                session = calendar['sessions']['2026-09-08']
                (session['krx_open_utc_us'], session['krx_close_utc_us'],
                 session['us_open_utc_us']) = times
                self.calendar.write_text(json.dumps(calendar))
                with self.assertRaisesRegex(ValueError, r'invalid \w+_utc_us'):
                    self.report()

    def test_a_one_sided_session_still_validates_the_open_side(self):
        # KRX open on a US holiday short-circuits to market_closed, so the
        # boundary branch never runs -- but TradingCalendar.load still
        # rejects the whole calendar for a malformed KRX pair, and so must
        # this. The mirror is the producer's entire session rule, not the
        # subset this run happens to read.
        for session, expected in (
            ({'krx_is_open': True, 'us_is_open': False,
              'krx_open_utc_us': None, 'krx_close_utc_us': TIMES[1]},
             r'invalid krx_open_utc_us'),
            ({'krx_is_open': True, 'us_is_open': False,
              'krx_open_utc_us': TIMES[1], 'krx_close_utc_us': TIMES[0]},
             r'krx_open_utc_us must be before krx_close_utc_us'),
            ({'krx_is_open': False, 'us_is_open': True,
              'us_open_utc_us': -1},
             r'invalid us_open_utc_us'),
        ):
            with self.subTest(session=session):
                calendar = json.loads(self.calendar.read_text())
                calendar['sessions']['2026-09-08'] = session
                self.calendar.write_text(json.dumps(calendar))
                with self.assertRaisesRegex(ValueError, expected):
                    self.report()

        # A well-formed one-sided session is an ordinary closed day.
        calendar = json.loads(self.calendar.read_text())
        calendar['sessions']['2026-09-08'] = {
            'krx_is_open': True, 'us_is_open': False,
            'krx_open_utc_us': TIMES[0], 'krx_close_utc_us': TIMES[1]}
        self.calendar.write_text(json.dumps(calendar))
        self.assertEqual(self.report()['days'][0]['status'], 'market_closed')

    def test_boundaries_must_fall_on_the_session_date(self):
        # A stale or alternate calendar whose 2026-09-08 entry carries
        # September 7's boundaries: ordered, well-formed, and wrong. The
        # partitions for that day exist in the fixture only by accident of
        # the window, so this would otherwise be recorded as a 09-08 result.
        calendar = json.loads(self.calendar.read_text())
        session = calendar['sessions']['2026-09-08']
        for key in ('krx_open_utc_us', 'krx_close_utc_us', 'us_open_utc_us'):
            session[key] -= 24 * HOUR
        self.calendar.write_text(json.dumps(calendar))
        with self.assertRaisesRegex(ValueError, 'do not fall on the session date'):
            self.report()

    def test_the_whole_calendar_is_validated_not_just_the_range(self):
        # A malformed session the requested range never visits. The
        # collector validates every entry and refuses the file as a unit, so
        # this is still a calendar it would not load.
        calendar = json.loads(self.calendar.read_text())
        calendar['sessions']['2026-12-24'] = {'krx_is_open': 'yes', 'us_is_open': False}
        self.calendar.write_text(json.dumps(calendar))
        with self.assertRaisesRegex(ValueError, '2026-12-24: krx_is_open/us_is_open must be booleans'):
            self.report()

    def test_open_connections_are_bounded(self):
        # A full 2026-2027 range touches ~1,900 hourly partitions; one open
        # connection each exhausts RLIMIT_NOFILE and the analysis fails on
        # exactly the multi-day range it is for. The hashes and manifests
        # are retained for every partition read, so bounding the handles
        # costs nothing the integrity check depends on.
        dataset = Dataset(self.root, max_open=1)
        try:
            first_hour = TIMES[0] // HOUR * HOUR
            self.assertIsNotNone(dataset.open_window([first_hour])[0])
            self.assertIsNotNone(dataset.open_window([TIMES[1] // HOUR * HOUR])[0])
            self.assertEqual(len(dataset.opened), 1)
            self.assertEqual(len(dataset.inventory), 2)
            # An evicted partition is reopened on demand and still answers.
            reopened = dataset.open_window([first_hour])[0]
            self.assertIsNotNone(reopened)
            self.assertEqual(len(dataset.opened), 1)
        finally:
            dataset.verify_and_close()

        # And the whole report still comes out the same under the bound.
        # Two is the floor here: a +/-900s window straddles at most two
        # hourly partitions, and the fixture holds four distinct ones, so
        # this run really does evict and reopen.
        with mock.patch.object(Dataset, 'MAX_OPEN', 2):
            bounded = self.report()
        self.assertEqual(bounded['days'][0]['status'], 'boundary_preflight_pass')
        self.assertEqual(bounded['days'], self.report()['days'])

    def test_an_evicted_partition_may_not_change_unnoticed(self):
        # verify_and_close covers every partition that was read, not only
        # the ones a bounded cache still holds open.
        dataset = Dataset(self.root, max_open=1)
        first_hour = TIMES[0] // HOUR * HOUR
        dataset.open_window([first_hour])
        dataset.open_window([TIMES[1] // HOUR * HOUR])
        self.assertEqual(len(dataset.opened), 1)
        self.path(first_hour).write_bytes(self.path(first_hour).read_bytes() + b'\0')
        with self.assertRaisesRegex(ValueError, 'input changed during analysis'):
            dataset.verify_and_close()

    def test_a_window_wider_than_the_bound_is_refused(self):
        dataset = Dataset(self.root, max_open=1)
        try:
            with self.assertRaisesRegex(ValueError, 'more than the 1 connections'):
                dataset.open_window([TIMES[0] // HOUR * HOUR, TIMES[1] // HOUR * HOUR])
        finally:
            dataset.verify_and_close()

    def test_the_code_hash_names_the_code_that_ran(self):
        script = engine_b_boundary_quality.__file__
        self.assertEqual(self.report()['code_sha256'], digest(Path(script)))

        # Python keeps executing the already-loaded module when the file is
        # replaced mid-run, so an end-of-run digest would name code that did
        # not produce the report.
        real, seen = engine_b_boundary_quality.digest, []

        def replaced(path):
            if str(path) == script:
                seen.append(path)
                return 'before' if len(seen) == 1 else 'after'
            return real(path)

        with mock.patch.object(engine_b_boundary_quality, 'digest', replaced):
            with self.assertRaisesRegex(ValueError, 'analysis code changed'):
                self.report()
        self.assertEqual(len(seen), 2)

    def test_market_id_must_agree_across_aliases_and_partitions(self):
        # Across aliases inside one manifest: provenance() keys on
        # (alias, symbol), so both survive it, and the boundary logic would
        # merge their events into one SNDK series.
        combined = json.loads(json.dumps(self.config))
        other = json.loads(json.dumps(self.config['venues'][0]))
        other['name'] = 'lighter_mainnet_context'
        other['markets'][0]['market_id'] = 140
        combined['venues'].append(other)
        self.sql(T0-SECOND, 'UPDATE collector_manifest SET config_json=?', (json.dumps(combined),))
        with self.assertRaisesRegex(ValueError, 'one market ID per symbol'):
            self.report()

        # Across partitions: each file's manifest is internally consistent
        # and the ID changes between hourly files.
        self.sql(T0-SECOND, 'UPDATE collector_manifest SET config_json=?', (json.dumps(self.config),))
        moved = json.loads(json.dumps(self.config))
        moved['venues'][0]['markets'][0]['market_id'] = 140
        self.sql(TIMES[1]-SECOND, 'UPDATE collector_manifest SET config_json=?', (json.dumps(moved),))
        with self.assertRaisesRegex(ValueError, 'one market ID per symbol'):
            self.report()

    def test_conflicting_alias_snapshot_is_ambiguous(self):
        combined = json.loads(json.dumps(self.config))
        old = dict(combined['venues'][0], name='lighter_mainnet_context')
        combined['venues'].append(old)
        self.sql(T0-SECOND, 'UPDATE collector_manifest SET config_json=?', (json.dumps(combined),))
        self.event(T0-SECOND, venue='lighter_mainnet_context', bid='199', ask='201')
        self.assertIn('ambiguous_latest_snapshot', self.cell(self.report())['reasons'])

    def test_rejects_symlinked_partitions(self):
        # A link to a live collector database: the WAL sits beside the
        # target, so a check beside the link misses it, SQLite opens the
        # target immutable and ignores the WAL, and the hash is stable
        # because writes are confined to it. The report would pass.
        live_dir = self.root / 'live'
        live_dir.mkdir()
        partition = self.path(T0)
        live = live_dir / partition.name
        partition.rename(live)
        (live_dir / (partition.name + '-wal')).touch()
        partition.symlink_to(live)
        with self.assertRaisesRegex(ValueError, 'not a symlink'):
            self.report()
        # Whatever it points at: a link to a genuinely closed copy is
        # refused too, because "closed offline copy" is the contract and a
        # link is not one.
        (live_dir / (partition.name + '-wal')).unlink()
        with self.assertRaisesRegex(ValueError, 'not a symlink'):
            self.report()

    def test_a_partition_swapped_for_a_symlink_mid_run_is_caught(self):
        dataset = Dataset(self.root, max_open=1)
        first_hour = T0 // HOUR * HOUR
        dataset.open_window([first_hour])
        dataset.open_window([TIMES[1] // HOUR * HOUR])  # evicts the first
        partition = self.path(first_hour)
        moved = self.root / ('moved-' + partition.name)
        partition.rename(moved)
        partition.symlink_to(moved)
        with self.assertRaisesRegex(ValueError, 'not a symlink'):
            dataset.verify_and_close()

    def test_rejects_hard_linked_partitions(self):
        # Same inode as a live database whose WAL sits beside the *other*
        # name: a regular file by every test the symlink guard makes.
        live_dir = self.root / 'live'
        live_dir.mkdir()
        partition = self.path(T0)
        live = live_dir / partition.name
        partition.rename(live)
        (live_dir / (partition.name + '-wal')).touch()
        os.link(live, partition)
        with self.assertRaisesRegex(ValueError, 'hard-linked'):
            self.report()

    def test_rejects_wal_input(self):
        Path(str(self.path(T0)) + '-wal').touch()
        with self.assertRaisesRegex(ValueError, 'offline copy'):
            self.report()


if __name__ == '__main__':
    unittest.main()
