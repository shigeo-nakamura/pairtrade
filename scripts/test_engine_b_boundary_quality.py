#!/usr/bin/env python3
import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from engine_b_boundary_quality import analyze, HOUR, SECOND

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

    def test_conflicting_alias_snapshot_is_ambiguous(self):
        combined = json.loads(json.dumps(self.config))
        old = dict(combined['venues'][0], name='lighter_mainnet_context')
        combined['venues'].append(old)
        self.sql(T0-SECOND, 'UPDATE collector_manifest SET config_json=?', (json.dumps(combined),))
        self.event(T0-SECOND, venue='lighter_mainnet_context', bid='199', ask='201')
        self.assertIn('ambiguous_latest_snapshot', self.cell(self.report())['reasons'])

    def test_rejects_wal_input(self):
        Path(str(self.path(T0)) + '-wal').touch()
        with self.assertRaisesRegex(ValueError, 'offline copy'):
            self.report()


if __name__ == '__main__':
    unittest.main()
