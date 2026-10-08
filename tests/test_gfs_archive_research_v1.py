from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from mardorf_collector.providers import gfs_archive_research_v1 as research
from mardorf_collector.storage.objects import LocalObjects, ObjectRef
from mardorf_collector.providers.svg_historical_research_v1 import sha


class GFSArchiveTests(unittest.TestCase):
    def test_native_knots_and_public_request_budget(self):
        tasks = research.plan('2016-10-08', '2016-10-09')
        self.assertEqual(len(tasks), 10)
        self.assertEqual([t['lead_hours'] for t in tasks[1:]], list(range(24, 49, 3)))
        for task in tasks[1:]:
            self.assertEqual(dict(task['params'])['accept'], 'netcdf3')
            self.assertEqual(len(dict(task['params'])['var'].split(',')), 8)
            self.assertNotIn('vertCoord', dict(task['params']))
            parameters = dict(task['params'])
            self.assertEqual((parameters['south'], parameters['north'], parameters['west'], parameters['east']), ('51.0','53.5','8.0','11.5'))
        for start, end in [('2016-10-08','2017-02-01'), ('2016-10-07','2016-10-08'), ('2026-10-08','2026-10-09')]:
            with self.assertRaises(ValueError): research.plan(start, end)

    def test_registration_precedes_http_and_transient_originals_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); output = root/'work'; backend = LocalObjects(root/'objects')
            seen = []
            def fetch(spec):
                self.assertTrue((output/'registration.json').exists())
                self.assertEqual(json.loads((output/'registration.json').read_bytes())['maximum_logical_requests'], 10)
                seen.append(spec)
                status = 503 if len(seen) == 1 else 200
                raw = b'initial service error' if status == 503 else b'CDF\x01exact original'
                return dict(status='captured' if status == 200 else 'provider_error', http_status=status,
                            bytes=len(raw), sha256=sha(raw)), raw
            with patch.object(research, 'fetch', side_effect=fetch), patch.object(research.time, 'sleep'), redirect_stdout(StringIO()):
                result = research.run('2023-01-15','2023-01-16', output, backend, workers=1)
            self.assertTrue(result['complete_requests']); self.assertEqual(len(seen), 11)
            self.assertFalse(result['native_admission']); self.assertFalse(result['production_head_updated'])
            record = result['records'][0]; self.assertEqual(len(record['attempts']), 2)
            for attempt in record['attempts']:
                cold = root/f"cold-{attempt['attempt']}.zip"; backend.get_file(ObjectRef.parse(attempt['container']), cold)
                with zipfile.ZipFile(cold) as archive:
                    self.assertEqual(sha(archive.read(attempt['member_path'])), attempt['sha256'])

    def test_time_budget_retains_unattempted_requests_without_claiming_complete_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ticks = iter([0]+[10]*10)
            with patch.object(research, 'fetch') as fetch, redirect_stdout(StringIO()):
                result = research.run('2023-01-15','2023-01-16', root/'work', LocalObjects(root/'objects'), workers=1, seconds=1, clock=lambda: next(ticks))
            fetch.assert_not_called(); self.assertFalse(result['complete_requests'])
            self.assertEqual(result['status_counts'], {'not_attempted_budget': 10})


if __name__ == '__main__': unittest.main()
