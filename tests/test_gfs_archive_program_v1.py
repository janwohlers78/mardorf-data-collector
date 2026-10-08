from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mardorf_collector.providers import gfs_archive_program_v1 as program
from mardorf_collector.storage.objects import LocalObjects


class GFSProgramTests(unittest.TestCase):
    def test_exact_registered_ten_year_coverage_and_no_repeated_prior_batches(self):
        from datetime import date, timedelta
        rows = program.ranges(1); days = []
        for row in rows:
            start, end = date.fromisoformat(row['start']), date.fromisoformat(row['end_exclusive'])
            self.assertLessEqual((end-start).days, 90)
            days.extend(start+timedelta(days=i) for i in range((end-start).days))
        self.assertEqual(len(days), 3652)
        self.assertEqual(len(set(days)), 3652)
        self.assertEqual((min(days).isoformat(), max(days).isoformat()), ('2016-10-08','2026-10-07'))
        self.assertEqual(program.ranges(3)[0]['batch'], 3)
        self.assertEqual(len(program.ranges(3)), len(rows)-2)
        for number in (0,len(rows)+1):
            with self.assertRaises(ValueError): program.ranges(number)
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory)/'registration.json'; bad.write_text('{}')
            with self.assertRaisesRegex(ValueError,'changed'): program.ranges(3,bad)

    def test_request_completion_never_claims_missing_forecast_is_available(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); backend = LocalObjects(root/'objects')
            result = dict(records=[dict(request=dict(lead_hours=None),status='captured'),
                                   dict(request=dict(lead_hours=24),status='provider_error')],
                          complete_requests=True, status_counts=dict(captured=1,provider_error=1))
            with patch.object(program,'run',return_value=result), redirect_stdout(StringIO()):
                summary = program.execute(3,root/'work',backend)
            self.assertEqual(summary['captured_forecast_files'],0)
            self.assertTrue(summary['attempted_requests_complete'])
            self.assertFalse(summary['archive_data_complete'])
            self.assertFalse(summary['scientific_release'])
            result['complete_requests'] = False
            with patch.object(program,'run',return_value=result), redirect_stdout(StringIO()):
                with self.assertRaisesRegex(ValueError,'incomplete'): program.execute(3,root/'incomplete',backend)
            self.assertTrue((root/'incomplete/result-reference.json').exists())

    def test_workflow_is_manual_serial_read_only_and_keeps_only_refs_in_artifacts(self):
        import yaml
        data = yaml.load((program.ROOT/'.github/workflows/gfs-archive-program-v1.yml').read_bytes(),Loader=yaml.BaseLoader)
        self.assertEqual(set(data['on']),{'workflow_dispatch'})
        self.assertEqual(data['permissions'],{'contents':'read'})
        self.assertEqual(data['jobs']['acquire']['strategy']['max-parallel'],'1')
        self.assertEqual(data['jobs']['acquire']['needs'],['plan','priority'])
        for job in data['jobs'].values():
            for step in job['steps']:
                if 'upload-artifact@' in step.get('uses',''):
                    self.assertEqual(step['with']['path'],'work/gfs-program/**/result-reference.json')
                self.assertNotIn('PRIVATE_REPO_TOKEN',step.get('env',{}))


if __name__ == '__main__': unittest.main()
