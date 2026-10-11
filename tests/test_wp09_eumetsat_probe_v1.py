import contextlib
import hashlib
import io
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import yaml
from mardorf_collector.runtime.wp09_eumetsat_probe_v1 import ROOT, POINTER, interval, main, publish


class Backend:
    def __init__(self):
        self.data = {}
        self.corrupt = False

    def put_bytes(self, key, body):
        self.data[key] = body
        value = dict(schema_version=1, key=key, sha256=hashlib.sha256(body).hexdigest(), bytes=len(body))
        return SimpleNamespace(**value, json=lambda: value)

    def get_bytes(self, ref):
        return b'corrupt' if self.corrupt else self.data[ref.key]


class ProbeTests(unittest.TestCase):
    def receipt(self, *, blocked=False):
        body = b'PKcomplete_original_BODY_TRAIL_manifest'
        receipt = dict(captured_utc='2026-10-10T00:20:00Z',
                       status='BLOCKED' if blocked else 'ORIGINALS_CAPTURED_QC_OPEN',
                       blocker='native_QC_spatial_and_parallax_qualification_OPEN',
                       records=[dict(quantity='LFL_test', source_sha256=hashlib.sha256(body).hexdigest(), source_bytes=len(body))])
        return receipt, {'LFL_test': body}

    def test_past_utc_hour_only(self):
        now = datetime(2026, 10, 11, tzinfo=timezone.utc)
        self.assertEqual(interval('2026-10-10T00:00:00Z', '2026-10-10T01:00:00Z', now=now)[0], '2026-10-10T00:00:00+00:00')
        for a, b in [(None, None), ('2026-10-10', '2026-10-11'),
                     ('2026-10-10T00:00:00Z', '2026-10-10T01:00:01Z'),
                     ('2026-10-10T00:00:00+02:00', '2026-10-10T01:00:00+02:00'),
                     ('2026-10-12T00:00:00Z', '2026-10-12T01:00:00Z')]:
            with self.assertRaises(ValueError): interval(a, b, now=now)

    def test_all_readbacks_before_one_cas_private_thin_reference_only(self):
        runtime = SimpleNamespace(backend=Backend(), publish=Mock())
        receipt, originals = self.receipt()
        result = publish(receipt, originals, runtime, event_id='123')
        self.assertEqual(result['status'], receipt['status'])
        changes = runtime.publish.call_args.args[0]
        self.assertEqual(len(changes), 1)
        stored = json.loads(next(iter(changes.values())))
        self.assertEqual(runtime.backend.get_bytes(SimpleNamespace(**stored['records'][0]['original'])), originals['LFL_test'])
        pointer = runtime.publish.call_args.kwargs['extra_refs'](None)[POINTER]
        self.assertFalse(pointer['bundle_ready']); self.assertFalse(pointer['detector_coverage'])
        self.assertFalse(pointer['routine_activation']); self.assertNotIn('records', pointer)
        self.assertEqual(pointer['metadata']['event_id'], '123')

    def test_bad_original_or_readback_never_publishes_cas(self):
        for bad_bytes, corrupt in [(True, False), (False, True)]:
            runtime = SimpleNamespace(backend=Backend(), publish=Mock())
            runtime.backend.corrupt = corrupt
            receipt, originals = self.receipt()
            if bad_bytes: originals['LFL_test'] = b'wrong'
            with self.assertRaises(ValueError): publish(receipt, originals, runtime)
            runtime.publish.assert_not_called()
            self.assertFalse(any('/receipts/' in k for k in runtime.backend.data))

    def test_blocked_receipt_preserved_and_job_fails_without_secret_output(self):
        runtime = SimpleNamespace(backend=Backend(), publish=Mock())
        receipt = dict(captured_utc='2026-10-10T00:20:00Z', status='BLOCKED',
                       blocker='EUMETSAT_authentication_failed', records=[])
        env = dict(LI_START_UTC='2026-10-10T00:00:00Z', LI_END_UTC='2026-10-10T00:10:00Z',
                   EUMETSAT_CONSUMER_KEY='CONFIDENTIAL_KEY', EUMETSAT_CONSUMER_SECRET='CONFIDENTIAL_SECRET')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(environ=env, capture_fn=Mock(return_value=(receipt, {})), runtime_factory=Mock(return_value=runtime))
        self.assertEqual(code, 1); runtime.publish.assert_called_once()
        self.assertNotIn('CONFIDENTIAL', output.getvalue())
        self.assertNotIn('CONFIDENTIAL', str(runtime.backend.data))

    def test_exception_details_are_never_logged(self):
        env = dict(LI_START_UTC='2026-10-10T00:00:00Z', LI_END_UTC='2026-10-10T00:10:00Z')
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(environ=env, capture_fn=Mock(side_effect=RuntimeError('Bearer SECRET')))
        self.assertEqual(code, 1); self.assertNotIn('SECRET', output.getvalue())

    def test_workflow_secrets_only_on_manual_native_route(self):
        workflow = yaml.safe_load((ROOT/'.github/workflows/wp06-single-run-daily.yml').read_text())
        trigger = workflow.get('on', workflow.get(True))
        self.assertEqual(trigger['schedule'], [{'cron': '35 5,6 * * *'}])
        self.assertFalse(trigger['workflow_dispatch']['inputs']['li_probe']['default'])
        self.assertEqual(len(workflow['jobs']), 1)
        steps = workflow['jobs']['capture']['steps']
        li = [s for s in steps if 'EUMETSAT_CONSUMER_KEY' in s.get('env', {})]
        self.assertEqual(len(li), 1)
        self.assertIn("github.event_name == 'workflow_dispatch'", li[0]['if'])
        self.assertIn('inputs.li_probe == true', li[0]['if'])
        self.assertIn('${{ inputs.li_start_utc }}', li[0]['env']['LI_START_UTC'])
        self.assertNotIn('${{', li[0]['run'])
        ordinary = next(s for s in steps if s.get('name') == 'Capture original and publish verified receipt')
        recovery = next(s for s in steps if s.get('id') == 'recovery')
        self.assertIn('inputs.li_probe != true', ordinary['if'])
        self.assertIn('inputs.li_probe != true', recovery['if'])
        for s in steps:
            if s.get('name') == 'Install pinned transport' or 'setup-python@' in s.get('uses', ''):
                self.assertIn('inputs.li_probe == true', s['if'])


if __name__ == '__main__':
    unittest.main()
