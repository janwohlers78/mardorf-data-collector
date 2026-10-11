"""Transport, budget-isolation, immutable capsule and monotonic publication."""
import contextlib
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import yaml
from mardorf_collector.runtime.thunder_warning_due_v1 import decision
from mardorf_collector.runtime.thunder_warning_v1 import capsule, main, publish, POINTER, PREFIX, ROOT
from test_wp09_eumetsat_probe_v1 import Backend


def fixture():
    raw = b'complete_native_failed_HTTP_body_no_credentials'
    record = dict(quantity='native', source_sha256=hashlib.sha256(raw).hexdigest(), source_bytes=len(raw), status='invalid', reason='HTTP_503')
    receipts = {s: dict(records=[dict(record)]) for s in ('li', 'dwd', 'metar')}
    originals = {s: {'native': raw} for s in receipts}
    result = dict(issued_at_utc='2026-10-11T01:10:00+00:00', slot_utc='2026-10-11T01:00:00+00:00',
                  sites=[dict(site_id='steinhude-svg-v1', status='UNKNOWN')])
    return result, receipts, originals


class ThunderTransportTests(unittest.TestCase):
    def test_originals_including_failures_readback_before_cas_thin_ref_only(self):
        result, receipts, originals = fixture(); runtime = SimpleNamespace(backend=Backend(), publish=Mock(return_value={'status': 'published'}))
        out = publish(result, receipts, originals, runtime, capsule_ref={}, provenance={}, event_id='456', commit='a'*40)
        self.assertEqual(out['private_routine_actions_minutes_increment'], 0)
        args = runtime.publish.call_args; pointer = args.kwargs['extra_refs'](None)[POINTER]
        self.assertEqual(pointer['metadata']['event_id'], '456'); self.assertNotIn('sites', pointer)
        for ref in result['source_receipts'].values():
            value = json.loads(runtime.backend.get_bytes(SimpleNamespace(**ref)))
            self.assertEqual(value['records'][0]['status'], 'invalid')
            self.assertEqual(runtime.backend.get_bytes(SimpleNamespace(**value['records'][0]['original'])), originals['li']['native'])

    def test_newer_or_same_slot_never_retracted_and_later_slot_publishes(self):
        r, receipts, originals = fixture(); runtime = SimpleNamespace(backend=Backend(), publish=Mock(return_value={'status': 'published'}))
        publish(r, receipts, originals, runtime, capsule_ref={}, provenance={}, event_id='456', commit='a'*40)
        merge = runtime.publish.call_args.kwargs['merge']
        for slot in ('2026-10-11T01:00:00+00:00', '2026-10-11T01:30:00+00:00'):
            current = SimpleNamespace(read=Mock(return_value=json.dumps({'slot_utc': slot}).encode()))
            self.assertEqual(merge(current, {'new': b'data'}), {})
        current = SimpleNamespace(read=Mock(return_value=b'{"slot_utc":"2026-10-11T00:30:00+00:00"}'))
        self.assertEqual(merge(current, {'new': b'data'}), {'new': b'data'})

    def test_corrupt_readback_and_original_never_updates_git(self):
        for corrupt in (True, False):
            r, receipts, originals = fixture(); runtime = SimpleNamespace(backend=Backend(), publish=Mock())
            if corrupt: runtime.backend.corrupt = True
            else: originals['dwd']['native'] = b'wrong'
            with self.assertRaises(ValueError): publish(r, receipts, originals, runtime, capsule_ref={}, provenance={}, event_id='456', commit='a'*40)
            runtime.publish.assert_not_called()

    def test_capsule_rejects_paths_or_hash_before_private_code_executes(self):
        b = io.BytesIO()
        with zipfile.ZipFile(b, 'w') as z:
            z.writestr('../engine.py', 'raise AssertionError("must_not_execute")')
        backend = Backend(); ref = backend.put_bytes(PREFIX+'/capsules/'+hashlib.sha256(b.getvalue()).hexdigest()+'.zip', b.getvalue()).json()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'warning_capsule_members'): capsule(backend, ref, directory)
            backend.corrupt = True
            with self.assertRaisesRegex(ValueError, 'warning_capsule_integrity'): capsule(backend, ref, directory)

    def test_failure_log_does_not_disclose_private_code_tokens_or_exception_text(self):
        output = io.StringIO()
        env = dict(EUMETSAT_CONSUMER_SECRET='SECRET_SENTINEL', PRIVATE_REPO_TOKEN='TOKEN_SENTINEL')
        with contextlib.redirect_stdout(output):
            code = main(environ=env, runtime_factory=Mock(side_effect=ValueError('SECRET_SENTINEL private code path')))
        self.assertEqual(code, 1); self.assertNotIn('SENTINEL', output.getvalue()); self.assertNotIn('private code', output.getvalue())

    def test_due_gate_slot_identity_future_unverified_and_recovery_grace(self):
        now = datetime(2026, 10, 11, 1, 30, tzinfo=timezone.utc)
        control = dict(artifact_version='svg-thunder-warning-control-v1', generated_at_utc='2026-10-11T01:17:00Z',
            bundle_ready=True, readback_verified=True, snapshot={'sha256': 'a'*64}, metadata={'receipt_sha256': 'a'*64}, receipt_sha256='a'*64)
        self.assertTrue(decision(control, now)[0]); self.assertFalse(decision(control, now, recovery=True)[0])
        self.assertTrue(decision(control, now.replace(hour=2), recovery=True)[0])
        self.assertFalse(decision(control, now.replace(minute=20))[0])
        control['receipt_sha256'] = 'b'*64; self.assertEqual(decision(control, now)[1], 'unverified_warning_control')
        control['receipt_sha256'] = 'a'*64; control['generated_at_utc'] = '2026-10-11T02:17:00Z'
        self.assertEqual(decision(control, now)[1], 'unverified_warning_control')

    def test_public_single_job_30min_route_excludes_normal_source_and_private_jobs(self):
        w = yaml.safe_load((ROOT/'.github/workflows/wp06-single-run-daily.yml').read_bytes()); on = w.get('on', w.get(True))
        self.assertIn({'cron': '17,47 * * * *'}, on['schedule']); self.assertFalse(on['workflow_dispatch']['inputs']['li_warning']['default'])
        self.assertEqual(set(w['jobs']), {'capture'}); self.assertEqual(w['jobs']['capture']['runs-on'], 'ubuntu-latest'); self.assertEqual(w['jobs']['capture']['timeout-minutes'], 5)
        steps = w['jobs']['capture']['steps']; ordinary = next(s for s in steps if s.get('name') == 'Capture original and publish verified receipt')
        self.assertIn("steps.mode.outputs.warning != 'true'", ordinary['if'])
        warning = next(s for s in steps if s.get('name') == 'Issue private experimental multi-site thunder warning')
        self.assertIn("steps.warning_due.outputs.due == 'true'", warning['if']); self.assertNotIn('${{', warning['run'])
        text = (ROOT/'src/mardorf_collector/runtime/thunder_warning_v1.py').read_text()
        self.assertNotIn('workflow_dispatch', text); self.assertNotIn('upload-artifact', text)
        watchdog = (ROOT/'.github/workflows/collector-watchdog.yml').read_text()
        self.assertIn('thunder_warning_due_v1 --recovery', watchdog); self.assertIn('-f li_warning=true', watchdog); self.assertIn('-f watchdog=true', watchdog)


if __name__ == '__main__': unittest.main()
