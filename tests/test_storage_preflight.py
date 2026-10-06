import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from mardorf_collector.runtime import storage_preflight as preflight


class StoragePreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / 'config').mkdir()
        self.profile(True)
        self.addCleanup(self.temp.cleanup)

    def profile(self, enabled):
        (self.root / 'config/dev03_cloud_runtime_v1.json').write_text(json.dumps({
            'schema_version': 1, 'artifact_version': 'dev03-cloud-runtime-v1',
            'production_enabled': enabled}))

    def test_reads_live_root_before_success_and_never_writes(self):
        runtime = Mock()
        result = preflight.check(self.root, environ={}, factory=lambda: runtime)
        self.assertEqual(result['status'], 'PASS')
        self.assertEqual(runtime.mock_calls, [unittest.mock.call.open()])

    def test_failed_root_has_no_success_or_fallback(self):
        runtime = Mock()
        runtime.open.side_effect = RuntimeError('canonical access denied')
        with self.assertRaises(RuntimeError):
            preflight.check(self.root, environ={}, factory=lambda: runtime)
        self.assertEqual(runtime.mock_calls, [unittest.mock.call.open()])

    def test_each_invocation_checks_recovery_instead_of_latching_outage(self):
        runtime = Mock()
        runtime.open.side_effect = [RuntimeError('outage'), None]
        with self.assertRaises(RuntimeError):
            preflight.check(self.root, environ={}, factory=lambda: runtime)
        self.assertEqual(preflight.check(self.root, environ={}, factory=lambda: runtime)['status'], 'PASS')
        self.assertEqual(runtime.open.call_count, 2)

    def test_inactive_profile_needs_no_cloud_but_canary_does(self):
        self.profile(False)
        factory = Mock(return_value=Mock())
        self.assertEqual(preflight.check(self.root, environ={}, factory=factory)['status'], 'NOT_REQUIRED')
        factory.assert_not_called()
        self.assertEqual(preflight.check(self.root, environ={'CLOUD_CANARY': 'true'}, factory=factory)['status'], 'PASS')
        factory.return_value.open.assert_called_once()

    def test_missing_profile_cannot_approve_acquisition(self):
        (self.root / 'config/dev03_cloud_runtime_v1.json').unlink()
        with self.assertRaises(ValueError):
            preflight.check(self.root, environ={}, factory=Mock())

    def test_failure_is_redacted_and_returns_failure_exit_code(self):
        prior = os.getcwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, prior)
        with patch.object(preflight, 'check', side_effect=RuntimeError('SECRET provider URL')):
            with patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(self.root/'summary.md')}):
                self.assertEqual(preflight.main(), 1)
        report = (self.root/'work/storage_access_preflight.json').read_text()
        self.assertEqual(json.loads(report)['status'], 'FAIL')
        self.assertNotIn('SECRET', report + (self.root/'summary.md').read_text())

    def test_actual_workflows_gate_all_expensive_and_always_steps(self):
        import yaml
        root = Path(__file__).resolve().parents[1]
        for kind in ('models', 'svg', 'secondary'):
            steps = yaml.safe_load((root/f'.github/workflows/collect-{kind}.yml').read_text())['jobs']['collect']['steps']
            names = [step.get('name', '') for step in steps]
            index = names.index('Verify canonical storage before provider acquisition')
            gate = steps[index]
            self.assertEqual(gate['id'], 'storage_gate')
            self.assertNotIn('continue-on-error', gate)
            self.assertIn('requirements-cloud.txt', gate['run'])
            for step in steps[index+1:]:
                self.assertIn("steps.storage_gate.outcome == 'success'", step.get('if', ''), (kind, step))
            if kind == 'models':
                self.assertLess(index, names.index('Install ecCodes'))
                self.assertIn('inputs.test_mode != true', gate['if'])
                self.assertIn('inputs.cloud_canary == true', gate['if'])


if __name__ == '__main__':
    unittest.main()
