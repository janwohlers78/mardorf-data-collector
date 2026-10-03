import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from mardorf_collector.runtime import private_state as state
from mardorf_collector.runtime import provider_cycle_gate as gate
from mardorf_collector.runtime import check_collection_due as due


class StateTests(unittest.TestCase):
    def test_cloud_failure_never_reads_git_weather_and_missing_optional_is_distinct(self):
        with patch.object(state, 'enabled', return_value=True), patch.object(state, 'cloud') as load, \
                patch.object(gate, '_request') as git:
            load.return_value.read.return_value = b'{"cycle":"original"}'
            self.assertEqual(gate.private_json('owner/repo', 'data/evidence.json', 'token'), {'cycle': 'original'})
            load.return_value.read.return_value = None
            self.assertIsNone(gate.private_json_optional('owner/repo', 'missing', 'token'))
            load.return_value.read.side_effect = RuntimeError('unavailable')
            with self.assertRaises(RuntimeError):
                gate.private_json_optional('owner/repo', 'data/evidence.json', 'token')
            git.assert_not_called()

    def test_disabled_profile_retains_git_route_and_profile_mismatch_is_rejected(self):
        with patch.object(state, 'enabled', return_value=False), patch.object(gate, '_request') as git:
            git.return_value = {'content': base64.b64encode(b'original').decode()}
            self.assertEqual(gate.private_bytes('owner/repo', 'data/seed.json', 'token'), b'original')
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); (root/'config').mkdir()
            path = root/'config/dev03_cloud_runtime_v1.json'
            path.write_text(json.dumps({'schema_version':1, 'artifact_version':'dev03-cloud-runtime-v1',
                                        'production_enabled':True, 'private_repository':'owner/repo'}))
            with self.assertRaises(ValueError):state.enabled('wrong/repo', root=root)
            self.assertTrue(state.enabled('owner/repo', root=root))
            path.write_text('{}')
            with self.assertRaises(ValueError):state.enabled('owner/repo', root=root)

    def test_watchdog_reads_only_verified_thin_metadata_without_cloud_sdk(self):
        ref={'key':'weather/archive/snapshots/'+'a'*64, 'sha256':'a'*64, 'bytes':100, 'schema_version':1}
        control={'schema_version':1, 'artifact_version':'cloud-collector-ref-v1', 'kind':'secondary',
                 'snapshot':ref, 'generated_at_utc':'2026-10-03T12:00:00Z',
                 'bundle_ready':True, 'readback_verified':True}
        response=Mock();response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        def body():return json.dumps({'content':base64.b64encode(json.dumps(control).encode()).decode()}).encode()
        response.read.side_effect=body
        with patch.object(state,'enabled',return_value=True), patch.object(state,'cloud') as cloud, \
                patch.object(due.urllib.request,'urlopen',return_value=response) as get:
            actual=due.fetch_latest('owner/repo','secondary','token')
            self.assertEqual(actual['source_generated_at_utc'],control['generated_at_utc'])
            self.assertIn('config/cloud_refs/collector_secondary_v1.json',get.call_args.args[0].full_url)
            cloud.assert_not_called()
            control['readback_verified']=False
            with self.assertRaises(ValueError):due.fetch_latest('owner/repo','secondary','token')
            control['readback_verified']=True;control['generated_at_utc']='2026-10-03T12:00:00'
            with self.assertRaises(ValueError):due.fetch_latest('owner/repo','secondary','token')
