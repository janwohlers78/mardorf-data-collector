import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml
from mardorf_collector.storage.objects import LocalObjects, B2Objects, ObjectError
from test_cloud_objects import FakeS3
import hashlib
from mardorf_collector.storage.verified_reads_v1 import VerifiedReads
from mardorf_collector.wp13.native_cold_reads_v1 import publish_object

class NativeRoutineIOTests(unittest.TestCase):
    def test_uncached_existing_remote_is_read_once_and_never_uploaded(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);local=LocalObjects(root/'local');remote=LocalObjects(root/'remote')
            ref=local.put_bytes('weather/native/original',b'complete original with all fields')
            remote.put_file(ref.key,local.root/ref.key)
            cached=VerifiedReads(remote,root/'cache')
            with patch.object(remote,'get_bytes',wraps=remote.get_bytes) as read, patch.object(remote,'put_file',wraps=remote.put_file) as put:
                self.assertEqual(publish_object(remote,local,ref,verified_reads=cached),ref)
                self.assertEqual(read.call_count,1);self.assertEqual(put.call_count,0)
            self.assertEqual(cached.get_bytes(ref),b'complete original with all fields')
            (local.root/ref.key).write_bytes(b'changed source')
            empty=VerifiedReads(remote,root/'empty-cache')
            with self.assertRaisesRegex(ValueError,'source identity'):
                publish_object(remote,local,ref,verified_reads=empty)

    def test_new_b2_object_uses_three_requests_and_forces_cold_read_on_deleted_cache_hit(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);local=LocalObjects(root/'local');body=b'full original, null fields and metadata'
            ref=local.put_bytes('weather/native/'+hashlib.sha256(body).hexdigest(),body)
            client=FakeS3();remote=B2Objects(dict(B2_ENDPOINT_URL='https://s3.eu-central-003.backblazeb2.com',
                B2_REGION='eu-central-003',B2_BUCKET='fixture-bucket'),client=client)
            cached=VerifiedReads(remote,root/'cache')
            start=remote.metrics['requests']
            self.assertEqual(publish_object(remote,local,ref,verified_reads=cached),ref)
            self.assertEqual(remote.metrics['requests']-start,3)
            self.assertEqual(cached.get_bytes(ref),body)
            del client.objects[ref.key];client.read_corrupt=True
            with self.assertRaisesRegex(ObjectError,'Verified object download failed'):
                publish_object(remote,local,ref,verified_reads=cached)

    def test_native_stages_preserve_failure_gates_and_canonical_cache_authority(self):
        root=Path(__file__).resolve().parents[1]
        workflow=yaml.load((root/'.github/workflows/collect-models.yml').read_text(),Loader=yaml.BaseLoader)
        steps=workflow['jobs']['collect']['steps'];byid={s.get('id'):s for s in steps}
        self.assertIn("steps.legacy_transfer.outcome == 'success'",byid['native_prepare']['if'])
        self.assertIn('inputs.test_mode != true',byid['native_prepare']['if'])
        self.assertIn("steps.native_prepare.outcome == 'success'",byid['native_transfer']['if'])
        self.assertIn('12m python -u',byid['native_transfer']['run'])
        self.assertIn("steps.native_transfer.outcome == 'success'",byid['native_cache_proof']['if'])

if __name__=='__main__':unittest.main()
