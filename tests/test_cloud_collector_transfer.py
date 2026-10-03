import hashlib
from pathlib import Path
import tempfile
import unittest
import json

from mardorf_collector.storage.objects import LocalObjects,ObjectError
from mardorf_collector.storage.archive import PackedArchive,canonical
from mardorf_collector.storage.runtime import CloudRuntime
from mardorf_collector.transfer.push_cloud import publish_collector


class Head:
    def __init__(self,ref):self.ref=ref;self.extra=[]
    def read(self):return 'a'*40,{'snapshot':self.ref}
    def advance(self,parent,snapshot,**kw):self.ref=snapshot;self.extra.append(kw['extra_refs']);return True


class TransferTests(unittest.TestCase):
    def test_exact_payload_receipt_and_late_attempt_cannot_move_success_ref_backward(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);backend=LocalObjects(root/'objects')
            base=PackedArchive(backend,prefix='weather/archive').export(iter([]),metadata={})
            config={'schema_version':1,'artifact_version':'dev03-cloud-runtime-v1','production_enabled':False,
                    'archive_prefix':'weather/archive','public_repository':'fixture/public','private_repository':'fixture/private',
                    'control_branch':'main','control_path':'config/cloud_refs/head.json','bootstrap_snapshot':base.json()}
            head=Head(base.json());runtime=CloudRuntime(config,{},backend=backend,head=head)
            raw=b'{\"station\":\"fixture\",\"measurement_at\":\"2000-01-01T00:00:00Z\"}'
            report={'kind':'svg','generated_at_utc':'2000-01-02T00:00:00.000000Z','input_file_present':True,
                    'input_payload_sha256':hashlib.sha256(raw).hexdigest(),'input_payload_bytes':len(raw),
                    'bundle_ready_for_private_revalidation':True,'error_count':0,'status':'PASS'}
            publish_collector(runtime,kind='svg',payload=raw,integrity=report,integrity_md='fixture',invocation={})
            success='data/inbox/public_collector/integrity/svg/latest_success.json'
            original=runtime.read(success)
            receipt=json.loads(runtime.read('data/inbox/public_collector/transfer_receipts/svg/latest.json'))
            self.assertTrue(receipt['readback_verified']);self.assertIsNone(receipt['verified_data_commit_sha'])
            self.assertEqual(receipt['source_generated_at_utc'],report['generated_at_utc'])
            self.assertEqual(receipt['payload_destination'],json.loads(original)['private_payload']['destination'])
            replay=publish_collector(runtime,kind='svg',payload=raw,integrity=report,integrity_md='fixture',invocation={})
            self.assertEqual(replay['status'],'unchanged')
            report=dict(report,generated_at_utc='2000-01-01T00:00:00.000000Z')
            publish_collector(runtime,kind='svg',payload=raw,integrity=report,integrity_md='fixture',invocation={})
            self.assertEqual(runtime.read(success),original);self.assertEqual(head.extra[-1],{})
            wrong=dict(report,input_payload_bytes=999)
            with self.assertRaises(ObjectError):publish_collector(runtime,kind='svg',payload=raw,integrity=wrong,integrity_md='fixture',invocation={})
            inconsistent=dict(report,error_count=1)
            before=head.ref
            with self.assertRaises(ObjectError):publish_collector(runtime,kind='svg',payload=raw,integrity=inconsistent,integrity_md='fixture',invocation={})
            self.assertEqual(head.ref,before)

    def test_raw_json_noop_does_not_touch_remote_runtime(self):
        from unittest.mock import Mock
        runtime=Mock()
        raw=b'{"provider_cycle_gate":{"any_work":false,"delta_prediction":"zero"}}'
        result=publish_collector(runtime,kind='models',payload=raw,integrity={'kind':'models'},integrity_md='',invocation={})
        self.assertEqual(result['status'],'suppressed_noop');runtime.publish.assert_not_called();runtime.open.assert_not_called()

if __name__=='__main__':unittest.main()
