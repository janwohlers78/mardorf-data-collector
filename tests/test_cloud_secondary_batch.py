import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from mardorf_collector.storage.objects import LocalObjects, ObjectError
from mardorf_collector.storage.archive import PackedArchive
from mardorf_collector.storage.runtime import CloudRuntime
from mardorf_collector.transfer.push_cloud import publish_collector
from mardorf_collector.transfer.finalize_cloud_secondary import publish_secondary
from test_cloud_collector_transfer import Head


class SecondaryTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        backend=LocalObjects(Path(self.directory.name)/'objects')
        base=PackedArchive(backend,prefix='weather/archive').export(iter([]),metadata={})
        config={'schema_version':1,'artifact_version':'dev03-cloud-runtime-v1','production_enabled':False,
                'archive_prefix':'weather/archive','public_repository':'fixture/public','private_repository':'fixture/private',
                'control_branch':'main','control_path':'config/cloud_refs/head.json','bootstrap_snapshot':base.json()}
        self.head=Head(base.json());self.runtime=CloudRuntime(config,{},backend=backend,head=self.head)
        self.invocation={'repository':'fixture/public','sha':'a'*40,'run_id':'123','run_attempt':'1'}

    def deliver(self, kind, clock, invocation=None):
        raw=json.dumps({'station':kind,'fixture':True}).encode()
        report={'kind':kind,'generated_at_utc':clock,'input_file_present':True,
                'input_payload_sha256':hashlib.sha256(raw).hexdigest(),'input_payload_bytes':len(raw),
                'bundle_ready_for_private_revalidation':True,'error_count':0,'status':'PASS'}
        publish_collector(self.runtime,kind=kind,payload=raw,integrity=report,integrity_md='fixture',
                          invocation=invocation or self.invocation)
        return report

    def test_pair_uses_immutable_receipts_replays_and_preserves_later_batch(self):
        reports={kind:self.deliver(kind,'2000-01-01T12:00:00.000000Z') for kind in ('wunstorf','etnw')}
        # Independent newer child must not replace the immutable child of this invocation.
        self.deliver('wunstorf','2000-01-02T12:00:00.000000Z',dict(self.invocation,run_id='other'))
        publish_secondary(self.runtime,reports,self.invocation)
        path='data/inbox/public_collector/transfer_receipts/secondary/latest.json'
        old=json.loads(self.runtime.read(path))
        self.assertEqual(old['children']['wunstorf']['source_generated_at_utc'],reports['wunstorf']['generated_at_utc'])
        self.assertIsNone(old['children']['wunstorf']['verified_data_commit_sha'])
        self.assertEqual(publish_secondary(self.runtime,reports,self.invocation)['status'],'unchanged')
        newer={kind:self.deliver(kind,'2000-01-03T12:00:00.000000Z') for kind in ('wunstorf','etnw')}
        publish_secondary(self.runtime,newer,self.invocation)
        current=self.runtime.read(path)
        # A previously unfinalized older pair can publish history but cannot rewind latest or thin ref.
        middle={kind:self.deliver(kind,'2000-01-02T18:00:00.000000Z') for kind in ('wunstorf','etnw')}
        publish_secondary(self.runtime,middle,self.invocation)
        self.assertEqual(self.runtime.read(path),current)
        self.assertEqual(self.head.extra[-1],{})

    def test_missing_or_wrong_invocation_never_publishes_partial_batch(self):
        reports={'wunstorf':self.deliver('wunstorf','2000-01-01T12:00:00Z')}
        before=self.head.ref
        with self.assertRaises(ObjectError):publish_secondary(self.runtime,reports,self.invocation)
        self.assertEqual(self.head.ref,before)
        reports['etnw']=self.deliver('etnw','2000-01-01T12:01:00Z',dict(self.invocation,run_attempt='2'))
        before=self.head.ref
        with self.assertRaises(ObjectError):publish_secondary(self.runtime,reports,self.invocation)
        self.assertEqual(self.head.ref,before)
        self.assertIsNone(self.runtime.read('data/inbox/public_collector/transfer_receipts/secondary/latest.json',required=False))

    def test_corrupt_source_proof_blocks_batch(self):
        reports={kind:self.deliver(kind,'2000-01-01T12:00:00Z') for kind in ('wunstorf','etnw')}
        receipt_path='data/inbox/public_collector/transfer_receipts/wunstorf/2000/01/01/receipt_20000101T120000000000Z.json'
        receipt=json.loads(self.runtime.read(receipt_path));receipt['readback'][0]['sha256']='0'*64
        original=self.runtime.read
        from unittest.mock import patch
        with patch.object(self.runtime,'read',side_effect=lambda path,**kw: json.dumps(receipt).encode() if path==receipt_path else original(path,**kw)):
            before=self.head.ref
            with self.assertRaises(ObjectError):publish_secondary(self.runtime,reports,self.invocation)
            self.assertEqual(self.head.ref,before)
