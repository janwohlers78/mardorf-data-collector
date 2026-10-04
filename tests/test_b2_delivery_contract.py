"""Both consumers reject incomplete original-delivery evidence before use."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from mardorf_collector.storage.archive import PackedArchive,canonical
from mardorf_collector.storage.objects import LocalObjects
from mardorf_collector.storage.runtime import CloudRuntime
from mardorf_collector.storage.delivery import verified_collector_delivery
from mardorf_collector.transfer.push_cloud import publish_collector
from mardorf_collector.runtime import provider_cycle_gate as gate,private_state
from test_cloud_collector_transfer import Head

class DeliveryTests(unittest.TestCase):
    def setUp(self):
        folder=tempfile.TemporaryDirectory();self.addCleanup(folder.cleanup)
        self.backend=LocalObjects(Path(folder.name)/'objects')
        base=PackedArchive(self.backend,prefix='weather/archive').export(iter([]),metadata={})
        config={'schema_version':1,'artifact_version':'dev03-cloud-runtime-v1','production_enabled':False,
                'archive_prefix':'weather/archive','public_repository':'fixture/public','private_repository':'fixture/private',
                'control_branch':'main','control_path':'config/cloud_refs/head.json','bootstrap_snapshot':base.json()}
        self.cloud=CloudRuntime(config,{},backend=self.backend,head=Head(base.json()))
        self.raw=canonical({'models':{'GFS':[{'run_time_utc':'2026-10-04T00:00:00Z'}]}})
        report={'kind':'models','generated_at_utc':'2026-10-04T01:00:00Z','input_file_present':True,
                'input_payload_sha256':hashlib.sha256(self.raw).hexdigest(),'input_payload_bytes':len(self.raw),
                'bundle_ready_for_private_revalidation':True,'error_count':0,'status':'PASS'}
        publish_collector(self.cloud,kind='models',payload=self.raw,integrity=report,integrity_md='fixture',
                          invocation={'repository':'fixture/public','sha':'a'*40,'run_id':'1','run_attempt':'1'})
        self.receipt_path='data/inbox/public_collector/transfer_receipts/models/2026/10/04/receipt_20261004T010000000000Z.json'
        self.receipt=json.loads(self.cloud.read(self.receipt_path))
    def read(self,**kw):
        return verified_collector_delivery(self.cloud.read,'models',public_repository='fixture/public',**kw)
    def test_original_payload_passes_shared_reader_and_b2_seed(self):
        self.assertEqual(self.read()[0],self.raw)
        with patch.object(private_state,'enabled',return_value=True),patch.object(private_state,'cloud',return_value=self.cloud),patch.object(gate,'_request') as git:
            latest,payload,destination,digest=gate.load_seed('fixture/private','unused')
            self.assertEqual(payload,json.loads(self.raw));self.assertEqual(digest,hashlib.sha256(self.raw).hexdigest())
            self.assertEqual(destination,latest['private_payload']['destination']);git.assert_not_called()
    def test_tampered_receipt_rejected_by_both_entrypoints_without_writes(self):
        receipt=copy.deepcopy(self.receipt);receipt['readback'][0]['sha256']='b'*64
        self.cloud.publish({self.receipt_path:canonical(receipt)},metadata={})
        before=self.backend.metrics['put_bytes']
        with self.assertRaises(ValueError):self.read()
        with patch.object(private_state,'enabled',return_value=True),patch.object(private_state,'cloud',return_value=self.cloud),patch.object(gate,'_request') as git:
            with self.assertRaises(ValueError):gate.load_seed('fixture/private','unused')
            git.assert_not_called()
        self.assertEqual(self.backend.metrics['put_bytes'],before)
    def test_latest_integrity_cannot_disagree_with_immutable_report(self):
        path='data/inbox/public_collector/integrity/models/latest_success.json'
        report=json.loads(self.cloud.read(path));report['sources']={'GFS':{'forged':True}}
        self.cloud.publish({path:canonical(report)},metadata={})
        with self.assertRaises(ValueError):self.read()
    def test_size_budget_rejected_before_decompression(self):
        with self.assertRaises(ValueError):self.read(max_bytes=len(self.raw)-1)
    def test_duplicate_proofs_rejected(self):
        receipt=copy.deepcopy(self.receipt);receipt['readback'].append(receipt['readback'][0])
        self.cloud.publish({self.receipt_path:canonical(receipt)},metadata={})
        with self.assertRaises(ValueError):self.read()
    def test_wrong_producer_rejected(self):
        with self.assertRaises(ValueError):verified_collector_delivery(self.cloud.read,'models',public_repository='other/repo')
