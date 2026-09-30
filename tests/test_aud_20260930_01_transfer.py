import base64
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import push_private as transfer
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
import validate_prep09_layout as layout_gate


class TransferAuditTests(unittest.TestCase):
    def item(self):
        return {'monotonic_guard':{'path':'latest.json','field':'generated_at_utc',
                                   'incoming_time':'2026-09-30T06:00:00Z'}}

    def test_only_missing_pointer_is_allowed_without_verified_existing_time(self):
        with patch.object(transfer,'content_meta',return_value=None):
            self.assertTrue(transfer.monotonic_allows('repo',self.item(),{},ref='parent'))
        for raw in [b'not-json',b'{}',b'[]',b'{"other":1}',
                    b'{"generated_at_utc":"2026-10-01T00:00:00Z","generated_at_utc":"2026-09-01T00:00:00Z"}']:
            meta={'content':base64.b64encode(raw).decode()}
            with self.subTest(raw=raw),patch.object(transfer,'content_meta',return_value=meta):
                with self.assertRaises((ValueError,RuntimeError)):
                    transfer.monotonic_allows('repo',self.item(),{},ref='parent')

    def test_unavailable_existing_bytes_and_invalid_base64_fail_closed(self):
        for meta in [{'encoding':'none','content':''},{'content':'%%%'}]:
            with self.subTest(meta=meta),self.assertRaises(RuntimeError):
                transfer.decoded_json_content(meta)

    def test_parent_bound_valid_newer_and_stale_pointer_behavior_is_preserved(self):
        for stamp,allowed in [('2026-09-29T06:00:00Z',True),('2026-10-01T06:00:00Z',False)]:
            meta={'content':base64.b64encode(json.dumps({'generated_at_utc':stamp}).encode()).decode()}
            with patch.object(transfer,'content_meta',return_value=meta) as call:
                self.assertEqual(transfer.monotonic_allows('repo',self.item(),{},ref='fixed-parent'),allowed)
                self.assertEqual(call.call_args.kwargs['ref'],'fixed-parent')

    def test_v3_pointer_loader_cannot_convert_corruption_into_absence(self):
        import mardorf_collector.transfer.push_private_v3 as v3
        meta={'content':base64.b64encode(b'invalid').decode()}
        with patch.object(transfer,'content_meta',return_value=meta),self.assertRaises(RuntimeError):
            v3._current_json('repo','latest.json',{},ref='parent')

    def test_corrected_package_sources_and_frozen_bridges_are_verified_in_ci(self):
        self.assertEqual(layout_gate.validate(ROOT)['status'],'PASS')
