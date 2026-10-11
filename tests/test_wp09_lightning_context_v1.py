import hashlib
import json
import unittest
from datetime import datetime,timezone
from mardorf_collector.runtime.wp09_lightning_context_v1 import capture,publication

XML=b'<konrad3d><head><metadata><reference_time>2026-10-11T01:00:00Z</reference_time></metadata></head><cells/></konrad3d>'
INDEX=b'KONRAD3D_20261011T010000.xml KONRAD3D_20261011T020000.xml'


class Response:
    status_code=200
    def __init__(self,raw):self.raw=raw
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def raise_for_status(self):pass
    def iter_content(self,n):yield self.raw


class LightningCaptureTests(unittest.TestCase):
    def test_latest_causal_original_two_requests_all_fields_retained(self):
        calls=[]
        def get(url,**kwargs):calls.append((url,kwargs));return Response(INDEX if len(calls)==1 else XML)
        r,o=capture(get=get,now=datetime(2026,10,11,1,10,tzinfo=timezone.utc))
        self.assertEqual(len(calls),2);self.assertTrue(calls[1][0].endswith('010000.xml'))
        self.assertEqual(o['konrad3d'],XML);self.assertTrue(r['all_original_fields_retained'])
        self.assertEqual(r['records'][1]['status'],'valid');self.assertFalse(calls[0][1]['allow_redirects'])
    def test_bad_xml_kept_but_not_promoted_as_valid(self):
        for payload in (b'<broken',XML.replace(b'01:00',b'02:00'),b'<!DOCTYPE x>'+XML):
            r,o=capture(get=lambda url,**kw:Response(INDEX if url.endswith('/') else payload),now=datetime(2026,10,11,1,10,tzinfo=timezone.utc))
            self.assertEqual(r['records'][-1]['status'],'invalid');self.assertEqual(o['konrad3d'],payload)
    def test_empty_index_no_fake_zero_original(self):
        r,o=capture(get=lambda *a,**kw:Response(b''))
        self.assertEqual(r['records'][0]['status'],'invalid');self.assertNotIn('konrad3d',o)
    def test_publisher_uses_separate_frozen_collection_and_checks_readback(self):
        from unittest.mock import patch
        with patch('mardorf_collector.runtime.wp09_lightning_context_v1.publish_originals') as publish:
            publication({}, {}, object())
            self.assertEqual(publish.call_args.kwargs,dict(domain='wp09',collection='lightning_context'))
    def test_shared_entrypoint_captures_and_publishes_source(self):
        from pathlib import Path
        s=(Path(__file__).resolve().parents[1]/'src/mardorf_collector/runtime/wp06_single_run_v1.py').read_text()
        self.assertIn('lightning_companion=lightning_capture()',s)
        self.assertIn('wp09_lightning_context_ingress_v1.json',s)


if __name__=='__main__':unittest.main()
