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
    def test_failed_http_original_is_retained_and_cannot_be_a_zero_label(self):
        response=Response(b'provider unavailable');response.status_code=503
        r,o=capture(get=lambda *a,**kw:response)
        self.assertEqual(r['records'][0]['status'],'invalid')
        self.assertEqual(r['records'][0]['http_status'],503)
        self.assertEqual(o['index'],b'provider unavailable')
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
    def test_external_namespace_publication_checks_bytes_and_readback(self):
        from types import SimpleNamespace
        class Backend:
            def __init__(self):self.data={};self.corrupt=False
            def put_bytes(self,key,body):
                self.data[key]=body;sha=hashlib.sha256(body).hexdigest()
                return SimpleNamespace(key=key,json=lambda:dict(key=key,sha256=sha,bytes=len(body),schema_version=1))
            def get_bytes(self,ref):return b'broken' if self.corrupt else self.data[ref.key]
        receipt=dict(captured_utc='2026-10-11T01:05:00Z',records=[dict(quantity='konrad3d',status='valid',source_sha256=hashlib.sha256(XML).hexdigest(),source_bytes=len(XML))])
        backend=Backend();paths,pointer=publication(receipt,{'konrad3d':XML},backend)
        self.assertIn('data/inbox/wp09_lightning_context/latest.json',paths)
        self.assertNotIn('data/inbox/wp09_events/latest.json',paths)
        stored=json.loads(paths['data/inbox/wp09_lightning_context/latest.json'])
        self.assertEqual(backend.data[stored['records'][0]['original']['key']],XML)
        self.assertNotIn('records',pointer);self.assertTrue(pointer['readback_verified'])
        failed=dict(receipt,records=[dict(receipt['records'][0],status='invalid')])
        _,bad_pointer=publication(failed,{'konrad3d':XML},backend)
        self.assertFalse(bad_pointer['bundle_ready'])
        only_index=dict(receipt,records=[dict(receipt['records'][0],quantity='index')])
        _,index_pointer=publication(only_index,{'index':XML},backend)
        self.assertFalse(index_pointer['bundle_ready'])
        backend.corrupt=True
        with self.assertRaises(ValueError):publication(receipt,{'konrad3d':XML},backend)
        backend.corrupt=False
        with self.assertRaises(ValueError):publication(receipt,{'konrad3d':b'wrong'},backend)


if __name__=='__main__':unittest.main()
