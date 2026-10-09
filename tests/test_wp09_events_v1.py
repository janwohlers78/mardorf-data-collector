import hashlib,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from mardorf_collector.runtime.wp09_events_v1 import validate,capture,publication

class Events(unittest.TestCase):
    def binding(self):return dict(maximum_daily_records=512)
    def test_original_extra_fields_and_raw_report_remain_intact(self):
        raw=json.dumps([dict(icaoId='ETNW',obsTime=1791501600,rawOb='METAR ETNW 082320Z 24005KT CAVOK 10/08 Q1005',future_extra={'native':1})]).encode()
        self.assertIn('future_extra',validate(raw,'ETNW',self.binding()))
        with self.assertRaises(ValueError):validate(raw,'EDDV',self.binding())
        with self.assertRaises(ValueError):validate(json.dumps([dict(icaoId='ETNW',obsTime=True,rawOb='x')]).encode(),'ETNW',self.binding())
    def test_failed_and_empty_source_responses_never_create_negative_labels(self):
        class Response:
            status_code=200
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def raise_for_status(self):pass
            def iter_content(self,*args):yield b'[]'
        receipt,originals=capture(get=lambda *a,**kw:Response())
        self.assertEqual(len(originals),2)
        self.assertTrue(all(x==b'[]' for x in originals.values()))
        self.assertNotIn('labels',receipt)
        self.assertTrue(all(x['available_utc']==x['captured_utc'] for x in receipt['records']))
    def test_shared_publication_uses_separate_immutable_event_namespace(self):
        class Backend:
            def __init__(self):self.data={}
            def put_bytes(self,key,raw):
                self.data[key]=raw;sha=hashlib.sha256(raw).hexdigest()
                return SimpleNamespace(key=key,sha256=sha,json=lambda:dict(key=key,sha256=sha,bytes=len(raw),schema_version=1))
            def get_bytes(self,ref):return self.data[ref.key]
        backend=Backend();raw=b'[]';receipt=dict(captured_utc='2026-10-09T16:00:00Z',records=[dict(quantity='ETNW',source_bytes=2,source_sha256=hashlib.sha256(raw).hexdigest(),status='valid')])
        paths,pointer=publication(receipt,{'ETNW':raw},backend)
        self.assertIn('data/inbox/wp09_events/latest.json',paths)
        self.assertTrue(any('/2026/10/09/' in p for p in paths))
        self.assertEqual(pointer['kind'],'wp09_events');self.assertTrue(pointer['readback_verified'])
        self.assertNotIn('records',pointer);self.assertTrue(all('/wp09-events/' in k for k in backend.data))
