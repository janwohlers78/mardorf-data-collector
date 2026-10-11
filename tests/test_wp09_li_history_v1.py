import hashlib,json,unittest
from datetime import datetime,timezone
from unittest.mock import patch
from test_wp09_eumetsat_probe_v1 import Backend
from mardorf_collector.runtime.wp09_li_history_v1 import days,lookup,save,capture_day,PREFIX

class Backend(Backend):
    def list_page(self,prefix,limit=10,cursor=None):
        return dict(keys=sorted(k for k in self.data if k.startswith(prefix))[:limit],cursor=None)
    def head(self,key):
        raw=self.data.get(key)
        return None if raw is None else dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())

class HistoryTests(unittest.TestCase):
    def test_completed_days_and_bounded_ranges(self):
        now=datetime(2026,10,11,tzinfo=timezone.utc)
        self.assertEqual(len(days('2024-07-04','2024-07-20',now)),16)
        for a,b in [('2024-07-03','2024-07-04'),('2024-07-04','2024-07-21'),('2026-10-10','2026-10-12'),('2025-01-01T00:00:00','2025-01-02')]:
            with self.assertRaises(ValueError):days(a,b,now)

    def test_incomplete_catalog_never_completes_day(self):
        backend=Backend();backend.head=lambda key:None
        catalog=json.dumps(dict(totalResults=145,features=[])).encode()
        with patch('mardorf_collector.runtime.wp09_li_history_v1.response',return_value=(catalog,'a','b')):
            with self.assertRaisesRegex(ValueError,'incomplete'):capture_day(datetime(2025,7,1,tzinfo=timezone.utc),backend,{},1)
        self.assertFalse(any('/days/' in k for k in backend.data))

    def test_corrupt_readback_never_returns_verified_reference(self):
        backend=Backend();backend.corrupt=True
        with self.assertRaises(ValueError):save(backend,b'original')

    def test_resume_reads_hash_bound_day_without_provider(self):
        backend=Backend();key=PREFIX+'/days/2025-07-01.json'
        v=dict(status='ORIGINALS_COMPLETE_QC_PENDING',day_utc='2025-07-01');raw=json.dumps(v).encode();backend.data[key+'/'+hashlib.sha256(raw).hexdigest()+'.json']=raw
        backend.head=lambda key:dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
        with patch('mardorf_collector.runtime.wp09_li_history_v1.response') as call:
            self.assertEqual(capture_day(datetime(2025,7,1,tzinfo=timezone.utc),backend,{},1),v);call.assert_not_called()

    def test_real_immutable_backend_accepts_digest_bound_day_index(self):
        import tempfile
        from pathlib import Path
        from mardorf_collector.storage.objects import LocalObjects
        with tempfile.TemporaryDirectory() as tmp:
            backend=LocalObjects(Path(tmp))
            catalog=json.dumps(dict(totalResults=0,features=[])).encode()
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response',return_value=(catalog,'a','b')):
                value=capture_day(datetime(2025,7,1,tzinfo=timezone.utc),backend,{},1)
            self.assertEqual(lookup(backend,PREFIX+'/days/2025-07-01.json'),value)
            self.assertFalse(value['detector_coverage'])

    def test_transient_stream_retry_and_integrity_failure_are_distinct(self):
        import requests,time
        from mardorf_collector.runtime.wp09_li_history_v1 import response
        with patch('mardorf_collector.runtime.wp09_li_history_v1.response_attempt',side_effect=[requests.ReadTimeout(),(b'ok','a','b')]) as call:
            self.assertEqual(response('https://api.eumetsat.int/path',{},time.monotonic()+10)[0],b'ok')
            self.assertEqual(call.call_count,2)
        with patch('mardorf_collector.runtime.wp09_li_history_v1.response_attempt',side_effect=ValueError('LI_history_native_identity')) as call:
            with self.assertRaises(ValueError):response('https://api.eumetsat.int/path',{},time.monotonic()+10)
            self.assertEqual(call.call_count,1)

    def test_only_transient_http_statuses_retry_inside_deadline(self):
        import time
        from mardorf_collector.runtime.wp09_li_history_v1 import response,ProviderHTTPFailure
        with patch('mardorf_collector.runtime.wp09_li_history_v1.response_attempt',side_effect=[ProviderHTTPFailure(503),(b'ok','a','b')]) as call,patch('mardorf_collector.runtime.wp09_li_history_v1.time.sleep'):
            self.assertEqual(response('https://api.eumetsat.int/path',{},time.monotonic()+10)[0],b'ok')
            self.assertEqual(call.call_count,2)
        for status in (401,403,404):
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response_attempt',side_effect=ProviderHTTPFailure(status)) as call:
                with self.assertRaisesRegex(ValueError,str(status)):response('https://api.eumetsat.int/path',{},time.monotonic()+10)
                self.assertEqual(call.call_count,1)

    def test_packs_preserve_each_whole_zip_hash_and_resume_without_network(self):
        import time,tempfile
        from pathlib import Path
        from mardorf_collector.storage.objects import LocalObjects,ObjectRef
        from mardorf_collector.runtime.wp09_li_history_v1 import packed_products
        feature=lambda i:dict(id=str(i),properties=dict(date='2025-07-01T00:00:00Z/2025-07-01T00:10:00Z',parentIdentifier='EO:EUM:DAT:0691',productInformation=dict(productType='MTILI2LFL'),acquisitionInformation=[dict(platform=dict(platformShortName='MTI1'))],links=dict(data=[dict(mediaType='application/zip',href='https://api.eumetsat.int/'+str(i))])))
        features=[feature(i) for i in range(17)]
        with tempfile.TemporaryDirectory() as tmp:
            backend=LocalObjects(Path(tmp));day=datetime(2025,7,1,tzinfo=timezone.utc)
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response',return_value=(b'PKwhole_native_fields','a','b')) as call,patch('mardorf_collector.runtime.wp09_li_history_v1.requests.Session') as session:
                records=packed_products(features,day,backend,{},time.monotonic()+10)
                self.assertEqual(call.call_count,17)
                self.assertLessEqual(session.call_count,4)
                self.assertTrue(all(c.kwargs['client'] is session.return_value for c in call.call_args_list))
                self.assertEqual(session.return_value.close.call_count,session.call_count)
            for record in records:
                r=record['original'];pack=backend.get_bytes(ObjectRef.parse(r['pack']));raw=pack[r['offset']:r['offset']+r['bytes']]
                self.assertEqual(raw,b'PKwhole_native_fields');self.assertEqual(hashlib.sha256(raw).hexdigest(),r['sha256'])
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response') as call:
                self.assertEqual(packed_products(features,day,backend,{},time.monotonic()+10),records);call.assert_not_called()

    def test_partial_group_preserves_successes_and_only_retries_missing_products(self):
        import time,tempfile,requests
        from pathlib import Path
        from mardorf_collector.storage.objects import LocalObjects
        from mardorf_collector.runtime.wp09_li_history_v1 import packed_products
        features=[dict(id=str(i),properties=dict(date='2025-07-01T00:00:00Z/2025-07-01T00:10:00Z',
            parentIdentifier='EO:EUM:DAT:0691',productInformation=dict(productType='MTILI2LFL'),
            acquisitionInformation=[dict(platform=dict(platformShortName='MTI1'))],
            links=dict(data=[dict(mediaType='application/zip',href='https://api.eumetsat.int/'+str(i))]))) for i in range(16)]
        with tempfile.TemporaryDirectory() as tmp:
            backend=LocalObjects(Path(tmp));day=datetime(2025,7,1,tzinfo=timezone.utc)
            def initial(url,*args,**kwargs):
                if url.endswith('/3'):raise requests.ReadTimeout()
                return b'PK'+url.encode(),'first_start','first_capture'
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response',side_effect=initial):
                with self.assertRaises(requests.ReadTimeout):packed_products(features,day,backend,{},time.monotonic()+10)
            self.assertFalse(backend.list_page(PREFIX+'/pieces/')['keys'])
            self.assertEqual(len(backend.list_page(PREFIX+'/partialpieces/')['keys']),1)
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response',return_value=(b'PKmissing','second_start','second_capture')) as call:
                records=packed_products(features,day,backend,{},time.monotonic()+10)
                self.assertEqual(call.call_count,1);self.assertTrue(call.call_args.args[0].endswith('/3'))
            self.assertEqual(len(records),16)
            self.assertTrue(all(r['captured_utc']=='first_capture' for r in records if r['product_id']!='3'))
            self.assertEqual(records[3]['captured_utc'],'second_capture')
            with patch('mardorf_collector.runtime.wp09_li_history_v1.response') as call:
                self.assertEqual(packed_products(features,day,backend,{},time.monotonic()+10),records);call.assert_not_called()

if __name__=='__main__':unittest.main()
