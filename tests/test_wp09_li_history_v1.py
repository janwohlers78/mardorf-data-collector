import hashlib,json,unittest
from datetime import datetime,timezone
from unittest.mock import patch
from test_wp09_eumetsat_probe_v1 import Backend
from mardorf_collector.runtime.wp09_li_history_v1 import days,lookup,save,capture_day,PREFIX

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
        v=dict(status='ORIGINALS_COMPLETE_QC_PENDING',day_utc='2025-07-01');raw=json.dumps(v).encode();backend.data[key]=raw
        backend.head=lambda key:dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
        with patch('mardorf_collector.runtime.wp09_li_history_v1.response') as call:
            self.assertEqual(capture_day(datetime(2025,7,1,tzinfo=timezone.utc),backend,{},1),v);call.assert_not_called()

if __name__=='__main__':unittest.main()
