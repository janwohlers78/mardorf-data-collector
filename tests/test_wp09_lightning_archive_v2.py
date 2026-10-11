import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from mardorf_collector.runtime.wp09_lightning_archive_v2 import capture, ROOT
from test_wp09_lightning_context_v1 import Response


class ArchiveTests(unittest.TestCase):
    def root(self, directory):
        root=Path(directory);(root/'config').mkdir()
        for name in ('wp09_lightning_context_v1.json','wp09_lightning_archive_v2.json'):
            (root/'config'/name).write_bytes((ROOT/'config'/name).read_bytes())
        return root
    def source(self,url,**kwargs):
        self.assertFalse(kwargs['allow_redirects'])
        if url.endswith('/'):
            return Response(b'KONRAD3D_20261011T010000.xml KONRAD3D_20261011T010500.xml KONRAD3D_20261011T011000.xml')
        stamp=url.split('_')[-1].split('.')[0]
        hour,minute=stamp[9:11],stamp[11:13]
        return Response(f'<konrad3d><head><metadata><reference_time>2026-10-11T{hour}:{minute}:00Z</reference_time></metadata></head><cells/></konrad3d>'.encode())
    def test_all_intermediate_originals_and_missing_slots(self):
        r,o=capture(get=self.source,now=datetime(2026,10,11,1,15,tzinfo=timezone.utc))
        a=r['archive_v2'];self.assertEqual(a['expected_slots'],48)
        self.assertEqual(len(a['originals_by_epoch']),3)
        self.assertEqual(len(a['missing_by_epoch']),45)
        self.assertFalse(a['negative_detector_coverage'])
        self.assertEqual(sum(q=='konrad3d' for q in o),1)
        self.assertEqual(len([q for q in o if 'history' in q]),2)
    def test_failed_history_retained_not_coverage(self):
        def get(url,**kwargs):
            if url.endswith('010000.xml'):return Response(b'<broken')
            return self.source(url,**kwargs)
        r,o=capture(get=get,now=datetime(2026,10,11,1,15,tzinfo=timezone.utc))
        bad=[x for x in r['records'] if x['status']=='invalid']
        self.assertEqual(len(bad),1);self.assertEqual(o[bad[0]['quantity']],b'<broken')
        self.assertEqual(len(r['archive_v2']['originals_by_epoch']),2)
    def test_aggregate_and_count_budgets_are_explicit(self):
        with TemporaryDirectory() as d:
            root=self.root(d);p=root/'config/wp09_lightning_archive_v2.json';j=json.loads(p.read_bytes())
            j.update(maximum_history_requests=1,maximum_history_bytes=1);p.write_text(json.dumps(j))
            r,_=capture(root=root,get=self.source,now=datetime(2026,10,11,1,15,tzinfo=timezone.utc))
            a=r['archive_v2'];self.assertEqual(a['planned_history_requests'],1)
            self.assertIn('history_time_or_aggregate_budget',a['missing_by_epoch'].values())
            self.assertIn('request_count_budget',a['missing_by_epoch'].values())
            self.assertEqual(len(a['originals_by_epoch']),1)
    def test_no_history_started_after_deadline(self):
        from mardorf_collector.runtime.wp09_lightning_context_v1 import capture as live
        now=datetime(2026,10,11,1,15,tzinfo=timezone.utc)
        current=live(get=self.source,now=now)
        with patch('mardorf_collector.runtime.wp09_lightning_archive_v2.live_capture',return_value=current), \
             patch('mardorf_collector.runtime.wp09_lightning_archive_v2.time.monotonic',side_effect=[0,100,101]):
            r,_=capture(get=self.source,now=now)
        self.assertEqual(r['archive_v2']['attempted_history_slots'],0)
        self.assertIn('history_time_or_aggregate_budget',r['archive_v2']['missing_by_epoch'].values())


if __name__=='__main__':unittest.main()
