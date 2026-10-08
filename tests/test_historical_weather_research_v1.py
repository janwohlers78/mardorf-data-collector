import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mardorf_collector.providers import historical_weather_research_v1 as research
from mardorf_collector.storage.objects import LocalObjects


class Response:
    status_code=200
    def __init__(self,body):self.body=body
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,n):yield self.body


class Session:
    def __init__(self,body):self.body=body;self.calls=[]
    def get(self,url,**kwargs):self.calls.append((url,kwargs));return Response(self.body)


class HistoricalWeatherTests(unittest.TestCase):
    def test_registered_urls_budgets_and_forecast_lead_are_fixed(self):
        tasks=research.plan('gfs-archive-probe');self.assertEqual(len(tasks),12)
        self.assertTrue(all(t['lead_hours']==30 for t in tasks))
        self.assertTrue(all(t['url'].startswith(research.GFS) for t in tasks))
        dwd=research.plan('dwd-recent');self.assertEqual(len(dwd),40)
        self.assertTrue(all('/recent/' in t['url'] for t in dwd))
        with self.assertRaises(ValueError):research.plan('arbitrary-source')

    def test_exact_original_xml_retention_no_redirect_and_oversize(self):
        task=research.plan('gfs-archive-probe')[0]
        raw=b'<gridDataset><grid name="Wind_speed_gust_surface" units="m/s" shape="time lat lon"/></gridDataset>'
        session=Session(raw);record,body=research.fetch(task,session=session)
        self.assertEqual(body,raw);self.assertEqual(record['sha256'],research.sha(raw))
        self.assertEqual(record['grid_fields'][0]['units'],'m/s')
        self.assertFalse(session.calls[0][1]['allow_redirects'])
        record,body=research.fetch(dict(task,limit_bytes=4),session=session)
        self.assertIsNone(body);self.assertEqual(record['status'],'oversize_quarantined')

    def test_registration_before_fetch_and_cold_packed_originals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);backend=LocalObjects(root/'objects');folder=root/'work'
            seen=[]
            def capture(spec):
                self.assertTrue((folder/'registration.json').exists())
                self.assertEqual(json.loads((folder/'registration.json').read_bytes())['maximum_logical_requests'],12)
                seen.append(spec)
                return dict(status='captured',bytes=3,sha256=research.sha(b'abc')),b'abc'
            with patch.object(research,'fetch',side_effect=capture),patch.object(research.time,'sleep'):
                research.run('gfs-archive-probe',folder,backend)
            index=json.loads((folder/'index.json').read_bytes())
            self.assertEqual(len(seen),12);self.assertFalse(index['production_head_updated'])
            self.assertTrue(all(r['container'] for r in index['records']))
