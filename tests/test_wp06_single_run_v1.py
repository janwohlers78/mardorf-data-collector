import json
import unittest
from datetime import datetime,timezone
from mardorf_collector.runtime.wp06_single_run_v1 import spec,validate,ALIASES,capture,latest_capture


class SingleRunTests(unittest.TestCase):
    def document(self):
        from datetime import timedelta
        run=datetime(2026,10,9,tzinfo=timezone.utc)
        x=dict(latitude=52.48,longitude=9.38,utc_offset_seconds=0,
               hourly={'time':[(run+timedelta(hours=i)).strftime('%Y-%m-%dT%H:%M') for i in range(121)]},hourly_units={})
        for alias in ALIASES:
            for field in ['wind_speed_10m','wind_gusts_10m']:
                key=field+'_'+alias;x['hourly'][key]=[None]+[5.]*120;x['hourly_units'][key]='m/s'
        return x
    def test_initialization_is_explicit_not_latest_blend(self):
        s=spec(datetime(2026,10,9,7,tzinfo=timezone.utc))
        self.assertEqual(s['params']['run'],'2026-10-09T00:00')
        validate(json.dumps(self.document()),s)
    def test_wrong_units_and_run_are_rejected(self):
        s=spec(datetime(2026,10,9,tzinfo=timezone.utc));x=self.document()
        x['hourly_units']['wind_speed_10m_gfs_global']='km/h'
        with self.assertRaises(ValueError):validate(json.dumps(x),s)
        x=self.document();x['hourly']['time'][1]='2026-10-09T02:00'
        with self.assertRaises(ValueError):validate(json.dumps(x),s)
    def test_http_failure_is_retained_not_promoted_to_valid(self):
        class Response:status_code=429;content=b'{"error":true}'
        r,b=capture(now=datetime(2026,10,9,tzinfo=timezone.utc),get=lambda *a,**k:Response())
        self.assertEqual(r['status'],'invalid');self.assertEqual(b,Response.content)
        self.assertEqual(r['availability_evidence'],'actual_source_receipt')
    def test_prior_initialization_only_on_explicit_missing_run(self):
        calls=[]
        class Response:
            status_code=400
            content=b'{"reason":"The requested model run is not available"}'
        def get(*a,**k):calls.append(k['params']['run']);return Response()
        r,b=latest_capture(now=datetime(2026,10,9,tzinfo=timezone.utc),get=get)
        self.assertEqual(calls,['2026-10-09T00:00','2026-10-08T00:00'])
        self.assertIn('unavailable_current_run',r)
