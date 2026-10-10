import unittest
from datetime import datetime,timezone,timedelta
from mardorf_collector.runtime.wp06_single_run_v1 import latest_profile_capture,promote_receipt,cycle_candidates,forecast_content_digest
from mardorf_collector.runtime.check_collection_due import current_source_due

class CurrentSourceTests(unittest.TestCase):
    def test_profiles_independently_select_actual_available_cycles_and_keep_failed_originals(self):
        now=datetime(2026,10,10,19,tzinfo=timezone.utc)
        for available in ['2026-10-10T06:00','2026-10-10T12:00']:
            calls=[]
            def capture(run):
                calls.append(run);valid=run==available
                return dict(status='valid' if valid else 'invalid',http_status=200 if valid else 400,request={'params':{'run':run}}),b'original' if valid else b'The requested model run is not available'
            r,b=latest_profile_capture(capture,now=now)
            self.assertEqual(r['request']['params']['run'],available);self.assertEqual(calls[-1],available)
            self.assertEqual(len(r['unavailable_newer_runs']),len(calls)-1);self.assertEqual(b,b'original')
            self.assertTrue(all('response_base64' in f for f in r['unavailable_newer_runs']))
    def test_rate_limit_is_visible_and_does_not_select_another_model_or_run(self):
        calls=[]
        def capture(run):calls.append(run);return dict(status='invalid',http_status=429),b'rate limit'
        r,b=latest_profile_capture(capture,now=datetime(2026,10,10,19,tzinfo=timezone.utc))
        self.assertEqual(len(calls),1);self.assertEqual(r['http_status'],429)
        self.assertEqual(len(cycle_candidates(datetime(2026,10,10,5,tzinfo=timezone.utc))),5)
    def test_recapture_clock_cannot_rewind_initialization_or_hide_valid_data(self):
        old=dict(status='valid',request={'params':{'run':'2026-10-10T12:00'}},source_sha256='a',captured_utc='2026-10-10T15:00Z')
        incoming=dict(old,captured_utc='2026-10-10T19:00Z')
        self.assertFalse(promote_receipt(old,incoming))
        self.assertTrue(promote_receipt(old,incoming,force_daily=True))
        incoming['request']={'params':{'run':'2026-10-10T00:00'}}
        self.assertFalse(promote_receipt(old,incoming,force_daily=True))
        incoming['status']='invalid';self.assertFalse(promote_receipt(None,incoming))
    def test_retry_is_bounded_and_not_suppressed_by_today_report(self):
        now=datetime(2026,10,10,19,tzinfo=timezone.utc)
        value=dict(artifact_version='wp06-source-check-control-v1',readback_verified=True,generated_at_utc=now.isoformat(),snapshot=dict(schema_version=1,key='check/a',sha256='a'*64,bytes=10))
        self.assertFalse(current_source_due(value,now+timedelta(minutes=179))[0])
        self.assertTrue(current_source_due(value,now+timedelta(minutes=180))[0])
        self.assertTrue(current_source_due(None,now)[0])
        value['generated_at_utc']=(now+timedelta(minutes=1)).isoformat();self.assertTrue(current_source_due(value,now)[0])

    def test_transport_timing_does_not_trigger_forecast_but_every_weather_field_does(self):
        import json
        body={'generationtime_ms':1.2,'hourly':{'temperature_2m':[12,None]},'hourly_units':{'temperature_2m':'°C'}}
        before=forecast_content_digest(json.dumps(body).encode());body['generationtime_ms']=9.1
        self.assertEqual(before,forecast_content_digest(json.dumps(body).encode()))
        body['hourly']['temperature_2m'][1]=13
        self.assertNotEqual(before,forecast_content_digest(json.dumps(body).encode()))
