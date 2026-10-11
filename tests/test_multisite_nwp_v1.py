import hashlib
import json
from datetime import datetime,timedelta,timezone
from pathlib import Path
from types import SimpleNamespace
import unittest

from mardorf_collector.providers.multisite_nwp_v1 import binding,spec,validate,capture,publication
from mardorf_collector.runtime.wp06_single_run_v1 import publish


class MultisiteOriginalTests(unittest.TestCase):
    def document(self):
        b=binding();units=json.loads((Path(__file__).resolve().parents[1]/'config/wp07_single_run_v1.json').read_bytes())['units']
        run=datetime(2026,10,11,tzinfo=timezone.utc)
        return [dict(latitude=s['latitude'],longitude=s['longitude'],elevation=100.,utc_offset_seconds=0,
            hourly_units=units,hourly=dict(time=[(run+timedelta(hours=i)).strftime('%Y-%m-%dT%H:%M') for i in range(121)],
                **{name:[None]+[1.]*120 for name in b['params']['hourly'].split(',')})) for s in b['sites']]

    def test_one_request_two_real_sites_all_fields_and_unknown_retained(self):
        d=self.document();d[0]['hourly']['new_field']=[None]*121
        raw=json.dumps(d).encode();documents=validate(raw,spec('2026-10-11T00:00'))
        self.assertEqual(len(documents),2);self.assertIn('new_field',documents[0]['hourly'])
        self.assertEqual(spec('2026-10-11T00:00')['params']['latitude'],'52.47371,52.25169')

    def test_swapped_sites_wrong_clock_unit_or_shape_rejected(self):
        for kind in ('swap','time','unit','shape'):
            d=self.document()
            if kind=='swap':d.reverse()
            if kind=='time':d[0]['hourly']['time'][1]='2026-10-11T03:00'
            if kind=='unit':d[1]['hourly_units']['temperature_2m']='F'
            if kind=='shape':d[1]['hourly']['wind_speed_10m'].pop()
            with self.assertRaises(ValueError):validate(json.dumps(d),spec('2026-10-11T00:00'))

    def test_http_failure_retained_once_without_substitution(self):
        class Response:status_code=429;content=b'{"error":true,"reason":"quota"}'
        calls=[]
        def get(*a,**k):calls.append(k);return Response()
        r,b=capture('2026-10-11T00:00',get=get)
        self.assertEqual(len(calls),1);self.assertEqual(b,Response.content);self.assertEqual(r['status'],'invalid')

    def test_current_capture_keeps_frozen_controls_and_one_transaction(self):
        class Backend:
            def __init__(self):self.values={}
            def put_bytes(self,key,raw):
                self.values[key]=raw;sha=hashlib.sha256(raw).hexdigest()
                return SimpleNamespace(key=key,sha256=sha,bytes=len(raw),json=lambda:dict(key=key,sha256=sha,bytes=len(raw),schema_version=1))
            def get_bytes(self,ref):return self.values[ref.key]
        class Cloud:
            def __init__(self):self.backend=Backend();self.calls=0
            def read(self,*a,**k):return None
            def publish(self,changes,*,metadata,merge,extra_refs):
                self.calls+=1;self.changes=merge(self,changes);self.refs=extra_refs(None);return {'status':'published'}
        cloud=Cloud();raw=b'fixture';r=dict(captured_utc='2026-10-11T06:00:00Z',source_sha256=hashlib.sha256(raw).hexdigest(),status='valid',event_id='1')
        publish(r,raw,cloud,companion=(r,raw),multisite_companion=(r,raw))
        self.assertEqual(cloud.calls,1)
        self.assertIn('config/cloud_refs/wp17_multisite_nwp_v1.json',cloud.refs)
        self.assertIn('config/cloud_refs/wp06_api_ingress_v1.json',cloud.refs)
        self.assertIn('config/cloud_refs/wp07_api_ingress_v1.json',cloud.refs)


if __name__=='__main__':unittest.main()
