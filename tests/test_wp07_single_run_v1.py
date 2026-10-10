import json
import unittest
from datetime import datetime,timedelta,timezone
from mardorf_collector.runtime.wp07_single_run_v1 import spec,validate,capture,binding
from mardorf_collector.runtime.wp06_single_run_v1 import publish
import hashlib
from types import SimpleNamespace


class CompanionTests(unittest.TestCase):
    def document(self):
        run=datetime(2026,10,9,tzinfo=timezone.utc); b=binding()
        return dict(latitude=52.47803,longitude=9.36803,elevation=36.,utc_offset_seconds=0,
            hourly_units=b['units'],hourly=dict(time=[(run+timedelta(hours=i)).strftime('%Y-%m-%dT%H:%M') for i in range(73)],
                **{k:[None]+[1.]*72 for k in b['params']['hourly'].split(',')}))

    def test_exact_own_product_fields_units_and_original_missing_values(self):
        s=spec('2026-10-09T00:00');x=self.document()
        validate(json.dumps(x),s)
        self.assertEqual(s['params']['models'],'ecmwf_ifs')
        self.assertEqual(len(s['params']['hourly'].split(',')),14)
        self.assertIsNone(x['hourly']['cape'][0])
        x['hourly_units']['temperature_2m']='°F'
        with self.assertRaises(ValueError):validate(json.dumps(x),s)

    def test_wrong_product_or_run_and_failed_original_are_not_replaced(self):
        s=spec('2026-10-09T00:00');s['params']['models']='ecmwf_ifs025'
        with self.assertRaises(ValueError):validate(json.dumps(self.document()),s)
        class Response:status_code=400;content=b'not available'
        r,b=capture('2026-10-09T00:00',get=lambda *a,**k:Response())
        self.assertEqual(r['status'],'invalid');self.assertEqual(b,b'not available')

    def test_both_receipts_become_visible_in_one_verified_transaction(self):
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
                self.calls+=1;self.changes=merge(self,changes);self.refs=extra_refs(None);return dict(status='published')
        cloud=Cloud();body=b'fixture';r=dict(captured_utc='2026-10-09T06:00:00Z',source_sha256=hashlib.sha256(body).hexdigest(),status='valid')
        result=publish(r,body,cloud,companion=(r,body))
        self.assertEqual(cloud.calls,1)
        self.assertEqual(set(cloud.refs),{'config/cloud_refs/wp06_api_ingress_v1.json','config/cloud_refs/wp07_api_ingress_v1.json','config/cloud_refs/wp06_current_source_check_v1.json'})
        self.assertEqual(len(cloud.changes),4)
        self.assertEqual(result['status'],'valid')


if __name__=='__main__':unittest.main()
