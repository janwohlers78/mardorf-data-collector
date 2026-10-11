import hashlib,io,json,tempfile,unittest,zipfile
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

from mardorf_collector.runtime.li_output_shadow_v1 import profile,warning_view,load,PREFIX
from mardorf_collector.runtime.thunder_warning_v1 import publish,recent_li
from test_thunder_warning_v1 import fixture
from test_wp09_eumetsat_probe_v1 import Backend


class LabelCarrierTests(unittest.TestCase):
    def control(self):
        return dict(artifact_version='wp09-li-output-label-carrier-v1',private_actions_minutes_increment=0,
            capture_window_minutes=70,maximum_products=16,warning_view_minutes=40,
            collection_start_utc='2026-10-11T00:00:00+00:00',collection_end_utc='2027-01-15T00:00:00+00:00')

    def test_finite_profile_and_expiry_preserve_default(self):
        with tempfile.TemporaryDirectory()as d:
            p=Path(d)/'config';p.mkdir();(p/'li_output_shadow_runtime_v1.json').write_text(json.dumps(self.control()))
            self.assertIsNotNone(profile(d,datetime(2026,10,11,tzinfo=timezone.utc)))
            self.assertIsNone(profile(d,datetime(2027,1,15,tzinfo=timezone.utc)))
            v=self.control();v['private_actions_minutes_increment']=1;(p/'li_output_shadow_runtime_v1.json').write_text(json.dumps(v))
            with self.assertRaisesRegex(ValueError,'carrier_profile'):profile(d,datetime(2026,10,11,tzinfo=timezone.utc))

    def test_frozen_warning_view_preserves_extra_originals_for_labels(self):
        records=[dict(quantity='catalog'),dict(quantity='LFL_old',native_end_utc='2026-10-11T11:00:00+00:00'),
                 dict(quantity='LFL_new',native_end_utc='2026-10-11T11:30:00+00:00')]
        r=dict(requested_end_utc='2026-10-11T12:00:00+00:00',records=records)
        originals={v['quantity']:b'original'for v in records};view,data=warning_view(r,originals)
        self.assertEqual([v['quantity']for v in view['records']],['catalog','LFL_new'])
        self.assertEqual(len(r['records']),3);self.assertIn('LFL_old',originals);self.assertNotIn('LFL_old',data)

    def test_shared_capture_expands_labels_within_same_deadline_and_original_profile_unchanged(self):
        def capture(a,b,*,root,environ):
            p=json.loads((Path(root)/'config/wp09_eumetsat_lfl_v2.json').read_bytes())
            self.assertEqual(p['maximum_products'],16);self.assertEqual(p['maximum_seconds'],60)
            self.assertEqual((datetime.fromisoformat(b)-datetime.fromisoformat(a)).total_seconds(),4200)
            return {'records':[]},{}
        with patch('mardorf_collector.runtime.li_output_shadow_v1.profile',return_value=self.control()),patch('mardorf_collector.runtime.thunder_warning_v1.li_capture',side_effect=capture):
            recent_li(now=datetime(2026,10,11,12,tzinfo=timezone.utc))

    def test_capsule_members_and_digest_checked_before_execution(self):
        b=io.BytesIO()
        with zipfile.ZipFile(b,'w')as z:z.writestr('../__init__.py','raise AssertionError("must_not_execute")')
        backend=Backend();ref=backend.put_bytes(PREFIX+'/label-capsules/'+hashlib.sha256(b.getvalue()).hexdigest(),b.getvalue()).json()
        with tempfile.TemporaryDirectory()as d:
            with self.assertRaisesRegex(ValueError,'capsule_members'):load(backend,ref,d)
            backend.corrupt=True
            with self.assertRaisesRegex(ValueError,'capsule_integrity'):load(backend,ref,d)

    def test_label_failure_does_not_retract_frozen_warning_or_leak_exception(self):
        r,receipts,originals=fixture();rt=SimpleNamespace(backend=Backend(),publish=Mock(return_value={'status':'published'}))
        with patch('mardorf_collector.runtime.li_output_shadow_v1.generate',side_effect=ValueError('secret_sentinel')):
            publish(r,receipts,originals,rt,capsule_ref={},provenance={'engine_sha256':'a'*64},event_id='123',commit='b'*40,li_profile=self.control())
        self.assertEqual(r['li_output_shadow']['status'],'BLOCKED');self.assertNotIn('sentinel',json.dumps(r))
        rt.publish.assert_called_once();self.assertEqual(len(r['li_output_shadow']['recent_receipts']),1)

    def test_hash_distinct_successor_capsules_never_reuse_old_imported_code(self):
        backend=Backend()
        def capsule(value):
            members={'li_shadow/__init__.py':b'from .li_native_support_v2 import VALUE\n',
                'li_shadow/li_native_support_v2.py':('VALUE='+str(value)+'\n').encode(),
                'li_shadow/li_observations_v1.py':b'', 'li_shadow/thunder_warning_v1.py':b''}
            members['li_shadow/policy.json']=json.dumps({'source_sha256':{k:hashlib.sha256(v).hexdigest()for k,v in members.items()}}).encode()
            b=io.BytesIO()
            with zipfile.ZipFile(b,'w')as z:
                for name,raw in members.items():z.writestr(name,raw)
            return backend.put_bytes(PREFIX+'/label-capsules/'+hashlib.sha256(b.getvalue()).hexdigest(),b.getvalue()).json()
        for value in(1,2):
            with tempfile.TemporaryDirectory()as directory:
                module,_=load(backend,capsule(value),directory);self.assertEqual(module.VALUE,value)

if __name__=='__main__':unittest.main()
