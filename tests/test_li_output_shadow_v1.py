import hashlib,io,json,tempfile,unittest,zipfile
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch

from mardorf_collector.runtime.li_output_shadow_v1 import profile,warning_view,load,generate,PREFIX
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
            members['li_shadow/policy.json']=json.dumps({'warning_engine_sha256':hashlib.sha256(b'').hexdigest(),'source_sha256':{k:hashlib.sha256(v).hexdigest()for k,v in members.items()}}).encode()
            b=io.BytesIO()
            with zipfile.ZipFile(b,'w')as z:
                for name,raw in members.items():z.writestr(name,raw)
            return backend.put_bytes(PREFIX+'/label-capsules/'+hashlib.sha256(b.getvalue()).hexdigest(),b.getvalue()).json()
        for value in(1,2):
            with tempfile.TemporaryDirectory()as directory:
                module,_=load(backend,capsule(value),directory);self.assertEqual(module.VALUE,value)

    def test_declared_decoder_hash_must_equal_verified_source_before_execution(self):
        members={'li_shadow/__init__.py':b'raise AssertionError("must_not_execute")',
            'li_shadow/li_observations_v1.py':b'', 'li_shadow/thunder_warning_v1.py':b''}
        members['li_shadow/policy.json']=json.dumps(dict(warning_engine_sha256='a'*64,
            source_sha256={k:hashlib.sha256(v).hexdigest()for k,v in members.items()})).encode()
        body=io.BytesIO()
        with zipfile.ZipFile(body,'w')as z:
            for name,raw in members.items():z.writestr(name,raw)
        backend=Backend();ref=backend.put_bytes(PREFIX+'/label-capsules/'+hashlib.sha256(body.getvalue()).hexdigest(),body.getvalue()).json()
        with tempfile.TemporaryDirectory()as directory:
            with self.assertRaisesRegex(ValueError,'decoder_claim'):load(backend,ref,directory)

    def test_label_packet_binds_loaded_capsule_and_decoder_without_changing_native_labels(self):
        backend=Backend();runtime=SimpleNamespace(backend=backend,read=Mock(return_value=None))
        capsule=dict(key=PREFIX+'/label-capsules/'+'a'*64,sha256='a'*64,bytes=1,schema_version=1)
        p=dict(capsule=capsule);hour=dict(start_epoch=1791676800,valid_epoch=1791680400,target_id='native-target',full_nominal_support=True,nominal_optical_y=0)
        packet=dict(recent_receipts=[],assembly=dict(labels=[hour]))
        hashes={'li_shadow/thunder_warning_v1.py':'b'*64,'li_shadow/li_observations_v1.py':'c'*64,'li_shadow/li_native_support_v2.py':'d'*64}
        module=SimpleNamespace(generate=Mock(return_value=packet));policy=dict(source_sha256=hashes,warning_engine_sha256='b'*64,spatial_support={})
        with patch('mardorf_collector.runtime.li_output_shadow_v1.load',return_value=(module,policy)):
            result,_=generate(runtime,p,{},None,put=backend.put_bytes,warning_engine_sha256='b'*64)
        original=json.loads(backend.get_bytes(__import__('mardorf_collector.storage.objects',fromlist=['ObjectRef']).ObjectRef.parse(result['record'])))
        self.assertEqual(original['assembly']['labels'],[hour]);self.assertEqual(original['source_binding']['capsule'],capsule)
        self.assertEqual(original['source_binding']['native_decoder_sha256'],'b'*64)
        self.assertEqual(original['artifact_version'],'wp09-LI-output-hour-label-packet-v3')

if __name__=='__main__':unittest.main()
