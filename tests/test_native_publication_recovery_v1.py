import io,json,zipfile,unittest,re
from pathlib import Path
import yaml
from mardorf_collector.wp13.native_publication_recovery_v1 import artifact_prepared

class NativeRecoveryTests(unittest.TestCase):
    def zip(self,prepared):
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:archive.writestr('model_originals_v1/prepared.json',json.dumps(prepared))
        return stream.getvalue()
    def test_incomplete_or_relabelled_source_never_enters_recovery(self):
        good=dict(artifact_version='wp15-native-model-prepared-v1',native_eps_new_capture=True,proof={'status':'PASS'},acquisition_seed={'all_native_sources_ready':True,'source_generated_at_utc':'2026-10-10T13:40:00Z'})
        got=artifact_prepared(self.zip(good));self.assertEqual(got['acquisition_seed']['source_generated_at_utc'],'2026-10-10T13:40:00Z')
        for key,value in [('proof',{'status':'FAIL'}),('native_eps_new_capture',False),('acquisition_seed',{'all_native_sources_ready':False})]:
            with self.assertRaises(ValueError):artifact_prepared(self.zip(dict(good,**{key:value})))
        with self.assertRaises(ValueError):artifact_prepared(b'0'*(8*1024**2+1))

    def test_recovery_installs_existing_locked_native_dependencies(self):
        root=Path(__file__).resolve().parents[1]
        workflow=yaml.load((root/'.github/workflows/collect-models.yml').read_text(),Loader=yaml.BaseLoader)
        job=workflow['jobs']['recover-publication']
        command=next(s['run'] for s in job['steps'] if s.get('name')=='Install native recovery runtime')
        self.assertIn('--require-hashes',command)
        names=re.findall(r'-r ([a-z0-9_.-]+)',command)
        self.assertEqual(set(names),{'requirements-runtime.txt','requirements-cloud.txt','requirements-wp13-grib.txt','requirements-wp15-models.txt'})
        self.assertTrue(all((root/n).is_file() for n in names))
