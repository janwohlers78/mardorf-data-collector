import io,json,zipfile,unittest,re
from pathlib import Path
import yaml
import tempfile,hashlib
from unittest.mock import patch
from types import SimpleNamespace
from mardorf_collector.storage.objects import LocalObjects
from mardorf_collector.storage.parents import ParentStore
from mardorf_collector.storage.archive import canonical
from mardorf_collector.wp13.model_originals_v1 import PREFIX
from mardorf_collector.wp13.native_publication_recovery_v1 import recover,REPO
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

    def test_complete_recovery_calls_keyword_only_publisher_after_canonical_restore(self):
        root=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as folder:
            backend=LocalObjects(Path(folder)/'remote');store=ParentStore(backend,prefix=PREFIX)
            f=Path(folder)/'grid';f.write_bytes(b'grid');grid=store.write_file(f,metadata={})
            def ref(key,body):return backend.put_bytes('weather/'+key,body).json()
            prepared=dict(artifact_version='wp15-native-model-prepared-v1',native_eps_new_capture=True,
                producer_release_sha256=hashlib.sha256((root/'config/dev03_wp15_model_reader_release_v1.json').read_bytes()).hexdigest(),
                proof={'status':'PASS'},catalog=ref('catalog',canonical({'parents':[],'fragments':[]})),
                native_eps_grid_original=grid.json(),native_eps_source=ref('source',b'source'),native_eps_grid_prefix=ref('prefix',b'prefix'),
                acquisition_seed=dict(all_native_sources_ready=True,payload=ref('payload',b'payload'),integrity=ref('health',b'health')))
            archive=self.zip(prepared);runtime=SimpleNamespace(backend=backend)
            class Response:
                status_code=302;headers={'Location':'https://example.invalid/blob'};content=archive
                def raise_for_status(self):pass
                def json(self):return self.value
            class Session:
                headers={}
                def get(self,url,**kwargs):
                    r=Response()
                    r.value={'status':'completed','path':'.github/workflows/collect-models.yml','head_sha':'a'*40,'run_attempt':1} if url.endswith('/runs/123') else {'artifacts':[{'name':'native-model-diagnostics-123-1','expired':False,'archive_download_url':'https://example.invalid/archive'}]}
                    return r
            def old(cmd,**kwargs):return (root/cmd[2].split(':',1)[1]).read_bytes()
            with patch('mardorf_collector.runtime.private_state.cloud',return_value=runtime),patch('mardorf_collector.wp13.native_models_v1.publish',autospec=True,return_value={'status':'PASS'}) as publisher,patch('requests.Session',return_value=Session()),patch('requests.get',return_value=Response()),patch('subprocess.run'),patch('subprocess.check_output',side_effect=old),patch.dict('os.environ',{'GITHUB_TOKEN':'test-token'}):
                directory=Path(folder)/'recovered';self.assertEqual(recover(directory,123),{'status':'PASS'})
                publisher.assert_called_once_with(cloud=runtime,directory=directory)
            recovered=json.loads((directory/'prepared.json').read_bytes())
            self.assertTrue(recovered['publication_recovery']['original_source_clocks_preserved'])
