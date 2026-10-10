"""Bounded manual publication recovery; no provider acquisition or new clocks."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import zipfile
import requests
from mardorf_collector.storage.objects import LocalObjects,ObjectRef
from mardorf_collector.storage.parents import ParentStore
from mardorf_collector.storage.archive import canonical
from .model_originals_v1 import WORK,PREFIX

REPO='janwohlers78/mardorf-data-collector'
MATH_PATHS=('src/mardorf_collector/providers/native_eps.py',
 'src/mardorf_collector/wp13/native_point_projection_v1.py',
 'src/mardorf_collector/wp13/model_store_v1.py',
 'src/mardorf_collector/wp13/native_grid_v1.py')


def artifact_prepared(body):
    if len(body)>8*1024**2:raise ValueError('Recovery ZIP budget')
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        name='model_originals_v1/prepared.json'
        info=archive.getinfo(name)
        if info.file_size>4*1024**2:raise ValueError('Recovery JSON budget')
        prepared=json.loads(archive.read(name))
    if (prepared.get('artifact_version')!='wp15-native-model-prepared-v1'
        or prepared.get('native_eps_new_capture')is not True
        or prepared.get('proof',{}).get('status')!='PASS'
        or prepared.get('acquisition_seed',{}).get('all_native_sources_ready')is not True):
        raise ValueError('Complete original acquisition proof required')
    return prepared


def recover(directory, run_id):
    if type(run_id)is not int or run_id<=0:raise ValueError('Actual source run required')
    from mardorf_collector.runtime.private_state import cloud
    from .native_models_v1 import publish
    runtime=cloud();root=Path(__file__).resolve().parents[3]
    session=requests.Session();session.headers['Authorization']='Bearer '+os.environ['GITHUB_TOKEN']
    def get(path):
        r=session.get('https://api.github.com/repos/'+REPO+'/'+path,timeout=45);r.raise_for_status();return r.json()
    run=get('actions/runs/'+str(run_id))
    if run.get('status')!='completed' or run.get('path')!='.github/workflows/collect-models.yml':
        raise ValueError('Completed source acquisition run required')
    source=run['head_sha']
    subprocess.run(['git','merge-base','--is-ancestor',source,'HEAD'],cwd=root,check=True,stdout=subprocess.DEVNULL)
    for path in MATH_PATHS:
        previous=subprocess.check_output(['git','show',source+':'+path],cwd=root)
        if previous!=(root/path).read_bytes():raise ValueError('Recovery mathematical runtime differs')
    artifacts=get('actions/runs/'+str(run_id)+'/artifacts?per_page=100')['artifacts']
    matches=[x for x in artifacts if x['name']==f'native-model-diagnostics-{run_id}-{run["run_attempt"]}' and not x['expired']]
    if len(matches)!=1:raise ValueError('Unique actual diagnostics artifact required')
    # GitHub token stays on api.github.com, never on the signed blob redirect.
    redirect=session.get(matches[0]['archive_download_url'],timeout=45,allow_redirects=False)
    if redirect.status_code!=302:raise ValueError('Authenticated artifact redirect required')
    response=requests.get(redirect.headers['Location'],timeout=60);response.raise_for_status()
    prepared=artifact_prepared(response.content)
    if prepared.get('producer_release_sha256')!=hashlib.sha256((root/'config/dev03_wp15_model_reader_release_v1.json').read_bytes()).hexdigest():
        raise ValueError('Frozen reader release differs')
    directory=Path(directory);local=LocalObjects(directory/'objects');refs=set()
    def stage(value):
        ref=ObjectRef.parse(value) if isinstance(value,dict) else value
        if ref.bytes>64*1024**2:raise ValueError('Bounded recovery object required')
        body=runtime.backend.get_bytes(ref)
        if local.put_bytes(ref.key,body)!=ref:raise ValueError('Recovery canonical identity differs')
        return ref
    stage(prepared['catalog'])
    catalog=json.loads(local.get_bytes(ObjectRef.parse(prepared['catalog'])));parents=ParentStore(local,prefix=PREFIX)
    parents_to_stage=[x['parent'] for x in catalog['parents']]+[prepared['native_eps_grid_original']]
    with ThreadPoolExecutor(max_workers=24) as pool:list(pool.map(stage,parents_to_stage))
    for value in parents_to_stage:
        refs.add(ObjectRef.parse(value));refs.update(ObjectRef.parse(v) for v in parents.manifest(value)['chunks'])
    if catalog.get('cycle_evidence'):refs.add(ObjectRef.parse(catalog['cycle_evidence']))
    for fragment in catalog['fragments']:
        refs.update(ObjectRef.parse(fragment[k]) for k in ('native','parquet'))
    refs.update(ObjectRef.parse(prepared[k]) for k in ('native_eps_source','native_eps_grid_prefix'))
    refs.update(ObjectRef.parse(prepared['acquisition_seed'][k]) for k in ('payload','integrity'))
    if sum(r.bytes for r in refs)>16*1024**3:raise ValueError('Recovery total byte budget')
    with ThreadPoolExecutor(max_workers=24) as pool:list(pool.map(stage,sorted(refs,key=lambda r:r.key)))
    prepared['publication_recovery']=dict(source_run_id=run_id,source_head=source,
        recovered_by_head=os.getenv('GITHUB_SHA'),original_source_clocks_preserved=True,
        provider_requests_added=0,all_staged_bytes_from_canonical=True)
    (directory/'prepared.json').write_bytes(canonical(prepared))
    return publish(runtime,directory)


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-id',type=int,required=True);p.add_argument('--directory',type=Path,default=WORK)
    a=p.parse_args();recover(a.directory,a.run_id)

if __name__=='__main__':main()
