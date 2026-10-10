"""Publish existing model originals and qualified compact extracts to B2."""
import argparse
import json
import os
import time
from pathlib import Path

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import LocalObjects, ObjectRef
from .model_originals_v1 import WORK
from .model_store_v1 import ModelReader, write_catalog, digest, READINESS_VERSION

INGRESS = 'data/inbox/native_models_wp15/'
READINESS = 'data/weather_native/model_consumer_readiness_v1.json'


def prepare(directory=WORK, *, snapshot=None):
    directory = Path(directory)
    snapshot = snapshot or json.loads(Path(os.environ.get('COLLECTOR_MODEL_FILE', 'work/model_snapshot.json')).read_bytes())
    started = time.monotonic()
    print('WP15_PREPARE_BEGIN', flush=True)
    captures = [json.loads(p.read_bytes()) for p in sorted(directory.glob('*.capture.json'))]
    if not captures:
        raise ValueError('No original model responses captured')
    stages = [json.loads(p.read_bytes()) for p in directory.glob('*.stage.json')]
    if any(stage.get('capture_errors') for stage in stages) or any(directory.glob('*.error.json')):
        raise ValueError('Model original capture errors require recovery')
    backend = LocalObjects(directory / 'objects')
    root = Path(__file__).resolve().parents[3]
    release_body = (root / 'config/dev03_wp15_model_reader_release_v1.json').read_bytes()
    release = json.loads(release_body)
    for name, expected in release['artifact_sha256'].items():
        if digest((root / name).read_bytes()) != expected:
            raise ValueError('Model qualification runtime release drift')
    print('WP15_CAPTURE_INVENTORY=' + json.dumps(dict(parents=len(captures), original_bytes=sum(c['bytes'] for c in captures))), flush=True)
    ref = write_catalog(backend, captures, snapshot['spot'], ensemble=snapshot.get('ensemble_hourly_source'))
    print('WP15_CATALOG_WRITTEN', flush=True)
    proof = ModelReader(backend).verify(ref)
    prepared = {'schema_version': 1, 'artifact_version': 'wp15-native-model-prepared-v1',
        'catalog': ref.json(), 'proof': proof,
        'producer_release_sha256': digest(release_body),
        'producer_commits': sorted({item['metadata']['producer_commit'] for item in captures})}
    if snapshot.get('native_eps_source'):
        source=snapshot['native_eps_source']
        body=canonical(source)
        prepared['native_eps_source']=backend.put_bytes('weather/model-native/wp15/v1/native-eps/'+digest(body),body).json()
        prepared['native_eps_new_capture']=any(x['metadata']['acquisition_model']=='ICON-D2-EPS' for x in captures)
        if prepared['native_eps_new_capture']:
            prepared['native_eps_grid_original']=source['grid_original']
            prepared['native_eps_grid_prefix']=source['grid_prefix']
    integrity_path=directory.parent/'model_integrity.json'
    if integrity_path.is_file():
        from mardorf_collector.runtime.native_source_seed import create
        prior=None
        try:
            from mardorf_collector.runtime.private_state import cloud
            from mardorf_collector.runtime.native_source_seed import PATH
            prior_raw=cloud().read(PATH,required=False)
            prior=json.loads(prior_raw) if prior_raw else None
        except (OSError,ValueError):
            # No prior scopes can be claimed from unavailable evidence.
            pass
        prepared['acquisition_seed']=create(backend,snapshot,json.loads(integrity_path.read_bytes()),ref.json(),
            ModelReader(backend).catalog(ref),proof,prior=prior,native_point_source=prepared.get('native_eps_source'))
    (directory / 'prepared.json').write_bytes(canonical(prepared))
    print('WP15_PREPARE_END=' + json.dumps(dict(seconds=time.monotonic()-started, proof=proof)), flush=True)
    return prepared


def projection_pack(backend, local, catalog_ref, catalog):
    """Bundle unchanged compact objects using the shared immutable archive.

    Consumption order keeps large deliveries sequential with a 32-MiB reader
    cache. Original chunks stay separate and every native/Parquet hash remains.
    """
    from mardorf_collector.storage.archive import PackedArchive
    from .model_store_v1 import EXTRACT_PREFIX
    references = []
    if catalog.get('cycle_evidence'):
        references.append(ObjectRef.parse(catalog['cycle_evidence']))
    for fragment in catalog['fragments']:
        references.extend(ObjectRef.parse(fragment[kind]) for kind in ('parquet', 'native'))
    references = list(dict.fromkeys(references))
    metadata = {'artifact_version': 'wp15-native-projection-pack-v1',
        'catalog_sha256': ObjectRef.parse(catalog_ref).sha256,
        'references_sha256': digest(canonical([ref.json() for ref in references]))}
    archive = PackedArchive(backend, prefix=EXTRACT_PREFIX+'/projection', file_bytes=8*1024**2)
    files = ((f'objects/{index:08d}', local.get_bytes(ref)) for index, ref in enumerate(references))
    return archive.export(files, metadata=metadata)


def publish(*, cloud=None, directory=WORK):
    from mardorf_collector.storage.runtime import load_runtime
    from mardorf_collector.runtime.cloud_environment import environment
    runtime = cloud or load_runtime(Path.cwd(), environ=environment(Path.cwd()))
    started = time.monotonic()
    print('WP15_NATIVE_PUBLISH_BEGIN', flush=True)
    ready = json.loads(runtime.read(READINESS))
    if ready.get('artifact_version') != READINESS_VERSION or ready.get('available') is not True:
        raise ValueError('Deployed versioned native model consumer required')
    directory = Path(directory)
    prepared = json.loads((directory / 'prepared.json').read_bytes())
    if ready.get('release_sha256') != prepared.get('producer_release_sha256'):
        raise ValueError('Native model producer/consumer release mismatch')
    local = LocalObjects(directory / 'objects')
    reader = ModelReader(local)
    reader.verify(prepared['catalog'])
    catalog = reader.catalog(prepared['catalog'])
    from .model_originals_v1 import PREFIX
    from mardorf_collector.storage.parents import ParentStore
    refs = {ObjectRef.parse(prepared['catalog'])}
    if catalog.get('cycle_evidence'):
        refs.add(ObjectRef.parse(catalog['cycle_evidence']))
    parents = ParentStore(local, prefix=PREFIX)
    for item in catalog['parents']:
        refs.add(ObjectRef.parse(item['parent']))
        refs.update(ObjectRef.parse(value) for value in parents.manifest(item['parent'])['chunks'])
    for fragment in catalog['fragments']:
        refs.update(ObjectRef.parse(fragment[kind]) for kind in ('native', 'parquet'))
    if prepared.get('native_eps_source'):
        refs.add(ObjectRef.parse(prepared['native_eps_source']))
        if prepared.get('native_eps_new_capture'):
            refs.update(ObjectRef.parse(prepared[key]) for key in ('native_eps_grid_original','native_eps_grid_prefix'))
            refs.update(ObjectRef.parse(value) for value in parents.manifest(prepared['native_eps_grid_original'])['chunks'])
    seed=prepared.get('acquisition_seed')
    if seed:
        refs.update(ObjectRef.parse(seed[key]) for key in ('payload','integrity'))
    from .native_cold_reads_v1 import ColdModelReads, publish_object, publish_parallel, NativeVerifiedReads, cache_balanced_references
    cache_root = os.environ.get('MARDORF_NATIVE_OBJECT_CACHE')
    verified_reads = None
    if cache_root:
        verified_reads = NativeVerifiedReads(runtime.backend, cache_root, max_bytes=2*1024**3)
    print('WP15_OBJECT_INVENTORY=' + json.dumps(dict(objects=len(refs), bytes=sum(r.bytes for r in refs))), flush=True)
    def upload(ref):
        result = publish_object(runtime.backend, local, ref, verified_reads=verified_reads)
        if result != ref:
            raise ValueError('Original model upload identity mismatch')
    def progress(count):
        if count == 1 or count % 128 == 0 or count == len(refs):
            print('WP15_UPLOAD_PROGRESS=' + json.dumps(dict(completed=count, total=len(refs),
                seconds=time.monotonic()-started, transport=dict(runtime.backend.metrics))), flush=True)
    publish_parallel(cache_balanced_references(refs), upload, workers=48, progress=progress)
    print('WP15_OBJECTS_UPLOADED=' + json.dumps(dict(seconds=time.monotonic()-started, transport=runtime.backend.metrics)), flush=True)
    from . import model_store_v1
    with ColdModelReads(verified_reads or runtime.backend, model_store_v1, prepared['catalog']) as reads:
        proof = ModelReader(reads).verify(prepared['catalog'])
    native_eps_proof=None
    if prepared.get('native_eps_source'):
        from mardorf_collector.providers.native_eps import verify_source
        source=json.loads(runtime.backend.get_bytes(ObjectRef.parse(prepared['native_eps_source'])))
        if prepared.get('native_eps_new_capture'):
            native_eps_proof=verify_source(source,verified_reads or runtime.backend,catalog)
        elif canonical(source)!=local.get_bytes(ObjectRef.parse(prepared['native_eps_source'])):
            raise ValueError('Carried native EPS source differs')
    if seed:
        for key in ('payload','integrity'):
            ref=ObjectRef.parse(seed[key])
            if runtime.backend.get_bytes(ref)!=local.get_bytes(ref):raise ValueError('Acquisition seed cold readback differs')
    print('WP15_CANONICAL_VERIFIED=' + json.dumps(dict(seconds=time.monotonic()-started, proof=proof)), flush=True)
    packed = projection_pack(runtime.backend, local, prepared['catalog'], catalog)
    marker = dict(prepared, artifact_version='wp15-native-model-ingress-v1', cold_readback=proof,
        projection_pack=packed.json())
    if native_eps_proof:marker['native_eps_cold_readback']=native_eps_proof
    if verified_reads is not None:
        marker['canonical_readback_mode'] = 'first_seen_canonical_cold_then_full_sha_verified_immutable_cache'
    identity = digest(canonical(marker))
    name = INGRESS + identity + '.json'
    body = canonical(marker)
    promote_control={'value':True}
    def merge(current, incoming):
        previous = current.read(name, required=False)
        if previous is not None and previous != body:
            raise ValueError('Immutable model ingress collision')
        selected={path:value for path,value in incoming.items() if current.read(path,required=False)!=value}
        if seed:
            from mardorf_collector.runtime.native_source_seed import PATH
            prior=current.read(PATH,required=False)
            if prior:
                old=json.loads(prior)
                older=old['source_generated_at_utc']>seed['source_generated_at_utc'] or any(
                    old.get('source_catalogs',{}).get(model,{}).get('run_time_utc','')>scope['run_time_utc']
                    for model,scope in seed['source_catalogs'].items())
                if older:
                    selected.pop(PATH,None);promote_control['value']=False
        return selected
    changes={name:body}
    if seed:
        from mardorf_collector.runtime.native_source_seed import PATH
        changes[PATH]=canonical(seed)
    extra=None
    if seed:
        from mardorf_collector.runtime.native_source_seed import CONTROL,control
        extra=lambda snapshot:{CONTROL:control(seed,snapshot.json(),event_id=os.getenv('GITHUB_RUN_ID','local'),
            repository=runtime.config['public_repository'])} if promote_control['value'] else {}
    publication = runtime.publish(changes, metadata={'channel': 'wp15-native-model-producer'}, merge=merge,extra_refs=extra)
    result = {'status': 'PASS', 'receipt_id': identity, 'catalog': prepared['catalog'],
        'cold_readback': proof, 'publication': publication, 'objects': len(refs),
        'provider_requests_added': 0, 'weather_git_bytes_written': 0}
    result['elapsed_seconds'] = time.monotonic()-started
    result['projection_pack'] = packed.json()
    if native_eps_proof:result['native_eps_cold_readback']=native_eps_proof
    result['canonical_readback_mode'] = marker.get('canonical_readback_mode', 'independent_canonical_cold')
    if verified_reads is not None:
        result['canonical_cache_metrics'] = dict(verified_reads.cache.metrics)
    (directory / 'published.json').write_bytes(canonical(result))
    print('WP15_NATIVE_MODELS=' + json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('prepare', 'publish'))
    args = parser.parse_args()
    if args.operation == 'prepare':
        print(json.dumps(prepare()['proof']))
    else:
        publish()


if __name__ == '__main__':
    main()
