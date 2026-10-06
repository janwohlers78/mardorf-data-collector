"""Publish existing model originals and qualified compact extracts to B2."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
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
    ref = write_catalog(backend, captures, snapshot['spot'], ensemble=snapshot.get('ensemble_hourly_source'))
    proof = ModelReader(backend).verify(ref)
    prepared = {'schema_version': 1, 'artifact_version': 'wp15-native-model-prepared-v1',
        'catalog': ref.json(), 'proof': proof,
        'producer_release_sha256': digest(release_body),
        'producer_commits': sorted({item['metadata']['producer_commit'] for item in captures})}
    (directory / 'prepared.json').write_bytes(canonical(prepared))
    return prepared


def publish(*, cloud=None, directory=WORK):
    from mardorf_collector.storage.runtime import load_runtime
    from mardorf_collector.runtime.cloud_environment import environment
    runtime = cloud or load_runtime(Path.cwd(), environ=environment(Path.cwd()))
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
    from .native_cold_reads_v1 import ColdModelReads, publish_object
    cache_root = os.environ.get('MARDORF_NATIVE_OBJECT_CACHE')
    verified_reads = None
    if cache_root:
        from mardorf_collector.storage.verified_reads_v1 import VerifiedReads
        verified_reads = VerifiedReads(runtime.backend, cache_root, max_bytes=2*1024**3)
    def upload(ref):
        result = publish_object(runtime.backend, local, ref, verified_reads=verified_reads)
        if result != ref:
            raise ValueError('Original model upload identity mismatch')
    # Same bounded concurrency already qualified for historical additive imports.
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(upload, sorted(refs, key=lambda ref: ref.key)))
    from . import model_store_v1
    with ColdModelReads(verified_reads or runtime.backend, model_store_v1, prepared['catalog']) as reads:
        proof = ModelReader(reads).verify(prepared['catalog'])
    marker = dict(prepared, artifact_version='wp15-native-model-ingress-v1', cold_readback=proof)
    if verified_reads is not None:
        marker['canonical_readback_mode'] = 'first_seen_canonical_cold_then_full_sha_verified_immutable_cache'
    identity = digest(canonical(marker))
    name = INGRESS + identity + '.json'
    body = canonical(marker)
    def merge(current, incoming):
        previous = current.read(name, required=False)
        if previous is not None and previous != body:
            raise ValueError('Immutable model ingress collision')
        return {} if previous == body else incoming
    publication = runtime.publish({name: body}, metadata={'channel': 'wp15-native-model-producer'}, merge=merge)
    result = {'status': 'PASS', 'receipt_id': identity, 'catalog': prepared['catalog'],
        'cold_readback': proof, 'publication': publication, 'objects': len(refs),
        'provider_requests_added': 0, 'weather_git_bytes_written': 0}
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
