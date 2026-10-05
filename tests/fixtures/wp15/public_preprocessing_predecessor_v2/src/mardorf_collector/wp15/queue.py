"""Sixteen bounded pending pages and atomic publication/admission bookkeeping."""
from copy import deepcopy
from datetime import datetime, timezone
import re

from ..storage.objects import ObjectRef
from ..wp13.core_v1 import canonical, digest, identify, strict_json, stamp, utc
from .prepared import PREFIX, policy, read_manifest
from .assets import ROOT

PENDING = 'data/weather_native/station_preprocessed_v1/pending/'
CATALOGS = 'data/weather_native/station_preprocessed_v1/admitted/'
READINESS = 'data/weather_native/consumer_readiness_station_preprocessed_v1.json'
ADMISSION = 'weather/station-admission/v1/'


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('Explicit publication digest required')
    return value


def pending_path(publication):
    return PENDING + identifier(publication)[0] + '.json'


def page(body, *, root=ROOT):
    value = strict_json(body) if body else {'schema_version':1, 'artifact_version':'wp15-station-pending-v1', 'entries':{}}
    if (set(value) != {'schema_version','artifact_version','entries'} or value['schema_version'] != 1
            or value['artifact_version'] != 'wp15-station-pending-v1'
            or not isinstance(value['entries'], dict)
            or len(value['entries']) > policy(root)['pending_page_entries']):
        raise ValueError('Pending page version/budget mismatch')
    for key, entry in value['entries'].items():
        identifier(key)
        ref = ObjectRef.parse(entry)
        if ref.key != PREFIX + '/manifests/' + key:
            raise ValueError('Pending entry identity mismatch')
    return value


def enqueue(cloud, publication, *, root=ROOT, changes=None, expected_state=None):
    identity = identifier(publication['publication_id'])
    reference = publication['manifest']
    doc = read_manifest(cloud.backend, reference, root=root)
    if doc['publication_id'] != identity:
        raise ValueError('Pending publication mismatch')
    path = pending_path(identity)
    extra = dict(changes or {})
    def merge(current, incoming):
        # A retry after a successful admission must not requeue or move clocks.
        completed = current.backend.head(ADMISSION + identity)
        if completed is not None:
            proof = strict_json(current.backend.get_bytes(ObjectRef(ADMISSION+identity, completed['sha256'], completed['bytes'])))
            if proof['manifest'] != reference:
                raise ValueError('Conflicting admitted publication replay')
            out = {}
        else:
            value = page(current.read(path, required=False), root=root)
            prior = value['entries'].get(identity)
            if prior is not None and prior != reference:
                raise ValueError('Pending publication collision')
            value['entries'][identity] = reference
            if len(value['entries']) > policy(root)['pending_page_entries']:
                raise ValueError('Pending backlog full; preserve progress and retry')
            out = {path:canonical(value)} if prior is None else {}
        for name, body in extra.items():
            if expected_state is not None and name in expected_state and current.read(name, required=False) != expected_state[name]:
                raise ValueError('Concurrent source cursor changed; retry')
            if current.read(name, required=False) != body:
                out[name] = body
        return out
    return cloud.publish(extra, metadata={'channel':'wp15-public-station-preprocessed'}, merge=merge)


def catalog_paths(doc):
    scope = doc['scope']
    if not re.fullmatch('[A-Za-z0-9_.-]{1,160}', scope['site_id']):
        raise ValueError('Invalid station catalog site')
    months = sorted({p['day'][:7] for p in doc['partitions']})
    return [CATALOGS + doc['configuration_sha256'] + '/' + scope['site_id'] + '/' + month + '.json' for month in months]


def admit(cloud, reference, *, expected_processor, root=ROOT):
    doc = read_manifest(cloud.backend, reference, expected_processor=expected_processor, root=root)
    identity = doc['publication_id']
    key = ADMISSION + identity
    prior = cloud.backend.head(key)
    if prior:
        ref = ObjectRef(key, prior['sha256'], prior['bytes'])
        receipt = strict_json(cloud.backend.get_bytes(ref))
        if receipt['manifest'] != reference:
            raise ValueError('Private admission replay conflict')
    else:
        at = stamp(datetime.now(timezone.utc))
        if utc(at) < utc(doc['public_verified_at_utc']):
            raise ValueError('Private first arrival precedes public verification')
        receipt = identify({'schema_version':1, 'artifact_version':'wp15-station-private-admission-v1',
            'publication_id':identity, 'manifest':reference, 'configuration_sha256':doc['configuration_sha256'],
            'processor_sha256':expected_processor, 'private_received_at_utc':at,
            'public_verified_at_utc':doc['public_verified_at_utc'], 'original_capture_at_utc':doc['envelope']['capture']['retrieved_at_utc'],
            'context':doc['envelope']['context'], 'field_count':doc['field_count'],
            'native_fields_sha256':doc['native_fields_sha256'], 'scientific_gate':'OPEN'}, 'receipt_id')
        ref = cloud.backend.put_bytes(key, canonical(receipt))
    # One private visibility commit installs all catalog references and removes
    # the exact pending entry. No weather bytes or re-normalization in private.
    path = pending_path(identity)
    def merge(current, changes):
        pending = page(current.read(path, required=False), root=root)
        prior_entry = pending['entries'].get(identity)
        if prior_entry is not None and prior_entry != reference:
            raise ValueError('Concurrent pending reference conflict')
        out = {}
        for name in catalog_paths(doc):
            body = current.read(name, required=False)
            catalog = strict_json(body) if body else {'schema_version':1, 'artifact_version':'wp15-station-admitted-catalog-v1', 'entries':{}}
            entry = {'manifest':reference, 'admission':ref.json(), 'scope':doc['scope'],
                'quantities':sorted(doc['quantity_counts']), 'field_count':doc['field_count'], 'context':doc['envelope']['context']}
            previous = catalog['entries'].get(identity)
            if previous is not None and previous != entry:
                raise ValueError('Private catalog admission collision')
            catalog['entries'][identity] = entry
            if len(catalog['entries']) > policy(root)['catalog_max_entries']:
                raise ValueError('Private catalog budget exceeded; shard successor required')
            if previous is None:
                out[name] = canonical(catalog)
        if prior_entry is not None:
            del pending['entries'][identity]
            out[path] = canonical(pending)
        return out
    cloud.publish({}, metadata={'channel':'wp15-private-station-admission'}, merge=merge)
    return receipt


def consume(cloud, *, expected_processor, root=ROOT, max_jobs=8):
    if type(max_jobs) is not int or not 1 <= max_jobs <= 8:
        raise ValueError('Private station admission budget required')
    report = {'status':'PASS', 'cases':[], 'pending_pages_read':0, 'normalizations':0,
        'provider_requests':0, 'full_original_reads':0, 'scientific_gate':'OPEN', 'measurement_gate':'NONBLOCKING_ROUTINE'}
    cloud.open()
    for prefix in '0123456789abcdef':
        entries = page(cloud.read(PENDING + prefix + '.json', required=False), root=root)['entries']
        report['pending_pages_read'] += 1
        for identity, reference in sorted(entries.items()):
            if len(report['cases']) >= max_jobs:
                return report
            if pending_path(identity) != PENDING + prefix + '.json':
                raise ValueError('Pending page shard mismatch')
            report['cases'].append(admit(cloud, reference, expected_processor=expected_processor, root=root))
    return report
