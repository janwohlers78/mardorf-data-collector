"""Shared, fully verified public station -> native/Parquet publication.

Only this producer normalizes new station publications. Readers verify immutable
content and consume the pinned projections without re-extracting source parents.
"""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
from pathlib import Path
import re
import struct
import time

import pyarrow as pa
import pyarrow.parquet as pq

from ..storage.objects import ObjectRef
from ..wp13.core_v1 import canonical, digest, identify, strict_json, stamp, utc
from .assets import ROOT, reference_root

PREFIX = 'weather/station-preprocessed/v1'
CONTRACT = 'config/dev03_wp15_preprocessing_contract_v1.json'
MAGIC = b'WP15ZST1'


def encode_sidecar(body):
    return MAGIC + struct.pack('<Q', len(body)) + pa.compress(body, codec='zstd').to_pybytes()


def decode_sidecar(body, maximum=16*1024**2):
    if not body.startswith(MAGIC) or len(body) < 17:
        raise ValueError('Prepared sidecar encoding mismatch')
    length = struct.unpack('<Q', body[8:16])[0]
    if length > maximum:
        raise ValueError('Prepared decoded sidecar budget exceeded')
    try:
        decoded = pa.decompress(body[16:], decompressed_size=length, codec='zstd').to_pybytes()
    except pa.ArrowException as exc:
        raise ValueError('Prepared sidecar decode failed') from exc
    if len(decoded) != length:
        raise ValueError('Prepared sidecar length mismatch')
    return decoded


def policy(root=ROOT):
    value = strict_json((Path(root) / CONTRACT).read_bytes())
    if value.get('artifact_version') != 'wp15-public-station-preprocessing-contract-v1':
        raise ValueError('Unsupported station preprocessing contract')
    return value


def processor_identity(root=ROOT):
    root = Path(root)
    value = policy(root)
    files = value['processor_files']
    if not files or len(files) != len(set(files)):
        raise ValueError('Explicit processor inventory required')
    return digest({'contract': value, 'artifacts': {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files}})


def formatter(configuration_sha256, root=ROOT):
    value = policy(root)
    if configuration_sha256 == value['providers']['dwd_cdc']['configuration_sha256']:
        from .format import RegionalWeatherFormatV1
        return RegionalWeatherFormatV1(root)
    if configuration_sha256 == value['providers']['svg']['configuration_sha256']:
        from .reference.fields_v2 import WeatherFormatV2
        return WeatherFormatV2(reference_root())
    raise ValueError('Unregistered station configuration')


def physical_row(record):
    n, c = record['native'], record['canonical']
    row = {key: n['domain'][key] for key in ('profile_id', 'site_id', 'quantity_id')}
    row.update(field_id=n['field_id'], record_id=record['record_id'],
        record_kind=n['record_kind'], configuration_sha256=n['configuration']['sha256'],
        provider_binding_id=n['source']['provider_binding_id'], member_id=n['member']['member_id'],
        ensemble_set_id=n['member']['ensemble_set_id'], normalization_status=c['status'],
        matching_status=c['matching_status'], value_canonical=c['value_canonical'],
        unit_canonical=c['unit_canonical'], physical_kind=c['physical_kind'], canonical_json=canonical(c).decode())
    for key in ('valid_start_utc', 'valid_end_utc'):
        row[key] = utc(n['time'][key])
    return row


def parquet_schema():
    strings = ['field_id', 'record_id', 'profile_id', 'site_id', 'quantity_id', 'record_kind',
        'configuration_sha256', 'provider_binding_id', 'member_id', 'ensemble_set_id',
        'normalization_status', 'matching_status', 'unit_canonical', 'physical_kind', 'canonical_json']
    return pa.schema([(key, pa.string()) for key in strings] + [
        ('value_canonical', pa.float64()), ('valid_start_utc', pa.timestamp('us', tz='UTC')),
        ('valid_end_utc', pa.timestamp('us', tz='UTC'))],
        metadata={b'weather_schema': b'dev03-wp14-long-parquet-v1'})


def check_ref(value, *, kind=None, maximum=64*1024**2):
    ref = ObjectRef.parse(value)
    if not ref.key.startswith(PREFIX + '/') or ref.bytes > maximum:
        raise ValueError('Prepared object scope/budget mismatch')
    if kind is not None and not ref.key.startswith(PREFIX + '/' + kind + '/'):
        raise ValueError('Prepared object class mismatch')
    return ref


def read_manifest(backend, reference, *, expected_processor=None, root=ROOT):
    ref = check_ref(reference, kind='manifests', maximum=policy(root)['manifest_max_bytes'])
    doc = strict_json(backend.get_bytes(ref))
    p = policy(root)
    required = {'schema_version', 'artifact_version', 'publication_id', 'processor_sha256',
        'configuration_sha256', 'envelope', 'originals', 'native', 'projections', 'partitions',
        'field_count', 'native_fields_sha256', 'quantity_counts', 'scope',
        'public_processed_at_utc', 'public_verified_at_utc', 'scientific_gate'}
    if (set(doc) != required or doc['schema_version'] != 1
            or doc['artifact_version'] != 'wp15-public-station-preprocessed-v1'
            or doc['scientific_gate'] != 'OPEN'
            or doc['processor_sha256'] != (expected_processor or processor_identity(root))):
        raise ValueError('Prepared publication version/processor mismatch')
    identity = digest({'envelope_id': doc['envelope']['envelope_id'],
        'processor_sha256': doc['processor_sha256'], 'configuration_sha256': doc['configuration_sha256']})
    if doc['publication_id'] != identity or ref.key != PREFIX + '/manifests/' + identity:
        raise ValueError('Prepared publication identity mismatch')
    if (doc['envelope']['configuration']['sha256'] != doc['configuration_sha256']
            or doc['configuration_sha256'] not in {v['configuration_sha256'] for v in p['providers'].values()}
            or type(doc['field_count']) is not int or not 1 <= doc['field_count'] <= p['max_fields']
            or sum(doc['quantity_counts'].values()) != doc['field_count']
            or len(doc['partitions']) > p['max_partitions']
            or sum(part['rows'] for part in doc['partitions']) != doc['field_count']):
        raise ValueError('Prepared publication configuration/completeness mismatch')
    if not re.fullmatch('[0-9a-f]{64}', str(doc['native_fields_sha256'])):
        raise ValueError('Prepared field digest required')
    envelope = doc['envelope']
    if digest({k:v for k,v in envelope.items() if k != 'envelope_id'}) != envelope['envelope_id']:
        raise ValueError('Prepared envelope identity mismatch')
    if not utc(envelope['capture']['verified_at_utc']) <= utc(doc['public_processed_at_utc']) <= utc(doc['public_verified_at_utc']):
        raise ValueError('Public processing clocks backdated')
    if envelope['context'] not in ('prospective', 'historical'):
        raise ValueError('Explicit station context required')
    if len(doc['originals']) != len(envelope['raw_objects']):
        raise ValueError('Incomplete prepared original inventory')
    for item in envelope['raw_objects']:
        original = doc['originals'].get(item['object_id'])
        if original is None or original['sha256'] != item['transport_sha256'] or original['bytes'] != item['transport_bytes']:
            raise ValueError('Prepared original binding mismatch')
        if 'ref' in original:
            parent = check_ref(original['ref'], kind='blobs', maximum=p['original_max_bytes'])
            if parent.sha256 != original['sha256'] or parent.bytes != original['bytes']:
                raise ValueError('Prepared original reference identity mismatch')
        elif original.get('archive_path') != 'data/weather_native/dwd_cdc_v1/originals/' + original['sha256'] + '.zip':
            raise ValueError('Unbound shared historical original')
    for name in ('native', 'projections'):
        check_ref(doc[name], kind='blobs', maximum=p['sidecar_max_bytes'])
    for part in doc['partitions']:
        check_ref(part['ref'], kind='blobs', maximum=p['sidecar_max_bytes'])
        if type(part['rows']) is not int or not 1 <= part['rows'] <= p['max_fields']:
            raise ValueError('Prepared partition row budget exceeded')
    if utc(doc['scope']['start_utc']) > utc(doc['scope']['end_utc']):
        raise ValueError('Prepared scope time mismatch')
    return doc


def read_records(backend, doc, *, verify_parquet=True):
    """Cold hash verification and physical parity; no normalizer or raw decode."""
    native = [strict_json(line) for line in decode_sidecar(backend.get_bytes(check_ref(doc['native']))).splitlines()]
    projections = [strict_json(line) for line in decode_sidecar(backend.get_bytes(check_ref(doc['projections']))).splitlines()]
    if (len(native) != doc['field_count'] or len(projections) != len(native)
            or len({f['field_id'] for f in native}) != len(native)
            or digest(sorted(native, key=lambda f:f['field_id'])) != doc['native_fields_sha256']):
        raise ValueError('Prepared native completeness mismatch')
    by_id = {p['native_field_id']:p for p in projections}
    if len(by_id) != len(native) or set(by_id) != {f['field_id'] for f in native}:
        raise ValueError('Prepared projection completeness mismatch')
    for projection in projections:
        if digest({k:v for k,v in projection.items() if k != 'projection_id'}) != projection['projection_id']:
            raise ValueError('Prepared projection identity mismatch')
    scope = doc['scope']
    if (Counter(f['domain']['quantity_id'] for f in native) != doc['quantity_counts']
            or any(f['configuration']['sha256'] != doc['configuration_sha256']
                or any(f['domain'][k] != scope[k] for k in ('profile_id','site_id','station_id')) for f in native)
            or min(f['time']['valid_end_utc'] for f in native) != scope['start_utc']
            or max(f['time']['valid_end_utc'] for f in native) != scope['end_utc']):
        raise ValueError('Prepared scope/configuration mismatch')
    records = [identify({'schema_version':1, 'artifact_version':'dev03-wp12-read-field-v1',
        'native':f, 'canonical':by_id[f['field_id']]}, 'record_id') for f in native]
    if verify_parquet:
        expected = {r['native']['field_id']:physical_row(r) for r in records}
        found = {}
        for part in doc['partitions']:
            table = pq.read_table(pa.BufferReader(backend.get_bytes(check_ref(part['ref']))))
            if not table.schema.equals(parquet_schema(), check_metadata=True) or table.num_rows != part['rows']:
                raise ValueError('Prepared Parquet schema/row mismatch')
            for row in table.to_pylist():
                if row['field_id'] in found or expected.get(row['field_id']) != row:
                    raise ValueError('Prepared Parquet/native projection mismatch')
                found[row['field_id']] = row
        if found != expected:
            raise ValueError('Prepared Parquet completeness mismatch')
    return records


def preprocess(backend, result, *, root=ROOT, archive_originals=None):
    """Validate all source parents once, normalize, cold-verify, publish last."""
    started = time.monotonic()
    p = policy(root)
    envelope, fields = result['envelope'], result['fields']
    for inventory, payloads in ((envelope['raw_objects'], result['raw_bytes']),
                               (envelope['field_fragments'], result['fragment_bytes'])):
        for item in inventory:
            name = item.get('object_id') or item['path']
            body = payloads[name]
            identity = item.get('transport_sha256', item.get('sha256'))
            length = item.get('transport_bytes', item.get('bytes'))
            if hashlib.sha256(body).hexdigest() != identity or (length is not None and len(body) != length):
                raise ValueError('Prepared input payload identity mismatch')
    processor = processor_identity(root)
    publication = digest({'envelope_id':envelope['envelope_id'], 'processor_sha256':processor,
        'configuration_sha256':envelope['configuration']['sha256']})
    key = PREFIX + '/manifests/' + publication
    prior = backend.head(key)
    expected_hash = digest(sorted(fields, key=lambda f:f['field_id']))
    if prior is not None:
        ref = ObjectRef(key, prior['sha256'], prior['bytes'])
        existing = read_manifest(backend, ref.json(), expected_processor=processor, root=root)
        if existing['native_fields_sha256'] != expected_hash:
            raise ValueError('Prepared replay native conflict')
        read_records(backend, existing)
        return {'manifest':ref.json(), 'publication_id':publication, 'fields':len(fields),
            'status':'verified_existing', 'normalizations':0, 'elapsed_seconds':time.monotonic()-started}
    if not 1 <= len(fields) <= p['max_fields']:
        raise ValueError('Prepared field budget exceeded')
    fmt = formatter(envelope['configuration']['sha256'], root)
    fmt.validate_transfer(envelope, fields, result['raw_bytes'], result['fragment_bytes'])
    if any(f['record_kind'] != 'observation' for f in fields):
        raise ValueError('Station processing does not accept model transfers')
    scopes = {(f['domain']['profile_id'], f['domain']['site_id'], f['domain']['station_id']) for f in fields}
    if len(scopes) != 1:
        raise ValueError('Single station/profile per prepared publication required')
    processed = stamp(datetime.now(timezone.utc))
    projections = [fmt.normalize(f) for f in fields]
    records = [identify({'schema_version':1, 'artifact_version':'dev03-wp12-read-field-v1',
        'native':f, 'canonical':c}, 'record_id') for f,c in zip(fields, projections)]
    def put(body, limit=p['sidecar_max_bytes']):
        if len(body) > limit:
            raise ValueError('Prepared payload budget exceeded')
        return backend.put_bytes(PREFIX + '/blobs/' + hashlib.sha256(body).hexdigest(), body).json()
    originals = {}
    archive_originals = archive_originals or {}
    for name, body in result['raw_bytes'].items():
        identity = hashlib.sha256(body).hexdigest()
        original = {'sha256':identity, 'bytes':len(body)}
        if identity in archive_originals:
            original['archive_path'] = archive_originals[identity]
        else:
            original['ref'] = put(body, p['original_max_bytes'])
        originals[name] = original
    partitions = []
    groups = {}
    for record in records:
        n = record['native']
        groups.setdefault(n['time']['valid_end_utc'][:7], []).append(record)
    for month, items in sorted(groups.items()):
        output = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([physical_row(r) for r in items], schema=parquet_schema()),
            output, compression='zstd', compression_level=3)
        partitions.append({'day':min(r['native']['time']['valid_end_utc'][:10] for r in items),
            'rows':len(items), 'quantities':sorted({r['native']['domain']['quantity_id'] for r in items}),
            'ref':put(output.getvalue())})
    profile, site, station = next(iter(scopes))
    doc = {'schema_version':1, 'artifact_version':'wp15-public-station-preprocessed-v1',
        'publication_id':publication, 'processor_sha256':processor,
        'configuration_sha256':envelope['configuration']['sha256'], 'envelope':envelope,
        'originals':originals, 'native':put(encode_sidecar(b''.join(canonical(f)+b'\n' for f in fields))),
        'projections':put(encode_sidecar(b''.join(canonical(c)+b'\n' for c in projections))),
        'partitions':partitions, 'field_count':len(fields), 'native_fields_sha256':expected_hash,
        'quantity_counts':dict(sorted(Counter(f['domain']['quantity_id'] for f in fields).items())),
        'scope':{'profile_id':profile, 'site_id':site, 'station_id':station,
            'start_utc':min(f['time']['valid_end_utc'] for f in fields),
            'end_utc':max(f['time']['valid_end_utc'] for f in fields)},
        'public_processed_at_utc':processed, 'public_verified_at_utc':processed, 'scientific_gate':'OPEN'}
    # Actual fresh backend reads, bypassing the runtime's caches. Originals are
    # additionally verified; shared archive parents were hash-checked by caller.
    for original in originals.values():
        if 'ref' in original:
            body = backend.get_bytes(check_ref(original['ref']))
            if len(body) != original['bytes'] or hashlib.sha256(body).hexdigest() != original['sha256']:
                raise ValueError('Original cold readback mismatch')
    read_records(backend, doc)
    doc['public_verified_at_utc'] = stamp(datetime.now(timezone.utc))
    body = canonical(doc)
    if len(body) > p['manifest_max_bytes']:
        raise ValueError('Prepared manifest budget exceeded')
    ref = backend.put_bytes(key, body)
    read_manifest(backend, ref.json(), expected_processor=processor, root=root)
    return {'manifest':ref.json(), 'publication_id':publication, 'fields':len(fields),
        'status':'published', 'normalizations':len(fields), 'elapsed_seconds':time.monotonic()-started}
