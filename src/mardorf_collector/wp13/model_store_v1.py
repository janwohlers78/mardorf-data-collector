"""Versioned large-parent/native-model reader, independent of frozen WP13 V1.

Originals use ordered 8 MiB objects (2 GiB assembled / 1 GiB decoded per parent).
Compact extracts retain original headers and native units; no normalization,
interpolation, operational model selection, fits or scientific release occurs.
"""
import bz2
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import re
import tempfile
from urllib.parse import parse_qs, urlsplit

from mardorf_collector.storage.archive import canonical, decode
from mardorf_collector.storage.objects import ObjectError, ObjectRef
from mardorf_collector.storage.parents import ParentStore
from .model_originals_v1 import PREFIX, MAX_RESPONSE_BYTES, safe_url

VERSION = 'wp15-native-model-catalog-v1'
READINESS_VERSION = 'wp15-native-model-readiness-v1'
EXTRACT_PREFIX = 'weather/model-native/wp15/v1'
MAX_DECODED_BYTES = 1024**3
MAX_FIELDS_PER_PARENT = 65536
MAX_CATALOG_BYTES = 8 * 1024**2
MAX_FRAGMENT_BYTES = 8 * 1024**2
MAX_PARENTS = 4096
MAX_DELIVERY_BYTES = 16 * 1024**3
MAX_TOTAL_FIELDS = 1024**2
MODELS = ('ICON-D2', 'ICON-EU', 'ICON-D2-EPS', 'GFS', 'GEFS-control', 'ECMWF-IFS')
HEADERS = ('centre', 'subCentre', 'shortName', 'name', 'paramId', 'discipline',
    'parameterCategory', 'parameterNumber', 'units', 'typeOfLevel', 'level',
    'stepType', 'startStep', 'endStep', 'stepUnits', 'stepRange', 'dataDate',
    'dataTime', 'validityDate', 'validityTime', 'gridType', 'Ni', 'Nj',
    'numberOfDataPoints', 'md5GridSection', 'uuidOfHGrid', 'numberOfGridUsed',
    'perturbationNumber', 'typeOfEnsembleForecast', 'numberOfForecastsInEnsemble')


def digest(body):
    return hashlib.sha256(body).hexdigest()


def point(value):
    if not isinstance(value, dict):
        raise ObjectError('Explicit requested native-model point required')
    lat, lon = value.get('latitude', value.get('lat')), value.get('longitude', value.get('lon'))
    if (type(lat) not in (int, float) or type(lon) not in (int, float)
            or not math.isfinite(lat) or not math.isfinite(lon)
            or not -90 <= lat <= 90 or not -180 <= lon <= 360):
        raise ObjectError('Invalid native-model point')
    return {'latitude': lat, 'longitude': lon}


def clock(date, hour):
    if type(date) is not int or type(hour) is not int:
        raise ObjectError('Original GRIB clock missing')
    return datetime.strptime(str(date) + f'{hour:04d}', '%Y%m%d%H%M').replace(tzinfo=timezone.utc).isoformat()


def base_record(item, index, parameter, header, *, run=None, valid=None):
    return {'schema_version': 1, 'artifact_version': 'wp15-native-model-field-v1',
        'acquisition_model': item['metadata']['acquisition_model'],
        'acquisition_stage': item['metadata']['stage'], 'source_sha256': item['sha256'],
        'parent': item['parent'], 'message_index': index, 'native_parameter': parameter,
        'header_native': header, 'run_time_utc': run, 'valid_time_utc': valid,
        'captured_at_utc': item['metadata']['captured_at_utc'],
        'requested_point': None, 'actual_point': None, 'value_native': None,
        'unit_native': header.get('units'), 'member_id_native': None,
        'point_available': False, 'raw_original_available': True}


def source_run(item):
    """Only provider-request clocks actually present in origin/request evidence."""
    metadata = item['metadata']
    request = metadata.get('sdk_request') or {}
    if 'date' in request and 'time' in request:
        date = str(request['date']).replace('-', '')
        hour = int(request['time'])
        hour = hour // 100 if hour > 23 else hour
        return clock(int(date), hour * 100)
    url = urlsplit(metadata['source_url'])
    match = re.search(r'_(\d{10})_\d{3}_', url.path)
    if match:
        cycle = match.group(1)
        return clock(int(cycle[:8]), int(cycle[8:]) * 100)
    query = parse_qs(url.query)
    match = re.search(r'/(?:gfs|gefs)\.(\d{8})/(\d{2})/', query.get('dir', [''])[0])
    if match:
        return clock(int(match.group(1)), int(match.group(2)) * 100)
    return None


def grib_records(path, item, requested):
    import eccodes as ec
    count = 0
    expected_run = source_run(item)
    with Path(path).open('rb') as stream:
        while (handle := ec.codes_grib_new_from_file(stream)) is not None:
            try:
                if count >= MAX_FIELDS_PER_PARENT:
                    raise ObjectError('Native model message budget exceeded')
                header = {key: ec.codes_get(handle, key) for key in HEADERS
                          if ec.codes_is_defined(handle, key) and not ec.codes_is_missing(handle, key)}
                if any(key not in header for key in ('centre', 'shortName', 'paramId', 'units',
                                                    'typeOfLevel', 'level', 'stepType', 'gridType')):
                    raise ObjectError('Required original GRIB header missing')
                centre = str(header['centre'])
                expected = ('ecmf', '98') if item['metadata']['acquisition_model'] == 'ECMWF-IFS' else (
                    ('kwbc', '7') if item['metadata']['acquisition_model'] in ('GFS', 'GEFS-control') else ('edzw', '78'))
                if centre not in expected:
                    raise ObjectError('Provider origin/native GRIB centre contradiction')
                run = clock(header.get('dataDate'), header.get('dataTime'))
                valid = clock(header.get('validityDate'), header.get('validityTime'))
                if datetime.fromisoformat(valid) < datetime.fromisoformat(run):
                    raise ObjectError('Native model validity precedes run')
                if expected_run is not None and run != expected_run:
                    raise ObjectError('Original provider request/native run contradiction')
                record = base_record(item, count, header['shortName'], header, run=run, valid=valid)
                record['member_id_native'] = header.get('perturbationNumber')
                record['requested_point'] = requested
                # Native triangular grids need the provider's external definition.
                # Retain their actual headers without inventing an extraction point.
                if header['gridType'] in ('unstructured_grid', 'unstructured'):
                    record['point_unavailable_reason'] = 'external_native_grid_definition_required'
                else:
                    near = ec.codes_grib_find_nearest(handle, requested['latitude'], requested['longitude'], npoints=1)[0]
                    value = near['value']
                    if ec.codes_is_defined(handle, 'missingValue') and value == ec.codes_get(handle, 'missingValue'):
                        value = None
                    if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                        raise ObjectError('Nonfinite native point value')
                    record.update(actual_point={'latitude': near['lat'], 'longitude': near['lon'],
                        'grid_point_index': near['index']}, value_native=value, point_available=True)
                record['field_id'] = digest(canonical(record))
                yield record
                count += 1
            finally:
                ec.codes_release(handle)
    if not count:
        raise ObjectError('No native GRIB messages')


def eps_records(path, item, requested, ensemble):
    body = Path(path).read_bytes()
    payload = decode(body)
    if 'hourly' not in payload and 'last_run_initialisation_time' in payload:
        record = base_record(item, 0, 'provider_metadata_document', payload)
        record['point_unavailable_reason'] = 'provider_cycle_metadata_has_no_point_values'
        record['field_id'] = digest(canonical(record))
        yield record
        return
    logical = digest(json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())
    if (not isinstance(ensemble, dict) or ensemble.get('response_sha256') != logical
            or item['metadata'].get('canonical_json_sha256') != logical):
        raise ObjectError('EPS original/verified cycle metadata identity mismatch')
    if (ensemble.get('model') != 'dwd_icon_d2_eps'
            or ensemble.get('response_run_binding', {}).get('metadata_stable_across_response') is not True
            or ensemble.get('spatial_provenance_verified') is not True
            or ensemble.get('expected_member_ids') != list(range(20))):
        raise ObjectError('EPS existing cycle/spatial/member qualification missing')
    returned = point({'latitude': payload.get('latitude'), 'longitude': payload.get('longitude')})
    if point(ensemble.get('returned_coordinate')) != returned or point(ensemble.get('requested_coordinate')) != requested:
        raise ObjectError('EPS requested/returned point contradiction')
    hourly, units = payload.get('hourly'), payload.get('hourly_units')
    if not isinstance(hourly, dict) or not isinstance(units, dict) or not isinstance(hourly.get('time'), list):
        raise ObjectError('EPS original hourly arrays/units missing')
    count = 0
    evidence_digest = digest(canonical(ensemble))
    for parameter, values in sorted(hourly.items()):
        if parameter == 'time':
            continue
        match = re.search(r'_member(\d+)$', parameter)
        member = int(match.group(1)) if match else 0
        base_parameter = parameter[:match.start()] if match else parameter
        unit = units.get(parameter, units.get(base_parameter))
        expected = ensemble.get('columns', {}).get(base_parameter, {}).get(str(member))
        if expected is None:
            expected = ensemble.get('columns', {}).get(base_parameter, {}).get(member)
        if (not isinstance(values, list) or len(values) != len(hourly['time'])
                or unit is None or expected != values or member not in ensemble['expected_member_ids']):
            raise ObjectError('EPS native axis/units/member values contradiction')
        for index, (stamp, value) in enumerate(zip(hourly['time'], values)):
            if count >= MAX_FIELDS_PER_PARENT:
                raise ObjectError('EPS native field budget exceeded')
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                raise ObjectError('EPS nonfinite native value')
            # Open-Meteo timezone is explicit; no run clock is invented from valid time.
            if payload.get('utc_offset_seconds') != 0:
                raise ObjectError('EPS original UTC axis required')
            parsed = datetime.fromisoformat(stamp)
            valid = parsed.replace(tzinfo=timezone.utc).isoformat() if parsed.tzinfo is None else parsed.astimezone(timezone.utc).isoformat()
            record = base_record(item, count, parameter, {'units': unit,
                'provider_parameter': parameter, 'time_native': stamp, 'array_index': index}, valid=valid)
            record.update(requested_point=requested, actual_point=returned, value_native=value,
                member_id_native=member, point_available=True,
                cycle_evidence_sha256=evidence_digest,
                cycle_evidence_basis='existing_verified_provider_metadata')
            record['field_id'] = digest(canonical(record))
            yield record
            count += 1
    if not count:
        raise ObjectError('No EPS native member fields')


def qualify(backend, item, requested, *, ensemble=None):
    """Read the retained original, preserving a distinct raw-only grid outcome."""
    parents = ParentStore(backend, prefix=PREFIX)
    manifest = parents.manifest(item['parent'])
    if (manifest['sha256'] != item['sha256'] or manifest['bytes'] != item['bytes']
            or manifest['metadata'] != item['metadata']
            or item['bytes'] > (2 * 1024**3 if item['metadata'].get('source_kind') == 'assembled_native_request' else MAX_RESPONSE_BYTES)
            or item['metadata'].get('acquisition_model') not in MODELS):
        raise ObjectError('Original capture/parent contradiction')
    safe_url(item['metadata']['source_url'])
    requested = point(requested)
    with tempfile.TemporaryDirectory(prefix='mardorf-model-qualify-') as folder:
        raw = parents.restore(item['parent'], Path(folder) / 'raw')
        with raw.open('rb') as stream:
            magic = stream.read(4)
        if magic.startswith(b'BZh'):
            native = Path(folder) / 'decoded.grib2'
            size = 0
            with bz2.open(raw, 'rb') as source, native.open('xb') as output:
                while block := source.read(min(1024**2, MAX_DECODED_BYTES - size + 1)):
                    size += len(block)
                    if size > MAX_DECODED_BYTES:
                        raise ObjectError('Explicit native model decoded byte budget exceeded')
                    output.write(block)
            return list(grib_records(native, item, requested))
        if magic == b'GRIB':
            return list(grib_records(raw, item, requested))
        if item['metadata']['acquisition_model'] == 'ICON-D2-EPS':
            return list(eps_records(raw, item, requested, ensemble))
        raise ObjectError('Original model payload type unsupported')


def parquet_bytes(records):
    import pyarrow as pa
    import pyarrow.parquet as pq
    # Canonical record bytes preserve native nulls, integer member identities,
    # original units, complete headers and causal evidence without type coercion.
    table = pa.table({'field_id': [r['field_id'] for r in records],
                      'record_json': [canonical(r) for r in records]})
    output = io.BytesIO()
    pq.write_table(table, output, compression='zstd', use_dictionary=False)
    return output.getvalue()


def write_catalog(backend, items, requested, *, ensemble=None):
    if not items or len(items) > MAX_PARENTS or sum(item['bytes'] for item in items) > MAX_DELIVERY_BYTES:
        raise ObjectError('Explicit native model parent inventory required')
    inventory, fragments, counts = [], [], Counter()
    evidence = None
    if ensemble is not None:
        body = canonical(ensemble)
        if len(body) > MAX_FRAGMENT_BYTES:
            raise ObjectError('Native model cycle evidence budget exceeded')
        evidence = backend.put_bytes(f'{EXTRACT_PREFIX}/evidence/{digest(body)}', body).json()
    def flush(records):
        if not records:
            return
        for kind, body in (('native', canonical(records)), ('parquet', parquet_bytes(records))):
            if len(body) > MAX_FRAGMENT_BYTES:
                raise ObjectError('Native-model fragment byte budget exceeded')
            reference = backend.put_bytes(f'{EXTRACT_PREFIX}/{kind}/{digest(body)}', body)
            refs[kind] = reference.json()
        fragments.append(dict(refs, fields=len(records)))
    for item in items:
        records = qualify(backend, item, requested, ensemble=ensemble)
        inventory.append(dict(item, fields=len(records), point_fields=sum(r['point_available'] for r in records)))
        batch, size, refs = [], 0, {}
        for record in records:
            encoded = canonical(record)
            if len(encoded) > MAX_FRAGMENT_BYTES // 2:
                raise ObjectError('Native-model individual record budget exceeded')
            if batch and size + len(encoded) > MAX_FRAGMENT_BYTES // 2:
                flush(batch); batch, size, refs = [], 0, {}
            batch.append(record); size += len(encoded) + 1
            counts[record['acquisition_model']] += 1
            if sum(counts.values()) > MAX_TOTAL_FIELDS:
                raise ObjectError('Explicit native model total field budget exceeded')
        flush(batch)
    catalog = {'schema_version': 1, 'artifact_version': VERSION, 'parents': inventory,
        'requested_point': point(requested), 'fragments': fragments,
        'fields': sum(counts.values()), 'model_field_counts': dict(sorted(counts.items())),
        'provider_requests_added': 0, 'qualification': 'native_originals_and_compact_reader_only',
        'scientific_gate': 'OPEN', 'legacy_wp13_limits_changed': False}
    catalog['cycle_evidence'] = evidence
    body = canonical(catalog)
    if len(body) > MAX_CATALOG_BYTES:
        raise ObjectError('Native-model catalog budget exceeded')
    return backend.put_bytes(f'{EXTRACT_PREFIX}/catalogs/{digest(body)}', body)


class ModelReader:
    """Compact point queries read no original chunks; audits explicitly do so."""
    def __init__(self, backend):
        self.backend = backend

    def catalog(self, reference):
        ref = ObjectRef.parse(reference) if isinstance(reference, dict) else reference
        if ref.key != f'{EXTRACT_PREFIX}/catalogs/{ref.sha256}' or ref.bytes > MAX_CATALOG_BYTES:
            raise ObjectError('Native-model catalog scope/budget')
        catalog = decode(self.backend.get_bytes(ref))
        if (catalog.get('artifact_version') != VERSION or catalog.get('schema_version') != 1
                or not isinstance(catalog.get('parents'), list) or not 0 < len(catalog['parents']) <= MAX_PARENTS
                or not isinstance(catalog.get('fragments'), list) or not 0 < len(catalog['fragments']) <= MAX_PARENTS * 64
                or type(catalog.get('fields')) is not int or not 0 < catalog['fields'] <= MAX_TOTAL_FIELDS
                or sum(item['bytes'] for item in catalog['parents']) > MAX_DELIVERY_BYTES):
            raise ObjectError('Invalid native-model catalog')
        return catalog

    def query(self, reference, *, model=None, parameter=None, valid_start=None, valid_end=None, member=None):
        catalog = self.catalog(reference)
        seen = set()
        for fragment in catalog['fragments']:
            ref = ObjectRef.parse(fragment['native'])
            if ref.key != f'{EXTRACT_PREFIX}/native/{ref.sha256}' or ref.bytes > MAX_FRAGMENT_BYTES:
                raise ObjectError('Native extract scope/budget')
            rows = decode(self.backend.get_bytes(ref))
            if not isinstance(rows, list) or len(rows) != fragment['fields']:
                raise ObjectError('Native extract row count contradiction')
            for row in rows:
                identity = row.get('field_id')
                unsigned = {k: v for k, v in row.items() if k != 'field_id'}
                if identity != digest(canonical(unsigned)) or identity in seen:
                    raise ObjectError('Native field identity/uniqueness contradiction')
                seen.add(identity)
                if (model is not None and row['acquisition_model'] != model
                        or parameter is not None and row['native_parameter'] != parameter
                        or member is not None and row['member_id_native'] != member):
                    continue
                valid = row['valid_time_utc']
                if ((valid_start is not None and (valid is None or valid < valid_start))
                        or (valid_end is not None and (valid is None or valid >= valid_end))):
                    continue
                yield row
        if len(seen) != catalog['fields']:
            raise ObjectError('Native-model catalog field count contradiction')

    def verify(self, reference, *, audit_originals=True):
        import pyarrow.parquet as pq
        catalog = self.catalog(reference)
        parents = ParentStore(self.backend, prefix=PREFIX)
        by_parent = {canonical(item['parent']): item for item in catalog['parents']}
        if len(by_parent) != len(catalog['parents']):
            raise ObjectError('Duplicate original parent')
        evidence_ref = None
        if catalog.get('cycle_evidence') is not None:
            evidence_ref = ObjectRef.parse(catalog['cycle_evidence'])
            if evidence_ref.key != f'{EXTRACT_PREFIX}/evidence/{evidence_ref.sha256}' or evidence_ref.bytes > MAX_FRAGMENT_BYTES:
                raise ObjectError('Cycle evidence scope/budget')
            self.backend.get_bytes(evidence_ref)
        if audit_originals:
            for item in catalog['parents']:
                manifest = parents.manifest(item['parent'])
                if manifest['sha256'] != item['sha256'] or manifest['bytes'] != item['bytes'] or manifest['metadata'] != item['metadata']:
                    raise ObjectError('Original parent/capture metadata contradiction')
                for _ in parents.iter_bytes(item['parent']):
                    pass
        # Compare incrementally. A large delivery never creates two complete
        # Python record inventories or an unbounded complete Parquet table.
        native = iter(self.query(reference))
        count, points, counts, point_counts, parent_counts = 0, 0, Counter(), Counter(), Counter()
        identity = hashlib.sha256(b'[')
        for fragment in catalog['fragments']:
            ref = ObjectRef.parse(fragment['parquet'])
            if ref.key != f'{EXTRACT_PREFIX}/parquet/{ref.sha256}' or ref.bytes > MAX_FRAGMENT_BYTES:
                raise ObjectError('Native projection scope/budget')
            table = pq.read_table(io.BytesIO(self.backend.get_bytes(ref)))
            if table.column_names != ['field_id', 'record_json'] or table.num_rows != fragment['fields']:
                raise ObjectError('Native projection schema/count contradiction')
            for field_id, body in zip(table['field_id'].to_pylist(), table['record_json'].to_pylist()):
                record = decode(body)
                original = next(native, None)
                if record.get('field_id') != field_id or record != original:
                    raise ObjectError('Native/parquet projection mismatch')
                parent_id = canonical(record['parent'])
                item = by_parent.get(parent_id)
                if (not item or record['source_sha256'] != item['sha256']
                        or record['acquisition_model'] != item['metadata']['acquisition_model']):
                    raise ObjectError('Native field/original parent contradiction')
                if record.get('cycle_evidence_sha256') is not None and (evidence_ref is None or record['cycle_evidence_sha256'] != evidence_ref.sha256):
                    raise ObjectError('Native EPS field/cycle evidence contradiction')
                if count:
                    identity.update(b',')
                identity.update(canonical(record))
                count += 1
                counts[record['acquisition_model']] += 1
                parent_counts[parent_id] += 1
                points += record['point_available']
                if record['point_available']:
                    point_counts[record['acquisition_model']] += 1
        if next(native, None) is not None or count != catalog['fields']:
            raise ObjectError('Native model projection total count contradiction')
        identity.update(b']')
        counts = dict(sorted(counts.items()))
        if counts != catalog['model_field_counts'] or any(parent_counts[key] != item['fields'] for key, item in by_parent.items()):
            raise ObjectError('Native model coverage contradiction')
        return {'status': 'PASS', 'fields': count, 'point_fields': points,
            'model_point_field_counts': dict(sorted(point_counts.items())),
            'original_parents': len(catalog['parents']), 'original_bytes': sum(i['bytes'] for i in catalog['parents']),
            'model_field_counts': counts, 'native_fields_sha256': identity.hexdigest(),
            'originals_fully_read': audit_originals, 'parquet_exact_native_match': True,
            'provider_requests_added': 0, 'scientific_gate': 'OPEN'}
