"""Small native00UTC Esch capture for a frozen experimental wind candidate.

No model, fit, observations, scheduler or private model path belongs here.
Heavy decoder imports are lazy; the controller admits its exact runtime first.
Publication callbacks must perform immutable OVH PUT and full readback before
returning a {file,bytes,sha256} CAS reference. Successful local staging is removed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import io
import json
import math
import os
import re
import urllib.error
import urllib.request

try:
    from ._m3_native_range import Budget, profile_cost, bounded_store
    from ._m3_ifs_contract import bilinear, feature_record, FIELDS
    from ._m3_coordinate_guard import coordinate_bounded_store
except ImportError:
    from _m3_native_range import Budget, profile_cost, bounded_store
    from _m3_ifs_contract import bilinear, feature_record, FIELDS
    from _m3_coordinate_guard import coordinate_bounded_store

TARGET = {'id': 'awekas-40749-esch-public-chart-local-v1',
          'latitude': 52.269502777777774, 'longitude': 8.673894444444445}
LEADS = tuple(range(24, 121, 6))
GEFS_URL = 'https://dynamical-noaa-gefs.s3.us-west-2.amazonaws.com/noaa-gefs-forecast-35-day/v0.2.0.icechunk'
IFS_PREFIX = 'https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/'
GEFS_SOURCE = 'gefs-dynamical-packets-v0.2.0'
IFS_SOURCE = 'ifs-public-aws-qualified-native-v1'
OPERATOR = 'bilinear_native_signed_10m_instant'
OPTIONAL_IFS = {'2d', 'tcc', 'tp', '10fg'}
RAM_DESIGN = 256 * 1024**2


def now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def initialization(day):
    value = datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    if value.strftime('%Y-%m-%d') != day:
        raise ValueError('literal_UTC_date_required')
    return value


class Publisher:
    def __init__(self, output, publish_bytes=None, publish_file=None):
        if (publish_bytes is None) == (publish_file is None):
            raise ValueError('exactly_one_verified_OVH_publication_callback_required')
        self.root = Path(output)
        if self.root.is_symlink():
            raise ValueError('own_output_not_symlink')
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.by_bytes, self.by_file = publish_bytes, publish_file
        self.refs = {}

    def save(self, name, value):
        if Path(name).name != name:
            raise ValueError('flat_owned_artifact_name_required')
        raw = value if isinstance(value, bytes) else canonical(value)
        path = self.root/name
        with path.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        # Callback implementation is a separately admitted broker/backend.
        ref = self.by_bytes(name, raw) if self.by_bytes else self.by_file(name, path)
        if (not isinstance(ref, dict) or set(ref) != {'file', 'bytes', 'sha256'}
                or ref['bytes'] != len(raw) or ref['sha256'] != digest(raw)):
            raise ValueError('publication_callback_did_not_return_exact_full_readback_CAS')
        self.refs[name] = ref
        path.unlink()  # Never delete until the verified publication returned.
        descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        return ref


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch(opener, url, *, byte_range=None):
    if not url.startswith(IFS_PREFIX):
        raise ValueError('only_existing_ECMWF_source_allowed')
    headers = {'Accept-Encoding': 'identity'}
    maximum = 4 * 1024**2
    if byte_range is not None:
        lo, hi = byte_range
        if type(lo) is not int or type(hi) is not int or not 0 <= lo <= hi:
            raise ValueError('positive_exact_HTTP_range')
        maximum = hi - lo + 1
        if 3 * maximum + 64 * 1024**2 > RAM_DESIGN:
            raise ValueError('own_GRIB_decode_peak_exceeds_existing256MiB_design')
        headers['Range'] = f'bytes={lo}-{hi}'
    started = now()
    try:
        response = opener.open(urllib.request.Request(url, headers=headers), timeout=30)
    except urllib.error.HTTPError as exc:
        return None, {'url': url, 'request_started_utc': started, 'received_finished_utc': now(),
            'status': exc.code, 'source_absent': exc.code in (404, 410),
            'date': exc.headers.get('Date'), 'retry_after': exc.headers.get('Retry-After')}
    with response:
        if byte_range is not None:
            expected = rf'bytes {byte_range[0]}-{byte_range[1]}/[0-9]+'
            if response.status != 206 or not re.fullmatch(expected, response.headers.get('Content-Range', '')):
                raise ValueError('exact_HTTP_range_not_honored_no_full_file_read')
        elif response.status != 200:
            raise ValueError('source_index_status')
        size = response.headers.get('Content-Length')
        if size is not None and int(size) > maximum:
            raise ValueError('HTTP_body_exceeds_admitted_request')
        raw = response.read(maximum + 1)
        if len(raw) > maximum or (byte_range is not None and len(raw) != maximum):
            raise ValueError('HTTP_returned_payload_not_exact_admitted_length')
        return raw, {'url': url, 'request_started_utc': started, 'received_finished_utc': now(),
            'status': response.status, 'date': response.headers.get('Date'),
            'age': response.headers.get('Age'), 'etag': response.headers.get('ETag'),
            'content_range': response.headers.get('Content-Range'), 'bytes': len(raw), 'sha256': digest(raw)}


def neighbours(grid, query):
    import numpy as np
    values = np.asarray(grid, dtype=float)
    if values.ndim != 1 or len(values) < 2 or not np.isfinite(values).all():
        raise ValueError('native_grid_vector_required')
    order = np.argsort(values); axis = values[order]
    if not np.allclose(np.diff(axis), .25, rtol=0, atol=1e-10) or not axis[0] < query < axis[-1]:
        raise ValueError('regular025_native_grid_without_extrapolation')
    hi = int(np.searchsorted(axis, query, side='right')); lo = hi - 1
    weight = float((query - axis[lo]) / (axis[hi] - axis[lo]))
    return [int(order[lo]), int(order[hi])], {'low': float(axis[lo]), 'high': float(axis[hi]), 'upper_weight': weight,
        'native_axis_sha256': digest(values.astype('<f8').tobytes())}


def gefs_metadata(dataset, store, zarr, requested):
    import numpy as np
    if (dataset.attrs.get('dataset_id') != 'noaa-gefs-forecast-35-day'
            or dataset.attrs.get('dataset_version') != '0.2.0'):
        raise ValueError('bound_GEFS_dataset_identity_or_version_changed')
    starts = np.asarray(dataset.init_time)
    wanted = np.datetime64(requested.replace(tzinfo=None), 'ns')
    if (starts.ndim != 1 or starts.dtype.kind != 'M' or np.isnat(starts).any()
            or len(np.unique(starts)) != len(starts)):
        raise ValueError('unique_native_initialization_axis')
    found = np.flatnonzero(starts == wanted)
    if len(found) != 1:
        raise ValueError('requested00UTC_initialization_absent_no_latest_fallback')
    index = int(found[0]); member = np.asarray(dataset.ensemble_member)
    if member.dtype.kind not in 'iu' or not np.array_equal(member, np.arange(31)):
        raise ValueError('all31_ordered_integer_native_members')
    delta = np.asarray(dataset.lead_time)
    if (delta.ndim != 1 or delta.dtype.kind != 'm' or np.isnat(delta).any()
            or np.any(np.diff(delta) <= np.timedelta64(0, 'h'))):
        raise ValueError('native_lead_axis')
    hours = delta / np.timedelta64(1, 'h')
    if not np.equal(hours, np.floor(hours)).all():
        raise ValueError('whole_native_leads')
    if not np.array_equal(hours[hours <= 120], np.arange(0, 121, 3)):
        raise ValueError('native_three_hour_axis_through120_required')
    lead_indices = [int(np.flatnonzero(hours == lead)[0]) for lead in LEADS if np.sum(hours == lead) == 1]
    if len(lead_indices) != len(LEADS):
        raise ValueError('all17_native_leads24_to120_required')
    lat_ix, lat = neighbours(dataset.latitude, TARGET['latitude'])
    lon_ix, lon = neighbours(dataset.longitude, TARGET['longitude'])
    for key, unit in [('latitude', 'degree_north'), ('longitude', 'degree_east')]:
        if dataset[key].dims != (key,) or dataset[key].attrs.get('units') != unit:
            raise ValueError('native_geodetic_coordinate_operator')
    metadata = {}; allowed = {}; costs = {}
    for field, component in [('wind_u_10m', 'eastward_wind'), ('wind_v_10m', 'northward_wind')]:
        value = dataset[field]
        expected = ['init_time', 'ensemble_member', 'lead_time', 'latitude', 'longitude']
        if (list(value.dims) != expected or value.attrs.get('units') != 'm s-1'
                or value.attrs.get('step_type') != 'instant'
                or value.attrs.get('standard_name') != component):
            raise ValueError('native_10m_vector_layout_unit_or_operator')
        for key in ('height', 'heightAboveGround', 'level'):
            if key in value.attrs and value.attrs[key] != 10:
                raise ValueError('contradictory_native_10m_height')
        m = zarr.open_array(store=store, path=field, mode='r').metadata.to_dict()
        cost = profile_cost(m, index, lead_indices, lat_ix, lon_ix)
        if cost['inner_chunks'] != 1 or cost['inner_decoded_bytes'] != 2158592:
            raise ValueError('source_inner_chunk_profile_changed_requalify_before_weather')
        if len(cost['outer_key_suffixes']) != 1:
            raise ValueError('one_narrow_inner_chunk_per_component_required')
        metadata[field] = {'attrs': dict(value.attrs), 'zarr': m}; costs[field] = cost
        allowed[field] = field + '/' + cost['outer_key_suffixes'][0]
    if 2 * max(v['inner_decoded_bytes'] for v in costs.values()) + 64 * 1024**2 > RAM_DESIGN:
        raise ValueError('own_decoded_inner_peak_exceeds256MiB_design')
    valid = np.asarray(dataset.valid_time.isel(init_time=[index], lead_time=lead_indices))
    if valid.shape != (1, 17) or not np.array_equal(valid, wanted + delta[lead_indices][None]):
        raise ValueError('native_run_plus_lead_valid_identity')
    return {'init_index': index, 'lead_indices': lead_indices, 'latitude_indices': lat_ix,
        'longitude_indices': lon_ix, 'latitude': lat, 'longitude': lon,
        'dataset_attributes': dict(dataset.attrs),
        'fields': metadata, 'costs': costs, 'allowed_weather_keys': allowed}


def gefs_day(day, publisher):
    import icechunk
    import numpy as np
    import xarray as xr
    import zarr
    from zarr.abc.store import RangeByteRequest
    requested = initialization(day); started = now()
    repository = icechunk.Repository.open(icechunk.http_storage(GEFS_URL))
    observed = repository.readonly_session(branch='main'); snapshot = str(observed.snapshot_id)
    if not snapshot:
        raise ValueError('immutable_source_snapshot_required')
    session = repository.readonly_session(snapshot_id=snapshot)
    policy = {'allowed_weather_keys': {}, 'resource_plan': {
        'max_metadata_requests': 128, 'metadata_coordinate_payload_budget_bytes': 4 * 1024**2,
        'max_weather_requests': 8, 'range_payload_max_bytes': 2224128,
        'suffix_index_max_bytes': 65536, 'received_weather_payload_budget_bytes': 8 * 1024**2}}
    budget = Budget(policy)
    coordinate_store = coordinate_bounded_store(session.store,zarr.storage.WrapperStore,RangeByteRequest)
    store = bounded_store(coordinate_store, budget, zarr.storage.WrapperStore)
    with xr.open_zarr(store=store, chunks=None, decode_timedelta=True) as dataset:
        proof = gefs_metadata(dataset, store, zarr, requested)
        budget.allowed = set(proof['allowed_weather_keys'].values())
        subset = dataset[['wind_u_10m', 'wind_v_10m']].isel(
            init_time=[proof['init_index']], lead_time=proof['lead_indices'],
            latitude=proof['latitude_indices'], longitude=proof['longitude_indices'])
        native = {}
        for field in ('wind_u_10m', 'wind_v_10m'):
            # Only this admitted four-cell selection reaches weather .load().
            native[field] = np.asarray(subset[field].load(), dtype=np.float32)
            if native[field].shape != (1, 31, 17, 2, 2):
                raise ValueError('exact_native_four_cell17lead31member_shape')
        retrieved = now()
        raw = io.BytesIO()
        np.savez_compressed(raw, **native, member_ids=np.arange(31, dtype=np.int64),
            lead_hours=np.asarray(LEADS, dtype=np.int64),
            init_epoch_hour=np.asarray([int(np.datetime64(day, 'h').astype(np.int64))]),
            metadata_json=np.frombuffer(canonical(proof), dtype=np.uint8))
        raw_ref = publisher.save('gefs-native-four-cells.npz', raw.getvalue())
        a, b = proof['latitude']['upper_weight'], proof['longitude']['upper_weight']
        point = {}
        for field, values in native.items():
            cells = values.astype(np.float64)
            result = cells[..., 0, 0] * (1-a) * (1-b) + cells[..., 0, 1] * (1-a) * b + cells[..., 1, 0] * a * (1-b) + cells[..., 1, 1] * a * b
            result[~np.isfinite(cells).all(axis=(-2, -1))] = np.nan
            point[field] = result[0].T
    access = budget.receipt()
    access['coordinate_conservative_peak_bytes_admitted']=dict(coordinate_store.approved)
    access['all_metadata_coordinate_reads_bounded_before_transfer']=True
    proof.update(source_id=GEFS_SOURCE, source_url=GEFS_URL, snapshot_id=snapshot,
        dataset_version='0.2.0', initialization_utc=requested.isoformat(),
        request_started_utc=started, retrieved_utc=retrieved, access=access,
        native_subset=raw_ref, scientific_generation_transfer_qualified=False,
        operational_model_cycle=None, source_publication_utc=None,
        operator=OPERATOR, target=TARGET, weather_time_interpolation=False)
    proof_ref = publisher.save('gefs-native-proof.json', proof); complete = now()
    rows = {}
    for j, lead in enumerate(LEADS):
        u, v = point['wind_u_10m'][j], point['wind_v_10m'][j]
        if not np.isfinite(u).all() or not np.isfinite(v).all():
            rows[lead] = {'status': 'NATIVE_MEMBER_NONFINITE', 'proof': proof_ref}; continue
        rows[lead] = {'source_id': GEFS_SOURCE, 'generation_id': 'dataset0.2.0/snapshot:' + snapshot,
            'initialization_utc': requested.isoformat(), 'lead_hours': lead,
            'valid_utc': (requested + timedelta(hours=lead)).isoformat(),
            'native_time_resolution_hours': 3, 'retrieved_utc': retrieved, 'known_by_utc': complete,
            'unit': 'm/s', 'operator': OPERATOR, 'member_ids': list(range(31)),
            'member_u_ms': u.tolist(), 'member_v_ms': v.tolist(), 'qualified': True,
            'provenance': {'native_four_cells': raw_ref, 'native_geometry_and_capture': proof_ref}}
    return rows, proof_ref


def decode_ifs(raw):
    import eccodes as ec
    header_keys = ['shortName', 'units', 'level', 'typeOfLevel', 'stepType',
        'indicatorOfUnitOfTimeRange', 'dataDate', 'dataTime', 'validityDate',
        'validityTime', 'startStep', 'endStep']
    generation_keys = ['edition', 'centre', 'subCentre', 'generatingProcessIdentifier',
        'typeOfGeneratingProcess', 'tablesVersion', 'localTablesVersion',
        'gridType', 'Ni', 'Nj', 'iDirectionIncrementInDegrees', 'jDirectionIncrementInDegrees', 'scanningMode']
    h = ec.codes_new_from_message(raw)
    try:
        header = {k: ec.codes_get_string(h, k) for k in header_keys}
        generation = {k: ec.codes_get_string(h, k) for k in generation_keys}
        if (generation['gridType'] != 'regular_ll'
                or float(generation['iDirectionIncrementInDegrees']) != .25
                or float(generation['jDirectionIncrementInDegrees']) != .25):
            raise ValueError('IFS_native025grid_required')
        nearest = ec.codes_grib_find_nearest(h, TARGET['latitude'], TARGET['longitude'], npoints=4)
        lat = [r['lat'] for r in nearest]; lon = [r['lon'] if r['lon'] <= 180 else r['lon']-360 for r in nearest]
        values = [r['value'] for r in nearest]
        missing = [not bool(v) for v in ec.codes_get_elements(h, 'bitmap', [r['index'] for r in nearest])] if ec.codes_get(h, 'bitmapPresent') else [False] * len(values)
        point, cells = bilinear(lat, lon, values, missing,
            target_latitude=TARGET['latitude'], target_longitude=TARGET['longitude'])
        return point, {'header': header, 'generation': generation, 'bracketing_cells': cells,
                       'packing': ec.codes_get_string(h, 'packingType')}
    finally:
        ec.codes_release(h)


def optional_ifs(point, proof, requested, lead):
    h = proof['header']; name = h['shortName']; run = str(h['dataDate']) + f"{int(h['dataTime']):04d}"
    valid = str(h['validityDate']) + f"{int(h['validityTime']):04d}"
    if (run != requested.strftime('%Y%m%d%H%M')
            or valid != (requested + timedelta(hours=lead)).strftime('%Y%m%d%H%M')
            or float(h['endStep']) != lead or not math.isfinite(point)):
        raise ValueError('optional_native_field_time_or_value')
    expected = {'2d': ('K', 'instant'), 'tcc': (('1', '%', 'percent'), 'instant'),
                'tp': ('m', 'accum'), '10fg': (('m s**-1', 'm s-1'), 'max')}
    unit, step = expected[name]
    start, end = float(h['startStep']), float(h['endStep'])
    if (h.get('indicatorOfUnitOfTimeRange') != 'h' or not math.isfinite(start)
            or not 0 <= start <= end or (step == 'instant' and start != end)
            or (name == '2d' and (str(h.get('level')) != '2' or h.get('typeOfLevel') != 'heightAboveGround'))
            or (name == '10fg' and (str(h.get('level')) != '10' or h.get('typeOfLevel') != 'heightAboveGround'))
            or (name == 'tp' and (str(h.get('level')) != '0' or h.get('typeOfLevel') != 'surface'))
            or (name == 'tcc' and (str(h.get('level')) != '0' or h.get('typeOfLevel') not in ('surface', 'entireAtmosphere')))):
        raise ValueError('optional_native_field_interval_or_vertical_operator_unqualified')
    if h['stepType'] != step or h['units'] not in (unit if isinstance(unit, tuple) else (unit,)):
        raise ValueError('optional_native_field_operator_unqualified')
    return {'native_value': point, 'native_units': h['units'], 'step_type': h['stepType'],
        'interval_start_hours': float(h['startStep']), 'interval_end_hours': float(h['endStep']),
        'calibration': 'RAW_UNCALIBRATED_NATIVE_CHANNEL', 'header': h}


def ifs_day(day, publisher, *, opener=None, decoder=decode_ifs):
    requested = initialization(day); stamp = requested.strftime('%Y%m%d')
    opener = opener or urllib.request.build_opener(NoRedirect())
    rows = {}; generation_seen = None
    for lead in LEADS:
        prefix = f'ifs-{lead:03d}'
        url = IFS_PREFIX + f'{stamp}/00z/ifs/0p25/oper/{stamp}000000-{lead}h-oper-fc.grib2'
        try:
            body, receipt = fetch(opener, url.removesuffix('.grib2') + '.index')
            ir = publisher.save(prefix+'-index-receipt.json', receipt)
            if body is None:
                rows[lead] = {'status': 'INDEX_UNAVAILABLE', 'proof': ir}; continue
            index = publisher.save(prefix+'.index', body)
            entries = [json.loads(l) for l in body.decode('ascii').splitlines() if l.strip()]
            selected = [r for r in entries if r.get('param') in set(FIELDS) | OPTIONAL_IFS
                and r.get('levtype') == 'sfc' and str(r.get('step')) == str(lead)
                and r.get('type', 'fc') == 'fc']
            required = [r for r in selected if r['param'] in FIELDS]
            if len(required) != 5 or {r['param'] for r in required} != set(FIELDS):
                raise ValueError('all5_unique_IFS_native_fields_required')
            headers = {}; values = {}; captures = []; optional = {}; retrieved = None
            optional_counts = {name: sum(e['param'] == name for e in selected) for name in OPTIONAL_IFS}
            for name, count in optional_counts.items():
                if count > 1: optional[name] = {'status': 'DUPLICATE_OPTIONAL_INDEX_OPERATOR_UNQUALIFIED', 'count': count}
            selected = required + [e for e in selected if e['param'] in OPTIONAL_IFS and optional_counts[e['param']] == 1]
            for entry in selected:
                name = entry['param']; lo, size = int(entry['_offset']), int(entry['_length'])
                if lo < 0 or size <= 0:
                    raise ValueError('source_index_positive_message_range_required')
                raw, wire = fetch(opener, url, byte_range=(lo, lo+size-1))
                wr = publisher.save(prefix+'-'+name+'-receipt.json', wire)
                if raw is None:
                    if name in FIELDS: raise ValueError('required_IFS_message_unavailable')
                    optional[name] = {'status': 'OPTIONAL_TRANSFER_UNAVAILABLE', 'receipt': wr}; continue
                if not raw.startswith(b'GRIB') or not raw.endswith(b'7777'):
                    raise ValueError('native_GRIB_message_framing')
                raw_ref = publisher.save(prefix+'-'+name+'.grib2', raw)
                try:
                    point, proof = decoder(raw)
                    if proof['header']['shortName'] != name:
                        raise ValueError('source_index_native_parameter_mismatch')
                except (ValueError, KeyError, TypeError) as exc:
                    if name in FIELDS: raise
                    optional[name] = {'status': 'RAW_PRESERVED_DECODER_NOT_QUALIFIED', 'raw': raw_ref,
                        'reason': type(exc).__name__ + ':' + str(exc)}
                    continue
                if name in FIELDS:
                    headers[name], values[name] = proof['header'], point
                    if generation_seen is None: generation_seen = proof['generation']
                    if proof['generation'] != generation_seen:
                        raise ValueError('mixed_IFS_native_generation_headers')
                else:
                    try:
                        if proof['generation'] != generation_seen:
                            raise ValueError('optional_generation_mismatch')
                        optional[name] = optional_ifs(point, proof, requested, lead)
                    except ValueError: optional[name] = {'status': 'RAW_PRESERVED_OPERATOR_NOT_QUALIFIED', 'proof': proof}
                captures.append({'parameter': name, 'raw': raw_ref, 'receipt': wr, 'native': proof})
                retrieved = wire['received_finished_utc']
            f = feature_record(headers, values, source_id=IFS_SOURCE, artifact_sha256=index['sha256'],
                generation=generation_seen, target=TARGET)
            if (f['initialization_utc'] != requested.isoformat() or f['lead_hours'] != lead):
                raise ValueError('requested_IFS_run_lead_binding')
            proof = {'schema': 'm3-daily-ifs-native-point-proof-v1', 'target': TARGET,
                'field_proofs': captures, 'feature_record': f, 'source_index': index,
                'index_receipt': ir, 'raw_channels': optional,
                'scientific_generation_transfer_qualified': False,
                'model_surface_not_target_barometer': True}
            pr = publisher.save(prefix+'-native-proof.json', proof); completed = now()
            rows[lead] = {'source_id': IFS_SOURCE,
                'generation_id': 'native-header-sha256:' + digest(canonical(generation_seen)),
                'initialization_utc': requested.isoformat(), 'valid_utc': (requested + timedelta(hours=lead)).isoformat(),
                'lead_hours': lead, 'native_time_resolution_hours': 3, 'retrieved_utc': retrieved,
                'known_by_utc': completed, 'unit': 'm/s', 'operator': OPERATOR,
                'u10_ms': f['wind_u_ms'], 'v10_ms': f['wind_v_ms'], 'speed10_ms': f['wind_speed_ms'],
                'provenance': {'native_fields_and_capture': pr},
                'raw_channels': {'temperature_c': f['temperature_c'],
                    'pressure_model_surface_hpa': f['pressure_model_surface_hpa'],
                    'pressure_msl_hpa': f['pressure_msl_hpa'], 'optional': optional,
                    'calibration': 'RAW_UNCALIBRATED_NATIVE_CHANNELS'}}
        except (ValueError, KeyError, TypeError, urllib.error.URLError) as exc:
            rows[lead] = {'status': 'NATIVE_IFS_CAPTURE_FAILED', 'reason': type(exc).__name__ + ':' + str(exc)}
    return rows


def collect(output, init_day, publish_bytes=None, publish_file=None, *,
            current=None, gefs_provider=gefs_day, ifs_provider=ifs_day):
    """Capture exactly17 attempted cases; never infer or self-admit the result."""
    requested = initialization(init_day); current = current or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() != timedelta(0) or requested > current:
        raise ValueError('actual_UTC_clock_and_nonfuture00UTC_initialization_required')
    publisher = Publisher(output, publish_bytes, publish_file)
    plan_ref = publisher.save('capture-plan.json', {'schema': 'm3-daily-native-capture-plan-v1',
        'created_utc': now(), 'target': TARGET, 'initialization_utc': requested.isoformat(),
        'native_leads': list(LEADS), 'GEFS_source_url': GEFS_URL, 'IFS_source_prefix': IFS_PREFIX,
        'working_RAM_design_bytes': RAM_DESIGN, 'full_GEFS_shard_reads_forbidden': True,
        'fits': False, 'truth_reads': False, 'scientific_release': False})
    try:
        gefs, gefs_proof = gefs_provider(init_day, publisher)
    except Exception as exc:
        reason = type(exc).__name__ + ':' + str(exc)
        failure = publisher.save('gefs-failure.json', {'status': 'GEFS_NATIVE_CAPTURE_FAILED', 'reason': reason, 'utc': now()})
        gefs, gefs_proof = {}, failure
    try:
        ifs = ifs_provider(init_day, publisher)
    except Exception as exc:
        failure = publisher.save('ifs-failure.json', {'status': 'IFS_NATIVE_CAPTURE_FAILED',
            'reason': type(exc).__name__ + ':' + str(exc), 'utc': now()})
        ifs = {lead: {'status': 'IFS_NATIVE_CAPTURE_FAILED', 'proof': failure} for lead in LEADS}
    records = []; attempts = []
    for lead in LEADS:
        i, g = ifs.get(lead), gefs.get(lead)
        valid = requested + timedelta(hours=lead)
        status = 'NATIVE_PAIR_CAPTURED_PENDING_EXTERNAL_ADMISSION'
        if not i or 'source_id' not in i: status = 'IFS_MISSING_OR_UNQUALIFIED'
        elif not g or 'source_id' not in g: status = 'GEFS_MISSING_OR_UNQUALIFIED'
        elif valid <= datetime.now(timezone.utc): status = 'CAPTURE_COMPLETE_AFTER_VALIDITY_NOT_PROSPECTIVE'
        else:
            if i['initialization_utc'] != g['initialization_utc'] or i['valid_utc'] != g['valid_utc'] or i['lead_hours'] != g['lead_hours']:
                raise ValueError('native_family_join_mismatch_no_case_substitution')
            records.append({'record_id': requested.strftime('%Y%m%d') + f'-00-L{lead:03d}',
                            'target': TARGET, 'ifs': i, 'gefs': g})
        attempts.append({'lead_hours': lead, 'valid_utc': valid.isoformat(), 'status': status,
            'IFS_status': i.get('status', 'CAPTURED') if i else 'MISSING',
            'GEFS_status': g.get('status', 'CAPTURED') if g else 'MISSING'})
    envelope = {'schema': 'm3-frozen-native-forecast-input-v1', 'target': TARGET, 'records': records}
    input_ref = publisher.save('native-input-envelope.json', envelope)
    result = {'schema': 'm3-daily-native-capture-result-v1', 'initialization_utc': requested.isoformat(),
        'actual_completed_utc': now(), 'input': input_ref, 'plan': plan_ref, 'GEFS_proof': gefs_proof,
        'attempts': attempts, 'expected_cases': 17, 'captured_native_pairs': len(records),
        'raw_ifs_records': [{'target': TARGET, 'ifs': row} for row in ifs.values() if row.get('source_id')],
        'status': 'CAPTURE_COMPLETE_PENDING_EXTERNAL_ADMISSION' if len(records) == 17 else 'PARTIAL_PENDING_EXTERNAL_ADMISSION',
        'artifacts': dict(publisher.refs),
        'independent_input_admission': False, 'source_generation_transfer_qualified': False,
        'truth_reads': False, 'fits': False, 'forecast_issued': False, 'scientific_release': False}
    receipt = publisher.save('capture-result.json', result)
    return {'result': result, 'receipt': receipt, 'input': input_ref}
