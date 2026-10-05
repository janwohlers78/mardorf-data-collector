"""Explicit monthly-window successor; source rows are decoded once per work unit."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from ..wp13.regional_stations_v1 import configuration, source_url, numeric, combine, POLICY, STATE, ARCHIVE, ORIGINALS
from ..wp13 import regional_stations_v1 as predecessor
from ..wp13.core_v1 import canonical, digest, identify, stamp, utc
from ..wp13.adapters_v1 import base_field
ROOT = Path(__file__).resolve().parents[3]
_window = None

def cache_window(raw, station, policy, start, end):
    global _window
    key = (hashlib.sha256(raw).hexdigest(), station, digest(policy))
    if _window is None or _window[0] != key or not utc(_window[1]) <= utc(start) < utc(end) <= utc(_window[2]):
        _window = (key, start, end, tuple(predecessor.rows(raw, station, policy, start, end)))
    return _window

def rows(raw, station, policy, start, end):
    cached = cache_window(raw, station, policy, start, end)
    return (item for item in cached[3] if utc(start) <= utc(item['measured_at_utc']) < utc(end))

def extraction(raw, station, product, policy, start, end, *, hourly, known=None):
    specs = policy['hourly_products' if hourly else 'current_products'][product]['fields']
    selected = []
    updates = {}
    for item in rows(raw, station, policy, start, end):
        native = item['native']
        if not set(specs) <= native.keys():
            raise ValueError('Configured CDC parameter missing from product')
        key = ('hourly' if hourly else '10_minutes') + '/' + product + '/' + native['MESS_DATUM']
        revision = digest(native)
        updates[key] = revision
        if known is not None and known.get(key) == revision:
            continue
        selected.append(dict(item, ts=int(utc(item['measured_at_utc']).timestamp()),
            arch_int=3600 if hourly else 600,
            values=dict({name: numeric(native[name]) for name in specs}, ts=int(utc(item['measured_at_utc']).timestamp())), revision=revision))
        if len(selected) * len(specs) > policy['max_fields_per_delivery']:
            raise ValueError('Bounded native field budget exceeded')
    return {'source_decoded_sha256': hashlib.sha256(raw).hexdigest(), 'station_id': station,
            'product': product, 'cadence': 'hourly' if hourly else '10_minutes', 'rows': selected}, updates

def prepare(raw, seen, station, product, *, start, end, hourly=False, historical=False,
            contracts=None, policy=None, commit=None, known=None):
    if policy is None or contracts is None:
        policy, contracts = configuration()
    record = next(s for s in policy['stations'] if s['id'] == station)
    if historical and (not utc(policy['archive_start_utc']) <= utc(start) < utc(end) <= utc(policy['archive_end_utc'])
                       or (utc(end) - utc(start)).total_seconds() > 31*86400):
        raise ValueError('Bounded selected historical archive window required')
    if utc(end) > utc(seen):
        raise ValueError('Window after actual capture')
    document, updates = extraction(raw, station, product, policy, start, end, hourly=hourly, known=known)
    if not document['rows']:
        return None, updates
    url = policy['historical_urls'][product][station] if historical else source_url(policy, station, product, hourly=hourly)
    job = identify({'station_id': record['station_id'], 'sensor_id': record['sensor_id'],
                   'profile_id': policy['profile_id'], 'site_ids': [record['site_id']],
                   'target_ids': [], 'provider_binding_id': policy['provider_id'],
                   'kind': 'observation_historic' if historical else 'observation_current',
                   'context': 'historical' if historical else 'prospective',
                   'window': {'start_utc': start, 'end_utc': end}, 'source_url': url}, 'job_id')
    meta = canonical(document)
    raw_bytes = {'raw-0': raw, 'raw-1': meta}
    if sum(map(len, raw_bytes.values())) > contracts.limits['max_payload_bytes']:
        raise ValueError('Native transfer original/metadata byte budget exceeded')
    source = {'url': url, 'model_id_native': None, 'product_id_native': 'cdc-'+document['cadence']+'-'+product}
    fields = []
    specs = policy['hourly_products' if hourly else 'current_products'][product]['fields']
    lon, lat = contracts.tables['sites'][record['site_id']]['geometry']['coordinates']
    for index, row in enumerate(document['rows']):
        if utc(row['measured_at_utc']) > utc(seen):
            raise ValueError('Measurement after actual capture')
        for parameter, (quantity, unit) in specs.items():
            field = base_field(contracts, job, source, 'raw-0', record['site_id'], quantity,
                               f'/rows/{index}/values/{parameter}', row['values'][parameter], seen)
            field['source'].update(native_parameter=parameter, native_metadata_object_id='raw-1', source_revision=row['revision'])
            field['binding_id'] = 'dwd-cdc-'+document['cadence']+'-'+product+'-'+parameter+'-v1'
            field['unit_native'], field['unit_evidence'] = unit, 'provider_definition'
            field['level'].update(measurement_height_status='unknown')
            field['time'].update(measured_at_utc=row['measured_at_utc'],valid_start_utc=row['measured_at_utc'],
                valid_end_utc=row['measured_at_utc'],operator='unknown',closure='unknown',resolution_method='dwd-cdc-mess-datum-utc-support-open-v1')
            field['spatial_support'].update(actual_latitude=lat,actual_longitude=lon,method='station_registry')
            # QN is processing provenance, not a per-value rejection flag.
            field['extensions']['dwd:cdc-source:v1'] = {
                'product': product, 'cadence': document['cadence'], 'original_member': row['member'],
                'original_line': row['line'], 'quality': {k:v for k,v in row['native'].items() if k.startswith('QN')},
                'native_missing_token': row['native'][parameter] if field['value_native'] is None else None,
                'support_status': 'OPEN', 'historical_position_status': 'dated_original_metadata_retained_not_resolved',
                'license': policy['license'], 'attribution': policy['attribution']}
            fields.append(identify(field, 'field_id'))
    fragment = b''.join(canonical(f)+b'\n' for f in fields)
    path = 'fields/'+job['job_id']+'.jsonl'
    objects = [{'object_id': key,'path':'raw/'+hashlib.sha256(body).hexdigest()+'.bin',
                'transport_sha256':hashlib.sha256(body).hexdigest(),'transport_bytes':len(body),
                'decoded_sha256':hashlib.sha256(body).hexdigest(),'compression':'none','encoding':'binary' if key=='raw-0' else 'utf-8',
                'media_type':'application/octet-stream' if key=='raw-0' else 'application/json'} for key,body in raw_bytes.items()]
    commit = commit or subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
    if not re.fullmatch('[0-9a-f]{40}', commit) or commit == '0'*40:
        raise ValueError('Actual collector commit required')
    envelope = identify({'schema_version':1,'artifact_version':'dev03-wp12-raw-transfer-v1',
        'context':job['context'],'intended_use':'development','configuration':contracts.configuration,
        'capture':{'collector_commit_sha':commit,'request_id':job['job_id'],'retrieved_at_utc':seen,
                   'verified_at_utc':seen,'publication_evidence':'historical_retrieval' if historical else 'unknown'},
        'raw_objects':objects,'field_fragments':[{'path':path,'sha256':hashlib.sha256(fragment).hexdigest(),
             'schema_version':'dev03-wp12-native-field-v1','row_count':len(fields)}],
        'ensemble_sets':[],'required_capabilities':['wp12-native-field-v1','wp12-strict-validation-v2'],
        'extensions':{'dwd:cdc-window:v1':dict(start_utc=start,end_utc=end,product=product,station_id=station,
                                              hourly=hourly,historical=historical)}},'envelope_id')
    return {'job':job,'status':'received','envelope':envelope,'fields':fields,'raw_bytes':raw_bytes,
            'fragment_bytes':{path:fragment},'source_status':[{'source_index':0,'status':'received','http_status':200,
            'observed_at_utc':seen,'fields':[]}], 'original_capture_at_utc':seen}, updates
