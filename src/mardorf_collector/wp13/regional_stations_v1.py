"""DWD CDC original capture and bounded native transfers for registered stations.

No fitting or wind features. ZIP originals and their dated metadata are retained;
CSV support remains scientifically open until precise product conventions bind.
"""
from copy import deepcopy
import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import zipfile

from .core_v1 import ContractsV1, canonical, digest, identify, stamp, utc
from .adapters_v1 import base_field
from .store_v1 import write_delivery

ROOT = Path(__file__).resolve().parents[3]
POLICY = 'config/dev03_wp15_regional_stations_v1.json'
STATE = 'data/weather_native/dwd_cdc_v1/state.json'
ORIGINALS = 'data/weather_native/dwd_cdc_v1/originals/'
ARCHIVE = 'data/weather_native/dwd_cdc_v1/archive_manifest.json'


def configuration(root=ROOT):
    policy = json.loads((Path(root) / POLICY).read_bytes())
    registry = json.loads((Path(root) / policy['registry_path']).read_bytes())
    if (policy['artifact_version'] != 'wp15-regional-stations-v1'
            or digest(registry) != policy['registry_sha256']
            or not 1 <= len(policy['stations']) <= 32
            or len({s['id'] for s in policy['stations']}) != len(policy['stations'])):
        raise ValueError('Regional station configuration identity mismatch')
    contracts = ContractsV1(root=root, registry=registry)
    return policy, contracts


def source_url(policy, station, product, *, hourly=False):
    if station not in {s['id'] for s in policy['stations']}:
        raise ValueError('Unregistered station')
    if hourly:
        spec = policy['hourly_products'][product]
        return policy['base_url'] + f"hourly/{product}/recent/stundenwerte_{spec['prefix']}_{station}_akt.zip"
    names = {'wind': 'wind', 'air_temperature': 'TU', 'precipitation': 'nieder'}
    return policy['base_url'] + f"10_minutes/{product}/now/10minutenwerte_{names[product]}_{station}_now.zip"


def fetch(url, policy):
    import requests
    if (not url.startswith(policy['base_url']) or '..' in url or '?' in url):
        raise ValueError('Unbound CDC source URL')
    with requests.get(url, stream=True, allow_redirects=False, timeout=(10, 90),
                      headers={'Accept-Encoding': 'identity', 'User-Agent': 'mardorf/WP15-I04'}) as response:
        response.raise_for_status()
        limit = policy['max_original_bytes']
        declared = response.headers.get('Content-Length')
        if declared is not None and not 0 <= int(declared) <= limit:
            raise ValueError('Original declared byte budget exceeded')
        body = bytearray()
        for part in response.iter_content(65536):
            if len(body) + len(part) > limit:
                raise ValueError('Original byte budget exceeded')
            body.extend(part)
        if declared is not None and len(body) != int(declared):
            raise ValueError('Original transport length mismatch')
        return bytes(body), stamp(datetime.now(timezone.utc))


def rows(raw, station, policy, start, end):
    """Stream a bounded ZIP member; row selection never reallocates full CSV."""
    if len(raw) > policy['max_original_bytes'] or not utc(start) < utc(end):
        raise ValueError('Original/window budget exceeded')
    begin, finish = utc(start), utc(end)
    lower, upper = begin.strftime('%Y%m%d%H%M'), (finish+timedelta(minutes=1)).strftime('%Y%m%d%H%M')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if (len(members) > policy['max_zip_members']
                or sum(x.file_size for x in members) > policy['max_zip_uncompressed_bytes']
                or any(PurePosixPath(x.filename).is_absolute() or '..' in PurePosixPath(x.filename).parts
                       or '\\' in x.filename or x.flag_bits & 1 for x in members)):
            raise ValueError('ZIP inventory budget or path violation')
        products = [x for x in members if Path(x.filename).name.startswith('produkt_') and x.filename.endswith('.txt')]
        if len(products) != 1:
            raise ValueError('Unique CDC product member required')
        member = products[0]
        with archive.open(member) as stream:
            text_stream = io.TextIOWrapper(stream, encoding='latin1')
            def bounded_lines():
                while True:
                    line = text_stream.readline(8193)
                    if len(line)>8192:
                        raise ValueError('CDC CSV line budget exceeded')
                    if not line: break
                    yield line
            reader = csv.DictReader(bounded_lines(), delimiter=';')
            columns = [x.strip() for x in reader.fieldnames or []]
            if len(columns) != len(set(columns)) or not {'STATIONS_ID', 'MESS_DATUM'} <= set(columns):
                raise ValueError('CDC station/time header missing or duplicated')
            previous = None
            for number, item in enumerate(reader, 2):
                row = {k.strip(): v.strip() if v is not None else None for k, v in item.items() if k is not None}
                if None in item or any(v is None for v in row.values()):
                    raise ValueError('Malformed CDC row')
                if row['STATIONS_ID'].zfill(5) != station:
                    raise ValueError('CDC native station contradiction')
                text = row['MESS_DATUM'].replace(':', '')
                if not re.fullmatch(r'\d{10}|\d{12}', text):
                    raise ValueError('Explicit hourly/ten-minute UTC stamp required')
                order = text.ljust(12, '0')
                if previous is not None and order <= previous:
                    raise ValueError('Duplicate or unordered CDC timestamp')
                previous = order
                if lower <= order < upper:
                    measured = datetime.strptime(text, '%Y%m%d%H' if len(text) == 10 else '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
                    if not begin <= measured < finish:
                        continue
                    yield {'member': member.filename, 'line': number, 'native': row, 'measured_at_utc': stamp(measured)}


def numeric(value):
    if value in ('', '-999', '-999.0', '-999.00', '---'):
        return None
    value = float(value)
    if not __import__('math').isfinite(value):
        raise ValueError('Nonfinite CDC number')
    return value


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
                       or (utc(end) - utc(start)).total_seconds() > 86400):
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



def combine(results):
    """One station delivery per poll; shared source proof, bounded common wire format."""
    if not results:
        return None
    if len(results)==1:
        return results[0]
    first=results[0]
    if any(r['job']['station_id']!=first['job']['station_id'] or r['job']['context']!=first['job']['context']
           or r['envelope']['configuration']!=first['envelope']['configuration'] for r in results):
        raise ValueError('Mixed station/context/configuration bundle')
    seen=max((r['original_capture_at_utc'] for r in results),key=utc)
    fields=[];raw={};objects=[];windows=[]
    for index,result in enumerate(results):
        names={'raw-0':'raw-'+str(index*2),'raw-1':'raw-'+str(index*2+1)}
        for name,body in result['raw_bytes'].items():raw[names[name]]=body
        for item in result['envelope']['raw_objects']:
            item=deepcopy(item);item['object_id']=names[item['object_id']];objects.append(item)
        for field in result['fields']:
            field=deepcopy(field);field['source']['raw_object_id']=names['raw-0']
            field['source']['native_metadata_object_id']=names['raw-1'];field['time']['first_seen_at_utc']=seen
            fields.append(identify(field,'field_id'))
        windows.append(result['envelope']['extensions']['dwd:cdc-window:v1'])
    if len(fields)>4096 or sum(map(len,raw.values()))>8*1024**2:
        raise ValueError('Bundled native transfer budget exceeded')
    job=deepcopy(first['job']);job['source_job_ids']=[r['job']['job_id'] for r in results];job=identify(job,'job_id')
    fragment=b''.join(canonical(f)+b'\n' for f in fields);path='fields/'+job['job_id']+'.jsonl'
    envelope=deepcopy(first['envelope']);envelope['raw_objects']=objects
    envelope['capture'].update(request_id=job['job_id'],retrieved_at_utc=seen,verified_at_utc=seen)
    envelope['field_fragments']=[{'path':path,'sha256':hashlib.sha256(fragment).hexdigest(),'schema_version':'dev03-wp12-native-field-v1','row_count':len(fields)}]
    envelope['extensions']={'dwd:cdc-windows:v1':{'windows':windows}};envelope=identify(envelope,'envelope_id')
    return dict(first,job=job,envelope=envelope,fields=fields,raw_bytes=raw,fragment_bytes={path:fragment},
                source_status=[entry for result in results for entry in result['source_status']],original_capture_at_utc=seen)


def publish_delivery(cloud, result, *, cursor=None, prior_cursor=None):
    import tempfile
    with tempfile.TemporaryDirectory(prefix='mardorf-dwd-delivery-') as folder:
        receipt = write_delivery(folder, result)['receipt_id']
        prefix = 'data/inbox/native_wp15/dwd_cdc/'+receipt+'/'
        files = {p.name:p.read_bytes() for p in (Path(folder)/receipt).iterdir() if p.is_file()}
    ready = {'schema_version':1,'artifact_version':'wp15-native-ingress-v1','receipt_id':receipt,
             'configuration_sha256':result['envelope']['configuration']['sha256'],
             'envelope_id':result['envelope']['envelope_id'],
             'files':{name:{'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()} for name,body in files.items()}}
    changes = {prefix+name:body for name,body in files.items()}
    if result['envelope']['context'] == 'historical':
        # Retain the shared historical ZIP once, never once per daily delivery.
        for key,body in result['raw_bytes'].items():
            if body.startswith(b'PK'):
                name = 'raw-' + hashlib.sha256(key.encode()).hexdigest()
                identity = hashlib.sha256(body).hexdigest()
                archive_path = ORIGINALS + identity + '.zip'
                original = cloud.read(archive_path)
                if len(original) != len(body) or hashlib.sha256(original).hexdigest() != identity:
                    raise ValueError('Shared historical CDC original identity mismatch')
                ready['files'][name]['archive_path'] = archive_path
                del changes[prefix+name]
    changes[prefix+'ready.json'] = canonical(ready)
    def merge(current, incoming):
        if cursor is not None and current.read(STATE, required=False) != prior_cursor:
            raise ValueError('Concurrent CDC cursor changed; retry acquisition')
        cursor_body = incoming.get(STATE)
        out = immutable_merge(current, {p:b for p,b in incoming.items() if p!=STATE})
        if cursor_body is not None: out[STATE] = cursor_body
        return out
    if cursor is not None: changes[STATE] = canonical(cursor)
    cloud.publish(changes,metadata={'channel':'wp15-regional-dwd'},merge=merge)
    return {'receipt_id':receipt,'fields':len(result['fields']),'original_bytes':len(result['raw_bytes']['raw-0'])}


def immutable_merge(current, changes):
    out = {}
    for path, body in changes.items():
        previous = current.read(path, required=False)
        if previous is not None and previous != body:
            raise ValueError('Immutable CDC publication collision')
        if previous is None:
            out[path] = body
    return out


def operate(cloud, *, root=ROOT, archive=False, history_station=None, history_product='wind', history_day=None):
    policy, contracts = configuration(root)
    if policy['enabled'] is not True:
        return {'status':'DISABLED'}
    ready = json.loads(cloud.read('data/weather_native/consumer_readiness_dwd_cdc_v1.json'))
    if ready.get('available') is not True or ready.get('configuration_sha256') != contracts.configuration['sha256']:
        raise ValueError('Deployed regional native consumer readiness required')
    state_body = cloud.read(STATE, required=False)
    prior = json.loads(state_body) if state_body else {'stations':{},'last_recent_utc':None}
    state = deepcopy(prior)
    report = {'status':'PASS','stations':{},'deliveries':[],'provider_requests':0,'weather_git_bytes_written':0,'scientific_gate':'OPEN'}
    now = datetime.now(timezone.utc)
    start,end = stamp(now-timedelta(hours=policy['current_window_hours'])),stamp(now)
    recent = not prior['last_recent_utc'] or (now-utc(prior['last_recent_utc'])).total_seconds() >= policy['recent_every_hours']*3600
    if not archive and history_station is None:
        for station in policy['stations']:
            known = state['stations'].setdefault(station['id'],{})
            station_results = []
            report['stations'][station['id']] = {'products':{},'status':'PASS'}
            for hourly, products in [(False,policy['current_products']), (True,policy['hourly_products'] if recent else {})]:
                for product in products:
                    raw,seen = fetch(source_url(policy,station['id'],product,hourly=hourly),policy)
                    report['provider_requests'] += 1
                    result, updates = prepare(raw,seen,station['id'],product,start=start,end=end,hourly=hourly,
                                              contracts=contracts,policy=policy,known=known)
                    if result:
                        station_results.append(result)
                    known.update(updates)
                    report['stations'][station['id']]['products'][('hourly/' if hourly else '10_minutes/')+product] = {'new_fields':len(result['fields']) if result else 0,'rows_in_window':len(updates)}
            # Atomic station cursor and delivery publication prevents partial retries from duplicating rows.
            state['stations'][station['id']] = {k:v for k,v in known.items() if k.rsplit('/',1)[-1] >= (now-timedelta(hours=72)).strftime('%Y%m%d%H')}
            combined = combine(station_results)
            if combined:
                report['deliveries'].append(publish_delivery(cloud,combined,cursor=state,prior_cursor=state_body))
                state_body = canonical(state)
        if recent:
            state['last_recent_utc'] = stamp(now)
        def merge_state(current, changes):
            if current.read(STATE, required=False) != state_body:
                raise ValueError('Concurrent CDC cursor changed; retry acquisition')
            return changes
        cloud.publish({STATE:canonical(state)},metadata={'channel':'wp15-regional-cursor'},merge=merge_state)
    elif archive:
        if cloud.read(ARCHIVE,required=False) is not None:
            return dict(report,status='VERIFIED_EXISTING_ARCHIVE')
        inventory = []
        for station in policy['stations']:
            for product,urls in dict(policy['historical_urls'], **policy.get('raw_archive_products',{})).items():
                if station['id'] not in urls:
                    continue
                url = urls[station['id']]
                raw,seen = fetch(url,policy)
                report['provider_requests'] += 1
                identity = hashlib.sha256(raw).hexdigest()
                # Force ZIP/header/station/time integrity through the complete selected archive window.
                count = sum(1 for _ in rows(raw,station['id'],policy,policy['archive_start_utc'],policy['archive_end_utc']))
                path = ORIGINALS+identity+'.zip'
                cloud.publish({path:raw},metadata={'channel':'wp15-regional-original'},merge=immutable_merge)
                if hashlib.sha256(cloud.read(path)).hexdigest() != identity:
                    raise ValueError('Cold original archive readback mismatch')
                inventory.append({'station_id':station['id'],'product':product,'source_url':url,'path':path,
                                  'sha256':identity,'bytes':len(raw),'captured_at_utc':seen,'selected_rows':count,
                                  'native_mapping_status':'bounded_backfill' if product in policy['hourly_products'] else 'original_only'})
        manifest = {'schema_version':1,'artifact_version':'wp15-dwd-cdc-archive-v1','configuration_sha256':contracts.configuration['sha256'],
                    'start_utc':policy['archive_start_utc'],'end_utc':policy['archive_end_utc'],'originals':inventory,
                    'normalization_status':'bounded_backfill_pending','scientific_gate':'OPEN','license':policy['license'],'attribution':policy['attribution']}
        cloud.publish({ARCHIVE:canonical(manifest)},metadata={'channel':'wp15-regional-archive'},merge=immutable_merge)
        report['archive'] = {'originals':len(inventory),'bytes':sum(x['bytes'] for x in inventory),'selected_rows':sum(x['selected_rows'] for x in inventory)}
    else:
        day = history_day or policy['initial_normalization_day']
        begin = datetime.strptime(day,'%Y-%m-%d').replace(tzinfo=timezone.utc)
        start,end = stamp(begin),stamp(begin+timedelta(days=1))
        manifest = json.loads(cloud.read(ARCHIVE))
        selected = [s for s in policy['stations'] if history_station in ('all',s['id'])]
        if not selected or history_product not in policy['hourly_products']:
            raise ValueError('Registered historical station/product required')
        for station in selected:
            cursor = 'data/weather_native/dwd_cdc_v1/history/'+station['id']+'/'+history_product+'/'+day+'.json'
            if cloud.read(cursor,required=False) is not None:
                continue
            original = next(x for x in manifest['originals'] if x['station_id']==station['id'] and x['product']==history_product)
            raw = cloud.read(original['path'])
            if hashlib.sha256(raw).hexdigest()!=original['sha256']:
                raise ValueError('Historical original integrity mismatch')
            result,_ = prepare(raw,original['captured_at_utc'],station['id'],history_product,start=start,end=end,
                               hourly=True,historical=True,contracts=contracts,policy=policy)
            proof = publish_delivery(cloud,result) if result else {'status':'VERIFIED_EMPTY','fields':0}
            cloud.publish({cursor:canonical(dict(proof,start_utc=start,end_utc=end,original_sha256=original['sha256']))},
                          metadata={'channel':'wp15-regional-history-cursor'},merge=immutable_merge)
            report['deliveries'].append(proof)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',action='store_true')
    parser.add_argument('--history-station')
    parser.add_argument('--history-product',default='wind')
    parser.add_argument('--history-day')
    args=parser.parse_args()
    from mardorf_collector.storage.runtime import load_runtime
    from mardorf_collector.runtime.cloud_environment import environment
    cloud=load_runtime(Path.cwd(),environ=environment(Path.cwd()))
    report=operate(cloud,archive=args.archive,history_station=args.history_station,
                   history_product=args.history_product,history_day=args.history_day)
    Path('work').mkdir(exist_ok=True)
    Path('work/wp15_regional_stations.json').write_bytes(canonical(report))
    print('WP15_REGIONAL_STATIONS='+json.dumps(report),flush=True)


if __name__=='__main__':
    main()
