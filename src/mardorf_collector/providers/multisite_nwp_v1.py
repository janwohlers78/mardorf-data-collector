"""One original response for a bounded site batch; no fitting or replacement.

This additive successor leaves frozen WP06/WP07 inputs untouched. Requests,
error bodies, unknown fields and initialization/capture clocks remain bound.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import time
import threading

import requests
from ..storage.archive import canonical

ROOT = Path(__file__).resolve().parents[3]
PREFIX = 'weather/archive/janwohlers78/mardorf-kitevorhersage/wp17-multisite/v1'
CONTROL = 'config/cloud_refs/wp17_multisite_nwp_v1.json'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def binding(root=ROOT):
    return json.loads((Path(root)/'config/wp17_multisite_nwp_v1.json').read_bytes())


def spec(run, root=ROOT):
    b = binding(root)
    stamp = datetime.fromisoformat(run.replace('Z', '+00:00'))
    if stamp.minute or stamp.second or stamp.hour not in (0, 6, 12, 18):
        raise ValueError('multisite_initialization_cycle')
    return dict(endpoint=b['endpoint'], params=dict(b['params'], run=stamp.strftime('%Y-%m-%dT%H:%M'),
        latitude=','.join(str(s['latitude']) for s in b['sites']),
        longitude=','.join(str(s['longitude']) for s in b['sites'])))


def validate(raw, request, root=ROOT):
    b = binding(root); documents = json.loads(raw)
    if request != spec(request['params']['run'], root) or not isinstance(documents, list) or len(documents) != len(b['sites']):
        raise ValueError('multisite_request_or_site_inventory')
    run = datetime.fromisoformat(request['params']['run']).replace(tzinfo=timezone.utc)
    units = json.loads((Path(root)/'config/wp07_single_run_v1.json').read_bytes())['units']
    for d, s in zip(documents, b['sites']):
        if (d.get('utc_offset_seconds') != 0 or abs(d['latitude']-s['latitude']) > .3
                or abs(d['longitude']-s['longitude']) > .3):
            raise ValueError('multisite_returned_grid_scope')
        h = d['hourly']
        if len(h['time']) != b['params']['forecast_hours'] or len(set(h['time'])) != len(h['time']):
            raise ValueError('multisite_time_inventory')
        for i, t in enumerate(h['time']):
            if (datetime.fromisoformat(t).replace(tzinfo=timezone.utc)-run).total_seconds() != i*3600:
                raise ValueError('multisite_initialization_axis')
        for f in b['params']['hourly'].split(','):
            if d['hourly_units'].get(f) != units[f] or len(h[f]) != len(h['time']):
                raise ValueError('multisite_field_unit_or_shape')
        # Retain additional fields. Validation is not a source column filter.
        for f, values in h.items():
            if len(values) != len(h['time']):
                raise ValueError('multisite_unknown_field_shape')
    return documents


def capture(run, *, root=ROOT, get=requests.get):
    b = binding(root); request = spec(run, root); began = datetime.now(timezone.utc).isoformat()
    raw = b''; status = 0; reason = None
    try:
        response = get(request['endpoint'], params=request['params'], timeout=(5,b['timeout_seconds']), allow_redirects=False)
        status, raw = response.status_code, response.content
        if len(raw) > b['max_response_bytes']:
            raise ValueError('multisite_response_budget')
        if status != 200:
            raise ValueError('multisite_HTTP_'+str(status))
        validate(raw, request, root)
    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
        reason = str(exc) if isinstance(exc,ValueError) else type(exc).__name__
    stamp = datetime.now(timezone.utc).isoformat()
    generation = b['generation_before'] if request['params']['run'] < b['generation_boundary_utc'][:16] else b['generation_after']
    receipt = dict(artifact_version='wp17-multisite-nwp-receipt-v1', request=request,
        requested_sites=b['sites'], source_binding_sha256=digest(canonical(b)),
        producer_code_sha256=digest(Path(__file__).read_bytes()), producer_commit=os.getenv('GITHUB_SHA'),
        event_id=os.getenv('GITHUB_RUN_ID') or 'local_authorized_capture',
        started_utc=began, captured_utc=stamp, available_utc=stamp, http_status=status,
        status='valid' if reason is None else 'invalid', reason=reason,
        generation=generation, generation_evidence=b['generation_evidence'],
        source_sha256=digest(raw), source_bytes=len(raw), raw_field_exclusions=[],
        historical_availability='retrospective_retrieval_NOT_original_publication',
        scientific_status='NOT_READY', native_admission=False)
    try:
        content=json.loads(raw)
        for document in content if isinstance(content,list) else []:document.pop('generationtime_ms',None)
        receipt['forecast_content_sha256']=digest(canonical(content))
    except (ValueError,TypeError):receipt['forecast_content_sha256']=digest(raw)
    return receipt, raw


def store(receipt, raw, backend):
    original = backend.put_bytes(PREFIX+'/originals/'+digest(raw), raw)
    if backend.get_bytes(original) != raw:
        raise ValueError('multisite_original_readback')
    value = dict(receipt, original=original.json()); body = canonical(value)
    ref = backend.put_bytes(PREFIX+'/receipts/'+digest(body)+'.json', body)
    if backend.get_bytes(ref) != body:
        raise ValueError('multisite_receipt_readback')
    return value, ref


def publication(receipt, raw, backend):
    value, ref = store(receipt, raw, backend)
    thin = dict(schema_version=1, artifact_version='wp17-multisite-nwp-control-v1', kind='wp17_multisite_nwp',
        generated_at_utc=receipt['captured_utc'], snapshot=ref.json(), readback_verified=True,
        bundle_ready=receipt['status']=='valid', metadata=dict(event_id=receipt['event_id'],payload_sha256=ref.sha256))
    paths = {'data/inbox/wp17_multisite/'+ref.sha256+'.json':canonical(value),
             'data/inbox/wp17_multisite/latest.json':canonical(value)}
    return paths, thin


def history(start, end_exclusive, output, backend, *, root=ROOT, seconds=900, previous=None):
    b = binding(root); a, z = date.fromisoformat(start), date.fromisoformat(end_exclusive)
    if not date.fromisoformat(b['historical_start']) <= a < z <= datetime.now(timezone.utc).date() or (z-a).days > b['max_history_days']:
        raise ValueError('multisite_completed_history_bounds')
    out = Path(output); out.mkdir(parents=True,exist_ok=True)
    runs = [(a+timedelta(days=i)).isoformat()+'T00:00' for i in range((z-a).days)]
    retained={}
    if previous is not None:
        parent=Path(previous);prior=json.loads(parent.read_bytes())
        for record in prior['records']:
            if record['status']=='valid':
                raw=(parent.parent/record['local_path']).read_bytes();validate(raw,record['receipt']['request'],root)
                if digest(raw)!=record['receipt']['source_sha256']:raise ValueError('multisite_resume_original_hash')
                (out/record['local_path']).write_bytes(raw)
                retained[record['receipt']['request']['params']['run']]=record
        runs=[r for r in runs if r not in retained]
    # Serve current-generation support before old hindcasts if a provider limit
    # intervenes. Calendar selection is fixed and does not inspect labels.
    runs=sorted(runs,reverse=True)
    registration = dict(artifact_version='wp17-multisite-acquisition-registration-v1', binding=b,
        run_initializations=runs, registered_at_utc=datetime.now(timezone.utc).isoformat(),
        max_logical_requests=len(runs), request_attempts=1, workers=b['workers'], seconds_budget=seconds,
        source_owner='public_mardorf_data_collector', scientific_release=False,
        min_request_interval_seconds=b['min_request_interval_seconds'],
        retained_parent_sha256=digest(Path(previous).read_bytes()) if previous else None,
        previous_attempts_retained=bool(previous))
    body = canonical(registration); registration_ref = backend.put_bytes(PREFIX+'/registrations/'+digest(body)+'.json',body)
    if backend.get_bytes(registration_ref) != body: raise ValueError('multisite_registration_readback')
    (out/'registration.json').write_bytes(body); started=time.monotonic()
    limiter=threading.Lock();last=[0.]
    def one(run):
        if time.monotonic()-started > seconds: return dict(run=run,status='NOT_ATTEMPTED_BUDGET')
        with limiter:
            delay=b['min_request_interval_seconds']-(time.monotonic()-last[0])
            if delay>0:time.sleep(delay)
            last[0]=time.monotonic()
        if time.monotonic()-started > seconds:return dict(run=run,status='NOT_ATTEMPTED_BUDGET')
        receipt, raw = capture(run,root=root)
        name=digest(raw)+'.original'; (out/name).write_bytes(raw)
        return dict(receipt=receipt, local_path=name, status=receipt['status'])
    records=list(retained.values())
    with ThreadPoolExecutor(max_workers=b['workers']) as pool:
        for record in pool.map(one,runs):
            records.append(record)
            completed=len(records)-len(retained)
            if completed%50==0: print(json.dumps(dict(multisite_completed=completed,total=len(runs),retained=len(retained))),flush=True)
    # Bulk archive shares one lossless container, avoiding thousands of tiny
    # remote transactions. No failed response is removed from this inventory.
    import io, zipfile
    buffer=io.BytesIO()
    with zipfile.ZipFile(buffer,'w',compression=zipfile.ZIP_DEFLATED) as archive:
        for name in sorted({r['local_path'] for r in records if 'local_path' in r}):
            info=zipfile.ZipInfo(name,date_time=(1980,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
            archive.writestr(info,(out/name).read_bytes())
    packed=buffer.getvalue(); pack=backend.put_bytes(PREFIX+'/original-packs/'+digest(packed)+'.zip',packed)
    if backend.get_bytes(pack)!=packed:raise ValueError('multisite_archive_readback')
    result=dict(artifact_version='wp17-multisite-history-v1',registration=registration_ref.json(),records=records,original_pack=pack.json(),
        complete=all(r['status']=='valid' for r in records), elapsed_seconds=time.monotonic()-started,
        raw_field_exclusions=[], scientific_status='NOT_READY',
        previous_history=json.loads(Path(previous).with_name('history-ref.json').read_bytes()) if previous else None)
    body=canonical(result); ref=backend.put_bytes(PREFIX+'/history/'+digest(body)+'.json',body)
    if backend.get_bytes(ref)!=body:raise ValueError('multisite_history_readback')
    (out/'history.json').write_bytes(body);(out/'history-ref.json').write_bytes(canonical(ref.json()))
    return result
