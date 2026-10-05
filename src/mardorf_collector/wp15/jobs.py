"""Shared current and bounded historical station jobs; telemetry never gates."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import time

from ..wp13 import regional_stations_v1 as old
from ..wp13.core_v1 import canonical, digest, strict_json, stamp, utc
from . import regional_v2 as dwd
from .assets import ROOT
from .prepared import CONTRACT, PREFIX, policy, processor_identity, preprocess, read_manifest, read_records
from .queue import READINESS, enqueue, ADMISSION


def readiness(cloud, *, root=ROOT):
    body = cloud.read(READINESS, required=False)
    if body is None:
        return None
    value = strict_json(body)
    if (value.get('artifact_version') != 'wp15-public-station-consumer-ready-v1'
            or value.get('available') is not True
            or value.get('processor_sha256') != processor_identity(root)
            or value.get('contract_sha256') != hashlib.sha256((Path(root)/CONTRACT).read_bytes()).hexdigest()):
        return None
    return value


def route_enabled(cloud, provider, *, station=None, root=ROOT):
    ready = readiness(cloud, root=root)
    if ready is None:
        return False
    if provider == 'svg':
        return ready.get('svg_enabled') is True
    return station in ready.get('dwd_station_ids', [])


def publish_result(cloud, result, *, root=ROOT, cursor=None, prior_cursor=None,
                   archive_originals=None, marker=None):
    report = preprocess(cloud.backend, result, root=root, archive_originals=archive_originals)
    changes = {old.STATE:canonical(cursor)} if cursor is not None else {}
    if marker is not None:
        name, proof = marker
        changes[name] = canonical(dict(proof, publication=report))
    expected = {old.STATE:prior_cursor} if cursor is not None else None
    enqueue(cloud, report, root=root, changes=changes, expected_state=expected)
    return report


def current(cloud, *, root=ROOT):
    station_policy, contracts = dwd.configuration(root)
    if not station_policy['enabled']:
        return {'status':'DISABLED'}
    state_body = cloud.read(old.STATE, required=False)
    state = strict_json(state_body) if state_body else {'stations':{}, 'last_recent_utc':None}
    state = deepcopy(state)
    now = datetime.now(timezone.utc)
    start, end = stamp(now-timedelta(hours=station_policy['current_window_hours'])), stamp(now)
    recent = not state['last_recent_utc'] or (now-utc(state['last_recent_utc'])).total_seconds() >= station_policy['recent_every_hours']*3600
    report = {'status':'PASS', 'stations':{}, 'deliveries':[], 'provider_requests':0,
        'measurement_gate':'NONBLOCKING_ROUTINE', 'scientific_gate':'OPEN'}
    commit = os.environ.get('GITHUB_SHA') or subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    for station in station_policy['stations']:
        native_ready = cloud.read('data/weather_native/consumer_readiness_dwd_cdc_v1.json', required=False)
        if not native_ready or strict_json(native_ready).get('configuration_sha256') != contracts.configuration['sha256']:
            raise ValueError('Existing DWD receiver readiness required')
        checkpoint = deepcopy(state)
        try:
            known = state['stations'].setdefault(station['id'], {})
            results = []
            report['stations'][station['id']] = {'products':{}}
            for hourly, products in [(False,station_policy['current_products']), (True,station_policy['hourly_products'] if recent else {})]:
                for product in products:
                    raw, seen = old.fetch(old.source_url(station_policy,station['id'],product,hourly=hourly),station_policy)
                    report['provider_requests'] += 1
                    result, updates = dwd.prepare(raw,seen,station['id'],product,start=start,end=end,hourly=hourly,
                        contracts=contracts,policy=station_policy,known=known,commit=commit)
                    if result:
                        results.append(result)
                    known.update(updates)
                    report['stations'][station['id']]['products'][('hourly/' if hourly else '10_minutes/')+product] = {'new_fields':len(result['fields']) if result else 0, 'rows':len(updates)}
            state['stations'][station['id']] = {k:v for k,v in known.items() if k.rsplit('/',1)[-1] >= (now-timedelta(hours=72)).strftime('%Y%m%d%H')}
            combined = old.combine(results)
            if combined:
                if route_enabled(cloud, 'dwd_cdc', station=station['id'], root=root):
                    proof = publish_result(cloud,combined,root=root,cursor=state,prior_cursor=state_body)
                    proof['route'] = 'public_preprocessed'
                else:
                    proof = old.publish_delivery(cloud,combined,cursor=state,prior_cursor=state_body)
                    proof['route'] = 'legacy_native'
                report['deliveries'].append(proof)
                state_body = canonical(state)
        except Exception as exc:
            state = checkpoint
            report['status'] = 'PARTIAL_RETRY_REQUIRED'
            report['stations'][station['id']]['failure'] = type(exc).__name__
    if recent and report['status'] == 'PASS':
        state['last_recent_utc'] = stamp(now)
    def merge(current, changes):
        if current.read(old.STATE,required=False) != state_body:
            raise ValueError('Concurrent routine cursor changed; retry')
        return {name:body for name,body in changes.items() if current.read(name,required=False)!=body}
    cloud.publish({old.STATE:canonical(state)},metadata={'channel':'wp15-regional-cursor'},merge=merge)
    return report


def backfill(cloud, *, stations, products, year, root=ROOT, max_units=1):
    station_policy, contracts = dwd.configuration(root)
    if (type(year) is not int or not 2016 <= year <= 2025
            or not stations or not set(stations) <= {s['id'] for s in station_policy['stations']}
            or not products or not set(products) <= set(station_policy['hourly_products'])
            or len(stations)*len(products)>policy(root)['max_work_units']
            or not 1<=max_units<=policy(root)['max_work_units']):
        raise ValueError('Bounded registered station/product/year work request required')
    if any(not route_enabled(cloud,'dwd_cdc',station=s,root=root) for s in stations):
        raise ValueError('Exact deployed station preprocessing readiness required')
    manifest = strict_json(cloud.read(old.ARCHIVE))
    started = time.monotonic()
    report = {'status':'PASS','provider_requests':0,'original_reads':0,'work_units':[],
        'publications':[],'scientific_gate':'OPEN','measurement_gate':'NONBLOCKING_ROUTINE'}
    commit = os.environ.get('GITHUB_SHA') or subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    for station in stations:
        for product in products:
            if len(report['work_units']) >= max_units or time.monotonic()-started > policy(root)['max_processing_seconds']:
                return dict(report,status='RESUMABLE_BUDGET_STOP')
            original = next(x for x in manifest['originals'] if x['station_id']==station and x['product']==product)
            raw = None
            unit = {'station':station,'product':product,'year':year,'months':[]}
            for month in range(1,13):
                cursor = f'data/weather_native/station_preprocessed_v1/backfill/{station}/{product}/{year}/{month:02d}.json'
                saved = cloud.read(cursor,required=False)
                if saved is not None:
                    checkpoint = strict_json(saved)
                    expected = {'station':station,'product':product,'year':year,'month':month,
                        'original_sha256':original['sha256'],'processor_sha256':processor_identity(root)}
                    if any(checkpoint.get(k) != v for k,v in expected.items()):
                        raise ValueError('Historical checkpoint processor/source mismatch')
                    continue
                if time.monotonic()-started > policy(root)['max_processing_seconds']:
                    return dict(report,status='RESUMABLE_BUDGET_STOP')
                if raw is None:
                    raw = cloud.read(original['path'])
                    report['original_reads'] += 1
                    if len(raw)!=original['bytes'] or hashlib.sha256(raw).hexdigest()!=original['sha256']:
                        raise ValueError('Historical archive identity mismatch')
                    dwd.cache_window(raw,station,station_policy,f'{year}-01-01T00:00:00Z',f'{year+1}-01-01T00:00:00Z')
                begin = datetime(year,month,1,tzinfo=timezone.utc)
                finish = datetime(year+1,1,1,tzinfo=timezone.utc) if month==12 else datetime(year,month+1,1,tzinfo=timezone.utc)
                result,_ = dwd.prepare(raw,original['captured_at_utc'],station,product,start=stamp(begin),end=stamp(finish),
                    hourly=True,historical=True,contracts=contracts,policy=station_policy,commit=commit)
                basis = {'station':station,'product':product,'year':year,'month':month,
                    'original_sha256':original['sha256'],'processor_sha256':processor_identity(root)}
                if result:
                    proof = publish_result(cloud,result,root=root,archive_originals={original['sha256']:original['path']},marker=(cursor,basis))
                    report['publications'].append(proof)
                else:
                    body=canonical(dict(basis,status='VERIFIED_EMPTY'))
                    cloud.publish({cursor:body},metadata={'channel':'wp15-empty-month'},merge=lambda current,changes:old.immutable_merge(current,changes))
                unit['months'].append(month)
            report['work_units'].append(unit)
    return report


def reconcile(cloud, *, root=ROOT, limit=20):
    """Recover staged manifests after crash, with a bounded cyclic list cursor."""
    if not 1<=limit<=100:
        raise ValueError('Bounded orphan reconciliation required')
    path='data/weather_native/station_preprocessed_v1/reconcile_cursor.json'
    prior=cloud.read(path,required=False)
    cursor=strict_json(prior)['cursor'] if prior else None
    listed=cloud.backend.list_page(PREFIX+'/manifests/',limit=limit,cursor=cursor)
    recovered=0
    for key in listed['keys']:
        if cloud.backend.head(ADMISSION+key.rsplit('/',1)[-1]) is not None:
            continue
        meta=cloud.backend.head(key)
        from ..storage.objects import ObjectRef
        reference=ObjectRef(key,meta['sha256'],meta['bytes']).json()
        staged=strict_json(cloud.backend.get_bytes(ObjectRef.parse(reference)))
        if staged.get('processor_sha256') != processor_identity(root):
            continue
        doc=read_manifest(cloud.backend,reference,root=root)
        # Deployment version changes do not admit arbitrary staged predecessors.
        station=next((s['id'] for s in dwd.configuration(root)[0]['stations'] if s['station_id']==doc['scope']['station_id']),None)
        provider='dwd_cdc' if station else 'svg'
        if not route_enabled(cloud,provider,station=station,root=root):
            continue
        enqueue(cloud,{'publication_id':doc['publication_id'],'manifest':reference},root=root)
        recovered+=1
    body=canonical({'schema_version':1,'cursor':listed['cursor']})
    def merge(current,changes):
        if current.read(path,required=False)!=prior:
            return {}
        return changes
    cloud.publish({path:body},metadata={'channel':'wp15-station-reconcile'},merge=merge)
    return {'status':'PASS','scanned':len(listed['keys']),'recovered':recovered,'bounded':True}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation',choices=('current','archive-capture','backfill','verify'))
    parser.add_argument('--stations',default='04745')
    parser.add_argument('--products',default='wind')
    parser.add_argument('--year',type=int,default=2016)
    parser.add_argument('--max-units',type=int,default=1)
    args=parser.parse_args()
    from ..storage.runtime import load_runtime
    from ..runtime.cloud_environment import environment
    cloud=load_runtime(Path.cwd(),environ=environment(Path.cwd()))
    started=time.monotonic();before=dict(cloud.backend.metrics)
    if args.operation=='current':
        report=current(cloud)
    elif args.operation=='archive-capture':
        report=old.operate(cloud,archive=True)
    elif args.operation=='backfill':
        report=backfill(cloud,stations=args.stations.split(','),products=args.products.split(','),year=args.year,max_units=args.max_units)
    else:
        ready=readiness(cloud)
        report={'status':'PASS' if ready else 'NOT_READY','processor_ready':ready is not None,'scientific_gate':'OPEN'}
    if args.operation in ('current','verify'):
        try:
            report['orphan_reconciliation']=reconcile(cloud)
        except Exception as exc:
            report['orphan_reconciliation']={'status':'RETRY_REQUIRED','failure':type(exc).__name__}
    report['metrics']={'elapsed_seconds':time.monotonic()-started,'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
        'transport':{k:v-before.get(k,0) for k,v in cloud.backend.metrics.items() if type(v) in (int,float)},
        'blocking':False,'mode':'routine_operation_telemetry'}
    Path('work').mkdir(exist_ok=True)
    Path('work/wp15_station_preprocessing.json').write_bytes(canonical(report))
    print('WP15_STATION_PREPROCESSING='+json.dumps(report),flush=True)


if __name__=='__main__':
    main()
