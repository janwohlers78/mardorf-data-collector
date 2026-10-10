"""One daily, original API capture for the frozen WP06 research product.

Provider acquisition belongs to the public collector. Original bytes and receipt
live in B2; Git receives only an immutable reference, never weather values.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo
import requests
import time

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.runtime import load_runtime
from .cloud_environment import environment

ENDPOINT='https://single-runs-api.open-meteo.com/v1/forecast'
ALIASES=('icon_d2','icon_eu','gfs_global','ecmwf_ifs025')
COORDINATE=dict(latitude=52.47371,longitude=9.37979)
POINTER='config/cloud_refs/wp06_api_ingress_v1.json'
PREFIX='weather/archive/janwohlers78/mardorf-kitevorhersage/wp06-api/v1'


def forecast_content_digest(body):
    # Keep the exact original intact. Only provider execution timing is ignored
    # for duplicate dispatch; every weather field, coordinate and unit is bound.
    try:
        document=json.loads(body)
        if not isinstance(document,dict):return hashlib.sha256(body).hexdigest()
        document.pop('generationtime_ms',None)
        return hashlib.sha256(canonical(document)).hexdigest()
    except (ValueError,TypeError):return hashlib.sha256(body).hexdigest()


def spec(now):
    now=now.astimezone(timezone.utc)
    return dict(endpoint=ENDPOINT,params=dict(COORDINATE,hourly='wind_speed_10m,wind_gusts_10m',
        wind_speed_unit='ms',timezone='GMT',run=now.strftime('%Y-%m-%dT00:00'),
        forecast_hours=121,models=','.join(ALIASES)))


def validate(body,request):
    x=json.loads(body)
    if x.get('utc_offset_seconds')!=0 or not all(isinstance(x.get(k),(int,float)) for k in COORDINATE):
        raise ValueError('API_coordinate_or_timezone')
    times=x['hourly']['time'];run=datetime.fromisoformat(request['params']['run']).replace(tzinfo=timezone.utc)
    if len(times)!=121:
        raise ValueError('API_expected_121_hourly_positions')
    for i,t in enumerate(times):
        actual=datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        if (actual-run).total_seconds()!=i*3600:
            raise ValueError('API_run_or_hourly_support')
    for alias in ALIASES:
        for field in ('wind_speed_10m','wind_gusts_10m'):
            key=field+'_'+alias
            if x['hourly_units'].get(key)!='m/s' or len(x['hourly'][key])!=121:
                raise ValueError('API_field_unit_or_shape')
            for v in x['hourly'][key]:
                if v is not None and (type(v) not in (int,float) or not 0<=v<1e4):
                    raise ValueError('API_invalid_wind_value')
    return x


def capture(*,now=None,get=requests.get,run=None,timeout=(8,35)):
    now=now or datetime.now(timezone.utc);request=spec(now)
    if run is not None:request['params']['run']=run
    began=datetime.now(timezone.utc).isoformat();body=b'';status=0;reason=None
    try:
        response=get(request['endpoint'],params=request['params'],timeout=timeout)
        body=response.content;status=response.status_code
        if len(body)>1024**2:raise ValueError('API_response_budget')
        if status!=200:raise ValueError('API_HTTP_'+str(status))
        validate(body,request)
    except (requests.RequestException,ValueError,KeyError,TypeError) as exc:
        # No credential/URL/response dump in the operational log.
        reason=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
    captured=datetime.now(timezone.utc).isoformat()
    receipt=dict(artifact_version='wp06-single-run-receipt-v1',request=request,started_utc=began,
        captured_utc=captured,available_utc=captured,status='valid' if reason is None else 'invalid',
        http_status=status,reason=reason,source_sha256=hashlib.sha256(body).hexdigest(),source_bytes=len(body),
        forecast_content_sha256=forecast_content_digest(body),forecast_comparison_ignored_metadata=['generationtime_ms'],
        original_issue='unknown; retained explicit initialization request',availability_evidence='actual_source_receipt')
    return receipt,body


def publish(receipt,body,runtime,*,companion=None,label_companion=None,event_companion=None,force_daily=False):
    backend=runtime.backend
    source=backend.put_bytes(PREFIX+'/originals/'+receipt['source_sha256'],body)
    if backend.get_bytes(source)!=body:raise ValueError('API_original_readback')
    value=dict(receipt,original=source.json());raw=canonical(value)
    ref=backend.put_bytes(PREFIX+'/receipts/'+hashlib.sha256(raw).hexdigest()+'.json',raw)
    if backend.get_bytes(ref)!=raw:raise ValueError('API_receipt_readback')
    pointer=dict(schema_version=1,artifact_version='wp06-api-ingress-control-v1',kind='wp06_api',
        generated_at_utc=receipt['captured_utc'],snapshot=ref.json(),receipt_sha256=ref.sha256,
        readback_verified=True,bundle_ready=receipt['status']=='valid')
    path='data/inbox/wp06_api/'+receipt['captured_utc'].replace(':','')+'.json'
    companion_paths={}; companion_pointer=None
    if companion is not None:
        r,b=companion;prefix=PREFIX.replace('wp06-api','wp07-api')
        original=backend.put_bytes(prefix+'/originals/'+r['source_sha256'],b)
        if backend.get_bytes(original)!=b:raise ValueError('WP07_original_readback')
        companion_raw=canonical(dict(r,original=original.json()))
        companion_ref=backend.put_bytes(prefix+'/receipts/'+hashlib.sha256(companion_raw).hexdigest()+'.json',companion_raw)
        if backend.get_bytes(companion_ref)!=companion_raw:raise ValueError('WP07_receipt_readback')
        companion_pointer=dict(schema_version=1,artifact_version='wp07-api-ingress-control-v1',kind='wp07_api',
            generated_at_utc=r['captured_utc'],snapshot=companion_ref.json(),readback_verified=True,
            bundle_ready=r['status']=='valid',receipt_sha256=companion_ref.sha256)
        companion_paths={'data/inbox/wp07_api/'+r['captured_utc'].replace(':','')+'.json':companion_raw,
                         'data/inbox/wp07_api/latest.json':companion_raw}
    labels_pointer=None
    if label_companion is not None:
        from .wp08_labels_v1 import publication
        paths,labels_pointer=publication(*label_companion,backend)
        companion_paths.update(paths)
    event_pointer=None
    if event_companion is not None:
        from .wp09_events_v1 import publication as event_publication
        paths,event_pointer=event_publication(*event_companion,backend)
        companion_paths.update(paths)
    profiles=[('data/inbox/wp06_api/latest.json',path,raw,pointer,POINTER)]
    if companion is not None:
        stamp=companion[0]['captured_utc'].replace(':','')
        profiles.append(('data/inbox/wp07_api/latest.json','data/inbox/wp07_api/'+stamp+'.json',
            companion_paths['data/inbox/wp07_api/'+stamp+'.json'],companion_pointer,
            'config/cloud_refs/wp07_api_ingress_v1.json'))
    def selected(reader,latest,new_raw):
        old=reader.read(latest,required=False)
        return promote_receipt(json.loads(old) if old else None,json.loads(new_raw),force_daily=force_daily)
    def merge(reader,changes):
        result=dict(changes)
        for latest,_,new_raw,_,_ in profiles:
            if not selected(reader,latest,new_raw):result.pop(latest,None)
        return result
    def refs(snapshot):
        pointers={control:ptr for latest,_,new_raw,ptr,control in profiles if selected(runtime,latest,new_raw)}
        # A small independently verified probe clock limits retries even when
        # all originals are unchanged or a provider run is still unavailable.
        pointers['config/cloud_refs/wp06_current_source_check_v1.json']=dict(pointer,
            artifact_version='wp06-source-check-control-v1',kind='wp06_source_check')
        if labels_pointer is not None:pointers['config/cloud_refs/wp08_labels_ingress_v1.json']=labels_pointer
        if event_pointer is not None:pointers['config/cloud_refs/wp09_events_ingress_v1.json']=event_pointer
        return pointers
    result=runtime.publish({path:raw,'data/inbox/wp06_api/latest.json':raw,**companion_paths},
        metadata=dict(channel='wp06-single-run-original-capture-v1',original_generated_at_utc=receipt['captured_utc'],
            producer_repository='janwohlers78/mardorf-data-collector',payload_sha256=receipt['source_sha256'],receipt_sha256=ref.sha256),
        merge=merge,extra_refs=refs)
    return dict(result,receipt=ref.json(),status=receipt['status'])


def promote_receipt(previous,incoming,*,force_daily=False):
    """Source initialization outranks recapture time; failures cannot hide usable data."""
    if incoming.get('status')!='valid':return False
    if previous is None or previous.get('status')!='valid':return True
    previous_run=previous.get('request',{}).get('params',{}).get('run','')
    incoming_run=incoming.get('request',{}).get('params',{}).get('run','')
    if incoming_run<previous_run:return False
    if incoming_run==previous_run and incoming.get('forecast_content_sha256',incoming.get('source_sha256'))==previous.get('forecast_content_sha256',previous.get('source_sha256')):
        elapsed=(datetime.fromisoformat(incoming['captured_utc'].replace('Z','+00:00'))-datetime.fromisoformat(previous['captured_utc'].replace('Z','+00:00'))).total_seconds()
        return elapsed>0 and (force_daily or elapsed>=10*3600)
    return incoming_run>previous_run or incoming['captured_utc']>previous['captured_utc']


def cycle_candidates(now):
    now=now.astimezone(timezone.utc)
    start=now.replace(hour=(now.hour//6)*6,minute=0,second=0,microsecond=0)
    return [(start-timedelta(hours=6*i)).strftime('%Y-%m-%dT%H:%M') for i in range(5)]


def latest_profile_capture(capture_run,*,now):
    """At most five explicit runs of the same product; no latest/blended endpoint."""
    import base64
    failures=[];began=time.monotonic()
    for run in cycle_candidates(now):
        if failures and time.monotonic()-began>=70:
            receipt['selection_blocker']='newer_run_probe_time_budget'
            break
        receipt,body=capture_run(run)
        if receipt['status']=='valid' or not (receipt['http_status']==400 and b'The requested model run is not available' in body):
            if failures:receipt['unavailable_newer_runs']=failures
            return receipt,body
        failures.append(dict(receipt,response_base64=base64.b64encode(body).decode()))
    receipt['unavailable_newer_runs']=failures[:-1]
    return receipt,body


def latest_capture(*,now=None,get=requests.get,current_cycles=False):
    now=now or datetime.now(timezone.utc)
    if current_cycles:
        return latest_profile_capture(lambda run:capture(now=now,get=get,run=run,timeout=(5,15)),now=now)
    # Preserve the registered daily 00 UTC cohort and its one-day fallback.
    first,body=capture(now=now,get=get)
    if first['http_status']==400 and b'The requested model run is not available' in body:
        older,older_body=capture(now=now-timedelta(days=1),get=get)
        import base64
        older['unavailable_current_run']=dict(first,response_base64=base64.b64encode(body).decode())
        older['initialization_fallback']='previous_00_UTC_same_product; explicit age and actual lead required'
        return older,older_body
    return first,body


def main():
    p=argparse.ArgumentParser();p.add_argument('--registered-daily-00',action='store_true');p.add_argument('--capture-only',type=Path);p.add_argument('--gate',action='store_true');p.add_argument('--with-wp07',action='store_true');p.add_argument('--with-wp08-labels',action='store_true');p.add_argument('--with-wp09-events',action='store_true');a=p.parse_args()
    if a.gate:
        print('true' if datetime.now(ZoneInfo('Europe/Berlin')).hour==7 else 'false');return
    now=datetime.now(timezone.utc)
    receipt,body=latest_capture(now=now,current_cycles=not a.registered_daily_00)
    companion=None
    if a.with_wp07:
        from .wp07_single_run_v1 import capture as companion_capture
        companion=(companion_capture(receipt['request']['params']['run']) if a.registered_daily_00 else
            latest_profile_capture(lambda run:companion_capture(run,timeout=(5,15)),now=now))
    label_companion=None
    if a.with_wp08_labels:
        from .wp08_labels_v1 import capture as label_capture
        label_companion=label_capture()
    event_companion=None
    if a.with_wp09_events:
        from .wp09_events_v1 import capture as event_capture
        event_companion=event_capture()
    if a.capture_only:
        a.capture_only.mkdir(parents=True,exist_ok=True)
        (a.capture_only/'receipt.json').write_bytes(canonical(receipt));(a.capture_only/'response.body').write_bytes(body)
        if companion:
            (a.capture_only/'wp07-receipt.json').write_bytes(canonical(companion[0]));(a.capture_only/'wp07-response.body').write_bytes(companion[1])
        if label_companion:
            (a.capture_only/'wp08-label-receipt.json').write_bytes(canonical(label_companion[0]))
            for name,raw in label_companion[1].items():(a.capture_only/('wp08-'+name+'.zip')).write_bytes(raw)
        if event_companion:
            (a.capture_only/'wp09-event-receipt.json').write_bytes(canonical(event_companion[0]))
            for name,raw in event_companion[1].items():(a.capture_only/('wp09-'+name+'.json')).write_bytes(raw)
        print(json.dumps({k:receipt[k] for k in ('status','http_status','reason','source_bytes','source_sha256')}));return
    root=Path(__file__).resolve().parents[3]
    result=publish(receipt,body,load_runtime(root,environ=environment(root)),companion=companion,label_companion=label_companion,event_companion=event_companion,force_daily=a.registered_daily_00)
    print(json.dumps(result))


if __name__=='__main__':main()
