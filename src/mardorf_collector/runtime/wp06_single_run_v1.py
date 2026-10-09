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

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.runtime import load_runtime
from .cloud_environment import environment

ENDPOINT='https://single-runs-api.open-meteo.com/v1/forecast'
ALIASES=('icon_d2','icon_eu','gfs_global','ecmwf_ifs025')
COORDINATE=dict(latitude=52.47371,longitude=9.37979)
POINTER='config/cloud_refs/wp06_api_ingress_v1.json'
PREFIX='weather/archive/janwohlers78/mardorf-kitevorhersage/wp06-api/v1'


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


def capture(*,now=None,get=requests.get):
    now=now or datetime.now(timezone.utc);request=spec(now)
    began=datetime.now(timezone.utc).isoformat();body=b'';status=0;reason=None
    try:
        response=get(request['endpoint'],params=request['params'],timeout=(8,35))
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
        original_issue='unknown; retained explicit initialization request',availability_evidence='actual_source_receipt')
    return receipt,body


def publish(receipt,body,runtime,*,companion=None,label_companion=None):
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
    def merge(reader,changes):
        previous=reader.read('data/inbox/wp06_api/latest.json',required=False)
        if previous and json.loads(previous)['captured_utc']>=receipt['captured_utc']:
            return {path:raw,**{p:b for p,b in companion_paths.items() if not p.endswith('/latest.json')}}
        return changes
    def refs(snapshot):
        # CAS retry may discover a newer receipt; do not rewind its Git pointer.
        previous=runtime.read('data/inbox/wp06_api/latest.json',required=False)
        if previous and json.loads(previous)['captured_utc']>receipt['captured_utc']:return {}
        pointers={POINTER:pointer}
        if companion_pointer is not None:pointers['config/cloud_refs/wp07_api_ingress_v1.json']=companion_pointer
        if labels_pointer is not None:pointers['config/cloud_refs/wp08_labels_ingress_v1.json']=labels_pointer
        return pointers
    result=runtime.publish({path:raw,'data/inbox/wp06_api/latest.json':raw,**companion_paths},
        metadata=dict(channel='wp06-single-run-original-capture-v1',original_generated_at_utc=receipt['captured_utc'],
            producer_repository='janwohlers78/mardorf-data-collector',payload_sha256=receipt['source_sha256'],receipt_sha256=ref.sha256),
        merge=merge,extra_refs=refs)
    return dict(result,receipt=ref.json(),status=receipt['status'])


def latest_capture(*,now=None,get=requests.get):
    """A missing current 00 UTC run permits one explicitly aged prior 00 run.

    Never switch endpoint/product or invent a run. The failed original request
    is preserved; the consumer reports initialization age and actual lead.
    """
    now=now or datetime.now(timezone.utc)
    first,body=capture(now=now,get=get)
    if first['http_status']==400 and b'The requested model run is not available' in body:
        older,older_body=capture(now=now-timedelta(days=1),get=get)
        import base64
        older['unavailable_current_run']=dict(first,response_base64=base64.b64encode(body).decode())
        older['initialization_fallback']='previous_00_UTC_same_product; explicit age and actual lead required'
        return older,older_body
    return first,body


def main():
    p=argparse.ArgumentParser();p.add_argument('--capture-only',type=Path);p.add_argument('--gate',action='store_true');p.add_argument('--with-wp07',action='store_true');p.add_argument('--with-wp08-labels',action='store_true');a=p.parse_args()
    if a.gate:
        print('true' if datetime.now(ZoneInfo('Europe/Berlin')).hour==7 else 'false');return
    receipt,body=latest_capture()
    companion=None
    if a.with_wp07:
        from .wp07_single_run_v1 import capture as companion_capture
        companion=companion_capture(receipt['request']['params']['run'])
    label_companion=None
    if a.with_wp08_labels:
        from .wp08_labels_v1 import capture as label_capture
        label_companion=label_capture()
    if a.capture_only:
        a.capture_only.mkdir(parents=True,exist_ok=True)
        (a.capture_only/'receipt.json').write_bytes(canonical(receipt));(a.capture_only/'response.body').write_bytes(body)
        if companion:
            (a.capture_only/'wp07-receipt.json').write_bytes(canonical(companion[0]));(a.capture_only/'wp07-response.body').write_bytes(companion[1])
        if label_companion:
            (a.capture_only/'wp08-label-receipt.json').write_bytes(canonical(label_companion[0]))
            for name,raw in label_companion[1].items():(a.capture_only/('wp08-'+name+'.zip')).write_bytes(raw)
        print(json.dumps({k:receipt[k] for k in ('status','http_status','reason','source_bytes','source_sha256')}));return
    root=Path(__file__).resolve().parents[3]
    result=publish(receipt,body,load_runtime(root,environ=environment(root)),companion=companion,label_companion=label_companion)
    print(json.dumps(result))


if __name__=='__main__':main()
