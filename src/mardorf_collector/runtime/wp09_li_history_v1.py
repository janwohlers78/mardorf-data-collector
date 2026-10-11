"""Resumable native LI research archive; no forecast publication or routine cron.

Every whole product is retained before its immutable index. Disjoint ranges
can run concurrently; boundary products converge to the first verified index.
HTTP catalog success never admits negative labels. Secrets never enter output.
"""
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests
from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import ObjectRef, ObjectError
from .wp09_eumetsat_lfl_v2 import ROOT

PREFIX='weather/archive/janwohlers78/mardorf-kitevorhersage/wp09-li-history/v1'
COLLECTION='EO:EUM:DAT:0691'
SEARCH='https://api.eumetsat.int/data/search-products/1.0.0/os'


def days(start,end,now=None):
    a=datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    b=datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    now=now or datetime.now(timezone.utc)
    if (len(start)!=10 or len(end)!=10 or not a<b<=now.replace(hour=0,minute=0,second=0,microsecond=0)
            or a<datetime(2024,7,4,tzinfo=timezone.utc) or (b-a).days>16):
        raise ValueError('LI_history_completed_UTC_days_maximum_16_from_20240704')
    return [a+timedelta(days=k) for k in range((b-a).days)]


def lookup(backend,key):
    meta=backend.head(key)
    return None if meta is None else json.loads(backend.get_bytes(ObjectRef(key,meta['sha256'],meta['bytes'])))


def save(backend,value):
    raw=value if isinstance(value,bytes) else canonical(value)
    ref=backend.put_bytes(PREFIX+'/objects/'+hashlib.sha256(raw).hexdigest(),raw)
    if backend.get_bytes(ref)!=raw:raise ValueError('LI_history_readback')
    return ref.json()


def response(url,headers,deadline,*,params=None,maximum=32*1024**2,backend=None):
    p=urlsplit(url)
    if p.scheme!='https' or p.netloc!='api.eumetsat.int' or p.query:raise ValueError('LI_history_untrusted_url')
    if time.monotonic()>=deadline:raise ValueError('LI_history_time_budget')
    started=datetime.now(timezone.utc).isoformat();body=bytearray()
    status=None;complete=False
    try:
        with requests.get(url,headers=headers,params=params,stream=True,allow_redirects=False,timeout=(5,20)) as r:
            status=r.status_code
            for chunk in r.iter_content(65536):
                if len(body)+len(chunk)>maximum or time.monotonic()>deadline:raise ValueError('LI_history_response_budget')
                body.extend(chunk)
            complete=True;raw=bytes(body)
            if r.status_code!=200:raise ValueError('LI_history_provider_HTTP_failure')
    except Exception:
        if backend is not None:
            save(backend,dict(status='FAILED_OR_PARTIAL_RESPONSE',http_status=status,
                complete_response=complete,url=url,params=params,started_utc=started,
                captured_utc=datetime.now(timezone.utc).isoformat(),original_prefix=save(backend,bytes(body))))
        raise
    return raw,started,datetime.now(timezone.utc).isoformat()


def product(f,backend,headers,deadline):
    prop=f['properties'];a,b=prop['date'].split('/')
    if (prop['parentIdentifier']!=COLLECTION or prop['productInformation']['productType']!='MTILI2LFL'
        or not any(x.get('platform',{}).get('platformShortName')=='MTI1' for x in prop['acquisitionInformation'])):
        raise ValueError('LI_history_foreign_identity')
    key=PREFIX+'/products/'+hashlib.sha256(f['id'].encode()).hexdigest()+'.json'
    old=lookup(backend,key)
    if old is not None:
        if (old['product_id'],old['native_start_utc'],old['native_end_utc'])!=(f['id'],a,b):raise ValueError('LI_history_resume_identity')
        backend.get_bytes(ObjectRef.parse(old['original']))
        return old
    links=[x['href'] for x in prop['links']['data'] if x['mediaType']=='application/zip']
    if len(links)!=1:raise ValueError('LI_history_download_identity')
    raw,started,captured=response(links[0],headers,deadline,backend=backend)
    if not raw.startswith(b'PK'):raise ValueError('LI_history_native_ZIP_required')
    value=dict(quantity='LFL_'+hashlib.sha256(f['id'].encode()).hexdigest(),product_id=f['id'],
        native_start_utc=a,native_end_utc=b,status='valid',source_sha256=hashlib.sha256(raw).hexdigest(),
        source_bytes=len(raw),started_utc=started,captured_utc=captured,available_utc=captured,
        original=save(backend,raw),url=links[0],all_original_fields_retained=True)
    encoded=canonical(value)
    try:
        ref=backend.put_bytes(key,encoded)
        if backend.get_bytes(ref)!=encoded:raise ValueError('LI_history_index_readback')
    except ObjectError:
        other=lookup(backend,key)
        if other is None or other['original']!=value['original'] or other['product_id']!=value['product_id']:raise
        value=other
    return value


def capture_day(day,backend,headers,deadline):
    key=PREFIX+'/days/'+day.strftime('%Y-%m-%d')+'.json'
    previous=lookup(backend,key)
    if previous is not None:
        if previous['status']!='ORIGINALS_COMPLETE_QC_PENDING':raise ValueError('LI_history_bad_day_index')
        # The consumer checks every original; this shortcut is acquisition resume.
        return previous
    end=day+timedelta(days=1)
    params=dict(pi=COLLECTION,format='json',c=200,dtstart=(day-timedelta(minutes=20)).isoformat(),dtend=(end+timedelta(minutes=20)).isoformat())
    raw,started,captured=response(SEARCH,headers,deadline,params=params,maximum=4*1024**2,backend=backend)
    catalog=json.loads(raw);features=catalog['features'];total=int(catalog['totalResults'])
    catalog_ref=save(backend,raw)
    if total!=len(features) or total>200 or len({x['id'] for x in features})!=len(features):
        raise ValueError('LI_history_incomplete_or_duplicate_catalog_page')
    # Reject catalogues whose worst-case bounded downloads exceed the day cap.
    # This bound is checked before any product download.
    if total*32*1024**2>5*1024**3:raise ValueError('LI_history_day_byte_budget')
    with ThreadPoolExecutor(max_workers=4) as executor:
        records=list(executor.map(lambda f:product(f,backend,headers,deadline),sorted(features,key=lambda f:f['id'])))
    if sum(x['source_bytes'] for x in records)>5*1024**3:raise ValueError('LI_history_day_byte_budget')
    value=dict(artifact_version='wp09-li-history-day-v1',day_utc=day.strftime('%Y-%m-%d'),
        requested_start_utc=day.isoformat(),requested_end_utc=end.isoformat(),boundary_guard_seconds=1200,records=records,
        catalog=dict(original=catalog_ref,started_utc=started,captured_utc=captured,params=params),
        status='ORIGINALS_COMPLETE_QC_PENDING',detector_coverage=False,optimizer_calls=0,
        all_original_fields_retained=True,provider_product_count=total)
    ref=backend.put_bytes(key,canonical(value))
    if backend.get_bytes(ref)!=canonical(value):raise ValueError('LI_history_day_readback')
    return value


def run(start,end,backend,*,environ=None,maximum_seconds=1450):
    env=os.environ if environ is None else environ;requested=days(start,end)
    registration=dict(artifact_version='wp09-li-history-registration-v1',start=start,end_exclusive=end,
        acquisition_code_sha256=hashlib.sha256(__import__('pathlib').Path(__file__).read_bytes()).hexdigest(),
        event_id=env.get('GITHUB_RUN_ID'),registered_at_utc=datetime.now(timezone.utc).isoformat(),
        maximum_seconds=maximum_seconds,workers=4,forecast_publication=False,scientific_status='NOT_READY')
    registration_ref=save(backend,registration);deadline=time.monotonic()+maximum_seconds
    response_token=requests.post('https://api.eumetsat.int/token',data={'grant_type':'client_credentials'},
        auth=(env.get('EUMETSAT_CONSUMER_KEY'),env.get('EUMETSAT_CONSUMER_SECRET')),timeout=(5,20),allow_redirects=False)
    if response_token.status_code!=200:raise ValueError('LI_history_authentication_failed')
    token=response_token.json()['access_token'];headers={'Authorization':'Bearer '+token}
    if not isinstance(token,str) or not token:raise ValueError('LI_history_invalid_token')
    references=[];failure=None
    for day in requested:
        try:
            value=capture_day(day,backend,headers,deadline)
            references.append(dict(day=value['day_utc'],receipt=save(backend,value)))
            print(json.dumps(dict(completed_days=len(references),requested_days=len(requested))),flush=True)
        except Exception as exc:
            failure=str(exc) if isinstance(exc,ValueError) and str(exc).startswith('LI_history_') else type(exc).__name__;break
    result=dict(artifact_version='wp09-li-history-result-v1',registration=registration_ref,days=references,
        requested_days=len(requested),completed_days=len(references),status='COMPLETE' if failure is None else 'OPEN',
        failure_type=failure,detector_coverage=False,scientific_status='NOT_READY',forecast_publication=False)
    ref=save(backend,result)
    print('WP09_LI_HISTORY_BEGIN');print(json.dumps(dict(status=result['status'],result=ref,completed_days=len(references),
        requested_days=len(requested),failure_type=failure),sort_keys=True));print('WP09_LI_HISTORY_END',flush=True)
    return result
