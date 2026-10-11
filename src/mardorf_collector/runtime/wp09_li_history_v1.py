"""Resumable native LI research archive; no forecast publication or routine cron.

Every whole product is retained before its immutable index. Disjoint ranges
can run concurrently; boundary products converge to the first verified index.
HTTP catalog success never admits negative labels. Secrets never enter output.
"""
import hashlib
import json
import os
import time
import threading
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


class ProviderHTTPFailure(ValueError):
    def __init__(self,status):
        self.status=status
        super().__init__('LI_history_provider_HTTP_failure_'+str(status))


def days(start,end,now=None):
    a=datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    b=datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    now=now or datetime.now(timezone.utc)
    if (len(start)!=10 or len(end)!=10 or not a<b<=now.replace(hour=0,minute=0,second=0,microsecond=0)
            or a<datetime(2024,7,4,tzinfo=timezone.utc) or (b-a).days>16):
        raise ValueError('LI_history_completed_UTC_days_maximum_16_from_20240704')
    return [a+timedelta(days=k) for k in range((b-a).days)]


def lookup(backend,key):
    page=backend.list_page(key+'/',limit=10)
    if page['cursor']:raise ValueError('LI_history_conflicting_index_budget')
    values=[]
    for name in page['keys']:
        meta=backend.head(name);values.append(json.loads(backend.get_bytes(ObjectRef(name,meta['sha256'],meta['bytes']))))
    if not values:return None
    def identity(v):
        if 'records' in v:
            return [(r['product_id'],r['source_sha256'],r['native_start_utc'],r['native_end_utc'],r['original']) for r in v['records']]
        return (v.get('product_id'),v.get('native_start_utc'),v.get('native_end_utc'),v.get('original'))
    if any(identity(v)!=identity(values[0]) for v in values):
        raise ValueError('LI_history_conflicting_original_versions')
    return min(values,key=lambda v:v.get('captured_utc') or min((r['captured_utc'] for r in v.get('records',[])),default=''))


def save(backend,value):
    raw=value if isinstance(value,bytes) else canonical(value)
    ref=backend.put_bytes(PREFIX+'/objects/'+hashlib.sha256(raw).hexdigest(),raw)
    if backend.get_bytes(ref)!=raw:raise ValueError('LI_history_readback')
    return ref.json()


def response_attempt(url,headers,deadline,*,params=None,maximum=32*1024**2,backend=None,client=None):
    p=urlsplit(url)
    if p.scheme!='https' or p.netloc!='api.eumetsat.int' or p.query:raise ValueError('LI_history_untrusted_url')
    if time.monotonic()>=deadline:raise ValueError('LI_history_time_budget')
    started=datetime.now(timezone.utc).isoformat();body=bytearray()
    status=None;complete=False
    try:
        with (client or requests).get(url,headers=headers,params=params,stream=True,allow_redirects=False,timeout=(5,45)) as r:
            status=r.status_code
            for chunk in r.iter_content(65536):
                if len(body)+len(chunk)>maximum or time.monotonic()>deadline:raise ValueError('LI_history_response_budget')
                body.extend(chunk)
            complete=True;raw=bytes(body)
            if r.status_code!=200:raise ProviderHTTPFailure(r.status_code)
    except Exception:
        if backend is not None:
            save(backend,dict(status='FAILED_OR_PARTIAL_RESPONSE',http_status=status,
                complete_response=complete,url=url,params=params,started_utc=started,
                captured_utc=datetime.now(timezone.utc).isoformat(),original_prefix=save(backend,bytes(body))))
        raise
    return raw,started,datetime.now(timezone.utc).isoformat()


def response(url,headers,deadline,*,params=None,maximum=32*1024**2,backend=None,client=None):
    # A failed historical stream is retained, then retried within the original
    # deadline. Identity/integrity/budget failures never trigger blind retries.
    for attempt in range(3):
        try:
            return response_attempt(url,headers,deadline,params=params,maximum=maximum,backend=backend,client=client)
        except (requests.Timeout,requests.ConnectionError):
            if attempt==2 or time.monotonic()>=deadline:raise
        except ProviderHTTPFailure as exc:
            if exc.status not in (429,500,502,503,504) or attempt==2 or time.monotonic()+2>=deadline:raise
            time.sleep(2)
    raise AssertionError('unreachable')


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
        ref=backend.put_bytes(key+'/'+hashlib.sha256(encoded).hexdigest()+'.json',encoded)
        if backend.get_bytes(ref)!=encoded:raise ValueError('LI_history_index_readback')
    except ObjectError:
        other=lookup(backend,key)
        if other is None or other['original']!=value['original'] or other['product_id']!=value['product_id']:raise
        value=other
    return value


def packed_products(features,day,backend,headers,deadline):
    """Whole unchanged ZIPs in <=32MiB verified packs, 16-product checkpoints.

    Avoid hundreds of separate object/index round trips per day. Each ZIP keeps
    its own digest, byte interval and actual first capture; there is no native
    field extraction or lossy spatial subset on this public transport route.
    """
    records=[];local=threading.local();clients=[]
    def initialize():
        local.client=requests.Session();clients.append(local.client)
    try:
        with ThreadPoolExecutor(max_workers=4,initializer=initialize) as executor:
            for offset in range(0,len(features),16):
                group=features[offset:offset+16]
                signature=hashlib.sha256(canonical([f['id'] for f in group])).hexdigest()
                key=PREFIX+'/pieces/'+day.strftime('%Y-%m-%d')+'/'+signature
                old=lookup(backend,key)
                if old is not None:
                    if [r['product_id'] for r in old['records']]!=[f['id'] for f in group]:raise ValueError('LI_history_piece_identity')
                    for ref in old['packs']:backend.get_bytes(ObjectRef.parse(ref))
                    records.extend(old['records']);continue
                # Partial receipts live outside the complete-piece prefix. Otherwise
                # lookup() would mistake intentionally growing subsets for conflicting
                # complete versions. Keep every successful original across failures.
                partial_key=PREFIX+'/partialpieces/'+day.strftime('%Y-%m-%d')+'/'+signature
                page=backend.list_page(partial_key+'/',limit=10)
                if page['cursor']:raise ValueError('LI_history_partial_receipt_budget')
                retained={};expected={f['id']:f['properties']['date'].split('/') for f in group}
                checked_packs={}
                for name in page['keys']:
                    meta=backend.head(name)
                    partial=json.loads(backend.get_bytes(ObjectRef(name,meta['sha256'],meta['bytes'])))
                    for record in partial['records']:
                        ident=record['product_id']
                        if ident not in expected or [record['native_start_utc'],record['native_end_utc']]!=expected[ident]:
                            raise ValueError('LI_history_partial_identity')
                        original=record['original'];ref=ObjectRef.parse(original['pack'])
                        if ref.sha256 not in checked_packs:
                            checked_packs[ref.sha256]=backend.get_bytes(ref)
                        packed=checked_packs[ref.sha256];start=original['offset'];size=original['bytes']
                        if type(start) is not int or start<0 or start+size>len(packed):raise ValueError('LI_history_partial_interval')
                        raw=packed[start:start+size]
                        if hashlib.sha256(raw).hexdigest()!=original['sha256'] or original['sha256']!=record['source_sha256'] or size!=record['source_bytes']:
                            raise ValueError('LI_history_partial_original_integrity')
                        if ident in retained:
                            previous=retained[ident]
                            if previous['source_sha256']!=record['source_sha256']:raise ValueError('LI_history_conflicting_original_versions')
                            if record['captured_utc']>=previous['captured_utc']:continue
                        retained[ident]=record
                def fetch(f):
                    prop=f['properties'];a,b=prop['date'].split('/')
                    if (prop['parentIdentifier']!=COLLECTION or prop['productInformation']['productType']!='MTILI2LFL'
                            or not any(x.get('platform',{}).get('platformShortName')=='MTI1' for x in prop['acquisitionInformation'])):
                        raise ValueError('LI_history_foreign_identity')
                    links=[x['href'] for x in prop['links']['data'] if x['mediaType']=='application/zip']
                    if len(links)!=1:raise ValueError('LI_history_download_identity')
                    raw,started,captured=response(links[0],headers,deadline,backend=backend,client=local.client)
                    if not raw.startswith(b'PK'):raise ValueError('LI_history_native_ZIP_required')
                    return raw,dict(quantity='LFL_'+hashlib.sha256(f['id'].encode()).hexdigest(),product_id=f['id'],
                        native_start_utc=a,native_end_utc=b,status='valid',source_sha256=hashlib.sha256(raw).hexdigest(),
                        source_bytes=len(raw),started_utc=started,captured_utc=captured,available_utc=captured,
                        url=links[0],all_original_fields_retained=True)
                captured=[];failures=[]
                submitted=[(f,executor.submit(fetch,f)) for f in group if f['id'] not in retained]
                for feature,future in submitted:
                    try:captured.append(future.result())
                    except Exception as exc:failures.append((feature['id'],exc))
                buffer=bytearray();pending=[];packs=[];items=[]
                def flush():
                    if not pending:return
                    ref=save(backend,bytes(buffer));packs.append(ref)
                    for record,start in pending:
                        record['original']=dict(kind='native_zip_in_verified_pack_v1',pack=ref,offset=start,
                            bytes=record['source_bytes'],sha256=record['source_sha256'])
                        items.append(record)
                    buffer.clear();pending.clear()
                for raw,record in captured:
                    if len(buffer)+len(raw)>32*1024**2:flush()
                    pending.append((record,len(buffer)));buffer.extend(raw)
                flush()
                retained.update({r['product_id']:r for r in items})
                items=[retained[f['id']] for f in group if f['id'] in retained]
                packs=list({r['original']['pack']['sha256']:r['original']['pack'] for r in items}.values())
                if failures:
                    value=dict(artifact_version='wp09-li-history-partial-piece-v3',records=items,packs=packs,
                        failures=[dict(product_id=ident,exception_type=type(exc).__name__) for ident,exc in failures])
                    encoded=canonical(value)
                    ref=backend.put_bytes(partial_key+'/'+hashlib.sha256(encoded).hexdigest()+'.json',encoded)
                    if backend.get_bytes(ref)!=encoded:raise ValueError('LI_history_partial_piece_readback')
                    raise failures[0][1]
                value=dict(artifact_version='wp09-li-history-piece-v2',records=items,packs=packs)
                encoded=canonical(value);ref=backend.put_bytes(key+'/'+hashlib.sha256(encoded).hexdigest()+'.json',encoded)
                if backend.get_bytes(ref)!=encoded:raise ValueError('LI_history_piece_readback')
                records.extend(items)
    finally:
        for client in clients:client.close()
    return records


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
    records=packed_products(sorted(features,key=lambda f:f['id']),day,backend,headers,deadline)
    if sum(x['source_bytes'] for x in records)>5*1024**3:raise ValueError('LI_history_day_byte_budget')
    value=dict(artifact_version='wp09-li-history-day-v2',day_utc=day.strftime('%Y-%m-%d'),
        requested_start_utc=day.isoformat(),requested_end_utc=end.isoformat(),boundary_guard_seconds=1200,records=records,
        catalog=dict(original=catalog_ref,started_utc=started,captured_utc=captured,params=params),
        status='ORIGINALS_COMPLETE_QC_PENDING',detector_coverage=False,optimizer_calls=0,
        all_original_fields_retained=True,provider_product_count=total)
    encoded=canonical(value)
    ref=backend.put_bytes(key+'/'+hashlib.sha256(encoded).hexdigest()+'.json',encoded)
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
            failure=str(exc) if isinstance(exc,(ObjectError,ValueError)) and (str(exc).startswith('LI_history_') or isinstance(exc,ObjectError)) else type(exc).__name__;break
    result=dict(artifact_version='wp09-li-history-result-v1',registration=registration_ref,days=references,
        requested_days=len(requested),completed_days=len(references),status='COMPLETE' if failure is None else 'OPEN',
        failure_type=failure,detector_coverage=False,scientific_status='NOT_READY',forecast_publication=False)
    ref=save(backend,result)
    print('WP09_LI_HISTORY_BEGIN');print(json.dumps(dict(status=result['status'],result=ref,completed_days=len(references),
        requested_days=len(requested),failure_type=failure),sort_keys=True));print('WP09_LI_HISTORY_END',flush=True)
    return result
