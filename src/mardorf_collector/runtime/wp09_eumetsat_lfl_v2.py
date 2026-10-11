"""Bounded, explicit offline acquisition of complete native LI flash products.

Uses the official OAuth/download routes. Credentials and access tokens never
enter receipts, URLs, exceptions or logs. No native quality/coverage is inferred
from an HTTP 200 catalog or an empty/black map. No scheduled owner is added.
"""
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
import requests
from .wp09_lightning_context_v1 import ROOT


def capture(start_utc, end_utc, *, root=ROOT, environ=None, session=None):
    env=os.environ if environ is None else environ
    path=Path(root)/'config/wp09_eumetsat_lfl_v2.json'; policy=json.loads(path.read_bytes())
    start=datetime.fromisoformat(start_utc.replace('Z','+00:00'));end=datetime.fromisoformat(end_utc.replace('Z','+00:00'))
    now=datetime.now(timezone.utc)
    if start.tzinfo is None or end.tzinfo is None or not start<end<=now or (end-start).total_seconds()>86400:
        raise ValueError('LFL_explicit_past_interval_maximum_one_day')
    result=dict(artifact_version='wp09-eumetsat-lfl-original-receipt-v2',
        policy_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),provider_owner='public_collector',
        requested_start_utc=start.isoformat(),requested_end_utc=end.isoformat(),
        captured_utc=now.isoformat(),records=[],status='BLOCKED',blocker=None,
        detector_coverage=False, all_original_fields_retained=True, optimizer_calls=0)
    key=env.get('EUMETSAT_CONSUMER_KEY');secret=env.get('EUMETSAT_CONSUMER_SECRET')
    if not key or not secret:
        result['blocker']='EUMETSAT_credentials_not_configured';return result,{}
    client=session or requests.Session();originals={};deadline=time.monotonic()+policy['maximum_seconds'];total=0
    try:
        response=client.post(policy['token_url'],data={'grant_type':'client_credentials'},
            auth=(key,secret),timeout=(5,15),allow_redirects=False)
        response.raise_for_status();token=response.json()['access_token']
        if not isinstance(token,str) or not token:raise ValueError('invalid_token')
    except (requests.RequestException,ValueError,KeyError,TypeError):
        result['blocker']='EUMETSAT_authentication_failed';return result,originals
    headers={'Authorization':'Bearer '+token}

    def get(quantity,url,maximum,params=None):
        nonlocal total
        parts=urlsplit(url)
        if parts.scheme!='https' or parts.netloc!='api.eumetsat.int' or parts.query:
            raise ValueError('LFL_untrusted_or_parameterized_link')
        raw=b'';status=0;reason=None;started=datetime.now(timezone.utc).isoformat()
        try:
            if time.monotonic()>deadline:raise ValueError('LFL_time_budget')
            with client.get(url,params=params,headers=headers,stream=True,timeout=(5,15),allow_redirects=False) as r:
                status=r.status_code
                for chunk in r.iter_content(65536):
                    if time.monotonic()>deadline:raise ValueError('LFL_time_budget')
                    if len(raw)+len(chunk)>maximum or total+len(raw)+len(chunk)>policy['maximum_total_bytes']:
                        raise ValueError('LFL_byte_budget')
                    raw+=chunk
                r.raise_for_status()
                if status!=200:raise ValueError('LFL_HTTP_not_200')
        except requests.RequestException:reason='LFL_request_failed'
        except ValueError as e:reason=str(e)
        captured=datetime.now(timezone.utc).isoformat();total+=len(raw);originals[quantity]=raw
        record=dict(quantity=quantity,url=url,request_params=params,started_utc=started,captured_utc=captured,
            available_utc=captured,http_status=status,status='valid' if reason is None else 'invalid',reason=reason,
            source_sha256=hashlib.sha256(raw).hexdigest(),source_bytes=len(raw))
        result['records'].append(record);return record,raw

    try:
        params={'pi':policy['collection'],'format':'json','c':policy['maximum_products'],
                'dtstart':start.isoformat(),'dtend':end.isoformat()}
        record,raw=get('catalog',policy['search_url'],4194304,params)
        if record['status']!='valid':raise ValueError('LFL_catalog_download_failed')
        catalog=json.loads(raw);products=catalog['features']
        if len(products)>policy['maximum_products']:raise ValueError('LFL_product_count_budget')
        for f in products:
            properties=f['properties'];a,b=properties['date'].split('/')
            a=datetime.fromisoformat(a.replace('Z','+00:00'));b=datetime.fromisoformat(b.replace('Z','+00:00'))
            if properties['parentIdentifier']!=policy['collection'] or properties['productInformation']['productType']!=policy['product_type']:
                raise ValueError('LFL_foreign_product')
            if not any(x.get('platform',{}).get('platformShortName')==policy['platform'] for x in properties['acquisitionInformation']):
                raise ValueError('LFL_foreign_platform')
            if b<=start or a>=end:continue
            links=[x for x in properties['links']['data'] if x['mediaType']=='application/zip']
            if len(links)!=1:raise ValueError('LFL_ambiguous_native_download')
            quantity='LFL_'+hashlib.sha256(f['id'].encode()).hexdigest()
            product,body=get(quantity,links[0]['href'],policy['maximum_product_bytes'])
            product.update(product_id=f['id'],native_start_utc=a.isoformat(),native_end_utc=b.isoformat())
            # Download success alone is no parse/QC/negative-coverage qualification.
            if product['status']=='valid' and not body.startswith(b'PK'):
                product.update(status='invalid',reason='LFL_native_ZIP_signature_missing')
        valid=[r for r in result['records'] if r['quantity'].startswith('LFL_') and r['status']=='valid']
        result.update(status='ORIGINALS_CAPTURED_QC_OPEN' if valid else 'BLOCKED',
            blocker='native_QC_spatial_and_parallax_qualification_OPEN' if valid else 'no_valid_native_products',
            catalog_total_results=catalog.get('totalResults'),captured_product_count=len(valid),
            bytes=total,elapsed_seconds=policy['maximum_seconds']-(deadline-time.monotonic()))
    except (ValueError,KeyError,TypeError):result['blocker']='LFL_catalog_or_native_identity_failure'
    result['captured_utc']=datetime.now(timezone.utc).isoformat();return result,originals


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--start',required=True);p.add_argument('--end',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if a.output.exists() or ROOT.resolve() in a.output.resolve().parents:raise ValueError('fresh_output_outside_Git_required')
    receipt,originals=capture(a.start,a.end);a.output.mkdir(parents=True)
    for name,raw in originals.items():(a.output/(name+'.original')).write_bytes(raw)
    (a.output/'receipt.json').write_text(json.dumps(receipt,sort_keys=True,separators=(',',':')))
    print(json.dumps({k:receipt.get(k) for k in ('status','blocker','captured_product_count','detector_coverage')}))
