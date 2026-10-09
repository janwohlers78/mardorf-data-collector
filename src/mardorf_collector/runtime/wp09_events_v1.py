"""Original METAR collection; no detector coverage or negative labels invented."""
import argparse,csv,hashlib,io,json
from datetime import datetime,timezone
from pathlib import Path
import requests
from .wp08_labels_v1 import publication as publish_originals

ROOT=Path(__file__).resolve().parents[3]

def validate(raw,station,binding):
    rows=json.loads(raw)
    if not isinstance(rows,list) or len(rows)>binding['maximum_daily_records']:raise ValueError('WP09_record_budget')
    for r in rows:
        if not isinstance(r,dict) or r.get('icaoId')!=station or type(r.get('obsTime')) is not int or not isinstance(r.get('rawOb'),str):raise ValueError('WP09_original_identity')
        if not r['rawOb'] or r['obsTime']<0:raise ValueError('WP09_empty_original_report')
    return sorted({k for r in rows for k in r})

def request_original(url,*,params=None,get=requests.get,maximum_bytes=16777216):
    start=datetime.now(timezone.utc).isoformat();raw=b'';status=0;reason=None
    try:
        with get(url,params=params,timeout=(8,55),stream=True,allow_redirects=False) as r:
            status=r.status_code;r.raise_for_status()
            for b in r.iter_content(65536):
                if len(raw)+len(b)>maximum_bytes:raise ValueError('WP09_original_byte_budget')
                raw+=b
    except (requests.RequestException,ValueError) as exc:reason=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
    captured=datetime.now(timezone.utc).isoformat()
    return dict(url=url,params=params,started_utc=start,captured_utc=captured,available_utc=captured,http_status=status,
                reason=reason,source_bytes=len(raw),source_sha256=hashlib.sha256(raw).hexdigest()),raw

def capture(*,root=ROOT,get=requests.get):
    path=Path(root)/'config/wp09_events_v1.json';binding=json.loads(path.read_bytes());records=[];originals={}
    for station,url in binding['sources'].items():
        record,raw=request_original(url,get=get,maximum_bytes=binding['maximum_original_bytes']);columns=[]
        if record['reason'] is None:
            try:columns=validate(raw,station,binding)
            except (ValueError,TypeError,KeyError):record['reason']='WP09_invalid_original_response'
        record.update(quantity=station,station_id=station,status='valid' if record['reason'] is None else 'invalid',original_columns=columns)
        records.append(record);originals[station]=raw
    return dict(artifact_version='wp09-native-event-receipt-v1',captured_utc=datetime.now(timezone.utc).isoformat(),
        binding_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),records=records,provider_owner='public_collector',all_original_fields_retained=True),originals

def publication(receipt,originals,backend):
    return publish_originals(receipt,originals,backend,domain='wp09',collection='events')

def historical(output,*,root=ROOT,get=requests.get):
    """One explicit research invocation; no history download in a routine job."""
    output=Path(output);output.mkdir(parents=True,exist_ok=False);path=Path(root)/'config/wp09_events_v1.json';binding=json.loads(path.read_bytes());records=[]
    for station in binding['stations']:
        for year in (2024,2025,2026):
            last=(2026,10,9) if year==2026 else (year+1,1,1)
            params=dict(station=station,data='metar',year1=year,month1=1,day1=1,year2=last[0],month2=last[1],day2=last[2],tz='Etc/UTC',format='onlycomma',latlon='yes',missing='M',trace='T',direct='no',report_type=[3,4])
            record,raw=request_original(binding['historical_endpoint'],params=params,get=get,maximum_bytes=binding['maximum_original_bytes'])
            columns=[]
            if record['reason'] is None:
                try:
                    reader=csv.DictReader(io.StringIO(raw.decode()));columns=reader.fieldnames
                    if not {'station','valid','metar','lon','lat'}<=set(columns or []):raise ValueError('WP09_archive_columns')
                    rows=list(reader)
                    if not rows or any(r['station']!=station for r in rows):raise ValueError('WP09_archive_station')
                except (UnicodeError,ValueError):record['reason']='WP09_invalid_historical_original'
            name=f'{station}-{year}.csv';(output/name).write_bytes(raw);record.update(path=name,station_id=station,original_columns=columns,status='valid' if record['reason'] is None else 'invalid');records.append(record)
    manifest=dict(artifact_version='wp09-historical-originals-v1',binding_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),provider_owner='public_collector',records=records,all_original_fields_retained=True)
    (output/'manifest.json').write_text(json.dumps(manifest,sort_keys=True));return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--history-output',type=Path);p.add_argument('--capture-only',type=Path);a=p.parse_args()
    if a.history_output:print(json.dumps(historical(a.history_output)));raise SystemExit
    receipt,originals=capture()
    if not a.capture_only:raise ValueError('WP09_use_existing_shared_publisher')
    a.capture_only.mkdir(parents=True,exist_ok=False);(a.capture_only/'receipt.json').write_text(json.dumps(receipt,sort_keys=True))
    for name,raw in originals.items():(a.capture_only/(name+'.json')).write_bytes(raw)
    print(json.dumps(receipt))
