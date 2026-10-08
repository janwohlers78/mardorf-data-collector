"""Bounded manual historical SVG acquisition, immutable B2 research objects only.

No Git weather writes, control-head update, forecast training or scheduled job.
All HTTP/empty/operator outcomes are retained, including inaccessible old days.
"""
import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path

from . import svg_history_v2 as history
from .fetch_svg_weatherlink import BASE, S, STATION_ID
from ..storage.runtime import load_runtime


ROOT = Path(__file__).resolve().parents[3]


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                       separators=(',', ':'))+'\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def days(start, end, probe_days=''):
    if probe_days:
        result = sorted(set(date.fromisoformat(x.strip()) for x in probe_days.split(',') if x.strip()))
        if not 1 <= len(result) <= 12:
            raise ValueError('Probe budget must be1–12 explicitly dated days')
    else:
        if not start < end or (end-start).days > 190:
            raise ValueError('Explicit historical range must be1–190days')
        result = [start+timedelta(days=i) for i in range((end-start).days)]
    if any(d < date(2000, 1, 1) or d >= datetime.now(timezone.utc).date() for d in result):
        raise ValueError('Completed historical UTC days from2000 only')
    return result


def capture(day, key, secret, *, session=S):
    began = datetime.now(timezone.utc).isoformat()
    start, end = history.request_bounds(day)
    spec = dict(station_id=STATION_ID, endpoint=f'{BASE}/historic/{STATION_ID}',
                start_timestamp=int(start.timestamp()), end_timestamp=int(end.timestamp()))
    record = dict(date_utc=day.isoformat(), request=spec, started_at_utc=began,
                  availability_role='retrospective_retrieval_NOT_original_publication')
    try:
        response = session.get(spec['endpoint'], params={'api-key':key,
            'start-timestamp':spec['start_timestamp'], 'end-timestamp':spec['end_timestamp']},
            headers={'X-Api-Secret':secret}, timeout=(10, 45))
    except Exception as exc:
        # Exception/URL/body text can contain credentials; fixed type only.
        record.update(status='transport_error', error_type=type(exc).__name__, http_status=None)
        return record, None, None
    raw = response.content
    record.update(http_status=response.status_code, source_response_sha256=sha(raw),
                  source_response_bytes=len(raw), retrieved_at_utc=datetime.now(timezone.utc).isoformat())
    if any(v and v.encode() in raw for v in (key, secret)):
        record.update(status='quarantined_sensitive_response', source_body_persisted=False)
        return record, None, None
    if response.status_code != 200:
        record['status']='provider_error'
        return record, gzip.compress(raw, mtime=0), None
    try:
        payload = json.loads(raw)
    except (ValueError, TypeError):
        record['status']='invalid_json'
        return record, gzip.compress(raw, mtime=0), None
    if payload.get('station_id') != STATION_ID:
        record.update(status='station_identity_mismatch', returned_station_id=payload.get('station_id'))
        return record, gzip.compress(raw, mtime=0), None
    source_sensors = payload.get('sensors', [])
    record['sensor_structures']=[{k:s.get(k) for k in ('sensor_type','data_structure_type','lsid')}
                                 for s in source_sensors]
    selected=[s for s in source_sensors if s.get('sensor_type')==48 and s.get('data_structure_type')==4]
    if len(selected) != 1:
        record['status']='unsupported_sensor_operator'
        return record, gzip.compress(raw, mtime=0), None
    records=selected[0].get('data', [])
    record['archive_interval_seconds']=dict(Counter(str(r.get('arch_int')) for r in records))
    record['native_field_inventory']=sorted({k for r in records for k in r})
    try:
        observations=history.normalize_historic(payload)
        coverage, observations=history.coverage(observations,day)
    except (ValueError,TypeError,KeyError,RuntimeError) as exc:
        record.update(status='normalization_error',error_type=type(exc).__name__)
        return record,gzip.compress(raw,mtime=0),None
    record.update(status='captured' if observations else 'empty',coverage=coverage,
                  five_minute_operator_verified=bool(records) and all(r.get('arch_int')==300 for r in records))
    normalized=canonical(dict(schema_version=1,station_id=STATION_ID,date_utc=day.isoformat(),
        source_response_sha256=sha(raw),retrieved_at_utc=record['retrieved_at_utc'],
        operator_verified=record['five_minute_operator_verified'],observations=observations))
    return record,gzip.compress(raw,mtime=0),normalized


def publish(backend, prefix, folder, name, raw):
    path=folder/name;path.write_bytes(raw)
    reference=backend.put_file(f'{prefix}/objects/{sha(raw)}'+Path(name).suffix,path)
    # Backend put_file already performs full readback; the separate explicit
    # readback also binds the receipt to these exact local bytes.
    restored=folder/('readback-'+name);backend.get_file(reference,restored)
    if sha(restored.read_bytes())!=sha(raw):
        raise RuntimeError('Historical source object readback mismatch')
    restored.unlink()
    return reference.json()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',type=date.fromisoformat,required=True)
    parser.add_argument('--end-exclusive',type=date.fromisoformat,required=True)
    parser.add_argument('--probe-days',default='')
    parser.add_argument('--output',type=Path,default=Path('work/svg-history-research'))
    args=parser.parse_args(argv)
    selected=days(args.start,args.end_exclusive,args.probe_days)
    args.output.mkdir(parents=True,exist_ok=True)
    key=os.getenv('WEATHERLINK_API_KEY');secret=os.getenv('WEATHERLINK_API_SECRET')
    if not key or not secret:
        raise RuntimeError('WeatherLink credential bindings unavailable')
    settings=json.loads((ROOT/'config/dev03_cloud_runtime_v1.json').read_bytes())
    env=dict(os.environ)
    for k,v in settings['b2_location'].items():
        if not env.get(k):env[k]=v
    cloud=load_runtime(ROOT,environ=env)
    prefix=settings['archive_prefix']+'/research/wp06-svg-history-v1'
    registration=dict(artifact_version='svg-history-research-registration-v1',
        registered_at_utc=datetime.now(timezone.utc).isoformat(),station_id=STATION_ID,
        days=[d.isoformat() for d in selected],max_days=190,probe_max_days=12,
        workflow_run_id=os.getenv('GITHUB_RUN_ID'),code_commit=os.getenv('GITHUB_SHA'),
        source_operator='retain native archive interval; only verified300s supports existing twelve-interval truth',
        scientific_release=False,production_head_updated=False,git_weather_bytes_written=0)
    registration_ref=publish(cloud.backend,prefix,args.output,'registration.json',canonical(registration))
    records=[]
    for day in selected:
        record,raw,normalized=capture(day,key,secret)
        if raw is not None:record['raw_object']=publish(cloud.backend,prefix,args.output,'source.json.gz',raw)
        if normalized is not None:record['normalized_object']=publish(cloud.backend,prefix,args.output,'normalized.json',normalized)
        records.append(record)
        index=dict(artifact_version='svg-history-research-index-v1',registration=registration_ref,
                   records=records,complete=len(records)==len(selected),scientific_release=False,
                   production_head_updated=False,git_weather_bytes_written=0)
        ref=publish(cloud.backend,prefix,args.output,'index.json',canonical(index))
        print(json.dumps(dict(date_utc=record['date_utc'],status=record['status'],http_status=record['http_status'],
            five_minute_operator_verified=record.get('five_minute_operator_verified'),
            timestamps=record.get('coverage',{}).get('unique_day_timestamps'),index=ref)),flush=True)
    result=dict(index=ref,status_counts=dict(Counter(r['status'] for r in records)),
                production_head_updated=False,git_weather_bytes_written=0)
    (args.output/'result.json').write_bytes(canonical(result))
    print(json.dumps(result),flush=True)
    return int(any(r['status'] not in ('captured','empty') for r in records))


if __name__=='__main__':
    raise SystemExit(main())
