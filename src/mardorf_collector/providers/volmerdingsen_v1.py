"""Public-only Volmerdingsen acquisition: exact, bounded, immutable originals.

Snapshots retain all advertised chart groups and NOAA archives. Chart summaries
are never called raw sensor exports. Routine capture shares the secondary job.
"""
import argparse
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import re
import time
import zipfile

import requests

from .svg_historical_research_v1 import canonical, sha, publish
from ..storage.configuration import b2_settings, resolve_b2_bucket
from ..storage.objects import B2Objects

BASE = 'https://wetter.roesnet.ddnss.de/'
SITE = 'volmerdingsen-roesnet-v1'
PREFIX = 'weather/archive/janwohlers78/mardorf-kitevorhersage/stations/volmerdingsen/v1'
ROOT = Path(__file__).resolve().parents[3]
RECENT = ('homepage', 'day', 'yesterday', 'week', 'Thunderstorm', 'Station')
MAX_BYTES = 32 * 1024**2


def fetch(path, session, *, now=None):
    if not re.fullmatch(r'[A-Za-z0-9_./-]+', path) or '..' in path:
        raise ValueError('Invalid station source path')
    record = dict(path=path, url=BASE+path, started_at_utc=(now or datetime.now(timezone.utc)).isoformat())
    try:
        with session.get(BASE+path, stream=True, allow_redirects=False,
                         timeout=(5, 15), headers={'Accept-Encoding': 'identity'}) as response:
            record['http_status'] = response.status_code
            body = bytearray()
            for block in response.iter_content(65536):
                if len(body)+len(block)>MAX_BYTES:
                    return dict(record, status='OVERSIZE'), None
                body.extend(block)
            raw = bytes(body)
    except requests.RequestException as exc:
        return dict(record, status='TRANSPORT_ERROR', error_type=type(exc).__name__), None
    record.update(captured_at_utc=datetime.now(timezone.utc).isoformat(),
                  sha256=sha(raw), bytes=len(raw),
                  status='CAPTURED' if response.status_code==200 else 'HTTP_ERROR')
    if record['status']=='CAPTURED':
        try:
            if path.endswith('.json') and not isinstance(json.loads(raw), dict):
                raise ValueError('chart must be an object')
            if path.startswith('NOAA/') and b'CLIMATOLOGICAL SUMMARY' not in raw:
                raise ValueError('NOAA identity missing')
        except (ValueError, TypeError):
            record['status']='INVALID_FORMAT'
    return record, raw


def archive_paths(script, now):
    match=re.search(rb'var graphgroups_raw\s*=\s*(\{[^\n]+\});', script)
    if not match:
        raise ValueError('Advertised station chart catalogue missing')
    groups=json.loads(match[1])
    if not isinstance(groups,dict) or len(groups)>24 or any(not re.fullmatch('[A-Za-z0-9_]+', k) for k in groups):
        raise ValueError('Station catalogue budget/identity')
    paths=['about/', 'reports/', 'robots.txt'] + ['json/'+k+'.json' for k in groups]
    for year in range(2021, now.year+1):
        paths.append(f'NOAA/NOAA-{year}.txt')
        for month in range(1, (now.month if year==now.year else 12)+1):
            paths.append(f'NOAA/NOAA-{year}-{month:02d}.txt')
    if len(paths)>160:
        raise ValueError('Historical station request budget')
    return paths


def run(mode, output, backend, *, session=None, now=None):
    if mode not in ('archive', 'recent'):
        raise ValueError('Explicit station capture mode required')
    now=now or datetime.now(timezone.utc)
    output=Path(output); output.mkdir(parents=True, exist_ok=True)
    session=session or requests.Session()
    files=[]; records=[]
    if mode=='archive':
        record, script=fetch('js/belchertown.js', session)
        records.append(record)
        if record['status']!='CAPTURED':
            raise ValueError('Cannot discover advertised station archive')
        files.append((record['path'],script)); paths=archive_paths(script,now)
    else:
        paths=['json/'+k+'.json' for k in RECENT]
        paths.append(now.strftime('NOAA/NOAA-%Y-%m.txt'))
    registration=dict(artifact_version='volmerdingsen-capture-registration-v1', site_id=SITE,
        mode=mode, requested_paths=paths, registered_at_utc=now.isoformat(),
        max_bytes_per_source=MAX_BYTES, max_requests=161, max_attempts=1,
        code_commit=os.getenv('GITHUB_SHA'),producer_code_sha256=sha(Path(__file__).read_bytes()), event_id=os.getenv('GITHUB_RUN_ID','local_authorized_capture'),
        provider_fetch_owner='public_collector', historical_hourly_export_verified=False,
        all_advertised_fields_retained=True, scientific_release=False)
    registration_ref=publish(backend,PREFIX,output,'registration.json',canonical(registration))
    for path in paths:
        record,raw=fetch(path,session); records.append(record)
        if raw is not None:
            files.append((path if not path.endswith('/') else path+'index.html',raw))
            record['member_path']=files[-1][0]
        time.sleep(.12)
    if mode=='archive':
        records[0]['member_path']='js/belchertown.js'
    members={name:dict(sha256=sha(raw),bytes=len(raw)) for name,raw in files}
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for name,raw in files:
            info=zipfile.ZipInfo(name); info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,raw)
        z.writestr('member-manifest.json',canonical(members))
    pack_ref=publish(backend,PREFIX,output,'originals.zip',stream.getvalue())
    complete=all(r['status']=='CAPTURED' for r in records)
    manifest=dict(artifact_version='volmerdingsen-originals-v1',site_id=SITE,mode=mode,
        registration=registration_ref,records=records,originals=pack_ref,members=members,
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        status='COMPLETE' if complete else 'PARTIAL', field_exclusions=0,
        historical_hourly_export_verified=False, scientific_status='NOT_READY')
    ref=publish(backend,PREFIX,output,'manifest.json',canonical(manifest))
    control=dict(schema_version=1,artifact_version='volmerdingsen-observation-control-v1',
        kind='volmerdingsen_observations',generated_at_utc=manifest['generated_at_utc'],snapshot=ref,
        readback_verified=True,scientific_status='NOT_READY',
        metadata=dict(event_id=registration['event_id'],payload_sha256=ref['sha256'],site_id=SITE,
                      mode=mode,source_requests=len(records),status=manifest['status']))
    (output/'control.json').write_bytes(canonical(control))
    print('VOLMERDINGSEN_CAPTURE_BEGIN',flush=True)
    print(json.dumps(control),flush=True)
    print('VOLMERDINGSEN_CAPTURE_END',flush=True)
    return control


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--mode',choices=['archive','recent'],default='recent')
    parser.add_argument('--output',type=Path,default=Path('work/volmerdingsen'))
    parser.add_argument('--publish-control',action='store_true')
    args=parser.parse_args()
    env=dict(os.environ)
    for k,v in json.loads((ROOT/'config/dev03_cloud_runtime_v1.json').read_bytes())['b2_location'].items():
        if not env.get(k):env[k]=v
    control=run(args.mode,args.output,B2Objects(resolve_b2_bucket(b2_settings(env))))
    if control['metadata']['status']!='COMPLETE':
        raise SystemExit('Station originals PARTIAL; no successful control advancement')
    if args.publish_control:
        from ..transfer.push_private import atomic_commit, hdr
        path='config/cloud_refs/volmerdingsen_observations_v1.json'
        atomic_commit('janwohlers78/mardorf-kitevorhersage',
            [dict(path=path,content=canonical(control),immutable=False,
                  monotonic_guard=dict(path=path,incoming_time=control['generated_at_utc'],field='generated_at_utc'))],
            'Retain verified Volmerdingsen station originals',hdr(os.environ['PRIVATE_REPO_TOKEN']))


if __name__=='__main__':main()
