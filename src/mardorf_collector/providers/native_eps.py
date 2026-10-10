"""Direct DWD EPS source successor. API member IDs/units/operators stay separate.

The registered three-hour source grid is acquired without interpolation. Each
complete original response, including all members and headers, remains retained.
This source is not an application of any frozen API-calibrated candidate.
"""
import bz2
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import tempfile
import math

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import LocalObjects, ObjectRef
from mardorf_collector.storage.parents import ParentStore
from mardorf_collector.wp13.model_originals_v1 import WORK, PREFIX
from mardorf_collector.wp13.native_grid_v1 import URLS, coordinates, MAX_BYTES
from mardorf_collector.wp13.native_point_projection_v1 import project_original, step_hours

VERSION='dwd-native-eps-source-v1'
# Every former API weather quantity has its native input; CIN/LPI are additions.
PARAMETERS=('u_10m','v_10m','vmax_10m','tot_prec','cape_ml','t_2m',
 'relhum_2m','td_2m','pmsl','ps','clct','aswdir_s','aswdifd_s','cin_ml','lpi')
MEMBERS=list(range(1,21))
WIND_NAMES={'u_10m':'10u','v_10m':'10v','vmax_10m':'max_i10fg'}


def utc(value):
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Native EPS UTC required')
    return stamp.astimezone(timezone.utc)


def first_header(body):
    import eccodes as ec
    decoder=bz2.BZ2Decompressor();decoded=decoder.decompress(body,max_length=64*1024**2+1)
    if len(decoded)>64*1024**2 or not decoder.eof or decoder.unused_data:raise ValueError('Native EPS decode bound')
    with tempfile.TemporaryFile() as stream:
        stream.write(decoded);stream.seek(0);handle=ec.codes_grib_new_from_file(stream)
        if handle is None:raise ValueError('Native EPS GRIB required')
        try:return {key:ec.codes_get(handle,key) for key in ('shortName','numberOfGridUsed','uuidOfHGrid','numberOfDataPoints')}
        finally:ec.codes_release(handle)


def grid_bytes(body, identity):
    # The entire compressed original is retained; the decoded coordinate prefix
    # is a separately hashed derivative. No partial response is called original.
    decoder=bz2.BZ2Decompressor();prefix=decoder.decompress(body,max_length=MAX_BYTES)
    coordinates(prefix,identity)
    return prefix


def fetch(leads, *, cycle=None, directory=WORK, workers=4):
    import mardorf_collector.providers.fetch_dwd_additional_models as dwd
    if type(workers)is not int or not 1<=workers<=4:raise ValueError('Bounded EPS workers required')
    if not leads or sorted(set(leads))!=leads or any(type(x)is not int or not 0<=x<=48 for x in leads):
        raise ValueError('Explicit native EPS lead grid required')
    cycle=cycle or dwd.discover_cycle('icon-d2-eps',max(leads))
    run=datetime.strptime(cycle,'%Y%m%d%H').replace(tzinfo=timezone.utc)
    urls={}
    for parameter in PARAMETERS:
        _,files=dwd.directory_hrefs('icon-d2-eps',cycle[-2:],parameter)
        for lead in leads:
            suffix=f'_{cycle}_{lead:03d}_2d_{parameter}.grib2.bz2'
            matches=[url for url in files if url.endswith(suffix)]
            if len(matches)!=1:raise ValueError(f'Native EPS field unavailable: {parameter} h{lead} cycle={cycle}')
            urls[(parameter,lead)]=matches[0]
    first_key=('u_10m',leads[0]);response=dwd.S.get(urls[first_key],timeout=90);response.raise_for_status()
    first=response.content;header=first_header(first)
    identity=dict(number_of_grid_used=header['numberOfGridUsed'],uuid_of_horizontal_grid=header['uuidOfHGrid'],number_of_data_points=header['numberOfDataPoints'])
    if identity['number_of_grid_used']!=47:raise ValueError('Registered EPS grid required')
    grid_response=dwd.S.get(URLS[47],timeout=90);grid_response.raise_for_status()
    if not 0<len(grid_response.content)<=512*1024**2:raise ValueError('Native grid original bound')
    directory=Path(directory);backend=LocalObjects(directory/'objects');parents=ParentStore(backend,prefix=PREFIX)
    with tempfile.NamedTemporaryFile() as stream:
        stream.write(grid_response.content);stream.flush()
        grid_ref=parents.write_file(stream.name,metadata={'artifact_version':'native-eps-grid-original-v1','url':URLS[47],
            'retrieved_at_utc':datetime.now(timezone.utc).isoformat()})
    prefix=grid_bytes(grid_response.content,identity)
    prefix_ref=backend.put_bytes('weather/model-native/wp15/v1/grid-prefixes/'+hashlib.sha256(prefix).hexdigest(),prefix)
    requested=dict(latitude=dwd.LAT,longitude=dwd.LON);geometry_cache={}
    def project(key):
        parameter,lead=key
        if key==first_key:body=first
        else:
            r=dwd.S.get(urls[key],timeout=90);r.raise_for_status();body=r.content
        h=first_header(body);name=h['shortName']
        if parameter in WIND_NAMES and name!=WIND_NAMES[parameter]:raise ValueError('Native EPS wind parameter contradiction')
        ref=dict(url=urls[key],sha256=hashlib.sha256(body).hexdigest(),bytes=len(body),retrieved_at_utc=datetime.now(timezone.utc).isoformat())
        # Calculate geometry once before parallel decoding; immutable lookup thereafter.
        try:
            proof=project_original(body,ref,prefix,prefix_ref.json(),requested,expected_run_utc=run.isoformat(),
                 expected_member_ids=MEMBERS,expected_parameter=name,geometry_cache=geometry_cache)
        except ValueError as exc:
            raise ValueError(f'Native EPS {parameter} h{lead}: {exc}') from exc
        return dict(parameter_native=parameter,lead_hours=lead,proof=proof)
    initial=project(first_key)
    rest=[key for key in urls if key!=first_key]
    # Bounded active tasks, original body and decoder buffers; no 255-body queue.
    results=[initial]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(0,len(rest),workers):
            results.extend(pool.map(project,rest[start:start+workers]))
    results.sort(key=lambda x:(x['lead_hours'],x['parameter_native']))
    source=dict(artifact_version=VERSION,provider_product='DWD ICON-D2-EPS native icosahedral GRIB2',
        run_time_utc=run.isoformat(),retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
        parameters=list(PARAMETERS),lead_hours=leads,native_member_ids=MEMBERS,fields=results,
        grid_original=grid_ref.json(),grid_prefix=prefix_ref.json(),requested_coordinate=requested,
        api_member_mapping='unverified_no_offset_or_control_assignment',time_support='native_GRIB_startStep_endStep_stepType',
        interpolation_performed=False,original_fields_excluded=0,frozen_candidate_applied=False,
        legacy_v15_compatible=False,scientific_status='NOT_READY',safety_release=False)
    source['geometry']=initial['proof']['geometry']
    rows=[]
    for lead in leads:
        selected={item['parameter_native']:item for item in results if item['lead_hours']==lead}
        values={p:{r['member_id_native']:r['value_native'] for r in item['proof']['records']} for p,item in selected.items()}
        members=[]
        for member in MEMBERS:
            value=dwd.derived(values['u_10m'][member],values['v_10m'][member],values['vmax_10m'][member]);value['member']=member;members.append(value)
        statistics=dwd.eps_statistics(members)
        central=dict(wind_speed_ms=statistics['wind_speed_ms_median'],wind_speed_kt=round(statistics['wind_speed_ms_median']*1.943844,2),
                     gust_ms=statistics['gust_ms_median'],gust_kt=round(statistics['gust_ms_median']*1.943844,2),ensemble_member_count=20)
        if statistics.get('wind_direction_deg_circular_mean')is not None:central['wind_direction_deg']=statistics['wind_direction_deg_circular_mean']
        rows.append(dict(model='ICON-D2-EPS',run_time_utc=run.isoformat(),forecast_lead_hours=lead,
            valid_time_utc=(run+timedelta(hours=lead)).isoformat(),source=source['provider_product'],
            source_urls=[item['proof']['original_ref']['url'] for item in selected.values()],native_source_version=VERSION,
            forecast_coordinate_or_grid_point=source['geometry']['actual_coordinate'],members=members,ensemble_statistics=statistics,
            derived=central,source_run_identity=dict(verification_status='verified_embedded_native_GRIB_v1',run_time_utc=run.isoformat(),
                expected_member_ids=MEMBERS,api_member_mapping='unverified',native_grid_uuid=identity['uuid_of_horizontal_grid'])))
    return rows,source


def audit_source(source, run, leads):
    failures=[]
    if not isinstance(source,dict) or source.get('artifact_version')!=VERSION:return [{'reason':'native_eps_source_missing'}],{}
    if (source.get('run_time_utc')!=run.isoformat() or source.get('lead_hours')!=sorted(leads)
        or source.get('parameters')!=list(PARAMETERS) or source.get('native_member_ids')!=MEMBERS
        or source.get('legacy_v15_compatible')is not False or source.get('frozen_candidate_applied')is not False):
        failures.append({'reason':'native_eps_source_scope_contradiction'})
    seen=set()
    for field in source.get('fields',[]):
        key=(field.get('parameter_native'),field.get('lead_hours'));proof=field.get('proof',{});records=proof.get('records',[])
        if key in seen:failures.append({'reason':'native_eps_duplicate_field'})
        seen.add(key)
        if sorted(r.get('member_id_native') for r in records)!=MEMBERS:failures.append({'reason':'native_eps_member_population'})
        if any(r.get('run_time_utc')!=run.isoformat() or step_hours(r.get('header',{}).get('endStep'),r.get('header',{}).get('stepUnits'))!=field.get('lead_hours') for r in records):
            failures.append({'reason':'native_eps_embedded_clock_contradiction'})
        if proof.get('geometry')!=source.get('geometry'):failures.append({'reason':'native_eps_geometry_contradiction'})
        original=proof.get('original_ref',{})
        if not original.get('url','').endswith(f"_{run:%Y%m%d%H}_{field.get('lead_hours'):03d}_2d_{field.get('parameter_native')}.grib2.bz2"):
            failures.append({'reason':'native_eps_original_field_binding'})
        if any(r.get('original_sha256')!=original.get('sha256') or not math.isfinite(r.get('value_native',float('nan')))
            or r.get('valid_time_utc')!=(run+timedelta(hours=field['lead_hours'])).isoformat() for r in records):
            failures.append({'reason':'native_eps_original_values_or_clock_contradiction'})
    if seen!={(p,h) for p in PARAMETERS for h in leads}:failures.append({'reason':'native_eps_field_lead_coverage'})
    return failures,dict(artifact_version=VERSION,source_product=source.get('provider_product'),native_member_ids=MEMBERS,
        parameters=source.get('parameters'),lead_hours=source.get('lead_hours'),legacy_v15_compatible=False,
        native_original_fields=sum(len(x.get('proof',{}).get('records',[])) for x in source.get('fields',[])))


def verify_source(source, backend, catalog):
    run=utc(source['run_time_utc']);failures,_=audit_source(source,run,source['lead_hours'])
    if failures:raise ValueError('Native EPS source scope invalid')
    original_by_sha={x['sha256']:x for x in catalog['parents']};prefix=backend.get_bytes(ObjectRef.parse(source['grid_prefix']))
    parents=ParentStore(backend,prefix=PREFIX);cache={}
    with tempfile.TemporaryDirectory() as folder:
        body=parents.restore(source['grid_original'],Path(folder)/'grid').read_bytes()
    if grid_bytes(body,dict(number_of_grid_used=47,uuid_of_horizontal_grid=source['geometry']['grid_uuid'],
                           number_of_data_points=source['fields'][0]['proof']['records'][0]['header']['numberOfDataPoints']))!=prefix:
        raise ValueError('Native grid original/prefix cold reproduction differs')
    for item in source['fields']:
        prior=item['proof'];original=original_by_sha.get(prior['original_ref']['sha256'])
        if original is None:raise ValueError('Native EPS original absent from verified catalog')
        with tempfile.TemporaryDirectory() as folder:
            body=parents.restore(original['parent'],Path(folder)/'raw').read_bytes()
        now=project_original(body,prior['original_ref'],prefix,source['grid_prefix'],source['requested_coordinate'],
             expected_run_utc=source['run_time_utc'],expected_member_ids=MEMBERS,
             expected_parameter=prior['records'][0]['header']['shortName'],geometry_cache=cache)
        if now!=prior:raise ValueError('Native EPS cold point reproduction differs')
    return dict(status='PASS',original_fields=len(source['fields'])*20,point_values_reproduced=True,
                full_original_SHA_verified=True,native_member_ids=MEMBERS,scientific_status='NOT_READY')
