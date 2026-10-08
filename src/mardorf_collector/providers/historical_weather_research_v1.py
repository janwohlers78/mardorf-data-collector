"""Bounded public CDC refresh / GFS archive access proof; immutable research only."""
import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
import xml.etree.ElementTree as ET

import requests

from .svg_historical_research_v1 import canonical, sha, publish, publish_pack
from ..storage.configuration import b2_settings, resolve_b2_bucket
from ..storage.objects import B2Objects
from ..wp13.regional_stations_v1 import configuration, source_url

ROOT=Path(__file__).resolve().parents[3]
GFS='https://tds.gdex.ucar.edu/thredds/ncss/grid/files/g/d084001/'
PROBE_DATES=('2016-10-08','2018-01-15','2021-05-01','2023-01-15','2024-03-15','2026-04-15')


def plan(operation, root=ROOT):
    if operation=='dwd-recent':
        policy,_=configuration(root)
        return [dict(url=source_url(policy,s['id'],p,hourly=True),params=None,
                     site=s['id'],product=p,limit_bytes=policy['max_original_bytes'],suffix='.zip')
                for s in policy['stations'] for p in policy['hourly_products']]
    if operation!='gfs-archive-probe':raise ValueError('Unregistered research operation')
    tasks=[]
    for value in PROBE_DATES:
        day=date.fromisoformat(value);stem=day.strftime('%Y/%Y%m%d/gfs.0p25.%Y%m%d00.f030.grib2')
        url=GFS+stem;valid=(datetime.combine(day,datetime.min.time(),tzinfo=timezone.utc)+timedelta(hours=30)).isoformat()
        tasks.append(dict(url=url+'/dataset.xml',params=None,run=value+'T00:00:00Z',
                          lead_hours=30,product='dataset_metadata',limit_bytes=2*1024**2,suffix='.xml'))
        tasks.append(dict(url=url,params=[['var','u-component_of_wind_height_above_ground'],
            ['var','v-component_of_wind_height_above_ground'],['var','Wind_speed_gust_surface'],
            ['latitude','52.47371'],['longitude','9.37979'],['vertCoord','10'],
            ['time',valid],['accept','csv'],['addLatLon','true']],run=value+'T00:00:00Z',
            lead_hours=30,product='original_NCSS_point_CSV_NOT_native_GRIB',limit_bytes=2*1024**2,suffix='.csv'))
    return tasks


def fetch(spec, session=requests):
    """Fail closed on redirect/oversize; retain exact HTTP error body too."""
    record=dict(request=spec,captured_at_utc=datetime.now(timezone.utc).isoformat())
    try:
        with session.get(spec['url'],params=spec['params'],stream=True,allow_redirects=False,
                         timeout=(10,60),headers={'Accept-Encoding':'identity'}) as response:
            record['http_status']=response.status_code;body=bytearray()
            for part in response.iter_content(65536):
                if len(body)+len(part)>spec['limit_bytes']:
                    return dict(record,status='oversize_quarantined'),None
                body.extend(part)
            raw=bytes(body)
    except requests.RequestException as error:
        return dict(record,status='transport_error',error_type=type(error).__name__),None
    record.update(status='captured' if response.status_code==200 else 'provider_error',
                  bytes=len(raw),sha256=sha(raw))
    if spec['product']=='dataset_metadata' and response.status_code==200:
        try:
            doc=ET.fromstring(raw)
            record['grid_fields']=[dict(name=g.get('name'),units=g.get('units'),
                                       description=g.get('desc'),shape=g.get('shape')) for g in doc.iter('grid')]
        except ET.ParseError:record['metadata_parse']='invalid_XML_retained'
    if spec['suffix']=='.csv' and response.status_code==200:
        record['csv_header']=raw.decode('utf-8',errors='replace').splitlines()[:2]
        record['csv_line_count']=len(raw.splitlines())
    return record,raw


def run(operation, output, backend, root=ROOT):
    output=Path(output);output.mkdir(parents=True,exist_ok=True);tasks=plan(operation,root)
    if len(tasks)>40:raise ValueError('Research request budget exceeded')
    prefix='weather/archive/janwohlers78/mardorf-kitevorhersage/research/wp06-auxiliary-weather-v1'
    registration=dict(artifact_version='wp06-auxiliary-weather-originals-v1',operation=operation,
        code_commit=os.getenv('GITHUB_SHA'),provider_acquisition_repository='public_mardorf_data_collector',
        requests=tasks,maximum_logical_requests=len(tasks),max_attempts=1,
        registered_at_utc=datetime.now(timezone.utc).isoformat(),scientific_release=False,
        forecast_operator='GFS NCSS instantaneous point winds/gust; full-hour truth compatibility remains unqualified',
        native_admission=False,production_head_updated=False,git_weather_bytes_written=0)
    reference=publish(backend,prefix,output,'registration.json',canonical(registration));records=[];files=[]
    for i,spec in enumerate(tasks):
        record,raw=fetch(spec);record['ordinal']=i
        if raw is not None:
            name=f'{i:02d}'+spec['suffix'];files.append((name,raw));record['member_path']=name
        records.append(record);time.sleep(.2)
    # Two bounded checkpoints keep40 CDC ZIP members within the pack limit.
    packs=[]
    for first in range(0,len(files),20):
        members=files[first:first+20];pack,_=publish_pack(backend,prefix,output,members);packs.append(pack)
        for record in records:
            if record.get('member_path') in {name for name,raw in members}:record['container']=pack
    result=dict(registration=reference,operation=operation,records=records,packs=packs,
                complete_requests=True,status_counts=dict(Counter(r['status'] for r in records)),
                scientific_release=False,native_admission=False,production_head_updated=False)
    index=publish(backend,prefix,output,'index.json',canonical(result))
    print(json.dumps(dict(index=index,status_counts=result['status_counts'],operation=operation)),flush=True)


def main():
    p=argparse.ArgumentParser();p.add_argument('operation',choices=['dwd-recent','gfs-archive-probe'])
    p.add_argument('--output',type=Path,default=Path('work/historical-weather-research'));a=p.parse_args()
    defaults=json.loads((ROOT/'config/dev03_cloud_runtime_v1.json').read_bytes())['b2_location']
    env=dict(os.environ)
    for name,value in defaults.items():
        if not env.get(name):env[name]=value
    backend=B2Objects(resolve_b2_bucket(b2_settings(env)))
    run(a.operation,a.output,backend)


if __name__=='__main__':main()
