"""Bounded public GFS research acquisition with native three-hour time support."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import time

from .historical_weather_research_v1 import GFS, fetch
from .svg_historical_research_v1 import canonical, publish, publish_pack

FIELDS = ('u-component_of_wind_height_above_ground', 'v-component_of_wind_height_above_ground',
          'Wind_speed_gust_surface', 'Temperature_height_above_ground',
          'Relative_humidity_height_above_ground', 'Pressure_reduced_to_MSL_msl',
          'Convective_available_potential_energy_surface', 'Planetary_Boundary_Layer_Height_surface')
LEADS = tuple(range(24, 49, 3))


def plan(start, end_exclusive):
    start, end = date.fromisoformat(start), date.fromisoformat(end_exclusive)
    if not 1 <= (end-start).days <= 90 or start < date(2016, 10, 8) or end > date(2026, 10, 8):
        raise ValueError('GFS research requires1-90days within the registered ten-year period')
    tasks = [dict(url='https://tds.gdex.ucar.edu/thredds/catalog/catalog_d084001.xml', params=None,
                  run=None, lead_hours=None, product='archive_collection_catalog',
                  limit_bytes=2*1024**2, suffix='.xml')]
    for offset in range((end-start).days):
        day = start+timedelta(days=offset)
        for lead in LEADS:
            url = GFS+day.strftime('%Y/%Y%m%d/gfs.0p25.%Y%m%d00.')+f'f{lead:03d}.grib2'
            valid = (datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)+timedelta(hours=lead)).isoformat()
            tasks.append(dict(url=url, params=[['var', ','.join(FIELDS)], ['time', valid],
                ['accept', 'netcdf3'], ['addLatLon', 'true'],
                ['north', '52.75'], ['south', '52.25'], ['west', '9.0'], ['east', '9.75']],
                run=day.isoformat()+'T00:00:00Z', lead_hours=lead,
                product='native_3h_GFS_small_grid_NCSS_NOT_native_GRIB', limit_bytes=2*1024**2, suffix='.nc'))
    return tasks


def run(start, end_exclusive, output, backend, *, workers=4, seconds=23*60, clock=None):
    if workers not in (1, 2, 4) or not 0 < seconds <= 23*60:
        raise ValueError('Unregistered resource budget')
    output = Path(output); output.mkdir(parents=True, exist_ok=True)
    tasks = plan(start, end_exclusive)
    prefix = 'weather/archive/janwohlers78/mardorf-kitevorhersage/research/wp06-GFS-ten-year-v1'
    registration = dict(artifact_version='wp06-GFS-public-range-originals-v1', operation='gfs-archive-range',
        code_commit=os.getenv('GITHUB_SHA'), provider_acquisition_repository='public_mardorf_data_collector',
        start=start, end_exclusive=end_exclusive, requests=tasks, maximum_logical_requests=len(tasks),
        max_attempts=2, workers=workers, seconds_budget=seconds, native_forecast_knots_hours=list(LEADS),
        registered_at_utc=datetime.now(timezone.utc).isoformat(),
        source_operator='NCSS GRIB-derived instantaneous grid fields; three-hourly knots, no invented intermediate forecasts',
        optional_meteorological_heights='All native heights retained; verify actual2m temperature/humidity and10m wind axes before admitting fields',
        forecast_generation='Changes must be independently dated and scoped before fitting; never silently pool ten years',
        scientific_release=False, native_admission=False, production_head_updated=False, git_weather_bytes_written=0)
    reference = publish(backend, prefix, output, 'registration.json', canonical(registration))
    clock = clock or time.monotonic
    deadline = clock()+seconds

    def capture(item):
        ordinal, spec = item; attempts = []; members = []
        if clock() >= deadline:
            return dict(ordinal=ordinal, request=spec, status='not_attempted_budget'), members
        for attempt in (1, 2):
            record, raw = fetch(spec)
            record['attempt'] = attempt
            if raw is not None:
                name = f'{ordinal:04d}-a{attempt}'+spec['suffix']
                members.append((name, raw)); record['member_path'] = name
            attempts.append(record)
            if record.get('http_status') not in (None, 429, 500, 502, 503, 504) or attempt == 2:
                break
            if clock()+3 >= deadline:
                break
            time.sleep(3)
        return dict(ordinal=ordinal, request=spec, status=attempts[-1]['status'], attempts=attempts,
                    chosen_attempt=attempts[-1]['attempt']), members

    records = []; members = []; packs = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for record, originals in pool.map(capture, enumerate(tasks)):
            records.append(record); members.extend(originals)
            while len(members) >= 20:
                pack, _ = publish_pack(backend, prefix, output, members[:20]); packs.append(pack)
                names = {name for name, body in members[:20]}
                for r in records:
                    for a in r.get('attempts', []):
                        if a.get('member_path') in names: a['container'] = pack
                del members[:20]
            if len(records) % 90 == 0:
                print(canonical(dict(progress=len(records), total=len(tasks), statuses=dict(Counter(r['status'] for r in records)))).decode(), flush=True)
    if members:
        pack, _ = publish_pack(backend, prefix, output, members); packs.append(pack)
        names = {name for name, body in members}
        for r in records:
            for a in r.get('attempts', []):
                if a.get('member_path') in names: a['container'] = pack
    complete = all(r['status'] != 'not_attempted_budget' for r in records)
    result = dict(registration=reference, operation='gfs-archive-range', records=records, packs=packs,
        complete_requests=complete, status_counts=dict(Counter(r['status'] for r in records)),
        scientific_release=False, native_admission=False, production_head_updated=False)
    index = publish(backend, prefix, output, 'index.json', canonical(result))
    print(canonical(dict(index=index, status_counts=result['status_counts'], complete_requests=complete,
                         start=start, end_exclusive=end_exclusive, operation='gfs-archive-range')).decode(), flush=True)
    return result
