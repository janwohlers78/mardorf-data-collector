"""Versioned DWD wind metadata capture after the frozen wind fetchers.

Existing forecast values and derived products never change. A fresh exact
run/valid response supplies native metadata only after an exact value/point
join; its later availability clock is retained. Historical rows are untouched.
"""
from copy import deepcopy
from datetime import datetime, timezone
import math
import re

VERSION = 'dwd-wind-native-metadata-capture-v1'
PARAMETERS = ('u_10m','v_10m','vmax_10m')
REQUIRED = ('units','typeOfLevel','level','stepType','startStep','endStep','stepUnits')
URL = re.compile(r'^https://opendata\.dwd\.de/weather/nwp/(?P<model>icon-d2|icon-eu)/grib/'
    r'(?P<hour>[0-9]{2})/(?P<param>u_10m|v_10m|vmax_10m)/(?P=model)_'
    r'(?P<domain>germany|europe)_regular-lat-lon_single-level_(?P<run>[0-9]{10})_'
    r'(?P<lead>[0-9]{3})_(?:2d_)?(?P=param)\.grib2\.bz2$',re.IGNORECASE)


def _items(value):
    return value if isinstance(value,list) else [value] if isinstance(value,dict) else []


def _finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def capture_jobs(model, row):
    """Only existing numeric wind fields and their exact persisted source URL."""
    run = datetime.fromisoformat(row['run_time_utc'].replace('Z','+00:00'))
    if run.tzinfo is None:
        return []
    cycle = run.astimezone(timezone.utc).strftime('%Y%m%d%H')
    provider = {'ICON-D2':'icon-d2','ICON-EU':'icon-eu'}[model]
    domain = 'germany' if model == 'ICON-D2' else 'europe'
    jobs = []
    for parameter in PARAMETERS:
        items = _items((row.get('values') or {}).get(parameter))
        if not any(_finite(i.get('value')) and any(i.get(k) is None for k in REQUIRED) for i in items):
            continue
        urls = []
        for url in row.get('source_urls',[]):
            match = URL.fullmatch(url)
            if (match and match['model'] == provider and match['domain'] == domain and match['param'] == parameter
                    and match['run'] == cycle and match['hour'] == cycle[-2:]
                    and int(match['lead']) == row['forecast_lead_hours']):
                urls.append(url)
        if len(set(urls)) == 1:
            jobs.append((parameter,urls[0]))
    return jobs


def merge_exact_metadata(row, parameter, captured, *, source_url):
    """No metadata graft across values, messages or extraction points."""
    result = {'method_version':VERSION,'parameter':parameter,'status':'not_joined','joined_fields':0,
        'source_url':source_url,'forecast_values_changed':False}
    existing = _items((row.get('values') or {}).get(parameter))
    if not existing or not isinstance(captured,list):
        return result
    point = row.get('forecast_coordinate_or_grid_point') or {}
    replacements = []
    for item in existing:
        if not _finite(item.get('value')):
            replacements.append(item);continue
        if all(item.get(k) is not None for k in REQUIRED):
            replacements.append(item);continue
        latitude = item.get('latitude',item.get('lat',point.get('latitude')))
        longitude = item.get('longitude',item.get('lon',point.get('longitude')))
        matches = []
        for native in captured:
            if (not _finite(native.get('value')) or native['value'] != item['value']
                    or not _finite(latitude) or not _finite(longitude)
                    or native.get('latitude') != latitude or native.get('longitude') != longitude
                    or any(native.get(k) is None for k in REQUIRED)
                    or native.get('availability_status') != 'received'
                    or not re.fullmatch('[0-9a-f]{64}',native.get('source_sha256',''))):
                continue
            if item.get('shortName') is not None and item['shortName'] != native.get('shortName'):
                continue
            if item.get('stepRange') is not None and item['stepRange'] != native.get('stepRange'):
                continue
            if any(item.get(k) is not None and item[k] != native.get(k) for k in REQUIRED):
                continue
            # Native metadata was observed now, not at the base value's receipt.
            try:
                clocks = [datetime.fromisoformat(native[k].replace('Z','+00:00'))
                          for k in ('availability_observed_at_utc','field_available_at_utc')]
                if any(t.tzinfo is None for t in clocks):
                    continue
            except (KeyError,ValueError,TypeError):
                continue
            matches.append(native)
        if len(matches) != 1:
            return dict(result,joined_fields=0,reason='missing_ambiguous_or_conflicting_exact_value_point_join')
        native = matches[0]
        merged = deepcopy(item)
        merged.update({k:v for k,v in native.items() if k not in ('value','latitude','longitude')})
        merged.update(native_metadata_capture_version=VERSION,native_metadata_capture_source_url=source_url,
            value_acquisition_available_at_utc=item.get('field_available_at_utc'),
            native_metadata_observed_at_utc=native['field_available_at_utc'])
        replacements.append(merged)
        result['joined_fields'] += 1
    original = row['values'][parameter]
    if result['joined_fields']:
        row['values'][parameter] = replacements if isinstance(original,list) else replacements[0]
        result['status'] = 'verified_metadata_attached'
    return result
