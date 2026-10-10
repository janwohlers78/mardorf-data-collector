"""Offline direct native point proof; existing pinned model readers stay frozen.

Consumes an already captured GRIB and verified external coordinate prefix. No
provider requests, interpolation, normalization, fitting or publication. This
successor experiment uses original native member IDs, never an assumed API map.
"""
import bz2
from datetime import datetime, timedelta, timezone
import hashlib
import math
import re
from urllib.parse import urlsplit
from .native_grid_v1 import coordinates, URLS, MAX_BYTES

HEADERS = ('centre','subCentre','shortName','paramId','units','dataDate','dataTime',
    'validityDate','validityTime','gridType','uuidOfHGrid','numberOfGridUsed','numberOfDataPoints',
    'perturbationNumber','typeOfEnsembleForecast','numberOfForecastsInEnsemble',
    'typeOfGeneratingProcess','generatingProcessIdentifier','tablesVersion','localTablesVersion',
    'productionStatusOfProcessedData','significanceOfReferenceTime','stepType','startStep','endStep',
    'typeOfLevel','level')


def verified(body, reference, limit):
    if (type(body) is not bytes or len(body)>limit or len(body)!=reference['bytes']
        or hashlib.sha256(body).hexdigest()!=reference['sha256']):
        raise ValueError('Original/prefix hash, length or byte budget mismatch')


def clock(date, time):
    return datetime.strptime(f'{int(date):08d}{int(time):04d}','%Y%m%d%H%M').replace(tzinfo=timezone.utc).isoformat()


def native_point(prefix, identity, requested):
    """Select the nearest cell by great-circle distance, preserving its offset."""
    arrays, checksum = coordinates(prefix, identity)
    lat, lon = (float(requested[k]) for k in ('latitude','longitude'))
    if not math.isfinite(lat+lon) or abs(lat)>90 or abs(lon)>180:
        raise ValueError('Explicit finite requested point required')
    a, b = math.radians(lat), math.radians(lon);best=None
    for i,(x,y) in enumerate(zip(arrays['clat'],arrays['clon'])):
        if not math.isfinite(x+y) or abs(x)>math.pi/2 or abs(y)>math.pi:
            raise ValueError('Invalid native coordinate')
        d=math.sin((x-a)/2)**2+math.cos(a)*math.cos(x)*math.sin((y-b)/2)**2
        if best is None or d<best[0]:best=(d,i,x,y)
    d,i,x,y=best
    return dict(method='direct_native_nearest_cell_great_circle_v1',requested_coordinate=requested,
        actual_coordinate=dict(latitude=math.degrees(x),longitude=math.degrees(y)),native_cell_index=i,
        representativeness_distance_m=12742000*math.asin(min(1.,math.sqrt(max(0.,d)))),
        grid_uuid=identity['uuid_of_horizontal_grid'],grid_definition_url=URLS[int(identity['number_of_grid_used'])],
        coordinate_span_sha256=checksum,representativeness_error='unquantified_no_height_correction')


def project_original(body, original_ref, prefix, prefix_ref, requested, *, expected_run_utc,
                     expected_member_ids, expected_parameter):
    """One original field, all explicitly expected members, direct embedded clocks."""
    import eccodes as ec
    verified(body,original_ref,MAX_BYTES);verified(prefix,prefix_ref,MAX_BYTES)
    capture=datetime.fromisoformat(original_ref['retrieved_at_utc'].replace('Z','+00:00'))
    if capture.tzinfo is None or capture.utcoffset().total_seconds()!=0:
        raise ValueError('Explicit original capture UTC required')
    url=urlsplit(original_ref['url'])
    name=re.search(r'icon-d2-eps_germany_icosahedral_single-level_(\d{10})_(\d{3})_2d_([a-z0-9_]+)\.grib2\.bz2$',url.path)
    if url.scheme!='https' or url.hostname!='opendata.dwd.de' or not name or not url.path.startswith('/weather/nwp/icon-d2-eps/grib/'):
        raise ValueError('Unbound direct DWD original URL')
    if clock(int(name[1][:8]),int(name[1][8:])*100)!=expected_run_utc:
        raise ValueError('Original URL/run mismatch')
    if (type(expected_member_ids) is not list or expected_member_ids!=sorted(set(expected_member_ids))
        or len(expected_member_ids)!=20 or any(type(x)is not int for x in expected_member_ids)):
        raise ValueError('Explicit full twenty-member native identity required')
    decoder=bz2.BZ2Decompressor();decoded=decoder.decompress(body,max_length=64*1024**2+1)
    if len(decoded)>64*1024**2 or not decoder.eof or decoder.unused_data:
        raise ValueError('Native decoded budget or compression framing mismatch')
    records=[];identities=set();geometry=None;first=None
    import tempfile
    with tempfile.TemporaryFile() as f:
        f.write(decoded);f.seek(0)
        while (handle:=ec.codes_grib_new_from_file(f)) is not None:
            try:
                if len(records)>=20:raise ValueError('Extra native member/message')
                h={k:ec.codes_get(handle,k) for k in HEADERS if ec.codes_is_defined(handle,k) and not ec.codes_is_missing(handle,k)}
                if any(k not in h for k in HEADERS):raise ValueError('Missing direct native source header')
                run,valid=clock(h['dataDate'],h['dataTime']),clock(h['validityDate'],h['validityTime'])
                if (str(h['centre']) not in ('edzw','78') or h['gridType'] not in ('unstructured','unstructured_grid')
                    or h['numberOfGridUsed']!=47 or run!=expected_run_utc or h['shortName']!=expected_parameter
                    or h['numberOfForecastsInEnsemble']!=20 or h['perturbationNumber'] not in expected_member_ids):
                    raise ValueError('Direct native provider/run/field/grid/member mismatch')
                if (datetime.fromisoformat(valid)!=datetime.fromisoformat(run)+timedelta(hours=int(name[2]))
                    or h['endStep']!=int(name[2])):
                    raise ValueError('Direct native lead/validity mismatch')
                common={k:v for k,v in h.items() if k!='perturbationNumber'}
                if first is not None and common!=first:raise ValueError('Mixed native member field supports')
                first=common
                member=h['perturbationNumber']
                if member in identities:raise ValueError('Duplicate native member')
                identities.add(member)
                identity=dict(number_of_grid_used=h['numberOfGridUsed'],uuid_of_horizontal_grid=h['uuidOfHGrid'],number_of_data_points=h['numberOfDataPoints'])
                if geometry is None:geometry=native_point(prefix,identity,requested)
                value=float(ec.codes_get_double_element(handle,'values',geometry['native_cell_index']))
                if not math.isfinite(value) or value==ec.codes_get(handle,'missingValue'):
                    raise ValueError('Missing/nonfinite original native point value')
                records.append(dict(original_sha256=original_ref['sha256'],message_index=len(records),header=h,
                    run_time_utc=run,valid_time_utc=valid,member_id_native=member,value_native=value,unit_native=h['units'],
                    prospective_feature_eligible_at_capture=datetime.fromisoformat(valid)>capture))
            finally:ec.codes_release(handle)
    if identities!=set(expected_member_ids):raise ValueError('Incomplete native member population')
    process={k:first[k] for k in ('centre','subCentre','typeOfGeneratingProcess','generatingProcessIdentifier',
        'tablesVersion','localTablesVersion','productionStatusOfProcessedData','significanceOfReferenceTime','uuidOfHGrid')}
    return dict(artifact_version='direct-native-point-development-proof-v1',original_ref=original_ref,
        grid_prefix_ref=prefix_ref,decoded_sha256=hashlib.sha256(decoded).hexdigest(),geometry=geometry,
        native_member_ids=sorted(identities),api_member_mapping='unverified_no_offset_or_control_assignment',
        process_headers=process,vendor_software_generation='unreported_process_id_is_not_software_version',
        records=records,initialization_evidence='embedded_in_each_original_GRIB_message',
        captured_at_utc=capture.isoformat(),private_first_reception_utc=None,
        historical_forecast_issuance_inferred=False,
        scientific_release=False,operational_promotion=False,production_reader_changed=False)
