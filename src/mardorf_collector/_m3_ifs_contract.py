"""Strict point extraction contract for separately admitted native IFS fields.

No I/O or decoder is hidden here. Callers must independently bind producer,
object and all codecs before passing decoded arrays. Output is a forecast
covariate, never a local measurement or an hourly observational wind mean.
"""
from datetime import datetime, timedelta, timezone
import math


class QualificationError(ValueError):
    pass


FIELDS = {
    '10u': ('m s**-1', '10', 'heightAboveGround'),
    '10v': ('m s**-1', '10', 'heightAboveGround'),
    '2t': ('K', '2', 'heightAboveGround'),
    'sp': ('Pa', '0', 'surface'),
    'msl': ('Pa', '0', None),
}


def forecast_times(header):
    """Validate native initialization, valid time and instantaneous lead."""
    if header.get('stepType') != 'instant':
        raise QualificationError('instantaneous_forecast_covariate_required')
    if header.get('indicatorOfUnitOfTimeRange') != 'h':
        raise QualificationError('explicit_hour_forecast_period_required')
    try:
        run = datetime.strptime(str(header['dataDate']) + f"{int(header['dataTime']):04d}", '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
        valid = datetime.strptime(str(header['validityDate']) + f"{int(header['validityTime']):04d}", '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
        lead = float(header['endStep'])
        start = float(header['startStep'])
    except (KeyError, TypeError, ValueError):
        raise QualificationError('native_time_metadata_invalid') from None
    if not math.isfinite(lead) or lead < 0 or lead != start or run + timedelta(hours=lead) != valid:
        raise QualificationError('native_forecast_time_identity_failed')
    return run, lead, valid


def field_contract(header):
    name = header.get('shortName')
    if name not in FIELDS:
        raise QualificationError('unsupported_covariate')
    unit, level, level_type = FIELDS[name]
    if header.get('units') != unit or str(header.get('level')) != level:
        raise QualificationError('native_unit_or_height_changed')
    if level_type is not None and header.get('typeOfLevel') != level_type:
        raise QualificationError('native_level_type_changed')
    if name == 'msl' and header.get('typeOfLevel') not in ('surface', 'meanSea'):
        raise QualificationError('mean_sea_pressure_reference_changed')
    return name, forecast_times(header)


def bilinear(latitude, longitude, values, missing, *, target_latitude, target_longitude):
    """Interpolate only four finite native bracketing cells, never extrapolate."""
    n = len(values)
    if not n or any(len(a) != n for a in (latitude, longitude, missing)):
        raise QualificationError('native_coordinate_grain_mismatch')
    if not all(math.isfinite(float(x)) for x in (target_latitude, target_longitude)):
        raise QualificationError('invalid_target_coordinate')
    cells = {}
    for y, x, value, absent in zip(latitude, longitude, values, missing):
        y, x, value = float(y), float(x), float(value)
        if not math.isfinite(y) or not math.isfinite(x) or not -90 <= y <= 90 or not -180 <= x <= 180:
            raise QualificationError('invalid_native_geography')
        if (y, x) in cells:
            raise QualificationError('duplicate_native_cell')
        cells[y, x] = None if absent or not math.isfinite(value) else value
    ys, xs = sorted({p[0] for p in cells}), sorted({p[1] for p in cells})
    if not ys[0] <= target_latitude <= ys[-1] or not xs[0] <= target_longitude <= xs[-1]:
        raise QualificationError('target_outside_native_bbox')
    ly = max(y for y in ys if y <= target_latitude); hy = min(y for y in ys if y >= target_latitude)
    lx = max(x for x in xs if x <= target_longitude); hx = min(x for x in xs if x >= target_longitude)
    needed = {(ly, lx), (ly, hx), (hy, lx), (hy, hx)}
    if any(cells.get(p) is None for p in needed):
        raise QualificationError('missing_native_interpolation_neighbour')
    fy = 0.0 if hy == ly else (target_latitude - ly) / (hy - ly)
    fx = 0.0 if hx == lx else (target_longitude - lx) / (hx - lx)
    result = (1-fy)*((1-fx)*cells[ly,lx]+fx*cells[ly,hx]) + fy*((1-fx)*cells[hy,lx]+fx*cells[hy,hx])
    return result, sorted(needed)


def feature_record(headers, point_values, *, source_id, artifact_sha256, generation, target):
    """Bind scalar/vector point features without relabelling their operator."""
    if set(point_values) != set(FIELDS) or set(headers) != set(FIELDS):
        raise QualificationError('incomplete_native_feature_set')
    times = []
    for name, header in headers.items():
        actual, timing = field_contract(header)
        if actual != name:
            raise QualificationError('field_identity_mismatch')
        times.append(timing)
    if any(t != times[0] for t in times) or not all(math.isfinite(float(v)) for v in point_values.values()):
        raise QualificationError('mixed_validity_or_nonfinite_feature')
    run, lead, valid = times[0]
    return dict(source_id=source_id, artifact_sha256=artifact_sha256, generation=generation,
        target=target, initialization_utc=run.isoformat(), lead_hours=lead,
        valid_utc=valid.isoformat(), native_operator='instantaneous_forecast_covariate',
        not_hourly_measurement_mean=True, historical_publication_latency_qualified=False,
        wind_u_ms=float(point_values['10u']), wind_v_ms=float(point_values['10v']),
        wind_speed_ms=math.hypot(point_values['10u'], point_values['10v']),
        temperature_c=float(point_values['2t'])-273.15,
        pressure_model_surface_hpa=float(point_values['sp'])/100,
        pressure_msl_hpa=float(point_values['msl'])/100,
        model_surface_not_target_barometer=True, scientific_release=False)
