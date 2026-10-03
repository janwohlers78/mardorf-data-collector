"""Native-only adapters: retain full raw, distinct messages and provider support."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import math
import re
from .core_v1 import CollectorError, canonical, digest, strict_json, utc, stamp

PARAMETERS = {
    "10u": "wind_u",
    "u": "wind_u",
    "u_10m": "wind_u",
    "10v": "wind_v",
    "v": "wind_v",
    "v_10m": "wind_v",
    "gust": "wind_gust",
    "10fg": "wind_gust",
    "vmax_10m": "wind_gust",
    "2t": "air_temperature",
    "t": "air_temperature",
    "t_2m": "air_temperature",
    "2d": "dewpoint_temperature",
    "dpt": "dewpoint_temperature",
    "td_2m": "dewpoint_temperature",
    "2r": "relative_humidity",
    "rh": "relative_humidity",
    "relhum_2m": "relative_humidity",
    "sp": "air_pressure",
    "msl": "air_pressure",
    "prmsl": "air_pressure",
    "ps": "air_pressure",
    "pmsl": "air_pressure",
    "tp": "rainfall_amount",
    "apcp": "rainfall_amount",
    "tot_prec": "rainfall_amount",
    "acpcp": "convective_precipitation",
    "rain_con": "convective_precipitation",
    "tcc": "cloud_fraction",
    "clct": "cloud_fraction",
    "ssrd": "shortwave_total",
    "dswrf": "shortwave_total",
    "aswdir_s": "shortwave_direct",
    "aswdifd_s": "shortwave_diffuse",
    "cape": "cape",
    "cape_ml": "cape",
    "cin": "cin",
    "cin_ml": "cin",
}
STEP_UNITS = {
    0: 60,
    1: 3600,
    2: 86400,
    10: 10800,
    11: 21600,
    12: 43200,
    13: 1,
    "m": 60,
    "h": 3600,
    "s": 1,
    "D": 86400,
}


def base_field(c, j, s, raw_id, site, quantity, pointer, value, seen):
    target = next(
        (
            t
            for t in j["target_ids"]
            if c.tables["targets"][t]["site_id"] == site
            and c.tables["targets"][t]["quantity_id"] == quantity
        ),
        None,
    )
    return {
        "schema_version": 1,
        "artifact_version": "dev03-wp12-native-field-v1",
        "configuration": c.configuration,
        "record_kind": (
            "observation" if j["kind"].startswith("observation") else "forecast"
        ),
        "domain": {
            "profile_id": j["profile_id"],
            "site_id": site,
            "station_id": j["station_id"],
            "sensor_id": j["sensor_id"],
            "quantity_id": quantity,
            "target_id": target,
        },
        "source": {
            "provider_binding_id": j["provider_binding_id"],
            "model_id_native": s["model_id_native"],
            "product_id_native": s["product_id_native"],
            "native_parameter": None,
            "source_url": s["url"],
            "raw_object_id": raw_id,
            "raw_record_pointer": pointer,
            "source_revision": j["job_id"],
            "native_metadata_object_id": None,
        },
        "level": {
            "type_native": None,
            "value_native": None,
            "measurement_height_status": "not_applicable",
            "measurement_height_m": None,
        },
        "time": {
            "run_time_utc": None,
            "published_at_utc": None,
            "first_seen_at_utc": seen,
            "first_seen_scope": "public_capture_observed",
            "private_first_seen_at_utc": None,
            "measured_at_utc": None,
            "valid_start_utc": None,
            "valid_end_utc": None,
            "operator": "unknown",
            "closure": "unknown",
            "resolution_method": "wp13-provider-native-support-v1",
        },
        "member": {
            "member_id": None,
            "member_id_native": None,
            "member_role": "not_applicable",
            "ensemble_set_id": None,
        },
        "value_native": value,
        "unit_native": None,
        "unit_evidence": "unknown",
        "binding_id": j["provider_binding_id"],
        "qc_native": [],
        "required_capabilities": ["wp12-native-field-v1"],
        "extensions": {},
        "spatial_support": {
            "requested_site_id": site,
            "actual_latitude": None,
            "actual_longitude": None,
            "native_grid_id": None,
            "method": "unresolved",
        },
    }


def observations(c, j, s, raw_id, decoded, seen):
    body = strict_json(decoded)
    station = c.tables["stations"][j["station_id"]]
    sensor = c.tables["sensors"][j["sensor_id"]]
    if (
        "station_id" in body
        and str(body["station_id"]) != station["provider_station_id"]
    ):
        raise CollectorError("native station contradiction")
    endpoint = "historic" if j["kind"] == "observation_historic" else "current"
    selector = sensor["provider_sensor_selector"]
    structure = selector[endpoint + "_data_structure_type"]
    sensor_type = selector["sensor_type"]
    selected = [
        (i, x)
        for i, x in enumerate(body.get("sensors", []))
        if x.get("sensor_type") == sensor_type
        and x.get("data_structure_type") == structure
    ]
    if len(selected) != 1:
        raise CollectorError("sensor selector missing or ambiguous")
    i, sensor_data = selected[0]
    rows = sensor_data.get("data", [])
    if not isinstance(rows, list):
        raise CollectorError("native data rows required")
    bindings = {
        b["native_parameter"]: b
        for b in c.binding["weatherlink_fields"]
        if b["provider_binding_id"] == j["provider_binding_id"]
        and b["endpoint_kind"] == endpoint
    }
    fields = []
    statuses = []
    for n, row in enumerate(rows):
        if type(row.get("ts")) is not int:
            raise CollectorError("native integer timestamp required")
        measured = datetime.fromtimestamp(row["ts"], timezone.utc)
        if measured > utc(seen):
            raise CollectorError("measurement after capture")
        if j["window"] and not utc(j["window"]["start_utc"]) <= measured < utc(
            j["window"]["end_utc"]
        ):
            continue
        if endpoint == "historic" and (
            type(row.get("arch_int")) is not int or row["arch_int"] <= 0
        ):
            raise CollectorError("native archive interval required")
        for parameter in sorted(row):
            binding = bindings.get(parameter)
            pointer = f"/sensors/{i}/data/{n}/" + parameter.replace("~", "~0").replace(
                "/", "~1"
            )
            if not binding or binding["quantity_id"] not in c.tables["quantities"]:
                statuses.append(
                    {
                        "native_parameter": parameter,
                        "row_pointer": pointer,
                        "status": "raw_only",
                    }
                )
                continue
            f = base_field(
                c,
                j,
                s,
                raw_id,
                j["site_ids"][0],
                binding["quantity_id"],
                pointer,
                row[parameter],
                seen,
            )
            f["binding_id"] = binding["id"]
            f["source"]["native_parameter"] = parameter
            height = sensor["measurement_height"]
            f["level"].update(
                measurement_height_status=height["status"],
                measurement_height_m=height["value_m"],
            )
            operator = binding["operator"]
            duration = (
                row["arch_int"]
                if endpoint == "historic"
                else 600 if operator in ("mean", "maximum") else 0
            )
            start = measured - timedelta(seconds=duration)
            f["time"].update(
                measured_at_utc=stamp(measured),
                valid_start_utc=stamp(start),
                valid_end_utc=stamp(measured),
                operator=operator,
                closure=(
                    "point"
                    if operator == "point"
                    else "(start,end]" if operator != "unknown" else "unknown"
                ),
            )
            lon, lat = c.tables["sites"][j["site_ids"][0]]["geometry"]["coordinates"]
            f["spatial_support"].update(
                actual_latitude=lat, actual_longitude=lon, method="station_registry"
            )
            f.update(unit_native=binding["unit"], unit_evidence="provider_definition")
            fields.append(f)
            statuses.append(
                {
                    "native_parameter": parameter,
                    "row_pointer": pointer,
                    "status": "native_null" if row[parameter] is None else "received",
                }
            )
        for parameter in sorted(set(bindings) - set(row)):
            statuses.append(
                {
                    "native_parameter": parameter,
                    "row_pointer": f"/sensors/{i}/data/{n}",
                    "status": "not_returned",
                }
            )
    return fields, statuses


def eccodes_decode(raw, latitude, longitude):
    """Every original GRIB message, its native header and actual nearest point."""
    import eccodes as ec
    import io

    keys = (
        "shortName",
        "name",
        "paramId",
        "discipline",
        "parameterCategory",
        "parameterNumber",
        "units",
        "typeOfLevel",
        "level",
        "stepType",
        "startStep",
        "endStep",
        "stepUnits",
        "stepRange",
        "dataDate",
        "dataTime",
        "validityDate",
        "validityTime",
        "gridType",
        "Ni",
        "Nj",
        "md5GridSection",
        "perturbationNumber",
        "typeOfEnsembleForecast",
        "numberOfForecastsInEnsemble",
    )
    stream = io.BytesIO(raw)
    out = []
    # Python ecCodes accepts a file object backed by a real descriptor.
    import tempfile

    with tempfile.TemporaryFile() as file:
        file.write(raw)
        file.seek(0)
        while True:
            handle = ec.codes_grib_new_from_file(file)
            if handle is None:
                break
            try:
                meta = {}
                for k in keys:
                    if ec.codes_is_defined(handle, k) and not ec.codes_is_missing(
                        handle, k
                    ):
                        meta[k] = ec.codes_get(handle, k)
                nearest = ec.codes_grib_find_nearest(
                    handle, latitude, longitude, npoints=1
                )[0]
                meta.update(
                    value=nearest["value"],
                    latitude=nearest["lat"],
                    longitude=nearest["lon"],
                    grid_point_index=nearest["index"],
                )
                if ec.codes_is_defined(handle, "missingValue") and nearest[
                    "value"
                ] == ec.codes_get(handle, "missingValue"):
                    meta["value"] = None
                out.append(meta)
            finally:
                ec.codes_release(handle)
    return out


def grib_time(meta):
    def date_time(date_key, time_key):
        if type(meta.get(date_key)) is not int or type(meta.get(time_key)) is not int:
            raise CollectorError("native run/valid clock missing")
        return datetime.strptime(
            str(meta[date_key]) + f"{meta[time_key]:04d}", "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc)

    run = date_time("dataDate", "dataTime")
    end = date_time("validityDate", "validityTime")
    operator = {
        "instant": "point",
        "avg": "mean",
        "accum": "accumulation",
        "max": "maximum",
    }.get(meta.get("stepType"), "unknown")
    start = end
    closure = "point" if operator == "point" else "unknown"
    if operator not in ("point", "unknown"):
        factor = STEP_UNITS.get(meta.get("stepUnits"))
        if (
            factor is None
            or type(meta.get("startStep")) not in (int, float)
            or type(meta.get("endStep")) not in (int, float)
        ):
            operator = "unknown"
        else:
            start = run + timedelta(seconds=meta["startStep"] * factor)
            if run + timedelta(seconds=meta["endStep"] * factor) != end or end <= start:
                raise CollectorError("GRIB step/valid interval contradiction")
            closure = "(start,end]"
    if (
        operator == "point"
        and STEP_UNITS.get(meta.get("stepUnits")) is not None
        and type(meta.get("endStep")) in (int, float)
        and run + timedelta(seconds=meta["endStep"] * STEP_UNITS[meta["stepUnits"]])
        != end
    ):
        raise CollectorError("GRIB point step/valid contradiction")
    if end < run:
        raise CollectorError("GRIB validity before run")
    return {
        "run_time_utc": stamp(run),
        "valid_start_utc": stamp(start),
        "valid_end_utc": stamp(end),
        "operator": operator,
        "closure": closure,
    }


def grib_fields(c, j, s, raw_id, decoded, seen, *, decoder=None):
    if not decoded.startswith(b"GRIB"):
        raise CollectorError("GRIB magic missing")
    decoder = decoder or eccodes_decode
    fields = []
    statuses = []
    for site in j["site_ids"]:
        lon, lat = c.tables["sites"][site]["geometry"]["coordinates"]
        rows = decoder(decoded, lat, lon)
        for n, meta in enumerate(rows):
            parameter = meta.get("shortName")
            quantity = PARAMETERS.get(str(parameter).lower())
            if quantity is None:
                statuses.append(
                    {
                        "native_parameter": parameter,
                        "row_pointer": f"/messages/{n}",
                        "status": "raw_only",
                    }
                )
                continue
            f = base_field(
                c,
                j,
                s,
                raw_id,
                site,
                quantity,
                f"/messages/{n}/value",
                meta.get("value"),
                seen,
            )
            native = grib_time(meta)
            if s["expected_run_time_utc"] is not None and utc(
                native["run_time_utc"]
            ) != utc(s["expected_run_time_utc"]):
                raise CollectorError("requested/native model run contradiction")
            f["time"].update(native)
            f["source"]["native_parameter"] = s["native_parameter"] or parameter
            f["level"].update(
                type_native=meta.get("typeOfLevel"), value_native=meta.get("level")
            )
            f.update(
                unit_native=meta.get("units"),
                unit_evidence="native_metadata" if meta.get("units") else "unknown",
            )
            if not all(
                type(meta.get(k)) in (int, float) and math.isfinite(meta[k])
                for k in ("latitude", "longitude")
            ):
                raise CollectorError("actual GRIB extraction coordinate missing")
            native_lon = ((meta["longitude"] + 180) % 360) - 180
            f["spatial_support"].update(
                actual_latitude=meta["latitude"],
                actual_longitude=native_lon,
                native_grid_id=str(meta.get("md5GridSection") or meta.get("gridType")),
                method="native_extraction_metadata",
            )
            binding = c.forecast[j["provider_binding_id"]]
            expected = binding.get("expected_member_ids")
            if expected:
                number = meta.get("perturbationNumber")
                if type(number) is not int:
                    raise CollectorError("native perturbation number missing")
                member = "c00" if number == 0 else f"p{number:02d}"
                if member not in expected:
                    raise CollectorError("unexpected native member")
                f["member"].update(
                    member_id=member,
                    member_id_native=member,
                    member_role=binding["member_roles"][member],
                    ensemble_set_id=digest(
                        {
                            "job": j["job_id"],
                            "run": native["run_time_utc"],
                            "product": s["product_id_native"],
                        }
                    ),
                )
            else:
                f["member"]["member_role"] = "deterministic"
            f["extensions"]["wp13:original-grib-header:v1"] = deepcopy(meta)
            fields.append(f)
            statuses.append(
                {
                    "native_parameter": parameter,
                    "row_pointer": f"/messages/{n}",
                    "status": (
                        "native_null" if meta.get("value") is None else "received"
                    ),
                }
            )
    return fields, statuses


def openmeteo_fields(c, j, s, raw_id, decoded, seen, *, metadata=None):
    body = strict_json(decoded)
    hourly = body.get("hourly", {})
    units = body.get("hourly_units", {})
    times = hourly.get("time", [])
    before = (metadata or {}).get("metadata_before")
    after = (metadata or {}).get("metadata_after")
    if (
        not before
        or not after
        or any(before.get(k) != after.get(k) for k in c.openmeteo["stability_keys"])
    ):
        raise CollectorError("EPS metadata cycle changed or missing")
    run_value = before.get(c.openmeteo["run_key"])
    publication_value = before.get(c.openmeteo["publication_key"])
    if type(run_value) is not int or type(publication_value) is not int:
        raise CollectorError("native EPS run/publication missing")
    run = stamp(datetime.fromtimestamp(run_value, timezone.utc))
    published = stamp(datetime.fromtimestamp(publication_value, timezone.utc))
    if utc(run) > utc(published) or utc(published) > utc(seen):
        raise CollectorError("EPS native availability contradiction")
    if s["expected_run_time_utc"] is not None and utc(run) != utc(
        s["expected_run_time_utc"]
    ):
        raise CollectorError("native model run contradiction")
    parsed_times = [utc(t if t.endswith("Z") else t + "Z") for t in times]
    if any(b <= a for a, b in zip(parsed_times, parsed_times[1:])):
        raise CollectorError("EPS time axis must be unique and increasing")
    from urllib.parse import parse_qs, urlparse

    if parse_qs(urlparse(s["url"]).query).get("models") != [c.openmeteo["model_query"]]:
        raise CollectorError("named ensemble model missing")
    mapping = {
        "temperature_2m": ("air_temperature", "heightAboveGround", 2, "point"),
        "wind_speed_10m": ("wind_speed", "heightAboveGround", 10, "point"),
        "wind_direction_10m": ("wind_direction", "heightAboveGround", 10, "point"),
        "wind_gusts_10m": ("wind_gust", "heightAboveGround", 10, "maximum"),
        "precipitation": ("rainfall_amount", "surface", 0, "accumulation"),
        "cape": ("cape", "surface", 0, "point"),
    }
    fields = []
    statuses = []
    binding = c.forecast[j["provider_binding_id"]]
    for site in j["site_ids"]:
        if len(j["site_ids"]) != 1:
            raise CollectorError("point JSON response cannot relabel multiple sites")
        for parameter, values in hourly.items():
            m = re.fullmatch(r"(.+)_member(\d+)", parameter)
            if not m or m[1] not in mapping:
                continue
            member = str(int(m[2]))
            quantity, level, height, operator = mapping[m[1]]
            if member not in binding["expected_member_ids"] or len(values) != len(
                times
            ):
                raise CollectorError("native member/time axis mismatch")
            for n, timestamp in enumerate(times):
                # Explicit API timezone contract: UTC/GMT only; original text retained.
                if body.get("utc_offset_seconds") != 0:
                    raise CollectorError("non-UTC EPS time axis")
                valid = timestamp if timestamp.endswith("Z") else timestamp + "Z"
                end = utc(valid)
                if end < utc(run):
                    continue
                f = base_field(
                    c,
                    j,
                    s,
                    raw_id,
                    site,
                    quantity,
                    f"/hourly/{parameter}/{n}",
                    values[n],
                    seen,
                )
                f["source"]["native_parameter"] = parameter
                start = end - timedelta(hours=1) if operator != "point" else end
                f["time"].update(
                    run_time_utc=run,
                    published_at_utc=published,
                    valid_start_utc=stamp(start),
                    valid_end_utc=stamp(end),
                    operator=operator,
                    closure="point" if operator == "point" else "(start,end]",
                )
                f["level"].update(type_native=level, value_native=height)
                f.update(
                    unit_native=units.get(parameter, units.get(m[1])),
                    unit_evidence="native_metadata",
                )
                f["member"].update(
                    member_id=member,
                    member_id_native=int(m[2]),
                    member_role="ensemble_member",
                    ensemble_set_id=digest(
                        {
                            "job": j["job_id"],
                            "run": run,
                            "product": s["product_id_native"],
                        }
                    ),
                )
                f["spatial_support"].update(
                    actual_latitude=body.get("latitude"),
                    actual_longitude=body.get("longitude"),
                    native_grid_id=None,
                    method="native_extraction_metadata",
                )
                fields.append(f)
                statuses.append(
                    {
                        "native_parameter": parameter,
                        "row_pointer": f"/hourly/{parameter}/{n}",
                        "status": "native_null" if values[n] is None else "received",
                    }
                )
    return fields, statuses


def finalize_forecasts(c, j, fields, objects):
    extra_objects = []
    extra_raw = {}
    binary_ids = {
        o["object_id"]: o["decoded_sha256"]
        for o in objects
        if o["media_type"] == "application/x-grib"
    }
    # Separate site extraction metadata so a shared parent never gets relabelled.
    groups = {}
    for f in fields:
        if f["source"]["raw_object_id"] in binary_ids:
            groups.setdefault(
                (f["source"]["raw_object_id"], f["domain"]["site_id"]), []
            ).append(f)
    for (raw_id, site), rows in groups.items():
        key = "extraction-" + digest({"raw": raw_id, "site": site})
        records = []
        for f in rows:
            metadata = {
                k: deepcopy(f[k])
                for k in ("level", "spatial_support", "member", "unit_native")
            }
            metadata["time"] = {
                k: v
                for k, v in f["time"].items()
                if k
                not in (
                    "first_seen_at_utc",
                    "first_seen_scope",
                    "private_first_seen_at_utc",
                )
            }
            records.append(
                {
                    "value": f["value_native"],
                    "metadata": metadata,
                    "native_header": f["extensions"]["wp13:original-grib-header:v1"],
                }
            )
        payload = canonical(
            {"source_decoded_sha256": binary_ids[raw_id], "messages": records}
        )
        sha = hashlib.sha256(payload).hexdigest()
        extra_raw[key] = payload
        extra_objects.append(
            {
                "object_id": key,
                "path": "metadata/" + key + ".json",
                "transport_sha256": sha,
                "decoded_sha256": sha,
                "transport_bytes": len(payload),
                "encoding": "utf-8",
                "compression": "none",
                "media_type": "application/json",
            }
        )
        for n, f in enumerate(rows):
            f["source"].update(
                native_metadata_object_id=key, raw_record_pointer=f"/messages/{n}/value"
            )
            f["extensions"]["wp12:native-metadata-proof:v1"] = {
                "raw_object_id": key,
                "pointer": f"/messages/{n}/metadata",
                "sha256": sha,
            }
    return fields, extra_objects, extra_raw
