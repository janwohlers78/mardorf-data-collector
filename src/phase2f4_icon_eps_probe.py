#!/usr/bin/env python3
"""One-shot Phase 2F-4 capability probe for ICON-D2-EPS member fields."""
import json
import math
import re
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

META = "https://api.open-meteo.com/data/dwd_icon_d2_eps/static/meta.json"
API = "https://ensemble-api.open-meteo.com/v1/ensemble"
LAT = 52.4942
LON = 9.3418
EXISTING = [
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "precipitation",
    "cape",
]
CANDIDATES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "shortwave_radiation",
    "cin",
]


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": "mardorf-data-collector/phase2f4-probe"})
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.0,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    s.mount(
        "https://",
        HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=2),
    )
    return s


S = session()


def metadata():
    response = S.get(META, timeout=45)
    response.raise_for_status()
    return response.json()


def query(fields):
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(fields),
        "models": "dwd_icon_d2_eps",
        "past_days": 1,
        "forecast_days": 4,
        "wind_speed_unit": "ms",
        "timezone": "GMT",
    }
    response = S.get(API, params=params, timeout=120)
    if response.status_code >= 400:
        return None, response.status_code, response.text[:1000]
    return response.json(), response.status_code, None


def normalize_time(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def member_columns(hourly, prefix):
    output = {}
    if prefix in hourly:
        output[0] = hourly[prefix]
    for key, values in hourly.items():
        match = re.fullmatch(re.escape(prefix) + r"_member(\d+)", key)
        if match:
            output[int(match.group(1))] = values
    return output


def main():
    before = metadata()
    run = datetime.fromtimestamp(
        int(before["last_run_initialisation_time"]), tz=timezone.utc
    )
    availability = datetime.fromtimestamp(
        int(before["last_run_availability_time"]), tz=timezone.utc
    )
    age = (datetime.now(timezone.utc) - availability).total_seconds()
    if age < 600:
        raise RuntimeError(
            f"run not settled: availability age {age:.1f}s < 600s"
        )

    capability = {}
    accepted = set()
    rejected = set()

    def classify(group):
        if not group:
            return
        payload, status, error = query(EXISTING + list(group))
        if payload is not None:
            for field in group:
                capability[field] = {
                    "http_status": status,
                    "accepted": True,
                    "error": None,
                }
                accepted.add(field)
            return
        if len(group) == 1:
            field = group[0]
            capability[field] = {
                "http_status": status,
                "accepted": False,
                "error": error,
            }
            rejected.add(field)
            return
        middle = len(group) // 2
        classify(group[:middle])
        classify(group[middle:])

    classify(CANDIDATES)
    fields = EXISTING + [field for field in CANDIDATES if field in accepted]
    payload, status, error = query(fields)
    if payload is None:
        raise RuntimeError(
            f"final combined request failed HTTP {status}: {error}"
        )

    after = metadata()
    stable = all(
        before.get(key) == after.get(key)
        for key in (
            "last_run_initialisation_time",
            "last_run_availability_time",
            "last_run_modification_time",
        )
    )
    if not stable:
        raise RuntimeError(
            "provider metadata changed across capability/final ensemble responses"
        )

    hourly = payload.get("hourly") or {}
    index = {
        normalize_time(value): i
        for i, value in enumerate(hourly.get("time") or [])
    }
    required = [
        (run + timedelta(hours=hour)).isoformat()
        for hour in range(49)
    ]
    missing_times = [value for value in required if value not in index]
    if missing_times:
        raise RuntimeError(
            f"required 0-48h axis missing: {missing_times[:5]}"
        )

    report = {
        "run_time_utc": run.isoformat(),
        "availability_age_seconds": round(age, 1),
        "metadata_stable": stable,
        "capability": capability,
        "accepted_candidates": [
            field for field in CANDIDATES if field in accepted
        ],
        "rejected_candidates": [
            field for field in CANDIDATES if field in rejected
        ],
        "final_request_fields": fields,
        "returned_coordinate": {
            "latitude": payload.get("latitude"),
            "longitude": payload.get("longitude"),
        },
        "response_http_status": status,
        "hourly_units": payload.get("hourly_units") or {},
        "fields": {},
    }

    for field in fields:
        columns = member_columns(hourly, field)
        member_ids = sorted(columns)
        complete_members = []
        failures = {}
        for member in range(20):
            values = columns.get(member)
            if values is None:
                failures[str(member)] = "column_missing"
                continue
            bad = []
            for valid_time in required:
                position = index[valid_time]
                value = values[position] if position < len(values) else None
                if (
                    not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(float(value))
                ):
                    bad.append(valid_time)
            if bad:
                failures[str(member)] = (
                    f"nonfinite_or_missing:{len(bad)}"
                )
            else:
                complete_members.append(member)
        report["fields"][field] = {
            "member_ids": member_ids,
            "member_count": len(member_ids),
            "complete_0_48_member_ids": complete_members,
            "complete_0_48_member_count": len(complete_members),
            "member_failures": failures,
            "unit": (payload.get("hourly_units") or {}).get(field),
            "same_final_response": True,
        }

    print(
        "PHASE2F4_PROBE="
        + json.dumps(report, sort_keys=True, separators=(",", ":"))
    )
    print("\nCapability:")
    for field in CANDIDATES:
        entry = capability[field]
        print(
            f"{field}: HTTP={entry['http_status']} "
            f"accepted={entry['accepted']} error={entry['error']}"
        )
    print("\nFinal combined response:")
    for field, entry in report["fields"].items():
        print(
            f"{field}: columns={entry['member_count']} "
            f"complete_0_48={entry['complete_0_48_member_count']} "
            f"unit={entry['unit']} failures={entry['member_failures']}"
        )

    with open("/tmp/phase2f4_probe.json", "w", encoding="utf-8") as handle:
        json.dump(report, handle, sort_keys=True, indent=2)


if __name__ == "__main__":
    main()
