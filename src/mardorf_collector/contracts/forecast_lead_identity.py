#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
import re


class ForecastLeadIdentityError(ValueError):
    pass


INT64_MAX = (1 << 63) - 1
RFC3339_UTC_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|\+00:00)$"
)


def _strict_non_negative_int(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ForecastLeadIdentityError(f"{field} must be a strict integer")
    if value < 0:
        raise ForecastLeadIdentityError(f"{field} must be non-negative")
    if value > INT64_MAX:
        raise ForecastLeadIdentityError(f"{field} must fit signed int64")
    return value


def validate_acquisition_lead_hours(value) -> int:
    return _strict_non_negative_int(value, "acquisition_lead_hours")


def lead_seconds_from_hours(value) -> int:
    hours = validate_acquisition_lead_hours(value)
    seconds = hours * 3600
    if seconds > INT64_MAX:
        raise ForecastLeadIdentityError("acquisition_lead_hours overflows lead_seconds int64")
    return seconds


def validate_lead_seconds(value) -> int:
    return _strict_non_negative_int(value, "lead_seconds")


def ensure_microsecond_precision(value, field: str = "timestamp") -> None:
    """Reject precision datetime/Arrow would silently lose; exact trailing zeros are safe."""
    if isinstance(value, str):
        for fraction in re.findall(r"[.,](\d+)", value):
            if len(fraction) > 6 and any(digit != "0" for digit in fraction[6:]):
                raise ForecastLeadIdentityError(f"{field} exceeds exact microsecond precision")


def parse_utc_timestamp(value, field: str) -> datetime:
    if not isinstance(value, str) or not RFC3339_UTC_RE.fullmatch(value):
        raise ForecastLeadIdentityError(
            f"{field} must be an RFC3339 UTC timestamp using T and Z/+00:00"
        )
    ensure_microsecond_precision(value, field)
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ForecastLeadIdentityError(f"{field} is not a valid RFC3339 UTC timestamp") from exc
    if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise ForecastLeadIdentityError(f"{field} must be explicitly UTC")
    return dt.astimezone(timezone.utc)


def canonical_utc_timestamp(value, field: str = "timestamp") -> str:
    return parse_utc_timestamp(value, field).isoformat().replace("+00:00", "Z")


def validate_forecast_lead_identity(
    *,
    run_time_utc,
    valid_time_utc,
    lead_seconds,
    acquisition_lead_hours=None,
):
    seconds = validate_lead_seconds(lead_seconds)
    run = parse_utc_timestamp(run_time_utc, "run_time_utc")
    valid = parse_utc_timestamp(valid_time_utc, "valid_time_utc")
    delta = valid - run
    if delta.total_seconds() < 0:
        raise ForecastLeadIdentityError("valid_time_utc must not precede run_time_utc")
    if delta.microseconds != 0:
        raise ForecastLeadIdentityError("forecast lead duration must be an exact whole number of seconds")
    exact_seconds = delta.days * 86400 + delta.seconds
    if exact_seconds != seconds:
        raise ForecastLeadIdentityError(
            f"lead_seconds mismatch: timestamps imply {exact_seconds}, got {seconds}"
        )
    hours = None
    if acquisition_lead_hours is not None:
        hours = validate_acquisition_lead_hours(acquisition_lead_hours)
        if hours * 3600 != seconds:
            raise ForecastLeadIdentityError(
                "acquisition_lead_hours and lead_seconds are contradictory"
            )
    return {
        "run_time_utc": run.isoformat().replace("+00:00", "Z"),
        "valid_time_utc": valid.isoformat().replace("+00:00", "Z"),
        "lead_seconds": seconds,
        "acquisition_lead_hours": hours,
    }
