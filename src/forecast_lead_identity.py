#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone


class ForecastLeadIdentityError(ValueError):
    pass


def _strict_non_negative_int(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ForecastLeadIdentityError(f"{field} must be a strict integer")
    if value < 0:
        raise ForecastLeadIdentityError(f"{field} must be non-negative")
    return value


def validate_acquisition_lead_hours(value) -> int:
    return _strict_non_negative_int(value, "acquisition_lead_hours")


def lead_seconds_from_hours(value) -> int:
    hours = validate_acquisition_lead_hours(value)
    return hours * 3600


def validate_lead_seconds(value) -> int:
    return _strict_non_negative_int(value, "lead_seconds")


def _parse_utc(value, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ForecastLeadIdentityError(f"{field} must be a non-empty RFC3339 UTC string")
    raw = value
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ForecastLeadIdentityError(f"{field} is not valid RFC3339/ISO-8601") from exc
    if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise ForecastLeadIdentityError(f"{field} must be explicitly UTC")
    return dt.astimezone(timezone.utc)


def validate_forecast_lead_identity(
    *,
    run_time_utc,
    valid_time_utc,
    lead_seconds,
    acquisition_lead_hours=None,
):
    seconds = validate_lead_seconds(lead_seconds)
    run = _parse_utc(run_time_utc, "run_time_utc")
    valid = _parse_utc(valid_time_utc, "valid_time_utc")
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
