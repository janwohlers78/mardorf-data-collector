#!/usr/bin/env python3
"""DEV03-WP03-I04 exact-parent convective-precipitation acquisition.

The successor path is deliberately isolated and is not wired into routine
collection in I04.  Every provider request is planned only after exact v2
parent payload verification and exact parent-cycle binding.  No newest-cycle
selection and no hidden HTTP retry are allowed.
"""
from __future__ import annotations

import bz2
import hashlib
import json
import math
import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

import availability_contract_v3 as availability_v3
import fetch_model_data as base
import icon_parameter_probe as icon_probe
import noaa_weather_context as noaa
from dev03_v3_parent_binding import (
    BUNDLE_METHOD,
    Dev03ParentBindingError,
    build_parent_cycle_inventory,
    collection_transaction_id,
    load_registry_v3,
    load_verified_parent_payload,
)
from forecast_lead_identity import canonical_utc_timestamp
from grib_identity import assert_grib_valid_time

ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "config" / "weather_acquisition_plan_v2.json"
SEMANTIC_ID = "convective_precipitation"
DWD_RAIN_CON_GRIB2_IDENTITY = {
    "discipline": 0,
    "parameterCategory": 1,
    "parameterNumber": 76,
}


class Dev03I04Error(ValueError):
    pass


class BudgetExceeded(Dev03I04Error):
    pass


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_nonnegative_int(value, field):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise Dev03I04Error(f"{field} must be a non-negative integer")
    return value


def _git_safe_copy(value):
    return json.loads(json.dumps(value, allow_nan=False))


def load_plan(path: str | Path | None = None) -> dict:
    p = Path(path) if path is not None else PLAN_PATH
    plan = json.loads(p.read_text(encoding="utf-8"))
    if plan.get("plan_version") != "weather-acquisition-plan-v2":
        raise Dev03I04Error("unexpected acquisition plan version")
    return plan


class BudgetLedger:
    """Daily I04 budget ledger.

    State is supplied by the caller so later workflow wiring can persist it in
    the receipt/control plane.  I04 itself does not create a new scheduler or
    mutable production pointer.
    """

    def __init__(self, state: dict, *, plan: dict | None = None):
        if not isinstance(state, dict):
            raise Dev03I04Error("budget_state must be an object")
        self.plan = plan or load_plan()
        limits = self.plan["resource_budget"]
        self.request_hard_max = _require_nonnegative_int(
            limits["daily_incremental_request_hard_max"],
            "daily_incremental_request_hard_max",
        )
        ratio = limits["deterministic_network_hard_incremental_ratio_max"]
        if isinstance(ratio, bool) or not isinstance(ratio, (int, float)) or not 0 < float(ratio) <= 1:
            raise Dev03I04Error("invalid deterministic network hard ratio")
        self.hard_ratio = float(ratio)
        day = state.get("utc_day")
        if not isinstance(day, str):
            raise Dev03I04Error("budget_state.utc_day is required")
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError as exc:
            raise Dev03I04Error("budget_state.utc_day must be YYYY-MM-DD") from exc
        self.utc_day = day
        self.requests_used = _require_nonnegative_int(state.get("requests_used"), "requests_used")
        self.response_bytes_used = _require_nonnegative_int(
            state.get("response_bytes_used"), "response_bytes_used"
        )
        self.retry_requests_used = _require_nonnegative_int(
            state.get("retry_requests_used", 0), "retry_requests_used"
        )
        self.matched_v2_response_bytes = _require_nonnegative_int(
            state.get("matched_v2_response_bytes"), "matched_v2_response_bytes"
        )
        if self.matched_v2_response_bytes <= 0:
            raise Dev03I04Error("matched_v2_response_bytes must be measured and > 0")
        self.byte_hard_max = math.floor(self.matched_v2_response_bytes * self.hard_ratio)
        if self.byte_hard_max <= 0:
            raise Dev03I04Error("matched v2 baseline is too small for a positive hard byte budget")
        self.hard_breach = bool(
            self.requests_used > self.request_hard_max
            or self.response_bytes_used > self.byte_hard_max
        )
        if self.hard_breach:
            raise BudgetExceeded("supplied budget state already exceeds an I04 hard budget")

    def reserve_request(self, *, attempt_kind: str):
        if attempt_kind not in {"initial", "retry"}:
            raise Dev03I04Error("attempt_kind must be initial or retry")
        if self.requests_used >= self.request_hard_max:
            self.hard_breach = True
            raise BudgetExceeded("daily incremental provider-request budget exhausted")
        if self.response_bytes_used >= self.byte_hard_max:
            self.hard_breach = True
            raise BudgetExceeded("daily deterministic-network byte budget exhausted")
        self.requests_used += 1
        if attempt_kind == "retry":
            self.retry_requests_used += 1

    def record_response_bytes(self, count: int):
        count = _require_nonnegative_int(count, "response byte count")
        self.response_bytes_used += count
        if self.response_bytes_used > self.byte_hard_max:
            self.hard_breach = True
            raise BudgetExceeded(
                "deterministic-network hard byte budget exceeded by provider response"
            )

    def snapshot(self) -> dict:
        return {
            "budget_version": self.plan["resource_budget"]["budget_version"],
            "utc_day": self.utc_day,
            "requests_used": self.requests_used,
            "retry_requests_used": self.retry_requests_used,
            "request_hard_max": self.request_hard_max,
            "response_bytes_used": self.response_bytes_used,
            "matched_v2_response_bytes": self.matched_v2_response_bytes,
            "response_byte_hard_max": self.byte_hard_max,
            "incremental_byte_ratio": self.response_bytes_used / self.matched_v2_response_bytes,
            "hard_breach": self.hard_breach,
        }


def _parse_utc(value: str) -> datetime:
    canonical = canonical_utc_timestamp(value, "provider cycle timestamp")
    return datetime.fromisoformat(canonical.replace("Z", "+00:00"))


def _coordinate(grid_identity: dict) -> dict:
    point = (grid_identity or {}).get("extraction_coordinate")
    if not isinstance(point, dict):
        raise Dev03I04Error(
            "I04 provider extraction requires exact parent extraction-coordinate identity"
        )
    lat = point.get("latitude")
    lon = point.get("longitude")
    if (
        isinstance(lat, bool)
        or isinstance(lon, bool)
        or not isinstance(lat, (int, float))
        or not isinstance(lon, (int, float))
        or not math.isfinite(float(lat))
        or not math.isfinite(float(lon))
    ):
        raise Dev03I04Error("parent extraction coordinate is not finite numeric identity")
    return {"latitude": float(lat), "longitude": float(lon), **(
        {"selection": point["selection"]}
        if isinstance(point.get("selection"), str) and point.get("selection")
        else {}
    )}


def _request_url(item: dict, provider: dict) -> str:
    run = _parse_utc(item["run_time_utc"])
    seconds = item["forecast_lead_seconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, int) or seconds < 0 or seconds % 3600:
        raise Dev03I04Error("I04 requires integral-hour parent lead_seconds")
    lead = seconds // 3600
    cycle = run.strftime("%Y%m%d%H")
    hh = run.strftime("%H")
    if item["model"] in {"ICON-D2", "ICON-EU"}:
        return provider["url_template"].format(
            cycle_hour=hh, cycle_yyyymmddhh=cycle, lead_3=f"{lead:03d}"
        )
    if item["model"] == "GFS":
        point = _coordinate(item["grid_identity"])
        lat, lon = point["latitude"], point["longitude"]
        query = {
            "file": f"gfs.t{hh}z.pgrb2.0p25.f{lead:03d}",
            "lev_surface": "on",
            "var_ACPCP": "on",
            "subregion": "",
            "leftlon": f"{lon - 0.3:.3f}",
            "rightlon": f"{lon + 0.3:.3f}",
            "toplat": f"{lat + 0.3:.3f}",
            "bottomlat": f"{lat - 0.3:.3f}",
            "dir": f"/gfs.{run:%Y%m%d}/{hh}/atmos",
        }
        return provider["endpoint"] + "?" + urlencode(query)
    raise Dev03I04Error(f"unsupported I04 provider path {item['model']!r}")


def plan_parent_pinned_requests(
    parent_payload_bytes: bytes,
    parent_binding: dict,
    *,
    plan: dict | None = None,
    registry: dict | None = None,
) -> tuple[list[dict], str]:
    """Verify exact parent bytes first, then build the only legal I04 requests."""
    plan = plan or load_plan()
    registry = registry or load_registry_v3()
    parent = load_verified_parent_payload(parent_payload_bytes, parent_binding)
    inventory, parent_cycle_binding_id = build_parent_cycle_inventory(parent, registry)
    allowed = plan["successor_acquisition"]["allowed_initial_provider_paths"]
    jobs = []
    for item in inventory:
        model = item["model"]
        if model not in allowed:
            continue
        provider = allowed[model]
        if item["provider_product"] != provider["parent_provider_product"]:
            raise Dev03I04Error(
                f"{model} parent provider_product mismatch: "
                f"{item['provider_product']!r} != {provider['parent_provider_product']!r}"
            )
        point = _coordinate(item["grid_identity"])
        job = {
            **_git_safe_copy(item),
            "grid_identity": {"extraction_coordinate": point},
            "semantic_id": SEMANTIC_ID,
            "parameter_native": provider["parameter_native"],
            "field_provider_product": provider["field_provider_product"],
        }
        if model in {"ICON-D2", "ICON-EU"}:
            job["expected_grib2_identity"] = _git_safe_copy(
                DWD_RAIN_CON_GRIB2_IDENTITY
            )
        job["source_url"] = _request_url(job, provider)
        if model == "GFS" and ("CPRAT" in job["source_url"] or "var_CPRAT" in job["source_url"]):
            raise Dev03I04Error("GFS CPRAT rate alias is forbidden for convective precipitation")
        jobs.append(job)
    return jobs, parent_cycle_binding_id


def _same_point(expected: dict, observed_lat, observed_lon) -> bool:
    return (
        abs(float(expected["latitude"]) - float(observed_lat)) <= 1e-9
        and abs(float(expected["longitude"]) - float(observed_lon)) <= 1e-9
    )


def _native_metadata(meta: dict) -> dict:
    keys = (
        "shortName", "name", "paramId", "discipline", "parameterCategory",
        "parameterNumber", "units", "typeOfLevel", "level",
        "stepType", "startStep", "endStep", "stepUnits", "stepRange",
    )
    return {k: meta[k] for k in keys if k in meta}


def _native_int(value):
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dwd_convective_identity_matches(meta: dict, job: dict) -> bool:
    """Match DWD RAIN_CON by provider-native GRIB2 numeric identity.

    ecCodes shortName is retained as metadata but is not authoritative here:
    different definition-table installations can render the same DWD field with
    a different/unknown shortName. The exact pinned rain_con URL plus the GRIB2
    discipline/category/number triple is the stable provider-native identity.
    """
    expected = job.get("expected_grib2_identity") or {}
    required = ("discipline", "parameterCategory", "parameterNumber")
    if any(_native_int(expected.get(k)) is None for k in required):
        raise Dev03I04Error("DWD convective job lacks exact expected GRIB2 identity")
    return all(_native_int(meta.get(k)) == _native_int(expected[k]) for k in required)


def _received_evidence(job: dict, raw: bytes, observed_at_utc: str) -> list[dict]:
    run = _parse_utc(job["run_time_utc"])
    valid = _parse_utc(job["valid_time_utc"])
    point = _coordinate(job["grid_identity"])
    source_sha = hashlib.sha256(raw).hexdigest()
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        if job["model"] in {"ICON-D2", "ICON-EU"}:
            packed = root / "field.grib2.bz2"
            grib = root / "field.grib2"
            selected = root / "selected.grib2"
            packed.write_bytes(raw)
            try:
                grib.write_bytes(bz2.decompress(raw))
            except Exception as exc:
                raise Dev03I04Error("DWD response is not valid bzip2 GRIB2") from exc
            icon_probe.select_exact_message(grib, selected, run, valid)
            meta = icon_probe.message_metadata(selected)
            nearest = base.grib_nearest(selected)
        else:
            grib = root / "field.grib2"
            grib.write_bytes(raw)
            if raw[:4] != b"GRIB":
                raise Dev03I04Error("NOAA response is not GRIB")
            assert_grib_valid_time(grib, run, valid, "DEV03 I04 GFS ACPCP")
            meta = noaa._metadata(grib)
            nearest_rows, selected_point = noaa._nearest(
                grib, point["latitude"], point["longitude"]
            )
            nearest = [
                {
                    "shortName": row["shortName"],
                    "stepRange": row["stepRange"],
                    "value": row["value"],
                    "lat": selected_point["latitude"],
                    "lon": selected_point["longitude"],
                }
                for row in nearest_rows
            ]

    if len(meta) != len(nearest):
        raise Dev03I04Error("native metadata/value row count mismatch")

    out = []
    for m, p in zip(meta, nearest):
        native = str(m.get("shortName") or p.get("shortName") or "")
        if job["model"] in {"ICON-D2", "ICON-EU"}:
            matches = _dwd_convective_identity_matches(m, job)
        else:
            label = str(m.get("name") or "").lower()
            matches = native.lower() == "acpcp" or (
                "convective precipitation" in label and "rate" not in label
            )
            if native.lower() == "cprat" or "rate" in label:
                matches = False
        if not matches:
            continue
        if not _same_point(point, p["lat"], p["lon"]):
            raise Dev03I04Error("provider extraction grid point differs from exact parent identity")
        value = p["value"]
        declaration = availability_v3.make_declaration(
            model=job["model"],
            semantic_id=SEMANTIC_ID,
            observed_at_utc=observed_at_utc,
            runtime_status="received",
            value=value,
            field_available_at_utc=observed_at_utc,
            evidence_type="dev03_i04_exact_parent_cycle_provider_fetch",
            parameter_native=job["parameter_native"],
            field_provider_product=job["field_provider_product"],
        )
        out.append({
            "model": job["model"],
            "semantic_id": SEMANTIC_ID,
            "run_time_utc": job["run_time_utc"],
            "valid_time_utc": job["valid_time_utc"],
            "forecast_lead_seconds": job["forecast_lead_seconds"],
            "parent_provider_product": job["provider_product"],
            "parameter_native": job["parameter_native"],
            "field_provider_product": job["field_provider_product"],
            "grid_identity": _git_safe_copy(job["grid_identity"]),
            "native_metadata": _native_metadata(m),
            "source_url": job["source_url"],
            "source_sha256": source_sha,
            "response_bytes": len(raw),
            "availability": declaration,
        })
    if not out:
        raise Dev03I04Error(
            f"{job['model']} response contained no exact {job['parameter_native']} native field"
        )
    return out


def _failure_evidence(job: dict, *, status: str, observed_at_utc: str, reason: str, response_bytes=0):
    declaration = availability_v3.make_declaration(
        model=job["model"],
        semantic_id=SEMANTIC_ID,
        observed_at_utc=observed_at_utc,
        runtime_status=status,
        evidence_type="dev03_i04_exact_parent_cycle_provider_fetch",
        reason=reason,
        parameter_native=job["parameter_native"],
        field_provider_product=job["field_provider_product"],
    )
    return {
        "model": job["model"],
        "semantic_id": SEMANTIC_ID,
        "run_time_utc": job["run_time_utc"],
        "valid_time_utc": job["valid_time_utc"],
        "forecast_lead_seconds": job["forecast_lead_seconds"],
        "parent_provider_product": job["provider_product"],
        "parameter_native": job["parameter_native"],
        "field_provider_product": job["field_provider_product"],
        "grid_identity": _git_safe_copy(job["grid_identity"]),
        "source_url": job["source_url"],
        "response_bytes": int(response_bytes),
        "availability": declaration,
    }


def _read_budgeted_response(response, ledger: BudgetLedger) -> bytes:
    """Read a measured 200 response only when its declared body fits the hard budget."""
    remaining = ledger.byte_hard_max - ledger.response_bytes_used
    if remaining <= 0:
        ledger.hard_breach = True
        response.close()
        raise BudgetExceeded("daily deterministic-network byte budget exhausted")

    header = response.headers.get("Content-Length") if hasattr(response, "headers") else None
    if header in (None, ""):
        response.close()
        raise Dev03I04Error(
            "provider response lacks Content-Length required for fail-closed byte budgeting"
        )
    try:
        declared = int(header)
    except (TypeError, ValueError) as exc:
        response.close()
        raise Dev03I04Error("provider Content-Length is not an integer") from exc
    if declared < 0:
        response.close()
        raise Dev03I04Error("provider Content-Length is negative")
    if declared > remaining:
        ledger.hard_breach = True
        response.close()
        raise BudgetExceeded(
            "provider Content-Length exceeds remaining deterministic-network byte budget"
        )

    raw = bytearray()
    try:
        for chunk in response.iter_content(chunk_size=min(65536, max(1, declared or 1))):
            if chunk:
                raw.extend(chunk)
                if len(raw) > declared:
                    raise Dev03I04Error("provider response exceeds declared Content-Length")
    finally:
        response.close()
    if len(raw) != declared:
        raise Dev03I04Error(
            f"provider response length mismatch: declared={declared} received={len(raw)}"
        )
    ledger.record_response_bytes(len(raw))
    return bytes(raw)

def _request_once(session, job: dict, ledger: BudgetLedger, *, attempt_kind: str, timeout: int):
    ledger.reserve_request(attempt_kind=attempt_kind)
    try:
        response = session.get(
            job["source_url"], timeout=timeout, allow_redirects=False, stream=True
        )
    except Exception as exc:
        return None, "fetch_error", f"{type(exc).__name__}: {exc}"
    if response.status_code in {404, 410}:
        response.close()
        return b"", "not_yet_published", f"HTTP {response.status_code} exact parent-cycle field unavailable"
    if response.status_code != 200:
        response.close()
        return b"", "fetch_error", f"HTTP {response.status_code} from exact parent-cycle field request"
    try:
        raw = _read_budgeted_response(response, ledger)
    except Dev03I04Error as exc:
        if isinstance(exc, BudgetExceeded):
            raise
        return b"", "fetch_error", f"budget_measurement_failed: {exc}"
    return raw, None, None


def acquire_convective_successor(
    *,
    parent_payload_bytes: bytes,
    parent_binding: dict,
    bundle_shell: dict,
    budget_state: dict,
    attempt_kind: str = "initial",
    session=None,
    observed_at_utc: str | None = None,
    plan: dict | None = None,
) -> tuple[dict, dict]:
    """Populate the isolated v3 shell with exact-parent convective evidence."""
    plan = plan or load_plan()
    observed = canonical_utc_timestamp(
        observed_at_utc or _now_utc(), "availability_observed_at_utc"
    )
    if observed[:10] != budget_state.get("utc_day"):
        raise Dev03I04Error("budget_state.utc_day must match acquisition observation UTC day")
    if not isinstance(bundle_shell, dict) or bundle_shell.get("method_version") != BUNDLE_METHOD:
        raise Dev03I04Error("verified collector-model-bundle-v3 shell required")
    expected_tx = collection_transaction_id(parent_binding)
    if bundle_shell.get("collection_transaction_id") != expected_tx:
        raise Dev03I04Error("bundle shell transaction does not match verified parent binding")
    jobs, parent_cycle_binding_id = plan_parent_pinned_requests(
        parent_payload_bytes, parent_binding, plan=plan
    )
    if bundle_shell.get("parent_cycle_binding_id") != parent_cycle_binding_id:
        raise Dev03I04Error("bundle shell parent_cycle_binding_id does not match exact parent inventory")

    ledger = BudgetLedger(budget_state, plan=plan)
    before_requests = ledger.requests_used
    before_bytes = ledger.response_bytes_used
    evidence = []
    deferred = []
    session = session or requests.Session()
    timeout = int(plan["request_policy"]["request_timeout_seconds"])

    for job in jobs:
        try:
            raw, failure_status, reason = _request_once(
                session, job, ledger, attempt_kind=attempt_kind, timeout=timeout
            )
        except BudgetExceeded as exc:
            deferred.append({
                "model": job["model"],
                "run_time_utc": job["run_time_utc"],
                "valid_time_utc": job["valid_time_utc"],
                "forecast_lead_seconds": job["forecast_lead_seconds"],
                "status": "retry_deferred_budget_exhausted"
                if attempt_kind == "retry" else "request_deferred_budget_exhausted",
                "reason": str(exc),
            })
            break

        now = canonical_utc_timestamp(_now_utc(), "provider response observed time")
        if failure_status is not None:
            evidence.append(_failure_evidence(
                job, status=failure_status, observed_at_utc=now,
                reason=reason, response_bytes=len(raw or b""),
            ))
            continue
        try:
            evidence.extend(_received_evidence(job, raw, now))
        except Exception as exc:
            evidence.append(_failure_evidence(
                job, status="fetch_error", observed_at_utc=now,
                reason=f"native_identity_validation_failed: {type(exc).__name__}: {exc}",
                response_bytes=len(raw),
            ))

    out = deepcopy(bundle_shell)
    out["status"] = (
        "v3_convective_acquisition_budget_blocked"
        if deferred or ledger.hard_breach
        else "v3_convective_acquisition_attempt_complete"
    )
    out["provider_evidence"] = evidence
    out["network_requests_performed"] = ledger.requests_used - before_requests
    out["network_response_bytes"] = ledger.response_bytes_used - before_bytes
    out["i04_acquisition"] = {
        "method_version": "dev03-wp03-i04-parent-pinned-convective-v1",
        "plan_version": plan["plan_version"],
        "semantic_id": SEMANTIC_ID,
        "attempt_kind": attempt_kind,
        "observed_at_utc": observed,
        "planned_parent_occurrences": len(jobs),
        "evidence_records": len(evidence),
        "deferred": deferred,
        "retry_deferred_budget_exhausted": sum(
            x["status"] == "retry_deferred_budget_exhausted" for x in deferred
        ),
        "budget": ledger.snapshot(),
        "newest_cycle_selection_performed": False,
        "hidden_http_retries_performed": 0,
        "operational_authority": "v16-c3-v9",
    }
    return out, ledger.snapshot()
