#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check(ok, message, problems):
    if not ok:
        problems.append(message)


def git_blob_sha(path: Path) -> str:
    raw = path.read_bytes()
    return hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()


def main():
    problems = []
    plan = json.loads((ROOT / "config" / "weather_acquisition_plan_v2.json").read_text())
    check(plan.get("plan_version") == "weather-acquisition-plan-v2", "plan version drift", problems)
    check(plan.get("implementation_step") == "WP03-I04", "implementation step drift", problems)

    predecessor = plan.get("predecessor") or {}
    old_plan = ROOT / predecessor.get("path", "")
    check(old_plan.exists(), "frozen predecessor acquisition plan missing", problems)
    if old_plan.exists():
        check(
            git_blob_sha(old_plan) == predecessor.get("git_blob_sha"),
            "frozen Phase-2 acquisition plan mutated",
            problems,
        )
    registry = plan.get("registry") or {}
    registry_path = ROOT / registry.get("path", "")
    check(registry_path.exists(), "Registry v3 missing", problems)
    if registry_path.exists():
        check(
            git_blob_sha(registry_path) == registry.get("git_blob_sha"),
            "Registry v3 changed during I04",
            problems,
        )

    parent = plan.get("parent_binding") or {}
    check(parent.get("exact_parent_payload_sha_required_before_planning") is True,
          "exact parent payload SHA gate missing", problems)
    check(parent.get("newest_or_mature_cycle_selection_allowed") is False,
          "cycle reselection must remain forbidden", problems)
    check(parent.get("provider_product_inference_allowed") is False,
          "provider product inference must remain forbidden", problems)

    successor = plan.get("successor_acquisition") or {}
    allowed = successor.get("allowed_initial_provider_paths") or {}
    check(set(allowed) == {"ICON-D2", "ICON-EU", "GFS"},
          "I04 provider request scope drift", problems)
    zero = set(successor.get("zero_additional_request_paths") or [])
    check(zero == {"ECMWF-IFS", "GEFS-control", "NOAA_GEFS_FULL", "ICON-D2-EPS"},
          "zero-request provider scope drift", problems)
    expected = {
        "ICON-D2": ("icon-d2_regular-lat-lon", "rain_con", "DWD ICON-D2 Open Data raw GRIB2"),
        "ICON-EU": ("icon-eu_regular-lat-lon", "rain_con", "DWD ICON-EU Open Data raw GRIB2"),
        "GFS": ("gfs_0p25", "ACPCP", "gfs_0p25"),
    }
    for model, values in expected.items():
        item = allowed.get(model) or {}
        check(
            (item.get("parent_provider_product"), item.get("parameter_native"), item.get("field_provider_product")) == values,
            f"{model} exact product/native identity drift",
            problems,
        )
    check((allowed.get("GFS") or {}).get("explicitly_forbidden_alias") == "CPRAT",
          "GFS CPRAT exclusion missing", problems)

    budget = plan.get("resource_budget") or {}
    check(budget.get("daily_incremental_request_hard_max") == 500,
          "500/day hard request budget drift", problems)
    check(budget.get("daily_opportunity_derivation") == {"ICON-D2": 136, "ICON-EU": 200, "GFS": 164},
          "500/day opportunity derivation drift", problems)
    check(budget.get("deterministic_network_hard_incremental_ratio_max") == 0.25,
          "25% deterministic network hard ceiling drift", problems)
    check(budget.get("matched_v2_baseline_required") is True,
          "matched v2 byte baseline must be mandatory", problems)

    request_policy = plan.get("request_policy") or {}
    check(request_policy.get("hidden_http_retries_allowed") is False,
          "hidden HTTP retries must stay disabled", problems)
    check(request_policy.get("retry_requires_new_v3_attempt") is True,
          "retry must be a new causal v3 attempt", problems)

    source = (ROOT / "src" / "dev03_v3_convective_acquisition.py").read_text()
    check(source.index("load_verified_parent_payload(") < source.index("build_parent_cycle_inventory("),
          "exact parent payload verification must precede cycle inventory", problems)
    for token in ("discover_cycle(", "discover_gfs_cycle(", "latest_dwd_icon_d2_cycle(", "HTTPAdapter(", "Retry("):
        check(token not in source, f"I04 source contains forbidden reselection/retry primitive {token}", problems)
    check("allow_redirects=False" in source, "redirect-induced hidden provider requests are not disabled", problems)
    check("var_ACPCP" in source and "var_CPRAT" in source,
          "GFS ACPCP/CPRAT explicit guard missing", problems)
    check("retry_deferred_budget_exhausted" in source,
          "retry budget exhaustion metric missing", problems)
    check("hard_budget_breach_after_response" in source,
          "response byte hard-stop evidence missing", problems)

    d2 = (ROOT / "src" / "fetch_model_data.py").read_text()
    eu = (ROOT / "src" / "fetch_dwd_additional_models.py").read_text()
    check("'provider_product':'icon-d2_regular-lat-lon'" in d2,
          "I04-BLK01 ICON-D2 parent product identity missing at producer", problems)
    check("'provider_product':'icon-eu_regular-lat-lon'" in eu,
          "I04-BLK01 ICON-EU parent product identity missing at producer", problems)

    controls = json.loads((ROOT / "config" / "dev03_shadow_channels_v1.json").read_text())
    control_map = controls.get("controls") or controls.get("shadow_controls") or {}
    for name in (
        "public_v3_generation_enabled",
        "public_v3_transfer_enabled",
        "private_v3_promotion_enabled",
        "archive_v6_write_enabled",
    ):
        value = control_map.get(name)
        if isinstance(value, dict):
            value = value.get("enabled")
        check(value is False, f"shadow control {name} must remain false in I04", problems)

    boundary = plan.get("operational_boundary") or {}
    check(boundary.get("routine_wiring_enabled") is False,
          "I04 must not enable routine successor wiring", problems)
    check(boundary.get("operational_authority") == "v16-c3-v9",
          "operational authority drift", problems)

    if problems:
        print(json.dumps({"status": "FAIL", "implementation_step": "WP03-I04", "problems": problems}, indent=2))
        raise SystemExit(1)
    print(json.dumps({
        "status": "PASS",
        "implementation_step": "WP03-I04",
        "network_requests_executed_by_validator": 0,
        "routine_wiring_enabled": False,
        "next_step": "WP03-I05",
    }, indent=2))


if __name__ == "__main__":
    main()
