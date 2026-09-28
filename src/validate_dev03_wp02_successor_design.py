#!/usr/bin/env python3
"""Executable contract gate for DEV03-WP02 successor design."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "config" / "dev03_wp02_l1_l2_successor_design_v1.json"

EXPECTED_SUCCESSORS = {
    "relevant-meteorology-v3",
    "weather-acquisition-plan-v2",
    "collector-model-bundle-v3",
    "model-field-availability-v3",
    "forecast-lead-identity-v1",
    "native-interval-resolution-v1",
    "canonical-model-record-v3",
    "field-availability-v3",
    "ensemble-member-storage-contract-v3",
    "mardorf-weather-archive-v6",
    "l2-l3-meteorology-read-contract-v1",
}
EXPECTED_FINDINGS = {
    "AUD-20260928-01-F02",
    "AUD-20260928-01-F03",
    "AUD-20260928-01-F04",
    "AUD-20260928-01-F05",
}
EXPECTED_VIEWS = {
    "l3_forecast_records_v1",
    "l3_field_values_v1",
    "l3_field_availability_v1",
    "l3_ensemble_members_v1",
    "l3_ensemble_aggregates_v1",
    "l3_ensemble_field_status_v1",
}


def load(path=CONTRACT):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _problem(condition, message, problems):
    if not condition:
        problems.append(message)


def validate_contract(contract):
    problems = []
    _problem(contract.get("contract_version") == "dev03-wp02-l1-l2-successor-design-v1",
             "contract version drift", problems)
    _problem(contract.get("development_phase") == "DEV-03", "development phase drift", problems)
    _problem(contract.get("work_package") == "DEV03-WP02", "work package drift", problems)
    _problem(contract.get("status") in {"candidate", "complete"}, "invalid lifecycle status", problems)

    scope = contract.get("scope") or {}
    _problem(set(scope.get("in_scope_findings") or []) == EXPECTED_FINDINGS,
             "F02-F05 scope must be exact", problems)

    successors = contract.get("successor_set") or []
    ids = [x.get("id") for x in successors]
    _problem(len(ids) == 11, f"expected 11 successors, got {len(ids)}", problems)
    _problem(len(ids) == len(set(ids)), "duplicate successor id", problems)
    _problem(set(ids) == EXPECTED_SUCCESSORS, "successor set drift", problems)
    exit_gate = contract.get("wp02_exit_gate") or {}
    _problem(exit_gate.get("exact_successor_count") == 11, "exit-gate successor count drift", problems)
    _problem(set(exit_gate.get("exact_successor_ids") or []) == EXPECTED_SUCCESSORS,
             "exit-gate successor ids drift", problems)

    f02 = contract.get("f02_registry_v3") or {}
    sem = f02.get("new_semantic") or {}
    _problem(sem.get("semantic_id") == "convective_precipitation", "F02 semantic drift", problems)
    _problem(sem.get("time_semantics") == "provider_native_accumulation",
             "convective precipitation must remain an accumulation semantic", problems)
    _problem(sem.get("derived_event_truth") is False, "model semantic cannot claim event truth", problems)
    providers = f02.get("provider_matrix") or {}
    _problem((providers.get("ICON-D2") or {}).get("parameter_native") == "rain_con",
             "ICON-D2 rain_con mapping missing", problems)
    _problem((providers.get("ICON-EU") or {}).get("parameter_native") == "rain_con",
             "ICON-EU rain_con mapping missing", problems)
    _problem((providers.get("GFS") or {}).get("parameter_native") == "ACPCP",
             "GFS ACPCP mapping missing", problems)
    _problem((providers.get("GFS") or {}).get("acquisition_policy_v2") == "required",
             "GFS convective precipitation must be required in initial acquisition v2", problems)
    _problem((providers.get("ICON-D2-EPS") or {}).get("acquisition_policy_v2") == "not_requested_by_policy",
             "ICON-D2-EPS initial resource-gated policy drift", problems)
    _problem("unsupported" in str((providers.get("ECMWF-IFS") or {}).get("provider_capability")),
             "ECMWF-IFS current unsupported status missing", problems)
    _problem("unsupported" in str((providers.get("GEFS-control") or {}).get("provider_capability")),
             "GEFS-control current unsupported status missing", problems)
    _problem("unsupported" in str((providers.get("NOAA_GEFS_FULL") or {}).get("provider_capability")),
             "full GEFS current unsupported status missing", problems)

    f03 = contract.get("f03_interval_resolution_v1") or {}
    _problem(f03.get("authority_field") == "interval_seconds", "interval_seconds must be authoritative", problems)
    _problem(f03.get("authority_type") == "int64", "interval authority must be int64", problems)
    _problem(set(f03.get("statuses") or []) == {"resolved", "point", "unknown", "ambiguous", "invalid"},
             "interval status family drift", problems)
    codes = set((f03.get("step_unit_contract") or {}).get("numeric_codes_supported") or {})
    _problem({"0", "1", "2", "10", "11", "12", "13"}.issubset(codes),
             "central resolver lacks required GRIB step-unit codes", problems)
    forbidden = " ".join(f03.get("forbidden_inference") or []).lower()
    _problem("forecast lead" in forbidden and "unitless steprange" in forbidden,
             "fail-closed interval inference rules incomplete", problems)

    f04 = contract.get("f04_l2_l3_read_contract_v1") or {}
    _problem(set(f04.get("required_logical_views") or []) == EXPECTED_VIEWS,
             "L2->L3 logical view set drift", problems)
    record = set(f04.get("required_record_identity") or [])
    _problem({"revision_event_id", "run_time_utc", "valid_time_utc", "lead_seconds",
              "provider_product", "source_archive_version"}.issubset(record),
             "L2->L3 record identity incomplete", problems)
    field = set(f04.get("required_field_identity") or [])
    _problem({"registry_version", "semantic_id", "field_identity_id", "parameter_native",
              "unit_native", "type_of_level_native", "level_native", "step_type_native",
              "step_range_native", "start_step_native", "end_step_native", "step_units_native",
              "interval_status", "interval_start_utc", "interval_end_utc", "interval_seconds",
              "identity_completeness_status", "source_provenance"}.issubset(field),
             "L2->L3 field identity incomplete", problems)
    _problem("one as_of=T" in str(f04.get("causal_read_rule")),
             "single-as_of read rule missing", problems)
    _problem("reject legacy_partial" in str(f04.get("primary_input_rule")),
             "legacy partial identity must fail closed for sensitive analyzers", problems)

    f05 = contract.get("f05_forecast_lead_identity_v1") or {}
    _problem((f05.get("l1_fields") or {}).get("forecast_lead_hours", {}).get("type") == "JSON integer",
             "forecast_lead_hours successor type drift", problems)
    _problem((f05.get("l1_fields") or {}).get("forecast_lead_seconds", {}).get("type") == "JSON integer",
             "forecast_lead_seconds successor type drift", problems)
    _problem((f05.get("l2_authority") or {}).get("field") == "lead_seconds",
             "L2 lead authority must be lead_seconds", problems)
    _problem((f05.get("l2_authority") or {}).get("type") == "int64",
             "L2 lead authority must be int64", problems)
    _problem("Never use int() truncation" in str(f05.get("legacy_adapter_rule")),
             "legacy lead truncation prohibition missing", problems)

    migration = contract.get("migration_topology") or {}
    _problem((migration.get("persistence") or {}).get("no_history_rewrite") is True,
             "history rewrite must remain forbidden", problems)
    _problem((migration.get("persistence") or {}).get("no_first_seen_backdating") is True,
             "first_seen backdating must remain forbidden", problems)
    _problem((migration.get("public_collection") or {}).get("legacy_channel") ==
             "collector-model-bundle-v2 remains operational and unchanged",
             "legacy public path must remain unchanged", problems)
    _problem((migration.get("operational_boundary") or {}).get("l4_authority") == "v16-c3-v9",
             "L4 authority drift", problems)
    _problem((migration.get("operational_boundary") or {}).get("dev03_cutover_allowed") is False,
             "DEV-03 must not authorize L4/L5 cutover", problems)

    if problems:
        raise AssertionError("DEV03-WP02 successor design gate failed:\n- " + "\n- ".join(problems))
    return {
        "status": "PASS",
        "contract_version": contract["contract_version"],
        "successor_count": len(ids),
        "findings": sorted(EXPECTED_FINDINGS),
        "logical_view_count": len(EXPECTED_VIEWS),
        "operational_l4_l5_changed": False,
    }


def fetch_remote_contract(repo):
    url = f"https://api.github.com/repos/{repo}/contents/config/dev03_wp02_l1_l2_successor_design_v1.json?ref=main"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "mardorf-dev03-wp02-validator",
    }
    token = os.getenv("GH_READ_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        payload = json.load(response)
    if payload.get("encoding") != "base64":
        raise AssertionError("unexpected GitHub contents encoding")
    return base64.b64decode(payload["content"])


def validate_remote_identity(local_bytes, remote_repo):
    remote = fetch_remote_contract(remote_repo)
    if local_bytes != remote:
        raise AssertionError(
            "DEV03-WP02 contract differs across repositories: "
            f"local_sha256={hashlib.sha256(local_bytes).hexdigest()} "
            f"remote_sha256={hashlib.sha256(remote).hexdigest()}"
        )
    return {
        "remote_repo": remote_repo,
        "byte_identical": True,
        "sha256": hashlib.sha256(local_bytes).hexdigest(),
    }


def validate_repository(root=ROOT, remote_repo=None):
    path = Path(root) / "config" / "dev03_wp02_l1_l2_successor_design_v1.json"
    local_bytes = path.read_bytes()
    report = validate_contract(json.loads(local_bytes.decode("utf-8")))
    if remote_repo:
        report["cross_repo"] = validate_remote_identity(local_bytes, remote_repo)
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=ROOT)
    ap.add_argument("--remote-repo")
    args = ap.parse_args()
    print(json.dumps(validate_repository(args.root, args.remote_repo), sort_keys=True))


if __name__ == "__main__":
    main()
