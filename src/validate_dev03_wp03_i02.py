#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import relevant_meteorology_registry_v2 as registry_v2
import relevant_meteorology_registry_v3 as registry_v3
import availability_contract_v3 as availability_v3

ROOT = Path(__file__).resolve().parents[1]


def validate_repository(root=ROOT):
    root = Path(root)
    problems = []
    def check(ok, msg):
        if not ok:
            problems.append(msg)

    v2 = json.loads((root / "config/relevant_meteorology_registry_v2.json").read_text(encoding="utf-8"))
    v3 = json.loads((root / "config/relevant_meteorology_registry_v3.json").read_text(encoding="utf-8"))
    try:
        registry_v2.validate_registry(v2)
    except Exception as exc:
        problems.append(f"Registry v2 predecessor no longer validates: {exc}")
    try:
        registry_v3.validate_registry(v3)
    except Exception as exc:
        problems.append(f"Registry v3 invalid: {exc}")

    v2_semantics = {x["semantic_id"] for x in v2["core_semantics"]}
    v3_semantics = {x["semantic_id"] for x in v3["core_semantics"]}
    check(v3_semantics == v2_semantics | {"convective_precipitation"}, "v3 must add exactly one model semantic")
    check(len(v2_semantics) == 15 and len(v3_semantics) == 16, "semantic counts drift")
    check(v3["predecessor"]["registry_sha256"] == v2["hashes"]["registry_sha256"], "v2 predecessor fingerprint drift")
    check(v3["operational_boundary"]["runtime_wiring_in_i02"] is False, "I02 runtime wiring forbidden")
    check(v3["operational_boundary"]["network_requests_authorized"] is False, "I02 network authorization forbidden")

    expected_states = {
        "received","unsupported_by_provider_or_product","intentionally_not_applicable",
        "not_requested_by_policy","not_yet_published","fetch_error","unknown_or_ambiguous",
    }
    check(availability_v3.STATES == expected_states, "Availability-v3 state family drift")

    observed = "2026-09-28T12:00:00Z"
    # Required/native without runtime evidence must not turn into false received.
    d = availability_v3.make_declaration(model="GFS", semantic_id="convective_precipitation", observed_at_utc=observed)
    check(d["availability_status"] == "unknown_or_ambiguous", "required field without runtime evidence must remain unknown")
    # Static unsupported beats disabled policy.
    d = availability_v3.make_declaration(model="ECMWF-IFS", semantic_id="convective_precipitation", observed_at_utc=observed)
    check(d["availability_status"] == "unsupported_by_provider_or_product", "unsupported capability precedence drift")
    # Native-capable but policy-disabled is distinct.
    d = availability_v3.make_declaration(model="ICON-D2-EPS", semantic_id="convective_precipitation", observed_at_utc=observed)
    check(d["availability_status"] == "not_requested_by_policy", "policy-disabled capability drift")
    # Only explicit runtime evidence may create received.
    d = availability_v3.make_declaration(
        model="GFS", semantic_id="convective_precipitation", observed_at_utc=observed,
        runtime_status="received", value=1.25, field_available_at_utc=observed,
        parameter_native="ACPCP", field_provider_product="gfs_0p25",
        evidence_type="provider_fetch_attempt",
    )
    try:
        availability_v3.validate_declaration(d)
    except Exception as exc:
        problems.append(f"valid received declaration rejected: {exc}")

    for status in ("not_yet_published","fetch_error","unknown_or_ambiguous"):
        try:
            d = availability_v3.make_declaration(
                model="GFS", semantic_id="convective_precipitation", observed_at_utc=observed,
                runtime_status=status, reason="synthetic I02 validation",
                evidence_type="provider_fetch_attempt", parameter_native="ACPCP", field_provider_product="gfs_0p25",
            )
            availability_v3.validate_declaration(d)
        except Exception as exc:
            problems.append(f"runtime state {status} rejected: {exc}")

    check(bool(d.get("availability_evidence_type")), "runtime state evidence type missing")
    try:
        availability_v3.make_declaration(
            model="GFS", semantic_id="convective_precipitation", observed_at_utc="2026-09-28T12:00:00Z",
            runtime_status="received", value=1.0, field_available_at_utc="2026-09-28T12:00:01Z",
            evidence_type="provider_fetch_attempt", parameter_native="ACPCP", field_provider_product="gfs_0p25",
        )
        problems.append("Availability-v3 accepted field availability later than observation")
    except ValueError:
        pass

    try:
        availability_v3.make_declaration(
            model="GFS", semantic_id="convective_precipitation", observed_at_utc=observed,
            runtime_status="fetch_error", reason="test", evidence_type="provider_fetch_attempt",
            parameter_native=None, field_provider_product="gfs_0p25",
        )
        problems.append("Availability-v3 accepted runtime attempt without parameter_native")
    except ValueError:
        pass
    try:
        availability_v3.make_declaration(
            model="GFS", semantic_id="convective_precipitation", observed_at_utc=observed,
            runtime_status="received", value=float("nan"), field_available_at_utc=observed,
            reason="test", evidence_type="provider_fetch_attempt",
            parameter_native="ACPCP", field_provider_product="gfs_0p25",
        )
        problems.append("Availability-v3 accepted non-finite received value")
    except ValueError:
        pass

    # Active Registry-v3 capability/policy fields must never claim runtime reception.
    # predecessor_v2_coverage intentionally preserves immutable historical v2 evidence,
    # including the old label native_received, and is not part of the active v3 matrix.
    active_capability_values = {
        value
        for provider in (v3.get("providers") or {}).values()
        for value in (provider.get("capability") or {}).values()
    }
    active_policy_values = {
        value
        for provider in (v3.get("providers") or {}).values()
        for value in (provider.get("acquisition_policy") or {}).values()
    }
    check("native_received" not in active_capability_values, "active Registry v3 capability must not encode legacy native_received")
    check("received" not in active_capability_values, "active Registry v3 capability must not encode runtime received")
    check("received" not in active_policy_values, "active Registry v3 policy must not encode runtime received")

    if problems:
        raise AssertionError("DEV03-WP03-I02 gate failed:\n- " + "\n- ".join(problems))
    return {
        "status":"PASS",
        "implementation_step":"WP03-I02",
        "semantic_count":len(v3_semantics),
        "source_path_count":len(v3["providers"]),
        "availability_state_count":len(availability_v3.STATES),
        "network_requests_allowed":False,
        "operational_authority":"v16-c3-v9",
    }


if __name__ == "__main__":
    print(json.dumps(validate_repository(), sort_keys=True))
