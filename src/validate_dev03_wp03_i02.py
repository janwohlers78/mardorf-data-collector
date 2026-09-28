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
            )
            availability_v3.validate_declaration(d)
        except Exception as exc:
            problems.append(f"runtime state {status} rejected: {exc}")

    # No static Registry v3 field may claim runtime reception.
    registry_text = json.dumps(v3, sort_keys=True)
    check('"native_received"' not in registry_text, "Registry v3 must not encode runtime native_received status")
    check('"received"' not in json.dumps(v3.get("providers"), sort_keys=True), "provider matrix must not encode received")

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
