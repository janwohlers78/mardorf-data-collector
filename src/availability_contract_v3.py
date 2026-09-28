#!/usr/bin/env python3
"""Model-field Availability v3 definitions for the DEV03 successor path.

This module is intentionally not wired into the operational v2 collector in I02.
It separates static Registry-v3 capability/policy from per-attempt evidence.
"""
from __future__ import annotations

from copy import deepcopy

import relevant_meteorology_registry_v3 as registry_v3

METHOD_VERSION = "model-field-availability-v3"
STATES = {
    "received",
    "unsupported_by_provider_or_product",
    "intentionally_not_applicable",
    "not_requested_by_policy",
    "not_yet_published",
    "fetch_error",
    "unknown_or_ambiguous",
}
RUNTIME_ATTEMPT_STATES = {"received", "not_yet_published", "fetch_error", "unknown_or_ambiguous"}


def _require_observed_at(value):
    if not isinstance(value, str) or not value:
        raise ValueError("availability_observed_at_utc is required")
    return value


def static_status(model, semantic_id):
    """Map Registry-v3 capability/policy to a non-runtime availability state.

    A required native field without per-attempt evidence is deliberately
    unknown_or_ambiguous, never received.
    """
    cap = registry_v3.capability_for(model, semantic_id)
    policy = registry_v3.acquisition_policy_for(model, semantic_id)
    if cap == "unsupported_by_provider_or_product":
        return "unsupported_by_provider_or_product"
    if cap == "intentionally_not_applicable":
        return "intentionally_not_applicable"
    if cap == "unknown_or_ambiguous":
        return "unknown_or_ambiguous"
    if cap != "native_available":
        raise ValueError(f"unknown Registry-v3 capability: {cap!r}")
    if policy == "not_requested_by_policy":
        return "not_requested_by_policy"
    if policy == "required":
        return "unknown_or_ambiguous"
    raise ValueError(f"unknown Registry-v3 acquisition policy: {policy!r}")


def make_declaration(
    *,
    model,
    semantic_id,
    observed_at_utc,
    runtime_status=None,
    value=None,
    field_available_at_utc=None,
    evidence_type=None,
    reason=None,
    parameter_native=None,
    field_provider_product=None,
):
    observed = _require_observed_at(observed_at_utc)
    capability = registry_v3.capability_for(model, semantic_id)
    policy = registry_v3.acquisition_policy_for(model, semantic_id)

    if runtime_status is None:
        status = static_status(model, semantic_id)
        if value is not None or field_available_at_utc is not None:
            raise ValueError("static capability/policy declaration cannot carry runtime value/time")
    else:
        status = str(runtime_status)
        if status not in RUNTIME_ATTEMPT_STATES:
            raise ValueError(f"runtime_status must be an attempt state, got {status!r}")
        if capability != "native_available":
            raise ValueError("runtime attempt evidence cannot override unsupported/inapplicable/ambiguous capability")
        if policy != "required":
            raise ValueError("runtime attempt evidence cannot override not_requested_by_policy")

    if status not in STATES:
        raise ValueError(f"unknown Availability-v3 state: {status!r}")
    if status == "received":
        if value is None:
            raise ValueError("received must carry a value")
        if not field_available_at_utc:
            raise ValueError("received must carry field_available_at_utc")
    else:
        if value is not None:
            raise ValueError("only received may carry a value")
        if field_available_at_utc is not None:
            raise ValueError("only received may carry field_available_at_utc")

    out = {
        "registry_version": "relevant-meteorology-v3",
        "availability_method_version": METHOD_VERSION,
        "model": model,
        "semantic_id": semantic_id,
        "provider_capability": capability,
        "acquisition_policy": policy,
        "availability_status": status,
        "availability_observed_at_utc": observed,
        "namespace": "availability",
        "value": value if status == "received" else None,
    }
    if status == "received":
        out["field_available_at_utc"] = field_available_at_utc
    if evidence_type:
        out["availability_evidence_type"] = str(evidence_type)
    if reason:
        out["availability_reason"] = str(reason)
    if parameter_native:
        out["parameter_native"] = str(parameter_native)
    if field_provider_product:
        out["field_provider_product"] = str(field_provider_product)
    return out


def validate_declaration(item):
    if not isinstance(item, dict):
        raise ValueError("Availability-v3 declaration must be an object")
    required = {
        "registry_version", "availability_method_version", "model", "semantic_id",
        "provider_capability", "acquisition_policy", "availability_status",
        "availability_observed_at_utc", "namespace", "value",
    }
    missing = required - set(item)
    if missing:
        raise ValueError(f"Availability-v3 declaration missing fields: {sorted(missing)}")
    if item["registry_version"] != "relevant-meteorology-v3":
        raise ValueError("Registry version drift")
    if item["availability_method_version"] != METHOD_VERSION:
        raise ValueError("Availability method drift")
    if item["namespace"] != "availability":
        raise ValueError("Availability namespace drift")
    if item["availability_status"] not in STATES:
        raise ValueError("Unknown Availability-v3 status")
    _require_observed_at(item["availability_observed_at_utc"])

    expected_cap = registry_v3.capability_for(item["model"], item["semantic_id"])
    expected_policy = registry_v3.acquisition_policy_for(item["model"], item["semantic_id"])
    if item["provider_capability"] != expected_cap:
        raise ValueError("Provider capability contradicts Registry v3")
    if item["acquisition_policy"] != expected_policy:
        raise ValueError("Acquisition policy contradicts Registry v3")

    status = item["availability_status"]
    if status == "received":
        if item.get("value") is None or not item.get("field_available_at_utc"):
            raise ValueError("received requires value and field_available_at_utc")
    else:
        if item.get("value") is not None:
            raise ValueError("only received may carry a value")
        if "field_available_at_utc" in item:
            raise ValueError("only received may carry field_available_at_utc")

    # Static precedence must win over any weaker policy/runtime interpretation.
    cap = item["provider_capability"]
    policy = item["acquisition_policy"]
    if cap == "unsupported_by_provider_or_product" and status != "unsupported_by_provider_or_product":
        raise ValueError("unsupported capability has highest precedence")
    if cap == "intentionally_not_applicable" and status != "intentionally_not_applicable":
        raise ValueError("inapplicable capability has highest precedence")
    if cap == "native_available" and policy == "not_requested_by_policy" and status != "not_requested_by_policy":
        raise ValueError("disabled acquisition policy must remain not_requested_by_policy")
    return True


def copy_declaration(item):
    validate_declaration(item)
    return deepcopy(item)
