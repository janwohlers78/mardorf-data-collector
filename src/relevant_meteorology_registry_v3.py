"""Relevant Meteorology Registry v3 shadow successor (DEV03-WP03-I02).

Registry v2 remains the operational predecessor. V3 separates static provider
capability, acquisition policy, and per-attempt runtime availability.
"""
from __future__ import annotations

import json
from pathlib import Path

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "config" / "relevant_meteorology_registry_v3.json"
EXPECTED_MODELS = {
    "ICON-D2", "ICON-EU", "ECMWF-IFS", "GFS", "GEFS-control",
    "ICON-D2-EPS", "NOAA_GEFS_FULL",
}
EXPECTED_SEMANTIC_COUNT = 16
CONVECTIVE = "convective_precipitation"
CAPABILITY_STATES = {
    "native_available",
    "unsupported_by_provider_or_product",
    "intentionally_not_applicable",
    "unknown_or_ambiguous",
}
POLICY_STATES = {"required", "not_requested_by_policy"}
AVAILABILITY_STATES = {
    "received", "unsupported_by_provider_or_product",
    "intentionally_not_applicable", "not_requested_by_policy",
    "not_yet_published", "fetch_error", "unknown_or_ambiguous",
}


def validate_registry(registry):
    problems = []
    def check(ok, msg):
        if not ok:
            problems.append(msg)

    check(registry.get("schema_version") == 3, "schema_version must be 3")
    check(registry.get("registry_version") == "relevant-meteorology-v3", "registry version drift")
    check(registry.get("status") == "implemented_shadow_definition", "registry status drift")
    check(registry.get("implementation_step") == "WP03-I02", "implementation step drift")
    check(registry.get("supersedes") == "relevant-meteorology-v2", "predecessor drift")
    check((registry.get("predecessor") or {}).get("mutation_allowed") is False, "v2 mutation must remain forbidden")
    check(set(registry.get("capability_status_values") or []) == CAPABILITY_STATES, "capability state family drift")
    check(set(registry.get("acquisition_policy_values") or []) == POLICY_STATES, "policy state family drift")
    check(set(registry.get("availability_status_values") or []) == AVAILABILITY_STATES, "availability family drift")

    semantics = [x.get("semantic_id") for x in registry.get("core_semantics") or []]
    check(len(semantics) == EXPECTED_SEMANTIC_COUNT, "expected 16 semantics")
    check(len(set(semantics)) == EXPECTED_SEMANTIC_COUNT, "semantic ids must be unique")
    check(CONVECTIVE in semantics, "convective_precipitation missing")
    check("convective_precipitation_or_event_proxy" not in semantics, "ambiguous event proxy must not be a model semantic")
    semantic_set = set(semantics)

    providers = registry.get("providers") or {}
    check(set(providers) == EXPECTED_MODELS, "seven-source provider matrix drift")
    for model, provider in providers.items():
        capability = provider.get("capability") or {}
        policy = provider.get("acquisition_policy") or {}
        check(set(capability) == semantic_set, f"{model} capability coverage mismatch")
        check(set(policy) == semantic_set, f"{model} policy coverage mismatch")
        check(set(capability.values()).issubset(CAPABILITY_STATES), f"{model} unknown capability state")
        check(set(policy.values()).issubset(POLICY_STATES), f"{model} unknown policy state")
        for semantic_id in semantic_set:
            cap = capability.get(semantic_id)
            pol = policy.get(semantic_id)
            if cap in {"unsupported_by_provider_or_product", "intentionally_not_applicable", "unknown_or_ambiguous"}:
                check(pol == "not_requested_by_policy", f"{model}/{semantic_id} non-native capability cannot be required")

    expected_conv = {
        "ICON-D2": ("native_available", "required", "rain_con"),
        "ICON-EU": ("native_available", "required", "rain_con"),
        "ECMWF-IFS": ("unsupported_by_provider_or_product", "not_requested_by_policy", None),
        "GFS": ("native_available", "required", "ACPCP"),
        "GEFS-control": ("unsupported_by_provider_or_product", "not_requested_by_policy", None),
        "ICON-D2-EPS": ("native_available", "not_requested_by_policy", "rain_con"),
        "NOAA_GEFS_FULL": ("unsupported_by_provider_or_product", "not_requested_by_policy", None),
    }
    for model, (cap, pol, native) in expected_conv.items():
        provider = providers.get(model) or {}
        check((provider.get("capability") or {}).get(CONVECTIVE) == cap, f"{model} convective capability drift")
        check((provider.get("acquisition_policy") or {}).get(CONVECTIVE) == pol, f"{model} convective policy drift")
        mapped = [k for k, v in (provider.get("native_mapping") or {}).items() if v == CONVECTIVE]
        if native is None:
            check(not mapped, f"{model} must not invent convective native parameter")
        else:
            check(native in mapped, f"{model} convective native mapping drift")

    gfs = providers.get("GFS") or {}
    ident = gfs.get("convective_precipitation_native_identity") or {}
    check(ident.get("selector") == "ACPCP|surface", "GFS ACPCP selector drift")
    check(ident.get("explicitly_excluded_alias") == "CPRAT", "GFS CPRAT exclusion missing")

    sep = registry.get("separation_contract") or {}
    check("never means a field was received" in str(sep.get("provider_capability")), "capability/received separation missing")
    check("only explicit runtime evidence" in str(sep.get("runtime_availability")), "runtime evidence boundary missing")
    op = registry.get("operational_boundary") or {}
    check(op.get("runtime_wiring_in_i02") is False, "I02 must not wire runtime acquisition")
    check(op.get("network_requests_authorized") is False, "I02 must not authorize network")
    check(op.get("operational_v2_registry_unchanged") is True, "v2 registry protection missing")
    check(op.get("operational_authority_unchanged") == "v16-c3-v9", "operational authority drift")

    if problems:
        raise ValueError("Registry v3 validation failed: " + "; ".join(problems))
    return True


def load_registry(path=REGISTRY_PATH):
    registry = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_registry(registry)
    return registry


REGISTRY = load_registry()
SEMANTIC_IDS = tuple(x["semantic_id"] for x in REGISTRY["core_semantics"])


def capability_for(model, semantic_id):
    return REGISTRY["providers"][model]["capability"][semantic_id]


def acquisition_policy_for(model, semantic_id):
    return REGISTRY["providers"][model]["acquisition_policy"][semantic_id]
