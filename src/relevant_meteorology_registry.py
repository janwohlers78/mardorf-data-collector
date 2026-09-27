"""Frozen Relevant Meteorology Registry v1 contract (Phase 2F-5).

This module validates semantic/provider coverage and immutable hashes. It does
not normalize units, convert native fields, derive missing values, or alter
analysis behavior.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "config" / "relevant_meteorology_registry_v1.json"
EXPECTED_MODELS = {
    "ICON-D2", "ICON-EU", "ICON-D2-EPS", "ECMWF-IFS", "GFS", "GEFS-control"
}
EXPECTED_SEMANTIC_COUNT = 13

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha256(value):
    if not isinstance(value, str):
        value = canonical(value)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def semantic_contract_payload(registry):
    return {key: registry[key] for key in (
        "registry_version", "coverage_status_values",
        "core_semantics", "identity_preservation",
    )}

def provider_matrix_payload(registry):
    return {"registry_version": registry["registry_version"], "providers": registry["providers"]}

def computed_hashes(registry):
    semantic = sha256(semantic_contract_payload(registry))
    providers = sha256(provider_matrix_payload(registry))
    prehash = dict(registry)
    prehash["hashes"] = {
        "semantic_contract_sha256": semantic,
        "provider_matrix_sha256": providers,
    }
    return {
        "semantic_contract_sha256": semantic,
        "provider_matrix_sha256": providers,
        "registry_sha256": sha256(prehash),
    }

def validate_registry(registry):
    if registry.get("schema_version") != 1:
        raise ValueError("Registry schema_version must be 1")
    if registry.get("registry_version") != "relevant-meteorology-v1":
        raise ValueError("unexpected Registry version")
    if registry.get("status") != "frozen_phase2f5":
        raise ValueError("Registry v1 is not frozen at Phase 2F-5")
    semantics = [x.get("semantic_id") for x in registry.get("core_semantics", [])]
    if len(semantics) != EXPECTED_SEMANTIC_COUNT or len(set(semantics)) != len(semantics):
        raise ValueError(f"expected {EXPECTED_SEMANTIC_COUNT} unique core semantics")
    semantic_set = set(semantics)
    statuses = set(registry.get("coverage_status_values", []))
    expected_statuses = {
        "native_received",
        "unsupported_by_provider_or_product",
        "intentionally_not_applicable",
    }
    if statuses != expected_statuses:
        raise ValueError(f"coverage status set drift: {sorted(statuses)}")
    providers = registry.get("providers") or {}
    if set(providers) != EXPECTED_MODELS:
        raise ValueError(f"provider matrix drift: {sorted(providers)}")
    for model, provider in providers.items():
        coverage = provider.get("coverage") or {}
        if set(coverage) != semantic_set:
            missing = sorted(semantic_set - set(coverage))
            extra = sorted(set(coverage) - semantic_set)
            raise ValueError(f"{model} coverage mismatch missing={missing} extra={extra}")
        unknown = {value for value in coverage.values() if value not in statuses}
        if unknown:
            raise ValueError(f"{model} has unknown coverage states: {sorted(unknown)}")
        exceptions = provider.get("exceptions") or {}
        for semantic_id, status in coverage.items():
            if status != "native_received" and semantic_id not in exceptions:
                raise ValueError(f"{model}/{semantic_id} non-native status lacks explicit reason")
    identity = registry.get("identity_preservation") or {}
    required_identity = {"units","vertical","temporal","product","ensemble","missing","derivation"}
    if set(identity) != required_identity:
        raise ValueError("identity-preservation contract drift")
    got = computed_hashes(registry)
    if registry.get("hashes") != got:
        raise ValueError(f"Registry hash mismatch stored={registry.get('hashes')} computed={got}")
    return got

def load_registry(path=REGISTRY_PATH):
    registry = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_registry(registry)
    return registry

REGISTRY = load_registry()
HASHES = dict(REGISTRY["hashes"])
SEMANTIC_IDS = tuple(x["semantic_id"] for x in REGISTRY["core_semantics"])

def coverage_for(model):
    return REGISTRY["providers"][model]["coverage"]
