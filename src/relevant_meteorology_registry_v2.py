"""Relevant Meteorology Registry v2 successor contract (Phase-2 audit B1).

Registry v1 remains immutable audit history. Registry v2 splits deterministic
DWD direct and diffuse shortwave semantics and permits a total only when it is
provider-native or explicitly derived with compatible intervals and provenance.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

REGISTRY_PATH = Path(__file__).resolve().parents[1] / "config" / "relevant_meteorology_registry_v2.json"
EXPECTED_MODELS = {
    "ICON-D2", "ICON-EU", "ICON-D2-EPS", "ECMWF-IFS", "GFS", "GEFS-control"
}
EXPECTED_SEMANTIC_COUNT = 15
DIRECT = "surface_downward_shortwave_direct"
DIFFUSE = "surface_downward_shortwave_diffuse"
TOTAL = "surface_downward_shortwave"

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha256(value):
    if not isinstance(value, str):
        value = canonical(value)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def semantic_contract_payload(registry):
    return {key: registry[key] for key in (
        "registry_version", "coverage_status_values", "core_semantics",
        "identity_preservation", "derived_semantics",
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
    if registry.get("schema_version") != 2:
        raise ValueError("Registry schema_version must be 2")
    if registry.get("registry_version") != "relevant-meteorology-v2":
        raise ValueError("unexpected Registry version")
    if registry.get("status") != "active_phase2_audit_repair":
        raise ValueError("Registry v2 is not the active Phase-2 audit repair registry")
    if registry.get("supersedes") != "relevant-meteorology-v1":
        raise ValueError("Registry v2 predecessor drift")
    semantics = [x.get("semantic_id") for x in registry.get("core_semantics", [])]
    if len(semantics) != EXPECTED_SEMANTIC_COUNT or len(set(semantics)) != len(semantics):
        raise ValueError(f"expected {EXPECTED_SEMANTIC_COUNT} unique core semantics")
    semantic_set = set(semantics)
    for required in (DIRECT, DIFFUSE, TOTAL):
        if required not in semantic_set:
            raise ValueError(f"missing B1 shortwave semantic: {required}")
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
    for model in ("ICON-D2", "ICON-EU"):
        provider = providers[model]
        if provider["native_mapping"].get("aswdir_s") != DIRECT:
            raise ValueError(f"{model} ASWDIR_S must map to direct shortwave")
        if provider["native_mapping"].get("aswdifd_s") != DIFFUSE:
            raise ValueError(f"{model} ASWDIFD_S must map to diffuse shortwave")
        if provider["coverage"].get(TOTAL) != "intentionally_not_applicable":
            raise ValueError(f"{model} must not claim a native total shortwave field")
    derived = registry.get("derived_semantics", {}).get(TOTAL) or {}
    if derived.get("automatic_derivation") is not False:
        raise ValueError("shortwave total derivation must be explicit, never automatic")
    required_components = {DIRECT, DIFFUSE}
    if set(derived.get("required_component_semantics") or []) != required_components:
        raise ValueError("shortwave total derivation component drift")
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
