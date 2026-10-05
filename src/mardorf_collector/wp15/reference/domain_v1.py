"""WP12 configurable domain identities; no collection or forecast activation.

Every resolution uses an explicit profile and UTC validity window. Identities
are copied on entry/exit and pinned by a canonical configuration digest.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

from .paths import REPOSITORY_ROOT

SCHEMA_PATH = REPOSITORY_ROOT / "config/schemas/dev03_domain_registry_v1.schema.json"
COLLECTIONS = ("sites", "providers", "stations", "sensors", "quantities",
               "observation_bindings", "targets", "profiles")


class DomainErrorV1(ValueError):
    """Unknown, ambiguous, invalid or expired domain configuration."""


def _utc(value):
    if not isinstance(value, str):
        raise DomainErrorV1("time must be an explicit UTC string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainErrorV1("invalid UTC time") from exc
    if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
        raise DomainErrorV1("time must use UTC")
    return dt.astimezone(timezone.utc)


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DomainErrorV1("configuration must be finite JSON") from exc


def validate_domain(value, schema_path=SCHEMA_PATH):
    """Validate the published schema, references and spatial/version semantics."""
    from jsonschema import Draft202012Validator, FormatChecker
    _canonical(value)
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    error = next(Draft202012Validator(schema, format_checker=FormatChecker())
                 .iter_errors(value), None)
    if error is not None:
        raise DomainErrorV1(f"schema: {error.message}")
    registry = {}
    for collection in COLLECTIONS:
        items = value[collection]
        registry[collection] = {item["id"]: item for item in items}
        if len(registry[collection]) != len(items):
            raise DomainErrorV1(f"duplicate ID in {collection}")
        for item in items:
            start = _utc(item["valid_from_utc"])
            end = item["valid_until_utc"]
            if end is not None and _utc(end) <= start:
                raise DomainErrorV1("empty or inverted identity validity")

    def ref(collection, identifier):
        try:
            return registry[collection][identifier]
        except KeyError as exc:
            raise DomainErrorV1(f"unknown {collection} ID: {identifier}") from exc

    for site in value["sites"]:
        lon, lat = site["geometry"]["coordinates"]
        if not (math.isfinite(lon) and math.isfinite(lat)
                and -180 <= lon <= 180 and -90 <= lat <= 90):
            raise DomainErrorV1("invalid geographic coordinates")
    for station in value["stations"]:
        ref("sites", station["site_id"])
        ref("providers", station["provider_id"])
    for sensor in value["sensors"]:
        ref("stations", sensor["station_id"])
    for binding in value["observation_bindings"]:
        ref("sensors", binding["sensor_id"])
        ref("quantities", binding["quantity_id"])
        if (binding["operator_status"] == "unresolved") != (binding["operator_ref"] is None):
            raise DomainErrorV1("operator evidence/status mismatch")
    for target in value["targets"]:
        ref("sites", target["site_id"])
        ref("quantities", target["quantity_id"])
        binding = ref("observation_bindings", target["observation_binding_id"])
        sensor = ref("sensors", binding["sensor_id"])
        station = ref("stations", sensor["station_id"])
        if target["quantity_id"] != binding["quantity_id"]:
            raise DomainErrorV1("target/binding quantity mismatch")
        if target["site_id"] != station["site_id"]:
            raise DomainErrorV1("direct station target cannot relabel another site")
    for profile in value["profiles"]:
        for collection, key in (("sites", "site_ids"), ("targets", "target_ids"),
                                ("providers", "provider_ids")):
            for identifier in profile[key]:
                ref(collection, identifier)
        for target_id in profile["target_ids"]:
            target = ref("targets", target_id)
            binding = ref("observation_bindings", target["observation_binding_id"])
            sensor = ref("sensors", binding["sensor_id"])
            station = ref("stations", sensor["station_id"])
            if target["site_id"] not in profile["site_ids"]:
                raise DomainErrorV1("target outside profile sites")
            if station["provider_id"] not in profile["provider_ids"]:
                raise DomainErrorV1("observation provider outside profile")
            if profile["usage"] == "development" and any(
                ref("sites", site_id)["usage"] == "fixture_only"
                for site_id in profile["site_ids"]
            ):
                raise DomainErrorV1("fixture site cannot enter development profile")
    return registry


def validate_successor(previous, successor):
    """Retain immutable prior IDs; changed semantics get new IDs and revision."""
    before, after = validate_domain(previous), validate_domain(successor)
    if previous["registry_revision"] == successor["registry_revision"]:
        raise DomainErrorV1("successor needs a new registry revision")
    for collection in COLLECTIONS:
        for identifier, value in before[collection].items():
            if identifier not in after[collection] or _canonical(value) != _canonical(after[collection][identifier]):
                raise DomainErrorV1("frozen identity changed or removed; use a new ID")
    return True


class DomainRegistryV1:
    def __init__(self, value):
        self._value = deepcopy(value)
        self._registry = validate_domain(self._value)
        self._digest = hashlib.sha256(_canonical(self._value)).hexdigest()

    @classmethod
    def load(cls, path):
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise DomainErrorV1("duplicate JSON key")
                result[key] = value
            return result
        return cls(json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=pairs))

    @property
    def content_sha256(self):
        return self._digest

    def resolve(self, profile_id, target_id, *, start_utc, end_utc):
        """Return one bound target; never choose a default site or profile.

        Windows are half-open [start, end); an identity ending at end is valid.
        Provider model level and observation sensor height remain separate.
        """
        start, end = _utc(start_utc), _utc(end_utc)
        if end <= start:
            raise DomainErrorV1("empty or inverted request window")

        def get(collection, identifier):
            try:
                item = self._registry[collection][identifier]
            except KeyError as exc:
                raise DomainErrorV1(f"unknown {collection} ID: {identifier}") from exc
            upper = item["valid_until_utc"]
            if start < _utc(item["valid_from_utc"]) or (upper is not None and end > _utc(upper)):
                raise DomainErrorV1("identity outside validity window")
            return item

        profile = get("profiles", profile_id)
        if target_id not in profile["target_ids"]:
            raise DomainErrorV1("unknown or out-of-profile target")
        target = get("targets", target_id)
        binding = get("observation_bindings", target["observation_binding_id"])
        sensor = get("sensors", binding["sensor_id"])
        station = get("stations", sensor["station_id"])
        providers = [get("providers", p) for p in profile["provider_ids"]]
        return deepcopy({"configuration_sha256": self._digest,
                         "registry_version": self._value["artifact_version"],
                         "profile": profile, "target": target,
                         "site": get("sites", target["site_id"]),
                         "observation_binding": binding, "sensor": sensor,
                         "station": station, "providers": providers,
                         "quantity": get("quantities", target["quantity_id"])})
