"""Inactive WP12 format validator and deterministic normalization reference.

Provider calls, file publication, training and temporal matching are outside
this module. Native bytes/metadata stay in the original objects and field row.
"""
from copy import deepcopy
from datetime import datetime
from fractions import Fraction
import bz2
import gzip
import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlparse

from jsonschema import Draft202012Validator, FormatChecker

from .paths import REPOSITORY_ROOT
from .domain_v1 import DomainRegistryV1, validate_successor

CONTRACT = REPOSITORY_ROOT / "config/dev03_wp12_format_contract_v1.json"


class FieldErrorV1(ValueError):
    pass


def canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode()
    except (TypeError, ValueError) as exc:
        raise FieldErrorV1("finite JSON required") from exc


def content_id(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def with_id(value, key):
    out = deepcopy(value)
    out.pop(key, None)
    out[key] = content_id(out)
    return out


def utc(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as exc:
        raise FieldErrorV1("explicit UTC timestamp required") from exc
    if result.tzinfo is None or result.utcoffset().total_seconds() != 0:
        raise FieldErrorV1("UTC required")
    return result


class WeatherFormatV1:
    def __init__(self, root=REPOSITORY_ROOT):
        self.root = Path(root)
        self.contract = self.read("config/dev03_wp12_format_contract_v1.json")
        for item in self.contract["predecessors"].values():
            if hashlib.sha256((self.root / item["path"]).read_bytes()).hexdigest() != item["sha256"]:
                raise FieldErrorV1("frozen predecessor checksum mismatch")
        self.domain = DomainRegistryV1.load(self.root / self.contract["domain_registry"])
        self.registry = self.read(self.contract["domain_registry"])
        previous = self.read(self.contract["predecessors"]["domain_v1"]["path"])
        validate_successor(previous, self.registry)
        self.bindings = self.read(self.contract["source_bindings"])
        for item in self.bindings["matching_methods"].values():
            if hashlib.sha256((self.root / item["predecessor"]).read_bytes()).hexdigest() != item["predecessor_sha256"]:
                raise FieldErrorV1("frozen matching method checksum mismatch")
        self.policy = self.read(self.contract["normalization_policy"])
        predecessor = self.policy["predecessor"]
        if hashlib.sha256((self.root / predecessor["path"]).read_bytes()).hexdigest() != predecessor["sha256"]:
            raise FieldErrorV1("unit predecessor checksum mismatch")
        self.schemas = {}
        for kind, path in self.contract["schemas"].items():
            schema = self.read(path)
            Draft202012Validator.check_schema(schema)
            self.schemas[kind] = Draft202012Validator(schema, format_checker=FormatChecker())
        self.weatherlink = {b["id"]: b for b in self.bindings["weatherlink_fields"]}
        self.forecast = {b["id"]: b for b in self.bindings["forecast_bindings"]}

    def read(self, path):
        def pairs(items):
            out = {}
            for key, value in items:
                if key in out:
                    raise FieldErrorV1("duplicate JSON key")
                out[key] = value
            return out
        return json.loads((self.root / path).read_text(), object_pairs_hook=pairs)

    def check(self, kind, value, id_key):
        canonical(value)
        error = next(self.schemas[kind].iter_errors(value), None)
        if error:
            raise FieldErrorV1(f"{kind} schema: {error.message}")
        if value[id_key] != content_id({k: v for k, v in value.items() if k != id_key}):
            raise FieldErrorV1("content identity mismatch")
        if "required_capabilities" in value:
            if not set(value["required_capabilities"]) <= set(self.contract["supported_capabilities"]):
                raise FieldErrorV1("unsupported required capability")

    def model_identity(self, source):
        binding = self.forecast.get(source["provider_binding_id"])
        if binding is None:
            return {"status": "unresolved", "model_id": None, "family": None, "method": "unresolved"}
        model, product = source["model_id_native"], source["product_id_native"]
        if model not in [None] + binding.get("native_model_ids", [binding["model_id"]]) or product not in [None] + binding.get("allowed_product_ids", [binding["product_id"]]):
            raise FieldErrorV1("native model/product contradicts binding")
        prefix = binding["official_url_prefix"]
        url = urlparse(source["source_url"])
        if prefix:
            expected = urlparse(prefix)
            official = (url.scheme == expected.scheme and url.hostname == expected.hostname
                        and url.port in (None, 443) and url.username is None and url.password is None
                        and url.path.startswith(expected.path)
                        and Path(url.path).name.startswith(binding["filename_prefix"])
                        and not any(p in (".", "..") for p in url.path.split("/")))
            if not official:
                raise FieldErrorV1("unproven official product context")
            return {"status": "confirmed", "model_id": binding["model_id"],
                    "product_id": binding["product_id"], "family": binding["family"], "method": "official_product_context_v1"}
        if model is None or product is None:
            return {"status": "unresolved", "model_id": None, "family": None, "method": "unresolved"}
        return {"status": "confirmed", "model_id": binding["model_id"], "product_id": product, "family": binding["family"],
                "method": "native_model_product_v1"}

    def validate_field(self, field):
        self.check("native_field", field, "field_id")
        config = field["configuration"]
        if config != {"registry_revision": self.registry["registry_revision"], "sha256": self.domain.content_sha256}:
            raise FieldErrorV1("configuration binding mismatch")
        tables = {k: {x["id"]: x for x in self.registry[k]} for k in
                  ("sites", "profiles", "stations", "sensors", "quantities", "targets")}
        d = field["domain"]
        spatial = field["spatial_support"]
        if spatial["requested_site_id"] != d["site_id"]:
            raise FieldErrorV1("spatial site binding mismatch")
        if spatial["method"] == "unresolved" and any(spatial[k] is not None for k in ("actual_latitude", "actual_longitude", "native_grid_id")):
            raise FieldErrorV1("unresolved grid cannot contain inferred geometry")
        if (spatial["actual_latitude"] is None) != (spatial["actual_longitude"] is None):
            raise FieldErrorV1("partial extraction coordinates")
        for key, table in (("site_id", "sites"), ("profile_id", "profiles"), ("quantity_id", "quantities")):
            if d[key] not in tables[table]:
                raise FieldErrorV1("unknown domain identity")
        profile = tables["profiles"][d["profile_id"]]
        if d["site_id"] not in profile["site_ids"]:
            raise FieldErrorV1("site outside profile")
        if field["source"]["provider_binding_id"] not in profile["provider_ids"]:
            raise FieldErrorV1("provider outside profile")
        if d["target_id"] is not None:
            target = tables["targets"].get(d["target_id"])
            if target is None or target["site_id"] != d["site_id"] or target["quantity_id"] != d["quantity_id"] or target["id"] not in profile["target_ids"]:
                raise FieldErrorV1("target binding mismatch")
        t = field["time"]
        start, end, seen = utc(t["valid_start_utc"]), utc(t["valid_end_utc"]), utc(t["first_seen_at_utc"])
        if t["private_first_seen_at_utc"] is not None and utc(t["private_first_seen_at_utc"]) < seen:
            raise FieldErrorV1("private first seen precedes public capture")
        if end < start or (t["operator"] == "point" and (start != end or t["closure"] != "point")):
            raise FieldErrorV1("invalid point/interval support")
        if t["operator"] not in ("point", "unknown") and (end <= start or t["closure"] not in ("(start,end]", "[start,end)")):
            raise FieldErrorV1("resolved interval needs explicit duration/closure")
        member = field["member"]
        native_member = member["member_id_native"]
        if member["member_id"] != (None if native_member is None else str(native_member)):
            raise FieldErrorV1("native/canonical member identity mismatch")
        if field["record_kind"] == "observation":
            station = tables["stations"].get(d["station_id"])
            sensor = tables["sensors"].get(d["sensor_id"])
            if station is None or sensor is None or sensor["station_id"] != station["id"] or station["site_id"] != d["site_id"]:
                raise FieldErrorV1("station/sensor/site mismatch")
            lon, lat = tables["sites"][d["site_id"]]["geometry"]["coordinates"]
            if spatial["method"] != "station_registry" or (spatial["actual_longitude"], spatial["actual_latitude"]) != (lon, lat) or spatial["native_grid_id"] is not None:
                raise FieldErrorV1("station spatial support mismatch")
            if t["run_time_utc"] is not None or t["measured_at_utc"] is None or member != {"member_id": None, "member_id_native": None, "member_role": "not_applicable", "ensemble_set_id": None}:
                raise FieldErrorV1("observation contains forecast identity")
            if utc(t["measured_at_utc"]) != end or seen < end:
                raise FieldErrorV1("observation measurement/availability mismatch")
            height = sensor["measurement_height"]
            if field["level"]["measurement_height_status"] != height["status"] or field["level"]["measurement_height_m"] != height["value_m"]:
                raise FieldErrorV1("observation sensor height relabeled")
            binding = self.weatherlink.get(field["binding_id"])
            if binding is None or binding["provider_binding_id"] != field["source"]["provider_binding_id"] or binding["native_parameter"] != field["source"]["native_parameter"] or binding["quantity_id"] != d["quantity_id"]:
                raise FieldErrorV1("observation source binding mismatch")
            if station["provider_id"] != field["source"]["provider_binding_id"]:
                raise FieldErrorV1("station provider mismatch")
            if binding["operator"] != t["operator"] or field["unit_native"] != binding["unit"]:
                raise FieldErrorV1("provider operator/unit mismatch")
        else:
            if d["station_id"] is not None or d["sensor_id"] is not None or t["run_time_utc"] is None or t["measured_at_utc"] is not None:
                raise FieldErrorV1("forecast contains observation identity")
            if spatial["method"] == "station_registry" or (spatial["method"] == "native_extraction_metadata" and spatial["actual_latitude"] is None):
                raise FieldErrorV1("forecast extraction metadata missing")
            if utc(t["run_time_utc"]) > start:
                raise FieldErrorV1("validity precedes model run")
            if utc(t["run_time_utc"]) > seen or (t["published_at_utc"] is not None and utc(t["run_time_utc"]) > utc(t["published_at_utc"])):
                raise FieldErrorV1("model run after availability/publication")
            if field["binding_id"] != field["source"]["provider_binding_id"]:
                raise FieldErrorV1("forecast binding mismatch")
            if field["level"]["measurement_height_status"] != "not_applicable" or field["level"]["measurement_height_m"] is not None:
                raise FieldErrorV1("forecast relabels sensor height")
            target = tables["targets"].get(d["target_id"])
            if target and target["forecast_level"]["kind"] == "height_above_ground":
                if field["level"]["type_native"] != "heightAboveGround" or field["level"]["value_native"] != target["forecast_level"]["value_m"]:
                    raise FieldErrorV1("forecast target level mismatch")
            self.model_identity(field["source"])
            if member["member_role"] == "deterministic":
                if member["member_id"] is not None or member["ensemble_set_id"] is not None:
                    raise FieldErrorV1("deterministic member identity mismatch")
            elif member["member_role"] not in ("control", "perturbed", "ensemble_member") or member["member_id"] is None or member["ensemble_set_id"] is None:
                raise FieldErrorV1("forecast member identity missing")
            binding = self.forecast.get(field["source"]["provider_binding_id"], {})
            if "member_roles" in binding and binding["member_roles"].get(member["member_id"]) != member["member_role"]:
                raise FieldErrorV1("native ensemble member role mismatch")
        if t["published_at_utc"] is not None and utc(t["published_at_utc"]) > seen:
            raise FieldErrorV1("publication after first seen")
        return deepcopy(field)

    def normalize(self, field):
        self.validate_field(field)
        quantity, value, unit = field["domain"]["quantity_id"], field["value_native"], field["unit_native"]
        out = dict(schema_version=1, artifact_version="dev03-wp12-canonical-field-v1",
                   native_field_id=field["field_id"], configuration_sha256=field["configuration"]["sha256"],
                   normalization_policy_sha256=content_id(self.policy), method_version="wp12-scalar-normalization-v1",
                   source_binding_policy_sha256=content_id(self.bindings),
                   qualification_scope="unit_and_matching_semantics_only",
                   source_identity=self.model_identity(field["source"]) if field["record_kind"] == "forecast" else
                       {"status": "confirmed", "model_id": None, "product_id": "weatherlink-v2-station", "family": None, "method": "provider_sensor_catalog_v1"},
                   status="normalized", value_canonical=None, unit_canonical=None, physical_kind=None,
                   matching_status="eligible", reasons=[], metadata_ref=field["field_id"])
        binding = self.weatherlink.get(field["binding_id"])
        if field["time"]["operator"] == "unknown":
            out.update(matching_status="open", reasons=["operator_unresolved"])
        if field["spatial_support"]["method"] == "unresolved":
            out.update(matching_status="open", reasons=out["reasons"] + ["spatial_support_unresolved"])
        if field["record_kind"] == "forecast" and self.model_identity(field["source"])["status"] != "confirmed":
            out.update(matching_status="open", reasons=out["reasons"] + ["model_identity_unresolved"])
        if field["record_kind"] == "forecast" and quantity in ("air_pressure", "cape", "cin"):
            out.update(matching_status="open", reasons=out["reasons"] + ["forecast_variant_binding_required"])
        if binding and binding["role"] == "semantic_open":
            out.update(matching_status="open", reasons=out["reasons"] + ["pressure_variant_unresolved"])
        if value is None:
            out.update(status="missing", matching_status="rejected", reasons=["native_null"])
        elif type(value) not in (int, float):
            out.update(status="invalid_numeric_value", matching_status="rejected", reasons=["non_numeric_native_value"])
        elif field["qc_native"]:
            out.update(status="qc_rejected", matching_status="rejected", reasons=list(field["qc_native"]))
        else:
            kinds = self.policy["quantities"].get(quantity, [])
            matches = [(kind, self.policy["quantity_units"][kind][unit]) for kind in kinds if unit in self.policy["quantity_units"][kind]]
            if unit == "sector16" and quantity == "wind_direction":
                if type(value) is not int or not 0 <= value <= 15:
                    out.update(status="qc_rejected", matching_status="rejected", reasons=["invalid_direction_sector"])
                else:
                    out.update(value_canonical=math.radians(value * 22.5), unit_canonical="rad", physical_kind="angle", method_version="wp12-sector16-to-radian-v1")
            elif len(matches) != 1:
                out.update(status="unsupported_unit", matching_status="open", reasons=["unknown_or_ambiguous_unit"])
            else:
                kind, rule = matches[0]
                try:
                    if rule.get("method") == "degrees_to_radians":
                        converted = math.radians(value)
                    else:
                        exact = Fraction(str(value)) * Fraction(*rule["scale"]) + Fraction(*rule["offset"])
                        converted = float(exact)
                    if not math.isfinite(converted) or (converted == 0 and value != 0 and rule.get("offset") == [0, 1]):
                        raise ValueError("overflow/underflow")
                except (ValueError, OverflowError):
                    out.update(status="invalid_numeric_value", matching_status="rejected", reasons=["unrepresentable_conversion"])
                else:
                    out.update(value_canonical=converted, unit_canonical=rule["unit_si"], physical_kind=kind,
                               method_version="wp12-unit-" + kind + "-v1")
            if out["value_canonical"] is not None:
                x = out["value_canonical"]
                limits = self.policy["qc_rules"].get(quantity, {})
                invalid = ("min" in limits and x < limits["min"]) or ("max" in limits and x > limits["max"])
                if quantity == "wind_direction" and unit != "sector16" and not 0 <= x <= 2 * math.pi:
                    invalid = True
                if invalid:
                    out.update(status="qc_rejected", matching_status="rejected", reasons=["physical_domain_violation"], value_canonical=None)
        out = with_id(out, "projection_id")
        self.check("canonical_field", out, "projection_id")
        return out

    def read_record(self, native, projection):
        """Reference read contract; join one native row and its pinned projection."""
        self.check("canonical_field", projection, "projection_id")
        if canonical(projection) != canonical(self.normalize(native)):
            raise FieldErrorV1("canonical projection differs from bound native/method")
        record = with_id({"schema_version": 1, "artifact_version": "dev03-wp12-read-field-v1",
                          "native": deepcopy(native), "canonical": deepcopy(projection)}, "record_id")
        self.check("read_field", record, "record_id")
        return record

    def validate_transfer(self, envelope, fields, raw_bytes, fragment_bytes):
        self.check("envelope", envelope, "envelope_id")
        config = {"registry_revision": self.registry["registry_revision"], "sha256": self.domain.content_sha256}
        if envelope["configuration"] != config:
            raise FieldErrorV1("envelope configuration mismatch")
        capture = envelope["capture"]
        if utc(capture["retrieved_at_utc"]) > utc(capture["verified_at_utc"]):
            raise FieldErrorV1("verification precedes retrieval")
        raw = {}
        object_info = {}
        if len({x["path"] for x in envelope["raw_objects"]}) != len(envelope["raw_objects"]):
            raise FieldErrorV1("duplicate raw path")
        for item in envelope["raw_objects"]:
            if item["object_id"] in raw or item["path"].startswith("/") or ".." in Path(item["path"]).parts:
                raise FieldErrorV1("duplicate raw object or unsafe path")
            if (item["media_type"] == "application/json") != (item["encoding"] == "utf-8"):
                raise FieldErrorV1("raw media/encoding mismatch")
            payload = raw_bytes.get(item["object_id"])
            if not isinstance(payload, bytes) or len(payload) != item["transport_bytes"] or hashlib.sha256(payload).hexdigest() != item["transport_sha256"]:
                raise FieldErrorV1("raw transport integrity mismatch")
            try:
                decoded = gzip.decompress(payload) if item["compression"] == "gzip" else bz2.decompress(payload) if item["compression"] == "bz2" else payload
                obj = json.loads(decoded) if item["media_type"] == "application/json" else decoded
            except (ValueError, OSError, EOFError) as exc:
                raise FieldErrorV1("invalid decoded raw object") from exc
            if hashlib.sha256(decoded).hexdigest() != item["decoded_sha256"]:
                raise FieldErrorV1("decoded raw integrity mismatch")
            raw[item["object_id"]] = obj
            object_info[item["object_id"]] = item
        if len({f["field_id"] for f in fields}) != len(fields):
            raise FieldErrorV1("duplicate native field ID")
        if sum(f["row_count"] for f in envelope["field_fragments"]) != len(fields):
            raise FieldErrorV1("fragment row count mismatch")
        fragment_rows = []
        if len({x["path"] for x in envelope["field_fragments"]}) != len(envelope["field_fragments"]):
            raise FieldErrorV1("duplicate fragment path")
        for fragment in envelope["field_fragments"]:
            path = fragment["path"]
            if path.startswith("/") or ".." in Path(path).parts:
                raise FieldErrorV1("unsafe fragment path")
            payload = fragment_bytes.get(path)
            if not isinstance(payload, bytes) or hashlib.sha256(payload).hexdigest() != fragment["sha256"]:
                raise FieldErrorV1("fragment integrity mismatch")
            rows = [json.loads(line) for line in payload.decode().splitlines()]
            if len(rows) != fragment["row_count"]:
                raise FieldErrorV1("fragment row count mismatch")
            fragment_rows.extend(rows)
        if canonical(fragment_rows) != canonical(fields):
            raise FieldErrorV1("fragment field content mismatch")
        sets = {s["ensemble_set_id"]: s for s in envelope["ensemble_sets"]}
        if len(sets) != len(envelope["ensemble_sets"]):
            raise FieldErrorV1("duplicate ensemble set")
        groups = {}
        for field in fields:
            self.validate_field(field)
            if field["time"]["first_seen_at_utc"] != capture["retrieved_at_utc"]:
                raise FieldErrorV1("field/capture availability mismatch")
            if field["configuration"] != envelope["configuration"]:
                raise FieldErrorV1("field/envelope configuration mismatch")
            source = field["source"]
            try:
                node = raw[source["raw_object_id"]]
                if isinstance(node, bytes):
                    metadata_id = source["native_metadata_object_id"]
                    node = raw[metadata_id]
                    if not isinstance(node, dict) or node.get("source_decoded_sha256") != object_info[source["raw_object_id"]]["decoded_sha256"]:
                        raise FieldErrorV1("binary extraction metadata source mismatch")
                elif source["native_metadata_object_id"] is not None:
                    raise FieldErrorV1("JSON source needs no binary metadata sidecar")
                parent = None
                for part in source["raw_record_pointer"].split("/")[1:]:
                    part = part.replace("~1", "/").replace("~0", "~")
                    parent = node
                    node = node[int(part)] if isinstance(node, list) else node[part]
            except FieldErrorV1:
                raise
            except (KeyError, IndexError, ValueError, TypeError) as exc:
                raise FieldErrorV1("raw pointer missing") from exc
            if canonical(node) != canonical(field["value_native"]):
                raise FieldErrorV1("native value differs from raw pointer")
            if field["record_kind"] == "observation":
                if not isinstance(parent, dict) or type(parent.get("ts")) is not int:
                    raise FieldErrorV1("native observation timestamp missing")
                measured = utc(field["time"]["measured_at_utc"])
                if measured.timestamp() != parent["ts"]:
                    raise FieldErrorV1("observation time differs from raw timestamp")
                binding = self.weatherlink[field["binding_id"]]
                duration = (utc(field["time"]["valid_end_utc"]) - utc(field["time"]["valid_start_utc"])).total_seconds()
                if binding["endpoint_kind"] == "historic":
                    if type(parent.get("arch_int")) is not int or parent["arch_int"] <= 0 or duration != parent["arch_int"]:
                        raise FieldErrorV1("archive interval differs from native arch_int")
                elif binding["operator"] in ("mean", "maximum") and duration != 600:
                    raise FieldErrorV1("current ten-minute operator duration mismatch")
            member = field["member"]
            if member["ensemble_set_id"] is not None:
                s = sets.get(member["ensemble_set_id"])
                if s is None or s["source_revision"] != source["source_revision"] or s["provider_binding_id"] != source["provider_binding_id"] or s["run_time_utc"] != field["time"]["run_time_utc"]:
                    raise FieldErrorV1("ensemble revision/run/source mismatch")
                key = (s["ensemble_set_id"], source["product_id_native"], field["domain"]["site_id"], field["domain"]["quantity_id"], content_id(field["level"]), content_id(field["time"]))
                groups.setdefault(key, []).append(member["member_id"])
        for key, members in groups.items():
            expected = sets[key[0]]["expected_member_ids"]
            binding = self.forecast.get(sets[key[0]]["provider_binding_id"], {})
            if binding.get("expected_member_ids") != expected or len(members) != len(set(members)) or set(members) != set(expected):
                raise FieldErrorV1("ensemble incomplete or undeclared member")
        if set(sets) != {key[0] for key in groups}:
            raise FieldErrorV1("declared ensemble has no complete field group")
        return {"status": "PASS", "fields": len(fields), "raw_objects": len(raw), "ensemble_groups": len(groups)}
