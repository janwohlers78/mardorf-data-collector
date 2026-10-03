"""WP15 explicit acquisition successor: original native headers never renamed.

Only the frozen collect/GRIB projection procedures are succeeded here, with a
configuration-backed quantity resolver. Shared transport, budgets, contracts,
observation/EPS adapters, metadata finalization and delivery store remain V1.
Future native aliases extend configuration, not collector algorithms.
"""

from copy import deepcopy
import hashlib
import math
import time
from .core_v1 import (
    CollectorV1,
    CollectorError,
    canonical,
    decode,
    identify,
    strict_json,
    utc,
)
from .adapters_v1 import PARAMETERS, base_field, eccodes_decode, grib_time, digest


def grib_fields_v2(c, j, s, raw_id, decoded, seen, *, decoder=None):
    if not decoded.startswith(b"GRIB"):
        raise CollectorError("GRIB magic missing")
    decoder = decoder or eccodes_decode
    fields = []
    statuses = []
    for site in j["site_ids"]:
        lon, lat = c.tables["sites"][site]["geometry"]["coordinates"]
        rows = decoder(decoded, lat, lon)
        for n, meta in enumerate(rows):
            parameter = meta.get("shortName")
            quantity = PARAMETERS.get(str(parameter).lower())
            alias = None
            if quantity is None:
                candidates = [
                    b
                    for b in c.native_aliases
                    if b["provider_binding_id"] == j["provider_binding_id"]
                    and b["requested_parameter"] == s["native_parameter"]
                    and b["short_name"] == parameter
                ]
                if candidates:
                    alias = candidates[0]
                    if any(
                        meta.get(k) != alias[k]
                        for k in (
                            "paramId",
                            "units",
                            "typeOfLevel",
                            "level",
                            "stepType",
                        )
                    ):
                        raise CollectorError(
                            "native parameter alias metadata contradiction"
                        )
                    quantity = alias["quantity_id"]
            if quantity is None:
                statuses.append(
                    {
                        "native_parameter": parameter,
                        "row_pointer": f"/messages/{n}",
                        "status": "raw_only",
                    }
                )
                continue
            f = base_field(
                c,
                j,
                s,
                raw_id,
                site,
                quantity,
                f"/messages/{n}/value",
                meta.get("value"),
                seen,
            )
            native = grib_time(meta)
            if s["expected_run_time_utc"] is not None and utc(
                native["run_time_utc"]
            ) != utc(s["expected_run_time_utc"]):
                raise CollectorError("requested/native model run contradiction")
            f["time"].update(native)
            f["source"]["native_parameter"] = s["native_parameter"] or parameter
            f["level"].update(
                type_native=meta.get("typeOfLevel"), value_native=meta.get("level")
            )
            f.update(
                unit_native=meta.get("units"),
                unit_evidence="native_metadata" if meta.get("units") else "unknown",
            )
            if not all(
                type(meta.get(k)) in (int, float) and math.isfinite(meta[k])
                for k in ("latitude", "longitude")
            ):
                raise CollectorError("actual GRIB extraction coordinate missing")
            native_lon = ((meta["longitude"] + 180) % 360) - 180
            f["spatial_support"].update(
                actual_latitude=meta["latitude"],
                actual_longitude=native_lon,
                native_grid_id=str(meta.get("md5GridSection") or meta.get("gridType")),
                method="native_extraction_metadata",
            )
            binding = c.forecast[j["provider_binding_id"]]
            expected = binding.get("expected_member_ids")
            if expected:
                number = meta.get("perturbationNumber")
                if type(number) is not int:
                    raise CollectorError("native perturbation number missing")
                member = "c00" if number == 0 else f"p{number:02d}"
                if member not in expected:
                    raise CollectorError("unexpected native member")
                f["member"].update(
                    member_id=member,
                    member_id_native=member,
                    member_role=binding["member_roles"][member],
                    ensemble_set_id=digest(
                        {
                            "job": j["job_id"],
                            "run": native["run_time_utc"],
                            "product": s["product_id_native"],
                        }
                    ),
                )
            else:
                f["member"]["member_role"] = "deterministic"
            f["extensions"]["wp13:original-grib-header:v1"] = deepcopy(meta)
            if alias is not None:
                f["extensions"]["wp15:native-alias:v1"] = {
                    "binding_id": alias["id"],
                    "policy_sha256": c.native_alias_policy_sha256,
                    "short_name_native": parameter,
                    "quantity_id": quantity,
                }
            fields.append(f)
            statuses.append(
                {
                    "native_parameter": parameter,
                    "row_pointer": f"/messages/{n}",
                    "status": (
                        "native_null" if meta.get("value") is None else "received"
                    ),
                }
            )
    return fields, statuses


class CollectorV2(CollectorV1):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        root = self.contracts.root
        body = (root / "config/dev03_wp15_native_aliases_v1.json").read_bytes()
        policy = strict_json(body)
        if policy["artifact_version"] != "dev03-wp15-native-aliases-v1":
            raise CollectorError("unsupported native alias policy")
        for path, expected in policy["predecessor_artifact_sha256"].items():
            if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
                raise CollectorError("native alias predecessor identity mismatch")
        keys = set()
        for binding in policy["bindings"]:
            key = (
                binding["provider_binding_id"],
                binding["requested_parameter"],
                binding["short_name"],
            )
            if (
                key in keys
                or binding["provider_binding_id"] not in self.contracts.forecast
                or binding["quantity_id"] not in self.contracts.tables["quantities"]
            ):
                raise CollectorError("native alias domain/uniqueness mismatch")
            keys.add(key)
        self.contracts.native_aliases = policy["bindings"]
        self.contracts.native_alias_policy_sha256 = hashlib.sha256(body).hexdigest()

    def collect(self, job, *, responses=None, started=None):
        from .adapters_v1 import observations, openmeteo_fields

        started = time.monotonic() if started is None else started
        job = self.contracts.validate_job(job)
        raw = {}
        objects = []
        all_fields = []
        statuses = []
        captures = []
        decoded_total = 0
        metadata = {}
        metadata_captures = {}
        pending = []
        for index, source in enumerate(job["sources"]):
            self.check_budget(started, sum(map(len, raw.values())), len(all_fields))
            object_id = "raw-" + str(index)
            try:
                response = (
                    responses[index]
                    if responses is not None
                    else self.fetch(
                        source,
                        job,
                        self.contracts.limits["max_payload_bytes"]
                        - sum(map(len, raw.values())),
                        started,
                    )
                )
                if not isinstance(response.payload, bytes):
                    raise CollectorError("response bytes required")
                utc(response.observed_at_utc)
                captures.append(response.observed_at_utc)
                raw[object_id] = response.payload
                self.check_budget(started, sum(map(len, raw.values())))
                decoded = decode(
                    response.payload,
                    source["compression"],
                    self.contracts.limits["max_decoded_bytes"],
                )
                decoded_total += len(decoded)
                if decoded_total > self.contracts.limits["max_total_decoded_bytes"]:
                    raise CollectorError("total decoded budget exceeded")
                objects.append(
                    {
                        "object_id": object_id,
                        "path": "raw/"
                        + str(index)
                        + "-"
                        + hashlib.sha256(response.payload).hexdigest()
                        + ".bin",
                        "transport_sha256": hashlib.sha256(
                            response.payload
                        ).hexdigest(),
                        "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
                        "transport_bytes": len(response.payload),
                        "encoding": (
                            "utf-8"
                            if source["media_type"] == "application/json"
                            else "binary"
                        ),
                        "compression": source["compression"],
                        "media_type": source["media_type"],
                    }
                )
                if response.status != 200:
                    statuses.append(
                        {
                            "source_index": index,
                            "status": "fetch_error",
                            "http_status": response.status,
                            "reason": "HTTP_non_200",
                            "fields": [],
                        }
                    )
                    continue
                if source["role"] in ("metadata_before", "metadata_after"):
                    metadata[source["role"]] = strict_json(decoded)
                    metadata_captures[source["role"]] = response.observed_at_utc
                    statuses.append(
                        {
                            "source_index": index,
                            "status": "received_metadata",
                            "http_status": 200,
                            "reason": None,
                            "fields": [],
                        }
                    )
                    continue
                args = (
                    self.contracts,
                    job,
                    source,
                    object_id,
                    decoded,
                    response.observed_at_utc,
                )
                if job["kind"].startswith("observation"):
                    fields, field_status = observations(*args)
                elif job["kind"] == "forecast_grib":
                    fields, field_status = grib_fields_v2(
                        *args, decoder=self.grib_decoder
                    )
                else:
                    pending.append(args)
                    continue
                all_fields.extend(fields)
                self.check_budget(started, sum(map(len, raw.values())), len(all_fields))
                statuses.append(
                    {
                        "source_index": index,
                        "status": "received" if fields else "empty",
                        "http_status": response.status,
                        "reason": None,
                        "fields": field_status,
                        "observed_at_utc": response.observed_at_utc,
                    }
                )
            except Exception as exc:
                statuses.append(
                    {
                        "source_index": index,
                        "status": "fetch_error",
                        "http_status": None,
                        "reason": type(exc).__name__,
                        "fields": [],
                    }
                )
        for args in pending:
            try:
                if (
                    not utc(metadata_captures["metadata_before"])
                    <= utc(args[-1])
                    <= utc(metadata_captures["metadata_after"])
                ):
                    raise CollectorError("EPS metadata captures do not bracket data")
                fields, field_status = openmeteo_fields(*args, metadata=metadata)
                all_fields.extend(fields)
                statuses.append(
                    {
                        "source_index": 1,
                        "status": "received" if fields else "empty",
                        "http_status": 200,
                        "reason": None,
                        "fields": field_status,
                    }
                )
            except Exception as exc:
                statuses.append(
                    {
                        "source_index": 1,
                        "status": "fetch_error",
                        "http_status": None,
                        "reason": type(exc).__name__,
                        "fields": [],
                    }
                )
        # One conservative capture for the atomic envelope; per-source original
        # capture remains in its status inventory. Never substitute model run.
        capture = max(captures, key=utc) if captures else self.clock()
        for field in all_fields:
            field["time"]["first_seen_at_utc"] = capture
        from .adapters_v1 import finalize_forecasts

        all_fields, extra_objects, extra_raw = finalize_forecasts(
            self.contracts, job, all_fields, objects
        )
        objects += extra_objects
        raw.update(extra_raw)
        fields = [identify(f, "field_id") for f in all_fields]
        expected = self.contracts.forecast.get(job["provider_binding_id"], {}).get(
            "expected_member_ids"
        )
        groups = {}
        sets = []
        for f in fields:
            if f["member"]["ensemble_set_id"]:
                key = (
                    f["member"]["ensemble_set_id"],
                    f["domain"]["site_id"],
                    f["domain"]["quantity_id"],
                    digest(f["level"]),
                    digest(f["time"]),
                    f["source"]["product_id_native"],
                    digest(f["spatial_support"]),
                    f["unit_native"],
                    f["binding_id"],
                )
                groups.setdefault(key, []).append(f["member"]["member_id"])
        incomplete = any(
            len(v) != len(set(v)) or set(v) != set(expected or [])
            for v in groups.values()
        )
        for set_id in sorted({k[0] for k in groups}):
            first = next(f for f in fields if f["member"]["ensemble_set_id"] == set_id)
            sets.append(
                {
                    "ensemble_set_id": set_id,
                    "provider_binding_id": job["provider_binding_id"],
                    "run_time_utc": first["time"]["run_time_utc"],
                    "source_revision": first["source"]["source_revision"],
                    "expected_member_ids": expected,
                }
            )
        failed = any(s["status"] == "fetch_error" for s in statuses) or incomplete
        fragment = b"".join(canonical(f) + b"\n" for f in fields)
        fragment_path = "fields/" + job["job_id"] + ".jsonl"
        if (
            len(objects) > self.contracts.limits["max_raw_objects"]
            or len(fragment) > self.contracts.limits["max_fragment_bytes"]
        ):
            raise CollectorError("derived inventory/fragment budget exceeded")
        envelope = None
        if fields and not failed:
            envelope = identify(
                {
                    "schema_version": 1,
                    "artifact_version": "dev03-wp12-raw-transfer-v1",
                    "context": job["context"],
                    "intended_use": (
                        "development"
                        if self.contracts.tables["profiles"][job["profile_id"]]["usage"]
                        == "development"
                        else "fixture_only"
                    ),
                    "configuration": self.contracts.configuration,
                    "capture": {
                        "collector_commit_sha": self.collector_commit_sha,
                        "request_id": job["job_id"],
                        "retrieved_at_utc": capture,
                        "verified_at_utc": capture,
                        "publication_evidence": (
                            "historical_retrieval"
                            if job["context"] == "historical"
                            else "unknown"
                        ),
                    },
                    "raw_objects": objects,
                    "field_fragments": [
                        {
                            "path": fragment_path,
                            "sha256": hashlib.sha256(fragment).hexdigest(),
                            "schema_version": "dev03-wp12-native-field-v1",
                            "row_count": len(fields),
                        }
                    ],
                    "ensemble_sets": sets,
                    "required_capabilities": [
                        "wp12-native-field-v1",
                        "wp12-strict-validation-v2",
                    ]
                    + (["wp12-ensemble-atomic-v1"] if sets else []),
                    "extensions": {},
                },
                "envelope_id",
            )
        self.check_budget(started, sum(map(len, raw.values())), len(fields))
        return {
            "job": job,
            "status": "failed" if failed else "received" if fields else "empty",
            "envelope": envelope,
            "fields": fields,
            "raw_bytes": raw,
            "fragment_bytes": {fragment_path: fragment} if envelope else {},
            "source_status": statuses,
            "original_capture_at_utc": capture,
        }
