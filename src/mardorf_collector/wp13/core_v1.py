"""Configured bounded acquisition and raw/native transfer, never routine wired."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import bz2
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import re
import resource
import time
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[3]


class CollectorError(ValueError):
    pass


def canonical(value):
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError) as exc:
        raise CollectorError("finite UTF-8 JSON required") from exc


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def identify(value, key):
    value = deepcopy(value)
    value.pop(key, None)
    value[key] = digest(value)
    return value


def utc(value):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise CollectorError("explicit UTC Z required")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CollectorError("invalid UTC") from exc
    frac = re.search(r"\.(\d+)Z$", value)
    if frac and any(c != "0" for c in frac.group(1)[6:]):
        raise CollectorError("submicrosecond precision loss")
    return dt


def stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def strict_json(payload):
    def pairs(items):
        result = {}
        for k, v in items:
            if k in result:
                raise CollectorError("duplicate JSON key")
            result[k] = v
        return result

    def bad(v):
        raise CollectorError("nonfinite JSON")

    try:
        out = json.loads(
            payload.decode("utf-8"), object_pairs_hook=pairs, parse_constant=bad
        )
        canonical(out)
        return out
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise CollectorError("strict UTF-8 JSON required") from exc


def decode(raw, compression, limit):
    if compression == "none":
        out = raw
    else:
        reader = (
            gzip.GzipFile(fileobj=io.BytesIO(raw))
            if compression == "gzip"
            else bz2.BZ2File(io.BytesIO(raw))
        )
        with reader:
            out = reader.read(limit + 1)
    if len(out) > limit:
        raise CollectorError("decoded byte budget exceeded")
    return out


@dataclass(frozen=True)
class Response:
    payload: bytes
    observed_at_utc: str
    status: int = 200
    headers: tuple = ()


class ContractsV1:
    def __init__(self, root=ROOT, *, registry=None):
        self.root = Path(root)
        manifest = strict_json(
            (self.root / "config/dev03_wp13_contract_bundle_v1.json").read_bytes()
        )
        self.embedded = manifest["embedded"]
        self.collection_enabled = manifest["collection_enabled"]
        self.openmeteo = manifest["openmeteo_binding"]
        for item in self.embedded.values():
            if hashlib.sha256(item["content"].encode()).hexdigest() != item["sha256"]:
                raise CollectorError("contract bundle checksum mismatch")
        self.registry = (
            deepcopy(registry)
            if registry
            else self.read("dev03_wp12_domain_registry_v2.json")
        )
        self.binding = self.read("dev03_wp12_source_bindings_v1.json")
        self.limits = self.read("dev03_wp12_lifecycle_contract_v2.json")["limits"]
        self.tables = {
            k: {i["id"]: i for i in self.registry[k]}
            for k in (
                "sites",
                "profiles",
                "stations",
                "sensors",
                "providers",
                "quantities",
                "targets",
            )
        }
        self.forecast = {i["id"]: i for i in self.binding["forecast_bindings"]}
        self.configuration = {
            "registry_revision": self.registry["registry_revision"],
            "sha256": digest(self.registry),
        }

    def read(self, name):
        return strict_json(self.embedded[name]["content"].encode())

    def validate_job(self, job):
        required = {
            "schema_version",
            "artifact_version",
            "job_id",
            "profile_id",
            "site_ids",
            "provider_binding_id",
            "kind",
            "context",
            "station_id",
            "sensor_id",
            "window",
            "sources",
            "target_ids",
            "refresh_id",
        }
        if (
            set(job) != required
            or job["schema_version"] != 1
            or job["artifact_version"] != "dev03-wp13-job-v1"
            or job["job_id"] != digest({k: v for k, v in job.items() if k != "job_id"})
        ):
            raise CollectorError("closed job schema/identity mismatch")
        if job["kind"] not in (
            "observation_current",
            "observation_historic",
            "forecast_grib",
            "forecast_openmeteo",
        ) or job["context"] not in ("prospective", "historical"):
            raise CollectorError("unknown job kind/context")
        sites = job["site_ids"]
        if (
            not isinstance(sites, list)
            or not sites
            or len(set(sites)) != len(sites)
            or len(sites) > self.limits["max_sites_per_job"]
        ):
            raise CollectorError("site job budget/identity")
        try:
            profile = self.tables["profiles"][job["profile_id"]]
            provider = self.tables["providers"][job["provider_binding_id"]]
        except KeyError as exc:
            raise CollectorError("unknown profile/provider") from exc
        if (
            not set(sites) <= set(profile["site_ids"])
            or job["provider_binding_id"] not in profile["provider_ids"]
        ):
            raise CollectorError("job outside profile")
        if (
            not isinstance(job["sources"], list)
            or not job["sources"]
            or len(job["sources"]) > self.limits["max_raw_objects"]
        ):
            raise CollectorError("source job budget")
        for source in job["sources"]:
            if set(source) != {
                "url",
                "compression",
                "media_type",
                "native_parameter",
                "model_id_native",
                "product_id_native",
                "expected_run_time_utc",
                "role",
            }:
                raise CollectorError("closed source request required")
            url = urlparse(source["url"])
            base = urlparse(provider["endpoint"])
            permitted_metadata = (
                job["kind"] == "forecast_openmeteo"
                and source["role"] in ("metadata_before", "metadata_after")
                and source["url"] == self.openmeteo["metadata_url"]
            )
            if (
                url.scheme != "https"
                or (url.hostname != base.hostname and not permitted_metadata)
                or url.username
                or url.password
                or url.port not in (None, 443)
            ):
                raise CollectorError("provider endpoint mismatch")
            if re.search(r"api[-_]?key|secret|token", url.query, re.I):
                raise CollectorError("credentials forbidden in persisted URL")
            if source["role"] not in ("data", "metadata_before", "metadata_after") or (
                job["kind"] != "forecast_openmeteo" and source["role"] != "data"
            ):
                raise CollectorError("unsupported source role")
            if source["compression"] not in ("none", "gzip", "bz2") or source[
                "media_type"
            ] not in ("application/json", "application/x-grib"):
                raise CollectorError("unsupported source encoding")
            if source["expected_run_time_utc"] is not None:
                utc(source["expected_run_time_utc"])
            forecast = self.forecast.get(job["provider_binding_id"])
            if job["kind"].startswith("forecast"):
                if forecast is None:
                    raise CollectorError("forecast binding required")
                if source["model_id_native"] not in forecast.get(
                    "native_model_ids", [forecast["model_id"]]
                ):
                    raise CollectorError("model binding contradiction")
                if source["product_id_native"] not in forecast.get(
                    "allowed_product_ids", [forecast["product_id"]]
                ):
                    raise CollectorError("product binding contradiction")
                prefix = forecast.get("official_url_prefix")
                if prefix and not source["url"].startswith(prefix):
                    raise CollectorError("official product path contradiction")
                filename = forecast.get("filename_prefix")
                if filename and not Path(url.path).name.startswith(filename):
                    raise CollectorError("official product filename contradiction")
                expected_media = (
                    "application/x-grib"
                    if job["kind"] == "forecast_grib"
                    else "application/json"
                )
                if source["media_type"] != expected_media:
                    raise CollectorError("forecast kind/encoding contradiction")
            elif (
                source["media_type"] != "application/json"
                or source["model_id_native"] is not None
                or source["product_id_native"] is not None
                or source["expected_run_time_utc"] is not None
            ):
                raise CollectorError(
                    "observation source identity/encoding contradiction"
                )
        if job["kind"] == "forecast_openmeteo" and [
            x["role"] for x in job["sources"]
        ] != ["metadata_before", "data", "metadata_after"]:
            raise CollectorError("EPS requires metadata before/data/after")
        if job["kind"].startswith("observation"):
            try:
                station = self.tables["stations"][job["station_id"]]
                sensor = self.tables["sensors"][job["sensor_id"]]
            except KeyError as exc:
                raise CollectorError("station/sensor required") from exc
            if (
                len(sites) != 1
                or station["site_id"] != sites[0]
                or sensor["station_id"] != station["id"]
                or station["provider_id"] != job["provider_binding_id"]
            ):
                raise CollectorError("station/sensor/site/provider contradiction")
            endpoint = (
                "historic" if job["kind"] == "observation_historic" else "current"
            )
            expected = (
                provider["endpoint"].rstrip("/")
                + "/"
                + endpoint
                + "/"
                + station["provider_station_id"]
            )
            if len(job["sources"]) != 1 or job["sources"][0]["url"] != expected:
                raise CollectorError("observation endpoint/station mismatch")
        elif job["station_id"] is not None or job["sensor_id"] is not None:
            raise CollectorError("forecast contains observation identity")
        if job["kind"] == "observation_historic":
            w = job["window"]
            if (
                job["context"] != "historical"
                or not isinstance(w, dict)
                or set(w) != {"start_utc", "end_utc"}
            ):
                raise CollectorError("history context/window required")
            start, end = utc(w["start_utc"]), utc(w["end_utc"])
            if (
                start.microsecond
                or end.microsecond
                or not 0
                < (end - start).total_seconds()
                <= self.limits["max_history_seconds_per_request"]
            ):
                raise CollectorError("bounded whole-second history window required")
        elif job["window"] is not None:
            raise CollectorError("nonhistory window unsupported")
        if not isinstance(job["target_ids"], list) or not set(job["target_ids"]) <= set(
            profile["target_ids"]
        ):
            raise CollectorError("target outside profile")
        return deepcopy(job)


class CollectorV1:
    def __init__(
        self,
        contracts=None,
        *,
        transport=None,
        grib_decoder=None,
        clock=None,
        collector_commit_sha=None
    ):
        self.contracts = contracts or ContractsV1()
        self.transport = transport
        self.grib_decoder = grib_decoder
        if collector_commit_sha is None:
            import subprocess

            collector_commit_sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip()
        if (
            not re.fullmatch("[0-9a-f]{40}", collector_commit_sha)
            or collector_commit_sha == "0" * 40
        ):
            raise CollectorError("actual collector commit required")
        self.collector_commit_sha = collector_commit_sha
        self.clock = clock or (lambda: stamp(datetime.now(timezone.utc)))

    def check_budget(self, started, raw_bytes=0, fields=0):
        l = self.contracts.limits
        if (
            time.monotonic() - started > l["max_runtime_seconds"]
            or resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
            > l["max_peak_rss_bytes"]
        ):
            raise CollectorError("runtime/RSS budget exceeded")
        if raw_bytes > l["max_payload_bytes"] or fields > l["max_member_fields"]:
            raise CollectorError("payload/field budget exceeded")

    def fetch(self, source, job, remaining, started):
        if self.transport:
            return self.transport(source, job, remaining)
        if not self.contracts.collection_enabled:
            raise CollectorError(
                "WP13 writer inactive; consumer/canary release required"
            )
        # Explicit library call only; no CLI live option or scheduled wiring.
        import os, requests

        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": "mardorf-collector/WP13-inactive-v1",
        }
        params = {}
        if job["kind"].startswith("observation"):
            key = os.environ.get("WEATHERLINK_API_KEY")
            secret = os.environ.get("WEATHERLINK_API_SECRET")
            if not key or not secret:
                raise CollectorError("WeatherLink credentials unavailable")
            params["api-key"] = key
            headers["X-Api-Secret"] = secret
            if job["window"]:
                params.update(
                    {
                        "start-timestamp": int(
                            utc(job["window"]["start_utc"]).timestamp()
                        )
                        - 1,
                        "end-timestamp": int(utc(job["window"]["end_utc"]).timestamp())
                        - 1,
                    }
                )
        timeout = max(
            0.1,
            self.contracts.limits["max_runtime_seconds"] - (time.monotonic() - started),
        )
        with requests.Session() as session:
            with session.get(
                source["url"],
                params=params,
                headers=headers,
                stream=True,
                allow_redirects=False,
                timeout=(min(5, timeout), timeout),
            ) as response:
                if response.headers.get("Content-Encoding", "identity") not in (
                    "",
                    "identity",
                ):
                    raise CollectorError("unexpected HTTP content encoding")
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > remaining:
                    raise CollectorError("declared transport byte budget exceeded")
                chunks = bytearray()
                for chunk in response.raw.stream(65536, decode_content=False):
                    if len(chunks) + len(chunk) > remaining:
                        raise CollectorError("transport byte budget exceeded")
                    chunks.extend(chunk)
                    self.check_budget(started, len(chunks))
                return Response(bytes(chunks), self.clock(), response.status_code)

    def collect(self, job, *, responses=None, started=None):
        from .adapters_v1 import observations, grib_fields, openmeteo_fields

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
                    fields, field_status = grib_fields(*args, decoder=self.grib_decoder)
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
