"""Bounded resumable history, shared source cache and one offline CLI."""

import argparse
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
import os
import re
from pathlib import Path
import tempfile
import time
from .core_v1 import (
    CollectorV1,
    CollectorError,
    Response,
    canonical,
    digest,
    identify,
    stamp,
    strict_json,
    utc,
)
from .store_v1 import write_delivery, read_delivery


def atomic_state(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if Path(name).exists():
            Path(name).unlink()


class SourceCacheV1:
    def __init__(self, root):
        self.root = Path(root)

    def key(self, source, job):
        return digest(
            {
                "source": source,
                "provider": job["provider_binding_id"],
                "window": job["window"],
                "context": job["context"],
                "refresh_id": job["refresh_id"],
            }
        )

    def get(self, source, job):
        p = self.root / "requests" / (self.key(source, job) + ".json")
        if not p.exists():
            return None
        ref = strict_json(p.read_bytes())
        if (
            not isinstance(ref, dict)
            or set(ref) != {"sha256", "observed_at_utc", "status", "headers"}
            or not re.fullmatch("[0-9a-f]{64}", str(ref["sha256"]))
            or ref["status"] != 200
        ):
            raise CollectorError("invalid cache reference")
        utc(ref["observed_at_utc"])
        body = (self.root / "objects" / ref["sha256"]).read_bytes()
        if hashlib.sha256(body).hexdigest() != ref["sha256"]:
            raise CollectorError("source cache corruption")
        return Response(
            body, ref["observed_at_utc"], ref["status"], tuple(ref["headers"])
        )

    def put(self, source, job, response):
        if response.status != 200:
            return
        utc(response.observed_at_utc)
        sha = hashlib.sha256(response.payload).hexdigest()
        p = self.root / "objects" / sha
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists() and p.read_bytes() != response.payload:
            raise CollectorError("raw cache immutable conflict")
        if not p.exists():
            with p.open("xb") as stream:
                stream.write(response.payload)
                stream.flush()
                os.fsync(stream.fileno())
        ref = {
            "sha256": sha,
            "observed_at_utc": response.observed_at_utc,
            "status": response.status,
            "headers": list(response.headers),
        }
        request = self.root / "requests" / (self.key(source, job) + ".json")
        if request.exists():
            old = strict_json(request.read_bytes())
            if old != ref:
                raise CollectorError("request cache immutable conflict")
        else:
            atomic_state(request, ref)


def history_manifest(job, *, start_utc, end_utc, chunk_seconds=21600, max_attempts=3):
    start, end = utc(start_utc), utc(end_utc)
    if (
        job["kind"] != "observation_historic"
        or end <= start
        or type(chunk_seconds) is not int
        or not 0 < chunk_seconds <= 86400
        or type(max_attempts) is not int
        or not 1 <= max_attempts <= 3
    ):
        raise CollectorError("bounded history plan required")
    plan = {
        "job": deepcopy(job),
        "start_utc": start_utc,
        "end_utc": end_utc,
        "chunk_seconds": chunk_seconds,
        "max_attempts": max_attempts,
    }
    return {
        "schema_version": 1,
        "artifact_version": "dev03-wp13-history-manifest-v1",
        "plan": plan,
        "plan_id": digest(plan),
        "cursor_utc": start_utc,
        "attempts": {},
        "completed": [],
        "status": "pending",
    }


def validate_history(manifest, collector, delivery_root):
    required = {
        "schema_version",
        "artifact_version",
        "plan",
        "plan_id",
        "cursor_utc",
        "attempts",
        "completed",
        "status",
    }
    if (
        not isinstance(manifest, dict)
        or set(manifest) != required
        or manifest["schema_version"] != 1
        or manifest["artifact_version"] != "dev03-wp13-history-manifest-v1"
    ):
        raise CollectorError("closed history manifest required")
    plan = manifest["plan"]
    if (
        set(plan) != {"job", "start_utc", "end_utc", "chunk_seconds", "max_attempts"}
        or digest(plan) != manifest["plan_id"]
    ):
        raise CollectorError("history plan identity mismatch")
    history_manifest(plan["job"], **{k: v for k, v in plan.items() if k != "job"})
    collector.contracts.validate_job(plan["job"])
    cursor, end = utc(plan["start_utc"]), utc(plan["end_utc"])
    if cursor.microsecond or end.microsecond:
        raise CollectorError("whole-second history bounds required")
    if not isinstance(manifest["completed"], list) or not isinstance(
        manifest["attempts"], dict
    ):
        raise CollectorError("history ledger required")
    for entry in manifest["completed"]:
        stop = min(cursor + timedelta(seconds=plan["chunk_seconds"]), end)
        job = deepcopy(plan["job"])
        job["window"] = {"start_utc": stamp(cursor), "end_utc": stamp(stop)}
        job = identify(job, "job_id")
        if (
            cursor >= end
            or set(entry) != {"window_id", "window", "status", "receipt_id"}
            or entry["window_id"] != job["job_id"]
            or entry["window"] != job["window"]
            or entry["status"] not in ("received", "empty")
        ):
            raise CollectorError("history cursor lacks contiguous window proof")
        result = read_delivery(delivery_root, entry["receipt_id"])
        if result["job"] != job or result["status"] != entry["status"]:
            raise CollectorError("history receipt/window contradiction")
        cursor = stop
    if cursor != utc(manifest["cursor_utc"]):
        raise CollectorError("history cursor lacks receipt proof")
    next_job = deepcopy(plan["job"])
    next_job["window"] = {
        "start_utc": stamp(cursor),
        "end_utc": stamp(min(cursor + timedelta(seconds=plan["chunk_seconds"]), end)),
    }
    next_id = identify(next_job, "job_id")["job_id"]
    if any(
        k != next_id or type(v) is not int or not 1 <= v <= plan["max_attempts"]
        for k, v in manifest["attempts"].items()
    ):
        raise CollectorError("invalid retry ledger")
    if manifest["status"] not in (
        "pending",
        "paused_budget",
        "failed_retryable",
        "retry_exhausted",
        "complete",
    ) or (manifest["status"] == "complete") != (cursor == end):
        raise CollectorError("history status/cursor contradiction")


def run_history(
    manifest, collector, *, cache, delivery_root, manifest_path, transport=None
):
    started = time.monotonic()
    validate_history(manifest, collector, delivery_root)
    m = deepcopy(manifest)
    plan = m["plan"]
    cursor, end = utc(m["cursor_utc"]), utc(plan["end_utc"])
    calls = 0
    windows = 0
    limit = collector.contracts.limits["history_requests_per_job"]
    while cursor < end and calls < limit and windows < limit:
        collector.check_budget(started)
        windows += 1
        stop = min(cursor + timedelta(seconds=plan["chunk_seconds"]), end)
        j = deepcopy(plan["job"])
        j["window"] = {"start_utc": stamp(cursor), "end_utc": stamp(stop)}
        j = identify(j, "job_id")
        window_id = j["job_id"]
        if m["attempts"].get(window_id, 0) >= plan["max_attempts"]:
            m["status"] = "retry_exhausted"
            break
        responses = []
        try:
            for source in j["sources"]:
                response = cache.get(source, j)
                if response is None:
                    if calls >= limit:
                        break
                    calls += 1
                    response = (
                        transport(
                            source, j, collector.contracts.limits["max_payload_bytes"]
                        )
                        if transport
                        else collector.fetch(
                            source,
                            j,
                            collector.contracts.limits["max_payload_bytes"],
                            started,
                        )
                    )
                    cache.put(source, j, response)
                responses.append(response)
            if len(responses) != len(j["sources"]):
                m["status"] = "paused_budget"
                break
            result = collector.collect(j, responses=responses, started=started)
            receipt = write_delivery(
                delivery_root,
                result,
                timeout_seconds=max(
                    0.001,
                    collector.contracts.limits["max_runtime_seconds"]
                    - (time.monotonic() - started),
                ),
            )
            readback = read_delivery(delivery_root, receipt["receipt_id"])
            collector.check_budget(started)
            if readback["status"] not in ("received", "empty"):
                raise CollectorError("history response unsuccessful")
        except Exception:
            # Persist bounded attempts; exception text may contain provider secrets.
            m["attempts"][window_id] = m["attempts"].get(window_id, 0) + 1
            m["status"] = "failed_retryable"
            atomic_state(manifest_path, m)
            break
        m["completed"].append(
            {
                "window_id": window_id,
                "window": j["window"],
                "status": readback["status"],
                "receipt_id": receipt["receipt_id"],
            }
        )
        cursor = stop
        m["cursor_utc"] = stamp(cursor)
        m["status"] = "complete" if cursor == end else "paused_budget"
        m["attempts"].pop(window_id, None)
        atomic_state(manifest_path, m)
    atomic_state(manifest_path, m)
    return {"manifest": m, "provider_requests": calls}


def legacy_observation_view(result):
    """Optional SVG compatibility view derived from retained original raw.

    Existing production output/entrypoints are unchanged. This is an explicit
    optional SVG adapter; no broad second forecast system is introduced.
    """
    from mardorf_collector.providers.fetch_svg_weatherlink import (
        normalize_current,
        normalize_history,
    )

    if result["job"]["provider_binding_id"] != "weatherlink-svg-v1":
        raise CollectorError("legacy view requires explicit SVG binding")
    body = next(
        (
            strict_json(raw)
            for key, raw in result["raw_bytes"].items()
            if key == "raw-0"
        ),
        None,
    )
    if body is None:
        return None
    if result["job"]["kind"] == "observation_current":
        return {"latest_observation": normalize_current(body)}
    return {"recent_historic_observations": normalize_history(body)}


def main():
    parser = argparse.ArgumentParser(
        description="Prepare one WP13 delivery offline; no provider calls or private upload."
    )
    parser.add_argument("--job", required=True)
    parser.add_argument(
        "--responses",
        required=True,
        help="JSON list of local path, observed_at_utc and status",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    job = strict_json(Path(args.job).read_bytes())
    spec = strict_json(Path(args.responses).read_bytes())
    responses = [
        Response(
            Path(x["path"]).read_bytes(), x["observed_at_utc"], x.get("status", 200)
        )
        for x in spec
    ]
    result = CollectorV1().collect(job, responses=responses)
    receipt = write_delivery(args.output, result)
    print(
        json.dumps(
            dict(receipt, source_status=result["status"], fields=len(result["fields"])),
            sort_keys=True,
        )
    )
    return 0 if result["status"] in ("received", "empty") else 1


if __name__ == "__main__":
    raise SystemExit(main())
