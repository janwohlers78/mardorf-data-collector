#!/usr/bin/env python3
"""Resumable WeatherLink SVG history with immutable source receipts.

Historical retrieval proves what the provider returned *now*. It does not prove
when a past measurement was first available, so these rows remain separate from
the prospective, receipt-timed observation shards.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from mardorf_collector.providers.fetch_svg_weatherlink import STATION_ID, S, BASE, normalize_history, sensor, HISTORIC_STRUCTURE, f
from mardorf_collector.transfer.push_private import atomic_commit, hdr

UTC = timezone.utc
DEFAULT_ROOT = Path("data/observations/svg_weatherlink/history_v2")


def normalize_historic(payload):
    rows = normalize_history(payload)
    source = {int(rec["ts"]): rec for rec in sensor(payload, HISTORIC_STRUCTURE).get("data", [])
              if isinstance(rec, dict) and rec.get("ts") is not None}
    for row in rows:
        rec = source[int(datetime.fromisoformat(row["time_utc"]).timestamp())]
        row["raw_source_values"] = {
            "wind_speed_avg_mph": f(rec.get("wind_speed_avg")),
            "wind_speed_hi_mph": f(rec.get("wind_speed_hi")),
            "wind_dir_of_prevail_code": f(rec.get("wind_dir_of_prevail")),
            "wind_dir_of_hi_code": f(rec.get("wind_dir_of_hi")),
        }
    return rows


def canonical_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def write_immutable(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError(f"immutable SVG history artifact changed: {path}")
        return
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(raw)
    os.replace(tmp, path)


def day_paths(root, day):
    base = Path(root) / day.strftime("%Y/%m/%d")
    return base, sorted(base.glob("manifest_*.json"))


def valid_previous_manifest(root, day):
    base, paths = day_paths(root, day)
    if not paths:
        return None
    manifests = [(json.loads(path.read_text(encoding="utf-8")), path) for path in paths]
    manifest, path = max(manifests, key=lambda pair: pair[0]["retrieved_at_utc"])
    for key, hash_key in (("raw_path", "raw_sha256"),
                          ("normalized_path", "normalized_sha256")):
        artifact = base / manifest[key]
        if not artifact.exists() or sha256(artifact.read_bytes()) != manifest[hash_key]:
            raise RuntimeError(f"SVG history manifest artifact hash mismatch: {artifact}")
    if manifest.get("date_utc") != day.isoformat():
        raise RuntimeError(f"SVG history manifest day mismatch: {path}")
    return manifest, path


def request_bounds(day):
    """Include midnight despite WeatherLink's exclusive lower bound.

    The 86,399-second request stays below the provider's 86,400-second limit.
    Midnight at the *next* day is captured by the next day's overlapping query.
    """
    start = datetime.combine(day, time.min, UTC)
    return start - timedelta(seconds=1), start + timedelta(days=1, seconds=-2)


def coverage(rows, day):
    start = datetime.combine(day, time.min, UTC)
    expected = {(start + timedelta(minutes=5*i)).isoformat() for i in range(288)}
    by_time = {}
    duplicate = 0
    for row in rows:
        timestamp = row.get("time_utc")
        if timestamp in by_time:
            duplicate += 1
        if timestamp in expected:
            by_time[timestamp] = row
    missing = sorted(expected - by_time.keys())
    complete_hours = 0
    for hour in range(24):
        stamps = [(start + timedelta(hours=hour, minutes=5*i)).isoformat()
                  for i in range(12)]
        if all(t in by_time and by_time[t].get("wind_speed_ms") is not None
               and by_time[t].get("wind_gust_ms") is not None for t in stamps):
            complete_hours += 1
    return {
        "source_rows": len(rows),
        "unique_day_timestamps": len(by_time),
        "duplicate_timestamps": duplicate,
        "missing_five_minute_timestamps": missing,
        "complete_timestamp_hours": complete_hours,
        "wind_speed_present": sum(r.get("wind_speed_ms") is not None for r in by_time.values()),
        "wind_gust_present": sum(r.get("wind_gust_ms") is not None for r in by_time.values()),
    }, [by_time[t] for t in sorted(by_time)]


def get(path, key, secret, params):
    response = S.get(BASE + path, params={"api-key": key, **params},
                     headers={"X-Api-Secret": secret}, timeout=(10, 45))
    response.raise_for_status()
    return response, response.json()


def fetch_day(root, day, key, secret, *, refresh=False):
    previous = valid_previous_manifest(root, day)
    prior_coverage = previous[0]["coverage"] if previous else {}
    complete = (prior_coverage.get("unique_day_timestamps") == 288
                and prior_coverage.get("complete_timestamp_hours") == 24)
    if previous and complete and not refresh:
        return {"date_utc": day.isoformat(), "status": "verified_existing",
                "unique_day_timestamps": previous[0]["coverage"]["unique_day_timestamps"]}
    begin, end = request_bounds(day)
    # Fetch only provider records; request/response URLs can contain the API key
    # and must never enter persisted errors or receipts.
    response, payload = get(f"/historic/{STATION_ID}", key, secret, {
        "start-timestamp": int(begin.timestamp()),
        "end-timestamp": int(end.timestamp()),
    })
    retrieved = datetime.now(UTC).isoformat()
    raw = response.content
    raw_hash = sha256(raw)
    rows = normalize_historic(payload)
    metrics, normalized_rows = coverage(rows, day)
    if not normalized_rows:
        raise RuntimeError(f"WeatherLink returned no SVG archive records for {day}")
    normalized = canonical_bytes({
        "schema_version": 2,
        "target_version": "svg42374-historical-retrieval-v2",
        "date_utc": day.isoformat(),
        "retrieved_at_utc": retrieved,
        "availability_evidence_type": "historical_retrieval_not_original_publication",
        "source_sha256": raw_hash,
        "observations": normalized_rows,
    })
    base, _ = day_paths(root, day)
    raw_path = f"weatherlink_{raw_hash}.json.gz"
    normalized_hash = sha256(normalized)
    norm_path = f"observations_{normalized_hash}.json"
    # gzip mtime=0 makes the source artifact stable for an exact replay.
    compressed = gzip.compress(raw, mtime=0)
    write_immutable(base / raw_path, compressed)
    write_immutable(base / norm_path, normalized)
    manifest = {
        "schema_version": 2,
        "date_utc": day.isoformat(),
        "station_id": STATION_ID,
        "provider": "WeatherLink v2",
        "endpoint": f"/historic/{STATION_ID}",
        "request_start_utc": begin.isoformat(),
        "request_end_utc": end.isoformat(),
        "retrieved_at_utc": retrieved,
        "availability_evidence_type": "historical_retrieval_not_original_publication",
        "raw_path": raw_path,
        "raw_sha256": sha256(compressed),
        "source_response_sha256": raw_hash,
        "source_response_bytes": len(raw),
        "normalized_path": norm_path,
        "normalized_sha256": normalized_hash,
        "coverage": metrics,
        "previous_manifest_sha256": sha256(previous[1].read_bytes()) if previous else None,
    }
    manifest_bytes = canonical_bytes(manifest)
    write_immutable(base / f"manifest_{sha256(manifest_bytes)}.json", manifest_bytes)
    return {"date_utc": day.isoformat(), "status": "fetched", **metrics}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end-exclusive", type=date.fromisoformat, required=True)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--private-cache", type=Path, required=True)
    args = parser.parse_args(argv)
    if not args.start < args.end_exclusive or (args.end_exclusive - args.start).days > 190:
        parser.error("start must precede end-exclusive")
    key = os.getenv("WEATHERLINK_API_KEY")
    secret = os.getenv("WEATHERLINK_API_SECRET")
    if not key or not secret:
        parser.error("WeatherLink credentials are unavailable")
    args.root = args.private_cache / DEFAULT_ROOT
    before = {str(p): sha256(p.read_bytes()) for p in args.root.rglob("*") if p.is_file()}
    day = args.start
    failures = []
    while day < args.end_exclusive:
        try:
            result = fetch_day(args.root, day, key, secret, refresh=args.refresh)
        except Exception as exc:
            # Errors must not expose an HTTP URL containing credentials.
            result = {"date_utc": day.isoformat(), "status": "error",
                      "error_type": type(exc).__name__}
            failures.append(day.isoformat())
        print(json.dumps(result, sort_keys=True), flush=True)
        day += timedelta(days=1)
    files = []
    for path in sorted(args.root.rglob("*")):
        if path.is_file() and before.get(str(path)) != sha256(path.read_bytes()):
            files.append({"path": path.relative_to(args.private_cache).as_posix(),
                          "content": path.read_bytes(), "immutable": True})
    if files:
        result = atomic_commit(os.environ["PRIVATE_REPO"], files,
                               "observations: verified historical SVG source receipts",
                               hdr(os.environ["PRIVATE_REPO_TOKEN"]))
        print(json.dumps({"transfer_commit": result.get("commit_sha"),
                          "paths": len(result["paths"]),
                          "readback_verified": result["readback_verified"]}), flush=True)
    if failures:
        print(json.dumps({"failed_days": failures}), flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
