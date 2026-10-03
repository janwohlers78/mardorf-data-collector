"""One local immutable prepared-delivery store; no private upload or workflow."""

import fcntl
import hashlib
import json
import os
import re
import time
from pathlib import Path
import shutil
import tempfile
from .core_v1 import CollectorError, canonical, digest, strict_json


def write_delivery(root, result, *, timeout_seconds=30):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise CollectorError("symlink delivery root forbidden")
    summary = {
        k: v for k, v in result.items() if k not in ("raw_bytes", "fragment_bytes")
    }
    summary["physical_inventory"] = {
        "raw_ids": sorted(result["raw_bytes"]),
        "fragment_paths": sorted(result["fragment_bytes"]),
    }
    receipt = digest(summary)
    files = {"receipt.json": canonical(summary)}
    files.update(
        {
            "raw-" + hashlib.sha256(k.encode()).hexdigest(): v
            for k, v in result["raw_bytes"].items()
        }
    )
    files.update(
        {
            "fragment-" + hashlib.sha256(k.encode()).hexdigest(): v
            for k, v in result["fragment_bytes"].items()
        }
    )
    manifest = {
        "schema_version": 1,
        "artifact_version": "dev03-wp13-delivery-manifest-v1",
        "receipt_id": receipt,
        "files": {
            k: {"sha256": hashlib.sha256(v).hexdigest(), "bytes": len(v)}
            for k, v in files.items()
        },
    }
    files["manifest.json"] = canonical(manifest)
    dest = root / receipt
    fd = os.open(
        root / (receipt + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
    )
    try:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise CollectorError("delivery lock deadline exceeded")
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        if dest.exists():
            if (
                dest.is_symlink()
                or {x.name for x in dest.iterdir()} != set(files)
                or any(
                    (dest / k).is_symlink() or (dest / k).read_bytes() != v
                    for k, v in files.items()
                )
            ):
                raise CollectorError("immutable delivery conflict/corruption")
            return {
                "receipt_id": receipt,
                "status": "verified_existing",
                "added_immutable_bytes": 0,
            }
        stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
        try:
            for k, v in files.items():
                with (stage / k).open("xb") as stream:
                    stream.write(v)
                    stream.flush()
                    os.fsync(stream.fileno())
            if any((stage / k).read_bytes() != v for k, v in files.items()):
                raise CollectorError("delivery readback mismatch")
            stage_fd = os.open(stage, os.O_DIRECTORY)
            try:
                os.fsync(stage_fd)
            finally:
                os.close(stage_fd)
            stage.rename(dest)
            directory_fd = os.open(root, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if stage.exists():
                shutil.rmtree(stage)
    finally:
        os.close(fd)
    return {
        "receipt_id": receipt,
        "status": "prepared",
        "added_immutable_bytes": sum(map(len, files.values())),
    }


def read_delivery(root, receipt_id):
    if not isinstance(receipt_id, str) or not re.fullmatch("[0-9a-f]{64}", receipt_id):
        raise CollectorError("invalid receipt identity")
    dest = Path(root) / receipt_id
    if (
        dest.is_symlink()
        or not dest.is_dir()
        or any(x.is_symlink() or not x.is_file() for x in dest.iterdir())
    ):
        raise CollectorError("invalid delivery directory")
    manifest = strict_json((dest / "manifest.json").read_bytes())
    if (
        set(manifest) != {"schema_version", "artifact_version", "receipt_id", "files"}
        or manifest["schema_version"] != 1
        or manifest["artifact_version"] != "dev03-wp13-delivery-manifest-v1"
        or not isinstance(manifest["files"], dict)
    ):
        raise CollectorError("invalid delivery manifest")
    if "receipt.json" not in manifest["files"] or any(
        k != "receipt.json" and not re.fullmatch("(raw|fragment)-[0-9a-f]{64}", k)
        for k in manifest["files"]
    ):
        raise CollectorError("invalid flat delivery inventory")
    if manifest["receipt_id"] != receipt_id or {x.name for x in dest.iterdir()} != set(
        manifest["files"]
    ) | {"manifest.json"}:
        raise CollectorError("delivery inventory mismatch")
    for k, m in manifest["files"].items():
        if (
            set(m) != {"sha256", "bytes"}
            or type(m["bytes"]) is not int
            or m["bytes"] < 0
            or not re.fullmatch("[0-9a-f]{64}", str(m["sha256"]))
        ):
            raise CollectorError("invalid delivery file metadata")
        body = (dest / k).read_bytes()
        if len(body) != m["bytes"] or hashlib.sha256(body).hexdigest() != m["sha256"]:
            raise CollectorError("delivery file hash mismatch")
    result = strict_json((dest / "receipt.json").read_bytes())
    if digest(result) != receipt_id:
        raise CollectorError("delivery receipt identity mismatch")
    inventory = result.pop("physical_inventory")
    result["raw_bytes"] = {
        key: (dest / ("raw-" + hashlib.sha256(key.encode()).hexdigest())).read_bytes()
        for key in inventory["raw_ids"]
    }
    result["fragment_bytes"] = {
        key: (
            dest / ("fragment-" + hashlib.sha256(key.encode()).hexdigest())
        ).read_bytes()
        for key in inventory["fragment_paths"]
    }
    return result
