"""Lossless optional field-outcome sidecar using the existing gzip raw format.

Native weather fields, their pointers and original provider bytes are untouched.
The source-level status and clocks remain directly readable. Detailed outcomes
are stored once as a bounded, hash-bound derived raw object and restored exactly.
"""

from copy import deepcopy
import gzip
import hashlib
import io

from .core_v1 import CollectorError, canonical, identify, strict_json

EXTENSION = "wp15:field-outcomes-gzip:v1"
OBJECT_ID = "wp15-field-outcomes"


def compact_outcomes(result, *, max_decoded_bytes=16 * 1024**2):
    out = deepcopy(result)
    if not out.get("envelope"):
        return out
    if EXTENSION in out["envelope"]["extensions"] or OBJECT_ID in out["raw_bytes"]:
        raise CollectorError("field outcomes already compacted")
    inventory = [s.get("fields", []) for s in out["source_status"]]
    body = canonical(inventory)
    if len(body) > max_decoded_bytes:
        raise CollectorError("field outcomes decoded budget exceeded")
    packed = gzip.compress(body, compresslevel=9, mtime=0)
    # Tiny GRIB inventories are already small; retain their original layout.
    if len(body) - len(packed) < 4096:
        return out
    decoded_sha = hashlib.sha256(body).hexdigest()
    out["raw_bytes"][OBJECT_ID] = packed
    out["envelope"]["raw_objects"].append(
        {
            "object_id": OBJECT_ID,
            "path": "metadata/field-outcomes.json.gz",
            "transport_sha256": hashlib.sha256(packed).hexdigest(),
            "decoded_sha256": decoded_sha,
            "transport_bytes": len(packed),
            "encoding": "utf-8",
            "compression": "gzip",
            "media_type": "application/json",
        }
    )
    out["envelope"]["extensions"][EXTENSION] = {
        "raw_object_id": OBJECT_ID,
        "decoded_sha256": decoded_sha,
        "decoded_bytes": len(body),
        "source_field_counts": [len(fields) for fields in inventory],
        "qualification": "derived_diagnostic_inventory_not_provider_response",
    }
    for status in out["source_status"]:
        status["fields"] = []
    out["envelope"] = identify(out["envelope"], "envelope_id")
    return out


def restore_outcomes(
    envelope, source_status, raw_bytes, *, max_decoded_bytes=16 * 1024**2
):
    """Return the exact original source-status structure, with bounded decoding."""
    ref = envelope.get("extensions", {}).get(EXTENSION)
    if ref is None:
        return deepcopy(source_status)
    if (
        type(ref.get("decoded_bytes")) is not int
        or not 0 <= ref["decoded_bytes"] <= max_decoded_bytes
        or len(ref.get("source_field_counts", [])) != len(source_status)
        or any(s.get("fields") != [] for s in source_status)
    ):
        raise CollectorError("field outcomes reference/inventory mismatch")
    obj = next(
        (o for o in envelope["raw_objects"] if o["object_id"] == ref["raw_object_id"]),
        None,
    )
    packed = raw_bytes.get(ref["raw_object_id"])
    if (
        obj is None
        or not isinstance(packed, bytes)
        or obj["compression"] != "gzip"
        or len(packed) != obj["transport_bytes"]
        or hashlib.sha256(packed).hexdigest() != obj["transport_sha256"]
    ):
        raise CollectorError("field outcomes transport integrity mismatch")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
            body = stream.read(ref["decoded_bytes"] + 1)
    except (OSError, EOFError) as exc:
        raise CollectorError("field outcomes gzip integrity mismatch") from exc
    if (
        len(body) != ref["decoded_bytes"]
        or hashlib.sha256(body).hexdigest() != ref["decoded_sha256"]
        or obj["decoded_sha256"] != ref["decoded_sha256"]
    ):
        raise CollectorError("field outcomes decoded integrity mismatch")
    inventory = strict_json(body)
    if (
        not isinstance(inventory, list)
        or len(inventory) != len(source_status)
        or any(
            not isinstance(f, list) or len(f) != n
            for f, n in zip(inventory, ref["source_field_counts"])
        )
    ):
        raise CollectorError("field outcomes source inventory mismatch")
    restored = deepcopy(source_status)
    for status, fields in zip(restored, inventory):
        status["fields"] = fields
    return restored
