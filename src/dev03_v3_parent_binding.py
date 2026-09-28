#!/usr/bin/env python3
"""Pure DEV03-WP03-I03 parent-binding and v3 transfer-shell primitives.

This module performs no network I/O.  It validates the exact verified v2
parent transfer, derives stable parent/attempt identities, binds the retained
provider cycles, and defines isolated v3 path/pointer ordering.  Provider
acquisition is intentionally deferred to WP03-I04.
"""
from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from pathlib import Path

from forecast_lead_identity import (
    ForecastLeadIdentityError,
    lead_seconds_from_hours,
    validate_forecast_lead_identity,
)

COLLECTION_TRANSACTION_METHOD = "dev03-collection-transaction-id-v2"
PARENT_CYCLE_METHOD = "dev03-parent-cycle-binding-v1"
ATTEMPT_METHOD = "dev03-v3-attempt-id-v2"
POINTER_METHOD = "dev03-v3-pointer-ordering-v2"
TRANSFER_METHOD = "private-transfer-readback-v2"
BUNDLE_METHOD = "collector-model-bundle-v3-shell"
V3_ROOT = "data/inbox/public_collector_v3"


class Dev03ParentBindingError(ValueError):
    pass


def canonical_json_bytes(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _utc(value, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise Dev03ParentBindingError(f"{field} must be a non-empty UTC timestamp")
    raw = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise Dev03ParentBindingError(f"{field} is not ISO/RFC3339") from exc
    if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise Dev03ParentBindingError(f"{field} must be explicitly UTC")
    return dt.astimezone(timezone.utc).isoformat()


def _hex64(value, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        raise Dev03ParentBindingError(f"{field} must be a 64-character SHA-256 hex string")
    try:
        int(value, 16)
    except ValueError as exc:
        raise Dev03ParentBindingError(f"{field} is not hexadecimal") from exc
    return value.lower()


def verify_parent_transfer_receipt(receipt_bytes: bytes, transfer_result: dict) -> dict:
    """Verify the exact immutable v2 transfer receipt and return F01 binding."""
    if not isinstance(receipt_bytes, (bytes, bytearray)) or not receipt_bytes:
        raise Dev03ParentBindingError("parent transfer receipt bytes are required")
    if not isinstance(transfer_result, dict):
        raise Dev03ParentBindingError("transfer_result must be an object")
    try:
        receipt = json.loads(bytes(receipt_bytes).decode("utf-8"))
    except Exception as exc:
        raise Dev03ParentBindingError("parent transfer receipt is not valid UTF-8 JSON") from exc

    if receipt.get("method_version") != TRANSFER_METHOD:
        raise Dev03ParentBindingError("parent receipt method_version mismatch")
    if receipt.get("kind") != "models" or receipt.get("readback_verified") is not True:
        raise Dev03ParentBindingError("parent receipt is not a verified models transfer")

    receipt_sha = sha256_hex(bytes(receipt_bytes))
    expected_receipt_sha = _hex64(
        transfer_result.get("transfer_receipt_sha256"), "transfer_receipt_sha256"
    )
    if receipt_sha != expected_receipt_sha:
        raise Dev03ParentBindingError("parent receipt immutable-byte SHA-256 mismatch")

    payload_sha = _hex64(
        transfer_result.get("payload_source_sha256"), "payload_source_sha256"
    )
    verified_commit = _hex64(
        transfer_result.get("verified_data_commit_sha"), "verified_data_commit_sha"
    )
    generated = _utc(
        transfer_result.get("source_generated_at_utc"), "source_generated_at_utc"
    )
    receipt_path = transfer_result.get("transfer_receipt_path")
    if not isinstance(receipt_path, str) or not receipt_path.startswith(
        "data/inbox/public_collector/transfer_receipts/models/"
    ):
        raise Dev03ParentBindingError("parent transfer receipt path is not the models namespace")

    checks = {
        "readback_verified": receipt.get("readback_verified") is True,
        "payload_source_sha256": receipt.get("payload_source_sha256") == payload_sha,
        "audit_input_payload_sha256": receipt.get("audit_input_payload_sha256") == payload_sha,
        "source_generated_at_utc": _utc(
            receipt.get("source_generated_at_utc"), "receipt.source_generated_at_utc"
        )
        == generated,
        "verified_data_commit_sha": receipt.get("verified_data_commit_sha")
        == verified_commit,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise Dev03ParentBindingError(
            "parent receipt binding mismatch: " + ", ".join(sorted(failed))
        )

    binding = {
        "parent_v2_payload_sha256": payload_sha,
        "parent_v2_collector_generated_at_utc": generated,
        "parent_v2_transfer_receipt_path": receipt_path,
        "parent_v2_transfer_receipt_sha256": receipt_sha,
        "parent_v2_verified_data_commit_sha": verified_commit,
    }
    binding["collection_transaction_id"] = collection_transaction_id(binding)
    return binding


def collection_transaction_id(binding: dict) -> str:
    inputs = {
        key: binding[key]
        for key in (
            "parent_v2_collector_generated_at_utc",
            "parent_v2_payload_sha256",
            "parent_v2_transfer_receipt_sha256",
            "parent_v2_verified_data_commit_sha",
        )
    }
    return sha256_hex(
        COLLECTION_TRANSACTION_METHOD.encode("utf-8") + canonical_json_bytes(inputs)
    )


def _grid_identity(row: dict) -> dict:
    grid_id = row.get("grid_id")
    if isinstance(grid_id, str) and grid_id:
        return {"grid_id": grid_id}
    point = row.get("forecast_coordinate_or_grid_point")
    if not isinstance(point, dict):
        raise Dev03ParentBindingError("parent occurrence lacks exact grid/coordinate identity")
    lat = point.get("latitude")
    lon = point.get("longitude")
    if isinstance(lat, bool) or isinstance(lon, bool) or not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        raise Dev03ParentBindingError("parent extraction coordinate is not numeric")
    lat = float(lat)
    lon = float(lon)
    if not math.isfinite(lat) or not math.isfinite(lon):
        raise Dev03ParentBindingError("parent extraction coordinate must be finite")
    if not -90.0 <= lat <= 90.0 or not -180.0 <= lon <= 180.0:
        raise Dev03ParentBindingError("parent extraction coordinate is outside geographic bounds")
    identity = {"latitude": lat, "longitude": lon}
    if isinstance(point.get("selection"), str) and point["selection"]:
        identity["selection"] = point["selection"]
    return {"extraction_coordinate": identity}


def build_parent_cycle_inventory(parent_payload: dict, registry: dict) -> tuple[list[dict], str]:
    """Build the exact F02 retained-occurrence inventory, failing closed."""
    models = parent_payload.get("models") if isinstance(parent_payload, dict) else None
    if not isinstance(models, dict) or not models:
        raise Dev03ParentBindingError("parent payload has no model records")

    inventory: list[dict] = []
    for model, rows in models.items():
        if not isinstance(model, str) or not isinstance(rows, list):
            raise Dev03ParentBindingError("parent model container is malformed")
        for row in rows:
            if not isinstance(row, dict):
                raise Dev03ParentBindingError("parent occurrence is not an object")
            row_model = row.get("model", model)
            if row_model != model:
                raise Dev03ParentBindingError("parent occurrence model key/value mismatch")
            hours = row.get("forecast_lead_hours")
            try:
                seconds = lead_seconds_from_hours(hours)
                lead = validate_forecast_lead_identity(
                    run_time_utc=row.get("run_time_utc"),
                    valid_time_utc=row.get("valid_time_utc"),
                    lead_seconds=seconds,
                    acquisition_lead_hours=hours,
                )
            except ForecastLeadIdentityError as exc:
                raise Dev03ParentBindingError(
                    f"invalid parent lead identity for {model}: {exc}"
                ) from exc
            product = row.get("provider_product")
            if not isinstance(product, str) or not product:
                raise Dev03ParentBindingError(
                    f"parent occurrence provider_product is missing for {model}; exact parent identity may not be inferred from Registry v3"
                )
            inventory.append(
                {
                    "model": model,
                    "run_time_utc": lead["run_time_utc"],
                    "valid_time_utc": lead["valid_time_utc"],
                    "forecast_lead_seconds": lead["lead_seconds"],
                    "provider_product": product,
                    "grid_identity": _grid_identity(row),
                }
            )

    canonical_items = [canonical_json_bytes(item).decode("utf-8") for item in inventory]
    if len(set(canonical_items)) != len(canonical_items):
        raise Dev03ParentBindingError(
            "duplicate parent retained occurrence identity is not allowed"
        )
    ordered = [item for _, item in sorted(zip(canonical_items, inventory), key=lambda pair: pair[0])]
    binding_id = sha256_hex(canonical_json_bytes(ordered))
    return ordered, binding_id


def new_attempt_nonce() -> str:
    return str(uuid.uuid4())


def v3_attempt_id(collection_transaction_id_value: str, v3_generated_at_utc: str, attempt_nonce: str) -> str:
    _hex64(collection_transaction_id_value, "collection_transaction_id")
    generated = _utc(v3_generated_at_utc, "v3_generated_at_utc")
    try:
        nonce = str(uuid.UUID(attempt_nonce))
    except Exception as exc:
        raise Dev03ParentBindingError("attempt_nonce must be a UUID") from exc
    inputs = {
        "collection_transaction_id": collection_transaction_id_value.lower(),
        "v3_generated_at_utc": generated,
        "attempt_nonce": nonce,
    }
    return sha256_hex(ATTEMPT_METHOD.encode("utf-8") + canonical_json_bytes(inputs))


def build_bundle_v3_shell(
    *,
    parent_binding: dict,
    parent_cycle_binding_id: str,
    v3_generated_at_utc: str,
    attempt_nonce: str,
) -> dict:
    generated = _utc(v3_generated_at_utc, "v3_generated_at_utc")
    expected_tx = collection_transaction_id(parent_binding)
    supplied_tx = parent_binding.get("collection_transaction_id")
    if supplied_tx is not None and _hex64(supplied_tx, "collection_transaction_id") != expected_tx:
        raise Dev03ParentBindingError("collection_transaction_id contradicts exact parent binding")
    tx = expected_tx
    attempt = v3_attempt_id(tx, generated, attempt_nonce)
    return {
        "schema_version": 3,
        "method_version": BUNDLE_METHOD,
        "status": "shell_no_v3_provider_acquisition",
        "v3_generated_at_utc": generated,
        "attempt_nonce": str(uuid.UUID(attempt_nonce)),
        "collection_transaction_id": tx,
        "v3_attempt_id": attempt,
        "parent_cycle_binding_method_version": PARENT_CYCLE_METHOD,
        "parent_cycle_binding_id": _hex64(
            parent_cycle_binding_id, "parent_cycle_binding_id"
        ),
        "parent_v2": {
            key: parent_binding[key]
            for key in (
                "parent_v2_payload_sha256",
                "parent_v2_collector_generated_at_utc",
                "parent_v2_transfer_receipt_path",
                "parent_v2_transfer_receipt_sha256",
                "parent_v2_verified_data_commit_sha",
            )
        },
        "network_requests_performed": 0,
        "provider_evidence": [],
        "operational_authority": "v16-c3-v9",
    }


def isolated_v3_paths(v3_generated_at_utc: str, attempt_id: str) -> dict:
    generated = _utc(v3_generated_at_utc, "v3_generated_at_utc")
    _hex64(attempt_id, "v3_attempt_id")
    dt = datetime.fromisoformat(generated)
    stamp = dt.strftime("%Y%m%dT%H%M%S") + f"{dt.microsecond:06d}Z"
    day = dt.strftime("%Y/%m/%d")
    suffix = f"{stamp}_{attempt_id}"
    return {
        "payload": f"{V3_ROOT}/models/{day}/models_{suffix}.json.gz",
        "integrity": f"{V3_ROOT}/integrity/models/{day}/integrity_{suffix}.json",
        "receipt": f"{V3_ROOT}/transfer_receipts/models/{day}/receipt_{suffix}.json",
        "current_parent_pointer": f"{V3_ROOT}/integrity/models/latest_success.json",
        "attempt_event_pointer": f"{V3_ROOT}/transfer_receipts/models/latest_attempt.json",
    }


def _pointer_key(pointer: dict, *, current_parent: bool) -> tuple:
    if current_parent:
        return (
            _utc(pointer["parent_v2_collector_generated_at_utc"], "parent_v2_collector_generated_at_utc"),
            _utc(pointer["v3_generated_at_utc"], "v3_generated_at_utc"),
            _hex64(pointer["v3_attempt_id"], "v3_attempt_id"),
        )
    return (
        _utc(pointer["v3_generated_at_utc"], "v3_generated_at_utc"),
        _hex64(pointer["v3_attempt_id"], "v3_attempt_id"),
    )


def current_parent_pointer_allows(existing: dict | None, incoming: dict) -> bool:
    if existing is None:
        return True
    return _pointer_key(incoming, current_parent=True) > _pointer_key(
        existing, current_parent=True
    )


def attempt_event_pointer_allows(existing: dict | None, incoming: dict) -> bool:
    if existing is None:
        return True
    return _pointer_key(incoming, current_parent=False) > _pointer_key(
        existing, current_parent=False
    )


def load_registry_v3(path: str | Path | None = None) -> dict:
    if path is None:
        path = Path(__file__).resolve().parents[1] / "config" / "relevant_meteorology_registry_v3.json"
    return json.loads(Path(path).read_text(encoding="utf-8"))
