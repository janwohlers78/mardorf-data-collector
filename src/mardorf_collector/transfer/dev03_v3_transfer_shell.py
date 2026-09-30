#!/usr/bin/env python3
"""Offline isolated transfer-plan shell for DEV03-WP03-I03.

No GitHub/provider request is made here.  WP03-I04 may later populate the
payload and execute the already-isolated path/readback plan after I03 PASS.
"""
from __future__ import annotations

import json

from mardorf_collector.contracts.dev03_v3_parent_binding import (
    BUNDLE_METHOD,
    TRANSFER_METHOD,
    attempt_event_pointer_allows,
    collection_transaction_id,
    current_parent_pointer_allows,
    isolated_v3_paths,
    sha256_hex,
    v3_attempt_id,
)


def build_transfer_plan(bundle: dict, payload_bytes: bytes) -> dict:
    if not isinstance(bundle, dict) or bundle.get("schema_version") != 3:
        raise ValueError("collector-model-bundle-v3 shell required")
    if bundle.get("method_version") != BUNDLE_METHOD:
        raise ValueError("collector-model-bundle-v3 method_version mismatch")
    if not isinstance(payload_bytes, (bytes, bytearray)):
        raise ValueError("payload_bytes must be exact bytes")
    expected_payload = canonical_payload_bytes(bundle)
    if bytes(payload_bytes) != expected_payload:
        raise ValueError("payload_bytes do not exactly match canonical collector-model-bundle-v3 bytes")
    attempt_id = bundle.get("v3_attempt_id")
    generated = bundle.get("v3_generated_at_utc")
    parent = bundle.get("parent_v2") or {}
    expected_tx = collection_transaction_id(parent)
    if bundle.get("collection_transaction_id") != expected_tx:
        raise ValueError("bundle collection_transaction_id contradicts exact parent binding")
    expected_attempt = v3_attempt_id(
        expected_tx, generated, bundle.get("attempt_nonce")
    )
    if attempt_id != expected_attempt:
        raise ValueError("bundle v3_attempt_id contradicts collection transaction/attempt identity")
    paths = isolated_v3_paths(generated, attempt_id)
    return {
        "schema_version": 1,
        "method_version": "dev03-v3-isolated-transfer-shell-v1",
        "readback_method_version": TRANSFER_METHOD,
        "v3_attempt_id": attempt_id,
        "v3_generated_at_utc": generated,
        "v3_payload_sha256": sha256_hex(payload_bytes),
        "collection_transaction_id": bundle.get("collection_transaction_id"),
        "parent_v2_payload_sha256": parent.get("parent_v2_payload_sha256"),
        "parent_v2_collector_generated_at_utc": parent.get(
            "parent_v2_collector_generated_at_utc"
        ),
        "parent_v2_transfer_receipt_sha256": parent.get(
            "parent_v2_transfer_receipt_sha256"
        ),
        "parent_v2_verified_data_commit_sha": parent.get(
            "parent_v2_verified_data_commit_sha"
        ),
        "paths": paths,
        "readback_required": True,
        "legacy_namespace_writes_allowed": False,
        "network_requests_performed_by_plan": 0,
    }


def pointer_decisions(*, current_parent, latest_attempt, incoming):
    return {
        "advance_current_parent": current_parent_pointer_allows(
            current_parent, incoming
        ),
        "advance_attempt_event": attempt_event_pointer_allows(
            latest_attempt, incoming
        ),
    }


def canonical_payload_bytes(bundle: dict) -> bytes:
    return (
        json.dumps(
            bundle,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
