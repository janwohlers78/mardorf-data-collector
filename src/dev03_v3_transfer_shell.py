#!/usr/bin/env python3
"""Offline isolated transfer-plan shell for DEV03-WP03-I03.

No GitHub/provider request is made here.  WP03-I04 may later populate the
payload and execute the already-isolated path/readback plan after I03 PASS.
"""
from __future__ import annotations

import json

from dev03_v3_parent_binding import (
    TRANSFER_METHOD,
    attempt_event_pointer_allows,
    current_parent_pointer_allows,
    isolated_v3_paths,
    sha256_hex,
)


def build_transfer_plan(bundle: dict, payload_bytes: bytes) -> dict:
    if bundle.get("schema_version") != 3:
        raise ValueError("collector-model-bundle-v3 shell required")
    attempt_id = bundle.get("v3_attempt_id")
    generated = bundle.get("v3_generated_at_utc")
    parent = bundle.get("parent_v2") or {}
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
