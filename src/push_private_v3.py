#!/usr/bin/env python3
"""CAS-safe isolated DEV03 v3 publisher used only by WP03-I10 live proof.

This module never writes public_collector v2 paths.  It reuses the established
private-transfer-readback-v2 data-commit/readback primitive, then publishes an
immutable v3 receipt plus channel-local pointers.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import push_private as tx
from dev03_v3_parent_binding import (
    current_parent_pointer_allows,
    attempt_event_pointer_allows,
    isolated_v3_paths,
)
from dev03_v3_transfer_shell import canonical_payload_bytes, build_transfer_plan

METHOD_VERSION="dev03-i10-v3-private-publisher-v1"
RECEIPT_METHOD="private-transfer-readback-v2"
PRIVATE_REPO="janwohlers78/mardorf-kitevorhersage"
V3_PREFIX="data/inbox/public_collector_v3/"


class V3PublishError(RuntimeError):
    pass


def _json_bytes(obj):
    return (json.dumps(obj,indent=2,sort_keys=True,ensure_ascii=False,allow_nan=False)+"\n").encode()


def _canonical_pointer_bytes(obj):
    return (json.dumps(obj,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False)+"\n").encode()


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _current_json(repo,path,h,ref=None):
    return tx.decoded_json_content(tx.content_meta(repo,path,h,ref=ref))


def _receipt_pointer(bundle,receipt_path,receipt_sha):
    parent=bundle["parent_v2"]
    return {
        "schema_version":1,
        "method_version":"dev03-v3-pointer-ordering-v2",
        "v3_attempt_id":bundle["v3_attempt_id"],
        "v3_generated_at_utc":bundle["v3_generated_at_utc"],
        "parent_v2_collector_generated_at_utc":parent["parent_v2_collector_generated_at_utc"],
        "parent_v2_payload_sha256":parent["parent_v2_payload_sha256"],
        "collection_transaction_id":bundle["collection_transaction_id"],
        "transfer_receipt_path":receipt_path,
        "transfer_receipt_sha256":receipt_sha,
        "operational_authority":"v16-c3-v9",
    }


def publish_v3(*,bundle,private_token,paired_v2_compressed_bytes,repo=PRIVATE_REPO):
    if not isinstance(private_token,str) or not private_token:
        raise V3PublishError("PRIVATE_REPO_TOKEN required")
    if isinstance(paired_v2_compressed_bytes,bool) or not isinstance(paired_v2_compressed_bytes,int) or paired_v2_compressed_bytes<=0:
        raise V3PublishError("paired_v2_compressed_bytes must be measured positive integer")
    payload_raw=canonical_payload_bytes(bundle)
    plan=build_transfer_plan(bundle,payload_raw)
    paths=plan["paths"]
    if not all(str(p).startswith(V3_PREFIX) for p in paths.values()):
        raise V3PublishError("v3 transfer plan escaped isolated namespace")

    packed=gzip.compress(payload_raw,compresslevel=9,mtime=0)
    if len(packed) > int(paired_v2_compressed_bytes*1.5):
        raise V3PublishError("compressed v3/v2 transfer ratio hard gate exceeded")

    integrity={
        "schema_version":1,
        "method_version":"dev03-i10-v3-transfer-integrity-v1",
        "status":"PASS",
        "v3_attempt_id":bundle["v3_attempt_id"],
        "v3_generated_at_utc":bundle["v3_generated_at_utc"],
        "v3_payload_sha256":_sha(payload_raw),
        "parent_v2_payload_sha256":bundle["parent_v2"]["parent_v2_payload_sha256"],
        "collection_transaction_id":bundle["collection_transaction_id"],
        "network_requests_performed":bundle.get("network_requests_performed"),
        "network_response_bytes":bundle.get("network_response_bytes"),
        "i04_acquisition":bundle.get("i04_acquisition"),
        "compressed_payload_bytes":len(packed),
        "paired_v2_compressed_bytes":paired_v2_compressed_bytes,
        "compressed_v3_to_v2_ratio":len(packed)/paired_v2_compressed_bytes,
        "operational_authority":"v16-c3-v9",
    }
    integrity_raw=_json_bytes(integrity)
    h=tx.hdr(private_token)

    data_files=[
        {"path":paths["payload"],"content":packed,"immutable":True,"gzip":True,"source_sha256":_sha(payload_raw)},
        {"path":paths["integrity"],"content":integrity_raw,"immutable":True},
    ]
    data=tx.atomic_commit(repo,data_files,f"dev03 i10: ingest v3 attempt {bundle['v3_attempt_id']}",h)
    if not data.get("readback_verified"):
        raise V3PublishError("v3 data commit exact-byte readback failed")

    # Idempotent recovery: if immutable data already exists, a previous receipt
    # may exist.  Never forge a new data commit identity in that case.
    if data.get("commit_sha") is None:
        existing=_current_json(repo,paths["receipt"],h)
        if not isinstance(existing,dict):
            raise V3PublishError("idempotent v3 data exists without immutable receipt")
        if existing.get("v3_payload_sha256")!=_sha(payload_raw):
            raise V3PublishError("existing v3 receipt conflicts with payload")
        return {
            "status":"already_published",
            "receipt_path":paths["receipt"],
            "receipt_sha256":_sha(tx.decoded_bytes(tx.content_meta(repo,paths["receipt"],h))),
            "v3_attempt_id":bundle["v3_attempt_id"],
            "data_commit_sha":existing.get("verified_data_commit_sha"),
            "network_requests_performed":bundle.get("network_requests_performed",0),
        }

    verified_at=datetime.now(timezone.utc).isoformat()
    receipt={
        "schema_version":2,
        "method_version":RECEIPT_METHOD,
        "kind":"models_v3",
        "readback_verified":True,
        "verified_at_utc":verified_at,
        "v3_attempt_id":bundle["v3_attempt_id"],
        "v3_generated_at_utc":bundle["v3_generated_at_utc"],
        "v3_payload_sha256":_sha(payload_raw),
        "parent_v2_payload_sha256":bundle["parent_v2"]["parent_v2_payload_sha256"],
        "parent_v2_collector_generated_at_utc":bundle["parent_v2"]["parent_v2_collector_generated_at_utc"],
        "collection_transaction_id":bundle["collection_transaction_id"],
        "verified_data_commit_sha":data["commit_sha"],
        "payload_destination":paths["payload"],
        "integrity_destination":paths["integrity"],
        "readback":data.get("readback") or [],
        "transfer_channel":"dev03-v3-shadow",
        "operational_authority":"v16-c3-v9",
    }
    receipt_raw=_json_bytes(receipt)
    receipt_sha=_sha(receipt_raw)
    pointer=_receipt_pointer(bundle,paths["receipt"],receipt_sha)

    current_parent=_current_json(repo,paths["current_parent_pointer"],h)
    latest_attempt=_current_json(repo,paths["attempt_event_pointer"],h)
    receipt_files=[{"path":paths["receipt"],"content":receipt_raw,"immutable":True}]
    if current_parent_pointer_allows(current_parent,pointer):
        receipt_files.append({
            "path":paths["current_parent_pointer"],
            "content":_canonical_pointer_bytes(pointer),
            "immutable":False,
            "monotonic_guard":{
                "path":paths["current_parent_pointer"],
                "field":"parent_v2_collector_generated_at_utc",
                "incoming_time":pointer["parent_v2_collector_generated_at_utc"],
            },
        })
    if attempt_event_pointer_allows(latest_attempt,pointer):
        receipt_files.append({
            "path":paths["attempt_event_pointer"],
            "content":_canonical_pointer_bytes(pointer),
            "immutable":False,
            "monotonic_guard":{
                "path":paths["attempt_event_pointer"],
                "field":"v3_generated_at_utc",
                "incoming_time":pointer["v3_generated_at_utc"],
            },
        })
    final=tx.atomic_commit(repo,receipt_files,f"dev03 i10: record verified v3 transfer {bundle['v3_attempt_id']}",h)
    if not final.get("readback_verified"):
        raise V3PublishError("v3 receipt publication readback failed")
    return {
        "schema_version":1,
        "method_version":METHOD_VERSION,
        "status":"published",
        "v3_attempt_id":bundle["v3_attempt_id"],
        "v3_payload_sha256":_sha(payload_raw),
        "receipt_path":paths["receipt"],
        "receipt_sha256":receipt_sha,
        "verified_data_commit_sha":data["commit_sha"],
        "receipt_commit_sha":final.get("commit_sha"),
        "current_parent_pointer_advanced":paths["current_parent_pointer"] in (final.get("paths") or []),
        "attempt_event_pointer_advanced":paths["attempt_event_pointer"] in (final.get("paths") or []),
        "compressed_payload_bytes":len(packed),
        "paired_v2_compressed_bytes":paired_v2_compressed_bytes,
        "compressed_v3_to_v2_ratio":len(packed)/paired_v2_compressed_bytes,
        "network_requests_performed":bundle.get("network_requests_performed",0),
        "operational_authority":"v16-c3-v9",
    }
