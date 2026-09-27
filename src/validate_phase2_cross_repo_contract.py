#!/usr/bin/env python3
"""Fail-closed cross-repository Phase-2 successor contract validation."""
import argparse
import base64
import hashlib
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

SUCCESSOR_PATH = "config/phase2_successor_contract_v2.json"
FROZEN_V1_PATH = "config/phase2_frozen_contract_v1.json"
EXPECTED_SUCCESSOR = "phase2-acquisition-storage-successor-v2"


def canonical_hash(value):
    raw=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def fetch_contents(repo,path,ref,token=""):
    url=(
        "https://api.github.com/repos/"
        + repo
        + "/contents/"
        + urllib.parse.quote(path,safe="/")
        + "?ref="
        + urllib.parse.quote(ref,safe="")
    )
    headers={
        "Accept":"application/vnd.github+json",
        "X-GitHub-Api-Version":"2022-11-28",
        "User-Agent":"mardorf-phase2-cross-repo-contract-gate",
    }
    if token:
        headers["Authorization"]="Bearer "+token
    req=urllib.request.Request(url,headers=headers)
    with urllib.request.urlopen(req,timeout=30) as response:
        payload=json.load(response)
    if payload.get("encoding")!="base64" or not isinstance(payload.get("content"),str):
        raise RuntimeError(f"GitHub contents response for {repo}:{path}@{ref} is not base64 content")
    return base64.b64decode(payload["content"].replace("\n",""))


def require_successor_invariants(contract,label):
    if contract.get("contract_version")!=EXPECTED_SUCCESSOR:
        raise AssertionError(f"{label}: wrong successor contract version")
    if contract.get("status")!="active_repair_contract_not_refrozen":
        raise AssertionError(f"{label}: successor contract status unexpectedly changed")
    core=contract.get("shared_core")
    if not isinstance(core,dict):
        raise AssertionError(f"{label}: shared_core missing")
    actual=canonical_hash(core)
    if actual!=contract.get("shared_core_sha256"):
        raise AssertionError(f"{label}: shared_core hash mismatch: {actual}")
    if core.get("shared_core_version")!="phase2-revision-event-shared-core-v2":
        raise AssertionError(f"{label}: wrong shared core version")
    identity=core["identity_and_revision_model"]
    if "three ordered revision events" not in identity["aba_rule"]:
        raise AssertionError(f"{label}: A->B->A invariant missing")
    if "idempotent" not in identity["exact_replay_identity"]:
        raise AssertionError(f"{label}: exact-replay invariant missing")
    if "visibility gate" not in identity["ordering_rule"]:
        raise AssertionError(f"{label}: private first_seen ordering invariant missing")
    causality=core["causality_and_as_of"]
    if "MUST NOT inherit" not in causality["metadata_leak_rule"]:
        raise AssertionError(f"{label}: metadata leak invariant missing")
    ensemble=core["ensemble_revision_contract"]
    if "same selected source revision set" not in ensemble["atomic_member_selection"]:
        raise AssertionError(f"{label}: ensemble atomicity invariant missing")
    if "MUST NOT displace" not in ensemble["old_source_replay_rule"]:
        raise AssertionError(f"{label}: old-source replay invariant missing")
    if core["evidence_publication_contract"]["gefs_cycle_evidence"]["required_model"]!="immutable_event_stream_plus_monotonic_index":
        raise AssertionError(f"{label}: GEFS evidence publication invariant missing")
    if core["family_independence"]["key"]!="model_family":
        raise AssertionError(f"{label}: family independence key drift")
    blockers=core["phase3_gate"]["feature_broker_blocked_until"]
    expected=[
        "C1 deterministic revision-event ledger green",
        "C2 revision-bound field availability green",
        "C3 adversarial causality matrix green",
    ]
    if blockers!=expected:
        raise AssertionError(f"{label}: Phase-3 causality blocker drift")
    required_impl_keys={
        "archive_successor",
        "availability_successor",
        "ensemble_successor",
        "registry_successor",
        "canonical_record_successor",
        "gefs_cycle_evidence_successor",
    }
    implementation_versions=contract.get("implementation_versions")
    if not isinstance(implementation_versions,dict) or set(implementation_versions)!=required_impl_keys:
        raise AssertionError(f"{label}: implementation-version ownership keys drift")
    if any(not isinstance(value,str) or not value.strip() for value in implementation_versions.values()):
        raise AssertionError(f"{label}: implementation-version value missing")


def compatibility_pins(v1):
    c=v1["contracts"]
    reg=c["registry"]
    acq=c["acquisition"]
    fg=c["full_gefs"]
    if "archive" in c:
        archive=c["archive"]
        ensemble=c["ensemble"]
        private_expectation={
            "archive_schema_version":archive["schema_version"],
            "availability_contract_version":archive["availability_contract_version"],
            "ensemble_contract_version":ensemble["contract_version"],
        }
        acq_version=acq["method_version"]
        retention_version=acq["retention_grid_version"]
        gefs_method=fg["source_method_version"]
    else:
        private_expectation=c["private_consumer_expectation"]
        acq_version=acq["acquisition_grid_version"]
        retention_version=acq["retention_grid_version"]
        gefs_method=fg["method_version"]
    return {
        "predecessor_contract_version":v1["contract_version"],
        "registry_version":reg["registry_version"],
        "registry_hashes":{
            "semantic_contract_sha256":reg["semantic_contract_sha256"],
            "provider_matrix_sha256":reg["provider_matrix_sha256"],
            "registry_sha256":reg["registry_sha256"],
        },
        "acquisition_grid_version":acq_version,
        "retention_grid_version":retention_version,
        "full_gefs":{
            "method_version":gefs_method,
            "policy_version":fg["policy_version"],
            "member_count":fg["member_count"],
        },
        "private_consumer_expectation":private_expectation,
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--remote-repo",required=True)
    ap.add_argument("--remote-ref",default="main")
    ap.add_argument("--token-env",default="CROSS_REPO_TOKEN")
    ap.add_argument("--local-contract",default=SUCCESSOR_PATH)
    ap.add_argument("--local-v1",default=FROZEN_V1_PATH)
    args=ap.parse_args()

    local_bytes=Path(args.local_contract).read_bytes()
    local=json.loads(local_bytes)
    require_successor_invariants(local,"local")

    token=os.environ.get(args.token_env,"")
    remote_bytes=fetch_contents(args.remote_repo,SUCCESSOR_PATH,args.remote_ref,token)
    remote=json.loads(remote_bytes)
    require_successor_invariants(remote,"remote")

    if local_bytes!=remote_bytes:
        raise AssertionError(
            "Phase-2 successor contract drift: local bytes differ from "
            f"{args.remote_repo}:{SUCCESSOR_PATH}@{args.remote_ref}"
        )

    local_v1=json.loads(Path(args.local_v1).read_text(encoding="utf-8"))
    remote_v1=json.loads(fetch_contents(args.remote_repo,FROZEN_V1_PATH,args.remote_ref,token))
    local_pins=compatibility_pins(local_v1)
    remote_pins=compatibility_pins(remote_v1)
    if local_pins!=remote_pins:
        raise AssertionError(
            "Phase-2 predecessor producer/consumer compatibility drift:\n"
            + json.dumps({"local":local_pins,"remote":remote_pins},indent=2,sort_keys=True)
        )

    result={
        "status":"pass",
        "remote_repo":args.remote_repo,
        "remote_ref":args.remote_ref,
        "contract_version":local["contract_version"],
        "contract_bytes_sha256":hashlib.sha256(local_bytes).hexdigest(),
        "shared_core_sha256":local["shared_core_sha256"],
        "predecessor_compatibility_pins":local_pins,
    }
    print(json.dumps(result,sort_keys=True))


if __name__=="__main__":
    main()
