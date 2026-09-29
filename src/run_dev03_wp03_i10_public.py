#!/usr/bin/env python3
"""WP03-I10 public live-proof runner.

Reads one already verified private v2 parent, measures a matched same-cycle v2
network baseline from its exact source URLs, performs only the I04-approved
convective-precipitation successor requests, and publishes only to
public_collector_v3.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from dev03_v3_parent_binding import (
    build_bundle_v3_shell,
    build_parent_cycle_inventory,
    load_registry_v3,
    load_verified_parent_payload,
    new_attempt_nonce,
    verify_parent_transfer_receipt,
)
from dev03_v3_convective_acquisition import (
    acquire_convective_successor,
    load_plan,
    plan_parent_pinned_requests,
)
import push_private as private_tx
import push_private_v3

PRIVATE_REPO="janwohlers78/mardorf-kitevorhersage"
API="https://api.github.com"
CONTROL_PATH=Path("config/dev03_wp03_i10_runtime_controls_v1.json")
METHOD_VERSION="dev03-wp03-i10-public-live-runner-v1"
ALLOWED_BASELINE_MODELS={"ICON-D2","ICON-EU","GFS"}


class I10PublicError(RuntimeError):
    pass


def _headers(token):
    return {
        "Authorization":f"Bearer {token}",
        "Accept":"application/vnd.github+json",
        "X-GitHub-Api-Version":"2022-11-28",
        "User-Agent":"mardorf-data-collector/DEV03-I10",
    }


def _content_bytes(repo,path,token,ref="main",*,required=True):
    r=requests.get(
        f"{API}/repos/{repo}/contents/{path}",
        headers=_headers(token),params={"ref":ref},timeout=30,
    )
    if r.status_code==404 and not required:
        return None
    if r.status_code!=200:
        raise I10PublicError(f"private content read failed {path}: HTTP {r.status_code}")
    data=r.json()
    encoding=data.get("encoding")
    if encoding=="base64" and data.get("content"):
        return base64.b64decode(data["content"].replace("\n",""))
    if encoding=="none":
        sha=data.get("sha")
        if not isinstance(sha,str) or not sha:
            raise I10PublicError(f"large private content metadata lacks blob SHA: {path}")
        blob=requests.get(
            f"{API}/repos/{repo}/git/blobs/{sha}",
            headers=_headers(token),timeout=30,
        )
        if blob.status_code!=200:
            raise I10PublicError(f"private git-blob read failed {path}: HTTP {blob.status_code}")
        body=blob.json()
        if body.get("encoding")!="base64" or not body.get("content"):
            raise I10PublicError(f"private git-blob encoding invalid: {path}")
        return base64.b64decode(body["content"].replace("\n",""))
    raise I10PublicError(f"private content encoding invalid: {path}")


def _directory_items(repo,path,token,ref="main"):
    r=requests.get(
        f"{API}/repos/{repo}/contents/{path}",
        headers=_headers(token),params={"ref":ref},timeout=30,
    )
    if r.status_code==404:
        return []
    if r.status_code!=200:
        raise I10PublicError(f"private directory read failed {path}: HTTP {r.status_code}")
    data=r.json()
    if not isinstance(data,list):
        raise I10PublicError(f"private directory path is not a directory: {path}")
    return data


def _strict_json(raw):
    def hook(pairs):
        out={}
        for k,v in pairs:
            if k in out:
                raise I10PublicError(f"duplicate JSON key: {k}")
            out[k]=v
        return out
    return json.loads(raw.decode("utf-8"),object_pairs_hook=hook)


def _controls():
    d=_strict_json(CONTROL_PATH.read_bytes())
    c=d.get("controls") or {}
    generation=c.get("public_v3_generation_enabled")
    transfer=c.get("public_v3_transfer_enabled")
    # PREP05 preferred rollback stage 3 is generation=true/transfer=false.
    # It must stop before any private read or provider request and is therefore
    # a clean disabled state, not a workflow error.
    if transfer is False:
        return False,c
    if transfer is True and generation is not True:
        raise I10PublicError("public v3 transfer cannot be enabled while generation is disabled")
    if generation is not True or transfer is not True:
        raise I10PublicError("public v3 runtime controls are invalid")
    return True,c


def _promoted_parent_locator(snapshot):
    if not isinstance(snapshot,dict):
        raise I10PublicError("promoted canonical model snapshot must be an object")
    ingest=snapshot.get("canonical_ingest") or {}
    if ingest.get("method_version")!="public-collector-canonical-promotion-v1":
        raise I10PublicError("private canonical model snapshot promotion method drift")
    parent_sha=ingest.get("source_payload_sha256")
    payload_path=ingest.get("source_payload_path")
    generated=ingest.get("collector_generated_at_utc")
    if (
        not isinstance(parent_sha,str) or len(parent_sha)!=64
        or not isinstance(payload_path,str)
        or not payload_path.startswith("data/inbox/public_collector/models/")
        or not payload_path.endswith(".json.gz")
        or not isinstance(generated,str)
    ):
        raise I10PublicError("private canonical model snapshot lacks exact promoted-parent identity")
    try:
        when=datetime.fromisoformat(generated.replace("Z","+00:00")).astimezone(timezone.utc)
    except ValueError as exc:
        raise I10PublicError("promoted parent collector time invalid") from exc
    name=Path(payload_path).name
    if not name.startswith("models_") or not name.endswith(".json.gz"):
        raise I10PublicError("promoted parent payload filename is not canonical")
    stamp=name[len("models_"):-len(".json.gz")]
    if not stamp:
        raise I10PublicError("promoted parent payload stamp missing")
    day=when.strftime("%Y/%m/%d")
    receipt_path=f"data/inbox/public_collector/transfer_receipts/models/{day}/receipt_{stamp}.json"
    manifest_path=(
        f"data/weather_archive/manifests/year={when:%Y}/month={when:%m}/day={when:%d}/"
        f"{parent_sha}.json"
    )
    return {
        "parent_v2_payload_sha256":parent_sha,
        "payload_path":payload_path,
        "collector_generated_at_utc":generated,
        "receipt_path":receipt_path,
        "v5_manifest_path":manifest_path,
    }


def load_verified_private_parent(token):
    snapshot_raw=_content_bytes(PRIVATE_REPO,"data/raw/model_snapshots/latest.json",token)
    snapshot=_strict_json(snapshot_raw)
    promoted=_promoted_parent_locator(snapshot)
    generated=promoted["collector_generated_at_utc"]
    when=datetime.fromisoformat(generated.replace("Z","+00:00")).astimezone(timezone.utc)
    age_h=(datetime.now(timezone.utc)-when).total_seconds()/3600
    if age_h < -0.01 or age_h > 48:
        raise I10PublicError(f"paired v2 parent outside LP03 live window: age_hours={age_h:.3f}")

    manifest_raw=_content_bytes(PRIVATE_REPO,promoted["v5_manifest_path"],token)
    manifest=_strict_json(manifest_raw)
    if (
        manifest.get("schema_version")!="mardorf-weather-archive-v5"
        or manifest.get("source_payload_sha256")!=promoted["parent_v2_payload_sha256"]
        or not (manifest.get("files") or [])
    ):
        raise I10PublicError("promoted parent lacks exact valid Archive-v5 manifest")

    immutable=promoted["receipt_path"]
    raw=_content_bytes(PRIVATE_REPO,immutable,token)
    receipt=_strict_json(raw)
    if (
        receipt.get("method_version")!="private-transfer-readback-v2"
        or receipt.get("readback_verified") is not True
        or receipt.get("payload_source_sha256")!=promoted["parent_v2_payload_sha256"]
        or receipt.get("payload_destination")!=promoted["payload_path"]
        or receipt.get("source_generated_at_utc")!=generated
    ):
        raise I10PublicError("promoted parent receipt does not match canonical/v5 authority")
    transfer_result={
        "transfer_receipt_path":immutable,
        "transfer_receipt_sha256":hashlib.sha256(raw).hexdigest(),
        "verified_data_commit_sha":receipt.get("verified_data_commit_sha"),
        "payload_source_sha256":receipt.get("payload_source_sha256"),
        "source_generated_at_utc":receipt.get("source_generated_at_utc"),
    }
    binding=verify_parent_transfer_receipt(raw,transfer_result)
    payload_path=promoted["payload_path"]
    packed=_content_bytes(PRIVATE_REPO,payload_path,token)
    proof=[x for x in receipt.get("readback",[]) if x.get("path")==payload_path]
    if len(proof)!=1 or proof[0].get("exact_bytes_match") is not True:
        raise I10PublicError("paired v2 payload lacks exact readback proof")
    if hashlib.sha256(packed).hexdigest()!=proof[0].get("sha256"):
        raise I10PublicError("paired v2 compressed payload SHA mismatch")
    parent_raw=gzip.decompress(packed)
    if hashlib.sha256(parent_raw).hexdigest()!=promoted["parent_v2_payload_sha256"]:
        raise I10PublicError("paired v2 decompressed payload SHA mismatch")
    parent=load_verified_parent_payload(parent_raw,binding)
    return {
        "binding":binding,
        "parent_raw":parent_raw,
        "parent":parent,
        "receipt":receipt,
        "immutable_receipt_path":immutable,
        "paired_v5_manifest_path":promoted["v5_manifest_path"],
        "paired_v2_compressed_bytes":len(packed),
        "age_hours":age_h,
    }

def _baseline_urls(parent):
    urls={}
    models=parent.get("models") or {}
    for model in sorted(ALLOWED_BASELINE_MODELS):
        for row in models.get(model) or []:
            for url in row.get("source_urls") or []:
                if isinstance(url,str) and url.startswith("https://"):
                    urls[url]=model
            for item in row.get("grib_evidence") or []:
                if isinstance(item,dict):
                    url=item.get("url")
                    if isinstance(url,str) and url.startswith("https://"):
                        urls[url]=model
    if not urls:
        raise I10PublicError("no exact parent source URLs available for matched v2 baseline")
    return urls


def measure_matched_v2_network(parent,workers=12):
    urls=_baseline_urls(parent)
    started=time.monotonic()
    def one(url,model):
        t=time.monotonic()
        r=requests.get(url,timeout=(10,60),allow_redirects=False)
        elapsed=time.monotonic()-t
        if r.status_code!=200:
            raise I10PublicError(f"matched v2 baseline request failed {model}: HTTP {r.status_code}")
        raw=r.content
        if not raw:
            raise I10PublicError(f"matched v2 baseline returned empty body for {model}")
        return {
            "url":url,"model":model,"bytes":len(raw),
            "sha256":hashlib.sha256(raw).hexdigest(),
            "elapsed_seconds":round(elapsed,3),
        }
    rows=[]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(one,u,m):(u,m) for u,m in urls.items()}
        for fut in as_completed(futs):
            rows.append(fut.result())
    rows.sort(key=lambda x:x["url"])
    total=sum(x["bytes"] for x in rows)
    if total<=0:
        raise I10PublicError("matched v2 response-byte measurement is empty")
    return {
        "method_version":"dev03-i10-matched-v2-network-live-v1",
        "request_count":len(rows),
        "response_bytes":total,
        "wall_seconds":round(time.monotonic()-started,3),
        "models":{m:{
            "request_count":sum(x["model"]==m for x in rows),
            "response_bytes":sum(x["bytes"] for x in rows if x["model"]==m),
        } for m in sorted(ALLOWED_BASELINE_MODELS)},
        "responses":rows,
    }


def _budget_reservation_path(day, attempt):
    return (
        "data/inbox/public_collector_v3/resource_budget/"
        f"{day.replace('-', '/')}/reservation_{attempt}.json"
    )


def _validate_budget_reservation(item, *, day, plan):
    if not isinstance(item,dict) or item.get("method_version")!="dev03-v3-daily-budget-reservation-v1":
        raise I10PublicError("invalid DEV03 daily budget reservation")
    if item.get("utc_day")!=day:
        raise I10PublicError("DEV03 daily budget reservation day mismatch")
    for key in ("planned_requests","matched_v2_response_bytes","reserved_response_bytes"):
        value=item.get(key)
        if isinstance(value,bool) or not isinstance(value,int) or value<0:
            raise I10PublicError(f"invalid DEV03 daily budget reservation {key}")
    ratio=float(plan["resource_budget"]["deterministic_network_hard_incremental_ratio_max"])
    if item["reserved_response_bytes"]!=int(item["matched_v2_response_bytes"]*ratio):
        raise I10PublicError("DEV03 daily byte reservation formula drift")
    return item


def _read_daily_reservations(token, day, plan):
    root="data/inbox/public_collector_v3/resource_budget/"+day.replace("-","/")
    rows=[]
    for meta in _directory_items(PRIVATE_REPO,root,token):
        name=meta.get("name")
        if not isinstance(name,str) or not name.startswith("reservation_") or not name.endswith(".json"):
            continue
        raw=_content_bytes(PRIVATE_REPO,f"{root}/{name}",token)
        rows.append(_validate_budget_reservation(_strict_json(raw),day=day,plan=plan))
    return rows


def reserve_daily_budget(token, *, day, attempt, generated, planned_requests, matched_v2_response_bytes, attempt_kind, plan):
    if attempt_kind not in {"initial","retry"}:
        raise I10PublicError("invalid budget reservation attempt_kind")
    ratio=float(plan["resource_budget"]["deterministic_network_hard_incremental_ratio_max"])
    reservation={
        "schema_version":1,
        "method_version":"dev03-v3-daily-budget-reservation-v1",
        "utc_day":day,
        "v3_attempt_id":attempt,
        "v3_generated_at_utc":generated,
        "attempt_kind":attempt_kind,
        "planned_requests":int(planned_requests),
        "matched_v2_response_bytes":int(matched_v2_response_bytes),
        "reserved_response_bytes":int(matched_v2_response_bytes*ratio),
        "operational_authority":"v16-c3-v9",
    }
    path=_budget_reservation_path(day,attempt)
    raw=(json.dumps(reservation,sort_keys=True,separators=(",",":"),allow_nan=False)+"\n").encode()
    h=private_tx.hdr(token)
    private_tx.atomic_commit(
        PRIVATE_REPO,
        [{"path":path,"content":raw,"immutable":True}],
        f"dev03 i10: reserve daily provider budget {attempt}",
        h,
    )
    rows=_read_daily_reservations(token,day,plan)
    by_attempt={x["v3_attempt_id"]:x for x in rows}
    if attempt not in by_attempt:
        raise I10PublicError("published daily budget reservation is not visible")
    request_total=sum(x["planned_requests"] for x in rows)
    baseline_total=sum(x["matched_v2_response_bytes"] for x in rows)
    byte_reserved_total=sum(x["reserved_response_bytes"] for x in rows)
    request_max=int(plan["resource_budget"]["daily_incremental_request_hard_max"])
    byte_max=int(baseline_total*ratio)
    if request_total>request_max:
        raise I10PublicError(
            f"persistent daily provider-request budget exhausted: reserved={request_total} max={request_max}"
        )
    if byte_reserved_total>byte_max:
        raise I10PublicError(
            f"persistent daily deterministic-network byte budget exhausted: reserved={byte_reserved_total} max={byte_max}"
        )
    current=by_attempt[attempt]
    prior_requests=request_total-current["planned_requests"]
    prior_bytes=byte_reserved_total-current["reserved_response_bytes"]
    return {
        "method_version":"dev03-v3-daily-budget-ledger-v1",
        "reservation_path":path,
        "utc_day":day,
        "reservation_count":len(rows),
        "requests_reserved_total":request_total,
        "request_hard_max":request_max,
        "matched_v2_response_bytes_total":baseline_total,
        "response_bytes_reserved_total":byte_reserved_total,
        "response_byte_hard_max":byte_max,
        "prior_requests_reserved":prior_requests,
        "prior_response_bytes_reserved":prior_bytes,
        "retry_requests_reserved_total":sum(
            x["planned_requests"] for x in rows if x.get("attempt_kind")=="retry"
        ),
    }


def _required_provider_live_acceptance(bundle, jobs):
    planned={}
    for job in jobs:
        planned[job["model"]]=planned.get(job["model"],0)+1
    received={}
    failures={}
    for item in bundle.get("provider_evidence") or []:
        model=item.get("model")
        status=(item.get("availability") or {}).get("availability_status")
        if status=="received":
            received[model]=received.get(model,0)+1
        else:
            failures[model]=failures.get(model,0)+1
    missing=[m for m,n in planned.items() if n>0 and received.get(m,0)==0]
    if missing:
        raise I10PublicError(
            "LP03 required provider path has no received successor evidence: "
            + ",".join(sorted(missing))
        )
    return {
        "planned_by_model":planned,
        "received_by_model":received,
        "non_received_by_model":failures,
        "all_planned_provider_paths_have_received_evidence":True,
    }


def run_live(token):
    enabled,controls=_controls()
    if not enabled:
        return {
            "schema_version":1,"method_version":METHOD_VERSION,
            "status":"disabled_noop","controls":controls,
            "provider_requests":0,"private_writes":0,
            "operational_authority":"v16-c3-v9",
        }
    overall_started=time.monotonic()
    parent=load_verified_private_parent(token)
    plan=load_plan()
    registry=load_registry_v3()
    inventory,binding_id=build_parent_cycle_inventory(parent["parent"],registry)
    generated=datetime.now(timezone.utc).isoformat()
    shell=build_bundle_v3_shell(
        parent_binding=parent["binding"],
        parent_cycle_binding_id=binding_id,
        v3_generated_at_utc=generated,
        attempt_nonce=new_attempt_nonce(),
    )
    jobs,_=plan_parent_pinned_requests(
        parent["parent_raw"],parent["binding"],plan=plan,registry=registry
    )
    baseline=measure_matched_v2_network(parent["parent"])
    successor_started=time.monotonic()
    day=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily=reserve_daily_budget(
        token,
        day=day,
        attempt=shell["v3_attempt_id"],
        generated=generated,
        planned_requests=len(jobs),
        matched_v2_response_bytes=baseline["response_bytes"],
        attempt_kind="initial",
        plan=plan,
    )
    budget={
        "utc_day":day,
        "requests_used":daily["prior_requests_reserved"],
        "response_bytes_used":daily["prior_response_bytes_reserved"],
        "retry_requests_used":daily["retry_requests_reserved_total"],
        "matched_v2_response_bytes":daily["matched_v2_response_bytes_total"],
    }
    bundle,budget_after=acquire_convective_successor(
        parent_payload_bytes=parent["parent_raw"],
        parent_binding=parent["binding"],
        bundle_shell=shell,
        budget_state=budget,
        attempt_kind="initial",
        plan=plan,
    )
    provider_acceptance=_required_provider_live_acceptance(bundle,jobs)
    bundle["i10_live_proof"]={
        "method_version":METHOD_VERSION,
        "persistent_daily_budget":daily,
        "required_provider_acceptance":provider_acceptance,
        "paired_v2_receipt_path":parent["immutable_receipt_path"],
        "paired_v5_manifest_path":parent["paired_v5_manifest_path"],
        "paired_v2_age_hours":parent["age_hours"],
        "matched_v2_network":baseline,
        "matched_v2_wall_seconds":baseline["wall_seconds"],
        "successor_runtime_started_after_matched_baseline":True,
        "zero_additional_request_paths":["ECMWF-IFS","GEFS-control","NOAA_GEFS_FULL","ICON-D2-EPS"],
        "operational_authority":"v16-c3-v9",
    }
    if bundle["i04_acquisition"]["deferred"] or budget_after["hard_breach"]:
        raise I10PublicError("I04 hard resource budget blocked live proof before transfer")
    if bundle.get("network_requests_performed",0)>500:
        raise I10PublicError("incremental request hard max exceeded")
    result=push_private_v3.publish_v3(
        bundle=bundle,
        private_token=token,
        paired_v2_compressed_bytes=parent["paired_v2_compressed_bytes"],
    )
    successor_elapsed=time.monotonic()-successor_started
    overall_elapsed=time.monotonic()-overall_started
    hard_max=max(120.0,0.35*float(baseline["wall_seconds"]))
    return {
        "schema_version":1,
        "method_version":METHOD_VERSION,
        "status":"published",
        "parent_v2_payload_sha256":parent["binding"]["parent_v2_payload_sha256"],
        "parent_v2_receipt_path":parent["immutable_receipt_path"],
        "paired_v5_manifest_path":parent["paired_v5_manifest_path"],
        "parent_age_hours":parent["age_hours"],
        "matched_v2_network":baseline,
        "v3_budget":budget_after,
        "persistent_daily_budget":daily,
        "required_provider_acceptance":provider_acceptance,
        "v3_transfer":result,
        "matched_v2_wall_seconds":baseline["wall_seconds"],
        "public_incremental_wall_seconds":round(successor_elapsed,3),
        "public_incremental_wall_hard_max_seconds":round(hard_max,3),
        "public_incremental_wall_hard_gate_pass":successor_elapsed<=hard_max,
        "public_total_measurement_plus_successor_wall_seconds":round(overall_elapsed,3),
        "zero_extra_gefs_or_icon_d2_eps_requests":True,
        "operational_authority":"v16-c3-v9",
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--result-out",default="work/dev03_i10_public_result.json")
    args=ap.parse_args()
    token=os.getenv("PRIVATE_REPO_TOKEN")
    if not token:
        raise I10PublicError("PRIVATE_REPO_TOKEN is not configured")
    result=run_live(token)
    path=Path(args.result_out);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(json.dumps(result,indent=2,sort_keys=True))
    if result.get("status")=="published" and not result.get("public_incremental_wall_hard_gate_pass"):
        raise SystemExit("I10 public incremental runtime hard gate exceeded")


if __name__=="__main__":
    main()
