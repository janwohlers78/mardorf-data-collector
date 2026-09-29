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
from dev03_v3_convective_acquisition import acquire_convective_successor, load_plan
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


def _content_bytes(repo,path,token,ref="main"):
    r=requests.get(
        f"{API}/repos/{repo}/contents/{path}",
        headers=_headers(token),params={"ref":ref},timeout=30,
    )
    if r.status_code!=200:
        raise I10PublicError(f"private content read failed {path}: HTTP {r.status_code}")
    data=r.json()
    if data.get("encoding")!="base64" or not data.get("content"):
        raise I10PublicError(f"private content encoding invalid: {path}")
    return base64.b64decode(data["content"].replace("\n",""))


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
    if generation is False and transfer is False:
        return False,c
    if generation is not True or transfer is not True:
        raise I10PublicError("public v3 generation/transfer controls must be enabled together")
    return True,c


def load_verified_private_parent(token):
    latest_path="data/inbox/public_collector/transfer_receipts/models/latest.json"
    latest_raw=_content_bytes(PRIVATE_REPO,latest_path,token)
    latest=_strict_json(latest_raw)
    if latest.get("method_version")!="private-transfer-readback-v2" or latest.get("readback_verified") is not True:
        raise I10PublicError("latest v2 model transfer is not verified")
    stamp=latest.get("stamp")
    generated=latest.get("source_generated_at_utc")
    if not isinstance(stamp,str) or not isinstance(generated,str):
        raise I10PublicError("latest v2 receipt lacks stamp/generated time")
    when=datetime.fromisoformat(generated.replace("Z","+00:00")).astimezone(timezone.utc)
    age_h=(datetime.now(timezone.utc)-when).total_seconds()/3600
    if age_h < -0.01 or age_h > 48:
        raise I10PublicError(f"paired v2 parent outside LP03 live window: age_hours={age_h:.3f}")
    day=when.strftime("%Y/%m/%d")
    immutable=f"data/inbox/public_collector/transfer_receipts/models/{day}/receipt_{stamp}.json"
    raw=_content_bytes(PRIVATE_REPO,immutable,token)
    receipt=_strict_json(raw)
    if receipt != latest:
        raise I10PublicError("latest receipt bytes differ from immutable paired receipt")
    transfer_result={
        "transfer_receipt_path":immutable,
        "transfer_receipt_sha256":hashlib.sha256(raw).hexdigest(),
        "verified_data_commit_sha":receipt.get("verified_data_commit_sha"),
        "payload_source_sha256":receipt.get("payload_source_sha256"),
        "source_generated_at_utc":receipt.get("source_generated_at_utc"),
    }
    binding=verify_parent_transfer_receipt(raw,transfer_result)
    payload_path=receipt.get("payload_destination")
    packed=_content_bytes(PRIVATE_REPO,payload_path,token)
    proof=[x for x in receipt.get("readback",[]) if x.get("path")==payload_path]
    if len(proof)!=1 or proof[0].get("exact_bytes_match") is not True:
        raise I10PublicError("paired v2 payload lacks exact readback proof")
    if hashlib.sha256(packed).hexdigest()!=proof[0].get("sha256"):
        raise I10PublicError("paired v2 compressed payload SHA mismatch")
    parent_raw=gzip.decompress(packed)
    parent=load_verified_parent_payload(parent_raw,binding)
    return {
        "binding":binding,
        "parent_raw":parent_raw,
        "parent":parent,
        "receipt":receipt,
        "immutable_receipt_path":immutable,
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
    baseline=measure_matched_v2_network(parent["parent"])
    successor_started=time.monotonic()
    budget={
        "utc_day":datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "requests_used":0,
        "response_bytes_used":0,
        "retry_requests_used":0,
        "matched_v2_response_bytes":baseline["response_bytes"],
    }
    bundle,budget_after=acquire_convective_successor(
        parent_payload_bytes=parent["parent_raw"],
        parent_binding=parent["binding"],
        bundle_shell=shell,
        budget_state=budget,
        attempt_kind="initial",
        plan=plan,
    )
    bundle["i10_live_proof"]={
        "method_version":METHOD_VERSION,
        "paired_v2_receipt_path":parent["immutable_receipt_path"],
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
        "parent_age_hours":parent["age_hours"],
        "matched_v2_network":baseline,
        "v3_budget":budget_after,
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
