#!/usr/bin/env python3
"""Attach DWD ICON Tier-A weather context to an existing model snapshot.

Tier A is optional to the wind-critical product.  The collector preserves
provider-native point values and GRIB metadata, records explicit missing
states, and never substitutes zero for unavailable data.
"""
from __future__ import annotations

import argparse
import bz2
import hashlib
import json
import os
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import mardorf_collector.providers.fetch_dwd_additional_models as dwd
import mardorf_collector.providers.fetch_model_data as base
import mardorf_collector.contracts.availability_contract as availability
import mardorf_collector.contracts.cape_native_identity as cape_identity
from mardorf_collector.providers.icon_parameter_probe import CANONICAL, message_metadata, select_exact_message

SNAP=Path(os.getenv("COLLECTOR_MODEL_FILE","work/model_snapshot.json"))
TIER_A=("t_2m","td_2m","relhum_2m","pmsl","ps","tot_prec","clct","aswdir_s","aswdifd_s","cape_ml","cin_ml")
MODEL_MAP={"ICON-D2":"icon-d2","ICON-EU":"icon-eu"}
METHOD_VERSION="phase2-b1-icon-registry-v2-routine-v1"
REGISTRY_VERSION="relevant-meteorology-v2"
WIND_SEMANTICS={"u_10m":"wind_u_10m","v_10m":"wind_v_10m","vmax_10m":"wind_gust_10m"}


def utc(value):
    x=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)


def url_inventory(provider_model,cycle,param):
    directory,hrefs=dwd.directory_hrefs(provider_model,cycle[-2:],param)
    out={}
    for href in sorted(hrefs):
        if cycle not in href or param not in href or "regular-lat-lon" not in href:
            continue
        m=re.search(r"_(\d{3})_",href)
        if m:
            out.setdefault(int(m.group(1)),href)
    return directory,out


def fetch_field(provider_model,cycle,lead,param,url,run,valid):
    started=time.monotonic()
    checked=datetime.now(timezone.utc).isoformat()
    semantic = CANONICAL.get(param) or WIND_SEMANTICS[param]
    stable={"parameter_native":param,"semantic_id":semantic,"registry_version":REGISTRY_VERSION,"value":None,"availability_status":"fetch_error","error_type":"OptionalFieldUnavailable","availability_observed_at_utc":checked}
    diagnostic={"parameter":param,"lead_hours":lead,"status":"fetch_error"}
    if not url:
        stable["availability_status"]="not_yet_published"
        diagnostic.update(status="not_yet_published",reason="not_published_for_selected_cycle_or_lead",elapsed_seconds=round(time.monotonic()-started,3))
        return stable,diagnostic,None
    diagnostic["source_url"]=url
    try:
        response=dwd.S.get(url,timeout=120)
        response.raise_for_status()
        compressed=response.content
        source_sha=hashlib.sha256(compressed).hexdigest()
        diagnostic.update(response_bytes=len(compressed),source_sha256=source_sha)
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            raw=root/f"{provider_model}_{param}_{lead:03d}.grib2"
            selected=root/f"{provider_model}_{param}_{lead:03d}_selected.grib2"
            raw.write_bytes(bz2.decompress(compressed))
            select_exact_message(raw,selected,run,valid)
            meta=message_metadata(selected)
            nearest=base.grib_nearest(selected)
        if len(meta)!=len(nearest):
            raise RuntimeError(f"metadata/nearest row mismatch metadata={len(meta)} nearest={len(nearest)}")
        values=[]
        for m,p in zip(meta,nearest):
            values.append({
                **m,
                "semantic_id":semantic,
                "registry_version":REGISTRY_VERSION,
                "value":p["value"],
                "latitude":p["lat"],
                "longitude":p["lon"],
                "availability_status":"received",
                "availability_evidence_type":"exact_grib_run_valid_selection",
                "availability_observed_at_utc":datetime.now(timezone.utc).isoformat(),
                "field_available_at_utc":datetime.now(timezone.utc).isoformat(),
                "source_sha256":source_sha,
            })
        diagnostic.update(status="received",value_count=len(values),
                          elapsed_seconds=round(time.monotonic()-started,3))
        return values,diagnostic,url
    except Exception as exc:
        diagnostic.update(status="fetch_error",error_type=type(exc).__name__,
                          error_message=str(exc)[:700],
                          elapsed_seconds=round(time.monotonic()-started,3))
        stable["availability_status"]="fetch_error"
        stable["availability_observed_at_utc"]=datetime.now(timezone.utc).isoformat()
        return stable,diagnostic,url



def annotate_registry_semantics(row):
    """Attach active Registry-v2 semantics without altering native field identity."""
    values=row.get("values") or {}
    for parameter,semantic_id in WIND_SEMANTICS.items():
        native=values.get(parameter)
        if isinstance(native,dict):
            items=[native]
        elif isinstance(native,list):
            items=native
        else:
            items=[]
        for item in items:
            if isinstance(item,dict):
                item.setdefault("semantic_id",semantic_id)
                item.setdefault("registry_version",REGISTRY_VERSION)


def attach(snapshot,workers=4,models=None):
    from mardorf_collector.providers.wind_metadata_v1 import capture_jobs, merge_exact_metadata, VERSION as WIND_METADATA_VERSION
    started=datetime.now(timezone.utc)
    selected=set(models or MODEL_MAP)
    unknown=selected-set(MODEL_MAP)
    if unknown:
        raise ValueError(f"unsupported Tier-A models: {sorted(unknown)}")
    jobs=[]
    wind_jobs=[]
    inventories={}
    for model,provider_model in MODEL_MAP.items():
        if model not in selected:
            continue
        rows=snapshot.get("models",{}).get(model,[])
        cycles=sorted({utc(r["run_time_utc"]).strftime("%Y%m%d%H") for r in rows if r.get("run_time_utc")})
        if len(cycles)!=1:
            raise RuntimeError(f"{model} Tier-A requires exactly one bound cycle, got {cycles}")
        cycle=cycles[0]
        for row in rows:
            annotate_registry_semantics(row)
        for param in TIER_A:
            _directory,lead_urls=url_inventory(provider_model,cycle,param)
            inventories[(model,param)]=lead_urls
        for idx,row in enumerate(rows):
            run=utc(row["run_time_utc"]); valid=utc(row["valid_time_utc"]); lead=int(row["forecast_lead_hours"])
            if abs((valid-run).total_seconds()/3600-lead)>1e-6:
                raise RuntimeError(f"{model} lead/time mismatch at {lead}")
            for param,url in capture_jobs(model,row):
                wind_jobs.append((model,idx,param,provider_model,cycle,lead,run,valid,url))
            for param in TIER_A:
                jobs.append((model,idx,param,provider_model,cycle,lead,run,valid,inventories[(model,param)].get(lead)))

    diagnostics=[]
    results={}
    with ThreadPoolExecutor(max_workers=max(1,int(workers))) as pool:
        futures={
            pool.submit(fetch_field,provider_model,cycle,lead,param,url,run,valid):(model,idx,param)
            for model,idx,param,provider_model,cycle,lead,run,valid,url in jobs
        }
        for future in as_completed(futures):
            key=futures[future]
            value,diag,url=future.result()
            results[key]=(value,url)
            diagnostics.append({"model":key[0],**diag})

    wind_diagnostics=[]
    with ThreadPoolExecutor(max_workers=max(1,int(workers))) as pool:
        futures={pool.submit(fetch_field,provider_model,cycle,lead,param,url,run,valid):(model,idx,param,url)
                 for model,idx,param,provider_model,cycle,lead,run,valid,url in wind_jobs}
        for future in as_completed(futures):
            model,idx,param,url=futures[future]
            value,diag,_=future.result()
            merged=merge_exact_metadata(snapshot['models'][model][idx],param,value,source_url=url)
            wind_diagnostics.append({'model':model,**diag,'metadata_join':merged})
    snapshot['wind_native_metadata_capture_v1']={
        'method_version':WIND_METADATA_VERSION,'requested_fields':len(wind_jobs),
        'verified_metadata_fields':sum(d['metadata_join']['joined_fields'] for d in wind_diagnostics),
        'response_bytes':sum(d.get('response_bytes',0) for d in wind_diagnostics),
        'diagnostics':sorted(wind_diagnostics,key=lambda d:(d['model'],d['lead_hours'],d['parameter'])),
        'forecast_values_changed':False,'historical_backfill':False}

    for (model,idx,param),(value,url) in results.items():
        row=snapshot["models"][model][idx]
        items=value if isinstance(value,list) else [value]
        for item in items:
            if isinstance(item,dict):
                item.setdefault("registry_version",REGISTRY_VERSION)
                if param=="cape_ml":
                    identity=cape_identity.identify_item(model,param,item)
                    item["cape_identity_contract_version"]=cape_identity.CONTRACT_VERSION
                    item["cape_native_identity_status"]=identity.get("status")
                    if identity.get("status")=="identified":
                        item["cape_native_identity_id"]=identity["identity_id"]
                    else:
                        item["cape_native_identity_reason"]=identity.get("reason")
        row.setdefault("values",{})[param]=value
        if url:
            urls=row.setdefault("source_urls",[])
            if url not in urls: urls.append(url)

    by_model={}
    for model in MODEL_MAP:
        if model not in selected:
            continue
        rows=[x for x in diagnostics if x["model"]==model]
        by_parameter={}
        for parameter in TIER_A:
            subset=[x for x in rows if x["parameter"]==parameter]
            by_parameter[parameter]={
                "requested_fields":len(subset),
                "received_fields":sum(x["status"]=="received" for x in subset),
                "missing_fields":sum(x["status"]!="received" for x in subset),
                "response_bytes":sum(int(x.get("response_bytes",0)) for x in subset),
            }
        by_model[model]={
            "requested_fields":len(rows),
            "received_fields":sum(x["status"]=="received" for x in rows),
            "missing_fields":sum(x["status"]!="received" for x in rows),
            "response_bytes":sum(int(x.get("response_bytes",0)) for x in rows),
            "elapsed_field_seconds_sum":round(sum(float(x.get("elapsed_seconds",0)) for x in rows),3),
            "status_counts":{s:sum(x["status"]==s for x in rows) for s in sorted({x["status"] for x in rows})},
            "by_parameter":by_parameter,
        }
    completed=datetime.now(timezone.utc)
    # Tier-A mutates existing forecast records after their wind-critical base
    # acquisition. The revision becomes visible only now; native fields keep
    # their own earlier field_available_at_utc when the response succeeded.
    for model in MODEL_MAP:
        if model not in selected:
            continue
        availability.stamp_rows(
            snapshot.get("models",{}).get(model,[]),
            observed_at=completed.isoformat(),
            replace_row_time=True,
        )
    summary={
        "schema_version":1,
        "method_version":METHOD_VERSION,
        "registry_version":REGISTRY_VERSION,
        "parameters":list(TIER_A),
        "semantic_mapping":{**WIND_SEMANTICS,**{p:CANONICAL[p] for p in TIER_A}},
        "started_at_utc":started.isoformat(),
        "completed_at_utc":completed.isoformat(),
        "wall_duration_seconds":round((completed-started).total_seconds(),3),
        "worker_count":max(1,int(workers)),
        "normalization_performed":False,
        "analysis_changed":False,
        "missing_policy":"explicit_missing_or_fetch_error_never_zero_fill",
        "models":by_model,
        "total_response_bytes":sum(x["response_bytes"] for x in by_model.values()),
        "diagnostics":sorted(diagnostics,key=lambda x:(x["model"],x["lead_hours"],x["parameter"])),
    }
    snapshot["icon_tier_a_acquisition"]=summary
    snapshot["retrieved_at_utc"]=completed.isoformat()
    return summary


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--workers",type=int,default=4)
    ap.add_argument("--models",nargs="+",choices=sorted(MODEL_MAP))
    args=ap.parse_args()
    if not SNAP.exists(): raise SystemExit(f"snapshot missing: {SNAP}")
    snapshot=json.loads(SNAP.read_text(encoding="utf-8"))
    summary=attach(snapshot,args.workers,args.models)
    SNAP.write_text(json.dumps(snapshot,separators=(",",":"),allow_nan=False)+"\n",encoding="utf-8")
    print(json.dumps({
        "method_version":summary["method_version"],
        "wall_duration_seconds":summary["wall_duration_seconds"],
        "total_response_bytes":summary["total_response_bytes"],
        "models":summary["models"],
        "output_bytes":SNAP.stat().st_size,
    },indent=2))
    # Tier A is optional: field misses are explicit and audited but do not turn
    # an otherwise valid wind bundle into a failed collection.


if __name__=="__main__":
    main()
