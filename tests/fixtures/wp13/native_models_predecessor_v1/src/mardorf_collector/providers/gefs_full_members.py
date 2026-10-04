#!/usr/bin/env python3
"""Sparse once-daily NOAA GEFS full-member acquisition for Phase 2F-3.

Policy:
- one authoritative full-member cycle per day: 00Z only;
- c00 + p01-p30;
- no full-member leads <=48 h;
- sparse native leads through 840 h;
- 0.25-degree pgrb2s through 240 h;
- 0.50-degree pgrb2a after 240 h;
- pgrb2b is NOT duplicated here: the existing GEFS-control compatibility path
  already retains c00 supplemental fields. Perturbed-member pgrb2b is
  explicitly not requested by policy.
- raw GRIB bytes are ephemeral.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

import mardorf_collector.providers.noaa_weather_context as noaa

LAT=52.4942
LON=9.3418
METHOD_VERSION="phase2f3-gefs-full-members-v1"
POLICY_VERSION="gefs-sparse-00z-policy-v1"
REGISTRY_VERSION="relevant-meteorology-v1"
ENSEMBLE_SYSTEM_ID="NOAA_GEFS"

MEMBERS=("c00",)+tuple(f"p{x:02d}" for x in range(1,31))
MEMBER_ROLES={"c00":"control_member",**{f"p{x:02d}":"perturbed_member" for x in range(1,31)}}

NEAR_LEADS=(60,72,84,96,108,120,144,168,192,216,240)
FAR_LEADS=(288,336,384,432,480,528,576,624,672,720,768,816,840)
LEADS=NEAR_LEADS+FAR_LEADS

# One filtered response per member+lead with all requested variables.
PRODUCTS={
    "gefs_0p25s":{
        "script":"filter_gefs_atmos_0p25s.pl",
        "directory":"pgrb2sp25",
        "suffix":"pgrb2s.0p25",
        "variables":("UGRD","VGRD","GUST","APCP","TCDC","CAPE","CIN"),
        "levels":("10_m_above_ground","surface","entire_atmosphere","180-0_mb_above_ground"),
        "padding":0.30,
        "semantics":(
            "wind_u_10m","wind_v_10m","wind_gust_10m","total_precipitation",
            "total_cloud_cover","cape","cin",
        ),
    },
    "gefs_0p50a":{
        "script":"filter_gefs_atmos_0p50a.pl",
        "directory":"pgrb2ap5",
        "suffix":"pgrb2a.0p50",
        "variables":("UGRD","VGRD","APCP","TCDC","CAPE","CIN"),
        "levels":("10_m_above_ground","surface","entire_atmosphere","180-0_mb_above_ground"),
        "padding":0.75,
        "semantics":(
            "wind_u_10m","wind_v_10m","total_precipitation",
            "total_cloud_cover","cape","cin",
        ),
    },
}

MAX_REQUESTS_PER_DAY=800
MAX_CONCURRENCY=2
MIN_REQUEST_INTERVAL_SECONDS=0.40
MAX_ATTEMPTS=4
QA_LEADS=(120,240)
QA_TOLERANCE_MEAN_MS=0.01
QA_TOLERANCE_SPREAD_MS=0.01

_thread=threading.local()
_rate_lock=threading.Lock()
_next_request_at=0.0

def utc(value):
    x=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    if x.tzinfo is None:
        x=x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def member_prefix(member):
    if member=="c00":
        return "gec00"
    if not re.fullmatch(r"p(?:0[1-9]|[12][0-9]|30)",member):
        raise ValueError(f"invalid GEFS member: {member}")
    return "ge"+member

def product_for_lead(lead):
    if lead in NEAR_LEADS:
        return "gefs_0p25s"
    if lead in FAR_LEADS:
        return "gefs_0p50a"
    raise ValueError(f"lead is outside sparse GEFS policy: {lead}")

def request_url(run,member,lead,product=None):
    run=utc(run)
    if run.hour!=0:
        raise ValueError("full GEFS member acquisition is 00Z-only")
    product=product or product_for_lead(int(lead))
    spec=PRODUCTS[product]
    if product!=product_for_lead(int(lead)):
        raise ValueError(f"wrong product {product} for sparse lead {lead}")
    prefix=member_prefix(member)
    hh=run.strftime("%H")
    filename=f"{prefix}.t{hh}z.{spec['suffix']}.f{int(lead):03d}"
    p=float(spec["padding"])
    q={
        "file":filename,
        "dir":f"/gefs.{run:%Y%m%d}/{hh}/atmos/{spec['directory']}",
        "subregion":"",
        "leftlon":f"{LON-p:.4f}","rightlon":f"{LON+p:.4f}",
        "toplat":f"{LAT+p:.4f}","bottomlat":f"{LAT-p:.4f}",
    }
    for v in spec["variables"]:
        q["var_"+v]="on"
    for level in spec["levels"]:
        q["lev_"+level]="on"
    return "https://nomads.ncep.noaa.gov/cgi-bin/"+spec["script"]+"?"+urlencode(q)

def request_plan(run):
    run=utc(run)
    if run.hour!=0:
        return []
    plan=[]
    for lead in LEADS:
        product=product_for_lead(lead)
        for member in MEMBERS:
            plan.append({
                "member_id":member,
                "member_role":MEMBER_ROLES[member],
                "lead_hours":lead,
                "valid_time_utc":(run+timedelta(hours=lead)).isoformat(),
                "provider_product":product,
                "semantics":list(PRODUCTS[product]["semantics"]),
                "url":request_url(run,member,lead,product),
            })
    if len(plan)!=len(MEMBERS)*len(LEADS):
        raise AssertionError("unexpected sparse GEFS request count")
    if len(plan)>MAX_REQUESTS_PER_DAY:
        raise AssertionError(f"sparse GEFS plan exceeds request gate: {len(plan)}>{MAX_REQUESTS_PER_DAY}")
    return plan

def policy_summary():
    return {
        "method_version":METHOD_VERSION,
        "policy_version":POLICY_VERSION,
        "registry_version":REGISTRY_VERSION,
        "ensemble_system_id":ENSEMBLE_SYSTEM_ID,
        "full_member_cycles_utc":[0],
        "expected_member_ids":list(MEMBERS),
        "member_roles":dict(MEMBER_ROLES),
        "near_leads_hours":list(NEAR_LEADS),
        "far_leads_hours":list(FAR_LEADS),
        "all_leads_hours":list(LEADS),
        "member_request_count_per_full_cycle":len(MEMBERS)*len(LEADS),
        "provider_summary_qa_request_count":len(QA_LEADS)*2,
        "request_count_per_full_cycle":len(MEMBERS)*len(LEADS)+len(QA_LEADS)*2,
        "max_requests_per_day":MAX_REQUESTS_PER_DAY,
        "max_concurrency":MAX_CONCURRENCY,
        "minimum_request_interval_seconds":MIN_REQUEST_INTERVAL_SECONDS,
        "max_attempts_per_request":MAX_ATTEMPTS,
        "pgrb2b_perturbed_members":"not_requested_by_policy",
        "pgrb2b_c00":"retained by existing GEFS-control compatibility/full-horizon path; not duplicated here",
        "native_time_policy":"provider-native selected forecast times only; no interpolation",
        "conditional_forecast_triggered_fetch":False,
    }

def source_matches_policy(source,run):
    if not isinstance(source,dict):
        return False
    try:
        sr=utc(source.get("run_time_utc"))
    except Exception:
        return False
    return (
        sr==utc(run)
        and source.get("method_version")==METHOD_VERSION
        and source.get("policy_version")==POLICY_VERSION
        and source.get("collection_status")=="complete"
        and source.get("expected_request_count")==len(MEMBERS)*len(LEADS)
        and source.get("received_request_count")==len(MEMBERS)*len(LEADS)
    )

def _probe_url(run,lead=840):
    # Minimal c00 U10 request used only to prove publication of the terminal
    # sparse 00Z horizon before selecting a cycle.
    run=utc(run)
    p=0.75
    q={
        "file":f"gec00.t00z.pgrb2a.0p50.f{int(lead):03d}",
        "dir":f"/gefs.{run:%Y%m%d}/00/atmos/pgrb2ap5",
        "subregion":"",
        "leftlon":f"{LON-p:.4f}","rightlon":f"{LON+p:.4f}",
        "toplat":f"{LAT+p:.4f}","bottomlat":f"{LAT-p:.4f}",
        "lev_10_m_above_ground":"on","var_UGRD":"on",
    }
    return "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p50a.pl?"+urlencode(q)

def discover_mature_00z(now=None,session=None,max_days=4):
    now=utc(now or datetime.now(timezone.utc))
    session=session or requests.Session()
    session.headers.update({"User-Agent":"mardorf-data-collector/gefs-full-cycle-probe-v1"})
    attempts=[]
    for dd in range(max_days):
        run=datetime.combine((now-timedelta(days=dd)).date(),datetime.min.time(),tzinfo=timezone.utc)
        url=_probe_url(run,840)
        try:
            r=session.get(url,timeout=(10,45))
            ok=r.status_code==200 and r.content[:4]==b"GRIB"
            attempts.append({"run_time_utc":run.isoformat(),"http_status":r.status_code,
                             "bytes":len(r.content),"published":ok})
            if ok:
                return run,attempts
        except Exception as exc:
            attempts.append({"run_time_utc":run.isoformat(),"published":False,
                             "exception_type":type(exc).__name__,"exception_message":str(exc)[:300]})
    raise RuntimeError(f"no mature GEFS 00Z full-member cycle found: {attempts}")

def _session():
    s=getattr(_thread,"session",None)
    if s is None:
        s=requests.Session()
        s.headers.update({"User-Agent":"mardorf-data-collector/gefs-full-members-v1"})
        _thread.session=s
    return s

def _rate_wait():
    global _next_request_at
    with _rate_lock:
        now=time.monotonic()
        start=max(now,_next_request_at)
        _next_request_at=start+MIN_REQUEST_INTERVAL_SECONDS
    delay=start-now
    if delay>0:
        time.sleep(delay)

def _download(url,attempts=MAX_ATTEMPTS):
    last=None
    for attempt in range(1,attempts+1):
        _rate_wait()
        started=time.monotonic()
        retry_after=None
        try:
            r=_session().get(url,timeout=(10,90))
            elapsed=time.monotonic()-started
            if r.status_code==200 and r.content[:4]==b"GRIB":
                return r.content,elapsed,attempt
            retry_after=r.headers.get("Retry-After")
            last=RuntimeError(
                f"HTTP {r.status_code}, bytes={len(r.content)}, retry_after={retry_after!r}")
            retryable=r.status_code in (408,425,429,500,502,503,504)
            if not retryable:
                break
        except Exception as exc:
            elapsed=time.monotonic()-started
            last=exc
        if attempt<attempts:
            delay=min(15.0,1.5*(2**(attempt-1)))+random.uniform(0.0,0.35)
            if retry_after is not None:
                try:
                    delay=max(delay,min(30.0,float(retry_after)))
                except (TypeError,ValueError):
                    pass
            time.sleep(delay)
    raise RuntimeError(f"GEFS request failed after {attempts} attempts: {last}")

def _field_key(item,product):
    identity={
        "provider_product":product,
        "shortName":item.get("shortName"),
        "paramId":item.get("paramId"),
        "typeOfLevel":item.get("typeOfLevel"),
        "level":item.get("level"),
        "stepType":item.get("stepType"),
        "units":item.get("units"),
    }
    return hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(",",":")).encode()).hexdigest()[:24]

def _fetch_unit(run,unit):
    started=datetime.now(timezone.utc)
    try:
        raw,elapsed,attempt_count=_download(unit["url"])
        sha=hashlib.sha256(raw).hexdigest()
        with tempfile.NamedTemporaryFile(suffix=".grib2") as fh:
            fh.write(raw);fh.flush()
            values,point=noaa.extract_native_values(
                Path(fh.name),LAT,LON,source_sha256=sha,product=unit["provider_product"])
        fields=[]
        for native,items in values.items():
            for item in items:
                semantic=item.get("semantic_id")
                if semantic not in set(unit["semantics"]):
                    continue
                fields.append({
                    "field_key":_field_key(item,unit["provider_product"]),
                    "semantic_id":semantic,
                    "parameter_native":str(item.get("shortName",native)),
                    "param_id_native":None if item.get("paramId") is None else str(item.get("paramId")),
                    "field_provider_product":unit["provider_product"],
                    "type_of_level_native":None if item.get("typeOfLevel") is None else str(item.get("typeOfLevel")),
                    "level_native":None if item.get("level") is None else str(item.get("level")),
                    "step_type_native":None if item.get("stepType") is None else str(item.get("stepType")),
                    "step_range_native":None if item.get("stepRange") is None else str(item.get("stepRange")),
                    "start_step_native":item.get("startStep"),
                    "end_step_native":item.get("endStep"),
                    "step_units_native":None if item.get("stepUnits") is None else str(item.get("stepUnits")),
                    "unit_native":item.get("units"),
                    "value_native":float(item["value"]),
                    "source_sha256":sha,
                })
        if not fields:
            raise RuntimeError("filtered GEFS response contained no Registry-v1 authoritative fields")
        returned_semantics=sorted({x["semantic_id"] for x in fields})
        missing=sorted(set(unit["semantics"])-set(returned_semantics))
        if missing:
            raise RuntimeError(f"GEFS response missing requested Registry semantics: {missing}")
        return {
            **{k:unit[k] for k in ("member_id","member_role","lead_hours","valid_time_utc","provider_product")},
            "run_time_utc":utc(run).isoformat(),
            "request_status":"received",
            "retrieved_at_utc":started.isoformat(),
            "response_bytes":len(raw),
            "response_sha256":sha,
            "attempt_count":attempt_count,
            "elapsed_seconds":round(elapsed,6),
            "returned_coordinate":point,
            "fields":fields,
        }
    except Exception as exc:
        return {
            **{k:unit[k] for k in ("member_id","member_role","lead_hours","valid_time_utc","provider_product")},
            "run_time_utc":utc(run).isoformat(),
            "request_status":"fetch_error",
            "retrieved_at_utc":started.isoformat(),
            "response_bytes":0,
            "attempt_count":MAX_ATTEMPTS,
            "exception_type":type(exc).__name__,
            "exception_message":str(exc)[:1000],
            "fields":[],
        }

def _summary_qa_filtered(run,product,lead):
    run=utc(run);pad=.30
    q={
        "file":f"{product}.t00z.pgrb2s.0p25.f{int(lead):03d}",
        "dir":f"/gefs.{run:%Y%m%d}/00/atmos/pgrb2sp25",
        "subregion":"",
        "leftlon":f"{LON-pad:.4f}","rightlon":f"{LON+pad:.4f}",
        "toplat":f"{LAT+pad:.4f}","bottomlat":f"{LAT-pad:.4f}",
        "lev_10_m_above_ground":"on","var_UGRD":"on","var_VGRD":"on",
    }
    url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl?"+urlencode(q)
    raw,elapsed,attempts=_download(url)
    with tempfile.NamedTemporaryFile(suffix=".grib2") as fh:
        fh.write(raw);fh.flush()
        values,_point=noaa.extract_native_values(Path(fh.name),LAT,LON,source_sha256=hashlib.sha256(raw).hexdigest(),product="gefs_0p25s")
    def one(*names):
        for name in names:
            if name in values and values[name]:
                return float(values[name][0]["value"])
        raise RuntimeError(f"summary QA missing {names}")
    return {
        "wind_u_10m":one("10u","u"),
        "wind_v_10m":one("10v","v"),
        "response_bytes":len(raw),
        "response_sha256":hashlib.sha256(raw).hexdigest(),
        "elapsed_seconds":elapsed,
        "attempt_count":attempts,
        "url":url,
    }


def provider_summary_qa(run,records):
    """Cross-check provider geavg/gespr against p01-p30; c00 is excluded by NOAA."""
    run=utc(run)
    comparisons=[];request_count=0;response_bytes=0;attempts=0;elapsed=0.0
    try:
        for lead in QA_LEADS:
            lead_records={r["member_id"]:r for r in records if int(r["lead_hours"])==lead and r.get("request_status")=="received"}
            if set(lead_records)!=set(MEMBERS):
                raise RuntimeError(f"QA lead {lead} lacks complete 31-member set")
            mean_product=_summary_qa_filtered(run,"geavg",lead)
            spread_product=_summary_qa_filtered(run,"gespr",lead)
            request_count+=2
            response_bytes+=mean_product["response_bytes"]+spread_product["response_bytes"]
            attempts+=mean_product["attempt_count"]+spread_product["attempt_count"]
            elapsed+=mean_product["elapsed_seconds"]+spread_product["elapsed_seconds"]
            for semantic in ("wind_u_10m","wind_v_10m"):
                perturbed=[]
                for member in MEMBERS:
                    if member=="c00": continue
                    fields=[x for x in lead_records[member].get("fields") or [] if x.get("semantic_id")==semantic]
                    if len(fields)!=1:
                        raise RuntimeError(f"QA {lead} {member} {semantic} count={len(fields)}")
                    perturbed.append(float(fields[0]["value_native"]))
                derived_mean=statistics.fmean(perturbed)
                derived_spread=statistics.stdev(perturbed)
                provider_mean=float(mean_product[semantic])
                provider_spread=float(spread_product[semantic])
                mean_delta=abs(derived_mean-provider_mean)
                spread_delta=abs(derived_spread-provider_spread)
                comparisons.append({
                    "lead_hours":lead,"semantic_id":semantic,
                    "member_set":"p01-p30","member_count":30,
                    "derived_mean":derived_mean,"provider_mean":provider_mean,
                    "mean_abs_delta":mean_delta,
                    "derived_sample_stdev":derived_spread,"provider_spread":provider_spread,
                    "spread_abs_delta":spread_delta,
                    "mean_tolerance_ms":QA_TOLERANCE_MEAN_MS,
                    "spread_tolerance_ms":QA_TOLERANCE_SPREAD_MS,
                    "status":"pass" if mean_delta<=QA_TOLERANCE_MEAN_MS and spread_delta<=QA_TOLERANCE_SPREAD_MS else "fail",
                    "provider_mean_source_sha256":mean_product["response_sha256"],
                    "provider_spread_source_sha256":spread_product["response_sha256"],
                })
        status="pass" if comparisons and all(x["status"]=="pass" for x in comparisons) else "fail"
        return {
            "method_version":"gefs-provider-summary-qa-v1",
            "measurement_basis_run_id":36295332340,
            "member_semantics":"NOAA geavg/gespr empirically reproduce p01-p30 only; c00 excluded",
            "spread_semantics":"sample standard deviation over p01-p30 (N-1 denominator)",
            "leads_hours":list(QA_LEADS),
            "mean_tolerance_ms":QA_TOLERANCE_MEAN_MS,
            "spread_tolerance_ms":QA_TOLERANCE_SPREAD_MS,
            "status":status,
            "comparisons":comparisons,
            "request_metrics":{"requests":request_count,"response_bytes":response_bytes,
                               "total_attempts":attempts,"http_elapsed_seconds_sum":round(elapsed,6)},
        }
    except Exception as exc:
        return {
            "method_version":"gefs-provider-summary-qa-v1",
            "measurement_basis_run_id":36295332340,
            "member_semantics":"NOAA geavg/gespr expected p01-p30 only; c00 excluded",
            "spread_semantics":"sample standard deviation over p01-p30 (N-1 denominator)",
            "leads_hours":list(QA_LEADS),
            "mean_tolerance_ms":QA_TOLERANCE_MEAN_MS,
            "spread_tolerance_ms":QA_TOLERANCE_SPREAD_MS,
            "status":"unavailable",
            "exception_type":type(exc).__name__,
            "exception_message":str(exc)[:1000],
            "comparisons":[],
            "request_metrics":{"requests":request_count,"response_bytes":response_bytes,
                               "total_attempts":attempts,"http_elapsed_seconds_sum":round(elapsed,6)},
        }


def build_source(run,records,cycle_probe=None):
    run=utc(run)
    records=sorted(records,key=lambda x:(x["lead_hours"],x["member_id"]))
    expected=len(MEMBERS)*len(LEADS)
    received=sum(x.get("request_status")=="received" for x in records)
    if len(records)!=expected:
        raise ValueError(f"GEFS source request inventory mismatch: {len(records)} != {expected}")
    # Each member/lead pair must appear exactly once.
    keys=[(x["member_id"],int(x["lead_hours"])) for x in records]
    if len(set(keys))!=expected:
        raise ValueError("GEFS source contains duplicate member/lead request units")
    payload_hash=hashlib.sha256(json.dumps([
        [x["member_id"],x["lead_hours"],x.get("response_sha256"),x.get("request_status")]
        for x in records
    ],sort_keys=True,separators=(",",":")).encode()).hexdigest()
    coords_by_product={}
    for product in PRODUCTS:
        coords=[x.get("returned_coordinate") for x in records
                if x.get("request_status")=="received" and x.get("provider_product")==product
                and isinstance(x.get("returned_coordinate"),dict)]
        if not coords:
            continue
        point=coords[0]
        if any(
            abs(float(x.get("latitude"))-float(point.get("latitude")))>1e-9
            or abs(float(x.get("longitude"))-float(point.get("longitude")))>1e-9
            for x in coords
        ):
            raise ValueError(f"GEFS full-member grid point changed within {product}")
        coords_by_product[product]=point
    source={
        "schema_version":1,
        "method_version":METHOD_VERSION,
        "policy_version":POLICY_VERSION,
        "registry_version":REGISTRY_VERSION,
        "ensemble_system_id":ENSEMBLE_SYSTEM_ID,
        "run_time_utc":run.isoformat(),
        "retrieved_at_utc":datetime.now(timezone.utc).isoformat(),
        "response_sha256":payload_hash,
        "returned_coordinates_by_product":coords_by_product,
        "identity_evidence_tier":"provider_embedded",
        "expected_member_ids":list(MEMBERS),
        "member_roles":dict(MEMBER_ROLES),
        "times_utc":[(run+timedelta(hours=h)).isoformat() for h in LEADS],
        "leads_hours":list(LEADS),
        "native_time_policy":"provider-native selected sparse times only; no interpolation",
        "records":records,
        "expected_request_count":expected,
        "received_request_count":received,
        "failed_request_count":expected-received,
        "collection_status":"complete" if received==expected else "partial",
        "cycle_probe":cycle_probe,
        "policy_omissions":[{
            "provider_product":"gefs_0p50b",
            "member_ids":[f"p{x:02d}" for x in range(1,31)],
            "leads_hours":list(FAR_LEADS),
            "availability_status":"not_requested_by_policy",
            "reason":"far supplemental fields are retained via c00 compatibility/QA only; perturbed-member duplication is outside optimized 2F3 policy",
        }],
        "qa_control_source":"existing GEFS-control compatibility/full-horizon path retains c00 pgrb2b where provider publishes it",
        "request_metrics":{
            "requests":expected,
            "response_bytes":sum(int(x.get("response_bytes") or 0) for x in records),
            "http_elapsed_seconds_sum":round(sum(float(x.get("elapsed_seconds") or 0) for x in records),6),
            "total_attempts":sum(int(x.get("attempt_count") or 0) for x in records),
        },
    }
    return source

def collect(run,workers=MAX_CONCURRENCY):
    run=utc(run)
    plan=request_plan(run)
    started=time.monotonic()
    records=[]
    with ThreadPoolExecutor(max_workers=max(1,min(int(workers),MAX_CONCURRENCY))) as ex:
        futures={ex.submit(_fetch_unit,run,unit):(unit["member_id"],unit["lead_hours"]) for unit in plan}
        for fut in as_completed(futures):
            records.append(fut.result())
    source=build_source(run,records)
    if source["collection_status"]=="complete":
        source["provider_summary_qa"]=provider_summary_qa(run,records)
        qa_metrics=(source["provider_summary_qa"].get("request_metrics") or {})
        member_metrics=source["request_metrics"]
        source["traffic_metrics"]={
            "member_requests":member_metrics["requests"],
            "provider_summary_qa_requests":int(qa_metrics.get("requests") or 0),
            "total_requests":member_metrics["requests"]+int(qa_metrics.get("requests") or 0),
            "member_response_bytes":member_metrics["response_bytes"],
            "provider_summary_qa_response_bytes":int(qa_metrics.get("response_bytes") or 0),
            "total_response_bytes":member_metrics["response_bytes"]+int(qa_metrics.get("response_bytes") or 0),
        }
    source["collection_wall_seconds"]=round(time.monotonic()-started,6)
    return source

def attach_to_snapshot(path,source):
    """Promote only a complete full-member cycle; retain partial attempt diagnostics.

    This keeps an optional ensemble failure from replacing the last complete
    archive source or blocking deterministic model freshness.
    """
    p=Path(path)
    data=json.loads(p.read_text(encoding="utf-8"))
    if source.get("collection_status")=="complete":
        data["gefs_full_member_source"]=source
        data.pop("gefs_full_member_attempt",None)
    else:
        failed=[
            {"member_id":r.get("member_id"),"lead_hours":r.get("lead_hours"),
             "provider_product":r.get("provider_product"),
             "exception_type":r.get("exception_type"),"exception_message":r.get("exception_message")}
            for r in source.get("records") or [] if r.get("request_status")!="received"
        ]
        by_reason={}
        by_product={}
        for item in failed:
            reason=str(item.get("exception_message") or item.get("exception_type") or "unknown")
            by_reason[reason]=by_reason.get(reason,0)+1
            product=str(item.get("provider_product") or "unknown")
            by_product[product]=by_product.get(product,0)+1
        data["gefs_full_member_attempt"]={
            "method_version":METHOD_VERSION,
            "policy_version":POLICY_VERSION,
            "run_time_utc":source.get("run_time_utc"),
            "attempted_at_utc":source.get("retrieved_at_utc"),
            "collection_status":source.get("collection_status"),
            "expected_request_count":source.get("expected_request_count"),
            "received_request_count":source.get("received_request_count"),
            "failed_request_count":source.get("failed_request_count"),
            "request_metrics":source.get("request_metrics"),
            "collection_wall_seconds":source.get("collection_wall_seconds"),
            "failure_counts_by_product":by_product,
            "failure_counts_by_reason":by_reason,
            "failed_units":failed,
        }
    data["retrieved_at_utc"]=datetime.now(timezone.utc).isoformat()
    p.write_text(json.dumps(data,separators=(",",":"),allow_nan=False)+"\n",encoding="utf-8")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input",default=os.getenv("COLLECTOR_MODEL_FILE","work/model_snapshot.json"))
    ap.add_argument("--workers",type=int,default=MAX_CONCURRENCY)
    ap.add_argument("--run")
    ap.add_argument("--plan-only",action="store_true")
    ap.add_argument("--force",action="store_true")
    args=ap.parse_args()
    if args.run:
        run=utc(args.run)
        attempts=[]
    else:
        run,attempts=discover_mature_00z()
    plan=request_plan(run)
    if args.plan_only:
        print(json.dumps({"run_time_utc":run.isoformat(),"request_count":len(plan),
                          "policy":policy_summary(),"cycle_probe":attempts},indent=2,sort_keys=True))
        return
    p=Path(args.input)
    if p.exists() and not args.force:
        data=json.loads(p.read_text(encoding="utf-8"))
        if source_matches_policy(data.get("gefs_full_member_source"),run):
            print(json.dumps({"status":"carry_forward","run_time_utc":run.isoformat(),
                              "reason":"same_complete_sparse_00z_source_already_present"},indent=2))
            return
    source=collect(run,args.workers)
    source["cycle_probe"]=attempts
    attach_to_snapshot(args.input,source)
    print(json.dumps({
        "status":source["collection_status"],
        "run_time_utc":source["run_time_utc"],
        "expected_requests":source["expected_request_count"],
        "received_requests":source["received_request_count"],
        "response_bytes":source["request_metrics"]["response_bytes"],
        "wall_seconds":source["collection_wall_seconds"],
    },indent=2,sort_keys=True))
    if source["collection_status"]!="complete":
        raise SystemExit(2)

if __name__=="__main__":
    main()
