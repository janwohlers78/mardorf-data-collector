#!/usr/bin/env python3
"""Plan provider-specific model work against private archived cycle evidence.

Phase 2E-2 keeps the existing bundle-level freshness gate as a cheap watchdog,
then performs provider-cycle discovery before any expensive full download.
A provider is carried forward only when:
  * the newest selected provider cycle matches the last verified public payload,
  * that exact cycle has private Archive-v4 cycle evidence, and
  * the prior payload still contains records for that cycle.

The script is deliberately fail-open. Any probe/private-state ambiguity marks
that provider for fetch rather than risking a missed cycle.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fetch_model_data as base
import fetch_extra_models as extra
import fetch_dwd_additional_models as dwd
import gefs_full_members as full_gefs
from full_horizon_contract import maximum_hours

API="https://api.github.com"
DEFAULT_REPO="janwohlers78/mardorf-kitevorhersage"
MODELS=("ICON-D2","GFS","GEFS-control","ICON-EU","ICON-D2-EPS","ECMWF-IFS")
OUTPUT_KEYS={
    "ICON-D2":"icon_d2",
    "GFS":"gfs",
    "GEFS-control":"gefs_control",
    "ICON-EU":"icon_eu",
    "ICON-D2-EPS":"icon_d2_eps",
    "ECMWF-IFS":"ecmwf_ifs",
}
METHOD_VERSION="provider-cycle-gate-v2-gefs-full"
GEFS_EVIDENCE_SUCCESSOR_VERSION="gefs-cycle-evidence-event-stream-index-v2"


def utc(value):
    x=datetime.fromisoformat(str(value).replace("Z","+00:00"))
    return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)


def output(name,value):
    path=os.getenv("GITHUB_OUTPUT")
    if path:
        with open(path,"a",encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
    print(f"{name}={value}")


def _request(repo,path,token):
    url=f"{API}/repos/{repo}/contents/{path}?ref=main"
    req=urllib.request.Request(url,headers={
        "Authorization":f"Bearer {token}",
        "Accept":"application/vnd.github+json",
        "X-GitHub-Api-Version":"2022-11-28",
        "User-Agent":"mardorf-data-collector-provider-cycle-gate/1.0",
    })
    with urllib.request.urlopen(req,timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def private_bytes(repo,path,token):
    meta=_request(repo,path,token)
    raw=meta.get("content")
    if not raw:
        raise RuntimeError(f"private contents API returned no inline content for {path}")
    return base64.b64decode(raw.replace("\n",""))


def private_json(repo,path,token):
    return json.loads(private_bytes(repo,path,token).decode("utf-8"))


def private_json_optional(repo,path,token):
    try:
        return private_json(repo,path,token)
    except urllib.error.HTTPError as exc:
        if exc.code==404:
            return None
        raise


def evidence_path(model,run):
    safe=model.lower().replace("_","-")
    return (
        f"data/weather_archive/cycle_evidence/{safe}/"
        f"year={run:%Y}/month={run:%m}/day={run:%d}/run={run:%Y%m%dT%H%M%SZ}.json"
    )


GEFS_EVIDENCE_EVENT_METHOD_VERSION="gefs-full-member-cycle-evidence-event-v2"
GEFS_EVIDENCE_INDEX_METHOD_VERSION="gefs-full-member-cycle-evidence-index-v2"


def gefs_full_evidence_path(run):
    """Current E1 monotonic index path for one GEFS 00Z cycle."""
    run=utc(run)
    return (
        "data/weather_archive/ensemble_cycle_evidence/noaa-gefs/indexes/"
        f"year={run:%Y}/month={run:%m}/day={run:%d}/run={run:%Y%m%dT%H%M%SZ}.json"
    )


def gefs_full_legacy_evidence_path(run):
    """Read-only predecessor path retained for already archived pre-E1 cycles."""
    run=utc(run)
    return (
        "data/weather_archive/ensemble_cycle_evidence/noaa-gefs/"
        f"year={run:%Y}/month={run:%m}/day={run:%d}/run={run:%Y%m%dT%H%M%SZ}.json"
    )


def _valid_gefs_event(item,run):
    if not isinstance(item,dict):
        return False
    try:
        recorded=utc(item.get("run_time_utc"))
    except Exception:
        return False
    return (
        recorded==utc(run)
        and item.get("ensemble_system_id")=="NOAA_GEFS"
        and item.get("collection_status")=="complete"
        and item.get("policy_version")==full_gefs.POLICY_VERSION
        and int(item.get("expected_member_count") or 0)==31
    )


def exact_archived_gefs_full_cycle(repo,token,run):
    """Return exact archived cycle evidence, preferring the E1 event/index model.

    The index is only a monotonic reference set; acceptance is based on the
    immutable latest event it names. Pre-E1 fixed-path evidence remains readable
    so historical cycles do not trigger redundant provider downloads.
    """
    run=utc(run)
    index_path=gefs_full_evidence_path(run)
    index=private_json_optional(repo,index_path,token)
    if isinstance(index,dict):
        try:
            recorded=utc(index.get("run_time_utc"))
        except Exception:
            recorded=None
        latest_path=index.get("latest_event_path")
        if (
            index.get("schema_version")==2
            and index.get("method_version")==GEFS_EVIDENCE_INDEX_METHOD_VERSION
            and index.get("ensemble_system_id")=="NOAA_GEFS"
            and recorded==run
            and int(index.get("event_count") or 0)>=1
            and isinstance(latest_path,str)
            and latest_path
        ):
            event=private_json_optional(repo,latest_path,token)
            if (
                isinstance(event,dict)
                and event.get("schema_version")==2
                and event.get("method_version")==GEFS_EVIDENCE_EVENT_METHOD_VERSION
                and event.get("evidence_event_id")==index.get("latest_event_id")
                and _valid_gefs_event(event,run)
            ):
                out=dict(event)
                out["_archive_cycle_evidence_path"]=index_path
                out["_archive_cycle_event_path"]=latest_path
                return out

    legacy_path=gefs_full_legacy_evidence_path(run)
    legacy=private_json_optional(repo,legacy_path,token)
    if _valid_gefs_event(legacy,run):
        out=dict(legacy)
        out["_archive_cycle_evidence_path"]=legacy_path
        return out
    return None


def exact_archived_cycle(repo,token,model,run):
    item=private_json_optional(repo,evidence_path(model,run),token)
    if not isinstance(item,dict):
        return None
    try:
        recorded=utc(item.get("run_time_utc"))
    except Exception:
        return None
    if recorded!=run or item.get("model")!=model:
        return None
    return item


def _ecmwf_probe(client,target,run=None,step=48):
    kwargs={}
    if run is not None:
        kwargs.update(date=run.strftime("%Y%m%d"),time=run.hour)
    client.retrieve(
        stream="oper",type="fc",step=[int(step)],param=["10u"],
        target=str(target),**kwargs)
    actual=extra.grib_run_time(target)
    if run is not None and actual!=run:
        raise RuntimeError(
            f"ECMWF probe returned wrong cycle: requested={run.isoformat()} actual={actual.isoformat()}")
    return actual


def _ecmwf_cycle(full_validation=True):
    """Probe the newest ECMWF cycle mature enough for this collection mode."""
    source=os.getenv("ECMWF_OPEN_DATA_SOURCE","azure")
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)
        client=extra.Client(source=source,model="ifs",resol="0p25",maximum_retries=2,retry_after=5)
        latest=_ecmwf_probe(client,root/"latest_f048.grib2",step=48)
        if not full_validation:
            return latest
        candidate=latest
        failures=[]
        for idx in range(8):
            terminal=maximum_hours("ECMWF-IFS",candidate)
            try:
                _ecmwf_probe(
                    client,root/f"candidate_{idx}_{terminal}.grib2",
                    run=candidate,step=terminal)
                return candidate
            except Exception as exc:
                failures.append({
                    "run_time_utc":candidate.isoformat(),
                    "terminal_lead_hours":terminal,
                    "exception_type":type(exc).__name__,
                    "exception_message":str(exc)[:300],
                })
                candidate=candidate-timedelta(hours=6)
        raise RuntimeError(f"No mature ECMWF cycle found from latest={latest.isoformat()}; failures={failures}")


def discover(model,full_validation=True):
    if model=="ICON-D2":
        return utc(datetime.strptime(base.latest_dwd_icon_d2_cycle(48),"%Y%m%d%H").replace(tzinfo=timezone.utc))
    if model=="GFS":
        lead=384 if full_validation else 48
        return utc(datetime.strptime(base.discover_gfs_cycle(lead),"%Y%m%d%H").replace(tzinfo=timezone.utc))
    if model=="GEFS-control":
        cycle,_evidence=extra.discover_gefs(
            48,require_far_horizon=bool(full_validation),return_evidence=True)
        return utc(datetime.strptime(cycle,"%Y%m%d%H").replace(tzinfo=timezone.utc))
    if model=="ICON-EU":
        try:
            cycle=dwd.discover_cycle("icon-eu",120)
        except Exception:
            cycle=dwd.discover_cycle("icon-eu",48)
        return utc(datetime.strptime(cycle,"%Y%m%d%H").replace(tzinfo=timezone.utc))
    if model=="ICON-D2-EPS":
        meta=dwd.fetch_eps_metadata()
        run=utc(meta["last_run_initialisation_time_utc"])
        available=utc(meta["last_run_availability_time_utc"])
        age=(datetime.now(timezone.utc)-available).total_seconds()
        if age < dwd.EPS_SETTLING_SECONDS:
            raise RuntimeError(
                f"ICON-D2-EPS newest run is not settled: run={run.isoformat()} age_seconds={age:.1f}")
        dwd.find_dwd_file("icon-d2-eps",run.strftime("%Y%m%d%H"),48,"u_10m")
        return run
    if model=="ECMWF-IFS":
        return _ecmwf_cycle(full_validation)
    raise ValueError(model)


def gefs_supplemental_due(payload,run,checked_at):
    archive=payload.get("full_horizon_archive") if isinstance(payload.get("full_horizon_archive"),dict) else {}
    sources=archive.get("sources") if isinstance(archive.get("sources"),dict) else {}
    source=sources.get("GEFS-control")
    if not isinstance(source,dict) or source.get("run_time_utc")!=run.isoformat():
        return False,None
    state=source.get("gefs_pgrb2b_supplemental_retry")
    if not isinstance(state,dict) or state.get("status")!="pending":
        return False,state
    try:
        attempts=int(state.get("attempts",0))
        maximum=int(state.get("maximum_attempts",1))
        due=utc(state.get("next_retry_not_before_utc"))
    except Exception:
        return False,state
    return attempts<maximum and checked_at>=due,state


def rows_match_cycle(payload,model,run):
    rows=[x for x in (payload.get("models") or {}).get(model,[]) if isinstance(x,dict)]
    if not rows:
        return False
    runs=set()
    for row in rows:
        try:
            runs.add(utc(row.get("run_time_utc")))
        except Exception:
            return False
    return runs=={run}


def source_matches_cycle(latest,model,run):
    source=(latest.get("sources") or {}).get(model)
    if not isinstance(source,dict):
        return False
    try:
        selected=utc(source.get("selected_run_time_utc"))
    except Exception:
        return False
    return (
        selected==run
        and source.get("provider_cycle_complete") is True
    )


def load_seed(repo,token):
    latest=private_json(repo,"data/inbox/public_collector/integrity/models/latest_success.json",token)
    private=latest.get("private_payload") if isinstance(latest.get("private_payload"),dict) else {}
    dest=private.get("destination")
    if not dest:
        raise RuntimeError("private latest_success has no model payload destination")
    packed=private_bytes(repo,dest,token)
    raw=gzip.decompress(packed)
    payload=json.loads(raw.decode("utf-8"))
    expected=private.get("source_sha256")
    if not isinstance(payload,dict) or not expected:
        raise RuntimeError("private seed payload identity is incomplete")
    import hashlib
    actual=hashlib.sha256(raw).hexdigest()
    if actual!=expected:
        raise RuntimeError(f"private seed payload SHA mismatch expected={expected} actual={actual}")
    return latest,payload,dest,actual


def build_plan(repo,token,full_validation=True,discover_fn=discover,discover_gefs_full_fn=None):
    checked=datetime.now(timezone.utc)
    latest,seed,seed_path,seed_sha=load_seed(repo,token)
    models={}
    for model in MODELS:
        entry={"action":"fetch","reason":"probe_not_run_fail_open"}
        try:
            run=discover_fn(model,full_validation)
            archived=exact_archived_cycle(repo,token,model,run)
            source_ok=source_matches_cycle(latest,model,run)
            rows_ok=rows_match_cycle(seed,model,run)
            entry.update(
                selected_run_time_utc=run.isoformat(),
                archive_cycle_evidence_path=evidence_path(model,run),
                archive_cycle_evidence_present=bool(archived),
                latest_success_source_matches=bool(source_ok),
                seed_payload_rows_match=bool(rows_ok),
            )
            if archived and source_ok and rows_ok:
                supplement_due,supplement_state=(
                    gefs_supplemental_due(seed,run,checked)
                    if model=="GEFS-control" else (False,None)
                )
                if supplement_due:
                    entry.update(
                        action="supplemental_retry",
                        reason="archived_cycle_has_due_bounded_pgrb2b_retry",
                        supplemental_retry_state=supplement_state,
                    )
                else:
                    entry.update(action="carry_forward",reason="selected_cycle_already_archived")
            else:
                missing=[]
                if not archived: missing.append("private_cycle_evidence")
                if not source_ok: missing.append("latest_success_source")
                if not rows_ok: missing.append("seed_payload_rows")
                entry["reason"]="fetch_fail_open_missing_"+"_".join(missing)
        except Exception as exc:
            entry.update(
                action="fetch",
                reason="provider_cycle_probe_failed_fail_open",
                probe_exception_type=type(exc).__name__,
                probe_exception_message=str(exc)[:700],
            )
        models[model]=entry

    full_entry={"action":"fetch","reason":"probe_not_run_fail_open"}
    source=seed.get("gefs_full_member_source")
    source_run=None
    try:
        if isinstance(source,dict) and source.get("collection_status")=="complete":
            source_run=utc(source.get("run_time_utc"))
    except Exception:
        source_run=None
    existing_evidence=exact_archived_gefs_full_cycle(repo,token,source_run) if source_run else None
    today_00=checked.replace(hour=0,minute=0,second=0,microsecond=0)
    if source_run==today_00 and existing_evidence and full_gefs.source_matches_policy(source,source_run):
        full_entry.update(
            action="carry_forward",
            reason="today_00z_full_member_cycle_already_archived_no_provider_probe",
            selected_run_time_utc=source_run.isoformat(),
            archive_cycle_evidence_path=existing_evidence.get("_archive_cycle_evidence_path",gefs_full_evidence_path(source_run)),
            archive_cycle_evidence_present=True,
            seed_payload_source_matches=True,
            publication_probe_attempts=[],
        )
    elif checked.hour<6 and source_run and existing_evidence and full_gefs.source_matches_policy(source,source_run):
        full_entry.update(
            action="carry_forward",
            reason="pre_06z_full_member_probe_window_carry_previous_complete_source",
            selected_run_time_utc=source_run.isoformat(),
            archive_cycle_evidence_path=existing_evidence.get("_archive_cycle_evidence_path",gefs_full_evidence_path(source_run)),
            archive_cycle_evidence_present=True,
            seed_payload_source_matches=True,
            publication_probe_attempts=[],
        )
    else:
        try:
            if discover_gefs_full_fn is None:
                run,probe_attempts=full_gefs.discover_mature_00z()
            else:
                discovered=discover_gefs_full_fn()
                if isinstance(discovered,tuple):
                    run,probe_attempts=discovered
                else:
                    run,probe_attempts=discovered,[]
            run=utc(run)
            archived=exact_archived_gefs_full_cycle(repo,token,run)
            source_ok=full_gefs.source_matches_policy(source,run)
            full_entry.update(
                selected_run_time_utc=run.isoformat(),
                archive_cycle_evidence_path=(
                    archived.get("_archive_cycle_evidence_path",gefs_full_evidence_path(run))
                    if archived else gefs_full_evidence_path(run)
                ),
                archive_cycle_evidence_present=bool(archived),
                seed_payload_source_matches=bool(source_ok),
                publication_probe_attempts=probe_attempts,
            )
            # Archive-v4 ensemble-cycle evidence is the authoritative proof that
            # an exact sparse 31-member cycle was already persisted. A later
            # deterministic/manual full-validation payload may intentionally omit
            # the archive-only gefs_full_member_source block; that omission must
            # not force a redundant NOAA re-download of an already archived cycle.
            # A genuinely newer cycle still has no exact private evidence and
            # therefore remains fail-open fetch.
            if archived:
                full_entry.update(
                    action="carry_forward",
                    reason=(
                        "selected_00z_full_member_cycle_already_archived"
                        if source_ok
                        else "selected_00z_full_member_cycle_archived_seed_source_optional"
                    ),
                )
            else:
                full_entry.update(
                    action="fetch",
                    reason="fetch_fail_open_missing_private_ensemble_cycle_evidence",
                )
        except Exception as exc:
            full_entry.update(
                action="fetch",
                reason="full_gefs_cycle_probe_failed_fail_open",
                probe_exception_type=type(exc).__name__,
                probe_exception_message=str(exc)[:700],
            )

    full_ensembles={"NOAA_GEFS":full_entry}
    any_work=(
        any(x["action"]!="carry_forward" for x in models.values())
        or full_entry["action"]!="carry_forward"
    )
    plan={
        "schema_version":1,
        "method_version":METHOD_VERSION,
        "checked_at_utc":checked.isoformat(),
        "private_repo":repo,
        "full_validation":bool(full_validation),
        "seed_payload_path":seed_path,
        "seed_payload_sha256":seed_sha,
        "seed_integrity_generated_at_utc":latest.get("generated_at_utc"),
        "models":models,
        "full_ensembles":full_ensembles,
        "any_work":any_work,
        "delta_prediction":"nonzero" if any_work else "zero",
        "no_op_transfer_suppressed":not any_work,
    }
    return plan,seed


def prepare_seed(seed,plan,path):
    out=json.loads(json.dumps(seed))
    out["retrieved_at_utc"]=plan["checked_at_utc"]
    out["provider_attempts"]=[]
    out["provider_cycle_gate"]=plan
    out.setdefault("quality",{}).setdefault("errors",[])
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    Path(path).write_text(json.dumps(out,separators=(",",":"),allow_nan=False)+"\n",encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--output",default=os.getenv("COLLECTOR_MODEL_FILE","work/model_snapshot.json"))
    ap.add_argument("--plan-out",default="work/provider_cycle_plan.json")
    args=ap.parse_args()
    token=os.getenv("PRIVATE_REPO_TOKEN","")
    repo=os.getenv("PRIVATE_REPO",DEFAULT_REPO)
    if not token:
        raise RuntimeError("PRIVATE_REPO_TOKEN is not configured for provider cycle gate")
    full=os.getenv("FULL_VALIDATION","").lower()=="true"

    try:
        plan,seed=build_plan(repo,token,full_validation=full)
    except Exception as exc:
        # If the shared private seed cannot be proven, fetch every provider.
        now=datetime.now(timezone.utc).isoformat()
        plan={
            "schema_version":1,
            "method_version":METHOD_VERSION,
            "checked_at_utc":now,
            "private_repo":repo,
            "full_validation":full,
            "models":{m:{
                "action":"fetch",
                "reason":"global_private_seed_unavailable_fail_open",
                "probe_exception_type":type(exc).__name__,
                "probe_exception_message":str(exc)[:700],
            } for m in MODELS},
            "full_ensembles":{"NOAA_GEFS":{
                "action":"fetch",
                "reason":"global_private_seed_unavailable_fail_open",
                "probe_exception_type":type(exc).__name__,
                "probe_exception_message":str(exc)[:700],
            }},
            "any_work":True,
            "delta_prediction":"nonzero",
            "no_op_transfer_suppressed":False,
        }
        seed={
            "schema_version":2,
            "retrieved_at_utc":now,
            "spot":{"lat":base.LAT,"lon":base.LON},
            "mode":"production",
            "leads_hours":[],
            "models":{},
            "quality":{"errors":[]},
            "provider_attempts":[],
        }

    Path(args.plan_out).parent.mkdir(parents=True,exist_ok=True)
    Path(args.plan_out).write_text(json.dumps(plan,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    if plan["any_work"]:
        prepare_seed(seed,plan,args.output)

    output("any_work","true" if plan["any_work"] else "false")
    output("delta_prediction",plan["delta_prediction"])
    for model,key in OUTPUT_KEYS.items():
        output(key,plan["models"][model]["action"])
    output("gefs_full",plan.get("full_ensembles",{}).get("NOAA_GEFS",{}).get("action","fetch"))
    print(json.dumps(plan,indent=2,sort_keys=True))


if __name__=="__main__":
    main()
