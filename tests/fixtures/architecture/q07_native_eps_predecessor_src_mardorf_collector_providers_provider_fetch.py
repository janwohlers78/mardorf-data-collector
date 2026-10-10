#!/usr/bin/env python3
"""Fetch exactly one model provider/stage and persist progress immediately.

This wrapper isolates provider failures so one slow source cannot prevent later
sources from being attempted. External shell timeout handles truly hung clients.
"""
from __future__ import annotations
import argparse,json,os,time
from datetime import datetime,timezone
from pathlib import Path

import mardorf_collector.providers.fetch_model_data as base
import mardorf_collector.providers.fetch_extra_models as extra
import mardorf_collector.providers.fetch_dwd_additional_models as dwd
import mardorf_collector.runtime.extend_model_horizon as ext
import mardorf_collector.contracts.availability_contract as availability
import mardorf_collector.providers.ecmwf_registry as ecmwf_registry

SNAP=Path(os.getenv("COLLECTOR_MODEL_FILE","work/model_snapshot.json"))

def now():
    return datetime.now(timezone.utc).isoformat()

def initial(test):
    return {
        "schema_version":2,
        "retrieved_at_utc":now(),
        "spot":{"lat":base.LAT,"lon":base.LON},
        "mode":"test" if test else "production",
        "leads_hours":[],
        "models":{},
        "quality":{"errors":[]},
        "provider_attempts":[],
    }

def load(test):
    if SNAP.exists():
        d=json.loads(SNAP.read_text(encoding="utf-8"))
        d.setdefault("models",{});d.setdefault("quality",{});d["quality"].setdefault("errors",[])
        d.setdefault("provider_attempts",[])
        return d
    d=initial(test);SNAP.parent.mkdir(parents=True,exist_ok=True)
    SNAP.write_text(json.dumps(d,separators=(",",":"))+"\n",encoding="utf-8")
    return d

def save(d):
    all_leads=sorted({int(r["forecast_lead_hours"]) for rows in d.get("models",{}).values() for r in rows if r.get("forecast_lead_hours") is not None})
    d["leads_hours"]=all_leads
    d["retrieved_at_utc"]=now()
    SNAP.parent.mkdir(parents=True,exist_ok=True)
    SNAP.write_text(json.dumps(d,separators=(",",":"))+"\n",encoding="utf-8")

def base_leads(model,test):
    if test:
        return [0,12,24,30,36,42,48] if model in ("ICON-D2","GFS") else [0,12,24,36,48]
    return list(range(0,49,3))

def quality_one(d,model,expected):
    rows=d.get("models",{}).get(model,[])
    got={int(r["forecast_lead_hours"]) for r in rows if r.get("derived") and r.get("forecast_lead_hours") is not None}
    complete=all(x in got for x in expected) and len(got)>=len(expected)
    d["quality"][model]={
        "records":len(rows),"derived_records":sum(bool(r.get("derived")) for r in rows),
        "success":complete,"complete_requested_horizon":complete,
        "expected_lead_count":len(expected),"max_derived_lead_hours":max(got) if got else None,
    }
    success=[n for n,q in d["quality"].items() if isinstance(q,dict) and q.get("success")]
    d["quality"]["successful_models"]=success
    families={("DWD-ICON" if n.startswith("ICON-") else "GFS" if n in ("GFS","GEFS-control") else n) for n in success}
    d["quality"]["minimum_two_independent_models_met"]=len(families)>=2

def fetch_eps_when_settled(leads, *, max_wait_seconds=180, poll_seconds=30):
    """Bridge short publication transitions without changing the frozen fetcher.

    Every poll reads current provider metadata. The monotonic deadline never
    resets when a cycle changes, and the original acquisition still independently
    verifies settling, bracketed metadata, member identity and spatial evidence.
    """
    if (type(max_wait_seconds) not in (int,float) or not 0<=max_wait_seconds<=180 or
            type(poll_seconds) not in (int,float) or not 0<poll_seconds<=60):
        raise ValueError("Invalid bounded provider readiness budget")
    deadline=time.monotonic()+max_wait_seconds
    while True:
        metadata=dwd.fetch_eps_metadata()
        availability_time=dwd._meta_dt(metadata,'last_run_availability_time_utc')
        age=(datetime.now(timezone.utc)-availability_time).total_seconds()
        remaining=dwd.EPS_SETTLING_SECONDS-age
        if remaining<=0:
            return dwd.fetch_icon_d2_eps_bundle(leads)
        budget=deadline-time.monotonic()
        if age<0 or remaining>budget:
            raise RuntimeError(
                "ICON-D2-EPS readiness exceeds bounded wait: "
                f"remaining_seconds={round(remaining,1)} budget_seconds={round(max(0,budget),1)}")
        pause=min(remaining,poll_seconds,budget)
        print(json.dumps({"model":"ICON-D2-EPS","stage":"publication_readiness",
                          "status":"waiting","wait_seconds":round(pause,3),
                          "settling_required_seconds":dwd.EPS_SETTLING_SECONDS}),flush=True)
        time.sleep(pause)

def fetch_base(d,model,test):
    leads=base_leads(model,test)
    if model=="ICON-D2":
        rows=base.derive(base.fetch_icon(leads))
    elif model=="GFS":
        rows=base.derive(base.fetch_gfs(leads))
    elif model=="ECMWF-IFS":
        gate=d.get("provider_cycle_gate") if isinstance(d.get("provider_cycle_gate"),dict) else {}
        entry=(gate.get("models") or {}).get(model) if isinstance(gate.get("models"),dict) else {}
        planned=entry.get("selected_run_time_utc") if isinstance(entry,dict) else None
        rows=extra.fetch_ifs(leads,run_time=planned)
    elif model=="GEFS-control":
        rows,selection_evidence=extra.fetch_gefs(leads,return_selection_evidence=True)
        d.setdefault("provider_selection_evidence",{})["GEFS-control"]=selection_evidence
    elif model=="ICON-EU":
        if os.getenv("FULL_VALIDATION","").lower()=="true" and not test:
            rows,selection_evidence=dwd.fetch_icon_eu(
                leads,required_cycle_lead=120,return_selection_evidence=True)
            d.setdefault("provider_selection_evidence",{})["ICON-EU"]=selection_evidence
        else:
            rows=dwd.fetch_icon_eu(leads,required_cycle_lead=None if test else 120)
    elif model=="ICON-D2-EPS":
        rows,hourly_source=fetch_eps_when_settled(leads)
        d["ensemble_hourly_source"]=hourly_source
    else:
        raise ValueError(model)
    availability.stamp_rows(rows, observed_at=now(), replace_row_time=True)
    d["models"][model]=rows
    if model=="ECMWF-IFS":
        summary=ecmwf_registry.acquisition_summary()
        summary.update(last_stage="base",last_updated_at_utc=now(),row_count=len(rows))
        d["ecmwf_registry_v1_acquisition"]=summary
    quality_one(d,model,leads)

def fetch_extension(d,model):
    if model=="GFS":
        new=ext.fetch_noaa(d,"GFS",False)
    elif model=="GEFS-control":
        new=ext.fetch_noaa(d,"GEFS-control",True)
    elif model=="ECMWF-IFS":
        new=ext.fetch_ifs(d)
    elif model=="ICON-EU":
        new=ext.fetch_icon_eu(d)
    else:
        raise ValueError(f"extension unsupported for {model}")
    availability.stamp_rows(new, observed_at=now(), replace_row_time=True)
    old=[r for r in d["models"].get(model,[]) if int(r.get("forecast_lead_hours",9999))<=48]
    d["models"][model]=old+new
    if model=="ECMWF-IFS":
        summary=ecmwf_registry.acquisition_summary()
        summary.update(last_stage="extension",last_updated_at_utc=now(),row_count=len(d["models"][model]))
        d["ecmwf_registry_v1_acquisition"]=summary
    ext.quality(d)
    d["horizon_extension_retrieved_at_utc"]=now()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--model",required=True,choices=("ICON-D2","GFS","ECMWF-IFS","GEFS-control","ICON-EU","ICON-D2-EPS"))
    ap.add_argument("--stage",required=True,choices=("base","extension"))
    ap.add_argument("--test",action="store_true")
    args=ap.parse_args()
    d=load(args.test)
    started=datetime.now(timezone.utc);source=os.getenv("ECMWF_OPEN_DATA_SOURCE") if args.model=="ECMWF-IFS" else None
    attempt={"model":args.model,"stage":args.stage,"status":"started","started_at_utc":started.isoformat()}
    if source:attempt["mirror"]=source
    d["provider_attempts"].append(attempt);save(d)
    try:
        if args.stage=="base":
            fetch_base(d,args.model,args.test)
        else:
            if args.test: raise RuntimeError("extension stage is not used in test mode")
            fetch_extension(d,args.model)
        ended=datetime.now(timezone.utc)
        attempt.update(status="success",completed_at_utc=ended.isoformat(),duration_seconds=round((ended-started).total_seconds(),3))
        save(d)
        print(json.dumps({"model":args.model,"stage":args.stage,"status":"success","mirror":source,"duration_seconds":attempt["duration_seconds"],"records":len(d["models"].get(args.model,[]))}))
    except Exception as e:
        ended=datetime.now(timezone.utc)
        attempt.update(status="failed",completed_at_utc=ended.isoformat(),duration_seconds=round((ended-started).total_seconds(),3),
                       exception_type=type(e).__name__,exception_message=str(e))
        save(d)
        print(json.dumps({"model":args.model,"stage":args.stage,"status":"failed","mirror":source,
                          "exception_type":type(e).__name__,"exception_message":str(e)},ensure_ascii=False))
        raise

if __name__=="__main__":
    main()
