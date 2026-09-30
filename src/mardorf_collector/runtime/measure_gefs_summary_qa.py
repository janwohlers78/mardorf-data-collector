#!/usr/bin/env python3
"""Bounded GEFS provider-summary QA calibration for Phase 2F-3."""
from __future__ import annotations
import hashlib,json,math,re,statistics,subprocess,tempfile
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
import requests

import mardorf_collector.providers.gefs_full_members as g

LEADS=(120,240)
SEMANTICS=("wind_u_10m","wind_v_10m")
UA="mardorf-phase2f3-summary-qa-measure/1.0"

def summary_filtered(run,product,lead,session):
    pad=.30
    q={
        "file":f"{product}.t00z.pgrb2s.0p25.f{lead:03d}",
        "dir":f"/gefs.{run:%Y%m%d}/00/atmos/pgrb2sp25",
        "subregion":"",
        "leftlon":f"{g.LON-pad:.4f}","rightlon":f"{g.LON+pad:.4f}",
        "toplat":f"{g.LAT+pad:.4f}","bottomlat":f"{g.LAT-pad:.4f}",
        "lev_10_m_above_ground":"on","var_UGRD":"on","var_VGRD":"on",
    }
    url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl?"+urlencode(q)
    r=session.get(url,timeout=(10,60));r.raise_for_status()
    if r.content[:4]!=b"GRIB":raise RuntimeError(f"summary filter non-GRIB {product} f{lead}")
    with tempfile.NamedTemporaryFile(suffix=".grib2") as fh:
        fh.write(r.content);fh.flush()
        p=subprocess.run(["grib_ls","-l",f"{g.LAT},{g.LON},1","-p","shortName",fh.name],
                         capture_output=True,text=True,check=True)
    values={}
    for line in p.stdout.splitlines():
        parts=line.strip().split()
        if len(parts)>=2:
            try:values[parts[0]]=float(parts[-1])
            except ValueError:pass
    if not {"10u","10v"}.issubset(values):
        raise RuntimeError(f"summary filtered parse missing U/V: {values}")
    return values,{"response_bytes":len(r.content),"response_sha256":hashlib.sha256(r.content).hexdigest(),"url":url}

def main():
    run=datetime(2026,9,26,tzinfo=timezone.utc)
    units=[x for x in g.request_plan(run) if x["lead_hours"] in LEADS]
    # all 31 members at both QA leads
    records=[g._fetch_unit(run,x) for x in units]
    bad=[x for x in records if x["request_status"]!="received"]
    if bad:raise RuntimeError(f"member fetch failures: {bad[:3]}")
    s=requests.Session();s.headers.update({"User-Agent":UA})
    output=[]
    for lead in LEADS:
        recs=[x for x in records if x["lead_hours"]==lead]
        mean_values,mean_meta=summary_filtered(run,"geavg",lead,s)
        spread_values,spread_meta=summary_filtered(run,"gespr",lead,s)
        for semantic,var in (("wind_u_10m","UGRD"),("wind_v_10m","VGRD")):
            by_member={}
            for r in recs:
                hits=[f["value_native"] for f in r["fields"] if f["semantic_id"]==semantic]
                if len(hits)!=1:raise RuntimeError(f"{lead} {r['member_id']} {semantic} hits={hits}")
                by_member[r["member_id"]]=float(hits[0])
            values=[by_member[m] for m in g.MEMBERS]
            perturbed=[by_member[m] for m in g.MEMBERS if m!="c00"]
            mean=statistics.fmean(values)
            pop=statistics.pstdev(values)
            sample=statistics.stdev(values)
            pert_mean=statistics.fmean(perturbed)
            pert_pop=statistics.pstdev(perturbed)
            pert_sample=statistics.stdev(perturbed)
            mp=mean_meta;sp=spread_meta
            native="10u" if var=="UGRD" else "10v"
            provider_mean=mean_values[native]
            provider_spread=spread_values[native]
            output.append({
                "lead_hours":lead,"semantic_id":semantic,"member_count":len(values),
                "member_mean":mean,"member_pstdev":pop,"member_sample_stdev":sample,
                "perturbed30_mean":pert_mean,"perturbed30_pstdev":pert_pop,
                "perturbed30_sample_stdev":pert_sample,
                "provider_mean":provider_mean,"provider_spread":provider_spread,
                "mean_abs_delta":abs(mean-provider_mean),
                "pstdev_abs_delta":abs(pop-provider_spread),
                "sample_stdev_abs_delta":abs(sample-provider_spread),
                "perturbed30_mean_abs_delta":abs(pert_mean-provider_mean),
                "perturbed30_pstdev_abs_delta":abs(pert_pop-provider_spread),
                "perturbed30_sample_stdev_abs_delta":abs(pert_sample-provider_spread),
                "mean_message":mp,"spread_message":sp,
            })
    result={
        "schema_version":1,"method_version":"phase2f3-gefs-provider-summary-qa-measure-v1",
        "run_time_utc":run.isoformat(),"leads_hours":list(LEADS),"comparisons":output,
        "max_mean_abs_delta":max(x["mean_abs_delta"] for x in output),
        "max_pstdev_abs_delta":max(x["pstdev_abs_delta"] for x in output),
        "max_sample_stdev_abs_delta":max(x["sample_stdev_abs_delta"] for x in output),
        "max_perturbed30_mean_abs_delta":max(x["perturbed30_mean_abs_delta"] for x in output),
        "max_perturbed30_pstdev_abs_delta":max(x["perturbed30_pstdev_abs_delta"] for x in output),
        "max_perturbed30_sample_stdev_abs_delta":max(x["perturbed30_sample_stdev_abs_delta"] for x in output),
        "summary_filter_request_count":len(LEADS)*2,
        "summary_filter_response_bytes":sum(
            x["mean_message"]["response_bytes"]+x["spread_message"]["response_bytes"]
            for x in output if x["semantic_id"]=="wind_u_10m"
        ),
    }
    print(json.dumps(result,indent=2,sort_keys=True))

if __name__=="__main__":main()
