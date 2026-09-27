#!/usr/bin/env python3
"""Bounded GEFS provider-summary QA calibration for Phase 2F-3."""
from __future__ import annotations
import hashlib,json,math,re,statistics,subprocess,tempfile
from datetime import datetime,timezone
from pathlib import Path
import requests

import gefs_full_members as g

LEADS=(120,240)
SEMANTICS=("wind_u_10m","wind_v_10m")
UA="mardorf-phase2f3-summary-qa-measure/1.0"

def idx(url,session):
    r=session.get(url+".idx",timeout=45);r.raise_for_status()
    rows=[]
    for line in r.text.splitlines():
        p=line.split(":")
        if len(p)<5:continue
        try:offset=int(p[1])
        except ValueError:continue
        rows.append({"offset":offset,"var":p[3],"level":p[4],"raw":line})
    for i,x in enumerate(rows):
        x["end"]=rows[i+1]["offset"]-1 if i+1<len(rows) else None
    return rows

def point(url,var,session):
    rows=idx(url,session)
    hit=next((x for x in rows if x["var"]==var and "10 m above ground" in x["level"]),None)
    if hit is None:raise RuntimeError(f"missing {var} 10m in {url}")
    end=hit["end"]
    if end is None:
        h=session.head(url,timeout=30);h.raise_for_status();end=int(h.headers["Content-Length"])-1
    r=session.get(url,headers={"Range":f"bytes={hit['offset']}-{end}"},timeout=60)
    if r.status_code!=206:raise RuntimeError(f"range expected 206 got {r.status_code}")
    with tempfile.NamedTemporaryFile(suffix=".grib2") as fh:
        fh.write(r.content);fh.flush()
        p=subprocess.run(["grib_ls","-l",f"{g.LAT},{g.LON},1","-p","shortName",fh.name],
                         capture_output=True,text=True,check=True)
    vals=[]
    for line in p.stdout.splitlines():
        parts=line.strip().split()
        if len(parts)>=2:
            try:vals.append((parts[0],float(parts[-1])))
            except ValueError:pass
    if len(vals)!=1:raise RuntimeError(f"unexpected summary message parse: {vals}")
    return vals[0][1],{"idx":hit["raw"],"message_bytes":len(r.content),
                       "message_sha256":hashlib.sha256(r.content).hexdigest()}

def summary_url(run,product,lead):
    return ("https://noaa-gefs-pds.s3.amazonaws.com/"
            f"gefs.{run:%Y%m%d}/00/atmos/pgrb2sp25/"
            f"{product}.t00z.pgrb2s.0p25.f{lead:03d}")

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
        for semantic,var in (("wind_u_10m","UGRD"),("wind_v_10m","VGRD")):
            values=[]
            for r in recs:
                hits=[f["value_native"] for f in r["fields"] if f["semantic_id"]==semantic]
                if len(hits)!=1:raise RuntimeError(f"{lead} {r['member_id']} {semantic} hits={hits}")
                values.append(float(hits[0]))
            mean=statistics.fmean(values)
            pop=statistics.pstdev(values)
            sample=statistics.stdev(values)
            provider_mean,mp=point(summary_url(run,"geavg",lead),var,s)
            provider_spread,sp=point(summary_url(run,"gespr",lead),var,s)
            output.append({
                "lead_hours":lead,"semantic_id":semantic,"member_count":len(values),
                "member_mean":mean,"member_pstdev":pop,"member_sample_stdev":sample,
                "provider_mean":provider_mean,"provider_spread":provider_spread,
                "mean_abs_delta":abs(mean-provider_mean),
                "pstdev_abs_delta":abs(pop-provider_spread),
                "sample_stdev_abs_delta":abs(sample-provider_spread),
                "mean_message":mp,"spread_message":sp,
            })
    result={
        "schema_version":1,"method_version":"phase2f3-gefs-provider-summary-qa-measure-v1",
        "run_time_utc":run.isoformat(),"leads_hours":list(LEADS),"comparisons":output,
        "max_mean_abs_delta":max(x["mean_abs_delta"] for x in output),
        "max_pstdev_abs_delta":max(x["pstdev_abs_delta"] for x in output),
        "max_sample_stdev_abs_delta":max(x["sample_stdev_abs_delta"] for x in output),
    }
    print(json.dumps(result,indent=2,sort_keys=True))

if __name__=="__main__":main()
