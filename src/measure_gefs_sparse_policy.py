#!/usr/bin/env python3
"""Bounded exact-policy Phase-2F-3 network measurement.

Fetches four representative members across every sparse policy lead using the
same production request builder/parser. It never writes canonical data.
"""
from __future__ import annotations
import argparse,json,time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import gefs_full_members as g

SAMPLE_MEMBERS=("c00","p01","p15","p30")
MAX_NETWORK_BYTES_PER_DAY=2*1024*1024

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--run",default="2026-09-26T00:00:00+00:00")
    ap.add_argument("--out",default="work/phase2f3_sparse_measurement.json")
    args=ap.parse_args()
    run=g.utc(args.run)
    units=[x for x in g.request_plan(run) if x["member_id"] in SAMPLE_MEMBERS]
    started=time.monotonic()
    records=[g._fetch_unit(run,x) for x in units]
    failed=[x for x in records if x["request_status"]!="received"]
    if failed:
        raise RuntimeError("bounded sparse GEFS measurement had failed requests: "+json.dumps(failed[:5]))
    by_product=defaultdict(lambda:{"requests":0,"bytes":0,"seconds":0.0,"fields":0})
    for x in records:
        p=by_product[x["provider_product"]]
        p["requests"]+=1;p["bytes"]+=x["response_bytes"];p["seconds"]+=x["elapsed_seconds"];p["fields"]+=len(x["fields"])
    projection=0.0
    product_projection={}
    for product,p in sorted(by_product.items()):
        sample_leads=len(g.NEAR_LEADS) if product=="gefs_0p25s" else len(g.FAR_LEADS)
        sample_units=len(SAMPLE_MEMBERS)*sample_leads
        full_units=len(g.MEMBERS)*sample_leads
        avg=p["bytes"]/sample_units
        projected=avg*full_units
        projection+=projected
        product_projection[product]={
            **p,
            "sample_units":sample_units,
            "full_units":full_units,
            "mean_response_bytes":avg,
            "projected_full_cycle_bytes":round(projected),
        }
    result={
        "schema_version":1,
        "method_version":"phase2f3-gefs-sparse-exact-network-measurement-v1",
        "run_time_utc":run.isoformat(),
        "sample_members":list(SAMPLE_MEMBERS),
        "sample_leads_hours":list(g.LEADS),
        "sample_request_count":len(records),
        "sample_response_bytes":sum(x["response_bytes"] for x in records),
        "sample_http_elapsed_seconds_sum":sum(x["elapsed_seconds"] for x in records),
        "measurement_wall_seconds":time.monotonic()-started,
        "product_measurements":product_projection,
        "full_cycle_request_count":len(g.request_plan(run)),
        "projected_full_cycle_network_bytes":round(projection),
        "network_gate_bytes":MAX_NETWORK_BYTES_PER_DAY,
        "network_gate_pass":projection<=MAX_NETWORK_BYTES_PER_DAY,
        "policy":g.policy_summary(),
    }
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    Path(args.out).write_text(json.dumps(result,indent=2,sort_keys=True)+"\n")
    print(json.dumps(result,indent=2,sort_keys=True))
    if not result["network_gate_pass"]:
        raise SystemExit(2)

if __name__=="__main__":
    main()
