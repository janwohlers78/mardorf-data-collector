#!/usr/bin/env python3
import json, os
import provider_cycle_gate as gate

token=os.environ.get("PRIVATE_REPO_TOKEN","")
if not token:
    raise RuntimeError("PRIVATE_REPO_TOKEN missing")
repo=os.environ.get("PRIVATE_REPO",gate.DEFAULT_REPO)
plan,_seed=gate.build_plan(repo,token,full_validation=True)
bad={m:x for m,x in plan["models"].items() if x.get("action")!="carry_forward"}
full=plan["full_ensembles"]["NOAA_GEFS"]
assert not bad,bad
assert full.get("action")=="carry_forward",full
assert plan["any_work"] is False,plan
assert plan["delta_prediction"]=="zero",plan
assert plan["no_op_transfer_suppressed"] is True,plan
result={
    "status":"PASS",
    "checked_at_utc":plan["checked_at_utc"],
    "models":{m:{
        "action":x["action"],
        "run":x.get("selected_run_time_utc"),
        "evidence":x.get("archive_cycle_evidence_present"),
    } for m,x in plan["models"].items()},
    "NOAA_GEFS":{
        "action":full["action"],
        "run":full.get("selected_run_time_utc"),
        "evidence":full.get("archive_cycle_evidence_present"),
    },
    "any_work":plan["any_work"],
    "no_op_transfer_suppressed":plan["no_op_transfer_suppressed"],
}
print("PHASE2G1_PROVIDER_CYCLE_GATE="+json.dumps(result,sort_keys=True))
