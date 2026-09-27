#!/usr/bin/env python3
from __future__ import annotations
import argparse,base64,gzip,hashlib,json,os,urllib.request
from pathlib import Path
PRIVATE_REPO="janwohlers78/mardorf-kitevorhersage"
API="https://api.github.com"
def _contents(path):
    token=os.environ["PRIVATE_REPO_TOKEN"]
    req=urllib.request.Request(
        f"{API}/repos/{PRIVATE_REPO}/contents/{path}?ref=main",
        headers={"Authorization":f"Bearer {token}","Accept":"application/vnd.github+json",
                 "X-GitHub-Api-Version":"2022-11-28",
                 "User-Agent":"mardorf-phase2f3-full-liveproof/2.0"})
    with urllib.request.urlopen(req,timeout=30) as r:
        meta=json.loads(r.read().decode())
    raw=meta.get("content")
    if not raw: raise RuntimeError(f"No inline content for {path}")
    return base64.b64decode(raw.replace("\n",""))
def seed(path):
    latest=json.loads(_contents("data/inbox/public_collector/integrity/models/latest_success.json"))
    private=latest["private_payload"]
    raw=gzip.decompress(_contents(private["destination"]))
    actual=hashlib.sha256(raw).hexdigest()
    if actual!=private["source_sha256"]:
        raise RuntimeError(f"seed SHA mismatch {actual} != {private['source_sha256']}")
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    Path(path).write_bytes(raw)
    print(json.dumps({"seed_generated_at_utc":latest.get("generated_at_utc"),
                      "seed_payload":private["destination"],"seed_sha256":actual,
                      "seed_integrity_status":latest.get("status"),
                      "seed_error_count":latest.get("error_count")},indent=2,sort_keys=True))
def gate(snapshot_path,integrity_path):
    d=json.loads(Path(snapshot_path).read_text())
    a=json.loads(Path(integrity_path).read_text())
    s=d["gefs_full_member_source"]
    metrics=s.get("traffic_metrics") or s.get("request_metrics") or {}
    total_requests=int(metrics.get("total_requests",metrics.get("requests",0)))
    response_bytes=int(metrics.get("total_response_bytes",metrics.get("response_bytes",0)))
    wall=float(s.get("collection_wall_seconds",0))
    summary={"run_time_utc":s["run_time_utc"],"members":len(s["expected_member_ids"]),
             "leads":len(s["leads_hours"]),"member_requests":s["received_request_count"],
             "total_requests_including_qa":total_requests,"response_bytes":response_bytes,
             "collection_wall_seconds":wall,
             "provider_summary_qa_status":(s.get("provider_summary_qa") or {}).get("status"),
             "integrity_status":a["status"],"integrity_errors":a["error_count"],
             "integrity_warnings":a["warning_count"]}
    print(json.dumps(summary,indent=2,sort_keys=True))
    assert s["collection_status"]=="complete"
    assert s["expected_request_count"]==744
    assert s["received_request_count"]==744
    assert len(s["expected_member_ids"])==31
    assert total_requests<=800
    assert response_bytes<=2*1024*1024
    assert wall<=600
    assert a["error_count"]==0
def diagnose(snapshot_path):
    d=json.loads(Path(snapshot_path).read_text())
    a=d.get("gefs_full_member_attempt")
    if not isinstance(a,dict):
        print("no partial GEFS attempt diagnostics")
        return
    print(json.dumps({
        "run_time_utc":a.get("run_time_utc"),
        "expected_request_count":a.get("expected_request_count"),
        "received_request_count":a.get("received_request_count"),
        "failed_request_count":a.get("failed_request_count"),
        "failure_counts_by_product":a.get("failure_counts_by_product"),
        "failure_counts_by_reason":a.get("failure_counts_by_reason"),
        "request_metrics":a.get("request_metrics"),
        "collection_wall_seconds":a.get("collection_wall_seconds"),
    },indent=2,sort_keys=True))
def main():
    ap=argparse.ArgumentParser()
    sp=ap.add_subparsers(dest="cmd",required=True)
    p=sp.add_parser("seed"); p.add_argument("--out",default="work/model_snapshot.json")
    p=sp.add_parser("gate"); p.add_argument("--snapshot",default="work/model_snapshot.json"); p.add_argument("--integrity",default="work/model_integrity.json")
    p=sp.add_parser("diagnose"); p.add_argument("--snapshot",default="work/model_snapshot.json")
    args=ap.parse_args()
    if args.cmd=="seed": seed(args.out)
    elif args.cmd=="gate": gate(args.snapshot,args.integrity)
    else: diagnose(args.snapshot)
if __name__=="__main__": main()
