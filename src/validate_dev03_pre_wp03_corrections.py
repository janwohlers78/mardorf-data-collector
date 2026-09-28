#!/usr/bin/env python3
"""Binding gate for AUD-20260928-02 correction contract."""
from __future__ import annotations
import argparse,base64,hashlib,json,os,urllib.request
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/"config"/"dev03_pre_wp03_l1_l2_corrections_v1.json"
EXPECTED_FIX={f"FIX-20260928-02-F{i:02d}" for i in range(1,11)}
EXPECTED_AVAIL={
 "received","unsupported_by_provider_or_product","intentionally_not_applicable",
 "not_requested_by_policy","not_yet_published","fetch_error","unknown_or_ambiguous"
}
EXPECTED_ZERO={"ECMWF-IFS","GEFS-control","NOAA_GEFS_FULL","ICON-D2-EPS"}

def check_contract(c):
    problems=[]
    def check(ok,msg):
        if not ok: problems.append(msg)
    check(c.get("contract_version")=="dev03-pre-wp03-l1-l2-corrections-v1","version drift")
    check(c.get("status")=="binding_pre_wp03","status drift")
    check(c.get("audit_id")=="AUD-20260928-02","audit id drift")
    op=c.get("operational_boundary") or {}
    check(op.get("implementation_activation_authorized") is False,"implementation activation must remain false")
    check(op.get("l4_l5_cutover_authorized") is False,"cutover must remain false")
    check(op.get("frozen_v2_v5_predecessors_unchanged") is True,"predecessor protection missing")
    check(op.get("wp03_start_condition")=="VAL-20260928-02 complete with zero open P0/P1/P2","WP03 start condition drift")

    fixes=c.get("fixes") or {}
    check(set(x.get("fix_id") for x in fixes.values())==EXPECTED_FIX,"fix set drift")

    f1=fixes.get("AUD-20260928-02-F01") or {}
    required=set(f1.get("parent_v2_binding_required_fields") or [])
    check({"parent_v2_payload_sha256","parent_v2_transfer_receipt_path","parent_v2_transfer_receipt_sha256","parent_v2_verified_data_commit_sha"}.issubset(required),"F01 parent receipt binding incomplete")
    tx=f1.get("collection_transaction_id_contract") or {}
    check(tx.get("method_version")=="dev03-collection-transaction-id-v2","F01 transaction version drift")
    check("parent_v2_transfer_receipt_sha256" in set(tx.get("canonical_inputs") or []),"F01 receipt SHA not in transaction identity")
    check("No exact verified parent receipt" in str(f1.get("fail_closed")),"F01 fail-closed rule missing")

    f2=fixes.get("AUD-20260928-02-F02") or {}
    dims=set(f2.get("binding_dimensions") or [])
    check({"model","run_time_utc","valid_time_utc","forecast_lead_seconds","provider_product"}.issubset(dims),"F02 cycle binding incomplete")
    check(any("Do not invoke newest-cycle" in x for x in f2.get("rules") or []),"F02 newest-cycle prohibition missing")

    f3=fixes.get("AUD-20260928-02-F03") or {}
    inv=f3.get("inventory_contract") or {}
    check(inv.get("parent_field_inventory_sha256")=="sha256(canonical sorted inventory entries)","F03 inventory hash missing")
    check("step_range_native" in set(inv.get("inventory_entry_fields") or []),"F03 native interval identity missing")
    check(any("Every parent received native field identity/variant" in x for x in f3.get("replacement_precondition") or []),"F03 parent-variant preservation missing")

    f4=fixes.get("AUD-20260928-02-F04") or {}
    check(f4.get("immutable_root")=="data/weather_archive_v6/","F04 v6 immutable root drift")
    receipt=f4.get("publication_receipt") or {}
    check(receipt.get("method_version")=="dev03-archive-v6-publication-receipt-v1","F04 publication receipt missing")
    check("v3_attempt_id" in set(receipt.get("required_fields") or []),"F04 receipt attempt binding missing")
    check("only manifests authorized by a valid publication receipt" in str(f4.get("reader_rule")),"F04 reader publication gate missing")

    f5=fixes.get("AUD-20260928-02-F05") or {}
    check(set(f5.get("status_family") or [])==EXPECTED_AVAIL,"F05 status family drift")
    precedence=" ".join(f5.get("mapping_precedence") or [])
    check("unsupported_by_provider_or_product" in precedence and "not_requested_by_policy" in precedence,"F05 capability/policy precedence missing")
    check("Only received may carry a value." in set(f5.get("invariants") or []),"F05 value invariant missing")

    f6=fixes.get("AUD-20260928-02-F06") or {}
    fd=set(f6.get("field_identity_dimensions") or [])
    check({"step_range_native","start_step_native","end_step_native","step_units_native"}.issubset(fd),"F06 exact step identity missing")
    check(any("after the valid-time-specific native interval metadata is known" in x for x in f6.get("rules") or []),"F06 per-valid identity rule missing")

    f7=fixes.get("AUD-20260928-02-F07") or {}
    attempt=f7.get("v3_attempt_id_contract") or {}
    check(attempt.get("method_version")=="dev03-v3-attempt-id-v1","F07 attempt id version drift")
    check("collection_transaction_id" in str(attempt.get("formula")) and "v3_generated_at_utc" in str(attempt.get("formula")),"F07 attempt formula incomplete")
    check("never collide" in str(f7.get("immutable_path_rule")),"F07 immutable retry path rule missing")

    f8=fixes.get("AUD-20260928-02-F08") or {}
    check("ensemble_source_revision_set_id" in set(f8.get("set_key") or []),"F08 set key missing source set id")
    pre=" ".join(f8.get("v6_over_v5_precondition") or [])
    check("matching member availability" in pre and "matching field status" in pre and "no v5/v6 rows are composed" in pre,"F08 atomic cross-archive rule incomplete")

    f9=fixes.get("AUD-20260928-02-F09") or {}
    check(f9.get("required_cross_repo_gate") is True,"F09 cross-repo gate missing")
    pub=f9.get("public_enforcement_subset") or {}
    check(pub.get("hard_incremental_provider_requests_per_day")==500,"F09 request budget drift")
    check(set(pub.get("zero_extra_provider_paths") or [])==EXPECTED_ZERO,"F09 zero-request paths drift")
    check(pub.get("full_gefs_hard_requests")==800 and pub.get("full_gefs_hard_filtered_bytes")==2097152,"F09 GEFS limits drift")
    check(pub.get("public_workflow_timeout_minutes")==65 and pub.get("timeout_increase_allowed") is False,"F09 timeout guard drift")

    f10=fixes.get("AUD-20260928-02-F10") or {}
    check(set(f10.get("required_bundle_times") or [])=={"parent_v2_collector_generated_at_utc","v3_generated_at_utc"},"F10 time split drift")
    rules=" ".join(f10.get("rules") or [])
    check("orders v3 latest pointers" in rules and "never copied from parent v2" in rules,"F10 chronology rule incomplete")

    identity=c.get("shared_native_field_identity_v1") or {}
    dims=set(identity.get("deterministic_field_dimensions") or [])
    check({"step_range_native","start_step_native","end_step_native","step_units_native"}.issubset(dims),"shared native identity incomplete")
    check(identity.get("registry_version_excluded_from_identity") is True,"registry version must not churn native identity")

    repl=c.get("archive_v6_replacement_v2") or {}
    check(repl.get("method_version")=="dev03-v5-v6-replacement-v2","replacement version drift")
    check(repl.get("no_cross_archive_field_mix") is True,"cross-archive mixing must be forbidden")
    check("parent_field_inventory_sha256" in " ".join(repl.get("required_before_deterministic_replacement") or []),"replacement inventory gate missing")

    vr=c.get("validation_requirements") or {}
    check(set(vr.get("required_fix_ids") or [])==EXPECTED_FIX,"required fix ids drift")
    check(len(vr.get("required_adversarial_regressions") or [])==10,"expected ten correction regressions")
    check(vr.get("wp03_start_condition")=="VAL-20260928-02 complete with zero open P0/P1/P2","validation WP03 block drift")
    if problems: raise AssertionError("AUD-20260928-02 correction gate failed:\n- "+"\n- ".join(problems))
    return {"status":"PASS","fix_count":len(fixes),"cross_repo_required":True,"wp03_start_condition":vr["wp03_start_condition"],"operational_authority":"v16-c3-v9"}

def fetch_remote(repo):
    url=f"https://api.github.com/repos/{repo}/contents/config/dev03_pre_wp03_l1_l2_corrections_v1.json?ref=main"
    headers={"Accept":"application/vnd.github+json","User-Agent":"mardorf-aud02-corrections"}
    token=os.getenv("GH_READ_TOKEN") or os.getenv("GITHUB_TOKEN") or os.getenv("CROSS_REPO_TOKEN")
    if token: headers["Authorization"]=f"Bearer {token}"
    req=urllib.request.Request(url,headers=headers)
    with urllib.request.urlopen(req,timeout=30) as response: payload=json.load(response)
    if payload.get("encoding")!="base64": raise AssertionError("unexpected GitHub contents encoding")
    return base64.b64decode(payload["content"])

def validate_repository(root=ROOT,remote_repo=None):
    path=Path(root)/"config"/"dev03_pre_wp03_l1_l2_corrections_v1.json"
    local=path.read_bytes()
    report=check_contract(json.loads(local.decode("utf-8")))
    report["sha256"]=hashlib.sha256(local).hexdigest()
    if remote_repo:
        remote=fetch_remote(remote_repo)
        if remote!=local:
            raise AssertionError(f"correction contract cross-repo mismatch local={hashlib.sha256(local).hexdigest()} remote={hashlib.sha256(remote).hexdigest()}")
        report["cross_repo"]={"remote_repo":remote_repo,"byte_identical":True,"sha256":report["sha256"]}
    return report

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--root",type=Path,default=ROOT);ap.add_argument("--remote-repo")
    a=ap.parse_args();print(json.dumps(validate_repository(a.root,a.remote_repo),sort_keys=True))
if __name__=="__main__": main()
