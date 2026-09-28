#!/usr/bin/env python3
"""Binding cross-repository gate for AUD-20260928-02 correction contract v2."""
from __future__ import annotations
import argparse,base64,hashlib,json,os,urllib.request
from collections import Counter
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/"config"/"dev03_pre_wp03_l1_l2_corrections_v2.json"
EXPECTED_FIX={f"FIX-20260928-02-F{i:02d}" for i in range(1,17)}
EXPECTED_AVAIL={
 "received","unsupported_by_provider_or_product","intentionally_not_applicable",
 "not_requested_by_policy","not_yet_published","fetch_error","unknown_or_ambiguous"
}

def check_contract(c):
    problems=[]
    def check(ok,msg):
        if not ok: problems.append(msg)
    check(c.get("contract_version")=="dev03-pre-wp03-l1-l2-corrections-v2","version drift")
    check(c.get("supersedes")=="dev03-pre-wp03-l1-l2-corrections-v1","supersedes drift")
    check(c.get("status")=="binding_pre_wp03","status drift")
    check(c.get("audit_id")=="AUD-20260928-02","audit id drift")
    op=c.get("operational_boundary") or {}
    check(op.get("implementation_activation_authorized") is False,"implementation activation must remain false")
    check(op.get("l4_l5_cutover_authorized") is False,"cutover must remain false")
    check(op.get("frozen_v2_v5_predecessors_unchanged") is True,"predecessor protection missing")
    check(op.get("wp03_start_condition")=="VAL-20260928-02 complete with zero open P0/P1/P2","WP03 start condition drift")

    fixes=c.get("fixes") or {}
    actual_fix_ids={x.get("fix_id") for x in fixes.values()}
    check(actual_fix_ids==EXPECTED_FIX,f"fix set drift: {sorted(actual_fix_ids)}")
    sev=Counter(x.get("severity") for x in fixes.values())
    check(sev==Counter({"P1":13,"P2":3}),f"severity counts drift: {dict(sev)}")

    f1=fixes.get("AUD-20260928-02-F01") or {}
    check("parent_v2_transfer_receipt_sha256" in set(f1.get("parent_v2_binding_required_fields") or []),"F01 receipt SHA missing")
    check("No exact verified parent receipt" in str(f1.get("fail_closed")),"F01 fail-closed missing")

    f2=fixes.get("AUD-20260928-02-F02") or {}
    dims=set(f2.get("binding_dimensions") or [])
    check({"run_time_utc","valid_time_utc","forecast_lead_seconds","provider_product"}.issubset(dims),"F02 cycle binding incomplete")
    check(any("Do not invoke newest-cycle" in x for x in f2.get("rules") or []),"F02 latest-cycle prohibition missing")

    f3=fixes.get("AUD-20260928-02-F03") or {}
    inv=f3.get("inventory_contract") or {}
    check("step_range_native" in set(inv.get("inventory_entry_fields") or []),"F03 native variant inventory incomplete")
    check(inv.get("parent_field_inventory_sha256")=="sha256(canonical sorted inventory entries)","F03 inventory hash drift")

    f4=fixes.get("AUD-20260928-02-F04") or {}
    check(f4.get("immutable_root")=="data/weather_archive_v6/","F04 root drift")
    check(f4.get("superseded_by_detail")=="FIX-20260928-02-F12 defines the implementable two-phase publication protocol; F04 immutability/reader-gating intent remains binding.","F04/F12 supersession missing")

    f5=fixes.get("AUD-20260928-02-F05") or {}
    check(set(f5.get("status_family") or [])==EXPECTED_AVAIL,"F05 availability family drift")
    check("Only received may carry a value." in set(f5.get("invariants") or []),"F05 value invariant missing")

    f6=fixes.get("AUD-20260928-02-F06") or {}
    check({"step_range_native","start_step_native","end_step_native","step_units_native"}.issubset(set(f6.get("field_identity_dimensions") or [])),"F06 interval identity incomplete")

    f7=fixes.get("AUD-20260928-02-F07") or {}
    v2=f7.get("superseded_attempt_identity_detail") or {}
    check(v2.get("method_version")=="dev03-v3-attempt-id-v2","F07 v2 attempt identity missing")
    check("attempt_nonce" in set(v2.get("canonical_inputs") or []),"F07 collision-safe nonce missing")
    check("wall-clock timestamp uniqueness" in str(v2.get("rule")),"F07 timestamp collision rule missing")

    f8=fixes.get("AUD-20260928-02-F08") or {}
    check("ensemble_source_revision_set_id" in set(f8.get("set_key") or []),"F08 set key missing")
    check("no v5/v6 rows are composed" in " ".join(f8.get("v6_over_v5_precondition") or []),"F08 cross-archive mixing guard missing")

    f9=fixes.get("AUD-20260928-02-F09") or {}
    check(f9.get("required_cross_repo_gate") is True,"F09 cross-repo gate missing")

    f10=fixes.get("AUD-20260928-02-F10") or {}
    check(set(f10.get("required_bundle_times") or [])=={"parent_v2_collector_generated_at_utc","v3_generated_at_utc"},"F10 time split drift")
    check(f10.get("pointer_ordering_superseded_by")=="FIX-20260928-02-F14","F10/F14 pointer supersession missing")

    f11=fixes.get("AUD-20260928-02-F11") or {}
    f11_rules=" ".join(f11.get("rules") or []).lower()\n    check("network request" in f11_rules and "exact parent" in f11_rules and ("may execute until" in f11_rules or "before" in f11_rules),"F11 pre-network parent binding missing")
    order=c.get("corrected_wp03_execution_order_v2") or []
    check([x.get("step_id") for x in order]==[f"WP03-I{i:02d}" for i in range(1,11)],"corrected WP03 step ids/order drift")
    i3=next((x for x in order if x.get("step_id")=="WP03-I03"),{})
    i4=next((x for x in order if x.get("step_id")=="WP03-I04"),{})
    check(i3.get("network_requests_allowed") is False and i4.get("network_requests_allowed") is True,"F11 I03/I04 network boundary drift")
    check("WP03-I03 PASS" in str(i4.get("precondition")),"F11 I04 parent-binding precondition missing")

    f12=fixes.get("AUD-20260928-02-F12") or {}
    proto=f12.get("publication_protocol") or {}
    receipt=f12.get("publication_receipt_v2") or {}
    check(proto.get("method_version")=="dev03-archive-v6-publication-protocol-v2","F12 publication protocol drift")
    fields=set(receipt.get("required_fields") or [])
    check("verified_data_commit_sha" in fields and "data_artifact_set_sha256" in fields,"F12 receipt data-commit binding missing")
    check("published_commit_sha_or_candidate_tree_sha" not in fields,"F12 self-referential field must not return")
    check("never contains its own receipt commit/tree SHA" in str(proto.get("phase_2")),"F12 self-reference prohibition missing")
    check("No valid phase-2 receipt" in str(f12.get("fail_closed")),"F12 fail-closed missing")

    f13=fixes.get("AUD-20260928-02-F13") or {}
    r13=" ".join(f13.get("rules") or [])
    check("500 incremental v3-only provider requests/day includes initial attempts and all retries combined" in r13,"F13 retry request accounting missing")
    check("25% hard ceiling" in r13,"F13 retry byte accounting missing")
    check("retry_deferred_budget_exhausted" in set(f13.get("required_metrics") or []),"F13 deferred retry metric missing")

    f14=fixes.get("AUD-20260928-02-F14") or {}
    ptr=f14.get("pointer_contract") or {}
    current=ptr.get("current_parent_pointer") or {}
    event=ptr.get("attempt_event_pointer") or {}
    check(current.get("primary_order")=="parent_v2_collector_generated_at_utc","F14 current-parent order drift")
    check(event.get("path")=="data/inbox/public_collector_v3/transfer_receipts/models/latest_attempt.json","F14 attempt-event pointer drift")
    check("older parent" in str(current.get("rule")).lower(),"F14 old-parent rollback protection missing")

    f15=fixes.get("AUD-20260928-02-F15") or {}
    promo=f15.get("promotion_contract") or {}
    check(promo.get("method_version")=="dev03-v3-receipt-drain-v1","F15 drain method drift")
    check("Enumerate immutable successful v3 transfer receipts" in str(promo.get("inventory")),"F15 receipt enumeration missing")
    check("drains all pending receipts" in str(promo.get("recovery_rule")),"F15 backlog drain guarantee missing")
    check(set(promo.get("high_water_fields") or [])=={"v3_generated_at_utc","v3_attempt_id","transfer_receipt_sha256"},"F15 high-water identity drift")

    f16=fixes.get("AUD-20260928-02-F16") or {}
    chron=f16.get("parent_source_chronology_contract") or {}
    exact=set(chron.get("exact_parent_fields") or [])
    check({"availability_status","availability_observed_at_utc","field_available_at_utc","source_provenance"}.issubset(exact),"F16 parent source chronology incomplete")
    check("must not be copied from v5" in str(chron.get("intentionally_not_equal")),"F16 private first_seen distinction missing")
    check("parent_source_chronology_sha256" in " ".join(f16.get("replacement_precondition") or []),"F16 replacement chronology gate missing")

    repl=c.get("archive_v6_replacement_v3") or {}
    check(repl.get("method_version")=="dev03-v5-v6-replacement-v3","replacement v3 method drift")
    req=" ".join(repl.get("required_before_deterministic_replacement") or [])
    check("parent_source_chronology_sha256" in req and "parent_field_inventory_sha256" in req,"replacement lossless gates incomplete")
    check(repl.get("no_cross_archive_field_mix") is True,"cross-archive field mixing must remain forbidden")

    vr=c.get("validation_requirements") or {}
    check(set(vr.get("required_fix_ids") or [])==EXPECTED_FIX,"validation fix set drift")
    check(len(vr.get("required_adversarial_regressions") or [])==16,"expected 16 adversarial correction regressions")
    check(vr.get("wp03_start_condition")=="VAL-20260928-02 complete with zero open P0/P1/P2","validation start condition drift")

    summary=c.get("audit_summary") or {}
    check(summary.get("finding_count")==16,"finding count drift")
    check(summary.get("severities")=={"P0":0,"P1":13,"P2":3,"P3":0},"severity summary drift")

    if problems:
        raise AssertionError("AUD-20260928-02 correction-v2 gate failed:\n- "+"\n- ".join(problems))
    return {
      "status":"PASS","contract_version":c["contract_version"],"fix_count":len(fixes),
      "severities":dict(sev),"cross_repo_required":True,
      "wp03_start_condition":vr["wp03_start_condition"],"operational_authority":"v16-c3-v9"
    }

def fetch_remote(repo):
    url=f"https://api.github.com/repos/{repo}/contents/config/dev03_pre_wp03_l1_l2_corrections_v2.json?ref=main"
    headers={"Accept":"application/vnd.github+json","User-Agent":"mardorf-aud02-corrections-v2"}
    token=os.getenv("GH_READ_TOKEN") or os.getenv("GITHUB_TOKEN") or os.getenv("CROSS_REPO_TOKEN")
    if token: headers["Authorization"]=f"Bearer {token}"
    req=urllib.request.Request(url,headers=headers)
    with urllib.request.urlopen(req,timeout=30) as response: payload=json.load(response)
    if payload.get("encoding")!="base64": raise AssertionError("unexpected GitHub contents encoding")
    return base64.b64decode(payload["content"])

def validate_repository(root=ROOT,remote_repo=None):
    path=Path(root)/"config"/"dev03_pre_wp03_l1_l2_corrections_v2.json"
    local=path.read_bytes()
    report=check_contract(json.loads(local.decode("utf-8")))
    report["sha256"]=hashlib.sha256(local).hexdigest()
    if remote_repo:
        remote=fetch_remote(remote_repo)
        if remote!=local:
            raise AssertionError(f"correction-v2 cross-repo mismatch local={hashlib.sha256(local).hexdigest()} remote={hashlib.sha256(remote).hexdigest()}")
        report["cross_repo"]={"remote_repo":remote_repo,"byte_identical":True,"sha256":report["sha256"]}
    return report

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--root",type=Path,default=ROOT);ap.add_argument("--remote-repo")
    a=ap.parse_args();print(json.dumps(validate_repository(a.root,a.remote_repo),sort_keys=True))
if __name__=="__main__": main()
