#!/usr/bin/env python3
"""Executable gate for DEV03-PREP05 migration/compatibility topology."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import urllib.request
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
CONTRACT=ROOT/"config"/"dev03_prep05_migration_topology_v1.json"

EXPECTED_SWITCHES={
    "public_v3_generation_enabled",
    "public_v3_transfer_enabled",
    "private_v3_promotion_enabled",
    "archive_v6_write_enabled",
}
FORBIDDEN_OPERATIONAL_WRITES={
    "data/raw/model_snapshots",
    "data/raw/ensemble_hourly",
    "data/weather_archive",
    "data/forecasts",
    "data/skill/v16*",
    "notification state",
}


def load(path=CONTRACT):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def validate_contract(c):
    problems=[]
    def check(ok,msg):
        if not ok: problems.append(msg)

    check(c.get("topology_version")=="dev03-prep05-migration-topology-v1","topology version drift")
    check(c.get("development_phase")=="DEV-03","development phase drift")
    check(c.get("preparation_step")=="DEV03-PREP05","PREP id drift")
    check(c.get("status") in {"candidate","complete"},"invalid status")

    dep=c.get("depends_on") or {}
    check(dep.get("work_package")=="DEV03-WP02","PREP05 must depend on WP02")
    check(dep.get("required_shared_blob")=="b30438fbfbb0122609edc0cb79a99f4d9b1247a0","WP02 blob drift")

    op=c.get("existing_operational_path") or {}
    check(op.get("private_inbox_root")=="data/inbox/public_collector","legacy inbox drift")
    check(op.get("integrity_latest_success")=="data/inbox/public_collector/integrity/models/latest_success.json","legacy trigger drift")
    check(op.get("canonical_latest")=="data/raw/model_snapshots/latest.json","legacy canonical latest drift")
    check(op.get("archive_root")=="data/weather_archive","Archive-v5 root drift")
    check(op.get("operational_authority")=="v16-c3-v9","operational authority drift")
    check(op.get("mutation_policy")=="unchanged","legacy mutation boundary missing")

    pub=c.get("public_v3_sidecar") or {}
    check(pub.get("work_bundle")=="work/model_bundle_v3.json","v3 work bundle drift")
    check(pub.get("legacy_bundle_must_not_be_mutated") is True,"v3 may not mutate v2 bundle")
    linkage=set(pub.get("linkage_required") or [])
    check({"parent_v2_payload_sha256","collection_transaction_id"}.issubset(linkage),"v3 linkage incomplete")
    check("successfully" in str(pub.get("v2_transfer_dependency")).lower(),"v3 must depend on successful v2 transfer")

    tx=c.get("v3_transfer_namespace") or {}
    check(tx.get("private_root")=="data/inbox/public_collector_v3","v3 inbox root drift")
    check(tx.get("integrity_latest_success")=="data/inbox/public_collector_v3/integrity/models/latest_success.json","v3 latest_success drift")
    check(tx.get("channel_id")=="dev03-v3-shadow","v3 channel drift")
    check(tx.get("legacy_trigger_collision_prohibited") is True,"legacy trigger collision must be prohibited")
    check(tx.get("readback_method_reused")=="private-transfer-readback-v2","readback method reuse drift")

    promo=c.get("private_v3_shadow_promotion") or {}
    check(promo.get("workflow_path")==".github/workflows/dev03-v3-shadow-promotion.yml","shadow workflow drift")
    check(promo.get("trigger_path")==tx.get("integrity_latest_success"),"private v3 trigger must use isolated latest_success")
    check(promo.get("promoter_script")=="src/promote_dev03_v3_shadow.py","shadow promoter drift")
    check(promo.get("network_access") is False,"shadow promoter must be network-free")
    check(set(promo.get("forbidden_writes") or [])==FORBIDDEN_OPERATIONAL_WRITES,"forbidden operational writes drift")

    a6=c.get("archive_v6") or {}
    check(a6.get("root")=="data/weather_archive_v6","Archive-v6 root drift")
    check(a6.get("no_write_to_v5") is True,"v6 must not write v5")
    check(a6.get("no_bulk_historical_copy") is True,"bulk legacy migration forbidden")
    check(a6.get("prospective_start_only") is True,"v6 must start prospectively")
    check("Never" in str(a6.get("private_first_seen_rule")) or "never" in str(a6.get("private_first_seen_rule")),"first_seen backdating rule missing")

    link=c.get("cross_archive_link") or {}
    check(link.get("method_version")=="dev03-v5-v6-occurrence-link-v1","occurrence-link version drift")
    check(link.get("output_field")=="migration_source_occurrence_id","occurrence-link output drift")
    formula=str(link.get("key_formula") or "")
    check("logical_record_id" in formula and "parent_or_v5_payload_sha256" in formula,"occurrence key formula incomplete")
    check(link.get("excluded_from_meteorological_identity") is True,"migration key must not become meteorological identity")
    check("revision_event_id" in str(link.get("reason_revision_event_id_not_used")),"revision-event non-use rationale missing")

    read=c.get("l2_l3_dual_read") or {}
    check(read.get("adapter_method_version")=="legacy-archive-v5-adapter-v1","legacy adapter drift")
    check("exact same as_of=T" in str(read.get("one_as_of_rule")),"single-as_of rule missing")
    algorithm=" ".join(read.get("selection_algorithm") or [])
    check("suppress the linked legacy occurrence" in algorithm,"v6-over-v5 precedence missing")
    check("Never compose v5 and v6 fields" in algorithm,"mixed-field prohibition missing")
    check("either a value or an explicit availability state" in str(read.get("v6_replacement_precondition")),"v6 replacement completeness gate missing")
    hist=read.get("historical_convective_precipitation_projection") or {}
    check(hist.get("status_for_capable_history")=="not_requested_by_policy","historical capable F02 status drift")
    check(hist.get("status_for_unsupported_history")=="unsupported_by_provider_or_product","historical unsupported F02 status drift")
    check(hist.get("value_backfill_forbidden") is True,"historical convective value backfill forbidden")
    check(hist.get("cape_or_total_precip_substitution_forbidden") is True,"CAPE/TP substitution forbidden")

    atom=c.get("write_atomicity") or {}
    check("cannot mutate v2" in str(atom.get("public_v3")),"public channel isolation missing")
    check("before canonical latest/promotion_state advance" in str(atom.get("private_v3")),"private promotion ordering missing")
    check("Never include operational v2/v5/v16 outputs" in str(atom.get("no_cross_namespace_atomic_commit")),"cross-namespace atomic commit prohibition missing")

    failures=c.get("failure_domains") or {}
    check(failures.get("operational_effect")=="none","v3 failures must have no operational effect")
    check("no v3 latest_success" in str(failures.get("v3_identity_or_schema_failure")),"identity failure boundary missing")
    check("degraded_fallback_legacy" in str(failures.get("l2_l3_v6_validation_failure")),"explicit degraded fallback missing")

    controls=c.get("control_plane") or {}
    check(set(controls.get("switches") or [])==EXPECTED_SWITCHES,"shadow control switches drift")
    check(controls.get("default_before_WP03_activation") is False,"shadow controls must default disabled")
    check("cannot authorize operational cutover" in str(controls.get("semantics")),"control plane cutover boundary missing")

    rollback=c.get("rollback") or {}
    check("do not erase immutable evidence" in str(rollback.get("principle")),"rollback evidence preservation missing")
    check(rollback.get("adapter_emergency_mode")=="legacy_only_degraded","emergency adapter mode drift")
    check("delete or rewrite v1-v5 history" in set(rollback.get("prohibited") or []),"legacy rewrite prohibition missing")

    cut=c.get("cutover_boundary") or {}
    check(cut.get("dev03_operational_cutover_allowed") is False,"DEV03 cutover must be false")
    check(cut.get("no_pointer_switch_cutover") is True,"pointer-switch cutover must be forbidden")
    check("v16/C3 decision authority" in set(cut.get("forbidden_targets") or []),"v16 authority boundary missing")

    gate=c.get("prep05_exit_gate") or {}
    check(gate.get("next_preparation_step")=="DEV03-PREP06","next PREP drift")
    check(gate.get("wp03_still_blocked_until")=="DEV03-PREP08 complete","WP03 block drift")

    if problems:
        raise AssertionError("DEV03-PREP05 topology gate failed:\n- "+"\n- ".join(problems))
    return {
        "status":"PASS",
        "topology_version":c["topology_version"],
        "v3_inbox_root":tx["private_root"],
        "archive_v6_root":a6["root"],
        "control_switch_count":len(EXPECTED_SWITCHES),
        "operational_authority":"v16-c3-v9",
        "cutover_allowed":False,
    }


def fetch_remote(repo):
    url=f"https://api.github.com/repos/{repo}/contents/config/dev03_prep05_migration_topology_v1.json?ref=main"
    headers={"Accept":"application/vnd.github+json","User-Agent":"mardorf-dev03-prep05-validator"}
    token=os.getenv("GH_READ_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token: headers["Authorization"]=f"Bearer {token}"
    req=urllib.request.Request(url,headers=headers)
    with urllib.request.urlopen(req,timeout=30) as response:
        payload=json.load(response)
    if payload.get("encoding")!="base64":
        raise AssertionError("unexpected GitHub contents encoding")
    return base64.b64decode(payload["content"])


def validate_remote_identity(local_bytes,repo):
    remote=fetch_remote(repo)
    if local_bytes!=remote:
        raise AssertionError(
            "PREP05 topology differs across repositories: "
            f"local={hashlib.sha256(local_bytes).hexdigest()} remote={hashlib.sha256(remote).hexdigest()}"
        )
    return {"remote_repo":repo,"byte_identical":True,"sha256":hashlib.sha256(local_bytes).hexdigest()}


def validate_repository(root=ROOT,remote_repo=None):
    path=Path(root)/"config"/"dev03_prep05_migration_topology_v1.json"
    local=path.read_bytes()
    report=validate_contract(json.loads(local.decode("utf-8")))
    if remote_repo:
        report["cross_repo"]=validate_remote_identity(local,remote_repo)
    return report


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",type=Path,default=ROOT)
    ap.add_argument("--remote-repo")
    args=ap.parse_args()
    print(json.dumps(validate_repository(args.root,args.remote_repo),sort_keys=True))


if __name__=="__main__":
    main()
