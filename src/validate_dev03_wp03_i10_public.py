#!/usr/bin/env python3
from __future__ import annotations
import hashlib
import json
import subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
FROZEN_V2_WORKFLOW_BLOB="b26223764a0702a04b6b40d90a63da6507a54d59"


def validate(root=ROOT):
    root=Path(root)
    problems=[]
    def check(ok,msg):
        if not ok: problems.append(msg)
    cfg=json.loads((root/"config/dev03_wp03_i10_live_proof_v1.json").read_text())
    frozen=json.loads((root/"config/dev03_shadow_channels_v1.json").read_text())["controls"]
    controls=json.loads((root/"config/dev03_wp03_i10_runtime_controls_v1.json").read_text())["controls"]
    workflow=(root/".github/workflows/dev03-v3-live-proof.yml").read_text()
    runner=(root/"src/run_dev03_wp03_i10_public.py").read_text()
    publisher=(root/"src/push_private_v3.py").read_text()

    check(cfg.get("artifact_version")=="dev03-wp03-i10-live-proof-v1","I10 public contract drift")
    check(cfg.get("trigger_policy")=="workflow_dispatch_or_versioned_runtime_control_push_no_cron","I10 public contract trigger label drift")
    check("schedule:" not in workflow,"I10 live workflow must not create a cron")
    check("workflow_dispatch:" in workflow,"I10 live workflow must retain manual trigger")
    check("config/dev03_wp03_i10_runtime_controls_v1.json" in workflow,"runtime-control push trigger missing")
    check(all(value is False for value in frozen.values()),"frozen I01 shadow defaults must remain false")
    check("contents: read" in workflow,"public I10 workflow must have read-only public-repo permission")
    check("data/inbox/public_collector_v3" in publisher,"isolated v3 namespace missing")
    check("data/inbox/public_collector/" not in publisher.replace("data/inbox/public_collector_v3",""),
          "v3 publisher contains legacy public_collector write path")
    check("ICON-D2" in runner and "ICON-EU" in runner and "GFS" in runner,"required deterministic paths missing")
    for token in ("ECMWF-IFS","GEFS-control","NOAA_GEFS_FULL","ICON-D2-EPS"):
        check(token in runner,f"zero-request proof label missing for {token}")
    check("matched_v2_response_bytes" in runner,"matched deterministic network measurement not wired")
    check("public_incremental_wall_hard_max_seconds" in runner,"public runtime hard gate missing")
    check("paired_v2_compressed_bytes" in publisher,"v3/v2 compressed transfer hard gate missing")

    gen=controls.get("public_v3_generation_enabled")
    transfer=controls.get("public_v3_transfer_enabled")
    check(gen is transfer,"public v3 generation and transfer controls must move together for I10")
    check(controls.get("private_v3_promotion_enabled") is False and controls.get("archive_v6_write_enabled") is False,
          "public repo must not activate private controls")

    try:
        blob=subprocess.check_output(
            ["git","hash-object",str(root/".github/workflows/collect-models.yml")],
            text=True,
        ).strip()
        check(blob==FROZEN_V2_WORKFLOW_BLOB,"frozen v2 collect-models workflow changed")
    except Exception as exc:
        problems.append(f"cannot verify frozen v2 workflow blob: {exc}")

    if problems:
        raise AssertionError("DEV03 WP03 I10 public wiring failed:\n- "+"\n- ".join(problems))
    return {
        "status":"PASS",
        "implementation_step":"WP03-I10",
        "frozen_defaults_all_false":all(value is False for value in frozen.values()),
        "public_generation_enabled":gen,
        "public_transfer_enabled":transfer,
        "frozen_v2_workflow_blob":FROZEN_V2_WORKFLOW_BLOB,
        "new_cron_jobs":0,
        "operational_authority":"v16-c3-v9",
    }


if __name__=="__main__":
    print(json.dumps(validate(),sort_keys=True))
