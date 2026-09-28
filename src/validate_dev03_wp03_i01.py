#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.request
from pathlib import Path

from forecast_lead_identity import (
    ForecastLeadIdentityError,
    INT64_MAX,
    lead_seconds_from_hours,
    validate_forecast_lead_identity,
)

ROOT = Path(__file__).resolve().parents[1]
LEAD_PATH = "config/forecast_lead_identity_v1.json"
SHADOW_PATH = "config/dev03_shadow_channels_v1.json"
CONTROLS = {
    "public_v3_generation_enabled",
    "public_v3_transfer_enabled",
    "private_v3_promotion_enabled",
    "archive_v6_write_enabled",
}


def _load(root: Path, path: str):
    return json.loads((root / path).read_text(encoding="utf-8"))


def validate_repository(root=ROOT):
    root = Path(root)
    lead = _load(root, LEAD_PATH)
    shadow = _load(root, SHADOW_PATH)
    problems = []

    def check(ok, msg):
        if not ok:
            problems.append(msg)

    check(lead.get("artifact_version") == "forecast-lead-identity-v1", "lead artifact version drift")
    check(lead.get("status") == "implemented_shadow_only", "lead status drift")
    check(lead.get("implementation_step") == "WP03-I01", "lead implementation step drift")
    auth = lead.get("authority") or {}
    check((auth.get("l1_acquisition_lead_hours") or {}).get("integral_hours_only") is True, "integral-hour L1 rule missing")
    check((auth.get("l2_lead_seconds") or {}).get("authoritative") is True, "lead_seconds authority missing")
    check((auth.get("timestamp_identity") or {}).get("equation") == "valid_time_utc - run_time_utc == lead_seconds seconds", "timestamp equation drift")
    text = " ".join(lead.get("invariants") or []).lower()
    check("never use int()" in text and "fails closed" in text, "non-coercive fail-closed invariants missing")
    op = lead.get("operational_boundary") or {}
    check(op.get("authorizes_network_requests") is False, "I01 must not authorize network")
    check(op.get("authorizes_l4_l5_cutover") is False, "lead contract must not authorize cutover")
    check(op.get("operational_authority_unchanged") == "v16-c3-v9", "operational authority drift")

    check(shadow.get("artifact_version") == "dev03-shadow-channels-v1", "shadow artifact version drift")
    check(shadow.get("status") == "implemented_disabled", "shadow status drift")
    check(shadow.get("implementation_step") == "WP03-I01", "shadow implementation step drift")
    controls = shadow.get("controls") or {}
    check(set(controls) == CONTROLS, "shadow control set drift")
    check(all(controls.get(k) is False for k in CONTROLS), "all shadow controls must default false")
    check(shadow.get("defaults_are_binding") is True, "binding disabled defaults missing")
    check(shadow.get("independent_controls_required") is True, "independent controls requirement missing")
    policy = shadow.get("activation_policy") or {}
    prereq = set(policy.get("minimum_prerequisites") or [])
    check(policy.get("automatic_activation_forbidden") is True, "automatic activation must be forbidden")
    check({"WP03-I03 PASS", "PREP07 resource instrumentation PASS", "Explicit versioned control change"}.issubset(prereq), "activation prerequisites incomplete")
    cutover = shadow.get("cutover_authority") or {}
    check(cutover.get("can_authorize_operational_cutover") is False, "shadow controls cannot authorize cutover")
    check(cutover.get("operational_authority_unchanged") == "v16-c3-v9", "shadow authority drift")
    network = shadow.get("network_boundary") or {}
    check(network.get("wp03_i01_network_requests_allowed") is False, "I01 network boundary drift")
    check(network.get("first_network_step") == "WP03-I04", "first network step drift")
    check("WP03-I03 PASS" in str(network.get("wp03_i04_precondition")), "I04 I03 precondition missing")

    try:
        check(lead_seconds_from_hours(2) == 7200, "hour-to-second exact conversion failed")
        sample = validate_forecast_lead_identity(
            run_time_utc="2026-09-28T00:00:00Z",
            valid_time_utc="2026-09-28T02:00:00Z",
            lead_seconds=7200,
            acquisition_lead_hours=2,
        )
        check(sample["lead_seconds"] == 7200, "sample exact lead validation failed")
    except ForecastLeadIdentityError as exc:
        problems.append(f"helper rejected valid exact identity: {exc}")

    for bad in (1.5, True, "1"):
        try:
            lead_seconds_from_hours(bad)
            problems.append(f"helper accepted lossy/non-strict acquisition lead: {bad!r}")
        except ForecastLeadIdentityError:
            pass
    for bad in (INT64_MAX + 1,):
        try:
            lead_seconds_from_hours(bad)
            problems.append("helper accepted acquisition lead whose seconds overflow int64")
        except ForecastLeadIdentityError:
            pass
    try:
        validate_forecast_lead_identity(
            run_time_utc="2026-09-28T00:00:00Z",
            valid_time_utc="2026-09-28T02:00:00Z",
            lead_seconds=3600,
        )
        problems.append("helper accepted contradictory timestamp/lead identity")
    except ForecastLeadIdentityError:
        pass

    if problems:
        raise AssertionError("DEV03-WP03-I01 gate failed:\n- " + "\n- ".join(problems))
    return {
        "status": "PASS",
        "implementation_step": "WP03-I01",
        "lead_contract": lead["artifact_version"],
        "shadow_contract": shadow["artifact_version"],
        "shadow_controls_disabled": 4,
        "network_requests_allowed": False,
        "operational_authority": "v16-c3-v9",
    }


def _remote_bytes(repo: str, ref: str, path: str) -> bytes:
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={ref}"
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    token = os.environ.get("GH_READ_TOKEN") or os.environ.get("CROSS_REPO_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        payload = json.load(response)
    return base64.b64decode(payload["content"])


def validate_cross_repo(remote_repo: str, remote_ref: str = "main", root=ROOT):
    root = Path(root)
    for path in (LEAD_PATH, SHADOW_PATH):
        local = (root / path).read_bytes()
        remote = _remote_bytes(remote_repo, remote_ref, path)
        if local != remote:
            raise AssertionError(f"cross-repository byte mismatch: {path}")
    result = validate_repository(root)
    result["cross_repo"] = "PASS"
    result["remote_repo"] = remote_repo
    result["remote_ref"] = remote_ref
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote-repo")
    parser.add_argument("--remote-ref", default="main")
    args = parser.parse_args()
    result = validate_cross_repo(args.remote_repo, args.remote_ref) if args.remote_repo else validate_repository()
    print(json.dumps(result, sort_keys=True))
