#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check(ok, message, problems):
    if not ok:
        problems.append(message)


def main():
    problems = []
    contract = json.loads(
        (ROOT / "config" / "dev03_wp03_i03_parent_transfer_contract_v1.json").read_text()
    )
    check(contract.get("implementation_step") == "WP03-I03", "step drift", problems)
    check(contract.get("network_requests_allowed") is False, "I03 must stay network-free", problems)
    check(contract.get("operational_authority") == "v16-c3-v9", "authority drift", problems)
    methods = contract.get("methods") or {}
    check(methods.get("collection_transaction_id") == "dev03-collection-transaction-id-v2", "transaction method drift", problems)
    check(methods.get("parent_cycle_binding") == "dev03-parent-cycle-binding-v1", "cycle method drift", problems)
    check(methods.get("v3_attempt_id") == "dev03-v3-attempt-id-v2", "attempt method drift", problems)
    check(methods.get("pointer_ordering") == "dev03-v3-pointer-ordering-v2", "pointer method drift", problems)
    check(methods.get("transfer_readback") == "private-transfer-readback-v2", "readback method drift", problems)
    required = set(contract.get("parent_v2_binding_required_fields") or [])
    check(required == {
        "parent_v2_payload_sha256",
        "parent_v2_collector_generated_at_utc",
        "parent_v2_transfer_receipt_path",
        "parent_v2_transfer_receipt_sha256",
        "parent_v2_verified_data_commit_sha",
    }, "parent binding field drift", problems)
    pointers = contract.get("mutable_pointers") or {}
    check((pointers.get("current_parent") or {}).get("older_parent_may_replace") is False, "old-parent rollback protection missing", problems)
    check((pointers.get("attempt_event") or {}).get("path") == "data/inbox/public_collector_v3/transfer_receipts/models/latest_attempt.json", "attempt pointer path drift", problems)
    source = (ROOT / "src" / "dev03_v3_parent_binding.py").read_text()
    transfer = (ROOT / "src" / "dev03_v3_transfer_shell.py").read_text()
    workflow = (ROOT / ".github" / "workflows" / "collect-models.yml").read_text()
    push = (ROOT / "src" / "push_private.py").read_text()
    forbidden_imports = ("import requests", "from requests", "import urllib", "from urllib")
    check(not any(token in source for token in forbidden_imports), "parent binding acquired network dependency", problems)
    check(not any(token in transfer for token in forbidden_imports), "transfer shell acquired network dependency", problems)
    check("data/inbox/public_collector_v3" in source, "isolated v3 namespace missing", problems)
    check("--result-json work/model_transfer_result.json" in workflow, "machine-readable parent transfer result not wired", problems)
    check("--receipt-copy work/model_transfer_receipt.json" in workflow, "exact parent receipt copy not wired", problems)
    check("transfer_receipt_sha256" in push, "v2 transfer does not expose immutable receipt sha", problems)
    check("verified_data_commit_sha" in push, "v2 transfer does not expose verified data commit", problems)
    controls = json.loads((ROOT / "config" / "dev03_shadow_channels_v1.json").read_text())
    control_map = controls.get("controls") or controls.get("shadow_controls") or {}
    for name in contract.get("shadow_controls_expected_false") or []:
        value = control_map.get(name)
        if isinstance(value, dict):
            value = value.get("enabled")
        check(value is False, f"shadow control {name} must remain false", problems)
    if problems:
        print(json.dumps({"status": "FAIL", "problems": problems}, indent=2))
        raise SystemExit(1)
    print(json.dumps({"status": "PASS", "implementation_step": "WP03-I03", "network_requests": 0}, indent=2))


if __name__ == "__main__":
    main()
