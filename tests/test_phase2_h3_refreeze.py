import hashlib
import json
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SUCCESSOR=ROOT/"config/phase2_successor_contract_v2.json"
REFREEZE=ROOT/"config/phase2_h3_successor_refreeze_v1.json"
FROZEN_V1=ROOT/"config/phase2_frozen_contract_v1.json"


def git_blob_sha(data):
    return hashlib.sha1(f"blob {len(data)}\0".encode("ascii")+data).hexdigest()


class Phase2H3SuccessorRefreezeTests(unittest.TestCase):
    def setUp(self):
        self.contract=json.loads(SUCCESSOR.read_text(encoding="utf-8"))
        self.refreeze_bytes=REFREEZE.read_bytes()
        self.refreeze=json.loads(self.refreeze_bytes)
        self.v1_bytes=FROZEN_V1.read_bytes()
        self.v1=json.loads(self.v1_bytes)

    def test_successor_is_explicitly_refrozen(self):
        self.assertEqual(self.contract["status"],"complete_frozen")
        self.assertEqual(self.contract["refrozen_on_utc_date"],"2026-09-28")
        self.assertEqual(self.contract["refreeze"]["refreeze_version"],"phase2-h3-successor-refreeze-v1")
        self.assertEqual(self.contract["refreeze"]["status"],"complete_frozen")
        self.assertEqual(
            git_blob_sha(self.refreeze_bytes),
            self.contract["refreeze"]["acceptance_artifact_git_blob_sha"],
        )

    def test_shared_core_and_accepted_versions_are_pinned(self):
        canonical=json.dumps(
            self.contract["shared_core"],
            sort_keys=True,
            separators=(",",":"),
            ensure_ascii=False,
        ).encode("utf-8")
        self.assertEqual(hashlib.sha256(canonical).hexdigest(),self.contract["shared_core_sha256"])
        self.assertEqual(
            self.refreeze["successor_contract"]["shared_core_sha256"],
            self.contract["shared_core_sha256"],
        )
        self.assertEqual(
            self.refreeze["successor_contract"]["accepted_implementation_versions"],
            self.contract["implementation_versions"],
        )

    def test_frozen_v1_history_is_unchanged_and_pinned(self):
        self.assertEqual(self.v1["contract_version"],"phase2-acquisition-storage-freeze-v1")
        self.assertEqual(self.v1["status"],"complete_frozen")
        pins=self.refreeze["immutable_predecessor_v1"]["repository_git_blob_sha"]
        self.assertIn(git_blob_sha(self.v1_bytes),set(pins.values()))
        self.assertFalse(self.refreeze["immutable_predecessor_v1"]["mutation_permitted"])

    def test_final_findings_close_and_phase3_handoff(self):
        self.assertEqual(
            self.refreeze["final_audit_closure"]["active_findings_after_H3"],
            {"P0":0,"P1":0,"P2":0,"P3":0,"total":0},
        )
        self.assertEqual(
            self.refreeze["final_audit_closure"]["closed_findings"],
            ["P2-AUDIT-008","P3-AUDIT-011"],
        )
        handoff=self.refreeze["phase3_handoff"]
        self.assertFalse(handoff["feature_broker_implemented_by_H3"])
        self.assertTrue(handoff["phase2_complete"])
        self.assertTrue(handoff["phase3_feature_broker_may_begin"])
        self.assertEqual(handoff["next_action"],"Phase 3 - Meteorology Feature Broker")


if __name__=="__main__":
    unittest.main()
