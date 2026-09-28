import json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
import validate_dev03_pre_wp03_corrections_v2 as gate

class PreWp03CorrectionV2Tests(unittest.TestCase):
    def setUp(self):
        self.c=json.loads((ROOT/"config/dev03_pre_wp03_l1_l2_corrections_v2.json").read_text())
    def test_repository_gate(self):
        r=gate.validate_repository(ROOT)
        self.assertEqual(r["status"],"PASS")
        self.assertEqual(r["fix_count"],16)
        self.assertEqual(r["severities"],{"P1":13,"P2":3})
    def test_parent_binding_precedes_network_fetch(self):
        order={x["step_id"]:x for x in self.c["corrected_wp03_execution_order_v2"]}
        self.assertFalse(order["WP03-I03"]["network_requests_allowed"])
        self.assertTrue(order["WP03-I04"]["network_requests_allowed"])
        self.assertIn("WP03-I03 PASS",order["WP03-I04"]["precondition"])
    def test_archive_publication_receipt_is_not_self_referential(self):
        f=self.c["fixes"]["AUD-20260928-02-F12"]
        fields=set(f["publication_receipt_v2"]["required_fields"])
        self.assertIn("verified_data_commit_sha",fields)
        self.assertNotIn("published_commit_sha_or_candidate_tree_sha",fields)
        self.assertIn("never contains its own receipt commit/tree SHA",f["publication_protocol"]["phase_2"])
    def test_retries_count_toward_hard_budget(self):
        f=self.c["fixes"]["AUD-20260928-02-F13"]
        text=" ".join(f["rules"])
        self.assertIn("initial attempts and all retries combined",text)
        self.assertIn("25% hard ceiling",text)
    def test_old_parent_retry_cannot_rollback_current_pointer(self):
        f=self.c["fixes"]["AUD-20260928-02-F14"]["pointer_contract"]
        self.assertEqual(f["current_parent_pointer"]["primary_order"],"parent_v2_collector_generated_at_utc")
        self.assertIn("older parent",f["current_parent_pointer"]["rule"].lower())
        self.assertTrue(f["attempt_event_pointer"]["path"].endswith("/latest_attempt.json"))
    def test_receipt_drain_preserves_all_attempt_events(self):
        f=self.c["fixes"]["AUD-20260928-02-F15"]["promotion_contract"]
        self.assertIn("Enumerate immutable successful v3 transfer receipts",f["inventory"])
        self.assertIn("drains all pending receipts",f["recovery_rule"])
    def test_parent_source_chronology_is_lossless(self):
        f=self.c["fixes"]["AUD-20260928-02-F16"]
        exact=set(f["parent_source_chronology_contract"]["exact_parent_fields"])
        self.assertTrue({"availability_status","availability_observed_at_utc","field_available_at_utc","source_provenance"}.issubset(exact))
        self.assertIn("parent_source_chronology_sha256"," ".join(f["replacement_precondition"]))
    def test_attempt_identity_uses_nonce(self):
        f=self.c["fixes"]["AUD-20260928-02-F07"]["superseded_attempt_identity_detail"]
        self.assertEqual(f["method_version"],"dev03-v3-attempt-id-v2")
        self.assertIn("attempt_nonce",f["canonical_inputs"])
    def test_validation_still_blocks_wp03(self):
        self.assertEqual(self.c["validation_requirements"]["wp03_start_condition"],
                         "VAL-20260928-02 complete with zero open P0/P1/P2")
        self.assertEqual(self.c["audit_summary"]["finding_count"],16)

if __name__=="__main__": unittest.main()
