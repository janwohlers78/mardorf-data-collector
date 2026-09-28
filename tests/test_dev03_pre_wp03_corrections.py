import json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
import validate_dev03_pre_wp03_corrections as gate

class PreWp03CorrectionTests(unittest.TestCase):
    def setUp(self):
        self.c=json.loads((ROOT/"config/dev03_pre_wp03_l1_l2_corrections_v1.json").read_text())
    def test_repository_gate(self):
        r=gate.validate_repository(ROOT)
        self.assertEqual(r["status"],"PASS");self.assertEqual(r["fix_count"],10)
    def test_parent_receipt_and_cycle_are_exact(self):
        f=self.c["fixes"]
        self.assertIn("parent_v2_transfer_receipt_sha256",f["AUD-20260928-02-F01"]["parent_v2_binding_required_fields"])
        self.assertIn("forecast_lead_seconds",f["AUD-20260928-02-F02"]["binding_dimensions"])
    def test_replacement_preserves_native_variants(self):
        f3=self.c["fixes"]["AUD-20260928-02-F03"]
        self.assertIn("step_range_native",f3["inventory_contract"]["inventory_entry_fields"])
        self.assertIn("parent_field_inventory_sha256"," ".join(self.c["archive_v6_replacement_v2"]["required_before_deterministic_replacement"]))
    def test_v6_publication_is_receipt_gated(self):
        f4=self.c["fixes"]["AUD-20260928-02-F04"]
        self.assertEqual(f4["immutable_root"],"data/weather_archive_v6/")
        self.assertIn("valid publication receipt",f4["reader_rule"])
    def test_availability_precedence_and_enum(self):
        f5=self.c["fixes"]["AUD-20260928-02-F05"]
        self.assertIn("unknown_or_ambiguous",f5["status_family"])
        self.assertIn("unsupported_by_provider_or_product"," ".join(f5["mapping_precedence"]))
    def test_ensemble_identity_and_cross_archive_atomicity(self):
        f6=self.c["fixes"]["AUD-20260928-02-F06"]
        self.assertTrue({"step_range_native","start_step_native","end_step_native","step_units_native"}.issubset(f6["field_identity_dimensions"]))
        f8=self.c["fixes"]["AUD-20260928-02-F08"]
        self.assertIn("ensemble_source_revision_set_id",f8["set_key"])
        self.assertIn("no v5/v6 rows are composed"," ".join(f8["v6_over_v5_precondition"]))
    def test_retry_and_time_chronology_are_separate(self):
        f7=self.c["fixes"]["AUD-20260928-02-F07"]
        self.assertIn("v3_attempt_id",f7["fields"])
        f10=self.c["fixes"]["AUD-20260928-02-F10"]
        self.assertEqual(set(f10["required_bundle_times"]),{"parent_v2_collector_generated_at_utc","v3_generated_at_utc"})
    def test_shared_resource_limits(self):
        p=self.c["fixes"]["AUD-20260928-02-F09"]["public_enforcement_subset"]
        self.assertEqual(p["hard_incremental_provider_requests_per_day"],500)
        self.assertEqual(p["full_gefs_hard_requests"],800)
        self.assertEqual(p["public_workflow_timeout_minutes"],65)
        self.assertFalse(p["timeout_increase_allowed"])
if __name__=="__main__": unittest.main()
