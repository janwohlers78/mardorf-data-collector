import json
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
import validate_dev03_prep05_migration_topology as prep05


class Dev03Prep05TopologyTests(unittest.TestCase):
    def setUp(self):
        self.c=json.loads((ROOT/"config/dev03_prep05_migration_topology_v1.json").read_text(encoding="utf-8"))

    def test_repository_gate(self):
        r=prep05.validate_repository(ROOT)
        self.assertEqual(r["status"],"PASS")
        self.assertEqual(r["v3_inbox_root"],"data/inbox/public_collector_v3")
        self.assertEqual(r["archive_v6_root"],"data/weather_archive_v6")
        self.assertFalse(r["cutover_allowed"])

    def test_v3_cannot_trigger_legacy_promotion(self):
        legacy=self.c["existing_operational_path"]["integrity_latest_success"]
        v3=self.c["v3_transfer_namespace"]["integrity_latest_success"]
        self.assertNotEqual(legacy,v3)
        self.assertTrue(v3.startswith("data/inbox/public_collector_v3/"))
        self.assertEqual(
            self.c["private_v3_shadow_promotion"]["trigger_path"],
            v3,
        )

    def test_v5_v6_link_is_schema_independent(self):
        link=self.c["cross_archive_link"]
        self.assertEqual(link["output_field"],"migration_source_occurrence_id")
        self.assertIn("logical_record_id",link["key_formula"])
        self.assertIn("parent_or_v5_payload_sha256",link["key_formula"])
        self.assertNotIn("content_revision_id",link["key_formula"])
        self.assertNotIn("revision_event_id",link["key_formula"])

    def test_dual_read_is_single_asof_and_no_field_mixing(self):
        read=self.c["l2_l3_dual_read"]
        self.assertIn("exact same as_of=T",read["one_as_of_rule"])
        text=" ".join(read["selection_algorithm"])
        self.assertIn("suppress the linked legacy occurrence",text)
        self.assertIn("Never compose v5 and v6 fields",text)
        self.assertIn("every Registry-v3 semantic",read["v6_replacement_precondition"])

    def test_rollback_and_cutover_are_isolated(self):
        rollback=self.c["rollback"]
        self.assertIn("delete or rewrite v1-v5 history",rollback["prohibited"])
        self.assertEqual(rollback["adapter_emergency_mode"],"legacy_only_degraded")
        cut=self.c["cutover_boundary"]
        self.assertFalse(cut["dev03_operational_cutover_allowed"])
        self.assertTrue(cut["no_pointer_switch_cutover"])
        self.assertIn("v16/C3 decision authority",cut["forbidden_targets"])


if __name__=="__main__":
    unittest.main()
