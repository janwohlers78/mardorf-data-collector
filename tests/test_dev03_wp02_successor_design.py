import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import validate_dev03_wp02_successor_design as wp02


class Dev03Wp02SuccessorDesignTests(unittest.TestCase):
    def setUp(self):
        self.contract = json.loads(
            (ROOT / "config/dev03_wp02_l1_l2_successor_design_v1.json").read_text(encoding="utf-8")
        )

    def test_repository_gate(self):
        report = wp02.validate_repository(ROOT)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["successor_count"], 11)
        self.assertEqual(report["logical_view_count"], 6)
        self.assertFalse(report["operational_l4_l5_changed"])

    def test_f02_separates_model_predictor_from_event_truth(self):
        f02 = self.contract["f02_registry_v3"]
        self.assertEqual(f02["new_semantic"]["semantic_id"], "convective_precipitation")
        self.assertFalse(f02["new_semantic"]["derived_event_truth"])
        self.assertEqual(f02["provider_matrix"]["ICON-D2"]["parameter_native"], "rain_con")
        self.assertEqual(f02["provider_matrix"]["GFS"]["parameter_native"], "ACPCP")
        self.assertEqual(
            f02["provider_matrix"]["ICON-D2-EPS"]["acquisition_policy_v2"],
            "not_requested_by_policy",
        )

    def test_f03_has_one_lossless_interval_authority(self):
        f03 = self.contract["f03_interval_resolution_v1"]
        self.assertEqual(f03["authority_field"], "interval_seconds")
        self.assertEqual(f03["authority_type"], "int64")
        self.assertTrue({"10", "11", "12"}.issubset(f03["step_unit_contract"]["numeric_codes_supported"]))
        self.assertIn("unknown", f03["statuses"])
        self.assertIn("ambiguous", f03["statuses"])
        self.assertIn("invalid", f03["statuses"])

    def test_f04_requires_complete_native_identity(self):
        f04 = self.contract["f04_l2_l3_read_contract_v1"]
        fields = set(f04["required_field_identity"])
        for key in (
            "parameter_native", "unit_native", "type_of_level_native", "level_native",
            "step_type_native", "step_range_native", "start_step_native", "end_step_native",
            "step_units_native", "interval_seconds", "identity_completeness_status",
        ):
            self.assertIn(key, fields)
        self.assertEqual(len(f04["required_logical_views"]), 6)

    def test_f05_rejects_truncation_and_preserves_operational_path(self):
        f05 = self.contract["f05_forecast_lead_identity_v1"]
        self.assertEqual(f05["l2_authority"], {
            "field": "lead_seconds",
            "type": "int64",
            "source": "validated exact timestamp delta and matching successor payload identity",
        })
        self.assertIn("Never use int() truncation", f05["legacy_adapter_rule"])
        migration = self.contract["migration_topology"]
        self.assertTrue(migration["persistence"]["no_history_rewrite"])
        self.assertTrue(migration["persistence"]["no_first_seen_backdating"])
        self.assertEqual(migration["operational_boundary"]["l4_authority"], "v16-c3-v9")
        self.assertFalse(migration["operational_boundary"]["dev03_cutover_allowed"])


if __name__ == "__main__":
    unittest.main()
