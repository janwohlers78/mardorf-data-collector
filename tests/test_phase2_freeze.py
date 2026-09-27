import json
import unittest
from pathlib import Path

import full_horizon_contract as horizon
import gefs_full_members as full_gefs
import relevant_meteorology_registry as registry

ROOT=Path(__file__).resolve().parents[1]


class Phase2FrozenContractTests(unittest.TestCase):
    def setUp(self):
        self.freeze=json.loads((ROOT/"config/phase2_frozen_contract_v1.json").read_text())
        self.plan=json.loads((ROOT/"config/weather_acquisition_plan.json").read_text())
        self.integrity=json.loads((ROOT/"config/integrity_policy.json").read_text())

    def test_phase2_contract_versions_are_frozen(self):
        c=self.freeze
        self.assertEqual(c["status"],"complete_frozen")
        self.assertEqual(c["contract_version"],"phase2-acquisition-storage-freeze-v1")
        self.assertEqual(horizon.ACQUISITION_GRID_VERSION,c["contracts"]["acquisition"]["acquisition_grid_version"])
        self.assertEqual(horizon.RETENTION_GRID_VERSION,c["contracts"]["acquisition"]["retention_grid_version"])
        self.assertEqual(full_gefs.METHOD_VERSION,c["contracts"]["full_gefs"]["method_version"])
        self.assertEqual(full_gefs.POLICY_VERSION,c["contracts"]["full_gefs"]["policy_version"])
        self.assertEqual(registry.REGISTRY["registry_version"],c["contracts"]["registry"]["registry_version"])
        self.assertEqual(registry.HASHES,{
            "semantic_contract_sha256":c["contracts"]["registry"]["semantic_contract_sha256"],
            "provider_matrix_sha256":c["contracts"]["registry"]["provider_matrix_sha256"],
            "registry_sha256":c["contracts"]["registry"]["registry_sha256"],
        })

    def test_live_acquisition_plan_has_no_pre_activation_gate_left(self):
        self.assertEqual(self.plan["status"],"phase2_complete_frozen")
        self.assertEqual(self.plan["phase2_freeze"]["state"],"complete_frozen")
        boundary=self.plan["acquisition_grid_v2"]["phase_boundary"]
        self.assertIn("complete_frozen",boundary)
        self.assertIn("gefs-sparse-00z-policy-v1",boundary)
        self.assertNotIn("gated until after 2E-3",boundary)

    def test_integrity_policy_points_to_frozen_contract(self):
        link=self.integrity["phase2_freeze_contract"]
        self.assertEqual(link["contract_version"],self.freeze["contract_version"])
        self.assertEqual(link["status"],"complete_frozen")


if __name__=="__main__":
    unittest.main()
