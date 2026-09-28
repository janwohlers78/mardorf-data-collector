import ast
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "config" / "phase2_h1_public_e2e_matrix_v1.json"


def test_functions(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


class Phase2H1PublicE2EMatrixTests(unittest.TestCase):
    def test_h1_public_matrix_covers_required_audit_findings(self):
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        self.assertEqual(matrix["matrix_version"], "phase2-h1-public-e2e-matrix-v1")
        expected = {
            "P1-AUDIT-001",
            "P1-AUDIT-003",
            "P1-AUDIT-004",
            "P1-AUDIT-005",
            "P2-AUDIT-007",
            "P2-AUDIT-008",
            "P1-AUDIT-016",
            "P2-AUDIT-010",
        }
        self.assertEqual(set(matrix["required_audit_ids"]), expected)
        self.assertEqual(set(matrix["regression_refs"]), expected)

    def test_every_h1_public_regression_reference_exists(self):
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        cache = {}
        for audit_id, refs in matrix["regression_refs"].items():
            self.assertGreater(len(refs), 0, audit_id)
            for ref in refs:
                rel, test_name = ref.split("::", 1)
                path = ROOT / rel
                self.assertTrue(path.exists(), f"{audit_id}: missing {rel}")
                funcs = cache.setdefault(rel, test_functions(path))
                self.assertIn(test_name, funcs, f"{audit_id}: missing {ref}")

    def test_h1_public_pipeline_invariants_are_closed(self):
        matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
        invariants = matrix["pipeline_invariants"]
        self.assertTrue(invariants["non_native_missingness_explicit"])
        self.assertTrue(invariants["native_identity_metadata_preserved"])
        self.assertTrue(invariants["provider_interval_metadata_preserved"])
        self.assertTrue(invariants["same_cycle_noop_suppresses_transfer"])
        self.assertTrue(invariants["supplemental_retry_is_bounded"])
        self.assertTrue(invariants["transfer_parent_bound_and_monotonic"])
        self.assertTrue(invariants["cross_repo_successor_contract_gate_required"])


if __name__ == "__main__":
    unittest.main()
