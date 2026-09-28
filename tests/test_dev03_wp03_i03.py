import hashlib
import json
import unittest

from src.dev03_v3_parent_binding import (
    ATTEMPT_METHOD,
    Dev03ParentBindingError,
    build_bundle_v3_shell,
    build_parent_cycle_inventory,
    canonical_json_bytes,
    collection_transaction_id,
    current_parent_pointer_allows,
    isolated_v3_paths,
    load_verified_parent_payload,
    v3_attempt_id,
    verify_parent_transfer_receipt,
)
from src.dev03_v3_transfer_shell import build_transfer_plan


def h(ch):
    return ch * 64


class Dev03Wp03I03Tests(unittest.TestCase):
    def parent_result_and_receipt(self):
        receipt = {
            "schema_version": 2,
            "method_version": "private-transfer-readback-v2",
            "kind": "models",
            "source_generated_at_utc": "2026-09-28T09:54:14.052852+00:00",
            "verified_data_commit_sha": h("a"),
            "readback_verified": True,
            "payload_source_sha256": h("b"),
            "audit_input_payload_sha256": h("b"),
        }
        raw = (json.dumps(receipt, indent=2) + "\n").encode()
        result = {
            "transfer_receipt_path": "data/inbox/public_collector/transfer_receipts/models/2026/09/28/receipt_x.json",
            "transfer_receipt_sha256": hashlib.sha256(raw).hexdigest(),
            "verified_data_commit_sha": h("a"),
            "payload_source_sha256": h("b"),
            "source_generated_at_utc": "2026-09-28T09:54:14.052852+00:00",
        }
        return result, raw

    def test_exact_parent_receipt_binds_transaction(self):
        result, raw = self.parent_result_and_receipt()
        binding = verify_parent_transfer_receipt(raw, result)
        self.assertEqual(binding["parent_v2_payload_sha256"], h("b"))
        self.assertEqual(binding["collection_transaction_id"], collection_transaction_id(binding))
        mutated = bytearray(raw)
        mutated[-2] = 32
        with self.assertRaises(Dev03ParentBindingError):
            verify_parent_transfer_receipt(bytes(mutated), result)

    def test_parent_payload_bytes_are_exactly_bound_to_verified_sha(self):
        payload = {"models":{"ICON-D2":[]}}
        raw = (json.dumps(payload, separators=(",",":")) + "\n").encode()
        binding = {"parent_v2_payload_sha256": hashlib.sha256(raw).hexdigest()}
        self.assertEqual(load_verified_parent_payload(raw,binding), payload)
        with self.assertRaises(Dev03ParentBindingError):
            load_verified_parent_payload(raw+b" ",binding)

    def test_parent_receipt_path_must_be_immutable_dated_path(self):
        result, raw = self.parent_result_and_receipt()
        result["transfer_receipt_path"] = "data/inbox/public_collector/transfer_receipts/models/latest.json"
        with self.assertRaises(Dev03ParentBindingError):
            verify_parent_transfer_receipt(raw,result)

    def test_parent_cycle_binding_is_order_independent_and_strict(self):
        registry = {
            "providers": {
                "ICON-D2": {
                    "convective_precipitation_native_identity": {
                        "provider_product": "DWD ICON-D2 Open Data raw GRIB2"
                    }
                }
            }
        }
        row0 = {
            "model": "ICON-D2",
            "run_time_utc": "2026-09-28T09:00:00+00:00",
            "valid_time_utc": "2026-09-28T09:00:00+00:00",
            "forecast_lead_hours": 0,
            "provider_product": "DWD ICON-D2 Open Data raw GRIB2",
            "forecast_coordinate_or_grid_point": {
                "latitude": 52.5,
                "longitude": 9.3,
                "selection": "ecCodes_nearest_grid_point",
            },
        }
        row3 = dict(row0)
        row3.update(
            valid_time_utc="2026-09-28T12:00:00+00:00",
            forecast_lead_hours=3,
        )
        inv1, bind1 = build_parent_cycle_inventory({"models": {"ICON-D2": [row0, row3]}}, registry)
        inv2, bind2 = build_parent_cycle_inventory({"models": {"ICON-D2": [row3, row0]}}, registry)
        self.assertEqual(inv1, inv2)
        self.assertEqual(bind1, bind2)
        bad = dict(row3)
        bad["forecast_lead_hours"] = 2
        with self.assertRaises(Dev03ParentBindingError):
            build_parent_cycle_inventory({"models": {"ICON-D2": [bad]}}, registry)

    def test_missing_provider_product_never_inferred_from_registry(self):
        row = {
            "model":"ICON-D2","run_time_utc":"2026-09-28T09:00:00+00:00",
            "valid_time_utc":"2026-09-28T09:00:00+00:00","forecast_lead_hours":0,
            "forecast_coordinate_or_grid_point":{"latitude":52.5,"longitude":9.3},
        }
        registry={"providers":{"ICON-D2":{"convective_precipitation_native_identity":{"provider_product":"guessed"}}}}
        with self.assertRaises(Dev03ParentBindingError):
            build_parent_cycle_inventory({"models":{"ICON-D2":[row]}},registry)

    def test_duplicate_parent_occurrence_fails_closed(self):
        row = {
            "model":"ICON-D2","run_time_utc":"2026-09-28T09:00:00+00:00",
            "valid_time_utc":"2026-09-28T09:00:00+00:00","forecast_lead_hours":0,
            "provider_product":"DWD ICON-D2 Open Data raw GRIB2",
            "forecast_coordinate_or_grid_point":{"latitude":52.5,"longitude":9.3},
        }
        with self.assertRaises(Dev03ParentBindingError):
            build_parent_cycle_inventory({"models":{"ICON-D2":[row,row]}},{"providers":{}})

    def test_ambiguous_provider_product_fails_closed(self):
        row = {
            "model": "X",
            "run_time_utc": "2026-09-28T09:00:00+00:00",
            "valid_time_utc": "2026-09-28T09:00:00+00:00",
            "forecast_lead_hours": 0,
            "forecast_coordinate_or_grid_point": {"latitude": 52.5, "longitude": 9.3},
        }
        registry = {"providers": {"X": {"products": ["a", "b"]}}}
        with self.assertRaises(Dev03ParentBindingError):
            build_parent_cycle_inventory({"models": {"X": [row]}}, registry)

    def test_parent_receipt_rejects_duplicate_json_keys(self):
        result, _ = self.parent_result_and_receipt()
        raw = b'{"method_version":"private-transfer-readback-v2","method_version":"private-transfer-readback-v2","kind":"models","readback_verified":true}'
        result["transfer_receipt_sha256"] = hashlib.sha256(raw).hexdigest()
        with self.assertRaises(Dev03ParentBindingError):
            verify_parent_transfer_receipt(raw,result)

    def test_attempt_nonce_prevents_same_timestamp_collision(self):
        tx = h("c")
        ts = "2026-09-28T12:00:00+00:00"
        a = v3_attempt_id(tx, ts, "11111111-1111-4111-8111-111111111111")
        b = v3_attempt_id(tx, ts, "22222222-2222-4222-8222-222222222222")
        self.assertNotEqual(a, b)
        expected = hashlib.sha256(
            ATTEMPT_METHOD.encode()
            + canonical_json_bytes(
                {
                    "collection_transaction_id": tx,
                    "v3_generated_at_utc": "2026-09-28T12:00:00Z",
                    "attempt_nonce": "11111111-1111-4111-8111-111111111111",
                }
            )
        ).hexdigest()
        self.assertEqual(a, expected)

    def test_bundle_rejects_forged_collection_transaction(self):
        result, raw = self.parent_result_and_receipt()
        binding = verify_parent_transfer_receipt(raw, result)
        binding["collection_transaction_id"] = h("f")
        with self.assertRaises(Dev03ParentBindingError):
            build_bundle_v3_shell(
                parent_binding=binding,parent_cycle_binding_id=h("e"),
                v3_generated_at_utc="2026-09-28T12:00:00+00:00",
                attempt_nonce="11111111-1111-4111-8111-111111111111")

    def test_old_parent_retry_cannot_roll_current_pointer_back(self):
        newer = {
            "parent_v2_collector_generated_at_utc": "2026-09-28T12:00:00+00:00",
            "v3_generated_at_utc": "2026-09-28T12:01:00+00:00",
            "v3_attempt_id": h("d"),
        }
        old_retry = {
            "parent_v2_collector_generated_at_utc": "2026-09-28T09:00:00+00:00",
            "v3_generated_at_utc": "2026-09-28T13:00:00+00:00",
            "v3_attempt_id": h("e"),
        }
        self.assertFalse(current_parent_pointer_allows(newer, old_retry))

    def test_shell_and_transfer_paths_are_isolated(self):
        result, raw = self.parent_result_and_receipt()
        binding = verify_parent_transfer_receipt(raw, result)
        shell = build_bundle_v3_shell(
            parent_binding=binding,
            parent_cycle_binding_id=h("f"),
            v3_generated_at_utc="2026-09-28T12:00:00+00:00",
            attempt_nonce="11111111-1111-4111-8111-111111111111",
        )
        payload = canonical_json_bytes(shell) + b"\n"
        plan = build_transfer_plan(shell, payload)
        with self.assertRaises(ValueError):
            build_transfer_plan(shell, payload + b"x")
        self.assertEqual(shell["network_requests_performed"], 0)
        self.assertEqual(plan["network_requests_performed_by_plan"], 0)
        self.assertFalse(plan["legacy_namespace_writes_allowed"])
        for path in plan["paths"].values():
            self.assertTrue(path.startswith("data/inbox/public_collector_v3/"))
        paths = isolated_v3_paths(shell["v3_generated_at_utc"], shell["v3_attempt_id"])
        self.assertIn(shell["v3_attempt_id"], paths["receipt"])


if __name__ == "__main__":
    unittest.main()
