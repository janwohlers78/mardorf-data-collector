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


def git_id(ch, length=40):
    return ch * length


class Dev03Wp03I03Tests(unittest.TestCase):
    def parent_result_and_receipt(self):
        receipt = {
            "schema_version": 2,
            "method_version": "private-transfer-readback-v2",
            "kind": "models",
            "source_generated_at_utc": "2026-09-28T09:54:14.052852+00:00",
            "verified_data_commit_sha": git_id("a"),
            "readback_verified": True,
            "payload_source_sha256": h("b"),
            "audit_input_payload_sha256": h("b"),
        }
        raw = (json.dumps(receipt, indent=2) + "\n").encode()
        result = {
            "transfer_receipt_path": "data/inbox/public_collector/transfer_receipts/models/2026/09/28/receipt_x.json",
            "transfer_receipt_sha256": hashlib.sha256(raw).hexdigest(),
            "verified_data_commit_sha": git_id("a"),
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

    def test_verified_data_commit_accepts_git_sha1_and_sha256_object_ids(self):
        result, raw = self.parent_result_and_receipt()
        binding = verify_parent_transfer_receipt(raw, result)
        self.assertEqual(binding["parent_v2_verified_data_commit_sha"], git_id("a",40))

        receipt = json.loads(raw)
        receipt["verified_data_commit_sha"] = git_id("c",64)
        raw64 = (json.dumps(receipt, indent=2) + "\n").encode()
        result64 = dict(result)
        result64["verified_data_commit_sha"] = git_id("c",64)
        result64["transfer_receipt_sha256"] = hashlib.sha256(raw64).hexdigest()
        binding64 = verify_parent_transfer_receipt(raw64, result64)
        self.assertEqual(binding64["parent_v2_verified_data_commit_sha"], git_id("c",64))

    def test_verified_data_commit_rejects_non_git_object_id_lengths_and_nonhex(self):
        for bad in ("a"*39, "a"*41, "g"*40):
            result, raw = self.parent_result_and_receipt()
            receipt = json.loads(raw)
            receipt["verified_data_commit_sha"] = bad
            mutated = (json.dumps(receipt, indent=2) + "\n").encode()
            result["verified_data_commit_sha"] = bad
            result["transfer_receipt_sha256"] = hashlib.sha256(mutated).hexdigest()
            with self.assertRaises(Dev03ParentBindingError):
                verify_parent_transfer_receipt(mutated, result)

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

    def test_missing_dwd_provider_product_can_be_proven_only_from_exact_parent_source_urls(self):
        row = {
            "model":"ICON-D2",
            "run_time_utc":"2026-09-28T09:00:00+00:00",
            "valid_time_utc":"2026-09-28T12:00:00+00:00",
            "forecast_lead_hours":3,
            "source_urls":[
                "https://opendata.dwd.de/weather/nwp/icon-d2/grib/09/u_10m/icon-d2_germany_regular-lat-lon_single-level_2026092809_003_2d_u_10m.grib2.bz2",
                "https://opendata.dwd.de/weather/nwp/icon-d2/grib/09/v_10m/icon-d2_germany_regular-lat-lon_single-level_2026092809_003_2d_v_10m.grib2.bz2",
            ],
            "forecast_coordinate_or_grid_point":{"latitude":52.5,"longitude":9.3},
        }
        inventory, _ = build_parent_cycle_inventory(
            {"models":{"ICON-D2":[row]}},
            {"providers":{"ICON-D2":{"convective_precipitation_native_identity":{"provider_product":"must-not-be-used"}}}},
        )
        self.assertEqual(inventory[0]["provider_product"], "icon-d2_regular-lat-lon")
        bad = json.loads(json.dumps(row))
        bad["source_urls"][0] = bad["source_urls"][0].replace("2026092809_003", "2026092812_003")
        with self.assertRaises(Dev03ParentBindingError):
            build_parent_cycle_inventory({"models":{"ICON-D2":[bad]}},{"providers":{}})

    def test_missing_eps_provider_product_requires_exact_parent_source_evidence(self):
        row = {
            "model":"ICON-D2-EPS",
            "run_time_utc":"2026-09-28T21:00:00+00:00",
            "valid_time_utc":"2026-09-29T00:00:00+00:00",
            "forecast_lead_hours":3,
            "source_url":"https://ensemble-api.open-meteo.com/v1/ensemble?latitude=52.4942&longitude=9.3418&models=dwd_icon_d2_eps",
            "source_run_identity":{
                "verification_status":"verified_stable_metadata_dwd_cycle_and_spatial_provenance",
                "model_id":"dwd_icon_d2_eps",
                "run_time_utc":"2026-09-28T21:00:00+00:00",
                "dwd_cycle_confirmation_url":"https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/21/u_10m/icon-d2-eps_germany_icosahedral_single-level_2026092821_048_2d_u_10m.grib2.bz2",
                "response_run_binding":{"response_sha256":h("d")},
            },
            "authoritative_member_source_response_sha256":h("d"),
            "forecast_coordinate_or_grid_point":{"latitude":52.5,"longitude":9.34},
        }
        inventory, _ = build_parent_cycle_inventory({"models":{"ICON-D2-EPS":[row]}},{"providers":{}})
        self.assertEqual(inventory[0]["provider_product"],"open_meteo:dwd_icon_d2_eps")

        for mutate in ("model_query","response_sha","run","dwd_cycle"):
            bad=json.loads(json.dumps(row))
            if mutate=="model_query":
                bad["source_url"]=bad["source_url"].replace("dwd_icon_d2_eps","dwd_icon_eu")
            elif mutate=="response_sha":
                bad["authoritative_member_source_response_sha256"]=h("e")
            elif mutate=="run":
                bad["source_run_identity"]["run_time_utc"]="2026-09-28T18:00:00+00:00"
            else:
                bad["source_run_identity"]["dwd_cycle_confirmation_url"]=bad["source_run_identity"]["dwd_cycle_confirmation_url"].replace("2026092821","2026092818")
            with self.assertRaises(Dev03ParentBindingError):
                build_parent_cycle_inventory({"models":{"ICON-D2-EPS":[bad]}},{"providers":{}})

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
