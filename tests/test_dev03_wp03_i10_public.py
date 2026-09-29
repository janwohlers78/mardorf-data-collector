import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))

import run_dev03_wp03_i10_public as live
import validate_dev03_wp03_i10_public as gate
from dev03_v3_parent_binding import current_parent_pointer_allows, attempt_event_pointer_allows


class I10PublicTests(unittest.TestCase):
    def test_wiring_gate(self):
        self.assertEqual(gate.validate(ROOT)["status"],"PASS")

    def test_disabled_controls_are_noop_before_private_read(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/"controls.json"
            p.write_text(json.dumps({"controls":{
                "public_v3_generation_enabled":False,
                "public_v3_transfer_enabled":False,
                "private_v3_promotion_enabled":False,
                "archive_v6_write_enabled":False,
            }}))
            with patch.object(live,"CONTROL_PATH",p):
                with patch.object(live,"load_verified_private_parent") as parent:
                    result=live.run_live("token")
            self.assertEqual(result["status"],"disabled_noop")
            self.assertEqual(result["provider_requests"],0)
            parent.assert_not_called()

    def test_safe_public_rollback_stage_is_clean_noop(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/"controls.json"
            p.write_text(json.dumps({"controls":{
                "public_v3_generation_enabled":True,
                "public_v3_transfer_enabled":False,
                "private_v3_promotion_enabled":False,
                "archive_v6_write_enabled":False,
            }}))
            with patch.object(live,"CONTROL_PATH",p):
                with patch.object(live,"load_verified_private_parent") as parent:
                    result=live.run_live("token")
            self.assertEqual(result["status"],"disabled_noop")
            self.assertEqual(result["provider_requests"],0)
            parent.assert_not_called()

    def test_transfer_without_generation_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/"controls.json"
            p.write_text(json.dumps({"controls":{
                "public_v3_generation_enabled":False,
                "public_v3_transfer_enabled":True,
                "private_v3_promotion_enabled":False,
                "archive_v6_write_enabled":False,
            }}))
            with patch.object(live,"CONTROL_PATH",p):
                with self.assertRaisesRegex(RuntimeError,"cannot be enabled"):
                    live.run_live("token")

    def test_matched_baseline_urls_are_exact_parent_urls_and_deduplicated(self):
        parent={"models":{
            "ICON-D2":[{"source_urls":["https://example/a","https://example/a"]}],
            "ICON-EU":[{"source_urls":["https://example/b"]}],
            "GFS":[{"source_urls":["https://example/c"],
                    "grib_evidence":[{"url":"https://example/c"},{"url":"https://example/d"}]}],
            "ECMWF-IFS":[{"source_urls":["https://example/forbidden"]}],
        }}
        urls=live._baseline_urls(parent)
        self.assertEqual(set(urls),{"https://example/a","https://example/b","https://example/c","https://example/d"})
        self.assertNotIn("https://example/forbidden",urls)

    def test_required_provider_acceptance_rejects_all_failed_path(self):
        jobs=[
            {"model":"ICON-D2"},{"model":"ICON-D2"},{"model":"ICON-EU"},{"model":"GFS"}
        ]
        bundle={"provider_evidence":[
            {"model":"ICON-D2","availability":{"availability_status":"fetch_error"}},
            {"model":"ICON-D2","availability":{"availability_status":"fetch_error"}},
            {"model":"ICON-EU","availability":{"availability_status":"received"}},
            {"model":"GFS","availability":{"availability_status":"received"}},
        ]}
        with self.assertRaisesRegex(RuntimeError,"ICON-D2"):
            live._required_provider_live_acceptance(bundle,jobs)

    def test_budget_reservation_validation_is_fail_closed(self):
        plan={"resource_budget":{
            "deterministic_network_hard_incremental_ratio_max":0.25,
            "daily_incremental_request_hard_max":500,
        }}
        row={
            "method_version":"dev03-v3-daily-budget-reservation-v1",
            "utc_day":"2026-09-29",
            "v3_attempt_id":"a"*64,
            "planned_requests":83,
            "matched_v2_response_bytes":1000,
            "reserved_response_bytes":250,
        }
        self.assertEqual(
            live._validate_budget_reservation(row,day="2026-09-29",plan=plan),
            row,
        )
        bad=dict(row);bad["reserved_response_bytes"]=249
        with self.assertRaisesRegex(RuntimeError,"formula drift"):
            live._validate_budget_reservation(bad,day="2026-09-29",plan=plan)

    def test_large_private_content_uses_git_blob_fallback(self):
        content=base64.b64encode(b"large-bytes").decode()
        first=MagicMock(status_code=200)
        first.json.return_value={"encoding":"none","sha":"abc123"}
        second=MagicMock(status_code=200)
        second.json.return_value={"encoding":"base64","content":content}
        with patch.object(live.requests,"get",side_effect=[first,second]):
            self.assertEqual(
                live._content_bytes("owner/repo","large.bin","token"),
                b"large-bytes",
            )

    def test_runtime_gate_excludes_matched_baseline_measurement(self):
        baseline_wall=86.922
        total_wall=153.87
        successor_delta=total_wall-baseline_wall
        hard_max=max(120.0,0.35*baseline_wall)
        self.assertAlmostEqual(successor_delta,66.948,places=3)
        self.assertEqual(hard_max,120.0)
        self.assertLessEqual(successor_delta,hard_max)

    def test_pointer_ordering_keeps_newer_parent_and_attempt(self):
        current={
            "parent_v2_collector_generated_at_utc":"2026-09-28T18:00:00Z",
            "v3_generated_at_utc":"2026-09-28T18:10:00Z",
            "v3_attempt_id":"f"*64,
        }
        older={
            "parent_v2_collector_generated_at_utc":"2026-09-28T15:00:00Z",
            "v3_generated_at_utc":"2026-09-28T19:00:00Z",
            "v3_attempt_id":"e"*64,
        }
        self.assertFalse(current_parent_pointer_allows(current,older))
        self.assertTrue(attempt_event_pointer_allows(current,older))


if __name__=="__main__":
    unittest.main()
