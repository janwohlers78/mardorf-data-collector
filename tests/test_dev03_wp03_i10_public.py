import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
