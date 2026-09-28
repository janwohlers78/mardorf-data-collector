import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from forecast_lead_identity import (
    ForecastLeadIdentityError,
    INT64_MAX,
    lead_seconds_from_hours,
    validate_forecast_lead_identity,
    validate_lead_seconds,
)
import validate_dev03_wp03_i01 as gate


class Dev03Wp03I01Tests(unittest.TestCase):
    def setUp(self):
        self.shadow = json.loads((ROOT / "config/dev03_shadow_channels_v1.json").read_text())

    def test_repository_gate(self):
        result = gate.validate_repository(ROOT)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["implementation_step"], "WP03-I01")
        self.assertEqual(result["shadow_controls_disabled"], 4)
        self.assertFalse(result["network_requests_allowed"])

    def test_exact_integral_hour_identity(self):
        self.assertEqual(lead_seconds_from_hours(3), 10800)
        result = validate_forecast_lead_identity(
            run_time_utc="2026-09-28T00:00:00Z",
            valid_time_utc="2026-09-28T03:00:00+00:00",
            lead_seconds=10800,
            acquisition_lead_hours=3,
        )
        self.assertEqual(result["lead_seconds"], 10800)

    def test_lossy_or_coerced_leads_fail_closed(self):
        for bad in (1.5, True, "3600", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ForecastLeadIdentityError):
                    validate_lead_seconds(bad)

    def test_fractional_acquisition_hours_fail_closed(self):
        for bad in (0.5, 1.25, "2", True):
            with self.subTest(bad=bad):
                with self.assertRaises(ForecastLeadIdentityError):
                    lead_seconds_from_hours(bad)

    def test_signed_int64_bounds_fail_closed(self):
        self.assertEqual(validate_lead_seconds(INT64_MAX), INT64_MAX)
        with self.assertRaises(ForecastLeadIdentityError):
            validate_lead_seconds(INT64_MAX + 1)
        with self.assertRaises(ForecastLeadIdentityError):
            lead_seconds_from_hours(INT64_MAX // 3600 + 1)

    def test_timestamp_contradiction_fails_closed(self):
        with self.assertRaises(ForecastLeadIdentityError):
            validate_forecast_lead_identity(
                run_time_utc="2026-09-28T00:00:00Z",
                valid_time_utc="2026-09-28T02:00:00Z",
                lead_seconds=3600,
            )

    def test_declared_rfc3339_format_is_enforced(self):
        with self.assertRaises(ForecastLeadIdentityError):
            validate_forecast_lead_identity(
                run_time_utc="2026-09-28 00:00:00+00:00",
                valid_time_utc="2026-09-28T01:00:00Z",
                lead_seconds=3600,
            )

    def test_non_utc_and_fractional_duration_fail_closed(self):
        with self.assertRaises(ForecastLeadIdentityError):
            validate_forecast_lead_identity(
                run_time_utc="2026-09-28T00:00:00+02:00",
                valid_time_utc="2026-09-28T01:00:00+02:00",
                lead_seconds=3600,
            )
        with self.assertRaises(ForecastLeadIdentityError):
            validate_forecast_lead_identity(
                run_time_utc="2026-09-28T00:00:00.500Z",
                valid_time_utc="2026-09-28T01:00:00Z",
                lead_seconds=3600,
            )

    def test_all_four_shadow_controls_default_disabled(self):
        expected = {
            "public_v3_generation_enabled",
            "public_v3_transfer_enabled",
            "private_v3_promotion_enabled",
            "archive_v6_write_enabled",
        }
        self.assertEqual(set(self.shadow["controls"]), expected)
        self.assertTrue(all(v is False for v in self.shadow["controls"].values()))
        self.assertFalse(self.shadow["cutover_authority"]["can_authorize_operational_cutover"])

    def test_i01_cannot_move_network_boundary(self):
        n = self.shadow["network_boundary"]
        self.assertFalse(n["wp03_i01_network_requests_allowed"])
        self.assertEqual(n["first_network_step"], "WP03-I04")
        self.assertIn("WP03-I03 PASS", n["wp03_i04_precondition"])


if __name__ == "__main__":
    unittest.main()
