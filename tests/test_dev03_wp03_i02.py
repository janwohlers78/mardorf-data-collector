import json
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))

import availability_contract_v3 as a
import relevant_meteorology_registry_v3 as r
import validate_dev03_wp03_i02 as gate


class Dev03Wp03I02Tests(unittest.TestCase):
    def setUp(self):
        self.registry=json.loads((ROOT/"config/relevant_meteorology_registry_v3.json").read_text())
        self.observed="2026-09-28T12:00:00Z"

    def test_repository_gate(self):
        got=gate.validate_repository(ROOT)
        self.assertEqual(got["status"],"PASS")
        self.assertEqual(got["semantic_count"],16)
        self.assertEqual(got["source_path_count"],7)
        self.assertEqual(got["availability_state_count"],7)
        self.assertFalse(got["network_requests_allowed"])

    def test_v3_adds_exactly_convective_precipitation(self):
        v2=json.loads((ROOT/"config/relevant_meteorology_registry_v2.json").read_text())
        s2={x["semantic_id"] for x in v2["core_semantics"]}
        s3={x["semantic_id"] for x in self.registry["core_semantics"]}
        self.assertEqual(s3-s2,{"convective_precipitation"})
        self.assertNotIn("convective_precipitation_or_event_proxy",s3)

    def test_convective_provider_matrix(self):
        expected={
            "ICON-D2":("native_available","required"),
            "ICON-EU":("native_available","required"),
            "GFS":("native_available","required"),
            "ICON-D2-EPS":("native_available","not_requested_by_policy"),
            "ECMWF-IFS":("unsupported_by_provider_or_product","not_requested_by_policy"),
            "GEFS-control":("unsupported_by_provider_or_product","not_requested_by_policy"),
            "NOAA_GEFS_FULL":("unsupported_by_provider_or_product","not_requested_by_policy"),
        }
        for model,pair in expected.items():
            p=self.registry["providers"][model]
            self.assertEqual((p["capability"]["convective_precipitation"],p["acquisition_policy"]["convective_precipitation"]),pair)

    def test_required_without_attempt_evidence_is_not_received(self):
        d=a.make_declaration(model="GFS",semantic_id="convective_precipitation",observed_at_utc=self.observed)
        self.assertEqual(d["availability_status"],"unknown_or_ambiguous")
        self.assertIsNone(d["value"])
        self.assertTrue(d["availability_evidence_type"])
        self.assertTrue(d["availability_reason"])

    def test_unsupported_precedes_policy_disabled(self):
        d=a.make_declaration(model="ECMWF-IFS",semantic_id="convective_precipitation",observed_at_utc=self.observed)
        self.assertEqual(d["availability_status"],"unsupported_by_provider_or_product")

    def test_capable_but_disabled_is_not_requested(self):
        d=a.make_declaration(model="ICON-D2-EPS",semantic_id="convective_precipitation",observed_at_utc=self.observed)
        self.assertEqual(d["provider_capability"],"native_available")
        self.assertEqual(d["availability_status"],"not_requested_by_policy")

    def test_received_requires_explicit_attempt_evidence_value_and_time(self):
        with self.assertRaises(ValueError):
            a.make_declaration(
                model="GFS",semantic_id="convective_precipitation",observed_at_utc=self.observed,
                runtime_status="received",value=None,field_available_at_utc=self.observed,
                evidence_type="provider_fetch_attempt")
        d=a.make_declaration(
            model="GFS",semantic_id="convective_precipitation",observed_at_utc=self.observed,
            runtime_status="received",value=0.0,field_available_at_utc=self.observed,
            parameter_native="ACPCP",field_provider_product="gfs_0p25",
            evidence_type="provider_fetch_attempt")
        self.assertEqual(d["availability_status"],"received")
        self.assertEqual(d["value"],0.0)
        self.assertTrue(a.validate_declaration(d))

    def test_runtime_attempt_cannot_override_unsupported_or_policy_disabled(self):
        for model in ("ECMWF-IFS","ICON-D2-EPS"):
            with self.subTest(model=model):
                with self.assertRaises(ValueError):
                    a.make_declaration(
                        model=model,semantic_id="convective_precipitation",observed_at_utc=self.observed,
                        runtime_status="received",value=1.0,field_available_at_utc=self.observed)

    def test_only_received_may_carry_value(self):
        for status in ("not_yet_published","fetch_error","unknown_or_ambiguous"):
            with self.subTest(status=status):
                with self.assertRaises(ValueError):
                    a.make_declaration(
                        model="GFS",semantic_id="convective_precipitation",observed_at_utc=self.observed,
                        runtime_status=status,value=0.0,evidence_type="provider_fetch_attempt",reason="test")

    def test_gfs_uses_acpcp_not_cprat(self):
        p=self.registry["providers"]["GFS"]
        self.assertEqual(p["native_mapping"]["ACPCP"],"convective_precipitation")
        self.assertNotIn("CPRAT",[k for k,v in p["native_mapping"].items() if v=="convective_precipitation"])
        self.assertEqual(p["convective_precipitation_native_identity"]["explicitly_excluded_alias"],"CPRAT")

    def test_runtime_states_require_explicit_evidence_and_reason(self):
        with self.assertRaises(ValueError):
            a.make_declaration(
                model="GFS", semantic_id="convective_precipitation", observed_at_utc=self.observed,
                runtime_status="received", value=1.0, field_available_at_utc=self.observed)
        for status in ("not_yet_published","fetch_error","unknown_or_ambiguous"):
            with self.subTest(status=status):
                with self.assertRaises(ValueError):
                    a.make_declaration(
                        model="GFS", semantic_id="convective_precipitation", observed_at_utc=self.observed,
                        runtime_status=status, evidence_type="provider_fetch_attempt")

    def test_availability_times_are_strict_utc_and_causal(self):
        with self.assertRaises(ValueError):
            a.make_declaration(model="GFS", semantic_id="convective_precipitation", observed_at_utc="not-a-time")
        with self.assertRaises(ValueError):
            a.make_declaration(
                model="GFS", semantic_id="convective_precipitation", observed_at_utc="2026-09-28T12:00:00Z",
                runtime_status="received", value=1.0, field_available_at_utc="2026-09-28T12:00:01Z",
                evidence_type="provider_fetch_attempt")

    def test_i02_does_not_activate_runtime(self):
        b=self.registry["operational_boundary"]
        self.assertFalse(b["runtime_wiring_in_i02"])
        self.assertFalse(b["network_requests_authorized"])
        self.assertTrue(b["operational_v2_registry_unchanged"])


if __name__=="__main__":
    unittest.main()
