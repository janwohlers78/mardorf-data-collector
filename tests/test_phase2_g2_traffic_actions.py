import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

import audit_integrity as audit
import collect_full_horizon as collect
import full_horizon_contract as horizon

ROOT=Path(__file__).resolve().parents[1]
POLICY=json.loads((ROOT/"config/integrity_policy.json").read_text(encoding="utf-8"))
PLAN=json.loads((ROOT/"config/weather_acquisition_plan.json").read_text(encoding="utf-8"))
G2=json.loads((ROOT/"config/phase2_g2_traffic_actions_v1.json").read_text(encoding="utf-8"))


class Phase2G2TrafficActionsTests(unittest.TestCase):
    def test_compatibility_horizon_is_not_provider_native_horizon(self):
        model_policy=POLICY["model_policy"]
        self.assertIn("compatibility_payload_horizon_by_cycle",model_policy)
        self.assertNotIn("provider_horizon_by_cycle",model_policy)
        self.assertEqual(audit.compatibility_payload_max("ECMWF-IFS",0,POLICY),120)
        self.assertEqual(audit.compatibility_payload_max("ECMWF-IFS",6,POLICY),90)

        run00=datetime(2026,9,25,0,tzinfo=timezone.utc)
        self.assertEqual(audit.compatibility_payload_max("GFS",0,POLICY),120)
        self.assertEqual(horizon.maximum_hours("GFS",run00),384)
        self.assertEqual(max(horizon.acquisition_leads("GFS",run00)),384)
        self.assertEqual(audit.compatibility_payload_max("GEFS-control",0,POLICY),120)
        self.assertEqual(horizon.maximum_hours("GEFS-control",run00),840)
        self.assertEqual(max(horizon.acquisition_leads("GEFS-control",run00)),840)

        semantics=PLAN["horizon_contract_semantics"]
        self.assertEqual(
            semantics["compatibility_payload_policy"],
            "config/integrity_policy.json:model_policy.compatibility_payload_horizon_by_cycle",
        )
        self.assertIn("full_horizon_contract.py::maximum_hours",semantics["provider_native_horizon_owner"])
        self.assertIn("independent",semantics["long_horizon_rule"])

    def test_normal_daily_acquisition_opportunity_baseline_is_pinned(self):
        cycle_hours={
            "ICON-D2":(0,3,6,9,12,15,18,21),
            "ICON-D2-EPS":(0,3,6,9,12,15,18,21),
            "ICON-EU":(0,3,6,9,12,15,18,21),
            "ECMWF-IFS":(0,6,12,18),
            "GFS":(0,6,12,18),
            "GEFS-control":(0,6,12,18),
        }
        actual=sum(
            len(horizon.acquisition_leads(model,datetime(2026,9,25,hour,tzinfo=timezone.utc)))
            for model,hours in cycle_hours.items() for hour in hours
        )
        self.assertEqual(actual,981)
        self.assertEqual(actual,G2["normal_routine"]["daily_core_lead_opportunities_reference"])
        self.assertEqual(G2["normal_routine"]["predecessor_daily_core_lead_opportunities_reference"],1388)
        self.assertLess(actual,1388)
        samples=G2["normal_routine"]["real_run_samples"]
        self.assertEqual(len(samples),4)
        self.assertEqual(
            max(x["icon_tier_a_response_bytes"] for x in samples),
            G2["normal_routine"]["icon_tier_a_observed_high_water_bytes"],
        )

    def test_sparse_gefs_live_measurement_remains_inside_frozen_budgets(self):
        measured=G2["sparse_gefs_00z"]
        policy=POLICY["model_policy"]["noaa_weather_context"]["full_member_live_proof"]
        guard=POLICY["model_policy"]["gefs_full_member_archive"]
        self.assertEqual(measured["live_proof_run_id"],policy["public_run_id"])
        self.assertEqual(measured["total_requests"],policy["total_requests"])
        self.assertEqual(measured["total_response_bytes"],policy["total_response_bytes"])
        self.assertEqual(measured["request_budget"],guard["max_total_requests_per_full_cycle"])
        self.assertEqual(measured["network_budget_bytes"],guard["max_total_filtered_network_bytes"])
        self.assertLessEqual(measured["total_requests"],measured["request_budget"])
        self.assertLessEqual(measured["total_response_bytes"],measured["network_budget_bytes"])
        self.assertLessEqual(
            measured["live_private_conservative_billable_minutes"],
            measured["private_additional_billable_minutes_budget_per_day"],
        )

    def test_supplemental_retry_is_bounded_and_cannot_become_full_refetch(self):
        supplemental=G2["supplemental_retry"]
        self.assertEqual(collect.PGRB2B_RETRY_DELAY_HOURS,supplemental["retry_delay_hours"])
        self.assertEqual(collect.PGRB2B_MAX_RETRIES,supplemental["maximum_retries"])
        run00=datetime(2026,9,25,0,tzinfo=timezone.utc)
        far=[h for h in horizon.acquisition_leads("GEFS-control",run00) if h>240]
        self.assertEqual(len(far),25)
        self.assertEqual(len(far),supplemental["max_requests_per_cycle_current_contract"])

        workflow=(ROOT/".github/workflows/collect-models.yml").read_text(encoding="utf-8")
        self.assertIn("steps.cycle_gate.outputs.gefs_control == 'fetch'",workflow)
        self.assertIn("steps.cycle_gate.outputs.gefs_full == 'fetch'",workflow)
        self.assertIn("steps.cycle_gate.outputs.any_work == 'true'",workflow)
        self.assertIn("Private transfer/promotion: suppressed.",workflow)

        source=Path(collect.__file__).read_text(encoding="utf-8")
        self.assertIn('if action=="supplemental_retry" and model!="GEFS-control"',source)
        self.assertIn('if action=="supplemental_retry":\n    supplement_jobs.extend',source)
        self.assertIn('candidates=[x for x in noaa_requests("GEFS-control",run,lead) if x[0]=="gefs_0p50b"]',source)

    def test_noop_and_private_action_measurements_are_explicit(self):
        noop=G2["no_op"]
        self.assertEqual(noop["provider_downloads"],0)
        self.assertEqual(noop["private_transfers"],0)
        self.assertEqual(noop["private_model_promotions"],0)
        self.assertEqual({x["workflow_elapsed_seconds"] for x in noop["real_samples"]},{8,9})
        private=G2["private_model_promotion"]
        self.assertEqual(private["observed_execution_seconds_total"],199)
        self.assertEqual(private["observed_conservative_billable_minutes_total"],4)
        self.assertEqual(private["successful_execution_seconds_range"],[51,55])
        self.assertEqual(private["latest_public_transfer_private_run_count"],1)


if __name__=="__main__":
    unittest.main()
