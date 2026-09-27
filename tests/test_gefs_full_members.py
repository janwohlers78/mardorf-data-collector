import json
import unittest
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import gefs_full_members as g


class SparseGefsPolicyTests(unittest.TestCase):
    def setUp(self):
        self.run=datetime(2026,9,26,0,tzinfo=timezone.utc)

    def query(self,url):
        return parse_qs(urlparse(url).query)

    def test_policy_is_once_daily_00z_sparse_and_under_request_gate(self):
        p=g.request_plan(self.run)
        self.assertEqual(len(p),31*24)
        self.assertLessEqual(len(p),800)
        self.assertEqual(sorted({x["lead_hours"] for x in p}),list(g.LEADS))
        self.assertTrue(all(x["lead_hours"]>48 for x in p))
        self.assertEqual(len({x["member_id"] for x in p}),31)
        self.assertEqual(g.request_plan(datetime(2026,9,26,6,tzinfo=timezone.utc)),[])

    def test_product_transition_is_exact_and_no_pgrb2b_is_duplicated(self):
        p=g.request_plan(self.run)
        near=[x for x in p if x["lead_hours"]<=240]
        far=[x for x in p if x["lead_hours"]>240]
        self.assertEqual({x["provider_product"] for x in near},{"gefs_0p25s"})
        self.assertEqual({x["provider_product"] for x in far},{"gefs_0p50a"})
        self.assertFalse(any("0p50b" in x["url"] for x in p))
        self.assertEqual(len(near),31*len(g.NEAR_LEADS))
        self.assertEqual(len(far),31*len(g.FAR_LEADS))

    def test_near_request_contains_only_ensemble_relevant_field_subset(self):
        q=self.query(g.request_url(self.run,"p01",120))
        for var in ("UGRD","VGRD","GUST","APCP","TCDC","CAPE","CIN"):
            self.assertEqual(q["var_"+var],["on"])
        for forbidden in ("TMP","DPT","RH","PRMSL","PRES","DSWRF"):
            self.assertNotIn("var_"+forbidden,q)
        self.assertIn("lev_10_m_above_ground",q)
        self.assertIn("lev_surface",q)
        self.assertIn("lev_entire_atmosphere",q)

    def test_far_request_drops_gust_and_nonensemble_context(self):
        q=self.query(g.request_url(self.run,"p30",840))
        for var in ("UGRD","VGRD","APCP","TCDC","CAPE","CIN"):
            self.assertEqual(q["var_"+var],["on"])
        for forbidden in ("GUST","TMP","DPT","RH","PRMSL","PRES","DSWRF"):
            self.assertNotIn("var_"+forbidden,q)
        self.assertIn("pgrb2a.0p50",q["file"][0])

    def test_member_identity_and_roles_are_frozen(self):
        self.assertEqual(g.MEMBERS[0],"c00")
        self.assertEqual(g.MEMBERS[-1],"p30")
        self.assertEqual(len(g.MEMBERS),31)
        self.assertEqual(g.MEMBER_ROLES["c00"],"control_member")
        self.assertEqual(g.MEMBER_ROLES["p01"],"perturbed_member")

    def test_source_complete_requires_all_member_lead_units(self):
        records=[]
        point={"latitude":52.5,"longitude":9.25,"selection":"test"}
        for unit in g.request_plan(self.run):
            records.append({
                "member_id":unit["member_id"],"member_role":unit["member_role"],
                "lead_hours":unit["lead_hours"],"valid_time_utc":unit["valid_time_utc"],
                "provider_product":unit["provider_product"],"run_time_utc":self.run.isoformat(),
                "request_status":"received","retrieved_at_utc":self.run.isoformat(),
                "response_bytes":100,"response_sha256":"a"*64,"attempt_count":1,
                "elapsed_seconds":0.1,"returned_coordinate":point,
                "fields":[{"field_key":"x","semantic_id":unit["semantics"][0],
                           "parameter_native":"x","param_id_native":"1",
                           "field_provider_product":unit["provider_product"],
                           "type_of_level_native":"surface","level_native":"0",
                           "step_type_native":"instant","step_range_native":str(unit["lead_hours"]),
                           "start_step_native":unit["lead_hours"],"end_step_native":unit["lead_hours"],
                           "step_units_native":"1","unit_native":"1","value_native":1.0,
                           "source_sha256":"a"*64}],
            })
        source=g.build_source(self.run,records)
        self.assertEqual(source["collection_status"],"complete")
        self.assertEqual(source["received_request_count"],31*24)
        self.assertEqual(source["policy_omissions"][0]["availability_status"],"not_requested_by_policy")
        self.assertIn("GEFS-control",source["qa_control_source"])

    def test_same_complete_source_is_carried_forward(self):
        source={
            "run_time_utc":self.run.isoformat(),
            "method_version":g.METHOD_VERSION,
            "policy_version":g.POLICY_VERSION,
            "collection_status":"complete",
            "expected_request_count":31*24,
            "received_request_count":31*24,
        }
        self.assertTrue(g.source_matches_policy(source,self.run))
        source["received_request_count"]-=1
        self.assertFalse(g.source_matches_policy(source,self.run))

    def test_policy_summary_records_no_interpolation_and_no_conditional_fetch(self):
        p=g.policy_summary()
        self.assertEqual(p["full_member_cycles_utc"],[0])
        self.assertIn("no interpolation",p["native_time_policy"])
        self.assertFalse(p["conditional_forecast_triggered_fetch"])
        self.assertEqual(p["pgrb2b_perturbed_members"],"not_requested_by_policy")


if __name__=="__main__":
    unittest.main()
