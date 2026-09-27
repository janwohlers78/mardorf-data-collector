import copy
import unittest

import cape_native_identity as cape


def gfs_item(level_type,level,value):
    return {
        "semantic_id":"cape",
        "shortName":"cape",
        "paramId":59,
        "typeOfLevel":level_type,
        "level":level,
        "stepType":"instant",
        "stepRange":"12",
        "units":"J kg**-1",
        "provider_product":"gfs_0p25",
        "value":value,
    }


class CapeNativeIdentityContractTests(unittest.TestCase):
    def test_gfs_level_variants_are_distinct_and_permutation_invariant(self):
        items=[
            gfs_item("surface",0,120.0),
            gfs_item("pressureFromGroundLayer",18000,180.0),
            gfs_item("pressureFromGroundLayer",9000,250.0),
            gfs_item("pressureFromGroundLayer",25500,420.0),
        ]
        a=cape.evaluate_records([("GFS",{"values":{"cape":items}})])
        b=cape.evaluate_records([("GFS",{"values":{"cape":list(reversed(items))}})])
        self.assertIs(a["signal"],True)
        self.assertEqual(a["status"],"positive")
        self.assertEqual(a["selected_identity"]["identity_id"],
                         "noaa-gfs:gfs_0p25:cape:pressureFromGroundLayer:25500")
        self.assertEqual(a["selected_identity"],b["selected_identity"])
        self.assertEqual(a["signal"],b["signal"])
        self.assertEqual({x["identity_id"] for x in a["candidates"]},{
            "noaa-gfs:gfs_0p25:cape:surface:0",
            "noaa-gfs:gfs_0p25:cape:pressureFromGroundLayer:18000",
            "noaa-gfs:gfs_0p25:cape:pressureFromGroundLayer:9000",
            "noaa-gfs:gfs_0p25:cape:pressureFromGroundLayer:25500",
        })

    def test_low_identified_cape_is_clear_only_when_identity_is_unambiguous(self):
        result=cape.evaluate_records([("GFS",{"values":{"cape":[gfs_item("surface",0,299.9)]}})])
        self.assertIs(result["signal"],False)
        self.assertEqual(result["status"],"negative")
        self.assertEqual(result["max_jkg"],299.9)

    def test_ambiguous_native_identity_never_becomes_false_clear(self):
        bad=gfs_item("pressureFromGroundLayer",12345,20.0)
        good=gfs_item("surface",0,25.0)
        result=cape.evaluate_records([("GFS",{"values":{"cape":[good,bad]}})])
        self.assertIsNone(result["signal"])
        self.assertEqual(result["status"],"ambiguous")
        self.assertTrue(result["problems"])

    def test_missing_cape_never_zero_fills_or_clears(self):
        result=cape.evaluate_records([("GFS",{"values":{}})])
        self.assertIsNone(result["signal"])
        self.assertEqual(result["status"],"missing")
        self.assertIsNone(result["max_jkg"])
        self.assertEqual(result["missing_models"],["GFS"])

    def test_positive_evidence_remains_conservative_even_with_ambiguity(self):
        high=gfs_item("surface",0,350.0)
        bad=gfs_item("pressureFromGroundLayer",12345,20.0)
        result=cape.evaluate_records([("GFS",{"values":{"cape":[bad,high]}})])
        self.assertIs(result["signal"],True)
        self.assertEqual(result["status"],"positive")
        self.assertEqual(result["selected_identity"]["identity_id"],
                         "noaa-gfs:gfs_0p25:cape:surface:0")

    def test_ecmwf_mucape_identity_is_exact(self):
        item={
            "semantic_id":"cape","shortName":"mucape","paramId":228235,
            "typeOfLevel":"mostUnstableParcel","level":0,"stepType":"instant",
            "stepRange":"12","units":"J kg**-1","provider_product":"ifs_oper_fc_0p25",
            "value":275.0,
        }
        got=cape.identify_item("ECMWF-IFS","mucape",item)
        self.assertEqual(got["status"],"identified")
        self.assertEqual(got["identity_id"],"ecmwf-ifs:mucape:mostUnstableParcel:0")

    def test_dwd_cape_ml_identity_is_not_inferred_from_generic_surface_cape(self):
        good={
            "semantic_id":"cape","shortName":"cape_ml","typeOfLevel":"unknown","level":0,
            "stepType":"instant","stepRange":"12","units":"J kg-1","value":20.0,
        }
        bad={**good,"shortName":"cape","typeOfLevel":"surface"}
        self.assertEqual(cape.identify_item("ICON-D2","cape_ml",good)["status"],"identified")
        self.assertEqual(cape.identify_item("ICON-D2","cape_ml",bad)["status"],"ambiguous")

    def test_eps_member_cape_requires_complete_member_identity_set(self):
        members=[{"member":i,"cape":float(i)} for i in range(20)]
        ok=cape.evaluate_records([("ICON-D2-EPS",{"members":members})])
        self.assertIs(ok["signal"],False)
        self.assertEqual(ok["candidate_count"],20)
        incomplete=cape.evaluate_records([("ICON-D2-EPS",{"members":members[:-1]})])
        self.assertIsNone(incomplete["signal"])
        self.assertEqual(incomplete["status"],"ambiguous")

    def test_producer_declared_identity_mismatch_fails_unknown(self):
        item=gfs_item("surface",0,100.0)
        item["cape_native_identity_id"]="wrong"
        result=cape.evaluate_records([("GFS",{"values":{"cape":[item]}})])
        self.assertIsNone(result["signal"])
        self.assertEqual(result["status"],"ambiguous")


if __name__=="__main__":
    unittest.main()
