import json
import unittest
from pathlib import Path

import collect_icon_tier_a as icon
import ecmwf_registry as ecmwf
import fetch_dwd_additional_models as eps
import icon_parameter_probe as icon_probe
import noaa_weather_context as noaa
import relevant_meteorology_registry as registry_v1
import relevant_meteorology_registry_v2 as registry_v2


class Phase2F5RegistryGateTest(unittest.TestCase):
    def test_frozen_v1_hashes_remain_immutable(self):
        self.assertEqual(registry_v1.HASHES,{
            "semantic_contract_sha256":"3f502094a90ac8901b103cda730209bb15d6bd20e3f2c59826f502d2c5f400d3",
            "provider_matrix_sha256":"b3c1643c7cdf0fafff98511f3e38c5c57de04f90b07a68e69a1c87605edaf104",
            "registry_sha256":"4a9c458bbd8595cb0e5ca4a823aa14992ce087bc2e91813c0c96c8d835153cc1",
        })
        self.assertEqual(len(registry_v1.SEMANTIC_IDS),13)
        for model in ("ICON-D2","ICON-EU"):
            provider=registry_v1.REGISTRY["providers"][model]
            self.assertEqual(provider["native_mapping"]["aswdir_s"],"surface_downward_shortwave")
            self.assertEqual(provider["native_mapping"]["aswdifd_s"],"surface_downward_shortwave")

    def test_registry_v2_hashes_and_shortwave_semantics(self):
        self.assertEqual(registry_v2.HASHES,{
            "semantic_contract_sha256":"169de0495c8b4b24f8d541ed7d9a829c494d4710bf5383e356c044bff2c7a141",
            "provider_matrix_sha256":"221529e2533616dd2e6fcf597c4765d6532323899da5a366bf482dd7ae23ffc1",
            "registry_sha256":"6ffbad0550201b075e1b5f6d527e7e340d95611d23cc193aca5855aec9d3c9a7",
        })
        self.assertEqual(len(registry_v2.SEMANTIC_IDS),15)
        self.assertEqual(len(set(registry_v2.SEMANTIC_IDS)),15)
        self.assertTrue({
            registry_v2.DIRECT,registry_v2.DIFFUSE,registry_v2.TOTAL
        } <= set(registry_v2.SEMANTIC_IDS))

    def test_dwd_runtime_mapping_matches_registry_v2_and_never_claims_native_total(self):
        actual={**icon.WIND_SEMANTICS,**icon_probe.CANONICAL}
        for model in ("ICON-D2","ICON-EU"):
            provider=registry_v2.REGISTRY["providers"][model]
            self.assertEqual(provider["native_mapping"],actual)
            self.assertEqual(provider["native_mapping"]["aswdir_s"],registry_v2.DIRECT)
            self.assertEqual(provider["native_mapping"]["aswdifd_s"],registry_v2.DIFFUSE)
            self.assertEqual(provider["coverage"][registry_v2.DIRECT],"native_received")
            self.assertEqual(provider["coverage"][registry_v2.DIFFUSE],"native_received")
            self.assertEqual(provider["coverage"][registry_v2.TOTAL],"intentionally_not_applicable")
        self.assertNotEqual(icon_probe.CANONICAL["aswdir_s"],icon_probe.CANONICAL["aswdifd_s"])

    def test_total_shortwave_derivation_is_explicit_and_provenance_bound(self):
        d=registry_v2.REGISTRY["derived_semantics"][registry_v2.TOTAL]
        self.assertFalse(d["automatic_derivation"])
        self.assertTrue(d["explicit_derivation_allowed"])
        self.assertEqual(set(d["required_component_semantics"]),{
            registry_v2.DIRECT,registry_v2.DIFFUSE
        })
        rules=" ".join(d["required_compatibility"])
        self.assertIn("interval_start_utc",rules)
        self.assertIn("interval_end_utc",rules)
        self.assertIn("compatible provider-native units",rules)
        self.assertIn("component field_identity_id values",d["required_provenance"])

    def test_other_providers_keep_native_total_without_false_component_claims(self):
        self.assertEqual(ecmwf.SEMANTICS["ssrd"],registry_v2.TOTAL)
        self.assertEqual(noaa.registry_semantic({"shortName":"dswrf"}),registry_v2.TOTAL)
        for model in ("ECMWF-IFS","GFS","GEFS-control","ICON-D2-EPS"):
            provider=registry_v2.REGISTRY["providers"][model]
            self.assertEqual(provider["coverage"][registry_v2.DIRECT],"unsupported_by_provider_or_product")
            self.assertEqual(provider["coverage"][registry_v2.DIFFUSE],"unsupported_by_provider_or_product")
            self.assertEqual(provider["coverage"][registry_v2.TOTAL],"native_received")

    def test_eps_polar_vector_and_unsupported_cin_remain_explicit(self):
        provider=registry_v2.REGISTRY["providers"]["ICON-D2-EPS"]
        self.assertEqual(provider["coverage"]["wind_u_10m"],"intentionally_not_applicable")
        self.assertEqual(provider["coverage"]["wind_v_10m"],"intentionally_not_applicable")
        self.assertEqual(provider["coverage"]["cin"],"unsupported_by_provider_or_product")
        self.assertEqual(
            {x["semantic_id"]:x["availability_status"] for x in eps.EPS_MEMBER_UNSUPPORTED},
            {"cin":"unsupported_by_provider_or_product"},
        )

    def test_provider_native_identity_metadata_remains_required(self):
        icon_keys=set(icon_probe.META_KEYS)
        self.assertTrue({"units","typeOfLevel","level","stepType","startStep","endStep","stepRange"}<=icon_keys)
        ecmwf_keys=set(ecmwf._METADATA_KEYS)
        self.assertTrue({"units","typeOfLevel","level","stepType","startStep","endStep","stepRange"}<=ecmwf_keys)
        noaa_keys=set(noaa.META_KEYS)
        self.assertTrue({"units","typeOfLevel","level","stepType","startStep","endStep","stepRange"}<=noaa_keys)
        self.assertEqual(set(eps.EPS_MEMBER_INTERVALS),set(eps.EPS_MEMBER_WEATHER_FIELDS))

    def test_operational_configs_pin_registry_v2_hashes(self):
        root=Path(__file__).resolve().parents[1]
        plan=json.loads((root/"config/weather_acquisition_plan.json").read_text())
        policy=json.loads((root/"config/integrity_policy.json").read_text())
        expected={"path":"config/relevant_meteorology_registry_v2.json",**registry_v2.HASHES}
        self.assertEqual(plan["relevant_meteorology_registry_version"],"relevant-meteorology-v2")
        self.assertEqual(plan["relevant_meteorology_registry_contract"],expected)
        self.assertEqual(policy["relevant_meteorology_registry_contract"],expected)
        shortwave={x["parameter_id"]:x for x in plan["parameters"] if "shortwave" in x["parameter_id"]}
        self.assertEqual(set(shortwave),{
            registry_v2.DIRECT,registry_v2.DIFFUSE,registry_v2.TOTAL
        })
        self.assertEqual(shortwave[registry_v2.DIRECT]["dwd_fields"],["aswdir_s"])
        self.assertEqual(shortwave[registry_v2.DIFFUSE]["dwd_fields"],["aswdifd_s"])
        self.assertEqual(shortwave[registry_v2.TOTAL]["dwd_fields"],[])


if __name__=="__main__":
    unittest.main()
