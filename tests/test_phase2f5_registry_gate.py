import json
import unittest
from pathlib import Path

import collect_icon_tier_a as icon
import ecmwf_registry as ecmwf
import fetch_dwd_additional_models as eps
import icon_parameter_probe as icon_probe
import noaa_weather_context as noaa
import relevant_meteorology_registry as registry

class Phase2F5RegistryGateTest(unittest.TestCase):
    def test_frozen_hashes_and_matrix_shape(self):
        self.assertEqual(registry.HASHES,{
            "semantic_contract_sha256":"3f502094a90ac8901b103cda730209bb15d6bd20e3f2c59826f502d2c5f400d3",
            "provider_matrix_sha256":"b3c1643c7cdf0fafff98511f3e38c5c57de04f90b07a68e69a1c87605edaf104",
            "registry_sha256":"4a9c458bbd8595cb0e5ca4a823aa14992ce087bc2e91813c0c96c8d835153cc1",
        })
        self.assertEqual(len(registry.SEMANTIC_IDS),13)
        self.assertEqual(set(registry.REGISTRY["providers"]),registry.EXPECTED_MODELS)
        for model in registry.EXPECTED_MODELS:
            self.assertEqual(set(registry.coverage_for(model)),set(registry.SEMANTIC_IDS))

    def test_dwd_icon_mapping_matches_frozen_matrix(self):
        actual={**icon.WIND_SEMANTICS,**icon_probe.CANONICAL}
        for model in ("ICON-D2","ICON-EU"):
            provider=registry.REGISTRY["providers"][model]
            self.assertEqual(provider["native_mapping"],actual)
            self.assertTrue(all(x=="native_received" for x in provider["coverage"].values()))

    def test_ecmwf_mapping_and_explicit_unsupported_match_matrix(self):
        provider=registry.REGISTRY["providers"]["ECMWF-IFS"]
        self.assertEqual(provider["native_mapping"],ecmwf.SEMANTICS)
        unsupported={x["semantic_id"] for x in ecmwf.UNSUPPORTED}
        self.assertEqual(unsupported,{"relative_humidity_2m","cin"})
        for semantic in registry.SEMANTIC_IDS:
            expected="unsupported_by_provider_or_product" if semantic in unsupported else "native_received"
            self.assertEqual(provider["coverage"][semantic],expected)

    def test_noaa_mapping_covers_all_core_semantics(self):
        cases=[
            ({"shortName":"10u"},"wind_u_10m"),({"shortName":"10v"},"wind_v_10m"),
            ({"shortName":"gust"},"wind_gust_10m"),({"shortName":"2t"},"air_temperature_2m"),
            ({"shortName":"dpt"},"dewpoint_temperature_2m"),
            ({"shortName":"rh","typeOfLevel":"heightAboveGround","level":2},"relative_humidity_2m"),
            ({"shortName":"prmsl"},"mean_sea_level_pressure"),({"shortName":"sp"},"surface_pressure"),
            ({"shortName":"apcp"},"total_precipitation"),({"shortName":"tcc"},"total_cloud_cover"),
            ({"shortName":"dswrf"},"surface_downward_shortwave"),({"shortName":"cape"},"cape"),
            ({"shortName":"cin"},"cin"),
        ]
        mapped=[]
        for raw,expected in cases:
            self.assertEqual(noaa.registry_semantic(raw),expected)
            mapped.append(expected)
        self.assertEqual(set(mapped),set(registry.SEMANTIC_IDS))
        for model in ("GFS","GEFS-control"):
            self.assertTrue(all(x=="native_received" for x in registry.coverage_for(model).values()))
        self.assertEqual(
            registry.REGISTRY["providers"]["GEFS-control"]["limitations"]["wind_gust_10m"]["outside_scope_status"],
            "unsupported_by_provider_or_product",
        )

    def test_eps_polar_vector_and_unsupported_cin_are_explicit(self):
        provider=registry.REGISTRY["providers"]["ICON-D2-EPS"]
        coverage=provider["coverage"]
        self.assertEqual(coverage["wind_u_10m"],"intentionally_not_applicable")
        self.assertEqual(coverage["wind_v_10m"],"intentionally_not_applicable")
        self.assertEqual(coverage["cin"],"unsupported_by_provider_or_product")
        self.assertEqual(
            {x["semantic_id"]:x["availability_status"] for x in eps.EPS_MEMBER_UNSUPPORTED},
            {"cin":"unsupported_by_provider_or_product"},
        )
        self.assertEqual(provider["native_vector_auxiliary"],{
            "wind_speed_10m":"wind_speed_10m","wind_direction_10m":"wind_direction_10m",
        })
        expected_native={
            native:semantic for native,semantic in eps.EPS_MEMBER_SEMANTICS.items()
            if native not in ("wind_speed_10m","wind_direction_10m")
        }
        self.assertEqual(provider["native_mapping"],expected_native)

    def test_provider_native_identity_metadata_remains_required(self):
        icon_keys=set(icon_probe.META_KEYS)
        self.assertTrue({"units","typeOfLevel","level","stepType","startStep","endStep","stepRange"}<=icon_keys)
        ecmwf_keys=set(ecmwf._METADATA_KEYS)
        self.assertTrue({"units","typeOfLevel","level","stepType","startStep","endStep","stepRange"}<=ecmwf_keys)
        noaa_keys=set(noaa.META_KEYS)
        self.assertTrue({"units","typeOfLevel","level","stepType","startStep","endStep","stepRange"}<=noaa_keys)
        self.assertEqual(set(eps.EPS_MEMBER_INTERVALS),set(eps.EPS_MEMBER_WEATHER_FIELDS))

    def test_operational_configs_pin_registry_hashes(self):
        root=Path(__file__).resolve().parents[1]
        plan=json.loads((root/"config/weather_acquisition_plan.json").read_text())
        policy=json.loads((root/"config/integrity_policy.json").read_text())
        expected={"path":"config/relevant_meteorology_registry_v1.json",**registry.HASHES}
        self.assertEqual(plan["relevant_meteorology_registry_contract"],expected)
        self.assertEqual(policy["relevant_meteorology_registry_contract"],expected)

if __name__=="__main__":
    unittest.main()
