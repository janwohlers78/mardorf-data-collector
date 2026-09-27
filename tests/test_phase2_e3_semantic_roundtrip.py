import json
import unittest
from pathlib import Path

import availability_contract as availability
import collect_icon_tier_a as icon
import ecmwf_registry as ecmwf
import fetch_dwd_additional_models as eps
import icon_parameter_probe as icon_probe
import noaa_weather_context as noaa
import relevant_meteorology_registry_v2 as registry


ROOT=Path(__file__).resolve().parents[1]
MATRIX=json.loads((ROOT/"config/phase2_semantic_roundtrip_matrix_v1.json").read_text())
REGISTRY=registry.REGISTRY
SEMANTICS=set(registry.SEMANTIC_IDS)
PROVIDERS=set(MATRIX["expected_provider_paths"])


NOAA_NATIVE_EXAMPLES={
    "wind_u_10m":{"shortName":"10u","name":"10 metre U wind component","typeOfLevel":"heightAboveGround","level":10},
    "wind_v_10m":{"shortName":"10v","name":"10 metre V wind component","typeOfLevel":"heightAboveGround","level":10},
    "wind_gust_10m":{"shortName":"gust","name":"Wind speed (gust)","typeOfLevel":"surface","level":0},
    "air_temperature_2m":{"shortName":"2t","name":"2 metre temperature","typeOfLevel":"heightAboveGround","level":2},
    "dewpoint_temperature_2m":{"shortName":"2d","name":"2 metre dew point temperature","typeOfLevel":"heightAboveGround","level":2},
    "relative_humidity_2m":{"shortName":"2r","name":"2 metre relative humidity","typeOfLevel":"heightAboveGround","level":2},
    "mean_sea_level_pressure":{"shortName":"prmsl","name":"Pressure reduced to MSL","typeOfLevel":"meanSea","level":0},
    "surface_pressure":{"shortName":"sp","name":"Surface pressure","typeOfLevel":"surface","level":0},
    "total_precipitation":{"shortName":"apcp","name":"Total precipitation","typeOfLevel":"surface","level":0},
    "total_cloud_cover":{"shortName":"tcc","name":"Total Cloud Cover","typeOfLevel":"atmosphere","level":0},
    "surface_downward_shortwave":{"shortName":"dswrf","name":"Downward short-wave radiation flux","typeOfLevel":"surface","level":0},
    "cape":{"shortName":"cape","name":"Convective available potential energy","typeOfLevel":"surface","level":0},
    "cin":{"shortName":"cin","name":"Convective inhibition","typeOfLevel":"surface","level":0},
}


class Phase2E3CollectorSemanticMatrixTests(unittest.TestCase):
    def test_historical_13_name_expands_to_all_15_active_v2_semantics(self):
        self.assertEqual(MATRIX["method_version"],"phase2-semantic-provider-roundtrip-matrix-v1")
        self.assertEqual(MATRIX["active_registry_version"],"relevant-meteorology-v2")
        self.assertEqual(MATRIX["expected_active_semantic_count"],15)
        self.assertEqual(len(SEMANTICS),15)
        self.assertEqual(len(registry_v1_semantics()),13)
        self.assertEqual(
            SEMANTICS-registry_v1_semantics(),
            {"surface_downward_shortwave_direct","surface_downward_shortwave_diffuse"},
        )
        self.assertEqual(MATRIX["expected_provider_semantic_cells"],90)

    def test_all_90_provider_semantic_cells_are_explicit(self):
        self.assertEqual(set(REGISTRY["providers"]),PROVIDERS)
        cells=[]
        for model in sorted(PROVIDERS):
            coverage=REGISTRY["providers"][model]["coverage"]
            self.assertEqual(set(coverage),SEMANTICS)
            for semantic in sorted(SEMANTICS):
                cells.append((model,semantic,coverage[semantic]))
        self.assertEqual(len(cells),90)
        allowed={"native_received","unsupported_by_provider_or_product","intentionally_not_applicable"}
        self.assertEqual({status for _,_,status in cells}<=allowed,True)
        self.assertEqual(sum(status=="native_received" for _,_,status in cells),75)
        self.assertEqual(sum(status!="native_received" for _,_,status in cells),15)

    def test_dwd_runtime_native_mapping_equals_registry_v2(self):
        actual={**icon.WIND_SEMANTICS,**icon_probe.CANONICAL}
        for model in ("ICON-D2","ICON-EU"):
            provider=REGISTRY["providers"][model]
            self.assertEqual(provider["native_mapping"],actual)
            self.assertEqual(provider["coverage"]["surface_downward_shortwave"],"intentionally_not_applicable")
            self.assertEqual(actual["aswdir_s"],"surface_downward_shortwave_direct")
            self.assertEqual(actual["aswdifd_s"],"surface_downward_shortwave_diffuse")

    def test_ecmwf_runtime_native_mapping_equals_registry_v2(self):
        provider=REGISTRY["providers"]["ECMWF-IFS"]
        self.assertEqual(provider["native_mapping"],ecmwf.SEMANTICS)
        self.assertEqual(
            {ecmwf.SEMANTICS[k] for k in ("10fg","10fg3","10fg6")},
            {"wind_gust_10m"},
        )
        self.assertEqual(ecmwf.SEMANTICS["ssrd"],"surface_downward_shortwave")
        # Pressure-level RH must never satisfy the 2 m RH semantic.
        self.assertNotIn("r",ecmwf.SEMANTICS)

    def test_noaa_runtime_mapping_covers_every_registry_native_semantic(self):
        for model in ("GFS","GEFS-control"):
            provider=REGISTRY["providers"][model]
            expected={
                semantic for semantic,status in provider["coverage"].items()
                if status=="native_received"
            }
            self.assertEqual(expected,set(NOAA_NATIVE_EXAMPLES))
            mapped={
                semantic:noaa.registry_semantic(dict(example))
                for semantic,example in NOAA_NATIVE_EXAMPLES.items()
            }
            self.assertEqual(mapped,{semantic:semantic for semantic in expected})
            self.assertNotIn("surface_downward_shortwave_direct",mapped.values())
            self.assertNotIn("surface_downward_shortwave_diffuse",mapped.values())

    def test_icon_eps_runtime_mapping_preserves_polar_vector_boundary(self):
        provider=REGISTRY["providers"]["ICON-D2-EPS"]
        actual={
            native:semantic for native,semantic in eps.EPS_MEMBER_SEMANTICS.items()
            if semantic in SEMANTICS
        }
        self.assertEqual(provider["native_mapping"],actual)
        self.assertEqual(provider["coverage"]["wind_u_10m"],"intentionally_not_applicable")
        self.assertEqual(provider["coverage"]["wind_v_10m"],"intentionally_not_applicable")
        self.assertEqual(eps.EPS_MEMBER_SEMANTICS["wind_speed_10m"],"wind_speed_10m")
        self.assertEqual(eps.EPS_MEMBER_SEMANTICS["wind_direction_10m"],"wind_direction_10m")
        self.assertNotIn("wind_speed_10m",SEMANTICS)
        self.assertNotIn("wind_direction_10m",SEMANTICS)

    def test_every_non_native_cell_has_explicit_registry_reason(self):
        for model,provider in REGISTRY["providers"].items():
            exceptions=provider.get("exceptions") or {}
            for semantic,status in provider["coverage"].items():
                if status=="native_received":
                    continue
                with self.subTest(model=model,semantic=semantic,status=status):
                    self.assertIn(semantic,exceptions)
                    self.assertTrue(str(exceptions[semantic]).strip())

    def test_runtime_metadata_contract_preserves_unit_level_and_interval_evidence(self):
        required={"units","typeOfLevel","level","stepType","stepRange","startStep","endStep","stepUnits"}
        self.assertTrue(required<=set(icon_probe.META_KEYS))
        self.assertTrue(required<=set(ecmwf._METADATA_KEYS))
        self.assertTrue(required<=set(noaa.META_KEYS))
        self.assertEqual(set(eps.EPS_MEMBER_INTERVALS),set(eps.EPS_MEMBER_WEATHER_FIELDS))
        self.assertEqual(set(eps.EPS_MEMBER_SEMANTICS),set(eps.EPS_MEMBER_WEATHER_FIELDS))
        for field in eps.EPS_MEMBER_WEATHER_FIELDS:
            self.assertTrue(eps.EPS_MEMBER_INTERVALS[field])
        # Open-Meteo member fields expose no provider-native vertical GRIB level;
        # E3 requires preservation of that absence, not invention of a level.
        self.assertNotIn("typeOfLevel",eps.EPS_MEMBER_INTERVALS)
        self.assertNotIn("level",eps.EPS_MEMBER_INTERVALS)

    def test_missingness_is_closed_and_non_received_never_carries_value(self):
        self.assertEqual(
            availability.STATES,
            {
                "received","unsupported_by_provider_or_product",
                "intentionally_not_applicable","not_requested_by_policy",
                "not_yet_published","fetch_error",
            },
        )
        for state in sorted(availability.STATES-{"received"}):
            with self.subTest(state=state):
                item={"availability_status":state,"value":None}
                got=availability.normalize_field_item(item,"2026-09-27T12:00:00+00:00")
                self.assertIsNone(got["value"])
                self.assertNotIn("field_available_at_utc",got)
                with self.assertRaises(ValueError):
                    availability.normalize_field_item(
                        {"availability_status":state,"value":0.0},
                        "2026-09-27T12:00:00+00:00",
                    )


def registry_v1_semantics():
    payload=json.loads((ROOT/"config/relevant_meteorology_registry_v1.json").read_text())
    return {item["semantic_id"] for item in payload["core_semantics"]}


if __name__=="__main__":
    unittest.main()
