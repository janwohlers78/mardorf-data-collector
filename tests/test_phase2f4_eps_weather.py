import copy
import unittest
from datetime import datetime, timedelta, timezone

from audit_integrity import audit_eps_hourly_source
from fetch_dwd_additional_models import (
    EPS_MEMBER_WEATHER_FIELDS,
    EPS_MEMBER_SEMANTICS,
    EPS_MEMBER_UNSUPPORTED,
)


class Phase2F4EpsWeatherTests(unittest.TestCase):
    def source(self):
        run=datetime(2026,9,27,3,tzinfo=timezone.utc)
        times=[(run+timedelta(hours=h)).isoformat() for h in range(49)]
        base_values={
            "wind_speed_10m":5.0,
            "wind_direction_10m":270.0,
            "wind_gusts_10m":7.0,
            "precipitation":0.2,
            "cape":120.0,
            "temperature_2m":15.0,
            "relative_humidity_2m":70.0,
            "dew_point_2m":10.0,
            "pressure_msl":1012.0,
            "surface_pressure":1004.0,
            "cloud_cover":55.0,
            "shortwave_radiation":150.0,
        }
        columns={
            field:{str(member):[base_values[field]]*49 for member in range(20)}
            for field in EPS_MEMBER_WEATHER_FIELDS
        }
        specs={
            field:{
                "semantic_id":EPS_MEMBER_SEMANTICS[field],
                "parameter_native":field,
                "field_provider_product":"open_meteo:dwd_icon_d2_eps",
                "unit":"1",
                "interval":"instantaneous",
                "aggregation":"circular" if field=="wind_direction_10m" else "scalar",
            }
            for field in EPS_MEMBER_WEATHER_FIELDS
        }
        completeness={
            field:{
                "expected_member_count":20,
                "complete_0_48_member_count":20,
                "complete_0_48_member_ids":list(range(20)),
                "completeness_status":"complete",
            }
            for field in EPS_MEMBER_WEATHER_FIELDS
        }
        return run,{
            "schema_version":2,
            "method_version":"phase2f4-icon-d2-eps-full-member-weather-v1",
            "registry_version":"relevant-meteorology-v1",
            "model":"dwd_icon_d2_eps",
            "cycle_evidence":"stable_provider_metadata_association",
            "run_time_utc":run.isoformat(),
            "retrieved_at_utc":(run+timedelta(hours=1)).isoformat(),
            "response_sha256":"a"*64,
            "times_utc":times,
            "columns":columns,
            "field_specs":specs,
            "field_completeness_0_48":completeness,
            "unsupported_registry_semantics":[dict(x) for x in EPS_MEMBER_UNSUPPORTED],
            "requested_coordinate":{"latitude":52.4942,"longitude":9.3418},
            "returned_coordinate":{"latitude":52.5,"longitude":9.34},
            "spatial_provenance_verified":True,
            "spatial_provenance_evidence":{
                "verified":True,
                "eps_native_grid_identity_verified":True,
                "eps_native_grid_identity":{"grid_type":"unstructured_grid"},
                "dwd_regular_grid_coordinate_parity_verified":True,
            },
            "dwd_eps_native_grid_identity_verified":True,
            "dwd_regular_grid_coordinate_parity_verified":True,
            "native_grid_parity_verified":False,
            "response_bound_run_identity_verified":False,
            "response_run_binding":{
                "status":"strong_indirect_bracketed_not_provider_embedded",
                "provider_response_embeds_run_time":False,
                "metadata_stable_across_response":True,
            },
        }

    def test_phase2f4_registry_contract_has_live_proven_fields_and_explicit_cin_gap(self):
        self.assertEqual(len(EPS_MEMBER_WEATHER_FIELDS),12)
        self.assertEqual(EPS_MEMBER_SEMANTICS["temperature_2m"],"air_temperature_2m")
        self.assertEqual(EPS_MEMBER_SEMANTICS["precipitation"],"total_precipitation")
        self.assertEqual(EPS_MEMBER_SEMANTICS["shortwave_radiation"],"surface_downward_shortwave")
        self.assertEqual(EPS_MEMBER_UNSUPPORTED[0]["semantic_id"],"cin")
        self.assertEqual(
            EPS_MEMBER_UNSUPPORTED[0]["availability_status"],
            "unsupported_by_provider_or_product",
        )

    def test_expanded_hourly_source_audit_accepts_20_members_for_all_fields(self):
        run,source=self.source()
        failures,summary=audit_eps_hourly_source(source,run,20)
        self.assertEqual(failures,[],failures)
        self.assertEqual(summary["registry_version"],"relevant-meteorology-v1")
        self.assertEqual(summary["member_weather_field_count"],12)
        self.assertEqual(
            summary["unsupported_registry_semantics"][0]["semantic_id"],
            "cin",
        )

    def test_expanded_hourly_source_audit_rejects_member_gap(self):
        run,source=self.source()
        broken=copy.deepcopy(source)
        del broken["columns"]["temperature_2m"]["19"]
        failures,_=audit_eps_hourly_source(broken,run,20)
        self.assertTrue(
            any(
                item["reason"]=="hourly_source_weather_member_identity_mismatch"
                and item["field"]=="temperature_2m"
                for item in failures
            ),
            failures,
        )


if __name__=="__main__":
    unittest.main()
