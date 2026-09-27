import unittest

import availability_contract as availability
import ecmwf_registry
import extend_model_horizon as ext
import fetch_extra_models as extra


class ECMWFRegistryTests(unittest.TestCase):
    def test_routine_parameter_set_matches_live_verified_surface_registry(self):
        expected = {
            "10u", "10v", "10fg", "10fg3", "tp", "mucape",
            "2t", "2d", "sp", "msl", "tcc", "ssrd",
        }
        self.assertEqual(set(ecmwf_registry.PARAMS), expected)
        self.assertEqual(set(extra.ECMWF_PARAMS), expected)
        self.assertEqual(set(ext.ECMWF_PARAMS), expected)
        self.assertNotIn("2r", expected)
        self.assertNotIn("r", expected)
        self.assertNotIn("cin", expected)
        self.assertNotIn("mucin", expected)

    def test_registry_mapping_keeps_gust_aliases_but_does_not_map_pressure_rh(self):
        self.assertEqual(ecmwf_registry.SEMANTICS["10fg"], "wind_gust_10m")
        self.assertEqual(ecmwf_registry.SEMANTICS["10fg3"], "wind_gust_10m")
        self.assertEqual(ecmwf_registry.SEMANTICS["ssrd"], "surface_downward_shortwave")
        self.assertEqual(ecmwf_registry.SEMANTICS["mucape"], "cape")
        self.assertNotIn("r", ecmwf_registry.SEMANTICS)
        self.assertNotIn("2r", ecmwf_registry.SEMANTICS)
        self.assertNotIn("cin", ecmwf_registry.SEMANTICS)

    def test_metadata_enrichment_preserves_native_temporal_semantics(self):
        nearest = [
            ("10u", "12", 3.0),
            ("10fg", "11-12", 8.0),
            ("tp", "0-12", 0.004),
            ("ssrd", "0-12", 12345.0),
        ]
        metadata = [
            {
                "shortName": "10u", "paramId": 165, "typeOfLevel": "heightAboveGround",
                "level": 10, "stepType": "instant", "stepRange": "12",
                "startStep": 12, "endStep": 12, "stepUnits": 1, "units": "m s**-1",
            },
            {
                "shortName": "10fg", "paramId": 49, "typeOfLevel": "heightAboveGround",
                "level": 10, "stepType": "max", "stepRange": "11-12",
                "startStep": 11, "endStep": 12, "stepUnits": 1, "units": "m s**-1",
            },
            {
                "shortName": "tp", "paramId": 228, "typeOfLevel": "surface",
                "level": 0, "stepType": "accum", "stepRange": "0-12",
                "startStep": 0, "endStep": 12, "stepUnits": 1, "units": "m",
            },
            {
                "shortName": "ssrd", "paramId": 169, "typeOfLevel": "surface",
                "level": 0, "stepType": "accum", "stepRange": "0-12",
                "startStep": 0, "endStep": 12, "stepUnits": 1, "units": "J m**-2",
            },
        ]
        got = ecmwf_registry.values_by_lead(
            nearest, metadata, [12], source_sha256="a" * 64
        )[12]
        self.assertEqual(got["10u"][0]["semantic_id"], "wind_u_10m")
        self.assertEqual(got["10fg"][0]["semantic_id"], "wind_gust_10m")
        self.assertEqual(got["10fg"][0]["stepType"], "max")
        self.assertEqual(got["10fg"][0]["startStep"], 11)
        self.assertEqual(got["tp"][0]["stepType"], "accum")
        self.assertEqual(got["tp"][0]["stepRange"], "0-12")
        self.assertEqual(got["ssrd"][0]["units"], "J m**-2")
        self.assertTrue(all(
            item[0]["availability_status"] == "received"
            for item in got.values()
        ))

    def test_unsupported_surface_rh_and_cin_are_explicit_registry_declarations(self):
        declarations = ecmwf_registry.unsupported_declarations()
        by_semantic = {x["semantic_id"]: x for x in declarations}
        self.assertEqual(
            set(by_semantic), {"relative_humidity_2m", "cin"}
        )
        self.assertEqual(
            by_semantic["relative_humidity_2m"]["availability_status"],
            "unsupported_by_provider_or_product",
        )
        self.assertEqual(by_semantic["relative_humidity_2m"]["parameter_native"], "2r")
        self.assertEqual(by_semantic["cin"]["parameter_native"], "cin")

    def test_final_availability_normalizer_preserves_explicit_registry_semantics(self):
        row = {
            "model": "ECMWF-IFS",
            "run_time_utc": "2026-09-26T18:00:00+00:00",
            "valid_time_utc": "2026-09-27T06:00:00+00:00",
            "forecast_lead_hours": 12,
            "provider_product": ecmwf_registry.PRODUCT,
            "values": {
                "2t": [{
                    "value": 281.0,
                    "semantic_id": "air_temperature_2m",
                    "availability_status": "received",
                }]
            },
            "field_availability_states": ecmwf_registry.unsupported_declarations(),
        }
        availability.stamp_rows(
            [row], observed_at="2026-09-27T00:00:00+00:00", replace_row_time=True
        )
        by_semantic = {
            x["semantic_id"]: x for x in row["field_availability_states"]
        }
        self.assertIn("relative_humidity_2m", by_semantic)
        self.assertIn("cin", by_semantic)
        self.assertEqual(
            by_semantic["relative_humidity_2m"]["availability_status"],
            "unsupported_by_provider_or_product",
        )
        self.assertEqual(
            row["values"]["2t"][0]["semantic_id"], "air_temperature_2m"
        )


if __name__ == "__main__":
    unittest.main()
