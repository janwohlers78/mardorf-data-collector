"""Synthetic header counterexamples for a separately evidenced real DWD alias."""

from copy import deepcopy
import unittest
from test_wp13_collector_v1 import collector, fake_decoder, job
from mardorf_collector.wp13.core_v1 import Response
from mardorf_collector.wp13.collector_v2 import CollectorV2


class AliasTests(unittest.TestCase):
    def decoder(self, raw, lat, lon):
        rows = fake_decoder(raw, lat, lon)
        rows[0].update(
            shortName="max_i10fg",
            name="Time-maximum 10 metre wind gust",
            paramId=237318,
            units="m s**-1",
            typeOfLevel="heightAboveGround",
            level=10,
            stepType="max",
            startStep=0,
            endStep=1,
        )
        return rows

    def result(self, decoder=None):
        j = job()
        j["sources"][0]["native_parameter"] = "vmax_10m"
        j["sources"][0]["url"] = j["sources"][0]["url"].replace("t_2m", "vmax_10m")
        from mardorf_collector.wp13.core_v1 import identify

        j = identify(j, "job_id")
        return CollectorV2(
            grib_decoder=decoder or self.decoder, collector_commit_sha="1" * 40
        ).collect(
            j, responses=[Response(b"GRIB-SYNTHETIC-GUST", "2026-10-03T00:20:00Z")]
        )

    def test_quantity_alias_keeps_original_native_header_and_units(self):
        result = self.result()
        self.assertEqual(result["status"], "received")
        field = result["fields"][0]
        self.assertEqual(field["domain"]["quantity_id"], "wind_gust")
        self.assertEqual(
            field["extensions"]["wp13:original-grib-header:v1"]["shortName"],
            "max_i10fg",
        )
        self.assertEqual(field["unit_native"], "m s**-1")
        self.assertEqual(field["time"]["operator"], "maximum")
        self.assertIn("wp15:native-alias:v1", field["extensions"])

    def test_alias_contradictory_units_height_and_parameter_rejected(self):
        for key, value in [
            ("units", "K"),
            ("level", 100),
            ("paramId", 0),
            ("stepType", "instant"),
        ]:

            def broken(raw, lat, lon):
                rows = self.decoder(raw, lat, lon)
                rows[0][key] = value
                return rows

            with self.subTest(key=key):
                result = self.result(broken)
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["envelope"])
                self.assertTrue(result["raw_bytes"])

    def test_existing_native_projection_unchanged(self):
        j = job()
        response = Response(b"GRIB-DWD", "2026-10-03T00:20:00Z")
        a = collector().collect(j, responses=[response])
        b = CollectorV2(
            grib_decoder=fake_decoder,
            collector_commit_sha="c4eb51b475299fe4b2e99dfb3df4b881ef09d4e6",
        ).collect(j, responses=[response])
        self.assertEqual(a["fields"], b["fields"])
        self.assertEqual(a["raw_bytes"], b["raw_bytes"])
        self.assertEqual(a["fragment_bytes"], b["fragment_bytes"])


if __name__ == "__main__":
    unittest.main()
