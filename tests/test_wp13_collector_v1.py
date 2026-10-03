import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from mardorf_collector.wp13.core_v1 import (
    ContractsV1,
    CollectorV1,
    CollectorError,
    Response,
    canonical,
    identify,
)
from mardorf_collector.wp13.store_v1 import write_delivery, read_delivery
from mardorf_collector.wp13.adapters_v1 import eccodes_decode


def job(kind="forecast_grib", provider="dwd-icon-d2-v1", *, contracts=None):
    c = contracts or ContractsV1()
    p = c.tables["providers"][provider]
    url = p["endpoint"].rstrip("/")
    if kind.startswith("observation"):
        url += "/" + ("historic" if kind.endswith("historic") else "current") + "/42374"
    elif provider == "dwd-icon-d2-v1":
        url += "/grib/00/t_2m/icon-d2_germany_regular-lat-lon_single-level_2026100300_001_2d_t_2m.grib2"
    else:
        url += "/fixture.grib2"
    binding = c.forecast.get(provider, {})
    source = {
        "url": url,
        "compression": "none",
        "media_type": (
            "application/json"
            if kind.startswith("observation")
            else "application/x-grib"
        ),
        "native_parameter": (
            None
            if kind.startswith("observation")
            else "t_2m" if provider == "dwd-icon-d2-v1" else "TMP"
        ),
        "model_id_native": binding.get("model_id"),
        "product_id_native": binding.get("product_id"),
        "expected_run_time_utc": (
            None if kind.startswith("observation") else "2026-10-03T00:00:00Z"
        ),
        "role": "data",
    }
    return identify(
        {
            "schema_version": 1,
            "artifact_version": "dev03-wp13-job-v1",
            "profile_id": "svg-general-weather-development-v2",
            "site_ids": ["steinhude-svg-v1"],
            "provider_binding_id": provider,
            "kind": kind,
            "context": "historical" if kind.endswith("historic") else "prospective",
            "station_id": "SVG-42374-v1" if kind.startswith("observation") else None,
            "sensor_id": (
                "svg-weatherlink-48-v1" if kind.startswith("observation") else None
            ),
            "window": (
                {"start_utc": "2026-09-19T00:00:00Z", "end_utc": "2026-09-20T00:00:00Z"}
                if kind.endswith("historic")
                else None
            ),
            "sources": [source],
            "target_ids": [],
            "refresh_id": None,
        },
        "job_id",
    )


def fake_decoder(raw, lat, lon):
    count = 31 if raw == b"GRIB-EPS" else 1
    return [
        dict(
            shortName="2t",
            units="K",
            typeOfLevel="heightAboveGround",
            level=2,
            stepType="instant",
            startStep=1,
            endStep=1,
            stepUnits=1,
            dataDate=20261003,
            dataTime=0,
            validityDate=20261003,
            validityTime=100,
            md5GridSection="synthetic-grid-1",
            latitude=round(lat * 4) / 4,
            longitude=round(lon * 4) / 4,
            value=280 + i / 10,
            **({"perturbationNumber": i} if count > 1 else {}),
        )
        for i in range(count)
    ]


def collector(contracts=None, decoder=fake_decoder):
    return CollectorV1(
        contracts,
        grib_decoder=decoder,
        collector_commit_sha="c4ebc0b1a5820f9119bdc7613a616ef134dee7c8",
    )


def observation_case(kind="observation_current"):
    name = "svg_historic" if kind.endswith("historic") else "svg_current"
    return job(kind, "weatherlink-svg-v1"), Response(
        (ROOT / f"tests/fixtures/wp13/{name}_v1.json").read_bytes(),
        "2026-10-03T02:00:00Z",
    )


def openmeteo_case():
    c = ContractsV1()
    j = job("forecast_openmeteo", "openmeteo-icon-d2-eps-v1")
    template = j["sources"][0]
    j["sources"] = [
        dict(
            template,
            url=c.openmeteo["metadata_url"],
            role="metadata_before",
            media_type="application/json",
        ),
        dict(
            template,
            url="https://ensemble-api.open-meteo.com/v1/ensemble?models=dwd_icon_d2_eps",
            role="data",
            media_type="application/json",
        ),
        dict(
            template,
            url=c.openmeteo["metadata_url"],
            role="metadata_after",
            media_type="application/json",
        ),
    ]
    j = identify(j, "job_id")
    meta = {
        "last_run_initialisation_time": 1790985600,
        "last_run_availability_time": 1790986200,
        "last_run_modification_time": 1790986200,
    }
    # Use UTC epochs computed from the fixture's declared native model clocks.
    from datetime import datetime, timezone

    meta["last_run_initialisation_time"] = int(
        datetime(2026, 10, 3, tzinfo=timezone.utc).timestamp()
    )
    meta["last_run_availability_time"] = meta["last_run_initialisation_time"] + 600
    meta["last_run_modification_time"] = meta["last_run_availability_time"]
    body = {
        "latitude": 52.5,
        "longitude": 9.5,
        "utc_offset_seconds": 0,
        "hourly": {
            "time": ["2026-10-03T01:00"],
            **{f"wind_speed_10m_member{i}": [3 + i / 10] for i in range(20)},
        },
        "hourly_units": {f"wind_speed_10m_member{i}": "m/s" for i in range(20)},
    }
    responses = [
        Response(canonical(meta), "2026-10-03T00:19:00Z"),
        Response(canonical(body), "2026-10-03T00:20:00Z"),
        Response(canonical(meta), "2026-10-03T00:21:00Z"),
    ]
    return j, responses


class CollectorTests(unittest.TestCase):
    def test_native_deterministic_and_complete_eps_raw_preserved(self):
        for provider, raw, count in [
            ("dwd-icon-d2-v1", b"GRIB-DWD", 1),
            ("noaa-gefs-v1", b"GRIB-EPS", 31),
        ]:
            result = collector().collect(
                job(provider=provider),
                responses=[Response(raw, "2026-10-03T00:20:00Z")],
            )
            self.assertEqual(result["status"], "received")
            self.assertEqual(len(result["fields"]), count)
            self.assertEqual(result["raw_bytes"]["raw-0"], raw)
            self.assertEqual(
                result["envelope"]["field_fragments"][0]["row_count"], count
            )
            self.assertTrue(
                all(f["source"]["native_metadata_object_id"] for f in result["fields"])
            )

    def test_current_and_historic_all_fields_and_unknown_height(self):
        for kind in ["observation_current", "observation_historic"]:
            j, response = observation_case(kind)
            result = collector().collect(j, responses=[response])
            self.assertEqual(result["status"], "received")
            self.assertGreater(len(result["fields"]), 4)
            self.assertEqual(result["raw_bytes"]["raw-0"], response.payload)
            self.assertTrue(
                all(
                    f["level"]["measurement_height_status"] == "unknown"
                    for f in result["fields"]
                )
            )
            self.assertIn(
                "raw_only", {s["status"] for s in result["source_status"][0]["fields"]}
            )

    def test_two_sites_one_download_no_coordinate_relabel(self):
        registry = json.loads(
            (ROOT / "tests/fixtures/wp13/domain_two_sites_v1.json").read_text()
        )
        c = ContractsV1(registry=registry)
        j = job(contracts=c)
        j.update(
            profile_id="two-site-fixture-v1",
            site_ids=["steinhude-svg-v1", "test-site-west-v1"],
        )
        j = identify(j, "job_id")
        calls = []

        def transport(source, job, limit):
            calls.append(source["url"])
            return Response(b"GRIB-DWD", "2026-10-03T00:20:00Z")

        result = CollectorV1(
            c,
            transport=transport,
            grib_decoder=fake_decoder,
            collector_commit_sha="c4ebc0b1a5820f9119bdc7613a616ef134dee7c8",
        ).collect(j)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(result["fields"]), 2)
        points = [f["spatial_support"] for f in result["fields"]]
        self.assertNotEqual(points[0]["actual_latitude"], points[1]["actual_latitude"])
        self.assertNotEqual(
            points[0]["actual_latitude"],
            c.tables["sites"][j["site_ids"][0]]["geometry"]["coordinates"][1],
        )

    def test_failed_and_incomplete_ensemble_no_transfer_envelope(self):
        def incomplete(raw, lat, lon):
            return fake_decoder(b"GRIB-EPS", lat, lon)[:-1]

        result = collector(decoder=incomplete).collect(
            job(provider="noaa-gefs-v1"),
            responses=[Response(b"GRIB-EPS", "2026-10-03T00:20:00Z")],
        )
        self.assertEqual(result["status"], "failed")
        self.assertIsNone(result["envelope"])
        j, response = observation_case()
        result = collector().collect(
            j, responses=[Response(response.payload, response.observed_at_utc, 503)]
        )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["raw_bytes"]["raw-0"], response.payload)

    def test_immutable_prepared_delivery_and_replay(self):
        j, response = observation_case()
        result = collector().collect(j, responses=[response])
        with tempfile.TemporaryDirectory() as root:
            first = write_delivery(root, result)
            self.assertEqual(write_delivery(root, result)["added_immutable_bytes"], 0)
            replay = read_delivery(root, first["receipt_id"])
            self.assertEqual(replay["raw_bytes"], result["raw_bytes"])
            p = Path(root) / first["receipt_id"] / "receipt.json"
            p.write_bytes(b"{}")
            with self.assertRaises(CollectorError):
                write_delivery(root, result)

    def test_contradictory_native_run_and_station_rejected(self):
        j = job()
        j["sources"][0]["expected_run_time_utc"] = "2026-10-02T00:00:00Z"
        j = identify(j, "job_id")
        self.assertEqual(
            collector().collect(
                j, responses=[Response(b"GRIB-DWD", "2026-10-03T00:20:00Z")]
            )["status"],
            "failed",
        )
        j, response = observation_case()
        body = json.loads(response.payload)
        body["station_id"] = 99999
        self.assertEqual(
            collector().collect(
                j, responses=[Response(canonical(body), response.observed_at_utc)]
            )["status"],
            "failed",
        )

    def test_openmeteo_stable_metadata_exact_member_identity(self):
        j, responses = openmeteo_case()
        result = collector().collect(j, responses=responses)
        self.assertEqual(result["status"], "received")
        self.assertEqual(len(result["fields"]), 20)
        self.assertTrue(
            all(
                f["member"]["member_role"] == "ensemble_member"
                for f in result["fields"]
            )
        )
        self.assertEqual(
            result["fields"][0]["time"]["run_time_utc"], "2026-10-03T00:00:00Z"
        )
        changed = json.loads(responses[-1].payload)
        changed["last_run_modification_time"] += 1
        responses[-1] = Response(canonical(changed), responses[-1].observed_at_utc)
        failed = collector().collect(j, responses=responses)
        self.assertEqual(failed["status"], "failed")
        self.assertIsNone(failed["envelope"])

    def test_exact_job_identity_unknown_dimensions_and_no_secrets(self):
        for update in [{"site_ids": ["unregistered"]}, {"extra": 1}]:
            j = identify(dict(job(), **update), "job_id")
            with self.assertRaises(CollectorError):
                collector().collect(j, responses=[])
        j = job()
        j["sources"][0]["url"] += "?api-key=fixture-sensitive"
        j = identify(j, "job_id")
        with self.assertRaises(CollectorError):
            collector().collect(j, responses=[])

    def test_actual_eccodes_native_fixture_when_dependency_installed(self):
        try:
            import eccodes
        except ImportError:
            self.skipTest("optional pinned WP13 GRIB adapter environment required")
        manifest = json.loads(
            (ROOT / "tests/fixtures/wp13/fixture_manifest_v1.json").read_text()
        )
        raw = (ROOT / "tests/fixtures/wp13/native_weather_v1.grib2").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), manifest["sha256"])
        for provider, payload, count in [
            ("dwd-icon-d2-v1", raw[: manifest["deterministic_bytes"]], 1),
            ("noaa-gefs-v1", raw[manifest["deterministic_bytes"] :], 31),
        ]:
            result = collector(decoder=eccodes_decode).collect(
                job(provider=provider),
                responses=[Response(payload, "2026-10-03T00:20:00Z")],
            )
            self.assertEqual(result["status"], "received")
            self.assertEqual(len(result["fields"]), count)


if __name__ == "__main__":
    unittest.main()
