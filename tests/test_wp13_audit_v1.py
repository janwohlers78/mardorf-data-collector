"""Adversarial WP13 contract regressions from AUD-20261003-02."""

import copy
import fcntl
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from test_wp13_collector_v1 import (
    collector,
    job,
    fake_decoder,
    observation_case,
    openmeteo_case,
)
from test_wp13_history_v1 import history_response
from mardorf_collector.wp13.core_v1 import (
    CollectorV1,
    CollectorError,
    Response,
    canonical,
    identify,
)
from mardorf_collector.wp13.history_v1 import (
    SourceCacheV1,
    history_manifest,
    run_history,
)
from mardorf_collector.wp13.store_v1 import write_delivery, read_delivery


class AuditTests(unittest.TestCase):
    def test_f01_registered_model_product_and_official_path(self):
        for change in [
            {"model_id_native": "ICON-EU"},
            {"product_id_native": "icon-eu_regular-lat-lon"},
            {
                "url": "https://opendata.dwd.de/weather/nwp/icon-eu/grib/icon-eu_test.grib2"
            },
        ]:
            j = job()
            j["sources"][0].update(change)
            j = identify(j, "job_id")
            with self.assertRaises(CollectorError):
                collector().collect(j, responses=[])

    def test_f02_member_point_and_unit_must_match(self):
        for key, value in [("latitude", 53.5), ("units", "degC")]:

            def decoder(raw, lat, lon):
                rows = fake_decoder(b"GRIB-EPS", lat, lon)
                rows[0][key] = value
                return rows

            r = collector(decoder=decoder).collect(
                job(provider="noaa-gefs-v1"),
                responses=[Response(b"GRIB-EPS", "2026-10-03T00:20:00Z")],
            )
            self.assertEqual(r["status"], "failed")
            self.assertIsNone(r["envelope"])

    def test_f03_inactive_writer_cannot_enter_network(self):
        with patch("requests.Session") as session:
            result = CollectorV1().collect(job())
            session.assert_not_called()
            self.assertEqual(result["status"], "failed")

    def history_setup(self, root):
        j, _ = observation_case("observation_historic")
        m = history_manifest(
            j, start_utc="2026-09-18T00:00:00Z", end_utc="2026-09-19T00:00:00Z"
        )
        return m, dict(
            cache=SourceCacheV1(Path(root) / "cache"),
            delivery_root=Path(root) / "delivery",
            manifest_path=Path(root) / "history.json",
            transport=history_response,
        )

    def test_f04_cursor_requires_contiguous_readback_proof(self):
        with tempfile.TemporaryDirectory() as root:
            m, kw = self.history_setup(root)
            bad = copy.deepcopy(m)
            bad["cursor_utc"] = m["plan"]["end_utc"]
            with self.assertRaises(CollectorError):
                run_history(bad, collector(), **kw)
            done = run_history(m, collector(), **kw)["manifest"]
            done["completed"][0]["status"] = "empty"
            with self.assertRaises(CollectorError):
                run_history(done, collector(), **kw)

    def test_f05_transport_exception_keeps_cursor_and_attempts(self):
        def fail(*args):
            raise TimeoutError("fixture sensitive diagnostic")

        with tempfile.TemporaryDirectory() as root:
            m, kw = self.history_setup(root)
            kw["transport"] = fail
            for n in range(1, 4):
                m = run_history(m, collector(), **kw)["manifest"]
                self.assertEqual(list(m["attempts"].values()), [n])
                self.assertEqual(m["cursor_utc"], m["plan"]["start_utc"])
            m = run_history(m, collector(), **kw)["manifest"]
            self.assertEqual(m["status"], "retry_exhausted")
            self.assertNotIn("sensitive", (Path(root) / "history.json").read_text())

    def test_f06_derived_objects_and_fragment_budgets(self):
        j = job()
        j["sources"] *= 33
        j = identify(j, "job_id")
        with self.assertRaises(CollectorError):
            collector().collect(
                j, responses=[Response(b"GRIB-DWD", "2026-10-03T00:20:00Z")] * 33
            )
        c = collector()
        c.contracts.limits["max_fragment_bytes"] = 1
        with self.assertRaises(CollectorError):
            c.collect(job(), responses=[Response(b"GRIB-DWD", "2026-10-03T00:20:00Z")])

    def test_metadata_captures_and_native_point_clock(self):
        j, responses = openmeteo_case()
        responses[-1] = Response(responses[-1].payload, "2026-10-03T00:18:00Z")
        self.assertEqual(
            collector().collect(j, responses=responses)["status"], "failed"
        )

        def decoder(raw, lat, lon):
            rows = fake_decoder(raw, lat, lon)
            rows[0]["endStep"] = 2
            return rows

        self.assertEqual(
            collector(decoder=decoder).collect(
                job(), responses=[Response(b"GRIB-DWD", "2026-10-03T00:20:00Z")]
            )["status"],
            "failed",
        )

    def test_file_names_receipt_identity_and_bounded_lock(self):
        with tempfile.TemporaryDirectory() as root:
            j, r = observation_case()
            result = collector().collect(j, responses=[r])
            receipt = write_delivery(root, result)["receipt_id"]
            with self.assertRaises(CollectorError):
                read_delivery(root, "../outside")
            with open(Path(root) / (receipt + ".lock"), "rb") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                with self.assertRaises(CollectorError):
                    write_delivery(root, result, timeout_seconds=0.02)
            folder = Path(root) / receipt
            manifest = json.loads((folder / "manifest.json").read_text())
            manifest["files"]["../outside"] = {"bytes": 0, "sha256": "0" * 64}
            (folder / "manifest.json").write_bytes(canonical(manifest))
            with self.assertRaises(CollectorError):
                read_delivery(root, receipt)


if __name__ == "__main__":
    unittest.main()
