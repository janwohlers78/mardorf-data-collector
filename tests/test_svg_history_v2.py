import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mardorf_collector.providers import svg_history_v2 as history


class SvgHistoryTest(unittest.TestCase):
    def test_request_includes_midnight_within_provider_limit(self):
        start, end = history.request_bounds(date(2026, 4, 2))
        self.assertEqual(start.isoformat(), "2026-04-01T23:59:59+00:00")
        self.assertEqual(end.isoformat(), "2026-04-02T23:59:58+00:00")
        self.assertEqual((end - start).total_seconds(), 86399)

    def test_coverage_requires_values_and_all_twelve_timestamps(self):
        start = datetime(2026, 4, 2, tzinfo=timezone.utc)
        rows = [
            {"time_utc": (start + history.timedelta(minutes=5*i)).isoformat(),
             "wind_speed_ms": 2.0, "wind_gust_ms": 3.0}
            for i in range(288)
        ]
        metrics, _ = history.coverage(rows, start.date())
        self.assertEqual((metrics["unique_day_timestamps"],
                          metrics["complete_timestamp_hours"]), (288, 24))
        rows[0]["wind_gust_ms"] = None
        rows.pop()
        metrics, _ = history.coverage(rows, start.date())
        self.assertEqual(metrics["complete_timestamp_hours"], 22)
        self.assertEqual(len(metrics["missing_five_minute_timestamps"]), 1)

    def test_verified_existing_day_is_idempotent_and_historical(self):
        day = date(2026, 4, 2)
        payload = {"sensors": []}

        class Response:
            content = json.dumps(payload).encode()

        start = datetime(2026, 4, 2, tzinfo=timezone.utc)
        rows = [{"time_utc": (start + history.timedelta(minutes=5*i)).isoformat(),
                 "wind_speed_ms": 2.0, "wind_gust_ms": 3.0} for i in range(288)]
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(history, "get", return_value=(Response(), payload)) as fetch:
                with patch.object(history, "normalize_historic", return_value=rows):
                    first = history.fetch_day(Path(tmp), day, "key", "secret")
                    second = history.fetch_day(Path(tmp), day, "key", "secret")
            self.assertEqual(first["status"], "fetched")
            self.assertEqual(second["status"], "verified_existing")
            self.assertEqual(fetch.call_count, 1)
            manifest_path = next((Path(tmp)/"2026/04/02").glob("manifest_*.json"))
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["availability_evidence_type"],
                             "historical_retrieval_not_original_publication")
            self.assertEqual(manifest["coverage"]["unique_day_timestamps"], 288)


if __name__ == "__main__":
    unittest.main()
