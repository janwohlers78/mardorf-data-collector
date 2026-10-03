import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tests")]
from test_wp13_collector_v1 import collector, observation_case
from mardorf_collector.wp13.core_v1 import (
    CollectorError,
    Response,
    canonical,
    stamp,
    utc,
)
from mardorf_collector.wp13.history_v1 import (
    SourceCacheV1,
    history_manifest,
    run_history,
    legacy_observation_view,
)
from mardorf_collector.wp13.store_v1 import write_delivery, read_delivery


def history_response(source, job, limit, *, empty=False, status=200):
    body = json.loads(observation_case("observation_historic")[1].payload)
    if empty:
        body["sensors"][0]["data"] = []
    else:
        body["sensors"][0]["data"][0]["ts"] = int(
            utc(job["window"]["start_utc"]).timestamp()
        )
    return Response(canonical(body), "2026-10-03T02:00:00Z", status)


class HistoryTests(unittest.TestCase):
    def setup_run(self, root, transport):
        j, _ = observation_case("observation_historic")
        m = history_manifest(
            j,
            start_utc="2026-09-18T00:00:00Z",
            end_utc="2026-09-21T00:00:00Z",
            chunk_seconds=21600,
        )
        return m, dict(
            cache=SourceCacheV1(Path(root) / "cache"),
            delivery_root=Path(root) / "deliveries",
            manifest_path=Path(root) / "history.json",
            transport=transport,
        )

    def test_eight_requests_budget_resume_and_raw_reuse(self):
        calls = []

        def transport(s, j, l):
            calls.append(j["window"])
            return history_response(s, j, l)

        with tempfile.TemporaryDirectory() as root:
            m, kwargs = self.setup_run(root, transport)
            first = run_history(m, collector(), **kwargs)
            self.assertEqual(first["provider_requests"], 8)
            self.assertEqual(len(first["manifest"]["completed"]), 8)
            self.assertEqual(first["manifest"]["status"], "paused_budget")
            restart = json.loads((Path(root) / "history.json").read_text())
            second = run_history(restart, collector(), **kwargs)
            self.assertEqual(second["provider_requests"], 4)
            self.assertEqual(second["manifest"]["status"], "complete")
            replay = run_history(second["manifest"], collector(), **kwargs)
            self.assertEqual(replay["provider_requests"], 0)
            self.assertEqual(len(calls), 12)
            # Replay the original plan uses existing raw receipts, no provider calls.
            before = len(calls)
            replay = run_history(m, collector(), **kwargs)
            self.assertEqual(len(calls), before)

    def test_empty_succeeded_and_failed_distinct_cursor(self):
        with tempfile.TemporaryDirectory() as root:
            m, kwargs = self.setup_run(
                root, lambda s, j, l: history_response(s, j, l, empty=True)
            )
            m["plan"]["end_utc"] = "2026-09-18T06:00:00Z"
            from mardorf_collector.wp13.core_v1 import digest

            m["plan_id"] = digest(m["plan"])
            result = run_history(m, collector(), **kwargs)
            self.assertEqual(result["manifest"]["status"], "complete")
            self.assertEqual(result["manifest"]["completed"][0]["status"], "empty")
            receipt = result["manifest"]["completed"][0]["receipt_id"]
            stored = read_delivery(kwargs["delivery_root"], receipt)
            self.assertTrue(stored["raw_bytes"])
        with tempfile.TemporaryDirectory() as root:
            m, kwargs = self.setup_run(
                root, lambda s, j, l: history_response(s, j, l, status=503)
            )
            result = run_history(m, collector(), **kwargs)
            self.assertEqual(result["manifest"]["cursor_utc"], m["cursor_utc"])
            self.assertEqual(result["manifest"]["status"], "failed_retryable")
            for _ in range(3):
                result = run_history(result["manifest"], collector(), **kwargs)
            self.assertEqual(result["manifest"]["status"], "retry_exhausted")

    def test_cached_raw_shared_and_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            cache = SourceCacheV1(root)
            j, r = observation_case("observation_historic")
            s = j["sources"][0]
            cache.put(s, j, r)
            self.assertEqual(cache.get(s, j, r) if False else cache.get(s, j), r)
            p = next((Path(root) / "objects").iterdir())
            p.write_bytes(b"{}")
            with self.assertRaises(CollectorError):
                cache.get(s, j)

    def test_legacy_current_and_history_equal_existing_adapter(self):
        from mardorf_collector.providers.fetch_svg_weatherlink import (
            normalize_current,
            normalize_history,
        )

        for kind in ["observation_current", "observation_historic"]:
            j, r = observation_case(kind)
            result = collector().collect(j, responses=[r])
            view = legacy_observation_view(result)
            body = json.loads(r.payload)
            if kind.endswith("current"):
                self.assertEqual(view["latest_observation"], normalize_current(body))
            else:
                self.assertEqual(
                    view["recent_historic_observations"], normalize_history(body)
                )

    def test_history_window_limits_and_no_implicit_job_defaults(self):
        j, _ = observation_case("observation_historic")
        for kwargs in [dict(chunk_seconds=86401), dict(max_attempts=4)]:
            with self.assertRaises(CollectorError):
                history_manifest(
                    j,
                    start_utc="2026-09-18T00:00:00Z",
                    end_utc="2026-09-19T00:00:00Z",
                    **kwargs
                )


if __name__ == "__main__":
    unittest.main()
