import hashlib
import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from dev03_v3_parent_binding import (
    Dev03ParentBindingError,
    build_bundle_v3_shell,
    build_parent_cycle_inventory,
    load_registry_v3,
)
from dev03_v3_convective_acquisition import (
    BudgetLedger,
    Dev03I04Error,
    acquire_convective_successor,
    load_plan,
    plan_parent_pinned_requests,
    _dwd_convective_identity_matches,
)


def h(ch):
    return ch * 64


class FakeResponse:
    def __init__(self, status_code=200, content=b"GRIB", content_length=True):
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Length": str(len(content))} if content_length else {}
        self.closed = False

    def iter_content(self, chunk_size=65536):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i:i + chunk_size]

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, response=None):
        self.response = response or FakeResponse()
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class Dev03Wp03I04Tests(unittest.TestCase):
    def setUp(self):
        self.plan = load_plan()
        self.registry = load_registry_v3()

    def fixture(self, model="GFS", product="gfs_0p25", lead=3):
        row = {
            "model": model,
            "run_time_utc": "2026-09-28T12:00:00+00:00",
            "valid_time_utc": f"2026-09-28T{12 + lead:02d}:00:00+00:00",
            "forecast_lead_hours": lead,
            "forecast_coordinate_or_grid_point": {
                "latitude": 52.5,
                "longitude": 9.3,
                "selection": "ecCodes_nearest_grid_point",
            },
        }
        if product is not None:
            row["provider_product"] = product
        if model == "ICON-D2":
            row["source_urls"] = [
                f"https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/u_10m/icon-d2_germany_regular-lat-lon_single-level_2026092812_{lead:03d}_2d_u_10m.grib2.bz2"
            ]
        elif model == "ICON-EU":
            row["source_urls"] = [
                f"https://opendata.dwd.de/weather/nwp/icon-eu/grib/12/u_10m/icon-eu_europe_regular-lat-lon_single-level_2026092812_{lead:03d}_0_u_10m.grib2.bz2"
            ]
        payload = {"models": {model: [row]}}
        raw = (json.dumps(payload, separators=(",", ":")) + "\n").encode()
        binding = {
            "parent_v2_payload_sha256": hashlib.sha256(raw).hexdigest(),
            "parent_v2_collector_generated_at_utc": "2026-09-28T12:10:00+00:00",
            "parent_v2_transfer_receipt_path": "data/inbox/public_collector/transfer_receipts/models/2026/09/28/receipt_parent.json",
            "parent_v2_transfer_receipt_sha256": h("a"),
            "parent_v2_verified_data_commit_sha": h("b"),
        }
        _, cycle_id = build_parent_cycle_inventory(payload, self.registry)
        shell = build_bundle_v3_shell(
            parent_binding=binding,
            parent_cycle_binding_id=cycle_id,
            v3_generated_at_utc="2026-09-28T12:11:00+00:00",
            attempt_nonce="11111111-1111-4111-8111-111111111111",
        )
        return raw, binding, shell

    def budget(self, **changes):
        state = {
            "utc_day": "2026-09-28",
            "requests_used": 0,
            "retry_requests_used": 0,
            "response_bytes_used": 0,
            "matched_v2_response_bytes": 100000,
        }
        state.update(changes)
        return state

    def test_gfs_request_is_exact_parent_cycle_acpcp_only(self):
        raw, binding, _ = self.fixture()
        jobs, _ = plan_parent_pinned_requests(raw, binding, plan=self.plan, registry=self.registry)
        self.assertEqual(len(jobs), 1)
        url = jobs[0]["source_url"]
        self.assertIn("gfs.t12z.pgrb2.0p25.f003", url)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["dir"], ["/gfs.20260928/12/atmos"])
        self.assertEqual(query["var_ACPCP"], ["on"])
        self.assertNotIn("var_CPRAT", query)
        self.assertEqual(jobs[0]["parameter_native"], "ACPCP")

    def test_dwd_requests_use_bound_cycle_without_directory_discovery(self):
        for model, product, token in (
            ("ICON-D2", "icon-d2_regular-lat-lon", "2026092812_003_2d_rain_con"),
            ("ICON-EU", "icon-eu_regular-lat-lon", "2026092812_003_RAIN_CON"),
        ):
            with self.subTest(model=model):
                raw, binding, _ = self.fixture(model=model, product=product)
                jobs, _ = plan_parent_pinned_requests(
                    raw, binding, plan=self.plan, registry=self.registry
                )
                self.assertEqual(len(jobs), 1)
                self.assertIn(token, jobs[0]["source_url"])
                self.assertIn("/rain_con/", jobs[0]["source_url"])

    def test_dwd_convective_identity_uses_grib2_numeric_identity_not_shortname(self):
        raw, binding, _ = self.fixture(model="ICON-D2", product="icon-d2_regular-lat-lon")
        jobs, _ = plan_parent_pinned_requests(raw, binding, plan=self.plan, registry=self.registry)
        job=jobs[0]
        self.assertTrue(_dwd_convective_identity_matches({
            "shortName":"unknown",
            "discipline":"0",
            "parameterCategory":"1",
            "parameterNumber":"76",
        },job))
        self.assertFalse(_dwd_convective_identity_matches({
            "shortName":"rain_con",
            "discipline":"0",
            "parameterCategory":"1",
            "parameterNumber":"77",
        },job))

    def test_frozen_dwd_parent_without_provider_product_uses_exact_source_evidence(self):
        for model, token in (
            ("ICON-D2", "2026092812_003_2d_rain_con"),
            ("ICON-EU", "2026092812_003_RAIN_CON"),
        ):
            with self.subTest(model=model):
                raw, binding, _ = self.fixture(model=model, product=None)
                jobs, _ = plan_parent_pinned_requests(
                    raw, binding, plan=self.plan, registry=self.registry
                )
                self.assertEqual(len(jobs), 1)
                self.assertIn(token, jobs[0]["source_url"])

    def test_tampered_parent_payload_fails_before_network(self):
        raw, binding, shell = self.fixture()
        session = FakeSession()
        with self.assertRaises(Dev03ParentBindingError):
            acquire_convective_successor(
                parent_payload_bytes=raw + b" ",
                parent_binding=binding,
                bundle_shell=shell,
                budget_state=self.budget(),
                session=session,
                observed_at_utc="2026-09-28T12:12:00+00:00",
            )
        self.assertEqual(session.calls, [])

    def test_parent_provider_product_is_never_inferred(self):
        raw, binding, _ = self.fixture(product="wrong-product")
        with self.assertRaises(Dev03I04Error):
            plan_parent_pinned_requests(raw, binding, plan=self.plan, registry=self.registry)

    def test_request_budget_blocks_before_provider_call(self):
        raw, binding, shell = self.fixture()
        session = FakeSession()
        out, state = acquire_convective_successor(
            parent_payload_bytes=raw,
            parent_binding=binding,
            bundle_shell=shell,
            budget_state=self.budget(requests_used=500),
            session=session,
            observed_at_utc="2026-09-28T12:12:00+00:00",
        )
        self.assertEqual(session.calls, [])
        self.assertEqual(out["network_requests_performed"], 0)
        self.assertEqual(out["status"], "v3_convective_acquisition_budget_blocked")
        self.assertEqual(out["i04_acquisition"]["deferred"][0]["status"], "request_deferred_budget_exhausted")
        self.assertEqual(state["requests_used"], 500)

    def test_retry_is_new_counted_attempt_not_hidden_http_retry(self):
        raw, binding, shell = self.fixture()
        session = FakeSession(FakeResponse(200, b"GRIBtiny"))
        with patch("dev03_v3_convective_acquisition._received_evidence", return_value=[{"evidence": "ok"}]):
            out, state = acquire_convective_successor(
                parent_payload_bytes=raw,
                parent_binding=binding,
                bundle_shell=shell,
                budget_state=self.budget(),
                attempt_kind="retry",
                session=session,
                observed_at_utc="2026-09-28T12:12:00+00:00",
            )
        self.assertEqual(len(session.calls), 1)
        self.assertFalse(session.calls[0][1]["allow_redirects"])
        self.assertEqual(out["network_requests_performed"], 1)
        self.assertEqual(out["i04_acquisition"]["hidden_http_retries_performed"], 0)
        self.assertEqual(state["retry_requests_used"], 1)

    def test_response_byte_hard_breach_is_recorded_and_stops_activation(self):
        raw, binding, shell = self.fixture()
        session = FakeSession(FakeResponse(200, b"x" * 30))
        out, state = acquire_convective_successor(
            parent_payload_bytes=raw,
            parent_binding=binding,
            bundle_shell=shell,
            budget_state=self.budget(matched_v2_response_bytes=100),
            session=session,
            observed_at_utc="2026-09-28T12:12:00+00:00",
        )
        self.assertEqual(len(session.calls), 1)
        self.assertTrue(state["hard_breach"])
        self.assertEqual(state["response_byte_hard_max"], 25)
        self.assertEqual(state["response_bytes_used"], 0)
        self.assertEqual(out["status"], "v3_convective_acquisition_budget_blocked")
        self.assertEqual(out["provider_evidence"], [])
        self.assertEqual(out["i04_acquisition"]["deferred"][0]["status"], "request_deferred_budget_exhausted")
        self.assertIn(
            "Content-Length exceeds remaining",
            out["i04_acquisition"]["deferred"][0]["reason"],
        )

    def test_budget_requires_measured_matched_v2_baseline(self):
        with self.assertRaises(Dev03I04Error):
            BudgetLedger(self.budget(matched_v2_response_bytes=0), plan=self.plan)


if __name__ == "__main__":
    unittest.main()
