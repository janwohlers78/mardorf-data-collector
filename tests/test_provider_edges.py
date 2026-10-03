import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
import contextlib
import io

import fetch_dwd_additional_models as dwd
import fetch_extra_models as extra
import fetch_model_data as base
import extend_model_horizon as ext
import mardorf_collector.providers.provider_fetch as provider

class ProviderEdgeTests(unittest.TestCase):
    def eps_metadata(self, age):
        return {'last_run_availability_time_utc':
                (datetime.now(timezone.utc)-timedelta(seconds=age)).isoformat()}

    def test_eps_short_publication_transition_is_polled_and_then_acquired(self):
        state={'elapsed':0.0}
        available=datetime.now(timezone.utc)-timedelta(seconds=449)
        def metadata():
            return {'last_run_availability_time_utc':(available-timedelta(seconds=state['elapsed'])).isoformat()}
        def wait(seconds):state['elapsed']+=seconds
        expected=([{'fixture':'row'}],{'fixture':'members'})
        with patch.object(dwd,'fetch_eps_metadata',side_effect=metadata) as meta, \
             patch.object(dwd,'fetch_icon_d2_eps_bundle',return_value=expected) as acquire, \
             patch.object(provider.time,'monotonic',side_effect=lambda:state['elapsed']), \
             patch.object(provider.time,'sleep',side_effect=wait),contextlib.redirect_stdout(io.StringIO()):
            self.assertIs(provider.fetch_eps_when_settled([0,48]),expected)
        self.assertGreaterEqual(state['elapsed'],150);self.assertLessEqual(state['elapsed'],151)
        self.assertGreater(meta.call_count,2);acquire.assert_called_once_with([0,48])

    def test_eps_unsettled_or_future_publication_beyond_budget_does_not_acquire(self):
        for age in (100,-1):
            with self.subTest(age=age),patch.object(dwd,'fetch_eps_metadata',return_value=self.eps_metadata(age)), \
                 patch.object(dwd,'fetch_icon_d2_eps_bundle') as acquire,patch.object(provider.time,'sleep') as sleep:
                with self.assertRaisesRegex(RuntimeError,'bounded wait'):provider.fetch_eps_when_settled([0])
                acquire.assert_not_called();sleep.assert_not_called()

    def test_eps_wait_deadline_is_not_reset_by_new_publication(self):
        state={'elapsed':0}
        metas=[self.eps_metadata(550),self.eps_metadata(449)]
        def wait(seconds):state['elapsed']+=seconds
        with patch.object(dwd,'fetch_eps_metadata',side_effect=metas), \
             patch.object(provider.time,'monotonic',side_effect=lambda:state['elapsed']), \
             patch.object(provider.time,'sleep',side_effect=wait), \
             patch.object(dwd,'fetch_icon_d2_eps_bundle') as acquire,contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError,'bounded wait'):provider.fetch_eps_when_settled([0])
        self.assertEqual(state['elapsed'],30);acquire.assert_not_called()

    def test_eps_mature_run_preserves_fetcher_failures_and_dispatch(self):
        with patch.object(dwd,'fetch_eps_metadata',return_value=self.eps_metadata(1200)), \
             patch.object(dwd,'fetch_icon_d2_eps_bundle',side_effect=RuntimeError('metadata changed')) as acquire, \
             patch.object(provider.time,'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError,'metadata changed'):provider.fetch_eps_when_settled([0])
            acquire.assert_called_once_with([0]);sleep.assert_not_called()
        with patch.object(provider,'fetch_eps_when_settled',return_value=([],{'fixture':True})) as acquire:
            data=provider.initial(False);provider.fetch_base(data,'ICON-D2-EPS',True)
            acquire.assert_called_once_with([0,12,24,36,48])
            self.assertEqual(data['ensemble_hourly_source'],{'fixture':True})

    def test_open_meteo_session_retries_transient_503_bounded(self):
        retry=dwd.S.get_adapter("https://").max_retries
        self.assertEqual(retry.total,3)
        self.assertEqual(retry.connect,3)
        self.assertEqual(retry.read,3)
        self.assertEqual(retry.status,3)
        self.assertIn(503,retry.status_forcelist)
        self.assertEqual(retry.allowed_methods,frozenset(["GET"]))

    def test_ecmwf_requests_both_gust_aliases(self):
        for params in (extra.ECMWF_PARAMS,ext.ECMWF_PARAMS):
            self.assertIn("10fg",params)
            self.assertIn("10fg3",params)
            self.assertIn("10u",params)
            self.assertIn("10v",params)

    def test_gfs_full_validation_binds_base_cycle_to_terminal_f384(self):
        values={
            "10u":[{"shortName":"10u","value":3.0}],
            "10v":[{"shortName":"10v","value":4.0}],
            "gust":[{"shortName":"gust","value":6.0}],
        }
        point={"latitude":52.5,"longitude":9.25,"selection":"ecCodes_nearest_grid_point"}
        with patch.dict("os.environ",{"FULL_VALIDATION":"true"}), \
             patch.object(base,"discover_gfs_cycle",return_value="2026092600") as discover, \
             patch.object(base,"get_grib",return_value=b"GRIBtest"), \
             patch.object(base,"assert_grib_valid_time"), \
             patch.object(base.noaa,"extract_native_values",return_value=(values,point)):
            out=base.fetch_gfs([0,48])
        discover.assert_called_once_with(384)
        self.assertEqual({x["forecast_lead_hours"] for x in out},{0,48})
        self.assertTrue(all(x["run_time_utc"]=="2026-09-26T00:00:00+00:00" for x in out))

    def test_gfs_normal_base_selection_keeps_requested_lead_probe(self):
        values={
            "10u":[{"shortName":"10u","value":3.0}],
            "10v":[{"shortName":"10v","value":4.0}],
            "gust":[{"shortName":"gust","value":6.0}],
        }
        point={"latitude":52.5,"longitude":9.25,"selection":"ecCodes_nearest_grid_point"}
        with patch.dict("os.environ",{"FULL_VALIDATION":"false"}), \
             patch.object(base,"discover_gfs_cycle",return_value="2026092606") as discover, \
             patch.object(base,"get_grib",return_value=b"GRIBtest"), \
             patch.object(base,"assert_grib_valid_time"), \
             patch.object(base.noaa,"extract_native_values",return_value=(values,point)):
            base.fetch_gfs([0,48])
        discover.assert_called_once_with(48)

    def test_ecmwf_gust_alias_is_accepted_by_value_selection(self):
        vals={"10fg3":[{"value":12.5}]}
        def one(*names):
            for name in names:
                if name in vals and vals[name]:
                    return vals[name][0]["value"]
            return None
        self.assertEqual(one("10fg","10fg3","10fg6"),12.5)

if __name__=="__main__":
    unittest.main()
