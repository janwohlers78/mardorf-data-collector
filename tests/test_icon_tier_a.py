import bz2
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import collect_icon_tier_a as t

UTC=timezone.utc


def row(model,run,lead):
    return {
        "model":model,
        "run_time_utc":run.isoformat(),
        "forecast_lead_hours":lead,
        "valid_time_utc":(run+timedelta(hours=lead)).isoformat(),
        "forecast_coordinate_or_grid_point":{"latitude":52.5,"longitude":9.25},
        "values":{},
        "derived":{"wind_speed_ms":5.0},
    }


class IconTierARoutineTests(unittest.TestCase):
    def test_inventory_uses_regular_grid_and_exact_cycle(self):
        hrefs=[
            "https://x/icon-d2_native_2026092603_012_2d_clct.grib2.bz2",
            "https://x/icon-d2_regular-lat-lon_2026092603_012_2d_clct.grib2.bz2",
            "https://x/icon-d2_regular-lat-lon_2026092600_012_2d_clct.grib2.bz2",
        ]
        with patch.object(t.dwd,"directory_hrefs",return_value=("https://x/",hrefs)):
            _,got=t.url_inventory("icon-d2","2026092603","clct")
        self.assertEqual(got,{12:hrefs[1]})

    def test_missing_field_is_explicit_null_never_zero(self):
        run=datetime(2026,9,26,3,tzinfo=UTC)
        value,diag,url=t.fetch_field("icon-d2","2026092603",12,"cin_ml",None,run,run+timedelta(hours=12))
        self.assertIsNone(value["value"])
        self.assertEqual(value["availability_status"],"not_yet_published")
        self.assertNotEqual(value["value"],0)
        self.assertEqual(diag["status"],"not_yet_published")
        self.assertIsNone(url)

    @patch("collect_icon_tier_a.base.grib_nearest")
    @patch("collect_icon_tier_a.message_metadata")
    @patch("collect_icon_tier_a.select_exact_message")
    @patch("collect_icon_tier_a.dwd.S.get")
    def test_received_field_preserves_native_metadata_and_hash(self,get,select,metadata,nearest):
        response=Mock()
        response.content=bz2.compress(b"GRIBfake")
        response.raise_for_status=Mock()
        get.return_value=response
        select.side_effect=lambda raw,selected,run,valid:selected.write_bytes(b"GRIBselected")
        metadata.return_value=[{
            "shortName":"tp","paramId":228,"units":"kg m**-2","typeOfLevel":"surface","level":0,
            "stepType":"accum","startStep":0,"endStep":12,"stepUnits":"1","stepRange":"0-12",
        }]
        nearest.return_value=[{"shortName":"tp","stepRange":"0-12","lat":52.5,"lon":9.25,"value":1.25}]
        run=datetime(2026,9,26,0,tzinfo=UTC)
        values,diag,url=t.fetch_field("icon-d2","2026092600",12,"tot_prec","https://x/file.bz2",run,run+timedelta(hours=12))
        self.assertEqual(diag["status"],"received")
        self.assertEqual(url,"https://x/file.bz2")
        self.assertEqual(values[0]["value"],1.25)
        self.assertEqual(values[0]["units"],"kg m**-2")
        self.assertEqual(values[0]["stepType"],"accum")
        self.assertEqual(values[0]["stepRange"],"0-12")
        self.assertEqual(values[0]["availability_status"],"received")
        self.assertEqual(values[0]["registry_version"],"relevant-meteorology-v2")
        self.assertIsNotNone(values[0]["field_available_at_utc"])
        self.assertIsNotNone(values[0]["availability_observed_at_utc"])
        self.assertEqual(len(values[0]["source_sha256"]),64)

    def test_attach_adds_registry_fields_to_both_models_without_changing_wind(self):
        run=datetime(2026,9,26,0,tzinfo=UTC)
        snapshot={"models":{
            "ICON-D2":[row("ICON-D2",run,0),row("ICON-D2",run,3)],
            "ICON-EU":[row("ICON-EU",run,0),row("ICON-EU",run,6)],
        }}
        def inventory(model,cycle,param):
            return "x",{0:f"https://x/{model}/{param}/0",3:f"https://x/{model}/{param}/3",6:f"https://x/{model}/{param}/6"}
        def fetch(provider_model,cycle,lead,param,url,run,valid):
            return ([{"value":float(lead),"units":"1","shortName":param,"paramId":1,"typeOfLevel":"surface",
                      "level":0,"stepType":"instant","startStep":lead,"endStep":lead,"stepUnits":"1",
                      "stepRange":str(lead),"semantic_id":t.CANONICAL[param],
                      "availability_status":"received","source_sha256":"a"*64}],
                    {"parameter":param,"lead_hours":lead,"status":"received","response_bytes":100,"elapsed_seconds":0.1},
                    url)
        with patch.object(t,"url_inventory",side_effect=inventory), patch.object(t,"fetch_field",side_effect=fetch):
            summary=t.attach(snapshot,workers=2)
        self.assertEqual(summary["total_response_bytes"],4400)
        self.assertEqual(summary["models"]["ICON-D2"]["received_fields"],22)
        self.assertEqual(summary["models"]["ICON-EU"]["received_fields"],22)
        for model in ("ICON-D2","ICON-EU"):
            for r in snapshot["models"][model]:
                self.assertEqual(set(t.TIER_A),set(r["values"]))
                self.assertEqual(r["derived"]["wind_speed_ms"],5.0)
                self.assertIsNotNone(r["retrieved_at_utc"])
                for parameter,item in r["values"].items():
                    self.assertEqual(item[0]["availability_status"],"received")
                    self.assertEqual(item[0]["semantic_id"],t.CANONICAL[parameter])
                    self.assertEqual(item[0]["registry_version"],"relevant-meteorology-v2")
                    self.assertIsNotNone(item[0]["field_available_at_utc"])

    def test_icon_eu_wind_fetchers_do_not_duplicate_tier_a_fields(self):
        from pathlib import Path
        root=Path(__file__).resolve().parents[1]
        base_text=(root/"src/fetch_dwd_additional_models.py").read_text()
        ext_text=(root/"src/extend_model_horizon.py").read_text()
        for parameter in t.TIER_A:
            self.assertNotIn(f"'vmax_10m','{parameter}'",base_text)
        self.assertIn("params=['u_10m','v_10m','vmax_10m']",base_text)
        self.assertIn("for param in ['u_10m','v_10m','vmax_10m']:",ext_text)

    def test_existing_wind_fields_receive_registry_semantics(self):
        run=datetime(2026,9,26,0,tzinfo=UTC)
        snapshot={"models":{
            "ICON-D2":[row("ICON-D2",run,0)],
            "ICON-EU":[row("ICON-EU",run,0)],
        }}
        for records in snapshot["models"].values():
            records[0]["values"]={
                "u_10m":[{"value":3.0}],
                "v_10m":[{"value":4.0}],
                "vmax_10m":[{"value":7.0}],
            }
        def inventory(model,cycle,param):
            return "x",{0:f"https://x/{model}/{param}/0"}
        def fetch(provider_model,cycle,lead,param,url,run,valid):
            return ([{"value":1.0,"units":"1","shortName":param,"paramId":1,
                      "typeOfLevel":"surface","level":0,"stepType":"instant",
                      "startStep":lead,"endStep":lead,"stepUnits":"1","stepRange":str(lead),
                      "semantic_id":t.CANONICAL[param],"availability_status":"received",
                      "source_sha256":"a"*64}],
                    {"parameter":param,"lead_hours":lead,"status":"received",
                     "response_bytes":1,"elapsed_seconds":0.01},url)
        with patch.object(t,"url_inventory",side_effect=inventory), patch.object(t,"fetch_field",side_effect=fetch):
            t.attach(snapshot,workers=2)
        for records in snapshot["models"].values():
            values=records[0]["values"]
            self.assertEqual(values["u_10m"][0]["semantic_id"],"wind_u_10m")
            self.assertEqual(values["v_10m"][0]["semantic_id"],"wind_v_10m")
            self.assertEqual(values["vmax_10m"][0]["semantic_id"],"wind_gust_10m")
            self.assertEqual(records[0]["derived"]["wind_speed_ms"],5.0)

    def test_icon_d2_dict_shaped_wind_fields_receive_registry_semantics(self):
        row={"values":{
            "u_10m":{"value":3.0},
            "v_10m":{"value":4.0},
            "vmax_10m":{"value":7.0},
        }}
        t.annotate_registry_semantics(row)
        self.assertEqual(row["values"]["u_10m"]["semantic_id"],"wind_u_10m")
        self.assertEqual(row["values"]["v_10m"]["semantic_id"],"wind_v_10m")
        self.assertEqual(row["values"]["vmax_10m"]["semantic_id"],"wind_gust_10m")

    def test_tier_a_contract_matches_phase2f1_registry_weather_fields(self):
        self.assertEqual(t.TIER_A,("t_2m","td_2m","relhum_2m","pmsl","ps","tot_prec","clct","aswdir_s","aswdifd_s","cape_ml","cin_ml"))
        self.assertEqual(t.REGISTRY_VERSION,"relevant-meteorology-v2")


if __name__=="__main__":
    unittest.main()
