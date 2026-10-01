"""Metadata capture must never change operational wind or backdate evidence."""
from copy import deepcopy
import unittest
from mardorf_collector.providers.wind_metadata_v1 import capture_jobs, merge_exact_metadata


def fixture():
    url='https://opendata.dwd.de/weather/nwp/icon-d2/grib/03/u_10m/icon-d2_germany_regular-lat-lon_single-level_2026092903_024_2d_u_10m.grib2.bz2'
    item={'value':3.0,'shortName':'10u','stepRange':'24','lat':52.5,'lon':9.34,'field_available_at_utc':'2026-09-29T06:00:00Z'}
    row={'run_time_utc':'2026-09-29T03:00:00Z','forecast_lead_hours':24,'source_urls':[url],
         'values':{'u_10m':item},'derived':{'wind_speed_ms':5.0}}
    native={'value':3.0,'shortName':'10u','stepRange':'24','latitude':52.5,'longitude':9.34,
        'units':'m s**-1','paramId':165,'typeOfLevel':'heightAboveGround','level':10,'stepType':'instant',
        'startStep':24,'endStep':24,'stepUnits':'1','source_sha256':'a'*64,'availability_status':'received',
        'availability_observed_at_utc':'2026-09-29T06:20:00Z','field_available_at_utc':'2026-09-29T06:20:00Z'}
    return row,native,url


class WindMetadataTests(unittest.TestCase):
    def test_exact_url_value_and_point_attach_metadata_preserve_operational_values(self):
        row,native,url=fixture(); old=deepcopy(row)
        self.assertEqual(capture_jobs('ICON-D2',row),[('u_10m',url)])
        result=merge_exact_metadata(row,'u_10m',[native],source_url=url)
        self.assertEqual(result['joined_fields'],1)
        self.assertEqual(row['values']['u_10m']['units'],'m s**-1')
        self.assertEqual(row['values']['u_10m']['value'],old['values']['u_10m']['value'])
        self.assertEqual(row['derived'],old['derived'])
        self.assertEqual(row['values']['u_10m']['field_available_at_utc'],native['field_available_at_utc'])
        self.assertEqual(row['values']['u_10m']['value_acquisition_available_at_utc'],old['values']['u_10m']['field_available_at_utc'])
        self.assertEqual(capture_jobs('ICON-D2',row),[])

    def test_mismatch_ambiguity_missing_native_or_clock_preserves_row(self):
        for change in (lambda n:n.update(value=3.1),lambda n:n.update(latitude=52.49),
                       lambda n:n.update(units=None),lambda n:n.update(field_available_at_utc='unknown'),
                       lambda n:n.update(source_sha256='bad'),lambda n:n.update(stepRange='23')):
            row,native,url=fixture(); before=deepcopy(row);change(native)
            result=merge_exact_metadata(row,'u_10m',[native],source_url=url)
            self.assertEqual(result['status'],'not_joined');self.assertEqual(row,before)
        row,native,url=fixture();before=deepcopy(row)
        merge_exact_metadata(row,'u_10m',[native,native],source_url=url)
        self.assertEqual(row,before)

    def test_wrong_cycle_lead_model_or_missing_original_url_never_fetched(self):
        for change in (lambda r:r.update(forecast_lead_hours=21),lambda r:r.update(run_time_utc='2026-09-29T00:00:00Z'),
                       lambda r:r.update(source_urls=[]),lambda r:r.update(source_urls=['https://example.com/u_10m'])):
            row,_,_=fixture();change(row);self.assertEqual(capture_jobs('ICON-D2',row),[])

    def test_list_shape_remains_list(self):
        row,native,url=fixture();row['values']['u_10m']=[row['values']['u_10m']]
        merge_exact_metadata(row,'u_10m',[native],source_url=url)
        self.assertIsInstance(row['values']['u_10m'],list)
        self.assertEqual(row['values']['u_10m'][0]['value'],3.0)
