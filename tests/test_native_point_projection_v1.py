import bz2
import hashlib
import unittest
import eccodes as ec
from mardorf_collector.wp13.native_point_projection_v1 import project_original
from test_native_grid_v1 import prefix


def native(members=range(1,21), *, uuid='a'*32, run=20261007):
    messages=[]
    for member in members:
        handle=ec.codes_grib_new_from_samples('GRIB2')
        try:
            for key,value in [('centre','edzw'),('subCentre',255),('gridType','unstructured_grid'),
                ('numberOfGridUsed',47),('uuidOfHGrid',uuid),('productDefinitionTemplateNumber',1),
                ('numberOfForecastsInEnsemble',20),('perturbationNumber',member),('typeOfEnsembleForecast',192),
                ('dataDate',run),('dataTime',0),('step',1),('typeOfLevel','heightAboveGround'),('level',10),('paramId',165)]:
                ec.codes_set(handle,key,value)
            ec.codes_set_values(handle,[float(member),float(member+10)])
            messages.append(ec.codes_get_message(handle))
        finally:ec.codes_release(handle)
    return bz2.compress(b''.join(messages))


def ref(body):return dict(bytes=len(body),sha256=hashlib.sha256(body).hexdigest())


class NativePointProjectionTests(unittest.TestCase):
    def project(self, body, *, grid=None, requested=None, expected_members=None):
        grid=prefix() if grid is None else grid
        original=dict(ref(body),url='https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/00/u_10m/icon-d2-eps_germany_icosahedral_single-level_2026100700_001_2d_u_10m.grib2.bz2',
                      retrieved_at_utc='2026-10-07T00:30:00+00:00')
        return project_original(body,original,grid,ref(grid),requested or {'latitude':52.502699112,'longitude':9.327636527},
            expected_run_utc='2026-10-07T00:00:00+00:00',expected_member_ids=list(range(1,21)) if expected_members is None else expected_members,
            expected_parameter='10u')

    def test_original_native_ids_values_times_and_point_without_api_mapping(self):
        report=self.project(native())
        self.assertEqual(report['native_member_ids'],list(range(1,21)))
        self.assertEqual([r['value_native'] for r in report['records']],list(range(1,21)))
        self.assertTrue(all(r['prospective_feature_eligible_at_capture'] for r in report['records']))
        self.assertEqual(report['geometry']['native_cell_index'],0)
        self.assertFalse(report['scientific_release']);self.assertFalse(report['production_reader_changed'])
        self.assertIn('unverified',report['api_member_mapping'])
        self.assertEqual(report['records'][0]['run_time_utc'],'2026-10-07T00:00:00+00:00')

    def test_station_has_own_cell_and_cannot_receive_other_point_values(self):
        report=self.project(native(),requested={'latitude':52.492985847,'longitude':9.347928045})
        self.assertEqual(report['geometry']['native_cell_index'],1)
        self.assertEqual([r['value_native'] for r in report['records']],list(range(11,31)))

    def test_wrong_run_grid_and_incomplete_or_duplicate_members_fail_closed(self):
        for body,grid in [(native(run=20261006),None),(native(uuid='b'*32),None),
                          (native(range(1,20)),None),(native([1]*20),None),(native(),prefix('b'*32))]:
            with self.assertRaises(ValueError):self.project(body,grid=grid)
        with self.assertRaisesRegex(ValueError,'twenty-member'):
            self.project(native(),expected_members=list(range(1,20)))
        # API's 0..19 population cannot be silently substituted for native 1..20.
        with self.assertRaisesRegex(ValueError,'member mismatch'):self.project(native(),expected_members=list(range(20)))

    def test_tampered_original_bytes_are_rejected_before_decoding(self):
        body=native();original=dict(ref(body),url='irrelevant')
        with self.assertRaisesRegex(ValueError,'hash'):
            project_original(body+b'!',original,prefix(),ref(prefix()),{},expected_run_utc='',
                             expected_member_ids=list(range(1,21)),expected_parameter='10u')


if __name__=='__main__':unittest.main()
