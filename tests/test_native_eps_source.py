import bz2,copy,hashlib,json,tempfile,unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
import eccodes as ec
from mardorf_collector.providers import native_eps as eps
from mardorf_collector.runtime.native_source_seed import create,load,PATH
from mardorf_collector.storage.objects import LocalObjects
from mardorf_collector.storage.archive import canonical
from mardorf_collector.wp13.native_point_projection_v1 import project_original,step_hours
from test_native_grid_v1 import prefix
from test_native_point_projection_v1 import native,ref

class NativeEpsSourceTests(unittest.TestCase):
    def test_minutes_encoded_zero_is_admitted_without_changing_original_header(self):
        messages=[]
        with tempfile.TemporaryFile() as stream:
            stream.write(bz2.decompress(native()));stream.seek(0)
            while (h:=ec.codes_grib_new_from_file(stream))is not None:
                try:ec.codes_set(h,'step',0);messages.append(ec.codes_get_message(h))
                finally:ec.codes_release(h)
        body=bz2.compress(b''.join(messages));original=dict(ref(body),url='https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/00/u_10m/icon-d2-eps_germany_icosahedral_single-level_2026100700_000_2d_u_10m.grib2.bz2',retrieved_at_utc='2026-10-07T00:30:00+00:00')
        cache={};grid=prefix()
        with patch('mardorf_collector.wp13.native_point_projection_v1.native_point',wraps=__import__('mardorf_collector.wp13.native_point_projection_v1',fromlist=['native_point']).native_point) as nearest:
            for _ in range(2):
                out=project_original(body,original,grid,ref(grid),{'latitude':52.502699112,'longitude':9.327636527},expected_run_utc='2026-10-07T00:00:00+00:00',expected_member_ids=eps.MEMBERS,expected_parameter='10u',geometry_cache=cache)
            self.assertEqual(nearest.call_count,1)
        h=out['records'][0]['header'];self.assertEqual(step_hours(h['endStep'],h['stepUnits']),0)
        self.assertEqual(out['native_member_ids'],list(range(1,21)))
        self.assertEqual(step_hours('180m',0),3)
        with self.assertRaises(ValueError):step_hours(3,255)

    def test_all_former_api_quantities_and_additional_convection_fields_are_registered(self):
        for name in ('t_2m','td_2m','relhum_2m','pmsl','ps','tot_prec','clct','aswdir_s','aswdifd_s','cape_ml','cin_ml','lpi'):
            self.assertIn(name,eps.PARAMETERS)
        self.assertEqual(eps.MEMBERS,list(range(1,21)))
        with self.assertRaises(ValueError):eps.fetch([0,48],workers=5)

    def test_acquisition_seed_cannot_relabel_or_hide_catalog_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            b=LocalObjects(folder);catalog={'parents':[{'metadata':{'acquisition_model':'GFS'}}]};catalog_ref=b.put_bytes('weather/catalog',canonical(catalog))
            health={'generated_at_utc':'2026-10-10T12:00:00+00:00','error_count':1,'sources':{'GFS':{'provider_cycle_complete':True,'selected_run_time_utc':'2026-10-10T06:00:00+00:00'}}}
            proof={'status':'PASS','originals_fully_read':True,'parquet_exact_native_match':True}
            marker=create(b,{'models':{'GFS':[{'run_time_utc':'2026-10-10T06:00:00+00:00'}]}},health,catalog_ref.json(),catalog,proof)
            class Reader:
                backend=b
                def read(self,path,required=False):return canonical(marker) if path==PATH else None
            reader=Reader();got=load(reader)
            self.assertEqual(got[0]['sources']['GFS']['selected_run_time_utc'],'2026-10-10T06:00:00+00:00')
            self.assertFalse(marker['all_native_sources_ready']);self.assertFalse(marker['operational_promotion'])
            marker['source_catalogs']['GFS']['run_time_utc']='2026-10-10T12:00:00+00:00'
            with self.assertRaisesRegex(ValueError,'catalog continuation'):load(reader)
            marker['source_catalogs']['GFS']['run_time_utc']='2026-10-10T06:00:00+00:00'
            marker['source_catalogs']['GFS']['cold_readback']['originals_fully_read']=False
            with self.assertRaisesRegex(ValueError,'catalog continuation'):load(reader)
            marker['source_catalogs']['GFS']['cold_readback']['originals_fully_read']=True
            (b.root/marker['payload']['key']).write_bytes(b'corrupt')
            with self.assertRaises(Exception):load(reader)

if __name__=='__main__':unittest.main()
