from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    import numpy as np
    import xarray as xr
except ImportError:
    np = xr = None
from mardorf_collector.providers import huellhorst_native_v1 as h


class Backend:
    def __init__(self):
        self.values={};self.corrupt=False
    def put_bytes(self,key,raw):
        self.values[key]=raw
        return SimpleNamespace(key=key,json=lambda:dict(key=key,sha256=h.digest(raw),bytes=len(raw)))
    def get_bytes(self,ref):
        return b'wrong' if self.corrupt else self.values[ref.key]


@unittest.skipUnless(np is not None and xr is not None, 'native adapter tests require the existing scientific runtime; no automatic installation')
class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.out=Path(self.temp.name)/'outputs'
        self.current=datetime(2026,10,31,18,tzinfo=timezone.utc)
        self.source=h.binding()['sources'][0]
        self.free=patch.object(h.shutil,'disk_usage',return_value=SimpleNamespace(free=32*1024**3))
        self.free.start()

    def tearDown(self):
        self.free.stop();self.temp.cleanup()

    def dataset(self):
        shape=(2,3,3,3,31)
        ds=xr.Dataset({'temperature_2m':(('init_time','lead_time','latitude','longitude','ensemble_member'),np.ones(shape,dtype=np.float32)),
                       'new_native_field':(('init_time','lead_time','latitude','longitude','ensemble_member'),np.full(shape,np.nan,dtype=np.float32))},
                      coords=dict(init_time=np.array(['2026-10-31T12','2026-11-01T00'],dtype='datetime64[h]'),
                                  lead_time=np.array([0,3,120],dtype='timedelta64[h]'),
                                  latitude=[52.,52.25,52.5],longitude=[8.5,8.75,9.],ensemble_member=np.arange(31)),
                      attrs={'publisher_attribute':'retained'})
        ds.temperature_2m.attrs=dict(units='K',step_type='instant')
        return ds

    def opener(self,source):
        return self.dataset(),'fixed-snapshot',object()

    @staticmethod
    def writer(ds,path):
        # Lossless, inspectable fixture writer, independent of production HDF5.
        body=json.dumps({'dims':dict(ds.sizes),'variables':list(ds.data_vars),'attrs':ds.attrs},sort_keys=True).encode()
        Path(path).write_bytes(body)

    def test_hourly_due_uses_aware_clocks(self):
        self.assertTrue(h.due(None,self.current))
        self.assertFalse(h.due((self.current-timedelta(minutes=59)).isoformat(),self.current))
        self.assertTrue(h.due((self.current-timedelta(hours=1)).isoformat(),self.current))
        with self.assertRaises(ValueError):h.due(None,self.current.replace(tzinfo=None))

    def test_latest_never_uses_future_initializations(self):
        ds=self.dataset();selected,stamp=h.selection(ds,self.source,self.current)
        self.assertEqual(str(stamp),'2026-10-31T12:00:00.000000000')
        self.assertEqual(selected.sizes['ensemble_member'],31)
        self.assertEqual(selected.sizes['init_time'],1)
        self.assertIn('new_native_field',selected)
        self.assertTrue(np.isnan(selected.new_native_field.values).all())
        np.testing.assert_array_equal(selected.lead_time.values,ds.lead_time.values)

    def test_missing_member_and_wrong_geometry_rejected(self):
        with self.assertRaisesRegex(ValueError,'full_member'):h.selection(self.dataset().isel(ensemble_member=slice(30)),self.source,self.current)
        ds=self.dataset().assign_coords(latitude=[50.,50.25,50.5])
        with self.assertRaisesRegex(ValueError,'outside_grid'):h.selection(ds,self.source,self.current)

    def test_capture_binds_source_snapshot_all_fields_and_separate_clocks(self):
        r,p=h.capture(self.source,self.out,self.writer,current=self.current,opener=self.opener)
        self.assertEqual(r['identity']['snapshot_id'],'fixed-snapshot')
        self.assertEqual(r['identity']['geometry_id'],'huellhorst-rathaus-20261030-v1')
        self.assertEqual(r['fields']['temperature_2m']['units'],'K')
        self.assertIsNone(r['fields']['new_native_field']['units'])
        self.assertIsNone(r['source_issue_utc'])
        self.assertFalse(r['operational_generation_qualified'])
        self.assertFalse(r['frozen_R1_geometry_replaced'])
        self.assertEqual(r['artifact']['sha256'],h.file_digest(p))
        r2,p2=h.capture(self.source,self.out,self.writer,current=self.current,opener=self.opener)
        self.assertEqual(r,r2);self.assertEqual(p,p2)
        p.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'existing_identity'):h.capture(self.source,self.out,self.writer,current=self.current,opener=self.opener)

    def test_new_snapshot_is_new_immutable_revision(self):
        a,p=h.capture(self.source,self.out,self.writer,current=self.current,opener=self.opener)
        b,q=h.capture(self.source,self.out,self.writer,current=self.current,opener=lambda _: (self.dataset(),'new-snapshot',object()))
        self.assertNotEqual(p,q);self.assertTrue(p.exists())

    def test_low_space_defers_before_directory_or_original_write(self):
        with patch.object(h.shutil,'disk_usage',return_value=SimpleNamespace(free=8*1024**3-1)):
            with self.assertRaisesRegex(ValueError,'reserve'):h.capture(self.source,self.out,self.writer,current=self.current,opener=self.opener)
        self.assertFalse(self.out.exists())

    def test_publication_requires_full_original_and_receipt_readback(self):
        r,p=h.capture(self.source,self.out,self.writer,current=self.current,opener=self.opener)
        backend=Backend();saved,ref=h.publish(r,p,backend)
        self.assertTrue(saved['full_original_readback_verified'])
        self.assertEqual(len(backend.values),2)
        backend.corrupt=True
        with self.assertRaisesRegex(ValueError,'readback'):h.publish(r,p,backend)

    def test_stale_archive_does_not_become_current_after_recapture(self):
        r,p=h.capture(self.source,self.out,self.writer,current=self.current+timedelta(days=10),opener=lambda _: (self.dataset().isel(init_time=[0]),'old-snapshot',object()))
        self.assertFalse(r['asof_within_native_validity'])
        self.assertEqual(r['status'],'CAPTURED_STALE_NO_CURRENT_VALIDITY')

    def test_one_failed_source_does_not_hide_other_committed_source(self):
        def opener(source):
            if source['name']=='GEFS':raise ValueError('publisher_unavailable')
            return self.dataset().isel(ensemble_member=[0]),'fixed-gfs-snapshot',object()
        backend=Backend()
        r=h.tick(self.out,self.writer,backend,current=self.current,opener=opener)
        self.assertEqual(r['status'],'PARTIAL')
        self.assertEqual([v['status'] for v in r['sources']],['FAILED','COMMITTED'])
        self.assertEqual(h.tick(self.out,self.writer,backend,current=self.current,opener=opener)['status'],'NOT_DUE')
        r2=h.tick(self.out,self.writer,backend,current=self.current+timedelta(hours=1),opener=opener)
        self.assertEqual(r2['sources'][1]['status'],'ALREADY_COMMITTED')


if __name__=='__main__':unittest.main()
