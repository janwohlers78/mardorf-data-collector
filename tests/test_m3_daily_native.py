import copy
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import types
import unittest
import urllib.error
from unittest.mock import patch

import numpy as np
import m3_daily_native as m


class Archive:
    def __init__(self): self.saved = {}
    def __call__(self, name, raw):
        self.saved[name] = raw
        return {'file': 'ovh-cas/' + name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


class Response:
    def __init__(self, raw, status=200, headers=None):
        self.raw, self.status, self.headers, self.reads = raw, status, headers or {}, []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self, maximum): self.reads.append(maximum); return self.raw[:maximum]


class Opener:
    def __init__(self, response): self.response = response
    def open(self, *args, **kwargs): return self.response


def metadata():
    return {'zarr_format': 3, 'data_type': 'float32',
        'dimension_names': ['init_time', 'ensemble_member', 'lead_time', 'latitude', 'longitude'],
        'shape': [2, 31, 181, 721, 1440],
        'chunk_grid': {'name': 'regular', 'configuration': {'chunk_shape': [1, 31, 192, 374, 368]}},
        'chunk_key_encoding': {'name': 'default', 'configuration': {'separator': '/'}},
        'codecs': [{'name': 'sharding_indexed', 'configuration': {
            'chunk_shape': [1, 31, 64, 17, 16], 'index_location': 'end',
            'codecs': [{'name': 'bytes', 'configuration': {'endian': 'little'}},
                {'name': 'blosc', 'configuration': {'cname': 'zstd', 'clevel': 3,
                    'shuffle': 'shuffle', 'typesize': 4, 'blocksize': 0}}],
            'index_codecs': [{'name': 'bytes', 'configuration': {'endian': 'little'}}, {'name': 'crc32c'}]}}]}


class Coordinate:
    def __init__(self, values, name, units=None):
        self.values, self.dims = np.asarray(values), (name,)
        self.attrs = {'units': units} if units else {}
    def __array__(self, dtype=None, copy=None): return np.asarray(self.values, dtype=dtype)


class Dataset:
    def __init__(self):
        self.attrs={'dataset_id':'noaa-gefs-forecast-35-day','dataset_version':'0.2.0'}
        self.init_time = Coordinate(np.array(['2026-11-03', '2026-11-04'], dtype='datetime64[ns]'), 'init_time')
        self.ensemble_member = Coordinate(np.arange(31), 'ensemble_member')
        self.lead_time = Coordinate(np.arange(0, 243, 3).astype('timedelta64[h]'), 'lead_time')
        self.latitude = Coordinate(np.arange(90, -90.01, -.25), 'latitude', 'degree_north')
        self.longitude = Coordinate(np.arange(0, 360, .25), 'longitude', 'degree_east')
        self.valid_time = types.SimpleNamespace(isel=lambda init_time, lead_time:
            np.asarray(self.init_time)[init_time, None] + np.asarray(self.lead_time)[None, lead_time])
        self.fields = {name: types.SimpleNamespace(dims=('init_time', 'ensemble_member', 'lead_time', 'latitude', 'longitude'),
            attrs={'units': 'm s-1', 'step_type': 'instant', 'standard_name': component})
            for name, component in [('wind_u_10m', 'eastward_wind'), ('wind_v_10m', 'northward_wind')]}
    def __getitem__(self, key): return self.fields[key] if key in self.fields else getattr(self, key)


def zarr_mock(value):
    return types.SimpleNamespace(open_array=lambda **kwargs:
        types.SimpleNamespace(metadata=types.SimpleNamespace(to_dict=lambda: copy.deepcopy(value))))


def header(name, lead, day='2026-11-04'):
    start = m.initialization(day); valid = start + timedelta(hours=lead)
    unit, level, kind = m.FIELDS.get(name, ('K', '2', 'heightAboveGround'))
    return {'shortName': name, 'units': unit, 'level': level, 'typeOfLevel': kind or 'meanSea',
        'stepType': 'instant', 'indicatorOfUnitOfTimeRange': 'h', 'dataDate': start.strftime('%Y%m%d'),
        'dataTime': '0', 'validityDate': valid.strftime('%Y%m%d'), 'validityTime': valid.strftime('%H%M'),
        'startStep': str(lead), 'endStep': str(lead)}


class IFSSource:
    def __init__(self, bad_generation=False, optional_duplicate=False, optional_bad=False, missing=False):
        self.bad_generation, self.optional_duplicate, self.optional_bad, self.missing = bad_generation, optional_duplicate, optional_bad, missing
        self.messages = {}
    def open(self, request, timeout=30):
        import re
        lead = int(re.search(r'-(\d+)h-oper-fc', request.full_url)[1])
        names = list(m.FIELDS)
        if self.missing and lead == 24: names.remove('sp')
        if self.optional_duplicate or self.optional_bad: names += ['2d']
        if self.optional_duplicate: names += ['2d']
        entries = []
        for j, name in enumerate(names):
            raw = b'GRIB' + f'{name}:{lead}'.encode().ljust(20, b' ') + b'7777'
            lo = j*len(raw); self.messages[(lead, lo)] = raw
            entries.append({'param': name, 'levtype': 'sfc', 'step': str(lead), 'type': 'fc', '_offset': lo, '_length': len(raw)})
        if request.full_url.endswith('.index'):
            return Response(b'\n'.join(json.dumps(e).encode() for e in entries))
        lo, hi = map(int, request.get_header('Range').removeprefix('bytes=').split('-'))
        return Response(self.messages[(lead, lo)], 206, {'Content-Range': f'bytes {lo}-{hi}/99999'})
    def decode(self, raw):
        name, lead = raw[4:-4].strip().decode().split(':'); lead = int(lead)
        if self.optional_bad and name == '2d': raise ValueError('bad_optional_decoder')
        generation = {'native': 'g2' if self.bad_generation and lead == 30 else 'g1'}
        value = {'10u': 3., '10v': 4., '2t': 280., 'sp': 99000., 'msl': 100000., '2d': 275.}[name]
        return value, {'header': header(name, lead), 'generation': generation}


class Tests(unittest.TestCase):
    def test_publisher_verifies_then_removes(self):
        with tempfile.TemporaryDirectory() as td:
            archive = Archive(); pub = m.Publisher(td, publish_bytes=archive)
            ref = pub.save('raw.bin', b'abc')
            self.assertEqual(ref['sha256'], m.digest(b'abc')); self.assertFalse((Path(td)/'raw.bin').exists())
            bad = m.Publisher(td, publish_bytes=lambda n, r: {'file':'x', 'bytes':len(r), 'sha256':'0'*64})
            with self.assertRaises(ValueError): bad.save('bad.bin', b'abc')
            self.assertEqual((Path(td)/'bad.bin').read_bytes(), b'abc')
            with self.assertRaises(FileExistsError): bad.save('bad.bin', b'abc')

    def test_descending_grid_preserves_native_indices(self):
        ix, p = m.neighbours([52.75,52.5,52.25,52.], m.TARGET['latitude'])
        self.assertEqual(ix, [2,1]); self.assertAlmostEqual(p['upper_weight'], .07801111111109637)
        for grid, q in [([52.,52.25,52.25],52.1),([52.,52.5],52.1),([52.,52.25],52.5)]:
            with self.assertRaises(ValueError): m.neighbours(grid,q)

    def test_http_range_refuses_full_body_before_read(self):
        response = Response(b'full',200)
        with self.assertRaisesRegex(ValueError,'not_honored'): m.fetch(Opener(response),m.IFS_PREFIX+'x',byte_range=(10,13))
        self.assertEqual(response.reads,[])
        good = Response(b'abcd',206,{'Content-Range':'bytes 10-13/100'})
        self.assertEqual(m.fetch(Opener(good),m.IFS_PREFIX+'x',byte_range=(10,13))[0],b'abcd')
        for raw in (b'abc',b'abcde'):
            with self.assertRaises(ValueError): m.fetch(Opener(Response(raw,206,{'Content-Range':'bytes 10-13/100'})),m.IFS_PREFIX+'x',byte_range=(10,13))

    def test_GRIB_peak_guard_before_network(self):
        with self.assertRaisesRegex(ValueError,'decode_peak'): m.fetch(None,m.IFS_PREFIX+'x',byte_range=(0,70*1024**2))

    def test_native_metadata_exact_profile_no_weather(self):
        proof = m.gefs_metadata(Dataset(),None,zarr_mock(metadata()),m.initialization('2026-11-04'))
        self.assertEqual(proof['init_index'],1)
        self.assertEqual(proof['costs']['wind_u_10m']['inner_chunks'],1)
        self.assertEqual(proof['costs']['wind_u_10m']['inner_decoded_bytes'],2158592)
        self.assertGreater(proof['costs']['wind_u_10m']['outer_logical_bytes'],3*1024**3)
        self.assertEqual(proof['latitude_indices'],[151,150])

    def test_metadata_rejects_changed_shard_before_load(self):
        changed=metadata(); changed['codecs'][0]['configuration']['chunk_shape']=[1,31,64,34,32]
        with self.assertRaises(ValueError): m.gefs_metadata(Dataset(),None,zarr_mock(changed),m.initialization('2026-11-04'))
        ds=Dataset(); ds.ensemble_member=Coordinate(np.arange(30),'ensemble_member')
        with self.assertRaisesRegex(ValueError,'all31'): m.gefs_metadata(ds,None,zarr_mock(metadata()),m.initialization('2026-11-04'))
        with self.assertRaisesRegex(ValueError,'no_latest'): m.gefs_metadata(Dataset(),None,zarr_mock(metadata()),m.initialization('2026-11-05'))

    def test_GEFS_dataset_generation_and_native_resolution_not_assumed(self):
        ds=Dataset(); ds.attrs['dataset_version']='0.3.0'
        with self.assertRaisesRegex(ValueError,'version_changed'):
            m.gefs_metadata(ds,None,zarr_mock(metadata()),m.initialization('2026-11-04'))
        ds=Dataset(); ds.lead_time=Coordinate(np.arange(0,243,6).astype('timedelta64[h]'),'lead_time')
        with self.assertRaisesRegex(ValueError,'three_hour_axis'):
            m.gefs_metadata(ds,None,zarr_mock(metadata()),m.initialization('2026-11-04'))

    def test_original_range_guard_blocks_full_shard(self):
        limits={'max_metadata_requests':128,'metadata_coordinate_payload_budget_bytes':4*1024**2,
            'max_weather_requests':8,'range_payload_max_bytes':2224128,'suffix_index_max_bytes':65536,
            'received_weather_payload_budget_bytes':8*1024**2}
        b=m.Budget({'allowed_weather_keys':{'u':'wind_u_10m/c/1/0/0/0/0'},'resource_plan':limits})
        with self.assertRaisesRegex(ValueError,'full weather'): b.before('wind_u_10m/c/1/0/0/0/0',None)
        RangeByteRequest=type('RangeByteRequest',(),{})
        request=RangeByteRequest(); request.start=0; request.end=2224129
        with self.assertRaisesRegex(ValueError,'one encoded'): b.before('wind_u_10m/c/1/0/0/0/0',request)
        request.end=100; ticket=b.before('wind_u_10m/c/1/0/0/0/0',request); b.after(ticket,100)
        self.assertEqual(b.receipt()['weather_payload_bytes_received'],100)

    def test_GEFS_capture_preserves31_signed_members_and_missing_slots(self):
        class Wrapper:
            def __init__(self, store): self._store=store; self.read_only=True
        for missing in (False, True):
            class NativeDataset(Dataset):
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def __getitem__(self, key):
                    if isinstance(key, list): return self
                    return super().__getitem__(key)
                def isel(self, **kwargs):
                    self.selected=kwargs
                    arrays={}
                    for field in ('wind_u_10m','wind_v_10m'):
                        values=np.ones((1,31,17,2,2),dtype=np.float32)*(3 if field=='wind_u_10m' else 4)
                        if field=='wind_u_10m': values[:,1::2]*=-1
                        if missing and field=='wind_u_10m': values[0,0,1,0,0]=np.nan
                        arrays[field]=types.SimpleNamespace(load=lambda a=values:a)
                    return arrays
            ds=NativeDataset()
            fake_ice=types.SimpleNamespace(http_storage=lambda url:url,Repository=types.SimpleNamespace(
                open=lambda storage:types.SimpleNamespace(readonly_session=lambda **kwargs:
                    types.SimpleNamespace(snapshot_id='IMMUTABLE_SNAPSHOT',store=object()))))
            z=zarr_mock(metadata()); z.storage=types.SimpleNamespace(WrapperStore=Wrapper)
            with tempfile.TemporaryDirectory() as td, patch.dict('sys.modules',{'icechunk':fake_ice,
                    'xarray':types.SimpleNamespace(open_zarr=lambda **kwargs:ds),'zarr':z,
                    'zarr.abc':types.ModuleType('zarr.abc'),'zarr.abc.store':types.SimpleNamespace(RangeByteRequest=type('RangeByteRequest',(),{}))}):
                archive=Archive(); rows, proof=m.gefs_day('2026-11-04',m.Publisher(td,publish_bytes=archive))
                self.assertEqual(ds.selected['latitude'],[151,150])
                self.assertEqual(rows[24]['member_u_ms'][:2],[3.,-3.])
                self.assertEqual(rows[24]['member_v_ms'][:2],[4.,4.])
                self.assertEqual(np.hypot(rows[24]['member_u_ms'],rows[24]['member_v_ms']).tolist(),[5.]*31)
                with np.load(io.BytesIO(archive.saved['gefs-native-four-cells.npz'])) as raw:
                    self.assertEqual(raw['wind_u_10m'].shape,(1,31,17,2,2))
                if missing: self.assertEqual(rows[30]['status'],'NATIVE_MEMBER_NONFINITE')
                else: self.assertEqual(len(rows[30]['member_ids']),31)
                self.assertFalse(json.loads(archive.saved['gefs-native-proof.json'])['scientific_generation_transfer_qualified'])

    def test_IFS_all17_exact_five_fields_raw_preserved(self):
        with tempfile.TemporaryDirectory() as td:
            archive=Archive(); source=IFSSource(); rows=m.ifs_day('2026-11-04',m.Publisher(td,publish_bytes=archive),opener=source,decoder=source.decode)
            self.assertEqual(len(rows),17); self.assertTrue(all(r.get('speed10_ms')==5 for r in rows.values()))
            self.assertEqual(sum(name.endswith('.grib2') for name in archive.saved),85)
            self.assertFalse(any(Path(td).iterdir()))
            self.assertEqual(rows[24]['raw_channels']['pressure_model_surface_hpa'],990.)

    def test_IFS_generation_change_and_missing_not_silently_filled(self):
        for kwargs, failed in [({'bad_generation':True},30),({'missing':True},24)]:
            with tempfile.TemporaryDirectory() as td:
                source=IFSSource(**kwargs); rows=m.ifs_day('2026-11-04',m.Publisher(td,publish_bytes=Archive()),opener=source,decoder=source.decode)
                self.assertEqual(rows[failed]['status'],'NATIVE_IFS_CAPTURE_FAILED')
                self.assertIn('source_id',rows[36])

    def test_optional_failure_does_not_erase_mandatory_wind(self):
        for kwargs, expected in [({'optional_duplicate':True},'DUPLICATE_OPTIONAL_INDEX_OPERATOR_UNQUALIFIED'),({'optional_bad':True},'RAW_PRESERVED_DECODER_NOT_QUALIFIED')]:
            with tempfile.TemporaryDirectory() as td:
                source=IFSSource(**kwargs); archive=Archive()
                rows=m.ifs_day('2026-11-04',m.Publisher(td,publish_bytes=archive),opener=source,decoder=source.decode)
                self.assertEqual(rows[24]['speed10_ms'],5.)
                self.assertEqual(rows[24]['raw_channels']['optional']['2d']['status'],expected)

    def test_optional_operator_requires_correct_clock_height(self):
        h=header('2d',24); h['startStep']='23'
        with self.assertRaises(ValueError): m.optional_ifs(275.,{'header':h},m.initialization('2026-11-04'),24)
        h=header('2d',24); h['level']='10'
        with self.assertRaises(ValueError): m.optional_ifs(275.,{'header':h},m.initialization('2026-11-04'),24)

    def test_collect_preserves17_ledger_missing_case_and_failures(self):
        day=datetime.now(timezone.utc).strftime('%Y-%m-%d'); run=m.initialization(day)
        def records(kind):
            return {lead:{'source_id':kind,'initialization_utc':run.isoformat(), 'valid_utc':(run+timedelta(hours=lead)).isoformat(),'lead_hours':lead} for lead in m.LEADS}
        def gefs(day,pub):
            rows=records('gefs'); del rows[30]; return rows,pub.save('proof.json',{'test':True})
        with tempfile.TemporaryDirectory() as td:
            a=Archive(); result=m.collect(td,day,publish_bytes=a,gefs_provider=gefs,ifs_provider=lambda d,p:records('ifs'))['result']
            self.assertEqual(result['captured_native_pairs'],16); self.assertEqual(len(result['attempts']),17);self.assertEqual(len(result['raw_ifs_records']),17)
            self.assertEqual(result['attempts'][1]['status'],'GEFS_MISSING_OR_UNQUALIFIED')
            self.assertFalse(result['independent_input_admission']); self.assertIn('proof.json',result['artifacts'])
        def failed(*args): raise ValueError('source_down')
        with tempfile.TemporaryDirectory() as td:
            result=m.collect(td,day,publish_bytes=Archive(),gefs_provider=failed,ifs_provider=failed)['result']
            self.assertEqual(result['captured_native_pairs'],0); self.assertEqual(len(result['attempts']),17)
            self.assertIn('ifs-failure.json',result['artifacts'])

    def test_public_source_no_private_model_paths(self):
        for name in ['m3_daily_native.py','_m3_native_range.py','_m3_ifs_contract.py']:
            text=Path(__file__).with_name(name).read_text()
            self.assertNotIn('/srv/',text); self.assertNotIn('bank.json',text)



class CoordinateGuardTests(unittest.IsolatedAsyncioTestCase):
    def build(self, values):
        from _m3_coordinate_guard import coordinate_bounded_store
        class Wrapper:
            def __init__(self,source):self._store=source;self.read_only=True
        class RangeByteRequest:
            def __init__(self,start,end):self.start,self.end=start,end
        class Source:
            def __init__(self):self.seen=[]
            async def get(self,key,prototype,byte_range=None):
                self.seen.append((key,byte_range));raw=values.get(key)
                if raw is None:return None
                self.assert_bounded(byte_range)
                return raw[byte_range.start:byte_range.end]
            def assert_bounded(self,r):
                if r is None:raise AssertionError('full metadata/coordinate GET reached source')
        source=Source();return coordinate_bounded_store(source,Wrapper,RangeByteRequest),source
    def coord(self,size=5475):
        return {'zarr_format':3,'node_type':'array','data_type':'int64','shape':[2226],
            'dimension_names':['init_time'],'chunk_grid':{'name':'regular','configuration':{'chunk_shape':[size]}},
            'storage_transformers':[],'codecs':[{'name':'bytes','configuration':{'endian':'little'}},
                {'name':'blosc','configuration':{'cname':'zstd','typesize':8,'clevel':3,'shuffle':'shuffle','blocksize':0}}]}
    async def test_coordinate_transfer_denied_until_actual_metadata_admitted(self):
        meta=self.coord();store,source=self.build({'init_time/zarr.json':m.canonical(meta),'init_time/c/0':b'abc'})
        with self.assertRaisesRegex(ValueError,'before_metadata'):await store.get('init_time/c/0',None)
        self.assertEqual(source.seen,[])
        await store.get('init_time/zarr.json',None)
        self.assertEqual(store.approved['init_time'],87600)
        self.assertEqual(await store.get('init_time/c/0',None),b'abc')
        self.assertTrue(all(request is not None for key,request in source.seen))
    async def test_coordinate_decode_bomb_rejected_at_metadata_before_chunk_GET(self):
        store,source=self.build({'init_time/zarr.json':m.canonical(self.coord(100_000_000)),'init_time/c/0':b'x'})
        with self.assertRaisesRegex(ValueError,'decoded_peak'):await store.get('init_time/zarr.json',None)
        with self.assertRaisesRegex(ValueError,'before_metadata'):await store.get('init_time/c/0',None)
        self.assertEqual([key for key,r in source.seen],['init_time/zarr.json'])
    async def test_consolidated_metadata_also_admits_before_eager_coordinate_load(self):
        root={'node_type':'group','consolidated_metadata':{'metadata':{'init_time':self.coord()}}}
        store,source=self.build({'zarr.json':m.canonical(root),'init_time/c/0':b'abc'})
        await store.get('zarr.json',None)
        self.assertEqual(await store.get('init_time/c/0',None),b'abc')
    async def test_encoded_coordinate_bound_before_return_and_codec_fail_closed(self):
        from _m3_coordinate_guard import ENCODED_MAX
        store,source=self.build({'init_time/zarr.json':m.canonical(self.coord()),'init_time/c/0':b'x'*(ENCODED_MAX+2)})
        await store.get('init_time/zarr.json',None)
        with self.assertRaisesRegex(ValueError,'before_decode'):await store.get('init_time/c/0',None)
        self.assertEqual(source.seen[-1][1].end,ENCODED_MAX+1)
        meta=self.coord();meta['codecs'][1]['configuration']['cname']='unknown'
        store,source=self.build({'init_time/zarr.json':m.canonical(meta)})
        with self.assertRaisesRegex(ValueError,'codec_changed'):await store.get('init_time/zarr.json',None)

    async def test_eager_full_index_allocation_rejected_even_with_tiny_chunks(self):
        meta=self.coord(1);meta['shape']=[10**9]
        store,source=self.build({'init_time/zarr.json':m.canonical(meta),'init_time/c/0':b'x'})
        with self.assertRaisesRegex(ValueError,'decoded_peak'):await store.get('init_time/zarr.json',None)
        with self.assertRaisesRegex(ValueError,'before_metadata'):await store.get('init_time/c/0',None)
        self.assertEqual([key for key,r in source.seen],['init_time/zarr.json'])

if __name__=='__main__': unittest.main()
