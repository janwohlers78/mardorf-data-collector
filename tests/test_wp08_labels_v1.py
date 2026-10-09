import io,zipfile,unittest,hashlib
from types import SimpleNamespace
from mardorf_collector.runtime.wp08_labels_v1 import validate,capture,publication
from mardorf_collector.storage.head import control_metadata

def archive(station='662',name='produkt_test.txt'):
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w') as z:z.writestr(name,'STATIONS_ID;MESS_DATUM;UNKNOWN_EXTRA;eor\n'+station+';2026100800;native;eor\n')
    return stream.getvalue()

class Labels(unittest.TestCase):
    def test_original_extra_columns_and_three_source_clocks_survive(self):
        raw=archive()
        class Response:
            status_code=200
            def __enter__(self):return self
            def __exit__(self,*a):return False
            def raise_for_status(self):pass
            def iter_content(self,*a):yield raw
        r,b=capture(get=lambda *a,**k:Response())
        self.assertEqual(len(b),3)
        for x in r['records']:
            self.assertEqual(x['status'],'valid');self.assertIn('UNKNOWN_EXTRA',x['original_columns'])
            self.assertEqual(x['available_utc'],x['captured_utc']);self.assertLessEqual(x['started_utc'],x['captured_utc'])
    def test_invalid_station_and_zip_paths_are_not_admitted(self):
        for raw in (archive('123'),archive(name='../produkt_test.txt')):
            with self.assertRaises(ValueError):validate(raw)
    def test_only_checked_external_originals_enter_control_reference(self):
        class Backend:
            def __init__(self):self.data={}
            def put_bytes(self,key,b):
                self.data[key]=b;sha=hashlib.sha256(b).hexdigest()
                return SimpleNamespace(key=key,sha256=sha,json=lambda:dict(key=key,sha256=sha,bytes=len(b),schema_version=1))
            def get_bytes(self,r):return self.data[r.key]
        b=archive();r=dict(captured_utc='2026-10-09T12:00:00Z',records=[dict(quantity='cloud',source_sha256=hashlib.sha256(b).hexdigest(),source_bytes=len(b),status='valid')]);backend=Backend()
        paths,ref=publication(r,{'cloud':b},backend)
        self.assertEqual(len(paths),2);self.assertEqual(len(backend.data),2);self.assertTrue(ref['readback_verified'])
        self.assertNotIn('records',ref);control_metadata(ref.get('metadata',{}))
