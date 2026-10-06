"""Real GRIB qualification, compact reads, immutable provenance and session scope."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

import requests
from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import LocalObjects, ObjectError
from mardorf_collector.wp13.model_originals_v1 import Capture, PREFIX
from mardorf_collector.wp13 import model_capture_v2 as capture_runtime
from mardorf_collector.wp13.model_store_v1 import ModelReader, write_catalog, qualify, digest, EXTRACT_PREFIX


class NativeModelTests(unittest.TestCase):
    def test_real_module_cli_shares_sdk_and_local_session_context_with_provider_imports(self):
        code = '''
import importlib, pathlib, requests, runpy, sys
from types import SimpleNamespace
real_import = importlib.import_module
pathlib.Path('sdk.grib2').write_bytes(b'GRIB exact assembled SDK bytes')
def main():
    module = real_import('mardorf_collector.wp13.model_capture_v2')
    assert module._active is not None
    with module.session() as session:
        assert session.hooks['response'] == [module._active.response]
    module.retain_sdk('sdk.grib2', 'google', {'step': [0, 12], 'param': ['10u']})
    capture = module._active.captures['ECMWF-IFS']
    assert len(capture.captures) == 1
    assert capture.captures[0]['metadata']['source_kind'] == 'assembled_native_request'
provider = SimpleNamespace(__file__='synthetic_provider.py', S=requests.Session(), main=main)
def imported(name, *args, **kwargs):
    if name.startswith('mardorf_collector.providers.') or name == 'mardorf_collector.runtime.extend_model_horizon':
        return provider
    return real_import(name, *args, **kwargs)
importlib.import_module = imported
sys.argv = ['model_capture_v2', 'provider', '--model', 'ECMWF-IFS', '--stage', 'base']
runpy.run_module('mardorf_collector.wp13.model_capture_v2', run_name='__main__', alter_sys=True)
'''
        with tempfile.TemporaryDirectory() as folder:
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'), GITHUB_SHA='a' * 40)
            subprocess.run([sys.executable, '-c', code], cwd=folder, env=env, check=True, capture_output=True, text=True)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.capture = Capture(model='ECMWF-IFS', stage='base', commit='a' * 40, directory=self.root)
        self.source = self.root / 'original.grib2'
        self.source.write_bytes((Path(__file__).parent / 'fixtures/wp13/native_weather_v1.grib2').read_bytes())
        import eccodes as ec
        chunks = []
        with self.source.open('rb') as stream:
            while (handle := ec.codes_grib_new_from_file(stream)) is not None:
                try:
                    ec.codes_set(handle, 'centre', 98)
                    chunks.append(ec.codes_get_message(handle))
                finally:
                    ec.codes_release(handle)
        self.source.write_bytes(b''.join(chunks))
        self.item = self.capture.retain(self.source, url='https://data.ecmwf.int/forecasts/',
            kind='assembled_native_request', captured_at_utc='2026-10-04T12:00:00+00:00',
            request={'step': [0, 12], 'param': ['10u', '10v']})
        self.point = {'latitude': 52.4942, 'longitude': 9.3418}

    def test_real_grib_original_native_parquet_exact_cold_read_and_compact_query(self):
        ref = write_catalog(self.capture.backend, [self.item], self.point)
        cold = LocalObjects(self.root / 'objects')
        reader = ModelReader(cold)
        proof = reader.verify(ref)
        self.assertEqual(proof['status'], 'PASS')
        self.assertTrue(proof['parquet_exact_native_match'])
        self.assertTrue(proof['originals_fully_read'])
        self.assertGreater(proof['point_fields'], 0)
        with patch.object(cold, 'get_bytes', wraps=cold.get_bytes) as reads:
            rows = list(reader.query(ref, model='ECMWF-IFS'))
            self.assertTrue(rows)
            self.assertFalse(any(call.args[0].key.startswith(PREFIX + '/blobs/') for call in reads.call_args_list))
        self.assertEqual(rows[0]['source_sha256'], hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertIn('md5GridSection', rows[0]['header_native'])
        self.assertEqual(rows[0]['unit_native'], rows[0]['header_native']['units'])
        self.assertIsNotNone(rows[0]['actual_point'])

    def test_original_metadata_and_digest_contradictions_fail(self):
        changed = json.loads(json.dumps(self.item))
        changed['sha256'] = 'b' * 64
        with self.assertRaisesRegex(ObjectError, 'contradiction'):
            qualify(self.capture.backend, changed, self.point)
        changed = json.loads(json.dumps(self.item))
        changed['metadata']['stage'] = 'extension'
        with self.assertRaisesRegex(ObjectError, 'contradiction'):
            qualify(self.capture.backend, changed, self.point)

    def test_provider_family_cannot_be_renamed_to_another_native_centre(self):
        capture = Capture(model='GFS', stage='base', commit='a' * 40, directory=self.root)
        item = capture.retain(self.source, url='https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs.pl',
            kind='http_response', captured_at_utc='2026-10-04T12:00:00+00:00')
        with self.assertRaisesRegex(ObjectError, 'centre contradiction'):
            qualify(capture.backend, item, self.point)

    def test_projection_cannot_change_native_units_even_with_valid_object_hash(self):
        from mardorf_collector.wp13.model_store_v1 import parquet_bytes
        ref = write_catalog(self.capture.backend, [self.item], self.point)
        reader = ModelReader(self.capture.backend)
        catalog = reader.catalog(ref)
        rows = list(reader.query(ref))
        rows[0]['unit_native'] = 'invented'
        body = parquet_bytes(rows)
        catalog['fragments'][0]['parquet'] = self.capture.backend.put_bytes(
            f'{EXTRACT_PREFIX}/parquet/{digest(body)}', body).json()
        body = canonical(catalog)
        forged = self.capture.backend.put_bytes(f'{EXTRACT_PREFIX}/catalogs/{digest(body)}', body)
        with self.assertRaisesRegex(ObjectError, 'projection mismatch'):
            reader.verify(forged)

    def test_named_sessions_route_shared_and_thread_local_responses_without_patching_requests(self):
        router = capture_runtime.Router('full_members', 'a' * 40, self.root)
        original_factory = requests.Session
        with patch.object(capture_runtime, '_active', router):
            with capture_runtime.session() as session:
                self.assertIs(requests.Session, original_factory)
                self.assertEqual(session.hooks['response'], [router.response])
                router.attach(session)
                self.assertEqual(len(session.hooks['response']), 1)
                response = requests.Response()
                response.status_code = 200
                response.url = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p50a.pl'
                response._content = self.source.read_bytes()
                router.response(response)
                self.assertEqual(len(router.captures['GEFS-control'].captures), 1)
                self.assertEqual(response.content, self.source.read_bytes())

    def test_sdk_keeps_successful_mirror_request_and_original_before_deletion(self):
        router = capture_runtime.Router('extension', 'a' * 40, self.root)
        with patch.object(capture_runtime, '_active', router):
            capture_runtime.retain_sdk(self.source, 'google', {'date': '20261004',
                'time': 12, 'step': [0, 12], 'param': ['10u'], 'target': str(self.source)})
        item = router.captures['ECMWF-IFS'].captures[0]
        self.source.unlink()
        self.assertEqual(item['metadata']['source_kind'], 'assembled_native_request')
        self.assertEqual(item['metadata']['source_url'], 'https://storage.googleapis.com/ecmwf-open-data/')
        self.assertEqual(item['metadata']['sdk_request']['step'], [0, 12])
        router.captures['ECMWF-IFS'].store.restore(item['parent'], self.root / 'recovered')
        self.assertEqual(digest((self.root / 'recovered').read_bytes()), item['sha256'])

    def test_eps_native_member_zero_units_and_indirect_cycle_evidence_are_preserved(self):
        capture = Capture(model='ICON-D2-EPS', stage='base', commit='a' * 40, directory=self.root)
        hourly = {'time': ['2026-10-04T12:00', '2026-10-04T13:00']}
        columns = {}
        for member in range(20):
            key = 'wind_speed_10m' + (f'_member{member:02d}' if member else '')
            hourly[key] = [float(member), float(member) + .5]
            columns[str(member)] = hourly[key]
        payload = dict(self.point, utc_offset_seconds=0, hourly=hourly,
                       hourly_units={'time': 'iso8601', 'wind_speed_10m': 'm/s'})
        response = requests.Response()
        response.status_code = 200
        response.url = 'https://ensemble-api.open-meteo.com/v1/ensemble?models=icon_d2'
        response._content = json.dumps(payload, indent=2).encode()
        capture.response(response)
        logical = digest(json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False).encode())
        ensemble = {'model': 'dwd_icon_d2_eps', 'response_sha256': logical,
            'returned_coordinate': self.point, 'requested_coordinate': self.point,
            'response_run_binding': {'metadata_stable_across_response': True,
                                     'provider_response_embeds_run_time': False},
            'spatial_provenance_verified': True, 'expected_member_ids': list(range(20)),
            'columns': {'wind_speed_10m': columns}}
        ref = write_catalog(capture.backend, capture.captures, self.point, ensemble=ensemble)
        reader = ModelReader(capture.backend)
        self.assertEqual(reader.verify(ref)['fields'], 40)
        zero = list(reader.query(ref, member=0))
        self.assertEqual(len(zero), 2)
        self.assertEqual(zero[0]['unit_native'], 'm/s')
        self.assertIsNone(zero[0]['run_time_utc'])
        self.assertEqual(zero[0]['cycle_evidence_basis'], 'existing_verified_provider_metadata')
        ensemble['columns']['wind_speed_10m']['0'][0] = 999
        with self.assertRaisesRegex(ObjectError, 'member values contradiction'):
            write_catalog(capture.backend, capture.captures, self.point, ensemble=ensemble)

    def test_parallel_cold_verifier_reads_all_original_bytes_and_preserves_full_proof(self):
        import threading,time
        from mardorf_collector.wp13 import model_store_v1 as model
        from mardorf_collector.wp13.native_cold_reads_v1 import ColdModelReads
        ref=write_catalog(self.capture.backend,[self.item],self.point)
        expected=ModelReader(self.capture.backend).verify(ref)
        backend=self.capture.backend
        class Reads:
            def __init__(self):
                self.active=0;self.maximum=0;self.keys=[];self.lock=threading.Lock()
            def get_bytes(self,reference):
                with self.lock:
                    self.active+=1;self.maximum=max(self.maximum,self.active);self.keys.append(reference.key)
                try:
                    time.sleep(.01)
                    return backend.get_bytes(reference)
                finally:
                    with self.lock:self.active-=1
        cold=Reads()
        with ColdModelReads(cold,model,ref.json()) as reads:
            proof=ModelReader(reads).verify(ref)
        self.assertEqual(proof,expected);self.assertTrue(proof['originals_fully_read'])
        self.assertLessEqual(cold.maximum,4);self.assertGreaterEqual(cold.maximum,2)
        parent=model.ParentStore(backend,prefix=model.PREFIX).manifest(self.item['parent'])
        self.assertTrue(all(chunk['key'] in cold.keys for chunk in parent['chunks']))

    def test_parallel_cold_verifier_still_rejects_forged_projection(self):
        from mardorf_collector.wp13 import model_store_v1 as model
        from mardorf_collector.wp13.native_cold_reads_v1 import ColdModelReads
        ref=write_catalog(self.capture.backend,[self.item],self.point)
        reader=ModelReader(self.capture.backend);catalog=reader.catalog(ref);rows=list(reader.query(ref))
        rows[0]['unit_native']='invented'
        body=model.parquet_bytes(rows)
        catalog['fragments'][0]['parquet']=self.capture.backend.put_bytes(EXTRACT_PREFIX+'/parquet/'+digest(body),body).json()
        body=canonical(catalog);forged=self.capture.backend.put_bytes(EXTRACT_PREFIX+'/catalogs/'+digest(body),body)
        with ColdModelReads(self.capture.backend,model,forged.json()) as reads:
            with self.assertRaisesRegex(ObjectError,'projection mismatch'):
                ModelReader(reads).verify(forged)


    def test_verified_canonical_cache_reuses_every_original_without_network_reread(self):
        from mardorf_collector.storage.verified_reads_v1 import VerifiedReads
        from mardorf_collector.wp13 import model_store_v1 as model
        from mardorf_collector.wp13.native_cold_reads_v1 import ColdModelReads
        ref=write_catalog(self.capture.backend,[self.item],self.point)
        cached=VerifiedReads(self.capture.backend,self.root/'canonical-cache')
        with patch.object(self.capture.backend,'get_bytes',wraps=self.capture.backend.get_bytes) as network:
            with ColdModelReads(cached,model,ref.json()) as reads:first=ModelReader(reads).verify(ref)
            self.assertGreater(network.call_count,0);network.reset_mock()
            with ColdModelReads(cached,model,ref.json()) as reads:second=ModelReader(reads).verify(ref)
            self.assertEqual(network.call_count,0)
        self.assertEqual(first,second);self.assertTrue(second['originals_fully_read'])

    def test_corrupt_canonical_cache_refetches_original_and_never_accepts_changed_bytes(self):
        from mardorf_collector.storage.verified_reads_v1 import VerifiedReads
        ref=self.capture.backend.put_bytes('weather/model-native/test/object',b'correct original')
        cached=VerifiedReads(self.capture.backend,self.root/'canonical-cache')
        self.assertEqual(cached.get_bytes(ref),b'correct original')
        (cached.cache.root/ref.sha256).write_bytes(b'corrupt original')
        with patch.object(self.capture.backend,'get_bytes',wraps=self.capture.backend.get_bytes) as network:
            self.assertEqual(cached.get_bytes(ref),b'correct original');self.assertEqual(network.call_count,1)
        self.assertEqual(cached.cache.metrics['corrupt_entries'],1)

    def test_cached_publication_requires_live_matching_head_and_restores_deleted_original(self):
        from mardorf_collector.storage.verified_reads_v1 import VerifiedReads
        from mardorf_collector.wp13.native_cold_reads_v1 import publish_object
        local=self.capture.backend;ref=local.put_bytes('weather/model-native/test/object',b'original')
        remote=LocalObjects(self.root/'canonical-remote');remote.put_file(ref.key,local.root/ref.key)
        cached=VerifiedReads(remote,self.root/'canonical-cache');cached.get_bytes(ref)
        with patch.object(remote,'put_file',wraps=remote.put_file) as upload:
            self.assertEqual(publish_object(remote,local,ref,verified_reads=cached),ref);self.assertEqual(upload.call_count,0)
            (remote.root/ref.key).unlink()
            self.assertEqual(publish_object(remote,local,ref,verified_reads=cached),ref);self.assertEqual(upload.call_count,1)
        (remote.root/ref.key).write_bytes(b'changed!')
        with self.assertRaisesRegex(ValueError,'Immutable cached publication conflict'):
            publish_object(remote,local,ref,verified_reads=cached)


if __name__ == '__main__':
    unittest.main()
