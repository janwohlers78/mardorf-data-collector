"""Real GRIB qualification, compact reads, immutable provenance and session scope."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests
from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import LocalObjects, ObjectError
from mardorf_collector.wp13.model_originals_v1 import Capture, PREFIX
from mardorf_collector.wp13 import model_capture_v2 as capture_runtime
from mardorf_collector.wp13.model_store_v1 import ModelReader, write_catalog, qualify, digest, EXTRACT_PREFIX


class NativeModelTests(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
