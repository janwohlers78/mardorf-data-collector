"""Capture the existing response once; keep legacy reads and provenance intact."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import requests
from urllib3.response import HTTPResponse

from mardorf_collector.storage.objects import ObjectError
from mardorf_collector.wp13.model_originals_v1 import Capture, safe_url


class ModelOriginalsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.capture = Capture(model='GFS', stage='base', commit='a' * 40,
                               directory=self.root)

    def response(self, body, *, headers=None):
        response = requests.Response()
        response.status_code = 206
        response.url = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs.pl?var_UGRD=on&token=secret'
        response.headers.update(headers or {'Content-Length': str(len(body))})
        response.raw = HTTPResponse(body=io.BytesIO(body), preload_content=False)
        return response

    def test_original_once_legacy_content_unchanged_and_range_bound(self):
        body = (Path(__file__).parent / 'fixtures/wp13/native_weather_v1.grib2').read_bytes()
        response = self.response(body, headers={'Content-Length': str(len(body)),
                                               'Content-Range': f'bytes 0-{len(body)-1}/{len(body)}'})
        with patch.object(response.raw, 'stream', wraps=response.raw.stream) as reads:
            self.assertIs(self.capture.response(response), response)
            self.assertEqual(response.content, body)
            self.assertEqual(response.content, body)
            self.assertEqual(reads.call_count, 1)
        captured = self.capture.captures[0]
        self.assertEqual(captured['sha256'], hashlib.sha256(body).hexdigest())
        self.assertNotIn('secret', json.dumps(captured))
        persisted = json.loads(next(self.root.glob('*.capture.json')).read_bytes())
        self.assertEqual(persisted, captured)
        self.assertEqual(self.capture.store.manifest(captured['parent'])['metadata'], captured['metadata'])
        self.assertIn('content_range', captured['metadata'])

    def test_capture_failure_does_not_discard_valid_provider_body(self):
        response = self.response(b'GRIB original')
        with patch.object(self.capture.store, 'write_file', side_effect=ObjectError('unavailable')):
            self.capture.response(response)
        self.assertEqual(response.content, b'GRIB original')
        self.assertEqual(self.capture.captures, [])
        self.assertEqual(self.capture.errors[0]['error_type'], 'ObjectError')
        self.assertFalse(self.capture.bind({})['all_captures_have_point_bindings'])

    def test_wire_failure_propagates_for_existing_provider_retry(self):
        response = self.response(b'GRIB')
        with patch.object(response.raw, 'stream', side_effect=requests.exceptions.ConnectionError('wire')):
            with self.assertRaises(requests.exceptions.ConnectionError):
                self.capture.response(response)
        self.assertEqual(self.capture.captures, [])

    def test_invalid_length_keeps_body_and_has_no_success_marker(self):
        response = self.response(b'GRIB', headers={'Content-Length': '5'})
        self.capture.response(response)
        self.assertEqual(response.content, b'GRIB')
        self.assertEqual(self.capture.captures, [])
        self.assertEqual(len(self.capture.errors), 1)

    def test_sessions_are_attached_once_and_only_allowed_urls_are_persisted(self):
        with requests.Session() as session:
            self.capture.attach([session, session])
            self.capture.attach([session])
            self.assertEqual(session.hooks['response'], [self.capture.response])
            self.assertEqual(session.headers['Accept-Encoding'], 'identity')
        self.assertEqual(safe_url('https://opendata.dwd.de/test?lead=12&sig=secret#private'),
                         'https://opendata.dwd.de/test?lead=12')
        for url in ('http://opendata.dwd.de/test', 'https://example.org/test',
                    'https://user:secret@opendata.dwd.de/test'):
            with self.subTest(url=url), self.assertRaises(ObjectError):
                safe_url(url)

    def test_existing_native_point_metadata_is_bound_only_to_matching_original(self):
        response = self.response(b'GRIB unchanged')
        self.capture.response(response)
        digest = hashlib.sha256(response.content).hexdigest()
        native = {'source_sha256': digest, 'value': 1.0, 'shortName': '10u',
                  'units': 'm s**-1', 'stepType': 'instant', 'startStep': 12, 'endStep': 12}
        row = {'run_time_utc': '2026-10-04T00:00:00Z', 'valid_time_utc': '2026-10-04T12:00:00Z',
               'forecast_lead_hours': 12, 'forecast_coordinate_or_grid_point':
                   {'latitude': 52.5, 'longitude': 9.25}, 'values': {'10u': [native]}}
        report = self.capture.bind({'spot': {'lat': 52.5, 'lon': 9.25}, 'models': {'GFS': [row]}})
        self.assertTrue(report['all_captures_have_point_bindings'])
        self.assertEqual(report['native_reader_admission'], 'NOT_QUALIFIED')
        self.assertEqual(report['captures'][0]['point_fields'][0]['native'], native)
        self.assertEqual(report['provider_requests_added'], 0)
        row['values']['10u'][0]['source_sha256'] = 'b' * 64
        self.assertFalse(self.capture.bind({'models': {'GFS': [row]}})['all_captures_have_point_bindings'])

    def test_eps_canonical_identity_is_distinct_from_untouched_http_bytes(self):
        capture = Capture(model='ICON-D2-EPS', stage='base', commit='a' * 40, directory=self.root)
        body = b'{ "latitude": 52.5, "longitude": 9.25, "hourly": {"time": []} }'
        response = self.response(body)
        response.url = 'https://ensemble-api.open-meteo.com/v1/ensemble?models=icon_d2'
        capture.response(response)
        logical = hashlib.sha256(json.dumps(json.loads(body), sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        report = capture.bind({'ensemble_hourly_source': {'response_sha256': logical,
            'requested_coordinate': {'latitude': 52.5, 'longitude': 9.25},
            'returned_coordinate': {'latitude': 52.5, 'longitude': 9.25}}})
        item = report['captures'][0]
        self.assertEqual(item['sha256'], hashlib.sha256(body).hexdigest())
        self.assertNotEqual(item['sha256'], logical)
        self.assertEqual(item['binding_digest_basis'], 'canonical_json')
        self.assertTrue(report['all_captures_have_point_bindings'])
        self.assertEqual(response.content, body)

    def test_sdk_request_and_returned_metadata_cannot_mutate_bound_capture(self):
        source = self.root / 'sdk.grib2'
        source.write_bytes(b'GRIB assembled request')
        request = {'step': [0, 12], 'param': ['10u', '10v'], 'target': '/temporary/path'}
        item = self.capture.retain(source, url='https://data.ecmwf.int/forecasts/',
            kind='assembled_native_request', captured_at_utc='2026-10-04T00:00:00Z', request=request)
        request['step'].append(24)
        item['metadata']['sdk_request']['param'].append('changed')
        bound = self.capture.captures[0]
        self.assertEqual(bound['metadata']['sdk_request'], {'step': [0, 12], 'param': ['10u', '10v']})
        self.assertEqual(self.capture.store.manifest(bound['parent'])['metadata'], bound['metadata'])


if __name__ == '__main__':
    unittest.main()
