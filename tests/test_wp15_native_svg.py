"""Native capture reuses original HTTP bytes and the existing delivery codec."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import requests
from mardorf_collector.wp13.native_svg_v1 import capture_responses, prepare
from mardorf_collector.wp13.store_v1 import read_delivery

ROOT = Path(__file__).resolve().parents[1]


class NativeSVGTests(unittest.TestCase):
    def test_current_capture_has_no_extra_request_or_persisted_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            provider = SimpleNamespace(S=requests.Session())
            captures = capture_responses(provider, folder)
            response = requests.Response()
            response.status_code = 200
            response.url = 'https://api.weatherlink.com/v2/current/42374?api-key=DO_NOT_PERSIST'
            response._content = (ROOT / 'tests/fixtures/wp13/svg_current_v1.json').read_bytes()
            provider.S.hooks['response'][-1](response)
            self.assertEqual(len(captures), 1)
            self.assertNotIn('DO_NOT_PERSIST', (Path(folder) / 'captures.json').read_text())
            self.assertEqual(captures[0]['body_sha256'], hashlib.sha256(response.content).hexdigest())
            prepared = prepare(captures, directory=folder, commit='a' * 40)
            result = read_delivery(Path(folder) / 'prepared', prepared[0]['receipt_id'])
            self.assertEqual(result['raw_bytes']['raw-0'], response.content)
            self.assertEqual(result['envelope']['capture']['collector_commit_sha'], 'a' * 40)
            self.assertTrue(result['fields'])

    def test_historic_request_bounds_and_original_bytes_are_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            provider = SimpleNamespace(S=requests.Session())
            captures = capture_responses(provider, folder)
            body = (ROOT / 'tests/fixtures/wp13/svg_historic_v1.json').read_bytes()
            payload = json.loads(body)
            timestamps = [row['ts'] for sensor in payload['sensors'] for row in sensor['data']]
            begin, end = min(timestamps), max(timestamps) + 300
            response = requests.Response()
            response.status_code = 200
            response.url = ('https://api.weatherlink.com/v2/historic/42374?api-key=PRIVATE'
                            f'&start-timestamp={begin - 1}&end-timestamp={end - 1}')
            response._content = body
            provider.S.hooks['response'][-1](response)
            self.assertEqual(captures[0]['window']['start_utc'],
                             datetime.fromtimestamp(begin, timezone.utc).isoformat().replace('+00:00', 'Z'))
            prepared = prepare(captures, directory=folder, commit='b' * 40)
            result = read_delivery(Path(folder) / 'prepared', prepared[0]['receipt_id'])
            self.assertEqual(result['raw_bytes']['raw-0'], body)
            self.assertTrue(result['fields'])

    def test_failed_http_is_never_a_successful_capture(self):
        with tempfile.TemporaryDirectory() as folder:
            provider = SimpleNamespace(S=requests.Session())
            captures = capture_responses(provider, folder)
            response = requests.Response()
            response.url = 'https://api.weatherlink.com/v2/current/42374'
            response.status_code = 500
            provider.S.hooks['response'][-1](response)
            self.assertEqual(captures, [])
            with self.assertRaisesRegex(ValueError, 'No successful'):
                prepare(captures, directory=folder, commit='b' * 40)


if __name__ == '__main__':
    unittest.main()
