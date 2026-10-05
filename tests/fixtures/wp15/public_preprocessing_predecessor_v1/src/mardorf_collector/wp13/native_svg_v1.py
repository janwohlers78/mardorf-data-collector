"""Retain original responses from the existing SVG fetch, without extra requests."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .core_v1 import ContractsV1, Response, canonical, identify, stamp
from .collector_v2 import CollectorV2
from .status_codec_v1 import compact_outcomes
from .store_v1 import write_delivery

ROOT = Path(__file__).resolve().parents[3]
WORK = Path('work/native_svg_v1')


def original_body(response, *, limit=8 * 1024**2):
    """Bound the wire read before allocation; retain normal requests JSON access."""
    if response.headers.get('Content-Encoding', 'identity') not in ('', 'identity'):
        raise ValueError('Native SVG original transport encoding unsupported')
    declared = response.headers.get('Content-Length')
    if declared is not None and (int(declared) < 0 or int(declared) > limit):
        raise ValueError('Native SVG declared response budget exceeded')
    if response._content is False:
        body = bytearray()
        for chunk in response.raw.stream(65536, decode_content=False):
            if len(body) + len(chunk) > limit:
                raise ValueError('Native SVG response budget exceeded')
            body.extend(chunk)
        response._content = bytes(body)
        response._content_consumed = True
    body = response.content
    if len(body) > limit:
        raise ValueError('Native SVG response budget exceeded')
    if declared is not None and int(declared) != len(body):
        raise ValueError('Native SVG response length mismatch')
    return body


def capture_responses(provider, destination=WORK):
    """Use the provider's single session and its original retries/authentication."""
    destination = Path(destination)
    captures = []
    def capture(response, *args, **kwargs):
        parsed = urlsplit(response.url)
        if parsed.hostname != 'api.weatherlink.com' or response.status_code != 200:
            return
        if parsed.path not in ('/v2/current/42374', '/v2/historic/42374'):
            return
        body = original_body(response)
        digest = hashlib.sha256(body).hexdigest()
        destination.mkdir(parents=True, exist_ok=True)
        (destination / (digest + '.bin')).write_bytes(body)
        query = parse_qs(parsed.query)
        window = None
        if '/historic/' in parsed.path:
            # WP13 windows translate to inclusive provider request bounds -1s.
            window = {name: stamp(datetime.fromtimestamp(int(query[key][0]) + 1, timezone.utc))
                      for name, key in (('start_utc', 'start-timestamp'), ('end_utc', 'end-timestamp'))}
        captures.append({'url': 'https://api.weatherlink.com' + parsed.path,
                         'body_sha256': digest, 'bytes': len(body),
                         'observed_at_utc': stamp(datetime.now(timezone.utc)),
                         'window': window})
        (destination / 'captures.json').write_bytes(canonical(captures))
    provider.S.headers['Accept-Encoding'] = 'identity'
    provider.S.hooks.setdefault('response', []).append(capture)
    return captures


def prepare(captures, *, directory=WORK, commit):
    """One configured native job per original provider response."""
    contracts = ContractsV1()
    contracts.collection_enabled = True
    station = contracts.tables['stations']['SVG-42374-v1']
    producer = CollectorV2(contracts=contracts, collector_commit_sha=commit)
    prepared = []
    for capture in captures:
        raw = (Path(directory) / (capture['body_sha256'] + '.bin')).read_bytes()
        if len(raw) != capture['bytes'] or hashlib.sha256(raw).hexdigest() != capture['body_sha256']:
            raise ValueError('Captured original SVG body changed')
        job = identify({'schema_version': 1, 'artifact_version': 'dev03-wp13-job-v1',
            'profile_id': 'svg-general-weather-development-v2', 'site_ids': [station['site_id']],
            'provider_binding_id': station['provider_id'],
            'kind': 'observation_historic' if capture['window'] else 'observation_current',
            'context': 'historical' if capture['window'] else 'prospective',
            'station_id': station['id'], 'sensor_id': 'svg-weatherlink-48-v1',
            'window': capture['window'], 'sources': [{'url': capture['url'], 'compression': 'none',
                'media_type': 'application/json', 'native_parameter': None, 'model_id_native': None,
                'product_id_native': None, 'expected_run_time_utc': None, 'role': 'data'}],
            'target_ids': [], 'refresh_id': None}, 'job_id')
        result = producer.collect(job, responses=[Response(raw, capture['observed_at_utc'])])
        if not result['envelope'] or not result['fields']:
            raise ValueError('No accepted native SVG response')
        result = compact_outcomes(result)
        prepared.append(write_delivery(Path(directory) / 'prepared', result))
    if not prepared:
        raise ValueError('No successful original SVG captures')
    (Path(directory) / 'prepared.json').write_bytes(canonical(prepared))
    return prepared


def fetch():
    from mardorf_collector.providers import fetch_svg_weatherlink as provider
    import os
    import subprocess
    captures = capture_responses(provider)
    result = provider.main()
    commit = os.environ.get('GITHUB_SHA') or subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    prepare(captures, commit=commit)
    return result


def publish(*, cloud=None, directory=WORK):
    from mardorf_collector.storage.runtime import load_runtime
    from mardorf_collector.runtime.cloud_environment import environment
    from .store_v1 import read_delivery
    runtime = cloud or load_runtime(Path.cwd(), environ=environment(Path.cwd()))
    ready = json.loads(runtime.read('data/weather_native/consumer_readiness_v1.json'))
    if ready.get('available') is not True or ready.get('configuration_sha256') != ContractsV1().configuration['sha256']:
        raise ValueError('Deployed native consumer readiness required')
    directory = Path(directory)
    prepared = json.loads((directory / 'prepared.json').read_bytes())
    reports = []
    for item in prepared:
        prepared_directory = directory / 'prepared' / item['receipt_id']
        verified = read_delivery(directory / 'prepared', item['receipt_id'])
        prefix = 'data/inbox/native_wp15/svg/' + item['receipt_id'] + '/'
        changes = {prefix + p.relative_to(prepared_directory).as_posix(): p.read_bytes()
                   for p in prepared_directory.rglob('*') if p.is_file()}
        changes[prefix + 'ready.json'] = canonical({'schema_version': 1,
            'artifact_version': 'wp15-native-ingress-v1', 'receipt_id': item['receipt_id'],
            'configuration_sha256': ready['configuration_sha256'],
            'envelope_id': verified['envelope']['envelope_id'],
            'files': {name.removeprefix(prefix): {'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)}
                      for name, body in changes.items()}})
        def merge(current, incoming):
            for path, body in incoming.items():
                previous = current.read(path, required=False)
                if previous is not None and previous != body:
                    raise ValueError('Immutable native SVG ingress collision')
            return {path: body for path, body in incoming.items() if current.read(path, required=False) != body}
        reports.append(runtime.publish(changes, metadata={'channel': 'wp15-native-svg'}, merge=merge))
    print('WP15_NATIVE_SVG=' + json.dumps({'deliveries': len(reports), 'provider_requests_added': 0,
                                         'weather_git_bytes_written': 0}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('fetch', 'publish'))
    args = parser.parse_args()
    if args.operation == 'publish':
        publish()
    else:
        raise SystemExit(fetch())


if __name__ == '__main__':
    main()
