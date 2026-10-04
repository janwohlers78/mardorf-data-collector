"""Retain the actual inputs of the existing model acquisition, without new GETs.

This captures bytes and binds existing native point metadata by source hash.
It does not normalize meteorological values or select a second model pipeline.
"""
from datetime import datetime, timezone
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import threading
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from mardorf_collector.storage.archive import canonical, decode
from mardorf_collector.storage.objects import LocalObjects, ObjectError, file_lock
from mardorf_collector.storage.parents import ParentStore

WORK = Path('work/model_originals_v1')
PREFIX = 'weather/provider-originals/wp15'
HOSTS = {'opendata.dwd.de', 'nomads.ncep.noaa.gov', 'ensemble-api.open-meteo.com',
         'api.open-meteo.com', 'data.ecmwf.int', 'ai4edataeuwest.blob.core.windows.net',
         'storage.googleapis.com'}
MIRRORS = {'azure': 'https://ai4edataeuwest.blob.core.windows.net/ecmwf/',
           'google': 'https://storage.googleapis.com/ecmwf-open-data/',
           'ecmwf': 'https://data.ecmwf.int/forecasts/'}
MAX_RESPONSE_BYTES = 512 * 1024**2
MAX_STAGE_BYTES = 8 * 1024**3


def safe_url(url):
    parsed = urlsplit(url)
    if parsed.scheme != 'https' or parsed.hostname not in HOSTS or parsed.port not in (None, 443) or parsed.username or parsed.password:
        raise ObjectError('Original model origin is not an allowed provider')
    # Authentication is not meteorological provenance. Never persist credentials.
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
             if not any(word in k.lower() for word in ('key', 'token', 'secret', 'signature', 'credential', 'password'))
             and k.lower() not in ('sig', 'sv', 'se', 'sp', 'sr')]
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ''))


class Capture:
    def __init__(self, *, model, stage, commit, directory=WORK):
        if model not in ('ICON-D2', 'ICON-EU', 'ICON-D2-EPS', 'GFS', 'GEFS-control', 'ECMWF-IFS'):
            raise ObjectError('Configured model identity required')
        if not isinstance(commit, str) or len(commit) != 40 or any(c not in '0123456789abcdef' for c in commit):
            raise ObjectError('Exact model capture code commit required')
        if stage not in ('base', 'extension', 'tier_a', 'full_horizon', 'full_members'):
            raise ObjectError('Configured model capture stage required')
        self.model, self.stage, self.commit = model, stage, commit
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.backend = LocalObjects(self.directory / 'objects')
        self.store = ParentStore(self.backend, prefix=PREFIX)
        self.captures = []
        self.errors = []
        self.bytes = 0
        self.lock = threading.Lock()

    def retain(self, path, *, url, kind, captured_at_utc, request=None, content_range=None,
               canonical_json_sha256=None):
        if kind not in ('http_response', 'assembled_native_request'):
            raise ObjectError('Explicit original source kind required')
        try:
            stamp = datetime.fromisoformat(captured_at_utc)
            if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
                raise ValueError('UTC capture time required')
        except (TypeError, ValueError) as exc:
            raise ObjectError('UTC capture time required') from exc
        size = Path(path).stat().st_size
        with self.lock, file_lock(self.directory / 'capture.lock'):
            if not 0 < size <= MAX_RESPONSE_BYTES or self.bytes + size > MAX_STAGE_BYTES:
                raise ObjectError('Model original stage byte budget exceeded')
            metadata = {'acquisition_model': self.model, 'stage': self.stage,
                'source_url': safe_url(url), 'source_kind': kind,
                'captured_at_utc': captured_at_utc, 'producer_commit': self.commit}
            if request is not None:
                metadata['sdk_request'] = deepcopy({key: request[key] for key in
                    ('date', 'time', 'stream', 'type', 'step', 'param') if key in request})
            if content_range is not None:
                metadata['content_range'] = content_range
            if canonical_json_sha256 is not None:
                metadata['canonical_json_sha256'] = canonical_json_sha256
            parent = self.store.write_file(path, metadata=metadata)
            manifest = self.store.manifest(parent)
            item = {'parent': parent.json(), 'sha256': manifest['sha256'],
                    'bytes': manifest['bytes'], 'metadata': metadata}
            self.bytes += size
            self.captures.append(item)
            target = self.directory / (parent.sha256 + '.capture.json')
            target.write_bytes(canonical(item))
            return deepcopy(item)

    def response(self, response, *args, **kwargs):
        parsed = urlsplit(response.url)
        if parsed.hostname not in HOSTS or response.status_code not in (200, 206):
            return
        is_model = (parsed.path.endswith(('.bz2', '.grib2', '.grib'))
                    or '/cgi-bin/filter_' in parsed.path
                    or parsed.hostname in ('ensemble-api.open-meteo.com', 'api.open-meteo.com'))
        if not is_model:
            return
        # The existing consumers require response.content. Use that single cached
        # body instead of consuming raw streams or maintaining a second decoder.
        # Requests errors propagate exactly as before; storage errors are separate.
        try:
            if response.headers.get('Content-Encoding', 'identity') not in ('', 'identity'):
                raise ObjectError('Original model HTTP encoding unsupported')
            length = response.headers.get('Content-Length')
            if length is not None and not 0 < int(length) <= MAX_RESPONSE_BYTES:
                raise ObjectError('Original model declared response budget exceeded')
        except (ObjectError, ValueError) as exc:
            self.error(exc, parsed.hostname)
            return response
        body = response.content
        try:
            if not 0 < len(body) <= MAX_RESPONSE_BYTES:
                raise ObjectError('Original model response budget exceeded')
            if length is not None and int(length) != len(body):
                raise ObjectError('Original model response length mismatch')
            with tempfile.NamedTemporaryFile(dir=self.directory) as stream:
                stream.write(body)
                stream.flush()
                json_sha = None
                if parsed.hostname in ('ensemble-api.open-meteo.com', 'api.open-meteo.com'):
                    # Existing EPS metadata hashes canonical JSON rather than
                    # wire bytes. Preserve both identities and label the join.
                    payload = decode(body)
                    json_sha = hashlib.sha256(json.dumps(payload, sort_keys=True,
                        separators=(',', ':'), allow_nan=False).encode()).hexdigest()
                self.retain(stream.name, url=response.url, kind='http_response',
                    captured_at_utc=datetime.now(timezone.utc).isoformat(),
                    content_range=response.headers.get('Content-Range'),
                    canonical_json_sha256=json_sha)
        except (ObjectError, OSError, ValueError, TypeError) as exc:
            self.error(exc, parsed.hostname)
        return response

    def error(self, exc, host):
        with self.lock:
            self.errors.append({'error_type': type(exc).__name__, 'provider_host': host})

    def attach(self, sessions):
        for session in {id(s): s for s in sessions}.values():
            session.headers['Accept-Encoding'] = 'identity'
            hooks = session.hooks.setdefault('response', [])
            if self.response not in hooks:
                hooks.append(self.response)

    def bind(self, snapshot):
        """Keep source/member/header/interval/point metadata exactly as emitted."""
        by_source = {}
        for row in snapshot.get('models', {}).get(self.model, []):
            values = row.get('values', {})
            for parameter, items in values.items():
                for item in items if isinstance(items, list) else [items] if isinstance(items, dict) else []:
                    digest = item.get('source_sha256')
                    if not isinstance(digest, str):
                        continue
                    by_source.setdefault(digest, []).append({'model': self.model,
                        'run_time_utc': row.get('run_time_utc'),
                        'valid_time_utc': row.get('valid_time_utc'),
                        'forecast_lead_hours': row.get('forecast_lead_hours'),
                        'requested_point': snapshot.get('spot'),
                        'actual_point': row.get('forecast_coordinate_or_grid_point'),
                        'native_parameter': parameter, 'native': item})
        ensemble = snapshot.get('ensemble_hourly_source')
        if self.model == 'ICON-D2-EPS' and isinstance(ensemble, dict):
            digest = ensemble.get('response_sha256')
            if isinstance(digest, str):
                by_source.setdefault(digest, []).append({'model': self.model,
                    'requested_point': ensemble.get('requested_coordinate'),
                    'actual_point': ensemble.get('returned_coordinate'),
                    'native_ensemble_source': ensemble})
        bound = []
        for item in self.captures:
            logical = item['metadata'].get('canonical_json_sha256', item['sha256'])
            bound.append(deepcopy(dict(item, point_fields=by_source.get(logical, []),
                binding_digest_basis='canonical_json' if logical != item['sha256'] else 'response_bytes')))
        report = {'schema_version': 1, 'artifact_version': 'wp15-model-original-stage-v1',
            'model': self.model, 'stage': self.stage, 'captures': bound,
            'capture_errors': self.errors, 'provider_requests_added': 0,
            'all_captures_have_point_bindings': bool(bound) and not self.errors and all(item['point_fields'] for item in bound),
            'native_reader_admission': 'NOT_QUALIFIED',
            'qualification': 'original_capture_and_native_metadata_only; no scientific release'}
        target = self.directory / (hashlib.sha256(canonical(report)).hexdigest() + '.stage.json')
        target.write_bytes(canonical(report))
        return report
