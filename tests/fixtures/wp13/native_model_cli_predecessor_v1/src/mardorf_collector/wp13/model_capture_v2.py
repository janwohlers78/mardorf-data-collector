"""Explicit session capture successor for the existing model acquisition calls.

The process-local router is attached only to named provider sessions. It never
patches requests, fetches a provider, or changes the provider's returned body.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from urllib.parse import urlsplit
import requests

from .model_originals_v1 import Capture, MIRRORS, WORK, safe_url, MAX_STAGE_BYTES
from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import ObjectError, file_lock

MAX_MODEL_WIRE_BYTES = 2 * 1024**3


class CaptureV2(Capture):
    """Explicit large SDK-target successor; V1 HTTP capture stays bounded."""
    def retain(self, path, *, url, kind, captured_at_utc, request=None,
               content_range=None, canonical_json_sha256=None):
        if kind != 'assembled_native_request':
            return super().retain(path, url=url, kind=kind, captured_at_utc=captured_at_utc,
                request=request, content_range=content_range, canonical_json_sha256=canonical_json_sha256)
        stamp = datetime.fromisoformat(captured_at_utc)
        if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
            raise ObjectError('UTC assembled model capture required')
        size = Path(path).stat().st_size
        with self.lock, file_lock(self.directory / 'capture.lock'):
            if not 0 < size <= MAX_MODEL_WIRE_BYTES or self.bytes + size > MAX_STAGE_BYTES:
                raise ObjectError('Explicit V2 assembled model byte budget exceeded')
            metadata = {'acquisition_model': self.model, 'stage': self.stage,
                'source_url': safe_url(url), 'source_kind': kind,
                'captured_at_utc': captured_at_utc, 'producer_commit': self.commit,
                'sdk_request': deepcopy({key: request[key] for key in
                    ('date', 'time', 'stream', 'type', 'step', 'param') if key in (request or {})})}
            parent = self.store.write_file(path, metadata=metadata)
            manifest = self.store.manifest(parent)
            item = {'parent': parent.json(), 'sha256': manifest['sha256'], 'bytes': manifest['bytes'], 'metadata': metadata}
            self.bytes += size
            self.captures.append(item)
            (self.directory / (parent.sha256 + '.capture.json')).write_bytes(canonical(item))
            return deepcopy(item)

    def error(self, exc, host):
        super().error(exc, host)
        report = {'model': self.model, 'stage': self.stage, 'error_type': type(exc).__name__,
                  'provider_host': host, 'captured_at_utc': datetime.now(timezone.utc).isoformat()}
        body = canonical(report)
        import hashlib
        try:
            (self.directory / (hashlib.sha256(body).hexdigest() + '.error.json')).write_bytes(body)
        except OSError:
            # The final stage report also preserves errors. Valid legacy bodies
            # remain usable even when the independent capture disk is unavailable.
            pass

_active = None


class Router:
    def __init__(self, stage, commit, directory=WORK):
        self.stage, self.commit, self.directory = stage, commit, Path(directory)
        self.captures = {}
        self.lock = threading.Lock()

    def capture(self, model):
        with self.lock:
            if model not in self.captures:
                self.captures[model] = CaptureV2(model=model, stage=self.stage,
                    commit=self.commit, directory=self.directory)
            return self.captures[model]

    def response(self, response, *args, **kwargs):
        url = urlsplit(response.url)
        model = ('ICON-D2-EPS' if url.hostname in ('ensemble-api.open-meteo.com', 'api.open-meteo.com')
                 or '/icon-d2-eps/' in url.path else
                 'ICON-EU' if '/icon-eu/' in url.path else
                 'ICON-D2' if '/icon-d2/' in url.path else
                 'GEFS-control' if 'gefs' in url.path else
                 'GFS' if 'gfs' in url.path else None)
        if model:
            return self.capture(model).response(response, *args, **kwargs)
        return response

    def attach(self, session):
        session.headers['Accept-Encoding'] = 'identity'
        hooks = session.hooks.setdefault('response', [])
        if self.response not in hooks:
            hooks.append(self.response)
        return session

    def finish(self, snapshot):
        for capture in self.captures.values():
            capture.bind(snapshot)


def session():
    """Called explicitly at the existing local-session construction sites."""
    result = requests.Session()
    return _active.attach(result) if _active else result


def retain_sdk(target, mirror, request):
    """Retain the successful assembled SDK result before its caller deletes it."""
    if _active is None:
        return
    capture = _active.capture('ECMWF-IFS')
    try:
        capture.retain(target, url=MIRRORS[mirror], kind='assembled_native_request',
            captured_at_utc=datetime.now(timezone.utc).isoformat(), request=request)
    except (OSError, ValueError, RuntimeError) as exc:
        capture.error(exc, urlsplit(MIRRORS[mirror]).hostname)


def acquire(operation, arguments):
    global _active
    routes = {'provider': ('providers.provider_fetch', None),
              'tier_a': ('runtime.collect_icon_tier_a', 'tier_a'),
              'full_horizon': ('runtime.collect_full_horizon', 'full_horizon'),
              'full_members': ('providers.gefs_full_members', 'full_members')}
    module_name, stage = routes[operation]
    if stage is None:
        stage = arguments[arguments.index('--stage') + 1]
    commit = os.environ.get('GITHUB_SHA') or subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], text=True).strip()
    _active = Router(stage, commit)
    module = importlib.import_module('mardorf_collector.' + module_name)
    # The protected provider sources and their package bridges remain intact.
    # These are the same explicitly owned sessions that the original calls use.
    for name in ('providers.fetch_model_data', 'providers.fetch_dwd_additional_models',
                 'providers.fetch_extra_models', 'runtime.extend_model_horizon'):
        provider = importlib.import_module('mardorf_collector.' + name)
        if hasattr(provider, 'S'):
            _active.attach(provider.S)
    sys.argv = [module.__file__, *arguments]
    try:
        return module.main()
    finally:
        snapshot = Path(os.environ.get('COLLECTOR_MODEL_FILE', 'work/model_snapshot.json'))
        _active.finish(json.loads(snapshot.read_bytes()) if snapshot.exists() else {})
        _active = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('provider', 'tier_a', 'full_horizon', 'full_members'))
    args, rest = parser.parse_known_args()
    return acquire(args.operation, rest)


if __name__ == '__main__':
    raise SystemExit(main())
