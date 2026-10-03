"""One explicit weather authority for acquisition decisions; no failure fallback."""
from functools import lru_cache
import json
from pathlib import Path

from mardorf_collector.paths import REPOSITORY_ROOT


def profile(root=REPOSITORY_ROOT):
    path = Path(root) / 'config/dev03_cloud_runtime_v1.json'
    if not path.exists():
        return None  # Compatibility with releases preceding cloud storage.
    value = json.loads(path.read_text())
    if (value.get('schema_version') != 1 or
            value.get('artifact_version') != 'dev03-cloud-runtime-v1' or
            type(value.get('production_enabled')) is not bool):
        raise ValueError('Explicit weather authority profile required')
    return value


def enabled(repository, *, root=REPOSITORY_ROOT):
    value = profile(root)
    if value is None or not value['production_enabled']:
        return False
    if repository != value['private_repository']:
        raise ValueError('Weather authority repository mismatch')
    return True


@lru_cache(maxsize=1)
def cloud():
    from mardorf_collector.storage.runtime import load_runtime
    # Public collectors already have a restricted WeatherWriter. Its read scope
    # is used here; private consumers require their separate read-only role.
    return load_runtime(REPOSITORY_ROOT)


def read(repository, path, *, reader=None):
    if not enabled(repository):
        raise ValueError('External weather authority is inactive')
    body = (reader or cloud()).read(path, required=False)
    if body is None:
        raise FileNotFoundError('Required external acquisition state absent')
    return body


def due_pointer(repository, kind):
    if kind not in ('models', 'svg', 'wunstorf', 'etnw', 'secondary'):
        raise ValueError('Unsupported acquisition kind')
    if enabled(repository):
        # Small Git control documents contain source clocks and immutable B2
        # references only. The cheap watchdog needs no SDK or weather download.
        return f'config/cloud_refs/collector_{kind}_v1.json', 'generated_at_utc'
    if kind == 'secondary':
        return ('data/inbox/public_collector/transfer_receipts/secondary/latest.json',
                'source_generated_at_utc')
    return f'data/inbox/public_collector/integrity/{kind}/latest_success.json', 'generated_at_utc'
