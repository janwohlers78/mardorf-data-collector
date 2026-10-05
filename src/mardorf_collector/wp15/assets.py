"""Materialize only hash-bound baseline contracts, never private runtime code."""
import atexit
import hashlib
from pathlib import Path, PurePosixPath
import shutil
import tempfile

from ..wp13.core_v1 import strict_json

ROOT = Path(__file__).resolve().parents[3]
_root = None


def reference_root():
    global _root
    if _root is not None:
        return _root
    document = strict_json((ROOT / 'config/dev03_wp15_reference_bundle_v1.json').read_bytes())
    if document.get('artifact_version') != 'wp15-format-reference-bundle-v1':
        raise ValueError('Unsupported reference bundle')
    destination = Path(tempfile.mkdtemp(prefix='mardorf-public-contracts-'))
    try:
        for name, item in document['files'].items():
            parts = PurePosixPath(name).parts
            if (not parts or parts[0] not in ('config', 'src') or '..' in parts
                    or str(PurePosixPath(name)) != name):
                raise ValueError('Invalid reference path')
            body = item['content'].encode('utf-8')
            if hashlib.sha256(body).hexdigest() != item['sha256']:
                raise ValueError('Reference predecessor mismatch')
            path = destination / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
    except Exception:
        shutil.rmtree(destination)
        raise
    atexit.register(shutil.rmtree, destination, ignore_errors=True)
    _root = destination
    return destination
