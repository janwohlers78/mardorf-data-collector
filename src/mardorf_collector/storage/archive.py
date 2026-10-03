"""Immutable packed archives: exact originals, bounded selective reads, no latest CAS."""
import gzip
import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath
import tempfile
import zlib

from .objects import ObjectError, ObjectRef, safe_key


VERSION = 'weather-packed-snapshot-v1'


def digest(body):
    return hashlib.sha256(body).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode()


def decode(body):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ObjectError('Duplicate archive JSON key')
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ObjectError('Nonfinite archive JSON')))


def file_path(value):
    if (not isinstance(value, str) or len(value.encode()) > 4096 or '\\' in value or '\0' in value
            or value.startswith('/') or str(PurePosixPath(value)) != value
            or any(p in ('', '.', '..') for p in value.split('/'))):
        raise ObjectError('Unsafe archive file path')
    return value


def git_blob(body):
    return hashlib.sha1(b'blob ' + str(len(body)).encode() + b'\0' + body).hexdigest()


class PackedArchive:
    """Small files share verified packs; every range also verifies original bytes.

    Snapshot roots are caller-pinned immutable objects. A directory/event journal
    may reference roots, but a mutable unverified latest pointer is never assumed.
    """
    def __init__(self, backend, *, prefix, pack_bytes=32*1024**2,
                 file_bytes=64*1024**2, shard_records=256):
        self.backend = backend
        self.prefix = safe_key(prefix.rstrip('/'))
        if (type(pack_bytes) is not int or not 0 < pack_bytes <= 32*1024**2 or
                type(file_bytes) is not int or not 0 < file_bytes <= 64*1024**2 or
                type(shard_records) is not int or not 0 < shard_records <= 256):
            raise ObjectError('Invalid archive budget')
        self.pack_bytes, self.file_bytes, self.shard_records = pack_bytes, file_bytes, shard_records

    def put_json(self, value, kind):
        body = canonical(value)
        if len(body) > 1024**2:
            raise ObjectError('Archive manifest exceeds budget')
        return self.backend.put_bytes(f'{self.prefix}/{kind}/{digest(body)}', body)

    def read_json(self, ref):
        ref = ObjectRef.parse(ref) if isinstance(ref, dict) else ref
        if not ref.key.startswith(self.prefix+'/') or ref.bytes > 1024**2:
            raise ObjectError('Archive manifest scope/budget mismatch')
        return decode(self.backend.get_bytes(ref))

    def export(self, files, *, metadata):
        """files: sorted unique (relative path, bytes) iterator; bounded pack staging."""
        shards, records, pack_records = [], [], []
        previous, count, original_bytes = None, 0, 0
        with tempfile.TemporaryDirectory(prefix='weather-pack-') as temporary:
            pack = Path(temporary)/'pack'
            stream = pack.open('wb')
            def flush_records():
                nonlocal records
                if not records:
                    return
                ref = self.put_json({'schema_version':1, 'records':records}, 'indexes')
                shards.append({'first':records[0]['path'], 'last':records[-1]['path'],
                               'count':len(records), 'ref':ref.json()})
                records = []
            def flush_pack():
                nonlocal stream, pack_records
                stream.close()
                if pack_records:
                    sha = hashlib.sha256()
                    with pack.open('rb') as source:
                        for block in iter(lambda:source.read(1024**2), b''):
                            sha.update(block)
                    ref = self.backend.put_file(f'{self.prefix}/packs/{sha.hexdigest()}', pack)
                    for item in pack_records:
                        records.append(dict(item, object=ref.json()))
                        if len(records) == self.shard_records:
                            flush_records()
                pack_records = []
                stream = pack.open('wb')
            try:
                for name, body in files:
                    name = file_path(name)
                    if previous is not None and name <= previous:
                        raise ObjectError('Archive paths must be unique and sorted')
                    previous = name
                    if not isinstance(body, bytes) or len(body) > self.file_bytes:
                        raise ObjectError('Archive original exceeds budget')
                    compressed = gzip.compress(body, compresslevel=3, mtime=0)
                    encoded, codec = (compressed, 'gzip') if len(compressed) < len(body) else (body, 'none')
                    if len(pack_records) >= 4096 or (stream.tell() and stream.tell()+len(encoded) > self.pack_bytes):
                        flush_pack()
                    if len(encoded) > self.pack_bytes:
                        raise ObjectError('Archive file requires explicit larger-file segmentation')
                    offset = stream.tell()
                    stream.write(encoded)
                    pack_records.append({'path':name, 'bytes':len(body), 'sha256':digest(body),
                                         'git_blob_sha1':git_blob(body), 'offset':offset,
                                         'stored_bytes':len(encoded), 'codec':codec})
                    count += 1
                    original_bytes += len(body)
                flush_pack()
                flush_records()
                root = {'schema_version':1, 'artifact_version':VERSION,
                        'metadata':metadata, 'file_count':count,
                        'original_bytes':original_bytes, 'shards':shards}
                # Only a complete, validated root is published; partial packs are invisible.
                self.validate_root(root)
                return self.put_json(root, 'snapshots')
            finally:
                stream.close()

    def validate_root(self, root):
        if (not isinstance(root, dict) or not isinstance(root.get('shards'), list) or
                type(root.get('schema_version')) is not int or root.get('schema_version') != 1 or
                root.get('artifact_version') != VERSION or not isinstance(root.get('metadata'), dict) or
                type(root.get('file_count')) is not int or root['file_count'] < 0 or
                type(root.get('original_bytes')) is not int or root['original_bytes'] < 0):
            raise ObjectError('Invalid archive snapshot')
        total, previous = 0, None
        for shard in root['shards']:
            first, last = file_path(shard['first']), file_path(shard['last'])
            if (first > last or (previous is not None and first <= previous) or
                    type(shard['count']) is not int or not 0 < shard['count'] <= self.shard_records):
                raise ObjectError('Invalid archive shard order/count')
            ref = ObjectRef.parse(shard['ref'])
            if not ref.key.startswith(self.prefix+'/indexes/') or ref.bytes > 1024**2:
                raise ObjectError('Invalid archive shard reference')
            previous = last
            total += shard['count']
        if total != root['file_count']:
            raise ObjectError('Incomplete archive file count')

    def records(self, snapshot, *, prefixes=('',), max_files=100000):
        root = self.read_json(snapshot)
        self.validate_root(root)
        if type(max_files) is not int or not 0 < max_files <= 100000:
            raise ObjectError('Invalid selective file budget')
        prefixes = tuple(prefixes)
        if not prefixes or any(not isinstance(p, str) or (p and file_path(p.rstrip('/')) != p.rstrip('/')) for p in prefixes):
            raise ObjectError('Invalid archive selection')
        count = 0
        for shard in root['shards']:
            if not any(not p or (shard['last'] >= p and shard['first'] < p+'\U0010ffff') for p in prefixes):
                continue
            document = self.read_json(shard['ref'])
            items = document['records']
            if len(items) != shard['count'] or [x['path'] for x in items] != sorted({x['path'] for x in items}):
                raise ObjectError('Invalid selected archive shard')
            if not items or items[0]['path'] != shard['first'] or items[-1]['path'] != shard['last']:
                raise ObjectError('Archive shard boundaries mismatch')
            for item in items:
                self.validate_item(item)
                if any(item['path'].startswith(p) for p in prefixes):
                    count += 1
                    if count > max_files:
                        raise ObjectError('Selective file count exceeded')
                    yield item

    def validate_item(self, item):
        if not isinstance(item, dict):
            raise ObjectError('Invalid archive original reference')
        try:
            file_path(item['path'])
            ref = ObjectRef.parse(item['object'])
            if (not ref.key.startswith(self.prefix+'/packs/') or ref.bytes > self.pack_bytes or
                    type(item['offset']) is not int or type(item['stored_bytes']) is not int or
                    item['offset'] < 0 or item['stored_bytes'] < 0 or
                    item['offset']+item['stored_bytes'] > ref.bytes or
                    type(item['bytes']) is not int or not 0 <= item['bytes'] <= self.file_bytes or
                    item['codec'] not in ('none','gzip') or
                    not isinstance(item['sha256'], str) or not re.fullmatch('[0-9a-f]{64}',item['sha256']) or
                    not isinstance(item['git_blob_sha1'], str) or not re.fullmatch('[0-9a-f]{40}',item['git_blob_sha1']) or
                    (item['codec'] == 'none' and item['stored_bytes'] != item['bytes'])):
                raise ObjectError('Invalid archive original reference')
        except (KeyError, TypeError) as exc:
            raise ObjectError('Invalid archive original reference') from exc
        return ref

    def read_file(self, item):
        ref = self.validate_item(item)
        if item['stored_bytes']:
            body = self.backend.get_range(ref, item['offset'], item['offset']+item['stored_bytes'])
        else:
            body = b''
        if item['codec'] == 'gzip':
            decompressor = zlib.decompressobj(31)
            body = decompressor.decompress(body, item['bytes']+1)
            if not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
                raise ObjectError('Invalid bounded archive compression')
        if len(body) != item['bytes'] or digest(body) != item['sha256'] or git_blob(body) != item['git_blob_sha1']:
            raise ObjectError('Archive original identity mismatch')
        return body

    def hydrate(self, snapshot, target, *, prefixes, max_bytes=512*1024**2):
        if type(max_bytes) is not int or not 0 <= max_bytes <= 4*1024**3:
            raise ObjectError('Invalid hydration budget')
        target = Path(target).absolute()
        if any(p.is_symlink() for p in (target,*target.parents)):
            raise ObjectError('Symlink archive destination')
        items = list(self.records(snapshot, prefixes=prefixes))
        if sum(x['bytes'] for x in items) > max_bytes:
            raise ObjectError('Hydration budget exceeded')
        target.mkdir(parents=True, exist_ok=True)
        for item in items:
            path = target/item['path']
            if any(p.is_symlink() for p in (path,*path.parents)):
                raise ObjectError('Symlink archive destination')
            body = self.read_file(item)
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(prefix='.cloud-stage-', dir=path.parent)
            try:
                with os.fdopen(fd, 'wb') as output:
                    output.write(body); output.flush(); os.fsync(output.fileno())
                os.replace(temporary,path)
            finally:
                Path(temporary).unlink(missing_ok=True)
        return {'files':len(items),'original_bytes':sum(x['bytes'] for x in items)}
