"""Stream large original provider files through bounded immutable objects.

One small manifest binds the exact whole file and its ordered chunks. Chunking
changes physical storage only; provider bytes are never re-encoded or renamed.
"""
import hashlib
from pathlib import Path

from .objects import ObjectError, ObjectRef, safe_key
from .archive import canonical, decode

VERSION = 'wp15-original-parent-v1'
CHUNK_BYTES = 8 * 1024**2
MAX_PARENT_BYTES = 4 * 1024**3


class ParentStore:
    def __init__(self, backend, *, prefix):
        self.backend = backend
        self.prefix = safe_key(prefix.rstrip('/'))

    def write_file(self, source, *, metadata):
        if not isinstance(metadata, dict):
            raise ObjectError('Original parent metadata object required')
        source = Path(source)
        if any(p.is_symlink() for p in (source, *source.parents)) or not source.is_file():
            raise ObjectError('Regular original parent file required')
        if not 0 < source.stat().st_size <= MAX_PARENT_BYTES:
            raise ObjectError('Original parent byte budget exceeded')
        digest = hashlib.sha256()
        size = 0
        chunks = []
        with source.open('rb') as stream:
            while block := stream.read(CHUNK_BYTES):
                size += len(block)
                if size > MAX_PARENT_BYTES:
                    raise ObjectError('Original parent grew beyond byte budget')
                digest.update(block)
                identity = hashlib.sha256(block).hexdigest()
                ref = self.backend.put_bytes(f'{self.prefix}/blobs/{identity}', block)
                chunks.append(ref.json())
        if size == 0:
            raise ObjectError('Original parent became empty during capture')
        manifest = {'schema_version': 1, 'artifact_version': VERSION,
            'sha256': digest.hexdigest(), 'bytes': size,
            'chunk_bytes': CHUNK_BYTES, 'chunks': chunks, 'metadata': metadata}
        body = canonical(manifest)
        if len(body) > 1024**2:
            raise ObjectError('Original parent manifest budget exceeded')
        ref = self.backend.put_bytes(f'{self.prefix}/parents/{hashlib.sha256(body).hexdigest()}', body)
        return ref

    def manifest(self, reference):
        ref = reference if isinstance(reference, ObjectRef) else ObjectRef.parse(reference)
        if ref.key != f'{self.prefix}/parents/{ref.sha256}' or ref.bytes > 1024**2:
            raise ObjectError('Original parent reference scope mismatch')
        manifest = decode(self.backend.get_bytes(ref))
        if (set(manifest) != {'schema_version', 'artifact_version', 'sha256', 'bytes',
                              'chunk_bytes', 'chunks', 'metadata'}
                or type(manifest['schema_version']) is not int or manifest['schema_version'] != 1
                or manifest['artifact_version'] != VERSION
                or type(manifest['bytes']) is not int or not 0 < manifest['bytes'] <= MAX_PARENT_BYTES
                or type(manifest['chunk_bytes']) is not int or manifest['chunk_bytes'] != CHUNK_BYTES
                or not isinstance(manifest['metadata'], dict)
                or not isinstance(manifest['chunks'], list)):
            raise ObjectError('Invalid original parent manifest')
        # Reuse ObjectRef's digest/size checks for the logical original identity.
        ObjectRef(f'{self.prefix}/originals/{manifest["sha256"]}', manifest['sha256'], manifest['bytes'])
        count = (manifest['bytes'] + CHUNK_BYTES - 1) // CHUNK_BYTES
        if len(manifest['chunks']) != count:
            raise ObjectError('Original parent chunk count mismatch')
        for index, value in enumerate(manifest['chunks']):
            chunk = ObjectRef.parse(value)
            expected = min(CHUNK_BYTES, manifest['bytes'] - index * CHUNK_BYTES)
            if chunk.key != f'{self.prefix}/blobs/{chunk.sha256}' or chunk.bytes != expected:
                raise ObjectError('Original parent chunk scope/size mismatch')
        return manifest

    def iter_bytes(self, reference):
        """Explicit original audit/restore; normal point reads use metadata only."""
        manifest = self.manifest(reference)
        digest = hashlib.sha256()
        for value in manifest['chunks']:
            block = self.backend.get_bytes(ObjectRef.parse(value))
            digest.update(block)
            yield block
        if digest.hexdigest() != manifest['sha256']:
            raise ObjectError('Original parent whole-file digest mismatch')

    def restore(self, reference, destination):
        """Publish a reconstructed file only after complete integrity validation."""
        import os
        import tempfile
        destination = Path(destination)
        if destination.exists() or any(p.is_symlink() for p in (destination, *destination.parents)):
            raise ObjectError('New nonsymlink restore destination required')
        destination.parent.mkdir(parents=True, exist_ok=True)
        name = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
                name = Path(stream.name)
                for block in self.iter_bytes(reference):
                    stream.write(block)
                stream.flush()
                os.fsync(stream.fileno())
            os.link(name, destination)
        finally:
            if name is not None:
                name.unlink(missing_ok=True)
        return destination
