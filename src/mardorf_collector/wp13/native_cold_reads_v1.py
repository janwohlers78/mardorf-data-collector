"""Bounded parallel IO around the unchanged complete native cold verifier.

Every original byte is still independently read from canonical transport and
hashed by the pinned reader. Metadata16MiB, four workers, three queued8MiB
bodies plus the consumed body; no historical scans or persistent proof cache.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from mardorf_collector.storage.objects import path_digest, B2Objects, safe_key
import base64
import hashlib

METADATA_BYTES = 16 * 1024**2
OBJECT_BYTES = 8 * 1024**2
WINDOW = 3


class ColdModelReads:
    def __init__(self, backend, module, reference):
        self.backend, self.module, self.reference = backend, module, reference
        self.cache, self.pending, self.plan = {}, deque(), iter(())
        self.executor = None

    def __enter__(self):
        ref = self.module.ObjectRef.parse(self.reference)
        body = self.backend.get_bytes(ref)
        self.cache[ref.key] = (ref, body)
        catalog = self.module.ModelReader(self).catalog(self.reference)
        refs = [self.module.ObjectRef.parse(item['parent']) for item in catalog['parents']]
        if sum(r.bytes for r in refs) + ref.bytes > METADATA_BYTES:
            # Keep the existing protocol admissible; an unusually large metadata
            # graph uses the unchanged serial reader instead of a large cache.
            self.cache.clear()
            return self
        for parent in refs:
            if parent.key != f'{self.module.PREFIX}/parents/{parent.sha256}' or parent.bytes > 1024**2:
                raise ValueError('Native model parent prefetch scope/budget')
        self.executor = ThreadPoolExecutor(max_workers=4)
        try:
            # The declared sum bounds even completed futures awaiting collection.
            bodies = self.executor.map(self.backend.get_bytes, refs)
            for parent, raw in zip(refs, bodies):
                self.cache[parent.key] = (parent, raw)
            parents = self.module.ParentStore(self, prefix=self.module.PREFIX)
            plan = []
            for parent in refs:
                manifest=parents.manifest(parent)
                item=next(item for item in catalog['parents'] if self.module.ObjectRef.parse(item['parent'])==parent)
                if manifest['sha256']!=item['sha256'] or manifest['bytes']!=item['bytes'] or manifest['metadata']!=item['metadata']:
                    raise ValueError('Native model parent/capture metadata contradiction')
                plan.extend(self.module.ObjectRef.parse(value) for value in manifest['chunks'])
            for fragment in catalog['fragments']:
                # This is the verifier's exact consumption order; its native
                # generator reads one matching fragment after each Parquet load.
                for kind in ('parquet', 'native'):
                    item = self.module.ObjectRef.parse(fragment[kind])
                    if item.key != f'{self.module.EXTRACT_PREFIX}/{kind}/{item.sha256}':
                        raise ValueError('Native model extract prefetch scope')
                    plan.append(item)
            if any(item.bytes > OBJECT_BYTES for item in plan):
                raise ValueError('Native model object prefetch budget')
            self.plan = iter(plan)
            self._fill()
            return self
        except BaseException:
            self.close()
            raise

    def _fill(self):
        while self.executor is not None and len(self.pending) < WINDOW:
            ref = next(self.plan, None)
            if ref is None:
                break
            self.pending.append((ref, self.executor.submit(self.backend.get_bytes, ref)))

    def get_bytes(self, reference):
        cached = self.cache.get(reference.key)
        if cached is not None:
            if cached[0] != reference:
                raise ValueError('Cached native model reference identity mismatch')
            return cached[1]
        if self.pending and self.pending[0][0] == reference:
            _, future = self.pending.popleft()
            body = future.result()
            self._fill()
            return body
        return self.backend.get_bytes(reference)

    def close(self):
        if self.executor is not None:
            self.executor.shutdown(wait=True, cancel_futures=True)
            self.executor = None
        self.pending.clear()
        self.cache.clear()

    def __exit__(self, *args):
        self.close()


def publish_object(backend, local, reference, *, verified_reads=None):
    """Skip unchanged uploads only with prior canonical bytes plus a live head.

    A miss uses the original collision checks and cold write readback. Cache
    contents are only inserted after an independent canonical read, never from
    producer-local files. A deleted object is restored, a conflict fails closed.
    """
    if verified_reads is not None:
        hit=verified_reads.cache.existing(backend,reference)
        if hit is not None:
            metadata=backend.head(reference.key)
            if metadata is not None:
                if metadata['bytes']!=reference.bytes or metadata['sha256']!=reference.sha256:
                    raise ValueError('Immutable cached publication conflict')
                return reference
    if verified_reads is not None:
        # Existing remote bytes need one cold hash-checked read, not a second
        # identical download through put_file followed by cache priming.
        metadata = backend.head(reference.key)
        if metadata is not None:
            if metadata['bytes'] != reference.bytes or metadata['sha256'] != reference.sha256:
                raise ValueError('Immutable cached publication conflict')
            if path_digest(local.root/reference.key, backend.max_object_bytes) != (reference.sha256, reference.bytes):
                raise ValueError('Original model source identity mismatch')
            verified_reads.get_bytes(reference)
            return reference
    if verified_reads is not None and isinstance(backend, B2Objects) and reference.bytes <= backend.part_bytes:
        # Bounded immutable single-part upload: one live HEAD, one PUT and one
        # independent canonical GET. General/multipart transports are unchanged.
        backend._bound(reference)
        key = safe_key(reference.key)
        if reference.sha256 not in key.split('/') and not any(p.startswith(reference.sha256+'.') for p in key.split('/')):
            raise ValueError('Native publication requires digest-bound immutable key')
        body = local.get_bytes(reference)
        backend.metrics['put_attempted_bytes'] += len(body)
        backend._call('put_object', Key=key, Body=body, ContentLength=len(body),
            ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode(),
            Metadata={'sha256':reference.sha256}, ServerSideEncryption='AES256')
        backend.metrics['put_bytes'] += len(body)
        # Force the fresh remote read even when restoring a deleted cached object.
        canonical_body = backend.get_bytes(reference)
        if len(canonical_body) != reference.bytes or hashlib.sha256(canonical_body).hexdigest() != reference.sha256:
            raise ValueError('Native canonical write readback identity mismatch')
        class CanonicalRead:
            def _bound(self, ref): backend._bound(ref)
            def get_file(self, ref, destination):
                if ref != reference: raise ValueError('Native canonical read binding')
                from pathlib import Path
                Path(destination).write_bytes(canonical_body)
        verified_reads.cache.read(CanonicalRead(), reference)
        return reference
    result = backend.put_file(reference.key,local.root/reference.key)
    if verified_reads is not None:
        verified_reads.get_bytes(reference)
    return result


def publish_parallel(references, upload, *, workers=48, progress=None):
    """Keep only a bounded window active and cancel pending work on first error.

    Waiting for thousands of queued objects during executor context exit hid the
    initial failure behind the workflow timeout. Running IO remains bounded by
    the transport timeout; no new object is submitted after an observed error.
    """
    references = iter(references)
    executor = ThreadPoolExecutor(max_workers=workers)
    pending = {}
    completed = 0
    def fill():
        while len(pending) < workers:
            reference = next(references, None)
            if reference is None: break
            pending[executor.submit(upload, reference)] = reference
    try:
        fill()
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            # Surface any failure before replenishing the active window.
            for future in done:
                try:
                    future.result()
                except BaseException as error:
                    import json
                    print('WP15_UPLOAD_FAILED=' + json.dumps(dict(
                        completed=completed, reference=pending[future].json(),
                        error_type=type(error).__name__)), flush=True)
                    raise
            for future in done:
                del pending[future]
                completed += 1
                if progress is not None: progress(completed)
            fill()
    except BaseException:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    return completed


class NativeVerifiedReads:
    """Sixteen independent disposable caches, bounded to one total disk quota.

    A single lock and a full directory scan per insertion serialize thousands
    of objects. SHA-prefix sharding bounds contention and reduces scan work;
    the unchanged VerifiedReads performs every byte/hash/quota check. The
    sharded cache has no new write or canonical-publication authority.
    """
    def __init__(self, backend, root, *, max_bytes=2*1024**3):
        from pathlib import Path
        from mardorf_collector.storage.verified_reads_v1 import VerifiedReads
        from mardorf_collector.storage.objects import positive, safe_root
        max_bytes = positive(max_bytes, 'native cache budget')
        if max_bytes < 16: raise ValueError('Native cache quota must cover sixteen shards')
        self.backend = backend
        self.root = safe_root(root)
        self.shards = tuple(VerifiedReads(backend, self.root / format(i, 'x'),
            max_bytes=max_bytes//16) for i in range(16))
        self.cache = self

    def _shard(self, reference):
        self.backend._bound(reference)
        return self.shards[int(reference.sha256[0], 16)]

    @property
    def metrics(self):
        keys = self.shards[0].cache.metrics
        return {key: sum(shard.cache.metrics[key] for shard in self.shards) for key in keys}

    def __getattr__(self, name): return getattr(self.backend, name)
    def existing(self, backend, reference): return self._shard(reference).cache.existing(backend, reference)
    def read(self, backend, reference): return self._shard(reference).cache.read(backend, reference)
    def get_bytes(self, reference): return self._shard(reference).get_bytes(reference)
    def get_range(self, reference, start, end): return self._shard(reference).get_range(reference, start, end)
    def get_file(self, reference, destination): return self.backend.get_file(reference, destination)


def cache_balanced_references(references):
    """Deterministic round robin avoids sorting every active object into one shard."""
    groups = [deque() for _ in range(16)]
    for reference in sorted(references, key=lambda ref: ref.key):
        groups[int(reference.sha256[0], 16)].append(reference)
    while any(groups):
        for group in groups:
            if group: yield group.popleft()
