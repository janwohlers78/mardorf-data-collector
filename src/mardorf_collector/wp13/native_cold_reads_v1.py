"""Bounded parallel IO around the unchanged complete native cold verifier.

Every original byte is still independently read from canonical transport and
hashed by the pinned reader. Metadata16MiB, four workers, three queued8MiB
bodies plus the consumed body; no historical scans or persistent proof cache.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from mardorf_collector.storage.objects import path_digest

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
    result = backend.put_file(reference.key,local.root/reference.key)
    if verified_reads is not None:
        verified_reads.get_bytes(reference)
    return result
