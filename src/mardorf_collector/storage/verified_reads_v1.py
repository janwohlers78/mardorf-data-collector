"""Disposable hash-verified cache of canonical immutable bytes; no head/write authority."""
import hashlib
import os
import re
from pathlib import Path
import tempfile
from mardorf_collector.storage.objects import (ObjectError, ObjectRef, file_lock,
                                               path_digest, positive, safe_root, sync_directory)


class ObjectCache:
    def __init__(self,root,*,max_bytes=256*1024*1024):
        self.root=safe_root(root);self.max_bytes=positive(max_bytes,'cache budget')
        self.metrics={'hits':0,'misses':0,'evictions':0,'corrupt_entries':0}

    def _entries(self):
        result=[]
        for p in self.root.iterdir():
            if p.is_symlink():raise ObjectError('Symlink cache entry forbidden')
            if p.name == '.cache.lock':
                continue
            if p.name.startswith('.stage-') and p.is_file():
                p.unlink()
                continue
            if not p.is_file() or not re.fullmatch('[0-9a-f]{64}',p.name):raise ObjectError('Unexpected cache entry')
            result.append(p)
        return result

    def read(self,backend,ref):
        backend._bound(ref)
        if ref.bytes>self.max_bytes:raise ObjectError('Single object exceeds cache budget')
        with file_lock(self.root/'.cache.lock'):
            dest=self.root/ref.sha256
            if dest.is_symlink():raise ObjectError('Symlink cache entry forbidden')
            if dest.exists():
                try:valid=path_digest(dest,self.max_bytes)==(ref.sha256,ref.bytes)
                except ObjectError:valid=False
                if valid:
                    os.utime(dest,None);self.metrics['hits']+=1;return dest.read_bytes()
                dest.unlink();self.metrics['corrupt_entries']+=1
            entries=self._entries()
            used=sum(p.stat().st_size for p in entries)
            for p in sorted(entries,key=lambda p:(p.stat().st_mtime_ns,p.name)):
                if used+ref.bytes<=self.max_bytes:break
                used-=p.stat().st_size;p.unlink();self.metrics['evictions']+=1
            if used+ref.bytes>self.max_bytes:raise ObjectError('Cache quota exceeded')
            # Reserve worst-case space before downloading; lock covers staging too.
            fd,name=tempfile.mkstemp(prefix='.stage-',dir=self.root);os.close(fd);os.unlink(name)
            try:
                backend.get_file(ref,name)
                if path_digest(name,self.max_bytes)!=(ref.sha256,ref.bytes):
                    raise ObjectError('Downloaded cache object identity mismatch')
                os.replace(name,dest);sync_directory(self.root)
            finally:
                if os.path.exists(name):os.unlink(name)
            self.metrics['misses']+=1
            return dest.read_bytes()

    def existing(self, backend, ref):
        """Verified optional hit; a range miss must not download a full archive."""
        backend._bound(ref)
        with file_lock(self.root/'.cache.lock'):
            dest=self.root/ref.sha256
            if not dest.exists():return None
            if dest.is_symlink():raise ObjectError('Symlink cache entry forbidden')
            try:valid=path_digest(dest,self.max_bytes)==(ref.sha256,ref.bytes)
            except ObjectError:valid=False
            if not valid:
                dest.unlink();self.metrics['corrupt_entries']+=1;return None
            os.utime(dest,None);self.metrics['hits']+=1
            return dest.read_bytes()


class VerifiedReads:
    """Shared disposable immutable-object cache; live heads always bypass it.

    Every hit checks length and SHA again. Range misses retain selective IO.
    Writes and publication readback use the original transport, never the cache.
    """
    def __init__(self, backend, root, *, max_bytes=512*1024**2):
        self.backend=backend
        self.cache=ObjectCache(root,max_bytes=max_bytes)

    def __getattr__(self,name):return getattr(self.backend,name)

    def get_bytes(self,ref):
        if ref.bytes>self.cache.max_bytes:return self.backend.get_bytes(ref)
        hit=self.cache.existing(self.backend,ref)
        if hit is not None:return hit
        # Keep concurrent native prefetch IO outside the quota lock. At most the
        # caller's bounded window of bodies is held; disk insertion is atomic.
        body=self.backend.get_bytes(ref)
        if len(body)!=ref.bytes or hashlib.sha256(body).hexdigest()!=ref.sha256:
            raise ObjectError('Downloaded cache object identity mismatch')
        backend=self.backend
        class Downloaded:
            def _bound(self,value):backend._bound(value)
            def get_file(self,value,dest):
                if value!=ref:raise ObjectError('Downloaded cache binding mismatch')
                Path(dest).write_bytes(body)
        return self.cache.read(Downloaded(),ref)

    def get_range(self,ref,start,end):
        if type(start)is not int or type(end)is not int or not 0<=start<end<=ref.bytes:
            raise ObjectError('Invalid cached object range')
        body=self.cache.existing(self.backend,ref)
        return body[start:end] if body is not None else self.backend.get_range(ref,start,end)

    def get_file(self,ref,dest):
        # Native cold verification and write readback retain independent IO.
        return self.backend.get_file(ref,dest)
