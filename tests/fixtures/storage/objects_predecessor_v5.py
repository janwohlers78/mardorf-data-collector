"""Immutable, hash-addressed storage with bounded IO and honest metrics.

Equal keys must describe equal bytes. No remote rename, locking or mutable CAS
is assumed. Multipart and its cancellation affect only this caller's upload.
"""
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import base64
import fcntl
import hashlib
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlsplit


class ObjectError(ValueError):
    """Safe error text; never SDK messages, URLs with credentials or secrets."""


def safe_key(key):
    if (not isinstance(key, str) or len(key) > 1024 or
        not re.fullmatch(r'[A-Za-z0-9_.\-/]+', key) or
        any(s in ('', '.', '..') for s in key.split('/'))):
        raise ObjectError('Unsafe object key')
    return key


def positive(value, name, *, allow_zero=False):
    if type(value) is not int or value < (0 if allow_zero else 1):
        raise ObjectError('Invalid '+name)
    return value


@dataclass(frozen=True)
class ObjectRef:
    key: str
    sha256: str
    bytes: int
    schema_version: int = 1

    def __post_init__(self):
        safe_key(self.key)
        if not isinstance(self.sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', self.sha256):
            raise ObjectError('Invalid object digest')
        positive(self.bytes, 'object size', allow_zero=True)
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ObjectError('Unsupported object reference version')

    def json(self):
        return asdict(self)

    @classmethod
    def parse(cls, value):
        if not isinstance(value, dict) or set(value) != {'key','sha256','bytes','schema_version'}:
            raise ObjectError('Invalid object reference')
        return cls(**value)


def path_digest(path, limit):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
        raise ObjectError('Regular nonsymlink upload source required')
    digest = hashlib.sha256(); size = 0
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            size += len(block)
            if size > limit: raise ObjectError('Upload byte limit exceeded')
            digest.update(block)
    return digest.hexdigest(), size


@contextmanager
def file_lock(path, timeout=30):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    started = time.monotonic()
    try:
        while True:
            try: fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB); break
            except BlockingIOError:
                if time.monotonic()-started >= timeout: raise ObjectError('Local lock timeout')
                time.sleep(0.01)
        yield
    finally:
        os.close(fd)


def safe_root(root):
    if '..' in Path(root).parts:raise ObjectError('Parent traversal storage root forbidden')
    root = Path(root).absolute()
    if any(p.is_symlink() for p in (root, *root.parents)):
        raise ObjectError('Symlink storage/cache root forbidden')
    root.mkdir(parents=True, exist_ok=True)
    return root


def sync_directory(root):
    fd = os.open(root, os.O_DIRECTORY | os.O_NOFOLLOW)
    try: os.fsync(fd)
    finally: os.close(fd)


class Objects:
    def __init__(self, *, max_object_bytes=64*1024*1024):
        self.max_object_bytes = positive(max_object_bytes, 'object budget')
        self.metrics = {'get_bytes':0, 'put_bytes':0, 'requests':0,
                        'reused_bytes':0, 'range_bytes':0, 'failed_requests':0,
                        'multipart_aborts':0,'provider_retry_attempts':0,
                        'byte_accounting_complete':True,'put_attempted_bytes':0}

    def _bound(self, ref):
        if not isinstance(ref, ObjectRef) or ref.bytes > self.max_object_bytes:
            raise ObjectError('Object reference/budget exceeded')

    def put_bytes(self, key, body):
        if not isinstance(body, bytes) or len(body) > self.max_object_bytes:
            raise ObjectError('Byte payload budget exceeded')
        with tempfile.NamedTemporaryFile() as temporary:
            temporary.write(body); temporary.flush()
            return self.put_file(key, temporary.name)

    def get_bytes(self, ref):
        self._bound(ref)
        with tempfile.TemporaryDirectory() as directory:
            dest=Path(directory)/'object'; self.get_file(ref,dest)
            return dest.read_bytes()

    def _verified_existing(self, ref):
        metadata=self.head(ref.key)
        if metadata is None: return False
        if metadata['bytes'] != ref.bytes or metadata['sha256'] != ref.sha256:
            raise ObjectError('Immutable object conflict')
        # User metadata alone is not proof of content, even on replay.
        with tempfile.TemporaryDirectory() as directory:
            self.get_file(ref,Path(directory)/'verify')
        self.metrics['reused_bytes'] += ref.bytes
        return True


class LocalObjects(Objects):
    def __init__(self, root, **kwargs):
        super().__init__(**kwargs); self.root=safe_root(root)

    def _path(self,key):
        path=self.root/safe_key(key)
        if any(p.is_symlink() for p in (path,*path.parents)):
            raise ObjectError('Symlink object path forbidden')
        return path

    def head(self,key):
        self.metrics['requests'] += 1; p=self._path(key)
        if not p.exists(): return None
        digest,size=path_digest(p,self.max_object_bytes)
        return {'sha256':digest,'bytes':size,'encryption':'local_not_asserted'}

    def put_file(self,key,source):
        digest,size=path_digest(source,self.max_object_bytes)
        ref=ObjectRef(safe_key(key),digest,size);dest=self._path(key)
        dest.parent.mkdir(parents=True,exist_ok=True)
        with file_lock(self.root/'.writer.lock'):
            if self._verified_existing(ref): return ref
            fd,name=tempfile.mkstemp(prefix='.stage-',dir=dest.parent)
            try:
                sha=hashlib.sha256();copied=0
                with os.fdopen(fd,'wb') as output,Path(source).open('rb') as stream:
                    for block in iter(lambda:stream.read(1024*1024),b''):
                        copied+=len(block)
                        if copied>size:raise ObjectError('Upload source changed')
                        output.write(block);sha.update(block)
                    output.flush();os.fsync(output.fileno())
                if copied!=size or sha.hexdigest()!=digest:raise ObjectError('Upload source changed')
                os.replace(name,dest);sync_directory(dest.parent)
                self.metrics['put_bytes']+=size;self.metrics['requests']+=1
            finally:
                if os.path.exists(name):os.unlink(name)
        return ref

    def get_file(self,ref,dest):
        self._bound(ref);source=self._path(ref.key);dest=Path(dest)
        if dest.exists() or dest.is_symlink() or any(p.is_symlink() for p in dest.parents) or '..' in dest.parts:
            raise ObjectError('Download destination must be new and nonsymlink')
        digest=hashlib.sha256();size=0;self.metrics['requests']+=1;created=False
        try:
            with source.open('rb') as stream,dest.open('xb') as output:
                created=True
                for block in iter(lambda:stream.read(min(1024*1024,ref.bytes-size+1)),b''):
                    size+=len(block);self.metrics['get_bytes']+=len(block)
                    if size>ref.bytes:raise ObjectError('Download byte limit exceeded')
                    output.write(block);digest.update(block)
                output.flush();os.fsync(output.fileno())
            if size!=ref.bytes or digest.hexdigest()!=ref.sha256:raise ObjectError('Object integrity mismatch')
        except Exception:
            if created and dest.exists():dest.unlink()
            raise

    def get_range(self,ref,start,end):
        self._bound(ref)
        if type(start)is not int or type(end)is not int or not 0<=start<end<=ref.bytes:
            raise ObjectError('Invalid half-open object range')
        # Local full hash check; returned range alone is not a full-object hash proof.
        if self.head(ref.key)!={'sha256':ref.sha256,'bytes':ref.bytes,'encryption':'local_not_asserted'}:
            raise ObjectError('Object range metadata mismatch')
        with self._path(ref.key).open('rb') as f:f.seek(start);body=f.read(end-start)
        self.metrics['get_bytes']+=len(body);self.metrics['range_bytes']+=len(body);return body

    def list_page(self,prefix,*,limit=100,cursor=None):
        safe_key(prefix.rstrip('/'));positive(limit,'list limit')
        if limit>1000:raise ObjectError('List page budget exceeded')
        if cursor is not None:safe_key(cursor)
        keys=[]
        # Local enumeration is administrative/recovery only, never a query dependency.
        parent=prefix.rsplit('/',1)[0] if '/' in prefix else None
        folder=self._path(parent) if parent else self.root
        scanned=0
        if folder.exists():
            for p in folder.rglob('*'):
                scanned+=1
                if scanned>10000:raise ObjectError('Recovery scan budget exceeded; use a narrower prefix')
                if p.is_symlink():raise ObjectError('Symlink in recovery inventory')
                if p.is_file() and not p.name.startswith('.stage-'):
                    key=p.relative_to(self.root).as_posix()
                    if key.startswith(prefix) and (cursor is None or key>cursor):keys.append(key)
        keys.sort();page=keys[:limit]
        return {'keys':page,'cursor':page[-1] if len(keys)>limit else None}


class B2Objects(Objects):
    """S3 transport. Immutable content keys allow benign same-byte racing writes.

    Caller must use digest-bound keys; mutable conditional pointers are forbidden.
    Full-object readback verifies completed upload, including multipart results.
    """
    def __init__(self,settings,*,client=None,part_bytes=8*1024*1024,**kwargs):
        super().__init__(**kwargs)
        endpoint=settings['B2_ENDPOINT_URL'];url=urlsplit(endpoint)
        match=re.fullmatch(r's3\.([a-z0-9-]+)\.backblazeb2\.com',url.hostname or '')
        if (url.scheme!='https' or not match or url.username or url.password or
            url.port not in (None,443) or url.path not in ('','/') or url.query or url.fragment or
            match[1]!=settings['B2_REGION']):raise ObjectError('Invalid B2 HTTPS region endpoint')
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]{2,62}',settings['B2_BUCKET']):
            raise ObjectError('Resolved bucket name required')
        self.bucket=settings['B2_BUCKET'];self.endpoint=endpoint.rstrip('/')
        self.part_bytes=positive(part_bytes,'multipart size')
        if self.part_bytes<5*1024*1024:raise ObjectError('Multipart part size below S3 minimum')
        if client is None:
            if not settings.get('B2_APPLICATION_KEY_ID') or not settings.get('B2_APPLICATION_KEY'):
                raise ObjectError('Explicit B2 credentials required')
            import boto3
            from botocore.config import Config
            client=boto3.client('s3',endpoint_url=self.endpoint,region_name=settings['B2_REGION'],
                aws_access_key_id=settings['B2_APPLICATION_KEY_ID'],aws_secret_access_key=settings['B2_APPLICATION_KEY'],
                config=Config(signature_version='s3v4',connect_timeout=5,read_timeout=15,
                    retries={'total_max_attempts':2,'mode':'standard'},s3={'addressing_style':'path'},
                    request_checksum_calculation='when_required',response_checksum_validation='when_required'))
            # Finite byte bodies with explicit length/MD5 avoid proxy 100-continue issues.
            def no_expect(params,**kwargs):params.get('headers',{}).pop('Expect',None)
            client.meta.events.register_last('before-call.s3.PutObject',no_expect)
            client.meta.events.register_last('before-call.s3.UploadPart',no_expect)
        self.client=client

    def _call(self,method,**kwargs):
        self.metrics['requests']+=1
        try:
            result=getattr(self.client,method)(Bucket=self.bucket,**kwargs)
            retries=result.get('ResponseMetadata',{}).get('RetryAttempts',0)
            if type(retries)is int and retries>0:
                self.metrics['provider_retry_attempts']+=retries
                self.metrics['byte_accounting_complete']=False
            return result
        except Exception as exc:
            response=getattr(exc,'response',None)
            response=response if isinstance(response,dict) else {}
            status=response.get('ResponseMetadata',{}).get('HTTPStatusCode')
            if method=='head_object' and status==404:return None
            self.metrics['failed_requests']+=1
            self.metrics['byte_accounting_complete']=False
            # Error code text is not copied from providers; this is safe fixed context.
            raise ObjectError('Object provider request failed: '+method) from None

    def head(self,key):
        r=self._call('head_object',Key=safe_key(key))
        if r is None:return None
        if r.get('ServerSideEncryption')!='AES256':raise ObjectError('Object encryption mismatch')
        size=r.get('ContentLength');positive(size,'remote object size',allow_zero=True)
        digest=r.get('Metadata',{}).get('sha256')
        if not isinstance(digest,str) or not re.fullmatch(r'[0-9a-f]{64}',digest):
            raise ObjectError('Remote object hash metadata missing')
        return {'bytes':size,'sha256':digest,'encryption':'AES256'}

    def put_file(self,key,source):
        digest,size=path_digest(source,self.max_object_bytes)
        key=safe_key(key)
        if digest not in key.split('/') and not any(p.startswith(digest+'.') for p in key.split('/')):
            raise ObjectError('Cloud writes require a digest-bound immutable key')
        ref=ObjectRef(key,digest,size)
        if self._verified_existing(ref):return ref
        if size<=self.part_bytes:
            body=Path(source).read_bytes()
            if len(body)!=size or hashlib.sha256(body).hexdigest()!=digest:raise ObjectError('Upload source changed')
            self.metrics['put_attempted_bytes']+=size
            self._call('put_object',Key=key,Body=body,ContentLength=size,ContentMD5=base64.b64encode(hashlib.md5(body).digest()).decode(),
                Metadata={'sha256':digest},ServerSideEncryption='AES256')
            self.metrics['put_bytes']+=size
        else:
            upload=self._call('create_multipart_upload',Key=key,Metadata={'sha256':digest},ServerSideEncryption='AES256')
            upload_id=upload.get('UploadId')
            if not isinstance(upload_id,str) or not upload_id:raise ObjectError('Multipart identity missing')
            parts=[];sha=hashlib.sha256();sent=0;completed=False
            try:
                with Path(source).open('rb') as stream:
                    for number in range(1,10001):
                        block=stream.read(self.part_bytes)
                        if not block:break
                        sent+=len(block)
                        if sent>size:raise ObjectError('Upload source changed')
                        sha.update(block)
                        self.metrics['put_attempted_bytes']+=len(block)
                        result=self._call('upload_part',Key=key,UploadId=upload_id,PartNumber=number,Body=block,
                            ContentLength=len(block),ContentMD5=base64.b64encode(hashlib.md5(block).digest()).decode())
                        self.metrics['put_bytes']+=len(block)
                        if not isinstance(result.get('ETag'),str):raise ObjectError('Multipart ETag missing')
                        parts.append({'ETag':result['ETag'],'PartNumber':number})
                if sent!=size or sha.hexdigest()!=digest:raise ObjectError('Upload source changed')
                self._call('complete_multipart_upload',Key=key,UploadId=upload_id,MultipartUpload={'Parts':parts})
                completed=True
            finally:
                if not completed:
                    # Never mask the original exception; only abort this own upload.
                    try:self._call('abort_multipart_upload',Key=key,UploadId=upload_id);self.metrics['multipart_aborts']+=1
                    except ObjectError:pass
        if not self._verified_existing(ref):raise ObjectError('Completed upload not visible')
        return ref

    def get_file(self,ref,dest):
        self._bound(ref);dest=Path(dest)
        if dest.exists() or dest.is_symlink() or any(p.is_symlink() for p in dest.parents) or '..' in dest.parts:
            raise ObjectError('Download destination must be new and nonsymlink')
        response=self._call('get_object',Key=ref.key);body=response.get('Body')
        if body is None:raise ObjectError('Download body missing')
        size=0;sha=hashlib.sha256();created=False
        try:
            if (response.get('ContentLength')!=ref.bytes or response.get('ServerSideEncryption')!='AES256' or
                response.get('Metadata',{}).get('sha256')!=ref.sha256):raise ObjectError('Download metadata mismatch')
            with dest.open('xb') as output:
                created=True
                while True:
                    block=body.read(min(1024*1024,ref.bytes-size+1))
                    if not block:break
                    size+=len(block);self.metrics['get_bytes']+=len(block)
                    if size>ref.bytes:raise ObjectError('Download byte limit exceeded')
                    output.write(block);sha.update(block)
                output.flush();os.fsync(output.fileno())
            if size!=ref.bytes or sha.hexdigest()!=ref.sha256:raise ObjectError('Object integrity mismatch')
        except Exception:
            if created and dest.exists():dest.unlink()
            raise ObjectError('Verified object download failed') from None
        finally:
            try:body.close()
            except Exception:
                if created and dest.exists():dest.unlink()
                raise ObjectError('Download stream close failed') from None

    def get_range(self,ref,start,end):
        self._bound(ref)
        if type(start)is not int or type(end)is not int or not 0<=start<end<=ref.bytes:
            raise ObjectError('Invalid half-open object range')
        meta=self.head(ref.key)
        if meta is None or meta['bytes']!=ref.bytes or meta['sha256']!=ref.sha256:raise ObjectError('Range object mismatch')
        result=self._call('get_object',Key=ref.key,Range=f'bytes={start}-{end-1}');body=result.get('Body')
        if body is None:raise ObjectError('Range body missing')
        try:
            value=body.read(end-start+1);self.metrics['get_bytes']+=len(value);self.metrics['range_bytes']+=len(value)
            if (len(value)!=end-start or result.get('ContentLength')!=end-start or
                result.get('ContentRange')!=f'bytes {start}-{end-1}/{ref.bytes}' or
                result.get('ServerSideEncryption')!='AES256' or
                result.get('Metadata',{}).get('sha256')!=ref.sha256):raise ObjectError('Range response mismatch')
            return value
        except Exception:
            raise ObjectError('Verified object range failed') from None
        finally:
            try:body.close()
            except Exception:raise ObjectError('Download stream close failed') from None

    def list_page(self,prefix,*,limit=100,cursor=None):
        safe_key(prefix.rstrip('/'));positive(limit,'list limit')
        if limit>1000:raise ObjectError('List page budget exceeded')
        kwargs={'Prefix':prefix,'MaxKeys':limit}
        if cursor is not None:
            if not isinstance(cursor,str) or len(cursor)>8192:raise ObjectError('Invalid list cursor')
            kwargs['ContinuationToken']=cursor
        result=self._call('list_objects_v2',**kwargs)
        values=result.get('Contents',[])
        if len(values)>limit:raise ObjectError('List response budget exceeded')
        keys=[safe_key(v['Key']) for v in values]
        if any(not k.startswith(prefix) for k in keys):raise ObjectError('List prefix violation')
        cursor=result.get('NextContinuationToken') if result.get('IsTruncated') else None
        if result.get('IsTruncated') and not cursor:raise ObjectError('Truncated inventory without cursor')
        return {'keys':keys,'cursor':cursor}
