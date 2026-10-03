"""Storage contract and fault tests independent of credentials or the network."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from mardorf_collector.storage.objects import B2Objects,LocalObjects,ObjectError,ObjectRef,path_digest


class Missing(Exception):
    response={'ResponseMetadata':{'HTTPStatusCode':404}}


class FakeS3:
    def __init__(self):self.objects={};self.uploads={};self.aborted=[];self.fail_part=False;self.read_corrupt=False
    def put_object(self,**kw):
        self.objects[kw['Key']]={'body':kw['Body'],'Metadata':kw['Metadata'],'ServerSideEncryption':kw['ServerSideEncryption']}
        return {}
    def head_object(self,**kw):
        if kw['Key'] not in self.objects:raise Missing()
        item=self.objects[kw['Key']];return {k:v for k,v in item.items() if k!='body'}|{'ContentLength':len(item['body'])}
    def get_object(self,**kw):
        meta=self.head_object(**kw);body=self.objects[kw['Key']]['body']
        if self.read_corrupt:body=b'X'+body[1:]
        if 'Range' in kw:
            start,end=map(int,kw['Range'].removeprefix('bytes=').split('-'))
            meta['ContentRange']=f'bytes {start}-{end}/{len(body)}';body=body[start:end+1];meta['ContentLength']=len(body)
        return meta|{'Body':io.BytesIO(body)}
    def create_multipart_upload(self,**kw):
        self.uploads['own-upload']={'parts':{},'kwargs':kw};return {'UploadId':'own-upload'}
    def upload_part(self,**kw):
        if self.fail_part and kw['PartNumber']==2:raise RuntimeError('SDK exception containing secret; must not surface')
        self.uploads[kw['UploadId']]['parts'][kw['PartNumber']]=kw['Body'];return {'ETag':'part-'+str(kw['PartNumber'])}
    def complete_multipart_upload(self,**kw):
        upload=self.uploads.pop(kw['UploadId']);body=b''.join(upload['parts'][x['PartNumber']] for x in kw['MultipartUpload']['Parts'])
        self.put_object(Key=kw['Key'],Body=body,Metadata=upload['kwargs']['Metadata'],ServerSideEncryption='AES256');return {}
    def abort_multipart_upload(self,**kw):self.aborted.append(kw['UploadId']);self.uploads.pop(kw['UploadId'],None);return {}
    def list_objects_v2(self,**kw):return {'Contents':[{'Key':k} for k in sorted(self.objects) if k.startswith(kw['Prefix'])][:kw['MaxKeys']],'IsTruncated':False}


class ObjectTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.s3=FakeS3();self.settings={'B2_ENDPOINT_URL':'https://s3.eu-central-003.backblazeb2.com','B2_REGION':'eu-central-003','B2_BUCKET':'fixture-bucket'}
    def backends(self):return [LocalObjects(self.root/'local'),B2Objects(self.settings,client=self.s3)]
    def test_same_contract_roundtrip_replay_range_zero_and_list(self):
        for backend in self.backends():
            for body in (b'example\x00\xff',b''):
                digest=hashlib.sha256(body).hexdigest();ref=backend.put_bytes('blobs/'+digest,body)
                self.assertEqual(backend.get_bytes(ref),body)
                before=backend.metrics['put_bytes'];self.assertEqual(backend.put_bytes(ref.key,body),ref)
                self.assertEqual(before,backend.metrics['put_bytes'])
                if body:self.assertEqual(backend.get_range(ref,2,5),body[2:5])
                self.assertIn(ref.key,backend.list_page('blobs/')['keys'])
    def test_immutable_conflict_or_corruption_never_overwritten(self):
        body=b'original';backend=B2Objects(self.settings,client=self.s3)
        ref=backend.put_bytes('blobs/'+hashlib.sha256(body).hexdigest(),body)
        self.s3.objects[ref.key]['body']=b'CORRUPT!'
        with self.assertRaises(ObjectError):backend.put_bytes(ref.key,body)
        self.assertEqual(self.s3.objects[ref.key]['body'],b'CORRUPT!')
    def test_hash_metadata_is_not_enough_and_failure_removes_partial_download(self):
        backend=B2Objects(self.settings,client=self.s3);body=b'original'
        ref=backend.put_bytes('blobs/'+hashlib.sha256(body).hexdigest(),body)
        self.s3.read_corrupt=True;dest=self.root/'out'
        with self.assertRaises(ObjectError):backend.get_file(ref,dest)
        self.assertFalse(dest.exists())
    def test_multipart_roundtrip_and_abort_only_own_upload(self):
        body=b'A'*(5*1024*1024)+b'B'*(5*1024*1024)+b'end'
        backend=B2Objects(self.settings,client=self.s3,part_bytes=5*1024*1024)
        ref=backend.put_bytes('blobs/'+hashlib.sha256(body).hexdigest(),body)
        self.assertEqual(backend.get_bytes(ref),body)
        self.assertFalse(self.s3.uploads)
        self.s3.fail_part=True;body=body+b'new';key='blobs/'+hashlib.sha256(body).hexdigest()
        with self.assertRaises(ObjectError) as error:backend.put_bytes(key,body)
        self.assertNotIn('secret',str(error.exception));self.assertNotIn(key,self.s3.objects)
        self.assertEqual(self.s3.aborted,['own-upload']);self.assertFalse(self.s3.uploads)
    def test_no_mutable_cloud_pointer_and_invalid_paths_rejected(self):
        backend=B2Objects(self.settings,client=self.s3)
        for key in ('../x','/root','a//b','a/./b','a\\b','latest.json'):
            with self.subTest(key=key),self.assertRaises(ObjectError):backend.put_bytes(key,b'anything')
    def test_source_mutation_rejected_before_publish(self):
        backend=LocalObjects(self.root/'local');source=self.root/'source';source.write_bytes(b'initial')
        old=path_digest(source,100);source.write_bytes(b'different')
        with patch('mardorf_collector.storage.objects.path_digest',return_value=old):
            with self.assertRaises(ObjectError):backend.put_file('blobs/'+old[0],source)
        self.assertFalse((backend.root/'blobs'/old[0]).exists())
    def test_invalid_endpoints_metadata_ranges_and_budget(self):
        for endpoint in ('http://s3.eu-central-003.backblazeb2.com','https://evil.example','https://s3.eu-central-003.backblazeb2.com/path'):
            with self.assertRaises(ObjectError):B2Objects(self.settings|{'B2_ENDPOINT_URL':endpoint},client=self.s3)
        backend=LocalObjects(self.root/'local',max_object_bytes=4)
        with self.assertRaises(ObjectError):backend.put_bytes('x',b'12345')
        with self.assertRaises(ObjectError):ObjectRef('x','a'*64,True)
        body=b'1234';ref=backend.put_bytes('blobs/'+hashlib.sha256(body).hexdigest(),body)
        for start,end in ((-1,1),(0,5),(2,2),(True,3)):
            with self.assertRaises(ObjectError):backend.get_range(ref,start,end)
    def test_symlinks_and_concurrent_replay(self):
        backend=LocalObjects(self.root/'local');body=b'same';key='blobs/'+hashlib.sha256(body).hexdigest()
        with ThreadPoolExecutor(max_workers=4) as pool:refs=list(pool.map(lambda _:backend.put_bytes(key,body),range(8)))
        self.assertEqual(len(set(refs)),1);self.assertEqual(backend.metrics['put_bytes'],len(body))
        (backend.root/'evil').symlink_to(self.root,target_is_directory=True)
        with self.assertRaises(ObjectError):backend.put_bytes('evil/x',body)

    def test_download_race_never_removes_someone_elses_destination(self):
        backend=B2Objects(self.settings,client=self.s3);body=b'original'
        ref=backend.put_bytes('blobs/'+hashlib.sha256(body).hexdigest(),body);dest=self.root/'raced'
        original=self.s3.get_object
        def racing(**kw):
            dest.write_bytes(b'other writer');return original(**kw)
        self.s3.get_object=racing
        with self.assertRaises(ObjectError):backend.get_file(ref,dest)
        self.assertEqual(dest.read_bytes(),b'other writer')

    def test_range_stream_errors_are_redacted_and_closed(self):
        backend=B2Objects(self.settings,client=self.s3);body=b'original'
        ref=backend.put_bytes('blobs/'+hashlib.sha256(body).hexdigest(),body)
        class Broken(io.BytesIO):
            def read(self,*args):raise RuntimeError('secret-value')
        stream=Broken();original=self.s3.get_object
        self.s3.get_object=lambda **kw:original(**kw)|{'Body':stream}
        with self.assertRaises(ObjectError) as caught:backend.get_range(ref,0,2)
        self.assertNotIn('secret-value',str(caught.exception));self.assertTrue(stream.closed)

    def test_list_partial_prefix_matches_and_remote_retry_is_honest(self):
        for backend in self.backends():
            body=b'payload';key='blobs/'+hashlib.sha256(body).hexdigest();backend.put_bytes(key,body)
            self.assertEqual(backend.list_page(key[:15])['keys'],[key])
        backend=B2Objects(self.settings,client=self.s3);original=self.s3.head_object
        self.s3.head_object=lambda **kw:original(**kw)|{'ResponseMetadata':{'RetryAttempts':1}}
        backend.head(key)
        self.assertEqual(backend.metrics['provider_retry_attempts'],1)
        self.assertFalse(backend.metrics['byte_accounting_complete'])

    def test_absent_object_is_normal_and_credentials_never_fall_back(self):
        backend=B2Objects(self.settings,client=self.s3)
        self.assertIsNone(backend.head('blobs/'+'a'*64))
        self.assertEqual(backend.metrics['failed_requests'],0)
        with self.assertRaisesRegex(ObjectError,'Explicit'):B2Objects(self.settings)

if __name__=='__main__':unittest.main()
