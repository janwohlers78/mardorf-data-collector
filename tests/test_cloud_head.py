import base64
import hashlib
import json
import unittest
from unittest.mock import Mock

from mardorf_collector.storage.head import GitHubHead,control_metadata
from mardorf_collector.storage.objects import ObjectError


REF={'key':'weather/snapshots/'+'a'*64,'sha256':'a'*64,'bytes':123,'schema_version':1}


class HeadTests(unittest.TestCase):
    def head(self):
        return GitHubHead('fixture/repository','secret',branch='main',
                          path='config/cloud_refs/head_v1.json',bootstrap=REF,session=Mock())

    def test_metadata_only_exact_unpublished_readback_and_non_forced_cas(self):
        head=self.head();body={}
        def call(method,path,**kw):
            if path=='/git/commits/'+'b'*40:return {'tree':{'sha':'c'*40}}
            if path=='/git/blobs':
                raw=base64.b64decode(kw['json']['content']);body['raw']=raw
                body['sha']=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
                return {'sha':body['sha']}
            if path.startswith('/git/blobs/'):return {'encoding':'base64','content':base64.b64encode(body['raw']).decode()}
            if path=='/git/trees':return {'sha':'d'*40}
            if path=='/git/commits':return {'sha':'e'*40}
            if path=='/git/commits/'+'e'*40:return {'tree':{'sha':'d'*40},'parents':[{'sha':'b'*40}]}
            if path.startswith('/contents/'):return {'sha':body['sha']}
            if path.startswith('/git/refs/'):
                self.assertFalse(kw['json']['force']);return None
            raise AssertionError('unexpected API route')
        head.call=call
        self.assertFalse(head.advance('b'*40,REF,metadata={'channel':'fixture'}))
        self.assertNotIn('temperature',body['raw'].decode())
        with self.assertRaisesRegex(ObjectError,'control metadata'):
            head.advance('b'*40,REF,metadata={'temperature':20.0})
        with self.assertRaises(ObjectError):
            head.advance('b'*40,REF,metadata={},extra_refs={'data/weather.json':{}})

    def test_network_failure_diagnostic_does_not_expose_token(self):
        head=self.head();head.session.request.side_effect=RuntimeError('signed-url secret')
        with self.assertRaises(ObjectError) as context:head.read()
        self.assertNotIn('secret',str(context.exception))

    def test_native_acquisition_control_roundtrips_existing_thin_reference_contract(self):
        from mardorf_collector.runtime.native_source_seed import control,CONTROL
        head=self.head();blobs={};paths={}
        marker=dict(source_generated_at_utc='2026-10-10T13:40:00Z',all_native_sources_ready=True)
        document=control(marker,REF,event_id=123,repository='fixture/repository')
        def call(method,path,**kw):
            if path=='/git/commits/'+'b'*40:return {'tree':{'sha':'c'*40}}
            if path=='/git/blobs':
                raw=base64.b64decode(kw['json']['content'])
                sha=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
                blobs[sha]=raw;return {'sha':sha}
            if path.startswith('/git/blobs/'):
                return {'encoding':'base64','content':base64.b64encode(blobs[path.rsplit('/',1)[1]]).decode()}
            if path=='/git/trees':
                paths.update({x['path']:x['sha'] for x in kw['json']['tree']});return {'sha':'d'*40}
            if path=='/git/commits':return {'sha':'e'*40}
            if path=='/git/commits/'+'e'*40:return {'tree':{'sha':'d'*40},'parents':[{'sha':'b'*40}]}
            if path.startswith('/contents/'):return {'sha':paths[path[len('/contents/'):]]}
            if path.startswith('/git/refs/'):
                self.assertFalse(kw['json']['force']);return {'object':{'sha':'e'*40}}
            raise AssertionError(path)
        head.call=call
        self.assertTrue(head.advance('b'*40,REF,metadata={},extra_refs={CONTROL:document}))
        saved=json.loads(blobs[paths[CONTROL]])
        self.assertEqual(saved['generated_at_utc'],marker['source_generated_at_utc'])
        self.assertEqual(saved['metadata']['channel'],'native-acquisition-only')

    def test_nested_weather_values_and_unknown_clocks_are_not_git_metadata(self):
        for value in ({'channel':{'temperature':20}}, {'producer_commit':'unknown'},
                      {'changed_path_count':True},{'original_generated_at_utc':'2000-01-01T00:00:00'},
                      {'previous_snapshot':dict(REF,bytes=-1)}):
            with self.subTest(value=value),self.assertRaises(ObjectError):control_metadata(value)

if __name__=='__main__':unittest.main()
