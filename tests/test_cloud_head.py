import base64
import hashlib
import json
import unittest
from unittest.mock import Mock

from mardorf_collector.storage.head import GitHubHead
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

if __name__=='__main__':unittest.main()
