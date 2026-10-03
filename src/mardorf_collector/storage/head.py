"""Small Git control reference with CAS; all weather bytes live in B2."""
import base64
import hashlib
import json
import re

from .objects import ObjectError,ObjectRef
from .archive import canonical,decode,file_path


class GitHubHead:
    """An unpublished control blob is verified before a non-forced ref advance."""
    def __init__(self,repository,token,*,branch,path,bootstrap,session=None):
        if (not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repository) or
            not re.fullmatch(r'[A-Za-z0-9_./-]{1,200}',branch) or '..' in branch or not token):
            raise ObjectError('Invalid cloud control configuration')
        if not file_path(path).startswith('config/cloud_refs/'):
            raise ObjectError('Cloud control path must be explicitly scoped')
        ObjectRef.parse(bootstrap)
        import requests
        self.session=session or requests.Session()
        self.headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json',
                      'X-GitHub-Api-Version':'2022-11-28'}
        self.repository,self.branch,self.path,self.bootstrap=repository,branch,path,bootstrap
        self.url='https://api.github.com/repos/'+repository

    def call(self,method,suffix,*,allowed=(),**kwargs):
        try:
            response=self.session.request(method,self.url+suffix,headers=self.headers,timeout=30,**kwargs)
        except Exception as exc:raise ObjectError('Cloud control network error') from exc
        if response.status_code in allowed:return None
        if not response.ok:raise ObjectError('Cloud control API request failed')
        if len(response.content)>2*1024**2:raise ObjectError('Cloud control response exceeds budget')
        return response.json()

    def read(self):
        commit=self.call('GET','/git/ref/heads/'+self.branch)['object']['sha']
        document=self.call('GET','/contents/'+self.path,allowed=(404,),params={'ref':commit})
        if document is None:return commit,{'schema_version':1,'artifact_version':'cloud-weather-head-v1','snapshot':self.bootstrap}
        if document.get('type')!='file' or document.get('encoding')!='base64' or document.get('size',0)>1024**2:
            raise ObjectError('Cloud head content metadata invalid')
        body=base64.b64decode(document['content'].replace('\n',''),validate=True)
        if (len(body)!=document['size'] or hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()!=document['sha']):
            raise ObjectError('Cloud head content identity mismatch')
        head=decode(body)
        if head.get('schema_version')!=1 or head.get('artifact_version')!='cloud-weather-head-v1':
            raise ObjectError('Unsupported cloud control head')
        ObjectRef.parse(head['snapshot'])
        return commit,head

    def advance(self,parent,snapshot,*,metadata,extra_refs=None):
        ObjectRef.parse(snapshot)
        if not re.fullmatch('[0-9a-f]{40}',parent):raise ObjectError('Invalid control parent')
        allowed={'channel','producer_repository','producer_commit','original_generated_at_utc','event_id','changed_path_count','payload_sha256','receipt_sha256','previous_snapshot'}
        if not isinstance(metadata,dict) or set(metadata)-allowed:raise ObjectError('Only control metadata allowed in Git')
        head={'schema_version':1,'artifact_version':'cloud-weather-head-v1','snapshot':snapshot,'metadata':metadata}
        controls={self.path:head}
        controls.update(extra_refs or {})
        tree=self.call('GET','/git/commits/'+parent)['tree']['sha']
        elements=[];expected_blobs={}
        for path,document in sorted(controls.items()):
            if not file_path(path).startswith('config/cloud_refs/'):raise ObjectError('Unsafe extra cloud control path')
            if not isinstance(document,dict) or set(document)-{'schema_version','artifact_version','snapshot','metadata','kind','generated_at_utc','receipt_sha256','readback_verified','bundle_ready'}:
                raise ObjectError('Only thin cloud reference documents allowed')
            if 'metadata' in document and (not isinstance(document['metadata'],dict) or set(document['metadata'])-allowed):
                raise ObjectError('Only control metadata allowed in Git')
            if 'snapshot' in document:ObjectRef.parse(document['snapshot'])
            body=canonical(document)
            if len(body)>65536:raise ObjectError('Thin cloud control exceeds budget')
            blob=self.call('POST','/git/blobs',json={'encoding':'base64','content':base64.b64encode(body).decode()})['sha']
            expected=hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()
            if blob!=expected:raise ObjectError('Cloud control upload Git blob identity mismatch')
            readback=self.call('GET','/git/blobs/'+blob)
            if readback.get('encoding')!='base64' or base64.b64decode(readback['content'].replace('\n',''),validate=True)!=body:
                raise ObjectError('Unpublished cloud control exact readback mismatch')
            expected_blobs[path]=blob
            elements.append({'path':path,'mode':'100644','type':'blob','sha':blob})
        tree=self.call('POST','/git/trees',json={'base_tree':tree,'tree':elements})['sha']
        new=self.call('POST','/git/commits',json={'message':'cloud: publish verified immutable weather reference',
                         'tree':tree,'parents':[parent]})['sha']
        committed=self.call('GET','/git/commits/'+new)
        if committed['tree']['sha']!=tree or [x['sha'] for x in committed['parents']]!=[parent]:
            raise ObjectError('Unpublished cloud control commit binding mismatch')
        for path,digest in expected_blobs.items():
            actual=self.call('GET','/contents/'+path,params={'ref':new})
            if actual.get('sha')!=digest:raise ObjectError('Unpublished cloud control tree path mismatch')
        result=self.call('PATCH','/git/refs/heads/'+self.branch,allowed=(409,422),json={'sha':new,'force':False})
        if result is None:return False
        if result['object']['sha']!=new:raise ObjectError('Cloud control ref advance mismatch')
        # A later fast-forward cannot retract an accepted immutable B2 root.
        return True
