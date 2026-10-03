"""Shared external-only archive reader/writer bound to one explicit control head."""
from pathlib import Path
import json
import os
from collections import OrderedDict

from .objects import B2Objects,ObjectError,ObjectRef
from .archive import PackedArchive,decode
from .configuration import b2_settings,resolve_b2_bucket
from .head import GitHubHead
from .workspace import update,materialize


class CachedArchive(PackedArchive):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs);self.manifests=OrderedDict();self.manifest_bytes=0
    def read_json(self,value):
        ref=ObjectRef.parse(value) if isinstance(value,dict) else value
        if not ref.key.startswith(self.prefix+'/') or ref.bytes>1024**2:raise ObjectError('Manifest scope/budget')
        key=(ref.key,ref.sha256,ref.bytes)
        body=self.manifests.get(key)
        if body is None:
            body=self.backend.get_bytes(ref)
            while self.manifests and self.manifest_bytes+len(body)>16*1024**2:
                _,old=self.manifests.popitem(last=False);self.manifest_bytes-=len(old)
            self.manifests[key]=body;self.manifest_bytes+=len(body)
        self.manifests.move_to_end(key)
        return decode(body)


class CloudRuntime:
    def __init__(self,config,environ,*,role='writer',backend=None,head=None):
        if (config.get('schema_version')!=1 or config.get('artifact_version')!='dev03-cloud-runtime-v1' or
            type(config.get('production_enabled')) is not bool):
            raise ObjectError('Explicit cloud runtime profile required')
        self.config=config
        self.role=role
        env=dict(environ)
        if role=='reader':
            if not env.get('B2_WEATHER_READER_KEY_ID') or not env.get('B2_WEATHER_READER_KEY'):
                raise ObjectError('Explicit read-only weather credentials required')
            env.update(B2_APPLICATION_KEY_ID=env['B2_WEATHER_READER_KEY_ID'],B2_APPLICATION_KEY=env['B2_WEATHER_READER_KEY'])
        elif role!='writer':raise ObjectError('Unknown weather role')
        self.backend=backend or B2Objects(resolve_b2_bucket(b2_settings(env)))
        self.archive=CachedArchive(self.backend,prefix=config['archive_prefix'])
        self.read_cache=OrderedDict();self.read_cache_bytes=0
        self.head=head or GitHubHead(config['private_repository'],env.get('PRIVATE_REPO_TOKEN') or env.get('GH_TOKEN',''),
                                    branch=env.get('CLOUD_CONTROL_BRANCH',config['control_branch']),
                                    path=env.get('CLOUD_CONTROL_PATH',config['control_path']),bootstrap=config['bootstrap_snapshot'])
        self.snapshot=None;self.parent=None

    def open(self):
        self.parent,head=self.head.read()
        self.snapshot=ObjectRef.parse(head['snapshot'])
        if not self.snapshot.key.startswith(self.config['archive_prefix']+'/snapshots/'):
            raise ObjectError('Cloud control snapshot namespace mismatch')
        self.archive.validate_root(self.archive.read_json(self.snapshot))
        return self.snapshot

    def read(self,path,*,required=True):
        if self.snapshot is None:self.open()
        entries=[item for item in self.archive.records(self.snapshot,prefixes=(path,)) if item['path']==path]
        if not entries:
            if required:raise ObjectError('Required external file absent')
            return None
        if len(entries)!=1:raise ObjectError('Duplicate external path')
        item=entries[0];key=(item['path'],item['sha256'],item['bytes'])
        body=self.read_cache.get(key)
        if body is None:
            body=self.archive.read_file(item)
            while self.read_cache and self.read_cache_bytes+len(body)>64*1024**2:
                _,old=self.read_cache.popitem(last=False);self.read_cache_bytes-=len(old)
            self.read_cache[key]=body;self.read_cache_bytes+=len(body)
        self.read_cache.move_to_end(key)
        return body

    def publish(self,changes,*,metadata,merge=None,extra_refs=None,attempts=8):
        if self.role!='writer':raise ObjectError('Reader runtime cannot publish weather')
        if not 1<=attempts<=8:raise ObjectError('Invalid cloud CAS retry budget')
        for attempt in range(attempts):
            self.open()
            selected=merge(self,changes) if merge is not None else changes
            if not selected:return {'status':'unchanged','snapshot':self.snapshot.json(),'cas_attempts':attempt+1}
            publication_metadata=dict(metadata,previous_snapshot=self.snapshot.json())
            result=update(self.archive,self.snapshot,selected,metadata=publication_metadata)
            # A full root with verified immutable objects exists before Git visibility.
            control=extra_refs(result) if extra_refs else {}
            if self.head.advance(self.parent,result.json(),metadata=publication_metadata,extra_refs=control):
                self.snapshot=result
                return {'status':'published','snapshot':result.json(),'cas_attempts':attempt+1,
                        'weather_git_bytes_written':0,'changed_paths':len(selected)}
        raise ObjectError('Cloud CAS exhausted; prior complete head remains authoritative')

    def hydrate(self,target,*,prefixes=(),paths=(),max_bytes=512*1024**2):
        self.open()
        return materialize(self.archive,self.snapshot,target,prefixes=prefixes,paths=paths,max_bytes=max_bytes)


def load_runtime(root,*,role='writer',environ=None):
    root=Path(root)
    config=json.loads((root/'config/dev03_cloud_runtime_v1.json').read_text())
    return CloudRuntime(config,os.environ if environ is None else environ,role=role)
