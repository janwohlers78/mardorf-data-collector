"""Copy-on-write exact archive updates and bounded ephemeral materialization."""
from bisect import bisect_left
from collections import defaultdict
import io
import os
from pathlib import Path
import tempfile

from .archive import PackedArchive,file_path
from .objects import ObjectError,ObjectRef


def update(archive, snapshot, changes, *, metadata):
    """No deletion. Reuse untouched indexes/packs; publish complete root last."""
    original=archive.read_json(snapshot);archive.validate_root(original)
    changes=dict(changes)
    if not changes:return snapshot
    if len(changes)>10000:raise ObjectError('Explicit larger change budget required')
    delta=archive.export(iter(sorted(changes.items())),metadata=metadata)
    incoming=list(archive.records(delta))
    shards=original['shards'];ends=[item['last'] for item in shards]
    assigned=defaultdict(list)
    for item in incoming:
        index=min(bisect_left(ends,item['path']),len(shards)-1) if shards else -1
        assigned[index].append(item)
    new_shards=[];added_files=0;added_bytes=0
    for index in range(-1,len(shards)):
        if index==-1 and shards:continue
        if index not in assigned:
            if index>=0:new_shards.append(shards[index])
            continue
        old=archive.read_json(shards[index]['ref'])['records'] if index>=0 else []
        if index>=0 and (len(old)!=shards[index]['count'] or not old or
                        old[0]['path']!=shards[index]['first'] or old[-1]['path']!=shards[index]['last'] or
                        [item['path'] for item in old]!=sorted({item['path'] for item in old})):
            raise ObjectError('Corrupt update shard')
        values={}
        for item in old:
            archive.validate_item(item);values[item['path']]=item
        for item in assigned[index]:
            previous=values.get(item['path'])
            added_files+=int(previous is None)
            added_bytes+=item['bytes']-(previous['bytes'] if previous else 0)
            values[item['path']]=item
        records=[values[name] for name in sorted(values)]
        for offset in range(0,len(records),archive.shard_records):
            chunk=records[offset:offset+archive.shard_records]
            ref=archive.put_json({'schema_version':1,'records':chunk},'indexes')
            new_shards.append({'first':chunk[0]['path'],'last':chunk[-1]['path'],'count':len(chunk),'ref':ref.json()})
    root=dict(original,metadata=metadata,shards=new_shards,file_count=original['file_count']+added_files,
              original_bytes=original['original_bytes']+added_bytes)
    archive.validate_root(root)
    return archive.put_json(root,'snapshots')


def materialize(archive,snapshot,target,*,prefixes=(),paths=(),max_bytes=512*1024**2,max_files=20000):
    """Select paths/prefixes before reads; parent packs shared by many files read once."""
    if type(max_bytes)is not int or not 0<max_bytes<=4*1024**3:raise ObjectError('Invalid workdir budget')
    paths=tuple(file_path(path) for path in paths)
    prefixes=tuple(prefixes)
    if not paths and not prefixes:raise ObjectError('Explicit workspace selection required')
    target=Path(target).absolute()
    if any(p.is_symlink() for p in (target,*target.parents)):raise ObjectError('Symlink workdir')
    selected=list(archive.records(snapshot,prefixes=prefixes+paths,max_files=max_files))
    selected=[item for item in selected if item['path'] in paths or any(item['path'].startswith(p) for p in prefixes)]
    missing=set(paths)-{item['path'] for item in selected}
    if missing:raise ObjectError('Required workspace paths absent')
    if sum(item['bytes'] for item in selected)>max_bytes:raise ObjectError('Workspace bytes exceeded')
    grouped=defaultdict(list)
    for item in selected:grouped[item['object']['key']].append(item)
    range_bytes=0;parent_bytes=0
    for items in grouped.values():
        ref=ObjectRef.parse(items[0]['object'])
        if any(ObjectRef.parse(item['object'])!=ref for item in items):raise ObjectError('Conflicting parent object identities')
        parent=None
        if len(items)>=8 and sum(item['stored_bytes'] for item in items)>=ref.bytes//2:
            parent=archive.backend.get_bytes(ref)
            parent_bytes+=len(parent)
        class SliceReader:
            def get_range(self,asked,start,end):
                if asked!=ref:raise ObjectError('Parent identity mismatch')
                return parent[start:end]
        reader=PackedArchive(SliceReader(),prefix=archive.prefix,pack_bytes=archive.pack_bytes,file_bytes=archive.file_bytes,shard_records=archive.shard_records) if parent is not None else archive
        for item in items:
            path=target/item['path']
            if any(p.is_symlink() for p in (path,*path.parents)):raise ObjectError('Symlink materialization path')
            body=reader.read_file(item)
            if parent is None:range_bytes+=item['stored_bytes']
            path.parent.mkdir(parents=True,exist_ok=True)
            fd,temporary=tempfile.mkstemp(prefix='.cloud-work-',dir=path.parent)
            try:
                with os.fdopen(fd,'wb') as output:
                    output.write(body);output.flush();os.fsync(output.fileno())
                os.replace(temporary,path)
            finally:Path(temporary).unlink(missing_ok=True)
    return {'files':len(selected),'original_bytes':sum(item['bytes'] for item in selected),
            'selected_range_bytes':range_bytes,'shared_full_parent_bytes':parent_bytes,
            'authoritative_snapshot':snapshot.json() if isinstance(snapshot,ObjectRef) else snapshot}
