import tempfile
from pathlib import Path
import unittest

from mardorf_collector.storage.objects import LocalObjects,ObjectError
from mardorf_collector.storage.archive import PackedArchive
from mardorf_collector.storage.runtime import CloudRuntime
from mardorf_collector.storage.workspace import update


class MemoryHead:
    def __init__(self,ref):self.ref=ref;self.count=0;self.race=False;self.competitor=None
    def read(self):return 'a'*40,{'snapshot':self.ref}
    def advance(self,parent,snapshot,**kwargs):
        self.count+=1
        if self.race:
            self.race=False
            if self.competitor:self.ref=self.competitor(self.ref)
            return False
        self.ref=snapshot;return True


class RuntimeTests(unittest.TestCase):
    def test_retry_and_cold_empty_workdir_have_one_external_authority(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);backend=LocalObjects(root/'objects')
            archive=PackedArchive(backend,prefix='weather/archive')
            base=archive.export(iter([('data/fixture.json',b'original')]),metadata={})
            config={'schema_version':1,'artifact_version':'dev03-cloud-runtime-v1','production_enabled':False,'archive_prefix':'weather/archive',
                    'private_repository':'fixture/repo','control_branch':'main','control_path':'config/cloud_refs/head.json','bootstrap_snapshot':base.json()}
            head=MemoryHead(base.json());head.race=True
            def competing_writer(ref):
                return update(archive,ref,{'data/other-writer.json':b'concurrent'},metadata={'channel':'other'}).json()
            head.competitor=competing_writer
            runtime=CloudRuntime(config,{},backend=backend,head=head)
            result=runtime.publish({'data/next.json':b'next'},metadata={'channel':'fixture'})
            self.assertEqual(result['cas_attempts'],2);self.assertEqual(result['weather_git_bytes_written'],0)
            runtime.hydrate(root/'fresh',prefixes=('data/',))
            self.assertEqual((root/'fresh/data/fixture.json').read_bytes(),b'original')
            self.assertEqual((root/'fresh/data/next.json').read_bytes(),b'next')
            self.assertEqual((root/'fresh/data/other-writer.json').read_bytes(),b'concurrent')
            predecessor=archive.read_json(runtime.snapshot)['metadata']['previous_snapshot']
            self.assertEqual(len(list(archive.records(predecessor))),2)
            with self.assertRaises(ObjectError):runtime.read('missing')

if __name__=='__main__':unittest.main()
