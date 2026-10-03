import tempfile
from pathlib import Path
import unittest

from mardorf_collector.storage.objects import LocalObjects,ObjectError
from mardorf_collector.storage.archive import PackedArchive
from mardorf_collector.storage.workspace import update,materialize


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.backend=LocalObjects(self.root/'objects')
        self.archive=PackedArchive(self.backend,prefix='weather',shard_records=2)
        self.base=self.archive.export(iter([('b',b'b'),('d',b'd'),('f',b'f'),('h',b'h')]),metadata={'original_clock':'unchanged'})

    def test_copy_on_write_preserves_untouched_shards_and_original_identity(self):
        root=self.archive.read_json(self.base)
        result=update(self.archive,self.base,{'a':b'a','b':b'changed','e':b'e','z':b'z'},metadata={'original_clock':'2000-01-01'})
        entries={x['path']:self.archive.read_file(x) for x in self.archive.records(result)}
        self.assertEqual(entries,dict(a=b'a',b=b'changed',d=b'd',e=b'e',f=b'f',h=b'h',z=b'z'))
        self.assertEqual(self.archive.read_json(result)['file_count'],7)
        self.assertEqual(self.archive.read_json(result)['original_bytes'],13)
        self.assertEqual(self.archive.read_file(list(self.archive.records(self.base))[0]),b'b')
        self.assertEqual(update(self.archive,result,{},metadata={}),result)

    def test_exact_selective_materialization_and_missing_path_fail_before_download(self):
        result=materialize(self.archive,self.base,self.root/'out',paths=('b',))
        self.assertEqual(result['files'],1);self.assertEqual((self.root/'out/b').read_bytes(),b'b')
        self.assertFalse((self.root/'out/d').exists())
        before=self.backend.metrics['range_bytes']
        with self.assertRaises(ObjectError):materialize(self.archive,self.base,self.root/'out',paths=('missing',))
        self.assertEqual(before,self.backend.metrics['range_bytes'])

    def test_empty_base_and_interrupted_updates_keep_prior_complete_snapshot(self):
        empty=self.archive.export(iter([]),metadata={})
        filled=update(self.archive,empty,{'first':b'123'},metadata={})
        self.assertEqual(len(list(self.archive.records(filled))),1)
        with self.assertRaises(ObjectError):update(self.archive,self.base,{'../escape':b'x'},metadata={})
        self.assertEqual(len(list(self.archive.records(self.base))),4)

if __name__=='__main__':unittest.main()
