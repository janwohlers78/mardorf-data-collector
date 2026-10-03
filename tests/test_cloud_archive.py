import tempfile
from pathlib import Path
import unittest

from mardorf_collector.storage.objects import LocalObjects, ObjectError
from mardorf_collector.storage.archive import PackedArchive, digest


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.backend=LocalObjects(self.root/'objects')
        self.archive=PackedArchive(self.backend,prefix='weather',pack_bytes=2048,shard_records=2)

    def export(self):
        files=[('data/calibration/a.json',b'{"native":123}\n'*100),
               ('data/raw/2026/10/01/b.bin',bytes(range(256))*3),
               ('data/raw/2026/10/02/c.json',b'{}'),('empty',b'')]
        return self.archive.export(iter(files),metadata={'source_commit':'original','original_clock':'unchanged'})

    def test_selective_restore_and_replay_keep_exact_original_identity(self):
        snapshot=self.export();before=self.backend.metrics['put_bytes'];self.assertEqual(self.export(),snapshot)
        self.assertEqual(before,self.backend.metrics['put_bytes'])
        entries=list(self.archive.records(snapshot,prefixes=('data/calibration/',)))
        self.assertEqual(len(entries),1);self.assertEqual(self.archive.read_file(entries[0]),b'{"native":123}\n'*100)
        result=self.archive.hydrate(snapshot,self.root/'restored',prefixes=('data/calibration/',))
        self.assertEqual(result['files'],1);self.assertFalse((self.root/'restored/data/raw').exists())
        self.assertEqual(self.archive.read_json(snapshot)['metadata']['original_clock'],'unchanged')

    def test_corrupt_range_and_false_decoded_size_fail_closed(self):
        entry=list(self.archive.records(self.export(),prefixes=('data/calibration/',)))[0]
        changed=dict(entry,bytes=entry['bytes']-1)
        with self.assertRaises(ObjectError):self.archive.read_file(changed)
        body=self.root/'objects'/entry['object']['key'];body.write_bytes(b'X'*body.stat().st_size)
        with self.assertRaises(ObjectError):self.archive.read_file(entry)

    def test_traversal_symlink_and_selection_budget_are_rejected(self):
        with self.assertRaises(ObjectError):self.archive.export(iter([('../escape',b'x')]),metadata={})
        snapshot=self.export()
        with self.assertRaises(ObjectError):list(self.archive.records(snapshot,max_files=1))
        target=self.root/'link';target.symlink_to(self.root/'objects',target_is_directory=True)
        with self.assertRaises(ObjectError):self.archive.hydrate(snapshot,target,prefixes=('data/',))
        with self.assertRaises(ObjectError):self.archive.hydrate(snapshot,self.root/'out',prefixes=('data/',),max_bytes=1)

    def test_direct_read_rejects_oversized_or_out_of_scope_reference_before_io(self):
        entry=list(self.archive.records(self.export()))[0]
        before=self.backend.metrics['requests']
        for changed in (dict(entry,bytes=65*1024**2), dict(entry,offset=-1),
                        dict(entry,sha256='z'*64),
                        dict(entry,object=dict(entry['object'],key='another/packs/a'))):
            with self.assertRaises(ObjectError):self.archive.read_file(changed)
        self.assertEqual(before,self.backend.metrics['requests'])

    def test_partial_export_never_publishes_snapshot_and_shard_tampering_is_rejected(self):
        def interrupted():
            yield 'a',b'x'
            raise OSError('interrupted')
        with self.assertRaises(OSError):self.archive.export(interrupted(),metadata={})
        self.assertEqual(self.backend.list_page('weather/snapshots/',limit=100)['keys'],[])
        snapshot=self.export();root=self.archive.read_json(snapshot);root['file_count']+=1
        bad=self.archive.put_json(root,'snapshots')
        with self.assertRaises(ObjectError):list(self.archive.records(bad))

if __name__=='__main__':unittest.main()
