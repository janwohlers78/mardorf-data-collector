"""Actual source fidelity, bounded chunks, mutation and interrupted restores."""
import hashlib
from pathlib import Path
import tempfile
import unittest

from mardorf_collector.storage.objects import LocalObjects, ObjectError
from mardorf_collector.storage.parents import ParentStore, CHUNK_BYTES
from mardorf_collector.storage.archive import canonical


class OriginalParentsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.backend = LocalObjects(self.root / 'objects')
        self.store = ParentStore(self.backend, prefix='weather/provider-originals/wp15')

    def test_exact_original_across_chunk_boundary_metadata_read_and_replay(self):
        original = Path(__file__).parent / 'fixtures/wp13/native_weather_v1.grib2'
        body = original.read_bytes()
        data = (body * (CHUNK_BYTES // len(body) + 2)) + b'original final bytes'
        source = self.root / 'source.grib2'
        source.write_bytes(data)
        ref = self.store.write_file(source, metadata={'capture': 'fixture'})
        manifest = self.store.manifest(ref)
        self.assertEqual(len(manifest['chunks']), 2)
        self.assertEqual(manifest['sha256'], hashlib.sha256(data).hexdigest())
        self.assertEqual(self.store.write_file(source, metadata={'capture': 'fixture'}), ref)
        restored = self.store.restore(ref, self.root / 'restored.grib2')
        self.assertEqual(restored.read_bytes(), data)

    def test_order_and_corruption_do_not_publish_incomplete_restored_file(self):
        source = self.root / 'source.grib2'
        source.write_bytes(b'a' * CHUNK_BYTES + b'b' * CHUNK_BYTES)
        ref = self.store.write_file(source, metadata={})
        manifest = self.store.manifest(ref)
        manifest['chunks'].reverse()
        changed = canonical(manifest)
        bad = self.backend.put_bytes(self.store.prefix + '/parents/' + hashlib.sha256(changed).hexdigest(), changed)
        target = self.root / 'bad.grib2'
        with self.assertRaisesRegex(ObjectError, 'whole-file'):
            self.store.restore(bad, target)
        self.assertFalse(target.exists())
        self.assertFalse(list(self.root.glob('tmp*')))

    def test_namespace_symlink_empty_and_missing_chunks_rejected(self):
        source = self.root / 'source'
        source.write_bytes(b'GRIB original bytes')
        link = self.root / 'link'
        link.symlink_to(source)
        with self.assertRaises(ObjectError):
            self.store.write_file(link, metadata={})
        source.write_bytes(b'')
        with self.assertRaises(ObjectError):
            self.store.write_file(source, metadata={})
        source.write_bytes(b'GRIB original bytes')
        ref = self.store.write_file(source, metadata={})
        manifest = self.store.manifest(ref)
        manifest['chunks'] = []
        changed = canonical(manifest)
        bad = self.backend.put_bytes(self.store.prefix + '/parents/' + hashlib.sha256(changed).hexdigest(), changed)
        with self.assertRaisesRegex(ObjectError, 'chunk count'):
            self.store.manifest(bad)


if __name__ == '__main__':
    unittest.main()
