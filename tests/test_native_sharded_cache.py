import tempfile
import unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from mardorf_collector.storage.objects import LocalObjects,ObjectError
from mardorf_collector.wp13.native_cold_reads_v1 import NativeVerifiedReads

class NativeShardedCacheTests(unittest.TestCase):
    def test_parallel_reads_preserve_every_byte_then_reuse_without_network(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);remote=LocalObjects(root/'remote');reads=NativeVerifiedReads(remote,root/'cache',max_bytes=16384)
            refs=[remote.put_bytes('weather/object/'+str(i),('null field, complete original '+str(i)).encode()) for i in range(128)]
            expected=[remote.get_bytes(ref) for ref in refs]
            with ThreadPoolExecutor(max_workers=48) as pool:got=list(pool.map(reads.get_bytes,refs))
            self.assertEqual(got,expected)
            with patch.object(remote,'get_bytes',side_effect=AssertionError('unexpected canonical reread')):
                self.assertEqual([reads.get_bytes(ref) for ref in refs],expected)
            self.assertEqual(reads.metrics['misses'],128)
            self.assertEqual(reads.metrics['hits'],128)
            self.assertLessEqual(sum(p.stat().st_size for p in (root/'cache').glob('*/*') if len(p.name)==64),16384)

    def test_corrupted_shard_is_retrieved_and_canonical_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);remote=LocalObjects(root/'remote');reads=NativeVerifiedReads(remote,root/'cache',max_bytes=16384)
            ref=remote.put_bytes('weather/original',b'all original fields');self.assertEqual(reads.get_bytes(ref),b'all original fields')
            path=root/'cache'/ref.sha256[0]/ref.sha256;path.write_bytes(b'corrupted cache')
            self.assertEqual(reads.get_bytes(ref),b'all original fields');self.assertEqual(reads.metrics['corrupt_entries'],1)
            path.write_bytes(b'corrupted cache');(remote.root/ref.key).write_bytes(b'corrupted canonical')
            with self.assertRaises(ObjectError):reads.get_bytes(ref)

    def test_quota_and_range_missing_shard_never_force_full_download(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);remote=LocalObjects(root/'remote');reads=NativeVerifiedReads(remote,root/'cache',max_bytes=16384)
            ref=remote.put_bytes('weather/original',b'a'*2048)
            with patch.object(remote,'get_bytes',side_effect=AssertionError('full download')):
                self.assertEqual(reads.get_range(ref,1,3),b'aa')
            self.assertEqual(reads.get_bytes(ref),b'a'*2048)
            self.assertEqual(reads.metrics['misses'],0)
