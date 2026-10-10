import unittest
from collections import Counter
from mardorf_collector.storage.objects import ObjectRef
from mardorf_collector.wp13.native_cold_reads_v1 import cache_balanced_references

class CacheBalancedOrderTests(unittest.TestCase):
    def test_digest_sorted_objects_do_not_all_enter_one_cache_shard(self):
        refs=[ObjectRef('weather/native/'+format(s,'x')+format(i,'063x'),format(s,'x')+format(i,'063x'),32114) for s in range(16) for i in range(128)]
        ordered=list(cache_balanced_references(refs))
        self.assertCountEqual(ordered,refs)
        self.assertEqual(Counter(ref.sha256[0] for ref in ordered[:48]),Counter({format(i,'x'):3 for i in range(16)}))
        self.assertEqual(ordered,list(cache_balanced_references(reversed(refs))))
    def test_unequal_and_empty_groups_preserve_all_objects_without_padding(self):
        refs=[ObjectRef('weather/native/'+format(i,'064x'),format(i,'064x'),1) for i in range(10)]
        self.assertEqual(list(cache_balanced_references(refs)),refs)
        self.assertEqual(list(cache_balanced_references([])),[])
