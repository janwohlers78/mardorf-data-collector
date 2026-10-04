"""Mirror fallback preserves one exact request and has a finite retry budget."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from mardorf_collector.providers.ecmwf_mirrors import MirrorClient

class MirrorTests(unittest.TestCase):
    def test_failed_mirror_cannot_leave_partial_bytes_or_change_request(self):
        calls=[]
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder)/'batch.grib2'
            request=dict(date='20261004',time=0,step=[144,150],param=['10u'],target=str(target))
            def factory(**options):
                source=options['source'];calls.append((options,[]))
                class Client:
                    def retrieve(self,**actual):
                        calls[-1][1].append(actual)
                        if source=='azure':target.write_bytes(b'partial');raise OSError('unavailable')
                        self_outer.assertFalse(target.exists());target.write_bytes(b'GRIB-original');return 'result'
                return Client()
            self_outer=self;client=MirrorClient(factory,'azure')
            self.assertEqual(client.retrieve(**request),'result');self.assertEqual(client.source,'google')
            self.assertEqual([row[0]['source'] for row in calls],['azure','google'])
            for options,seen in calls:
                self.assertEqual(seen,[request]);self.assertEqual(options['maximum_retries'],2)
                self.assertEqual(options['retry_after'],5);self.assertFalse(options['use_server_retry_after'])
    def test_all_mirrors_fail_once_in_requested_order(self):
        seen=[]
        def factory(**options):
            seen.append(options['source'])
            class Client:
                def retrieve(self,**request):raise TimeoutError('url with private query not exposed')
            return Client()
        with self.assertRaisesRegex(RuntimeError,'mirrors exhausted') as result:MirrorClient(factory,'google').retrieve(step=[360])
        self.assertEqual(seen,['google','azure','ecmwf']);self.assertNotIn('private query',str(result.exception))
    def test_success_does_not_call_other_mirrors(self):
        seen=[]
        def factory(**options):
            seen.append(options['source'])
            class Client:
                def retrieve(self,**request):return b'original'
            return Client()
        self.assertEqual(MirrorClient(factory,'ecmwf').retrieve(step=[360]),b'original')
        self.assertEqual(seen,['ecmwf'])
    def test_invalid_source_rejected_without_network(self):
        with self.assertRaises(ValueError):MirrorClient(lambda **kw:self.fail('network'),'unreviewed-host')
    def test_concurrent_batches_have_independent_selected_mirrors(self):
        def acquire(preferred):
            def factory(**options):
                class Client:
                    def retrieve(self,**request):return options['source']
                return Client()
            client=MirrorClient(factory,preferred);client.retrieve(step=[144]);return client.source
        with ThreadPoolExecutor(max_workers=3) as workers:
            self.assertEqual(list(workers.map(acquire,['azure','google','ecmwf'])),['azure','google','ecmwf'])
