import contextlib
import io
import unittest
import threading
from mardorf_collector.storage.objects import ObjectRef
from mardorf_collector.wp13.native_cold_reads_v1 import publish_parallel

class NativeFailFastTests(unittest.TestCase):
    def test_first_error_does_not_drain_thousands_of_queued_objects(self):
        release=threading.Event();started=[]
        refs=[ObjectRef('weather/test/'+str(i),'a'*64,1) for i in range(3663)]
        def upload(ref):
            started.append(ref)
            if ref==refs[0]:raise ValueError('checked fixture failure')
            release.wait(2)
        output=io.StringIO()
        try:
            with contextlib.redirect_stdout(output),self.assertRaisesRegex(ValueError,'checked fixture failure'):
                publish_parallel(refs,upload,workers=4)
            self.assertLessEqual(len(started),4)
            self.assertIn('WP15_UPLOAD_FAILED=',output.getvalue())
            self.assertNotIn('UPLOAD_COMPLETE',output.getvalue())
        finally:release.set()

    def test_success_publishes_every_reference_exactly_once(self):
        refs=[ObjectRef('weather/test/'+str(i),'a'*64,1) for i in range(100)]
        got=[];progress=[]
        self.assertEqual(publish_parallel(refs,got.append,workers=4,progress=progress.append),100)
        self.assertCountEqual(got,refs)
        self.assertEqual(progress,list(range(1,101)))
