import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock
from mardorf_collector.runtime.wp09_eumetsat_lfl_v2 import capture
from test_wp09_lightning_context_v1 import Response


class EumetsatCaptureTests(unittest.TestCase):
    def test_no_secret_means_explicit_block_no_network(self):
        session=Mock();r,o=capture('2026-10-09T00:00:00Z','2026-10-10T00:00:00Z',environ={},session=session)
        self.assertEqual(r['blocker'],'EUMETSAT_credentials_not_configured');self.assertEqual(o,{})
        session.post.assert_not_called();session.get.assert_not_called();self.assertFalse(r['detector_coverage'])
    def test_complete_native_download_kept_without_invented_coverage(self):
        product=dict(id='native_product',properties=dict(parentIdentifier='EO:EUM:DAT:0691',
            productInformation={'productType':'MTILI2LFL'},acquisitionInformation=[{'platform':{'platformShortName':'MTI1'}}],
            date='2026-10-09T01:00:00Z/2026-10-09T01:10:00Z',
            links={'data':[{'mediaType':'application/zip','href':'https://api.eumetsat.int/native-product'}]}))
        token='TEST_TOKEN_DO_NOT_PERSIST';session=Mock()
        session.post.return_value=SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'access_token':token})
        session.get.side_effect=[Response(json.dumps({'features':[product],'totalResults':1}).encode()),Response(b'PKnative_body_trailer_manifest')]
        r,o=capture('2026-10-09T00:00:00Z','2026-10-10T00:00:00Z',environ={'EUMETSAT_CONSUMER_KEY':'TEST_KEY','EUMETSAT_CONSUMER_SECRET':'TEST_SECRET'},session=session)
        self.assertEqual(r['captured_product_count'],1);self.assertFalse(r['detector_coverage'])
        self.assertIn(b'PKnative_body_trailer_manifest',o.values());self.assertNotIn(token,json.dumps(r))
        self.assertNotIn('TEST_SECRET',json.dumps(r));self.assertNotIn('TEST_KEY',json.dumps(r))
        self.assertEqual(session.get.call_args.kwargs['headers'],{'Authorization':'Bearer '+token})
        self.assertFalse(session.get.call_args.kwargs['allow_redirects'])
    def test_identity_failure_and_authenticated_error_are_not_zero(self):
        session=Mock();session.post.return_value=SimpleNamespace(raise_for_status=lambda:None,json=lambda:{'access_token':'TOKEN'})
        session.get.return_value=Response(json.dumps({'features':[{'properties':{'date':'2026-10-09T01:00:00Z/2026-10-09T01:10:00Z','parentIdentifier':'FOREIGN'}}]}).encode())
        r,_=capture('2026-10-09T00:00:00Z','2026-10-10T00:00:00Z',environ={'EUMETSAT_CONSUMER_KEY':'TEST','EUMETSAT_CONSUMER_SECRET':'TEST'},session=session)
        self.assertEqual(r['status'],'BLOCKED');self.assertFalse(r['detector_coverage'])
    def test_future_and_unbounded_intervals_rejected(self):
        for start,end in [('2026-10-09','2026-10-10'),('2026-10-01T00:00:00Z','2026-10-10T00:00:00Z'),('2100-01-01T00:00:00Z','2100-01-02T00:00:00Z')]:
            with self.assertRaises(ValueError):capture(start,end,environ={})


if __name__=='__main__':unittest.main()
