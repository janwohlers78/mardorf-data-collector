import gzip
import json
from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout
from io import StringIO

from mardorf_collector.providers import svg_historical_research_v1 as research
from mardorf_collector.storage.objects import LocalObjects


class Session:
    def __init__(self,payload,status=200):
        self.payload=payload;self.status=status;self.calls=[]
    def get(self,url,**kwargs):
        self.calls.append((url,kwargs))
        return type('Response',(),dict(status_code=self.status,content=json.dumps(self.payload).encode()))()


class HistoricalSourceTests(unittest.TestCase):
    def payload(self):
        return dict(station_id=42374,sensors=[dict(sensor_type=48,data_structure_type=4,data=[
            dict(ts=1672531200,arch_int=300,wind_speed_avg=2.,wind_speed_hi=4.,
                 wind_dir_of_prevail=4,wind_dir_of_hi=5,wind_samples=117)])])

    def test_older_days_allowed_and_ranges_bounded(self):
        self.assertEqual(research.days(date(2023,1,1),date(2023,1,3)),[date(2023,1,1),date(2023,1,2)])
        with self.assertRaises(ValueError):research.days(date(2023,1,1),date(2024,1,1))
        with self.assertRaises(ValueError):research.days(date(2023,1,1),date(2023,1,2),','.join(f'2023-01-{i:02}' for i in range(1,14)))

    def test_exact_raw_retained_and_authentication_not_in_receipt(self):
        session=Session(self.payload());record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=session)
        self.assertEqual(record['status'],'captured');self.assertTrue(record['five_minute_operator_verified'])
        self.assertEqual(json.loads(gzip.decompress(raw)),self.payload())
        self.assertEqual(json.loads(norm)['observations'][0]['time_utc'],'2023-01-01T00:00:00+00:00')
        self.assertNotIn('dummy-key',json.dumps(record));self.assertNotIn('dummy-secret',json.dumps(record))
        self.assertEqual(session.calls[0][1]['params']['end-timestamp']-session.calls[0][1]['params']['start-timestamp'],86399)

    def test_empty_is_distinct_from_authenticated_failure(self):
        payload=self.payload();payload['sensors'][0]['data']=[]
        record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session(payload))
        self.assertEqual(record['status'],'empty');self.assertFalse(record['five_minute_operator_verified'])
        self.assertIsNotNone(raw);self.assertIsNotNone(norm)
        error,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session({'error':'forbidden'},403))
        self.assertEqual(error['status'],'provider_error');self.assertEqual(error['http_status'],403);self.assertIsNone(norm)

    def test_window_probe_is_explicit_bounded_and_preserves_provider_rejection(self):
        session=Session({'error':'window exceeds maximum'},400)
        r,raw,norm=research.capture(date(2025,1,15),'dummy-key','dummy-secret',session=session,window_days=7)
        self.assertEqual(r['http_status'],400);self.assertEqual(r['status'],'provider_error')
        self.assertEqual(r['request']['end_timestamp']-r['request']['start_timestamp'],7*86400-1)
        self.assertIsNotNone(raw);self.assertIsNone(norm)
        with self.assertRaises(ValueError):research.capture(date(2025,1,15),'dummy-key','dummy-secret',session=session,window_days=365)

    def test_station_and_nonfive_minute_operators_are_not_silently_admitted(self):
        payload=self.payload();payload['station_id']=1
        record,_,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session(payload))
        self.assertEqual(record['status'],'station_identity_mismatch');self.assertIsNone(norm)
        payload=self.payload();payload['sensors'][0]['data'][0]['arch_int']=900
        record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session(payload))
        self.assertEqual(record['status'],'captured');self.assertFalse(record['five_minute_operator_verified'])
        self.assertIsNotNone(raw);self.assertFalse(json.loads(norm)['operator_verified'])

    def test_sensitive_response_and_exception_text_never_persisted(self):
        record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session({'reason':'dummy-key'}))
        self.assertEqual(record['status'],'quarantined_sensitive_response');self.assertIsNone(raw);self.assertIsNone(norm)
        class Failure:
            def get(self,*args,**kwargs):raise RuntimeError('URL dummy-key dummy-secret')
        record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Failure())
        self.assertNotIn('dummy-key',json.dumps(record));self.assertEqual(record['status'],'transport_error')

    def test_publication_requires_exact_immutable_readback(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);folder=root/'work';folder.mkdir();backend=LocalObjects(root/'objects')
            raw=b'original exact historical bytes';reference=research.publish(backend,'research/svg',folder,'source.gz',raw)
            self.assertEqual(reference['sha256'],research.sha(raw));self.assertIn(research.sha(raw),reference['key'])

    def test_checkpoint_preserves_every_original_byte_and_is_bounded(self):
        import zipfile
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);backend=LocalObjects(root/'objects');folder=root/'work';folder.mkdir()
            files=[('2016-10-08/source.json.gz',gzip.compress(b'original exact source',mtime=0)),
                   ('2016-10-08/normalized.json',b'{"normalized":true}\n')]
            reference,members=research.publish_pack(backend,'research/svg',folder,files)
            with zipfile.ZipFile(backend.root/reference['key']) as archive:
                for name,raw in files:
                    self.assertEqual(archive.read(name),raw);self.assertEqual(members[name]['sha256'],research.sha(raw))
            with self.assertRaises(ValueError):research.publish_pack(backend,'research/svg',folder,files*15)

    def test_complete_research_cli_needs_no_github_control_credentials(self):
        record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session(self.payload()))
        with tempfile.TemporaryDirectory() as root:
            root=Path(root);backend=LocalObjects(root/'objects');output=root/'result'
            with patch.dict(research.os.environ,{'WEATHERLINK_API_KEY':'dummy-key','WEATHERLINK_API_SECRET':'dummy-secret'},clear=True), \
                 patch.object(research,'capture',return_value=(record,raw,norm)), \
                 patch.object(research,'b2_settings',return_value={}), \
                 patch.object(research,'resolve_b2_bucket',return_value={}), \
                 patch.object(research,'B2Objects',return_value=backend),redirect_stdout(StringIO()):
                self.assertEqual(research.main(['--start','2023-01-01','--end-exclusive','2023-01-02','--output',str(output)]),0)
            result=json.loads((output/'result.json').read_bytes())
            self.assertFalse(result['production_head_updated']);self.assertEqual(result['git_weather_bytes_written'],0)
            self.assertEqual(result['status_counts'],{'captured':1})

    def test_additive_workflow_rejects_schedule_even_with_restamped_hash(self):
        import importlib.util
        root=research.ROOT
        spec=importlib.util.spec_from_file_location('research_layout',root/'tools/validate_prep09_layout.py')
        gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
        proof=json.loads((root/'config/architecture_refactoring_v1.json').read_bytes())
        relative='.github/workflows/svg-historical-research-v1.yml'
        with tempfile.TemporaryDirectory() as temporary:
            target=Path(temporary)
            for name in ['config/architecture_refactoring_v1.json','tools/validate_prep09_layout.py',
                         proof['historical_validator']['path'],proof['research_validator_predecessor']['path'],
                         proof['window_probe_validator_predecessor']['path'],relative]:
                path=target/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes((root/name).read_bytes())
            path=target/relative
            path.write_text(path.read_text().replace('on:\n','on:\n  schedule:\n    - cron: "0 * * * *"\n',1))
            proof['added_workflows'][0]['sha256']=research.sha(path.read_bytes())
            (target/'config/architecture_refactoring_v1.json').write_text(json.dumps(proof))
            with self.assertRaisesRegex(ValueError,'Bounded manual research workflow drift'):
                gate.validate(target)

    def test_multiday_checkpoint_cli_keeps_source_gaps_and_exact_index_lineage(self):
        record,raw,norm=research.capture(date(2023,1,1),'dummy-key','dummy-secret',session=Session({'error':'not available'},403))
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);backend=LocalObjects(root/'objects');output=root/'output'
            def outcome(day,*args):return dict(record,date_utc=day.isoformat()),raw,norm
            with patch.dict(research.os.environ,{'WEATHERLINK_API_KEY':'dummy-key','WEATHERLINK_API_SECRET':'dummy-secret'},clear=True), \
                 patch.object(research,'capture',side_effect=outcome), \
                 patch.object(research.time,'sleep') as pacing, \
                 patch.object(research,'b2_settings',return_value={}), \
                 patch.object(research,'resolve_b2_bucket',return_value={}), \
                 patch.object(research,'B2Objects',return_value=backend),redirect_stdout(StringIO()):
                self.assertEqual(research.main(['--start','2016-10-08','--end-exclusive','2016-10-22','--output',str(output)]),0)
                self.assertEqual(pacing.call_count,14)
                pacing.assert_called_with(4)
            result=json.loads((output/'result.json').read_bytes());index=json.loads((output/'index.json').read_bytes())
            self.assertEqual(result['status_counts'],{'provider_error':14})
            self.assertEqual(result['source_outcome'],'COMPLETE_REQUESTS_WITH_SOURCE_GAPS')
            self.assertTrue(index['complete']);self.assertFalse(index['scientific_release'])
            self.assertTrue(all('raw_archive' in r and 'container' in r['raw_archive'] for r in index['records']))


if __name__=='__main__':unittest.main()
