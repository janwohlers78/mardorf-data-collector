"""Source parity, bounded queue, causal admission and immutable retry proof."""
from copy import deepcopy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from mardorf_collector.storage.objects import LocalObjects
from mardorf_collector.storage.archive import PackedArchive
from mardorf_collector.storage.runtime import CloudRuntime
from mardorf_collector.wp13.core_v1 import canonical, digest, identify
from mardorf_collector.wp13 import native_svg_v1 as svg
from mardorf_collector.wp13.store_v1 import read_delivery
from mardorf_collector.wp15.assets import ROOT
from mardorf_collector.wp15 import prepared, queue, jobs, regional_v2 as dwd
from mardorf_collector.wp15.reader import VerifiedReader


class Head:
    def __init__(self, ref):self.ref=ref
    def read(self):return 'a'*40,{'snapshot':self.ref}
    def advance(self,parent,snapshot,**kwargs):self.ref=snapshot;return True


def source(station='04745',value='2.5',rows=None):
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as z:
        z.writestr(zipfile.ZipInfo('produkt_wind.txt',date_time=(2020,1,1,0,0,0)),'STATIONS_ID;MESS_DATUM;QN;FF_10;DD_10;eor\n'+
            (rows or f'{station};202610050000;3;{value};0;eor\n'))
    return stream.getvalue()


def result(value='2.5'):
    p,c=dwd.configuration()
    return dwd.prepare(source(value=value),'2026-10-05T02:00:00Z','04745','wind',
        start='2026-10-04T23:00:00Z',end='2026-10-05T01:00:00Z',policy=p,contracts=c,commit='a'*40)[0]


class PreprocessingTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.backend=LocalObjects(Path(self.temp.name)/'objects')
        ref=PackedArchive(self.backend,prefix='weather/archive').export(iter([]),metadata={})
        self.cloud=CloudRuntime({'schema_version':1,'artifact_version':'dev03-cloud-runtime-v1',
            'production_enabled':False,'archive_prefix':'weather/archive','private_repository':'fixture/repo',
            'control_branch':'main','control_path':'config/cloud_refs/test.json','bootstrap_snapshot':ref.json()},
            {},backend=self.backend,head=Head(ref.json()))

    def publish(self,value='2.5'):
        publication=prepared.preprocess(self.backend,result(value))
        queue.enqueue(self.cloud,publication)
        return publication

    def entries(self,doc):
        out={}
        for path in queue.catalog_paths(doc):out.update(json.loads(self.cloud.read(path))['entries'])
        return out

    def test_native_missing_zero_and_quality_survive_si_and_parquet(self):
        publication=self.publish('-999');doc=prepared.read_manifest(self.backend,publication['manifest'])
        rows=prepared.read_records(self.backend,doc)
        self.assertIsNone(rows[0]['native']['value_native']);self.assertIsNone(rows[0]['canonical']['value_canonical'])
        self.assertEqual(rows[1]['native']['value_native'],0)
        self.assertEqual(rows[0]['native']['extensions']['dwd:cdc-source:v1']['quality'],{'QN':'3'})
        self.assertEqual(rows[1]['canonical']['matching_status'],'open')

    def test_replay_does_not_normalize_or_add_immutable_bytes(self):
        r=result();first=prepared.preprocess(self.backend,r);before=self.backend.metrics['put_bytes']
        with patch('mardorf_collector.wp15.prepared.formatter',side_effect=AssertionError('re-normalized')):
            again=prepared.preprocess(self.backend,r)
        self.assertEqual(first['manifest'],again['manifest']);self.assertEqual(again['normalizations'],0)
        self.assertEqual(before,self.backend.metrics['put_bytes'])

    def test_rehashed_native_cannot_forge_csv_source_value(self):
        r=result();r['fields'][0]['value_native']=88;r['fields'][0]=identify(r['fields'][0],'field_id')
        name=next(iter(r['fragment_bytes']));body=b''.join(canonical(f)+b'\n' for f in r['fields']);r['fragment_bytes'][name]=body
        import hashlib
        r['envelope']['field_fragments'][0]['sha256']=hashlib.sha256(body).hexdigest();r['envelope']=identify(r['envelope'],'envelope_id')
        with self.assertRaisesRegex(ValueError,'raw pointer|extraction'):prepared.preprocess(self.backend,r)

    def test_unit_contradiction_rejected(self):
        r=result();r['fields'][0]['unit_native']='mph';r['fields'][0]=identify(r['fields'][0],'field_id')
        with self.assertRaises(ValueError):prepared.preprocess(self.backend,r)

    def test_partial_cold_readback_never_publishes_ready(self):
        with patch.object(self.backend,'get_bytes',side_effect=ValueError('cold failure')):
            with self.assertRaisesRegex(ValueError,'cold failure'):prepared.preprocess(self.backend,result())
        self.assertEqual(self.backend.list_page(prepared.PREFIX+'/manifests/')['keys'],[])

    def test_private_admission_only_reads_manifest_and_preserves_first_clock(self):
        publication=self.publish();calls=[];get=self.backend.get_bytes
        def tracked(ref):calls.append(ref.key);return get(ref)
        with patch.object(self.backend,'get_bytes',side_effect=tracked):
            receipt=queue.admit(self.cloud,publication['manifest'],expected_processor=prepared.processor_identity())
        self.assertFalse(any('/blobs/' in key for key in calls))
        again=queue.admit(self.cloud,publication['manifest'],expected_processor=prepared.processor_identity())
        self.assertEqual(receipt,again)
        queue.enqueue(self.cloud,publication)
        self.assertEqual(queue.page(self.cloud.read(queue.pending_path(publication['publication_id'])))['entries'],{})

    def test_reader_is_causal_and_never_normalizes(self):
        publication=self.publish();doc=prepared.read_manifest(self.backend,publication['manifest'])
        receipt=queue.admit(self.cloud,publication['manifest'],expected_processor=prepared.processor_identity())
        reader=VerifiedReader(self.backend,expected_processor=prepared.processor_identity())
        args=dict(profile_id=doc['scope']['profile_id'],site_id=doc['scope']['site_id'],
            quantity_ids=['wind_speed','wind_direction'],configuration_sha256=doc['configuration_sha256'],
            start_utc='2026-10-05T00:00:00Z',end_utc='2026-10-05T01:00:00Z',view='private_operational')
        before=reader.read_weather(self.entries(doc),as_of_utc='2026-10-05T02:00:00Z',**args)
        self.assertEqual(before['rows'],[])
        with patch('mardorf_collector.wp15.reference.fields_v1.WeatherFormatV1.normalize',side_effect=AssertionError('normalize called')):
            actual=reader.read_weather(self.entries(doc),as_of_utc=receipt['private_received_at_utc'],**args)
        self.assertEqual(len(actual['rows']),2);self.assertEqual(actual['metrics']['normalizations'],0)
        self.assertEqual([x['record']['native'] for x in actual['rows']],sorted(result()['fields'],key=lambda f:__import__('mardorf_collector.wp15.reader',fromlist=['group_key']).group_key(f)))

    def test_bounded_pending_pages_no_reads_of_accepted_ready_files(self):
        publication=self.publish();queue.admit(self.cloud,publication['manifest'],expected_processor=prepared.processor_identity())
        with patch.object(self.backend,'get_bytes',wraps=self.backend.get_bytes) as reads:
            report=queue.consume(self.cloud,expected_processor=prepared.processor_identity())
        self.assertEqual(report['pending_pages_read'],16);self.assertEqual(report['cases'],[])
        self.assertFalse(any('/manifests/' in call.args[0].key for call in reads.call_args_list))

    def test_month_window_successor_preserves_day_fields_and_reads_original_once(self):
        p,c=dwd.configuration();raw=io.BytesIO()
        with zipfile.ZipFile(raw,'w') as z:z.writestr('produkt_ff.txt','STATIONS_ID;MESS_DATUM;QN_3;F;D;eor\n04745;2016010100;3;2.5;0;eor\n04745;2016010200;3;3;90;eor\n')
        raw=raw.getvalue()
        from mardorf_collector.wp13 import regional_stations_v1 as old
        dwd._window=None
        with patch.object(old,'rows',wraps=old.rows) as parsed:
            dwd.cache_window(raw,'04745',p,'2016-01-01T00:00:00Z','2017-01-01T00:00:00Z')
            r,_=dwd.prepare(raw,'2026-10-05T02:00:00Z','04745','wind',start='2016-01-01T00:00:00Z',end='2016-02-01T00:00:00Z',hourly=True,historical=True,policy=p,contracts=c,commit='a'*40)
            publication=prepared.preprocess(self.backend,r)
        self.assertEqual(parsed.call_count,1)
        self.assertEqual(len(prepared.read_manifest(self.backend,publication['manifest'])['partitions']),1)

    def test_svg_current_and_history_use_identical_shared_normalizer(self):
        for kind in ('current','historic'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as folder:
                body=(ROOT/f'tests/fixtures/wp13/svg_{kind}_v1.json').read_bytes()
                payload=json.loads(body);window=None
                if kind=='historic':
                    times=[row['ts'] for sensor in payload['sensors'] for row in sensor['data']]
                    window={'start_utc':datetime.fromtimestamp(min(times),timezone.utc).isoformat().replace('+00:00','Z'),
                        'end_utc':datetime.fromtimestamp(max(times)+300,timezone.utc).isoformat().replace('+00:00','Z')}
                import hashlib
                identity=hashlib.sha256(body).hexdigest();Path(folder,identity+'.bin').write_bytes(body)
                capture={'url':f'https://api.weatherlink.com/v2/{kind}/42374','body_sha256':identity,'bytes':len(body),'observed_at_utc':'2026-10-05T02:00:00Z','window':window}
                item=svg.prepare([capture],directory=folder,commit='b'*40)[0]
                r=read_delivery(Path(folder)/'prepared',item['receipt_id']);pub=prepared.preprocess(self.backend,r)
                doc=prepared.read_manifest(self.backend,pub['manifest']);records=prepared.read_records(self.backend,doc)
                fmt=prepared.formatter(doc['configuration_sha256'])
                self.assertEqual([x['canonical'] for x in records],[fmt.normalize(f) for f in r['fields']])

    def test_orphan_recovery_and_admission_cas_retry_preserve_first_arrival(self):
        pub=prepared.preprocess(self.backend,result())
        ready={'artifact_version':'wp15-public-station-consumer-ready-v1','available':True,
            'processor_sha256':prepared.processor_identity(),'contract_sha256':__import__('hashlib').sha256((ROOT/prepared.CONTRACT).read_bytes()).hexdigest(),
            'dwd_station_ids':['04745'],'svg_enabled':False}
        self.cloud.publish({queue.READINESS:canonical(ready)},metadata={})
        self.assertEqual(jobs.reconcile(self.cloud)['recovered'],1)
        original=self.cloud.publish
        with patch.object(self.cloud,'publish',side_effect=ValueError('CAS interruption')):
            with self.assertRaisesRegex(ValueError,'CAS interruption'):
                queue.admit(self.cloud,pub['manifest'],expected_processor=prepared.processor_identity())
        meta=self.backend.head(queue.ADMISSION+pub['publication_id'])
        from mardorf_collector.storage.objects import ObjectRef
        first=json.loads(self.backend.get_bytes(ObjectRef(queue.ADMISSION+pub['publication_id'],meta['sha256'],meta['bytes'])))
        again=queue.admit(self.cloud,pub['manifest'],expected_processor=prepared.processor_identity())
        self.assertEqual(first,again)
        self.assertEqual(jobs.reconcile(self.cloud)['recovered'],0)

    def test_year_shard_month_checkpoint_resume_and_wrong_version_rejection(self):
        import hashlib
        from mardorf_collector.wp13 import regional_stations_v1 as old
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as z:
            z.writestr('produkt_ff.txt','STATIONS_ID;MESS_DATUM;QN_3;F;D;eor\n'+''.join(f'04745;2016{m:02d}0100;3;2.5;90;eor\n' for m in range(1,13)))
        raw=stream.getvalue();sha=hashlib.sha256(raw).hexdigest();path='data/weather_native/dwd_cdc_v1/originals/'+sha+'.zip'
        self.cloud.publish({path:raw,old.ARCHIVE:canonical({'originals':[{'station_id':'04745','product':'wind','path':path,'sha256':sha,'bytes':len(raw),'captured_at_utc':'2026-10-05T02:00:00Z'}]})},metadata={})
        with patch.object(jobs,'route_enabled',return_value=True),patch.object(old,'rows',wraps=old.rows) as parsed:
            first=jobs.backfill(self.cloud,stations=['04745'],products=['wind'],year=2016)
            self.assertEqual(first['original_reads'],1);self.assertEqual(parsed.call_count,1)
            self.assertEqual(len(first['publications']),12)
            second=jobs.backfill(self.cloud,stations=['04745'],products=['wind'],year=2016)
            self.assertEqual(second['original_reads'],0);self.assertEqual(second['publications'],[])
            cursor='data/weather_native/station_preprocessed_v1/backfill/04745/wind/2016/01.json'
            wrong=json.loads(self.cloud.read(cursor));wrong['processor_sha256']='0'*64
            self.cloud.publish({cursor:canonical(wrong)},metadata={})
            with self.assertRaisesRegex(ValueError,'checkpoint processor/source'):
                jobs.backfill(self.cloud,stations=['04745'],products=['wind'],year=2016)

    def test_readiness_exact_versions_and_staged_sources(self):
        self.assertFalse(jobs.route_enabled(self.cloud,'svg'))
        ready={'artifact_version':'wp15-public-station-consumer-ready-v1','available':True,'processor_sha256':prepared.processor_identity(),
            'contract_sha256':__import__('hashlib').sha256((ROOT/prepared.CONTRACT).read_bytes()).hexdigest(),
            'dwd_station_ids':['04745'],'svg_enabled':False}
        self.cloud.publish({queue.READINESS:canonical(ready)},metadata={})
        self.assertTrue(jobs.route_enabled(self.cloud,'dwd_cdc',station='04745'));self.assertFalse(jobs.route_enabled(self.cloud,'dwd_cdc',station='00963'))
        self.assertFalse(jobs.route_enabled(self.cloud,'svg'))


if __name__=='__main__':unittest.main()
