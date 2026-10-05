"""Archive scope, immutable recovery and actual private-clock causal proof."""
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import zipfile
from test_wp15_preprocessing import PreprocessingTests
from mardorf_collector.wp13 import regional_stations_v1 as old
from mardorf_collector.wp13.core_v1 import canonical,digest
from mardorf_collector.wp15 import packages_v1 as packages,prepared_package_v1 as prepared,regional_v2 as dwd
from mardorf_collector.wp15.package_reader_v1 import VerifiedReader
from mardorf_collector.wp15.assets import ROOT

class PackageTests(PreprocessingTests):
    def setUp(self):
        super().setUp()
        ready=dict(available=True,engine_sha256=prepared.processor_identity(),contract_sha256=hashlib.sha256((ROOT/prepared.CONTRACT).read_bytes()).hexdigest())
        self.cloud.publish({packages.READY:canonical(ready)},metadata={})
        self.p,self.c=dwd.configuration();originals=[];changes={}
        for product,spec in self.p['hourly_products'].items():
            fields=list(spec['fields']);stream=io.BytesIO()
            with zipfile.ZipFile(stream,'w') as z:
                z.writestr('produkt_'+spec['prefix']+'.txt',';'.join(['STATIONS_ID','MESS_DATUM','QN',*fields,'eor'])+'\n'+';'.join(['04745','2016010100','3',*['-999' if i==0 else '0' for i in range(len(fields))],'eor'])+'\n')
            raw=stream.getvalue();sha=hashlib.sha256(raw).hexdigest();path=old.ORIGINALS+sha+'.zip';changes[path]=raw
            originals.append(dict(station_id='04745',product=product,path=path,sha256=sha,bytes=len(raw),captured_at_utc='2026-10-05T02:00:00Z'))
        changes[old.ARCHIVE]=canonical(dict(originals=originals));self.cloud.publish(changes,metadata={})

    def bundle(self):
        report=packages.build(self.cloud,station='04745',year=2016,workers=1)
        return report,packages.read_bundle(self.backend,report['bundle'])

    def test_complete_matrix_and_empty_months_replay_no_source_or_normalization(self):
        report,doc=self.bundle()
        self.assertEqual(len(doc['months']),120);self.assertEqual(doc['field_count'],13)
        self.assertEqual(sum(e['status']=='VERIFIED_EMPTY' for e in doc['months'].values()),110)
        rows=packages.month_records(self.backend,doc['months']['wind/01']['document'])
        self.assertTrue(any(r['native']['value_native'] is None for r in rows))
        with patch.object(dwd,'prepare',side_effect=AssertionError('re-extracted')):
            again=packages.build(self.cloud,station='04745',year=2016,workers=1)
        self.assertEqual(again['bundle'],report['bundle']);self.assertEqual(again['status'],'VERIFIED_EXISTING')
        release=packages.release(self.cloud,stations=['04745'],years=[2016])
        self.assertEqual(release,packages.release(self.cloud,stations=['04745'],years=[2016]))

    def test_rehashed_incomplete_wrong_scope_original_and_clock_rejected(self):
        _,doc=self.bundle()
        mutations=[lambda d:d['months'].pop('wind/12'),lambda d:d.update(year=2017),
            lambda d:d['originals']['wind'].update(sha256='a'*64),lambda d:d.update(public_verified_at_utc='2026-10-05T00:00:00Z'),
            lambda d:d.update(field_count=14)]
        for mutate in mutations:
            bad=deepcopy(doc);mutate(bad);bad['package_id']=digest({k:v for k,v in bad.items() if k not in ('package_id','public_verified_at_utc')})
            with self.assertRaises((ValueError,KeyError)):packages.validate_bundle(bad)
        with self.assertRaisesRegex(ValueError,'incomplete'):packages.release(self.cloud,stations=['04745'],years=[2016,2017])
        with self.assertRaises(ValueError):packages.build(self.cloud,station='12345',year=2016,workers=1)
        with self.assertRaises(ValueError):packages.build(self.cloud,station='04745',year=2015,workers=1)

    def test_interrupted_private_commit_preserves_real_receipts_one_visibility(self):
        _,doc=self.bundle();release=packages.release(self.cloud,stations=['04745'],years=[2016])
        with patch.object(self.cloud,'publish',side_effect=ValueError('CAS interrupted')):
            with self.assertRaisesRegex(ValueError,'CAS interrupted'):packages.consume_release(self.cloud,release['release_id'])
        ref,ack=packages.admission_ref(self.backend,doc['package_id'],ROOT)
        with patch.object(prepared,'formatter',side_effect=AssertionError('private normalization')):
            result=packages.consume_release(self.cloud,release['release_id'])
        self.assertEqual(result['normalizations'],0);self.assertEqual(result['visibility_transactions'],1)
        self.assertEqual((ref,ack),packages.admission_ref(self.backend,doc['package_id'],ROOT))
        self.assertEqual(packages.consume_release(self.cloud,release['release_id'])['publication_status'],'unchanged')
        index=json.loads(self.cloud.read(packages.INDEX));self.assertEqual(len(index['entries']),1)
        m=doc['months']['wind/01'];child=m['document']
        entry=dict(manifest=m['manifest'],scope=child['scope'],quantities=sorted(child['quantity_counts']),context='historical',field_count=child['field_count'],bundle=index['entries'][doc['package_id']]['bundle'],admission=ref,package_id=doc['package_id'])
        reader=VerifiedReader(self.backend,expected_processor=__import__('mardorf_collector.wp15.prepared',fromlist=['processor_identity']).processor_identity())
        args=dict(profile_id=doc['months']['wind/01']['document']['scope']['profile_id'],site_id=doc['site_id'],quantity_ids=['wind_speed','wind_direction'],start_utc='2016-01-01T00:00:00Z',end_utc='2016-01-02T00:00:00Z',configuration_sha256=doc['configuration_sha256'])
        self.assertFalse(reader.read_weather({child['publication_id']:entry},view='private_operational',as_of_utc='2026-10-05T02:00:00Z',**args)['rows'])
        self.assertEqual(len(reader.read_weather({child['publication_id']:entry},view='public_reconstruction',as_of_utc='2026-10-05T02:00:00Z',**args)['rows']),2)
        got=reader.read_weather({child['publication_id']:entry},view='private_operational',as_of_utc=ack['private_received_at_utc'],**args)
        self.assertEqual(len(got['rows']),2);self.assertEqual(got['metrics']['original_reads'],0)
        bad=deepcopy(ack);bad['private_received_at_utc']='2026-10-05T02:00:00Z';bad['receipt_id']=digest({k:v for k,v in bad.items() if k!='receipt_id'})
        with self.assertRaises(ValueError):packages.validate_receipt(bad,ref,doc,entry['bundle'])

if __name__=='__main__':unittest.main()
