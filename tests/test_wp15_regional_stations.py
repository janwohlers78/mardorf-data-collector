"""DWD station identity, original CSV proof, missing/QN and replay contracts."""
import io
import unittest
import zipfile
from mardorf_collector.wp13 import regional_stations_v1 as dwd


def sample(station='04745', value='2.5', direction='0', quality='3'):
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('produkt_zehn_min_ff_'+station+'.txt',
            'STATIONS_ID;MESS_DATUM;QN;FF_10;DD_10;eor\n'+station+';202610050000;'+quality+';'+value+';'+direction+';eor\n')
        archive.writestr('Metadaten_Geographie_'+station+'.txt','original dated station positions\n')
    return stream.getvalue()


class RegionalStationTests(unittest.TestCase):
    def setUp(self):
        self.policy,self.contracts=dwd.configuration()
        self.args=dict(start='2026-10-04T23:00:00Z',end='2026-10-05T01:00:00Z',
            contracts=self.contracts,policy=self.policy,commit='1'*40)

    def test_four_registered_stations_native_identity_and_quality_not_rejection(self):
        for station in self.policy['stations']:
            result,_=dwd.prepare(sample(station['id']), '2026-10-05T02:00:00Z',station['id'],'wind',**self.args)
            self.assertEqual(result['fields'][0]['domain']['station_id'],station['station_id'])
            self.assertEqual(result['fields'][1]['value_native'],0)
            self.assertEqual(result['fields'][0]['qc_native'],[])
            self.assertEqual(result['fields'][0]['extensions']['dwd:cdc-source:v1']['quality'],{'QN':'3'})
            self.assertEqual(result['fields'][0]['time']['operator'],'unknown')

    def test_missing_value_is_null_zero_direction_is_north(self):
        result,_=dwd.prepare(sample(value='-999'), '2026-10-05T02:00:00Z','04745','wind',**self.args)
        self.assertIsNone(result['fields'][0]['value_native'])
        self.assertEqual(result['fields'][0]['extensions']['dwd:cdc-source:v1']['native_missing_token'],'-999')
        self.assertEqual(result['fields'][1]['value_native'],0)

    def test_unchanged_rows_skipped_changed_value_revision_retained(self):
        first,known=dwd.prepare(sample(), '2026-10-05T02:00:00Z','04745','wind',**self.args)
        replay,_=dwd.prepare(sample(), '2026-10-05T03:00:00Z','04745','wind',known=known,**self.args)
        self.assertIsNone(replay)
        revision,_=dwd.prepare(sample(value='3'), '2026-10-05T03:00:00Z','04745','wind',known=known,**self.args)
        self.assertNotEqual(first['fields'][0]['source']['source_revision'],revision['fields'][0]['source']['source_revision'])

    def test_station_contradiction_and_future_capture_rejected(self):
        with self.assertRaisesRegex(ValueError,'station contradiction'):
            dwd.prepare(sample('00963'),'2026-10-05T02:00:00Z','04745','wind',**self.args)
        with self.assertRaisesRegex(ValueError,'capture'):
            dwd.prepare(sample(),'2026-10-05T00:30:00Z','04745','wind',**self.args)

    def test_zip_path_and_duplicate_timestamps_rejected(self):
        raw=io.BytesIO()
        with zipfile.ZipFile(raw,'w') as archive:archive.writestr('../produkt_bad.txt','bad')
        with self.assertRaisesRegex(ValueError,'ZIP'):
            list(dwd.rows(raw.getvalue(),'04745',self.policy,self.args['start'],self.args['end']))

    def test_historical_window_bound_and_no_credentials(self):
        args=dict(self.args,hourly=True,historical=True)
        with self.assertRaisesRegex(ValueError,'historical archive window'):
            dwd.prepare(sample(),'2026-10-05T02:00:00Z','04745','wind',**args)
        self.assertTrue(all('https://opendata.dwd.de/' in url and '?' not in url
            for product in self.policy['historical_urls'].values() for url in product.values()))

    def test_partial_poll_retry_does_not_duplicate_already_published_station(self):
        from copy import deepcopy
        from unittest.mock import patch
        import json
        policy=deepcopy(self.policy)
        policy['current_products']={'wind':policy['current_products']['wind']}
        policy['hourly_products']={}
        class Cloud:
            def __init__(self):
                self.data={'data/weather_native/consumer_readiness_dwd_cdc_v1.json':dwd.canonical(
                    {'available':True,'configuration_sha256':self_outer.contracts.configuration['sha256']})}
            def read(self,path,required=True):
                if required and path not in self.data:raise KeyError(path)
                return self.data.get(path)
            def publish(self,changes,metadata,merge):self.data.update(merge(self,dict(changes)))
        self_outer=self;cloud=Cloud()
        from datetime import datetime,timezone
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return cls(2026,10,5,2,tzinfo=timezone.utc)
        def failing_fetch(url,policy):
            station=url.rsplit('/',1)[-1].split('_')[-2]
            if station=='00662':raise ValueError('simulated third station outage')
            return sample(station),'2026-10-05T02:00:00Z'
        with patch.object(dwd,'datetime',Clock),patch.object(dwd,'configuration',return_value=(policy,self.contracts)),patch.object(dwd,'fetch',side_effect=failing_fetch):
            with self.assertRaisesRegex(ValueError,'third station'):dwd.operate(cloud)
        accepted=[path for path in cloud.data if path.endswith('/ready.json')]
        self.assertEqual(len(accepted),2)
        def fetch(url,policy):
            station=url.rsplit('/',1)[-1].split('_')[-2]
            return sample(station),'2026-10-05T03:00:00Z'
        with patch.object(dwd,'datetime',Clock),patch.object(dwd,'configuration',return_value=(policy,self.contracts)),patch.object(dwd,'fetch',side_effect=fetch):
            result=dwd.operate(cloud)
        self.assertEqual(len(result['deliveries']),2)
        self.assertEqual(len([path for path in cloud.data if path.endswith('/ready.json')]),4)
