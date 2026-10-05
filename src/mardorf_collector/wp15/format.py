"""Explicit WP12 format successor for DWD CDC stations; science remains open."""
from copy import deepcopy
import json
from pathlib import Path

from .reference.domain_v1 import DomainRegistryV1, validate_successor
from .reference.domain_v2 import validate_domain_v2, exact_utc
from .reference.fields_v1 import WeatherFormatV1, canonical, with_id
from .reference.fields_v2 import WeatherFormatV2, FieldErrorV2
from . import regional_v2 as adapter
from .assets import reference_root, ROOT


class RegionalWeatherFormatV1(WeatherFormatV2):
    def __init__(self, root=None):
        super().__init__(reference_root())
        self.adapter = adapter
        self.station_runtime_root = Path(root or ROOT)
        self.station_policy, self.station_contracts = adapter.configuration(self.station_runtime_root)
        previous=self.registry
        self.registry=deepcopy(self.station_contracts.registry)
        validate_successor(previous,self.registry)
        validate_domain_v2(self.registry)
        self.domain=DomainRegistryV1(self.registry)
        for cadence,products in [('hourly',self.station_policy['hourly_products']),('10_minutes',self.station_policy['current_products'])]:
            for product,spec in products.items():
                for parameter,(quantity,unit) in spec['fields'].items():
                    identifier='dwd-cdc-'+cadence+'-'+product+'-'+parameter+'-v1'
                    self.weatherlink[identifier]={'id':identifier,'provider_binding_id':self.station_policy['provider_id'],
                        'native_parameter':parameter,'quantity_id':quantity,'operator':'unknown','unit':unit,
                        'endpoint_kind':'cdc','role':'semantic_open'}
        self.bindings=deepcopy(self.bindings)
        self.bindings['dwd_cdc_fields']=list(self.weatherlink.values())
        self.policy=deepcopy(self.policy)
        self.policy['quantities'].update(sea_level_pressure=['pressure'],visibility=['length'],sunshine_duration=['duration'])
        self.policy['quantity_units'].update(length={'m':{'unit_si':'m','scale':[1,1],'offset':[0,1]}},
                                           duration={'min':{'unit_si':'s','scale':[60,1],'offset':[0,1]}})

    def validate_field(self, field):
        # The V1 closed schema and all identity/sensor/unit guards remain active.
        # V2's WeatherLink-specific endpoint proof is replaced only for CDC.
        WeatherFormatV1.validate_field(self,field)
        source=field['source'];url=source['source_url']
        if source['provider_binding_id']!=self.station_policy['provider_id']:
            raise FieldErrorV2('Regional format only accepts its configured CDC provider')
        for key,value in field['time'].items():
            if key.endswith('_utc') and value is not None:exact_utc(value)
        extension=field['extensions'].get('dwd:cdc-source:v1',{})
        station=next(s for s in self.station_policy['stations'] if s['station_id']==field['domain']['station_id'])
        product=extension.get('product');hourly=extension.get('cadence')=='hourly'
        try:
            allowed=[self.adapter.source_url(self.station_policy,station['id'],product,hourly=hourly)]
            if hourly:allowed.append(self.station_policy['historical_urls'][product][station['id']])
        except KeyError as exc:
            raise FieldErrorV2('Unregistered CDC product') from exc
        if url not in allowed or not __import__('re').fullmatch(r'raw-[0-9]+',source['native_metadata_object_id'] or ''):
            raise FieldErrorV2('CDC station/product endpoint contradiction')
        if field['time']['operator']!='unknown' or field['time']['closure']!='unknown':
            raise FieldErrorV2('Unproven CDC temporal support cannot be resolved')
        return deepcopy(field)

    def normalize(self,field):
        projection=super().normalize(field)
        projection['source_identity']={'status':'confirmed','model_id':None,'product_id':field['source']['product_id_native'],
                                      'family':None,'method':'dwd_cdc_original_row_v1'}
        if projection['status']=='normalized':
            projection.update(matching_status='open',reasons=['cdc_temporal_support_unresolved','regional_science_not_integrated'])
        return with_id(projection,'projection_id')

    def validate_transfer(self,envelope,fields,raw_bytes,fragment_bytes):
        validated=super().validate_transfer(envelope,fields,raw_bytes,fragment_bytes)
        extension=envelope['extensions']
        windows=extension.get('dwd:cdc-windows:v1',{}).get('windows') or [extension.get('dwd:cdc-window:v1')]
        if any(not isinstance(w,dict) for w in windows) or set(raw_bytes)!={'raw-'+str(i) for i in range(2*len(windows))}:
            raise FieldErrorV2('CDC bound windows/originals required')
        rebuilt_results=[]
        for index,window in enumerate(windows):
            original=raw_bytes['raw-'+str(index*2)]
            document=json.loads(raw_bytes['raw-'+str(index*2+1)])
            expected,updates=self.adapter.extraction(original,window['station_id'],window['product'],
                self.station_policy,window['start_utc'],window['end_utc'],hourly=window['hourly'])
            candidates={canonical(row):row for row in expected['rows']}
            selected=document.get('rows',[])
            if (len({canonical(row) for row in selected})!=len(selected)
                    or any(canonical(row) not in candidates for row in selected)
                    or {k:v for k,v in document.items() if k!='rows'}!={k:v for k,v in expected.items() if k!='rows'}):
                raise FieldErrorV2('CDC extraction contradicts original CSV')
            selected_times={row['native']['MESS_DATUM'] for row in selected}
            known={key:value for key,value in updates.items() if key.rsplit('/',1)[-1] not in selected_times}
            rebuilt,_=self.adapter.prepare(original,envelope['capture']['retrieved_at_utc'],window['station_id'],
                window['product'],start=window['start_utc'],end=window['end_utc'],hourly=window['hourly'],historical=window['historical'],
                contracts=self.station_contracts,policy=self.station_policy,commit=envelope['capture']['collector_commit_sha'],known=known)
            if rebuilt is None:
                raise FieldErrorV2('Empty CDC extraction cannot be in a delivery')
            rebuilt_results.append(rebuilt)
        rebuilt=self.adapter.combine(rebuilt_results)
        if canonical(rebuilt['fields'])!=canonical(fields) or canonical(rebuilt['envelope'])!=canonical(envelope):
            raise FieldErrorV2('CDC complete transfer extraction mismatch')
        return validated
