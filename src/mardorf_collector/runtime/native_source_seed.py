"""Acquisition-only continuation, independent of frozen operational admission.

One immutable, readback-verified seed retains the original rows and field/source
scope. Individual canonical catalog proofs are carried by reference. This does
not relax scientific gates or publish an operational latest_success.
"""
import json
from mardorf_collector.storage.objects import ObjectRef
from mardorf_collector.storage.archive import canonical

PATH='data/inbox/native_models_wp15/acquisition_seed/latest.json'
VERSION='native-acquisition-seed-v1'
CONTROL='config/cloud_refs/collector_models_acquisition_v1.json'


def control(marker, snapshot, *, event_id, repository):
    return dict(schema_version=1,artifact_version='native-acquisition-control-v1',kind='models',
        generated_at_utc=marker['source_generated_at_utc'],snapshot=snapshot,
        bundle_ready=marker['all_native_sources_ready'],readback_verified=True,
        acquisition_only=True,operational_promotion=False,
        metadata=dict(event_id=str(event_id),producer_repository=repository))


def load(runtime):
    raw=runtime.read(PATH,required=False)
    if raw is None:return None
    marker=json.loads(raw)
    if (marker.get('artifact_version')!=VERSION or marker.get('canonical_readback_verified')is not True
        or marker.get('acquisition_only')is not True or marker.get('operational_promotion')is not False):
        raise ValueError('Native acquisition continuation proof required')
    payload_raw=runtime.backend.get_bytes(ObjectRef.parse(marker['payload']))
    payload=json.loads(payload_raw)
    health=json.loads(runtime.backend.get_bytes(ObjectRef.parse(marker['integrity'])))
    if (health.get('input_payload_sha256')!=ObjectRef.parse(marker['payload']).sha256
        or health.get('input_payload_bytes')!=len(payload_raw)):
        raise ValueError('Native acquisition payload/integrity contradiction')
    scopes=marker.get('source_catalogs',{})
    for model,scope in scopes.items():
        source=health.get('sources',{}).get(model,{})
        ref=ObjectRef.parse(scope['catalog'])
        catalog=json.loads(runtime.backend.get_bytes(ref))
        proof=scope.get('cold_readback',{})
        for extra in scope.get('additional_catalogs',[]):
            older=json.loads(runtime.backend.get_bytes(ObjectRef.parse(extra['catalog'])))
            checked=extra.get('cold_readback',{})
            if (checked.get('status')!='PASS' or checked.get('originals_fully_read')is not True
                or checked.get('parquet_exact_native_match')is not True
                or not any(x.get('metadata',{}).get('acquisition_model')==model for x in older.get('parents',[]))):
                raise ValueError('Native source additional catalog contradiction')
        if (source.get('provider_cycle_complete')is not True
            or scope.get('run_time_utc')!=source.get('selected_run_time_utc')
            or proof.get('status')!='PASS' or proof.get('originals_fully_read')is not True
            or proof.get('parquet_exact_native_match')is not True
            or not any(x.get('metadata',{}).get('acquisition_model')==model for x in catalog.get('parents',[]))):
            raise ValueError('Native source catalog continuation contradiction')
    eps=scopes.get('ICON-D2-EPS',{})
    if eps.get('native_point_source'):
        ref=ObjectRef.parse(eps['native_point_source'])
        if runtime.backend.get_bytes(ref)!=canonical(payload.get('native_eps_source')):
            raise ValueError('Native EPS point source continuation differs')
    health['_native_acquisition_cycles']=scopes
    return health,payload,marker['payload']['key'],marker['payload']['sha256']


def create(local, snapshot, integrity, catalog_ref, catalog, proof, *, prior=None, native_point_source=None):
    payload_raw=canonical(snapshot)
    # The captured original snapshot hash uses its actual wire representation;
    # canonical serialization is explicitly a new acquisition seed derivative.
    health=dict(integrity,input_payload_sha256=__import__('hashlib').sha256(payload_raw).hexdigest(),input_payload_bytes=len(payload_raw))
    payload=local.put_bytes('weather/model-native/wp15/v1/seeds/'+health['input_payload_sha256'],payload_raw)
    raw=canonical(health);integrity_ref=local.put_bytes('weather/model-native/wp15/v1/seed-health/'+__import__('hashlib').sha256(raw).hexdigest(),raw)
    scopes={}
    captured={x.get('metadata',{}).get('acquisition_model') for x in catalog['parents']}
    for model,source in health.get('sources',{}).items():
        if source.get('provider_cycle_complete')is not True:continue
        run=source.get('selected_run_time_utc')
        if model in captured:
            scopes[model]=dict(run_time_utc=run,catalog=catalog_ref,cold_readback=proof)
            previous=(prior or {}).get('source_catalogs',{}).get(model,{})
            if previous.get('run_time_utc')==run:
                extras=[dict(catalog=previous['catalog'],cold_readback=previous['cold_readback'])]+previous.get('additional_catalogs',[])
                unique={item['catalog']['sha256']:item for item in extras if item['catalog']['sha256']!=catalog_ref['sha256']}
                if unique:scopes[model]['additional_catalogs']=list(unique.values())
            if model=='ICON-D2-EPS' and native_point_source:scopes[model]['native_point_source']=native_point_source
        elif prior and prior.get('source_catalogs',{}).get(model,{}).get('run_time_utc')==run:
            scopes[model]=prior['source_catalogs'][model]
    return dict(artifact_version=VERSION,payload=payload.json(),integrity=integrity_ref.json(),source_catalogs=scopes,
        canonical_readback_verified=True,acquisition_only=True,operational_promotion=False,
        all_native_sources_ready=len(scopes)==6 and health.get('error_count')==0,
        legacy_v15_compatible=False,source_generated_at_utc=health['generated_at_utc'])
