"""Public archive packages and small private release adoption; no private rows.

Monthly files retain their physical schema. Archive objects have a separate
namespace so the routine orphan reconciler cannot enqueue their monthly pieces.
"""
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from ..storage.objects import ObjectRef
from ..storage.runtime import load_runtime
from ..wp13.core_v1 import canonical, digest, identify, stamp, strict_json, utc
from ..wp13 import regional_stations_v1 as old
from . import prepared as legacy
from . import prepared_package_v1 as prepared
from . import regional_v2 as dwd
from .assets import ROOT

PREFIX = 'weather/station-packages/v1'
READY = 'data/weather_native/consumer_readiness_station_packages_v1.json'
INDEX = 'data/weather_native/station_packages_v1/admitted/index.json'


def policy(root=ROOT):
    return prepared.policy(root)


def dimensions(root, station, year):
    configured, contracts = dwd.configuration(root)
    record = next((s for s in configured['stations'] if s['id'] == station), None)
    if record is None or type(year) is not int or year not in policy(root)['archive_years']:
        raise ValueError('Registered station and bounded archive year required')
    return configured, contracts, record


def put(backend, prefix, value, maximum):
    body = canonical(value)
    if len(body) > maximum:
        raise ValueError('Package metadata byte budget exceeded')
    return backend.put_bytes(prefix + '/' + hashlib.sha256(body).hexdigest(), body).json()


def read(backend, reference, prefix, maximum):
    ref = ObjectRef.parse(reference)
    if not ref.key.startswith(prefix + '/') or not ref.key.endswith('/' + ref.sha256) or ref.bytes > maximum:
        raise ValueError('Package content-reference scope/size mismatch')
    body = backend.get_bytes(ref)
    if len(body) != ref.bytes or hashlib.sha256(body).hexdigest() != ref.sha256:
        raise ValueError('Package content hash mismatch')
    return strict_json(body)


def existing(backend, prefix, maximum):
    listing = backend.list_page(prefix + '/', limit=2)
    if listing['cursor'] is not None:
        raise ValueError('Package replay generation budget exceeded')
    found = []
    for key in listing['keys']:
        meta = backend.head(key)
        ref = ObjectRef(key, meta['sha256'], meta['bytes']).json()
        doc = read(backend, ref, prefix, maximum)
        found.append((doc.get('private_received_at_utc', doc.get('public_verified_at_utc', '')), key, ref, doc))
    return min(found)[2:] if found else None


def month_document(backend, reference, *, root):
    ref = ObjectRef.parse(reference)
    module = legacy if ref.key.startswith(legacy.PREFIX + '/') else prepared
    expected = module.processor_identity(root)
    return module.read_manifest(backend, reference, expected_processor=expected, root=root)


def month_records(backend, doc):
    module = legacy if doc['artifact_version'] == 'wp15-public-station-preprocessed-v1' else prepared
    return module.read_records(backend, doc)


def validate_bundle(doc, *, root=ROOT):
    p = policy(root)
    required = {'schema_version', 'artifact_version', 'package_id', 'engine_sha256',
        'configuration_sha256', 'station_id', 'site_id', 'year', 'products', 'originals',
        'months', 'field_count', 'public_verified_at_utc', 'scientific_gate'}
    if (set(doc) != required or doc['schema_version'] != 1
            or doc['artifact_version'] != 'wp15-station-year-package-v1'
            or doc['scientific_gate'] != 'OPEN' or doc['engine_sha256'] != prepared.processor_identity(root)
            or digest({k:v for k,v in doc.items() if k not in ('package_id','public_verified_at_utc')}) != doc['package_id']):
        raise ValueError('Package version/engine/content identity mismatch')
    configured, contracts, station = dimensions(root, doc['station_id'], doc['year'])
    products = sorted(configured['hourly_products'])
    if (doc['configuration_sha256'] != contracts.configuration['sha256'] or doc['site_id'] != station['site_id']
            or doc['products'] != products or set(doc['originals']) != set(products)
            or set(doc['months']) != {f'{product}/{m:02d}' for product in products for m in range(1,13)}):
        raise ValueError('Incomplete station/year/product/month matrix')
    total = 0
    engines = {legacy:legacy.processor_identity(root), prepared:doc['engine_sha256']}
    for product, original in doc['originals'].items():
        if (original['station_id'] != doc['station_id'] or original['product'] != product
                or original['path'] != old.ORIGINALS + original['sha256'] + '.zip'
                or not re.fullmatch('[0-9a-f]{64}', original['sha256'])
                or type(original['bytes']) is not int or not 0 < original['bytes'] <= p['original_max_bytes']):
            raise ValueError('Package original binding mismatch')
        utc(original['captured_at_utc'])
    for slot, entry in doc['months'].items():
        product, month = slot.split('/')
        if entry.get('status') == 'VERIFIED_EMPTY':
            if set(entry) != {'status'}:
                raise ValueError('Empty month cannot carry weather references')
            continue
        if set(entry) != {'status','manifest','document'} or entry['status'] != 'VERIFIED':
            raise ValueError('Explicit verified month state required')
        child = entry['document']
        body = canonical(child);ref = ObjectRef.parse(entry['manifest'])
        if ref.bytes != len(body) or ref.sha256 != hashlib.sha256(body).hexdigest():
            raise ValueError('Embedded monthly manifest hash mismatch')
        class Embedded:
            def get_bytes(self, asked):
                if asked != ref: raise ValueError('Unexpected embedded reference')
                return body
        module = legacy if ref.key.startswith(legacy.PREFIX + '/') else prepared
        module.read_manifest(Embedded(), ref.json(), expected_processor=engines[module], root=root)
        begin = datetime(doc['year'],int(month),1,tzinfo=timezone.utc)
        end = datetime(doc['year']+1,1,1,tzinfo=timezone.utc) if month == '12' else datetime(doc['year'],int(month)+1,1,tzinfo=timezone.utc)
        window = child['envelope']['extensions'].get('dwd:cdc-window:v1')
        original = doc['originals'][product]
        if (child['scope']['site_id'] != station['site_id'] or child['scope']['station_id'] != station['station_id']
                or child['scope']['profile_id'] != configured['profile_id'] or child['envelope']['context'] != 'historical'
                or window != dict(start_utc=stamp(begin),end_utc=stamp(end),product=product,station_id=doc['station_id'],hourly=True,historical=True)
                or not begin <= utc(child['scope']['start_utc']) <= utc(child['scope']['end_utc']) < end
                or child['envelope']['capture']['retrieved_at_utc'] != original['captured_at_utc']
                or child['originals']['raw-0'].get('archive_path') != original['path']
                or child['originals']['raw-0']['sha256'] != original['sha256']
                or child['originals']['raw-0']['bytes'] != original['bytes']
                or set(child['quantity_counts']) - {q for q,u in configured['hourly_products'][product]['fields'].values()}
                or utc(child['public_verified_at_utc']) > utc(doc['public_verified_at_utc'])):
            raise ValueError('Package monthly source/scope/time binding mismatch')
        total += child['field_count']
    if type(doc['field_count']) is not int or doc['field_count'] != total:
        raise ValueError('Package total field count mismatch')
    return doc


def read_bundle(backend, reference, *, root=ROOT):
    return validate_bundle(read(backend, reference, PREFIX + '/bundles', policy(root)['package_max_bytes']), root=root)


def require_ready(cloud, root):
    body = cloud.read(READY, required=False)
    ready = strict_json(body) if body else {}
    if (ready.get('available') is not True or ready.get('engine_sha256') != prepared.processor_identity(root)
            or ready.get('contract_sha256') != hashlib.sha256((Path(root)/prepared.CONTRACT).read_bytes()).hexdigest()):
        raise ValueError('Exact deployed private package capability required')


def product_worker(root, station, year, product, original, commit):
    root = Path(root);cloud = load_runtime(root);cloud.open()
    return build_product(cloud, root=root, station=station, year=year, product=product, original=original, commit=commit)


def build_product(cloud, *, root, station, year, product, original, commit):
    configured, contracts, record = dimensions(root, station, year)
    engine = prepared.processor_identity(root)
    unit_id = digest(dict(station=station,year=year,product=product,original_sha256=original['sha256'],engine_sha256=engine))
    prefix = f'{PREFIX}/products/{station}/{year}/{product}/{unit_id}'
    prior = existing(cloud.backend, prefix, policy(root)['package_max_bytes'])
    if prior:
        ref, doc = prior
        if doc['unit_id'] != unit_id or set(doc['months']) != {f'{m:02d}' for m in range(1,13)}:
            raise ValueError('Product checkpoint binding mismatch')
        return doc
    raw = None;months = {};original_reads = 0
    for month in range(1,13):
        key = f'{month:02d}'
        old_cursor = f'data/weather_native/station_preprocessed_v1/backfill/{station}/{product}/{year}/{key}.json'
        old_body = cloud.read(old_cursor,required=False)
        if old_body:
            old_doc = strict_json(old_body)
            expected = dict(station=station,product=product,year=year,month=month,original_sha256=original['sha256'],processor_sha256=legacy.processor_identity(root))
            if any(old_doc.get(k) != v for k,v in expected.items()):
                raise ValueError('Frozen I05 checkpoint identity mismatch')
            if old_doc.get('status') == 'VERIFIED_EMPTY':
                months[key] = {'status':'VERIFIED_EMPTY'};continue
            ref = old_doc['publication']['manifest']
            doc = month_document(cloud.backend,ref,root=root)
            months[key] = dict(status='VERIFIED',manifest=ref,document=doc);continue
        if raw is None:
            raw = cloud.read(original['path']);original_reads += 1
            if len(raw) != original['bytes'] or hashlib.sha256(raw).hexdigest() != original['sha256']:
                raise ValueError('Historical original bytes/hash mismatch')
            dwd.cache_window(raw,station,configured,f'{year}-01-01T00:00:00Z',f'{year+1}-01-01T00:00:00Z')
        begin = datetime(year,month,1,tzinfo=timezone.utc)
        end = datetime(year+1,1,1,tzinfo=timezone.utc) if month == 12 else datetime(year,month+1,1,tzinfo=timezone.utc)
        result,_ = dwd.prepare(raw,original['captured_at_utc'],station,product,start=stamp(begin),end=stamp(end),hourly=True,historical=True,contracts=contracts,policy=configured,commit=commit)
        if result is None:
            months[key] = {'status':'VERIFIED_EMPTY'};continue
        proof = prepared.preprocess(cloud.backend,result,root=root,archive_originals={original['sha256']:original['path']})
        doc = month_document(cloud.backend,proof['manifest'],root=root)
        months[key] = dict(status='VERIFIED',manifest=proof['manifest'],document=doc)
    value = dict(schema_version=1,artifact_version='wp15-package-product-checkpoint-v1',unit_id=unit_id,
        months=months,public_verified_at_utc=stamp(datetime.now(timezone.utc)))
    put(cloud.backend,prefix,value,policy(root)['package_max_bytes'])
    return value


def build(cloud, *, station, year, root=ROOT, workers=2):
    started=time.monotonic();p=policy(root)
    if type(workers) is not int or not 1 <= workers <= p['max_package_workers']:
        raise ValueError('Bounded package worker count required')
    configured,contracts,record=dimensions(root,station,year);require_ready(cloud,root)
    engine=prepared.processor_identity(root);prefix=f'{PREFIX}/bundles/{engine}/{station}/{year}'
    prior=existing(cloud.backend,prefix,p['package_max_bytes'])
    if prior:
        ref,doc=prior;validate_bundle(doc,root=root)
        return dict(status='VERIFIED_EXISTING',station=station,year=year,bundle=ref,package_id=doc['package_id'],fields=doc['field_count'],elapsed_seconds=time.monotonic()-started)
    archive=strict_json(cloud.read(old.ARCHIVE));products=sorted(configured['hourly_products'])
    originals={product:next(x for x in archive['originals'] if x['station_id']==station and x['product']==product) for product in products}
    commit=os.environ.get('GITHUB_SHA') or subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    if workers == 1:
        units={product:build_product(cloud,root=root,station=station,year=year,product=product,original=originals[product],commit=commit) for product in products}
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures={product:executor.submit(product_worker,str(root),station,year,product,originals[product],commit) for product in products}
            units={product:future.result(timeout=max(1,p['max_package_processing_seconds']-(time.monotonic()-started))) for product,future in futures.items()}
    months={f'{product}/{month}':value for product,unit in units.items() for month,value in unit['months'].items()}
    doc=dict(schema_version=1,artifact_version='wp15-station-year-package-v1',engine_sha256=engine,
        configuration_sha256=contracts.configuration['sha256'],station_id=station,site_id=record['site_id'],year=year,
        products=products,originals=originals,months=months,field_count=sum(e['document']['field_count'] for e in months.values() if e['status']=='VERIFIED'),scientific_gate='OPEN')
    doc['package_id']=digest(doc);doc['public_verified_at_utc']=stamp(datetime.now(timezone.utc));validate_bundle(doc,root=root)
    ref=put(cloud.backend,prefix,doc,p['package_max_bytes']);read_bundle(cloud.backend,ref,root=root)
    return dict(status='PASS',station=station,year=year,bundle=ref,package_id=doc['package_id'],fields=doc['field_count'],elapsed_seconds=time.monotonic()-started)


def release(cloud, *, stations, years, root=ROOT):
    require_ready(cloud,root);p=policy(root);engine=prepared.processor_identity(root)
    if (not stations or len(stations)!=len(set(stations)) or not years or len(years)!=len(set(years))
            or len(stations)*len(years)>p['max_packages']):raise ValueError('Bounded unique release dimensions required')
    refs={};fields=0
    for station in sorted(stations):
        for year in sorted(years):
            dimensions(root,station,year)
            prefix=f'{PREFIX}/bundles/{engine}/{station}/{year}'
            prior=existing(cloud.backend,prefix,p['package_max_bytes'])
            if not prior:raise ValueError('Release cannot include an incomplete station/year')
            ref,doc=prior;validate_bundle(doc,root=root);refs[f'{station}/{year}']=ref;fields+=doc['field_count']
    doc=dict(schema_version=1,artifact_version='wp15-station-package-release-v1',engine_sha256=engine,
        stations=sorted(stations),years=sorted(years),packages=refs,field_count=fields,scientific_gate='OPEN')
    doc['release_id']=digest(doc);doc['public_verified_at_utc']=stamp(datetime.now(timezone.utc))
    prefix=f'{PREFIX}/releases/{engine}/{doc["release_id"]}'
    prior=existing(cloud.backend,prefix,p['release_max_bytes'])
    ref=prior[0] if prior else put(cloud.backend,prefix,doc,p['release_max_bytes'])
    return dict(status='PASS',release_id=doc['release_id'],release=ref,packages=len(refs),fields=fields)


def read_release(backend, release_id, *, root=ROOT):
    if not re.fullmatch('[0-9a-f]{64}',release_id):raise ValueError('Exact release digest required')
    p=policy(root);engine=prepared.processor_identity(root)
    prior=existing(backend,f'{PREFIX}/releases/{engine}/{release_id}',p['release_max_bytes'])
    if not prior:raise ValueError('Requested immutable archive release absent')
    ref,doc=prior
    if (set(doc)!={'schema_version','artifact_version','engine_sha256','stations','years','packages','field_count','scientific_gate','release_id','public_verified_at_utc'}
            or type(doc['field_count']) is not int or doc['field_count']<0
            or doc.get('artifact_version')!='wp15-station-package-release-v1' or type(doc.get('schema_version')) is not int or doc['schema_version']!=1
            or doc['release_id']!=release_id or doc['engine_sha256']!=engine or doc['scientific_gate']!='OPEN'
            or digest({k:v for k,v in doc.items() if k not in ('release_id','public_verified_at_utc')})!=release_id
            or len(doc['packages'])>p['max_packages'] or not doc['packages']
            or doc['stations']!=sorted(set(doc['stations'])) or doc['years']!=sorted(set(doc['years']))
            or set(doc['packages'])!={f'{s}/{y}' for s in doc['stations'] for y in doc['years']}):
        raise ValueError('Archive release identity/completeness mismatch')
    for station in doc['stations']:
        for year in doc['years']:dimensions(root,station,year)
    utc(doc['public_verified_at_utc'])
    return ref,doc


def admission_ref(backend, identity, root):
    return existing(backend,f'{PREFIX}/admissions/{identity}',policy(root)['admission_max_bytes'])


def validate_receipt(ack, reference, doc, bundle_ref, *, root=ROOT):
    ref=ObjectRef.parse(reference)
    required={'schema_version','artifact_version','package_id','bundle','engine_sha256','site_id','year',
        'field_count','private_received_at_utc','public_verified_at_utc','scientific_gate','receipt_id'}
    if (set(ack)!=required or ack['schema_version']!=1 or ack['artifact_version']!='wp15-private-package-admission-v1'
            or ack['scientific_gate']!='OPEN' or digest({k:v for k,v in ack.items() if k!='receipt_id'})!=ack['receipt_id']
            or ref.key!=f'{PREFIX}/admissions/{doc["package_id"]}/{ref.sha256}'
            or ref.bytes>policy(root)['admission_max_bytes'] or ack['bundle']!=bundle_ref
            or any(ack[k]!=doc[k] for k in ('package_id','engine_sha256','site_id','year','field_count','public_verified_at_utc'))
            or utc(ack['private_received_at_utc'])<utc(doc['public_verified_at_utc'])):
        raise ValueError('Private package receipt binding/clock mismatch')
    body=canonical(ack)
    if len(body)!=ref.bytes or hashlib.sha256(body).hexdigest()!=ref.sha256:
        raise ValueError('Private package receipt content hash mismatch')
    return ack


def consume_release(cloud, release_id, *, root=ROOT):
    """One private clock per verified bundle, one visibility CAS for the release."""
    started=time.monotonic();ref,release_doc=read_release(cloud.backend,release_id,root=root)
    engine=prepared.processor_identity(root)
    def checked(item):
        key,bundle_ref=item;doc=read_bundle(cloud.backend,bundle_ref,root=root)
        if key!=f'{doc["station_id"]}/{doc["year"]}' or utc(doc['public_verified_at_utc'])>utc(release_doc['public_verified_at_utc']):
            raise ValueError('Release package scope/clock mismatch')
        return key,bundle_ref,doc
    with ThreadPoolExecutor(max_workers=8) as executor:
        packages=list(executor.map(checked,sorted(release_doc['packages'].items())))
    if sum(doc['field_count'] for _,_,doc in packages)!=release_doc['field_count']:
        raise ValueError('Release field total mismatch')
    def receipt(item):
        key,bundle_ref,doc=item;prior=admission_ref(cloud.backend,doc['package_id'],root)
        if prior:
            receipt_ref,ack=prior
            if ack['bundle']!=bundle_ref or ack['engine_sha256']!=engine:raise ValueError('Private package replay conflict')
        else:
            at=stamp(datetime.now(timezone.utc))
            if utc(at)<utc(doc['public_verified_at_utc']):raise ValueError('Private package arrival precedes public verification')
            ack=identify(dict(schema_version=1,artifact_version='wp15-private-package-admission-v1',package_id=doc['package_id'],
                bundle=bundle_ref,engine_sha256=engine,site_id=doc['site_id'],year=doc['year'],field_count=doc['field_count'],
                private_received_at_utc=at,public_verified_at_utc=doc['public_verified_at_utc'],scientific_gate='OPEN'),'receipt_id')
            receipt_ref=put(cloud.backend,f'{PREFIX}/admissions/{doc["package_id"]}',ack,policy(root)['admission_max_bytes'])
        validate_receipt(ack,receipt_ref,doc,bundle_ref,root=root)
        return doc['package_id'],dict(bundle=bundle_ref,admission=receipt_ref,site_id=doc['site_id'],year=doc['year'],
            field_count=doc['field_count'],engine_sha256=engine)
    with ThreadPoolExecutor(max_workers=8) as executor:
        accepted=dict(executor.map(receipt,packages))
    def merge(current, changes):
        body=current.read(INDEX,required=False)
        index=strict_json(body) if body else dict(schema_version=1,artifact_version='wp15-private-package-index-v1',entries={})
        if index.get('artifact_version')!='wp15-private-package-index-v1' or len(index['entries'])>80:raise ValueError('Private package catalog budget/version mismatch')
        changed=False
        for identity,entry in accepted.items():
            previous=index['entries'].get(identity)
            if previous is not None and previous!=entry:raise ValueError('Concurrent package receipt conflict')
            if previous is None:index['entries'][identity]=entry;changed=True
        if len(index['entries'])>80:raise ValueError('Private package index generation budget exceeded')
        return {INDEX:canonical(index)} if changed else {}
    publication=cloud.publish({},metadata=dict(channel='wp15-private-archive-release',release_id=release_id),merge=merge)
    return dict(status='PASS',release_id=release_id,release=ref,packages=len(packages),fields=release_doc['field_count'],
        visibility_transactions=1,publication_status=publication['status'],normalizations=0,full_original_reads=0,provider_requests=0,
        scientific_gate='OPEN',elapsed_seconds=time.monotonic()-started)


def main():
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('operation',choices=['build','release'])
    parser.add_argument('--station');parser.add_argument('--year',type=int);parser.add_argument('--workers',type=int,default=2)
    parser.add_argument('--stations',default='all');parser.add_argument('--years',default='all');args=parser.parse_args()
    cloud=load_runtime(ROOT);configured,_=dwd.configuration(ROOT)
    if args.operation=='build':report=build(cloud,station=args.station,year=args.year,workers=args.workers)
    else:
        stations=[s['id'] for s in configured['stations']] if args.stations=='all' else args.stations.split(',')
        years=policy(ROOT)['archive_years'] if args.years=='all' else [int(y) for y in args.years.split(',')]
        report=release(cloud,stations=stations,years=years)
    Path('work').mkdir(exist_ok=True);Path('work/wp15_archive_packages.json').write_bytes(canonical(report)+b'\n')
    print('WP15_ARCHIVE_PACKAGES='+json.dumps(report,sort_keys=True),flush=True)


if __name__=='__main__':main()
