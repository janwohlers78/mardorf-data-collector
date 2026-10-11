"""Public transport for a private, finite LI label capsule; no weather model."""
import importlib.util
import io
import json
import sys
import tempfile
import zipfile
from datetime import datetime,timezone,timedelta
from hashlib import sha256
from pathlib import Path

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import ObjectRef

PREFIX='weather/archive/janwohlers78/mardorf-kitevorhersage/wp09-li-prospective/v1'
DAY_PREFIX='data/observations/wp09_li_output/days/'


def profile(root,now):
    path=Path(root)/'config/li_output_shadow_runtime_v1.json'
    if not path.exists():return None
    p=json.loads(path.read_bytes())
    if (p['artifact_version']!='wp09-li-output-label-carrier-v1' or p['private_actions_minutes_increment']!=0
            or p['capture_window_minutes']!=70 or p['maximum_products']!=16 or p['warning_view_minutes']!=40):
        raise ValueError('LI_label_carrier_profile')
    if datetime.fromisoformat(p['collection_start_utc'])<=now<datetime.fromisoformat(p['collection_end_utc']):return p
    return None


def load(backend,ref,directory):
    ref=ObjectRef.parse(ref)
    if ref.bytes>8*1024**2 or not ref.key.startswith(PREFIX+'/label-capsules/'):raise ValueError('LI_label_capsule_scope')
    raw=backend.get_bytes(ref)
    if len(raw)!=ref.bytes or sha256(raw).hexdigest()!=ref.sha256:raise ValueError('LI_label_capsule_integrity')
    names={'li_shadow/__init__.py','li_shadow/li_observations_v1.py','li_shadow/thunder_warning_v1.py','li_shadow/policy.json'}
    with zipfile.ZipFile(io.BytesIO(raw))as z:
        entries=z.infolist()
        if len(entries)!=4 or {e.filename for e in entries}!=names or sum(e.file_size for e in entries)>16*1024**2:
            raise ValueError('LI_label_capsule_members')
        for e in entries:
            p=Path(directory)/e.filename;p.parent.mkdir(exist_ok=True);p.write_bytes(z.read(e))
    package=Path(directory)/'li_shadow';policy=json.loads((package/'policy.json').read_bytes())
    for name,h in policy['source_sha256'].items():
        if name not in names or name.endswith('policy.json') or sha256((Path(directory)/name).read_bytes()).hexdigest()!=h:
            raise ValueError('LI_label_capsule_source')
    if set(policy['source_sha256'])!=names-{'li_shadow/policy.json'}:raise ValueError('LI_label_capsule_inventory')
    spec=importlib.util.spec_from_file_location('private_li_output_shadow',package/'__init__.py',submodule_search_locations=[str(package)])
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    return module,policy


def warning_view(receipt,originals):
    """The frozen warning still receives its original 40-minute evidence view."""
    cutoff=datetime.fromisoformat(receipt['requested_end_utc'])-timedelta(minutes=40)
    records=[r for r in receipt['records']if not r['quantity'].startswith('LFL_') or datetime.fromisoformat(r['native_end_utc'])>cutoff]
    return dict(receipt,records=records,warning_consumer_view_minutes=40),{r['quantity']:originals[r['quantity']]for r in records}


def generate(runtime,p,receipt_ref,previous,*,put,warning_engine_sha256,original_cache=None):
    with tempfile.TemporaryDirectory(prefix='li-output-label-')as directory:
        module,policy=load(runtime.backend,p['capsule'],directory)
        if policy['warning_engine_sha256']!=warning_engine_sha256:raise ValueError('LI_label_frozen_decoder_binding')
        now=datetime.now(timezone.utc).isoformat()
        def read(ref):
            cached=(original_cache or {}).get(ref['sha256'])
            return cached if cached is not None else runtime.backend.get_bytes(ObjectRef.parse(ref))
        packet=module.generate(read,receipt_ref,previous,policy['spatial_support'],as_of_utc=now)
    raw=canonical(packet);ref=put(PREFIX+'/label-packets/'+sha256(raw).hexdigest()+'.json',raw)
    available=datetime.now(timezone.utc).isoformat();files={};days={}
    for row in packet['assembly']['labels']:
        date=datetime.fromtimestamp(row['start_epoch'],timezone.utc).strftime('%Y/%m/%d')
        path=DAY_PREFIX+date+'.json'
        if path not in days:
            old=runtime.read(path,required=False)
            days[path]=json.loads(old) if old else dict(artifact_version='wp09-LI-output-day-labels-v1',target_id=row['target_id'],labels={})
        bound=dict(row,available_utc=available,proof=ref.json(),physical_ground_release=False)
        bound['revision_sha256']=sha256(canonical(bound)).hexdigest()
        days[path]['labels'][str(row['valid_epoch'])]=bound
    for path,day in days.items():
        body=canonical(day)
        if len(body)>65536:raise ValueError('LI_label_day_budget')
        files[path]=body
    return dict(status='PASS_nominal_output_labels',record=ref.json(),recent_receipts=packet['recent_receipts'],
        covered_hours=sum(r['full_nominal_support']for r in packet['assembly']['labels']),
        unknown_hours=sum(r['nominal_optical_y']is None for r in packet['assembly']['labels']),
        physical_ground_release=False,provider_requests=0,optimizer_calls=0),files
