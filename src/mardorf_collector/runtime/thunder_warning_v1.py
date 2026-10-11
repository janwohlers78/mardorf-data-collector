"""Bounded public transport/execution carrier; analysis and weather stay private.

One batch serves all configured locations. Only a pinned, private source capsule
is executed; no private repository checkout, artifact upload or model fitting.
"""
import importlib.util
import io
import json
import os
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit

import requests

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import ObjectRef
from mardorf_collector.storage.runtime import load_runtime
from .cloud_environment import environment
from .wp09_eumetsat_lfl_v2 import capture as li_capture
from .wp09_lightning_context_v1 import capture as dwd_capture
from .wp09_events_v1 import capture as metar_capture

ROOT = Path(__file__).resolve().parents[3]
PREFIX = 'weather/archive/janwohlers78/mardorf-kitevorhersage/thunder-warning/v1'
POINTER = 'config/cloud_refs/svg_thunder_warning_v1.json'
LATEST = 'reports/thunder_warning/v1/latest.json'


def put(backend, key, raw):
    ref = backend.put_bytes(key, raw)
    if backend.get_bytes(ref) != raw: raise ValueError('warning_object_readback')
    return ref


def capsule(backend, ref, directory):
    """Exact immutable identity and allowlisted flat members before execution."""
    ref = ObjectRef.parse(ref)
    if ref.bytes > 8*1024**2 or not ref.key.startswith(PREFIX+'/capsules/'):
        raise ValueError('warning_capsule_scope_or_budget')
    raw = backend.get_bytes(ref)
    if len(raw) != ref.bytes or sha256(raw).hexdigest() != ref.sha256: raise ValueError('warning_capsule_integrity')
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        entries = z.infolist()
        if (len(entries) != 4 or {e.filename for e in entries} != {'engine.py', 'policy.json', 'lut.npz', 'provenance.json'}
                or sum(e.file_size for e in entries) > 16*1024**2):
            raise ValueError('warning_capsule_members_or_expansion')
        for e in entries: (Path(directory)/e.filename).write_bytes(z.read(e))
    provenance = json.loads((Path(directory)/'provenance.json').read_bytes())
    for name, field in [('engine.py', 'engine_sha256'), ('policy.json', 'policy_original_sha256'), ('lut.npz', 'runtime_LUT_sha256')]:
        if sha256((Path(directory)/name).read_bytes()).hexdigest() != provenance[field]: raise ValueError('warning_component_provenance')
    spec = importlib.util.spec_from_file_location('private_warning_engine', Path(directory)/'engine.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    policy = json.loads((Path(directory)/'policy.json').read_bytes()); module.policy_check(policy)
    import numpy as np
    with np.load(Path(directory)/'lut.npz', allow_pickle=False) as data:
        if set(data.files) != {'lat', 'lon', 'shifts', 'bounds'}: raise ValueError('warning_LUT_fields')
        aux = {k: data[k] for k in data.files}
    if aux['shifts'].shape != (12, len(aux['lat']), 3) or aux['lat'].shape != aux['lon'].shape:
        raise ValueError('warning_LUT_shapes')
    return module, policy, aux, provenance


def recent_li(*, root=ROOT, environ=None, now=None):
    """Explicit bounded successor profile; immutable historical v2 unchanged."""
    now = now or datetime.now(timezone.utc)
    profile = json.loads((Path(root)/'config/wp09_eumetsat_lfl_v2.json').read_bytes())
    profile.update(artifact_version='wp09-LFL-warning-capture-profile-v1', maximum_products=8,
                   maximum_seconds=60, maximum_total_bytes=32*1024**2)
    with tempfile.TemporaryDirectory(prefix='li-warning-profile-') as directory:
        path = Path(directory)/'config'; path.mkdir()
        (path/'wp09_eumetsat_lfl_v2.json').write_bytes(canonical(profile))
        receipt, originals = li_capture((now-timedelta(minutes=40)).isoformat(), now.isoformat(), root=directory, environ=environ)
    receipt['warning_capture_profile'] = profile
    count = receipt.get('catalog_total_results')
    if count is not None and (type(count) is not int or count > 8):
        receipt.update(status='BLOCKED', blocker='warning_catalog_product_budget_incomplete')
    return receipt, originals


def bounded_metar_get(*args, **kwargs):
    # Two station requests remain separate complete native JSON originals.
    kwargs['timeout'] = (5, 12)
    return requests.get(*args, **kwargs)


def recent_metar(*, root=ROOT):
    """Warnings need the latest received report; legacy 48h capture is unchanged.

    Explicit two-hour query profile retains every returned row and field, reduces
    repeated old bytes and avoids rejecting busy 48h SPECI windows at 512 rows.
    """
    profile = json.loads((Path(root)/'config/wp09_events_v1.json').read_bytes())
    profile['artifact_version'] = 'wp09-warning-reported-thunder-binding-v1'
    for station, url in profile['sources'].items():
        parts = urlsplit(url); params = dict(parse_qsl(parts.query)); params['hours'] = '2'
        profile['sources'][station] = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), ''))
    with tempfile.TemporaryDirectory(prefix='metar-warning-profile-') as directory:
        path = Path(directory)/'config'; path.mkdir()
        (path/'wp09_events_v1.json').write_bytes(canonical(profile))
        receipt, originals = metar_capture(root=directory, get=bounded_metar_get)
    receipt['warning_capture_profile'] = profile
    return receipt, originals


def capture_all(*, root=ROOT, environ=None):
    with ThreadPoolExecutor(max_workers=3) as pool:
        tasks = {'li': pool.submit(recent_li, root=root, environ=environ),
                 'dwd': pool.submit(dwd_capture, root=root),
                 'metar': pool.submit(recent_metar, root=root)}
        receipts = {}; originals = {}
        for name, task in tasks.items():
            try: receipts[name], originals[name] = task.result()
            except Exception as exc:
                receipts[name] = dict(records=[], status='BLOCKED', blocker='capture_'+type(exc).__name__)
                originals[name] = {}
    return receipts, originals


def publish(result, receipts, originals, runtime, *, capsule_ref, provenance, event_id, commit):
    """Native failed/partial responses included; no old slot can retract new one."""
    stored_receipts = {}
    for source, receipt in receipts.items():
        if set(originals[source]) != {r['quantity'] for r in receipt['records']}: raise ValueError('warning_original_inventory')
        records = []
        for r in receipt['records']:
            raw = originals[source][r['quantity']]
            if len(raw) != r['source_bytes'] or sha256(raw).hexdigest() != r['source_sha256']: raise ValueError('warning_original_identity')
            ref = put(runtime.backend, PREFIX+'/originals/'+r['source_sha256'], raw)
            records.append(dict(r, original=ref.json()))
        raw = canonical(dict(receipt, records=records))
        ref = put(runtime.backend, PREFIX+'/receipts/'+sha256(raw).hexdigest()+'.json', raw)
        stored_receipts[source] = ref.json()
    result.update(source_receipts=stored_receipts, capsule=capsule_ref, provenance=provenance,
                  event_id=event_id, producer_commit=commit)
    raw = canonical(result); ref = put(runtime.backend, PREFIX+'/results/'+sha256(raw).hexdigest()+'.json', raw)
    metadata = dict(channel='experimental-thunder-warning-v1', producer_repository='janwohlers78/mardorf-data-collector',
                    producer_commit=commit, event_id=event_id, receipt_sha256=ref.sha256)
    pointer = dict(schema_version=1, artifact_version='svg-thunder-warning-control-v1', snapshot=ref.json(),
        generated_at_utc=result['issued_at_utc'], metadata=metadata, receipt_sha256=ref.sha256,
        readback_verified=True, bundle_ready=True, kind='thunder_warning')
    def merge(current, changes):
        prior = current.read(LATEST, required=False)
        if prior:
            old = json.loads(prior)
            if old['slot_utc'] >= result['slot_utc']: return {}
        return changes
    path = 'reports/thunder_warning/v1/slots/'+result['slot_utc'].replace(':', '')+'.json'
    outcome = runtime.publish({path: raw, LATEST: raw}, metadata=metadata, merge=merge,
        extra_refs=lambda _: {POINTER: pointer})
    return dict(outcome, report=ref.json(), generated_at_utc=result['issued_at_utc'],
                slot_utc=result['slot_utc'], event_id=event_id,
                private_routine_actions_minutes_increment=0,
                site_count=len(result['sites']), scientific_status='NOT_READY')


def main(*, environ=None, root=ROOT, runtime_factory=load_runtime, capture_fn=capture_all):
    env = dict(os.environ if environ is None else environ); began = time.monotonic()
    try:
        runtime = runtime_factory(root, environ=environment(root, environ=env))
        now = datetime.now(timezone.utc); slot = datetime.fromtimestamp(int(now.timestamp())//1800*1800, timezone.utc)
        prior_raw = runtime.read(LATEST, required=False); prior = json.loads(prior_raw) if prior_raw else None
        if prior and prior['slot_utc'] >= slot.isoformat():
            print(json.dumps(dict(status='unchanged', reason='halfhour_slot_already_published'))); return 0
        control = json.loads((Path(root)/'config/thunder_warning_runtime_v1.json').read_bytes())
        if (control.get('schema_version') != 1 or control.get('artifact_version') != 'thunder-warning-runtime-v1'
                or control.get('analysis_owner') != 'private_repository'
                or control.get('execution_runner') != 'public_standard_linux'
                or control.get('private_actions_minutes_increment') != 0
                or control.get('maximum_seconds') != 300 or control.get('cadence_seconds') != 1800):
            raise ValueError('warning_runtime_contract')
        with tempfile.TemporaryDirectory(prefix='private-warning-') as directory:
            module, policy, aux, provenance = capsule(runtime.backend, control['capsule'], directory)
            receipts, originals = capture_fn(root=root, environ=env)
            issue = datetime.now(timezone.utc)
            # If processing crossed a slot boundary, its actual output belongs
            # to the new slot. Source availability is still checked as received.
            slot = datetime.fromtimestamp(int(issue.timestamp())//1800*1800, timezone.utc)
            result = module.generate(receipts, originals, policy, aux,
                issued_at_utc=issue.isoformat(), slot_utc=slot.isoformat(), previous=prior)
            outcome = publish(result, receipts, originals, runtime, capsule_ref=control['capsule'], provenance=provenance,
                event_id=env['GITHUB_RUN_ID'], commit=env['GITHUB_SHA'])
        outcome['elapsed_seconds'] = time.monotonic()-began
    except Exception as exc:
        # No private code, raw weather, URL, response, token or traceback in
        # public job logs. Detailed source status is stored in private results.
        print(json.dumps(dict(status='BLOCKED', failure_type=type(exc).__name__))); return 1
    print('SVG_THUNDER_WARNING_BEGIN'); print(json.dumps(outcome, sort_keys=True)); print('SVG_THUNDER_WARNING_END')
    return 0


if __name__ == '__main__': raise SystemExit(main())
