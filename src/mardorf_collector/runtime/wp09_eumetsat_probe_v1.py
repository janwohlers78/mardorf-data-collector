"""Explicit, manual access probe; complete originals remain in private B2.

No scheduled source owner, local labels, calibration or forecast dispatch.
"""
import json
import os
from datetime import datetime, timezone
from hashlib import sha256

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.runtime import load_runtime
from .cloud_environment import environment
from .wp09_eumetsat_lfl_v2 import ROOT, capture

PREFIX = 'weather/archive/janwohlers78/mardorf-kitevorhersage/wp09-eumetsat-lfl/v2'
POINTER = 'config/cloud_refs/wp09_li_lfl_ingress_v2.json'


def interval(start, end, *, now=None):
    """Small access probe, not an unbounded archive subscription."""
    try:
        a = datetime.fromisoformat(start.replace('Z', '+00:00'))
        b = datetime.fromisoformat(end.replace('Z', '+00:00'))
        now = now or datetime.now(timezone.utc)
        if (a.tzinfo is None or b.tzinfo is None or a.utcoffset().total_seconds() != 0
                or b.utcoffset().total_seconds() != 0 or not a < b <= now
                or (b-a).total_seconds() > 3600):
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise ValueError('LI_probe_requires_explicit_past_UTC_interval_maximum_one_hour') from None
    return a.isoformat(), b.isoformat()


def publish(receipt, originals, runtime, *, event_id=None):
    """All byte checks and readbacks precede receipt and final cloud-head CAS."""
    records = []
    if (len(receipt['records']) != len(originals)
            or set(originals) != {r['quantity'] for r in receipt['records']}):
        raise ValueError('LI_probe_original_inventory_mismatch')
    for record in receipt['records']:
        raw = originals[record['quantity']]
        digest = sha256(raw).hexdigest()
        if digest != record['source_sha256'] or len(raw) != record['source_bytes']:
            raise ValueError('LI_probe_original_identity_mismatch')
        ref = runtime.backend.put_bytes(PREFIX+'/originals/'+digest, raw)
        if runtime.backend.get_bytes(ref) != raw:
            raise ValueError('LI_probe_original_readback_failed')
        records.append(dict(record, original=ref.json()))
    stored = canonical(dict(receipt, records=records))
    ref = runtime.backend.put_bytes(PREFIX+'/receipts/'+sha256(stored).hexdigest()+'.json', stored)
    if runtime.backend.get_bytes(ref) != stored:
        raise ValueError('LI_probe_receipt_readback_failed')
    metadata = dict(channel='wp09-manual-eumetsat-access-probe-v1',
                    producer_repository='janwohlers78/mardorf-data-collector', receipt_sha256=ref.sha256)
    if event_id is not None:
        metadata['event_id'] = event_id
    pointer = dict(schema_version=1, artifact_version='wp09-li-lfl-ingress-control-v2',
                   generated_at_utc=receipt['captured_utc'], snapshot=ref.json(), metadata=metadata,
                   readback_verified=True, kind='wp09_li_lfl', receipt_sha256=ref.sha256,
                   bundle_ready=False)
    path = 'data/inbox/wp09_li_lfl/'+receipt['captured_utc'].replace(':', '')+'.json'
    runtime.publish({path: stored}, metadata=metadata, extra_refs=lambda _: {POINTER: pointer})
    return dict(status=receipt['status'], blocker=receipt['blocker'], snapshot=ref.json(),
                captured_product_count=receipt.get('captured_product_count', 0),
                detector_coverage=False, routine_activation=False)


def main(*, environ=None, capture_fn=capture, runtime_factory=load_runtime):
    env = dict(os.environ if environ is None else environ)
    try:
        start, end = interval(env.get('LI_START_UTC'), env.get('LI_END_UTC'))
        receipt, originals = capture_fn(start, end, environ=env)
        result = publish(receipt, originals,
                         runtime_factory(ROOT, environ=environment(ROOT, environ=env)),
                         event_id=env.get('GITHUB_RUN_ID'))
    except Exception as exc:
        # Never include request exception/response/URL or a credential value.
        print(json.dumps(dict(status='BLOCKED', failure_type=type(exc).__name__)))
        return 1
    print('WP09_LI_ACCESS_BEGIN')
    print(json.dumps(result, sort_keys=True))
    print('WP09_LI_ACCESS_END')
    return 0 if result['status'] == 'ORIGINALS_CAPTURED_QC_OPEN' else 1


if __name__ == '__main__':
    raise SystemExit(main())
