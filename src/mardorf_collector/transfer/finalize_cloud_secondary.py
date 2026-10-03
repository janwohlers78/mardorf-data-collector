"""Publish a coherent secondary batch from two immutable cloud deliveries."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path

from mardorf_collector.storage.archive import canonical, file_path
from mardorf_collector.storage.objects import ObjectError
from mardorf_collector.storage.runtime import load_runtime
from .finalize_secondary_batch import CHILDREN, compact_stamp, immutable_receipt_path
from .push_private import parse_time, workflow_provenance

PREFIX = 'data/inbox/public_collector'
METHOD = 'cloud-secondary-batch-receipt-v1'
INVOCATION_FIELDS = ('repository', 'sha', 'run_id', 'run_attempt')


def bound_child(runtime, kind, report, invocation):
    """Re-read original bytes at the pinned root, never a mutable child pointer."""
    if (report.get('kind') != kind or report.get('bundle_ready_for_private_revalidation') is not True or
            report.get('status') not in ('PASS', 'PASS_WITH_WARNINGS') or
            type(report.get('error_count')) is not int or report['error_count'] != 0):
        raise ObjectError('Secondary integrity report is not ready')
    clock = report['generated_at_utc']
    if datetime.fromisoformat(clock.replace('Z', '+00:00')).tzinfo is None:
        raise ObjectError('Secondary source clock lacks timezone')
    when = parse_time(clock)
    stamp = compact_stamp(clock)
    path = immutable_receipt_path(kind, clock)
    receipt = json.loads(runtime.read(path))
    expected = {'method_version': 'cloud-transfer-readback-v1', 'kind': kind, 'stamp': stamp,
                'source_generated_at_utc': clock, 'readback_verified': True,
                'verified_data_commit_sha': None}
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise ObjectError('Secondary immutable receipt identity mismatch')
    integrity_path = f'{PREFIX}/integrity/{kind}/{when:%Y/%m/%d}/integrity_{stamp}.json'
    destination = f'{PREFIX}/{kind}/{when:%Y/%m/%d}/{kind}_{stamp}.json.gz'
    if receipt.get('payload_destination') != destination:
        raise ObjectError('Secondary payload destination mismatch')
    proofs = receipt.get('readback')
    if not isinstance(proofs, list) or len({p.get('path') for p in proofs}) != len(proofs):
        raise ObjectError('Secondary readback proofs invalid')
    bodies = {}
    for proof in proofs:
        name = file_path(proof['path'])
        if not name.startswith((f'{PREFIX}/', 'reports/collector-health/')):
            raise ObjectError('Secondary readback path outside collector scope')
        body = runtime.read(name)
        if (proof.get('exact_bytes_match') is not True or proof.get('bytes') != len(body) or
                proof.get('sha256') != hashlib.sha256(body).hexdigest()):
            raise ObjectError('Secondary exact readback mismatch')
        bodies[name] = body
    if integrity_path not in bodies or destination not in bodies:
        raise ObjectError('Secondary original integrity/payload proofs absent')
    archived = json.loads(bodies[integrity_path])
    for key, value in report.items():
        if archived.get(key) != value:
            raise ObjectError('Secondary local/archived integrity mismatch')
    archived_invocation = archived.get('public_workflow_invocation', {})
    if any(not invocation.get(key) or archived_invocation.get(key) != invocation[key]
           for key in INVOCATION_FIELDS):
        raise ObjectError('Secondary children do not belong to this invocation')
    raw = gzip.decompress(bodies[destination])
    digest = hashlib.sha256(raw).hexdigest()
    payload_proof = next(p for p in proofs if p['path'] == destination)
    if (len(raw) != report.get('input_payload_bytes') or digest != report.get('input_payload_sha256') or
            digest != receipt.get('payload_source_sha256') or digest != receipt.get('audit_input_payload_sha256') or
            digest != payload_proof.get('decompressed_sha256')):
        raise ObjectError('Secondary audited source bytes mismatch')
    return {key: receipt.get(key) for key in ('kind', 'stamp', 'source_generated_at_utc', 'verified_at_utc',
            'verified_data_commit_sha', 'payload_source_sha256', 'audit_input_payload_sha256',
            'payload_destination', 'readback_verified')} | {'receipt_path': path}


def publish_secondary(runtime, reports, invocation, *, now=None):
    if set(reports) != set(CHILDREN):
        raise ObjectError('Exactly two secondary reports required')
    runtime.open()
    children = {kind: bound_child(runtime, kind, reports[kind], invocation) for kind in CHILDREN}
    when = max(parse_time(child['source_generated_at_utc']) for child in children.values())
    batch = {'schema_version': 1, 'method_version': METHOD, 'kind': 'secondary', 'complete': True,
             'source_generated_at_utc': when.isoformat(),
             'published_at_utc': (now or datetime.now(timezone.utc)).isoformat(),
             'public_workflow_run_id': invocation['run_id'], 'public_workflow_run_attempt': invocation['run_attempt'],
             'public_workflow_invocation': invocation, 'children': children}
    immutable = immutable_receipt_path('secondary', batch['source_generated_at_utc'])
    latest = f'{PREFIX}/transfer_receipts/secondary/latest.json'
    raw = canonical(batch)

    def merge(current, incoming):
        # CAS retries revalidate both dependencies against the new root.
        for kind in CHILDREN:
            if bound_child(current, kind, reports[kind], invocation) != children[kind]:
                raise ObjectError('Secondary immutable dependency changed')
        existing = current.read(immutable, required=False)
        body = raw
        if existing is not None:
            original = json.loads(existing)
            if any(original.get(key) != value for key, value in batch.items() if key != 'published_at_utc'):
                raise ObjectError('Conflicting immutable secondary batch')
            body = existing
        changes = {immutable: body}
        old = current.read(latest, required=False)
        if old is None or parse_time(json.loads(old)['source_generated_at_utc']) <= when:
            changes[latest] = body
        return {path: value for path, value in changes.items() if current.read(path, required=False) != value}

    def refs(snapshot):
        old = runtime.read(latest, required=False)
        if old is not None and parse_time(json.loads(old)['source_generated_at_utc']) > when:
            return {}
        return {'config/cloud_refs/collector_secondary_v1.json': {
            'schema_version': 1, 'artifact_version': 'cloud-collector-ref-v1', 'kind': 'secondary',
            'snapshot': snapshot.json(), 'generated_at_utc': when.isoformat(),
            'readback_verified': True, 'bundle_ready': True}}

    metadata = {'channel': 'collector-secondary', 'producer_repository': invocation['repository'],
                'producer_commit': invocation['sha'], 'original_generated_at_utc': when.isoformat()}
    result = runtime.publish({immutable: raw}, metadata=metadata, merge=merge, extra_refs=refs)
    return dict(result, kind='secondary', method_version=METHOD, batch_receipt_path=immutable,
                source_generated_at_utc=when.isoformat())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wunstorf-integrity', required=True)
    parser.add_argument('--etnw-integrity', required=True)
    args = parser.parse_args()
    reports = {kind: json.loads(Path(getattr(args, kind + '_integrity')).read_text()) for kind in CHILDREN}
    from mardorf_collector.runtime.cloud_environment import environment
    print(json.dumps(publish_secondary(load_runtime(Path.cwd(),environ=environment(Path.cwd())), reports, workflow_provenance()), sort_keys=True))


if __name__ == '__main__':
    main()
