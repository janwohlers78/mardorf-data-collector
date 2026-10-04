"""Read-only validation of the original collector delivery and its receipt.

The caller supplies a snapshot-bound byte reader. Transport readiness is not
permission to replace a currently published model or ensemble cycle.
"""
import gzip
import hashlib
import io
import json
import re
from datetime import datetime, timezone


def verified_collector_delivery(read, kind, *, public_repository, max_bytes=64*1024**2):
    """Read one original immutable receipt-bound payload; never publish."""
    prefix='data/inbox/public_collector'
    if kind not in ('models','svg','wunstorf','etnw') or type(max_bytes) is not int or not 0<max_bytes<=64*1024**2:
        raise ValueError('Explicit bounded collector delivery required')
    report_raw=read(f'{prefix}/integrity/{kind}/latest_success.json')
    if len(report_raw)>256*1024:raise ValueError('Collector integrity budget exceeded')
    report=json.loads(report_raw)
    if not isinstance(report,dict):raise ValueError('Collector integrity object required')
    if (report.get('kind')!=kind or report.get('bundle_ready_for_private_revalidation') is not True or
            report.get('status') not in ('PASS','PASS_WITH_WARNINGS') or
            type(report.get('error_count')) is not int or report['error_count']!=0):
        raise ValueError('Collector replay source is not ready')
    current_invocation=report.get('public_workflow_invocation',{})
    if not isinstance(current_invocation,dict):raise ValueError('Collector invocation object required')
    if current_invocation.get('repository')!=public_repository:
        raise ValueError('Collector replay source repository differs')
    when=datetime.fromisoformat(report['generated_at_utc'].replace('Z','+00:00'))
    if when.tzinfo is None:raise ValueError('Collector replay source timezone absent')
    when=when.astimezone(timezone.utc);stamp=f'{when:%Y%m%dT%H%M%S}{when.microsecond:06d}Z'
    receipt=json.loads(read(f'{prefix}/transfer_receipts/{kind}/{when:%Y/%m/%d}/receipt_{stamp}.json'))
    destination=f'{prefix}/{kind}/{when:%Y/%m/%d}/{kind}_{stamp}.json.gz'
    private=report.get('private_payload',{})
    if not isinstance(private,dict) or not isinstance(receipt,dict):
        raise ValueError('Collector receipt and private payload objects required')
    if (receipt.get('readback_verified') is not True or receipt.get('payload_destination')!=destination or
            receipt.get('kind')!=kind or private.get('destination')!=destination or
            receipt.get('source_generated_at_utc')!=report['generated_at_utc'] or
            not isinstance(report.get('input_payload_sha256'),str) or
            not re.fullmatch('[0-9a-f]{64}',report['input_payload_sha256']) or
            any(value!=report['input_payload_sha256'] for value in (
                private.get('source_sha256'),receipt.get('payload_source_sha256'),
                receipt.get('audit_input_payload_sha256')))):
        raise ValueError('Collector replay original receipt differs')
    count=report['input_payload_bytes']
    if type(count)is not int or not 0<count<=64*1024**2 or count>max_bytes:
        raise ValueError('Collector replay payload budget exceeded')
    packed=read(destination)
    if len(packed)>64*1024**2+1024**2:raise ValueError('Collector compressed payload budget exceeded')
    integrity_path=f'{prefix}/integrity/{kind}/{when:%Y/%m/%d}/integrity_{stamp}.json'
    immutable_report=read(integrity_path)
    health_path=f'reports/collector-health/{kind}/{when:%Y/%m/%d}/integrity_{stamp}.md'
    health_raw=read(health_path)
    if len(immutable_report)>256*1024 or len(health_raw)>1024**2:
        raise ValueError('Collector supporting evidence budget exceeded')
    proofs=receipt.get('readback')
    if (not isinstance(proofs,list) or any(not isinstance(row,dict) or not isinstance(row.get('path'),str) for row in proofs) or
            len({row.get('path') for row in proofs})!=len(proofs) or immutable_report!=report_raw):
        raise ValueError('Collector receipt inventory or immutable integrity differs')
    proofs={row.get('path'):row for row in proofs}
    for path,body in ((destination,packed),(integrity_path,immutable_report),(health_path,health_raw)):
        proof=proofs.get(path,{})
        if (proof.get('exact_bytes_match') is not True or type(proof.get('bytes')) is not int or
                proof['bytes']!=len(body) or proof.get('sha256')!=hashlib.sha256(body).hexdigest()):
            raise ValueError('Collector receipt exact-byte proof differs')
    if proofs[destination].get('decompressed_sha256')!=report['input_payload_sha256']:
        raise ValueError('Collector receipt original payload digest differs')
    with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:payload=stream.read(count+1)
    if len(payload)!=count or hashlib.sha256(payload).hexdigest()!=report['input_payload_sha256']:
        raise ValueError('Collector replay original source bytes differ')
    if not isinstance(json.loads(payload),dict):raise ValueError('Collector payload object required')
    return payload,report,health_raw.decode(),current_invocation

