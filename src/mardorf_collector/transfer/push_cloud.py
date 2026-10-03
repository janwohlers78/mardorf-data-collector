"""Successor collector publication: exact audited bytes, B2 archive, one CAS head."""
import argparse
from datetime import datetime,timezone
import gzip
import hashlib
import json
import os
from pathlib import Path

from mardorf_collector.storage.archive import canonical
from mardorf_collector.storage.objects import ObjectError
from mardorf_collector.storage.runtime import load_runtime
from .push_private import parse_time,workflow_provenance


def publish_collector(runtime,*,kind,payload,integrity,integrity_md,invocation):
    if kind not in ('models','svg','skm','wunstorf','etnw'):raise ObjectError('Unsupported collector kind')
    if integrity.get('kind')!=kind:raise ObjectError('Collector kind mismatch')
    parsed=json.loads(payload) if isinstance(payload,bytes) else payload
    gate=parsed.get('provider_cycle_gate') if isinstance(parsed,dict) else None
    if isinstance(gate,dict) and gate.get('any_work') is False and gate.get('delta_prediction')=='zero':
        return {'status':'suppressed_noop','weather_git_bytes_written':0}
    when=parse_time(integrity['generated_at_utc']);stamp=when.strftime('%Y%m%dT%H%M%S')+f'{when.microsecond:06d}Z'
    day=when.strftime('%Y/%m/%d')
    prefix='data/inbox/public_collector'
    report=dict(integrity)
    report['public_workflow_invocation']=invocation
    report['private_transfer_protocol']={'method_version':'cloud-transfer-readback-v1',
        'publish_gate':'All immutable B2 packs and source ranges verified before non-forced metadata CAS',
        'clock_policy':'Original provider clocks retained; cloud storage creation time is not source availability'}
    changes={}
    source=payload if isinstance(payload,bytes) else canonical(payload) if payload is not None else None
    if source is not None:
        if len(source)!=integrity.get('input_payload_bytes') or hashlib.sha256(source).hexdigest()!=integrity.get('input_payload_sha256'):
            raise ObjectError('Audited source bytes differ')
        packed=gzip.compress(source,compresslevel=9,mtime=0)
        destination=f'{prefix}/{kind}/{day}/{kind}_{stamp}.json.gz'
        changes[destination]=packed
        report['private_payload']={'destination':destination,'source_sha256':hashlib.sha256(source).hexdigest(),
                                  'source_bytes':len(source),'compressed_bytes':len(packed)}
    elif integrity.get('input_file_present') or integrity.get('input_payload_sha256'):
        raise ObjectError('Audited payload missing')
    integrity_path=f'{prefix}/integrity/{kind}/{day}/integrity_{stamp}.json'
    health_path=f'reports/collector-health/{kind}/{day}/integrity_{stamp}.md'
    changes[integrity_path]=canonical(report);changes[health_path]=integrity_md.encode()
    receipt_path=f'{prefix}/transfer_receipts/{kind}/{day}/receipt_{stamp}.json'
    latest=f'{prefix}/integrity/{kind}/latest.json'
    success=f'{prefix}/integrity/{kind}/latest_success.json'
    ready=(report.get('bundle_ready_for_private_revalidation') is True and
           report.get('status') in ('PASS','PASS_WITH_WARNINGS') and
           type(report.get('error_count')) is int and report['error_count']==0 and source is not None)
    if report.get('bundle_ready_for_private_revalidation') is True and not ready:
        raise ObjectError('Collector readiness conflicts with integrity or missing payload')
    metadata={'channel':'collector-'+kind,'producer_repository':invocation.get('repository') or runtime.config['public_repository'],
              'original_generated_at_utc':when.isoformat(),
              'payload_sha256':integrity.get('input_payload_sha256') or hashlib.sha256(b'').hexdigest()}
    if invocation.get('sha'):metadata['producer_commit']=invocation['sha']
    # Build receipt in a private prepared root; nothing is visible without the final CAS.
    def merge(current,incoming):
        selected=dict(changes)
        for path,body in changes.items():
            old=current.read(path,required=False)
            if old is not None and old!=body:raise ObjectError('Immutable collector attempt collision')
        receipt={'method_version':'cloud-transfer-readback-v1','kind':kind,'stamp':stamp,'source_generated_at_utc':report['generated_at_utc'],
                 'verified_at_utc':datetime.now(timezone.utc).isoformat(),'readback_verified':True,
                 'verified_data_commit_sha':None,'cloud_protocol':'Exact B2 immutable packs + original SHA/Gitblob; Git holds metadata only',
                 'payload_destination':report.get('private_payload',{}).get('destination'),
                 'payload_source_sha256':integrity.get('input_payload_sha256'),'audit_input_payload_sha256':integrity.get('input_payload_sha256'),
                 'readback':[{'path':path,'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body),'exact_bytes_match':True,
                              **({'decompressed_sha256':hashlib.sha256(source).hexdigest()} if path.endswith('.json.gz') else {})}
                             for path,body in sorted(changes.items())]}
        existing_receipt=current.read(receipt_path,required=False)
        if existing_receipt is not None:
            existing=json.loads(existing_receipt)
            if (existing.get('payload_source_sha256')!=receipt['payload_source_sha256'] or
                existing.get('source_generated_at_utc')!=receipt['source_generated_at_utc'] or existing.get('readback_verified') is not True):
                raise ObjectError('Existing cloud receipt conflicts')
            receipt_raw=existing_receipt
        else:receipt_raw=canonical(receipt)
        selected[receipt_path]=receipt_raw
        old_latest=current.read(latest,required=False)
        if old_latest is None or parse_time(json.loads(old_latest)['generated_at_utc'])<=when:
            selected[latest]=canonical(report)
            selected[f'{prefix}/transfer_receipts/{kind}/latest.json']=receipt_raw
        old_success=current.read(success,required=False)
        if ready and (old_success is None or parse_time(json.loads(old_success)['generated_at_utc'])<=when):
            selected[success]=canonical(report)
        # Existing semantic paths are retained when this is an older attempt.
        return {path:body for path,body in selected.items() if current.read(path,required=False)!=body}
    def refs(snapshot):
        if not ready:return {}
        previous=runtime.read(success,required=False)
        if previous is not None and parse_time(json.loads(previous)['generated_at_utc'])>when:return {}
        return {f'config/cloud_refs/collector_{kind}_v1.json':
                {'schema_version':1,'artifact_version':'cloud-collector-ref-v1','kind':kind,'snapshot':snapshot.json(),
                 'generated_at_utc':when.isoformat(),'readback_verified':True,'bundle_ready':True}}
    result=runtime.publish(changes,metadata=metadata,merge=merge,extra_refs=refs)
    result.update(kind=kind,source_generated_at_utc=report['generated_at_utc'],payload_source_sha256=integrity.get('input_payload_sha256'),
                  readback_verified=True,cloud_transfer_method='cloud-transfer-readback-v1')
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind',required=True,choices=('models','svg','skm','wunstorf','etnw'))
    parser.add_argument('--file');parser.add_argument('--integrity-json',required=True);parser.add_argument('--integrity-md',required=True)
    parser.add_argument('--result-json');parser.add_argument('--receipt-copy')
    args=parser.parse_args()
    runtime=load_runtime(Path.cwd())
    payload=Path(args.file).read_bytes() if args.file and Path(args.file).exists() else None
    integrity=json.loads(Path(args.integrity_json).read_text())
    result=publish_collector(runtime,kind=args.kind,payload=payload,integrity=integrity,
                             integrity_md=Path(args.integrity_md).read_text(),invocation=workflow_provenance())
    text=json.dumps(result,sort_keys=True)
    if args.result_json:Path(args.result_json).write_text(text+'\n')
    if args.receipt_copy and result.get('source_generated_at_utc'):
        when=parse_time(result['source_generated_at_utc']);stamp=when.strftime('%Y%m%dT%H%M%S')+f'{when.microsecond:06d}Z'
        body=runtime.read(f'data/inbox/public_collector/transfer_receipts/{args.kind}/{when:%Y/%m/%d}/receipt_{stamp}.json')
        Path(args.receipt_copy).write_bytes(body)
    print(text)


if __name__=='__main__':main()
