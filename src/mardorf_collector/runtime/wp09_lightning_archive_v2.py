"""Keep the live v1 context; collect native history with explicit missing slots.

The four-hour overlap tolerates ordinary scheduler jitter. Its existence is
archive availability, never proof of lightning-detector negative coverage.
"""
import hashlib
import json
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import requests
from .wp09_lightning_context_v1 import capture as live_capture, ROOT


def capture(*, root=ROOT, get=requests.get, now=None):
    now = now or datetime.now(timezone.utc)
    receipt, originals = live_capture(root=root, get=get, now=now)
    path = Path(root)/'config/wp09_lightning_archive_v2.json'
    policy = json.loads(path.read_bytes())
    started = time.monotonic()
    deadline = started + policy['maximum_history_seconds']
    index = next((r for r in receipt['records'] if r['quantity']=='index'), None)
    live = next((r for r in receipt['records'] if r['quantity']=='konrad3d'), None)
    end = int(now.timestamp())//300*300
    slots = list(range(end-policy['lookback_seconds']+300, end+1, 300))
    names = {}
    if index and index['status']=='valid':
        for name in set(re.findall(rb'KONRAD3D_(\d{8}T\d{6})\.xml', originals['index'])):
            try:
                epoch = int(datetime.strptime(name.decode(), '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc).timestamp())
                if epoch in slots: names[epoch] = name.decode()
            except ValueError: pass
    complete = {}
    if live and live['status']=='valid':
        epoch = int(datetime.fromisoformat(live['observed_at_utc']).timestamp())
        if epoch in slots: complete[epoch] = live['quantity']
    missing = {epoch:'not_listed_at_capture' for epoch in slots if epoch not in names and epoch not in complete}
    pending = sorted((e for e in names if e not in complete), reverse=True)
    for epoch in pending[policy['maximum_history_requests']:]: missing[epoch]='request_count_budget'
    pending = pending[:policy['maximum_history_requests']]
    base = json.loads((Path(root)/'config/wp09_lightning_context_v1.json').read_bytes())['index_url']

    def fetch(epoch):
        name = names[epoch]; quantity = 'konrad3d_history_'+name
        url = base+'KONRAD3D_'+name+'.xml'
        start = datetime.now(timezone.utc).isoformat(); raw = b''; status = 0; reason = None
        try:
            if time.monotonic()>=deadline: raise ValueError('history_time_budget')
            with get(url, timeout=(policy['request_connect_seconds'], policy['request_read_seconds']),
                     stream=True, allow_redirects=False) as response:
                status = response.status_code
                for chunk in response.iter_content(65536):
                    if time.monotonic()>deadline: raise ValueError('history_time_budget')
                    if len(raw)+len(chunk)>policy['maximum_original_bytes']: raise ValueError('history_original_budget')
                    raw += chunk
                response.raise_for_status()
                if status!=200: raise ValueError('history_HTTP_not_200')
            if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper(): raise ValueError('history_XML_declaration')
            tree=ET.fromstring(raw)
            observed=datetime.fromisoformat(tree.findtext('head/metadata/reference_time').replace('Z','+00:00'))
            if tree.tag!='konrad3d' or observed.tzinfo is None or observed.timestamp()!=epoch:
                raise ValueError('history_native_clock_identity')
        except (requests.RequestException, ValueError, TypeError, AttributeError, ET.ParseError) as exc:
            reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        captured = datetime.now(timezone.utc).isoformat()
        record = dict(quantity=quantity, url=url, started_utc=start, captured_utc=captured,
            available_utc=captured, observed_at_utc=datetime.fromtimestamp(epoch,timezone.utc).isoformat(),
            http_status=status, status='valid' if reason is None else 'invalid', reason=reason,
            source_bytes=len(raw), source_sha256=hashlib.sha256(raw).hexdigest())
        return epoch, record, raw

    retained=0; attempted=0
    # Submit one worker-sized batch at a time: Executor.map alone is eager.
    # Consequently completed bodies cannot accumulate behind a slow first job.
    with ThreadPoolExecutor(max_workers=policy['workers']) as pool:
        offset=0
        while offset<len(pending):
            capacity=(policy['maximum_history_bytes']-retained)//policy['maximum_original_bytes']
            width=min(policy['workers'],capacity)
            if time.monotonic()>=deadline or width<1:
                for epoch in pending[offset:]:missing[epoch]='history_time_or_aggregate_budget'
                break
            batch=pending[offset:offset+width];offset+=len(batch)
            for epoch, record, raw in pool.map(fetch, batch):
                attempted+=1
                retained+=len(raw);receipt['records'].append(record);originals[record['quantity']]=raw
                if record['status']=='valid':complete[epoch]=record['quantity']
                else:missing[epoch]=record['reason']
    receipt['archive_v2']=dict(artifact_version=policy['artifact_version'],
        policy_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        requested_at_utc=now.isoformat(), start_epoch=slots[0], end_epoch=slots[-1],
        cadence_seconds=policy['cadence_seconds'], expected_slots=len(slots),
        originals_by_epoch={str(e):q for e,q in sorted(complete.items())},
        missing_by_epoch={str(e):r for e,r in sorted(missing.items())},
        planned_history_requests=len(pending), attempted_history_slots=attempted,
        history_bytes=retained,
        elapsed_history_seconds=time.monotonic()-started, negative_detector_coverage=False,
        all_original_fields_retained=True, retrospective_capture_not_as_issued=True)
    receipt['captured_utc']=datetime.now(timezone.utc).isoformat()
    return receipt, originals
