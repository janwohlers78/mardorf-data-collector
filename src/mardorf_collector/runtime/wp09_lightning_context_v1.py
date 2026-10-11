"""Two bounded public requests; preserve native cell-linked lightning originals."""
import hashlib
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
import requests
from .wp08_labels_v1 import publication as publish_originals

ROOT = Path(__file__).resolve().parents[3]


def capture(*, root=ROOT, get=requests.get, now=None):
    path = Path(root)/'config/wp09_lightning_context_v1.json'
    binding = json.loads(path.read_bytes())
    now = now or datetime.now(timezone.utc)
    records, originals = [], {}
    deadline = time.monotonic()+30

    def request(quantity, url, maximum):
        raw, status, reason = b'', 0, None
        start = datetime.now(timezone.utc).isoformat()
        try:
            with get(url, timeout=(5, 12), stream=True, allow_redirects=False) as response:
                status = response.status_code
                response.raise_for_status()
                if status != 200:
                    raise ValueError('lightning_HTTP_not_200')
                for chunk in response.iter_content(65536):
                    if time.monotonic()>deadline:
                        raise ValueError('lightning_capture_time_budget')
                    if len(raw)+len(chunk) > maximum:
                        raise ValueError('lightning_original_budget')
                    raw += chunk
        except (requests.RequestException, ValueError) as exc:
            reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        captured = datetime.now(timezone.utc).isoformat()
        record = dict(quantity=quantity, url=url, started_utc=start, captured_utc=captured,
            available_utc=captured, http_status=status, reason=reason,
            status='valid' if reason is None else 'invalid', source_bytes=len(raw),
            source_sha256=hashlib.sha256(raw).hexdigest())
        records.append(record); originals[quantity] = raw
        return record, raw

    index, raw = request('index', binding['index_url'], binding['maximum_index_bytes'])
    if index['status'] == 'valid':
        names = sorted(set(re.findall(rb'KONRAD3D_(\d{8}T\d{6})\.xml', raw)))
        causal=[]
        for stamp in names:
            try:
                if datetime.strptime(stamp.decode(), '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)<=now:causal.append(stamp)
            except ValueError:pass
        names=causal
        if not names:
            index.update(status='invalid', reason='lightning_no_causal_native_filename')
        else:
            stamp = names[-1].decode()
            record, raw = request('konrad3d', binding['index_url']+'KONRAD3D_'+stamp+'.xml', binding['maximum_original_bytes'])
            if record['status'] == 'valid':
                try:
                    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
                        raise ValueError('lightning_XML_declaration')
                    tree = ET.fromstring(raw)
                    reference = tree.findtext('head/metadata/reference_time')
                    expected = datetime.strptime(stamp, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                    if tree.tag != 'konrad3d' or datetime.fromisoformat(reference.replace('Z', '+00:00')) != expected:
                        raise ValueError('lightning_native_clock_identity')
                    if len(tree.findall('cells/feature')) > binding['maximum_cells']:
                        raise ValueError('lightning_cell_budget')
                    record['observed_at_utc'] = expected.isoformat()
                except (ET.ParseError, ValueError, TypeError, AttributeError) as exc:
                    record.update(status='invalid', reason=str(exc) if isinstance(exc, ValueError) else 'lightning_invalid_XML')
    return dict(artifact_version='wp09-native-lightning-context-receipt-v1',
        captured_utc=datetime.now(timezone.utc).isoformat(),
        binding_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), records=records,
        provider_owner='public_collector', all_original_fields_retained=True), originals


def publication(receipt, originals, backend):
    return publish_originals(receipt, originals, backend, domain='wp09', collection='lightning_context')
