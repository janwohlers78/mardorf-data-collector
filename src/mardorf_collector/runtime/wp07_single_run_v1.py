"""Companion original, using the existing WP06 clock and publication transaction."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import math
import requests
from .wp06_single_run_v1 import forecast_content_digest

ROOT = Path(__file__).resolve().parents[3]


def binding(root=ROOT):
    return json.loads((Path(root)/'config/wp07_single_run_v1.json').read_bytes())


def spec(run, root=ROOT):
    b = binding(root)
    return dict(endpoint=b['endpoint'], params=dict(b['params'], run=run))


def validate(body, request, root=ROOT):
    x = json.loads(body); b = binding(root); run = datetime.fromisoformat(request['params']['run']).replace(tzinfo=timezone.utc)
    if request != spec(request['params']['run'], root) or x.get('utc_offset_seconds') != 0:
        raise ValueError('WP07_request_scope_or_timezone')
    if any(type(x.get(k)) not in (int, float) or not math.isfinite(x[k]) for k in ('latitude','longitude','elevation')):
        raise ValueError('WP07_returned_coordinate')
    h = x['hourly']; units = x['hourly_units']; times = h['time']
    if units != b['units'] or len(times) != 73 or set(h) != set(units):
        raise ValueError('WP07_frozen_field_unit_schema')
    for i, t in enumerate(times):
        if (datetime.fromisoformat(t).replace(tzinfo=timezone.utc)-run).total_seconds() != i*3600:
            raise ValueError('WP07_original_run_axis')
    for name in b['params']['hourly'].split(','):
        if len(h[name]) != 73 or any(v is not None and (type(v) not in (int,float) or not math.isfinite(v)) for v in h[name]):
            raise ValueError('WP07_original_field_shape_or_value')
    return x


def capture(run, *, get=requests.get, root=ROOT, timeout=(8,35)):
    request = spec(run, root); b = binding(root); began = datetime.now(timezone.utc).isoformat()
    body = b''; status = 0; reason = None
    try:
        response = get(request['endpoint'], params=request['params'], timeout=timeout)
        body, status = response.content, response.status_code
        if len(body) > 1024**2: raise ValueError('WP07_response_budget')
        if status != 200: raise ValueError('WP07_HTTP_'+str(status))
        validate(body, request, root)
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
    captured = datetime.now(timezone.utc).isoformat()
    receipt = dict(artifact_version='wp07-single-run-receipt-v1', request=request, started_utc=began,
        captured_utc=captured, available_utc=captured, http_status=status, status='valid' if reason is None else 'invalid',
        reason=reason, source_sha256=hashlib.sha256(body).hexdigest(), source_bytes=len(body),forecast_content_sha256=forecast_content_digest(body),
        forecast_comparison_ignored_metadata=['generationtime_ms'],
        availability_evidence='actual_source_receipt', generation=b['generation'],
        generation_evidence=b['generation_evidence'], source_binding_sha256=hashlib.sha256((Path(root)/'config/wp07_single_run_v1.json').read_bytes()).hexdigest())
    return receipt, body
