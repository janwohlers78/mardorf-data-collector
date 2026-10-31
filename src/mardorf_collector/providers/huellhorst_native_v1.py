"""Hourly, append-only native GEFS/GFS input acquisition for Hüllhorst Rathaus.

The caller supplies the existing lossless writer and OVH backend. This adapter
does not install a daemon, create credentials, alter H1/R1, fit, or publish to B2.
Heavy optional modules are imported only inside the native capture operation.
"""
from datetime import datetime, timezone
import fcntl
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import tempfile

ROOT = Path(__file__).resolve().parents[3]
CONFIG = 'config/huellhorst_native_v1.json'
PREFIX = 'weather/archive/janwohlers78/mardorf-kitevorhersage/huellhorst-native/v1'


def canonical(value):
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(',', ':')).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def binding(root=ROOT):
    return json.loads((Path(root)/CONFIG).read_bytes())


def output_directory(directory, root=ROOT):
    out = Path(directory)
    existing = out
    while not existing.exists():
        existing = existing.parent
    if shutil.disk_usage(existing).free < binding(root)['reserve_bytes'] + 16*1024**2:
        raise ValueError('native_output_reserve_admission')
    out.mkdir(parents=True, exist_ok=True)
    return out


def due(last_attempt_utc, current, *, root=ROOT):
    if current.tzinfo is None:
        raise ValueError('native_current_clock_timezone_required')
    if last_attempt_utc is None:
        return True
    previous = datetime.fromisoformat(last_attempt_utc.replace('Z', '+00:00'))
    if previous.tzinfo is None or previous > current:
        raise ValueError('native_previous_clock_invalid')
    return (current-previous).total_seconds() >= binding(root)['poll_interval_seconds']


def latest_initialization(values, current):
    import numpy as np
    if current.tzinfo is None:
        raise ValueError('native_current_clock_timezone_required')
    a = np.asarray(values).astype('datetime64[ns]')
    if a.ndim != 1 or np.isnat(a).any() or len(np.unique(a)) != len(a):
        raise ValueError('native_initialization_axis_invalid')
    now = np.datetime64(current.astimezone(timezone.utc).replace(tzinfo=None), 'ns')
    past = a[a <= now]
    if not len(past):
        raise ValueError('native_no_nonfuture_initialization')
    return past.max()


def selection(ds, source, current, *, root=ROOT):
    import numpy as np
    b = binding(root)
    latest = latest_initialization(ds.init_time.values, current)
    lead = np.asarray(ds.lead_time.values)/np.timedelta64(1, 'h')
    if (not np.isfinite(lead).all() or len(np.unique(lead)) != len(lead)
            or (np.diff(lead) <= 0).any()):
        raise ValueError('native_lead_axis_invalid')
    if source['members'] > 1:
        if 'ensemble_member' not in ds.coords or not np.array_equal(ds.ensemble_member.values, np.arange(source['members'])):
            raise ValueError('native_full_member_axis_required')
    elif 'ensemble_member' in ds.coords and len(ds.ensemble_member) != 1:
        raise ValueError('native_deterministic_member_axis_invalid')
    margin = b['native_grid_margin_degrees']
    indexes = {}
    for name, point in [('latitude', b['latitude']), ('longitude', b['longitude'])]:
        axis = np.asarray(ds[name].values)
        if axis.ndim != 1 or not np.isfinite(axis).all() or len(np.unique(axis)) != len(axis):
            raise ValueError('native_grid_axis_invalid')
        if point < axis.min() or point > axis.max():
            raise ValueError('native_target_outside_grid')
        ix = np.flatnonzero(abs(axis-point) <= margin)
        if len(ix) < 2 or not axis[ix].min() <= point <= axis[ix].max():
            raise ValueError('native_target_stencil_incomplete')
        indexes[name] = ix
    indexes['lead_time'] = np.flatnonzero((lead >= 0) & (lead <= b['forecast_horizon_hours']))
    if not len(indexes['lead_time']):
        raise ValueError('native_no_horizon')
    # Keep all native data variables and all members/vertical dimensions.
    # There is no target interpolation, pressure conversion or hourly resample.
    return ds.sel(init_time=[latest]).isel(**indexes), latest


def opened_native(source):
    import icechunk
    import xarray as xr
    b = binding()
    if source not in b['sources']:
        raise ValueError('native_source_not_registered')
    repository = icechunk.Repository.open(icechunk.http_storage(source['url']))
    session = repository.readonly_session(branch='main')
    ds = xr.open_zarr(session.store, chunks=None, decode_timedelta=True)
    # Keep the session alive as long as the dataset's lazy references exist.
    return ds, str(session.snapshot_id), session


def capture(source, directory, writer, *, current=None, root=ROOT, opener=opened_native, runtime_binding=None):
    current = current or datetime.now(timezone.utc)
    b = binding(root)
    if source not in b['sources']:
        raise ValueError('native_source_not_registered')
    writer_file = Path(inspect.getsourcefile(writer))
    writer_binding = dict(file=str(writer_file), sha256=file_digest(writer_file),
                          function_source_sha256=digest(inspect.getsource(writer).encode()))
    producer = digest(Path(__file__).read_bytes())
    out = output_directory(directory, root)
    began = datetime.now(timezone.utc).isoformat()
    ds, snapshot, session = opener(source)
    try:
        if not isinstance(snapshot, str) or not snapshot.strip():
            raise ValueError('native_snapshot_missing')
        selected, initialization = selection(ds, source, current, root=root)
        # Writer may hold both a materialized dataset and its lossless file.
        estimated_peak = 2*int(selected.nbytes) + 64*1024**2
        if shutil.disk_usage(out).free - estimated_peak < b['reserve_bytes']:
            raise ValueError('native_full_field_peak_exceeds_reserve')
        native_attrs = dict(selected.attrs)
        identity = dict(site_id=b['site_id'], geometry_id=b['geometry_id'],
            latitude=b['latitude'], longitude=b['longitude'], source=source,
            snapshot_id=snapshot, initialization=str(initialization.astype('datetime64[s]')),
            variables=sorted(selected.data_vars),
            lead_hours=(selected.lead_time.values/__import__('numpy').timedelta64(1, 'h')).tolist(),
            source_binding_sha256=digest(canonical(b)), producer_sha256=producer,
            writer_binding=writer_binding, runtime_binding=runtime_binding)
        key = digest(canonical(identity))
        path = out/(key+'.nc')
        receipt_path = out/(key+'.json')
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_bytes())
            if (receipt['identity'] != identity or not path.exists()
                    or path.stat().st_size != receipt['artifact']['bytes']
                    or file_digest(path) != receipt['artifact']['sha256']):
                raise ValueError('native_existing_identity_or_bytes_changed')
            return receipt, path
        if path.exists():
            raise ValueError('native_unfinished_original_requires_reconciliation')
        selected.attrs = dict(native_attrs, mardorf_icechunk_snapshot=snapshot,
            mardorf_capture_utc=began,
            mardorf_generation_qualification='archive snapshot only; operational generation unqualified')
        fields = {name: dict(units=selected[name].attrs.get('units'),
                            step_type=selected[name].attrs.get('step_type'),
                            dimensions=list(selected[name].dims), dtype=str(selected[name].dtype))
                  for name in selected.data_vars}
        writer(selected, path)
        receipt = dict(artifact_version='huellhorst-native-capture-v1', identity=identity,
            artifact=dict(bytes=path.stat().st_size, sha256=file_digest(path)),
            started_utc=began, retrieved_utc=datetime.now(timezone.utc).isoformat(),
            observed_asof_utc=current.astimezone(timezone.utc).isoformat(),
            source_publication_utc=None, source_issue_utc=None,
            initialisation_utc=identity['initialization']+'Z',
            source_native_attributes=native_attrs, fields=fields,
            time_resolution='unchanged native lead axis; see identity.lead_hours',
            measurement_operator='unchanged native variable attributes; no station-pressure or hourly-mean conversion',
            target_ground_height_m=b['ground_height_m'], target_ground_vertical_crs=b['ground_vertical_crs'],
            barometer_height_m=None, raw_field_exclusions=[],
            operational_generation_qualified=False, scientific_release=False,
            frozen_R1_geometry_replaced=False, production_changed=False,
            producer_sha256=producer)
        receipt.update(writer_binding=writer_binding,
                       runtime_binding=runtime_binding, runtime_bound=runtime_binding is not None)
        initialized = datetime.fromisoformat(identity['initialization']).replace(tzinfo=timezone.utc)
        age = (current.astimezone(timezone.utc)-initialized).total_seconds()/3600
        receipt.update(source_age_hours=age,
            asof_within_native_validity=age <= max(identity['lead_hours']),
            status='CAPTURED_CURRENT_HORIZON' if age <= max(identity['lead_hours']) else 'CAPTURED_STALE_NO_CURRENT_VALIDITY')
        with receipt_path.open('xb') as f:
            f.write(canonical(receipt));f.flush();os.fsync(f.fileno())
        return receipt, path
    finally:
        ds.close()


def file_digest(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def publish(receipt, path, backend):
    """Existing OVH backend protocol only; full GET/readback precedes commit."""
    raw = Path(path).read_bytes()
    if len(raw) != receipt['artifact']['bytes'] or digest(raw) != receipt['artifact']['sha256']:
        raise ValueError('native_original_changed_before_publication')
    original = backend.put_bytes(PREFIX+'/originals/'+digest(raw)+'.nc', raw)
    if backend.get_bytes(original) != raw:
        raise ValueError('native_original_full_readback_failed')
    saved = dict(receipt, original=original.json(), full_original_readback_verified=True)
    body = canonical(saved)
    reference = backend.put_bytes(PREFIX+'/receipts/'+digest(body)+'.json', body)
    if backend.get_bytes(reference) != body:
        raise ValueError('native_receipt_full_readback_failed')
    return saved, reference


def freshness(identity, current):
    initialized = datetime.fromisoformat(identity['initialization']).replace(tzinfo=timezone.utc)
    age = (current.astimezone(timezone.utc)-initialized).total_seconds()/3600
    return dict(current_source_age_hours=age,
                current_asof_within_native_validity=0 <= age <= max(identity['lead_hours']))


def tick(directory, writer, backend, *, current=None, root=ROOT, opener=opened_native, runtime_binding=None):
    """Called by an existing scheduler; one source failure does not hide another."""
    current = current or datetime.now(timezone.utc)
    out = output_directory(directory, root)
    with (out/'collection.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = out/'last-attempt.json'
        previous = json.loads(state.read_bytes()) if state.exists() else None
        if not due(previous.get('attempted_utc') if previous else None, current, root=root):
            return {'status': 'NOT_DUE'}
        rows = []
        for source in binding(root)['sources']:
            try:
                receipt, path = capture(source, out/source['name'], writer,
                    current=current, root=root, opener=opener, runtime_binding=runtime_binding)
                committed = out/(path.stem+'-committed.json')
                if committed.exists():
                    rows.append(dict(source=source['name'], status='ALREADY_COMMITTED', identity=receipt['identity'],
                                     **freshness(receipt['identity'],current)))
                    continue
                saved, reference = publish(receipt, path, backend)
                with committed.open('xb') as f:
                    f.write(canonical(dict(status='COMMITTED', receipt=reference.json(), identity=receipt['identity'])));f.flush();os.fsync(f.fileno())
                rows.append(dict(source=source['name'], status='COMMITTED', receipt=reference.json(), identity=receipt['identity'],
                                 **freshness(receipt['identity'],current)))
            except Exception as exc:
                rows.append(dict(source=source['name'], status='FAILED', reason=type(exc).__name__+':'+str(exc)))
        result = dict(artifact_version='huellhorst-native-tick-v1', attempted_utc=current.isoformat(),
            status='COMPLETE' if all(r['status'] in ['COMMITTED','ALREADY_COMMITTED'] for r in rows) else 'PARTIAL',
            sources=rows, scientific_release=False, production_changed=False)
        stamp = current.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        with (out/(stamp+'-attempt.json')).open('xb') as f:
            f.write(canonical(result));f.flush();os.fsync(f.fileno())
        fd, name = tempfile.mkstemp(dir=out, prefix='.last-attempt-')
        with os.fdopen(fd,'wb') as f:
            f.write(canonical(result));f.flush();os.fsync(f.fileno())
        os.replace(name, state)
        return result
