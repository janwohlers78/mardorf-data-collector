"""Pinned public study archive -> immutable B2 originals/yearly Parquet.

No study scripts execute. Indices are standardised projections, not probabilities.
The EOF zero sentinel outside its training scope is null, never 'no regime'.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import re
import time
from urllib.request import urlopen
import zipfile

from ..paths import REPOSITORY_ROOT
from ..storage.objects import ObjectRef

CONTRACT = 'config/dev03_wp17_regime_archive_contract_v1.json'
PREFIX = 'weather/regimes/v1'
UTC = timezone.utc


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def stamp(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def utc(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value):
        raise ValueError('Exact UTC required')
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def bound(body, spec):
    if len(body) != spec['bytes'] or sha(body) != spec['sha256']:
        raise ValueError('Study original size/hash mismatch')
    return body


def parse_table(body, *, columns):
    rows = []
    for line in body.decode('utf-8').splitlines():
        cells = line.split()
        if not cells or not re.fullmatch(r'-?\d+', cells[0]):
            continue
        if len(cells) != columns or not re.fullmatch(r'\d{8}_\d{2}', cells[1]):
            raise ValueError('Invalid study data row')
        date = datetime.strptime(cells[1], '%Y%m%d_%H').replace(tzinfo=UTC)
        if (date - datetime(1979, 1, 1, tzinfo=UTC)).total_seconds() != int(cells[0]) * 3600:
            raise ValueError('Contradictory study timestamps')
        values = list(map(float, cells[2:]))
        if not all(math.isfinite(v) for v in values):
            raise ValueError('Nonfinite regime data')
        rows.append((date, values))
    if not rows:
        raise ValueError('Empty study table')
    return rows


def parse_tables(classes, indices, *, start_utc, end_utc):
    left, right = parse_table(classes, columns=5), parse_table(indices, columns=9)
    start, end = utc(start_utc), utc(end_utc)
    if start >= end or (end-start).total_seconds() % 10800:
        raise ValueError('Invalid study period')
    expected = int((end-start).total_seconds()/10800)
    if len(left) != expected or len(right) != expected:
        raise ValueError('Incomplete study period')
    result = []
    for i, ((date, cls), (other, proj)) in enumerate(zip(left, right)):
        if date != start + timedelta(hours=3*i) or other != date:
            raise ValueError('Duplicate, disordered or noncontiguous regime series')
        if any(v != int(v) or not 0 <= v <= 7 for v in cls):
            raise ValueError('Invalid regime class')
        valid_eof = datetime(1979,1,11,tzinfo=UTC) <= date < datetime(2020,1,1,tzinfo=UTC) and date.hour % 6 == 0
        if (valid_eof and cls[0] == 0) or (not valid_eof and cls[0] != 0):
            raise ValueError('EOF attribution scope mismatch')
        if not 1 <= cls[1] <= 7:
            raise ValueError('Invalid max-index attribution')
        row = dict(valid_time_utc=date, eof_class=int(cls[0]) if valid_eof else None,
                   max_index_class=int(cls[1]), lifecycle_class=int(cls[2]))
        row.update({name:value for name,value in zip(('AT','ZO','ScTr','AR','EuBL','ScBL','GL'),proj)})
        result.append(row)
    return result


def schema():
    import pyarrow as pa
    return pa.schema([('valid_time_utc',pa.timestamp('us',tz='UTC')),('eof_class',pa.int8()),
                      ('max_index_class',pa.int8()),('lifecycle_class',pa.int8())]
                     + [(name,pa.float64()) for name in ('AT','ZO','ScTr','AR','EuBL','ScBL','GL')],
                     metadata={b'role':b'retrospective_diagnostic_only',b'indices':b'not_probabilities'})


def parquet(rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    output=io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows,schema=schema()),output,compression='zstd',version='2.6')
    return output.getvalue()


def read_partition(backend, reference, *, year, expected_rows):
    import pyarrow.parquet as pq
    ref=ObjectRef.parse(reference)
    if (not ref.key.startswith(f'{PREFIX}/partitions/year/{year}/')
            or not ref.key.endswith('/'+ref.sha256) or ref.bytes > 2*1024**2):
        raise ValueError('Regime partition reference scope')
    body=backend.get_bytes(ref)
    bound(body,ref.json())
    table=pq.read_table(io.BytesIO(body))
    if not table.schema.equals(schema(),check_metadata=True) or len(table)!=expected_rows:
        raise ValueError('Regime physical schema/count mismatch')
    rows=table.to_pylist()
    if any(r['valid_time_utc'].year!=year for r in rows):
        raise ValueError('Regime partition year mismatch')
    return rows


def prepare(backend, archive, *, root=REPOSITORY_ROOT):
    root=Path(root);raw=(root/CONTRACT).read_bytes();p=json.loads(raw)
    if p['artifact_version']!='wp17-public-regime-archive-contract-v1' or p['role']!='retrospective_diagnostic_only':
        raise ValueError('Explicit diagnostic source contract required')
    bound(archive,p['archive']);z=zipfile.ZipFile(io.BytesIO(archive));members=z.infolist()
    if (len(members)>128 or len({m.filename for m in members})!=len(members)
            or sum(m.file_size for m in members)>p['max_zip_expanded_bytes']
            or any(m.flag_bits&1 or '..' in Path(m.filename).parts or m.filename.startswith('/') for m in members)):
        raise ValueError('Study ZIP member budget/path mismatch')
    blobs={name:bound(z.read(p[name]['member']),p[name]) for name in ('readme','classes','indices')}
    rows=parse_tables(blobs['classes'],blobs['indices'],**{k:p['period'][k] for k in ('start_utc','end_utc')})
    output_bytes=0
    def put(folder,body):
        nonlocal output_bytes
        output_bytes+=len(body)
        if output_bytes>p['max_output_bytes']:
            raise ValueError('Regime output budget exceeded')
        return backend.put_bytes(PREFIX+'/'+folder+'/'+sha(body),body).json()
    original=put('originals',archive);readme=put('originals',blobs['readme'])
    partitions={}
    for year in sorted({r['valid_time_utc'].year for r in rows}):
        selected=[r for r in rows if r['valid_time_utc'].year==year]
        reference=put(f'partitions/year/{year}',parquet(selected))
        if read_partition(backend,reference,year=year,expected_rows=len(selected))!=selected:
            raise ValueError('Regime cold readback changed values')
        partitions[str(year)]=dict(reference=reference,rows=len(selected),start_utc=stamp(selected[0]['valid_time_utc']),
                                   end_utc=stamp(selected[-1]['valid_time_utc']+timedelta(hours=3)))
    document=dict(schema_version=1,artifact_version='wp17-regime-archive-v1',source=p,
        contract_sha256=sha(raw),processor_sha256=sha(Path(__file__).read_bytes()),
        original=original,readme=readme,partitions=partitions,rows=len(rows),format='parquet-arrow25-zstd',
        forecast_feature_allowed=False,scientific_gate='OPEN')
    reference=put('manifests',canonical(document))
    if backend.get_bytes(ObjectRef.parse(reference))!=canonical(document):
        raise ValueError('Regime manifest readback failed')
    return reference,document,output_bytes


def main():
    from ..runtime.cloud_environment import environment
    from ..storage.runtime import load_runtime
    started=time.monotonic();root=REPOSITORY_ROOT;p=json.loads((root/CONTRACT).read_bytes())
    with urlopen(p['archive']['url'],timeout=60) as response:
        archive=response.read(p['archive']['bytes']+1)
    cloud=load_runtime(root,environ=environment(root));cloud.open()
    ref,doc,output_bytes=prepare(cloud.backend,archive,root=root)
    if time.monotonic()-started>p['max_runtime_seconds']:
        raise ValueError('Regime preparation runtime budget exceeded')
    index=canonical(dict(schema_version=1,artifact_version='wp17-regime-index-v1',manifest=ref,
                         source_id=p['source_id'],role=p['role']))
    result=cloud.publish({p['index_path']:index},metadata={'channel':'wp17-regime-archive-v1'},
        merge=lambda current,changes:{path:body for path,body in changes.items() if current.read(path,required=False)!=body})
    report=dict(status='PASS',manifest=ref,rows=doc['rows'],years=len(doc['partitions']),
        output_bytes=output_bytes,provider_requests=1,normalizations=1,weather_git_bytes_written=0,
        role=p['role'],forecast_feature_allowed=False,publication=result,
        metrics=cloud.backend.metrics,elapsed_seconds=round(time.monotonic()-started,3))
    destination=root/'work/wp17_regime_archive.json';destination.parent.mkdir(exist_ok=True)
    destination.write_bytes(canonical(report));print(json.dumps(report,sort_keys=True))


if __name__=='__main__':
    main()
