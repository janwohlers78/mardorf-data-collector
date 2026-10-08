"""Explicit manual ten-year program; original bounded acquisitions run serially."""
import argparse
import json
import os
from pathlib import Path

from .gfs_archive_research_v1 import run
from .svg_historical_research_v1 import canonical, publish, sha
from ..storage.configuration import b2_settings, resolve_b2_bucket
from ..storage.objects import B2Objects

ROOT = Path(__file__).resolve().parents[3]
REGISTRATION = ROOT/'config/research/wp06_gfs_ten_year_v2.json'
EXPECTED_SHA256 = 'e0f0fd635209d37b4c6faf0b9e53a0813e2d67809ee5ce6c9991f05070cce12f'
PREFIX = 'weather/archive/janwohlers78/mardorf-kitevorhersage/research/wp06-GFS-ten-year-v1'


def ranges(first_batch, path=REGISTRATION):
    if sha(path.read_bytes()) != EXPECTED_SHA256:
        raise ValueError('Registered ten-year requests changed')
    rows = json.loads(path.read_bytes())['ranges']
    if not 1 <= first_batch <= len(rows):
        raise ValueError('Explicit first remaining batch outside registered program')
    # Retained split roles affect ordering only, never expose truth to this route.
    return [dict(batch=i+1, **row) for i, row in enumerate(rows) if i+1 >= first_batch]


def execute(batch, output, backend):
    rows = ranges(1)
    if not 1 <= batch <= len(rows): raise ValueError('Unregistered batch')
    spec = rows[batch-1]; output = Path(output); output.mkdir(parents=True, exist_ok=True)
    result = run(spec['start'], spec['end_exclusive'], output, backend)
    index = publish(backend, PREFIX, output, 'index.json', canonical(result))
    forecasts = [r for r in result['records'] if r['request']['lead_hours'] is not None]
    summary = dict(batch=batch, range={k:spec[k] for k in ('start','end_exclusive','days','role')},
        code_commit=os.getenv('GITHUB_SHA'), index=index,
        program_registration_sha256=EXPECTED_SHA256,
        expected_forecast_files=spec['days']*9,
        captured_forecast_files=sum(r['status']=='captured' for r in forecasts),
        attempted_requests_complete=result['complete_requests'],
        archive_data_complete=all(r['status']=='captured' for r in forecasts),
        status_counts=result['status_counts'], scientific_release=False, native_admission=False)
    output.mkdir(parents=True, exist_ok=True)
    (output/'result-reference.json').write_bytes(canonical(summary))
    publish(backend, PREFIX, output, 'result-reference.json', canonical(summary))
    # Missing originals are auditable gaps, not invented forecasts or fit permission.
    if not result['complete_requests']:
        raise ValueError('Request budget incomplete; explicit narrower successor required')
    return summary


def main():
    parser = argparse.ArgumentParser(); parser.add_argument('command', choices=['plan','priority','batch'])
    parser.add_argument('--first-batch', type=int, default=int(os.getenv('GFS_FIRST_BATCH','3')))
    parser.add_argument('--batch', type=int, default=int(os.getenv('GFS_BATCH','0')))
    parser.add_argument('--output', type=Path, default=Path('work/gfs-program'))
    args = parser.parse_args(); rows = ranges(args.first_batch)
    if args.command == 'plan':
        # First four remaining batches use a serial priority job before the matrix.
        priority, rest = rows[:4], rows[4:]
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write('matrix='+json.dumps(dict(include=rest or [dict(batch=0)]))+'\n')
            stream.write('has_remaining='+str(bool(rest)).lower()+'\n')
        print(json.dumps(dict(priority=priority, remaining=rest, registration_sha256=EXPECTED_SHA256)))
        return
    defaults = json.loads((ROOT/'config/dev03_cloud_runtime_v1.json').read_bytes())['b2_location']
    env = dict(os.environ)
    for name, value in defaults.items():
        if not env.get(name): env[name] = value
    backend = B2Objects(resolve_b2_bucket(b2_settings(env)))
    selected = rows[:4] if args.command == 'priority' else [r for r in rows if r['batch']==args.batch]
    if not selected: raise ValueError('Batch not in explicit remaining program')
    for row in selected:
        execute(row['batch'], args.output/f"batch-{row['batch']:02d}", backend)


if __name__ == '__main__': main()
