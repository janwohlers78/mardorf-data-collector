import io
import json
from datetime import datetime,timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from mardorf_collector.providers import volmerdingsen_v1 as station
from mardorf_collector.storage.objects import LocalObjects,ObjectRef


class Response:
    def __init__(self,raw,status=200):self.raw=raw;self.status_code=status
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,size):yield self.raw


class Session:
    def __init__(self,raw,status=200):self.raw=raw;self.status=status;self.calls=[]
    def get(self,url,**kw):self.calls.append((url,kw));return Response(self.raw,self.status)


class VolmerdingsenCollectorTests(unittest.TestCase):
    def test_archive_discovers_all_groups_and_completed_months(self):
        script=b'var graphgroups_raw = {"day": [], "UnknownNewSensor": []};\n'
        paths=station.archive_paths(script,datetime(2026,10,11,tzinfo=timezone.utc))
        self.assertIn('json/UnknownNewSensor.json',paths)
        self.assertIn('NOAA/NOAA-2021-01.txt',paths)
        self.assertIn('NOAA/NOAA-2026-10.txt',paths)
        self.assertNotIn('NOAA/NOAA-2026-11.txt',paths)
        with self.assertRaises(ValueError):station.archive_paths(b'no catalogue',datetime.now(timezone.utc))

    def test_no_redirect_budget_and_http_error_bytes_retained(self):
        s=Session(b'{}');r,raw=station.fetch('json/day.json',s)
        self.assertFalse(s.calls[0][1]['allow_redirects']);self.assertEqual(raw,b'{}')
        with patch.object(station,'MAX_BYTES',1):
            r,raw=station.fetch('json/day.json',s)
        self.assertEqual(r['status'],'OVERSIZE');self.assertIsNone(raw)
        r,raw=station.fetch('json/day.json',Session(b'error',503))
        self.assertEqual(r['status'],'HTTP_ERROR');self.assertEqual(raw,b'error')

    def test_exact_unknown_fields_and_source_errors_are_in_readback_pack(self):
        original=b'{"unknown_native_sensor": [1, null, 2]}'
        with tempfile.TemporaryDirectory() as temp:
            backend=LocalObjects(Path(temp)/'b2')
            with patch.object(station.time,'sleep'),patch.object(station,'RECENT',('day',)):
                control=station.run('recent',Path(temp)/'out',backend,session=Session(original),
                    now=datetime(2026,10,11,tzinfo=timezone.utc))
            # The NOAA response is invalid but remains preserved; partial never
            # represents a successful collector pointer or scientific release.
            self.assertEqual(control['metadata']['status'],'PARTIAL')
            manifest=json.loads(backend.get_bytes(ObjectRef.parse(control['snapshot'])))
            with zipfile.ZipFile(io.BytesIO(backend.get_bytes(ObjectRef.parse(manifest['originals']))))as z:
                self.assertEqual(z.read('json/day.json'),original)
                self.assertEqual(z.read('NOAA/NOAA-2026-10.txt'),original)
            self.assertEqual(manifest['field_exclusions'],0)
            self.assertFalse(manifest['historical_hourly_export_verified'])

    def test_existing_secondary_job_collects_without_new_private_workflow(self):
        root=Path(__file__).resolve().parents[1]
        workflow=(root/'.github/workflows/collect-secondary.yml').read_text()
        self.assertIn('volmerdingsen_v1 --mode recent --publish-control',workflow)
        self.assertIn('continue-on-error: true',workflow)
        self.assertIn('timeout-minutes: 6',workflow)
        self.assertIn('if: ${{ !cancelled() && steps.storage_gate.outcome',workflow)


if __name__=='__main__':unittest.main()
