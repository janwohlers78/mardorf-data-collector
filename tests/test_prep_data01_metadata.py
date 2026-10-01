"""Fail-closed shared metadata diagnostics and acquisition receipt binding."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from prep_data01_metadata import acquisition_inventory, dimension_report


class MetadataTests(unittest.TestCase):
    def field(self):
        return {'shortName': '2t', 'units': 'K', 'typeOfLevel': 'heightAboveGround',
                'level': 2, 'stepType': 'instant', 'value': 280}

    def test_missing_unknown_unrecognized_and_conflicts_are_distinct(self):
        for value, state in [(None, 'missing'), ('', 'missing'), ('unknown', 'unknown'),
                             ('arbitrary_new_level', 'unrecognized'), (False, 'invalid')]:
            field = self.field(); field['typeOfLevel'] = value
            report = dimension_report(field)
            self.assertEqual(report['dimension_states']['type_of_level_native'], state)
            self.assertFalse(report['scientifically_known'])
        field = self.field(); field['level_native'] = '3'
        self.assertEqual(dimension_report(field)['dimension_states']['level_native'], 'conflicting')

    def test_zero_level_is_present_bool_is_invalid_no_native_inference(self):
        field = self.field(); field['level'] = 0
        self.assertTrue(dimension_report(field)['scientifically_known'])
        field['level'] = True
        self.assertEqual(dimension_report(field)['dimension_states']['level_native'], 'invalid')
        api = {'parameter_native': 'temperature_2m', 'unit': '°C', 'interval': 'instantaneous'}
        report = dimension_report(api)
        self.assertEqual(report['dimension_states']['level_native'], 'missing')
        self.assertEqual(report['dimension_states']['step_type_native'], 'missing')

    def test_product_specifications_preserve_actual_members_without_counting_values(self):
        source = {'model': 'EPS', 'ensemble_system_id': 'EPS',
                  'field_specs': {'temperature': {'parameter_native': 'temperature', 'unit': '°C'}},
                  'columns': {'temperature': {'0': [1, 2], '1': [2, 3]}}}
        before = deepcopy(source)
        report = acquisition_inventory(source)
        self.assertEqual(report['record_count'], 2)
        self.assertEqual({r['member_id'] for r in report['records']}, {'0', '1'})
        self.assertTrue(all(r['grain'] == 'product_field_member_specification' for r in report['records']))
        self.assertEqual(source, before)
        report['records'][0]['product_metadata']['unit'] = 'changed'
        self.assertEqual(source, before)

    def test_native_fields_keep_source_paths_and_model_identity(self):
        report = acquisition_inventory({'models': {'GFS': [{'values': {'2t': [self.field()]}}]}})
        self.assertEqual(report['record_count'], 1)
        self.assertEqual(report['records'][0]['path'], '/models/GFS/0/values/2t/0')
        self.assertEqual(report['records'][0]['model'], 'GFS')
        self.assertTrue(report['records'][0]['scientifically_known'])

    def test_cli_preserves_payload_and_existing_integrity_and_rejects_stale_audit(self):
        script = Path(__file__).resolve().parents[1] / 'src/prep_data01_metadata.py'
        with tempfile.TemporaryDirectory() as td:
            payload, integrity = Path(td) / 'payload.json', Path(td) / 'integrity.json'
            raw = json.dumps({'models': {'GFS': [{'values': {'2t': [self.field()]}}]}}).encode()
            payload.write_bytes(raw)
            original = {'kind': 'models', 'status': 'pass', 'input_payload_sha256': hashlib.sha256(raw).hexdigest()}
            integrity.write_text(json.dumps(original))
            cmd = [sys.executable, str(script), '--input', str(payload), '--integrity-json', str(integrity)]
            subprocess.run(cmd, check=True, capture_output=True)
            after = json.loads(integrity.read_bytes())
            self.assertEqual({k: after[k] for k in original}, original)
            self.assertEqual(after['prep_data01_metadata']['record_count'], 1)
            self.assertEqual(payload.read_bytes(), raw)
            payload.write_text('{}')
            self.assertNotEqual(subprocess.run(cmd, capture_output=True).returncode, 0)


if __name__ == '__main__':
    unittest.main()
