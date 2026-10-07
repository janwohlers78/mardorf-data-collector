import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]


class NativeCacheLifecycleTests(unittest.TestCase):
    def setUp(self):
        workflow = yaml.load((ROOT/'.github/workflows/collect-models.yml').read_text(), Loader=yaml.BaseLoader)
        self.steps = workflow['jobs']['collect']['steps']
        self.qualify = next(s for s in self.steps if s.get('id') == 'native_cache_proof')

    def saved(self, value):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            if value is not None:
                path = root/'work/model_originals_v1/published.json'
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(value))
            out = root/'output'
            subprocess.run(['bash','-e','-c',self.qualify['run']],cwd=root,
                           env=dict(os.environ,GITHUB_OUTPUT=str(out)),check=True)
            return out.read_text().strip()

    def test_cache_is_preserved_after_complete_native_proof_despite_later_audit_error(self):
        value = dict(status='PASS', cold_readback=dict(status='PASS', originals_fully_read=True,
                     parquet_exact_native_match=True), canonical_cache_metrics=dict(hits=0,misses=2))
        self.assertEqual(self.saved(value),'save=true')
        self.assertIn('always()',self.qualify['if'])
        self.assertIn("steps.native_transfer.outcome == 'success'",self.qualify['if'])
        save = next(s for s in self.steps if s.get('uses') == 'actions/cache/save@v4')
        self.assertEqual(save['if'],"always() && steps.storage_gate.outcome == 'success' && steps.native_cache_proof.outputs.save == 'true'")
        restore = next(s for s in self.steps if s.get('uses') == 'actions/cache/restore@v4')
        self.assertEqual(save['with']['key'],restore['with']['key'])
        fail = next(s for s in self.steps if s.get('name') == 'Fail workflow when granular integrity audit found errors')
        self.assertIn('raise SystemExit(1)',fail['run'])

    def test_missing_failed_or_partial_proofs_never_save(self):
        self.assertEqual(self.saved(None),'save=false')
        full = dict(status='PASS',cold_readback=dict(status='PASS',originals_fully_read=True,
                    parquet_exact_native_match=True),canonical_cache_metrics={})
        for key in ('originals_fully_read','parquet_exact_native_match'):
            value = json.loads(json.dumps(full));value['cold_readback'][key]=False
            self.assertEqual(self.saved(value),'save=false')
        full['status']='FAIL';self.assertEqual(self.saved(full),'save=false')


if __name__ == '__main__':
    unittest.main()
