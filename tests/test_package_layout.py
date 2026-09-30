import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class PackageLayoutTests(unittest.TestCase):
    def test_legacy_imports_are_identical_to_canonical_modules(self):
        layout = json.loads((ROOT / 'config/prep09_package_layout_v1.json').read_text())
        for item in layout['modules']:
            with self.subTest(module=item['module']):
                legacy = importlib.import_module(Path(item['legacy_path']).stem)
                canonical = importlib.import_module(item['module'])
                self.assertIs(legacy, canonical)

    def test_cli_help_and_rooted_registry_work_without_provider_or_cwd_dependency(self):
        with tempfile.TemporaryDirectory() as td:
            env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
            commands = [[sys.executable, '-m', 'mardorf_collector', 'audit-integrity', '--help'],
                        [sys.executable, str(ROOT / 'src/audit_integrity.py'), '--help']]
            results = [subprocess.check_output(c, cwd=td, env=env, text=True) for c in commands]
            self.assertEqual(results[0], results[1])
            code = "from mardorf_collector.contracts.relevant_meteorology_registry_v3 import REGISTRY_PATH; assert REGISTRY_PATH.is_file()"
            subprocess.check_call([sys.executable, '-c', code], cwd=td, env=env)

    def test_watchdog_package_command_uses_only_standard_library_dependencies(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        # Watchdog deliberately does not install the provider dependency lockfile.
        subprocess.check_call([sys.executable, '-S', '-m', 'mardorf_collector', 'check-collection-due', '--help'], env=env, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    unittest.main()
