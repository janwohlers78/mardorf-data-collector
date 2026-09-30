#!/usr/bin/env python3
"""Read-only package-source and workflow-control verification for both repos."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import yaml


def controls(data):
    keys = ('on', 'permissions', 'concurrency', 'env')
    result = {k: data.get(k) for k in keys}
    result['jobs'] = {}
    for name, job in data.get('jobs', {}).items():
        result['jobs'][name] = {k: job.get(k) for k in ('if', 'permissions', 'concurrency', 'env', 'timeout-minutes')}
        result['jobs'][name]['steps'] = [
            {k: s.get(k) for k in ('name', 'id', 'uses', 'if', 'continue-on-error', 'timeout-minutes', 'env')}
            for s in job.get('steps', [])
            if s.get('name') != 'Validate package CLI and active project context'
        ]
    return result


def signature(data):
    return hashlib.sha256(json.dumps(controls(data), sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate(root):
    root = Path(root).resolve()
    layout = json.loads((root / 'config/prep09_package_layout_v1.json').read_text())
    proof = json.loads((root / 'docs/inventory/package_source_parity_v1.json').read_text())
    routes = json.loads((root / 'config/prep09_workflow_routes_v1.json').read_text())
    if not layout['source_commit'] == proof['source_commit'] == routes['source_commit']:
        raise ValueError('source-commit binding drift')
    modules = layout['modules']
    reverse = {m['module']: Path(m['legacy_path']).stem for m in modules}
    class Normalize(ast.NodeTransformer):
        def visit_ImportFrom(self, node):
            if node.module in ('mardorf.paths', 'mardorf_collector.paths'):
                return None
            node.module = reverse.get(node.module, node.module)
            return self.generic_visit(node)
        def visit_Import(self, node):
            for alias in node.names:
                if alias.name in reverse:
                    old = reverse[alias.name]
                    if alias.asname == old:
                        alias.asname = None
                    alias.name = old
            return node
        def visit_Name(self, node):
            return ast.parse('Path(__file__).resolve().parents[1]', mode='eval').body if node.id == 'REPOSITORY_ROOT' else node
    corrections_path = root / 'docs/inventory/package_source_corrections_v1.json'
    corrections_doc = json.loads(corrections_path.read_text()) if corrections_path.exists() else {}
    if corrections_doc and (corrections_doc.get('schema_version') != 1 or
                            corrections_doc.get('baseline_source_commit') != proof['source_commit']):
        raise ValueError('source correction manifest baseline drift')
    corrections = {item['implementation_path']: item for item in corrections_doc.get('modules', [])}
    if len(corrections) != len(corrections_doc.get('modules', [])):
        raise ValueError('duplicate corrected source path')
    if set(corrections) - {item['implementation_path'] for item in proof['modules']}:
        raise ValueError('correction outside the historical package parity scope')
    by_path = {m['implementation_path']: m for m in modules}
    if len(by_path) != len(modules):
        raise ValueError('duplicate implementation path')
    for item in proof['modules']:
        path = item['implementation_path']
        if by_path[path]['baseline_sha256'] != item['baseline_sha256']:
            raise ValueError('baseline hash binding drift')
        tree = Normalize().visit(ast.parse((root / path).read_bytes()))
        digest = hashlib.sha256(ast.dump(tree, include_attributes=False).encode()).hexdigest()
        correction = corrections.get(path)
        if correction is not None:
            # Historical mechanical-migration proof remains immutable. Each intended
            # bug fix binds that exact baseline and the current corrected bytes/AST.
            if (correction.get('baseline_normalized_ast_sha256') != item['normalized_ast_sha256'] or
                    correction.get('corrected_normalized_ast_sha256') != digest or
                    correction.get('corrected_source_sha256') != hashlib.sha256((root / path).read_bytes()).hexdigest() or
                    not correction.get('finding_ids') or not corrections_doc.get('audit_id')):
                raise ValueError(f'unverified source correction: {path}')
        elif digest != item['normalized_ast_sha256']:
            raise ValueError(f'non-mechanical source change: {path}')
    for item in modules:
        if item.get('kind') == 'protected_implementation_bridge':
            if hashlib.sha256((root / item['implementation_path']).read_bytes()).hexdigest() != item['baseline_sha256']:
                raise ValueError('protected implementation bytes drift')
            text = (root / item['package_bridge_path']).read_text()
            legacy = Path(item['legacy_path']).stem
            if f'_importlib.import_module("{legacy}")' not in text or '_sys.modules[__name__] = _implementation' not in text:
                raise ValueError('protected package bridge drift')
            continue
        text = (root / item['legacy_path']).read_text()
        if f'_importlib.import_module("{item["module"]}")' not in text or '_sys.modules[__name__] = _implementation' not in text:
            raise ValueError(f'legacy adapter drift: {item["legacy_path"]}')
    workflows = {p.name: p for p in (root / '.github/workflows').glob('*.yml')}
    expected = {Path(w['path']).name: w for w in routes['workflows']}
    if workflows.keys() != expected.keys():
        raise ValueError('workflow added/removed without route review')
    for name, path in workflows.items():
        data = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        package_steps = [step for job in data.get('jobs', {}).values() for step in job.get('steps', [])
                         if step.get('name') == 'Validate package CLI and active project context']
        is_private_gate = (layout['repository'] == 'janwohlers78/mardorf-kitevorhersage' and
                           name == 'full-integration-gate.yml')
        if is_private_gate:
            required_commands = ['PYTHONPATH=src python -m mardorf context validate',
                                 'python tools/validate_prep09_layout.py',
                                 'PYTHONPATH=src python tools/validate_prep09_formats.py']
            if len(package_steps) != 1 or set(package_steps[0]) != {'name', 'run'}:
                raise ValueError('mandatory package/context/format CI step missing, duplicated or guarded')
            commands = [line.strip() for line in package_steps[0]['run'].splitlines() if line.strip()]
            if commands != required_commands:
                raise ValueError('mandatory package/context/format CI commands drift')
        elif package_steps:
            raise ValueError('package validation signature exception outside its approved CI path')
        if signature(data) != expected[name]['source_control_sha256']:
            raise ValueError(f'trigger/permission/concurrency/guard drift: {name}')
        if expected[name].get('frozen_git_blob'):
            raw = path.read_bytes()
            blob = hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()
            if blob != expected[name]['frozen_git_blob']:
                raise ValueError('frozen workflow byte drift')
    bridges = sum(m.get('kind') == 'protected_implementation_bridge' for m in modules)
    return {'status': 'PASS', 'normalized_source_modules': len(proof['modules']),
            'source_corrections': len(corrections), 'legacy_adapters': len(modules) - bridges, 'protected_bridges': bridges,
            'workflow_controls': len(workflows)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    print(json.dumps(validate(parser.parse_args().root), sort_keys=True))
