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
    successor_v6_path=root/'docs/inventory/collector_runtime_successor_v6.json'
    successor_v6=json.loads(successor_v6_path.read_text()) if successor_v6_path.exists() else None
    successor_v5_path=root/'docs/inventory/collector_runtime_successor_v5.json'
    successor_v5=json.loads(successor_v5_path.read_text()) if successor_v5_path.exists() else None
    successor_v4_path=root/'docs/inventory/collector_runtime_successor_v4.json'
    successor_v4=json.loads(successor_v4_path.read_text()) if successor_v4_path.exists() else None
    successor_v3_path = root/'docs/inventory/collector_runtime_successor_v3.json'
    successor_v3 = json.loads(successor_v3_path.read_text()) if successor_v3_path.exists() else None
    successor_v2_path = root/'docs/inventory/collector_runtime_successor_v2.json'
    successor_v2 = json.loads(successor_v2_path.read_text()) if successor_v2_path.exists() else None
    successor_path=root/'docs/inventory/collector_runtime_successor_v1.json'
    if successor_path.exists():
        successor=json.loads(successor_path.read_text())
        release_path=root/'config/dev03_wp13_collector_release_v1.json'
        release=json.loads(release_path.read_text())
        frozen=root/successor['preserved_validator_path']
        if (successor.get('schema_version')!=1 or not successor.get('audit_id') or
                successor.get('predecessor_sha256')!=hashlib.sha256(corrections_path.read_bytes()).hexdigest() or
                successor.get('frozen_release_sha256')!=hashlib.sha256(release_path.read_bytes()).hexdigest() or
                hashlib.sha256(frozen.read_bytes()).hexdigest()!=release['artifact_sha256']['tools/validate_prep09_layout.py'] or
                successor.get('validator_sha256')!=hashlib.sha256((root/(successor_v2['preserved_validator_path'] if successor_v2 else 'tools/validate_prep09_layout.py')).read_bytes()).hexdigest()):
            raise ValueError('unverified collector runtime successor')
        for item in successor.get('modules',[]):
            path=item['implementation_path']
            if path in corrections:raise ValueError('duplicate runtime source successor')
            if any(m['implementation_path']==path and m.get('kind')=='protected_implementation_bridge' for m in modules):
                raise ValueError('runtime successor changed protected provider')
            corrections[path]=item
    if successor_v2 is not None:
        if (successor_v2.get('schema_version') != 1 or not successor_v2.get('audit_id') or
                successor_v2.get('artifact_version') != 'collector-runtime-successor-v2' or
                successor_v2.get('predecessor_sha256') != hashlib.sha256(successor_path.read_bytes()).hexdigest() or
                successor_v2.get('preserved_validator_sha256') != hashlib.sha256((root/successor_v2['preserved_validator_path']).read_bytes()).hexdigest() or
                successor_v2.get('validator_sha256') != hashlib.sha256((root/(successor_v3['preserved_validator_path'] if successor_v3 else 'tools/validate_prep09_layout.py')).read_bytes()).hexdigest()):
            raise ValueError('unverified collector cloud runtime successor')
        for item in successor_v2.get('modules', []):
            path = item['implementation_path']
            if path in corrections or any(m['implementation_path'] == path and m.get('kind') == 'protected_implementation_bridge' for m in modules):
                raise ValueError('duplicate or protected cloud source successor')
            if hashlib.sha256((root/item['preserved_source_path']).read_bytes()).hexdigest() != item['previous_source_sha256']:
                raise ValueError('collector predecessor source was not preserved')
            corrections[path] = item
    if successor_v3 is not None:
        if (successor_v3.get('schema_version') != 1 or not successor_v3.get('audit_id') or
                successor_v3.get('artifact_version') != 'collector-runtime-successor-v3' or
                successor_v3.get('predecessor_sha256') != hashlib.sha256(successor_v2_path.read_bytes()).hexdigest() or
                successor_v3.get('preserved_validator_sha256') != hashlib.sha256((root/successor_v3['preserved_validator_path']).read_bytes()).hexdigest() or
                successor_v3.get('validator_sha256') != hashlib.sha256((root/(successor_v4['preserved_validator_path'] if successor_v4 else 'tools/validate_prep09_layout.py')).read_bytes()).hexdigest()):
            raise ValueError('unverified collector workflow routing successor')
        for artifact in successor_v3.get('auxiliary_artifacts', []):
            if (hashlib.sha256((root/artifact['path']).read_bytes()).hexdigest() != artifact['sha256'] or
                    hashlib.sha256((root/artifact['preserved_path']).read_bytes()).hexdigest() != artifact['preserved_sha256']):
                raise ValueError('unverified auxiliary workflow validation successor')
    if successor_v4 is not None:
        if (successor_v4.get('artifact_version')!='collector-runtime-successor-v4' or
                successor_v4.get('predecessor_sha256')!=hashlib.sha256(successor_v3_path.read_bytes()).hexdigest() or
                successor_v4.get('preserved_validator_sha256')!=hashlib.sha256((root/successor_v4['preserved_validator_path']).read_bytes()).hexdigest() or
                successor_v4.get('preserved_validator_sha256')!=successor_v3.get('validator_sha256') or
                successor_v4.get('validator_sha256')!=hashlib.sha256((root/(successor_v5['preserved_validator_path'] if successor_v5 else 'tools/validate_prep09_layout.py')).read_bytes()).hexdigest()):
            raise ValueError('unverified manual model canary successor')
    if successor_v5 is not None:
        if (successor_v5.get('artifact_version')!='collector-runtime-successor-v5' or
                successor_v5.get('predecessor_sha256')!=hashlib.sha256(successor_v4_path.read_bytes()).hexdigest() or
                successor_v5.get('preserved_validator_sha256')!=successor_v4.get('validator_sha256') or
                successor_v5.get('preserved_validator_sha256')!=hashlib.sha256((root/successor_v5['preserved_validator_path']).read_bytes()).hexdigest() or
                successor_v5.get('validator_sha256')!=hashlib.sha256((root/(successor_v6['preserved_validator_path'] if successor_v6 else 'tools/validate_prep09_layout.py')).read_bytes()).hexdigest()):
            raise ValueError('unverified bounded acquisition/delivery successor')
        for artifact in successor_v5.get('auxiliary_artifacts',[]):
            if (hashlib.sha256((root/artifact['path']).read_bytes()).hexdigest()!=artifact['sha256'] or
                    hashlib.sha256((root/artifact['preserved_path']).read_bytes()).hexdigest()!=artifact['preserved_sha256']):
                raise ValueError('correctness auxiliary validator drift')
        seen=set()
        allowed={'src/mardorf_collector/runtime/provider_cycle_gate.py','src/mardorf_collector/providers/fetch_extra_models.py','src/extend_model_horizon.py'}
        if {row['implementation_path'] for row in successor_v5.get('modules',[])}!=allowed:
            raise ValueError('correctness successor changed its reviewed scope')
        historical={row['implementation_path']:row for row in proof['modules']}
        for row in successor_v5.get('modules',[]):
            path=row['implementation_path']
            if path in seen or path not in historical:raise ValueError('invalid correctness successor scope')
            seen.add(path)
            previous=corrections.get(path)
            expected=previous['corrected_source_sha256'] if previous else row['previous_source_sha256']
            if (row.get('previous_source_sha256')!=expected or
                    hashlib.sha256((root/row['preserved_source_path']).read_bytes()).hexdigest()!=expected):
                raise ValueError('correctness predecessor source drift')
            prior_ast=hashlib.sha256(ast.dump(Normalize().visit(ast.parse((root/row['preserved_source_path']).read_bytes())),include_attributes=False).encode()).hexdigest()
            expected_ast=previous['corrected_normalized_ast_sha256'] if previous else historical[path]['normalized_ast_sha256']
            if prior_ast!=expected_ast:raise ValueError('correctness predecessor AST drift')
            corrections[path]=row
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
            approved=corrections.get(item['implementation_path'])
            expected_sha=approved['corrected_source_sha256'] if approved else item['baseline_sha256']
            if hashlib.sha256((root / item['implementation_path']).read_bytes()).hexdigest() != expected_sha:
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
    successor_path = root / 'config/prep10_workflow_routes_v2.json'
    if successor_path.exists():
        successor = json.loads(successor_path.read_text())
        if successor.get('baseline_sha256') != hashlib.sha256((root / 'config/prep09_workflow_routes_v1.json').read_bytes()).hexdigest():
            raise ValueError('workflow successor baseline mismatch')
        for reviewed in successor['workflows']:
            name = Path(reviewed['path']).name
            baseline = expected.get(name, {})
            if reviewed.get('baseline_control_sha256') != baseline.get('source_control_sha256'):
                raise ValueError('workflow successor control binding mismatch')
            if not reviewed.get('reason') or not reviewed.get('frozen_git_blob'):
                raise ValueError('workflow successor requires reason and exact source bytes')
            expected[name] = reviewed
    cloud_routes_path = root/'config/prep10_workflow_routes_v3.json'
    if cloud_routes_path.exists():
        cloud_routes = json.loads(cloud_routes_path.read_text())
        if cloud_routes.get('baseline_sha256') != hashlib.sha256((root/'config/prep10_workflow_routes_v2.json').read_bytes()).hexdigest():
            raise ValueError('cloud workflow predecessor drift')
        seen = set()
        for reviewed in cloud_routes['workflows']:
            name = Path(reviewed['path']).name
            if name in seen or name not in expected or not reviewed.get('reason'):
                raise ValueError('invalid cloud workflow successor scope')
            seen.add(name)
            previous = (root/reviewed['preserved_workflow_path']).read_bytes()
            blob = hashlib.sha1(b'blob '+str(len(previous)).encode()+b'\0'+previous).hexdigest()
            if (signature(yaml.load(previous,Loader=yaml.BaseLoader)) != reviewed.get('baseline_control_sha256') or
                    reviewed.get('baseline_control_sha256') != expected[name]['source_control_sha256'] or
                    reviewed.get('preserved_git_blob') != blob or
                    (expected[name].get('frozen_git_blob') and expected[name]['frozen_git_blob'] != blob)):
                raise ValueError('cloud workflow original controls/bytes drift')
            expected[name] = reviewed
    model_review_path=root/'config/prep10_workflow_routes_v4.json'
    if model_review_path.exists():
        review=json.loads(model_review_path.read_text());rows=review.get('workflows',[])
        if review.get('baseline_sha256')!=hashlib.sha256(cloud_routes_path.read_bytes()).hexdigest() or len(rows)!=1:
            raise ValueError('manual model canary predecessor drift')
        row=rows[0];name='collect-models.yml'
        previous=(root/row['preserved_workflow_path']).read_bytes()
        if (row.get('path')!='.github/workflows/'+name or row.get('approved_scope')!='manual_isolated_model_canary_only' or
                row.get('baseline_control_sha256')!=expected[name]['source_control_sha256'] or
                row.get('preserved_git_blob')!=expected[name]['frozen_git_blob'] or
                hashlib.sha1(b'blob '+str(len(previous)).encode()+b'\0'+previous).hexdigest()!=row['preserved_git_blob'] or not row.get('reason')):
            raise ValueError('manual model canary scope/bytes drift')
        current=previous.decode()
        changes=[('      watchdog:\n', "      cloud_canary:\n        description: 'Manual isolated B2 delivery proof; no productive private branch changes'\n        type: boolean\n        default: false\n      watchdog:\n"), ('  group: public-model-collector\n', "  group: public-model-collector-${{ inputs.cloud_canary && 'b2-canary' || 'normal' }}\n"), ('          if python -c "import json,sys; sys.exit(0 if json.load(open(\'config/dev03_cloud_runtime_v1.json\'))[\'production_enabled\'] is True else 1)"; then', '          if [ \'${{ inputs.cloud_canary }}\' = \'true\' ] || python -c "import json,sys; sys.exit(0 if json.load(open(\'config/dev03_cloud_runtime_v1.json\'))[\'production_enabled\'] is True else 1)"; then'), (' && inputs.test_mode != true\n        shell: bash\n        env:', ' && (inputs.test_mode != true || inputs.cloud_canary == true)\n        shell: bash\n        env:'), ('          PYTHONPATH=src python -m mardorf_collector.transfer.dispatch publish "${args[@]}"', '          PYTHONPATH=src python -m mardorf_collector.transfer.dispatch publish --cloud-canary "${{ inputs.cloud_canary && \'true\' || \'false\' }}" "${args[@]}"')]
        for pattern,replacement in changes:
            if current.count(pattern)!=1:raise ValueError('ambiguous manual model canary scope')
            current=current.replace(pattern,replacement)
        active_path=(successor_v5['preserved_workflow_path'] if successor_v5 else row['path'])
        if current!=(root/active_path).read_text():raise ValueError('model canary changed acquisition or science gates')
        expected[name]=row
    if successor_v5 is not None:
        name='collect-models.yml'
        previous=(root/successor_v5['preserved_workflow_path']).read_bytes()
        row=successor_v5['workflow']
        if (row['baseline_control_sha256']!=expected[name]['source_control_sha256'] or
                hashlib.sha1(b'blob '+str(len(previous)).encode()+b'\0'+previous).hexdigest()!=expected[name]['frozen_git_blob'] or
                row['source_control_sha256']!=row['baseline_control_sha256']):
            raise ValueError('bounded mirror workflow controls drift')
        current=previous.decode()
        for stage in ('base','extension'):
            anchor='name: '+('Fetch ECMWF-IFS base' if stage=='base' else 'Extend ECMWF-IFS')
            start=current.index('          last=1\n',current.index(anchor))
            end=current.index('          exit "$last"',start)+len('          exit "$last"')
            args=' "${args[@]}"' if stage=='base' else ''
            replacement=('          set +e\n'
                '          timeout --signal=TERM --kill-after=15s 6m python src/provider_fetch.py --model ECMWF-IFS --stage '+stage+args+'\n'
                '          rc=$?\n          set -e\n'
                '          if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then\n'
                '            python src/record_provider_failure.py --model ECMWF-IFS --stage '+stage+' --exit-code "$rc" --reason "hard_timeout_6m"\n'
                '          fi\n          exit "$rc"')
            current=current[:start]+replacement+current[end:]
        if current!=(root/row['path']).read_text():raise ValueError('mirror successor changed unrelated workflow bytes')
        expected[name]=row
    if successor_v5 is not None:
        reviewed=successor_v5['variables_workflow']
        previous=(root/reviewed['preserved_path']).read_bytes()
        if (reviewed['path']!='.github/workflows/validate.yml' or
                hashlib.sha256(previous).hexdigest()!=reviewed['preserved_sha256'] or
                hashlib.sha256(previous).hexdigest()!=json.loads((root/'docs/inventory/wp13_ci_contract_v1.json').read_text())['workflow_sha256']):
            raise ValueError('variable access workflow predecessor drift')
        original=previous.decode()
        if original.count(reviewed['insert_before'])!=1:raise ValueError('ambiguous variable access workflow insertion')
        current=(root/reviewed['path']).read_bytes()
        if (original.replace(reviewed['insert_before'],reviewed['inserted_block']+reviewed['insert_before']).encode()!=current or
                hashlib.sha256(current).hexdigest()!=reviewed['sha256']):
            raise ValueError('variable access workflow changed unrelated bytes')
        expected['validate.yml']={'source_control_sha256':signature(yaml.load(current,Loader=yaml.BaseLoader))}
    if successor_v6 is not None:
        if (successor_v6.get('artifact_version')!='collector-runtime-successor-v6' or
                successor_v6.get('predecessor_sha256')!=hashlib.sha256(successor_v5_path.read_bytes()).hexdigest() or
                successor_v6.get('preserved_validator_sha256')!=successor_v5.get('validator_sha256') or
                hashlib.sha256((root/successor_v6['preserved_validator_path']).read_bytes()).hexdigest()!=successor_v6['preserved_validator_sha256'] or
                hashlib.sha256((root/'tools/validate_prep09_layout.py').read_bytes()).hexdigest()!=successor_v6['validator_sha256']):
            raise ValueError('Unverified native SVG runtime successor')
        row=successor_v6['workflow'];name='collect-svg.yml'
        previous=(root/row['preserved_path']).read_bytes()
        if (row['path']!='.github/workflows/'+name or row['approved_scope']!='native_svg_shared_acquisition_and_delivery_only' or
                hashlib.sha256(previous).hexdigest()!=row['previous_sha256'] or
                hashlib.sha1(b'blob '+str(len(previous)).encode()+b'\0'+previous).hexdigest()!=expected[name]['frozen_git_blob']):
            raise ValueError('Native SVG workflow predecessor drift')
        current=previous.decode()
        for change in row['replacements']:
            if current.count(change['before'])!=1:raise ValueError('Ambiguous native SVG command change')
            current=current.replace(change['before'],change['after'])
        active=(root/row['path']).read_bytes()
        if (current.encode()!=active or hashlib.sha256(active).hexdigest()!=row['sha256'] or
                signature(yaml.load(active,Loader=yaml.BaseLoader))!=expected[name]['source_control_sha256']):
            raise ValueError('Native SVG changed acquisition controls or unrelated workflow bytes')
        allowed={'src/mardorf_collector/wp13/native_svg_v1.py','tests/test_wp15_native_svg.py'}
        if set(successor_v6['artifacts'])!=allowed:
            raise ValueError('Native SVG runtime scope changed')
        for name,digest in successor_v6['artifacts'].items():
            if hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest:
                raise ValueError('Native SVG runtime artifact drift')
        expected['collect-svg.yml']=dict(expected['collect-svg.yml'],
            frozen_git_blob=hashlib.sha1(b'blob '+str(len(active)).encode()+b'\0'+active).hexdigest())
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
            # Explicit successors bind approved CI-only paths to the frozen controls.
            correction = next((item for item in corrections_doc.get('workflow_ci_path_corrections', [])
                               if item.get('path') == '.github/workflows/' + name), None)
            baseline_data = json.loads(json.dumps(data))
            paths = baseline_data.get('on', {}).get('pull_request', {}).get('paths', [])
            if (layout['repository'] != 'janwohlers78/mardorf-data-collector' or name != 'validate.yml' or
                    correction is None or paths.count('requirements-ci.txt') != 1):
                raise ValueError(f'trigger/permission/concurrency/guard drift: {name}')
            paths.remove('requirements-ci.txt')
            wp13_path=root/'docs/inventory/wp13_ci_contract_v1.json'
            wp13=json.loads(wp13_path.read_text()) if wp13_path.exists() else None
            if wp13 is not None:
                if (wp13.get('predecessor_sha256')!=hashlib.sha256(corrections_path.read_bytes()).hexdigest() or wp13.get('baseline_control_sha256')!=expected[name]['source_control_sha256'] or wp13.get('workflow_sha256')!=hashlib.sha256(path.read_bytes()).hexdigest() or paths.count('requirements-wp13-grib.txt')!=1):
                    raise ValueError('unverified WP13 CI successor')
                paths.remove('requirements-wp13-grib.txt')
            if (signature(baseline_data) != expected[name]['source_control_sha256'] or
                    correction.get('baseline_control_sha256') != expected[name]['source_control_sha256'] or
                    (wp13 is None and correction.get('corrected_source_sha256') != hashlib.sha256(path.read_bytes()).hexdigest()) or
                    not correction.get('finding_ids')):
                raise ValueError(f'unverified CI path correction: {name}')
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
