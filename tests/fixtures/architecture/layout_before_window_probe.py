#!/usr/bin/env python3
"""Validate current bounded refactorings and retain immutable PREP09 evidence.

Historical validation is read-only in a temporary filesystem view. Only explicitly
hash-bound changed workflow/validator paths show their preserved predecessors;
all other source and contracts are the current checkout. New changes are verified
by exact replacement recipes before historical rules are evaluated.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def historical_module(root):
    proof=json.loads((root/'config/architecture_refactoring_v1.json').read_text())
    item=proof['historical_validator'];path=root/item['path']
    if sha(path)!=item['sha256']:
        raise ValueError('Historical layout validator bytes changed')
    spec=importlib.util.spec_from_file_location('mardorf_historical_layout',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module,proof


def validate(root):
    root=Path(root).resolve();module,proof=historical_module(root)
    if proof.get('schema_version')!=1 or proof.get('audit_id')!='AUD-20261005-ARCH':
        raise ValueError('Explicit architecture successor identity required')
    if sha(root/'tools/validate_prep09_layout.py')!=proof['current_validator_sha256']:
        raise ValueError('Current layout validator bytes changed without review')
    predecessor = proof.get('research_validator_predecessor')
    if predecessor and sha(root/predecessor['path']) != predecessor['sha256']:
        raise ValueError('Research route validator predecessor byte drift')
    overrides={'tools/validate_prep09_layout.py':root/proof['historical_validator']['path']}
    # Additive research routes are checked in the current tree. The immutable
    # historical gate then sees exactly its original workflow set.
    added = set()
    import yaml
    for item in proof.get('added_workflows', []):
        name = item['path']
        if name != '.github/workflows/svg-historical-research-v1.yml' or name in added:
            raise ValueError('Invalid additive research route')
        path = root/name
        data = yaml.load(path.read_bytes(), Loader=yaml.BaseLoader)
        if (sha(path) != item['sha256'] or not item.get('reason')
                or set(data['on']) != {'workflow_dispatch'}
                or data['permissions'] != {'contents': 'read'}
                or data['concurrency'] != {'group': 'svg-historical-research-v1', 'cancel-in-progress': 'false'}
                or data['jobs']['acquire']['timeout-minutes'] != '60'
                or set(data['jobs']) != {'acquire'}):
            raise ValueError('Bounded manual research workflow drift')
        added.add(name)
    for item in proof['workflows'] + proof.get('artifact_successors', []):
        name=item['path']
        if not name.startswith(('.github/workflows/', 'tests/', 'src/', 'config/')) or '..' in Path(name).parts or name in overrides:
            raise ValueError('Duplicate or invalid workflow review path')
        before=root/item['preserved_path'];current=root/name
        if sha(before)!=item['baseline_sha256'] or sha(current)!=item['current_sha256']:
            raise ValueError('Reviewed workflow byte drift: '+name)
        expected=before.read_text()
        for replacement in item['replacements']:
            if expected.count(replacement['before'])!=replacement['count'] or replacement['count']<1:
                raise ValueError('Workflow replacement scope changed: '+name)
            expected=expected.replace(replacement['before'],replacement['after'])
        if expected!=current.read_text():
            raise ValueError('Workflow differs from its explicit replacement review: '+name)
        overrides[name]=before
    # Clone only directories containing overrides, linking all untouched files.
    # No application code is imported, weather data copied or jobs executed.
    with tempfile.TemporaryDirectory(prefix='mardorf-layout-review-') as temporary:
        view=Path(temporary)
        def populate(relative):
            source=root/relative;target=view/relative;key=relative.as_posix()
            if key in added:return
            if key in overrides:
                target.symlink_to(overrides[key]);return
            prefix='' if key=='.' else key+'/'
            if source.is_dir() and any(name.startswith(prefix) for name in set(overrides) | added):
                target.mkdir(exist_ok=True)
                for child in sorted(source.iterdir()):populate(relative/child.name)
            else:
                target.symlink_to(source,target_is_directory=source.is_dir())
        populate(Path('.'))
        result=module.validate(view)
    return dict(result,architecture_reviewed_workflows=len(proof['workflows']),
                architecture_added_workflows=len(added))


def reviewed_predecessor(root, relative):
    """Historical final-byte assertions remain bound to their reviewed predecessor.

    Validate current artifacts as well: this is not a hash-validation exemption.
    """
    root=Path(root).resolve()
    validate(root)
    proof=json.loads((root/'config/architecture_refactoring_v1.json').read_text())
    if relative=='tools/validate_prep09_layout.py':
        return root/proof['historical_validator']['path']
    for row in proof['workflows']+proof.get('artifact_successors',[]):
        if row['path']==relative:return root/row['preserved_path']
    return root/relative


def controls(data):
    module,_=historical_module(Path(__file__).resolve().parents[1])
    return module.controls(data)


def signature(data):
    module,_=historical_module(Path(__file__).resolve().parents[1])
    return module.signature(data)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    print(json.dumps(validate(parser.parse_args().root),sort_keys=True))
