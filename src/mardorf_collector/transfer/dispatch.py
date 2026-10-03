"""One configurable publication route; manual canaries use an isolated head."""
import argparse
import importlib
import os
import re
import sys

from mardorf_collector.paths import REPOSITORY_ROOT
from mardorf_collector.runtime.private_state import profile


def select_route(*, canary=False, environ=None, specification=None):
    env = os.environ if environ is None else environ
    config = profile() if specification is None else specification
    if config is None:
        raise ValueError('Explicit publication profile required')
    repository = env.get('PRIVATE_REPO', config['private_repository'])
    if repository != config['private_repository']:
        raise ValueError('Publication repository differs from configured authority')
    if canary:
        if env.get('GITHUB_EVENT_NAME') != 'workflow_dispatch' or not all(
                re.fullmatch(r'[1-9][0-9]*', env.get(key, '')) for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT')):
            raise ValueError('Canary publication requires explicit manual workflow invocation')
        return 'cloud', f"codex/b2-p05-canary-{env['GITHUB_RUN_ID']}-{env['GITHUB_RUN_ATTEMPT']}"
    if env.get('CLOUD_CONTROL_BRANCH') or env.get('CLOUD_CONTROL_PATH'):
        raise ValueError('Routine publication cannot override configured control head')
    return ('cloud' if config['production_enabled'] else 'git'), None


def prepare_canary(branch):
    from mardorf_collector.storage.runtime import load_runtime
    runtime = load_runtime(REPOSITORY_ROOT)
    parent, _ = runtime.head.read()
    # Reuse this invocation's branch for both children and finalization.
    existing = runtime.head.call('GET', '/git/ref/heads/' + branch, allowed=(404,))
    if existing is None:
        runtime.head.call('POST', '/git/refs', json={'ref': 'refs/heads/' + branch, 'sha': parent})
    os.environ['CLOUD_CONTROL_BRANCH'] = branch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('publish', 'secondary'))
    parser.add_argument('--cloud-canary', choices=('true', 'false'), default='false')
    args, remaining = parser.parse_known_args()
    route, branch = select_route(canary=args.cloud_canary == 'true')
    if branch:
        # Respect reruns of this invocation, never an arbitrary supplied head.
        os.environ.pop('CLOUD_CONTROL_BRANCH', None)
        os.environ.pop('CLOUD_CONTROL_PATH', None)
        prepare_canary(branch)
    modules = {'publish': {'git': 'push_private', 'cloud': 'push_cloud'},
               'secondary': {'git': 'finalize_secondary_batch', 'cloud': 'finalize_cloud_secondary'}}
    sys.argv = [sys.argv[0], *remaining]
    importlib.import_module('.' + modules[args.operation][route], __package__).main()


if __name__ == '__main__':
    main()
