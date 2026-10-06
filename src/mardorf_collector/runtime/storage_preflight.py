"""Read the current canonical root before expensive provider acquisition.

A storage outage is a failed prerequisite, never a successful no-op or provider
cycle evidence. There is no local/Git weather fallback, clock rewrite, write or
quota change. Each due invocation probes again so recovery resumes acquisition.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from mardorf_collector.paths import REPOSITORY_ROOT
from .private_state import profile


def check(root=REPOSITORY_ROOT, *, environ=None, factory=None):
    config = profile(root)
    if config is None:
        raise ValueError('Explicit storage authority profile required')
    env = dict(os.environ if environ is None else environ)
    active = config['production_enabled'] or env.get('CLOUD_CANARY') == 'true'
    if not active:
        return {'status': 'NOT_REQUIRED', 'reason': 'cloud_authority_inactive'}
    if factory is None:
        from .cloud_environment import environment
        from mardorf_collector.storage.runtime import load_runtime
        factory = lambda: load_runtime(root, environ=environment(root, environ=env))
    runtime = factory()
    # Current live head and canonical full root hash/structure validation. A
    # stale cache, partial range or merely successful HEAD cannot establish this.
    runtime.open()
    return {'status': 'PASS', 'reason': 'current_canonical_root_verified'}


def main():
    try:
        result = check()
    except Exception as exc:
        # Provider exception text can contain request/credential details. Keep
        # only fixed context and exception class; never imply capture succeeded.
        result = {'status': 'FAIL', 'reason': 'canonical_storage_unavailable',
                  'exception_type': type(exc).__name__}
    result['as_of_utc'] = datetime.now(timezone.utc).isoformat()
    path = Path('work/storage_access_preflight.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as target:
            target.write('\n## Canonical storage prerequisite\n\n'
                         + result['status'] + ': ' + result['reason'] + '.\n')
            if result['status'] == 'FAIL':
                target.write('Provider acquisition blocked before expensive work. '
                             'This is an outage; no collection success or freshness claimed.\n')
    return 1 if result['status'] == 'FAIL' else 0


if __name__ == '__main__':
    raise SystemExit(main())
