"""Resolve non-secret storage variables from explicit project configuration."""
import json
import os
from pathlib import Path

from mardorf_collector.paths import REPOSITORY_ROOT


def environment(root=REPOSITORY_ROOT, *, environ=None):
    env=dict(os.environ if environ is None else environ)
    location=json.loads((Path(root)/'config/dev03_cloud_runtime_v1.json').read_text()).get('b2_location',{})
    for name in ('B2_ENDPOINT_URL','B2_REGION','B2_BUCKET'):
        if not env.get(name):
            value=location.get(name)
            if not isinstance(value,str) or not value.strip():
                raise ValueError('Explicit project storage location is missing')
            env[name]=value
    # Credential values have no defaults and remain in the invocation environment.
    return env
