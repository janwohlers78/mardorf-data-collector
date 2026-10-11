"""Cheap stdlib gate before installing private-decoder dependencies."""
import base64
import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from .check_collection_due import output, parse_time

PATH = 'config/cloud_refs/svg_thunder_warning_v1.json'


def decision(value, now, *, recovery=False):
    if value is None: return True, 'missing_warning_control', None
    try:
        if (value['artifact_version'] != 'svg-thunder-warning-control-v1'
                or value['readback_verified'] is not True or value['bundle_ready'] is not True
                or value['snapshot']['sha256'] != value['metadata']['receipt_sha256']
                or value['snapshot']['sha256'] != value['receipt_sha256']):
            raise ValueError()
        age = (now-parse_time(value['generated_at_utc'])).total_seconds()
        if age < 0: raise ValueError()
        # At most one issuance per UTC half-hour, with chronological CAS again
        # at publication. A same-slot trigger is harmless and skips all capture.
        due = int(now.timestamp())//1800 > int(parse_time(value['generated_at_utc']).timestamp())//1800
        if recovery and age < 2100:
            return False, 'scheduled_warning_recovery_grace', age/60
        return due, 'next_halfhour_due' if due else 'halfhour_already_published', age/60
    except (KeyError, ValueError, TypeError): return True, 'unverified_warning_control', None


def fetch(environ):
    repository = environ.get('PRIVATE_REPO', 'janwohlers78/mardorf-kitevorhersage')
    token = environ.get('PRIVATE_REPO_TOKEN', '')
    if not token: return None
    request = urllib.request.Request('https://api.github.com/repos/'+repository+'/contents/'+PATH+'?ref=main',
        headers={'Authorization': 'Bearer '+token, 'Accept': 'application/vnd.github+json'})
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            raw = response.read(131073)
            if len(raw) > 131072: return None
            item = json.loads(raw)
            return json.loads(base64.b64decode(item['content']))
    except (OSError, ValueError, KeyError, TypeError): return None


def main():
    p = argparse.ArgumentParser(); p.add_argument('--recovery', action='store_true'); args = p.parse_args()
    due, reason, age = decision(fetch(os.environ), datetime.now(timezone.utc), recovery=args.recovery)
    output('due', 'true' if due else 'false'); output('reason', reason)
    output('age_minutes', age if age is not None else 'unknown'); output('threshold_minutes', 'UTC_halfhour')


if __name__ == '__main__': main()
