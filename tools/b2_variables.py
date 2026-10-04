#!/usr/bin/env python3
"""Inspect/prepare the private B2 job switch with the existing credential.

Preparation preserves existing values and creates an absent switch as false.
It never activates production or prints the token. No other variables change.
The public collector uses its profile; it has no Actions variable switch.
"""
import argparse
import json
import os
import urllib.error
import urllib.request

REPOSITORIES = ('janwohlers78/mardorf-kitevorhersage',)
NAME = 'B2_PRODUCTION_ENABLED'


def request(token, repository, method='GET', value=None):
    path='/actions/variables'+('' if method=='POST' else '/'+NAME)
    body=None if value is None else json.dumps({'name':NAME,'value':value}).encode()
    req=urllib.request.Request('https://api.github.com/repos/'+repository+path,data=body,method=method,
        headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json',
                 'X-GitHub-Api-Version':'2022-11-28','Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=20) as response:
            raw=response.read(16*1024+1)
            if len(raw)>16*1024:raise ValueError('Variable response budget exceeded')
            return response.status,json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        if exc.code in (401,403,404,409,422):return exc.code,{}
        raise


def inspect(token, *, prepare=False):
    if not token:raise ValueError('Cross-repository credential absent')
    rows=[]
    for repository in REPOSITORIES:
        status,body=request(token,repository)
        row={'repository':repository,'read_http_status':status,'name':NAME,'production_activated':False}
        if status==200:
            if body.get('name')!=NAME or body.get('value') not in ('true','false'):
                raise ValueError('Explicit Boolean B2 activation variable required')
            row['value']=body['value']
        if prepare and status in (200,404):
            value=row.get('value','false')
            # Same-value readback verifies this credential's write capability;
            # missing switches default to the already effective inactive state.
            written,_=request(token,repository,'PATCH' if status==200 else 'POST',value)
            row['write_http_status']=written
            if written in (201,204):
                verified,after=request(token,repository)
                if verified!=200 or after.get('name')!=NAME or after.get('value')!=value:
                    raise ValueError('Activation variable exact readback failed')
                row.update(value=value,write_readback_verified=True)
        rows.append(row)
    return {'schema_version':1,'artifact_version':'b2-variable-access-v1',
            'operation':'prepare' if prepare else 'inspect','variables':rows,
            'production_activated':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare',action='store_true')
    args=parser.parse_args()
    print(json.dumps(inspect(os.environ.get('CROSS_REPO_TOKEN',''),prepare=args.prepare),sort_keys=True))


if __name__=='__main__':main()
