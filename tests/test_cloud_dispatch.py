import unittest
import json
from pathlib import Path
import tempfile
from mardorf_collector.transfer.dispatch import select_route
from mardorf_collector.runtime.cloud_environment import environment


class DispatchTests(unittest.TestCase):
    def test_location_uses_explicit_variables_with_no_credential_defaults(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'config').mkdir()
            path=root/'config/dev03_cloud_runtime_v1.json'
            path.write_text(json.dumps({'b2_location':{'B2_ENDPOINT_URL':'https://s3.region.backblazeb2.com',
                                                      'B2_REGION':'region','B2_BUCKET':'fixture-bucket'}}))
            env=environment(root,environ={'B2_REGION':'explicit-region','B2_APPLICATION_KEY':'fixture-key'})
            self.assertEqual(env['B2_REGION'],'explicit-region')
            self.assertEqual(env['B2_BUCKET'],'fixture-bucket')
            self.assertEqual(env['B2_APPLICATION_KEY'],'fixture-key')
            self.assertNotIn('B2_APPLICATION_KEY_ID',env)
            path.write_text('{}')
            with self.assertRaises(ValueError):environment(root,environ={})

    def test_explicit_authority_and_manual_canary_keep_production_branch_separate(self):
        config={'private_repository':'fixture/private','production_enabled':False}
        self.assertEqual(select_route(environ={},specification=config),('git',None))
        config['production_enabled']=True
        self.assertEqual(select_route(environ={},specification=config),('cloud',None))
        env={'GITHUB_EVENT_NAME':'workflow_dispatch','GITHUB_RUN_ID':'123','GITHUB_RUN_ATTEMPT':'2'}
        self.assertEqual(select_route(canary=True,environ=env,specification=config),('cloud','codex/b2-p05-canary-123-2'))
        for bad in ({'PRIVATE_REPO':'wrong/repo'},{'CLOUD_CONTROL_BRANCH':'main'},{'CLOUD_CONTROL_PATH':'other'}):
            with self.assertRaises(ValueError):select_route(environ=bad,specification=config)
        for key,value in (('GITHUB_EVENT_NAME','schedule'),('GITHUB_RUN_ID','../main'),('GITHUB_RUN_ATTEMPT','')):
            with self.assertRaises(ValueError):select_route(canary=True,environ=dict(env,**{key:value}),specification=config)
