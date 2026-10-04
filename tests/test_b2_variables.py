import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('variables',Path(__file__).parents[1]/'tools/b2_variables.py')
v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
class VariableTests(unittest.TestCase):
    def test_inspect_never_writes_and_permission_denial_is_distinct(self):
        with patch.object(v,'request',side_effect=[(403,{}),(404,{})]) as request:
            result=v.inspect('credential')
        self.assertEqual([r['read_http_status'] for r in result['variables']],[403,404])
        self.assertTrue(all(len(c.args)==2 for c in request.call_args_list))
    def test_prepare_creates_missing_as_false_and_verifies_readback(self):
        with patch.object(v,'request',side_effect=[(404,{}),(201,{}),(200,{'name':v.NAME,'value':'false'}),(403,{})]) as request:
            result=v.inspect('credential',prepare=True)
        self.assertEqual(request.call_args_list[1].args[2:],('POST','false'))
        self.assertTrue(result['variables'][0]['write_readback_verified']);self.assertFalse(result['production_activated'])
    def test_existing_value_is_preserved_even_when_production_already_active(self):
        with patch.object(v,'request',side_effect=[(200,{'name':v.NAME,'value':'true'}),(204,{}),(200,{'name':v.NAME,'value':'true'}),(403,{})]) as request:
            v.inspect('credential',prepare=True)
        self.assertEqual(request.call_args_list[1].args[2:],('PATCH','true'))
    def test_changed_readback_is_rejected_without_second_write(self):
        with patch.object(v,'request',side_effect=[(404,{}),(201,{}),(200,{'name':v.NAME,'value':'true'})]) as request:
            with self.assertRaises(ValueError):v.inspect('credential',prepare=True)
        self.assertEqual(len(request.call_args_list),3)
    def test_non_boolean_switch_is_rejected(self):
        with patch.object(v,'request',return_value=(200,{'name':v.NAME,'value':'yes'})):
            with self.assertRaises(ValueError):v.inspect('credential',prepare=True)
