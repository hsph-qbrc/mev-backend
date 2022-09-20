import unittest.mock as mock
import os
import json
import uuid

from django.core.exceptions import ImproperlyConfigured
from rest_framework.exceptions import ValidationError

from api.utilities.operations import read_operation_json, \
    validate_operation_inputs, \
    resource_operations_file_is_valid
from api.tests.base import BaseAPITestCase
from api.models import Operation as OperationDbModel
from api.models import Workspace

# the api/tests dir
TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(TESTDIR, 'operation_test_files')

class OperationUtilsTester(BaseAPITestCase):

    def setUp(self):
        self.filepath = os.path.join(TESTDIR, 'valid_workspace_operation.json')
        fp = open(self.filepath)
        self.valid_dict = json.load(fp)
        fp.close()


        # the function we are testing requires an Operation database instance
        # Note, however, that we mock out the function that converts that into
        # the dictionary object describing the Operation. Hence, the actual
        # database instance we pass to the function does not matter.
        # Same goes for the Workspace (at least in this test).
        self.db_op = OperationDbModel.objects.create(id=uuid.uuid4())

        workspaces = Workspace.objects.all()
        if len(workspaces) == 0:
            raise ImproperlyConfigured('Need at least one Workspace to run this test.')
        self.workspace = workspaces[0]
        self.establish_clients()

    @mock.patch('api.utilities.operations.read_local_file')
    def test_read_operation_json(self, mock_read_local_file):

        # test that a properly formatted file returns 
        # a dict as expected:

        fp = open(self.filepath)
        mock_read_local_file.return_value = fp
        d = read_operation_json(self.filepath)
        self.assertDictEqual(d, self.valid_dict)

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_user_input_validation(self, mock_get_operation_instance_data):
        '''
        Test that we receive back an appropriate object following
        successful validation. All the inputs below are valid
        '''
        f = os.path.join(
            TESTDIR,
            'sample_for_basic_types_no_default.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d

        # some valid user inputs corresponding to the input specifications
        sample_inputs = {
            'int_no_default_type': 10, 
            'positive_int_no_default_type': 3, 
            'nonnegative_int_no_default_type': 0, 
            'bounded_int_no_default_type': 2, 
            'float_no_default_type':0.2, 
            'bounded_float_no_default_type': 0.4, 
            'positive_float_no_default_type': 0.01, 
            'nonnegative_float_no_default_type': 0.1, 
            'string_no_default_type': 'abc', 
            'boolean_no_default_type': True
        }

        workspaces = Workspace.objects.all()
        if len(workspaces) == 0:
            raise ImproperlyConfigured('Need at least one Workspace to run this test.')
        validate_operation_inputs(self.regular_user_1, 
            sample_inputs, self.db_op, self.workspace)

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_no_default_for_required_param(self, mock_get_operation_instance_data):
        '''
        Test that a missing required parameter triggers a validation error
        '''
        f = os.path.join(
            TESTDIR,
            'required_without_default.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d

        # one input was optional, one required. An empty payload
        # qualifies as a problem since it's missing the required key
        sample_inputs = {}

        with self.assertRaises(ValidationError):
            validate_operation_inputs(self.regular_user_1, 
                sample_inputs, self.db_op, self.workspace)

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_optional_value_filled_by_default(self, mock_get_operation_instance_data):
        '''
        Test that a missing optional parameter gets the default value
        '''
        f = os.path.join(
            TESTDIR,
            'required_without_default.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d

        # one input was optional, one required. An empty payload
        # qualifies as a problem since it's missing the required key
        sample_inputs = {'required_int_type': 22}

        final_inputs = validate_operation_inputs(self.regular_user_1, 
            sample_inputs, self.db_op, self.workspace)
        self.assertEqual(final_inputs['required_int_type'].submitted_value, 22)
        expected_default = d['inputs']['optional_int_type']['spec']['default']
        self.assertEqual(
            final_inputs['optional_int_type'].submitted_value, expected_default)

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_optional_value_overridden(self, mock_get_operation_instance_data):
        '''
        Test that the optional parameter is overridden when given
        '''
        f = os.path.join(
            TESTDIR,
            'required_without_default.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d

        sample_inputs = {
            'required_int_type': 22,
            'optional_int_type': 33
        }

        final_inputs = validate_operation_inputs(self.regular_user_1, 
            sample_inputs, self.db_op, self.workspace)
        self.assertEqual(final_inputs['required_int_type'].submitted_value, 22)
        self.assertEqual(final_inputs['optional_int_type'].submitted_value, 33)

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_optional_without_default_becomes_none(self, mock_get_operation_instance_data):
        '''
        Generally, Operations with optional inputs should have defaults. However,
        if that is violated, the "input" should be assigned to be None
        '''
        f = os.path.join(
            TESTDIR,
            'optional_without_default.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d

        # the only input is optional, so this is technically fine.
        sample_inputs = {}

        #with self.assertRaises(ValidationError):
        final_inputs = validate_operation_inputs(self.regular_user_1, 
            sample_inputs, self.db_op, self.workspace)
        self.assertIsNone(final_inputs['optional_int_type'])

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_list_attr_inputs(self, mock_get_operation_instance_data):
        '''
        Test the case where inputs are of a list type (e.g. a list of strings)
        Check that it all validates as expected
        '''
        # first test one where we expect an empty list-- no resources
        # are used or created:
        f = os.path.join(
            TESTDIR,
            'valid_op_with_list_inputs.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d
        l1 = ['https://foo.com/bar', 'https://foo.com/baz']
        l2 = ['abc', 'def']
        inputs = {
            'link_list': l1,
            'regular_string_list': l2
        }
        ops = OperationDbModel.objects.all()
        op = ops[0]
        result = validate_operation_inputs(self.regular_user_1,
                inputs, op, None)
        self.assertIsNone(result['optional_input'])
        self.assertCountEqual(result['link_list'].get_value(), l1)
        self.assertCountEqual(result['regular_string_list'].get_value(), l2)
        
    def test_resource_operation_file_formatting(self):
        '''
        Test that we correctly parse resource operation specification files
        and reject those that do not conform to the spec.
        '''

        good_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileB',
                    'path':'/path/to/B.txt',
                    'resource_type':'MTX'
                }
            ]
        }
        inputs = {
            'inputA': '' # doesn't matter what the key points at. Only need the name
        }
        self.assertTrue(resource_operations_file_is_valid(good_op_resource_data, inputs.keys()))

        # change the inputs to have two necessary keys
        inputs = {
            'inputA': '',
            'inputB': ''
        }
        # should be false since we only have inputA in our "spec"
        self.assertFalse(resource_operations_file_is_valid(good_op_resource_data, inputs.keys()))

        bad_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileB',
                    'path':'/path/to/B.txt',
                    'resource_type':'MTX'
                }
            ],
            # should point at a list, but here points at a dict, which
            # could be a common formatting mistake
            'inputB': {
                'name':'fileC',
                'path':'/path/to/C.txt',
                'resource_type':'MTX'     
            }
        }

        # inputs to two necessary keys
        inputs = {
            'inputA': '',
            'inputB': ''
        }
        self.assertFalse(resource_operations_file_is_valid(bad_op_resource_data, inputs.keys()))

        good_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileB',
                    'path':'/path/to/B.txt',
                    'resource_type':'MTX'
                }
            ],
            'inputB': [
                {
                    'name':'fileC',
                    'path':'/path/to/C.txt',
                    'resource_type':'MTX'     
                }
            ]
        }

        # inputs to two necessary keys
        inputs = {
            'inputA': '',
            'inputB': ''
        }
        self.assertTrue(resource_operations_file_is_valid(good_op_resource_data, inputs.keys()))

        bad_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileB',
                    'path':'/path/to/A.txt', # path is the same as above. Not allowed.
                    'resource_type':'MTX'
                }
            ]
        }

        inputs = {
            'inputA': '',
        }
        self.assertFalse(resource_operations_file_is_valid(bad_op_resource_data, inputs.keys()))

        bad_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileA', # name matches above. Not allowed.
                    'path':'/path/to/B.txt',
                    'resource_type':'MTX'
                }
            ]
        }

        inputs = {
            'inputA': '',
        }
        self.assertFalse(resource_operations_file_is_valid(bad_op_resource_data, inputs.keys()))

        good_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileB',
                    'path':'/path/to/B.txt',
                    'resource_type':'MTX'
                }
            ],
            'inputB': [
                {
                    # the name/path match above, but since it's part of
                    # a different input, this is fine.
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'     
                }
            ]
        }
        inputs = {
            'inputA': '',
            'inputB': ''
        }
        self.assertTrue(resource_operations_file_is_valid(good_op_resource_data, inputs.keys()))

        # one of the 'resources' is missing the resource_type key
        bad_op_resource_data = {
            'inputA': [
                {
                    'name':'fileA',
                    'path':'/path/to/A.txt',
                    'resource_type':'MTX'
                },
                {
                    'name':'fileA', # name matches above. Not allowed.
                    'path':'/path/to/B.txt'
                }
            ]
        }

        inputs = {
            'inputA': '',
        }
        self.assertFalse(resource_operations_file_is_valid(bad_op_resource_data, inputs.keys()))

    @mock.patch('api.utilities.operations.get_operation_instance_data')
    def test_optional_boolean_value_filled_by_default(self, mock_get_operation_instance_data):
        '''
        Test that a missing optional boolean parameter gets the default value
        '''
        f = os.path.join(
            TESTDIR,
            'valid_op_with_default_bool.json'
        )
        d = read_operation_json(f)
        mock_get_operation_instance_data.return_value = d

        # one input was optional, one required. An empty payload
        # qualifies as a problem since it's missing the required key
        sample_inputs = {}

        final_inputs = validate_operation_inputs(self.regular_user_1, 
            sample_inputs, self.db_op, self.workspace)
        self.assertEqual(final_inputs['some_boolean'].submitted_value, False)
        expected_default = d['inputs']['some_boolean']['spec']['default']
        self.assertEqual(
            final_inputs['some_boolean'].submitted_value, expected_default)