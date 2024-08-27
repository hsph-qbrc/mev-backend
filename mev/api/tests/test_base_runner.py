import unittest
import unittest.mock as mock
import os
import json
import uuid
from tempfile import NamedTemporaryFile

from django.test import override_settings

from data_structures.operation import Operation

from exceptions import OutputConversionException

from api.tests.base import BaseAPITestCase
from api.runners.base import OperationRunner, \
    TemplatedCommandMixin
from api.models import Resource
from api.converters.basic_attributes import BooleanAsIntegerConverter

# the api/tests dir
TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(TESTDIR, 'operation_test_files')


class BaseRunnerTester(BaseAPITestCase):

    def setUp(self):
        filepath = os.path.join(TESTDIR, 'multiresource_output.json')
        with open(filepath) as fp:
            self.op = Operation(json.load(fp))

    @mock.patch('api.runners.base.import_string')
    def test_input_mapping(self, mock_import_string):

        runner = OperationRunner()
        final_path = '/some/file/path.txt'
        mock_converter1 = mock.MagicMock()
        mock_converter2 = mock.MagicMock()
        mock_converter1.convert_input.return_value = final_path
        mock_converter2.convert_input.return_value = 0.01
        mock_get_converter = mock.MagicMock()
        mock_get_converter.side_effect = [mock_converter1, mock_converter2]
        runner._get_converter = mock_get_converter

        r = Resource.objects.all()[0]
        validated_inputs = {
            'count_matrix': str(r.pk),
            'p_val': 0.01 
        }
        mock_op_dir = '/some/op/dir'
        converted_inputs = runner._convert_inputs(self.op, mock_op_dir, validated_inputs, '')
        expected_outputs = {
            'count_matrix': final_path,
            'p_val': 0.01 
        }
        self.assertDictEqual(converted_inputs, expected_outputs)


    @mock.patch('api.runners.base.alert_admins')
    def test_bad_keys(self, mock_alert_admins):
        '''
        This tests that differing keys between the operation spec 
        and the actual outputs are handled appropriately.
        '''

        runner = OperationRunner()
        mock_cleanup = mock.MagicMock()
        runner.cleanup_on_error = mock_cleanup

        # this is missing a dge_table key
        bad_outputs = {
            "norm_counts": "/path/to/norm_counts.tsv"
        }
        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'
        mock_converter =  mock.MagicMock()
        with self.assertRaisesRegex(OutputConversionException, 
            'Could not locate the output'):
            runner._convert_outputs(
                mock_executed_op,
                self.op,
                bad_outputs
            )

        mock_cleanup.assert_called_with(
            self.op.outputs,
            {}
        )
        mock_alert_admins.assert_called()

        # run another test here-- now there will be a
        # resource which needs cleanup so that we don't leave any 
        # 'hanging' outputs.
        mock_cleanup.reset_mock()
        mock_alert_admins.reset_mock()

        # this is missing a norm_counts key
        bad_outputs = {
            "dge_table": "/path/to/dge.tsv"
        }
        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'
        mock_converter = mock.MagicMock()
        u = str(uuid.uuid4())
        mock_converter.convert_output.return_value = u
        mock_get_converter = mock.MagicMock()
        mock_get_converter.return_value = mock_converter
        runner._get_converter = mock_get_converter
        with self.assertRaisesRegex(OutputConversionException, 
        'Could not locate the output') as ex:
            runner._convert_outputs(
                mock_executed_op,
                self.op,
                bad_outputs
            )
        mock_cleanup.assert_called_with(
            self.op.outputs,
            {
                'dge_table': u
            }
        )
        mock_alert_admins.assert_called()

    def test_output_conversion_for_non_resource_types(self):
        '''
        This tests that proper/valid output conversions go as planned
        for the "primitive" types
        '''

        filepath = os.path.join(TESTDIR, 'non_resource_outputs.json')
        op = Operation(json.load(open(filepath)))

        runner = OperationRunner()
        mock_cleanup = mock.MagicMock()
        runner.cleanup_on_error = mock_cleanup

        # these outputs correspond to the multiresource_output.json
        outputs = {
            "pval": 0.01,
            "some_integer": 5,
            "some_bool": True
        }

        # don't bother mocking out the get_converter
        # since the op spec has valid converters

        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'

        d = runner._convert_outputs(
            mock_executed_op,
            op,
            outputs
        )
        self.assertDictEqual(
            d,
            {'pval': 0.01, 'some_bool': 1, 'some_integer': 5}
        )
        mock_cleanup.assert_not_called()

    def test_output_conversion(self):
        '''
        This tests that proper/valid output conversions go as planned
        '''
        runner = OperationRunner()
        mock_cleanup = mock.MagicMock()
        runner.cleanup_on_error = mock_cleanup

        # these outputs correspond to the multiresource_output.json
        outputs = {
            "norm_counts": "/path/to/norm_counts.tsv" ,
            "dge_table": {
                "path": "/path/to/dge_table.tsv",
                "resource_type": "MTX"
            }
        }
        u1 = str(uuid.uuid4())
        u2 = str(uuid.uuid4())

        mock_converter1 = mock.MagicMock()
        mock_converter1.convert_output.return_value = u1
        mock_converter2 = mock.MagicMock()
        mock_converter2.convert_output.return_value = u2
        mock_get_converter = mock.MagicMock()
        mock_get_converter.side_effect = [mock_converter1, mock_converter2]
        runner._get_converter = mock_get_converter

        # mock_converter =  mock.MagicMock()
        # # The ordering below is set b/c the convert_outputs
        # # performs a sort on the keys of the operation's output spec
        # mock_converter.convert_output.side_effect = [
        #     u1, # this will be the UUID for dge_Table
        #     u2 # the UUID for norm_counts
        # ]

        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'

        d = runner._convert_outputs(
            mock_executed_op,
            self.op,
            outputs
        )
        self.assertDictEqual(
            d,
            {
                # note the ordering is due to a sorting
                # we perform on the keys of the output dict.
                # Since dge_table comes first when sorted, it gets
                # u1 for this test.
                'norm_counts': u2,
                'dge_table': u1
            }
        )
        mock_cleanup.assert_not_called()


    def test_output_conversion_error(self):
        '''
        If any of the outputs do not pass validation (either b/c the tool developer
        did not create outputs.json correctly OR the file is corrupted and doesn't
        pass validation) then we have to do some cleanup so we don't have 'incomplete'
        outputs. If only a portion of the outputs validated, the final results may not
        make sense.

        This test checks that the cleanup method is called when an error occurs. Here, since
        the first output fails validation, there is effectively nothing to clean up. (Recall that 
        the cleanup method here really only operates on OTHER inputs which did validate
        successfully)
        '''

        runner = OperationRunner()
        mock_cleanup = mock.MagicMock()
        runner.cleanup_on_error = mock_cleanup

        # these outputs correspond to the multiresource_output.json
        outputs = {
            "norm_counts": "/path/to/norm_counts.tsv",
            "dge_table": "/path/to/dge.tsv"
        }

        mock_converter1 = mock.MagicMock()
        mock_converter1.convert_output.side_effect = OutputConversionException('ack')
        mock_converter2 = mock.MagicMock()
        mock_converter2.convert_output.side_effect = OutputConversionException('ack')
        mock_get_converter = mock.MagicMock()
        mock_get_converter.side_effect = [mock_converter1, mock_converter2]
        runner._get_converter = mock_get_converter

        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'

        with self.assertRaises(OutputConversionException) as ex:
            runner._convert_outputs(
                mock_executed_op,
                self.op,
                outputs
            )
            print(ex)

        mock_cleanup.assert_called_with(
            self.op.outputs,
            {}
        )

    def test_output_conversion_error_case2(self):
        '''
        If any of the outputs do not pass validation (either b/c the tool developer
        did not create outputs.json correctly OR the file is corrupted and doesn't
        pass validation) then we have to do some cleanup so we don't have 'incomplete'
        outputs. If only a portion of the outputs validated, the final results may not
        make sense.

        This test checks that the cleanup method is called when an error occurs
        '''
        runner = OperationRunner()
        mock_cleanup = mock.MagicMock()
        runner.cleanup_on_error = mock_cleanup

        # these outputs correspond to the multiresource_output.json
        outputs = {
            "norm_counts": "/path/to/norm_counts.tsv" ,
            "dge_table": {
                "path": "/path/to/dge_table.tsv",
                "resource_type": "MTX"
            }
        }
        u = str(uuid.uuid4())

        mock_converter1 = mock.MagicMock()
        mock_converter1.convert_output.return_value = u
        mock_converter2 = mock.MagicMock()
        mock_converter2.convert_output.side_effect = OutputConversionException('ack')
        mock_get_converter = mock.MagicMock()
        mock_get_converter.side_effect = [mock_converter1, mock_converter2]
        runner._get_converter = mock_get_converter

        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'

        with self.assertRaises(OutputConversionException) as ex:
            runner._convert_outputs(
                mock_executed_op,
                self.op,
                outputs
            )

        mock_cleanup.assert_called_with(
            self.op.outputs,
            {
                'dge_table': u
            }
        )

    @mock.patch('api.runners.base.alert_admins')
    def test_output_conversion_error_case3(self, mock_alert_admins):
        '''
        If the outputs of a tool contain extra keys (but is otherwise OK)
        we warn the admins and move on silently. No cleanup is called.
        '''
        runner = OperationRunner()
        mock_cleanup = mock.MagicMock()
        runner.cleanup_on_error = mock_cleanup

        # these outputs correspond to the multiresource_output.json
        outputs = {
            "norm_counts": "/path/to/norm_counts.tsv" ,
            "dge_table": {
                "path": "/path/to/dge_table.tsv",
                "resource_type": "MTX"
            },
            "extra_key": 123 # <--- extra, not in spec
        }
        
        u1 = str(uuid.uuid4())
        u2 = str(uuid.uuid4())
        mock_converter1 = mock.MagicMock()
        mock_converter1.convert_output.return_value = u1
        mock_converter2 = mock.MagicMock()
        mock_converter2.convert_output.return_value = u2
        mock_get_converter = mock.MagicMock()
        mock_get_converter.side_effect = [mock_converter1, mock_converter2]
        runner._get_converter = mock_get_converter

        mock_executed_op = mock.MagicMock()
        mock_executed_op.operation.id = 'abc'

        result = runner._convert_outputs(
            mock_executed_op,
            self.op,
            outputs
        )

        self.assertDictEqual(
            result,
            {
                'dge_table': u1,
                'norm_counts': u2
            }
        )
        mock_alert_admins.assert_called()
        # we don't call cleanup since the extra output
        # did not affect the known/valid outputs
        mock_cleanup.assert_not_called()

    def test_handles_bad_converter_gracefully(self):
        '''
        In the case that a converter class is not found for the input 
        (mocked by raising an exception from the _get_converter method)
        see that we get an exception raised.
        '''
        # need a resource to populate the field
        all_r = Resource.objects.all()
        r = all_r[0]
        inputs = {
            'count_matrix': str(r.id),
            'p_val': 0.05
        }

        runner = OperationRunner()
        mock_get_converter = mock.MagicMock()
        mock_get_converter.side_effect = Exception('!!!')
        runner._get_converter = mock_get_converter
        with self.assertRaises(Exception):
            runner._convert_inputs(self.op, '', inputs, '')

    def test_converter_class_import(self):
        '''
        Tests that the converter import works as expected
        '''
        runner = OperationRunner()
        c = runner._get_converter(
            'api.converters.basic_attributes.BooleanAsIntegerConverter')
        b = BooleanAsIntegerConverter()
        self.assertTrue(type(c) == type(b))

        with self.assertRaisesRegex(Exception, 'Failed when importing'):
            c = runner._get_converter('some.garbage')

    @mock.patch('api.runners.base.delete_resource_by_pk')
    def test_cleanup_on_error(self, mock_delete):
        runner = OperationRunner()
        u1 = str(uuid.uuid4())
        u2 = str(uuid.uuid4())
        mock_outputs = {
            'norm_counts': u1,
            'dge_table': u2
        }
        runner.cleanup_on_error(self.op.outputs, mock_outputs)
        mock_delete.assert_has_calls([
            mock.call(u1),
            mock.call(u2)
        ])    

    @override_settings(OPERATION_EXECUTION_DIR='/some/fake/dir')
    @mock.patch('api.runners.base.rmtree')
    def test_cleanup_call(self, mock_rmtree):
        '''
        If a job completes successfully, we can delete the execution
        directory since no debugging will be required. 

        Child classes of OperationRunner will call the method tested here
        '''
        runner = OperationRunner()
        mock_job_id = str(uuid.uuid4())
        runner._clean_following_success(mock_job_id)
        mock_rmtree.assert_called_with(f'/some/fake/dir/{mock_job_id}')


class TemplatedCommandMixinTester(BaseAPITestCase):

    def test_entrypoint_command_creation(self):
        '''
        Tests that we create the entrypoint command
        from the template as expected
        '''
        input_path = '/home/xyz/input.tsv'
        a_arg = 10
        cmd = b'Rscript something.R {{input_file}} -a {{a_arg}}'
        expected_cmd = 'Rscript something.R ' + \
            input_path + ' -a ' + str(a_arg)
        runner = TemplatedCommandMixin()
        with NamedTemporaryFile() as tf:
            tf.write(cmd)
            tf.seek(0)
            fname = tf.name
            arg_dict = {
                'a_arg': a_arg,
                'input_file': input_path
            }
            final_cmd = runner._get_entrypoint_command(fname, arg_dict)
            self.assertEqual(final_cmd, expected_cmd)