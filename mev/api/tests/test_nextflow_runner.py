import unittest.mock as mock
import os
import json
import textwrap
from tempfile import TemporaryDirectory

from django.test import override_settings


from api.tests.base import BaseAPITestCase
from api.runners.nextflow import NextflowRunner, \
    LocalNextflowRunner, \
    AWSBatchNextflowRunner

# the api/tests dir
TESTDIR = os.path.join(os.path.dirname(__file__), 'operation_test_files')
DEMO_TESTDIR = os.path.join(TESTDIR, 'demo_nextflow_workflow')

# used for overriding from settings, but made into variables so we
# can refer to these in multiple places
NEXTFLOW_EXE_OVERRIDE = '/opt/nextflow'
NEXTFLOW_STATUS_UPDATE_OVERRIDE = 'http://localhost:8080/api/nextflow/status-update'


class NextflowRunnerTester(BaseAPITestCase):

    @mock.patch('api.runners.nextflow.edit_nf_containers')
    @mock.patch('api.runners.nextflow.get_container_names')
    @mock.patch('api.runners.nextflow.check_image_name_validity')
    @mock.patch('api.runners.nextflow.check_image_exists')
    def test_operation_prep_makes_proper_calls(self, mock_check_image_exists,
        mock_check_image_name_validity,
        mock_get_container_names,
        mock_edit_nf_containers
    ):
        '''
        Test that the api.runners.nextflow.NextflowRunner.prepare_operation
        method makes the proper calls and raises the appropriate exceptions.
        '''
        nf_runner = NextflowRunner()
        op_dir = '/some/op/dir'
        repo_name = 'repo-name'
        git_hash = 'abc123'

        # check the expected calls are made when everything works
        mock_get_container_names.return_value = ['a','b']
        mock_check_image_name_validity.side_effect = ['final_a', 'final_b']
        mock_check_image_exists.return_value = True
        nf_runner.prepare_operation(op_dir, repo_name, git_hash)
        mock_check_image_name_validity.assert_has_calls([
            mock.call('a', repo_name, git_hash),
            mock.call('b', repo_name, git_hash)
        ])
        mock_edit_nf_containers.assert_called_with(
            op_dir,
            {
                'a': 'final_a',
                'b': 'final_b'
            }
        )

        # check that we raise an exception if the image is not found:
        mock_edit_nf_containers.reset_mock()
        mock_check_image_name_validity.reset_mock()
        mock_get_container_names.return_value = ['a','b']
        mock_check_image_name_validity.side_effect = ['final_a', 'final_b']
        mock_check_image_exists.side_effect = [True, False]
        with self.assertRaisesRegex(Exception, 'final_b'):
            nf_runner.prepare_operation(op_dir, repo_name, git_hash)
        mock_check_image_name_validity.assert_has_calls([
            mock.call('a', repo_name, git_hash),
            mock.call('b', repo_name, git_hash)
        ])
        mock_edit_nf_containers.assert_not_called()

    def test_create_params_json(self):
        '''
        Tests that we properly write the input params JSON file.
        '''
        nf_runner = NextflowRunner()
        mock_convert_inputs = mock.MagicMock()
        v = {'input_a': 'abc', 'input_b': [1,2,3]}
        mock_convert_inputs.return_value = v
        nf_runner._convert_inputs = mock_convert_inputs
        mock_op = mock.MagicMock()
        op_dir = '/some/dir'
        validated_inputs = {'input_a': 'ABC', 'input_b': ['x','y','z']}
        staging_dir = '/tmp'
        nf_input_path = nf_runner._create_params_json(mock_op, op_dir, validated_inputs, staging_dir)
        self.assertEqual(nf_input_path, f'{staging_dir}/{nf_runner.NF_INPUTS}')
        mock_convert_inputs.assert_called_once_with(mock_op, op_dir, validated_inputs, staging_dir)
        j = json.load(open(nf_input_path))
        self.assertEqual(j, v)
        os.remove(nf_input_path)

    @mock.patch('api.runners.nextflow.copy_local_resource')
    def test_create_params_json(self, mock_copy_local_resource):
        '''
        Tests that we properly write the input params JSON file.
        '''
        nf_runner = NextflowRunner()
        nf_path = os.path.join(DEMO_TESTDIR, 'main.nf')
        with TemporaryDirectory() as tmp:
            nf_runner._copy_workflow_contents(DEMO_TESTDIR, tmp)
            mock_copy_local_resource.assert_has_calls([
                mock.call(
                    nf_path,
                    os.path.join(tmp, 'main.nf')
                )
            ])

    @override_settings(OPERATION_LIBRARY_DIR='/some/dir')
    @override_settings(NEXTFLOW_EXE=NEXTFLOW_EXE_OVERRIDE)
    @override_settings(NEXTFLOW_STATUS_UPDATE_URL=NEXTFLOW_STATUS_UPDATE_OVERRIDE)
    @mock.patch('api.runners.nextflow.run_shell_command')
    def test_nf_job_run_call(self, mock_run_shell_command):
        '''
        Tests that we make the proper calls to start the nextflow job
        '''
        nf_runner = NextflowRunner()
        mock_executed_op = mock.MagicMock()
        mock_uuid = 'executed_op_uuid'
        mock_executed_op.id = mock_uuid
        mock_op = mock.MagicMock()
        mock_op.id = 'op_uuid'
        validated_inputs = {'a':1, 'b':2}
        staging_dir = '/some/staging/dir'
        inputs_path = '/some/staging/dir/params.json'
        config_path = '/some/staging/dir/nextflow.config'
        output_dir = '/an/output/dir'

        # mock out methods that are tested elsewhere. Not testing those here- just
        # ensuring they're called as expected
        mock_create_execution_dir = mock.MagicMock()
        mock_create_execution_dir.return_value = staging_dir
        nf_runner._create_execution_dir = mock_create_execution_dir

        mock_copy_workflow_contents = mock.MagicMock()
        mock_copy_workflow_contents.return_value = None
        nf_runner._copy_workflow_contents = mock_copy_workflow_contents

        mock_create_params_json = mock.MagicMock()
        mock_create_params_json.return_value = inputs_path
        nf_runner._create_params_json = mock_create_params_json

        mock_prepare_config_template = mock.MagicMock()
        mock_prepare_config_template.return_value = config_path
        nf_runner._prepare_config_template = mock_prepare_config_template

        mock_get_outputs_dir = mock.MagicMock()
        mock_get_outputs_dir.return_value = output_dir
        nf_runner._get_outputs_dir = mock_get_outputs_dir

        nf_runner.run(mock_executed_op, mock_op, validated_inputs)

        mock_create_execution_dir.assert_called()
        mock_create_params_json.assert_called()
        mock_prepare_config_template.assert_called()
        mock_get_outputs_dir.assert_called()

        expected_cmd = f'{NEXTFLOW_EXE_OVERRIDE} -bg run' \
              f' {os.path.join(staging_dir, "main.nf")}' \
              f' -c {os.path.join(staging_dir, NextflowRunner.CONFIG_FILE_NAME)}' \
              f' -name {mock_uuid}' \
              f' -params-file {os.path.join(staging_dir, NextflowRunner.NF_INPUTS)}' \
              f' --output_dir {output_dir}' \
              f' -with-weblog {NEXTFLOW_STATUS_UPDATE_OVERRIDE}' \
              f' >{os.path.join(staging_dir, NextflowRunner.STDOUT_FILE_NAME)}' \
              F' 2>{os.path.join(staging_dir, NextflowRunner.STDERR_FILE_NAME)}'
        mock_run_shell_command.assert_called_with(expected_cmd)


class AWSBatchNextflowRunnerTester(BaseAPITestCase):
    
    def test_creates_config(self):
        nf_runner = LocalNextflowRunner()
        p = nf_runner._prepare_config_template('/tmp')
        self.assertTrue(p == f'/tmp/{NextflowRunner.CONFIG_FILE_NAME}')
        contents = open(p, 'r').read()
        expected_contents = "process.executor = 'local'\ndocker.enabled = true\nworkDir = '/tmp/work'"
        self.assertTrue(contents == expected_contents)


class LocalNextflowRunnerTester(BaseAPITestCase):

    @override_settings(AWS_BATCH_QUEUE='my-queue')
    @override_settings(AWS_REGION='us-east-2')
    @override_settings(NEXTFLOW_BUCKET_NAME='my-nextflow-bucket')
    def test_creates_config(self):
        nf_runner = AWSBatchNextflowRunner()
        p = nf_runner._prepare_config_template('/tmp')
        self.assertTrue(p == f'/tmp/{NextflowRunner.CONFIG_FILE_NAME}')
        contents = open(p, 'r').read()
        expected_contents = """
        //indicate that we want to use awsbatch
        process.executor = 'awsbatch'

        //indicate the name of the AWS Batch job queue we want to use
        process.queue = 'my-queue'

        //region where we want to run this in
        aws.region = 'us-east-2'

        //Important note!!! Since we created a custom AMI
        //we need to specify the path to the aws cli tool
        aws.batch.cliPath = '/opt/aws-cli/bin/aws'

        //Additionally if we want to use S3 to hold intermediate files we can specify the work directory
        workDir = 's3://my-nextflow-bucket/tmp'

        docker.enabled = true
        """
        expected_contents = textwrap.dedent(expected_contents)
        self.assertTrue(contents == expected_contents)