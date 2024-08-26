import unittest.mock as mock
import os
import uuid
import time
import shlex
from tempfile import TemporaryDirectory, TemporaryFile

from django.test import override_settings

from api.tests.base import BaseAPITestCase
from api.runners.ecs import ECSRunner
from api.models import ECSTaskDefinition, \
    Operation as OperationDb
from exceptions import MissingRequiredFileException

from data_structures.operation import Operation

from api.utilities.operations import read_operation_json

TESTDIR = os.path.dirname(__file__)


class ECSRunnerTester(BaseAPITestCase):

    @mock.patch('api.runners.ecs.check_image_exists')
    def test_fails_if_image_does_not_exist(self, mock_check_image_exists):
        '''
        Image should exist in the container repo- if it
        doesn't yet, then fail out and warn
        '''
        runner = ECSRunner()
        op_dir = '/some/op/dir'
        repo_name = 'repo-name'
        git_hash = 'abc123'
        mock_op_db_obj = mock.MagicMock()

        mock_check_image_exists.return_value = False
        with self.assertRaisesRegex(Exception, 'Could not locate'):
            runner.prepare_operation(mock_op_db_obj, op_dir, repo_name, git_hash)

    def test_task_registration_failure(self):
        '''
        If the ECS task registration fails for some
        reason, check that we fail and warn.
        '''
        runner = ECSRunner()
        op_dir = '/some/op/dir'
        repo_name = 'repo-name'
        git_hash = 'abc123'
        mock_op_db_obj = mock.MagicMock()

        mock_check_image = mock.MagicMock()
        mock_check_image.return_value = 'docker-img-url'
        runner._check_for_image = mock_check_image
        mock_register = mock.MagicMock()
        mock_register.side_effect = Exception('!!!')
        runner._register_new_task = mock_register
        with self.assertRaisesRegex(Exception, '!!!'):
            runner.prepare_operation(mock_op_db_obj, op_dir, repo_name, git_hash)
        mock_register.assert_called_once_with(repo_name, op_dir, 'docker-img-url')

    def test_missing_file(self):
        # mock there being only an entrypoint file, but no
        # file specifying resource requirements
        with TemporaryDirectory() as td:
            entrypoint_file = os.path.join(td, 'entrypoint.txt')
            runner = ECSRunner()
            with self.assertRaisesRegex(MissingRequiredFileException, 'resources.json'):
                runner.check_required_files(td)
        

    def test_malformatted_resource_json(self):
        '''
        The resource JSON file dictates the cpu/mem requirements
        for a particular ECS task
        '''
        runner = ECSRunner()
        with TemporaryFile() as tf:
            tf.write(b'{"cpu":5, "mem_mb": 45}')
            tf.seek(0)
            result = runner._get_resource_requirements(tf)
            self.assertDictEqual(result, {'cpu':5, 'mem_mb': 45})
                
        # test if not a JSON format file
        with TemporaryFile() as tf:
            tf.write(b'abc')
            tf.seek(0)
            with self.assertRaisesRegex(Exception, 'Failed to parse the resource JSON'):
                runner._get_resource_requirements(tf)

        # test if a required key (mem_mb) is missing
        with TemporaryFile() as tf:
            tf.write(b'{"cpu":5}')
            tf.seek(0)
            with self.assertRaisesRegex(Exception, 'mem_mb'):
                runner._get_resource_requirements(tf)

    # note that we mock out the settings rather than override 
    # since we don't need the actual settings values for the test
    @mock.patch('api.runners.ecs.settings')
    @mock.patch('api.runners.ecs.boto3')
    def test_task_registration(self, mock_boto3, mock_settings):
                
        mock_arn = 'arn::123'
        mock_response = {'taskDefinition':
                            {
                                'some_key': 'abc', 
                                'taskDefinitionArn': mock_arn
                            }
                        }
        mock_client = mock.MagicMock()
        mock_register_task_definition = mock.MagicMock()
        mock_register_task_definition.return_value = mock_response
        mock_client.register_task_definition = mock_register_task_definition
        mock_boto3.client.return_value = mock_client

        runner = ECSRunner()
        mock_get_container_defs = mock.MagicMock()
        mock_get_container_defs.return_value = []
        runner._get_container_defs = mock_get_container_defs
        result = runner._register_new_task('','','')
        self.assertEqual(result, mock_arn)


    def test_file_mapping(self):

        f = os.path.join(
            TESTDIR,
            'operation_test_files',
            'op_with_single_and_multiple_resource_inputs.json'
        )
        d = read_operation_json(f)
        op = Operation(d)
        
        mock_inputs = {
            'input_matrices': [
                's3://mybucket/obj_a.tsv',
                's3://mybucket/obj_b.tsv'
            ],
            'primary_matrix': 's3://mybucket/obj_c.tsv',
            'num_neighbors': 10,
            'alpha': 0.3,
            'num_clusters': 4
        }

        runner = ECSRunner()
        ex_op_uuid = '123-abc'
        result = runner._create_file_mapping(ex_op_uuid, op, mock_inputs)
        # only includes file-like keys
        self.assertCountEqual(result.keys(), ['input_matrices', 'primary_matrix'])
        self.assertTrue(len(result['input_matrices']) == 2)
        # will fail if not UUIDs. Note we strip off the prefix dictating the directory
        # in the EFS volume
        prefix = f'{ECSRunner.EFS_DATA_DIR}/{ex_op_uuid}'
        [uuid.UUID(x[len(prefix) + 1:]) for x in result['input_matrices']]
        uuid.UUID(result['primary_matrix'][len(prefix) + 1:])

    def test_input_copy_overrides_single(self):
        '''
        Tests that the proper commands are constructed
        for the initial step of the ECS task (copies)
        '''
        ex_op_uuid = '123-abc'
        mock_inputs = {
            'primary_matrix': 's3://mybucket/obj_A.tsv',
            'num_neighbors': 10,
            'alpha': 0.3,
            'num_clusters': 4
        }

        mapping = {
            'primary_matrix': f'{ECSRunner.EFS_DATA_DIR}/{ex_op_uuid}/uuid1'
        }

        runner = ECSRunner()
        data_dir = ECSRunner.EFS_DATA_DIR
        cmd = runner._create_input_copy_overrides(ex_op_uuid, mock_inputs, mapping)
        self.assertEqual(
            f'mkdir -p {data_dir}/{ex_op_uuid} && {ECSRunner.AWS_CLI_PATH} s3 cp s3://mybucket/obj_A.tsv {data_dir}/{ex_op_uuid}/uuid1', 
            cmd[0]
        )

    def test_input_copy_overrides_multiple(self):
        '''
        Tests that the proper commands are constructed
        for the initial step of the ECS task (copies)
        '''
        ex_op_uuid = '123-abc'
        data_dir = ECSRunner.EFS_DATA_DIR
        mock_inputs = {
            'input_matrices': [
                's3://mybucket/obj_a.tsv',
                's3://mybucket/obj_b.tsv'
            ],
            'primary_matrix': 's3://mybucket/obj_c.tsv',
            'num_neighbors': 10,
            'alpha': 0.3,
            'num_clusters': 4
        }

        mapping = {
            'input_matrices': [
                f'{data_dir}/{ex_op_uuid}/uuid1',
                f'{data_dir}/{ex_op_uuid}/uuid2'
            ], 
            'primary_matrix': f'{data_dir}/{ex_op_uuid}/uuid3'
        }

        runner = ECSRunner()
        cmd = runner._create_input_copy_overrides(ex_op_uuid, mock_inputs, mapping)
        expected_cp_commands = [
            f'{ECSRunner.AWS_CLI_PATH} s3 cp s3://mybucket/obj_a.tsv {data_dir}/{ex_op_uuid}/uuid1',
            f'{ECSRunner.AWS_CLI_PATH} s3 cp s3://mybucket/obj_b.tsv {data_dir}/{ex_op_uuid}/uuid2',
            f'{ECSRunner.AWS_CLI_PATH} s3 cp s3://mybucket/obj_c.tsv {data_dir}/{ex_op_uuid}/uuid3'
        ]
        cp_str = f'mkdir -p {data_dir}/{ex_op_uuid} && {expected_cp_commands[0]} && {expected_cp_commands[1]} && {expected_cp_commands[2]}'
        self.assertEqual(cp_str, cmd[0])

    @override_settings(OPERATION_LIBRARY_DIR='/data/operations')
    def test_run(self):
        '''
        Tests that the proper calls are made when `run`
        is called
        '''
        runner = ECSRunner()
        mock_create_execution_dir =  mock.MagicMock()
        mock_staging_dir = '/data/ex_op/<UUID>'
        mock_create_execution_dir.return_value = mock_staging_dir
        mock_convert_inputs = mock.MagicMock()
        mock_converted_inputs = {'a': 'bucket_path', 'b':2}
        mock_convert_inputs.return_value = mock_converted_inputs
        mock_create_file_mapping = mock.MagicMock()
        mock_file_mapping = {'a': 'efs_path'}
        mock_create_file_mapping.return_value = mock_file_mapping
        mock_create_input_copy_overrides = mock.MagicMock()
        input_copy_overrides = ['input cp']
        mock_create_input_copy_overrides.return_value = input_copy_overrides
        mock_create_output_copy_overrides = mock.MagicMock()
        output_copy_overrides = ['output cp']
        mock_create_output_copy_overrides.return_value = output_copy_overrides
        mock_get_entrypoint_cmd = mock.MagicMock()
        mock_cmd = 'python3 /usr/local/bin/myscript -i /data/foo.tsv'
        mock_get_entrypoint_cmd.return_value = mock_cmd
        mock_get_task_def_arn = mock.MagicMock()
        mock_task_def_arn = 'arn::123'
        mock_get_task_def_arn.return_value = mock_task_def_arn
        mock_submit_to_ecs = mock.MagicMock()
        runner._create_execution_dir = mock_create_execution_dir
        runner._convert_inputs = mock_convert_inputs
        runner._create_file_mapping = mock_create_file_mapping
        runner._create_input_copy_overrides = mock_create_input_copy_overrides
        runner._create_output_copy_overrides = mock_create_output_copy_overrides
        runner._get_entrypoint_command = mock_get_entrypoint_cmd
        runner._get_task_definition_arn = mock_get_task_def_arn
        runner._submit_to_ecs = mock_submit_to_ecs

        mock_ex_op = mock.MagicMock()
        mock_uuid = '<EX OP UUID>'
        mock_ex_op.id = mock_uuid
        mock_op = mock.MagicMock()
        mock_op.id = '<OP UUID>'
        mock_validated_inputs = {}

        runner.run(mock_ex_op, mock_op, mock_validated_inputs)
        mock_create_execution_dir.assert_called_once_with(mock_uuid)
        mock_convert_inputs.assert_called_once_with(mock_op, 
            '/data/operations/<OP UUID>', 
            mock_validated_inputs,
            mock_staging_dir)
        mock_create_file_mapping.assert_called_once_with(mock_uuid, mock_op, 
            mock_converted_inputs)
        mock_create_input_copy_overrides.assert_called_once_with(
            mock_uuid, mock_converted_inputs, mock_file_mapping)
        mock_get_entrypoint_cmd.assert_called_once_with(
            '/data/operations/<OP UUID>/entrypoint.txt',
            {'a': 'efs_path', 'b':2}) # <-- note that we have combined the dicts from above to create this
        mock_create_output_copy_overrides.assert_called_once_with(mock_uuid)
        mock_get_task_def_arn.assert_called_once_with('<OP UUID>')
        mock_submit_to_ecs.assert_called_once_with(
            mock_ex_op,
            mock_task_def_arn,
            input_copy_overrides,
            shlex.split(mock_cmd),
            output_copy_overrides)

    @override_settings(AWS_ECS_CLUSTER='mycluster')
    @override_settings(AWS_ECS_SUBNET='subnet-01')
    @override_settings(AWS_ECS_SECURITY_GROUP='sg-01')
    def test_ecs_submission(self):
        runner = ECSRunner()
        mock_get_ecs_client = mock.MagicMock()
        mock_client = mock.MagicMock()
        mock_get_ecs_client.return_value = mock_client
        mock_response = mock.MagicMock()
        mock_client.run_task.return_value = mock_response
        mock_handle_ecs_submission_response = mock.MagicMock()

        runner._get_ecs_client = mock_get_ecs_client
        runner._handle_ecs_submission_response = mock_handle_ecs_submission_response

        mock_ex_op = mock.MagicMock()
        runner._submit_to_ecs(mock_ex_op, 'arn::123', [], 'some cmd', [])
        mock_client.run_task.assert_called()
        mock_handle_ecs_submission_response.assert_called_once_with(
            mock_ex_op, mock_response)

    def test_submission_handler(self):
        runner = ECSRunner()
        mock_response = {
            "tasks": [
            {
                "attachments": [
                {
                    "id": "ae96e092-a054-4882-a35b-aa64fd6d8243",
                    "type": "ElasticNetworkInterface",
                    "status": "PRECREATED",
                    "details": [
                    {
                        "name": "subnetId",
                        "value": "subnet-049b9a2c4b205347f"
                    }
                    ]
                }
                ],
                "attributes": [
                {
                    "name": "ecs.cpu-architecture",
                    "value": "x86_64"
                }
                ],
                "availabilityZone": "us-east-2a",
                "clusterArn": "arn:aws:ecs:us-east-2:286060835461:cluster/demo_aug6",
                "containers": [
                {
                    "containerArn": "arn:aws:ecs:us-east-2:286060835461:container/demo_aug6/129c114baf3b44b0959eeee9a82c69bd/828ea167-3f50-4094-ac8b-502fe6377c49",
                    "taskArn": "arn:aws:ecs:us-east-2:286060835461:task/demo_aug6/129c114baf3b44b0959eeee9a82c69bd",
                    "name": "file-retriever",
                    "image": "286060835461.dkr.ecr.us-east-2.amazonaws.com/aws-cli:latest",
                    "lastStatus": "PENDING",
                    "networkInterfaces": [],
                    "cpu": "256",
                    "memory": "512"
                },
                {
                    "containerArn": "arn:aws:ecs:us-east-2:286060835461:container/demo_aug6/129c114baf3b44b0959eeee9a82c69bd/653de5fa-2b84-43b5-802d-adb31223eed2",
                    "taskArn": "arn:aws:ecs:us-east-2:286060835461:task/demo_aug6/129c114baf3b44b0959eeee9a82c69bd",
                    "name": "analysis",
                    "image": "286060835461.dkr.ecr.us-east-2.amazonaws.com/demo-pca:latest",
                    "lastStatus": "PENDING",
                    "networkInterfaces": [],
                    "cpu": "512",
                    "memory": "1024"
                },
                {
                    "containerArn": "arn:aws:ecs:us-east-2:286060835461:container/demo_aug6/129c114baf3b44b0959eeee9a82c69bd/59a2ab70-1856-4373-81dd-b3eb27b8e01a",
                    "taskArn": "arn:aws:ecs:us-east-2:286060835461:task/demo_aug6/129c114baf3b44b0959eeee9a82c69bd",
                    "name": "file-pusher",
                    "image": "286060835461.dkr.ecr.us-east-2.amazonaws.com/aws-cli:latest",
                    "lastStatus": "PENDING",
                    "networkInterfaces": [],
                    "cpu": "256",
                    "memory": "512"
                }
                ],
                "cpu": "1024",
                "createdAt": "2024-08-12 15:31:00.002000-04:00",
                "desiredStatus": "RUNNING",
                "enableExecuteCommand": False,
                "group": "family:demo-task-aug6",
                "lastStatus": "PROVISIONING",
                "launchType": "FARGATE",
                "memory": "3072",
                "overrides": {
                "containerOverrides": [
                ],
                "inferenceAcceleratorOverrides": []
                },
                "platformVersion": "1.4.0",
                "tags": [],
                "taskArn": "TASKARN",
                "taskDefinitionArn": "arn:aws:ecs:us-east-2:286060835461:task-definition/demo-task-aug6:20",
                "version": 1,
                "ephemeralStorage": {
                "sizeInGiB": 20
                }
            }
            ],
            "failures": [],
            "ResponseMetadata": {
            "RequestId": "019b6d7e-4db0-40b3-9b48-73fd606e1b1c",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amzn-requestid": "019b6d7e-4db0-40b3-9b48-73fd606e1b1c",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "2524",
                "date": "Mon, 12 Aug 2024 19:30:59 GMT"
            },
            "RetryAttempts": 0
            }
        }
        mock_ex_op = mock.MagicMock()
        runner._handle_ecs_submission_response(mock_ex_op, mock_response)
        self.assertTrue(mock_ex_op.job_id == 'TASKARN')
        mock_ex_op.save.assert_called()

    def test_submission_handler_failure(self):
        runner = ECSRunner()
        mock_handle_submission_problem = mock.MagicMock()
        runner._handle_submission_problem = mock_handle_submission_problem
        mock_ex_op = mock.MagicMock()
        runner._handle_ecs_submission_response(mock_ex_op, {})
        mock_ex_op.save.assert_not_called()
        mock_handle_submission_problem.assert_called_once_with(mock_ex_op)

    def test_task_defn_query(self):
        op1 = OperationDb.objects.create(
            active=True,
            name = 'DESeq2',
            successful_ingestion = True,
            workspace_operation = True
        )
        # add a couple definitions:
        task_arn_1 = 'arn::task:1'
        ECSTaskDefinition.objects.create(task_arn=task_arn_1, operation=op1)
        time.sleep(1)
        task_arn_2 = 'arn::task:2'
        ECSTaskDefinition.objects.create(task_arn=task_arn_2, operation=op1)

        runner = ECSRunner()
        result = runner._get_task_definition_arn(op1.pk)
        self.assertTrue(result == task_arn_2)

    @override_settings(JOB_BUCKET_NAME='job-bucket')
    @override_settings(AWS_ECS_SUBNET='subnet-01')
    @override_settings(AWS_ECS_SECURITY_GROUP='sg-01')
    def test_output_copy_override(self):
        runner = ECSRunner()
        exec_op_uuid = 'abd-123'
        cp_str = f'{ECSRunner.AWS_CLI_PATH} s3 cp --recursive {ECSRunner.EFS_DATA_DIR}/{exec_op_uuid} s3://job-bucket/{exec_op_uuid}/ && rm -rf {ECSRunner.EFS_DATA_DIR}/{exec_op_uuid}'
        cmd = runner._create_output_copy_overrides(exec_op_uuid)
        self.assertEqual(cp_str, cmd[0])

    def test_task_resource_set(self):
        '''
        ECS requires that the resources for the individual
        container definitions sum to less than the total
        specified for the task. Test the function that checks this
        '''
        runner = ECSRunner()
        mock_defs = [
            {
                'cpu': 256
            },
            {
                'cpu': 512
            },
            {
                'cpu': 256
            }]
        acceptable_values = [256, 512, 1024, 2048]
        result = runner._get_resource_limit(mock_defs, 'cpu', acceptable_values)
        self.assertEqual(result, 1024)

        acceptable_values = [256, 512]
        with self.assertRaisesRegex(Exception, 'exceeded'):
            runner._get_resource_limit(mock_defs, 'cpu', acceptable_values)

    def test_raises_error_if_invalid_resource_spec(self):
        '''
        ECS has some 'encoded' values for cpu/mem resources which
        are acceptable. If a task specifies something that is not
        compatible, check that we raise an exception
        '''
        runner = ECSRunner()
        # patch these constants for testing ease:
        ECSRunner.ACCEPTABLE_CPU_VALUES = [256, 512]
        ECSRunner.ACCEPTABLE_MEM_VALUES = [256, 512, 1024]

        # the cpu key is incorrect
        resource_dict = {
            runner.CPU_KEY: 288,
            runner.MEM_KEY: 512
        }
        with self.assertRaisesRegex(Exception, 'not acceptable'):
            runner._verify_task_requirements(resource_dict)

        # seemingly plausible values, but the 2048 exceeds the max of 1024
        resource_dict = {
            runner.CPU_KEY: 512,
            runner.MEM_KEY: 2048
        }
        with self.assertRaisesRegex(Exception, 'not acceptable'):
            runner._verify_task_requirements(resource_dict)

        # acceptable values- no problems.
        resource_dict = {
            runner.CPU_KEY: 512,
            runner.MEM_KEY: 512
        }
        runner._verify_task_requirements(resource_dict)

    def test_parse_task_status_response_case1(self):
        '''
        Test that we issue an 'is finished' response
        if the task has a STOPPED status and all exit codes
        are zero
        '''

        runner = ECSRunner()

        mock_success_response = {
            'tasks': [
                {
                    'taskArn': 'arn::123',
                    'lastStatus': 'STOPPED',
                    'containers': [
                        {
                            'exitCode': 0
                        },
                        {
                            'exitCode': 0
                        },
                        {
                            'exitCode': 0
                        }
                    ]
                }
            ]
        }

        is_running = runner._parse_task_status_response(mock_success_response)
        self.assertFalse(is_running)
        
    def test_parse_task_status_response_case2(self):
        '''
        Test that we issue an 'is finished' response
        if the task has a STOPPED status and NOT all exit codes
        are zero
        '''

        runner = ECSRunner()

        mock_success_response = {
            'tasks': [
                {
                    'taskArn': 'arn::123',
                    'lastStatus': 'STOPPED',
                    'containers': [
                        {
                            'exitCode': 0
                        },
                        {
                            'exitCode': 1
                        },
                        {
                            'exitCode': 0
                        }
                    ]
                }
            ]
        }

        is_running = runner._parse_task_status_response(mock_success_response)
        self.assertFalse(is_running)

    def test_parse_task_status_response_case1(self):
        '''
        Test that we issue a 'not finished' response
        if the task has a non-STOPPED value
        '''

        runner = ECSRunner()

        mock_success_response = {
            'tasks': [
                {
                    'taskArn': 'arn::123',
                    'lastStatus': 'PROVISIONING',
                    'containers': []
                }
            ]
        }

        is_running = runner._parse_task_status_response(mock_success_response)
        self.assertTrue(is_running)

    @mock.patch('api.runners.ecs.alert_admins')
    def test_finalization_case1(self, mock_alert_admins):
        '''
        Tests the successful run case
        '''
        runner = ECSRunner()

        # patch a couple methods on the class:
        mock_locate_outputs = mock.MagicMock()
        mock_initial_outputs_dict = {'outputA': 1}
        mock_locate_outputs.return_value = mock_initial_outputs_dict
        runner._locate_outputs = mock_locate_outputs

        mock_convert_outputs = mock.MagicMock()
        mock_final_outputs = {'outputA': 'something'}
        mock_convert_outputs.return_value =  mock_final_outputs
        runner._convert_outputs = mock_convert_outputs

        mock_clean = mock.MagicMock()
        runner._clean_job_bucket = mock_clean

        mock_get_ecs_task_info = mock.MagicMock()
        mock_info = {
            'tasks': [
                {
                    'stopCode': 'EssentialContainerExited'
                }
            ]
        }
        mock_get_ecs_task_info.return_value = mock_info
        runner._get_ecs_task_info = mock_get_ecs_task_info

        mock_ex_op = mock.MagicMock()
        mock_ex_op.pk = 'my-pk'
        mock_ex_op.job_id = 'job-id'
        mock_op = mock.MagicMock()
        runner.finalize(mock_ex_op, mock_op)
        mock_get_ecs_task_info.assert_called_once_with('job-id')
        mock_alert_admins.assert_not_called()
        mock_locate_outputs.assert_called_once_with('my-pk')
        mock_convert_outputs.assert_called_once_with(mock_ex_op, mock_op, mock_initial_outputs_dict)
        self.assertDictEqual(mock_ex_op.outputs, mock_final_outputs)
        mock_clean.assert_called_once_with(mock_ex_op)

    @mock.patch('api.runners.ecs.alert_admins')
    def test_finalization_case2(self, mock_alert_admins):
        '''
        Tests the case where the analysis task fails for some reason
        '''
        runner = ECSRunner()

        # patch a couple methods on the class:
        mock_locate_outputs = mock.MagicMock()
        mock_initial_outputs_dict = {'outputA': 1}
        mock_locate_outputs.return_value = mock_initial_outputs_dict
        runner._locate_outputs = mock_locate_outputs

        mock_convert_outputs = mock.MagicMock()
        mock_final_outputs = {'outputA': 'something'}
        mock_convert_outputs.return_value =  mock_final_outputs
        runner._convert_outputs = mock_convert_outputs

        mock_clean = mock.MagicMock()
        runner._clean_job_bucket = mock_clean

        mock_check_logs = mock.MagicMock()
        runner._check_logs = mock_check_logs

        mock_get_ecs_task_info = mock.MagicMock()
        mock_info = {
            'tasks': [
                {
                    'stopCode': 'TaskFailedToStart'
                }
            ]
        }
        mock_get_ecs_task_info.return_value = mock_info
        runner._get_ecs_task_info = mock_get_ecs_task_info

        mock_ex_op = mock.MagicMock()
        mock_ex_op.pk = 'my-pk'
        mock_ex_op.job_id = 'job-id'
        mock_op = mock.MagicMock()
        runner.finalize(mock_ex_op, mock_op)
        mock_get_ecs_task_info.assert_called_once_with('job-id')
        mock_alert_admins.assert_called()
        mock_locate_outputs.assert_not_called()
        mock_convert_outputs.assert_not_called()
        mock_clean.assert_not_called()
        mock_check_logs.assert_called()

    def test_check_logs(self):
        mock_job_info = {
            "containers" : [
                {
                    "containerArn": "arn:aws:ecs:us-east-2:286060835461:container/dev-mev-ecs-cluster/1dc4f34f3df340f2a6a35a3824b4fde5/250931b6-fcfd-4b93-b845-f01012daefb0",
                    "taskArn": "arn:aws:ecs:us-east-2:286060835461:task/dev-mev-ecs-cluster/1dc4f34f3df340f2a6a35a3824b4fde5",
                    "name": "file-pusher",
                    "image": "amazon/aws-cli:latest",
                    "imageDigest": "sha256:3a97c55bad2d70a7adb2a4b9e6d84de1eb45bf019a2791781ddc33f47ea1c8bb",
                    "runtimeId": "1dc4f34f3df340f2a6a35a3824b4fde5-1993217325",
                    "lastStatus": "STOPPED",
                    "networkInterfaces": [
                        {
                        "attachmentId": "755872a2-1c06-441c-9587-ef3544911899",
                        "privateIpv4Address": "172.16.0.54",
                        "ipv6Address": "2600:1f16:1924:e500:cb19:5f2e:26cd:a3a8"
                        }
                    ],
                    "healthStatus": "UNKNOWN",
                    "cpu": "256",
                    "memory": "512"
                    },
                    {
                    "containerArn": "arn:aws:ecs:us-east-2:286060835461:container/dev-mev-ecs-cluster/1dc4f34f3df340f2a6a35a3824b4fde5/89ba8b7d-7d53-4e09-8934-576d43571683",
                    "taskArn": "arn:aws:ecs:us-east-2:286060835461:task/dev-mev-ecs-cluster/1dc4f34f3df340f2a6a35a3824b4fde5",
                    "name": "analysis",
                    "image": "ghcr.io/web-mev/mev-limma-voom:sha-acad58346d00a81305f6cc39652904d515f406b7",
                    "imageDigest": "sha256:4ecd0e0f7d3f4dd753645e4eceb49e342e58e8f2fda4234e24eb4568c0863c13",
                    "runtimeId": "1dc4f34f3df340f2a6a35a3824b4fde5-3156543011",
                    "lastStatus": "STOPPED",
                    "exitCode": 1,
                    "networkBindings": [],
                    "networkInterfaces": [
                        {
                        "attachmentId": "755872a2-1c06-441c-9587-ef3544911899",
                        "privateIpv4Address": "172.16.0.54",
                        "ipv6Address": "2600:1f16:1924:e500:cb19:5f2e:26cd:a3a8"
                        }
                    ],
                    "healthStatus": "UNKNOWN",
                    "cpu": "2048",
                    "memory": "16384"
                    },
                    {
                    "containerArn": "arn:aws:ecs:us-east-2:286060835461:container/dev-mev-ecs-cluster/1dc4f34f3df340f2a6a35a3824b4fde5/daf52cb9-41f0-4e87-8498-2b227801070c",
                    "taskArn": "arn:aws:ecs:us-east-2:286060835461:task/dev-mev-ecs-cluster/1dc4f34f3df340f2a6a35a3824b4fde5",
                    "name": "file-retriever",
                    "image": "amazon/aws-cli:latest",
                    "imageDigest": "sha256:3a97c55bad2d70a7adb2a4b9e6d84de1eb45bf019a2791781ddc33f47ea1c8bb",
                    "runtimeId": "1dc4f34f3df340f2a6a35a3824b4fde5-3349646512",
                    "lastStatus": "STOPPED",
                    "exitCode": 0,
                    "networkBindings": [],
                    "networkInterfaces": [
                        {
                        "attachmentId": "755872a2-1c06-441c-9587-ef3544911899",
                        "privateIpv4Address": "172.16.0.54",
                        "ipv6Address": "2600:1f16:1924:e500:cb19:5f2e:26cd:a3a8"
                        }
                    ],
                    "healthStatus": "UNKNOWN",
                    "cpu": "256",
                    "memory": "512"
                }
            ]
        }

        runner = ECSRunner()
        mock_query_logs = mock.MagicMock()
        mock_query_logs.return_value = ['a','b']
        runner._query_log_stream = mock_query_logs
        result = runner._check_logs(mock_job_info)
        self.assertCountEqual(result, ['a','b'])
        mock_query_logs.assert_called_once_with(
            'analysis', '1dc4f34f3df340f2a6a35a3824b4fde5')