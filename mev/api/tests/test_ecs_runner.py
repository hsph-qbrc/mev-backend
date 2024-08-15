import unittest.mock as mock
import os
from tempfile import TemporaryDirectory, TemporaryFile

from api.tests.base import BaseAPITestCase
from api.runners.ecs import ECSRunner
from exceptions import MissingRequiredFileException


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

