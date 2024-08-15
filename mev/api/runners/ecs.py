import logging
import os
import json

from django.conf import settings

import boto3

from api.runners.base import OperationRunner
from api.models import ECSTask

from api.utilities.docker import check_image_exists, \
    get_image_name_and_tag

logger = logging.getLogger(__name__)


class ECSRunner(OperationRunner):
    '''
    Handles execution of `Operation`s using AWS 
    Elastic Container Service
    '''

    NAME = 'ecs'

    # public Docker image which has the AWS CLI.
    # Used when pulling/pushing files from S3
    AWS_CLI_IMAGE = 'amazon/aws-cli:latest'

    # used to 'name' the step performing the analysis
    # and used to establish the dependency chain of
    # steps comprising an ECS task
    ANALYSIS_CONTAINER_NAME = 'analysis'

    # a consistent reference for the 'alias' of the EFS volume
    # which is shared between the steps of the ECS task
    EFS_VOLUME_ALIAS = 'efs-vol'

    # a file that specifies the command to be run:
    ENTRYPOINT_FILE = 'entrypoint.txt'

    # a JSON that specifies the estimated resources required:
    RESOURCE_FILE = 'resources.json'

    # A list of files that are required to be part of the repository
    REQUIRED_FILES = OperationRunner.REQUIRED_FILES + [
        RESOURCE_FILE,
        ENTRYPOINT_FILE
    ]

    # in the resources JSON file, we need to know the cpu and memory
    # (and possibly other parameters). This provides a consistent 
    # reference to these keys so we can address them
    CPU_KEY = 'cpu'
    MEM_KEY = 'mem_mb'
    REQUIRED_RESOURCE_KEYS = [CPU_KEY, MEM_KEY]


    def prepare_operation(self, operation_db_obj, operation_dir, repo_name, git_hash):
        '''
        Performs required steps to include a new tool in our ECS cluster.

        This includes:
        - checking that the image exists
        - create a task definition and add to the ECS cluster
        - save task ARN to database
        '''
        image_url = self._check_for_image(repo_name, git_hash)
        task_arn = self._register_new_task(repo_name, operation_dir, image_url)
        
        # associate the task ARN with the Operation object in the database
        ECSTask.objects.create(task_arn=task_arn, operation=operation_db_obj)

    def _check_for_image(self, repo_name, git_hash):
        image_url = get_image_name_and_tag(repo_name, git_hash)
        image_found = check_image_exists(image_url)
        if not image_found:
            raise Exception('Could not locate the following'
                f' image: {image_url}. Aborting')
        return image_url

    def _get_resource_requirements(self, handle):
        # we have already ensured the file exists. Need to load and
        # verify that it has the proper format        
        try:
            j = json.load(handle)
        except json.JSONDecodeError as ex:
            raise Exception(f'Failed to parse the resource JSON.'
                            f' Exception was {ex}')

        for k in ECSRunner.REQUIRED_RESOURCE_KEYS:
            if not k in j:
                raise Exception('The resource file was missing a'
                                f' required key {k}.')
        return j

    def _register_new_task(self, repo_name, op_dir, image_url):
        '''
        Performs the actual registration of the task
        with ECS
        '''
        container_definitions = self._get_container_defs(op_dir, image_url)

        client = boto3.client('ecs')
        try:
            response = client.register_task_definition(
                family=repo_name,
                taskRoleArn=settings.AWS_ECS_TASK_ROLE,
                executionRoleArn=settings.AWS_ECS_EXECUTION_ROLE,
                networkMode='awsvpc',
                containerDefinitions=container_definitions,
                volumes=[
                    {
                        'name': ECSRunner.EFS_VOLUME_ALIAS,
                        'efsVolumeConfiguration': {
                            'fileSystemId': settings.AWS_EFS_ID,
                            'transitEncryption': 'ENABLED',
                            'authorizationConfig': {
                                'accessPointId': settings.AWS_EFS_ACCESS_POINT,
                            }
                        }
                    }
                ],
                requiresCompatibilities=['FARGATE'],
                cpu='1024', # required for fargate. 
                memory='3072',# required for fargate. 
                runtimePlatform={
                    'cpuArchitecture': 'X86_64',
                    'operatingSystemFamily': 'LINUX'
                }
            )
        except Exception as ex:
            logger.error(f'Failed to register task. Reason was {ex}')
            raise ex

        try:
            return response['taskDefinition']['taskDefinitionArn']
        except KeyError:
            logger.error('Could not determine the task ARN.')
            raise Exception('Failed to get new task definition ARN.')


    def _get_container_defs(self, op_dir, image_url):
        '''
        Create the array of container definitions required
        for an ECS task definition.

        Note that the application code in each analysis tool
        repository is agnostic to AWS/ECS. To work on user files,
        the task must have an initial step which pulls the required
        files and must have a final step which pushes the output
        files to permanent storage. This function stitches together
        those with the actual step performing the analysis
        '''
        container_defs = []

        # initial step where the files are copied to the EFS shared between
        # the task steps
        container_defs.append(self._get_file_pull_container_definition)

        with os.path.join(op_dir, ECSRunner.RESOURCE_FILE) as fh:
            resource_dict = self._get_resource_requirements(fh)

        # the main step
        analysis_def = {
            "name": ECSRunner.ANALYSIS_CONTAINER_NAME,
            "image": image_url,
            "cpu": resource_dict[ECSRunner.CPU_KEY],
            "memory": resource_dict[ECSRunner.MEM_KEY],
            "essential": False,
            # this command will be overridden when the job
            # is executed
            "command": ['<SOME COMMAND>'],
            "mountPoints": [
                {
                    "sourceVolume": ECSRunner.EFS_VOLUME_ALIAS,
                    "containerPath": "/data",
                    "readOnly": False
                }
            ],
            "dependsOn": [
                {
                    "containerName": "file-retriever",
                    "condition": "SUCCESS"
                }
            ]
        }
        container_defs.append(analysis_def)

        # now the final step which pushes files to permanent storage
        container_defs.append(self._get_file_push_container_definition())

        return container_defs


    def _get_file_pull_container_definition(self):
        '''
        Returns a dict specifying the initial step
        of the ECS task where the required file(s)
        is pulled
        '''
        return {
            "name": "file-retriever",
            "image": ECSRunner.AWS_CLI_IMAGE,
            # 0.25 vCPU and 0.5 GB RAM
            # see https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definitions
            "cpu": 256, 
            "memory": 512,
            "essential": False,

            # this command is overridden when the task
            # is called, so this command is simply a clue
            # for what it does.
            "command": [
                "s3",
                "cp",
                "<SOURCE PATH>",
                "<LOCAL PATH>"
            ],
            "mountPoints": [
                {
                    "sourceVolume": ECSRunner.EFS_VOLUME_ALIAS,
                    "containerPath": "/data",
                    "readOnly": False
                }
            ]
        }

    def _get_file_push_container_definition(self):

        return {
            "name": "file-pusher",
            "image": ECSRunner.AWS_CLI_IMAGE,
            # 0.25 vCPU and 0.5 GB RAM
            # see https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definitions
            "cpu": 256, 
            "memory": 512,
            "essential": True,
            
            # this command is overridden when the task
            # is called, so this command is simply a clue
            # for what it does.
            "command": [
                "s3",
                "cp",
                "<LOCAL PATH>",
                "<FINAL STORAGE PATH IN S3>"
            ],
            "mountPoints": [
                {
                    "sourceVolume": ECSRunner.EFS_VOLUME_ALIAS,
                    "containerPath": "/data",
                    "readOnly": True
                }
            ],
            "dependsOn": [
                {
                    "containerName": ECSRunner.ANALYSIS_CONTAINER_NAME,
                    "condition": "SUCCESS"
                }
            ]
        }
