import logging
import os
import json
import uuid
import datetime
import shlex

from django.conf import settings

import boto3

from api.runners.base import OperationRunner, \
    TemplatedCommandMixin
from api.models import ECSTaskDefinition
from api.storage import S3_PREFIX
from api.utilities.docker import check_image_exists, \
    get_image_name_and_tag
from api.utilities.admin_utils import alert_admins
from exceptions import JobSubmissionException

from data_structures.data_resource_attributes import get_all_data_resource_typenames

logger = logging.getLogger(__name__)


class ECSRunner(OperationRunner, TemplatedCommandMixin):
    '''
    Handles execution of `Operation`s using AWS 
    Elastic Container Service
    '''

    NAME = 'ecs'

    # public Docker image which has the AWS CLI.
    # Used when pulling/pushing files from S3
    AWS_CLI_IMAGE = 'amazon/aws-cli:latest'

    # used to 'name' the steps comprising an ECS task
    ANALYSIS_CONTAINER_NAME = 'analysis'
    FILE_RETRIEVER = 'file-retriever'
    FILE_PUSHER = 'file-pusher'

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

    # For consistent reference to the EFS volume which is shared
    # across the ECS task
    EFS_DATA_DIR = '/data'

    # in the resources JSON file, we need to know the cpu and memory
    # (and possibly other parameters). This provides a consistent 
    # reference to these keys so we can address them
    CPU_KEY = 'cpu'
    MEM_KEY = 'mem_mb'
    REQUIRED_RESOURCE_KEYS = [CPU_KEY, MEM_KEY]

    # Note the path of the AWS CLI in the Docker image:
    AWS_CLI_PATH = '/usr/local/bin/aws'
    AWS_CP_TEMPLATE = AWS_CLI_PATH + ' s3 cp {src} {dest}'
    AWS_DIR_CP_TEMPLATE = AWS_CLI_PATH + ' s3 cp --recursive {src} {dest}'

    def _get_ecs_client(self):
        return boto3.client('ecs', region_name=settings.AWS_REGION)

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
        logger.info(f'Associating {operation_db_obj} with {task_arn=}')
        ECSTaskDefinition.objects.create(task_arn=task_arn, operation=operation_db_obj)

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
        logger.info(f'Registering new task with ECS for {repo_name=}')
        container_definitions = self._get_container_defs(op_dir, image_url)

        client = self._get_ecs_client()
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
            logger.info('No exceptions raised when registering. Get task ARN')
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
        container_defs.append(self._get_file_pull_container_definition())

        with open(os.path.join(op_dir, ECSRunner.RESOURCE_FILE)) as fh:
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
                    "containerPath": ECSRunner.EFS_DATA_DIR,
                    "readOnly": False
                }
            ],
            "user": "root",
            "dependsOn": [
                {
                    "containerName": ECSRunner.FILE_RETRIEVER,
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
            "name": ECSRunner.FILE_RETRIEVER,
            "image": ECSRunner.AWS_CLI_IMAGE,
            # 0.25 vCPU and 0.5 GB RAM
            # see https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definitions
            "cpu": 256, 
            "memory": 512,
            "essential": False,
            "entryPoint": [
                "sh",
                "-c"
            ],
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
                    "containerPath": ECSRunner.EFS_DATA_DIR,
                    "readOnly": False
                }
            ]
        }

    def _get_file_push_container_definition(self):

        return {
            "name": ECSRunner.FILE_PUSHER,
            "image": ECSRunner.AWS_CLI_IMAGE,
            # 0.25 vCPU and 0.5 GB RAM
            # see https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definitions
            "cpu": 256, 
            "memory": 512,
            "essential": True,
            "entryPoint": [
                "sh",
                "-c"
            ],
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
                    "containerPath": ECSRunner.EFS_DATA_DIR,
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

    def run(self, executed_op, op, validated_inputs):
        logger.info(f'Executing job using ECS runner.')
        super().run(executed_op, op, validated_inputs)

        # the UUID identifying the execution of this operation:
        execution_uuid = str(executed_op.id)

        # get the operation dir so we can look at which converters and command to use:
        op_dir = os.path.join(
            settings.OPERATION_LIBRARY_DIR,
            str(op.id)
        )

        # create a sandbox directory where we will stage for potential debugging:
        staging_dir = self._create_execution_dir(execution_uuid)

        # convert user inputs to something compatible with ECS.
        # For example, take file inputs and map them from the resource UUID
        # to a bucket-based path 
        arg_dict = self._convert_inputs(
            op, op_dir, validated_inputs, staging_dir)

        logger.info('After mapping the user inputs, we have the'
                    f' following structure: {arg_dict}')

        # Since we ultimately have to use an AWS cli container to copy
        # bucket-based files to EFS (such that ECS can "share" files between steps
        # of the task), we need to construct those copy commands
        # as an override. We will assign those copied files to unique
        # file paths (e.g. aws s3 cp s3://some-bucket/file.tsv /data/<UUID>).
        # Note, however, that we need to know those UUIDs so we can provide a proper
        # command template for the override in the main analysis container.
        file_mapping = self._create_file_mapping(execution_uuid, op, arg_dict)

        # create the copy commands for the inputs:
        input_copy_overrides = self._create_input_copy_overrides(execution_uuid, arg_dict, file_mapping)

        # Construct the command that will be run in the container.
        # Recall that if the operation includes file-like objects,
        # the `arg_dict` has bucket-based paths while `file_mapping`
        # has the paths on the EFS volume. The command needs the latter.
        # This union operation replaces the s3 paths with local efs paths
        arg_dict_for_container = arg_dict | file_mapping
        entrypoint_file_path = os.path.join(op_dir, self.ENTRYPOINT_FILE)
        entrypoint_cmd_str = self._get_entrypoint_command(
            entrypoint_file_path, arg_dict_for_container)
        # currently entrypoint_cmd is a string, but it needs to be 
        # provided to the container in array form
        entrypoint_cmd = shlex.split(entrypoint_cmd_str)

        output_copy_overrides = self._create_output_copy_overrides()

        # get the most recent task ARN associated with this operation
        task_def_arn = self._get_task_definition_arn(op.id)
        self._submit_to_ecs(executed_op,
                            task_def_arn, 
                            input_copy_overrides,
                            entrypoint_cmd,
                            output_copy_overrides)

    def _get_task_definition_arn(self, op_pk):
        task_def = ECSTaskDefinition.objects.filter(operation__pk = op_pk).order_by('-revision_date')[0]
        return task_def.task_arn

    def _submit_to_ecs(self, 
                       executed_op,
                       task_def_arn,
                       input_copy_overrides,
                       entrypoint_cmd,
                       output_copy_overrides):

        overrides = {
            'containerOverrides': [
                    {
                        'name': ECSRunner.FILE_RETRIEVER,
                        'command': input_copy_overrides
                    },
                    {
                        'name': ECSRunner.ANALYSIS_CONTAINER_NAME,
                        'command': entrypoint_cmd
                    },
                    {
                        'name': ECSRunner.FILE_PUSHER,
                        'command': output_copy_overrides
                    }
                ]
        }

        client = self._get_ecs_client()
        response = client.run_task(
            taskDefinition=task_def_arn,
            cluster=settings.AWS_ECS_CLUSTER,
            count=1,
            launchType='FARGATE',
            platformVersion='LATEST',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [settings.AWS_ECS_SUBNET],
                    'securityGroups': [
                        settings.AWS_ECS_SECURITY_GROUP
                    ],
                    'assignPublicIp': 'ENABLED',
                }
            },
            overrides=overrides,
        )
        self._handle_ecs_submission_response(executed_op, response)

    def _handle_ecs_submission_response(self, executed_op, response):
        try:
            task_id = response['tasks'][0]['taskArn']
            executed_op.job_id = task_id
            executed_op.save()
        except Exception as ex:
            logger.error('Encountered an unexpected data structure in'
                f' response from ECS. Exception was {ex}')
            self._handle_submission_problem(executed_op)

    def _handle_submission_problem(self, executed_op):
        executed_op.execution_start_datetime = datetime.datetime.now()
        executed_op.execution_stop_datetime = datetime.datetime.now()
        executed_op.status = 'Job submission failed. An admin has been notified.'
        executed_op.job_failed = True
        executed_op.save()
        alert_admins('There was an issue with job submission for executed'
            f' operation with id: {executed_op.id}')
        raise JobSubmissionException()

    def _create_output_copy_overrides(self, execution_uuid):
        src = f'{ECSRunner.EFS_DATA_DIR}/{execution_uuid}'
        dest = f'{S3_PREFIX}{settings.JOB_BUCKET_NAME}/{execution_uuid}/'
        cp_cmd = ECSRunner.AWS_DIR_CP_TEMPLATE.format(src=src, dest=dest)
        return [cp_cmd]
        
    def _create_file_mapping(self, execution_uuid, op, arg_dict):
        '''
        When we submit jobs to ECS, the first step is to copy file resources
        onto the EFS volume. This function creates a mapping from the
        bucket-based path to a unique path. In this way, we can
        consistently refer to the same files in our command.

        As as example, consider the following `arg_dict`:
        {
            'input_A': 2, 
            'input_B': 's3://bucket/obj.tsv', 
            'input_C': 's3://bucket/another_obj.tsv'
        }
        When we perform a copy in the first step of the ECS task,
        we ultimately make a call like 'aws s3 cp s3://bucket/obj.tsv /data/<job UUID>/<UUID1>'
        and 'aws s3 cp s3://bucket/another_obj.tsv /data/<job UUID>/<UUID2>' to copy
        files onto the EFS volume
        However, we need to keep track that the file placed at /data/<job UUID>/UUID1 corresponds
        to input_B, etc. This way, when the command is run, e.g.
        <some script> -i /data/<job UUID>/<UUID1> -j /data/<job UUID>/<UUID2>, 
        the inputs are correctly associated with the command arguments.
        '''
        mapping = {}
        op_inputs = op.inputs
        for k,v in arg_dict.items():
            # look at the specification for this input to check if it's file-like
            op_input = op_inputs[k]
            spec = op_input.spec.to_dict()
            if spec['attribute_type'] in get_all_data_resource_typenames():
                if spec['many']:
                    assert(type(v) is list)
                    mapping[k] = [f'{ECSRunner.EFS_DATA_DIR}/{execution_uuid}/{str(uuid.uuid4())}' for _ in v ]
                else:
                    assert(type(v) is str)
                    mapping[k] = f'{ECSRunner.EFS_DATA_DIR}/{execution_uuid}/{str(uuid.uuid4())}'
        return mapping

    def _create_input_copy_overrides(self, execution_uuid, arg_dict, file_mapping):
        '''
        This method constructs the override command which is run as 
        the first step of the ECS task.

        `arg_dict` has the converted inputs
        `file_mapping` maps the file-like inputs to unique
                       identifiers 
        '''
        cp_commands = []
        for input_key, mapped_val in file_mapping.items():

            # the converted inputs. Could be a single s3 path
            # or a list of s3 paths
            src = arg_dict[input_key]

            # if a single path, simply put into a list
            # so we can process everything in the same way
            if type(src) is str:
                src = [src]
                mapped_val = [mapped_val]

            for i,s in enumerate(src):
                dest = mapped_val[i]
                cp_commands.append(ECSRunner.AWS_CP_TEMPLATE.format(
                    src = s,
                    dest = dest
                ))
        return [f'mkdir -p {ECSRunner.EFS_DATA_DIR}/{execution_uuid} && ' + ' && '.join(cp_commands)]

    def check_status(self, job_id):
        '''
        Method used by all runners to determine when job
        is complete. Returns boolean
        '''
        is_running = self._check_ecs_task_status(job_id)
        if is_running:
            return False
        else:
            return True

    def _check_ecs_task_status(self, job_id):
        client = self._get_ecs_client()
        response = client.describe_tasks(
            cluster=settings.AWS_ECS_CLUSTER,
            tasks=[
                job_id
            ]
        )
        return self._parse_task_status_response(response)

    def _parse_task_status_response(self, response):
        '''
        Looks at the response from ECS and returns a boolean
        indicating whether complete or not
        '''
        task = response['tasks'][0]
        last_status = task['lastStatus']
        task_arn = task['taskArn']
        logger.info(f'Checking status on {task_arn=}')
        if last_status == 'STOPPED':
            logger.info(f'Task {task_arn=} had STOPPED status')
            # check that all steps exited with exit code 0:
            for container_status in response['tasks'][0]['containers']:
                if container_status['exitCode'] != 0:
                    logger.info(f'Encountered situation where ECS task'
                        ' {task_arn} was stopped, but a container exited'
                        ' with non-zero exit code.'
                    )
                    logger.info(json.dumps(task, indent=2, default=str))
            return True
        else:
            logger.info(f'Task {task_arn=} had {last_status=}.')
            return False

    def finalize(self, executed_op, op):
        logger.info(f'In ECSRunner.finalize for job {executed_op.pk}')