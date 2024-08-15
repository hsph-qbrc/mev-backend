import os
import json
import datetime
import logging

from django.conf import settings

from api.runners.base import OperationRunner, \
    TemplatedCommandMixin
from api.utilities.docker import check_if_container_running, \
    check_container_exit_code, \
    get_finish_datetime, \
    remove_container, \
    get_logs, \
    pull_image, \
    get_image_name_and_tag
from api.utilities.basic_utils import run_shell_command
from api.utilities.admin_utils import alert_admins
from api.models import ExecutedOperation

logger = logging.getLogger(__name__)


class LocalDockerRunner(OperationRunner, TemplatedCommandMixin):
    '''
    Class that handles execution of `Operation`s using Docker on the local
    machine
    '''
    NAME = 'local_docker'

    # the name of the Dockerfile which resides in the docker directory.
    # Used to build the Docker image
    DOCKERFILE = 'Dockerfile'

    # a file that specifies the entrypoint "command" that is run:
    ENTRYPOINT_FILE = 'entrypoint.txt'

    # A list of files that are required to be part of the repository
    REQUIRED_FILES = OperationRunner.REQUIRED_FILES + [
        os.path.join(OperationRunner.DOCKER_DIR, DOCKERFILE),
        ENTRYPOINT_FILE
    ]

    # the template docker command to be run:
    DOCKER_RUN_CMD = ('docker run -d --name {container_name}'
                      ' -v {execution_mount}:{work_dir}'
                      ' -u {uid}:{gid}'
                      ' --env WORKDIR={job_dir}'
                      ' {docker_image} {cmd}')

    def check_status(self, job_uuid):
        container_is_running = check_if_container_running(job_uuid)
        if container_is_running:
            return False
        else:
            return True

    def load_outputs_file(self, job_id):
        '''
        Loads and returns the contents of the expected
        outputs json file. If it does not exist or if there
        was a parsing error, it will raise an exception to be 
        caught in the calling function
        '''
        execution_dir = os.path.join(
            settings.OPERATION_EXECUTION_DIR, job_id)

        try:
            outputs_dict = json.load(open(
                os.path.join(execution_dir, self.OUTPUTS_JSON)
            ))
            logger.info('After parsing the outputs file,'
                        f' we have: {json.dumps(outputs_dict)}')
            return outputs_dict
        except FileNotFoundError as ex:
            logger.info(f'The outputs file for job {job_id} was not'
                        ' found.')
            raise Exception('The outputs file was not found. An administrator'
                            ' should check the analysis operation.')
        except json.decoder.JSONDecodeError:
            raise Exception('There was an issue preparing the outputs. An administrator'
                ' has been notified to resolve this.')

    def finalize(self, executed_op, op):
        '''
        Finishes up an ExecutedOperation. Does things like registering files 
        with a user, cleanup, etc.

        `executed_op` is an instance of api.models.ExecutedOperation
        `op` is an instance of data_structures.operation.Operation
        '''
        job_id = str(executed_op.job_id)
        exit_code = check_container_exit_code(job_id)
        finish_datetime = get_finish_datetime(job_id)
        executed_op.execution_stop_datetime = finish_datetime

        if exit_code != 0:
            logger.info('Received a non-zero exit code'
                        f' ({exit_code}) from container'
                        f' executing job: {executed_op.job_id}')
            executed_op.job_failed = True
            executed_op.status = ExecutedOperation.COMPLETION_ERROR

            # collect the errors that are  reported in the logs
            log_msg = get_logs(job_id)
            message_list = [log_msg, ]

            # handle the out of memory error-- we can't do it all!
            if exit_code == 137:
                logger.info(f'Executed job {executed_op.job_id}'
                            ' exhausted the available memory.')
                message_list.append('The process ran out of memory and exited.'
                                    ' Sometimes the job parameters can result in analyses'
                                    ' exceeding the processing capabilities of WebMeV.')

            executed_op.error_messages = message_list
            alert_admins(','.join(message_list))

        else:
            logger.info('Container exit code was zero. Fetch outputs.')
            # read the outputs json file and convert to mev-compatible outputs:
            try:
                outputs_dict = self.load_outputs_file(job_id)

                converted_outputs = self._convert_outputs(
                    executed_op, op, outputs_dict)
                executed_op.outputs = converted_outputs

                executed_op.job_failed = False
                executed_op.status = ExecutedOperation.COMPLETION_SUCCESS

                # if everything went well, including conversion of outputs,
                # we can delete the execution directory.
                self._clean_following_success(job_id)

            except Exception as ex:
                # if the outputs file was not found or if some
                # other exception was raised, mark the job failed.
                executed_op.job_failed = True
                executed_op.status = str(ex)
                alert_admins(str(ex))

        # finally, we cleanup the docker container
        remove_container(job_id)

        # so future requests don't think it is still finalizing:
        executed_op.is_finalizing = False
        executed_op.save()
        return

    def prepare_operation(self, operation_db_obj, operation_dir, repo_name, git_hash):
        '''
        Prepares the Operation, including pulling the Docker container

        `operation_dir` is the directory where the staged repository is held
        `repo_name` is the name of the repository. Used for the 
            Docker image name
        `git_hash` is the commit hash and it allows us to version 
            the docker container the same as the git repository
        '''
        image_url = get_image_name_and_tag(repo_name, git_hash)
        pull_image(image_url)

    def run(self, executed_op, op, validated_inputs):
        logger.info('Running in local Docker mode.')
        super().run(executed_op, op, validated_inputs)

        # the UUID identifying the execution of this operation:
        execution_uuid = str(executed_op.id)

        # get the operation dir so we can look at which converters and command to use:
        op_dir = os.path.join(
            settings.OPERATION_LIBRARY_DIR,
            str(op.id)
        )

        # To avoid conflicts or corruption of user data, we run each operation in its
        # own sandbox. We must first copy over their files to that sandbox dir.
        execution_dir = self._create_execution_dir(execution_uuid)

        # convert the user inputs into args compatible with commandline usage:
        # For instance, a differential gene expression requires one to specify
        # the samples that are in each group-- to do this, the Operation requires
        # two ObservationSet instances are submitted as arguments. The "translator"
        # will take the ObservationSet data structures and turn them into something
        # that the call with use- e.g. making a CSV list to submit as one of the args
        # like:
        # docker run <image> run_something.R -a sampleA,sampleB -b sampleC,sampleD
        arg_dict = self._convert_inputs(
            op, op_dir, validated_inputs, execution_dir)

        logger.info('After mapping the user inputs, we have the'
                    f' following structure: {arg_dict}')

        # Construct the command that will be run in the container:
        entrypoint_file_path = os.path.join(op_dir, self.ENTRYPOINT_FILE)
        if not os.path.exists(entrypoint_file_path):
            logger.error('Could not find the required entrypoint'
                f' file at {entrypoint_file_path}.'
                ' Something must have corrupted the operation directory.')
            raise Exception('The repository must have been corrupted.'
                            ' Failed to find the entrypoint file.'
                            f' Check dir at: {op_dir}')
        entrypoint_cmd = self._get_entrypoint_command(
            entrypoint_file_path, arg_dict)

        image_str = get_image_name_and_tag(
            op.repository_name,
            op.git_hash
        )

        cmd = self.DOCKER_RUN_CMD.format(
            container_name=execution_uuid,
            execution_mount=settings.OPERATION_EXECUTION_DIR,
            work_dir=settings.OPERATION_EXECUTION_DIR,
            uid=os.getuid(),
            gid=os.getgid(),
            job_dir=execution_dir,
            docker_image=image_str,
            cmd=entrypoint_cmd
        )
        try:
            run_shell_command(cmd)
            executed_op.job_id = execution_uuid
            executed_op.save()
        except Exception as ex:
            logger.info('Failed when running shell command: {c}'.format(c=cmd))
            logger.info('Exception was: {ex}'.format(ex=ex))
            # if an exception is raised when issuing the Docker run
            # command, then the job has failed. This error is likely
            # not due to user error, but something with the issuing
            # command or allocating appropriate Docker resources.
            executed_op.job_failed = True
            executed_op.execution_stop_datetime = datetime.datetime.now()
            executed_op.status = ExecutedOperation.ADMIN_NOTIFIED
            executed_op.save()
            alert_admins(str(ex))
