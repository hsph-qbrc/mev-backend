import logging
import os
import glob
import json
import datetime

from django.conf import settings
from django.core.files.storage import default_storage

from api.storage import S3_PREFIX
from api.runners.base import OperationRunner
from api.utilities.nextflow_utils import NF_SUFFIX, \
    NEXTFLOW_COMPLETED, \
    NEXTFLOW_ERROR, \
    get_container_names, \
    edit_nf_containers, \
    read_final_nextflow_metadata, \
    job_succeeded, \
    get_error_report
from api.utilities.docker import check_image_exists, \
    check_image_name_validity
from api.utilities.basic_utils import copy_local_resource, \
    run_shell_command
from api.models import ExecutedOperation
from api.utilities.admin_utils import alert_admins
from api.utilities.executed_op_utilities import get_execution_directory_path


logger = logging.getLogger(__name__)


class NextflowRunner(OperationRunner):
    '''
    Handles execution of `Operation`s using Nextflow
    '''

    MAIN_NF = 'main.nf'
    NF_INPUTS = 'params.json'
    # canonical name for the config file that will be created
    # for each job run. To be created in the execution directory
    CONFIG_FILE_NAME = 'nextflow.config'
    STDOUT_FILE_NAME = 'nf_stdout.txt'
    STDERR_FILE_NAME = 'nf_stderr.txt'
    RUN_CMD = '{nextflow_exe} -bg run {main_nf} -c {config}' \
              ' -name {job_name}' \
              ' -params-file {params} --output_dir {output_dir}' \
              ' -with-weblog {status_update_url} >{stdout} 2>{stderr}'

    # A list of files that are required to be part of the repository
    REQUIRED_FILES = OperationRunner.REQUIRED_FILES + [
        # the nf script
        MAIN_NF,
        # the input json file, as a template
        NF_INPUTS
    ]

    JOB_PREFIX = 'job_'

    def prepare_operation(self, operation_db_obj, operation_dir, repo_name, git_hash):

        container_image_names = get_container_names(operation_dir)
        logger.info('Found the following image names among the'
            f' Nextflow files: {", ".join(container_image_names)}')

        name_mapping = {}
        for full_image_name in container_image_names:
            final_image_name = check_image_name_validity(full_image_name,
                repo_name,
                git_hash)
            image_found = check_image_exists(final_image_name)
            if not image_found:
                raise Exception('Could not locate the following'
                    f' image: {final_image_name}. Aborting')

            # keep track of any "edited" image names so we can modify
            # the Nextflow files
            name_mapping[full_image_name] = final_image_name

        # change the name of the image in the NF file(s), saving them in-place:
        edit_nf_containers(operation_dir, name_mapping)

    def _create_params_json(self, op, op_dir, validated_inputs, staging_dir):
        '''
        Takes the inputs (which are MEV-native data structures)
        and make them into something that we can inject into a
        JSON-format params file for Nextflow

        For instance, this takes a DataResource (which is a UUID identifying
        the file), and turns it into a cloud-based path that Nextflow's
        process executor can access.

        Note that `op` is an instance of data_structures.operation.Operation,
        NOT an instance of api.models.operation.Operation
        '''
        # create/write the input JSON to a file in the staging location
        arg_dict = self._convert_inputs(op, op_dir, validated_inputs, staging_dir)
        nf_input_path = os.path.join(staging_dir, self.NF_INPUTS)
        with open(nf_input_path, 'w') as fout:
            json.dump(arg_dict, fout)
        return nf_input_path

    def _copy_workflow_contents(self, op_dir, staging_dir):
        '''
        Copy over NF files and other elements necessary to submit
        the job to Nextflow. Does not mean that we copy EVERYTHING
        in the op dir.
        '''
        # copy Nextflow (nf) files over to staging:
        nf_files = glob.glob(
            os.path.join(op_dir, '*' + NF_SUFFIX)
        )
        for f in nf_files:
            dest = os.path.join(staging_dir, os.path.basename(f))
            copy_local_resource(f, dest)

    def run(self, executed_op, op, validated_inputs):
        logger.info(f'Executing job using Nextflow runner.')
        super().run(executed_op, op, validated_inputs)

        # the UUID identifying the execution of this operation:
        execution_uuid = str(executed_op.id)

        # get the operation dir so we can look at which converters to use:
        op_dir = os.path.join(
            settings.OPERATION_LIBRARY_DIR, 
            str(op.id)
        )

        # create a sandbox directory where we will store the files:
        staging_dir = self._create_execution_dir(execution_uuid)

        # create the Nextflow-compatible JSON-format inputs file from the user inputs
        inputs_path = self._create_params_json(op,
                                               op_dir,
                                               validated_inputs,
                                               staging_dir)

        # copy over the workflow contents:
        self._copy_workflow_contents(op_dir, staging_dir)

        # create the nextflow config file. This dictates whether we use
        # a local nf run, one on AWS Batch, etc.
        runtime_config_path = self._prepare_config_template(staging_dir)

        # nextflow needs an output directory to direct files, which depends
        # on the process executor- child classes will dictate this
        nf_outputs_dir = self._get_outputs_dir(staging_dir, executed_op.id)

        # Nextflow jobs need to match the following regex:
        # ^[a-z](?:[a-z\d]|[-_](?=[a-z\d])){0,79}$
        # so a generic UUID (which can start with a digit)
        # does not work
        job_id = f'{self.JOB_PREFIX}{str(execution_uuid)}'

        cmd = self.RUN_CMD.format(
            nextflow_exe=settings.NEXTFLOW_EXE,
            main_nf=os.path.join(staging_dir, self.MAIN_NF),
            job_name=job_id,
            config=runtime_config_path,
            params=inputs_path,
            output_dir=nf_outputs_dir,
            status_update_url=settings.NEXTFLOW_STATUS_UPDATE_URL,
            stdout=os.path.join(staging_dir, self.STDOUT_FILE_NAME),
            stderr=os.path.join(staging_dir, self.STDERR_FILE_NAME)
        )
        # set and save prior to calling `run_shell_command` since
        # we can end up with a race condition otherwise (nextflow
        # immediately POSTS to say "started" but the database cannot
        # find the job ID since it hasn't been committed)
        executed_op.job_id = job_id
        executed_op.save()
        try:
            run_shell_command(cmd)
        except Exception as ex:
            logger.info(f'Failed when running shell command: {cmd}')
            logger.info(f'Exception was: {ex}')
            # if an exception is raised when issuing the Docker run
            # command, then the job has failed. This error is likely
            # not due to user error, but something with the issuing
            # command or allocating appropriate Docker resources.
            executed_op.job_failed = True
            executed_op.execution_stop_datetime = datetime.datetime.now()
            executed_op.status = ExecutedOperation.ADMIN_NOTIFIED
            executed_op.save()
            alert_admins(str(ex))

    def check_status(self, job_id):
        '''
        The runner interface is created such that we have a periodic task that
        calls this function. Nextflow will use a special localhost url to 
        update our status. Once the job is complete, the view backing that 
        localhost url will set the appropriate field. Then the periodic task
        will "finalize" the run (moving files around, etc.)
        '''
        executed_op = ExecutedOperation.objects.get(job_id=job_id)
        logger.info(f'Checking status for Nextflow-based job {job_id}')
        if executed_op.status == NEXTFLOW_COMPLETED:
            return True
        elif executed_op.status == NEXTFLOW_ERROR:
            return True
        else:
            return False

    def _find_outputs(self, executed_op, op):
        '''
        Locates the outputs and returns a dictionary.

        Based on the way Nextflow works, there's no direct
        way to get the outputs by querying nextflow or
        reading an output file. Hence, we have to go through
        the expected outputs and locate the files.
        '''
        # regardless of whether local or remote, we need the staging dir:
        staging_dir = get_execution_directory_path(executed_op.id)

        # returns the output directory specific to the runner (whether local or remote)
        nf_outputs_dir = self._get_outputs_dir(staging_dir, executed_op.id)

        # get the operation spec so we know the expected outputs
        op_spec_outputs = op.outputs

        # note that the sort is not necessary, but it incurs little penalty.
        # However, it does make unit testing easier.
        outputs = {}
        for k in sorted(op_spec_outputs.keys()):
            # note that we append a '/'. For S3-based storage,
            # it will not recognize it as a 'directory' otherwise
            expected_dir = os.path.join(nf_outputs_dir, k  + '/')
            if default_storage.check_if_exists(expected_dir):
                file_list = default_storage.get_file_listing(expected_dir)
            else:
                # did not find an output directory. It's possible it was
                # NOT a required output. If that's the case, it's handled
                # during 'conversion'. We simply put a blank value
                # and that will be checked for validity during conversion
                file_list = []
            outputs[k] = file_list
        return outputs

    def finalize(self, executed_op, op):
        '''
        Finishes up an ExecutedOperation. Does things like registering files 
        with a user, cleanup, etc.

        `executed_op` is an instance of api.models.ExecutedOperation
        `op` is an instance of data_structures.operation.Operation
        '''
        job_metadata = read_final_nextflow_metadata(executed_op.pk)
        if job_succeeded(job_metadata):
            executed_op.job_failed = False
            outputs_dict = self._find_outputs(executed_op, op)
            converted_outputs = self._convert_outputs(
                    executed_op, op, outputs_dict)
            executed_op.outputs = converted_outputs
            executed_op.status = ExecutedOperation.COMPLETION_SUCCESS

            # if everything went well, including conversion of outputs,
            # we can delete the execution directory.
            self._clean_following_success(executed_op.pk)
        else:
            executed_op.job_failed = True
            error_report = get_error_report(job_metadata)
            logger.error(error_report)
            executed_op.error_messages = [error_report]
            alert_admins(f'Nextflow job ({str(executed_op.pk)}) failed.'
                         f' Report is {error_report}')
            executed_op.status = ExecutedOperation.COMPLETION_ERROR

        executed_op.execution_stop_datetime = datetime.datetime.now()
        executed_op.save()


class LocalNextflowRunner(NextflowRunner):
    '''
    Implementation of the NextflowRunner that runs locally.
    '''
    NAME = 'nf_local'
    CONFIG_FILE_TEMPLATE = os.path.join(os.path.dirname(__file__), 
                                        'nextflow_config_templates',
                                        'local.config')
    # to keep nextflow from 'polluting' the execution directory on a local
    # run, we direct nextflow to drop the final output/published files
    # into a subdirectory of our local execution directory. hence, this would
    # be something like /data/operation_executions/<exec op UUID>/<OUTPUT_DIR_NAME>
    OUTPUT_DIR_NAME = 'nf_outputs'

    def _prepare_config_template(self, execution_dir):
        template_text = open(self.CONFIG_FILE_TEMPLATE, 'r').read()
        runtime_config_path = os.path.join(execution_dir, self.CONFIG_FILE_NAME)
        with open(runtime_config_path, 'w') as fout:
            fout.write(template_text.format(
                nextflow_work_dir=execution_dir
            ))
        return runtime_config_path

    def _get_outputs_dir(self, execution_dir, executed_op_pk):
        '''
        This method dictates where nextflow will put output files.
        Since this is a local runner, we just put them in the
        execution directory under a nextflow-specific subdir
        '''
        return os.path.join(execution_dir, self.OUTPUT_DIR_NAME)


class AWSBatchNextflowRunner(NextflowRunner):
    '''
    Implementation of the NextflowRunner that interfaces with
    AWS Batch
    '''
    NAME = 'nf_batch'
    CONFIG_FILE_TEMPLATE = os.path.join(os.path.dirname(__file__), 
                                        'nextflow_config_templates',
                                        'aws_batch.config')

    def _prepare_config_template(self, execution_dir):
        template_text = open(self.CONFIG_FILE_TEMPLATE, 'r').read()
        runtime_config_path = os.path.join(execution_dir, self.CONFIG_FILE_NAME)
        with open(runtime_config_path, 'w') as fout:
            fout.write(template_text.format(
                aws_batch_queue=settings.AWS_BATCH_QUEUE,
                aws_region=settings.AWS_REGION,
                nextflow_bucket_name=settings.JOB_BUCKET_NAME,
                uuid=os.path.basename(execution_dir)
            ))
        return runtime_config_path

    def _get_outputs_dir(self, execution_dir, executed_op_pk):
        '''
        This method dictates where nextflow will put output files.
        Since this is a remote runner, we send them to a bucket
        associated with the job execution
        '''
        return S3_PREFIX + \
            os.path.join(settings.JOB_BUCKET_NAME, str(executed_op_pk))
