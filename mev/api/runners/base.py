import os
import logging
from shutil import rmtree

from django.conf import settings
from django.utils.module_loading import import_string

from exceptions import OutputConversionException, \
    MissingRequiredFileException

from data_structures.data_resource_attributes import \
    get_all_data_resource_typenames

from api.utilities.admin_utils import alert_admins
from api.utilities.basic_utils import make_local_directory
from api.utilities.resource_utilities import delete_resource_by_pk
from api.utilities.executed_op_utilities import get_execution_directory_path

logger = logging.getLogger(__name__)


class OperationRunner(object):
    '''
    A base class for classes which handle the execution of jobs/operations
    '''

    # the name of the folder (relative to the repository) which has the docker
    # context
    DOCKER_DIR = 'docker'

    # A list of files that are required to be part of the repository.
    # Derived classes can add to this for their specific needs
    REQUIRED_FILES = []

    # the name of the file which will direct us to the outputs from an ExecutedOperation.
    # It will be placed in the execution directory by the job
    OUTPUTS_JSON = 'outputs.json'

    def check_if_ready(self):
        '''
        This is called on startup to see if a particular implementation
        of a runner is indeed ready to run. Examples include checking that
        the Cromwell server is online, etc.

        This will typically be overridden by a child class. If not, then
        it will assume it's ready (by not raising an exception)
        '''
        pass


    def prepare_operation(self, operation_db_obj, operation_dir, repo_name, git_hash):
        '''
        Used during ingestion to perform setup/prep before an operation can 
        be executed. Use for things like building docker images, etc.

        Typically, the subclasses will implement something specific to their 
        execution mode.
        '''
        pass

    def check_required_files(self, operation_dir):
        '''
        Checks that the files required for a particular run mode are, in fact,
        in the directory.

        `operation_dir` is a path (local) to a directory containing the files
        defining the Operation
        '''
        logger.info('Checking required files are present in the respository')
        for f in self.REQUIRED_FILES:
            expected_path = os.path.join(operation_dir, f)
            logging.info(f'Look for: {expected_path}')
            if not os.path.exists(expected_path):
                logging.info('Could not find the required file:'
                    f' {expected_path}')
                raise MissingRequiredFileException('Could not locate the'
                    f' required file ({f}) in the repository at'
                    f' {operation_dir}')
        logger.info('Done checking for required files.')

    def _create_execution_dir(self, execution_uuid):
        # To avoid conflicts or corruption of user data, we run each operation in its
        # own sandbox directory.
        execution_dir = get_execution_directory_path(execution_uuid)
        make_local_directory(execution_dir)
        return execution_dir

    def _get_converter(self, converter_class_str):
        try:
            converter_class = import_string(converter_class_str)
            return converter_class()
        except (Exception, ModuleNotFoundError) as ex:
            message = ('Failed when importing the converter'
                f' class: {converter_class_str}.'
                f'Error was: {ex}')
            logger.error(message)
            raise Exception(message) 

    def _convert_inputs(self, op, op_dir, validated_inputs, staging_dir):
        '''
        Takes the inputs (which are MEV-native data structures)
        and make them into something that we can pass to a command-line
        call. 

        Note that `op` is an instance of data_structures.operation.Operation

        For instance, this might take a DataResource (which is a UUID identifying
        the file), and turns it into a local path. The actual mapping depends
        on the converter class defined by the Operation. Different operations
        might "transform" a Resource into different things (e.g. a path, a delimited
        string of the path and a resource type, etc.) depending on the requirements
        of the analysis.
        '''
        arg_dict = {}
        for k,v in validated_inputs.items():
            op_input = op.inputs[k]
            # instantiate the converter and convert the arg:
            converter = self._get_converter(op_input.converter)
            arg_dict[k] = converter.convert_input(v, op_dir, staging_dir)

        logger.info('After mapping the user inputs, we have the'
            f' following structure: {arg_dict}')
        return arg_dict

    def _convert_outputs(self, executed_op, op, outputs_dict):
        '''
        Handles the mapping from outputs (as provided by the runner)
        to MEV-compatible data structures or resources.
        '''
        # the workspace so we know which workspace to associate outputs with:
        user_workspace = getattr(executed_op, 'workspace', None)

        # get the operation spec so we know which types
        # correspond to each output
        op_spec_outputs = op.outputs

        converted_outputs_dict = {}
        try:
            # note that the sort is not necessary, but it incurs little penalty.
            # However, it does make unit testing easier.
            for k in sorted(op_spec_outputs.keys()):
                current_output_spec = op_spec_outputs[k]
                try:
                    v = outputs_dict[k]
                except KeyError as ex:
                    error_msg = (f'Could not locate the output with key={k}'
                        ' in the outputs of executed operation with ID:'
                        f' {executed_op.id}'
                    )
                    logger.info(error_msg)
                    alert_admins(error_msg)
                    raise OutputConversionException(error_msg)

                else:
                    if v is not None:
                        converter = self._get_converter(
                            current_output_spec.converter)
                        logger.info('Attempt to convert using'
                            f' {converter}')
                        converted_outputs_dict[k] = converter.convert_output(
                            executed_op, user_workspace,
                            current_output_spec, v)
                    else:
                        logger.info('Executed operation output was null/None.')
                        converted_outputs_dict[k] = None

            # If here, we had all the required output keys and they converted properly.
            # However, the analysis might have specified EXTRA outputs. This isn't necessarily
            # an error, but we treat it as such since it's clear there is a discrepancy between
            # the "spec" and the actual output. 
            # We don't fail the job, but we alert the admins.
            extra_keys = set(outputs_dict.keys()).difference(op_spec_outputs.keys())
            if len(extra_keys) > 0:
                error_msg = (f'There were extra keys ({",".join(extra_keys)})'
                    ' in the output of the operation. Check this.')
                logger.info(error_msg)
                alert_admins(error_msg)

            return converted_outputs_dict
        except OutputConversionException as ex:
            logger.info('Requesting cleanup of an ExecutedOperation due to failure'
                ' while converting outputs.'
            )
            self.cleanup_on_error(op_spec_outputs, converted_outputs_dict)
            raise ex

    def cleanup_on_error(self, op_spec_outputs, converted_outputs_dict):
        '''
        If there is an error during conversion of the outputs, we don't want
        any Resource instances to be kept. For instance, if there are multiple
        output files created and one fails validation, we don't want to expose the
        others since it may cause a situation where the output state is ambiguous.

        `op_spec_outputs` is the "operation spec" from the `Operation` instance. That
        details what the expected output(s) should be.

        `converted_outputs_dict` is a dict that has outputs that have already been
        converted.
        '''

        # the types that we should clean up on error. 
        data_resource_typenames = get_all_data_resource_typenames()
        for k,v in converted_outputs_dict.items():
            spec = op_spec_outputs[k].spec.to_dict()
            output_attr_type = spec['attribute_type']
            if output_attr_type in data_resource_typenames:
                logger.info(f'Will cleanup the output "{k}" with'
                    f' value of {v}')

                # ok, so we are dealing with an output type
                # that represents a file/Resource. This can either
                # be singular (so the value v is a UUID) or multiple
                # in which case the value is a list of UUIDs

                # if a single UUID, put that in a list. This way
                # we can handle single and multiple outputs 
                # in the same way
                if (type(v) is str) or (type(v) is uuid.UUID):
                    v = [str(v),]
                
                for resource_uuid in v:
                    delete_resource_by_pk(resource_uuid)

    def _clean_following_success(self, job_id):
        '''
        If a job completed succesfully, we have no need to retain
        the execution directory and its contents since there is no debugging
        to perform.
        '''
        execution_dir = os.path.join(
            settings.OPERATION_EXECUTION_DIR, str(job_id))
        rmtree(execution_dir)