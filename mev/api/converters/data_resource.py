import os
import logging

from django.core.files import File
from django.core.files.storage import default_storage
from numpy import result_type

from exceptions import OutputConversionException, StorageException

from api.utilities.resource_utilities import get_resource_by_pk, \
    localize_resource, \
    initiate_resource_validation, \
    delete_resource_by_pk, \
    retrieve_resource_class_standard_format, \
    create_resource
from api.utilities.admin_utils import alert_admins

from api.converters.mixins import CsvMixin, SpaceDelimMixin
from api.models import ResourceMetadata

logger = logging.getLogger(__name__)


class BaseResourceConverter(object):
    '''
    A base class for operations common to all the types of
    DataResource-like classes
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        raise NotImplementedError('You must implement the convert_input'
                                  ' method in the chosen class.')

    def convert_output(self,
                       executed_op, workspace, output_definition, output_val):
        raise NotImplementedError('You must implement the convert_output'
                                  ' method in the chosen class.')

    def _handle_storage_failure(self, output_required):
        '''
        A method to customize how we react if a DataResource fails to convert
        (i.e. create a valid Resource instance)

        Override in a subclass if further customization is needed.
        '''
        if output_required:
            raise OutputConversionException(
                'Failed to convert a required output.')

    def _handle_invalid_resource_type(self, resource):
        '''
        If a resource fails to validate to its expected type, then we delete
        that Resource.

        If more customization is required, override in a subclass.
        '''
        logger.info('The validation method did not set the resource type'
                    ' which indicates that validation did not succeed. Hence,'
                    f' the resource ({resource.pk}) will be removed.')

        # Delete the current Resource that failed
        # The cast to a str is not necessary, but makes unit testing
        # slightly simpler
        delete_resource_by_pk(str(resource.pk))

    def _create_output_filename(self, path, job_name):
        '''
        Names the output file based on the user-submitted job name.
        '''
        if len(job_name) > 0:
            return f'{job_name}.{os.path.basename(path)}'
        else:
            return os.path.basename(path)

    def _attempt_resource_addition(self, executed_op,
                                   workspace, path, resource_type, 
                                   output_required):
        # the "name"  of the file as the user will see it.

        name = self._create_output_filename(path, executed_op.job_name)

        # try to create the resource in the db. This involves
        # moving/copying files, so there can be exceptions raised
        # Initialize `resource` to None. If we successfully
        # return from the `create_resource` method, then this
        # will be reset.
        resource = None
        try:
            # this self._create_resource calls a method
            # defined in a derived class. Depending on whether
            # the resource is local or remote, the behavior
            # is different.
            resource = self._create_resource(executed_op,
                                             workspace, path, name)
        except (FileNotFoundError, StorageException):
            # TODO: assert this does not change with other runners like Nextflow
            # For optional outputs, Cromwell will report output files
            # that do not actually exist. In this case, the copy will
            # fail, but it's not necessarily a problem.
            logger.info('Received a file not found or storage exception for a'
                        ' job output. Depending on the analysis, this may not'
                        ' be a problem.'
                        )
        except Exception as ex:
            logger.info('Received an unexpected exception when creating a'
                        ' resource for a job output'
                        )
            # since this was unexpected, we want the admins to know 
            # about it, even if the output was optional
            alert_admins('Unexpected exception was raised during resource'
                         f' creation for executed operation {executed_op.pk}'
                         )

        if resource is None:
            self._handle_storage_failure(output_required)
            # if handle_storage_failure does not raise an
            # exception, then it was "ok" that this file
            # did not store correctly (as in the case of
            # optional outputs)
            return None

        # Now attempt to validate the resource. IF this succeeds, 
        # the resource_type field will be set.
        # If there is an unrecoverable failure, this method will raise
        # an exception that is caught in the calling method
        try:
            # by default, the outputs we create should be in 
            # the standard format for their resource type
            file_format = retrieve_resource_class_standard_format(
                resource_type)
            initiate_resource_validation(resource, resource_type, file_format)
        except Exception as ex:
            # if the validation fails, it will raise an Exception (or derived 
            # class of an Exception). If that's the case, we need to roll-back
            self._handle_invalid_resource_type(resource)
            alert_admins(f'Failed to validate an output file to the expected format'
                f' for resource {resource.pk} on job {executed_op.pk}')
            raise OutputConversionException('Failed to validate the expected'
                                            ' resource type'
                                            )

        # everything worked out correctly!
        resource.is_active = True
        resource.save()

        # add the info about the parent operation to the resource metadata
        rm = ResourceMetadata.objects.get(resource=resource)
        rm.parent_operation = executed_op
        rm.save()
        return str(resource.pk)

    def _convert_resource_output(
            self, executed_op, workspace, output_definition, output_val):
        '''
        A method that handles the addition of a single resource output.

        Returns the UUID of the new Resource or raises 
        an OutputConversionException
        '''
        path, resource_type = self._get_output_path_and_resource_type(
            output_val, output_definition.spec)

        output_required = output_definition.required
        try:
            new_resource_uuid = self._attempt_resource_addition(
                executed_op, workspace, path, resource_type, output_required
            )
            return new_resource_uuid
        except Exception as ex:
            # raise this exception which will trigger cleanup of any
            # other outputs for this ExecutedOperation
            raise OutputConversionException(str(ex))


class DataResourceMixin(object):
    '''
    This class handles common behavior for converting DataResource instances
    which have a single, fixed resource type
    '''

    def _get_output_path_and_resource_type(self, output_val, output_spec):
        '''
        This method looks at the output value and spec, checks that 
        everything is compatible, and returns a tuple of the path
        and the requested resource type.

        Used for converting outputs.

        `output_val` is a path to the file
        `output_spec` is an instance of data_structures.output_spec.OutputSpec
        '''
        return (output_val, output_spec.value.resource_type)


class VariableDataResourceMixin(object):
    '''
    This class handles common behavior for converting VariableDataResource
    instances which can be of multiple types.
    '''

    def _get_output_path_and_resource_type(self, output_val, output_spec):
        '''
        This method looks at the output value and spec, checks that 
        everything is compatible, and returns a tuple of the path
        and the requested resource type.

        Used for converting outputs.

        `output_val` is a dict which has the `path` and `resource_type`
        `output_spec` is an instance of data_structures.output_spec.OutputSpec
        '''

        if not type(output_val) is dict:
            raise OutputConversionException('For a VariableResourceType,'
                                            ' we expect that the output is'
                                            ' an object/dict.'
                                            ' The value received was:'
                                            f' {output_val}'
                                            )

        # in the case where the output has variable type, we need
        # to ensure the potential types and the requested type
        # are compatible
        potential_resource_types = output_spec.value.resource_types
        try:
            p = output_val['path']
            resource_type = output_val['resource_type']
        except KeyError as ex:
            raise OutputConversionException('In the output object, we expect'
                                            f' the following key: {ex}')

        if resource_type in potential_resource_types:
            return (p, resource_type)
        else:
            raise OutputConversionException('The specified resource type of'
                                    f' {result_type} was not consistent'
                                    ' with the permitted types of'
                                    f' {",".join(potential_resource_types)}'
                                    )


class SingleDataResourceMixin(object):

    def _convert_output(
            self, executed_op, workspace, output_definition, output_val):

        return self._convert_resource_output(
            executed_op, workspace, output_definition, output_val)

    def _convert_input(self, user_input, staging_dir):

        # `convert_single_resource` is dependent on
        # the runner, so the method is found in the
        # mixin specific to local, nextflow, etc.
        return self._convert_resource_input(user_input, staging_dir)


class MultipleDataResourceMixin(object):

    def _convert_input(self, user_input, staging_dir):
        '''
        Used for looping over the >=1 inputs (UUIDs)
        '''
        path_list = []
        if type(user_input) == list:
            for u in user_input:
                path_list.append(
                    # `convert_single_resource` is dependent on
                    # the runner, so the method is found in the
                    # mixin specific to local, nextflow, etc.
                    self._convert_resource_input(u, staging_dir)
                )
        elif type(user_input) == str:
            # technically COULD HAVE been multiple resources, but only a single
            # was provided (hence, a single string, not a list)
            path_list.append(
                self._convert_resource_input(user_input, staging_dir)
            )
        else:
            logger.error('Unrecognized type submitted for DataResource'
                f' value: {user_input}')
        return path_list

    def _cleanup_other_outputs(self, resource_uuids):
        '''
        For situations where one of multiple outputs fail to convert, we
        need to cleanup other Resource instances. Note that this cleanup
        is limited to the particular output field we are converting
        '''
        # also delete other Resources associated with this output key
        [delete_resource_by_pk(x) for x in resource_uuids]

    def _convert_output(
            self, executed_op, workspace, output_definition, output_val):

        # the specification- what kind of output data do we expect?
        output_spec = output_definition.spec

        if not output_spec.value.many:
            raise OutputConversionException('This converter is only'
                                ' appropriate for multiple resource outputs.')

        if not type(output_val) is list:
            raise OutputConversionException('When there are multiple'
                                            ' outputs, we expect a list.')

        resource_uuids = []
        for x in output_val:
            try:
                u = self._convert_resource_output(
                    executed_op, workspace, output_definition, x)
                resource_uuids.append(u)
            except OutputConversionException as ex:
                # if one of the attempts raises an exception (e.g.
                # if a required output was missing) then we need to
                # cleanup other resources that were part of this output
                # field. This way we don't generate partial outputs
                self._cleanup_other_outputs(resource_uuids)

                # we then re-raise the exception so that OTHER output
                # fields get removed. If we are here, then we are missing
                # a required output for a tool. Not only do we cleanup
                # the outputs for this single output field, but we need
                # to cleanup the other output fields.
                raise ex
        return resource_uuids


class LocalResourceMixin(object):
    '''
    This mixin class holds actions/behavior for local-based Resources/files.
    Note that "local" doesn't mean our storage system. Rather, it means
    that we need the file/resource local so that Docker can use it.
    '''

    def _convert_resource_input(self, resource_uuid, staging_dir):
        '''
        Takes a resource UUID and copies the associated file to `staging_dir`
        Returns a path to the copied file
        '''
        resource_instance = get_resource_by_pk(resource_uuid)
        return localize_resource(resource_instance, staging_dir)

    def _create_resource(self, executed_op, workspace, path, name):
        logger.info('From executed operation outputs based on a local job,'
                    ' create a resource with name {n}'.format(
                        n=name
                    )
                    )
        fh = File(open(path, 'rb'), name)
        return create_resource(
            executed_op.owner,
            file_handle=fh,
            name=name,
            workspace=workspace
        )


class RemoteResourceMixin(object):
    '''
    Contains behavior for DataResource conversion related to
    the remote storage (bucket). Can be used with Nextflow
    or ECS-based runners which directly access files stored
    in buckets.
    '''

    def _convert_resource_input(self, resource_uuid, staging_dir):
        '''
        Handles the conversion of a UUID referencing an instance that
        is a child of api.models.AbstractResource 

        Returns the full path to the file, e.g.
        s3://<mev storage bucket>/<object path>
        '''
        r = get_resource_by_pk(resource_uuid)
        return default_storage.get_absolute_path(r.datafile.name)

    def _create_resource(self, executed_op, workspace, path, name):
        '''
        Returns an instance of api.models.Resource based on the 
        passed parameters.

        Note that `path` is the path in the Nextflow bucket and 
        we have to move the file into WebMeV storage.
        '''
        logger.info('From executed operation outputs based on a remote Nextflow'
                    f'-based job, create a resource with name {name}')
        try:
            r = default_storage.create_resource_from_interbucket_copy(
                executed_op.owner,
                path
            )
            if workspace is not None:
                r.workspaces.add(workspace)
            r.name = name
            return r
        except Exception as ex:
            logger.info('Caught exception when copying a Nextflow job output'
                        ' to our storage. Removing the dummy Resource'
                        ' and re-raising.')
            raise ex


class LocalDockerSingleDataResourceConverter(
        BaseResourceConverter,
        LocalResourceMixin,
        SingleDataResourceMixin,
        DataResourceMixin):
    '''
    This converter handles inputs and outputs which are related to jobs
    that are run using the local Docker-based runner.

    This converter is appropriate for DataResource types which have
    a single output and a fixed resource_type
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        '''
        This converter takes a DataResource instance (for a single file,
        which is simply a pk/UUID), copies the file to a staging directory 
        and returns the path to the copied file. That file path
        is then used to run the local, Docker-based job
        '''
        return self._convert_resource_input(user_input, staging_dir)

    def convert_output(self,
                       executed_op, workspace, output_definition, output_val):
        '''
        This converts a single output resource (a path) to a Resource instance
        and returns the pk/UUID for that newly created database resource.
        '''
        return self._convert_output(
            executed_op, workspace, output_definition, output_val)


class LocalDockerSingleVariableDataResourceConverter(
        BaseResourceConverter,
        LocalResourceMixin,
        SingleDataResourceMixin,
        VariableDataResourceMixin):
    '''
    This converter handles inputs and outputs which are related to jobs
    that are run using the local Docker-based runner.

    This converter is appropriate for VariableDataResource types which can
    assume multiple resource types. Examples of this include operations that
    perform generic operations on multitude of file types.

    As the name suggests, use for converting:
    - VariableDataResource
    - a single output
    - local runner
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        '''
        This converter takes a DataResource instance (for a single file,
        which is simply a pk/UUID), copies the file to a staging directory 
        and returns the path to the copied file. That file path
        is then used to run the local, Docker-based job
        '''
        return self._convert_resource_input(user_input, staging_dir)

    def convert_output(self,
                       executed_op, workspace, output_definition, output_val):
        '''
        This converts a single output resource (a path) to a Resource instance
        and returns the pk/UUID for that newly created database resource.
        '''
        return self._convert_output(
            executed_op, workspace, output_definition, output_val)


class LocalDockerSingleDataResourceWithTypeConverter(
        BaseResourceConverter,
        LocalResourceMixin,
        SingleDataResourceMixin,
        DataResourceMixin):
    '''
    This converter takes a DataResource instance (for a single file,
    which is simply a UUID) and returns a delimited string which includes
    the path and the resource type

    This converter takes that UUID, finds the Resource/file, brings it
    local, and returns the local path concatenated with the resource type.

    This converter saves us from having to define additional input fields 
    for certain ops. 

    As an example:
    consider an operation where we can take in a type of matrix 
    (e.g. MTX, EXP_MTX, etc.) and subset it (by rows or cols). 
    To output a resource of the same type, we need to know the input type.
    We don't need to solicit that input from the user since we already have
    it in our db.
    Hence, we convert to a delimited string which will indicate to the tool the
    resource type of the input. Often this means that the tool will perform 
    some operation and echo back that resource type in the outputs.
    '''

    # This is the delimiter we use to separate the path from the resource type
    DELIMITER = ':::'

    def convert_input(self, user_input, op_dir, staging_dir):
        '''
        user_input is the dictionary-representation of a 
        api.data_structures.UserOperationInput
        '''
        resource_uuid = user_input
        resource = get_resource_by_pk(resource_uuid)
        resource_type = resource.resource_type
        dest = self._convert_resource_input(resource_uuid, staging_dir)
        formatted_str = f'{dest}{self.DELIMITER}{resource_type}'
        return formatted_str


class LocalDockerMultipleDataResourceConverter(
        BaseResourceConverter,
        LocalResourceMixin,
        MultipleDataResourceMixin,
        DataResourceMixin):
    '''
    This converter takes a DataResource instance (for >1 file) and
    returns the path to the local files as a list. Typically, this 
    is then used with a mixin class to format the paths as a comma-delimited
    list, a space-delimited list, etc.

    For example, given the following DataResource:
    {
        'attribute_type': 'DataResource', 
        'value': [<UUID>, <UUID>, <UUID>]
    }

    This converter takes the list UUIDs, finds each Resource/file,
    brings it local, copies to a staging dir and returns a list of
    paths to those copies.
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        return self._convert_input(user_input, staging_dir)

    def convert_output(self, 
        executed_op, workspace, output_definition, output_val):
        '''
        This converts multiple output resources (paths) to Resource instances
        and returns their pk/UUIDs for the newly created database resources.
        '''
        return self._convert_output(
            executed_op, workspace, output_definition, output_val)


class LocalDockerMultipleVariableDataResourceConverter(
        BaseResourceConverter,
        LocalResourceMixin,
        MultipleDataResourceMixin,
        VariableDataResourceMixin):
    '''
    This converter takes a DataResource instance (for >1 file) and
    returns the path to the local files as a list. Typically, this 
    is then used with a mixin class to format the paths as a comma-delimited
    list, a space-delimited list, etc.

    For example, given the following DataResource:
    {
        'attribute_type': 'VariableDataResource', 
        'value': [<UUID>, <UUID>, <UUID>]
    }

    This converter takes the list UUIDs, finds each Resource/file, brings it local,
    copies to a staging dir and returns a list of paths to those copies.
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        return self._convert_input(user_input, staging_dir)

    def convert_output(self, 
        executed_op, workspace, output_definition, output_val):
        '''
        This converts multiple output resources (paths) to Resource instances
        and returns their pk/UUIDs for the newly created database resources.
        '''
        return self._convert_output(
            executed_op, workspace, output_definition, output_val)


class LocalDockerDelimitedResourceConverter(
        BaseResourceConverter,
        LocalResourceMixin,
        MultipleDataResourceMixin,
        DataResourceMixin):
    '''
    This is a base class for a converter that takes multiple
    inputs and creates a delimited string. The derived child 
    classes define the delimiter.
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        path_list = self._convert_input(user_input, staging_dir)
        return self.to_string(path_list)


class LocalDockerCsvResourceConverter(
        LocalDockerDelimitedResourceConverter,
        CsvMixin):
    '''
    Takes multiple resource inputs and converts them to a comma-delimited
    string with the multiple paths
    '''
    pass


class LocalDockerSpaceDelimResourceConverter(
        LocalDockerDelimitedResourceConverter,
        SpaceDelimMixin):
    '''
    Takes multiple resource inputs and converts them to a space-delimited
    string with the multiple paths
    '''
    pass


class RemoteNextflowSingleDataResourceConverter(
    BaseResourceConverter,
    RemoteResourceMixin,
    SingleDataResourceMixin,
    DataResourceMixin
):
    '''
    This converter (for inputs) takes a DataResource instance (for a single file,
    which is simply a UUID) and returns the path to 
    the file in cloud storage.

    Note that if remote execution is enabled, we do not allow local storage, 
    so we do not need to handle cases where we might have to push a 
    local file into cloud-based storage.
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        return self._convert_resource_input(user_input, staging_dir)

    def convert_output(
            self, executed_op, workspace, output_definition, output_val):
        '''
        This converts a single output resource (a path) to a Resource instance
        and returns the pk/UUID for that newly created database resource.

        Note, however, that the `output_val` is a list due to the nature
        of locating Nextflow-based job outputs. Hence, we pass the first
        element
        '''
        if type(output_val) is not list:
            raise OutputConversionException('The RemoteNextflowSingleDataResourceConverter'
            ' converter expects a list')
        else:
            if len(output_val) > 1:
                raise OutputConversionException('The RemoteNextflowSingleDataResourceConverter'
                    ' expects a list of length 1. The passed'
                    f' list had length of {len(output_val)}')
            return self._convert_output(
                executed_op, workspace, output_definition, output_val[0])


class RemoteNextflowSingleVariableDataResourceConverter(
    BaseResourceConverter,
    RemoteResourceMixin,
    SingleDataResourceMixin,
    VariableDataResourceMixin
):
    '''
    This converter takes a VariableDataResource instance 
    (for a single file,which is simply a UUID) and returns 
    the path to the file in cloud storage.
    '''

    def convert_input(self, user_input, op_dir, staging_dir):
        return self._convert_resource_input(user_input, staging_dir)

    def convert_output(
            self, executed_op, workspace, output_definition, output_val):
        '''
        This converts a single output resource (a path) to a Resource instance
        and returns the pk/UUID for that newly created database resource.

        Note, however, that the `output_val` is a list due to the nature
        of locating Nextflow-based job outputs.
        '''
        if type(output_val) is not list:
            raise OutputConversionException(f'The {RemoteNextflowSingleVariableDataResourceConverter}'
            ' converter expects a list')
        return self._convert_output(
            executed_op, workspace, output_definition, output_val[0])

# TODO: Can re-implement when needed. This code was originally
#       developed in the context of remote Cromwell runners but 
#       also covers a relatively rare use-case
# class RemoteNextflowMultipleDataResourceConverter(
#         BaseResourceConverter,
#         RemoteNextflowResourceMixin,
#         MultipleDataResourceMixin,
#         DataResourceMixin):
#     '''
#     This converter takes a DataResource instance (for >1 file) and 
#     returns the path to the remote files as a list. Typically, this 
#     is then used with a mixin class to format the paths as a comma-delimited
#     list, a space-delimited list, etc.

#     For example, given the following DataResource:
#     {
#         'attribute_type': 'DataResource', 
#         'value': [<UUID>, <UUID>, <UUID>], 
#         'many': True, 
#         'resource_types': ['MTX', 'I_MTX', 'EXP_MTX', 'RNASEQ_COUNT_MTX']
#     }

#     This converter takes the list UUIDs, finds each Resource/file and returns
#     a list of the remote paths.
#     '''

#     def convert_input(self, user_input, op_dir, staging_dir):
#         return self._convert_input(user_input, staging_dir)

#     def convert_output(
#             self, executed_op, workspace, output_definition, output_val):
#         '''
#         This converts a an output corresponding to multiple
#         resources (paths) to Resource instances
#         and returns those pk/UUID for the newly created database resources.
#         '''
#         return self._convert_output(
#             executed_op, workspace, output_definition, output_val)
