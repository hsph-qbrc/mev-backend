import unittest
import unittest.mock as mock
import os
import json
import uuid

from django.core.exceptions import ImproperlyConfigured
from rest_framework.exceptions import ValidationError

from api.tests.base import BaseAPITestCase
from api.models import Workspace, \
    Resource, \
    ExecutedOperation, \
    WorkspaceExecutedOperation, \
    Operation
from api.converters.output_converters import BaseOutputConverter, \
    LocalOutputConverter, \
    RemoteCromwellOutputConverter
from api.exceptions import OutputConversionException, \
    StorageException, \
    ResourceValidationException

# the api/tests dir
TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(TESTDIR, 'operation_test_files')

class ExecutedOperationOutputConverterTester(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()

    def test_basic_attribute_outputs(self):

        all_user_workspaces = Workspace.objects.filter(owner=self.regular_user_1)
        if len(all_user_workspaces) < 1:
            raise ImproperlyConfigured('Need at least one Workspace for the regular user.')
        workspace = all_user_workspaces[0]

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )

        j = json.load(open(os.path.join(TESTDIR, 'non_resource_outputs.json')))
        output_definition = j['outputs']['pval']
        return_val = c.convert_output(executed_op, workspace, output_definition, 0.2)
        self.assertEqual(return_val, 0.2)

        with self.assertRaises(OutputConversionException) as ex:
            return_val = c.convert_output(executed_op, workspace, output_definition, -0.2)

        output_definition = j['outputs']['some_integer']
        with self.assertRaises(OutputConversionException) as ex:
            return_val = c.convert_output(executed_op, workspace, output_definition, 0.2)

        # we are strict and don't accept string-cast integers. 
        output_definition = j['outputs']['some_integer']
        with self.assertRaises(OutputConversionException) as ex:
            return_val = c.convert_output(executed_op, workspace, output_definition, '1')

        output_definition = j['outputs']['some_bool']
        self.assertTrue(c.convert_output(executed_op, workspace, output_definition, True))
        with self.assertRaises(OutputConversionException) as ex:
            return_val = c.convert_output(executed_op, workspace, output_definition, '1')


class ResourceOutputTester(BaseAPITestCase):
    '''
    This test class has tests that ensure the proper workings of methods related to
    validation and addition of resource outputs (i.e. both DataResource and VariableDataResource)
    '''
    def setUp(self):

        self.establish_clients()

        all_user_workspaces = Workspace.objects.filter(owner=self.regular_user_1)
        if len(all_user_workspaces) < 1:
            raise ImproperlyConfigured('Need at least one Workspace for the regular user.')
        self.workspace = all_user_workspaces[0]

        self.base_converter = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        self.job_name = 'foo'
        self.executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = self.job_name
        )

    def test_handle_storage_failure(self):
        resource_uuid = uuid.uuid4()
        mock_resource = mock.MagicMock()
        mock_resource.pk = resource_uuid
        # there WAS a storage failure, but given that we pass False,
        # it's not something we need to worry about. Hence, we simply
        # delete the resource and silently move on.
        self.base_converter.handle_storage_failure(mock_resource, False)
        mock_resource.delete.assert_called()

        mock_resource2 = mock.MagicMock()
        mock_resource2.pk = resource_uuid
        with self.assertRaises(OutputConversionException) as ex:
            self.base_converter.handle_storage_failure(mock_resource2, True)
        mock_resource2.delete.assert_called()

        # this should just silently move on.
        self.base_converter.handle_storage_failure(None, False)

        with self.assertRaises(OutputConversionException) as ex:
            self.base_converter.handle_storage_failure(None, True)

    @mock.patch('api.converters.output_converters.ResourceMetadata')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.create_output_filename')
    @mock.patch('api.converters.output_converters.initiate_resource_validation')
    @mock.patch('api.converters.output_converters.retrieve_resource_class_standard_format')
    def _test_resource_addition_makes_proper_calls(self, \
        converter,
        mock_resource,
        mock_retrieve_resource_class_standard_format, \
        mock_initiate_resource_validation, \
        mock_create_output_filename, \
        mock_resourcemetadata_model
        ):
        '''
        Test that all the expected calls are made when everything works as expected.

        Note that this function isn't directly used-- you call it from an actual
        test function (e.g. starts with 'test_') and pass it an implementation of
        the storage class.
        '''
        mock_path = '/some/path/to/file.tsv'
        resource_type = 'MTX'
        output_required = True
        mock_name = 'foo.tsv'
        mock_create_output_filename.return_value = mock_name

        file_format = 'abc'
        mock_retrieve_resource_class_standard_format.return_value = file_format

        mock_resource_metadata_obj = mock.MagicMock()
        mock_resourcemetadata_model.objects.get.return_value = mock_resource_metadata_obj
        return_val = converter.attempt_resource_addition(
            self.executed_op, self.workspace, mock_path, resource_type, output_required
        )
        converter.create_resource.assert_called_with(
            self.executed_op,
            self.workspace,
            mock_path, 
            mock_name,
            output_required
        )
        mock_initiate_resource_validation.assert_called_with(
            mock_resource, resource_type, file_format)

        mock_resourcemetadata_model.objects.get.assert_called()
        mock_resource_metadata_obj.save.assert_called()
        self.assertEqual(return_val, str(mock_resource.pk))
        self.assertTrue(mock_resource.is_active)
        mock_resource.save.assert_called()

    @mock.patch('api.converters.output_converters.ResourceMetadata')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.handle_invalid_resource_type')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.create_output_filename')
    @mock.patch('api.converters.output_converters.initiate_resource_validation')
    @mock.patch('api.converters.output_converters.retrieve_resource_class_standard_format')
    def _test_resource_addition_makes_proper_calls_if_resource_type_invalid(self, \
        converter,
        mock_resource,
        mock_retrieve_resource_class_standard_format, \
        mock_initiate_resource_validation, \
        mock_create_output_filename, \
        mock_handle_invalid_resource_type, \
        mock_resourcemetadata_model
        ):
        '''
        Test that all the expected calls are made when the output does not 
        properly validate as expected. Here a ResourceValidationException is raised.
        That particular exception indicates that file was "acceptable" (in the sense that
        it could be opened and parsed), but that the validation failed for a reason such 
        as an integer matrix having a float or similar.

        Note that this method is not directly tested, but rather used as a way to test
        the children classes of BaseOutputConverter.
        '''
        mock_path = '/some/path/to/file.tsv'
        resource_type = 'MTX'
        output_required = True
        mock_name = 'foo.tsv'
        mock_create_output_filename.return_value = mock_name
  
        file_format = 'abc'
        mock_retrieve_resource_class_standard_format.return_value = file_format

        # here, the validation method raises a ResourceValidationException which is a signal
        # that it failed to conform to our expected specification. This is a predictable failure
        # that we should specifically catch.
        mock_initiate_resource_validation.side_effect = ResourceValidationException('nope.')

        with self.assertRaises(OutputConversionException) as ex:
            return_val = converter.attempt_resource_addition(
                self.executed_op, self.workspace, mock_path, resource_type, output_required
            )

        converter.create_resource.assert_called_with(
            self.executed_op,
            self.workspace,
            mock_path, 
            mock_name,
            output_required
        )
        mock_initiate_resource_validation.assert_called_with(
            mock_resource, resource_type, file_format)
        mock_resourcemetadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_called_with(mock_resource)
        self.assertFalse(mock_resource.is_active)

    @mock.patch('api.converters.output_converters.ResourceMetadata')        
    @mock.patch('api.converters.output_converters.BaseOutputConverter.handle_invalid_resource_type')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.create_output_filename')
    @mock.patch('api.converters.output_converters.initiate_resource_validation')
    @mock.patch('api.converters.output_converters.retrieve_resource_class_standard_format')
    def _test_resource_addition_makes_proper_calls_if_resource_type_invalid_case2(self, \
        converter, \
        mock_resource, \
        mock_retrieve_resource_class_standard_format, \
        mock_initiate_resource_validation, \
        mock_create_output_filename, \
        mock_handle_invalid_resource_type, \
        mock_resourcemetadata_model
        ):
        '''
        Test that all the expected calls are made when the output does not 
        properly validate as expected. Here a generic exception is raised
        '''
        mock_path = '/some/path/to/file.tsv'
        resource_type = 'MTX'
        output_required = True
        mock_name = 'foo.tsv'
        mock_create_output_filename.return_value = mock_name
  
        file_format = 'abc'
        mock_retrieve_resource_class_standard_format.return_value = file_format

        # here, the validation method raises a general Exception which is a signal
        # that something unexpected is going on. If a file is generally well-formed
        # but simply fails to validate (e.g. fails due to an integer matrix having floats)
        # then a more specific exception is raised.
        # In either case, that's a problem.
        mock_initiate_resource_validation.side_effect = Exception('ack!')

        with self.assertRaises(OutputConversionException) as ex:
            return_val = converter.attempt_resource_addition(
                self.executed_op, self.workspace, mock_path, resource_type, output_required
            )

        converter.create_resource.assert_called_with(
            self.executed_op,
            self.workspace,
            mock_path, 
            mock_name,
            output_required
        )
        mock_initiate_resource_validation.assert_called_with(
            mock_resource, resource_type, file_format)
        mock_resourcemetadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_called_with(mock_resource)
        self.assertFalse(mock_resource.is_active)

    def test_local_resource_converter(self):
        '''
        This function instantiates a LocalOutputConverter
        and passes it to another function that runs the
        test. This way we don't have to duplicate the test code
        for the multiple derived classes of BaseOutputConverter
        '''
        converter = LocalOutputConverter()

        # create a mock Resource
        resource_uuid = uuid.uuid4()
        mock_resource = mock.MagicMock()
        mock_resource.pk = resource_uuid
        mock_resource.is_active = False
        converter.create_resource = mock.MagicMock()
        converter.create_resource.return_value = mock_resource

        # now call the function that runs the test
        self._test_resource_addition_makes_proper_calls(converter, mock_resource)

        mock_resource.is_active = False
        self._test_resource_addition_makes_proper_calls_if_resource_type_invalid(
            converter, mock_resource)

        mock_resource.is_active = False
        self._test_resource_addition_makes_proper_calls_if_resource_type_invalid_case2(
            converter, mock_resource)

    def test_remote_cromwell_resource_converter(self):
        '''
        This function instantiates a RemoteCromwellOutputConverter
        converter and passes it to another function that runs the
        test. This way we don't have to duplicate the test code
        for the multiple derived classes of BaseOutputConverter
        '''
        converter = RemoteCromwellOutputConverter()

        # create a mock Resource
        resource_uuid = uuid.uuid4()
        mock_resource = mock.MagicMock()
        mock_resource.pk = resource_uuid
        mock_resource.is_active = False
        converter.create_resource = mock.MagicMock()
        converter.create_resource.return_value = mock_resource

        # now call the function that runs the test
        self._test_resource_addition_makes_proper_calls(converter, mock_resource)

        # reset the mock resource
        mock_resource.is_active = False
        self._test_resource_addition_makes_proper_calls_if_resource_type_invalid(
            converter, mock_resource)

        mock_resource.is_active = False
        self._test_resource_addition_makes_proper_calls_if_resource_type_invalid_case2(
            converter, mock_resource)
    
    @mock.patch('api.converters.output_converters.ResourceMetadata')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.handle_invalid_resource_type')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.create_output_filename')
    @mock.patch('api.converters.output_converters.initiate_resource_validation')
    @mock.patch('api.converters.output_converters.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.output_converters.alert_admins')
    def test_local_resource_addition_raises_storage_exception(self, \
        mock_alert_admins, \
        mock_retrieve_resource_class_standard_format, \
        mock_initiate_resource_validation, \
        mock_create_output_filename, \
        mock_handle_invalid_resource_type, \
        mock_resource_metadata_model
        ):
        '''
        Test that we respond appropriately if a storage exception is raised when
        attempting to add a local resource (e.g. from a Docker-based analysis). 
        '''
        converter = LocalOutputConverter()
        mock_create_resource = mock.MagicMock()
        mock_create_resource.side_effect = StorageException('!!')
        converter.create_resource = mock_create_resource

        mock_path = '/some/path/to/file.tsv'
        resource_type = 'MTX'
        output_required = True
        mock_name = 'foo.tsv'
        mock_create_output_filename.return_value = mock_name
  
        with self.assertRaises(OutputConversionException):
            converter.attempt_resource_addition(
                self.executed_op, self.workspace, mock_path, resource_type, output_required
            )
        mock_create_resource.assert_called_with(
            self.executed_op,
            self.workspace,
            mock_path, 
            mock_name,
            output_required
        )
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_initiate_resource_validation.assert_not_called()

        # no metadata was added, etc. since there was no file to deal with
        mock_resource_metadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_not_called()

        # Now try with an optional output. Don't need to reset the mocks
        # since they were not called above.
        output_required = False
        converter.attempt_resource_addition(
            self.executed_op, self.workspace, mock_path, resource_type, output_required
        )
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_initiate_resource_validation.assert_not_called()

        # no metadata was added, etc. since there was no file to deal with
        mock_resource_metadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_not_called()

        # Now have the create_resource method raise a general exception.
        # This might happen if something completely unexpected occurs
        mock_create_resource.side_effect = Exception('!!')
        converter.attempt_resource_addition(
            self.executed_op, self.workspace, mock_path, resource_type, output_required
        )
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_initiate_resource_validation.assert_not_called()

        # no metadata was added, etc. since there was no file to deal with
        mock_resource_metadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_not_called()
        mock_alert_admins.assert_called()

    @mock.patch('api.converters.output_converters.ResourceMetadata')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.handle_invalid_resource_type')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.create_output_filename')
    @mock.patch('api.converters.output_converters.initiate_resource_validation')
    @mock.patch('api.converters.output_converters.retrieve_resource_class_standard_format')
    @mock.patch('api.converters.output_converters.alert_admins')
    def test_cromwell_resource_addition_raises_storage_exception(self, \
        mock_alert_admins, \
        mock_retrieve_resource_class_standard_format, \
        mock_initiate_resource_validation, \
        mock_create_output_filename, \
        mock_handle_invalid_resource_type, \
        mock_resource_metadata_model
        ):
        '''
        Test that we respond appropriately if a storage exception is raised when
        attempting to add a remote, Cromwell-based resource. 
        '''
        converter = RemoteCromwellOutputConverter()
        mock_create_resource = mock.MagicMock()
        mock_create_resource.side_effect = StorageException('!!')
        converter.create_resource = mock_create_resource

        mock_path = '/some/path/to/file.tsv'
        resource_type = 'MTX'
        output_required = True
        mock_name = 'foo.tsv'
        mock_create_output_filename.return_value = mock_name
  
        with self.assertRaises(OutputConversionException):
            converter.attempt_resource_addition(
                self.executed_op, self.workspace, mock_path, resource_type, output_required
            )
        mock_create_resource.assert_called_with(
            self.executed_op,
            self.workspace,
            mock_path, 
            mock_name,
            output_required
        )
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_initiate_resource_validation.assert_not_called()

        # no metadata was added, etc. since there was no file to deal with
        mock_resource_metadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_not_called()

        # Now try with an optional output. Don't need to reset the mocks
        # since they were not called above.
        output_required = False
        converter.attempt_resource_addition(
            self.executed_op, self.workspace, mock_path, resource_type, output_required
        )
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_initiate_resource_validation.assert_not_called()

        # no metadata was added, etc. since there was no file to deal with
        mock_resource_metadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_not_called()

        # Now have the create_resource method raise a general exception.
        # This might happen if something completely unexpected occurs
        mock_create_resource.side_effect = Exception('!!')
        converter.attempt_resource_addition(
            self.executed_op, self.workspace, mock_path, resource_type, output_required
        )
        mock_retrieve_resource_class_standard_format.assert_not_called()
        mock_initiate_resource_validation.assert_not_called()

        # no metadata was added, etc. since there was no file to deal with
        mock_resource_metadata_model.objects.get.assert_not_called()
        mock_handle_invalid_resource_type.assert_not_called()
        mock_alert_admins.assert_called()

    def test_create_output_filename(self):
        r = self.base_converter.create_output_filename('/some/random/path/myfile.tsv', 'myjob')
        self.assertEqual(r, 'myjob.myfile.tsv')

        r = self.base_converter.create_output_filename('/some/random/path/myfile.tsv', '')
        self.assertEqual(r, 'myfile.tsv')

class DataResourceOutputConverterTester(BaseAPITestCase):
    '''
    These tests check that outputs corresponding to DataResource instances are
    handled appropriately. Typically, they will check that the proper methods are called,
    but the methods themselves are mocked
    '''
    def setUp(self):
        self.establish_clients()

        all_user_workspaces = Workspace.objects.filter(owner=self.regular_user_1)
        if len(all_user_workspaces) < 1:
            raise ImproperlyConfigured('Need at least one Workspace for the regular user.')
        self.workspace = all_user_workspaces[0]

    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_dataresource_converts_properly(self,mock_attempt_resource_addition):
        '''
        When a DataResource is created as part of an ExecutedOperation,
        the outputs give it as a path (or list of paths). Here we test the single value

        The converter's job is to register that as a new file with the workspace
        and return the UUID of this new Resource
        '''

        resource_type = 'MTX'
        resource_uuid = str(uuid.uuid4())
        mock_attempt_resource_addition.return_value = resource_uuid

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_spec = {
            'attribute_type': 'DataResource',
            'many': False,
            'resource_type': resource_type
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        mock_path = '/some/output/path.txt'
        return_val = c.convert_output(executed_op, self.workspace, output_definition, mock_path)

        mock_attempt_resource_addition.assert_called_with(
            executed_op,
            self.workspace,
            mock_path,
            resource_type,
            True
        )
        self.assertEqual(return_val, str(resource_uuid))


    @mock.patch('api.converters.output_converters.initiate_resource_validation')
    @mock.patch('api.converters.output_converters.ResourceMetadata')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_dataresource_converts_list_properly(self,
        mock_attempt_resource_addition, 
        mock_resourcemetadata_model, 
        mock_initiate_resource_validation):
        '''
        When a DataResource is created as part of an ExecutedOperation,
        the outputs give it as a path (or list of paths). Here we test the list of paths

        The converter's job is to register that as a new file with the workspace
        and return the UUID of this new Resources
        '''

        resource_type = 'MTX'
        resource1_uuid = str(uuid.uuid4())
        resource2_uuid = str(uuid.uuid4())
        mock_attempt_resource_addition.side_effect = [resource1_uuid, resource2_uuid]

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_spec = {
            'attribute_type': 'DataResource',
            'many': True,
            'resource_type': resource_type
        }
        output_required = True
        output_definition = {
            'required': output_required,
            'spec': output_spec
        }
        mock_path1 = '/some/output/path1.txt'
        mock_path2 = '/some/output/path2.txt'
        return_val = c.convert_output(executed_op, 
            self.workspace, 
            output_definition, 
            [
                mock_path1,
                mock_path2
            ]
        )

        call1 = mock.call(
            executed_op,
            self.workspace,
            mock_path1,
            resource_type,
            output_required
        )
        call2 = mock.call(
            executed_op,
            self.workspace,
            mock_path2,
            resource_type,
            output_required
        )
        mock_attempt_resource_addition.assert_has_calls([
            call1,
            call2
        ])
        self.assertCountEqual(return_val, [str(resource1_uuid), str(resource2_uuid)])

    @mock.patch('api.converters.output_converters.BaseOutputConverter.cleanup')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_dataresource_failure_handled_properly_case1(self,
        mock_attempt_resource_addition,
        mock_clean):
        '''
        Ensures that the cleanup method is called in the case where an exception
        is raised when attempting to add a resource (e.g. through failure to 
        validate). 

        Here, only a single file is requested (which fails)
        '''
        mock_attempt_resource_addition.side_effect = OutputConversionException('something bad')

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        resource_type = 'MTX'
        output_spec = {
            'attribute_type': 'DataResource',
            'many': False,
            'resource_type': resource_type
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        mock_path = '/some/output/path.txt'
        with self.assertRaises(OutputConversionException) as ex:
            c.convert_output(executed_op, self.workspace, output_definition, mock_path)
        mock_clean.assert_called_with([])
        mock_attempt_resource_addition.assert_called_with(
            executed_op,
            self.workspace,
            mock_path,
            resource_type,
            True
        )

    @mock.patch('api.converters.output_converters.BaseOutputConverter.cleanup')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_dataresource_failure_handled_properly_case2(self,
        mock_attempt_resource_addition,
        mock_clean):
        '''
        Ensures that the cleanup method is called in the case where an exception
        is raised when attempting to add a resource (e.g. through failure to 
        validate). 

        Here, we test a situation where the first file passes
        but the second file fails.
        '''
        mock_uuid = str(uuid.uuid4())
        mock_attempt_resource_addition.side_effect = [
            mock_uuid,
            OutputConversionException('something bad')
        ]
        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_required = True
        resource_type = 'MTX'
        output_spec = {
            'attribute_type': 'DataResource',
            'many': True,
            'resource_type': resource_type
        }
        output_definition = {
            'required': output_required,
            'spec': output_spec
        }
        mock_path1 = '/some/output/path1.txt'
        mock_path2 = '/some/output/path2.txt'
        mock_paths = [mock_path1, mock_path2]
        with self.assertRaises(OutputConversionException) as ex:
            c.convert_output(executed_op, self.workspace, output_definition, mock_paths)

        call1 = mock.call(
            executed_op,
            self.workspace,
            mock_path1,
            resource_type,
            output_required
        )
        call2 = mock.call(
            executed_op,
            self.workspace,
            mock_path2,
            resource_type,
            output_required
        )
        mock_attempt_resource_addition.assert_has_calls([
            call1,
            call2
        ])
        mock_clean.assert_called_with([mock_uuid])


    


class VariableDataResourceOutputConverterTester(BaseAPITestCase):
    '''
    These tests check that outputs corresponding to VariableDataResource instances are
    handled appropriately. Typically, they will check that the proper methods are called,
    but the methods themselves are mocked
    '''
    def setUp(self):
        self.establish_clients()

        all_user_workspaces = Workspace.objects.filter(owner=self.regular_user_1)
        if len(all_user_workspaces) < 1:
            raise ImproperlyConfigured('Need at least one Workspace for the regular user.')
        self.workspace = all_user_workspaces[0]

    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_variabledataresource_conversion_failures_handled_properly(self,
        mock_attempt_resource_addition):
        '''
        When a VariableDataResource is created as part of an ExecutedOperation,
        the outputs give it as an object with keys of `path` and `resource_type`. 
        Recall that VariableDataResource instances allow us to dynamically set the 
        resource type of the output files.

        The converter's job is to register that as a new file with the workspace
        and return the UUID of this new Resource

        Here, we test the various ways this conversion can fail due to improperly formatted
        "payloads" produced by the ExecutedOperation. Failures there are limited to those
        introduced by analysis tool developers (e.g. they did not produce a correctly 
        formatted outputs.json as part of the analysis)
        '''

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_spec = {
            'attribute_type': 'VariableDataResource',
            'many': False,
            'resource_types': ['MTX', 'I_MTX']
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        # try with a string "value" (should be a dict). This should raise 
        # an exception since the resource type is not known otherwise
        with self.assertRaisesRegex(OutputConversionException, 'provided as an object/dict'):
            c.convert_output(executed_op, self.workspace, output_definition, '/some/output/path.txt')
        mock_attempt_resource_addition.assert_not_called()

        # try with a dict, but one that does not have the correct keys 
        # (missing the resource_type key)
        mock_attempt_resource_addition.reset_mock()
        with self.assertRaisesRegex(OutputConversionException, 'resource_type') as ex:
            c.convert_output(executed_op, self.workspace, output_definition, 
                {
                    'path':'/some/output/path.txt',
                }
            )
        mock_attempt_resource_addition.assert_not_called()

        # try with a dict, but one that does not have the correct keys 
        # (missing the path key)
        mock_attempt_resource_addition.reset_mock()
        with self.assertRaisesRegex(OutputConversionException, 'path') as ex:
            c.convert_output(executed_op, self.workspace, output_definition, 
                {
                    'pathS':'/some/output/path.txt',
                    'resource_type': 'MTX'
                }
            )
        mock_attempt_resource_addition.assert_not_called()

        # a resource_type that doesn't match the spec
        mock_attempt_resource_addition.reset_mock()
        with self.assertRaisesRegex(OutputConversionException, 'ANN'):
            c.convert_output(executed_op, self.workspace, output_definition, 
                {
                    'path':'/some/output/path.txt',
                    'resource_type': 'ANN'
                }
            )
        mock_attempt_resource_addition.assert_not_called()

        # the output is a list (with many =False set above)
        mock_attempt_resource_addition.reset_mock()
        with self.assertRaisesRegex(OutputConversionException, 'dict'):
            c.convert_output(executed_op, self.workspace, output_definition, 
                [{
                    'path':'/some/output/path.txt',
                    'resource_type': 'MTX'
                },]
            )
        mock_attempt_resource_addition.assert_not_called()

        # change the output spec so that we accept multiple outputs (many=True)
        output_spec = {
            'attribute_type': 'VariableDataResource',
            'many': True,
            'resource_types': ['MTX', 'I_MTX']
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        # since many=True, then we should be passing a list of objects. Here,
        # we only pass a single object.
        mock_attempt_resource_addition.reset_mock()
        with self.assertRaisesRegex(OutputConversionException, 'expect a list') as ex:
            c.convert_output(executed_op, self.workspace, output_definition, 
                {
                    'path':'/some/output/path.txt',
                    'resource_type': 'ANN'
                }
            )
        mock_attempt_resource_addition.assert_not_called()

        # here, the first and only item has the wrong type
        mock_attempt_resource_addition.reset_mock()
        with self.assertRaisesRegex(OutputConversionException, 'ANN') as ex:
            c.convert_output(executed_op, self.workspace, output_definition, 
                [{
                    'path':'/some/output/path.txt',
                    'resource_type': 'ANN'
                }]
            )
        mock_attempt_resource_addition.assert_not_called()

    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_variabledataresource_conversion_handled_properly(self,
        mock_attempt_resource_addition):
        '''
        When a VariableDataResource is created as part of an ExecutedOperation,
        the outputs give it as an object with keys of `path` and `resource_type`. 
        Recall that VariableDataResource instances allow us to dynamically set the 
        resource type of the output files.

        The converter's job is to register that as a new file with the workspace
        and return the UUID of this new Resource
        '''
        resource_type = 'MTX'
        resource_uuid = str(uuid.uuid4())
        mock_attempt_resource_addition.return_value = resource_uuid

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_spec = {
            'attribute_type': 'VariableDataResource',
            'many': False,
            'resource_types': ['MTX', 'I_MTX']
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        # a good request- has the proper format for the output and the resource_type is 
        # permitted (based on the output_spec)
        mock_path = '/some/output/path.txt'
        return_val = c.convert_output(executed_op, self.workspace, output_definition, 
            {
                'path': mock_path,
                'resource_type': resource_type
            }
        )
        expected_name = '{n}.path.txt'.format(n=job_name)
        mock_attempt_resource_addition.assert_called_with(
            executed_op,
            self.workspace,
            mock_path,
            resource_type,
            True  
        )
        self.assertEqual(return_val, str(resource_uuid))

    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_variabledataresource_converts_properly_for_many(self, 
        mock_attempt_resource_addition):
        '''
        When a VariableDataResource is created as part of an ExecutedOperation,
        the outputs give it as a list of objects. Each of those objects has keys 
        of `path` and `resource_type`. 

        The converter's job is to register that as a new file with the workspace
        and return the UUID of this new Resource
        '''

        resource_type = 'MTX'
        other_resource_type = 'I_MTX'

        resource1_uuid = str(uuid.uuid4())
        resource2_uuid = str(uuid.uuid4())

        mock_attempt_resource_addition.side_effect = [resource1_uuid, resource2_uuid]

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_spec = {
            'attribute_type': 'VariableDataResource',
            'many': True,
            'resource_types': [resource_type, other_resource_type]
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        mock_path1 = '/some/output/path1.txt'
        mock_path2 = '/some/output/path2.txt'
        return_val = c.convert_output(executed_op, 
            self.workspace, 
            output_definition, 
            [
                {
                    'path':mock_path1,
                    'resource_type': resource_type
                },
                {
                    'path':mock_path2,
                    'resource_type': resource_type
                }
            ]
        )

        call1 = mock.call(
            executed_op,
            self.workspace,
            mock_path1,
            resource_type,
            True 
        )
        call2 = mock.call(
            executed_op,
            self.workspace,
            mock_path2,
            resource_type,
            True 
        )
        mock_attempt_resource_addition.assert_has_calls([
            call1,
            call2
        ])
        self.assertCountEqual(return_val, [resource1_uuid, resource2_uuid])

    @mock.patch('api.converters.output_converters.BaseOutputConverter.cleanup')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_variabledataresource_converts_properly_for_many_with_failure(self, 
        mock_attempt_resource_addition,
        mock_clean):        
        '''
        Here we test that the proper cleanup method is called if one of the resources
        does not have a type that is acceptable for the expected operation outputs
        '''
        unacceptable_resource_type = 'RNASEQ_COUNT_MTX'
        resource_type = 'MTX'
        other_resource_type = 'I_MTX'

        resource1_uuid = str(uuid.uuid4())
        mock_attempt_resource_addition.return_value = resource1_uuid

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        # the spec allows two output types
        output_spec = {
            'attribute_type': 'VariableDataResource',
            'many': True,
            'resource_types': [resource_type, other_resource_type]
        }
        output_definition = {
            'required': True,
            'spec': output_spec
        }
        mock_path1 = '/some/output/path1.txt'
        mock_path2 = '/some/output/path2.txt'
        with self.assertRaises(OutputConversionException):
            c.convert_output(executed_op, 
                self.workspace, 
                output_definition, 
                [
                    {
                        'path':mock_path1,
                        'resource_type': resource_type
                    },
                    # this second output does not have a correct resource
                    # type for our operation
                    {
                        'path':mock_path2,
                        'resource_type': unacceptable_resource_type
                    }
                ]
            )

        # check that the first output was fine and we call the method
        # that would add the resource
        mock_attempt_resource_addition.assert_has_calls([
            mock.call(executed_op,
                self.workspace,
                mock_path1,
                resource_type,
                True 
            )]
        )
        # check that the cleanup method was called for the first resource
        # since we don't want to leave incomplete outputs
        mock_clean.assert_has_calls([
                mock.call([resource1_uuid])
            ]
        )
 
    @mock.patch('api.converters.output_converters.BaseOutputConverter.cleanup')
    @mock.patch('api.converters.output_converters.BaseOutputConverter.attempt_resource_addition')
    def test_variabledataresource_converts_properly_for_many_with_failure_case2(self, 
        mock_attempt_resource_addition,
        mock_clean):        
        '''
        Here we test that the proper cleanup method is called if one of the resources
        fails its validation ()
        '''
        resource_type = 'MTX'
        other_resource_type = 'I_MTX'

        resource1_uuid = str(uuid.uuid4())

        # mock the situation where the first resource validates, but the 
        # second raises an exception
        mock_attempt_resource_addition.side_effect = [resource1_uuid, OutputConversionException]

        c = BaseOutputConverter()
        job_id = str(uuid.uuid4())
        op = Operation.objects.all()[0]
        job_name = 'foo'
        executed_op = WorkspaceExecutedOperation.objects.create(
            id=job_id,
            workspace=self.workspace,
            owner=self.regular_user_1,
            inputs = {},
            operation = op,
            mode = '',
            job_name = job_name
        )
        output_required = True
        # the spec allows two output types
        output_spec = {
            'attribute_type': 'VariableDataResource',
            'many': True,
            'resource_types': [resource_type, other_resource_type]
        }
        output_definition = {
            'required': output_required,
            'spec': output_spec
        }
        mock_path1 = '/some/output/path1.txt'
        mock_path2 = '/some/output/path2.txt'
        with self.assertRaises(OutputConversionException):
            c.convert_output(executed_op, 
                self.workspace, 
                output_definition, 
                [
                    {
                        'path':mock_path1,
                        'resource_type': resource_type
                    },
                    # this second output does not have a correct resource
                    # type for our operation
                    {
                        'path':mock_path2,
                        'resource_type': resource_type
                    }
                ]
            )

        # check that we attempt to add both files. Recall that
        # we mocked a failure for the second.
        call1 = mock.call(
            executed_op,
            self.workspace,
            mock_path1,
            resource_type,
            output_required
        )
        call2 = mock.call(
            executed_op,
            self.workspace,
            mock_path2,
            resource_type,
            output_required
        )
        mock_attempt_resource_addition.assert_has_calls([call1, call2])

        # check that the cleanup method was called for the first resource
        # since we don't want to leave incomplete outputs
        mock_clean.assert_has_calls([
                mock.call([resource1_uuid])
            ]
        )
