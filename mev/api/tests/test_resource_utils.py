from io import BytesIO
import os
import copy
import random
import uuid
import unittest.mock as mock

import pandas as pd

from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured
from django.core.files import File

from rest_framework.exceptions import ValidationError

from constants import DB_RESOURCE_KEY_TO_HUMAN_READABLE, \
    OBSERVATION_SET_KEY, \
    FEATURE_SET_KEY, \
    PARENT_OP_KEY, \
    RESOURCE_KEY, \
    TSV_FORMAT, \
    CSV_FORMAT, \
    WILDCARD, \
    UNSPECIFIED_FORMAT, \
    MATRIX_KEY, \
    INTEGER_MATRIX_KEY, \
    ANNOTATION_TABLE_KEY

from data_structures.observation_set import ObservationSet

from resource_types import RESOURCE_MAPPING, \
    GeneralResource, \
    AnnotationTable, \
    Matrix, \
    IntegerMatrix

from api.models import Resource, \
    ResourceMetadata, \
    Operation as OperationDb, \
    OperationResource
from api.serializers.resource_metadata import ResourceMetadataSerializer
from api.utilities.resource_utilities import initiate_resource_validation, \
    handle_valid_resource, \
    handle_invalid_resource, \
    check_file_format_against_type, \
    add_metadata_to_resource, \
    get_resource_by_pk, \
    write_resource, \
    retrieve_resource_class_instance, \
    check_resource_request_validity, \
    delete_resource_by_pk, \
    retrieve_metadata, \
    retrieve_resource_class_standard_format, \
    check_if_resource_unset
from exceptions import NoResourceFoundException, \
    ResourceValidationException, \
    InactiveResourceException, \
    OwnershipException
from api.tests.base import BaseAPITestCase
from api.tests.test_helpers import associate_file_with_resource

BASE_TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(BASE_TESTDIR, 'operation_test_files')
VALIDATION_TESTDIR = os.path.join(BASE_TESTDIR, 'resource_validation_test_files')


class TestResourceUtilities(BaseAPITestCase):
    '''
    Tests the functions contained in the api.utilities.resource_utilities
    module.
    '''
    def setUp(self):
        self.establish_clients()
        
    def get_unset_resource(self):
        all_resources = Resource.objects.all()
        unset_resources = []
        for r in all_resources:
            if not r.resource_type:
                unset_resources.append(r)
        
        if len(unset_resources) == 0:
            raise ImproperlyConfigured('Need at least one'
                ' Resource without a type to test properly.'
            )
        return unset_resources[0]

    @mock.patch('api.utilities.resource_utilities.get_resource_by_pk')
    def test_resource_request_validity(self, mock_get_resource_by_pk):
        '''
        Test that we receive the proper result when
        a api.models.Resource instance is requested.
        This function is used to ensure that users can only
        access their own Resource instances
        '''
        active_owned_resources = Resource.objects.filter(owner=self.regular_user_1, is_active=True)
        inactive_owned_resources = Resource.objects.filter(owner=self.regular_user_1, is_active=False)
        active_resource = active_owned_resources[0]
        inactive_resource = inactive_owned_resources[0]
        active_pk = str(active_resource.pk)
        inactive_pk = str(inactive_resource.pk)

        mock_get_resource_by_pk.return_value = active_resource
        result = check_resource_request_validity(self.regular_user_1, active_pk)
        self.assertEqual(active_resource, result)

        mock_get_resource_by_pk.return_value = inactive_resource
        with self.assertRaises(InactiveResourceException):
            result = check_resource_request_validity(self.regular_user_1, inactive_pk)

        mock_get_resource_by_pk.return_value = active_resource
        with self.assertRaises(OwnershipException):
            result = check_resource_request_validity(self.regular_user_2, active_pk)

        mock_get_resource_by_pk.side_effect = NoResourceFoundException
        with self.assertRaises(NoResourceFoundException):
            check_resource_request_validity(self.regular_user_1, active_pk)


    def test_get_resource_by_pk_works_for_all_resources(self):
        '''
        We use the api.utilities.resource_utilities.get_resource_by_pk
        function to check for the existence of all children of the 
        AbstractResource class. Test that it all works as expected.
        '''
        with self.assertRaises(NoResourceFoundException):
            get_resource_by_pk(uuid.uuid4())

        r = Resource.objects.all()
        r = r[0]
        r2 = get_resource_by_pk(r.pk)
        self.assertEqual(r,r2)

        ops = OperationDb.objects.all()
        op = ops[0]
        r3 = OperationResource.objects.create(
            operation = op,
            input_field = 'foo',
            name = 'foo.txt',
            resource_type = 'MTX',
            file_format = TSV_FORMAT,
            datafile = File(BytesIO(), 'xyz.tsv')
        )
        r4 = get_resource_by_pk(r3.pk)
        self.assertEqual(r3,r4)

    @mock.patch('api.utilities.resource_utilities.alert_admins')
    @mock.patch('api.utilities.resource_utilities.get_resource_by_pk')
    def test_resource_delete(self, mock_get_resource_by_pk, mock_alert_admins):
        '''
        Tests that the function for deleting a database record works as expected
        '''
        # mock the simple/expected deletion
        mock_resource = mock.MagicMock()
        mock_get_resource_by_pk.return_value = mock_resource
        mock_pk = str(uuid.uuid4())
        delete_resource_by_pk(mock_pk)
        mock_get_resource_by_pk.assert_called_with(mock_pk)
        mock_resource.delete.assert_called()

        # mock that the resource was inactive. This function does NOT care about that.
        # Any deletion logic (e.g. disallowing for inactive files) should be implemented
        # prior to calling this function. Hence, the delete should succeed below:
        mock_resource = mock.MagicMock()
        mock_resource.is_active = False
        mock_get_resource_by_pk.return_value = mock_resource
        delete_resource_by_pk(mock_pk)
        mock_get_resource_by_pk.assert_called_with(mock_pk)
        mock_resource.delete.assert_called()

        # check that a database deletion failure will notify the admins
        mock_resource = mock.MagicMock()
        mock_resource.delete.side_effect = Exception('ack!')
        mock_get_resource_by_pk.return_value = mock_resource
        delete_resource_by_pk(mock_pk)
        mock_get_resource_by_pk.assert_called_with(mock_pk)
        mock_resource.delete.assert_called()
        mock_alert_admins.assert_called()

    def test_retrieve_metadata(self):
        mock_resource_class_instance = mock.MagicMock()
        mock_metadata = {
            'mock_key': 'mock_value'
        }
        mock_resource_class_instance.extract_metadata.return_value = mock_metadata
        mock_path = '/some/mock/path.tsv'
        result = retrieve_metadata(mock_path, mock_resource_class_instance)
        self.assertDictEqual(result, mock_metadata)
        mock_resource_class_instance.extract_metadata.assert_called_with(mock_path)

        v = ValidationError({'key': 'val'})
        mock_resource_class_instance.extract_metadata.side_effect = v
        with self.assertRaisesRegex(ResourceValidationException, 'key:val') as ex:
            retrieve_metadata(mock_path, mock_resource_class_instance)

        mock_resource_class_instance.extract_metadata.side_effect = Exception('ack')
        with self.assertRaisesRegex(Exception, 'unexpected issue') as ex:
            retrieve_metadata(mock_path, mock_resource_class_instance)

    @mock.patch('api.utilities.resource_utilities.get_standard_format')
    def test_retrieve_standard_format(self, mock_get_standard_format):
        expected_format = 'foo_123'
        mock_type = 'foobar'
        mock_get_standard_format.return_value = expected_format
        result = retrieve_resource_class_standard_format(mock_type)
        self.assertEqual(result, expected_format)

        mock_get_standard_format.side_effect = KeyError('abc!')
        with self.assertRaisesRegex(Exception, mock_type):
            retrieve_resource_class_standard_format(mock_type)

    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.perform_validation')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    def test_invalid_handler_called(self, mock_handle_valid_resource, \
            mock_perform_validation, \
            mock_handle_invalid_resource, \
            mock_retrieve_resource_class_instance):
        '''
        Here we test that a failure to validate the resource calls the proper
        handler function.
        '''
        unset_resource = self.get_unset_resource()

        mock_resource_class_instance = mock.MagicMock()
        mock_resource_class_instance.performs_validation.return_value = True
        mock_msg = 'some error message'
        mock_perform_validation.return_value = (False, mock_msg)
        mock_retrieve_resource_class_instance.return_value = mock_resource_class_instance

        initiate_resource_validation(unset_resource, 'MTX', 'csv')

        mock_handle_invalid_resource.assert_called_with(
            unset_resource,
            'MTX',
            'csv',
            mock_msg
        )
        mock_retrieve_resource_class_instance.assert_called_with('MTX')
        mock_handle_valid_resource.assert_not_called()

        # requery the resource
        r = Resource.objects.get(pk=unset_resource.pk)
        self.assertIsNone(r.resource_type)
        if unset_resource.file_format == '':
            self.assertEqual(r.file_format, '')
        else:
            self.assertIsNone(r.file_format)

    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    @mock.patch('api.utilities.resource_utilities.perform_validation')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    def test_valid_handler_called(self, mock_handle_valid_resource, \
            mock_perform_validation, \
            mock_check_file_format_against_type, \
            mock_handle_invalid_resource, \
            mock_retrieve_resource_class_instance):
        '''
        Here we test that a successful validation calls the proper
        handler function.
        '''
        unset_resource = self.get_unset_resource()

        mock_resource_class_instance = mock.MagicMock()
        mock_resource_class_instance.STANDARD_FORMAT = TSV_FORMAT
        mock_resource_class_instance.performs_validation.return_value = True
        mock_retrieve_resource_class_instance.return_value = mock_resource_class_instance
        
        mock_perform_validation.return_value = (True, None)

        initiate_resource_validation(unset_resource, 'MTX', TSV_FORMAT)

        mock_handle_valid_resource.assert_called_with(
            unset_resource,
            mock_resource_class_instance
        )
        mock_handle_invalid_resource.assert_not_called()

        # requery the resource
        r = Resource.objects.get(pk=unset_resource.pk)
        self.assertTrue(r.resource_type == 'MTX')
        self.assertTrue(r.file_format == TSV_FORMAT)

    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    def test_proper_exceptions_raised_case1(self, \
        mock_check_file_format_against_type, \
        mock_handle_invalid_resource, \
        mock_handle_valid_resource, \
        mock_retrieve_resource_class_instance):
        '''
        If unexpected errors (like connecting to cloud storage occur), check that we raise exceptions
        that provide helpful errors.

        Here, we test if the `check_file_format_against_type` function raises an exception. Note that if
        a predictable failure there (i.e. an inconsistent resource type and format were specified), then
        the function raises a ResourceValidationException. Since this exception is not expected, it is NOT
        of that type.
        '''
        mock_check_file_format_against_type.side_effect = [Exception('something unexpected!')]

        unset_resource = self.get_unset_resource()

        with self.assertRaisesRegex(Exception, 'something unexpected'):
            initiate_resource_validation(unset_resource, 'MTX', TSV_FORMAT)
        mock_handle_valid_resource.assert_not_called()
        mock_handle_invalid_resource.assert_not_called()
        mock_retrieve_resource_class_instance.assert_not_called()

    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    def test_proper_exceptions_raised_case2(self, \
        mock_check_file_format_against_type, \
        mock_handle_invalid_resource, \
        mock_handle_valid_resource, \
        mock_retrieve_resource_class_instance):
        '''
        If unexpected errors (like connecting to cloud storage occur), check that we raise exceptions
        that provide helpful errors.

        Here, we test if the `retrieve_resource_class_instance` function raises an exception from something
        unexpected
        '''
        mock_retrieve_resource_class_instance.side_effect = [Exception('ack'),]

        unset_resource = self.get_unset_resource()

        with self.assertRaisesRegex(Exception, 'ack'):
            initiate_resource_validation(unset_resource, 'MTX', TSV_FORMAT)
        mock_handle_valid_resource.assert_not_called()
        mock_handle_invalid_resource.assert_not_called()
        mock_retrieve_resource_class_instance.assert_called_with('MTX')


    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    def test_proper_exceptions_raised_case3(self, \
        mock_check_file_format_against_type, \
        mock_handle_invalid_resource, \
        mock_handle_valid_resource, \
        mock_retrieve_resource_class_instance):
        '''
        If unexpected errors (like connecting to cloud storage occur), check that we raise exceptions
        that provide helpful errors.

        Here, we test if the `retrieve_resource_class_instance` function raises an exception from an unknown
        resource type (a keyError). 
        '''
        mock_retrieve_resource_class_instance.side_effect = [KeyError('ZZZ'),]

        unset_resource = self.get_unset_resource()

        with self.assertRaisesRegex(Exception, 'ZZZ'):
            initiate_resource_validation(unset_resource, 'ZZZ', TSV_FORMAT)
        mock_handle_valid_resource.assert_not_called()
        mock_handle_invalid_resource.assert_not_called()
        mock_retrieve_resource_class_instance.assert_called_with('ZZZ')
    
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.perform_validation')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    def test_proper_exceptions_raised_case4(self, \
        mock_check_file_format_against_type, \
        mock_perform_validation, \
        mock_retrieve_resource_class_instance, \
        mock_handle_invalid_resource, \
        mock_handle_valid_resource):
        '''
        If unexpected errors (like connecting to cloud storage occur), check that we raise exceptions
        that provide helpful errors.

        Here, we test if the validation method fails unexpectedly.
        '''
        mock_resource_class_instance = mock.MagicMock()
        mock_resource_class_instance.performs_validation.return_value = True
        mock_retrieve_resource_class_instance.return_value = mock_resource_class_instance

        err_msg = 'something unexpected.'
        mock_perform_validation.side_effect = [Exception(err_msg)]

        unset_resource = self.get_unset_resource()

        with self.assertRaisesRegex(Exception, err_msg):
            initiate_resource_validation(unset_resource, 'MTX', TSV_FORMAT)

        mock_perform_validation.assert_called_with(
            unset_resource,
            mock_resource_class_instance,
            TSV_FORMAT
        )
        mock_handle_valid_resource.assert_not_called()
        mock_handle_invalid_resource.assert_not_called()

    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_invalid_resource')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    @mock.patch('api.utilities.resource_utilities.perform_validation')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    def test_proper_exceptions_raised_case5(self, mock_handle_valid_resource, \
            mock_perform_validation, \
            mock_check_file_format_against_type, \
            mock_handle_invalid_resource, \
            mock_retrieve_resource_class_instance):
        '''
        Here we test that a failure to extract metadata results in the resource
        not updating its fields. For instance, if we perform a 'standardization'
        but we ultimately cannot extract/save the metadata, we do NOT want to 
        update the Resource.datafile attribute to that new standardized version. We only
        want to update the path to the standardized version if everything goes perfectly.
        '''

        def mock_save_in_standardized_format(resource, file_format):
            '''
            This allows us to avoid the actual implementation of the 
            'save_in_standardized_format' method.

            Here we just save some empty content which (since we are 
            ultimately mocking a failure) doesn't matter. It does the 
            same thing as the real method in that it re-assigns the
            datafile attribute to a new file.
            '''
            u = str(uuid.uuid4())
            with BytesIO() as b:
                resource.datafile = File(b,u)
                resource.save()

        unset_resource = self.get_unset_resource()
        original_path = unset_resource.datafile.name
        self.assertTrue(len(original_path)>0)

        mock_resource_class_instance = mock.MagicMock()
        mock_resource_class_instance.STANDARD_FORMAT = TSV_FORMAT
        mock_resource_class_instance.performs_validation.return_value = True
        mock_resource_class_instance.save_in_standardized_format = mock_save_in_standardized_format
        mock_retrieve_resource_class_instance.return_value = mock_resource_class_instance
        
        mock_perform_validation.return_value = (True, None)

        mock_handle_valid_resource.side_effect = Exception('something bad!')

        with self.assertRaisesRegex(Exception, 'something bad'):
            initiate_resource_validation(unset_resource, 'MTX', TSV_FORMAT)

        mock_handle_valid_resource.assert_called_with(
            unset_resource,
            mock_resource_class_instance
        )
        mock_handle_invalid_resource.assert_not_called()

        # requery the resource
        r = Resource.objects.get(pk=unset_resource.pk)

        # Note that the path did NOT remain the same.
        self.assertTrue(r.datafile.path != original_path)
        # the type and format were not set to the requested values:
        self.assertTrue(r.resource_type != 'MTX')
        self.assertTrue(r.file_format != TSV_FORMAT)

    @mock.patch('api.utilities.resource_utilities.check_if_resource_unset')
    def test_unset_resource_type_does_not_change_if_validation_fails(self, \
            mock_check_if_resource_unset):
        '''
        For an unset resource (i.e. no resource_type or format) requesting
        a change that fails validation results in NO change to the resource_type
        attribute
        '''
        mock_check_if_resource_unset.return_value = True # True means previously unset
        unset_resource = self.get_unset_resource()

        with self.assertRaises(ResourceMetadata.DoesNotExist):
            metadata = ResourceMetadata.objects.get(resource=unset_resource)

        handle_invalid_resource(unset_resource, 'MTX', TSV_FORMAT, 'some error message')
        self.assertIsNone(unset_resource.resource_type)

        # now the metadata query should succeed
        metadata = ResourceMetadata.objects.get(resource=unset_resource)
        metadata = ResourceMetadataSerializer(metadata).data
        self.assertIsNone(metadata[PARENT_OP_KEY])
        self.assertIsNone(metadata[OBSERVATION_SET_KEY])
        self.assertIsNone(metadata[FEATURE_SET_KEY])
        self.assertEqual(metadata[RESOURCE_KEY], unset_resource.pk)

    def test_check_if_resource_unset(self):
        mock_resource = mock.MagicMock()
        self.assertTrue(check_if_resource_unset(mock_resource))

        mock_resource = mock.MagicMock()
        mock_resource.resource_type = MATRIX_KEY
        self.assertFalse(check_if_resource_unset(mock_resource))

        mock_resource = mock.MagicMock()
        mock_resource.file_format = TSV_FORMAT
        self.assertFalse(check_if_resource_unset(mock_resource))

        mock_resource = mock.MagicMock()
        mock_resource.resource_type = MATRIX_KEY
        mock_resource.file_format = TSV_FORMAT
        self.assertFalse(check_if_resource_unset(mock_resource))

    @mock.patch('api.utilities.resource_utilities.check_if_resource_unset')
    @mock.patch('api.utilities.resource_utilities.add_metadata_to_resource')
    def test_resource_type_does_not_change_if_validation_fails(self, \
        mock_add_metadata_to_resource,
        mock_check_if_resource_unset
    ):
        '''
        If we had previously validated a resource successfully, requesting
        a change that fails validation results in NO change to the resource_type
        attribute
        '''
        mock_check_if_resource_unset.return_value = False

        all_resources = Resource.objects.all()
        set_resources = []
        for r in all_resources:
            if r.resource_type:
                set_resources.append(r)
        
        if len(set_resources) == 0:
            raise ImproperlyConfigured('Need at least one'
                ' Resource with a type to test properly.'
            )

        resource = set_resources[0]
        original_type = resource.resource_type
        other_type = original_type
        while other_type == original_type:
            other_type = random.choice(list(RESOURCE_MAPPING.keys()))
        handle_invalid_resource(resource, other_type, TSV_FORMAT, 'some error message')

        # we don't query for an 'updated' Resource since the `handle_invalid_resource`
        # function doesn't save. It only updates attributes.
        self.assertTrue(resource.resource_type == original_type)
        self.assertTrue(resource.status.startswith(Resource.REVERTED.format(
            requested_resource_type=DB_RESOURCE_KEY_TO_HUMAN_READABLE[other_type],
            original_resource_type = DB_RESOURCE_KEY_TO_HUMAN_READABLE[original_type],
            requested_file_format = TSV_FORMAT,
            file_format = resource.file_format
            )
        ))
        mock_add_metadata_to_resource.assert_not_called()


    @mock.patch.dict('api.utilities.resource_utilities.DB_RESOURCE_KEY_TO_HUMAN_READABLE', \
        {'foo_type': 'Table'})
    @mock.patch('api.utilities.resource_utilities.format_is_acceptable_for_type')
    @mock.patch('api.utilities.resource_utilities.get_acceptable_formats')
    def test_inconsistent_file_extension_raises_proper_ex(self,
        mock_get_acceptable_formats,
        mock_format_is_acceptable_for_type):
        '''
        This tests the case where a user selects a resource type but the
        file does not have a format that is consistent with that type. We need
        to enforce canonical formats so we know how to try parsing files.
        '''
        mock_format_is_acceptable_for_type.return_value = False
        mock_get_acceptable_formats.return_value = ['tsv', 'csv', 'abc']
        requested_type = 'foo_type'
        human_readable_type = 'Table'
        file_format = 'xyz'
        with self.assertRaises(ResourceValidationException) as ex:
            check_file_format_against_type(requested_type, file_format)
            expected_status = Resource.UNKNOWN_FORMAT_ERROR.format(
                readable_resource_type = human_readable_type,
                fmt = resource.file_format,
                extensions_csv = 'tsv,csv,abc'
            )
            self.assertEqual(str(ex), expected_status)

    @mock.patch('api.utilities.resource_utilities.format_is_acceptable_for_type')
    def test_bad_resource_type_when_checking_type_and_format(self,
        mock_format_is_acceptable_for_type):
        '''
        This tests the case where a user selects a resource type that does not
        exist and the underlying function raises an exception
        '''
        mock_format_is_acceptable_for_type.side_effect = KeyError('ack')
        requested_type = 'foo_type'
        file_format = 'xyz'
        with self.assertRaisesRegex(ResourceValidationException, 'foo_type is not a known type'):
            check_file_format_against_type(requested_type, file_format)

    @mock.patch('api.utilities.resource_utilities.get_resource_type_instance')
    def test_bad_resource_type_when_retrieving_resource_type_instance(self,
        mock_get_resource_type_instance):
        '''
        This tests the case where a user selects a resource type that does not
        exist and the underlying function raises a KeyError
        '''
        mock_get_resource_type_instance.side_effect = KeyError('ack')
        requested_type = 'foo_type'
        with self.assertRaises(ResourceValidationException) as ex:
            retrieve_resource_class_instance(requested_type)
            expected_status = Resource.UNKNOWN_RESOURCE_TYPE_ERROR.format(
                requested_resource_type = requested_type
            )
            self.assertEqual(str(ex), expected_status)

    @mock.patch('api.utilities.resource_utilities.get_resource_type_instance')
    def test_unexpected_exception_when_retrieving_resource_type_instance(self,
        mock_get_resource_type_instance):
        '''
        This tests the case where a user selects a resource type that does not
        exist and the underlying function raises an exception
        '''
        mock_get_resource_type_instance.side_effect = Exception('ack')
        with self.assertRaises(Exception) as ex:
            retrieve_resource_class_instance(requested_type)

    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.handle_valid_resource')
    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    def test_proper_steps_taken_with_wildcard_resource(self, \
        mock_check_file_format_against_type, \
        mock_handle_valid_resource, \
        mock_retrieve_resource_class_instance):
        '''
        Here we test that a resource type with a "wildcard" type goes through the proper
        steps. That is, we should skip the validation, etc.
        '''
        all_resources = Resource.objects.all()
        r = all_resources[0]

        g = GeneralResource()
        mock_retrieve_resource_class_instance.return_value = g

        initiate_resource_validation(r, WILDCARD, UNSPECIFIED_FORMAT)

        mock_handle_valid_resource.assert_called_with(r,g)

        r = Resource.objects.get(pk=r.pk)
        self.assertTrue(r.resource_type == WILDCARD)
        self.assertTrue(r.file_format == UNSPECIFIED_FORMAT)

    def test_check_file_format_against_type_for_wildcard_resource(self):
        '''
        Checks that the type + format checking method just returns silently
        since we are trying to set to a wildcard/generic resource type
        '''
        self.assertIsNone(check_file_format_against_type(WILDCARD, ''))

    @mock.patch('api.utilities.resource_utilities.add_metadata_to_resource')
    @mock.patch('api.utilities.resource_utilities.retrieve_metadata')
    def test_check_handle_valid_resource_for_wildcard_type(self, \
        mock_retrieve_metadata, \
        mock_add_metadata_to_resource):
        '''
        Check that we do the proper things when we handle the apparently 
        "valid" resource. For wildcard types, they are trivially valid, but
        we need to check that we are not calling any methods that wouldn't 
        make sense in this context.
        '''
        mock_metadata = {
            'dummy_key': 'dummy_value'
        }
        mock_retrieve_metadata.return_value = mock_metadata

        all_resources = Resource.objects.all()
        r = all_resources[0]
        g = GeneralResource()
        handle_valid_resource(r, g)

        mock_retrieve_metadata.assert_called()
        mock_add_metadata_to_resource.assert_called_with(
            r,
            mock_metadata
        )

    @mock.patch('api.utilities.resource_utilities.retrieve_metadata')
    @mock.patch('api.utilities.resource_utilities.add_metadata_to_resource')
    def test_metadata_addition_failure(self, 
        mock_add_metadata_to_resource,
        mock_retrieve_metadata):
        '''
        Check that we do the proper things if the addition of the metadata
        to the resource fails.

        For instance, we had a case where the metadata extraction worked,
        but the sample IDs were too long. In that case, the `add_metadata_to_resource`
        function raised an uncaught exception and the Resource.path attribute
        was not set correctly.
        '''
        mock_metadata = {
            'mock_key': 'mock_val'
        }
        mock_retrieve_metadata.return_value = mock_metadata
        mock_add_metadata_to_resource.side_effect = ValidationError('ack')

        all_resources = Resource.objects.all()
        r = all_resources[0]
        mock_path = '/some/path/to/file.tsv'
        mock_resource_class_instance = mock.MagicMock()
        with self.assertRaisesRegex(Exception, 'ack'):
            handle_valid_resource(r, mock_resource_class_instance)

        expected_calls = [
            mock.call(r, mock_metadata),
            mock.call(r, {RESOURCE_KEY: r.pk})
        ]
        mock_add_metadata_to_resource.assert_has_calls(expected_calls)
        mock_retrieve_metadata.assert_called_with(r, mock_resource_class_instance)

    def test_add_metadata(self):
        '''
        Test that we gracefully handle updates
        when associating metadata with a resource.

        Have a case where we update and we create a new ResourceMetadata
        '''
        # create a new Resource
        r = Resource.objects.create(
            name='foo.txt',
            owner = self.regular_user_1,
            datafile = File(BytesIO(), 'foo.txt')
        )
        rm = ResourceMetadata.objects.create(
            resource=r
        )
        rm_pk = rm.pk

        mock_obs_set = {
            'elements': [
                {
                    'id': 'sampleA'
                },
                {
                    'id': 'sampleB'
                }
            ]
        }
        # verify that the mock above is valid
        oss = ObservationSet(mock_obs_set)

        add_metadata_to_resource(
            r, 
            {
                OBSERVATION_SET_KEY:mock_obs_set
            }
        )

        # query again, see that it was updated
        rm2 = ResourceMetadata.objects.get(pk=rm_pk)
        expected_obs_set = copy.deepcopy(mock_obs_set)
        elements = expected_obs_set['elements']
        for el in elements:
            el.update({'attributes': {}})
        self.assertCountEqual(rm2.observation_set['elements'], elements)

        # OK, now get a Resource that does not already have metadata
        # associated with it:        
        r = Resource.objects.create(
            name='bar.txt',
            owner = self.regular_user_1,
            datafile = File(BytesIO(), 'bar.txt')
        )
        with self.assertRaises(ResourceMetadata.DoesNotExist):
            ResourceMetadata.objects.get(resource=r)
        add_metadata_to_resource(
            r, 
            {OBSERVATION_SET_KEY:mock_obs_set}
        )

        # query again, see that it was updated
        rm3 = ResourceMetadata.objects.get(resource=r)
        expected_obs_set = copy.deepcopy(mock_obs_set)
        elements = expected_obs_set['elements']
        for el in elements:
            el.update({'attributes': {}})
        self.assertCountEqual(rm3.observation_set['elements'], elements)

    def test_add_metadata_with_null(self):
        '''
        Test that we can successfully add resource metadata 
        that contains null values.

        As an example, consider a basic annotation file where an entry is missing.
        The observation set we extract from that file will contain a null value
        '''
        # create a new Resource
        r = Resource.objects.create(
            name='foo.txt',
            owner = self.regular_user_1,
            datafile = File(BytesIO(), 'foo.txt')
        )
        rm = ResourceMetadata.objects.create(
            resource=r
        )
        rm_pk = rm.pk

        mock_obs_set = {
            'elements': [
                {
                    'id': 'sampleA',
                    'attributes' : {
                        "alcohol_history": {
                            "attribute_type": "UnrestrictedString",
                            "value": "yes"
                        },
                        "age_at_index": {
                            "attribute_type": "Float",
                            "value": 13.2
                        },
                    }
                },
                {
                    'id': 'sampleB',
                    'attributes' : {
                        "alcohol_history": {
                            "attribute_type": "UnrestrictedString",
                            "value": "yes"
                        },
                        "age_at_index": {
                            "attribute_type": "Float",
                            "value": None
                        },
                    }
                }
            ]
        }
        # verify that the mock above is valid
        oss = ObservationSet(mock_obs_set, permit_null_attributes=True)

        # if no errors here, then we are ok
        add_metadata_to_resource(
            r, 
            {
                OBSERVATION_SET_KEY:mock_obs_set
            }
        )

    @mock.patch('api.utilities.resource_utilities.retrieve_metadata')
    @mock.patch('api.utilities.resource_utilities.alert_admins')
    def test_catch_large_metadata_addition(self, \
        mock_alert_admins, \
        mock_retrieve_metadata
    ):
        '''
        Test that we gracefully handle problems when saving metadata

        This test stems from a bug where an annotation file (in tsv format) was
        parsed as a CSV. This produced a dataframe with shape (x,1), which is technically
        compliant. However, the metadata extracted from this table caused an issue saving
        in the database since the field exceeded the character limit. This was due to the 
        fact that an entire row of the file was parsed as a single entry (a very long string)
        '''
        mock_obs_set = {
            'multiple': True,
            'elements': [
                {
                    # this will cause metadata to fail since the ID is WAY too big
                    'id': 'sampleA'*5000 
                },
                {
                    'id': 'sampleB'
                }
            ]
        }
        mock_retrieve_metadata.return_value = {
            OBSERVATION_SET_KEY: mock_obs_set
        }

        # create a new Resource
        r = Resource.objects.create(
            name='foo.txt',
            owner = self.regular_user_1,
            datafile = File(BytesIO(), 'foo.txt')
        )
        # call the tested function
        resource_type = ANNOTATION_TABLE_KEY
        resource_class_instance = retrieve_resource_class_instance(resource_type)
        handle_valid_resource(r, resource_class_instance)
        mock_alert_admins.assert_called()

        rm = ResourceMetadata.objects.filter(resource=r)
        self.assertTrue(len(rm) == 1)
        rm0 = rm[0]
        self.assertIsNone(rm0.observation_set)
        self.assertTrue(rm0.resource == r)



    @mock.patch('api.utilities.resource_utilities.ResourceMetadataSerializer')
    @mock.patch('api.utilities.resource_utilities.alert_admins')
    def test_add_metadata_case2(self, mock_alert_admins, mock_serializer_cls):
        '''
        Test that we gracefully handle updates and save failures
        when associating metadata with a resource.

        Inspired by a runtime failure where the FeatureSet was too
        large for the database field. In such a case, we want to alert
        admins, but not stop a user from moving forward. Hence, we 
        recover from the failure by saving a bare-minimum metadata
        payload.
        '''
        # create a new Resource
        r = Resource.objects.create(
            name='foo.txt',
            owner = self.regular_user_1,
            datafile = File(BytesIO(), 'foo.txt')
        )
        # ensure it has no associated metadata
        with self.assertRaises(ResourceMetadata.DoesNotExist):
            ResourceMetadata.objects.get(resource=r)

        # create some legitimate metadata to add. We ultimately 
        # mock there being a failure when trying to save this,
        # but we at least give it real data here
        mock_obs_set = {
            'elements': [
                {
                    'id': 'sampleA'
                },
                {
                    'id': 'sampleB'
                }
            ]
        }
        # verify that the mock above is valid
        oss = ObservationSet(mock_obs_set)

        # create a mock object that will raise an exception
        from django.db.utils import OperationalError
        mock_serializer1 = mock.MagicMock()
        mock_serializer1.is_valid.return_value = True
        mock_serializer1.save.side_effect = OperationalError
        # The first time we ask for a ResourceMetadataSerializer, we mock
        # out the implementation so that we can fake an issue with its save
        # method. The second time, we use the actual class so we can verify
        # that we save only "basic" data in the event of an OperationalError
        basic_data = {
            RESOURCE_KEY: r.pk
        }
        real_instance = ResourceMetadataSerializer(data=basic_data)
        mock_serializer_cls.side_effect = [mock_serializer1, real_instance]
        add_metadata_to_resource(
            r, 
            {
                OBSERVATION_SET_KEY:mock_obs_set
            }
        )
        mock_alert_admins.assert_called()

        # check that we did actually persist the basic metadata to the db:
        rm = ResourceMetadata.objects.get(resource=r)
        rmd = ResourceMetadataSerializer(rm).data
        expected_metadata = {
            PARENT_OP_KEY: None,
            OBSERVATION_SET_KEY: None,
            FEATURE_SET_KEY: None,
            RESOURCE_KEY: r.pk
        }
        self.assertDictEqual(expected_metadata, rmd)

    @mock.patch('api.utilities.resource_utilities.ResourceMetadataSerializer')
    @mock.patch('api.utilities.resource_utilities.alert_admins')
    def test_add_metadata_case3(self, mock_alert_admins, mock_serializer_cls):
        '''
        Test that we gracefully handle updates and save failures
        when associating metadata with a resource.

        This covers the case where we encounter a generic Exception when
        trying to save the metadata. In such a case, we want to alert
        admins, but not stop a user from moving forward. Hence, we 
        recover from the failure by saving a bare-minimum metadata
        payload.
        '''
        # create a new Resource
        r = Resource.objects.create(
            name='foo.txt',
            owner = self.regular_user_1,
            datafile = File(BytesIO(), 'foo.txt')
        )
        # ensure it has no associated metadata
        with self.assertRaises(ResourceMetadata.DoesNotExist):
            ResourceMetadata.objects.get(resource=r)

        # create some legitimate metadata to add. We ultimately 
        # mock there being a failure when trying to save this,
        # but we at least give it real data here
        mock_obs_set = {
            'elements': [
                {
                    'id': 'sampleA'
                },
                {
                    'id': 'sampleB'
                }
            ]
        }
        # verify that the mock above is valid
        oss = ObservationSet(mock_obs_set)

        # create a mock object that will raise an exception
        mock_serializer1 = mock.MagicMock()
        mock_serializer1.is_valid.return_value = True
        mock_serializer1.save.side_effect = Exception('ack!')
        # The first time we ask for a ResourceMetadataSerializer, we mock
        # out the implementation so that we can fake an issue with its save
        # method. The second time, we use the actual class so we can verify
        # that we save only "basic" data in the event of an unexpected Exception
        basic_data = {
            RESOURCE_KEY: r.pk
        }
        real_instance = ResourceMetadataSerializer(data=basic_data)
        mock_serializer_cls.side_effect = [mock_serializer1, real_instance]
        add_metadata_to_resource(
            r, 
            {
                OBSERVATION_SET_KEY:mock_obs_set
            }
        )
        mock_alert_admins.assert_called()

        # check that we did actually persist the basic metadata to the db:
        rm = ResourceMetadata.objects.get(resource=r)
        rmd = ResourceMetadataSerializer(rm).data
        expected_metadata = {
            PARENT_OP_KEY: None,
            OBSERVATION_SET_KEY: None,
            FEATURE_SET_KEY: None,
            RESOURCE_KEY: r.pk
        }
        self.assertDictEqual(expected_metadata, rmd)


    @mock.patch('api.utilities.resource_utilities.make_local_directory')
    @mock.patch('api.utilities.resource_utilities.os')
    def test_resource_write_dir_fails(self, mock_os, mock_make_local_directory):
        '''
        Tests the case where we fail to create a directory
        to write into. Check that this is handled appropriately.
        '''
        mock_os.path.dirname.return_value = '/some/dir'
        mock_os.path.exists.return_value = False
        mock_make_local_directory.side_effect = Exception('something bad happened!')
        with self.assertRaises(Exception):
            write_resource('some content', '')

    @mock.patch('api.utilities.resource_utilities.make_local_directory')
    def test_resource_write_works_case1(self, mock_make_local_directory):
        '''
        Tests that we do, in fact, write correctly.
        Here, we use the /tmp folder, which exists
        '''
        self.assertTrue(os.path.exists('/tmp'))
        destination = '/tmp/some_file.txt'
        content = 'some content'
        write_resource(content, destination)
        self.assertTrue(os.path.exists(destination))
        read_content = open(destination).read()
        self.assertEqual(read_content, content)
        mock_make_local_directory.assert_not_called()
        # cleanup
        os.remove(destination)

    def test_resource_write_works_case2(self):
        '''
        Tests that we do, in fact, write correctly.
        Here, we write in a folder which doesn't already exist
        '''
        self.assertFalse(os.path.exists('/tmp/foo'))
        destination = '/tmp/foo/some_file.txt'
        content = 'some content'
        write_resource(content, destination)
        self.assertTrue(os.path.exists(destination))
        read_content = open(destination).read()
        self.assertEqual(read_content, content)
        # cleanup
        os.remove(destination)
        os.removedirs('/tmp/foo')

    def test_resource_write_only_writes_string(self):
        '''
        Tests that this function only handles strings.
        Below, we try to have it write a dict and that 
        should not work
        '''
        destination = '/tmp/some_file.txt'
        content = {'some_key': 'some_val'}
        with self.assertRaises(AssertionError):
            write_resource(content, destination)


    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.localize_resource')
    @mock.patch('api.utilities.resource_utilities.get_resource_size')
    def test_metadata_when_type_changed(self, mock_get_resource_size, \
        mock_localize_resource, \
        mock_retrieve_resource_class_instance, \
        mock_check_file_format_against_type):
        '''
        Checks that the update of resource metadata is updated. Related to a bug where
        a file was initially set to a general type (and thus the metadata was effectively empty).
        After trying to validate it as an annotation type, it was raising json serializer errors.
        '''
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_annotation_valid.tsv')

        # define this mock function so we can patch the class
        # implementing the validation methods
        def mock_save_in_standardized_format(resource_instance, format):
            return None

        patched_ann_table_instance = AnnotationTable()
        patched_ann_table_instance.save_in_standardized_format = mock_save_in_standardized_format
        mock_retrieve_resource_class_instance.side_effect = [
            # note that we don't need to patch this since GeneralResource instances
            # do not perform validation
            GeneralResource(),
            patched_ann_table_instance
        ]

        mock_localize_resource.return_value = resource_path
        mock_size = 100
        mock_get_resource_size.return_value = mock_size

        # create an empty/dummy Resource
        r = Resource.objects.create(
            name = 'test_annotation_valid.tsv',
            owner = self.regular_user_1,
            is_active=True,
            datafile = File(BytesIO(), 'test_annotation_valid.tsv')
        )
        #...and associate it with the real file
        associate_file_with_resource(r, resource_path)

        initiate_resource_validation(r, WILDCARD, UNSPECIFIED_FORMAT)
        rm = ResourceMetadata.objects.get(resource=r)
        self.assertTrue(rm.observation_set is None)
        initiate_resource_validation(r, 'ANN', TSV_FORMAT)
        rm = ResourceMetadata.objects.get(resource=r)
        self.assertFalse(rm.observation_set is None)
        

    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    @mock.patch('api.utilities.resource_utilities.retrieve_resource_class_instance')
    @mock.patch('api.utilities.resource_utilities.localize_resource')
    @mock.patch('api.utilities.resource_utilities.get_resource_size')
    def test_metadata_when_type_changed_case2(self, mock_get_resource_size, \
        mock_localize_resource, \
        mock_retrieve_resource_class_instance, \
        mock_check_file_format_against_type):

        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_matrix.tsv')
        mock_localize_resource.return_value = resource_path

        # define this mock function so we can patch the class
        # implementing the validation methods
        def mock_save_in_standardized_format(local_path, format):
            return resource_path

        patched_mtx_instance = Matrix()
        patched_mtx_instance.save_in_standardized_format = mock_save_in_standardized_format
        mock_retrieve_resource_class_instance.side_effect = [
            # note that we don't need to patch this since GeneralResource instances
            # do not perform validation
            GeneralResource(),
            patched_mtx_instance
        ]
        mock_localize_resource.return_value = resource_path
        mock_get_resource_size.return_value = 100
        
        r = Resource.objects.create(
            name = 'test_matrix',
            owner = self.regular_user_1,
            is_active=True,
            datafile = File(BytesIO(), 'test_matrix')
        )
        associate_file_with_resource(r, resource_path)

        initiate_resource_validation(r, WILDCARD, UNSPECIFIED_FORMAT)
        rm = ResourceMetadata.objects.get(resource=r)
        self.assertTrue(rm.observation_set is None)
        initiate_resource_validation(r, 'MTX', TSV_FORMAT)
        rm = ResourceMetadata.objects.get(resource=r)
        obs_set = rm.observation_set
        samples = [x['id'] for x in obs_set['elements']]
        expected = ['SW1_Control','SW2_Control','SW3_Control','SW4_Treated','SW5_Treated','SW6_Treated']
        self.assertCountEqual(samples, expected)
        

    def test_resource_metadata_entered_in_db(self):
        '''
        Here we test that an instance of ResourceMetadata is created
        and tied to the appropriate resource. Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        # create a Resource and give it our test integer matrix
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_integer_matrix.tsv')

        # note that we can't mock the class implementing the resource type, as
        # we need its implementation to get the metadata. HOWEVER, we need to ensure
        # that the file type above is ALREADY in the standardized format.
        file_format = TSV_FORMAT
        self.assertTrue(file_format == IntegerMatrix.STANDARD_FORMAT)

        resource_type = INTEGER_MATRIX_KEY
        file_format = TSV_FORMAT
        r = Resource.objects.create(
            datafile = File(BytesIO(), 'foo.tsv'),
            name = 'foo.tsv',
            resource_type = INTEGER_MATRIX_KEY,
            file_format = TSV_FORMAT,
            owner = get_user_model().objects.all()[0]
        )
        associate_file_with_resource(r, resource_path)

        # check the original count for ResourceMetadata
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertTrue(n0 == 0)

        # call the tested function
        resource_class_instance = retrieve_resource_class_instance(resource_type)
        handle_valid_resource(r, resource_class_instance)

        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)  
        self.assertTrue(n1 == 1)

        

    @mock.patch('api.utilities.resource_utilities.check_file_format_against_type')
    def test_resource_metadata_updated_in_db(self, mock_check_file_format_against_type):
        '''
        Here we test that an instance of ResourceMetadata is updated
        when it previously existed (for instance, upon update of a
        Resource type).

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''

        # get one of the test resources (which has type of None):
        rr = Resource.objects.filter(owner=self.regular_user_1, resource_type=None)
        r = rr[0]

        # give that Resource our test integer matrix
        fname = 'test_integer_matrix.tsv'
        resource_path = os.path.join(VALIDATION_TESTDIR, fname)
        resource_type = INTEGER_MATRIX_KEY
        file_format = TSV_FORMAT
        r = Resource.objects.create(
            datafile = File(BytesIO(), fname),
            name = fname,
            resource_type = resource_type,
            file_format = file_format,
            owner = get_user_model().objects.all()[0]
        )
        associate_file_with_resource(r, resource_path)

        # note that we can't mock the class implementing the resource type, as
        # we need its implementation to get the metadata. HOWEVER, we need to ensure
        # that the file type above is ALREADY in the standardized format.
        self.assertTrue(r.file_format == IntegerMatrix.STANDARD_FORMAT)

        # create a ResourceMetadata instance associated with that Resource
        ResourceMetadata.objects.create(
            resource=r,
            parent_operation = None,
            observation_set = None,
            feature_set = None
        )

        # check the original count for ResourceMetadata
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 1)
        rm_original = rm[0]

        # call the tested function
        resource_class_instance = retrieve_resource_class_instance(resource_type)
        handle_valid_resource(r, resource_class_instance)

        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)  

        # check that no new ResourceMetadata objects were created
        self.assertEqual(n1-n0, 0)
        rm_final = rm[0]
        
        # check that the observation_set changed as expected:
        self.assertFalse(rm_original.observation_set == rm_final.observation_set)

    def test_full_validation_success(self):
        '''
        Here we test that the full process to validate executes.

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        # get one of the test resources which is "unset" (no resource type or format)
        rr = Resource.objects.filter(
            owner=self.regular_user_1, 
            resource_type=None
        )
        r = rr[0]
        self.assertTrue((r.file_format == '') or (r.file_format is None))
        self.assertIsNone(r.resource_type)

        # provide a real test integer matrix
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_integer_matrix.tsv')
        associate_file_with_resource(r,resource_path)
        r.name = 'test_integer_matrix.tsv'
        r.save()

        file_format = TSV_FORMAT
        self.assertTrue(file_format == IntegerMatrix.STANDARD_FORMAT)

        # check the original count for ResourceMetadata. Should be nothing.
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 0)

        initiate_resource_validation(r, INTEGER_MATRIX_KEY, file_format)

        # Check that we now have metadata
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)  

        r = Resource.objects.get(pk=r.pk)
        self.assertTrue(r.resource_type == INTEGER_MATRIX_KEY)
        self.assertTrue(r.file_format == file_format)
        

    def test_full_validation_success_with_format_change(self):
        '''
        Here we test that the full process to validate executes. Here, the input
        file is a CSV and it correctly reads it as such. However, since we ultimately
        convert it to the standard format (TSV) then we need to ensure that the format
        attribute is correctly updated.

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        # get one of the test resources which is "unset" (no resource type or format)
        rr = Resource.objects.filter(
            owner=self.regular_user_1, 
            resource_type=None
        )
        r = rr[0]
        self.assertTrue((r.file_format == '') or (r.file_format is None))
        self.assertIsNone(r.resource_type)

        # provide a real test integer matrix
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_integer_matrix.csv')
        associate_file_with_resource(r,resource_path)
        r.name = 'test_integer_matrix.csv'
        r.save()

        orig_df = pd.read_csv(resource_path, index_col=0)

        file_format = CSV_FORMAT
        self.assertTrue(file_format != IntegerMatrix.STANDARD_FORMAT)

        # check the original count for ResourceMetadata. Should be nothing.
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 0)

        initiate_resource_validation(r, INTEGER_MATRIX_KEY, file_format)

        # Check that we now have metadata
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)  

        r = Resource.objects.get(pk=r.pk)
        self.assertTrue(r.resource_type == INTEGER_MATRIX_KEY)
        self.assertTrue(r.file_format == TSV_FORMAT)

        # check that the file contents are the same:
        final_df = pd.read_table(r.datafile.open(), index_col=0)
        eq_df = orig_df == final_df
        self.assertTrue(all(eq_df))

    @mock.patch('api.utilities.resource_utilities.get_resource_size')
    def test_full_validation_failure_case1(self, mock_get_resource_size):
        '''
        Here we test that the full process to validate executes. In this case
        we simulate a situation where the user specified the wrong resource type.
        The actual file contains floats, but the user attempts to validate as
        an integer matrix
        Note the file is actually parsed and set, just like the "success" test
        above. Then we intentionally break it.

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        rr = Resource.objects.filter(
            owner=self.regular_user_1, 
            resource_type=None
        )
        r = rr[0]
        self.assertTrue((r.file_format == '') or (r.file_format is None))
        self.assertIsNone(r.resource_type)

        # provide a real test matrix with floats.
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_matrix.tsv')
        associate_file_with_resource(r,resource_path)
        r.name = 'test_matrix.tsv'
        r.save()

        file_format = TSV_FORMAT
        self.assertTrue(file_format == IntegerMatrix.STANDARD_FORMAT)

        # check the original count for ResourceMetadata. Should be nothing.
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 0)

        mock_final_path = '/path/to/final/dir/file.tsv'
        mock_get_resource_size.return_value = 100

        # validate it. This should succeed since we are correctly validating
        # a float matrix
        initiate_resource_validation(r, MATRIX_KEY, file_format)

        # Check that we now have metadata
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)  
        self.assertEqual(n1,1)
        original_rm = rm[0]

        # check that all the fields are properly set:
        r = Resource.objects.get(pk=r.pk)
        self.assertTrue(r.resource_type == MATRIX_KEY)
        self.assertTrue(r.file_format == TSV_FORMAT)

        # Again we call the tested function. Note that we are trying to validate
        # a float matrix as an integer. It SHOULD fail.
        initiate_resource_validation(r, INTEGER_MATRIX_KEY, file_format)

        # Check that everything stayed the same:
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)
        final_rm = rm[0]
        self.assertEqual(original_rm.observation_set, final_rm.observation_set)

        r = Resource.objects.get(pk=r.pk)
        self.assertTrue(r.resource_type == MATRIX_KEY)
        self.assertTrue(r.file_format == TSV_FORMAT)
        self.assertTrue('Reverting' in r.status)

    def test_full_validation_failure_case2(self):
        '''
        Here we test that the full process to validate executes. In this case
        we simulate a situation where the user specified the wrong resource type.
        The actual file contains floats, but the user attempts to validate as
        an integer matrix
        Note the file was not previously validated

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        # get one of the test resources which is "unset" (no resource type or format)
        rr = Resource.objects.filter(
            owner=self.regular_user_1, 
            resource_type=None
        )
        r = rr[0]
        self.assertTrue((r.file_format == '') or (r.file_format is None))
        self.assertIsNone(r.resource_type)

        # provide a real test float matrix
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_matrix.tsv')
        associate_file_with_resource(r,resource_path)
        r.name = 'test_matrix.tsv'
        r.save()

        file_format = TSV_FORMAT

        # check the original count for ResourceMetadata. Should be nothing.
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 0)

        # call the tested function. Note that we are trying to validate
        # a float matrix as an integer. It SHOULD fail.
        initiate_resource_validation(r, INTEGER_MATRIX_KEY, file_format)

        # Check that we now have metadata, but it's trivial:
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)
        rm = rm[0]
        self.assertIsNone(rm.observation_set)
        self.assertIsNone(rm.feature_set)
        self.assertIsNone(rm.parent_operation)

        r = Resource.objects.get(pk=r.pk)
        self.assertIsNone(r.resource_type)
        self.assertTrue(
            (r.file_format == '')
            |
            (r.file_format is None)
        )        
        self.assertTrue('contained non-integer entries' in r.status)
        

    def test_success_after_failure(self):
        '''
        Here we test that the full process to validate executes. In this case
        we simulate a situation where the user specified the wrong resource type.
        The actual file contains floats, but the user attempts to validate as
        an integer matrix. The user then initiates a proper request. Check
        that everything is filled out properly

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        # get one of the test resources which is "unset" (no resource type or format)
        rr = Resource.objects.filter(
            owner=self.regular_user_1, 
            resource_type=None
        )
        r = rr[0]
        self.assertTrue((r.file_format == '') or (r.file_format is None))
        self.assertIsNone(r.resource_type)

        # provide a real test float matrix
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_matrix.tsv')
        associate_file_with_resource(r,resource_path)
        # r.name = 'test_matrix.tsv'
        # r.save()

        file_format = TSV_FORMAT

        # check the original count for ResourceMetadata. Should be nothing.
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 0)

        # call the tested function. Note that we are trying to validate
        # a float matrix as an integer. It SHOULD fail.
        initiate_resource_validation(r, INTEGER_MATRIX_KEY, file_format)

        # Check that we now have metadata, but it's trivial:
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)
        rm = rm[0]
        self.assertIsNone(rm.observation_set)
        self.assertIsNone(rm.feature_set)
        self.assertIsNone(rm.parent_operation)

        r = Resource.objects.get(pk=r.pk)
        self.assertIsNone(r.resource_type)
        self.assertTrue(
            (r.file_format == '')
            |
            (r.file_format is None)
        )
        self.assertTrue('contained non-integer entries' in r.status)

        # Now validate as a float matrix:
        initiate_resource_validation(r, MATRIX_KEY, file_format)

        # Check that the metadata updated everything stayed the same:
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)
        final_rm = rm[0]
        self.assertTrue(len(final_rm.observation_set) > 0)

        r = Resource.objects.get(pk=r.pk)
        self.assertTrue(r.resource_type == MATRIX_KEY)
        self.assertTrue(r.file_format == TSV_FORMAT)

        

    @mock.patch('api.utilities.resource_utilities.retrieve_metadata')
    def test_full_metadata_failure(self, mock_retrieve_metadata):
        '''
        Here we test that the full process to validate executes. In this test, we set 
        it up such that the file validates properly, but there is an issue when 
        extracting metadata. There are some edge cases where a file is technically 
        compliant with the type+format, but breaks when we attempt to create
        metadata

        Not a "true" unit test in the sense
        that it doesn't mock out intermediate functions, etc. It uses real data.
        '''
        # get one of the test resources which is "unset" (no resource type or format)
        rr = Resource.objects.filter(
            owner=self.regular_user_1, 
            resource_type=None
        )
        r = rr[0]
        self.assertTrue((r.file_format == '') or (r.file_format is None))
        self.assertIsNone(r.resource_type)

        # provide a real test integer matrix
        resource_path = os.path.join(VALIDATION_TESTDIR, 'test_integer_matrix.tsv')
        associate_file_with_resource(r, resource_path)
        r.name = 'test_integer_matrix.tsv'
        r.save()

        file_format = TSV_FORMAT
        self.assertTrue(file_format == IntegerMatrix.STANDARD_FORMAT)

        # check the original count for ResourceMetadata. Should be nothing.
        rm = ResourceMetadata.objects.filter(resource=r)
        n0 = len(rm)
        self.assertEqual(n0, 0)

        mock_retrieve_metadata.side_effect = Exception('something bad!')

        with self.assertRaises(Exception):
            initiate_resource_validation(r, INTEGER_MATRIX_KEY, file_format)

        # Check that we still don't have metadata
        rm = ResourceMetadata.objects.filter(resource=r)
        n1 = len(rm)  
        self.assertEqual(n0, 0)

        r = Resource.objects.get(pk=r.pk)
        self.assertIsNone(r.resource_type)
        self.assertTrue(
            (r.file_format == '')
            |
            (r.file_format is None)
        )