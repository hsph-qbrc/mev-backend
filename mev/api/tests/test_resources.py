from io import BytesIO
import uuid
import os
import json
import unittest.mock as mock

from django.urls import reverse
from django.core.exceptions import ImproperlyConfigured
from rest_framework import status
from django.conf import settings
from django.core.files import File

from api.models import Resource, Workspace
from constants import DATABASE_RESOURCE_TYPES, \
    FASTQ_KEY, \
    FEATURE_TABLE_KEY, \
    INTEGER_MATRIX_KEY, \
    JSON_FILE_KEY, \
    MATRIX_KEY, \
    TSV_FORMAT, \
    CSV_FORMAT, \
    JSON_FORMAT, \
    NARROWPEAK_FILE_KEY, \
    NEGATIVE_INF_MARKER, \
    POSITIVE_INF_MARKER
from resource_types.table_types import NarrowPeakFile

from api.tests.base import BaseAPITestCase
from api.tests import test_settings
from api.tests.test_helpers import associate_file_with_resource

class ResourceListTests(BaseAPITestCase):

    def setUp(self):

        self.url = reverse('resource-list')
        self.establish_clients()

    def test_list_resource_requires_auth(self):
        """
        Test that general requests to the endpoint generate 401
        """
        response = self.regular_client.get(self.url)
        self.assertTrue((response.status_code == status.HTTP_401_UNAUTHORIZED) 
        | (response.status_code == status.HTTP_403_FORBIDDEN))

    def test_admin_can_list_resource(self):
        """
        Test that admins can only see Resources they own.  They can't list
        other people's resources, despite admin privileges.
        """
        response = self.authenticated_admin_client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        all_admin_resource_uuids = set([str(x.pk) for x in Resource.objects.filter(owner=self.admin_user)])
        received_resource_uuids = set([x['id'] for x in response.data])
        self.assertEqual(all_admin_resource_uuids, received_resource_uuids)

    def test_admin_cannot_create_resource(self):
        """
        Test that even admins can't create a Resource via the api
        """
        # get all initial instances before anything happens:
        initial_resource_uuids = set([str(x.pk) for x in Resource.objects.all()])

        payload = {
            'owner_email': self.regular_user_1.email,
            'name': 'some_file.txt',
            'resource_type':'MTX'
        }
        response = self.authenticated_admin_client.post(self.url, data=payload, format='json')
        self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

        # get current instances:
        current_resource_uuids = set([str(x.pk) for x in Resource.objects.all()])
        difference_set = current_resource_uuids.difference(initial_resource_uuids)
        self.assertEqual(len(difference_set), 0)


    def test_regular_user_post_raises_error(self):
        """
        Test that regular users cannot post to this endpoint (i.e. to
        create a Resource).  All Resource creation should be handled by
        the upload methods or be initiated by an admin.
        """
        payload = {'owner_email': self.regular_user_1.email,
            'name': 'some_file.txt',
            'resource_type': 'MTX'
        }
        response = self.authenticated_regular_client.post(self.url, data=payload, format='json')
        self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)


    def test_users_can_list_resource(self):
        """
        Test that regular users can list ONLY their own resources
        """
        response = self.authenticated_regular_client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        all_known_resource_uuids = set([str(x.pk) for x in Resource.objects.all()])
        personal_resources = Resource.objects.filter(owner=self.regular_user_1)
        personal_resource_uuids = set([str(x.pk) for x in personal_resources])
        received_resource_uuids = set([x['id'] for x in response.data])

        # checks that the test below is not trivial.  i.e. there are Resources owned by OTHER users
        self.assertTrue(len(all_known_resource_uuids.difference(personal_resource_uuids)) > 0)

        # checks that they only get their own resources (by checking UUID)
        self.assertEqual(personal_resource_uuids, received_resource_uuids)


class ResourceContentTests(BaseAPITestCase):
    '''
    Tests the endpoint which returns the file contents in full
    '''
    def setUp(self):

        self.establish_clients()
        self.TESTDIR = os.path.join(
            os.path.dirname(__file__),
            'resource_contents_test_files'    
        )
        # get an example from the database:
        regular_user_resources = Resource.objects.filter(
            owner=self.regular_user_1,
        )
        if len(regular_user_resources) == 0:
            msg = '''
                Testing not setup correctly.  Please ensure that there is at least one
                Resource instance for the user {user}
            '''.format(user=self.regular_user_1)
            raise ImproperlyConfigured(msg)
        for r in regular_user_resources:
            if r.is_active:
                active_resource = r
                break
        self.resource = active_resource
        self.url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        for r in regular_user_resources:
            if not r.is_active:
                inactive_resource = r
                break
        self.inactive_resource_url = reverse(
            'resource-contents', 
            kwargs={'pk':inactive_resource.pk}
        )

    def test_page_param_ignored_for_non_paginated_resource(self):
        '''
        Certain resource types (e.g. JSON) don't have straightforward
        pagination schemes. If the JSON was a list, fine...but generally
        that's not the case.

        Check that any page params supplied are ignored and the entire
        resource is returned
        '''
        f = os.path.join(self.TESTDIR, 'json_file.json')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = JSON_FILE_KEY
        self.resource.file_format = JSON_FORMAT
        self.resource.save()

        # check that full file works without query params
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        file_contents = json.load(open(f))
        self.assertDictEqual(results, file_contents)

        # add the query params onto the end of the url.
        # See that it still works (i.e. query params are ignored)
        url = base_url + '?page=1'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        j = response.json()
        self.assertDictEqual(j, file_contents)

        url = base_url + '?page=2'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertTrue(response.status_code == status.HTTP_404_NOT_FOUND)


    def test_response_with_na_and_inf(self):
        '''
        Tests the case where the requested resource has infinities and NA's
        '''
        f = os.path.join(self.TESTDIR, 'demo_file1.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()

        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        j = response.json()

        # the second row (index=1) has a negative infinity.
        self.assertTrue(j[1]['values']['log2FoldChange'] == NEGATIVE_INF_MARKER)

        # the third row (index=2) has a positive infinity.
        self.assertTrue(j[2]['values']['log2FoldChange'] == POSITIVE_INF_MARKER)
        
        # the third row has a padj of NaN, which gets converted to None 
        self.assertIsNone(j[2]['values']['padj'])

    def test_response_with_na_and_large_number(self):
        '''
        Tests the case where the requested resource has np.nan AND a large
        number.

        Note that this arose from the case where a user had a file with VERY
        large values (e+280) and ALSO had some NaN values. With the combination
        of both of those in a matrix, the pandas.table.mask method, we were raising 
        OverflowErrors: Python int too large to convert to C long


        '''
        #
        f = os.path.join(self.TESTDIR, 'file_with_large_value.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()

        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        j = response.json()


    def test_content_request_from_non_owner(self):
        '''
        Tests where content is requested from someone else's
        resource
        '''
        response = self.authenticated_other_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_403_FORBIDDEN)

    def test_content_request_for_inactive_fails(self):
        '''
        Tests where content is requested for a resource
        that is inactive.
        '''
        response = self.authenticated_regular_client.get(
            self.inactive_resource_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)


    @mock.patch('api.views.resource_views.check_resource_request')
    @mock.patch('api.views.resource_views.get_resource_type_instance')
    def test_error_reported(self,
            mock_get_resource_type_instance,
            mock_check_resource_request):
        '''
        If there was some error in preparing the preview, 
        the returned data will have an 'error' key
        '''
        mock_check_resource_request.return_value = (True, self.resource)
        mock_resource_type = mock.MagicMock()
        mock_resource_type.get_contents.side_effect = Exception('something bad happened!')
        mock_get_resource_type_instance.return_value = mock_resource_type
        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_500_INTERNAL_SERVER_ERROR)   

        self.assertTrue('error' in response.json())     

    @mock.patch('api.views.resource_views.check_resource_request')
    @mock.patch('api.utilities.resource_utilities.localize_resource')
    def test_contents_for_resource_without_view(self, mock_localize_resource, mock_check_resource_request):
        '''
        Test that resources which don't allow content access (e.g. large FASTQ files)
        return a 200 and state that contents are not available for the resource
        '''
        # the actual file doesn't matter so we don't have to associate
        self.resource.resource_type = FASTQ_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue('info' in j)

    @mock.patch('api.views.resource_views.check_resource_request')
    @mock.patch('api.utilities.resource_utilities.localize_resource')
    def test_expected_response(self, mock_localize_resource, mock_check_resource_request):
        '''
        Test that the returned payload matches our expectations
        '''
        f = os.path.join(self.TESTDIR, 'demo_file2.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = INTEGER_MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)
        mock_localize_resource.return_value = f

        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()

        expected_return = [
            {
                "rowname": "gA",
                "values": {
                "colA": 0,
                "colB": 1,
                "colC": 2
                }
            },
            {
                "rowname": "gB",
                "values": {
                "colA": 10,
                "colB": 11,
                "colC": 12
                }
            },
            {
                "rowname": "gC",
                "values": {
                "colA": 20,
                "colB": 21,
                "colC": 22
                }
            }
        ]
        self.assertEqual(expected_return, j) 

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_sort_for_json(self, mock_check_resource_request):
        '''
        For certain types of json data structures, we can perform filtering. 
        For instance, if we have an array of simple items where our filter key is at the "top level",
        we can perform a filter based on that.
        '''
        f = os.path.join(self.TESTDIR, 'json_array_file_test_filter.json')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = JSON_FILE_KEY
        self.resource.file_format = JSON_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 13)

        # sort without a filter:
        url = base_url + '?page=1&page_size=20&sort_vals=[asc]:pval'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 13)
        self.assertTrue(results[-1]['name'] == 'MM')
        self.assertFalse('pval' in results[-1])
        # remove the last item since it doesn't have pval field
        returned_pvals = [x['pval'] for x in results[:-1]]
        self.assertEqual(sorted(returned_pvals), returned_pvals)

        # sort AND filter:
        filter_val = 0.06
        url = base_url + '?page=1&page_size=10&pval=[lt]:{v}&sort_vals=[asc]:pval'.format(v=filter_val)
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 9)
        returned_pvals = [x['pval'] for x in results]
        self.assertEqual(sorted(returned_pvals), returned_pvals)

        # check descending sort
        filter_val = 0.06
        url = base_url + '?page=1&page_size=10&pval=[lt]:{v}&sort_vals=[desc]:pval'.format(v=filter_val)
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        # note the -1 to sort descending
        returned_pvals = [-1*x['pval'] for x in results]
        self.assertEqual(sorted(returned_pvals), returned_pvals)

        # try sorting on multiple fields and check that it fails.
        url = base_url + '?page=1&page_size=20&sort_vals=[asc]:pval,[asc]:name'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        self.assertTrue('error' in response.json())

        # try sorting on a nonexistent field
        url = base_url + '?page=1&page_size=20&sort_vals=[asc]:xxx'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 13)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_filter_for_json(self,  mock_check_resource_request):
        '''
        For certain types of json data structures, we can perform filtering. 
        For instance, if we have an array of simple items where our filter key is at the "top level",
        we can perform a filter based on that.
        '''
        f = os.path.join(self.TESTDIR, 'json_array_file_test_filter.json')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = JSON_FILE_KEY
        self.resource.file_format = JSON_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 13)

        # add the query params onto the end of the url:
        url = base_url + '?page=1&page_size=10'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 10)

        # add the query params onto the end of the url:
        url = base_url + '?page=2&page_size=10'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 3)

        #add the query params onto the end of the url:
        filter_val = 0.06
        url = base_url + '?page=1&page_size=10&pval=[lt]:{v}'.format(v=filter_val)
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 9)
        for x in results:
            self.assertTrue(x['pval'] < filter_val)

        # query multiple fields
        filter_val = 0.06
        url = base_url + '?page=1&page_size=10&pval=[lt]:{v}&name=BB'.format(v=filter_val)
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 1)

        # query a field that only exists on one of the items.
        url = base_url + '?page=1&page_size=10&other=X'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 1)

        # query a field that only exists on one of the items, but query doesn't match
        url = base_url + '?page=1&page_size=10&other=Y'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 0)

        # attempt to query on a field that doesn't exist on any item:
        url = base_url + '?page=1&page_size=10&xxx=[lt]:0.1'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 0)

        # provide a bad query string and check that it returns 400
        # (missing the brackets on the query param)
        url = base_url + '?page=1&page_size=10&pval=lt:0.1'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        j = response.json()
        self.assertTrue('error' in j)

        # provide a bad query string and check that it returns 400
        # (the value to compare to can't be cast as a float)
        url = base_url + '?page=1&page_size=10&pval=[lt]:a'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        j = response.json()
        self.assertTrue('error' in j)


    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_filter_for_json_with_na(self, mock_check_resource_request):
        '''
        For certain types of json data structures, we can perform filtering. 
        For instance, if we have an array of simple items where our filter key is at the "top level",
        we can perform a filter based on that.

        Here, we test a numeric comparison for a field which can contain non-numerics. This can happen, 
        for instance, if a p-value field is assigned a "NA" value.
        '''
        f = os.path.join(self.TESTDIR, 'json_array_file_with_na.json')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = JSON_FILE_KEY
        self.resource.file_format = JSON_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 7)

        # add the query params onto the end of the url:
        url = base_url + '?pval=[lte]:0.005'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 1)

        # add the query params onto the end of the url:
        url = base_url + '?name=[startswith]:aaa'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        j = response.json()
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        self.assertTrue(len(j) == 3)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_pagination_for_json(self, mock_check_resource_request):
        f = os.path.join(self.TESTDIR, 'json_array_file.json')
        N = 60 # the number of records in our demo file

        # just a double-check to ensure the test data is large enough
        # for the pagination to be general
        self.assertTrue(N > settings.REST_FRAMEWORK['PAGE_SIZE'])
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = 'JSON'
        self.resource.file_format = JSON_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        # add the query params onto the end of the url:
        url = base_url + '?page=1'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == settings.REST_FRAMEWORK['PAGE_SIZE'])

        # add the query params onto the end of the url:
        url = base_url + '?page=last'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        leftover_size = N % settings.REST_FRAMEWORK['PAGE_SIZE']
        self.assertTrue(len(results) == leftover_size)

        # test the page_size param:
        page_size = 10
        suffix = '?page_size=%d&page=2' % page_size
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == page_size)
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['idx'] == 10)
        self.assertTrue(final_record['idx'] == 19)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_pagination(self, mock_check_resource_request):
        f = os.path.join(self.TESTDIR, 'demo_table_for_pagination.tsv')
        N = 155 # the number of records in our demo file

        # just a double-check to ensure the test data is large enough
        # for the pagination to be general
        self.assertTrue(N > settings.REST_FRAMEWORK['PAGE_SIZE'])
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g0')
        self.assertTrue(final_record['rowname'] == 'g154')

        # add the query params onto the end of the url:
        url = base_url + '?page=1'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == settings.REST_FRAMEWORK['PAGE_SIZE'])
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g0')
        self.assertTrue(final_record['rowname'] == 'g49')

        # add the query params onto the end of the url:
        url = base_url + '?page=2'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == settings.REST_FRAMEWORK['PAGE_SIZE'])
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g50')
        self.assertTrue(final_record['rowname'] == 'g99')

        # add the query params onto the end of the url:
        url = base_url + '?page=last'
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        leftover_size = N % settings.REST_FRAMEWORK['PAGE_SIZE']
        self.assertTrue(len(results) == leftover_size)
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g150')
        self.assertTrue(final_record['rowname'] == 'g154')

        # by itself the page_size param doesn't do anything.
        # It needs the page param
        page_size = 20
        suffix = '?page_size=%d' % page_size
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g0')
        self.assertTrue(final_record['rowname'] == 'g154')

        # test the page_size param:
        page_size = 20
        suffix = '?page_size=%d&page=2' % page_size
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == page_size)
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g20')
        self.assertTrue(final_record['rowname'] == 'g39')

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_rowname_filter(self, mock_check_resource_request):
        '''
        We allow filtering of tables by the rownames (commonly gene name). Test that
        the implementation works as expected
        '''
        f = os.path.join(self.TESTDIR, 'demo_table_for_pagination.tsv')
        N = 155 # the number of records in our demo file

        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)
        first_record = results[0]
        final_record = results[-1]
        self.assertTrue(first_record['rowname'] == 'g0')
        self.assertTrue(final_record['rowname'] == 'g154')

        # the "genes" are named like g0, g1, ...
        # try some row name filters:
        # the "startswith" G1 filter should match g1, g10-g19, g100-g154
        suffix = '?__rowname__=[startswith]:G1'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 66)

        # doesn't match anything
        suffix = '?__rowname__=[startswith]:X'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 0)

        # doesn't match anything
        suffix = '?__rowname__=[eq]:X&__rowmean__=[gte]:0&page=1&page_size=10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j['results']) == 0)
        self.assertTrue(j['count'] == 0)

        # the G1 case-insensitive filter should match only g1
        suffix = '?__rowname__=[case-ins-eq]:G1'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 1)

        # the G1 case-sensitive filter doesn't match anything
        suffix = '?__rowname__=[eq]:G1'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 0)

        # the G1 case-sensitive filter doesn't match anything
        suffix = '?__rowname__=[eq]:g1'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 1)
        self.assertTrue(j[0]['rowname'] == 'g1')

        # add on some pagination queries
        # the "startswith" G1 filter should 66 entries in total
        pg_size = 10
        suffix = '?page=1&page_size={n}&__rowname__=[startswith]:G1'.format(n=pg_size)
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 10)
        idx = [1, *list(range(10,19))]
        expected_rows = ['g%d' % x for x in idx]
        self.assertCountEqual(expected_rows, [x['rowname'] for x in results]) 

        # get page 2 on the size 66 query. Should be g19, g100, ..., g108
        pg_size = 10
        suffix = '?page=2&page_size={n}&__rowname__=[startswith]:G1'.format(n=pg_size)
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 10)
        idx = [19, *list(range(100,109))]
        expected_rows = ['g%d' % x for x in idx]
        self.assertCountEqual(expected_rows, [x['rowname'] for x in results]) 

        # check the "in" query. We use this for selecting a subset of a matrix for genes of interest
        # such as when getting the data for a specific FeatureSet
        selected_genes = ['g8', 'g68', 'g102']
        suffix = '?__rowname__=[in]:{s}'.format(s=','.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 3)
        returned_genes = [x['rowname'] for x in j]
        self.assertCountEqual(returned_genes, selected_genes)

        # check that duplicate gene requests are 'ignored'
        selected_genes = ['g8', 'g68', 'g102', 'g8'] # g8 shows up twice
        suffix = '?__rowname__=[in]:{s}'.format(s=','.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 3)
        returned_genes = [x['rowname'] for x in j]
        self.assertCountEqual(returned_genes, list(set(selected_genes)))

        # check the "in" query. We use this for selecting a subset of a matrix for genes of interest
        # such as when getting the data for a specific FeatureSet
        selected_genes = ['g8', 'g68', 'gXYZ'] # last one not there, so ignored
        suffix = '?__rowname__=[in]:{s}'.format(s=','.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 2)
        returned_genes = [x['rowname'] for x in j]
        self.assertCountEqual(returned_genes, selected_genes[:2])

        # check the "in" query. We use this for selecting a subset of a matrix for genes of interest
        # such as when getting the data for a specific FeatureSet
        selected_genes = ['g%d' % x for x in range(10,50)]
        suffix = '?__rowname__=[in]:{s}&page=1&page_size=10'.format(s=','.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 10)
        returned_genes = [x['rowname'] for x in results]
        expected_genes = selected_genes[:10]
        self.assertCountEqual(returned_genes, expected_genes)

        selected_genes = ['gABC', 'gXYZ'] # last one not there
        suffix = '?__rowname__=[in]:{s}'.format(s=','.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 0)

        # mess up the formatting by not using a comma. In fact, the semicolon
        # causes the url params to get split so that it tries to filter on a 
        # column named "g68" which does not exist.
        selected_genes = ['g8', 'g68']
        suffix = '?__rowname__=[in]:{s}'.format(s=';'.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 0)

        # space-delimited doesn't work- should return nothing.
        selected_genes = ['g8', 'g68']
        suffix = '?__rowname__=[in]:{s}'.format(s=' '.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 0)

        # a comma with a space is ok
        selected_genes = ['g8', 'g68']
        suffix = '?__rowname__=[in]:{s}'.format(s=', '.join(selected_genes))
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        self.assertTrue(len(j) == 2)
        returned_genes = [x['rowname'] for x in j]
        self.assertCountEqual(returned_genes, selected_genes[:2])

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_table_filter(self, mock_check_resource_request):
        '''
        For testing if table-based resources are filtered correctly
        '''
        f = os.path.join(self.TESTDIR, 'demo_deseq_table.tsv')
        N = 39 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)
        
        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        suffix = '?pvalue=[lte]:0.4'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 2)
        returned_set = set([x['rowname'] for x in results])
        self.assertEqual({'HNRNPUL2', 'MAP1A'}, returned_set)

        # an empty result set
        suffix = '?pvalue=[lte]:0.004'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 0)

        suffix = '?pvalue=[lte]:0.4&log2FoldChange=[gt]:0'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 1)
        returned_set = set([x['rowname'] for x in results])
        self.assertEqual({'HNRNPUL2'}, returned_set)

        # note the missing delimiter, which makes the suffix invalid. Should return 400
        suffix = '?pvalue=[lte]:0.4&log2FoldChange=[gt]0'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        # note the value ("a") can't be parsed as a number
        suffix = '?pvalue=[lte]:a'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        # filter on a column that does not exist
        suffix = '?xyz=[lte]:0'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)
        expected_error = (
            'There was a problem when parsing the request:'
            ' The column "xyz" is not available for filtering.'
        )
        self.assertEqual(results['error'], expected_error)

       # filter on a column that does not exist, but also including a valid field
        suffix = '?pvalue=[lte]:0.1&abc=[lte]:0'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)
        expected_error = (
            'There was a problem when parsing the request:'
            ' The column "abc" is not available for filtering.'
        )
        self.assertEqual(results['error'], expected_error)


    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_abs_val_table_filter(self, mock_check_resource_request):
        '''
        For testing if table-based resources are filtered correctly using the absolute value filters
        '''
        f = os.path.join(self.TESTDIR, 'demo_deseq_table.tsv')
        N = 39 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        suffix = '?log2FoldChange=[absgt]:2.0'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 5)
        returned_set = set([x['rowname'] for x in results])
        self.assertEqual({'KRT18P27', 'PWWP2AP1', 'AMBP', 'ADH5P2', 'MMGT1'}, returned_set)

        suffix = '?log2FoldChange=[absgt]:2.0&pvalue=[lt]:0.5'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 1)
        returned_set = set([x['rowname'] for x in results])
        self.assertEqual({'AMBP'}, returned_set)


    @mock.patch('api.views.resource_views.check_resource_request')
    def test_table_filter_with_string(self, mock_check_resource_request):
        '''
        For testing if table-based resources are filtered correctly on string fields
        '''
        f = os.path.join(self.TESTDIR, 'table_with_string_field.tsv')
        N = 3 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        suffix = '?colB=abc'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 2)
        returned_set = set([x['rowname'] for x in results])
        self.assertEqual({'A', 'C'}, returned_set)

        suffix = '?colB=abc&colA=[lt]:0.02'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 1)
        returned_set = set([x['rowname'] for x in results])
        self.assertEqual({'A'}, returned_set)

        # filter which gives zero results
        suffix = '?colB=aaa'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 0)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_sort(self, mock_check_resource_request):
        '''
        For testing if table-based resources are sorted correctly
        '''
        f = os.path.join(self.TESTDIR, 'demo_deseq_table.tsv')
        N = 39 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        suffix = '?{s}={a}:padj,{d}:log2FoldChange'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING,
            d = settings.DESCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        gene_ordering = [x['rowname'] for x in results]
        expected_ordering = [
            'AC011841.4', 
            'UNCX', 
            'KRT18P27', 
            'ECHS1', 
            'ADH5P2', 
            'OR2L8', 
            'RN7SL99P', 
            'KRT18P19', 
            'CTD-2532D12.5', 
            'HNRNPUL2', 
            'TFAMP1', 
            'MAP1A', 
            'AC000123.4', 
            'HTR7P1', 
            'PWWP2AP1', 
            'AMBP', 
            'MMGT1'
        ]
        # only compare the first few. After that, the sorting is arbitrary as there
        # are na's, etc.
        self.assertEqual(
            gene_ordering[:len(expected_ordering)], 
            expected_ordering
        )

        # flip the order of the log2FoldChange:
        suffix = '?{s}={a}:padj,{a}:log2FoldChange'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        gene_ordering = [x['rowname'] for x in results]
        # the first few are unambiguous, as the padj are different
        # the later ones have the same padj, but different log2FoldChange
        # We include some +/-inf values also.
        expected_ordering = [
            'UNCX', 
            'AC011841.4', 
            'KRT18P27', 
            'ECHS1', 
            'MMGT1',
            'AMBP',
            'PWWP2AP1',
            'HTR7P1',
            'AC000123.4',
            'MAP1A',
            'TFAMP1',
            'HNRNPUL2',
            'CTD-2532D12.5',
            'KRT18P19',
            'RN7SL99P',
            'OR2L8',
            'ADH5P2'
        ]
        # only compare the first few. After that, the sorting is arbitrary as there
        # are na's, etc.
        self.assertEqual(
            gene_ordering[:len(expected_ordering)], 
            expected_ordering
        )

        # add on pagination params to check that things work as expected
        page_size = 10
        suffix = '?page=1&page_size={n}&{s}={a}:padj,{a}:log2FoldChange'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING,
            n = page_size
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        j = response.json()
        results = j['results']
        self.assertTrue(len(results) == 10)
        gene_ordering = [x['rowname'] for x in results]
        # the first few are unambiguous, as the padj are different
        # the later ones have the same padj, but different log2FoldChange
        # We include some +/-inf values also.
        expected_ordering = [
            'UNCX', 
            'AC011841.4', 
            'KRT18P27', 
            'ECHS1', 
            'MMGT1',
            'AMBP',
            'PWWP2AP1',
            'HTR7P1',
            'AC000123.4',
            'MAP1A'
        ]
        # only compare the first few. After that, the sorting is arbitrary as there
        # are na's, etc.
        self.assertEqual(
            gene_ordering, 
            expected_ordering
        )

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_matrix_specific_content_requests(self, mock_check_resource_request):
        '''
        For testing if table-based resources are sorted correctly when used
        with filters.
        '''
        f = os.path.join(self.TESTDIR, 'rowmeans_test_file.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 12)
        self.assertTrue(type(results) is list)
        self.assertFalse(any(['__rowmean__' in x for x in results]))
        
        # add on a rowmeans query without any 'value'. This should be valid.
        suffix = '?__incl_rowmeans__'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(all(['__rowmean__' in x for x in results]))

        # in addition to the rowmeans query, request a sort
        suffix = '?__incl_rowmeans__&sort_vals=[desc]:__rowmean__'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(all(['__rowmean__' in x for x in results]))
        expected_gene_ordering = ['g%d' % x for x in range(12,0,-1)]
        returned_order = [x['rowname'] for x in results]
        self.assertEqual(expected_gene_ordering, returned_order)

        # need to request the rowmean if a sort is requested. Otherwise the request
        # is a bit ambiguous
        suffix = '?sort_vals=[desc]:__rowmean__'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)

        # add on a rowmeans query with explicit true value.
        suffix = '?__incl_rowmeans__=true'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(all(['__rowmean__' in x for x in results]))

        # mis-spell the parameter-- should be '__incl_rowmeans__' (with the underscores)
        # This should fail
        suffix = '?incl_rowmeans'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        # add on a rowmeans query to limit the results
        suffix = '?__rowmean__=[lt]:20'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        expected_genes = ['g1', 'g2']
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # add on a rowmeans query to limit the results
        suffix = '?__rowmean__=[eq]:20'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 0)

        # attempt a rowmeans query to filter, but leave off the value
        suffix = '?__rowmean__=[lt]'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        # attempt a rowmeans query to filter, but give a non-float value
        suffix = '?__rowmean__=[lt]:a'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        suffix = '?__rowmean__=[gt]:20'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        expected_genes = ['g%d' % i for i in range(3,13)]
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # check that it works with pagination
        # would normally return 11 records with pagination
        suffix = '?page=1&page_size=10&__rowmean__=[gt]:10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        payload = response.json()
        # if the response is paginated, then it would have these keywords:
        paginated_keywords = [
            'previous',
            'next',
            'count',
            'results'
        ]
        for kw in paginated_keywords:
            self.assertTrue(kw in payload.keys())

        # now get the actual results from that payload
        results = payload['results']
        self.assertTrue(len(results) == 10)
        expected_genes = ['g%d' % i for i in range(2,12)]
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # check that it works with pagination
        # would normally return 11 records with pagination
        suffix = '?page=2&page_size=10&__rowmean__=[gt]:10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()['results']
        self.assertTrue(len(results) == 1)
        expected_gene = 'g12'
        returned_gene = results[0]['rowname']
        self.assertEqual(expected_gene, returned_gene)

        # check a malformed request-- there is no '&' delimiter
        # the paginator ignores it and just returns everything
        suffix = '?page=1&page_size=10__rowmean__=[gt]:10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()['results']
        self.assertTrue(len(results) == 12)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_matrix_specific_content_requests_with_na_and_infty(self, mock_check_resource_request):
        '''
        For testing if table-based resources are sorted correctly when used
        with filters.
        '''
        f = os.path.join(self.TESTDIR, 'rowmeans_test_file_with_na.tsv')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = MATRIX_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 12)
        self.assertFalse(any(['__rowmean__' in x for x in results]))
        
        # add on a rowmeans query without any 'value'. This should be valid.
        suffix = '?__incl_rowmeans__'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(all(['__rowmean__' in x for x in results]))

        # in addition to the rowmeans query, request a sort
        suffix = '?__incl_rowmeans__&sort_vals=[desc]:__rowmean__'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(all(['__rowmean__' in x for x in results]))
        expected_gene_ordering = ['g%d' % x for x in [1, *range(12,1,-1)]]
        returned_order = [x['rowname'] for x in results]
        self.assertEqual(expected_gene_ordering, returned_order)

        # add on a rowmeans query with explicit true value.
        suffix = '?__incl_rowmeans__=true'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(all(['__rowmean__' in x for x in results]))

        # mis-spell the parameter-- should be '__incl_rowmeans__' (with the underscores)
        # This should fail
        suffix = '?incl_rowmeans'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        # add on a rowmeans query to limit the results
        suffix = '?__rowmean__=[lt]:20'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        expected_genes = ['g2'] # in this test, 'g1' has an infinity
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # add on a rowmeans query to limit the results
        suffix = '?__rowmean__=[eq]:20'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 0)

        # attempt a rowmeans query to filter, but leave off the value
        suffix = '?__rowmean__=[lt]'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        # attempt a rowmeans query to filter, but give a non-float value
        suffix = '?__rowmean__=[lt]:a'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)

        suffix = '?__rowmean__=[gt]:20'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        expected_genes = ['g%d' % i for i in [1, *range(3,13)]]
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # check that it works with pagination
        # would normally return 11 records with pagination
        suffix = '?page=1&page_size=10&__rowmean__=[gt]:10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()['results']
        self.assertTrue(len(results) == 10)
        expected_genes = ['g%d' % i for i in range(1,11)]
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # check that it works with pagination
        # would normally return 11 records with pagination
        suffix = '?page=2&page_size=10&__rowmean__=[gt]:10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()['results']
        self.assertTrue(len(results) == 2)
        expected_genes = ['g11','g12']
        returned_genes = [x['rowname'] for x in results]
        self.assertEqual(expected_genes, returned_genes)

        # check a malformed request-- there is no '&' delimiter
        # the paginator ignores it and just returns everything
        suffix = '?page=1&page_size=10__rowmean__=[gt]:10'
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()['results']
        self.assertTrue(len(results) == 12)


    @mock.patch('api.views.resource_views.check_resource_request')
    def test_resource_contents_sort_and_filter(self, mock_check_resource_request):
        '''
        For testing if table-based resources are sorted correctly when used
        with filters.
        '''
        f = os.path.join(self.TESTDIR, 'demo_deseq_table.tsv')
        N = 39 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        suffix = '?padj=[lt]:0.3&{s}={a}:padj,{d}:log2FoldChange'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING,
            d = settings.DESCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == 2)
        gene_ordering = [x['rowname'] for x in results]
        expected_ordering = [
            'AC011841.4', 
            'UNCX'
        ] 
        # only compare the first few. After that, the sorting is arbitrary as there
        # are na's, etc.
        self.assertEqual(
            gene_ordering, 
            expected_ordering
        )

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_malformatted_sort_and_filter(self, mock_check_resource_request):
        '''
        For testing that bad request params are handled well.
        '''
        f = os.path.join(self.TESTDIR, 'demo_deseq_table.tsv')
        N = 39 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)


        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        bad_sort_kw = 'sort'
        self.assertFalse(bad_sort_kw == settings.SORT_PARAM) # to ensure that it's indeed a "bad" param
        suffix = '?{s}={a}:padj,{d}:log2FoldChange'.format(
            s = bad_sort_kw,
            a = settings.ASCENDING,
            d = settings.DESCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )

        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)
        expected_error = (
            'There was a problem when parsing the request:'
            ' The column "{b}" is not available for filtering.'.format(b=bad_sort_kw)
        )
        self.assertEqual(results['error'], expected_error)

        bad_asc_param = 'aaa'
        self.assertFalse(bad_asc_param == settings.ASCENDING)
        suffix = '?{s}={a}:padj,{d}:log2FoldChange'.format(
            s = settings.SORT_PARAM,
            a = bad_asc_param,
            d = settings.DESCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )

        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        self.assertTrue('error' in results)
        expected_error = (
            'There was a problem when parsing the request: '
            'The sort order "{b}" is not an available option. Choose from: {a},{d}'.format(
                b = bad_asc_param,
                a = settings.ASCENDING,
                d = settings.DESCENDING  
            )
        )
        self.assertEqual(results['error'], expected_error)

        # bad column (should be padj, not adjP)
        suffix = '?{s}={a}:adjP,{d}:log2FoldChange'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING,
            d = settings.DESCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)
        results = response.json()
        expected_error = (
            'There was a problem when parsing the request:'
            ' The column identifier "adjP" does not exist in this resource.'
            ' Options are: overall_mean,Control,Experimental,log2FoldChange,lfcSE,stat,pvalue,padj'
        )
        self.assertEqual(results['error'], expected_error)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_sort_on_string_field(self, mock_check_resource_request):
        '''
        For testing if table-based resources are sorted correctly when
        sorting on a non-numeric/string field
        '''
        f = os.path.join(self.TESTDIR, 'table_with_string_field.tsv')
        N = 3 # the number of rows in the table
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = FEATURE_TABLE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        self.assertTrue(len(results) == N)

        suffix = '?{s}={a}:colB,{d}:colA'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING,
            d = settings.DESCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        expected_ordering = ['C', 'A', 'B']
        self.assertEqual([x['rowname'] for x in results], expected_ordering)

        suffix = '?{s}={a}:colB,{a}:colA'.format(
            s = settings.SORT_PARAM,
            a = settings.ASCENDING
        )
        url = base_url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        expected_ordering = ['A', 'C', 'B']
        self.assertEqual([x['rowname'] for x in results], expected_ordering)

    @mock.patch('api.views.resource_views.check_resource_request')
    def test_bed_like_files_return_contents_with_header(self, mock_check_resource_request):
        '''
        For testing that the JSON payload returned by this endpoint has column
        headers so that the previews, etc. show up properly.
        '''

        f = os.path.join(self.TESTDIR, 'narrowpeak_example.bed')
        associate_file_with_resource(self.resource, f)
        self.resource.resource_type = NARROWPEAK_FILE_KEY
        self.resource.file_format = TSV_FORMAT
        self.resource.save()
        mock_check_resource_request.return_value = (True, self.resource)

        # the base url (no query params) should return all the records
        base_url = reverse(
            'resource-contents', 
            kwargs={'pk':self.resource.pk}
        )
        response = self.authenticated_regular_client.get(
            base_url, format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_200_OK)
        results = response.json()
        col_headers = list(results[0]['values'].keys())
        self.assertCountEqual(col_headers, NarrowPeakFile.NAMES)


class ResourceDetailTests(BaseAPITestCase):

    def setUp(self):

        self.establish_clients()

        # get an example from the database:
        regular_user_resources = Resource.objects.filter(
            owner=self.regular_user_1,
        )
        if len(regular_user_resources) == 0:
            msg = '''
                Testing not setup correctly.  Please ensure that there is at least one
                Resource instance for the user {user}
            '''.format(user=self.regular_user_1)
            raise ImproperlyConfigured(msg)

        active_resources = []
        inactive_resources = []
        for r in regular_user_resources:
            if r.is_active:
                active_resources.append(r)
            else:
                inactive_resources.append(r)
        if len(active_resources) == 0:
            raise ImproperlyConfigured('Need at least one active resource.')
        if len(inactive_resources) == 0:
            raise ImproperlyConfigured('Need at least one INactive resource.')
        # grab the first:
        self.active_resource = active_resources[0]
        self.inactive_resource = inactive_resources[0]


        # we need some Resources that are associated with a Workspace and 
        # some that are not.
        unassociated_resources = []
        workspace_resources = []
        for r in regular_user_resources:
            workspace_set = r.workspaces.all()
            if (len(workspace_set) > 0) and (r.is_active):
                workspace_resources.append(r)
            elif r.is_active:
                unassociated_resources.append(r)
        
        # need an active AND unattached resource
        active_and_unattached = set(
                [x.pk for x in active_resources]
            ).intersection(set(
                [x.pk for x in unassociated_resources]
            )
        )
        if len(active_and_unattached) == 0:
            raise ImproperlyConfigured('Need at least one active and unattached'
                ' Resource to run this test.'
        )

        self.regular_user_unattached_resource = unassociated_resources[0]
        self.regular_user_workspace_resource = workspace_resources[0]
        self.populated_workspace = self.regular_user_workspace_resource.workspaces.all()[0]
        active_unattached_resource_pk = list(active_and_unattached)[0]
        self.regular_user_active_unattached_resource = Resource.objects.get(
            pk=active_unattached_resource_pk)

        self.url_for_unattached = reverse(
            'resource-detail', 
            kwargs={'pk':self.regular_user_unattached_resource.pk}
        )
        self.url_for_active_unattached = reverse(
            'resource-detail', 
            kwargs={'pk':self.regular_user_active_unattached_resource.pk}
        )
        self.url_for_workspace_resource = reverse(
            'resource-detail', 
            kwargs={'pk':self.regular_user_workspace_resource.pk}
        )
        self.url_for_active_resource = reverse(
            'resource-detail', 
            kwargs={'pk':self.active_resource.pk}
        )
        self.url_for_inactive_resource = reverse(
            'resource-detail', 
            kwargs={'pk':self.inactive_resource.pk}
        )

    def test_resource_detail_requires_auth(self):
        """
        Test that general requests to the endpoint generate 401
        """
        response = self.regular_client.get(self.url_for_unattached)
        self.assertTrue((response.status_code == status.HTTP_401_UNAUTHORIZED) 
        | (response.status_code == status.HTTP_403_FORBIDDEN))
        response = self.regular_client.get(self.url_for_workspace_resource)
        self.assertTrue((response.status_code == status.HTTP_401_UNAUTHORIZED) 
        | (response.status_code == status.HTTP_403_FORBIDDEN))

    def test_admin_cannot_view_resource_detail(self):
        """
        Test that admins can't view the Workpace detail for anyone
        """
        response = self.authenticated_admin_client.get(self.url_for_unattached)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @mock.patch('api.views.resource_views.async_delete_file')
    def test_admin_cannot_delete_resource(self, mock_delete_file):
        """
        Test that admin users can't delete even an unattached Resource via the api
        """
        response = self.authenticated_admin_client.delete(self.url_for_active_unattached)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        mock_delete_file.delay.assert_not_called()
        # this will raise an Exception if it was not found. So a successful query means we pass
        Resource.objects.get(pk=self.regular_user_active_unattached_resource.pk)

    @mock.patch('api.views.resource_views.async_delete_file')
    def test_admin_cannot_delete_workspace_resource(self, mock_delete_file):
        """
        Test that even admin users can't delete a workspace-associated Resource if it 
        has not been used. This is the case whether or not operations were performed
        using that resource.
        """
        response = self.authenticated_admin_client.delete(self.url_for_workspace_resource)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        mock_delete_file.delay.assert_not_called()
        Resource.objects.get(pk=self.regular_user_workspace_resource.pk)

    def test_users_can_view_own_resource_detail(self):
        """
        Test that regular users can view their own Resource detail.
        """
        response = self.authenticated_regular_client.get(self.url_for_unattached)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(str(self.regular_user_unattached_resource.pk), response.data['id'])


    @mock.patch('api.views.resource_views.async_delete_file')
    def test_users_cannot_delete_attached_resource(self, mock_delete_file):
        """
        Test that regular users can't delete their own Resource even if it has 
        NOT been used within a Workspace. Users need to unattach it. Check that the 
        direct call to delete is rejected.
        """
        response = self.authenticated_regular_client.delete(self.url_for_workspace_resource)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        mock_delete_file.delay.assert_not_called()
        # check that the resource still exists
        Resource.objects.get(pk=self.regular_user_workspace_resource.pk)

    def test_other_users_cannot_delete_resource(self):
        """
        Test that another regular users can't delete someone else's Workpace.
        """
        response = self.authenticated_other_client.delete(self.url_for_unattached)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_other_user_cannot_view_resource_detail(self):
        """
        Test that another regular user can't view the Workpace detail.
        """
        response = self.authenticated_other_client.get(self.url_for_unattached)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


    def test_users_cannot_change_owner(self):
        '''
        Regular users cannot change the owner of a Resource.  That
        would amount to assigning a Resource to someone else- do not
        want that.
        '''
        payload = {'owner_email':self.regular_user_2.email}
        response = self.authenticated_regular_client.patch(
            self.url_for_unattached, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        payload = {'owner_email':self.regular_user_2.email}
        response = self.authenticated_regular_client.patch(
            self.url_for_workspace_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_user_cannot_directly_edit_resource_workspace(self):
        '''
        Test that the put/patch to the resources endpoint 
        ignores any request to change the workspace
        '''
        # get the workspace to which the resource is assigned:
        all_workspaces = self.regular_user_workspace_resource.workspaces.all()
        workspace1 = all_workspaces[0]

        # get another workspace owned by that user:
        all_user_workspaces = Workspace.objects.filter(
            owner=self.regular_user_workspace_resource.owner
        )
        other_workspaces = [x for x in all_user_workspaces if not x==workspace1]
        if len(other_workspaces) == 0:
            raise ImproperlyConfigured('Need to create another Workspace for'
                ' user {user_email}'.format(
                    user_email=self.regular_user_workspace_resource.owner.email
                )
            )
        other_workspace = other_workspaces[0]
        payload = {'workspace': other_workspace.pk}

        # try for a resource already attached to a workspace
        response = self.authenticated_regular_client.patch(
            self.url_for_workspace_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_user_cannot_change_active_status(self):
        '''
        The `is_active` boolean cannot be altered by a regular user.
        `is_active` is used to block edits while validation is processing, etc.

        The `is_active` is ignored for requests from regular users
        so there is no 400 returned.  Rather, we check that the flag
        has not changed.
        ''' 
        # check that it was not active to start:
        self.assertTrue(self.regular_user_workspace_resource.is_active)
        payload = {'is_active': False}
        response = self.authenticated_regular_client.patch(
            self.url_for_workspace_resource, payload, format='json'
        )
        r = Resource.objects.get(pk=self.regular_user_workspace_resource.pk)
        self.assertTrue(r.is_active)

    def test_admin_cannot_change_active_status(self):
        '''
        The `is_active` boolean cannot be reset via the API, even by
        an admin
        ''' 
        # find the status at the start:
        initial_status = self.regular_user_unattached_resource.is_active
        final_status = not initial_status

        payload = {'is_active': final_status}
        response = self.authenticated_admin_client.patch(
            self.url_for_unattached, payload, format='json'
        )
        r = Resource.objects.get(pk=self.regular_user_unattached_resource.pk)

        # check that the bool changed:
        self.assertEqual(r.is_active, initial_status)

    def test_cannot_change_status_message(self):
        '''
        The `status` string canNOT be reset by a regular user
        ''' 
        # check that it was not active to start:
        orig_status = self.regular_user_unattached_resource.status

        payload = {'status': 'something'}
        response = self.authenticated_regular_client.patch(
            self.url_for_unattached, payload, format='json'
        )
        r = Resource.objects.get(pk=self.regular_user_unattached_resource.pk)
        self.assertTrue(r.status == orig_status)
        # the change was effectively ignored so we get a 200
        self.assertEqual(response.status_code, 200)

        # Now check that admins can't make changes iether
        orig_status = self.active_resource.status
        payload = {'status': 'something'}
        response = self.authenticated_admin_client.patch(
            self.url_for_active_resource, payload, format='json'
        )
        r = Resource.objects.get(pk=self.active_resource.pk)
        self.assertTrue(r.status == orig_status)
        self.assertEqual(response.status_code, 403)

    def test_user_cannot_change_date_added(self):
        '''
        Once the Resource has been added, there is no editing
        of the DateTime.
        '''
        orig_datetime = self.regular_user_unattached_resource.creation_datetime
        original_pk = self.regular_user_unattached_resource.pk

        date_str = 'May 20, 2018 (16:00:07)'
        payload = {'created': date_str}
        response = self.authenticated_regular_client.patch(
            self.url_for_unattached, payload, format='json'
        )
        # since the field is ignored, it will not raise any exception.
        # Still want to check that the object is unchanged:
        r = Resource.objects.get(pk=original_pk)
        self.assertEqual(orig_datetime, r.creation_datetime)
        orig_datestring = orig_datetime.strftime('%B %d, %Y, (%H:%M:%S)')
        self.assertTrue(orig_datestring != date_str)


    def test_user_cant_make_resource_public(self):
        '''
        Regular users are not allowed to effect public/private
        chanage on Resources
        '''
        private_resources = Resource.objects.filter(
            owner = self.regular_user_1,
            is_active = True,
            is_public = False
        )
        if len(private_resources) > 0:
            private_resource = private_resources[0]

            url = reverse(
                'resource-detail', 
                kwargs={'pk':private_resource.pk}
            )
            payload = {'is_public': True}
            response = self.authenticated_regular_client.patch(
                url, payload, format='json'
            )
            # we basically ignore it, so 200 response
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            r = Resource.objects.get(pk=private_resource.pk)
            self.assertFalse(r.is_public)

        else:
            raise ImproperlyConfigured('To properly run this test, you'
            ' need to have at least one public Resource.')

    def test_admin_user_cannot_make_resource_public(self):
        '''
        Admin users can't affect public/private status
        '''
        private_resources = Resource.objects.filter(
            owner = self.regular_user_1,
            is_active = True,
            is_public = False
        )
        if len(private_resources) > 0:
            private_resource = private_resources[0]

            url = reverse(
                'resource-detail', 
                kwargs={'pk':private_resource.pk}
            )
            payload = {'is_public': True}
            response = self.authenticated_admin_client.patch(
                url, payload, format='json'
            )
            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
            r = Resource.objects.get(pk=private_resource.pk)
            self.assertFalse(r.is_public)

        else:
            raise ImproperlyConfigured('To properly run this test, you'
            ' need to have at least one public Resource.')

    def test_user_cant_make_resource_private(self):
        '''
        If a Resource was public, regular users can't make it private
        '''
        active_and_public_resources = Resource.objects.filter(
            is_active = True,
            is_public = True,
            owner = self.regular_user_1
        )
        if len(active_and_public_resources) == 0:
            raise ImproperlyConfigured('To properly run this test, you'
            ' need to have at least one public AND active Resource.')
        r = active_and_public_resources[0]
        url = reverse(
            'resource-detail', 
            kwargs={'pk':r.pk}
        )
        payload = {'is_public': False}
        response = self.authenticated_regular_client.patch(
            url, payload, format='json'
        )
        # it was ok to try, but we just ignore it.
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        updated_resource = Resource.objects.get(pk=r.pk)
        self.assertTrue(updated_resource.is_public)


    def test_admin_user_cant_make_resource_private(self):
        '''
        Admins users can't edit public/private status
        '''
        active_and_public_resources = Resource.objects.filter(
            is_active = True,
            is_public = True,
            owner = self.regular_user_1
        )
        if len(active_and_public_resources) == 0:
            raise ImproperlyConfigured('To properly run this test, you'
            ' need to have at least one public AND active Resource.')
        r = active_and_public_resources[0]
        url = reverse(
            'resource-detail', 
            kwargs={'pk':r.pk}
        )
        payload = {'is_public': False}
        response = self.authenticated_admin_client.patch(
            url, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        updated_resource = Resource.objects.get(pk=r.pk)
        self.assertTrue(updated_resource.is_public)

    def test_cannot_make_changes_when_inactive(self):
        '''
        Test that no changes can be made when the resource is inactive.
        '''
        self.assertFalse(self.inactive_resource.is_active)

        # just try to change the name
        payload = {'name': 'some_name'}
        response = self.authenticated_regular_client.patch(
            self.url_for_inactive_resource, payload, format='json'
        )
        self.assertTrue(response.status_code == status.HTTP_400_BAD_REQUEST)

    def test_user_can_change_resource_name(self):
        '''
        Users may change the Resource name.  This does NOT
        change anything about the path, file format, etc.

        '''
        original_name = self.active_resource.name
        original_format = self.active_resource.file_format
        payload = {'name': 'newname.txt'}
        response = self.authenticated_regular_client.patch(
            self.url_for_active_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        json_obj = response.json()
        self.assertTrue(json_obj['name'], 'newname.txt')

        # just double check that the original name wasn't the same
        # by chance
        self.assertTrue(original_name != 'newname.txt')

        # check that this did NOT change anything about the file format
        self.assertTrue(self.active_resource.file_format == json_obj['file_format'])

    @mock.patch('api.serializers.resource.api_tasks')
    def test_user_can_change_resource_format(self, mock_api_tasks):
        '''
        Users may change the Resource format.  This does NOT
        change anything about the name, path, etc.

        '''

        # set the type + format explicitly and save
        self.active_resource.resource_type = MATRIX_KEY
        original_format = CSV_FORMAT
        self.active_resource.file_format = original_format
        self.active_resource.save()

        # Set the requested format and also ensure we 
        # are actually making a change
        requested_format = TSV_FORMAT
        self.assertTrue(original_format != requested_format)

        payload = {'file_format': requested_format}
        response = self.authenticated_regular_client.patch(
            self.url_for_active_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        json_obj = response.json()

        # check that this did NOT change anything about the file format
        # since it has only entered the validation phase.
        self.assertTrue(requested_format != json_obj['file_format'])

        # active state set to False
        self.assertFalse(json_obj['is_active'])

        # check that the validation method was called.
        mock_api_tasks.validate_resource.delay.assert_called_with(
            self.active_resource.pk, 
            self.active_resource.resource_type, 
            requested_format
        )

    @mock.patch('api.serializers.resource.api_tasks')
    def test_changing_resource_type_resets_status(self,  
        mock_api_tasks):
        '''
        If an attempt is made to change the resource type
        ensure that the resource has its "active" state 
        set to False and that the status changes.

        Since the validation can take some time, it will call
        the asynchronous validation process.
        '''
        current_resource_type = self.active_resource.resource_type
        other_types = set(
            [x[0] for x in DATABASE_RESOURCE_TYPES]
            ).difference(set([current_resource_type]))
        newtype = list(other_types)[0]

        # verify that we are actually changing the type 
        # in this request (i.e. not a trivial test)
        self.assertFalse(
            newtype == current_resource_type
        )
        payload = {'resource_type': newtype}
        response = self.authenticated_regular_client.patch(
            self.url_for_active_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        r = Resource.objects.get(pk=self.active_resource.pk)

        # active state set to False
        self.assertFalse(r.is_active)

        # check that the validation method was called.
        mock_api_tasks.validate_resource.delay.assert_called_with(
            self.active_resource.pk, newtype, self.active_resource.file_format)

    @mock.patch('api.serializers.resource.api_tasks')
    def test_change_both_resource_type_and_format(self,  
        mock_api_tasks):
        '''
        Test that we can simultaneously set the resource type and format
        as one might do upon an initial upload

        Since the validation can take some time, it will call
        the asynchronous validation process.
        '''
        current_resource_type = MATRIX_KEY
        self.active_resource.resource_type = current_resource_type
        other_types = set(
            [x[0] for x in DATABASE_RESOURCE_TYPES]
            ).difference(set([current_resource_type]))
        newtype = list(other_types)[0]

        # verify that we are actually changing the type 
        # in this request (i.e. not a trivial test)
        self.assertFalse(
            newtype == current_resource_type
        )

        # set the format explicitly and save
        original_format = CSV_FORMAT
        self.active_resource.file_format = original_format
        self.active_resource.save()

        # Set the requested format and also ensure we 
        # are actually making a change
        requested_format = TSV_FORMAT
        self.assertTrue(original_format != requested_format)

        payload = {
            'resource_type': newtype,
            'file_format': requested_format
        }
        response = self.authenticated_regular_client.patch(
            self.url_for_active_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        r = Resource.objects.get(pk=self.active_resource.pk)

        # active state set to False
        self.assertFalse(r.is_active)

        # check that the validation method was called.
        mock_api_tasks.validate_resource.delay.assert_called_with(
            self.active_resource.pk, newtype, requested_format)


    @mock.patch('api.serializers.resource.api_tasks')
    def test_initial_resource_validation_with_incomplete_data(self,  
        mock_api_tasks):
        '''
        Upon the initial upload, a user has not set the resource_type
        or the file_format fields. Once those have values, a user can
        change one or the other independently. However, prior to that, 
        the initial validation needs to get BOTH fields or it will never
        validate.
        '''

        r = Resource.objects.create(
            name = 'some_file',
            owner = self.regular_user_1,
            is_active=True,
            datafile = File(BytesIO(), 'abc'),
        )
        resource_pk = r.pk
        url = reverse(
            'resource-detail', 
            kwargs={'pk':resource_pk}
        )
        self.assertIsNone(r.resource_type)
        self.assertIsNone(r.file_format)
        resource_type = MATRIX_KEY
        payload = {
            'resource_type': resource_type
        }
        response = self.authenticated_regular_client.patch(
            url, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # check that the validation method was called.
        # Note that in practice this call would fail since we are NOT
        # setting both of the required fields. However, since it's a mock 
        # here, we are only testing the args which are passed.
        mock_api_tasks.validate_resource.delay.assert_called_with(
            resource_pk, resource_type, None)

        

    def test_setting_workspace_to_null_fails(self):
        '''
        Test that directly setting the workspace to null fails.
        Users can't change a Resource's workspace.  They can only 
        remove unused Resources from a Workspace.
        '''
        payload = {'workspace': None}

        # get the original set of workspaces for the resource
        orig_workspaces = [x.pk for x in self.regular_user_workspace_resource.workspaces.all()]

        # try for an attached resource
        response = self.authenticated_regular_client.patch(
            self.url_for_workspace_resource, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # query the resource again, see that the workspaces have not 
        # changed
        rr = Resource.objects.get(pk=self.regular_user_workspace_resource.pk)
        current_workspaces = [x.pk for x in rr.workspaces.all()]
        self.assertEqual(current_workspaces, orig_workspaces)

        # try for an unattached resource
        # get the original set of workspaces for the resource
        orig_workspaces = [x.pk for x in self.regular_user_unattached_resource.workspaces.all()]

        response = self.authenticated_regular_client.patch(
            self.url_for_unattached, payload, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # query the resource again, see that the workspaces have not 
        # changed
        rr = Resource.objects.get(pk=self.regular_user_unattached_resource.pk)
        current_workspaces = [x.pk for x in rr.workspaces.all()]
        self.assertEqual(current_workspaces, orig_workspaces)

class ResourceContentTransformTests(BaseAPITestCase):
    '''
    Tests the endpoint which returns transformed file contents
    '''
    def setUp(self):
        '''
        Set up the api clients, the url we are testing, and 
        get an active resource to test with
        '''
        self.establish_clients()
        self.TESTDIR = os.path.join(
            os.path.dirname(__file__),
            'resource_contents_test_files'    
        )
        # get an example from the database:
        regular_user_resources = Resource.objects.filter(
            owner=self.regular_user_1,
        )
        if len(regular_user_resources) == 0:
            msg = '''
                Testing not setup correctly.  Please ensure that there is at least one
                Resource instance for the user {user}
            '''.format(user=self.regular_user_1)
            raise ImproperlyConfigured(msg)
        for r in regular_user_resources:
            if r.is_active:
                active_resource = r
                break
        self.resource = active_resource
        self.url = reverse(
            'resource-contents-transform', 
            kwargs={'pk':self.resource.pk}
        )
        
    @mock.patch('api.views.resource_views.get_transformation_function')
    def test_response(self, mock_get_transformation_function):
        '''
        Test that we get the expected response from a correct call
        '''
        mock_result = {'a':1}
        mock_fn = mock.MagicMock()
        mock_get_transformation_function.return_value = mock_fn
        mock_fn.return_value = mock_result

        suffix = '?transform-name=x&a=0'
        url = self.url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        j = response.json()
        self.assertDictEqual(j, mock_result)

    @mock.patch('api.views.resource_views.get_transformation_function')
    def test_err(self, mock_get_transformation_function):
        '''
        Test that we handle an error appropriately in the called
        transformation fn
        '''
        mock_fn = mock.MagicMock()
        mock_get_transformation_function.return_value = mock_fn
        mock_fn.side_effect = [Exception('!!!'),]

        suffix = '?transform-name=x&a=0'
        url = self.url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        j = response.json()
        self.assertEqual(j['error'], '!!!')

    @mock.patch('api.views.resource_views.get_transformation_function')
    def test_invalid_transform(self, mock_get_transformation_function):
        '''
        Test that we handle an error appropriately if a transform is requested
        that does not exist
        '''
        mock_fn = mock.MagicMock()
        mock_get_transformation_function.side_effect = [Exception('not found'),]

        suffix = '?transform-name=x&a=0'
        url = self.url + suffix
        response = self.authenticated_regular_client.get(
            url, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        j = response.json()
        self.assertEqual(j['error'], 'not found')

    @mock.patch('api.views.resource_views.get_transformation_function')
    def test_missing_transform_name_param(self, mock_get_transformation_function):
        '''
        Test that we respond appropriately if the "transform-name" 
        query param is not given.
        '''
        response = self.authenticated_regular_client.get(
            self.url, format='json'
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        j = response.json()
        self.assertTrue( 'transform-name' in j['error'])

class BucketResourceAddTests(BaseAPITestCase):

    def setUp(self):

        self.url = reverse('bucket-resource-add')
        self.establish_clients()

    def test_requires_auth(self):
        """
        Test that general requests to the endpoint generate 401
        """
        response = self.regular_client.get(self.url)
        self.assertTrue((response.status_code == status.HTTP_401_UNAUTHORIZED) 
        | (response.status_code == status.HTTP_403_FORBIDDEN))

    def test_local_storage_backend_fails_request(self):
        '''
        If the storage backend is NOT bucket-based, then we 
        can't reasonably handle requests to this endpoint.
        Hence, fail all the requests in this case.
        '''
        response = self.authenticated_regular_client.post(
            self.url, 
            data = {'bucket_path': 'gs://some-bucket/some-path'},
            format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)

    def test_missing_key(self):
        '''
        Test that we ask for the required key and ignore other payload data
        '''

        # typo on 'bucket_path'
        response = self.authenticated_regular_client.post(
            self.url, 
            data = {'bucket_pathS': 'gs://some-bucket/some-path', 'abc':'foo'},
            format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)

    @mock.patch('api.views.resource_views.default_storage')
    def test_nonexistent_url(self, mock_default_storage):
        '''
        Tests the case where the supplied file path does not exist 
        OR it is not accessible. The tests that actually check existence
        vs access are contained at the storage-class implementation level.
        
        '''
        mock_default_storage.create_resource_from_interbucket_copy.side_effect = FileNotFoundError
        response = self.authenticated_regular_client.post(
            self.url, 
            data = {'bucket_path': 'gs://some-bucket/some-path'},
            format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_400_BAD_REQUEST)

    @mock.patch('api.views.resource_views.async_validate_resource')
    @mock.patch('api.views.resource_views.default_storage')
    @mock.patch('api.views.resource_views.ResourceSerializer')
    def test_path_added_correctly(self, 
        mock_serializer,
        mock_default_storage,
        mock_async_validate_resource):
        '''
        Tests the case where everything works-- check that the file goes
        where we expect.
        '''
        u = str(uuid.uuid4())
        mock_resource = mock.MagicMock()
        mock_resource.pk = u
        mock_default_storage.create_resource_from_interbucket_copy.return_value = mock_resource

        mocked_serialization = mock.MagicMock
        mocked_serialization.data = {}
        mock_serializer.return_value = mocked_serialization
        mock_path = 'some_path'
        response = self.authenticated_regular_client.post(
            self.url, 
            data = {'bucket_path': mock_path},
            format='json'
        )
        self.assertEqual(response.status_code, 
            status.HTTP_201_CREATED)

        mock_default_storage.create_resource_from_interbucket_copy.assert_called_with(
            self.regular_user_1,
            mock_path
        )
        mock_async_validate_resource.delay.assert_called_with(
            u,
            None,
            None
        )