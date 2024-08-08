import unittest.mock as mock

from django.test import override_settings
from django.conf import settings

from resource_types.table_types import TableResource, \
    Matrix
from api.tests.base import BaseAPITestCase


class TableResourceContentsTests(BaseAPITestCase):

    @override_settings(WEBMEV_DEPLOYMENT_PLATFORM=settings.AMAZON)
    @mock.patch('resource_types.table_types.TableResource._get_local_contents')
    @mock.patch('resource_types.table_types.TableResource._query_contents_on_s3')
    @mock.patch('resource_types.table_types.alert_admins')
    def test_contents_call(self, mock_alert_admins, 
                                 mock_s3_contents, 
                                 mock_local_contents):
        """
        Test that we execute the proper functions when
        retrieving resource contents
        """
        t = TableResource()
        mock_resource = mock.MagicMock()
        t.get_contents(mock_resource, {})
        mock_s3_contents.assert_called()
        mock_local_contents.assert_not_called()
        # a check that we have this attribute defined:
        self.assertTrue(len(t.additional_exported_cols) == 0)

        mock_s3_contents.reset_mock()
        mock_local_contents.reset_mock()
        t.requires_local_processing = True
        t.get_contents(mock_resource, {})
        mock_local_contents.assert_called()
        mock_s3_contents.assert_not_called()

        # mock an error with the s3 query- ensure that
        # we fall back on a local query
        mock_s3_contents.reset_mock()
        mock_local_contents.reset_mock()
        mock_s3_contents.side_effect = Exception('!!!')
        t.requires_local_processing = False
        t.get_contents(mock_resource, {})
        mock_local_contents.assert_called()
        mock_s3_contents.assert_called()
        mock_alert_admins.assert_called()

    @override_settings(WEBMEV_DEPLOYMENT_PLATFORM=settings.AMAZON)
    @mock.patch('resource_types.table_types.TableResource._get_local_contents')
    @mock.patch('resource_types.table_types.TableResource._query_contents_on_s3')
    def test_rowmeans_query_acts_locally(self, mock_s3_contents, mock_local_contents):
        '''
        S3 select does not support an aggregation function that
        will perform an operation like calculating the row-wise
        means of a matrix. Hence, we need to ensure that we fall
        back to the local method if such a query is encountered.

        This tests that we use the s3 method when there are no special
        operations needed AND that it will fall back to the local
        method if special aggregation is required.
        '''
        m = Matrix()

        # first check that we use s3 select if no special aggregation
        # functions are required:
        query_params = {
            '__rowname__': '[eq]:abc'
        }
        mock_resource = mock.MagicMock()
        contents = m.get_contents(mock_resource, query_params)
        mock_s3_contents.assert_called()
        mock_local_contents.assert_not_called()
        self.assertTrue(len(m.additional_exported_cols) == 0)

        # now change the query params to require an
        # aggregation that s3 does not offer
        m = Matrix()
        mock_s3_contents.reset_mock()
        mock_local_contents.reset_mock()
        query_params = {
            '__rowmean__': '[gte]:1.2',
            'sort_vals':'[desc]:__rowmean__'
        }
        contents = m.get_contents(mock_resource, query_params)
        mock_s3_contents.assert_not_called()
        mock_local_contents.assert_called()


    @override_settings(WEBMEV_DEPLOYMENT_PLATFORM=settings.AMAZON)
    @mock.patch('resource_types.table_types.TableResource._get_local_contents')
    @mock.patch('resource_types.table_types.TableResource._query_contents_on_s3')
    def test_uses_local_query_on_abs_value_request(self, mock_s3_contents, mock_local_contents):
        m = Matrix()

        # first check that we use s3 select if we pass a simple 'greater than':
        query_params = {
            'log2FoldChange': '[gt]:2'
        }
        mock_resource = mock.MagicMock()
        contents = m.get_contents(mock_resource, query_params)
        mock_s3_contents.assert_called()
        mock_local_contents.assert_not_called()

        # now test that we use local query if we have an absolute value request
        m = Matrix()
        mock_s3_contents.reset_mock()
        mock_local_contents.reset_mock()
        query_params = {
            'log2FC':'[absgt]:2.0'
        }
        contents = m.get_contents(mock_resource, query_params)
        mock_s3_contents.assert_not_called()
        mock_local_contents.assert_called()

        m = Matrix()
        mock_s3_contents.reset_mock()
        mock_local_contents.reset_mock()
        query_params = {
            'log2FC':'[foo]:2.0'
        }
        contents = m.get_contents(mock_resource, query_params)
        mock_s3_contents.assert_called()
        mock_local_contents.assert_not_called()