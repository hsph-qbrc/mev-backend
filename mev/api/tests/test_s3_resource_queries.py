import unittest.mock as mock

from django.test import override_settings
from django.conf import settings

from resource_types.table_types import TableResource, \
    Matrix
from api.tests.base import BaseAPITestCase


class TableResourceS3QueryTests(BaseAPITestCase):

    def test_sql_row_filter_commands(self):
        """
        Test that we construct the proper SQL
        commands given the parameter string
        """
        t = TableResource()

        # basic single-row query
        query_params = {
            '__rowname__': '[eq]:abc'
        }
        sql = t.construct_s3_query_params(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.__id__ = 'abc' LIMIT 1"
        self.assertEqual(sql, expected_sql)

        # query for multiple rows
        query_params = {
            '__rowname__': '[in]:abc,def'
        }
        sql = t.construct_s3_query_params(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.__id__ IN ('abc','def') LIMIT 2"
        self.assertEqual(sql, expected_sql)

        # case-insensitive single row match
        query_params = {
            '__rowname__': '[case-ins-eq]:ABC'
        }
        sql = t.construct_s3_query_params(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE LOWER(s.__id__) = 'abc' LIMIT 1"
        self.assertEqual(sql, expected_sql)

        # a startswith
        query_params = {
            '__rowname__': '[startswith]:ab'
        }
        sql = t.construct_s3_query_params(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.__id__ LIKE 'ab%'"
        self.assertEqual(sql, expected_sql)

    def test_sql_combination_filter_commands(self):
        """
        Test that we construct the proper SQL
        commands given the parameter string
        """
        t = TableResource()

        # basic single-row query
        query_params = {
            '__colname__': '[eq]:abc',
            '__rowname__': '[eq]:xyz'
        }
        sql = t.construct_s3_query_params(query_params)
        expected_sql = 'SELECT s."__id__",s."abc" FROM s3object s WHERE s.__id__ = \'xyz\' LIMIT 1'
        self.assertEqual(sql, expected_sql)

    def test_sql_column_filter_commands(self):
        """
        Test that we construct the proper SQL
        commands given the parameter string
        """
        t = TableResource()

        # basic single-row query
        query_params = {
            '__colname__': '[eq]:abc'
        }
        sql = t.construct_s3_query_params(query_params)
        expected_sql = 'SELECT s."__id__",s."abc" FROM s3object s'
        self.assertEqual(sql, expected_sql)

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
