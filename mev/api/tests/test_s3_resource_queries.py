import unittest.mock as mock

from django.test import override_settings
from django.conf import settings

import pandas as pd

from resource_types.table_types import TableResource, \
    Matrix, \
    IntegerMatrix, \
    RnaSeqCountMatrix, \
    S3_RECORD_DELIMITER

from constants import FIRST_COLUMN_ID

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
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.__id__ = 'abc' LIMIT 1"
        self.assertEqual(sql, expected_sql)

        # query for multiple rows
        query_params = {
            '__rowname__': '[in]:abc,def'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.__id__ IN ('abc','def') LIMIT 2"
        self.assertEqual(sql, expected_sql)

        # case-insensitive single row match
        query_params = {
            '__rowname__': '[case-ins-eq]:ABC'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE LOWER(s.__id__) = 'abc' LIMIT 1"
        self.assertEqual(sql, expected_sql)

        # a startswith
        query_params = {
            '__rowname__': '[startswith]:ab'
        }
        sql = t._construct_s3_query_sql(query_params)
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
        sql = t._construct_s3_query_sql(query_params)
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
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = 'SELECT s."__id__",s."abc" FROM s3object s'
        self.assertEqual(sql, expected_sql)

    @mock.patch('resource_types.table_types.TableResource._make_s3_select')
    def test_payload_parse(self, mock_make_s3_select):

        # s3 select returns records that look like:
        record1 = '{"'+ FIRST_COLUMN_ID +'":"geneA","colA":"10","colB":"11"}'
        record2 = '{"'+ FIRST_COLUMN_ID +'":"geneB","colA":"12","colB":"13"}'

        returned_string = f'{record1}{S3_RECORD_DELIMITER}{record2}'
        mock_make_s3_select.return_value = returned_string

        mock_resource = mock.MagicMock()
        mock_key = 'some-key'
        mock_resource.datafile.name = mock_key
        mock_sql = 'some_sql'

        t = TableResource()
        t._issue_s3_select_query(mock_resource, mock_sql)
        actual_df = t.table
        expected_df = pd.DataFrame([["10","11"],["12","13"]], 
                                   index=['geneA', 'geneB'],
                                columns=['colA', 'colB'])
        expected_df.index.name = FIRST_COLUMN_ID
        self.assertTrue(expected_df.equals(actual_df))
        mock_make_s3_select.assert_called_with(mock_key, mock_sql)

        mock_make_s3_select.reset_mock()
        t = Matrix()
        t._issue_s3_select_query(mock_resource, mock_sql)
        actual_df = t.table
        expected_df = pd.DataFrame([[10.0,11.0],[12.0,13.0]], 
                            index=['geneA', 'geneB'],
                        columns=['colA', 'colB'])
        expected_df.index.name = FIRST_COLUMN_ID
        self.assertTrue(expected_df.equals(actual_df))
        mock_make_s3_select.assert_called_with(mock_key, mock_sql)

        mock_make_s3_select.reset_mock()
        t = RnaSeqCountMatrix()
        t._issue_s3_select_query(mock_resource, mock_sql)
        actual_df = t.table
        expected_df = pd.DataFrame([[10,11],[12,13]], 
                                   index=['geneA', 'geneB'],
                                columns=['colA', 'colB'])
        self.assertTrue(expected_df.equals(actual_df))
        mock_make_s3_select.assert_called_with(mock_key, mock_sql)