import unittest.mock as mock

from django.test import override_settings
from django.conf import settings

import pandas as pd
import numpy as np

from resource_types.table_types import TableResource, \
    Matrix, \
    RnaSeqCountMatrix, \
    S3_RECORD_DELIMITER, \
    PREVIEW_NUM_LINES

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
        expected_sql = "SELECT * FROM s3object s WHERE LOWER(s.__id__) = 'abc'"
        self.assertEqual(sql, expected_sql)

        # a startswith
        query_params = {
            '__rowname__': '[startswith]:ab'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.__id__ LIKE 'ab%'"
        self.assertEqual(sql, expected_sql)

    def test_sql_for_preview(self):
        """
        Test that we construct the proper SQL
        commands when requesting a preview of a resource
        """
        t = TableResource()
        query_params = {}
        sql = t._construct_s3_query_sql(query_params, preview=True)
        expected_sql = f'SELECT * FROM s3object s LIMIT {PREVIEW_NUM_LINES}'
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

    def test_sql_numerical_filter_commands(self):
        """
        Test that we construct the proper SQL
        commands given the parameter string when we
        are attempting to filter on column contents.
        """
        t = TableResource()

        query_params = {
            'padj': '[lte]:0.05'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = ("SELECT * FROM s3object s WHERE LOWER(s.padj) != 'inf'"
         " and LOWER(s.padj) != '-inf' and s.padj != '' and s.padj != 'NA' and"
         " CAST(s.padj AS FLOAT) <= 0.05")
        self.assertEqual(sql, expected_sql)

        query_params = {
            'padj': '[lte]:0.05',
            'log2FoldChange': '[gt]:2'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = ("SELECT * FROM s3object s WHERE "
        "LOWER(s.padj) != 'inf' and LOWER(s.padj) != '-inf' "
        "and s.padj != '' and s.padj != 'NA' and CAST(s.padj AS FLOAT) <= 0.05 "
        "and LOWER(s.log2FoldChange) != 'inf' "
        "and LOWER(s.log2FoldChange) != '-inf' and s.log2FoldChange != '' "
        "and s.log2FoldChange != 'NA' and CAST(s.log2FoldChange AS FLOAT) > 2.0")
        self.assertEqual(sql, expected_sql)

    def test_sql_numerical_equality_filter_commands(self):
        """
        Test that we format the SQL properly for a strict equality
        """
        t = TableResource()

        query_params = {
            'padj': '[eq]:0.05'
        }
        sql = t._construct_s3_query_sql(query_params)
        # note that even though we are requesting an allegedly
        # numerical value to be equal to 0.05, we do NOT 
        # perform the cast
        expected_sql = 'SELECT * FROM s3object s WHERE s.padj = 0.05'
        self.assertEqual(sql, expected_sql)

    def test_sql_case_insensitive_filter(self):
        """
        Test that we format the SQL properly for a case insensitive
        match
        """
        t = TableResource()

        query_params = {
            'gender': '[case-ins-eq]:m'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE lower(s.gender) = 'm'"
        self.assertEqual(sql, expected_sql)

    def test_sql_column_value_startswith_filter(self):
        """
        Test that we format the SQL properly for a startswith
        filter on a particular column
        """
        t = TableResource()

        query_params = {
            'gender': '[startswith]:m'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.gender LIKE 'm%'"
        self.assertEqual(sql, expected_sql)

    def test_sql_column_value_includes_filter(self):
        """
        Test that we format the SQL properly for a 'is in'
        filter on a particular column
        """
        t = TableResource()

        query_params = {
            'gender': '[in]:m,f'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = "SELECT * FROM s3object s WHERE s.gender IN ('m','f')"
        self.assertEqual(sql, expected_sql)

    def test_sql_column_value_and_row_query(self):
        """
        Test that we format the SQL properly for a combination of 'is in'
        filter on a particular column AND rows
        """
        t = TableResource()

        query_params = {
            'gender': '[in]:m,f',
            '__rowname__': '[in]:a,b'
        }
        sql = t._construct_s3_query_sql(query_params)
        # note the limit 2 due to teh fact that the __id__ column contains unique entries. Hence,
        # we should only get back two results.
        expected_sql = "SELECT * FROM s3object s WHERE s.gender IN ('m','f') and s.__id__ IN ('a','b') LIMIT 2"
        self.assertEqual(sql, expected_sql)

    def test_sql_column_value_and_colname_query(self):
        """
        Test that we format the SQL properly for a 'is in'
        filter on a particular column
        """
        t = TableResource()

        query_params = {
            'gender': '[in]:m,f',
            '__colname__': '[in]:a,b'
        }
        sql = t._construct_s3_query_sql(query_params)
        expected_sql = 'SELECT s."__id__",s."a",s."b" FROM s3object s WHERE s.gender IN (\'m\',\'f\')'
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


    @mock.patch('resource_types.table_types.TableResource._make_s3_select')
    def test_empty_payload_response(self, mock_make_s3_select):

        mock_make_s3_select.return_value = ''

        mock_resource = mock.MagicMock()
        mock_key = 'some-key'
        mock_resource.datafile.name = mock_key
        mock_sql = 'some_sql'

        t = TableResource()
        t._issue_s3_select_query(mock_resource, mock_sql)
        actual_df = t.table
        expected_df = pd.DataFrame([])
        self.assertTrue(expected_df.equals(actual_df))
        mock_make_s3_select.assert_called_with(mock_key, mock_sql)

    @override_settings(TMP_DIR='/data/tmp')
    @mock.patch('resource_types.table_types.TableResource._make_s3_select')
    @mock.patch('resource_types.table_types.time')
    def test_handles_malformatted_response(self, mock_time, mock_make_s3_select):
        '''
        Tests the situation where the payload was truncated or similar
        and we can't parse the resulting string as JSON properly.

        Ensures we dump the result into a file for inspection at a later time
        '''
        mock_time.time.return_value = 123

        # s3 select returns records that look like:
        record1 = '{"'+ FIRST_COLUMN_ID +'":"geneA","colA":"10","colB":"11"}'
        record2 = '{"'+ FIRST_COLUMN_ID +'":"geneB","colA":"12","co' # <-- truncated!

        returned_string = f'{record1}{S3_RECORD_DELIMITER}{record2}'
        mock_make_s3_select.return_value = returned_string

        mock_resource = mock.MagicMock()
        mock_key = 'some-key'
        mock_pk = 'some-pk'
        mock_resource.datafile.name = mock_key
        mock_resource.pk = mock_pk
        mock_sql = 'some_sql'

        t = TableResource()
        mock_open = mock.mock_open()
        with self.assertRaises(Exception):
            with mock.patch('resource_types.table_types.open', mock_open):
                t._issue_s3_select_query(mock_resource, mock_sql)
        mock_open.assert_called_once_with(f'/data/tmp/sql_dump.{mock_pk}.123.txt', 'w')
        mock_make_s3_select.assert_called_with(mock_key, mock_sql)

    def test_handle_error_response_from_s3(self):
        '''
        Depending on the response code, we can catch different potential
        errors
        '''

        t = TableResource()
        mock_selection_exception = mock.MagicMock()
        mock_response = {
            'Error': {
                'Code': 'MissingHeaders',
                'Message': 'Some headers in the query...'
            }
        }
        mock_selection_exception.response = mock_response
        s = t._handle_s3_select_problem(mock_selection_exception)
        expected_str = 'One or more of the columns was not properly specified.'
        self.assertEqual(s, expected_str)

        # some other response:
        mock_response = {
            'Error': {
                'Code': '???',
                'Message': 'Some error'
            }
        }
        mock_selection_exception.response = mock_response
        s = t._handle_s3_select_problem(mock_selection_exception)
        expected_str = 'Experienced a problem when querying from S3 select.'
        self.assertEqual(s, expected_str)

        # finally, an exception with a different format
        mock_response = {
            'XYZ': {
                'keyA': '???',
            }
        }
        mock_selection_exception.response = mock_response
        s = t._handle_s3_select_problem(mock_selection_exception)
        expected_str = 'Experienced a problem when querying from S3 select.'
        self.assertEqual(s, expected_str)

    def test_cast_case1(self):
        '''
        Here, we test that we can properly handle the returned
        payload from an s3 select query.

        Technically, our integer matrices allow NA values in validation.
        The resulting file written to our storage (post-validation) contains
        a blank value where the NA value was. We need to ensure that we 
        handle this case when retrieving the contents of the resource.
        Note that this doesn't affect anything related to analyses,etc
        since they do not use the 'get_contents' functionality.
        '''
        # the payload returned from S3 after it has been converted to JSON.
        # Note the blank value for (g4, sB)
        j = [
                {'__id__': 'g1', 'sA': '0', 'sB': '1.0', 'sC': '2'},
                {'__id__': 'g2', 'sA': '10', 'sB': '11.0', 'sC': '12'},
                {'__id__': 'g3', 'sA': '20', 'sB': '21.0', 'sC': '22'},
                {'__id__': 'g4', 'sA': '30', 'sB': '', 'sC': '32'},
                {'__id__': 'g5', 'sA': '40', 'sB': '41.0', 'sC': '42'}
            ]

        m = Matrix()
        m.table = pd.DataFrame(j).set_index(FIRST_COLUMN_ID, drop=True)
        m._attempt_type_cast()
        self.assertTrue(np.isnan(m.table.loc['g4', 'sB']))