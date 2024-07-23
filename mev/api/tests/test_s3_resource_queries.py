from resource_types.table_types import TableResource

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