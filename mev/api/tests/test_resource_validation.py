from io import BytesIO
import unittest.mock as mock
import os
import uuid
import random
import string

import pandas as pd

from django.core.files import File

from constants import TSV_FORMAT, \
    CSV_FORMAT, \
    XLS_FORMAT, \
    XLSX_FORMAT, \
    FIRST_COLUMN_ID
from api.models import Resource
from resource_types.table_types import TableResource, \
    Matrix, \
    IntegerMatrix, \
    RnaSeqCountMatrix, \
    Network, \
    AnnotationTable, \
    BED3File, \
    BED6File, \
    NarrowPeakFile, \
    PARSE_ERROR, \
    PARSER_NOT_FOUND_ERROR, \
    NON_NUMERIC_ERROR, \
    NON_INTEGER_ERROR, \
    TRIVIAL_TABLE_ERROR, \
    BED_FORMAT_ERROR, \
    NUMBERED_COLUMN_NAMES_ERROR, \
    NUMBERED_ROW_NAMES_ERROR, \
    NONUNIQUE_ROW_NAMES_ERROR, \
    NA_ROW_NAMES_ERROR, \
    MISSING_HEADER_WARNING, \
    EMPTY_TABLE_ERROR, \
    NAME_ERROR_LIMIT

from exceptions import StringIdentifierException
from helpers import normalize_identifier
from api.tests.base import BaseAPITestCase
from api.tests.test_helpers import associate_file_with_resource

# the api/tests dir
TESTDIR = os.path.dirname(__file__)
TESTDIR = os.path.join(TESTDIR, 'resource_validation_test_files')

class TestBasicTable(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_reader_type(self):
        '''
        Is able to return the correct parser for the file extension

        Note that none of the paths below actually have to exist.  
        The method is just examining the file extension.
        '''
        t = TableResource()
        reader = t.get_reader( TSV_FORMAT)
        self.assertEqual(reader, pd.read_table)

        reader = t.get_reader(CSV_FORMAT)
        self.assertEqual(reader, pd.read_csv)

        reader = t.get_reader( CSV_FORMAT)
        self.assertEqual(reader, pd.read_csv)       

        reader = t.get_reader( TSV_FORMAT)
        self.assertEqual(reader, pd.read_table)

        reader = t.get_reader(XLS_FORMAT)
        self.assertEqual(reader, pd.read_excel)
        
        reader = t.get_reader(XLSX_FORMAT)
        self.assertEqual(reader, pd.read_excel)

        reader = t.get_reader('abc')
        self.assertIsNone(reader)

        reader = t.get_reader('odc')
        self.assertIsNone(reader)

    def test_fails_at_empty_table(self):
        '''
        If the file is literally empty, check that it fails
        '''
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_empty.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, PARSE_ERROR)
        
    def test_reads_basic_table(self):
        '''
        Is able to parse and report validation on a well-formatted
        general table.
        '''
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_general_table.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err)

    def test_reports_unknown_parser(self):
        t = TableResource()
        is_valid, err = t.validate_type(self.r, 'abc')
        self.assertFalse(is_valid)
        self.assertEqual(err, PARSER_NOT_FOUND_ERROR) 

    def test_fails_malformatted_basic_table(self):
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_malformatted_table.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

    def test_handles_comment_headerlines_appropriately(self):
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_general_table.with_comment.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err)

    def test_fails_to_validate_tables_with_unicode_ids(self):
        '''
        Since we can't control how downstream tools will handle
        tables with rows or columns that contain non-ascii,
        we block that and inform users.
        '''
        t1 = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_table_with_unicode_cols.tsv'))
        is_valid, err = t1.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        
        t2 = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_table_with_unicode_rows.tsv'))
        is_valid, err = t2.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

    def test_fails_to_validate_tables_with_invalid_ids(self):
        # if the columns or rows contain an invalid identifier,
        # we fail the validation
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_table_with_invalid_id.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

    @mock.patch('resource_types.table_types.uuid')
    def test_saves_cleaned_data_in_standard_format(self, mock_uuid):
        '''
        This tests that we save the "cleaned" data after rows with only NA
        are removed.

        This stemmed from a situation where a TSV-format table was 
        submitted that had a full row of NAs. We parsed the table and
        removed the offending row(s), but then did NOT bother to save
        this "updated" data since the original data was ALREADY in
        the desired "target" format (recall we ultimately save all
        table-based files in a standard format, e.g. TSV).

        This test ensures that we save that version where the NA-rows
        were removed. Those rows can cause challenges for downstream
        analysis tools.
        '''

        # check the situation where we are supplied a TSV
        # and save to a TSV. Previously, this was not done
        # and hence the final file still had the row of NAs.
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_table_with_full_na_row.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        u = uuid.uuid4()
        mock_uuid.uuid4.return_value = u
        t.save_in_standardized_format(self.r, TSV_FORMAT)
        df = pd.read_table(self.r.datafile.open(), index_col=0)
        self.assertCountEqual(df.index.values, ['ENSG1','ENSG3'])

        # check that we get the same behavior starting from a
        # non-standard file format (CSV)
        mock_uuid.reset_mock()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_table_with_full_na_row.csv'))
        is_valid, err = t.validate_type(self.r, CSV_FORMAT)
        self.assertTrue(is_valid)
        u = uuid.uuid4()
        mock_uuid.uuid4.return_value = u
        t.save_in_standardized_format(self.r, CSV_FORMAT)
        df = pd.read_table(self.r.datafile.open(), index_col=0)
        self.assertCountEqual(df.index.values, ['ENSG1','ENSG3'])

    @mock.patch('resource_types.table_types.uuid')
    def test_save_in_standard_adds_index_header(self, mock_uuid):
        '''
        This tests that we add a consistent identifier as a 
        column header in the first column
        '''
        # check the situation where we are supplied a TSV
        # and save to a TSV. Previously, this was not done
        # and hence the final file still had the row of NAs.
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_table_with_full_na_row.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        # check that we initially had a different name for the first col:
        self.assertFalse(t.table.index.name == FIRST_COLUMN_ID)

        u = uuid.uuid4()
        mock_uuid.uuid4.return_value = u
        t.save_in_standardized_format(self.r, TSV_FORMAT)
        df = pd.read_table(self.r.datafile.open(), index_col=0)
        self.assertCountEqual(df.index.values, ['ENSG1','ENSG3'])
        # check that the index identifier is now changed
        self.assertTrue(df.index.name == FIRST_COLUMN_ID)

        # now load a file where the first column header
        # was blank
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.no_gene_label.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        # check that we initially had a different name for the first col:
        self.assertFalse(t.table.index.name == FIRST_COLUMN_ID)

        u = uuid.uuid4()
        mock_uuid.uuid4.return_value = u
        t.save_in_standardized_format(self.r, TSV_FORMAT)
        df = pd.read_table(self.r.datafile.open(), index_col=0)

        # check that the index identifier is now changed
        self.assertTrue(df.index.name == FIRST_COLUMN_ID)

    def test_handles_excel_table_without_header(self):
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'excel_no_header.xlsx'))
        is_valid, err = t.validate_type(self.r, XLSX_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NUMBERED_COLUMN_NAMES_ERROR)  

    def test_handles_excel_table_parsed_as_tsv(self):
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'excel_no_header.xlsx'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

    def test_handles_excel_with_date(self):
        '''
        This test covers the case where someone has an Excel table
        which has converted MAR1, etc. to dates. Warn appropriately
        '''
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'xlsx_with_date_error.xlsx'))
        is_valid, err = t.validate_type(self.r, XLSX_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('datetime' in err)

    def test_index_name_valid_func(self):
        # create a couple lists, but also check that it's valid
        # since the validating function could potentially change
        valid_names = ['a','b','c','d','e']
        invalid_names = valid_names + ['f', 'g/h', 'i']

        [normalize_identifier(x) for x in valid_names]

        with self.assertRaises(StringIdentifierException):
            [normalize_identifier(x) for x in invalid_names]

        t = TableResource()
        is_valid, bad_names = t.index_names_valid(valid_names)
        self.assertTrue(is_valid)
        self.assertCountEqual(bad_names, [])
        is_valid, bad_names = t.index_names_valid(invalid_names)
        self.assertFalse(is_valid)
        self.assertCountEqual(bad_names, ['g/h'])

        # check that it with the expected number of hits if 
        # a long list of invalid names is provided:
        # we only report NAME_ERROR_LIMIT problems to not overwhelm
        # the frontend application. Here, we create more problems
        # and assert that we only report the first NAME_ERROR_LIMIT
        n = NAME_ERROR_LIMIT + 1
        long_list_of_invalids = [
            random.choice(string.ascii_letters)+'/' for _ in range(n)] 
        # check that this is actually invalid:  
        with self.assertRaises(StringIdentifierException):
            [normalize_identifier(x) for x in invalid_names]

        is_valid, bad_names = t.index_names_valid(long_list_of_invalids)
        self.assertFalse(is_valid)
        self.assertCountEqual(bad_names, long_list_of_invalids)

    def test_catches_parse_exception(self):
        '''
        This tests that we appropriately handle issues like
        "jagged" tables, which can occur with extra fields
        in certain rows.

        This can happen a lot of Excel files if the user
        has an empty cell on the right side of the table-
        the file is saved like it has some "empty data" there
        but the parser believes it to be a jagged table.
        '''
        t = TableResource()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'jagged_table_case1.csv'))
        is_valid, err = t.validate_type(self.r, CSV_FORMAT)
        self.assertFalse(is_valid)
        expected_err = 'Expected 7 fields in line 3, saw 8'
        self.assertTrue(expected_err in err)
 
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'jagged_table_case2.csv'))
        is_valid, err = t.validate_type(self.r, CSV_FORMAT)
        self.assertFalse(is_valid)
        expected_err = 'Expected 7 fields in line 3, saw 8'
        self.assertTrue(expected_err in err)

class TestMatrix(BaseAPITestCase):
    '''
    Tests tables where all entries must be numeric
    (float or int)
    '''
    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_reads_float_table(self):
        '''
        Capable of parsing a table of mixed numeric types
        '''
        m = Matrix()
        associate_file_with_resource(self.r, 
            os.path.join(TESTDIR, 'test_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_reads_integer_table(self):
        '''
        Tables of integers also pass validation
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_reads_table_without_gene_label(self):
        '''
        Tables with a blank first column name are OK
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_incorrect_table(self):
        '''
        Tests that a table with a string entry fails
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_incorrect_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        bad_col_str = 'SW2_Control (column 2)'
        expected_err_str = NON_NUMERIC_ERROR.format(cols=bad_col_str)
        self.assertEqual(err, expected_err_str) 

    def test_table_without_header(self):
        '''
        Tables without a header row fail
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.no_header.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NUMBERED_COLUMN_NAMES_ERROR)

    def test_table_without_rownames(self):
        '''
        Tables without row names fails
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.no_rownames.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NUMBERED_ROW_NAMES_ERROR)

    def test_duplicate_rownames_fails(self):
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix.duplicate_rownames.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NONUNIQUE_ROW_NAMES_ERROR)

    def test_reads_float_table_with_na(self):
        '''
        Capable of parsing a table containing missing
        data
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix.with_na.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_table_with_empty_rowname_fails(self):
        '''
        Tests that a table with an empty rowname fails
        '''
        m = Matrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix_with_na_rowname.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NA_ROW_NAMES_ERROR)

    def test_table_with_invalid_rownames_fails(self):
        '''
        Tests that a table with invalid row names fails
        '''
        m = Matrix()

        def _check_test_valid(r):
            # need to check that the test is not trivially passing 
            # by checking the row names for invalid entries
            m.read_resource(r, CSV_FORMAT)
            df = m.table
            rownames = df.index
            invalid_list = []
            for x in rownames:
                try:
                    normalize_identifier(x)
                except StringIdentifierException:
                    invalid_list.append(x)
            return invalid_list

        # check a file that has many errors (more than we would print to the user)
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix_with_invalid_rownames_case1.csv'))
        invalid_list = _check_test_valid(self.r)
        self.assertTrue(len(invalid_list) > NAME_ERROR_LIMIT)

        # OK, now ready to run the test if we made it this far.
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)

        self.assertFalse(is_valid)
        expected_err = 'gk/vs, gu/uo, gu/ga, gr/ol, gf/ls, and 1 other(s)'
        self.assertTrue(expected_err in err)

        # now check a file that has only several errors 
        # and we can reasonably print them all
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix_with_invalid_rownames_case2.csv'))
        invalid_list = _check_test_valid(self.r)
        self.assertTrue(len(invalid_list) <= NAME_ERROR_LIMIT)

        # OK, now ready to run the test if we made it this far.
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)

        self.assertFalse(is_valid)
        expected_err = 'gk/vs, gu/uo, gu/ga'
        self.assertTrue(expected_err in err)

class TestIntegerMatrix(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_fails_with_float_table(self):
        '''
        Capable of parsing a table of mixed numeric types
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

    def test_reads_integer_table(self):
        '''
        Tables of integers pass validation
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.csv'))
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_table_without_header(self):
        '''
        Tables without a header row fail
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.no_header.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NUMBERED_COLUMN_NAMES_ERROR)

    def test_reads_table_without_gene_label(self):
        '''
        Tables with a blank first column name are OK
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.no_gene_label.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_table_without_rownames(self):
        '''
        Tables without row names fails
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.no_rownames.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NUMBERED_ROW_NAMES_ERROR)

    def test_duplicate_rownames_fails(self):
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_matrix.duplicate_rownames.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, NONUNIQUE_ROW_NAMES_ERROR)

    def test_reads_int_table_with_na(self):
        '''
        Capable of parsing an integer table containing missing
        data.  Note that this requires some special handling
        since NaN's force the column to be parsed as a float,
        even if all other values in the column are integers
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.with_na.csv'))
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 

    def test_fails_parsing_int_table_with_na(self):
        '''
        Here, we take a NaN value which would be typically handled
        gracefully as in the TestIntegerMatrix.test_reads_int_table_with_na
        test function above.  However, we also put a non-integer in the same
        column to test that the special case handling is working properly.
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.with_na_and_float.csv'))
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)
        self.assertFalse(is_valid)

    def test_permits_negative_numbers(self):
        '''
        Test that the IntegerMatrix permits negative numbers.
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.with_negative.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)

    def test_fails_parsing_int_table_with_na_and_float(self):
        '''
        Here, we take a NaN value which would be typically handled
        gracefully as in the TestIntegerMatrix.test_reads_int_table_with_na
        test function above.  However, we also put a non-integer in a
        different column (5) to test that the special case handling is 
        working properly.
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.with_multiple_na_and_float.csv'))
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)
        self.assertFalse(is_valid)
        bad_col_str = 'SW5_Treated (column 5)'
        expected_err_str = NON_INTEGER_ERROR.format(cols=bad_col_str)
        self.assertEqual(err, expected_err_str) 

    def test_excel_parses_correctly(self):
        '''
        Test that we can parse an excel spreadsheet provided the 
        data is contained in the first sheet
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.xlsx'))
        is_valid, err = m.validate_type(self.r, 'xlsx')
        self.assertTrue(is_valid)

    def test_excel_fails_if_not_in_first_sheet(self):
        '''
        If the data is contained on a different sheet than "the first"
        the table is empty.  If the first sheet contained data, then 
        there's really nothing we can do to correct that.
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.second_sheet.xlsx'))
        is_valid, err = m.validate_type(self.r, 'xlsx')
        self.assertFalse(is_valid)
        self.assertEqual(err, EMPTY_TABLE_ERROR)

    def test_fails_if_filetype_incorrect(self):
        '''
        If a user specifies CSV but the file is, in fact,
        a TSV, we fail out.

        We are unable to decipher (without looking at the table)
        that it was due to an incorrect file extension, but the
        file still fails validation.
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_tsv_integer_matrix_labeled_as_csv.csv'))
        is_valid, err = m.validate_type(self.r, CSV_FORMAT)
        self.assertFalse(is_valid)

    def test_fails_if_filetype_incorrect_case2(self):
        '''
        If a user specifies TSV but the file is, in fact,
        a CSV, we fail out.

        We are unable to decipher (without looking at the table)
        that it was due to an incorrect file extension, but the
        file still fails validation.
        '''
        m = IntegerMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_csv_integer_matrix_labeled_as_tsv.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

class TestRnaSeqCountMatrix(BaseAPITestCase):
    '''
    Tests the specific behavior of RNA-seq count matrices
    which extend those of the integer matrix
    '''

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_validates_properly(self):
        '''
        Capable of parsing a table of integers
        '''
        m = RnaSeqCountMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        print(err)
        self.assertTrue(is_valid)

    def test_fails_with_negative(self):
        '''
        Reports an error when parsing an integer table with 
        a negative entry
        '''
        m = RnaSeqCountMatrix()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_integer_matrix.with_negative.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)

class TestAnnotationMatrix(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_table_without_header(self):
        '''
        Tables without a header row pass, but get warned
        if there are duplicate values
        '''
        t = AnnotationTable()
        associate_file_with_resource(self.r,os.path.join(
            TESTDIR, 'test_annotation.no_header.tsv') )
        is_valid, msg = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertEqual(msg, MISSING_HEADER_WARNING)

    def test_table_with_extra_columns_and_extra_row(self):
        '''
        The table we test here has extra rows and columns. Common for XLS exports.
        Should NOT fail this since it's common and would be hard for frontend
        users to diagnose
        '''
        t = AnnotationTable()
        p = os.path.join(TESTDIR, 'annotation_with_extra_cols_and_rows.csv')
        associate_file_with_resource(self.r, p)
        is_valid, msg = t.validate_type(self.r, CSV_FORMAT)
        self.assertTrue(is_valid)
        metadata = t.extract_metadata(self.r, CSV_FORMAT)

    def test_table_with_single_column_fails(self):
        '''
        Tables with only a single column fails since
        it is not useful
        '''
        t = AnnotationTable()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'single_column_annotation.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, EMPTY_TABLE_ERROR)

    def test_duplicate_rownames_fails(self):
        '''
        Cannot have more than one annotation per 'sample'
        '''
        t = AnnotationTable()
        associate_file_with_resource(self.r,
            os.path.join(TESTDIR, 'two_column_annotation.duplicate_rows.tsv'))
        is_valid, err = t.validate_type(
            self.r,
            TSV_FORMAT
        )
        self.assertFalse(is_valid)
        self.assertEqual(err, NONUNIQUE_ROW_NAMES_ERROR)

    def test_parses_proper_annotations(self):
        t = AnnotationTable()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'two_column_annotation.tsv'))
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err)

    def test_parses_compliant_table_with_uncompliant_attribute(self):
        '''
        In this test, we have the case where a user submits an annotation file
        which has a column of strings. One of those strings, however, is entirely
        numeric. That is accepted, however, as there are situations where annotations
        could be presented in this manner.
        '''
        t = AnnotationTable()
        p = os.path.join(
            TESTDIR, 'test_annotation_with_noncompliant_str.tsv')
        associate_file_with_resource(self.r, p)
        is_valid, err = t.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err)
        metadata = t.extract_metadata(self.r, TSV_FORMAT)

class TestBed3(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_bed_with_header_fails(self):
        '''
        Technically, BED format does not allow a header.  
        Since BED files may feed into downstream processes, we 
        reject these malformatted BED files
        '''
        b = BED3File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed_with_header.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertEqual(err, BED_FORMAT_ERROR.format(cols='2,3'))


    def test_bed_with_extra_fields_passes(self):
        '''
        This allows some of the extended BED formats.
        '''
        b = BED3File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'five_column.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)

    def test_save_in_standard_format(self):
        b = BED3File()
        example_file_path = os.path.join(
            TESTDIR, 'example_bed.bed')
        associate_file_with_resource(self.r, example_file_path)
        df1 = pd.read_table(self.r.datafile.open(), header=None)
        df2 = pd.read_table(example_file_path, header=None)
        self.assertTrue(df1.equals(df2))


class TestBed6(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_bed3_fails(self):
        '''
        If we attempt to parse a BED3 file using a BED6 parser, we fail
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'example_bed.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('found 3' in err)

    def test_bed6_with_format_error_fails_case1(self):
        '''
        In this test, there is an error in the first 3-columns
        (a 'stop' value is a string). Ensure we report failure
        properly
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_malformatted_case1.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('column(s): 3' in err)

    def test_bed6_score_exceeds_max_fails(self):
        '''
        In this test, one of the scores exceeds the BED
        max of 1000
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_malformatted_case2.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('[100,1002]' in err)

    def test_bed6_score_below_min_fails(self):
        '''
        In this test, one of the scores is below the BED
        min of 0
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_malformatted_case3.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('[-10,100]' in err)

    def test_bed6_score_non_integer(self):
        '''
        In this test, one of the scores is not an int
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_malformatted_case4.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('Please check that the 5th column ' in err)

    def test_bed6_score_col_empty_fails(self):
        '''
        In this test, the score column is empty. Fail it.
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_malformatted_case5.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('one or more "NA" values' in err)

    def test_bed6_invalid_strand_val_fails(self):
        '''
        In this test, the strand column contains an 
        unacceptable value of 'x'. Fail it.
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_malformatted_case6.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('6th column' in err)
        self.assertTrue('x' in err)

    def test_bed6_passes(self):
        '''
        In this test, everything should be fine.
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_example.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)

    def test_bed6_with_multiple_strand_values_passes(self):
        '''
        In this test, the strand column contains all the potential
        acceptable values for strand. Should pass.
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_example2.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)

    def test_bed6_metadata(self):
        '''
        Populates emtpy metadata dict
        '''
        b = BED6File()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_example.bed'))
        metadata = b.extract_metadata(self.r)
        self.assertTrue(all([metadata[k] is None for k in metadata]))

    def test_save_in_standard_format(self):
        b = BED6File()
        example_file_path = os.path.join(
            TESTDIR, 'bed6_example.bed')
        associate_file_with_resource(self.r, example_file_path)
        df1 = pd.read_table(self.r.datafile.open(), header=None)
        df2 = pd.read_table(example_file_path, header=None)
        self.assertTrue(df1.equals(df2))

class TestNarrowPeak(BaseAPITestCase):

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_bed3_fails(self):
        '''
        If we attempt to parse a BED3 file using a narrowpeak parser, we fail
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'example_bed.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('found 3' in err)

    def test_bed6_fails(self):
        '''
        If we attempt to parse a BED6 file using a narrowpeak parser, we fail
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'bed6_example.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('found 6' in err)

    def test_success(self):
        '''
        Test that we correctly parse a valid narrowpeak file.
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_example.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)

    def test_narrowpeak_malformatted_fails_case1(self):
        '''
        Test problems with signal column (7) are raised when it's
        not a number
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case1.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('only numbers' in err)

    def test_narrowpeak_malformatted_fails_case2(self):
        '''
        Test problem with p/q-value column.

        Specifically, a string is found
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case2.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('Non-numerical values were found' in err)

    def test_narrowpeak_malformatted_fails_case3(self):
        '''
        Test problem with p/q-value column.

        Specifically, a negative value is found that is not
        -1 (the 'null' value)
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case3.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('negative values other than -1' in err)

    def test_narrowpeak_malformatted_fails_case4(self):
        '''
        Test problem with peak col (10).

        Specifically, the column has floats, which does not make
        sense since it's a 0-based offset (or -1)
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case4.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('permits only integers' in err)

    def test_narrowpeak_malformatted_fails_case5(self):
        '''
        Test problem with peak col (10).

        Specifically, the column has a single negative value other
        than -1
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case5.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('Found a value of -2' in err)

    def test_narrowpeak_malformatted_fails_case6(self):
        '''
        Test problem with peak col (10).

        Specifically, the column has negative values other
        than -1
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case6.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('Found values of -3,-1' in err)

    def test_narrowpeak_malformatted_fails_case7(self):
        '''
        Here, one of the columns is delimted by a space instead of a tab.
        Assert that we flag this as a problem
        '''
        b = NarrowPeakFile()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'narrowpeak_malformatted_case7.bed'))
        is_valid, err = b.validate_type(self.r, TSV_FORMAT)
        self.assertFalse(is_valid)
        self.assertTrue('one or more "NA" values' in err)

    def test_save_in_standard_format(self):
        b = NarrowPeakFile()
        example_file_path = os.path.join(
            TESTDIR, 'narrowpeak_example.bed')
        associate_file_with_resource(self.r, example_file_path)
        df1 = pd.read_table(self.r.datafile.open(), header=None)
        df2 = pd.read_table(example_file_path, header=None)
        self.assertTrue(df1.equals(df2))

class TestNetworkStructure(BaseAPITestCase):
    '''
    Specific tests for network data structures. Since these mirror
    many of the specific tests we perform on `Matrix` types above, 
    this test suite is pretty sparse.

    We do, however, have a specific test file for a network as a double-check.
    '''

    def setUp(self):
        self.establish_clients()
        self.r = Resource.objects.create(
            owner=self.regular_user_1,
            file_format='',
            resource_type='',
            datafile=File(BytesIO(), 'foo.tsv')
        )

    def test_validates_network_file(self):
        m = Network()
        associate_file_with_resource(self.r, os.path.join(
            TESTDIR, 'test_network_file.tsv'))
        is_valid, err = m.validate_type(self.r, TSV_FORMAT)
        self.assertTrue(is_valid)
        self.assertIsNone(err) 
