# This file contains information about the different table-
# based file types and methods for validating them
import logging
import re
import uuid
from functools import reduce
from io import BytesIO
from collections import OrderedDict

import pandas as pd
import numpy as np

from django.conf import settings
from django.core.paginator import Paginator, Page
from rest_framework.pagination import PageNumberPagination

from constants import CSV_FORMAT, \
    TSV_FORMAT, \
    XLS_FORMAT, \
    XLSX_FORMAT, \
    OBSERVATION_SET_KEY, \
    PARENT_OP_KEY, \
    POSITIVE_INF_MARKER, \
    NEGATIVE_INF_MARKER

from exceptions import ParseException, \
    FileParseException, \
    UnexpectedFileParseException, \
    UnexpectedTypeValidationException, \
    ParserNotFoundException, \
    StringIdentifierException

from helpers import normalize_identifier

from .base import DataResource

from data_structures.helpers import convert_dtype
from data_structures.observation import Observation
from data_structures.observation_set import ObservationSet
from data_structures.simple_attribute_factory import SimpleAttributeFactory

from api.utilities.admin_utils import alert_admins

logger = logging.getLogger(__name__)


# Some error messages:
PARSE_ERROR = ('There was an unexpected problem when'
    ' parsing and validating the file.')

SPECIFIC_PARSE_ERROR = ('There was an error when parsing your file. This can often happen if'
    ' the incorrect format was selected. Other problems can occur from extra blank cells in'
    ' spreadsheets. Please double-check your file format and try again.'
    ' The specific error report was: {ex}')

PARSER_NOT_FOUND_ERROR = ('Could not find an appropriate parser'
    ' for the resource.  Please check the instructions.')

NON_NUMERIC_ERROR = 'Some of your columns contained non-numeric entries. Please check the file.'

NON_INTEGER_ERROR = 'Some of your columns contained non-integer entries. Please check the file.'

NEGATIVE_COUNTS_ERROR = ('Some of your entries were negative and this is not'
    ' permitted for this resource type. Please check the file.')

TRIVIAL_TABLE_ERROR = ('The file contained only a single column'
    ' which provided an index.  No data was provided in additional columns.')

BED_FORMAT_ERROR = ('When parsing the BED file, we detected issues with'
    ' column(s): {cols}. Note that BED files must NOT have column headers and can'
    ' contain only integers in the second and third columns, which correspond'
    ' to the start and end of a genomic location.  Please check your entries and'
    ' ensure your file does not have a header line.')

NUMBERED_COLUMN_NAMES_ERROR = ('All the column names were numbers.  Often this is due'
    ' to a missing column header.  In that case, there will be a missing row in'
    ' your table. If you named your columns with numbers, please change them'
    ' to something else to avoid incorrect parsing of the file.')

NUMBERED_ROW_NAMES_ERROR = ('All the row names were numbers.  We use the first column'
    ' to uniquely identify the rows for filtering purposes. If you named your rows with'
    ' numbers, please change them to something else (e.g. add "x" to the beginning'
    ' to avoid incorrect parsing of the file.')

NONUNIQUE_ROW_NAMES_ERROR = ('Your row names were not unique, which could cause'
    ' unexpected behavior.')

NA_ROW_NAMES_ERROR = ('One or more of your row names (items in the first column) was empty'
    ' or contained invalid values. If you wish to keep the row, simply provide any unique'
    ' value so that it\'s not empty.')

MISSING_HEADER_WARNING = ('One of your column names matched the values in the'
    ' corresponding problem.  This is not an error, but may indicate that a'
    ' proper header line was missing.  Please check to ensure the file was'
    ' parsed correctly.')

EMPTY_TABLE_ERROR = ('The parsed table was empty. This can happen if you are'
    ' importing a table with only a single column. Alternatively, if you are'
    ' trying to import an Excel spreadsheet, please ensure that the data is'
    ' contained in the first sheet of the workbook.')

# If a file has row/column identifiers that are invalid, we would like to let 
# them know which ones need fixing. However, we don't want to report an
# exceptionally long list of issues. This sets the maximum number before
# the name checking gives up and reports the error
NAME_ERROR_LIMIT = 5

NAMING_ERROR = ('The {idx}s of your table contained identifiers with'
    ' characters that we do not permit. Since we depend on software'
    ' tools that may not support these characters, we only allow'
    ' letters A-Z, numbers 0-9, dots (.), dashes (-), and underscores (_).'
    ' Problems include {bad_identifiers}{suffix}.')

EXCEL_DATETIME_ERROR = ('A datetime ({x}) was found when parsing your file.'
                        ' This is a common issue for certain gene names'
                        ' (such as MARCH1) when the file has been opened'
                        ' or edited with Excel. Try exporting your file'
                        ' as CSV to avoid this.')

# If requesting a preview of the table, how many lines do we return?
PREVIEW_NUM_LINES = 5

def col_str_formatter(x):
    '''
    x is a tuple with the column number
    and column name
    '''
    return '%s (column %d)' % (x[0],x[1])


class TableResourcePage(Page):
    '''
    Overrides some methods of the django.core.paginator.Page
    class. The default version does a cast to a list, which is a problem
    when the "query set" is a dataframe instead of a QuerySet instance
    '''
    def __getitem__(self, index):
        if not isinstance(index, (int, slice)):
            raise TypeError(
                'Page indices must be integers or slices, not %s.'
                % type(index).__name__
            )     
        return self.object_list.iloc[index]


class TableResourcePaginator(Paginator):
    '''
    This overrides the default Django paginator, which
    typically expects to deal with a database query (a QuerySet).

    Dataframes are list-like (sort of...) but the problem is that
    the default django paginator ultimately does a cast to a list. When you 
    cast a pandas dataframe as a list, you only get a list of the
    column names, NOT a list of the "records" (rows).

    The django default Paginator mentions using the _get_page method as a
    way to override the django.core.paginator.Page class (which is where the
    cast occurs). By overriding that method, we can make our own Page-like
    class. 

    Finally, this class can be provided to a subclass of DRF's Paginator
    classes. 
    '''
    def _get_page(self, *args, **kwargs):
        '''
        Method to override the default behavior of the django core
        Paginator class. Should return a Page-like class. 
        '''
        return TableResourcePage(*args, **kwargs)


class TableResourcePageNumberPagination(PageNumberPagination):
    django_paginator_class = TableResourcePaginator
    page_size_query_param = settings.PAGE_SIZE_PARAM


class TableResource(DataResource):
    '''
    The `TableResource` is the most generic form of a delimited file.  Any
    type of data that can be represented as rows and columns.

    This or any of the more specific subclasses can be contained in files
    saved in CSV, TSV, or Excel (xls/xlsx) format.  If in Excel format, the 
    data of interest must reside in the first sheet of the workbook.

    Note that unless you create a "specialized" implementation (e.g. like
    for a BED file), then we assume you have features as rows and observables
    as columns.
    '''
    ACCEPTABLE_FORMATS = [
        CSV_FORMAT,
        TSV_FORMAT,
        XLS_FORMAT,
        XLSX_FORMAT
    ]

    # the "standardized" format we will save all table-based files as:
    STANDARD_FORMAT = TSV_FORMAT

    # Create a list of query params that are "reserved" and we ignore when
    # attempting to filter on the actual table content (e.g. the column/rows)
    IGNORED_QUERY_PARAMS = [settings.PAGE_SIZE_PARAM, settings.PAGE_PARAM, settings.SORT_PARAM]

    def __init__(self):
        self.table = None

    @staticmethod
    def get_paginator():
        return TableResourcePageNumberPagination()

    @staticmethod
    def get_reader(file_format):
        '''
        By using the file extension, we infer the delimiter
        Returns a pandas "reader" (e.g. `read_csv` or `read_table`)
        '''
        file_format = file_format.lower()
        if file_format == CSV_FORMAT:
            return pd.read_csv
        elif file_format == TSV_FORMAT:
            return pd.read_table
        elif file_format in [XLS_FORMAT, XLSX_FORMAT]:
            return pd.read_excel
        else:
            logger.error(f'Unrecognized file format: {file_format}')
            return None

    @staticmethod
    def index_all_numbers(names):
        '''
        Works for both row and column indexes.  Returns
        True if all the index labels are numbers.  
        '''
        try:
            # if this comprehension succeeds, then all the column headers
            # or row names were able to be parsed as numbers.
            for x in names:
                try:
                    float(x)
                except TypeError as ex:
                    if 'datetime.datetime' in str(ex):
                        raise TypeError(EXCEL_DATETIME_ERROR.format(x=x))
                    raise Exception(f'Unexpected TypeError raised {ex}')
            return True
        except ValueError:
            return False

    @staticmethod
    def index_names_valid(names):
        '''
        We check the identifiers (row or column names) to ensure 
        we do not run into any unicode, etc. errors with downstream tools.
        Since we cannot control how those tools will treat non-ascii,
        we restrict the available characters for naming rows and columns  
        
        Works for both row and column indexes.  Returns
        True if all the index labels are valid. 
        '''
        bad_names = []
        for x in names:
            try:
                normalize_identifier(x)
            except StringIdentifierException:
                bad_names.append(x)

        if len(bad_names) == 0:
            return (True, [])
        else:
            return (False, bad_names)

    def read_resource(self, resource_instance, requested_file_format=None, preview=False):
        '''
        One common spot to define how the file is read.

        Note that validated files "know" their file format.
        The `requested_file_format` kwarg is used when we are attempting
        a read on an unvalidated file.
        '''
        if requested_file_format is None:
            file_format = resource_instance.file_format
        else:
            file_format = requested_file_format

        reader = TableResource.get_reader(file_format)
        if reader is None:
            raise ParserNotFoundException('')
        else:
            try:
                # read the table using the appropriate parser:
                if preview:
                    nrows = PREVIEW_NUM_LINES
                else:
                    nrows = None

                self.table = reader(
                    resource_instance.datafile.open(),
                    index_col=0, comment='#', nrows=nrows)

                # drop extra/empty cols and rows
                self.table.dropna(axis=0, how='all', inplace=True)
                self.table.dropna(axis=1, how='all', inplace=True)

            except pd.errors.ParserError as ex:
                logger.info('Pandas parser exception raised.')
                raise FileParseException(str(ex))

            except Exception as ex:
                logger.info(f'Could not use {reader} to parse the resource'
                f' with pk={resource_instance.pk}')     
                raise UnexpectedFileParseException('Failed when parsing'
                    ' the table-based resource.')

    def performs_validation(self):
        '''
        Since we have methods to validate table-based DataResource types, we 
        override this method and return True, which indicates that we CAN
        perform validation.
        '''
        return True

    def validate_type(self, resource_instance, file_format):
        '''
        In this base method, we determine attempt to parse the file.
        If there are no restrictions on content, succesful parsing
        of the file is good enough.

        More specific constraints on the file content are handled in child
        classes.  This method, however, fills in the `self.table` member
        which is then accessible to children.
        '''
        try:
            self.read_resource(resource_instance, requested_file_format=file_format)
            if self.table.shape == (0,0):
                return (False, EMPTY_TABLE_ERROR )

            if self.table.shape[1] == 0:
                return (False, TRIVIAL_TABLE_ERROR)

            # check if all the column or row names are numbers.
            # For the columns, this would USUALLY
            # indicate a missing header. For the rows, we 
            #  assume it to be a data column or just a dummy index 
            # which is not permitted.
            index_dict = {
                'column': self.table.columns,
                'row': self.table.index
            }
            for key, values in index_dict.items():
                try:
                    all_numbers = TableResource.index_all_numbers(values)
                    if all_numbers:
                        if key == 'column':
                            return (False, NUMBERED_COLUMN_NAMES_ERROR)
                        else:
                            return (False, NUMBERED_ROW_NAMES_ERROR)
                except TypeError as ex:
                    return  (False, str(ex))
                except Exception:
                    return  (False, PARSE_ERROR)

            # check that all the row names were valid (e.g. not NAs)
            if pd.isnull(self.table.index).any():
                return (False, NA_ROW_NAMES_ERROR)

            # check for duplicate row names
            if self.table.index.has_duplicates:
                return (False, NONUNIQUE_ROW_NAMES_ERROR)

            # check that the column names are "valid"
            for key, values in index_dict.items():
                all_valid, bad_names = TableResource.index_names_valid(values)
                if not all_valid:
                    if len(bad_names) > NAME_ERROR_LIMIT:
                        suffix = f', and {len(bad_names) - NAME_ERROR_LIMIT} other(s)'
                    else:
                        suffix = ''
                    return (False, NAMING_ERROR.format(
                        idx=key,
                        bad_identifiers=', '.join([str(x) for x in bad_names[:NAME_ERROR_LIMIT]]),
                        suffix=suffix))

            # passed the basic checks-- looks good so far. Derived classes
            # can apply more specific checks.
            return (True, None)

        except ParserNotFoundException as ex:
            return (False, PARSER_NOT_FOUND_ERROR)

        except FileParseException as ex:
            msg = str(ex)
            return (False, SPECIFIC_PARSE_ERROR.format(ex = msg))

        except UnexpectedFileParseException as ex:
            return (False, PARSE_ERROR)
     
    def do_type_cast(self, v, dtype):
        '''
        Used for casting the type when query params are provided.
        '''
        if re.fullmatch('(int|float)\d{0,2}', dtype):
            try:
                val = float(v)
            except ValueError as ex:
                raise ParseException(f'Could not parse "{v}" as a number.')
        else:
            val = v 
        return val

    def perform_sorting(self, query_params):
        '''
        Sorts the table as requested. Sorts self.table in place

        A general sort string (after the equals sign) would be:
        <sort kw>:<col>,<sort kw>:<col>,...
        e.g. for sorting q-value (padj) ascending followed by fold-change descending, 
        [asc]:padj,[desc]:log2Foldchange
        '''
        if settings.SORT_PARAM in query_params:
            sort_strings = query_params[settings.SORT_PARAM].split(',')
            sort_order_list = []
            column_list = []
            for s in sort_strings:
                try:
                    sort_order, col = s.split(settings.QUERY_PARAM_DELIMITER)
                except ValueError:
                    raise ParseException('The sorting request was not properly formatted. '
                        'Please use "<ordering keyword>:<column name>"')
                if sort_order in settings.SORTING_OPTIONS:
                    sort_order_list.append(sort_order)
                else:
                    raise ParseException(f'The sort order "{sort_order}" is'
                        ' not an available option. Choose from:'
                        f' {",".join(settings.SORTING_OPTIONS)}')

                if col in self.table.columns:
                    column_list.append(col)
                else:
                    raise ParseException(f'The column identifier "{col}" does'
                        ' not exist in this resource. Options are:'
                        f' {",".join(self.table.columns)}')
            # at this point, all the sort orders and cols were OK. Now perform the sorting:
            # Need to convert our strings (e.g. "[asc]") to bools for the pandas sort_values method.
            order_bool = [True if x==settings.ASCENDING else False for x in sort_order_list]                
            self.table.sort_values(by=column_list, ascending=order_bool, inplace=True)

    def filter_against_query_params(self, query_params):
        '''
        Looks through the query params to subset the table
        '''
        table_cols = self.table.columns

        # guard against some edge case where the table we are filtering happens to have 
        # columns that conflict with the pagination parameters. We simply inform the admins
        # and ignore that conflict by not using that filter

        if any([x in self.IGNORED_QUERY_PARAMS for x in table_cols]):
            logger.warning('One of the column names conflicted with the'
                ' pagination query params.')
            alert_admins('Edge-case error: when filtering on a column,'
                ' one of the column names conflicted with the pagination query'
                f' params. Query params was: {query_params} and column'
                f' names were: {",".join(table_cols)}')
        filters = []

        for k,v in query_params.items():
            split_v = v.split(settings.QUERY_PARAM_DELIMITER)
            if (not k in self.IGNORED_QUERY_PARAMS) and (k in table_cols):
                # v is either a value (in the case of strict equality)
                # or a delimited string which will dictate the comparison.
                # For example, to filter on the 'pval' column for values less than or equal to 0.01, 
                # v would be "[lte]:0.01". The "[lte]" string is set in our general settings file.

                # self.table.dtypes gives a pd.Series which maps the column names to numpy types like
                # int64, float64, object
                column_type = str(self.table.dtypes[k]) # a string like int64, object
                if len(split_v) == 1:
                    # strict equality
                    val = self.do_type_cast(v, column_type)
                    try:
                        filters.append(self.table[k] == val)
                    except Exception as ex:
                        logger.error('Encountered exception!!')
                elif len(split_v) == 2:
                    val = self.do_type_cast(split_v[1], column_type)
                    try:
                        op = settings.OPERATOR_MAPPING[split_v[0]]
                    except KeyError as ex:
                        raise ParseException(f'The operator string'
                            f' ("{split_v[0]}") was not understood. Choose'
                            ' from among:'
                            f' {",".join(settings.OPERATOR_MAPPING.keys())}')
                    filters.append(
                        self.table[k].apply(lambda x: op(x, val))
                    )
                else:
                    raise ParseException(f'The query param string ({v}) for'
                        f' filtering on the {k} column was not'
                        ' formatted properly.')
            elif k == settings.ROWNAME_FILTER:
                if len(split_v) != 2:
                    raise ParseException('The query for filtering on the rows'
                        ' was not properly formatted. It should be [<op>]:<value>')
                # we don't allow indexes that are all numbers, so don't worry about casting
                # the filter value from a string
                val = split_v[1]
                try:
                    op = settings.OPERATOR_MAPPING[split_v[0]]
                except KeyError as ex:
                    raise ParseException('The operator string'
                        f' ("{split_v[0]}") was not understood. Choose'
                        ' from among:'
                        f' {",".join(settings.OPERATOR_MAPPING.keys())}')
                try:
                    rowname_filter = self.table.index.to_series().apply(lambda x: op(x, val))
                    filters.append(rowname_filter)
                except Exception as ex:
                    alert_admins('Error when attempting to perform a row'
                        f' filter. Exception was: {ex}')
                    raise ParseException('Error encountered with filter on rows.'
                        ' Admin has been notified.'
                    )
            elif k in self.IGNORED_QUERY_PARAMS:
                # need to have this here or else the 'ignored' query params
                # are treated as unknown columns to sort on in the `else` statement
                # below
                pass
            else:
                raise ParseException(f'The column "{k}" is'
                    ' not available for filtering.')
        if len(filters) > 1:
            combined_filter = reduce(lambda x,y: x & y, filters)
            self.table = self.table.loc[combined_filter]
        elif len(filters) == 1:
            self.table = self.table.loc[filters[0]]

    def get_type_dict(self, **kwargs):
        '''
        Gets a mapping from the pandas native dtype to a MEV-compatible
        type.
        '''
        type_dict = {}
        for c in self.table.dtypes.index:
            # the convert_dtype function takes the native pandas dtype
            # and returns an attribute "type" that MEV understands.
            type_dict[c] = convert_dtype(str(self.table.dtypes[c]), **kwargs)
        return type_dict

    @staticmethod
    def replace_special_values(df):
        '''
        NaN and Inf values cause issues when we are serializing into JSON. If we have 
        an action/event that requires serialization of the table-based resource, then we
        should use this method to safely convert those values

        If there is any filtering involved (e.g. filtering for values greater than x)
        ensure you don't call this method first, as it will replace infinity values with
        strings and the filter won't work properly.
        '''
        return df.replace({
            -np.infty: NEGATIVE_INF_MARKER,
            np.infty: POSITIVE_INF_MARKER,
            np.nan: None
        })

    def _resource_specific_modifications(self):
        '''
        This is a hook where derived classes can implement
        special filtering/behavior/etc. (if necessary)
        when the resource contents are requested.  

        Called by the `get_contents` method.
        '''
        pass

    def to_json(self, content):
        '''
        Takes the content (either a pd.DataFrame or list of pd.Series)
        and prepares a JSON-compatible data structure. 
        This includes removing any out-of-bounds content
        such as NaN (not supported by JSON standard).

        Note that this simply does not simply use self.table since
        there may have been some alteration to the content we are
        returning over the API. For example, in a paginated request
        the paginator selects out the records to return- we only
        want to convert that content, NOT the whole of the dataframe
        contained in self.table

        Note that `standard_cols` is a list of the columns that are
        located in the original dataframe (as when loaded from a file). 

        additional_cols are 'extra' columns that we might add to 'supplement'
        the original table contents. An example might be a row-mean for a 
        numerical matrix. Those columns aren't part of the original table so 
        we want a way to keep them 'separate' in the returned data payload
        '''
        if type(content) is pd.DataFrame:
            logger.info('Content was a dataframe. Convert to JSON')
            df = content
        elif type(content) is list:
            try:
                logger.info('Content was a list. Convert to JSON')
                df = pd.DataFrame(content)
            except Exception as ex:
                logger.error('Failed to convert to pd.DataFrame.'
                    f' The reason was {ex}')
                alert_admins('Failed to convert list-based contents to JSON'
                    ' for a table-based resource.')
                raise ex
        else:
            alert_admins('Failed to convert contents to JSON for a'
                ' table-based resource. Unexpected content.')
            raise Exception('The to_json method expects a dataframe or a list of'
                ' pd.Series which can be coerced into a pd.DataFrame.')

        if len(self.additional_exported_cols) > 0:
            standard_cols = [x for x in df.columns if not x in self.additional_exported_cols]
        else:
            standard_cols = df.columns

        def standard_row_converter(row):
            '''
            This function converts the row of a dataframe to a dict.
            It only converts the 'standard' columns which were part of
            the original data (i.e. the data stored in a flat file).  
            We keep the original data separate by
            nesting inside a 'values' key.
            '''
            return {'rowname': row.name, 'values': row.to_dict(OrderedDict)}

        def additional_row_converter(row):
            '''
            The additional content can be things like calculated row means, etc.
            which are not part of the original data. Hence, those get added into
            their own fields, NOT part of the 'values' key.
            '''
            d = {'rowname': row.name}
            d.update(row.to_dict(OrderedDict))
            return d

        df = TableResource.replace_special_values(df)
        if df.shape[0] > 0:
            content = df[standard_cols].apply(standard_row_converter, axis=1).tolist()
            if len(self.additional_exported_cols) > 0:
                additional_content = df[self.additional_exported_cols].apply(
                    additional_row_converter, axis=1).tolist()
                for x,y in zip(content, additional_content):
                    x.update(y)
            return content
        else:
            return []

    def get_contents(self, resource_instance, query_params={}, preview=False):
        '''
        Returns a dataframe of the table contents

        The dataframe allows the caller to subset as needed to 'paginate'
        the rows of the table
        '''
        try:
            logger.info(f'Read resource ({resource_instance.pk})')
            self.read_resource(resource_instance, preview=preview)
            self.additional_exported_cols = []

            # if there were any filtering params requested, apply those
            self.filter_against_query_params(query_params)

            # permits class-specific (i.e. implemented in child class)
            # behavior, such as calculating row-wise means, which 
            # can be made available on numeric tables, for instance.
            self._resource_specific_modifications()

            self.perform_sorting(query_params)

            return self.table

        # for these first two exceptions, we already have logged
        # any problems when we called the `read_resource` method
        except ParserNotFoundException as ex:
            raise ex
        except ParseException as ex:
            raise ex
        # catch any other types of exceptions that we did not anticipate.
        except Exception as ex:
            logger.error('An unexpected error occurred when preparing'
                f' a resource preview for resource ({resource_instance.pk}).'
                f' Exception was: {ex}')
            raise ex

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        '''
        This method extracts metadata from the Resource in question and 
        saves it to the database.

        In the case of new Resources being added, the `parent_op` is None
        since no MEV-based analyses were responsible for the creation of the 
        Resource.  If the Resource is created by some MEV-based analysis,
        the primary-key for that ExecutedOperation will be passed.

        '''
        logger.info('Extracting metadata from resource'
            f' ({resource_instance.pk})')

        # If the self.table field was not already filled, we need to 
        # read the data
        if self.table is None:
            logger.info(f'Resource ({resource_instance.pk}) was not '
                'previously parsed.  Do that now.')
            # if we are extracting metadata, then the file should have been deemed "valid"
            # and hence saved into the standardized format.
            is_valid, message = self.validate_type(resource_instance, self.STANDARD_FORMAT)
            if not is_valid:
                raise UnexpectedTypeValidationException(message)
        else:
            logger.info(f'Resource ({resource_instance.pk}) was '
                'previously parsed.')        
            
        # now we have a table loaded at self.table.  

        # call the super method to initialize the self.metadata
        # dictionary
        logger.info('Pre-populate/setup empty metadata.')
        super().setup_metadata()
        logger.info('Done with setting up the metadata.')
        # now add the information to self.metadata:
        if parent_op_pk:
            self.metadata[PARENT_OP_KEY] = parent_op_pk

    def save_in_standardized_format(self, resource_instance, current_file_format):
        '''
        To avoid all the analyses having to concern themselves with data formats
        like csv, tsv, xlsx, etc. we just save table-based formats in a standard
        format, determind by self.STANDARD_FORMAT

        Note that `current_file_format` is the format BEFORE we have standardized.
        This can obviously match our standard format

        Note that even if the data was already in the standard format, we STILL
        go through the process of writing a new resource. This is because we can
        guarantee the file has been appropriately cleaned. For instance, if an entire
        row of NA (blank) values is submitted, we have already removed that. Edge cases
        like that will often cause problem for downstream tools, so we remove those rows.
        '''
        logger.info(f'Saving resource ({resource_instance.pk}) to the standard'
            ' format for a table-based resource. The original name'
            f' was: {resource_instance.datafile.name}')

        # If the self.table field was not already filled, we need to 
        # read the data
        if self.table is None:
            logger.info(f'Resource ({resource_instance.pk}) was not '
                'previously parsed.  Do that now.')
            try:
                # this call will set the self.table member
                self.read_resource(resource_instance, 
                    requested_file_format=current_file_format)
            except Exception as ex:
                logger.error('Failed when trying to save resource'
                    f' ({resource_instance.pk}) in standard format.'
                    ' Specifically, failed when reading'
                    '  the resource, as self.table was None.')
                raise ex

        # ok, self.table is set-- save it.
        # Recall that our storage mechanism will place the file
        # into a user-associated directory. We only need the basename
        new_path = str(uuid.uuid4())
        with BytesIO() as fh:
            self.table.to_csv(fh, sep='\t')
            resource_instance.write_to_file(fh, new_path)


class Matrix(TableResource):
    '''
    A `Matrix` is a delimited table-based file that has only numeric types.
    These types can be mixed, like floats and integers
    '''
    ACCEPTABLE_FORMATS = [
        CSV_FORMAT,
        TSV_FORMAT,
        XLS_FORMAT,
        XLSX_FORMAT
    ]

    DESCRIPTION = 'A table of where all the entries are numbers'\
        ' except the first column (which names the rows) and the' \
        ' first line (which gives the column names). The cell at the' \
        ' first row and column may be left blank.' 

    EXAMPLE = [
        {
            "rowname":"gene1",
            "values": {
                "sampleA": 1.1,
                "sampleB": 2.2,
                "sampleC": 3.3,
            }
        },
        {
            "rowname":"gene2",
            "values": {
                "sampleA": 11.1,
                "sampleB": 22.2,
                "sampleC": 33.3,
            }
        },
        {
            "rowname":"gene3",
            "values": {
                "sampleA": 111.1,
                "sampleB": 222.2,
                "sampleC": 333.3,
            }
        }
    ]

    # looking for integers OR floats.  Both are acceptable  
    TARGET_PATTERN = '(float|int)\d{0,2}'

    # Define some additional filtering/sorting params that
    # are specific to this type and its children.
    # A special filter for allowing filtering of numeric tables based on
    # the row means
    ROWMEAN_KEYWORD = '__rowmean__'
    INCLUDE_ROWMEANS = '__incl_rowmeans__'
    EXTRA_MATRIX_QUERY_PARAMS = [
        ROWMEAN_KEYWORD,
        INCLUDE_ROWMEANS
    ]

    # Copy the ignored params from the parent (don't want to modify that)
    IGNORED_QUERY_PARAMS = [x for x in TableResource.IGNORED_QUERY_PARAMS]
    IGNORED_QUERY_PARAMS.extend(EXTRA_MATRIX_QUERY_PARAMS)

    def check_column_types(self, target_pattern):
        '''
        Checks each column against a specific numpy/pandas dtype.
        The specific dtype comes from the class member.
        '''
        problem_columns = []
        for i,col in enumerate(self.table.dtypes):
            if not re.match(target_pattern, str(col)):
                colname = self.table.columns[i]
                problem_columns.append(
                    (colname, i+1)
                )
        return problem_columns


    def validate_type(self, resource_instance, file_format):
        is_valid, error_msg = super().validate_type(resource_instance, file_format)
        if not is_valid:
            return (False, error_msg)

        # was able to at least open/parse the file.
        # now check for numeric types
        problem_columns = self.check_column_types(Matrix.TARGET_PATTERN)

        if len(problem_columns) > 0:
            error_message = NON_NUMERIC_ERROR
            return (False, error_message)

        return (True, None)

    def extract_metadata(self, resource_instance, parent_op_pk=None):

        super().extract_metadata(resource_instance, parent_op_pk=parent_op_pk)

        # Note: removed the addition of FeatureSets to the metadata as it was causing
        # issues with large json objects being inserted into the database.
        # the FeatureSet comes from the rows:
        # f_set = FeatureSet([Feature(x) for x in self.table.index])
        # self.metadata[DataResource.FEATURE_SET] = FeatureSetSerializer(f_set).data

        # the ObservationSet comes from the cols:
        o_set = ObservationSet({
            'elements': [{'id': x} for x in self.table.columns]
        })
        self.metadata[OBSERVATION_SET_KEY] = o_set.to_simple_dict()
        return self.metadata

    def _resource_specific_modifications(self):
        if not self.extra_query_params:
            return

        filters = []
        if self.ROWMEAN_KEYWORD in self.extra_query_params:
            self.additional_exported_cols.append(self.ROWMEAN_KEYWORD)
            self.table[self.ROWMEAN_KEYWORD] = self.table.mean(axis=1)
            filter_string = self.extra_query_params[self.ROWMEAN_KEYWORD]
            split_str = filter_string.split(settings.QUERY_PARAM_DELIMITER)
            if len(split_str) == 1:
                # strict equality
                try:
                    val = float(split_str[0])
                except ValueError as ex:
                    raise ParseException('Could not parse the request'
                        ' for filtering on the row means. The value'
                        ' could not be interpreted as a number.'
                    )

                filters.append(self.table[self.ROWMEAN_KEYWORD] == val)

            elif len(split_str) == 2:
                try:
                    val = float(split_str[1])
                except ValueError as ex:
                    raise ParseException('Could not parse the request'
                        ' for filtering on the row means. The value'
                        ' could not be interpreted as a number.'
                    )
                try:
                    op = settings.OPERATOR_MAPPING[split_str[0]]
                except KeyError as ex:
                    raise ParseException('The operator string'
                        f' ("{split_str[0]}") was not understood. Choose'
                        ' from among:'
                        f' {",".join(settings.OPERATOR_MAPPING.keys())}')
                filters.append(self.table[self.ROWMEAN_KEYWORD].apply(
                    lambda x: op(x, val)))
            else:
                raise ParseException(f'The query param string ({filter_string})'
                    ' for filtering on the mean values was not'
                    ' formatted properly.')

        if self.INCLUDE_ROWMEANS in self.extra_query_params:
            # if the rowmean filter was also applied, we can skip-- 
            # This query param is requesting the row means, which is inferred by the rowmean filter
            if not self.ROWMEAN_KEYWORD in self.extra_query_params:
                # if here, no filtering was requested on the row means. Simply calculate.
                # Note that we use the rowmean_keyword as the column name and in the 
                # export columns list so that the final converter knows to send this column
                # in the response.
                self.additional_exported_cols.append(self.ROWMEAN_KEYWORD)
                self.table[self.ROWMEAN_KEYWORD] = self.table.mean(axis=1)

        # apply filters (if any)
        if len(filters) > 1:
            combined_filter = reduce(lambda x,y: x & y, filters)
            self.table = self.table.loc[combined_filter]
        elif len(filters) == 1:
            self.table = self.table.loc[filters[0]]            

    def get_contents(self, resource_instance, query_params={}, preview=False):
        '''
        This method allows us to add on additional content that is
        allowable for matrix types, as they are all numeric.
        Examples include providing the row means, etc.
        '''
        # pull out any matrix-specific query params so they don't interfere
        # with the general query params passed to the parent.
        # If the request doesn't contain any of these params, then 
        # the self.current_query_params dict is empty and no further
        # modifications to the returned content are made.
        self.extra_query_params = {}
        for p in self.EXTRA_MATRIX_QUERY_PARAMS:
            if p in query_params:
                self.extra_query_params[p] = query_params[p]

        # additional filtering/behavior specific to a Matrix (if requested)
        # is handled in the _resource_specific_modifications method
        return super().get_contents(resource_instance, query_params, preview)


class IntegerMatrix(Matrix):
    '''
    An `IntegerMatrix` further specializes the `Matrix`
    to admit only integers.
    '''
    # looking for only integers. 
    TARGET_PATTERN = 'int\d{0,2}'

    DESCRIPTION = 'A table where all the entries are integers'\
        ' except the first column (which names the rows) and the' \
        ' first line (which gives the column names). The cell at the' \
        ' first row and column may be left blank.'

    EXAMPLE = [
        {
            "rowname":"gene1",
            "values": {
                "sampleA": 1,
                "sampleB": 2,
                "sampleC": 3,
            }
        },
        {
            "rowname":"gene2",
            "values": {
                "sampleA": 11,
                "sampleB": 22,
                "sampleC": 33,
            }
        },
        {
            "rowname":"gene3",
            "values": {
                "sampleA": 111,
                "sampleB": 222,
                "sampleC": 333,
            }
        }
    ]

    def validate_type(self, resource_instance, file_format):
        # first check that it has all numeric types.  If that fails
        # immediately return--
        is_valid, error_message = super().validate_type(resource_instance, file_format)
        
        if not is_valid:
            return (False, error_message)

        # was valid for numeric types.  Now check for integer
        problem_columns = self.check_column_types(IntegerMatrix.TARGET_PATTERN)
        if len(problem_columns) > 0:

            # one problem with pandas is that NaN values cause a column
            # to be parsed as a float, even if all other values in the 
            # column are integers.  We can do a secondary check, however, 
            # to see if the remaining values (non-NaN) are basically
            # integers.  We check if the numbers look like "2.0".
            # If that is the case, we remove that column from the 
            # "problem columns".  
            for i,c in enumerate(problem_columns):
                # recall c is a tuple of (colname, col number)
                if all([
                        re.match('\d+\.0', str(x)) 
                        for x in self.table[c[0]].dropna()]):
                    problem_columns.pop(i)

            # if there are still any remaining problematic cols,
            # we now issue an error
            if len(problem_columns) > 0:
                error_message = NON_INTEGER_ERROR
                return (False, error_message)
            
        return (True, None)


class RnaSeqCountMatrix(IntegerMatrix):
    '''
    A very-explicit class which extends the IntegerMatrix to PREVENT
    negative counts
    '''
    DESCRIPTION = 'A table of integer-based counts corresponding to'\
        ' the number of sequencing reads associated with a particular' \
        ' gene or transcript. Note that all counts must be greater than or' \
        ' equal to zero-- negative numbers are not permitted.'

    EXAMPLE = IntegerMatrix.EXAMPLE

    def validate_type(self, resource_instance, file_format):
        '''
        Asserts that we don't allow negatives. Extends all the 
        other requirements of an IntegerMatrix
        '''
        is_valid, error_message = super().validate_type(resource_instance, file_format)
        
        if not is_valid:
            return (False, error_message)

        # if here, we had a valid InterMatrix. Now assert all entries are positive.
        # Double sum since the first sum will return a pd.Series of column sums.
        # As of writing, no way to reduce over both axes simultaneously.
        if (self.table < 0).sum().sum() > 0:
            return (False, NEGATIVE_COUNTS_ERROR)

        # all good!
        return (True, None)


class Network(Matrix):
    '''
    An explicit class that helps users disambiguate matrices that are
    intended to represent graph data structures (e.g. an adajacency matrix)
    '''

    DESCRIPTION = 'A table that represents a network or graph.'\
        ' Both the first column and first row provide the names of the graph\'s nodes' \
        ' which can be different, in general. Each cell of the table provides information' \
        ' about the connection between the nodes given by the corresponding row and column.' \
        ' An example is an adjacency matrix, where a value of 1 indicates a direct connection/edge' \
        ' between the nodes of interest (and zero indicates no connection).'\
        ' Alternatively, a general numeric value can represent the' \
        ' strength of connection/evidence as might be the case for an inferred regulatory network.'

    EXAMPLE = [
        {
            "rowname":"gene1",
            "values": {
                "geneA": 1.1,
                "geneB": 2.2,
                "geneC": 3.3,
            }
        },
        {
            "rowname":"gene2",
            "values": {
                "geneA": 11.1,
                "geneB": 22.2,
                "geneC": 33.3,
            }
        },
        {
            "rowname":"gene3",
            "values": {
                "geneA": 111.1,
                "geneB": 222.2,
                "geneC": 333.3,
            }
        }
    ]


class ElementTable(TableResource):
    '''
    An ElementTable captures common behavior of tables which
    annotate `Observation`s (AnnotationTable) or `Feature`s (FeatureTable)

    It's effectively an abstract class. See the derived classes which implement
    the specific behavior for `Observation`s or `Feature`s.
    '''

    ACCEPTABLE_FORMATS = [
        CSV_FORMAT,
        TSV_FORMAT,
        XLS_FORMAT,
        XLSX_FORMAT
    ]

    MAX_OBSERVATIONS = 10000
    MAX_FEATURES = 100

    def validate_type(self, resource_instance, file_format):

        # check that file can be parsed:
        is_valid, error_message = super().validate_type(resource_instance, file_format)
        if not is_valid:
            return (False, error_message)
        
        # check that the file is "useful" in that it has
        # more than one column.  It's not REALLY an error, but it does not 
        # provide any information.  This can also be caught earlier, but
        # we provide it here just as a secondary guard.
        if self.table.shape[1] == 0:
            return (False, TRIVIAL_TABLE_ERROR)
        return (True, None)

    def prep_metadata(self, element_class):
        '''
        When we extract the metadata from an ElementTable, we 
        expect the Element instances (Observations or Features) 
        to be contained in the rows.  

        Additional columns specify attributes which we incorporate.

        The `element_class` arg is a class which implements the specific
        type we want (i.e. Observation or Feature)
        '''
        logger.info(f'For element type {type(element_class)},'
            ' extract out metadata')

        if (self.table.shape[0] > ElementTable.MAX_OBSERVATIONS) \
            or \
            (self.table.shape[1] > ElementTable.MAX_FEATURES):
            logger.info('The annotation matrix of size'
                f' ({self.table.shape[0]}, {self.table.shape[1]}) was too'
                ' large. Returning empty metadata')
            return []

        # Go through the columns and find out the primitive types
        # for each column/covariate.
        # Note that we can't determine specific types (e.g. bounded integers)
        # from general annotations.  We basically allow floats, integers, and
        # "other" types, which get converted to strings.
        # Also note that we pass the `allow_unrestricted_strings` kwarg
        # which will allow the attribute values to take on arbitrary string
        # values. 
        type_dict = self.get_type_dict(allow_unrestricted_strings=True)

        # convert NaN and infs to our special marker values
        self.table = TableResource.replace_special_values(self.table)

        element_list = []
        # Note that we do the `astype('object')` cast to address
        # the following pandas issue:
        # https://github.com/pandas-dev/pandas/issues/12859
        # Namely, columns known to be integers can have their
        # values cast to floats, which can violate the type
        # constraints.
        for id, row_series in self.table.astype('object').iterrows():
            d = row_series.to_dict()
            attr_dict = {}
            for key, val in d.items():
                # Note the 'allow_null=True', so that attributes can 
                # be properly serialized if they are missing a value. 
                # This happens, for instance, in FeatureTable
                # instances where p-values were not assigned.
                attr = SimpleAttributeFactory(
                    {
                        'attribute_type': type_dict[key],
                        'value': val
                    },
                    allow_null=True
                )
                # We create `attr` above as a means to verify
                # the attribute is correctly formatted
                attr_dict[key] = attr.to_dict()

            # Now create the data_structures.element.Element instance

            # adding the 'permit_null_attributes' kwarg
            # allows us to ignore rows that might be missing
            # an entry.
            element = element_class(
                {
                    'id': id,
                    'attributes': attr_dict
                },
                permit_null_attributes=True
            )
            # the dict representation of the Element has
            # an `attribute_type` and `value` key. We only
            # want the value:
            element_list.append(element.to_dict()['value'])
        return element_list


class AnnotationTable(ElementTable):
    '''
    An `AnnotationTable` is a special type of table that will be responsible
    for annotating Observations/samples (e.g. adding sample names and 
    associated attributes like experimental group or other covariates)

    The first column will give the sample names and the remaining columns will
    each individually represent different covariates associated with that sample.
    '''

    DESCRIPTION = 'This type of file is used to add metadata to your samples.' \
        ' The first column has the sample name and the remaining columns contain' \
        ' metadata about each sample (for instance, experimental group,'\
        ' treatment, or similar).' 

    EXAMPLE = [
        {
            "rowname":"SampleA",
            "values": {
                "cell_type": 'CD4',
                "treatment": 'Y'
            }
        },
        {
            "rowname":"SampleB",
            "values": {
                "cell_type": 'CD8',
                "treatment": 'Y'
            }
        },
        {
            "rowname":"SampleC",
            "values": {
                "cell_type": 'Monocyte',
                "treatment": 'N'
            }
        }
    ]

    def validate_type(self, resource_instance, file_format):

        # check that file can be parsed:
        is_valid, error_message = super().validate_type(resource_instance, file_format)
        if not is_valid:
            return (False, error_message)

        # it is hard to check for proper headers for annotation
        # files since they have relatively free format.  However,
        # if the column name matches any values in its column, the
        # annotation was likely missing a header line.  For example,
        # >>> df
        # SW1_Control   CTRL
        # 0  SW2_Control   CTRL
        # 1  SW3_Control   CTRL
        # 2  SW4_Treated  TREAT
        # 3  SW5_Treated  TREAT
        # 4  SW6_Treated  TREAT
        #
        # Here, "CTRL" becomes the header, but it's clearly 
        # just due to a missing header.  We don't issue an 
        # error, but we do warn the user by adding a comment.
        flagged_columns = []
        for c in self.table.columns:
            if np.sum(c == self.table[c]) > 0:
                flagged_columns.append(c)
        if len(flagged_columns) > 0:
            return (True, MISSING_HEADER_WARNING)

        return (True, None)

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        '''
        When we extract the metadata from an AnnotationTable, we 
        expect the Observation instances to be the rows.  

        Additional columns specify attributes of each Observation,
        which we incorporate
        '''
        logger.info('Extract metadata for an AnnotationTable instance.')
        super().extract_metadata(resource_instance, parent_op_pk=parent_op_pk)
        observation_list = super().prep_metadata(Observation)
        o_set = ObservationSet({'elements': observation_list}, 
            permit_null_attributes=True)
        self.metadata[OBSERVATION_SET_KEY] = o_set.to_simple_dict()
        return self.metadata


class FeatureTable(ElementTable):
    '''
    A `FeatureTable` is a type of table that has aggregate information about
    the features, but does not have any "observations" in the columns.  An example
    would be the results of a differential expression analysis.  Each row corresponds
    to a gene (feature) and the columns are information about that gene (such as p-value).

    Another example could be a table of metadata about genes (e.g. pathways or perhaps a 
    mapping to a different gene identifier).

    The first column will give the feature/gene identifiers and the remaining columns will
    have information about that gene
    '''

    DESCRIPTION = 'This type of file describes the "features" of your data.  In the genomics' \
        ' context, one example of a feature is a gene.  Therefore, you could use this table' \
        ' to give additional information about each gene, such as alternative symbols,' \
        ' oncogene status, or similar.  Each row contains information about a single gene.' \
        ' Note, however, that this concept is completely general and not restricted' \
        ' to information about genes or transcripts.'

    EXAMPLE = [
        {
            "rowname":"gene1",
            "values": {
                "logFoldChange": 1.1,
                "pvalue": 0.03,
                "FDR": 0.2,
            }
        },
        {
            "rowname":"gene2",
            "values": {
                "logFoldChange": 2.2,
                "pvalue": 0.01,
                "FDR": 0.03,
            }
        },
        {
            "rowname":"gene3",
            "values": {
                "logFoldChange": 3.3,
                "pvalue": 0.000003,
                "FDR": 0.001,
            }
        }
    ]

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        '''
        When we extract the metadata from a FeatureTable, we 
        expect the Feature instances to be the rows.  

        Additional columns specify attributes of each Feature,
        which we incorporate
        '''
        logger.info('Extract metadata from a FeatureTable')
        super().extract_metadata(resource_instance, parent_op_pk=parent_op_pk)

        # Note: removed the addition of FeatureSets to the metadata as it was causing
        # issues with large json objects being inserted into the database.
        # feature_list = super().prep_metadata(Feature)
        # f_set = FeatureSet(feature_list)
        # self.metadata[FEATURE_SET_KEY] = FeatureSetSerializer(f_set).data
        return self.metadata


class BaseBEDFile(TableResource):
    '''
    A "base" BED file format that handles common elements of all BED
    (and extended BED) formats; corresponds to the BED3 format.  This is
    the minimal BED format, which has:

    - chromosome
    - start position
    - end position

    Additional columns are ignored.

    By default, BED files do NOT contain headers and we enforce that here.
    '''

    ACCEPTABLE_FORMATS = [TSV_FORMAT,]

    def _read_resource(self, resource_instance, names, column_numbers, preview=False):

        # Note that we don't use the TableResource.read_resource since we have
        # a different way of parsing the table here.
        reader = TableResource.get_reader(TSV_FORMAT)

        if preview:
            nrows = PREVIEW_NUM_LINES
        else:
            nrows = None

        # if the BED file has a header, the reader below will incorporate
        # that into the columns and the 2nd and 3rd columns will no longer have
        # the proper integer type.
        try:
            self.table = reader(resource_instance.datafile.open(), 
                names=names,
                usecols=column_numbers,
                nrows=nrows)
        except pd.errors.ParserError as ex:
            logger.info(f'Failed to parse. Exception: {ex}')
            raise ex

        # check if any NAs- not permissible.
        # TODO: can change this awkward double sum application
        #       to sum(axis=None) if pandas==v2.0.0
        if self.table.isna().sum().sum() > 0:
            logger.info(f'Encountered a "NA" value. Table was {self.table}')
            raise Exception('Encountered one or more "NA" values. This is'
                ' often caused by blank table cells or using spaces instead'
                ' of tabs to separate fields.')

    def save_in_standardized_format(self, resource_instance, current_file_format):
        '''
        We override this method from TableResource since BED (and related) files
        do NOT have headers.

        Since the original file passed validation, we do nothing-- it was
        already in the desired format.
        '''
        logger.info(f'Saving BED-like resource ({resource_instance.pk})'
            ' to the standard TSV format. Nothing to do.')
        pass

    def _validate(self):

        start_col_int = re.match('int\d{0,2}', str(self.table['start'].dtype))
        stop_col_int = re.match('int\d{0,2}', str(self.table['stop'].dtype))
        if start_col_int and stop_col_int:
            return (True, None)
        else:
            problem_columns = []
            if start_col_int is None:
                problem_columns.append(2)
            if stop_col_int is None:
                problem_columns.append(3)

            cols = ','.join([str(x) for x in problem_columns])
            error_message = BED_FORMAT_ERROR.format(cols=cols)
            return (False, error_message)

    def extract_metadata(self, resource_instance, parent_op_pk=None):
        super().extract_metadata(resource_instance, parent_op_pk=parent_op_pk)
        return self.metadata


class BED3File(BaseBEDFile):
    '''
    A basic three-column BED file format (BED3):
    - chromosome
    - start position
    - end position

    Additional columns are ignored.
    '''
    DESCRIPTION = 'A three-column BED-format file (https://ensembl.org/info/website/upload/bed.html)'\
        ' BED files do NOT have column headers and are expected to be stored' \
        ' in a tab-delimited format.'

    NAMES = ['chrom', 'start', 'stop']
    COLUMN_NUMBERS = list(range(3))

    def read_resource(self, resource_instance, preview=False):
        self._read_resource(resource_instance,
            BED3File.NAMES, BED3File.COLUMN_NUMBERS, preview=preview)

    def validate_type(self, resource_instance, file_format):
        try:
            self.read_resource(resource_instance)
        except Exception as ex:
            return (False, str(ex))

        return super()._validate()


class BED6File(BED3File):
    '''
    An extended six-column BED file format:

    - chromosome
    - start position
    - end position
    - name
    - score
    - strand

    Additional columns are ignored.

    By default, BED files do NOT contain headers and we enforce that here.
    '''
    DESCRIPTION = 'A six-column BED-format file (columns 1 through 6 defined at'\
        ' https://ensembl.org/info/website/upload/bed.html)'\
        ' BED files do NOT have column headers and are expected to be stored' \
        ' in a tab-delimited format.'

    NAMES = ['chrom', 'start', 'stop', 'name', 'score', 'strand']
    COLUMN_NUMBERS = list(range(6))
    ACCEPTABLE_STRAND_VALUES = set(['.', '+', '-'])

    def read_resource(self, resource_instance, preview=False):
        self._read_resource(resource_instance,
            BED6File.NAMES, BED6File.COLUMN_NUMBERS, preview=preview)

    def _validate(self):
        is_valid, error_message = super()._validate()
                
        if is_valid: #passes BED3
            error_message = ''

            # ensure the score and strand columns are OK.
            score_valid, err = self._check_score_column()
            error_message += err

            strand_valid, err = self._check_strand_column()
            error_message += '.' + err

            if score_valid and strand_valid:
                return (True, '')
            else:
                return (False, error_message)
        else:
            # did not pass the check on the first 3 columns
            return (is_valid, error_message)

    def validate_type(self, resource_instance, file_format):
        try:
            self.read_resource(resource_instance)
        except Exception as ex:
            return (False, str(ex))
        return self._validate()

    def _check_strand_column(self):
        unique_strand_values = set(self.table['strand'].unique())
        diff_set = unique_strand_values.difference(
            BED6File.ACCEPTABLE_STRAND_VALUES)
        if len(diff_set) > 0:
            error_message = ('Your 6th column (strand) contained'
            ' unexpected values. We accept only'
            f' {",".join(BED6File.ACCEPTABLE_STRAND_VALUES)}'
            f' and yours contained {",".join(diff_set)}.')
            return (False, error_message)
        return (True, '') 

    def _check_score_column(self):
        # the score column needs to be integers between 0 and 1000
        score_col_int = re.match('int\d{0,2}', str(self.table['score'].dtype))
        if score_col_int:
            min_score = self.table['score'].min()
            max_score = self.table['score'].max()
            if (min_score < 0) or (max_score > 1000):
                error_message = ('BED format requires that the 5th column'
                ' (score) has integer values between 0 and 1000. Your'
                f' range of [{min_score},{max_score}] exceeds this.')
                return (False, error_message)
            return (True, '')
        else:
            # score column was not an integer
            error_message = ('Please check that the 5th column (score)'
            ' has only integer values between 0 and 1000. We could not'
            ' parse this column as only integers. If empty, simply fill'
            ' with all zeroes.')
            return (False, error_message)


class NarrowPeakFile(BED6File):
    '''
    An extended ten-column BED file format:
    http://genome.ucsc.edu/FAQ/FAQformat.html#format12
    - chromosome
    - start position
    - end position
    - name
    - score
    - strand
    - signalValue (number)
    - p_value (float or -1)
    - q_value (float or -1)
    - peak (0-based offset, or -1)

    Note that the p and q values are transformed 
    via -log10, so their range is [0,+inf)
    '''
    DESCRIPTION = 'A 10-column BED-format file (columns 1 through 6 defined at'\
        ' http://genome.ucsc.edu/FAQ/FAQformat.html#format12.'\
        ' BED files do NOT have column headers and are expected to be stored' \
        ' in a tab-delimited format.'

    NAMES = BED6File.NAMES + ['signal_value', 'pval', 'qval', 'peak']
    COLUMN_NUMBERS = list(range(10))

    def read_resource(self, resource_instance, preview=False):
        self._read_resource(resource_instance,
            NarrowPeakFile.NAMES, NarrowPeakFile.COLUMN_NUMBERS,
            preview=preview)

    def _check_signal_column(self):
        '''
        Signal column is simply a number. No constraints specified in the spec.
        '''
        if re.match('(int|float)\d{0,2}', 
            str(self.table['signal_value'].dtype)):
            return (True, '')
        else:
            error_message = ('The 7th column needs to have only numbers.')
            return (False, error_message)

    def _check_peak_column(self):
        '''
        The 10th column is supposed to be a 0-based offset or -1. Thus,
        we only permit the union of positive integers and -1
        '''
        error_message = ('The 10th is a either 0-based offset (relative to'
                ' chromosome start) or -1. This permits only integers. Please'
                ' check your 10th column.')

        if re.match('int\d{0,2}', 
            str(self.table['peak'].dtype)):
            values = self.table['peak']
            negative_values = values[values < 0].unique()
            if len(negative_values) == 1:
                if negative_values[0] != -1:
                    error_message += f' Found a value of {negative_values[0]}'
                    return (False, error_message)
                return (True, '')
            elif len(negative_values) > 1:
                negative_values.sort()
                error_message += (' Found values of'
                    f' {",".join([str(x) for x in negative_values])}')
                return (False, error_message)
            else:
                # if here, there were no negative values
                return  (True, '')
        else:
            return (False, error_message)

    def _check_p_or_q_column(self, col_name):
        '''
        This handles validating the p and q-value columns 
        (8th or 9th col in 1-based index).
        These can be floats on [0,+inf] or -1 if not provided.
        `col_num` should be the zero-based index
        '''
        col_num = 7 if col_name == 'p-value' else 8

        # needs to at least by numerical, marked by either int or float dtypes
        if re.match('(float|int)\d{0,2}', str(self.table[col_name].dtype)):
            values = self.table[col_name]
            negative_values = values[values < 0]
            if not np.allclose(negative_values, -1.0):
                error_message = ('The significance values in column'
                    f' {col_num + 1} ({col_name}) should be positive or -1.'
                    ' Found negative values other than -1.')
                return (False, error_message)
            return (True, '')
        else:
            error_message = (f'The {col_num + 1}th column ({col_name})'
                ' needs to be a number. Non-numerical values were found.')
            return (False, error_message)

    def _validate(self):
        is_valid, error_message = super()._validate()
        if is_valid: # passes BED6 format (and, by transitive property, BED3)
            error_message = ''

            # ensure the score and strand columns are OK.
            signal_valid, err = self._check_signal_column()
            error_message += err

            pval_valid, err = self._check_p_or_q_column('pval')
            error_message += '. ' + err

            qval_valid, err = self._check_p_or_q_column('qval')
            error_message += '. ' + err

            peak_valid, err = self._check_peak_column()
            error_message += '. ' + err

            if all([signal_valid, pval_valid, qval_valid, peak_valid]):
                return (True, '')
            else:
                return (False, error_message)
        else:
            return (False, error_message)

    def validate_type(self, resource_instance, file_format):
        try:
            self.read_resource(resource_instance)
        except Exception as ex:
            return (False, str(ex))
        return self._validate()

    
